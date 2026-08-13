//! Strategy intent store.
//!
//! Holds posted [`SignedStrategyExecution`]s ("intents") until the scooper can
//! execute them against their target strategy orders. Intents arrive via the
//! admin server's `POST /v4/strategy-intents` endpoint (and, eventually, via
//! gossip from peer scoopers).
//!
//! Hygiene rules (dead intents must not pile up or become a DoS vector):
//! - Reject at ingest: undecodable CBOR, unknown/spent order, order without a
//!   strategy constraint, unsatisfiable auth, empty or already-expired window.
//! - Drop on events: order spent (any scooper executed it) or window expired.
//! - Hard caps: total stored intents and per-order intents.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use pallas_crypto::hash::Hasher;
use pallas_primitives::PlutusData;
use plutus_parser::AsPlutus;

use crate::multisig::Multisig;
use crate::persistence::{PersistedStrategyIntent, StrategyIntentDao};
use crate::sundaev4::types::{
    Constraint, IntervalBoundType, SignedStrategyExecution, SundaeV4Order,
};

/// Hard cap on total stored intents. Posting is unauthenticated (signatures
/// prove authority over an *order*, not over the scooper), so the store must
/// stay bounded no matter what arrives.
const MAX_TOTAL_INTENTS: usize = 10_000;
// There is no per-order cap: an order holds at most one live intent, so the
// total cap above is the only bound needed.
//
// One live intent per order: a new (valid) intent replaces the previous
// one. The winner is ranked by the intent *content* first — max by
// (validity lower bound, expiry) — so peers converge on the same intent no
// matter what order gossip delivers them in. The lower bound acts as the
// signing timestamp: a replacement signed later carries a later valid-from
// (the CLI signs valid-from ≈ now), so it beats the standing intent even
// when both are open-ended.
//
// When content ties — signers that leave valid-from unset (it reads as 0)
// and reuse the same expiry produce exact ties — receipt time breaks it, so
// the intent posted last replaces the one already held. Ranking straight on
// intent_id there (as this once did) meant a freshly posted replacement
// could silently lose to the intent it was meant to supersede, with no way
// for the poster to force it through. Only when receipt times also tie (same
// millisecond) does the id decide, which keeps the outcome deterministic for
// simultaneous arrivals.

/// Optional client-provided hint about how to execute an intent. Extensible
/// by adding variants; unknown `type` values are rejected at the API boundary
/// so a client knows immediately that this scooper can't honor the hint.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ExecutionHint {
    /// Execute as a claim against the given pool (ident hex). Used by the
    /// RealFi peg stability module: its strategies will likely never be
    /// satisfiable as swaps, only as CS bounty claims.
    Claim { pool: String },
}

/// Key identifying the order an intent targets.
pub type OrderKey = (Vec<u8>, u64);

#[derive(Debug, Clone)]
pub struct StoredIntent {
    /// blake2b-256 of the SSE wire bytes — dedup key for storage and gossip.
    pub intent_id: Vec<u8>,
    pub sse: SignedStrategyExecution,
    /// Wire bytes as received. The signature covers the CBOR of the
    /// `execution` field, so these bytes (not a re-encoding) are what gets
    /// gossiped onward.
    pub sse_cbor: Vec<u8>,
    pub hint: Option<ExecutionHint>,
    pub expiry_ms: u64,
    pub received_at_ms: u64,
}

/// Outcome of a successful submit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SubmitOutcome {
    #[serde(serialize_with = "hex_id")]
    pub intent_id: Vec<u8>,
    /// False if we already held this exact intent (gossip echo) — callers
    /// should only re-gossip when true.
    pub newly_stored: bool,
    /// True when this intent lost to a live intent for the same order with
    /// a later validity window (or was previously replaced by one): it was
    /// not stored and will not execute.
    pub superseded: bool,
    /// Previously-live intents for the same order this submission replaced.
    /// The caller marks them terminal in persistence. Not serialized:
    /// intent ids are query capabilities.
    #[serde(skip)]
    pub replaced: Vec<Vec<u8>>,
}

fn hex_id<S: serde::Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&hex::encode(v))
}

/// Terminal outcome of an intent, retained (small) after the live entry is
/// gone so the submitter can still query it by intent id. The intent id is
/// the capability: it's blake2b-256 of the signed execution bytes, so only
/// someone holding those bytes (the submitter) can derive it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct IntentTombstone {
    /// "executed" | "expired" | "order-gone" | "replaced"
    pub status: &'static str,
    /// For "executed": the spending tx hash (any scooper's — read from chain).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<Vec<u8>>,
    /// When the tombstone itself can be deep-cleaned.
    pub cleanup_after_ms: u64,
}

/// How long a terminal status stays queryable.
const TOMBSTONE_TTL_MS: u64 = 24 * 60 * 60 * 1000;

#[derive(Default)]
pub struct IntentStore {
    /// Intents keyed by target order, each order's list ordered by arrival.
    by_order: BTreeMap<OrderKey, Vec<StoredIntent>>,
    /// Terminal statuses by intent id.
    tombstones: BTreeMap<Vec<u8>, IntentTombstone>,
    total: usize,
}

impl IntentStore {
    /// Rehydrate from persistence, dropping anything already expired.
    pub async fn load(dao: &dyn StrategyIntentDao, now_ms: u64) -> Result<Self> {
        let mut store = Self::default();
        let mut dead: Vec<Vec<u8>> = Vec::new();
        for p in dao.load_intents().await? {
            if let Some(status) = p.status.as_deref() {
                let status_static: &'static str = match status {
                    "executed" => "executed",
                    "order-gone" => "order-gone",
                    "replaced" => "replaced",
                    _ => "expired",
                };
                store.tombstones.insert(
                    p.intent_id.clone(),
                    IntentTombstone {
                        status: status_static,
                        tx_hash: p.status_tx.clone(),
                        cleanup_after_ms: p.expiry_ms.saturating_add(TOMBSTONE_TTL_MS),
                    },
                );
                if now_ms > p.expiry_ms.saturating_add(TOMBSTONE_TTL_MS) {
                    store.tombstones.remove(&p.intent_id);
                    dead.push(p.intent_id);
                }
                continue;
            }
            if p.expiry_ms <= now_ms {
                store.tombstones.insert(
                    p.intent_id.clone(),
                    IntentTombstone {
                        status: "expired",
                        tx_hash: None,
                        cleanup_after_ms: p.expiry_ms.saturating_add(TOMBSTONE_TTL_MS),
                    },
                );
                dead.push(p.intent_id);
                continue;
            }
            let Ok(pd) = minicbor::decode::<PlutusData>(&p.sse_cbor) else {
                dead.push(p.intent_id);
                continue;
            };
            let Ok(sse) = SignedStrategyExecution::from_plutus(pd) else {
                dead.push(p.intent_id);
                continue;
            };
            let hint = p.hint.as_deref().and_then(|h| serde_json::from_str(h).ok());
            store.insert_unchecked(StoredIntent {
                intent_id: p.intent_id,
                sse,
                sse_cbor: p.sse_cbor,
                hint,
                expiry_ms: p.expiry_ms,
                received_at_ms: p.received_at_ms,
            });
        }
        dao.delete_intents(&dead).await?;
        Ok(store)
    }

    fn insert_unchecked(&mut self, intent: StoredIntent) {
        let key = order_key(&intent.sse);
        self.by_order.entry(key).or_default().push(intent);
        self.total += 1;
    }

    /// Validate and store a posted intent. `find_order` resolves an order by
    /// (tx_id, index) from the *current* indexed state — returning `None`
    /// means unknown or already spent, either way the intent is refused.
    ///
    /// Does NOT persist — the caller persists on `newly_stored` (persistence
    /// is async and the store sits behind a sync mutex).
    /// Replacement rank of an intent: (validity lower bound, expiry,
    /// received-at, id). Later-signed intents (later valid-from) win; when
    /// the signed window is identical the intent received later wins, so a
    /// re-post always replaces what it targets; the id breaks same-millisecond
    /// ties deterministically across gossiping peers.
    fn replacement_rank(
        sse: &SignedStrategyExecution,
        expiry_ms: u64,
        received_at_ms: u64,
        id: &[u8],
    ) -> (u64, u64, u64, Vec<u8>) {
        let lower_ms = match &sse.execution.validity_range.lower_bound.bound_type {
            IntervalBoundType::Finite(t) => big_to_u64(t).unwrap_or(0),
            _ => 0,
        };
        (lower_ms, expiry_ms, received_at_ms, id.to_vec())
    }

    pub fn submit(
        &mut self,
        sse_cbor: Vec<u8>,
        hint: Option<ExecutionHint>,
        find_order: impl Fn(&OrderKey) -> Option<std::sync::Arc<SundaeV4Order>>,
        describe_invalid: impl Fn(&OrderKey) -> Option<String>,
        now_ms: u64,
    ) -> Result<(SubmitOutcome, Option<StoredIntent>)> {
        let pd: PlutusData = minicbor::decode(&sse_cbor)
            .map_err(|e| anyhow::anyhow!("signed_execution is not valid CBOR PlutusData: {e}"))?;

        // Extract the execution field's *wire bytes* before typed decoding:
        // the signature covers exactly these bytes (the on-chain validator
        // recomputes them via cbor.serialise), so verification must use the
        // submitted encoding, never a re-encode with different conventions.
        let PlutusData::Constr(ref c) = pd else {
            bail!("signed_execution must be a Constr");
        };
        let fields: Vec<PlutusData> = c.fields.clone().to_vec();
        let execution_pd = fields
            .first()
            .context("signed_execution missing execution field")?;
        let payload = minicbor::to_vec(execution_pd)
            .map_err(|e| anyhow::anyhow!("re-encode execution: {e}"))?;

        let sse = SignedStrategyExecution::from_plutus(pd.clone())
            .map_err(|e| anyhow::anyhow!("decode SignedStrategyExecution: {e}"))?;

        // Window checks.
        let expiry_ms = match &sse.execution.validity_range.upper_bound.bound_type {
            IntervalBoundType::Finite(t) => {
                big_to_u64(t).context("validity upper bound out of range")?
            }
            IntervalBoundType::PositiveInfinity => u64::MAX,
            IntervalBoundType::NegativeInfinity => bail!("validity window is empty"),
        };
        if expiry_ms <= now_ms {
            bail!("intent is already expired (upper bound {expiry_ms} <= now {now_ms})");
        }

        // The target order must exist, be unspent, and carry a strategy
        // constraint whose auth this intent's signatures satisfy.
        let key = order_key(&sse);
        let order = match find_order(&key) {
            Some(o) => o,
            None => {
                // Distinguish "we indexed this order but it's unparseable" from
                // a genuine miss, so the poster learns *why* it can't be used.
                if let Some(reason) = describe_invalid(&key) {
                    bail!(
                        "order {}#{} is malformed and cannot accept intents: {reason}",
                        hex::encode(&key.0),
                        key.1
                    );
                }
                bail!(
                    "order {}#{} not found (unknown, spent, or not yet indexed)",
                    hex::encode(&key.0),
                    key.1
                );
            }
        };
        let Constraint::Strategy { ref constraints } = order.constraint else {
            bail!("target order does not carry a strategy constraint");
        };
        if !multisig_satisfied(&constraints.auth, &payload, &sse.signatures, now_ms) {
            bail!("signatures do not satisfy the order's strategy auth");
        }

        // Bounds on the optional final destination index.
        if let Some(ref idx) = sse.execution.final_destination {
            let i = big_to_u64(idx).context("final destination index out of range")?;
            if i as usize >= constraints.final_destinations.len() {
                bail!(
                    "final destination index {i} out of bounds ({} configured)",
                    constraints.final_destinations.len()
                );
            }
        }

        let intent_id = Hasher::<256>::hash(&sse_cbor).to_vec();

        // Already terminal (executed / expired / replaced / order-gone):
        // gossip echoes of dead intents must not resurrect them.
        if let Some(t) = self.tombstones.get(&intent_id) {
            let superseded = t.status == "replaced";
            return Ok((
                SubmitOutcome { intent_id, newly_stored: false, superseded, replaced: vec![] },
                None,
            ));
        }

        let entry = self.by_order.entry(key).or_default();
        if let Some(existing) = entry.iter_mut().find(|i| i.intent_id == intent_id) {
            // Duplicate bytes. A hint can still be attached or replaced —
            // latest Some(hint) wins; None means "no opinion", keep what we
            // have. Returning the updated intent makes the caller persist
            // and re-gossip it, so hint updates propagate and converge.
            if hint.is_some() && existing.hint != hint {
                existing.hint = hint;
                let updated = existing.clone();
                return Ok((
                    SubmitOutcome {
                        intent_id,
                        newly_stored: false,
                        superseded: false,
                        replaced: vec![],
                    },
                    Some(updated),
                ));
            }
            return Ok((
                SubmitOutcome {
                    intent_id,
                    newly_stored: false,
                    superseded: false,
                    replaced: vec![],
                },
                None,
            ));
        }

        // One live intent per order — see replacement_rank for who wins.
        let new_rank = Self::replacement_rank(&sse, expiry_ms, now_ms, &intent_id);
        if let Some(best) = entry
            .iter()
            .map(|i| Self::replacement_rank(&i.sse, i.expiry_ms, i.received_at_ms, &i.intent_id))
            .max()
        {
            if best > new_rank {
                // The incoming intent loses: tombstone it in memory so echoes
                // die quickly, but don't disturb the winner.
                self.tombstones.insert(
                    intent_id.clone(),
                    IntentTombstone {
                        status: "replaced",
                        tx_hash: None,
                        cleanup_after_ms: now_ms.saturating_add(TOMBSTONE_TTL_MS),
                    },
                );
                return Ok((
                    SubmitOutcome {
                        intent_id,
                        newly_stored: false,
                        superseded: true,
                        replaced: vec![],
                    },
                    None,
                ));
            }
        }

        if self.total >= MAX_TOTAL_INTENTS {
            bail!("intent store is full (max {MAX_TOTAL_INTENTS})");
        }

        // The incoming intent wins: replace whatever was live for this order.
        let mut replaced = Vec::new();
        for old in entry.drain(..) {
            self.tombstones.insert(
                old.intent_id.clone(),
                IntentTombstone {
                    status: "replaced",
                    tx_hash: None,
                    cleanup_after_ms: now_ms.saturating_add(TOMBSTONE_TTL_MS),
                },
            );
            replaced.push(old.intent_id);
            self.total -= 1;
        }

        let stored = StoredIntent {
            intent_id: intent_id.clone(),
            sse,
            sse_cbor,
            hint,
            expiry_ms,
            received_at_ms: now_ms,
        };
        entry.push(stored.clone());
        self.total += 1;
        Ok((
            SubmitOutcome { intent_id, newly_stored: true, superseded: false, replaced },
            Some(stored),
        ))
    }

    /// Intents currently valid for `key`: window open at `now_ms`. Callers
    /// still re-check order liveness — the store lags the chain slightly.
    pub fn valid_for_order(&self, key: &OrderKey, now_ms: u64) -> Vec<&StoredIntent> {
        self.by_order
            .get(key)
            .map(|v| {
                v.iter()
                    .filter(|i| {
                        i.expiry_ms > now_ms
                            && window_open_at(&i.sse, now_ms).unwrap_or(false)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// All stored intents (for the observability endpoint).
    pub fn all(&self) -> impl Iterator<Item = &StoredIntent> {
        self.by_order.values().flatten()
    }

    pub fn len(&self) -> usize {
        self.total
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Expire live intents past their window; returns the transitioned ids
    /// so the caller persists the terminal status. Also deep-cleans stale
    /// tombstones (returned separately for deletion).
    pub fn prune_expired(&mut self, now_ms: u64) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let mut expired = Vec::new();
        self.by_order.retain(|_, intents| {
            intents.retain(|i| {
                if i.expiry_ms <= now_ms {
                    self.tombstones.insert(
                        i.intent_id.clone(),
                        IntentTombstone {
                            status: "expired",
                            tx_hash: None,
                            cleanup_after_ms: i.expiry_ms.saturating_add(TOMBSTONE_TTL_MS),
                        },
                    );
                    expired.push(i.intent_id.clone());
                    false
                } else {
                    true
                }
            });
            !intents.is_empty()
        });
        self.total -= expired.len();
        let mut cleaned = Vec::new();
        self.tombstones.retain(|id, t| {
            if now_ms > t.cleanup_after_ms {
                cleaned.push(id.clone());
                false
            } else {
                true
            }
        });
        (expired, cleaned)
    }

    /// Transition all intents targeting a spent/gone order. `tx_hash` is the
    /// spending tx when known (status "executed"), else "order-gone".
    pub fn on_order_spent(
        &mut self,
        key: &OrderKey,
        tx_hash: Option<Vec<u8>>,
        now_ms: u64,
    ) -> Vec<Vec<u8>> {
        match self.by_order.remove(key) {
            Some(intents) => {
                self.total -= intents.len();
                let status: &'static str =
                    if tx_hash.is_some() { "executed" } else { "order-gone" };
                intents
                    .into_iter()
                    .map(|i| {
                        self.tombstones.insert(
                            i.intent_id.clone(),
                            IntentTombstone {
                                status,
                                tx_hash: tx_hash.clone(),
                                cleanup_after_ms: now_ms.saturating_add(TOMBSTONE_TTL_MS),
                            },
                        );
                        i.intent_id
                    })
                    .collect()
            }
            None => Vec::new(),
        }
    }

    /// Operator-facing summary: live intents (id, order, hint kind, expiry)
    /// plus tombstone status tallies. Private-surface only — this exposes
    /// order refs, which are strategy-sensitive.
    pub fn summary(&self) -> serde_json::Value {
        let live: Vec<serde_json::Value> = self
            .by_order
            .iter()
            .flat_map(|(key, intents)| {
                let key = key.clone();
                intents.iter().map(move |i| {
                    serde_json::json!({
                        "intent_id": hex::encode(&i.intent_id),
                        "order": format!("{}#{}", hex::encode(&key.0), key.1),
                        "hint": i.hint.as_ref().map(|h| match h {
                            ExecutionHint::Claim { pool } =>
                                format!("claim:{}", &pool[..12.min(pool.len())]),
                        }),
                        "expiry_ms": i.expiry_ms,
                        "received_at_ms": i.received_at_ms,
                    })
                })
            })
            .collect();
        let mut tallies: BTreeMap<&'static str, usize> = BTreeMap::new();
        for t in self.tombstones.values() {
            *tallies.entry(t.status).or_default() += 1;
        }
        serde_json::json!({
            "live": live,
            "terminal": tallies,
        })
    }

    /// Look up an intent by id: live entry or tombstone.
    pub fn find(&self, intent_id: &[u8]) -> Option<IntentLookup<'_>> {
        for intents in self.by_order.values() {
            if let Some(i) = intents.iter().find(|i| i.intent_id == intent_id) {
                return Some(IntentLookup::Live(i));
            }
        }
        self.tombstones.get(intent_id).map(IntentLookup::Terminal)
    }
}

pub enum IntentLookup<'a> {
    Live(&'a StoredIntent),
    Terminal(&'a IntentTombstone),
}

fn big_to_u64(t: &crate::bigint::BigInt) -> Option<u64> {
    use num_traits::ToPrimitive;
    t.clone().unwrap().to_u64()
}

pub fn order_key(sse: &SignedStrategyExecution) -> OrderKey {
    (
        sse.execution.order_ref.transaction_id.clone(),
        sse.execution.order_ref.output_index,
    )
}

/// Whether the execution's validity window contains the whole tx window
/// `[start_ms, end_ms]`. The on-chain check is `interval.includes(strategy_
/// range, tx_range)` — the *execution* window must contain the *tx*
/// validity range, so a scoop is only safe when the full window fits.
pub fn window_covers(sse: &SignedStrategyExecution, start_ms: u64, end_ms: u64) -> bool {
    let lower_ok = match &sse.execution.validity_range.lower_bound.bound_type {
        IntervalBoundType::NegativeInfinity => true,
        IntervalBoundType::Finite(t) => big_to_u64(t).map(|t| t <= start_ms).unwrap_or(false),
        IntervalBoundType::PositiveInfinity => false,
    };
    let upper_ok = match &sse.execution.validity_range.upper_bound.bound_type {
        IntervalBoundType::PositiveInfinity => true,
        IntervalBoundType::Finite(t) => big_to_u64(t).map(|t| end_ms <= t).unwrap_or(false),
        IntervalBoundType::NegativeInfinity => false,
    };
    lower_ok && upper_ok
}

/// Synthesize a swap-shaped [`Constraint`] for a strategy order from an
/// authorized execution, so the ordinary batching/routing pipeline can
/// handle it. Phase 1 supports the common shape: the order offers a single
/// non-ADA asset; the execution's `min_deltas` optionally bounds that
/// asset's outflow (a ≤ 0 delta) plus what must be received in exchange.
///
/// Returns `None` when the shape isn't (yet) supported: ADA-only or
/// multi-asset offers, nothing consumable, or nothing to receive.
pub fn synthesize_swap_constraint(
    order: &SundaeV4Order,
    sse: &SignedStrategyExecution,
) -> Option<crate::sundaev4::types::Constraint> {
    use num_traits::Signed;

    // The offered asset: exactly one non-ADA asset in the order's value.
    let mut offered: Option<(crate::cardano_types::AssetClass, crate::bigint::BigInt)> = None;
    for (policy, tokens) in &order.value.0 {
        if policy.is_empty() {
            continue;
        }
        for (name, qty) in tokens {
            if !qty.is_positive() {
                continue;
            }
            if offered.is_some() {
                return None; // multi-asset offer: not yet supported
            }
            offered = Some((
                crate::cardano_types::AssetClass { policy: policy.clone(), token: name.clone() },
                qty.clone(),
            ));
        }
    }
    let (offer_asset, balance) = offered?;

    // min_deltas entries are per-asset DELTAS (output − input) since SUN-109
    // bounded settlement by signed deltas. The offered asset's entry is ≤ 0
    // and bounds the outflow: at most (−amount) may leave the order. An
    // absent entry leaves the offer unconstrained (full balance consumable).
    // Receive-side entries are lower bounds on inflow, which for assets the
    // order doesn't already hold equals the absolute received amount (the
    // only shape this phase-1 synthesizer supports).
    let mut consumable = balance.clone();
    let mut swap_min: Vec<(crate::cardano_types::AssetClass, crate::bigint::BigInt)> = Vec::new();
    for (asset, amount) in &sse.execution.min_received {
        if *asset == offer_asset {
            let cap = -amount.clone();
            if cap < consumable {
                consumable = cap;
            }
        } else {
            swap_min.push((asset.clone(), amount.clone()));
        }
    }
    if swap_min.is_empty() {
        return None; // nothing to receive — nothing for a swap to do
    }
    if !consumable.is_positive() {
        return None; // outflow bound pins the whole offer
    }

    Some(crate::sundaev4::types::Constraint::Swap {
        offered: offer_asset,
        original_offered: consumable.clone(),
        remaining_offered: consumable,
        min_received: swap_min,
    })
}

/// Whether the execution's validity window contains `now_ms`.
fn window_open_at(sse: &SignedStrategyExecution, now_ms: u64) -> Option<bool> {
    let lower_ok = match &sse.execution.validity_range.lower_bound.bound_type {
        IntervalBoundType::NegativeInfinity => true,
        IntervalBoundType::Finite(t) => big_to_u64(t)? <= now_ms,
        IntervalBoundType::PositiveInfinity => false,
    };
    let upper_ok = match &sse.execution.validity_range.upper_bound.bound_type {
        IntervalBoundType::PositiveInfinity => true,
        IntervalBoundType::Finite(t) => now_ms < big_to_u64(t)?,
        IntervalBoundType::NegativeInfinity => false,
    };
    Some(lower_ok && upper_ok)
}

/// Off-chain mirror of `multisig.satisfied_payload` (aicone). `Script`
/// credentials can only be proven inside a transaction (withdrawal presence),
/// so they are rejected at ingest — the scoop itself would still satisfy them,
/// but we have no way to know that in advance.
fn multisig_satisfied(
    auth: &Multisig,
    payload: &[u8],
    signatures: &[(Vec<u8>, Vec<u8>)],
    now_ms: u64,
) -> bool {
    match auth {
        Multisig::Signature(key_hash) => signatures.iter().any(|(vk, sig)| {
            if Hasher::<224>::hash(vk).as_ref() != key_hash.as_slice() {
                return false;
            }
            let (Ok(vk_arr), Ok(sig_arr)) = (
                <[u8; 32]>::try_from(vk.as_slice()),
                <[u8; 64]>::try_from(sig.as_slice()),
            ) else {
                return false;
            };
            let public = pallas_crypto::key::ed25519::PublicKey::from(vk_arr);
            let signature = pallas_crypto::key::ed25519::Signature::from(sig_arr);
            public.verify(payload, &signature)
        }),
        Multisig::AllOf(scripts) => scripts
            .iter()
            .all(|s| multisig_satisfied(s, payload, signatures, now_ms)),
        Multisig::AnyOf(scripts) => scripts
            .iter()
            .any(|s| multisig_satisfied(s, payload, signatures, now_ms)),
        Multisig::AtLeast(required, scripts) => {
            let n = scripts
                .iter()
                .filter(|s| multisig_satisfied(s, payload, signatures, now_ms))
                .count();
            big_to_u64(required).map(|r| n as u64 >= r).unwrap_or(false)
        }
        Multisig::Before(t) => big_to_u64(t).map(|t| now_ms < t).unwrap_or(false),
        Multisig::After(t) => big_to_u64(t).map(|t| now_ms >= t).unwrap_or(false),
        Multisig::Script(_) => false,
    }
}

/// The intent store plus its persistence and gossip context, shared between
/// the admin server (ingest), the scooper (execution), and the prune loop.
pub struct IntentService {
    pub store: tokio::sync::Mutex<IntentStore>,
    dao: Box<dyn StrategyIntentDao>,
    /// Peer scooper base URLs (e.g. "https://scooper-2.example.com") to
    /// forward newly-accepted intents to.
    peers: Vec<String>,
}

pub type IntentServiceHandle = std::sync::Arc<IntentService>;

/// Wall-clock now in POSIX milliseconds (execution windows are in POSIX ms).
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl IntentService {
    pub async fn load(dao: Box<dyn StrategyIntentDao>, peers: Vec<String>) -> Result<Self> {
        let store = IntentStore::load(dao.as_ref(), now_ms()).await?;
        if !store.is_empty() {
            tracing::info!(count = store.len(), "rehydrated strategy intents from persistence");
        }
        Ok(Self { store: tokio::sync::Mutex::new(store), dao, peers })
    }

    /// Validate, store, persist, and (if new) gossip an intent.
    /// `find_order` resolves the target order from current indexed state.
    pub async fn submit(
        &self,
        sse_cbor: Vec<u8>,
        hint: Option<ExecutionHint>,
        find_order: impl Fn(&OrderKey) -> Option<std::sync::Arc<SundaeV4Order>>,
        describe_invalid: impl Fn(&OrderKey) -> Option<String>,
    ) -> Result<SubmitOutcome> {
        let (outcome, stored) = {
            let mut store = self.store.lock().await;
            store.submit(sse_cbor, hint, find_order, describe_invalid, now_ms())?
        };
        if !outcome.replaced.is_empty() {
            self.dao.mark_terminal(&outcome.replaced, "replaced", None).await?;
        }
        if let Some(stored) = stored {
            self.dao.save_intent(&to_persisted(&stored)).await?;
            self.gossip(stored);
        }
        Ok(outcome)
    }

    /// Forward an accepted intent to all configured peers, fire-and-forget.
    /// Peers dedup by intent id (newly_stored=false), so echoes don't loop.
    fn gossip(&self, intent: StoredIntent) {
        for peer in &self.peers {
            let url = format!("{}/v4/strategy-intents", peer.trim_end_matches('/'));
            let body = serde_json::json!({
                "signed_execution": hex::encode(&intent.sse_cbor),
                "hint": intent.hint,
            });
            tokio::spawn(async move {
                let client = reqwest::Client::new();
                let result = client
                    .post(&url)
                    .json(&body)
                    .timeout(std::time::Duration::from_secs(5))
                    .send()
                    .await;
                match result {
                    Ok(resp) if resp.status().is_success() => {}
                    Ok(resp) => tracing::debug!(url, status = %resp.status(), "intent gossip rejected"),
                    Err(e) => tracing::debug!(url, "intent gossip failed: {e}"),
                }
            });
        }
    }

    /// Transition intents whose window expired or whose target order is
    /// gone, keeping queryable tombstones; deep-clean stale tombstones.
    /// `disposition` reports each order's fate from current indexed state.
    pub async fn prune(
        &self,
        disposition: impl Fn(&OrderKey) -> OrderDisposition,
    ) -> Result<usize> {
        let now = now_ms();
        let (expired, cleaned, executed, gone) = {
            let mut store = self.store.lock().await;
            let (expired, cleaned) = store.prune_expired(now);
            let mut executed: Vec<(Vec<u8>, Vec<Vec<u8>>)> = Vec::new();
            let mut gone: Vec<Vec<u8>> = Vec::new();
            let keys: Vec<OrderKey> = store.by_order.keys().cloned().collect();
            for key in keys {
                match disposition(&key) {
                    OrderDisposition::Live => {}
                    OrderDisposition::Spent(Some(tx)) => {
                        let ids = store.on_order_spent(&key, Some(tx.clone()), now);
                        if !ids.is_empty() {
                            executed.push((tx, ids));
                        }
                    }
                    OrderDisposition::Spent(None) => {
                        gone.extend(store.on_order_spent(&key, None, now));
                    }
                }
            }
            (expired, cleaned, executed, gone)
        };
        let mut n = expired.len() + gone.len();
        self.dao.mark_terminal(&expired, "expired", None).await?;
        self.dao.mark_terminal(&gone, "order-gone", None).await?;
        for (tx, ids) in executed {
            n += ids.len();
            self.dao.mark_terminal(&ids, "executed", Some(&tx)).await?;
        }
        if !cleaned.is_empty() {
            self.dao.delete_intents(&cleaned).await?;
        }
        if n > 0 {
            tracing::info!(transitioned = n, "strategy intents moved to terminal status");
        }
        Ok(n)
    }
}

/// What became of an intent's target order, per current indexed state.
pub enum OrderDisposition {
    Live,
    /// Order is gone; the spending tx hash when the indexer still knows it.
    Spent(Option<Vec<u8>>),
}

/// Convert a stored intent to its persisted form.
pub fn to_persisted(intent: &StoredIntent) -> PersistedStrategyIntent {
    let key = order_key(&intent.sse);
    PersistedStrategyIntent {
        intent_id: intent.intent_id.clone(),
        order_tx_id: key.0,
        order_index: key.1,
        sse_cbor: intent.sse_cbor.clone(),
        hint: intent
            .hint
            .as_ref()
            .and_then(|h| serde_json::to_string(h).ok()),
        expiry_ms: intent.expiry_ms,
        received_at_ms: intent.received_at_ms,
        status: None,
        status_tx: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bigint::BigInt;
    use crate::cardano_types::{AssetClass, Value};
    use crate::multisig::Multisig;
    use crate::sundaev4::types::{
        Destination, IntervalBound, OrderDatum, OutputRef, StrategyConstraints,
        StrategyExecution, StrategyValidityRange, SundaeV4Order,
    };
    use pallas_crypto::key::ed25519::SecretKey;

    const NOW_MS: u64 = 1_750_000_000_000;

    fn finite(t: u64) -> IntervalBound {
        IntervalBound {
            bound_type: IntervalBoundType::Finite(BigInt::from(t)),
            is_inclusive: true,
        }
    }

    fn test_execution(expiry: u64) -> StrategyExecution {
        StrategyExecution {
            order_ref: OutputRef {
                transaction_id: vec![0xAB; 32],
                output_index: 1,
            },
            validity_range: StrategyValidityRange {
                lower_bound: finite(NOW_MS - 1000),
                upper_bound: finite(expiry),
            },
            min_received: vec![(
                AssetClass { policy: vec![0xCC; 28], token: b"TOK".to_vec() },
                BigInt::from(1_000_000),
            )],
            final_destination: None,
            extension: pallas_primitives::PlutusData::Constr(pallas_primitives::Constr {
                tag: 121,
                any_constructor: None,
                fields: pallas_codec::utils::MaybeIndefArray::Def(vec![]),
            }),
        }
    }

    fn signed_sse_cbor(sk: &SecretKey, execution: StrategyExecution) -> Vec<u8> {
        let exec_pd = execution.clone().to_plutus();
        let payload = minicbor::to_vec(&exec_pd).unwrap();
        let sig = sk.sign(&payload);
        let sse = SignedStrategyExecution {
            execution,
            signatures: vec![(
                sk.public_key().as_ref().to_vec(),
                sig.as_ref().to_vec(),
            )],
        };
        minicbor::to_vec(&sse.to_plutus()).unwrap()
    }

    fn strategy_order(sk: &SecretKey) -> std::sync::Arc<SundaeV4Order> {
        let key_hash = Hasher::<224>::hash(sk.public_key().as_ref()).to_vec();
        let constraints = StrategyConstraints {
            auth: Multisig::Signature(key_hash),
            final_destinations: vec![],
        };
        let unit = pallas_primitives::PlutusData::Constr(pallas_primitives::Constr {
            tag: 121,
            any_constructor: None,
            fields: pallas_codec::utils::MaybeIndefArray::Def(vec![]),
        });
        std::sync::Arc::new(SundaeV4Order {
            input: crate::cardano_types::TransactionInput::new([0xAB; 32].into(), 1),
            value: Value::default(),
            datum: OrderDatum {
                owner: Multisig::Signature(vec![0x11; 28]),
                destination: Destination::SelfDestination,
                service_budget: BigInt::from(3_000_000),
                max_per_execution: BigInt::from(3_000_000),
                config_token: vec![],
                constraints: vec![(vec![0xEE; 28], constraints.clone().to_plutus())],
                extension: unit,
            },
            constraint: Constraint::Strategy { constraints },
            slot: 100,
        })
    }

    fn key() -> SecretKey {
        SecretKey::from([0x42; 32])
    }

    #[test]
    fn accepts_valid_intent_and_dedups() {
        let sk = key();
        let order = strategy_order(&sk);
        let cbor = signed_sse_cbor(&sk, test_execution(NOW_MS + 60_000));
        let mut store = IntentStore::default();

        let hint = Some(ExecutionHint::Claim { pool: "cafe01".into() });
        let (outcome, stored) = store
            .submit(cbor.clone(), hint.clone(), |_| Some(order.clone()), |_| None, NOW_MS)
            .expect("valid intent should be accepted");
        assert!(outcome.newly_stored);
        let stored = stored.expect("stored intent returned");
        assert_eq!(stored.hint, hint);
        // Hint survives the persisted round-trip (JSON in the sqlite row).
        let persisted = to_persisted(&stored);
        assert_eq!(
            persisted.hint.as_deref(),
            Some(r#"{"type":"claim","pool":"cafe01"}"#),
        );
        assert_eq!(store.len(), 1);

        // Same bytes again: dedup, no error, not re-stored.
        let (echo, stored2) = store
            .submit(cbor.clone(), None, |_| Some(order.clone()), |_| None, NOW_MS)
            .expect("duplicate intent should be a no-op");
        assert!(!echo.newly_stored);
        assert!(stored2.is_none());
        assert_eq!(store.len(), 1);
        assert_eq!(echo.intent_id, outcome.intent_id);

        // A duplicate can attach/replace a hint (latest Some wins) …
        let new_hint = Some(ExecutionHint::Claim { pool: "beef02".into() });
        let (echo2, updated) = store
            .submit(cbor.clone(), new_hint.clone(), |_| Some(order.clone()), |_| None, NOW_MS)
            .expect("hint update should succeed");
        assert!(!echo2.newly_stored);
        assert_eq!(updated.expect("updated intent returned").hint, new_hint);
        // … but a hint-less duplicate leaves the stored hint untouched.
        let (_, none_update) = store
            .submit(cbor.clone(), None, |_| Some(order.clone()), |_| None, NOW_MS)
            .expect("no-hint duplicate is a no-op");
        assert!(none_update.is_none());
        let key0 = (vec![0xAB; 32], 1u64);
        assert_eq!(store.valid_for_order(&key0, NOW_MS)[0].hint, new_hint);

        // Valid-for-order sees it inside the window, not outside.
        let key = (vec![0xAB; 32], 1u64);
        assert_eq!(store.valid_for_order(&key, NOW_MS).len(), 1);
        assert_eq!(store.valid_for_order(&key, NOW_MS + 120_000).len(), 0);
    }

    #[test]
    fn rejects_wrong_signer() {
        let sk = key();
        let interloper = SecretKey::from([0x43; 32]);
        let order = strategy_order(&sk);
        let cbor = signed_sse_cbor(&interloper, test_execution(NOW_MS + 60_000));
        let mut store = IntentStore::default();
        let err = store
            .submit(cbor, None, |_| Some(order.clone()), |_| None, NOW_MS)
            .unwrap_err();
        assert!(err.to_string().contains("do not satisfy"), "got: {err}");
    }

    #[test]
    fn rejects_tampered_execution() {
        let sk = key();
        let order = strategy_order(&sk);
        // Sign one execution, then submit a *different* one with that signature.
        let signed = test_execution(NOW_MS + 60_000);
        let mut tampered = signed.clone();
        tampered.min_received[0].1 = BigInt::from(1); // lower the floor
        let exec_pd = signed.to_plutus();
        let payload = minicbor::to_vec(&exec_pd).unwrap();
        let sig = sk.sign(&payload);
        let sse = SignedStrategyExecution {
            execution: tampered,
            signatures: vec![(sk.public_key().as_ref().to_vec(), sig.as_ref().to_vec())],
        };
        let cbor = minicbor::to_vec(&sse.to_plutus()).unwrap();
        let mut store = IntentStore::default();
        assert!(store.submit(cbor, None, |_| Some(order.clone()), |_| None, NOW_MS).is_err());
    }

    #[test]
    fn rejects_expired_and_unknown_order() {
        let sk = key();
        let order = strategy_order(&sk);
        let mut store = IntentStore::default();

        let expired = signed_sse_cbor(&sk, test_execution(NOW_MS - 1));
        assert!(store.submit(expired, None, |_| Some(order.clone()), |_| None, NOW_MS).is_err());

        let fine = signed_sse_cbor(&sk, test_execution(NOW_MS + 60_000));
        assert!(store.submit(fine, None, |_| None, |_| None, NOW_MS).is_err());
        assert!(store.is_empty());
    }

    #[test]
    fn malformed_order_reports_its_reason() {
        let sk = key();
        let mut store = IntentStore::default();
        // Order isn't in the valid set (find_order misses) but is on record as
        // malformed — the poster should learn why rather than getting a bare
        // "not found".
        let fine = signed_sse_cbor(&sk, test_execution(NOW_MS + 60_000));
        let err = store
            .submit(
                fine,
                None,
                |_| None,
                |_| Some("unsupported constraint modules: [feedface]".to_string()),
                NOW_MS,
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("malformed"), "got: {msg}");
        assert!(msg.contains("feedface"), "got: {msg}");
        assert!(store.is_empty());
    }

    #[test]
    fn synthesizes_swap_from_execution() {
        let sk = key();
        let mut order = strategy_order(&sk);
        let offer_asset = AssetClass { policy: vec![0xDD; 28], token: b"TOKENA".to_vec() };
        {
            let o = std::sync::Arc::get_mut(&mut order).unwrap();
            o.value.insert(&offer_asset, BigInt::from(5_000_000));
        }

        // Offered-asset delta bound −3M: at most 3M may leave, consumable = 3M.
        let mut exec = test_execution(NOW_MS + 60_000);
        exec.min_received.push((offer_asset.clone(), BigInt::from(-3_000_000)));
        let sse = SignedStrategyExecution { execution: exec, signatures: vec![] };
        let c = synthesize_swap_constraint(&order, &sse).expect("synthesizable");
        match c {
            Constraint::Swap { offered, original_offered, remaining_offered, min_received } => {
                assert_eq!(offered, offer_asset);
                assert_eq!(original_offered, BigInt::from(3_000_000));
                assert_eq!(remaining_offered, BigInt::from(3_000_000));
                assert_eq!(min_received.len(), 1); // leftover entry stripped
                assert_ne!(min_received[0].0, offer_asset);
            }
            other => panic!("expected Swap, got {other:?}"),
        }

        // Zero outflow bound (pure-claim shape) → nothing consumable → not
        // synthesizable as a swap.
        let mut exec = test_execution(NOW_MS + 60_000);
        exec.min_received.push((offer_asset.clone(), BigInt::from(0)));
        let sse = SignedStrategyExecution { execution: exec, signatures: vec![] };
        assert!(synthesize_swap_constraint(&order, &sse).is_none());

        // Only an offered-asset bound (nothing to receive) → None.
        let mut exec = test_execution(NOW_MS + 60_000);
        exec.min_received = vec![(offer_asset.clone(), BigInt::from(-1_000_000))];
        let sse = SignedStrategyExecution { execution: exec, signatures: vec![] };
        assert!(synthesize_swap_constraint(&order, &sse).is_none());
    }

    #[test]
    fn window_covers_requires_full_containment() {
        let exec = test_execution(NOW_MS + 60_000); // lower = NOW-1000 (finite)
        let sse = SignedStrategyExecution { execution: exec, signatures: vec![] };
        // Fully inside.
        assert!(window_covers(&sse, NOW_MS, NOW_MS + 30_000));
        // End pokes past the upper bound.
        assert!(!window_covers(&sse, NOW_MS, NOW_MS + 60_001));
        // Start before the lower bound.
        assert!(!window_covers(&sse, NOW_MS - 5_000, NOW_MS + 30_000));
    }

    #[test]
    fn standing_intent_replaced_by_later_valid_from() {
        let sk = key();
        let order = strategy_order(&sk);
        let mut store = IntentStore::default();

        // Two open-ended (no expiry) intents; only the valid-from differs.
        // The fresher signature must win regardless of arrival order — the
        // lower bound is the freshness signal when upper bounds tie.
        let unbounded = |lower: u64| {
            let mut e = test_execution(0);
            e.validity_range.lower_bound = finite(lower);
            e.validity_range.upper_bound = IntervalBound {
                bound_type: IntervalBoundType::PositiveInfinity,
                is_inclusive: true,
            };
            e
        };
        let old_cbor = signed_sse_cbor(&sk, unbounded(NOW_MS - 10_000));
        let new_cbor = signed_sse_cbor(&sk, unbounded(NOW_MS));

        let (o1, _) = store
            .submit(old_cbor.clone(), None, |_| Some(order.clone()), |_| None, NOW_MS)
            .unwrap();
        let (o2, _) = store
            .submit(new_cbor.clone(), None, |_| Some(order.clone()), |_| None, NOW_MS)
            .unwrap();
        assert!(o2.newly_stored, "later valid-from replaces the standing intent");
        assert_eq!(o2.replaced, vec![o1.intent_id.clone()]);
        assert_eq!(store.len(), 1);

        // Reversed arrival order converges on the same winner.
        let mut store2 = IntentStore::default();
        let (r2, _) = store2
            .submit(new_cbor, None, |_| Some(order.clone()), |_| None, NOW_MS)
            .unwrap();
        let (r1, _) = store2
            .submit(old_cbor, None, |_| Some(order.clone()), |_| None, NOW_MS)
            .unwrap();
        assert!(r2.newly_stored);
        assert!(r1.superseded);
        assert_eq!(store2.len(), 1);
        assert_eq!(r2.intent_id, o2.intent_id);
    }

    /// Signers that leave valid-from unset (it reads as 0) or reuse a window
    /// produce intents that tie on content. The one posted last must still
    /// replace what's held, whichever order they arrive in — ranking on
    /// intent_id alone used to let a standing intent beat the replacement
    /// that was meant to supersede it, and tombstone the replacement.
    #[test]
    fn tied_windows_replace_in_arrival_order() {
        let sk = key();
        let order = strategy_order(&sk);

        // Identical windows (no lower bound, same expiry); only the ask differs.
        let tied = |min: u64| {
            let mut e = test_execution(NOW_MS + 60_000);
            e.validity_range.lower_bound = IntervalBound {
                bound_type: IntervalBoundType::NegativeInfinity,
                is_inclusive: true,
            };
            e.min_received = vec![(
                AssetClass { policy: vec![0xCC; 28], token: b"TOK".to_vec() },
                BigInt::from(min),
            )];
            e
        };
        let a = signed_sse_cbor(&sk, tied(1_000_000));
        let b = signed_sse_cbor(&sk, tied(2_000_000));

        // Both arrival orders: whichever lands second wins, so the id order
        // (fixed by the bytes) can't decide the outcome.
        for (first, second) in [(a.clone(), b.clone()), (b, a)] {
            let mut store = IntentStore::default();
            let (o1, _) = store
                .submit(first, None, |_| Some(order.clone()), |_| None, NOW_MS)
                .unwrap();
            let (o2, _) = store
                .submit(second, None, |_| Some(order.clone()), |_| None, NOW_MS + 1)
                .unwrap();
            assert!(o2.newly_stored, "the intent posted second replaces the standing one");
            assert!(!o2.superseded);
            assert_eq!(o2.replaced, vec![o1.intent_id.clone()]);
            assert_eq!(store.len(), 1);
            assert!(matches!(
                store.find(&o1.intent_id),
                Some(IntentLookup::Terminal(t)) if t.status == "replaced"
            ));
        }
    }

    #[test]
    fn replacement_and_pruning() {
        let sk = key();
        let order = strategy_order(&sk);
        let mut store = IntentStore::default();

        // A newer intent (later expiry) replaces the standing one.
        let first = signed_sse_cbor(&sk, test_execution(NOW_MS + 60_000));
        let (o1, _) = store.submit(first.clone(), None, |_| Some(order.clone()), |_| None, NOW_MS).unwrap();
        assert!(o1.newly_stored);
        let second = signed_sse_cbor(&sk, test_execution(NOW_MS + 90_000));
        let (o2, _) = store.submit(second, None, |_| Some(order.clone()), |_| None, NOW_MS).unwrap();
        assert!(o2.newly_stored);
        assert_eq!(o2.replaced, vec![o1.intent_id.clone()]);
        assert_eq!(store.len(), 1);
        assert!(matches!(
            store.find(&o1.intent_id),
            Some(IntentLookup::Terminal(t)) if t.status == "replaced"
        ));

        // A gossip echo of the replaced intent loses deterministically:
        // not stored, flagged superseded, winner untouched.
        let (echo, stored) = store.submit(first, None, |_| Some(order.clone()), |_| None, NOW_MS).unwrap();
        assert!(!echo.newly_stored);
        assert!(echo.superseded);
        assert!(stored.is_none());
        assert_eq!(store.len(), 1);

        // An older intent arriving *after* the winner also loses, even
        // without a tombstone (deterministic (expiry, id) ranking).
        let stale = signed_sse_cbor(&sk, test_execution(NOW_MS + 70_000));
        let (o3, stored) = store.submit(stale, None, |_| Some(order.clone()), |_| None, NOW_MS).unwrap();
        assert!(!o3.newly_stored);
        assert!(o3.superseded);
        assert!(stored.is_none());
        assert_eq!(store.len(), 1);
        assert!(matches!(
            store.find(&o3.intent_id),
            Some(IntentLookup::Terminal(t)) if t.status == "replaced"
        ));

        // The winner expires and leaves a tombstone.
        let (expired, cleaned) = store.prune_expired(NOW_MS + 120_000);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], o2.intent_id);
        assert!(cleaned.is_empty());
        assert!(store.is_empty());
        assert!(matches!(
            store.find(&o2.intent_id),
            Some(IntentLookup::Terminal(t)) if t.status == "expired"
        ));

        // Spent-order pruning drops the bucket and records execution.
        let cbor = signed_sse_cbor(&sk, test_execution(NOW_MS + 200_000));
        store.submit(cbor, None, |_| Some(order.clone()), |_| None, NOW_MS).unwrap();
        let removed = store.on_order_spent(&(vec![0xAB; 32], 1), Some(vec![0x77; 32]), NOW_MS);
        assert_eq!(removed.len(), 1);
        assert!(store.is_empty());
        assert!(matches!(
            store.find(&removed[0]),
            Some(IntentLookup::Terminal(t))
                if t.status == "executed" && t.tx_hash == Some(vec![0x77; 32])
        ));

        // Tombstones deep-clean after their TTL: the two replaced, the one
        // expired, and the one executed.
        let (_, cleaned) = store.prune_expired(NOW_MS + TOMBSTONE_TTL_MS + 300_000);
        assert_eq!(cleaned.len(), 4);
        assert!(store.find(&removed[0]).is_none());
    }
}
