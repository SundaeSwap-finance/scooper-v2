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
/// Hard cap per target order. An order can only be scooped once per intent
/// window, so there is no legitimate reason to hold many candidates.
const MAX_INTENTS_PER_ORDER: usize = 4;

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
}

fn hex_id<S: serde::Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&hex::encode(v))
}

#[derive(Default)]
pub struct IntentStore {
    /// Intents keyed by target order, each order's list ordered by arrival.
    by_order: BTreeMap<OrderKey, Vec<StoredIntent>>,
    total: usize,
}

impl IntentStore {
    /// Rehydrate from persistence, dropping anything already expired.
    pub async fn load(dao: &dyn StrategyIntentDao, now_ms: u64) -> Result<Self> {
        let mut store = Self::default();
        let mut dead: Vec<Vec<u8>> = Vec::new();
        for p in dao.load_intents().await? {
            if p.expiry_ms <= now_ms {
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
    pub fn submit(
        &mut self,
        sse_cbor: Vec<u8>,
        hint: Option<ExecutionHint>,
        find_order: impl Fn(&OrderKey) -> Option<std::sync::Arc<SundaeV4Order>>,
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
        let order = find_order(&key).with_context(|| {
            format!(
                "order {}#{} not found (unknown, spent, or not yet indexed)",
                hex::encode(&key.0),
                key.1
            )
        })?;
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
        let entry = self.by_order.entry(key).or_default();
        if let Some(existing) = entry.iter_mut().find(|i| i.intent_id == intent_id) {
            // Duplicate bytes. A hint can still be attached or replaced —
            // latest Some(hint) wins; None means "no opinion", keep what we
            // have. Returning the updated intent makes the caller persist
            // and re-gossip it, so hint updates propagate and converge.
            if hint.is_some() && existing.hint != hint {
                existing.hint = hint;
                let updated = existing.clone();
                return Ok((SubmitOutcome { intent_id, newly_stored: false }, Some(updated)));
            }
            return Ok((SubmitOutcome { intent_id, newly_stored: false }, None));
        }
        if entry.len() >= MAX_INTENTS_PER_ORDER {
            bail!("too many pending intents for this order (max {MAX_INTENTS_PER_ORDER})");
        }
        if self.total >= MAX_TOTAL_INTENTS {
            bail!("intent store is full (max {MAX_TOTAL_INTENTS})");
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
        Ok((SubmitOutcome { intent_id, newly_stored: true }, Some(stored)))
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

    /// Drop expired intents; returns the removed ids so the caller can also
    /// delete them from persistence.
    pub fn prune_expired(&mut self, now_ms: u64) -> Vec<Vec<u8>> {
        let mut removed = Vec::new();
        self.by_order.retain(|_, intents| {
            intents.retain(|i| {
                if i.expiry_ms <= now_ms {
                    removed.push(i.intent_id.clone());
                    false
                } else {
                    true
                }
            });
            !intents.is_empty()
        });
        self.total -= removed.len();
        removed
    }

    /// Drop all intents targeting a spent order; returns removed ids.
    pub fn on_order_spent(&mut self, key: &OrderKey) -> Vec<Vec<u8>> {
        match self.by_order.remove(key) {
            Some(intents) => {
                self.total -= intents.len();
                intents.into_iter().map(|i| i.intent_id).collect()
            }
            None => Vec::new(),
        }
    }
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
    ) -> Result<SubmitOutcome> {
        let (outcome, stored) = {
            let mut store = self.store.lock().await;
            store.submit(sse_cbor, hint, find_order, now_ms())?
        };
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

    /// Drop expired intents and intents whose target order is gone (spent or
    /// never re-indexed). `order_exists` checks current indexed state.
    pub async fn prune(&self, order_exists: impl Fn(&OrderKey) -> bool) -> Result<usize> {
        let removed: Vec<Vec<u8>> = {
            let mut store = self.store.lock().await;
            let mut removed = store.prune_expired(now_ms());
            let dead_orders: Vec<OrderKey> = store
                .by_order
                .keys()
                .filter(|k| !order_exists(k))
                .cloned()
                .collect();
            for key in &dead_orders {
                removed.extend(store.on_order_spent(key));
            }
            removed
        };
        let n = removed.len();
        if n > 0 {
            self.dao.delete_intents(&removed).await?;
            tracing::info!(pruned = n, "pruned dead strategy intents");
        }
        Ok(n)
    }
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
                budget: BigInt::from(3_000_000),
                share_batcher: BigInt::from(0),
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
            .submit(cbor.clone(), hint.clone(), |_| Some(order.clone()), NOW_MS)
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
            .submit(cbor.clone(), None, |_| Some(order.clone()), NOW_MS)
            .expect("duplicate intent should be a no-op");
        assert!(!echo.newly_stored);
        assert!(stored2.is_none());
        assert_eq!(store.len(), 1);
        assert_eq!(echo.intent_id, outcome.intent_id);

        // A duplicate can attach/replace a hint (latest Some wins) …
        let new_hint = Some(ExecutionHint::Claim { pool: "beef02".into() });
        let (echo2, updated) = store
            .submit(cbor.clone(), new_hint.clone(), |_| Some(order.clone()), NOW_MS)
            .expect("hint update should succeed");
        assert!(!echo2.newly_stored);
        assert_eq!(updated.expect("updated intent returned").hint, new_hint);
        // … but a hint-less duplicate leaves the stored hint untouched.
        let (_, none_update) = store
            .submit(cbor.clone(), None, |_| Some(order.clone()), NOW_MS)
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
            .submit(cbor, None, |_| Some(order.clone()), NOW_MS)
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
        assert!(store.submit(cbor, None, |_| Some(order.clone()), NOW_MS).is_err());
    }

    #[test]
    fn rejects_expired_and_unknown_order() {
        let sk = key();
        let order = strategy_order(&sk);
        let mut store = IntentStore::default();

        let expired = signed_sse_cbor(&sk, test_execution(NOW_MS - 1));
        assert!(store.submit(expired, None, |_| Some(order.clone()), NOW_MS).is_err());

        let fine = signed_sse_cbor(&sk, test_execution(NOW_MS + 60_000));
        assert!(store.submit(fine, None, |_| None, NOW_MS).is_err());
        assert!(store.is_empty());
    }

    #[test]
    fn per_order_cap_and_pruning() {
        let sk = key();
        let order = strategy_order(&sk);
        let mut store = IntentStore::default();
        for i in 0..MAX_INTENTS_PER_ORDER as u64 {
            let cbor = signed_sse_cbor(&sk, test_execution(NOW_MS + 60_000 + i));
            store
                .submit(cbor, None, |_| Some(order.clone()), NOW_MS)
                .expect("under cap should be accepted");
        }
        let over = signed_sse_cbor(&sk, test_execution(NOW_MS + 999_999));
        assert!(store.submit(over, None, |_| Some(order.clone()), NOW_MS).is_err());
        assert_eq!(store.len(), MAX_INTENTS_PER_ORDER);

        // Everything expires by NOW + 60s + cap.
        let removed = store.prune_expired(NOW_MS + 120_000);
        assert_eq!(removed.len(), MAX_INTENTS_PER_ORDER);
        assert!(store.is_empty());

        // Spent-order pruning drops the whole bucket.
        let cbor = signed_sse_cbor(&sk, test_execution(NOW_MS + 60_000));
        store.submit(cbor, None, |_| Some(order.clone()), NOW_MS).unwrap();
        let removed = store.on_order_spent(&(vec![0xAB; 32], 1));
        assert_eq!(removed.len(), 1);
        assert!(store.is_empty());
    }
}
