use acropolis_common::Point;
use anyhow::Context as _;
use pallas_addresses::ScriptHash;
use pallas_primitives::PlutusData;
use plutus_parser::AsPlutus;
use serde::Serializer;
use serde::ser::SerializeStruct;

use crate::bigint::BigInt;
use crate::cardano_types::{AssetClass, TransactionInput, Value};
use crate::multisig::Multisig;
use crate::sundaev3::{Ident, PlutusAddress};

/// Serde helpers for encoding `Vec<u8>` fields as hex strings in JSON.
mod hex_ser {
    use serde::Serializer;

    pub fn bytes<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(v))
    }

    pub fn vec_bytes<S: Serializer>(v: &Vec<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(Some(v.len()))?;
        for b in v {
            seq.serialize_element(&hex::encode(b))?;
        }
        seq.end()
    }

    pub fn vec_bytes_pair_as_map<S: Serializer>(
        v: &Vec<(Vec<u8>, Vec<u8>)>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = s.serialize_map(Some(v.len()))?;
        for (k, v) in v {
            map.serialize_entry(&hex::encode(k), &hex::encode(v))?;
        }
        map.end()
    }
}

/// Void / unit as PlutusData — `Constr 0 []`. The audit-final PoolDatum's
/// `extension` field is Void on every pool the CLI creates. Used in tests
/// to build pool datums.
#[cfg(test)]
pub fn plutus_void() -> PlutusData {
    PlutusData::Constr(pallas_primitives::Constr {
        tag: 121,
        any_constructor: None,
        fields: pallas_codec::utils::MaybeIndefArray::Def(vec![]),
    })
}

// ──────────────────────────────────────────────────────────────────────────────
// Pool types
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PoolDatum {
    pub assets: Vec<(AssetClass, BigInt)>,
    pub total_lp: BigInt,
    pub circulating_lp: BigInt,
    pub preminted_lp: BigInt,
    pub identifier: Ident,
    pub actions: Vec<ActionEntry>,
    #[serde(serialize_with = "hex_ser::vec_bytes_pair_as_map")]
    pub module_state: Vec<(Vec<u8>, Vec<u8>)>,
    /// Lovelace surplus floor pinned from PoolConfig.min_surplus at Create
    /// (ADR-0012; audit-final addition). Preserved verbatim on every spend.
    pub min_surplus: BigInt,
    /// Reserved scooper-writable scratch (audit-final addition). The datum-
    /// preserving paths carry it through unchanged.
    pub extension: PlutusData,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ActionEntry {
    pub tag: BigInt,
    pub enabled: bool,
    #[serde(serialize_with = "hex_ser::vec_bytes")]
    pub modules: Vec<Vec<u8>>,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PoolState {
    pub assets: Vec<(AssetClass, BigInt)>,
    pub total_lp: BigInt,
    pub circulating_lp: BigInt,
    pub preminted_lp: BigInt,
}

impl PoolState {
    /// Construct from a pool datum. Used in tests.
    #[cfg(test)]
    pub fn from_pool(datum: &PoolDatum) -> Self {
        PoolState {
            assets: datum.assets.clone(),
            total_lp: datum.total_lp.clone(),
            circulating_lp: datum.circulating_lp.clone(),
            preminted_lp: datum.preminted_lp.clone(),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Transcript types
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TranscriptEntry {
    pub state_after: PoolState,
    pub fee_budget: BigInt,
    pub operation_tag: BigInt,
    pub operation_data: PlutusData,
}

#[derive(AsPlutus, Debug, PartialEq, Eq)]
pub enum PoolRedeemer {
    EscapeHatch {
        redeemed_lp: BigInt,
    },
    Upgrade,
    EmergencyDisable {
        target_tag: BigInt,
        set_enabled: bool,
    },
    Action {
        tag: BigInt,
        transcript: Vec<TranscriptEntry>,
        pool_input_index: BigInt,
        pool_output_index: BigInt,
    },
    /// SUN-103/ADR-0006 teardown spend path (constructor 4, appended —
    /// EscapeHatch 0, Upgrade 1, EmergencyDisable 2, Action 3 are stable).
    /// Parsed only (indexer tolerance); the scooper never builds it.
    Destroy,
}

// ──────────────────────────────────────────────────────────────────────────────
// Order types
// ──────────────────────────────────────────────────────────────────────────────

/// Order destination. Matches Aiken's `Destination { Fixed { address, datum } | Self }`.
/// `PlutusAddress` here is structurally identical to Aiken's `cardano/address.{Address}`
/// (payment credential + optional referenced stake credential), so the on-wire Constr
/// encoding round-trips with the contract's redesigned `Address` type unchanged.
#[derive(Clone, AsPlutus, Debug, PartialEq, Eq)]
pub enum Destination {
    Fixed(PlutusAddress, Option<PlutusData>),
    SelfDestination,
}

impl serde::Serialize for Destination {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Destination::SelfDestination => serializer.serialize_str("self"),
            Destination::Fixed(addr, _datum) => {
                let mut s = serializer.serialize_struct("Destination", 1)?;
                s.serialize_field("address", addr)?;
                s.end()
            }
        }
    }
}

/// Wire-format order datum, mirroring Aiken's `OrderDatum` after the modular
/// order-constraints refactor (PR #11).
///
/// The pre-PR-#11 shape carried a single tagged-union `constraints` Data; the
/// tag (0=Deposit, 1=Withdraw, 2=Swap, 3=Claim) picked the variant. The new
/// shape carries a list of `(constraint_script_hash, constraint_data)` tuples;
/// the constraint *type* is now identified by which script's hash is keyed in
/// the list. The inner Data still uses the same ctor tags (Swap=2; Basic 0/1/3
/// for Deposit/Withdraw/Claim) so decoding the inner Data is unchanged.
///
/// `config_token` is the asset name of the OrderConfig settings entry whose
/// `required_constraints` set this order claims to satisfy. The order
/// validator's withdraw handler looks up that entry as a reference input and
/// checks the order's constraints list matches `required_constraints` exactly.
#[derive(Clone, AsPlutus, Debug, PartialEq, Eq, serde::Serialize)]
pub struct OrderDatum {
    pub owner: Multisig,
    pub destination: Destination,
    /// Mutable lifetime service-fee counter (SUNDAE-2587): every partial-fill
    /// continuation must carry `service_budget - fee_deducted`; the fee
    /// constraint (when the order's config requires it) pins the delta to
    /// exactly `order_fee`. Cancel returns the remainder.
    pub service_budget: BigInt,
    /// Immutable flat cap on lovelace deducted in a single scoop, AND the
    /// terminal settlement amount: a terminal (order-consuming) fill deducts
    /// exactly `min(max_per_execution, service_budget)` (ADR-0001).
    pub max_per_execution: BigInt,
    /// Asset name of the OrderConfig settings entry this order's constraints
    /// must match.
    #[serde(serialize_with = "hex_ser::bytes")]
    pub config_token: Vec<u8>,
    /// List of `(constraint_script_hash, constraint_data)` tuples. Decode
    /// one by hash via [`OrderDatum::find_constraint_by_hash`], then pass
    /// the inner Data to [`Constraint::from_plutus_constraint`].
    pub constraints: Vec<(Vec<u8>, PlutusData)>,
    pub extension: PlutusData,
}

impl OrderDatum {
    /// Look up the constraint Data for a given constraint script hash.
    /// Returns `None` if the order doesn't carry that constraint.
    pub fn find_constraint_by_hash(&self, hash: &[u8]) -> Option<&PlutusData> {
        self.constraints.iter().find_map(|(h, d)| if h == hash { Some(d) } else { None })
    }
}

/// Parse a route constraint's data payload. On-chain it is the order's pool
/// whitelist (`List<Ident>` — see route.ak); empty means unrestricted, and
/// `route_lib.check_pool_whitelisted` rejects any pool outside a non-empty
/// list. A payload that isn't a list of byte strings can never satisfy the
/// module's `expect pool_whitelist: List<Ident>`, so callers should treat
/// `Err` as "order can never validate".
pub fn parse_route_whitelist(data: &PlutusData) -> Result<Vec<Ident>, String> {
    let PlutusData::Array(items) = data else {
        return Err("route constraint payload is not a list".to_string());
    };
    items
        .iter()
        .map(|item| match item {
            PlutusData::BoundedBytes(b) => Ok(Ident::new(b.as_ref())),
            _ => Err("route whitelist entry is not a byte string".to_string()),
        })
        .collect()
}

/// Decoded form of a single constraint entry pulled out of
/// `OrderDatum.constraints`. The constructor tag of the inner Data picks
/// the variant; the constraint *class* (swap vs basic) is identified by
/// which script hash keyed the entry in the parent list.
///
/// Shapes match the contract's per-class extractors (see `lib/constraints/`):
/// - Basic (Deposit/Withdraw/Claim): `offered` and `min_received` are `List<(AssetClass, Int)>`.
/// - Swap: `offered: AssetClass` (no quantity), `original_offered: Int`,
///   `remaining_offered: Int`, `min_received: List<(AssetClass, Int)>`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum Constraint {
    /// Tag 0
    Deposit {
        offered: Vec<(AssetClass, BigInt)>,
        min_received: Vec<(AssetClass, BigInt)>,
    },
    /// Tag 1
    Withdraw {
        offered: Vec<(AssetClass, BigInt)>,
        min_received: Vec<(AssetClass, BigInt)>,
    },
    /// Tag 2 — partial-fill capable. `original_offered` is the immutable quote
    /// reference; `remaining_offered` shrinks as fills happen.
    Swap {
        offered: AssetClass,
        original_offered: BigInt,
        remaining_offered: BigInt,
        min_received: Vec<(AssetClass, BigInt)>,
    },
    /// Tag 3
    Claim {
        offered: Vec<(AssetClass, BigInt)>,
        min_received: Vec<(AssetClass, BigInt)>,
    },
    /// strategy_order constraint: the trade parameters arrive off-chain as a
    /// [`SignedStrategyExecution`] posted to the strategy-intents endpoint.
    /// Not directly batchable — the scooper pairs it with a valid intent
    /// before it becomes scoopable.
    Strategy { constraints: StrategyConstraints },
}

impl OrderDatum {
    /// Continuation datum for a partial fill: byte-identical except the
    /// swap constraint entry's `remaining_offered` (field 2 of the tag-2
    /// constr) becomes `new_remaining`. Mirrors swap.ak's
    /// check_swap_continuation, which demands the whole datum equal except
    /// that one replaced constraint entry.
    pub fn with_swap_remaining(
        &self,
        swap_hash: &[u8],
        new_remaining: &BigInt,
    ) -> anyhow::Result<OrderDatum> {
        use plutus_parser::AsPlutus;
        let mut datum = self.clone();
        let entry = datum
            .constraints
            .iter_mut()
            .find(|(h, _)| h.as_slice() == swap_hash)
            .ok_or_else(|| anyhow::anyhow!("order carries no swap constraint entry"))?;
        let PlutusData::Constr(c) = &mut entry.1 else {
            anyhow::bail!("swap constraint data is not a Constr");
        };
        let mut fields: Vec<PlutusData> = c.fields.clone().to_vec();
        anyhow::ensure!(fields.len() == 4, "swap constraint must have 4 fields");
        fields[2] = new_remaining.clone().to_plutus();
        c.fields = pallas_primitives::MaybeIndefArray::Def(fields);
        Ok(datum)
    }

    /// Continuation datum for a partial fill (SUNDAE-2587): remaining_offered
    /// drops by the fill AND service_budget drops by the fee deducted this
    /// execution. swap.ak pins the continuation datum equal to the input
    /// datum EXCEPT service_budget and the swap constraint's own entry, so
    /// both must move here and nothing else.
    pub fn with_swap_continuation(
        &self,
        swap_hash: &[u8],
        new_remaining: &BigInt,
        fee_deducted: &BigInt,
    ) -> anyhow::Result<OrderDatum> {
        use num_traits::Signed;
        let mut datum = self.with_swap_remaining(swap_hash, new_remaining)?;
        datum.service_budget = &datum.service_budget - fee_deducted;
        anyhow::ensure!(
            !datum.service_budget.is_negative(),
            "fee {fee_deducted} exceeds the order's remaining service_budget"
        );
        Ok(datum)
    }
}

impl Constraint {
    /// Decode a `Constr`-tagged constraint payload. Returns the unrecognised tag
    /// in the error case so the caller can decide how to surface it.
    pub fn from_plutus_constraint(pd: &PlutusData) -> anyhow::Result<Self> {
        let PlutusData::Constr(c) = pd else {
            anyhow::bail!("constraint must be a Constr");
        };
        // Pallas tags: Constr 0..6 → cbor 121..127, Constr 7+ → 1280+. Strip the offset.
        let tag = if c.tag >= 121 && c.tag <= 127 {
            c.tag - 121
        } else if c.tag >= 1280 {
            c.tag - 1280 + 7
        } else {
            c.tag
        };
        let fields: Vec<PlutusData> = c.fields.clone().to_vec();
        let f = |i: usize| -> anyhow::Result<&PlutusData> {
            fields.get(i).ok_or_else(|| anyhow::anyhow!("constraint missing field {i}"))
        };
        let list_pair = |i: usize| -> anyhow::Result<Vec<(AssetClass, BigInt)>> {
            <Vec<(AssetClass, BigInt)>>::from_plutus(f(i)?.clone())
                .map_err(|e| anyhow::anyhow!("decode list pair: {e}"))
        };
        Ok(match tag {
            0 => Constraint::Deposit {
                offered: list_pair(0)?,
                min_received: list_pair(1)?,
            },
            1 => Constraint::Withdraw {
                offered: list_pair(0)?,
                min_received: list_pair(1)?,
            },
            2 => Constraint::Swap {
                offered: AssetClass::from_plutus(f(0)?.clone())
                    .map_err(|e| anyhow::anyhow!("decode swap.offered: {e}"))?,
                original_offered: BigInt::from_plutus(f(1)?.clone())
                    .map_err(|e| anyhow::anyhow!("decode swap.original_offered: {e}"))?,
                remaining_offered: BigInt::from_plutus(f(2)?.clone())
                    .map_err(|e| anyhow::anyhow!("decode swap.remaining_offered: {e}"))?,
                min_received: list_pair(3)?,
            },
            3 => Constraint::Claim {
                offered: list_pair(0)?,
                min_received: list_pair(1)?,
            },
            t => anyhow::bail!("unknown constraint tag {t}"),
        })
    }

    /// Decode a basic_order constraint payload. The basic module's data is
    /// always `(offered: List<(AssetClass, Int)>, min_received: List<…>)`
    /// regardless of ctor tag — on-chain, `extract_basic_fields` ignores the
    /// tag entirely and enforces only aggregate consumption/floors. The tag
    /// is scooper-side dispatch metadata: Deposit=0, Withdraw=1, Swap=2,
    /// Claim=3. A tag-2 basic entry with a single offered asset maps onto
    /// `Constraint::Swap` so the whole routing/blending dispatch applies;
    /// the aggregate on-chain check is satisfied by any execution shape the
    /// router produces.
    pub fn from_basic_plutus_constraint(pd: &PlutusData) -> anyhow::Result<Self> {
        let PlutusData::Constr(c) = pd else {
            anyhow::bail!("constraint must be a Constr");
        };
        let tag = if c.tag >= 121 && c.tag <= 127 {
            c.tag - 121
        } else if c.tag >= 1280 {
            c.tag - 1280 + 7
        } else {
            c.tag
        };
        let fields: Vec<PlutusData> = c.fields.clone().to_vec();
        let list_pair = |i: usize| -> anyhow::Result<Vec<(AssetClass, BigInt)>> {
            let f = fields
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("basic constraint missing field {i}"))?;
            <Vec<(AssetClass, BigInt)>>::from_plutus(f.clone())
                .map_err(|e| anyhow::anyhow!("decode list pair: {e}"))
        };
        Ok(match tag {
            0 => Constraint::Deposit {
                offered: list_pair(0)?,
                min_received: list_pair(1)?,
            },
            1 => Constraint::Withdraw {
                offered: list_pair(0)?,
                min_received: list_pair(1)?,
            },
            2 => {
                let offered = list_pair(0)?;
                let min_received = list_pair(1)?;
                match offered.as_slice() {
                    [(asset, amount)] => Constraint::Swap {
                        offered: asset.clone(),
                        original_offered: amount.clone(),
                        remaining_offered: amount.clone(),
                        min_received,
                    },
                    _ => anyhow::bail!(
                        "basic swap (tag 2) must offer exactly one asset to be \
                         routable (got {})",
                        offered.len()
                    ),
                }
            }
            3 => Constraint::Claim {
                offered: list_pair(0)?,
                min_received: list_pair(1)?,
            },
            t => anyhow::bail!("unknown basic constraint tag {t}"),
        })
    }

    /// Decode the constraint of interest from an OrderDatum by walking its
    /// constraints list to find an entry under either `swap_order_hash` or
    /// `basic_order_hash`, then decoding that inner Data with the matching
    /// class's field shapes (the two modules encode different layouts for
    /// tag 2).
    pub fn from_order_datum(
        datum: &OrderDatum,
        swap_order_hash: &[u8],
        basic_order_hash: &[u8],
    ) -> anyhow::Result<Self> {
        if let Some(data) = datum.find_constraint_by_hash(swap_order_hash) {
            return Self::from_plutus_constraint(data);
        }
        if let Some(data) = datum.find_constraint_by_hash(basic_order_hash) {
            return Self::from_basic_plutus_constraint(data);
        }
        anyhow::bail!("order has neither swap_order nor basic_order constraint")
    }

    /// Like [`Constraint::from_order_datum`], but also recognises
    /// strategy_order constraints. Strategy constraint data is a
    /// `StrategyConstraints { auth, final_destinations }` (no dispatch tag),
    /// so it must be selected by hash, never by ctor tag.
    pub fn from_order_datum_with_strategy(
        datum: &OrderDatum,
        swap_order_hash: &[u8],
        basic_order_hash: &[u8],
        strategy_order_hash: &[u8],
    ) -> anyhow::Result<Self> {
        if !strategy_order_hash.is_empty()
            && let Some(data) = datum.find_constraint_by_hash(strategy_order_hash)
        {
            let constraints = StrategyConstraints::from_plutus(data.clone())
                .map_err(|e| anyhow::anyhow!("decode StrategyConstraints: {e}"))?;
            return Ok(Constraint::Strategy { constraints });
        }
        Self::from_order_datum(datum, swap_order_hash, basic_order_hash)
    }

    /// For Swap orders: `(offered_asset, remaining_offered_qty)` borrowed from
    /// the constraint. Returns `None` for non-Swap orders — the scooper's
    /// batching path only handles swaps.
    pub fn swap_offered(&self) -> Option<(&AssetClass, &BigInt)> {
        match self {
            Constraint::Swap {
                offered,
                remaining_offered,
                ..
            } => Some((offered, remaining_offered)),
            _ => None,
        }
    }

    /// First entry of a Swap's `min_received` list, borrowed. Today's batching
    /// path treats orders as having a single counter-asset; multi-asset
    /// `min_received` constraints are TODO.
    pub fn swap_min_received(&self) -> Option<(&AssetClass, &BigInt)> {
        match self {
            Constraint::Swap { min_received, .. } => {
                let (a, q) = min_received.first()?;
                Some((a, q))
            }
            _ => None,
        }
    }
}

/// Decode an order's constraint into `(OrderDatum, Constraint)`, or an
/// enriched error string suitable for surfacing to operators / the API.
///
/// On failure the message names the constraint module hashes present on the
/// order that are *not* among the executable set (`swap`/`basic`/`strategy`)
/// — i.e. exactly the modules that make this scooper unable to execute the
/// order (route, fairness, a mismatched/undeployed strategy build, etc.).
/// Callers that hit `Err` should record the order as invalid with this
/// reason rather than dropping it silently.
pub fn decode_order_constraint(
    datum: &OrderDatum,
    swap_order_hash: &[u8],
    basic_order_hash: &[u8],
    strategy_order_hash: &[u8],
) -> Result<Constraint, String> {
    Constraint::from_order_datum_with_strategy(
        datum,
        swap_order_hash,
        basic_order_hash,
        strategy_order_hash,
    )
    .map_err(|e| {
        let supported: [&[u8]; 3] = [swap_order_hash, basic_order_hash, strategy_order_hash];
        let unsupported: Vec<String> = datum
            .constraints
            .iter()
            .filter(|(h, _)| !supported.contains(&h.as_slice()))
            .map(|(h, _)| hex::encode(h))
            .collect();
        if unsupported.is_empty() {
            format!("constraint decode: {e}")
        } else {
            format!(
                "constraint decode: {e}; unsupported constraint modules: [{}]",
                unsupported.join(", ")
            )
        }
    })
}

/// An order can be spent either to Scoop (execute) it, or to cancel it
#[derive(AsPlutus, Debug, PartialEq, Eq)]
pub enum OrderRedeemer {
    Cancel,
    Scoop { own_input_index: u64 },
}

// ──────────────────────────────────────────────────────────────────────────────
// Settings types
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SettingsDatum {
    pub settings_admin: Multisig,
    pub treasury_admin: Multisig,
    // Audit-final shape: entries are MultisigScript (usually Signature),
    // not raw key hashes; security_council added by GH #198.
    pub authorized_scoopers: Option<Vec<Multisig>>,
    pub security_council: Multisig,
    pub extension: PlutusData,
}

impl SettingsDatum {
    /// Position of `keyhash` in authorized_scoopers, matching plain
    /// Signature entries only. Multi-sig scooper entries never match a
    /// single key.
    pub fn scooper_index(&self, keyhash: &[u8]) -> Option<u64> {
        self.authorized_scoopers.as_ref().and_then(|list| {
            list.iter()
                .position(|m| matches!(m, Multisig::Signature(kh) if kh.as_slice() == keyhash))
                .map(|i| i as u64)
        })
    }
}
// SUN-301 removed `treasury_address` (unused), `order_modules`, and
// `min_share_batcher` from the global settings: order dispatch is
// OrderConfig-based (constraint script hashes from module config, not a
// settings tag map), and service-fee parameters live in the dedicated
// FeeSettings node (docs/fee-system.md).

// ──────────────────────────────────────────────────────────────────────────────
// Shared Plutus types (must be structs, not tuples, to match Aiken's Constr encoding)
// ──────────────────────────────────────────────────────────────────────────────

/// Aiken `Rational { num, den }` — encoded as Constr(0, [num, den]).
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Rational {
    pub num: BigInt,
    pub den: BigInt,
}

/// Aiken `OutputReference { transaction_id, output_index }`.
/// In PlutusV3, TxId is de-newtyped so this is Constr(0, [bytes, idx]).
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct OutputRef {
    #[serde(serialize_with = "hex_ser::bytes")]
    pub transaction_id: Vec<u8>,
    pub output_index: u64,
}

// ──────────────────────────────────────────────────────────────────────────────
// Pool type enum
// ──────────────────────────────────────────────────────────────────────────────

/// Identifies a pool's swap module and carries its parameters.
///
/// Using an enum (not traits) gives exhaustive compile-time checks when adding
/// new variants and avoids boxing/dynamic dispatch.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum PoolType {
    ConstantProduct {
        fee: Rational,
    },
    ConstantSum {
        prices: Vec<BigInt>,
        fee: Rational,
        bounty_k: Rational,
        balance_fee: Rational,
    },
    /// Single-range concentrated liquidity. `sqrt_price_a` and `sqrt_price_b`
    /// bound the pool's price range (`a < b`). The validator works on
    /// virtual reserves `VA = a·spb_num + L·spb_den`, `VB = b·spa_den + L·spa_num`
    /// and the CP invariant `VA·VB = L²·spb_num·spa_den`. Cross-range
    /// execution is done by the router splitting across multiple CL pools.
    ConcentratedLiquidity {
        sqrt_price_a: Rational,
        sqrt_price_b: Rational,
        fee: Rational,
    },
    /// Curve-style stableswap (`validators/modules/stableswap.ak`). The whole
    /// config preimage rides along: the Operate redeemer re-sends it, the
    /// output datum's `module_state` slot is its hash, and `rates` may change
    /// on chain through a manager-signed tag-7 step, so the scooper must
    /// always price against the config it holds for the pool's current UTxO.
    StableSwap {
        config: StableSwapConfig,
    },
}

// ──────────────────────────────────────────────────────────────────────────────
// Module config types
// ──────────────────────────────────────────────────────────────────────────────

/// `StableSwapConfig` (`validators/modules/stableswap.ak`). Stored in the
/// pool datum's `module_state` as `blake2b_256(serialise_data(config))`; the
/// preimage travels in every `StableSwapRedeemer::Operate` entry.
///
/// `rates` is one positive multiplier per pool asset, positionally aligned
/// with the pool's `assets`. A tag-7 transcript step (rate update, signed by
/// `rate_manager`) replaces it and the module rewrites the hash; the other
/// fields change only through an Upgrade ceremony.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct StableSwapConfig {
    pub linear_amplification: BigInt,
    pub fee: Rational,
    pub rates: Vec<BigInt>,
    pub rate_manager: Option<Multisig>,
    pub monotone_rates: bool,
    pub max_rate_step: Option<Rational>,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ConstantProductConfig {
    pub fee: Rational,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ConstantSumConfig {
    pub prices: Vec<BigInt>,
    pub fee: Rational,
    /// Quadratic rebalance bounty parameter. `(0, 1)` disables the bounty
    /// mechanism.
    pub bounty_k: Rational,
    /// SUN-310: fee rate charged on the swap portion of a `tag_claim` step,
    /// in place of `fee`. Must satisfy `0 <= balance_fee <= fee`; `0` is the
    /// full waiver (value-neutral swap portion, `v_increase = 0`,
    /// `fee_budget = 0`). The claim step's LP-budget pin is on the
    /// OP-PORTION V (claim restored), so the claim is bounded by cap_b plus
    /// the no-overshoot guard — cap_a (claim <= v_increase) is gone.
    pub balance_fee: Rational,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ConcentratedLiquidityConfig {
    pub sqrt_price_a: Rational,
    pub sqrt_price_b: Rational,
    pub fee: Rational,
}

// ──────────────────────────────────────────────────────────────────────────────
// Withdrawal redeemer types
// ──────────────────────────────────────────────────────────────────────────────

/// Redeemer for the base order_validator withdraw handler (PR #11).
///
/// `configs[i]` enumerates each OrderConfig settings entry referenced by the
/// orders in this tx, by its reference-input index + token name. `entries[j]`
/// then enumerates each order being scooped — `output_index` points at the
/// order's fulfillment output and `config_index` picks which entry of
/// `configs` the order's `config_token` matches.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct OrderValidatorRedeemer {
    pub configs: Vec<OrderValidatorConfig>,
    pub entries: Vec<OrderValidatorEntry>,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct OrderValidatorConfig {
    pub ref_index: u64,
    pub token: Vec<u8>,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct OrderValidatorEntry {
    pub output_index: u64,
    pub config_index: u64,
}

/// Settings-entry datum for an OrderConfig (PR #11). One of these is minted
/// per (role, constraint set) pair — e.g. role="swap" gets
/// `[swap_order, route_order, fairness_order]`. Each order's `config_token`
/// references the entry whose `required_constraints` it claims to fulfill.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct OrderConfig {
    #[serde(serialize_with = "hex_ser::bytes")]
    pub label: Vec<u8>,
    #[serde(serialize_with = "hex_ser::vec_bytes")]
    pub required_constraints: Vec<Vec<u8>>,
}

/// Redeemer for the fairness_order_constraint withdraw handler — pins which
/// authorized scooper signed the tx by referencing the global settings entry.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct FairnessOrderRedeemer {
    pub settings_input_index: u64,
    pub authorized_scooper_index: u64,
}

// ──────────────────────────────────────────────────────────────────────────────
// Strategy orders (lib/types/strategy.ak, validators/constraints/strategy_order.ak)
// ──────────────────────────────────────────────────────────────────────────────

/// Aiken `IntervalBoundType` (aiken/interval): NegativeInfinity = Constr 0,
/// Finite(t) = Constr 1 [t] (POSIX ms), PositiveInfinity = Constr 2.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub enum IntervalBoundType {
    NegativeInfinity,
    Finite(BigInt),
    PositiveInfinity,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct IntervalBound {
    pub bound_type: IntervalBoundType,
    pub is_inclusive: bool,
}

/// Aiken `ValidityRange` = `Interval { lower_bound, upper_bound }`, in POSIX ms.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct StrategyValidityRange {
    pub lower_bound: IntervalBound,
    pub upper_bound: IntervalBound,
}

/// Aiken `StrategyExecution` (lib/types/strategy.ak). The bytes the strategy
/// key signs are the CBOR of *this* structure alone (the on-chain validator
/// recomputes them via `cbor.serialise(sse.execution)`).
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct StrategyExecution {
    /// The order UTxO this execution authorizes a scoop of.
    pub order_ref: OutputRef,
    /// Window (POSIX ms) the execution is valid in; the scoop tx's validity
    /// range must sit inside it.
    pub validity_range: StrategyValidityRange,
    /// Minimum quantities the fulfillment output must carry.
    pub min_received: Vec<(AssetClass, BigInt)>,
    /// Index into the strategy constraint's `final_destinations` list; `None`
    /// = use the order datum's destination. (Named `final` in Aiken.)
    pub final_destination: Option<BigInt>,
    /// Opaque extension Data.
    pub extension: PlutusData,
}

/// Aiken `SignedStrategyExecution`: the execution plus Ed25519 signatures.
/// Each signature pair is `(verification_key, signature)` — the validator
/// matches `blake2b_224(verification_key)` against the constraint's
/// `auth` multisig and verifies the signature over `cbor(execution)`.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SignedStrategyExecution {
    pub execution: StrategyExecution,
    #[serde(serialize_with = "hex_ser::vec_bytes_pair_as_map")]
    pub signatures: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Decoded `constraint_data` for a strategy_order constraint entry
/// (lib/constraints/strategy.ak `StrategyConstraints`).
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct StrategyConstraints {
    /// Who may sign executions for this order.
    pub auth: crate::multisig::Multisig,
    /// Candidate destinations an execution may pick via `final_destination`.
    pub final_destinations: Vec<Destination>,
}

/// `BountyClaim` — the `operation_data` of a CS `tag_claim` (5) transcript
/// entry (lib/modules/cs_check.ak). `amount` units of `asset` are extracted
/// from the pool as a rebalance bounty, on top of the entry's swap portion.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BountyClaim {
    pub asset: AssetClass,
    pub amount: BigInt,
}

// ──────────────────────────────────────────────────────────────────────────────
// CS operation tag constants (lib/modules/cs_check.ak)
// ──────────────────────────────────────────────────────────────────────────────

/// Constant-sum swap step.
pub const TAG_SWAP: u64 = 3;
/// Constant-sum LP-redemption step (SUN-202).
pub const TAG_WITHDRAW: u64 = 4;
/// Constant-sum bounty-claim step. `operation_data` is `BountyClaim { asset, amount }`.
pub const TAG_CLAIM: u64 = 5;
/// Constant-sum proportional-deposit step.
pub const TAG_DEPOSIT: u64 = 6;
/// Stableswap rate-update step (`ss_check.tag_update_rates`). Admitted only
/// as the first transcript entry, manager-signed; the scooper never builds
/// one but must recognise it when it indexes a scoop.
pub const TAG_UPDATE_RATES: u64 = 7;

// ──────────────────────────────────────────────────────────────────────────────
// Stableswap transcript operation_data (lib/modules/ss_check.ak)
// ──────────────────────────────────────────────────────────────────────────────

/// `operation_data` of a stableswap tag-3 swap step. Both values are
/// computed off-chain (`ss_math`) and pinned on chain: `raw_swap_result` is
/// the gross output in numeraire units at `calc_precision` scale, before the
/// fee; `next_sum_invariant` is `D` for the post-step reserves.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SwapStep {
    pub raw_swap_result: BigInt,
    pub next_sum_invariant: BigInt,
}

/// `operation_data` of a stableswap tag-6 deposit or tag-4 withdraw step.
/// `target_delta_d` is the declared change in `D` (scaled), positive for a
/// deposit and negative for a withdrawal.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct LiquidityStep {
    pub target_delta_d: BigInt,
    pub next_sum_invariant: BigInt,
}

/// `operation_data` of a stableswap tag-7 rate update. `rates` replaces the
/// config's rate vector for every later step and for the output datum's
/// config hash.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RateUpdate {
    pub rates: Vec<BigInt>,
    pub next_sum_invariant: BigInt,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum ConstantProductRedeemer {
    Create {
        initial_state: ConstantProductConfig,
    },
    Operate {
        entries: Vec<CPOperateEntry>,
    },
    /// SUN-103/ADR-0006 teardown; parsed only (indexer tolerance).
    Destroy {
        entries: Vec<PlutusData>,
    },
}

/// Redeemer for the pool_mint policy. The scooper only uses `MintLP` (to mint
/// LP tokens for Deposits); `CreatePool` is for pool genesis and `BurnPool`
/// for full withdrawals — both run by the CLI, not the scooper.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum PoolMintRedeemer {
    CreatePool {
        seed_utxo: OutputRef,
        settings_ref_index: u64,
    },
    /// SUN-102: one MintLP redeemer may mint/burn LP for several pools.
    MintLP {
        pool_idents: Vec<Ident>,
    },
    BurnPool {
        pool_ident: Ident,
    },
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct CPOperateEntry {
    pub pool_oref: OutputRef,
    pub config: ConstantProductConfig,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum ConstantSumRedeemer {
    /// SUN-005: Create pins the created pool's output index (initial_state
    /// stays field 0 — pool_mint hashes it for module_state).
    Create {
        initial_state: PlutusData,
        pool_output_index: u64,
    },
    Operate {
        entries: Vec<CSOperateEntry>,
    },
    /// SUN-103/ADR-0006 teardown; parsed only.
    Destroy {
        entries: Vec<PlutusData>,
    },
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum ConcentratedLiquidityRedeemer {
    Create {
        initial_state: ConcentratedLiquidityConfig,
    },
    Operate {
        entries: Vec<CLOperateEntry>,
    },
    /// SUN-103/ADR-0006 teardown; parsed only.
    Destroy {
        entries: Vec<PlutusData>,
    },
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct CLOperateEntry {
    pub pool_oref: OutputRef,
    pub config: ConcentratedLiquidityConfig,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct CSOperateEntry {
    pub pool_oref: OutputRef,
    pub config: ConstantSumConfig,
}

// `Create` carries the whole config; `AsPlutus` has no `Box` support and the
// enum is parsed, not stored, so the size gap is harmless.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum StableSwapRedeemer {
    /// `initial_state` stays field 0 (pool_mint hashes it for module_state).
    /// `sum_invariant` is `D` for the pool's initial reserves.
    Create {
        initial_state: StableSwapConfig,
        pool_output_index: u64,
        sum_invariant: BigInt,
    },
    Operate {
        entries: Vec<SSOperateEntry>,
    },
    /// SUN-103/ADR-0006 teardown; parsed only.
    Destroy {
        entries: Vec<PlutusData>,
    },
}

/// One pool's entry in `StableSwapRedeemer::Operate`. `config` is the config
/// as stored in the pool INPUT's `module_state`; `sum_invariant` is `D` for
/// the pool input's reserves at `config.rates`, which the module checks
/// before it walks the transcript.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct SSOperateEntry {
    pub pool_oref: OutputRef,
    pub config: StableSwapConfig,
    pub sum_invariant: BigInt,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FeeSplitConfig {
    pub protocol_share: Rational,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum FeeSplitRedeemer {
    /// SUN-005/ADR-0003: Create pins the pool output and the PoolConfig +
    /// Approved Stake List reference-input indices (fee tiers). `config`
    /// stays field 0 (hashed for module_state).
    Create {
        config: FeeSplitConfig,
        pool_output_index: u64,
        settings_ref_index: u64,
        stake_list_ref_index: u64,
    },
    Operate {
        entries: Vec<FSOperateEntry>,
    },
    /// SUN-103/ADR-0006 teardown; parsed only.
    Destroy {
        entries: Vec<PlutusData>,
    },
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct FSOperateEntry {
    pub pool_oref: OutputRef,
    pub config: FeeSplitConfig,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum FairnessRedeemer {
    Create,
    Operate {
        entries: Vec<FairnessOperateEntry>,
    },
    /// SUN-103/ADR-0006 teardown; parsed only.
    Destroy {
        entries: Vec<PlutusData>,
    },
}

// Audit-final shape: names the signing scooper by index into the settings'
// authorized_scoopers list instead of (pool_ident, raw key).
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct FairnessOperateEntry {
    pub pool_oref: OutputRef,
    pub scooper_idx: BigInt,
}

// ──────────────────────────────────────────────────────────────────────────────
// Slot-to-POSIX-time configuration
// ──────────────────────────────────────────────────────────────────────────────

/// Parameters for converting slot numbers to POSIX milliseconds.
/// The Cardano ledger uses POSIX time in ScriptContext validity ranges.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SlotConfig {
    /// Slot number at the start of the reference era (e.g. Shelley start).
    pub zero_slot: u64,
    /// POSIX time in milliseconds at `zero_slot`.
    pub zero_time: u64,
    /// Slot length in milliseconds (typically 1000).
    pub slot_length: u64,
}

impl SlotConfig {
    /// Convert a slot number to POSIX time in milliseconds.
    pub fn slot_to_posix_ms(&self, slot: u64) -> u64 {
        self.zero_time + (slot.saturating_sub(self.zero_slot)) * self.slot_length
    }

    /// Inverse of [`slot_to_posix_ms`](Self::slot_to_posix_ms).
    pub fn posix_ms_to_slot(&self, posix_ms: u64) -> u64 {
        self.zero_slot + posix_ms.saturating_sub(self.zero_time) / self.slot_length.max(1)
    }

    /// The slot the chain is at *right now*, by wall clock.
    ///
    /// The observed tip only advances when a block arrives, so it lags by
    /// however long the current block gap has run. Anything that means "now"
    /// — a transaction's TTL, chain-expiry, quarantine windows — wants this,
    /// not the tip. See [`ValidityWindow`](crate::sundaev4::tx_builder::ValidityWindow).
    pub fn wall_clock_slot(&self) -> u64 {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(self.zero_time);
        self.posix_ms_to_slot(now_ms)
    }
}

// Execution configuration
// ──────────────────────────────────────────────────────────────────────────────

/// Per-pool config for pools that require operator-provided parameters.
///
/// CS pools carry `{ prices, fee }` in their on-chain module config hash,
/// but the hash is derived from the config — the full config is not on-chain.
/// The operator must provide it here, keyed by pool ident (hex).
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum PoolConfig {
    ConstantSum {
        prices: Vec<i64>,
        fee: (u64, u64),
    },
    /// Operator-provided override for CL pools whose Create-redeemer
    /// config wasn't recoverable from chain history. Stored as a sqrt-
    /// price range `[a, b]` with `a < b`, fee as `(num, den)`.
    ConcentratedLiquidity {
        sqrt_price_a: (i64, i64),
        sqrt_price_b: (i64, i64),
        fee: (u64, u64),
    },
}

/// The network id the scooper writes into every address it builds.
///
/// Not a v4 config key: `main` derives it from
/// `acropolis.global.startup.network-name`, the key that already selects the
/// chain, so the two cannot disagree. The `Testnet` default exists for
/// directly-constructed test fixtures only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AddressNetwork {
    #[default]
    Testnet,
    Mainnet,
}

impl AddressNetwork {
    /// "mainnet" is the only network with network id 1; preprod, preview and
    /// custom devnets all use id 0.
    pub fn from_network_name(name: &str) -> Self {
        if name == "mainnet" {
            AddressNetwork::Mainnet
        } else {
            AddressNetwork::Testnet
        }
    }

    pub fn pallas(self) -> pallas_addresses::Network {
        match self {
            AddressNetwork::Testnet => pallas_addresses::Network::Testnet,
            AddressNetwork::Mainnet => pallas_addresses::Network::Mainnet,
        }
    }

    pub fn id(self) -> u8 {
        match self {
            AddressNetwork::Testnet => 0,
            AddressNetwork::Mainnet => 1,
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ScooperExecution {
    /// Set at startup by `SundaeV4Protocol::set_network`; see [`AddressNetwork`].
    #[serde(skip)]
    pub network: AddressNetwork,
    #[serde(default)]
    pub scooper_secret_key: String,
    #[serde(default)]
    pub scooper_secret_key_file: Option<String>,
    /// Optional 28-byte hex stake key hash to attach as the delegation part
    /// of the scooper's address. CIP-1852 wallets use base addresses (payment
    /// & staking), funds sent to those addresses are unreachable from an
    /// enterprise (payment-only) address. Leave unset to derive an enterprise
    /// address (works for fresh testnet keys with no staking).
    #[serde(default)]
    pub scooper_stake_keyhash: Option<String>,
    pub submit_url: String,
    pub fee: (u64, u64),
    pub protocol_share: (u64, u64),
    pub module_scripts: ModuleScripts,
    pub plutus_v3_cost_model: Vec<i64>,
    /// PlutusV2 cost model — required only for partner-protocol legs whose
    /// validators are V2 (Butane's mint/spend/upgradable). Absent = V2
    /// scripts can't be evaluated and butane edges stay disabled.
    #[serde(default)]
    pub plutus_v2_cost_model: Option<Vec<i64>>,
    pub slot_config: SlotConfig,
    /// Per-pool configs for pools requiring operator-provided parameters.
    /// Keyed by pool ident (hex string).
    #[serde(default)]
    pub pool_configs: std::collections::BTreeMap<String, PoolConfig>,
    /// Maximum transaction execution memory units. Default: 14_000_000.
    #[serde(default = "default_max_tx_ex_mem")]
    pub max_tx_ex_mem: u64,
    /// Maximum transaction execution CPU steps. Default: 10_000_000_000.
    #[serde(default = "default_max_tx_ex_steps")]
    pub max_tx_ex_steps: u64,
    /// Maximum transaction size in bytes. Default: 16_384.
    #[serde(default = "default_max_tx_size")]
    pub max_tx_size: usize,
    /// Budget padding as (numerator, denominator). Padded = raw * num / den.
    /// Budgets are evaluated on the first-pass tx; the final rebuild shifts
    /// output values (~0.2% on a redeemer) and uplc-turbo's step accounting
    /// differs from cardano-node's (~0.04%). Default: (21, 20), 5%.
    #[serde(default = "default_budget_padding")]
    pub budget_padding: (u64, u64),
    /// Pool idents (hex) to exclude from scooping. Useful as an operator
    /// escape hatch for pools the scooper can't currently fulfill (e.g. a
    /// pool whose on-chain config the scooper hasn't been able to recover,
    /// or any pool the operator wants to skip).
    #[serde(default)]
    pub blacklisted_pools: std::collections::BTreeSet<String>,
    /// Lovelace charged against an order's budget for each pool its route
    /// touches. 0 = no limit. Together with `cost_per_step_lovelace` this
    /// gates router fan-out by what the order paid for: a 1-ADA order gets a
    /// direct match, a 5-ADA order can spread across many pools and hops.
    #[serde(default)]
    pub cost_per_pool_lovelace: u64,
    /// Lovelace charged against an order's budget for each routing "step"
    /// (split entry across all hops). 0 = no limit.
    #[serde(default)]
    pub cost_per_step_lovelace: u64,
    /// Peer scooper base URLs to gossip accepted strategy intents to
    /// (e.g. "https://scooper-2.example.com"). Peers dedup by intent id, so
    /// forwarding loops terminate.
    #[serde(default)]
    pub strategy_peers: Vec<String>,
    /// Off-protocol conversion edges the router may consider (Butane ADAb
    /// mint, staking wrappers, …). Each edge must be enabled AND have its
    /// tx composition implemented before the router will use it — see
    /// `conversions::routable_edges`.
    #[serde(default)]
    pub conversions: Vec<crate::sundaev4::conversions::ConversionEdgeConfig>,
    /// Butane v2 integration (mint synthetics via the underlying window as
    /// router conversion edges). Absent/broken config degrades to disabled.
    #[serde(default)]
    pub butane: Option<crate::sundaev4::butane::ButaneConfig>,
    /// Enable partial fills with this profitability margin (num, den): a
    /// partial fill must cover at least margin × the scooper's per-order fee
    /// share in pro-rata allowance. None (default) = partial fills disabled.
    /// swap.ak caps each fill's fee at allowance·fill/original, so the fill
    /// fraction must clear margin·fee_share/allowance to be worth a scoop.
    #[serde(default)]
    pub partial_fill_margin: Option<(u64, u64)>,
    /// Estimated per-order fee share used by the partial-fill floor
    /// (conservative; the real fee is known only after building).
    #[serde(default = "default_partial_fill_fee_estimate")]
    pub partial_fill_fee_estimate: u64,
}

fn default_partial_fill_fee_estimate() -> u64 {
    2_500_000
}

fn default_max_tx_ex_mem() -> u64 {
    14_000_000
}
fn default_max_tx_ex_steps() -> u64 {
    10_000_000_000
}
fn default_max_tx_size() -> usize {
    16_384
}
pub(crate) fn default_budget_padding() -> (u64, u64) {
    (21, 20)
}

impl ScooperExecution {
    /// If `scooper_secret_key_file` is set, read the file and populate
    /// `scooper_secret_key`. Either source is normalized to raw hex of a
    /// 32-byte or 64-byte key — the two forms every downstream key reader
    /// accepts. Call this once at startup.
    pub fn resolve_secret_key(&mut self) -> anyhow::Result<()> {
        if let Some(path) = &self.scooper_secret_key_file {
            let contents = std::fs::read_to_string(path)
                .with_context(|| format!("reading secret key file: {path}"))?;
            self.scooper_secret_key = normalize_secret_key_hex(&contents)
                .with_context(|| format!("secret key file: {path}"))?;
        } else if !self.scooper_secret_key.is_empty() {
            self.scooper_secret_key =
                normalize_secret_key_hex(&self.scooper_secret_key).context("scooper-secret-key")?;
        }
        anyhow::ensure!(
            !self.scooper_secret_key.is_empty(),
            "scooper-secret-key or scooper-secret-key-file must be set"
        );
        Ok(())
    }
}

/// Normalize a signing key to raw hex of a 32-byte (standard) or 64-byte
/// (extended) ed25519 secret key.
///
/// Accepts raw hex, or a cardano-cli text envelope whose `cborHex` is a CBOR
/// bytestring of:
/// - 32 bytes (`5820`): `PaymentSigningKeyShelley_ed25519`;
/// - 64 bytes (`5840`): a bare extended secret key;
/// - 128 bytes (`5880`): `PaymentExtendedSigningKeyShelley_ed25519_bip32`,
///   laid out `extended_secret(64) ‖ public_key(32) ‖ chain_code(32)`. The
///   chain code only matters for HD derivation, so it is dropped. The embedded
///   public key must match the one derived from the secret, so a file with a
///   different layout is rejected instead of signing with the wrong key.
pub fn normalize_secret_key_hex(input: &str) -> anyhow::Result<String> {
    let trimmed = input.trim();
    if !trimmed.starts_with('{') {
        return Ok(trimmed.to_string());
    }
    let envelope: serde_json::Value =
        serde_json::from_str(trimmed).context("invalid signing key JSON envelope")?;
    let cbor_hex =
        envelope["cborHex"].as_str().context("missing cborHex field in signing key envelope")?;
    let cbor = hex::decode(cbor_hex).context("invalid cborHex")?;
    let key = match cbor.as_slice() {
        [0x58, len, rest @ ..] if *len as usize == rest.len() => rest,
        _ => anyhow::bail!(
            "cborHex must be a CBOR bytestring of 32, 64 or 128 bytes (prefix 5820, 5840 or 5880)"
        ),
    };
    match key.len() {
        32 | 64 => Ok(hex::encode(key)),
        128 => {
            use pallas_crypto::key::ed25519::SecretKeyExtended;
            let secret: [u8; 64] = key[..64].try_into().unwrap();
            let derived = SecretKeyExtended::from_bytes(secret)
                .map_err(|e| anyhow::anyhow!("invalid extended ed25519 secret key: {e}"))?
                .public_key();
            anyhow::ensure!(
                derived.as_ref() == &key[64..96],
                "128-byte extended key: embedded public key does not match the secret key"
            );
            Ok(hex::encode(secret))
        }
        n => anyhow::bail!("signing key must be 32, 64 or 128 bytes, got {n}"),
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ModuleScripts {
    /// Optional: only required when scooping constant-product pools (the
    /// audit-final cs-launch deployment does not publish CP).
    #[serde(default)]
    pub constant_product: Option<ScriptRefInfo>,
    pub fee_split: ScriptRefInfo,
    pub fairness: ScriptRefInfo,
    pub pool: ScriptRefInfo,
    pub order: ScriptRefInfo,
    pub pool_mint: ScriptRefInfo,
    pub settings: ScriptRefInfo,
    /// Optional: only required when scooping constant-sum pools.
    #[serde(default)]
    pub constant_sum: Option<ScriptRefInfo>,
    /// Optional: only required when scooping concentrated-liquidity pools.
    #[serde(default)]
    pub concentrated_liquidity: Option<ScriptRefInfo>,
    /// Optional: only required when scooping stableswap pools. Without it
    /// the scooper cannot classify a stableswap pool and skips it (see
    /// `detect_pool_type`).
    #[serde(default)]
    pub stableswap: Option<ScriptRefInfo>,
    /// Per-class constraint validators (modular order constraints, PR #11).
    /// Every order's OrderConfig lists which constraint hashes it requires;
    /// the order_validator's withdraw handler requires each listed constraint
    /// to also withdraw. Slots are `Option` so a deployment that doesn't use
    /// e.g. strategy orders can omit that module.
    #[serde(default)]
    pub swap_order: Option<ScriptRefInfo>,
    /// `basic_order_constraint` covers Deposit/Withdraw/Claim shapes.
    #[serde(default)]
    pub basic_order: Option<ScriptRefInfo>,
    /// `route_order_constraint` carries the per-order route table (pool
    /// whitelist + per-step pool_input / transcript_step indices).
    #[serde(default)]
    pub route_order: Option<ScriptRefInfo>,
    /// `fairness_order_constraint` pins the authorized scooper for the tx
    /// via the global settings entry's authorized_scoopers list.
    #[serde(default)]
    pub fairness_order: Option<ScriptRefInfo>,
    /// `strategy_order_constraint` accepts a `List<SignedStrategyExecution>`
    /// redeemer signed off-chain. The scooper doesn't yet construct these —
    /// optional until strategy execution ingestion is designed.
    #[serde(default)]
    pub strategy_order: Option<ScriptRefInfo>,
    /// `fee_constraint` — the once-per-scoop service-fee aggregator
    /// (docs/fee-system.md). Optional: absent = fee-bearing OrderConfigs
    /// can't be scooped.
    #[serde(default)]
    pub fee_constraint: Option<ScriptRefInfo>,
}

#[serde_with::serde_as]
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ScriptRefInfo {
    pub hash: ScriptHash,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub ref_utxo: crate::cardano_types::TransactionInput,
    /// Hex-encoded CBOR-wrapped script (double-wrapped: CBOR bytestring containing FLAT-encoded UPLC)
    /// Hex-encoded script CBOR, populated from blueprint. Retained for serde round-trip.
    #[serde(default)]
    #[allow(dead_code)]
    pub script_cbor: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Indexed state wrapper types
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize)]
pub struct SundaeV4Pool {
    pub input: TransactionInput,
    /// The pool UTxO's full address bytes, stake credential included. A scoop
    /// pays the pool back to exactly this address: the pool validator requires
    /// the continuation at the same address, and a pool may carry any stake
    /// credential.
    #[serde(serialize_with = "hex_ser::bytes")]
    pub address: Vec<u8>,
    pub value: Value,
    pub pool_datum: PoolDatum,
    pub pool_type: PoolType,
    pub slot: u64,
    /// Resolved per-pool fee_split config (`protocol_share`), recovered from
    /// the fee_split module's `Create`/`Operate` redeemer. `None` if not yet
    /// recovered — tx_builder falls back to `exec.protocol_share` in that
    /// case, which is wrong for any pool created with a non-default share.
    pub fee_split_config: Option<FeeSplitConfig>,
}

impl PartialOrd for SundaeV4Pool {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.slot.cmp(&other.slot))
    }
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct SundaeV4Order {
    pub input: TransactionInput,
    pub value: Value,
    pub datum: OrderDatum,
    /// Decoded constraint, computed once at index time so consumers don't re-parse.
    pub constraint: Constraint,
    pub slot: u64,
}

impl SundaeV4Order {
    /// Convenience: `(offered_asset, remaining_offered_qty)` for Swap orders.
    /// Panics on non-Swap; only call from paths that have already filtered to
    /// swap-shaped orders.
    pub fn swap_offered(&self) -> (&AssetClass, &BigInt) {
        self.constraint
            .swap_offered()
            .expect("SundaeV4Order::swap_offered called on non-Swap constraint")
    }

    /// First entry of the Swap's min_received list. See
    /// [`Constraint::swap_min_received`].
    pub fn swap_min_received(&self) -> (&AssetClass, &BigInt) {
        self.constraint
            .swap_min_received()
            .expect("SundaeV4Order::swap_min_received called on non-Swap constraint")
    }

    /// Test-only constructor for a Swap-shaped order. `offer` is `(asset, qty)`
    /// where qty becomes both `original_offered` and `remaining_offered` (no
    /// partial-fill state). `min_received` becomes a single-entry list. Uses
    /// `unit` for `extension`; `budget` is the per-order tx-fee budget.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub fn test_swap_order(
        input: TransactionInput,
        value: Value,
        owner: Multisig,
        destination: Destination,
        offer: (AssetClass, BigInt),
        min_received: (AssetClass, BigInt),
        budget: BigInt,
        slot: u64,
    ) -> Self {
        let (offer_asset, offer_qty) = offer;
        let min_recv_list = vec![min_received];
        let swap_data = PlutusData::Constr(pallas_primitives::Constr {
            tag: 121 + 2, // Swap
            any_constructor: None,
            fields: pallas_codec::utils::MaybeIndefArray::Def(vec![
                offer_asset.to_plutus(),
                offer_qty.clone().to_plutus(), // original_offered
                offer_qty.to_plutus(),         // remaining_offered
                min_recv_list.to_plutus(),
            ]),
        });
        let unit = PlutusData::Constr(pallas_primitives::Constr {
            tag: 121,
            any_constructor: None,
            fields: pallas_codec::utils::MaybeIndefArray::Def(vec![]),
        });
        // Test-only synthetic constraint hash for the swap_order_constraint.
        // The decoded `Constraint` only depends on the inner Data's ctor tag,
        // so any hash that matches the one passed to `from_order_datum`
        // works. We use a fixed test hash here and decode via
        // `from_plutus_constraint` directly (which doesn't care about hashes).
        const TEST_SWAP_HASH: [u8; 28] = [0xAA; 28];
        let constraints = vec![(TEST_SWAP_HASH.to_vec(), swap_data.clone())];
        let datum = OrderDatum {
            owner,
            destination,
            // Single-execution shape: service_budget == max_per_execution,
            // so the terminal settlement deducts exactly the budget.
            service_budget: budget.clone(),
            max_per_execution: budget,
            config_token: Vec::new(),
            constraints,
            extension: unit,
        };
        let constraint = Constraint::from_plutus_constraint(&swap_data)
            .expect("test_swap_order: constraint should decode");
        SundaeV4Order {
            input,
            value,
            datum,
            constraint,
            slot,
        }
    }
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct SundaeV4Settings {
    pub input: TransactionInput,
    pub value: crate::cardano_types::Value,
    pub datum: SettingsDatum,
    pub slot: u64,
}

/// The FeeSettings settings node (docs/fee-system.md):
/// `FeeSettings { base_fee: Int }` under the configured entry token.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SundaeV4FeeSettings {
    pub input: TransactionInput,
    /// The node's entry-token name under the settings policy — needed to
    /// reconstruct the node's value for local evaluation.
    pub token: Vec<u8>,
    pub base_fee: u64,
    pub slot: u64,
}

/// FeeSettings node datum. Kept minimal: base_fee is the only field the
/// launch fee system reads (flat per-execution pricing, ADR-0009).
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FeeSettingsDatum {
    pub base_fee: BigInt,
}

// ──────────────────────────────────────────────────────────────────────────────
// Protocol configuration
// ──────────────────────────────────────────────────────────────────────────────

#[serde_with::serde_as]
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SundaeV4Protocol {
    pub pool_script_hash: ScriptHash,
    pub order_script_hashes: Vec<ScriptHash>,
    pub settings_script_hash: ScriptHash,
    pub settings_nft: AssetClass,
    pub pool_nft_policy: ScriptHash,
    /// Token name (hex) of the FeeSettings settings node (docs/fee-system.md).
    /// Required to scoop fee-bearing OrderConfigs; the indexer tracks the
    /// node's UTxO + base_fee under this token at the settings address.
    #[serde(default)]
    pub fee_settings_token: Option<String>,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub starting_point: Point,
    pub execution: Option<ScooperExecution>,
    /// Local-node mempool monitor (N2C LocalTxMonitor). Absent = disabled.
    #[serde(default)]
    pub mempool: Option<crate::mempool::MempoolMonitorConfig>,
    /// Optional blueprint for deriving scripts. Retained for serde round-trip.
    #[serde(default)]
    #[allow(dead_code)]
    pub blueprint: Option<crate::blueprint::Blueprint>,
    /// Set at startup by `set_network`; see [`AddressNetwork`].
    #[serde(skip)]
    pub network: AddressNetwork,
}

impl SundaeV4Protocol {
    /// Record the address network here and on the execution config, which the
    /// tx builder receives without the rest of the protocol config.
    pub fn set_network(&mut self, network: AddressNetwork) {
        self.network = network;
        if let Some(exec) = &mut self.execution {
            exec.network = network;
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod secret_key_tests {
    use super::normalize_secret_key_hex;
    use pallas_crypto::key::ed25519::SecretKeyExtended;

    /// A throwaway extended key with the ed25519 bit tweaks applied.
    fn extended_secret() -> [u8; 64] {
        let mut k: [u8; 64] = std::array::from_fn(|i| (i as u8).wrapping_mul(37).wrapping_add(11));
        k[0] &= 0b1111_1000;
        k[31] = (k[31] & 0b0011_1111) | 0b0100_0000;
        k
    }

    fn envelope(type_: &str, cbor_hex: &str) -> String {
        format!(
            r#"{{"type": "{type_}", "description": "Payment Signing Key", "cborHex": "{cbor_hex}"}}"#
        )
    }

    #[test]
    fn bip32_extended_envelope_yields_the_64_byte_secret() {
        let secret = extended_secret();
        let public = SecretKeyExtended::from_bytes(secret).unwrap().public_key();
        let chain_code = [0xccu8; 32];
        let cbor_hex = format!(
            "5880{}{}{}",
            hex::encode(secret),
            hex::encode(public.as_ref()),
            hex::encode(chain_code)
        );
        let file = envelope("PaymentExtendedSigningKeyShelley_ed25519_bip32", &cbor_hex);
        assert_eq!(
            normalize_secret_key_hex(&file).unwrap(),
            hex::encode(secret)
        );
    }

    #[test]
    fn bip32_extended_envelope_with_wrong_public_key_is_rejected() {
        let cbor_hex = format!("5880{}{}", hex::encode(extended_secret()), "00".repeat(64));
        let file = envelope("PaymentExtendedSigningKeyShelley_ed25519_bip32", &cbor_hex);
        let err = normalize_secret_key_hex(&file).unwrap_err().to_string();
        assert!(err.contains("does not match"), "{err}");
    }

    #[test]
    fn standard_and_bare_extended_envelopes_strip_the_cbor_header() {
        let seed = "ab".repeat(32);
        let file = envelope("PaymentSigningKeyShelley_ed25519", &format!("5820{seed}"));
        assert_eq!(normalize_secret_key_hex(&file).unwrap(), seed);

        let ext = hex::encode(extended_secret());
        let file = envelope(
            "PaymentExtendedSigningKeyShelley_ed25519",
            &format!("5840{ext}"),
        );
        assert_eq!(normalize_secret_key_hex(&file).unwrap(), ext);
    }

    #[test]
    fn raw_hex_passes_through_trimmed() {
        let seed = "ab".repeat(32);
        assert_eq!(
            normalize_secret_key_hex(&format!("  {seed}\n")).unwrap(),
            seed
        );
    }

    #[test]
    fn cbor_length_mismatch_is_rejected() {
        // Header claims 32 bytes but carries 31.
        let file = envelope(
            "PaymentSigningKeyShelley_ed25519",
            &format!("5820{}", "ab".repeat(31)),
        );
        assert!(normalize_secret_key_hex(&file).is_err());
    }
}

#[cfg(test)]
mod tests {
    /// Preview's real anchor: slot 0 at 2022-10-25T00:00:00Z, 1s slots.
    fn preview_slots() -> super::SlotConfig {
        super::SlotConfig {
            zero_slot: 0,
            zero_time: 1_666_656_000_000,
            slot_length: 1000,
        }
    }

    #[test]
    fn slot_and_posix_ms_round_trip() {
        let sc = preview_slots();
        // The slot the live investigation turned on: the block that would
        // have carried scoop f6fdb185… if its TTL hadn't closed first.
        assert_eq!(sc.slot_to_posix_ms(118_726_081), 1_785_382_081_000);
        assert_eq!(sc.posix_ms_to_slot(1_785_382_081_000), 118_726_081);
        // Sub-slot remainders floor, so the round trip never reports a slot
        // the chain hasn't reached.
        assert_eq!(sc.posix_ms_to_slot(1_785_382_081_999), 118_726_081);
        for slot in [0u64, 1, 208, 118_725_900, u32::MAX as u64] {
            assert_eq!(sc.posix_ms_to_slot(sc.slot_to_posix_ms(slot)), slot);
        }
    }

    #[test]
    fn posix_ms_to_slot_survives_degenerate_configs() {
        use super::SlotConfig;
        // A zero slot_length would divide by zero; times before the anchor
        // would underflow. Neither can arise from a sane genesis file, but
        // both come from operator config.
        let sc = SlotConfig {
            zero_slot: 42,
            zero_time: 1_000_000,
            slot_length: 0,
        };
        assert_eq!(sc.posix_ms_to_slot(2_000_000), 42 + 1_000_000);
        let sc = preview_slots();
        assert_eq!(sc.posix_ms_to_slot(0), 0);
    }

    #[test]
    fn wall_clock_slot_tracks_real_time() {
        let sc = preview_slots();
        let now_ms =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
                as u64;
        let slot = sc.wall_clock_slot();
        // Within a second of the slot derived from the same clock, and well
        // past the preview slots that were live when this was written.
        assert!(slot.abs_diff(sc.posix_ms_to_slot(now_ms)) <= 1);
        assert!(
            slot > 118_726_081,
            "wall clock slot {slot} is before 2026-07-30"
        );
    }

    #[test]
    fn route_whitelist_parses_idents() {
        use super::*;
        let idents = [vec![0xAA; 28], vec![0xBB; 28]];
        let pd = PlutusData::Array(pallas_primitives::MaybeIndefArray::Def(
            idents.iter().map(|i| PlutusData::BoundedBytes(i.clone().into())).collect(),
        ));
        let wl = parse_route_whitelist(&pd).unwrap();
        assert_eq!(wl, vec![Ident::new(&[0xAA; 28]), Ident::new(&[0xBB; 28])]);
    }

    #[test]
    fn route_whitelist_empty_is_unrestricted() {
        use super::*;
        let pd = PlutusData::Array(pallas_primitives::MaybeIndefArray::Def(vec![]));
        assert_eq!(parse_route_whitelist(&pd).unwrap(), Vec::<Ident>::new());
        // Indefinite-length encoding parses the same.
        let pd = PlutusData::Array(pallas_primitives::MaybeIndefArray::Indef(vec![]));
        assert_eq!(parse_route_whitelist(&pd).unwrap(), Vec::<Ident>::new());
    }

    #[test]
    fn route_whitelist_rejects_malformed() {
        use super::*;
        // Not a list at all.
        let pd = PlutusData::BoundedBytes(vec![0xAA; 28].into());
        assert!(parse_route_whitelist(&pd).is_err());
        // A list whose entry isn't a byte string.
        let pd = PlutusData::Array(pallas_primitives::MaybeIndefArray::Def(vec![
            PlutusData::BigInt(pallas_primitives::BigInt::Int(1i64.into())),
        ]));
        assert!(parse_route_whitelist(&pd).is_err());
    }

    #[test]
    fn basic_tag2_decodes_as_swap() {
        use super::*;
        use plutus_parser::AsPlutus;
        let asset = AssetClass {
            policy: vec![0xAA; 28],
            token: b"IN".to_vec(),
        };
        let want = AssetClass {
            policy: vec![0xBB; 28],
            token: b"OUT".to_vec(),
        };
        let offered: Vec<(AssetClass, BigInt)> = vec![(asset.clone(), BigInt::from(100))];
        let mins: Vec<(AssetClass, BigInt)> = vec![(want.clone(), BigInt::from(95))];
        let pd = PlutusData::Constr(pallas_primitives::Constr {
            tag: 123, // ctor 2
            any_constructor: None,
            fields: pallas_primitives::MaybeIndefArray::Def(vec![
                offered.to_plutus(),
                mins.clone().to_plutus(),
            ]),
        });
        let c = Constraint::from_basic_plutus_constraint(&pd).unwrap();
        match c {
            Constraint::Swap {
                offered,
                original_offered,
                remaining_offered,
                min_received,
            } => {
                assert_eq!(offered, asset);
                assert_eq!(original_offered, BigInt::from(100));
                assert_eq!(remaining_offered, BigInt::from(100));
                assert_eq!(min_received, mins);
            }
            other => panic!("expected Swap, got {other:?}"),
        }

        // Multi-asset offered is not routable — must error, not misparse.
        let offered2: Vec<(AssetClass, BigInt)> =
            vec![(asset, BigInt::from(1)), (want.clone(), BigInt::from(1))];
        let pd2 = PlutusData::Constr(pallas_primitives::Constr {
            tag: 123,
            any_constructor: None,
            fields: pallas_primitives::MaybeIndefArray::Def(vec![
                offered2.to_plutus(),
                Vec::<(AssetClass, BigInt)>::new().to_plutus(),
            ]),
        });
        assert!(Constraint::from_basic_plutus_constraint(&pd2).is_err());
    }

    use super::*;

    #[test]
    fn test_decode_v4_pool_datum() {
        // PoolDatum with 2 assets (ADA + token), total_lp=1000000, circ_lp=500, preminted=999500
        // identifier=0xdeadbeef, 1 action (tag=100, enabled=true, modules=[0xaa]),
        // module_state=[(0xaa, 0xbb)]
        let bytes = hex::decode(concat!(
            "d8799f",                         // Constr 0 (PoolDatum)
            "9f",                             // List: assets
            "9f9f4040ff00ff",                 // (("",""), 0) - ADA with 0 reserves
            "9f9f44010203044405060708ff01ff", // ((0x01020304,0x05060708),1) - token
            "ff",
            "1a000f4240",       // total_lp = 1_000_000
            "1901f4",           // circulating_lp = 500
            "1a000f3e4c",       // preminted_lp = 999_500
            "44deadbeef",       // identifier
            "9f",               // List: actions
            "d8799f1864d87a80", // ActionEntry { tag: 100, enabled: true,
            "9f41aaffff",       // modules: [0xaa] }
            "ff",
            "9f",           // List: module_state
            "9f41aa41bbff", // (0xaa, 0xbb)
            "ff",
            "00",     // min_surplus = 0 (audit-final)
            "d87980", // extension = Void (audit-final)
            "ff"
        ))
        .unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let pool: PoolDatum = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(pool.identifier, Ident::new(&[0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(pool.total_lp, BigInt::from(1_000_000));
        assert_eq!(pool.circulating_lp, BigInt::from(500));
        assert_eq!(pool.preminted_lp, BigInt::from(998_988));
        assert_eq!(pool.assets.len(), 2);
        assert_eq!(pool.actions.len(), 1);
        assert_eq!(pool.actions[0].tag, BigInt::from(100));
        assert!(pool.actions[0].enabled);
        assert_eq!(pool.module_state.len(), 1);
    }

    #[test]
    fn test_decode_v4_pool_state() {
        // PoolState with 2 assets, total_lp=1000, circ_lp=500, preminted=500
        let bytes = hex::decode(concat!(
            "d8799f",
            "9f9f9f4040ff1a00989680ff9f9f44010203044405060708ff1a004c4b40ffff",
            "1903e8", // total_lp = 1000
            "1901f4", // circulating_lp = 500
            "1901f4", // preminted_lp = 500
            "ff"
        ))
        .unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let vs: PoolState = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(vs.total_lp, BigInt::from(1000));
        assert_eq!(vs.circulating_lp, BigInt::from(500));
        assert_eq!(vs.preminted_lp, BigInt::from(500));
        assert_eq!(vs.assets.len(), 2);
    }

    #[test]
    fn test_cp_config_hash_matches_ts() {
        use pallas_crypto::hash::Hasher;
        let config = ConstantProductConfig {
            fee: Rational {
                num: BigInt::from(3),
                den: BigInt::from(1000),
            },
        };
        let cbor = minicbor::to_vec(config.to_plutus()).unwrap();
        let hash = hex::encode(Hasher::<256>::hash(&cbor));
        eprintln!("CP CBOR: {}", hex::encode(&cbor));
        eprintln!("CP hash: {}", hash);
        // TS produces: d8799fd8799f031903e8ffff → hash 191f6d4b...
        assert_eq!(hex::encode(&cbor), "d8799fd8799f031903e8ffff");
        assert_eq!(
            hash,
            "191f6d4b97693d5268090e9d918bfad9e171e5699b155bdc9bd944e005891a5a"
        );
    }

    #[test]
    fn test_decode_v4_order_datum_swap() {
        // OrderDatum with a Swap constraint (tag=2):
        //   offered = ADA (asset only)
        //   original_offered = remaining_offered = 5_000_000
        //   min_received = [(token, 1_000_000)]
        let ada = AssetClass {
            policy: vec![],
            token: vec![],
        };
        let token = AssetClass {
            policy: vec![0x01, 0x02, 0x03, 0x04],
            token: vec![0x05, 0x06, 0x07, 0x08],
        };
        let unit = PlutusData::Constr(pallas_primitives::Constr {
            tag: 121,
            any_constructor: None,
            fields: pallas_codec::utils::MaybeIndefArray::Def(vec![]),
        });
        let min_recv: Vec<(AssetClass, BigInt)> = vec![(token.clone(), BigInt::from(1_000_000))];
        let constraints = PlutusData::Constr(pallas_primitives::Constr {
            tag: 121 + 2, // Swap
            any_constructor: None,
            fields: pallas_codec::utils::MaybeIndefArray::Def(vec![
                ada.clone().to_plutus(),             // offered: AssetClass
                BigInt::from(5_000_000).to_plutus(), // original_offered: Int
                BigInt::from(5_000_000).to_plutus(), // remaining_offered: Int
                min_recv.to_plutus(),                // min_received: List<(AssetClass, Int)>
            ]),
        });
        const SWAP_HASH: [u8; 28] = [0xAA; 28];
        let datum = OrderDatum {
            owner: Multisig::Signature(vec![0xaa; 28]),
            destination: Destination::SelfDestination,
            service_budget: BigInt::from(1_000_000),
            max_per_execution: BigInt::from(400_000),
            config_token: vec![0xbb; 32],
            constraints: vec![(SWAP_HASH.to_vec(), constraints.clone())],
            extension: unit,
        };

        let pd = datum.clone().to_plutus();
        let decoded: OrderDatum = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(decoded.owner, Multisig::Signature(vec![0xaa; 28]));
        assert_eq!(decoded.destination, Destination::SelfDestination);
        assert_eq!(decoded.service_budget, BigInt::from(1_000_000));
        assert_eq!(decoded.max_per_execution, BigInt::from(400_000));

        let inner = decoded
            .find_constraint_by_hash(&SWAP_HASH)
            .expect("decoded order should carry the test swap constraint");
        let parsed = Constraint::from_plutus_constraint(inner).unwrap();
        match parsed {
            Constraint::Swap {
                offered,
                original_offered,
                remaining_offered,
                min_received,
            } => {
                assert_eq!(offered, ada);
                assert_eq!(original_offered, BigInt::from(5_000_000));
                assert_eq!(remaining_offered, BigInt::from(5_000_000));
                assert_eq!(min_received.len(), 1);
                assert_eq!(min_received[0].0, token);
                assert_eq!(min_received[0].1, BigInt::from(1_000_000));
            }
            other => panic!("expected Swap, got {other:?}"),
        }
    }

    #[test]
    fn test_decode_v4_order_redeemer_cancel() {
        // Cancel = Constr 0
        let bytes = hex::decode("d87980").unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let redeemer: OrderRedeemer = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(redeemer, OrderRedeemer::Cancel);
    }

    #[test]
    fn test_decode_v4_order_redeemer_scoop() {
        // Scoop { own_input_index: 3 } = Constr 1 [3]
        let bytes = hex::decode("d87a9f03ff").unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let redeemer: OrderRedeemer = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(redeemer, OrderRedeemer::Scoop { own_input_index: 3 });
    }

    #[test]
    fn test_decode_v4_pool_redeemer_action() {
        // PoolRedeemer::Action { tag: 100, transcript: [], pool_input_index: 0, pool_output_index: 0 }
        // = Constr 3 [100, [], 0, 0]
        let bytes = hex::decode("d87c9f18649fff0000ff").unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let redeemer: PoolRedeemer = AsPlutus::from_plutus(pd).unwrap();
        match redeemer {
            PoolRedeemer::Action {
                tag,
                transcript,
                pool_input_index,
                pool_output_index,
            } => {
                assert_eq!(tag, BigInt::from(100));
                assert!(transcript.is_empty());
                assert_eq!(pool_input_index, BigInt::from(0));
                assert_eq!(pool_output_index, BigInt::from(0));
            }
            _ => panic!("expected Action"),
        }
    }

    #[test]
    fn test_decode_v4_settings_datum() {
        // Round-trip the SUN-301 shape (treasury_address, order_modules and
        // min_share_batcher were removed from the global settings).
        let datum = SettingsDatum {
            settings_admin: Multisig::Signature(vec![0xaa; 28]),
            treasury_admin: Multisig::Signature(vec![0xbb; 28]),
            authorized_scoopers: Some(vec![Multisig::Signature(vec![0xdd; 28])]),
            security_council: Multisig::Signature(vec![0xcc; 28]),
            extension: PlutusData::Constr(pallas_primitives::Constr {
                tag: 121,
                any_constructor: None,
                fields: pallas_codec::utils::MaybeIndefArray::Def(vec![]),
            }),
        };
        let pd = datum.clone().to_plutus();
        let decoded: SettingsDatum = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(decoded.settings_admin, Multisig::Signature(vec![0xaa; 28]));
        assert_eq!(decoded.authorized_scoopers.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_constant_sum_config_cbor_round_trip() {
        // ConstantSumConfig { prices: [1, 2], fee: 3/1000, bounty_k: 0/1 }
        let cfg = ConstantSumConfig {
            prices: vec![BigInt::from(1), BigInt::from(2)],
            fee: Rational {
                num: BigInt::from(3),
                den: BigInt::from(1000),
            },
            bounty_k: Rational {
                num: BigInt::from(0),
                den: BigInt::from(1),
            },
            balance_fee: Rational {
                num: BigInt::from(0),
                den: BigInt::from(1),
            },
        };
        let cbor = minicbor::to_vec(cfg.clone().to_plutus()).unwrap();
        // Persisted byte shape used by sqlite tests in persistence::sqlite.
        // If this changes, update those test fixtures.
        // Trailing `d8799f0001ff` = Rational 0/1 for balance_fee (SUN-310;
        // replaces the old waive_fee_on_claim Bool).
        assert_eq!(
            hex::encode(&cbor),
            "d8799f9f0102ffd8799f031903e8ffd8799f0001ffd8799f0001ffff"
        );

        let pd: PlutusData = minicbor::decode(&cbor).unwrap();
        let decoded: ConstantSumConfig = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(decoded.prices, cfg.prices);
        assert_eq!(decoded.fee.num, cfg.fee.num);
        assert_eq!(decoded.fee.den, cfg.fee.den);
        assert_eq!(decoded.bounty_k.num, cfg.bounty_k.num);
        assert_eq!(decoded.bounty_k.den, cfg.bounty_k.den);
        assert_eq!(decoded.balance_fee.num, BigInt::from(0));
        assert_eq!(decoded.balance_fee.den, BigInt::from(1));
    }

    #[test]
    fn test_decode_v4_constant_product_config() {
        // ConstantProductConfig { fee: Rational { num: 3, den: 1000 } }
        // Constr(0, [Constr(0, [3, 1000])]) — both struct and Rational are Constr-encoded
        let bytes = hex::decode("d8799fd8799f031903e8ffff").unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let config: ConstantProductConfig = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(config.fee.num, BigInt::from(3));
        assert_eq!(config.fee.den, BigInt::from(1000));
    }

    #[test]
    fn test_pool_state_from_pool() {
        let pool = PoolDatum {
            assets: vec![
                (
                    AssetClass {
                        policy: vec![],
                        token: vec![],
                    },
                    BigInt::from(100),
                ),
                (
                    AssetClass {
                        policy: vec![1],
                        token: vec![2],
                    },
                    BigInt::from(200),
                ),
            ],
            total_lp: BigInt::from(1000),
            circulating_lp: BigInt::from(500),
            preminted_lp: BigInt::from(500),
            identifier: Ident::new(&[0xab]),
            actions: vec![],
            module_state: vec![],
            min_surplus: BigInt::from(0),
            extension: crate::sundaev4::types::plutus_void(),
        };
        let state = PoolState::from_pool(&pool);
        assert_eq!(state.assets, pool.assets);
        assert_eq!(state.total_lp, pool.total_lp);
        assert_eq!(state.circulating_lp, pool.circulating_lp);
        assert_eq!(state.preminted_lp, pool.preminted_lp);
    }

    #[test]
    fn test_pool_redeemer_encoding() {
        use crate::cardano_types::AssetClass;

        // Build a minimal PoolRedeemer::Action and check its CBOR hex
        let state = PoolState {
            assets: vec![(
                AssetClass {
                    policy: vec![0xaa],
                    token: vec![0xbb],
                },
                BigInt::from(100),
            )],
            total_lp: BigInt::from(1000),
            circulating_lp: BigInt::from(500),
            preminted_lp: BigInt::from(500),
        };
        let entry = TranscriptEntry {
            state_after: state.clone(),
            fee_budget: BigInt::from(1),
            operation_tag: BigInt::from(100),
            operation_data: PoolState {
                assets: vec![],
                total_lp: BigInt::from(0),
                circulating_lp: BigInt::from(0),
                preminted_lp: BigInt::from(0),
            }
            .to_plutus(),
        };
        let redeemer = PoolRedeemer::Action {
            tag: BigInt::from(100),
            transcript: vec![entry],
            pool_input_index: BigInt::from(0u64),
            pool_output_index: BigInt::from(0u64),
        };
        let pd = redeemer.to_plutus();
        let cbor = minicbor::to_vec(&pd).unwrap();
        let hex = hex::encode(&cbor);
        eprintln!("PoolRedeemer CBOR hex: {hex}");

        // Verify structure: should be Constr(3, [tag, transcript, pool_input_idx, pool_output_idx])
        if let PlutusData::Constr(c) = &pd {
            assert_eq!(c.tag, 124, "Action should be variant 3 → tag 124");
            let fields = c.fields.clone().to_vec();
            assert_eq!(fields.len(), 4, "Action should have 4 fields");
        } else {
            panic!("expected Constr");
        }
    }
}
