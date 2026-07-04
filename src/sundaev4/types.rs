use anyhow::Context as _;
use acropolis_common::Point;
use pallas_addresses::ScriptHash;
use pallas_primitives::PlutusData;
use plutus_parser::AsPlutus;
use serde::ser::SerializeStruct;
use serde::Serializer;

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
        for b in v { seq.serialize_element(&hex::encode(b))?; }
        seq.end()
    }

    pub fn vec_bytes_pair_as_map<S: Serializer>(v: &Vec<(Vec<u8>, Vec<u8>)>, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = s.serialize_map(Some(v.len()))?;
        for (k, v) in v { map.serialize_entry(&hex::encode(k), &hex::encode(v))?; }
        map.end()
    }

    pub fn opt_vec_bytes<S: Serializer>(v: &Option<Vec<Vec<u8>>>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(list) => vec_bytes(list, s),
            None => s.serialize_none(),
        }
    }
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
    /// Per-order tx-fee budget. Contract requires `budget * n_orders >= tx_fee`.
    pub budget: BigInt,
    /// Scooper share of `budget - fee_share` surplus, in basis points (0..=10000).
    pub share_batcher: BigInt,
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

impl Constraint {
    /// Decode a `Constr`-tagged constraint payload. Returns the unrecognised tag
    /// in the error case so the caller can decide how to surface it.
    pub fn from_plutus_constraint(pd: &PlutusData) -> anyhow::Result<Self> {
        let PlutusData::Constr(c) = pd else {
            anyhow::bail!("constraint must be a Constr");
        };
        // Pallas tags: Constr 0..6 → cbor 121..127, Constr 7+ → 1280+. Strip the offset.
        let tag = if c.tag >= 121 && c.tag <= 127 {
            (c.tag - 121) as u64
        } else if c.tag >= 1280 {
            (c.tag - 1280 + 7) as u64
        } else {
            c.tag as u64
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
            0 => Constraint::Deposit { offered: list_pair(0)?, min_received: list_pair(1)? },
            1 => Constraint::Withdraw { offered: list_pair(0)?, min_received: list_pair(1)? },
            2 => Constraint::Swap {
                offered: AssetClass::from_plutus(f(0)?.clone())
                    .map_err(|e| anyhow::anyhow!("decode swap.offered: {e}"))?,
                original_offered: BigInt::from_plutus(f(1)?.clone())
                    .map_err(|e| anyhow::anyhow!("decode swap.original_offered: {e}"))?,
                remaining_offered: BigInt::from_plutus(f(2)?.clone())
                    .map_err(|e| anyhow::anyhow!("decode swap.remaining_offered: {e}"))?,
                min_received: list_pair(3)?,
            },
            3 => Constraint::Claim { offered: list_pair(0)?, min_received: list_pair(1)? },
            t => anyhow::bail!("unknown constraint tag {t}"),
        })
    }

    /// Decode the constraint of interest from an OrderDatum by walking its
    /// constraints list to find an entry under either `swap_order_hash` or
    /// `basic_order_hash`, then decoding that inner Data. The constraint
    /// *class* is implicit in the hash; the inner Data's ctor tag picks
    /// the variant (Swap=2 for swap_order; Deposit=0 / Withdraw=1 /
    /// Claim=3 for basic_order).
    pub fn from_order_datum(
        datum: &OrderDatum,
        swap_order_hash: &[u8],
        basic_order_hash: &[u8],
    ) -> anyhow::Result<Self> {
        let constraint_data = datum
            .find_constraint_by_hash(swap_order_hash)
            .or_else(|| datum.find_constraint_by_hash(basic_order_hash))
            .ok_or_else(|| {
                anyhow::anyhow!("order has neither swap_order nor basic_order constraint")
            })?;
        Self::from_plutus_constraint(constraint_data)
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
        if !strategy_order_hash.is_empty() {
            if let Some(data) = datum.find_constraint_by_hash(strategy_order_hash) {
                let constraints = StrategyConstraints::from_plutus(data.clone())
                    .map_err(|e| anyhow::anyhow!("decode StrategyConstraints: {e}"))?;
                return Ok(Constraint::Strategy { constraints });
            }
        }
        Self::from_order_datum(datum, swap_order_hash, basic_order_hash)
    }

    /// Constraint tag (0=Deposit, 1=Withdraw, 2=Swap, 3=Claim). Matches the
    /// `settings.order_modules` lookup key.
    pub fn tag(&self) -> u64 {
        match self {
            Constraint::Deposit { .. } => 0,
            Constraint::Withdraw { .. } => 1,
            Constraint::Swap { .. } => 2,
            Constraint::Claim { .. } => 3,
            // Strategy constraints have no ctor-tag dispatch — they're
            // selected by script hash. No caller should route on this.
            Constraint::Strategy { .. } => u64::MAX,
        }
    }

    /// For Swap orders: `(offered_asset, remaining_offered_qty)` borrowed from
    /// the constraint. Returns `None` for non-Swap orders — the scooper's
    /// batching path only handles swaps.
    pub fn swap_offered(&self) -> Option<(&AssetClass, &BigInt)> {
        match self {
            Constraint::Swap { offered, remaining_offered, .. } => Some((offered, remaining_offered)),
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
    #[serde(serialize_with = "hex_ser::bytes")]
    pub treasury_address: Vec<u8>,
    #[serde(serialize_with = "hex_ser::opt_vec_bytes")]
    pub authorized_scoopers: Option<Vec<Vec<u8>>>,
    /// Maps order constraint tag (0=Deposit, 1=Withdraw, 2=Swap, 3=Claim) → module script hash.
    pub order_modules: Vec<(BigInt, Vec<u8>)>,
    /// Minimum scooper share in basis points; orders with `share_batcher < min_share_batcher` are rejected.
    pub min_share_batcher: BigInt,
    pub extension: PlutusData,
}

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
    ConstantProduct { fee: Rational },
    ConstantSum { prices: Vec<BigInt>, fee: Rational, bounty_k: Rational, waive_fee_on_claim: bool },
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
}

// ──────────────────────────────────────────────────────────────────────────────
// Module config types
// ──────────────────────────────────────────────────────────────────────────────

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
    /// When `true`, the swap portion of a `tag_claim` step is value-neutral
    /// (`dy = dx · p_in / p_out` exactly, no fee retained) and the on-chain
    /// validator requires `v_increase = 0`, `fee_budget = 0`,
    /// `before_lp == after_lp`. The claim itself is bounded only by cap_b.
    pub waive_fee_on_claim: bool,
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

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum ConstantProductRedeemer {
    Create { initial_state: ConstantProductConfig },
    Operate { entries: Vec<CPOperateEntry> },
}

/// Redeemer for the pool_mint policy. The scooper only uses `MintLP` (to mint
/// LP tokens for Deposits); `CreatePool` is for pool genesis and `BurnPool`
/// for full withdrawals — both run by the CLI, not the scooper.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum PoolMintRedeemer {
    CreatePool { seed_utxo: OutputRef, settings_ref_index: u64 },
    MintLP { pool_ident: Ident },
    BurnPool { pool_ident: Ident },
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct CPOperateEntry {
    pub pool_oref: OutputRef,
    pub config: ConstantProductConfig,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum ConstantSumRedeemer {
    Create { initial_state: PlutusData },
    Operate { entries: Vec<CSOperateEntry> },
    Destroy,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum ConcentratedLiquidityRedeemer {
    Create { initial_state: ConcentratedLiquidityConfig },
    Operate { entries: Vec<CLOperateEntry> },
    Destroy,
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

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FeeSplitConfig {
    pub protocol_share: Rational,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum FeeSplitRedeemer {
    Create { config: FeeSplitConfig },
    Operate { entries: Vec<FSOperateEntry> },
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct FSOperateEntry {
    pub pool_oref: OutputRef,
    pub config: FeeSplitConfig,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum FairnessRedeemer {
    Create,
    Operate { entries: Vec<FairnessOperateEntry> },
    Destroy,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct FairnessOperateEntry {
    pub pool_oref: OutputRef,
    pub pool_ident: Ident,
    pub scooper: Vec<u8>,
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

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ScooperExecution {
    #[serde(default)]
    pub scooper_secret_key: String,
    #[serde(default)]
    pub scooper_secret_key_file: Option<String>,
    /// Optional 28-byte hex stake key hash to attach as the delegation part
    /// of the scooper's address. CIP-1852 wallets use base addresses (payment
    /// + staking); funds sent to those addresses are unreachable from an
    /// enterprise (payment-only) address. Leave unset to derive an enterprise
    /// address (works for fresh testnet keys with no staking).
    #[serde(default)]
    pub scooper_stake_keyhash: Option<String>,
    pub submit_url: String,
    pub fee: (u64, u64),
    pub protocol_share: (u64, u64),
    pub module_scripts: ModuleScripts,
    pub plutus_v3_cost_model: Vec<i64>,
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
    /// Default: (6, 5) i.e. 20% padding.
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
}

fn default_max_tx_ex_mem() -> u64 { 14_000_000 }
fn default_max_tx_ex_steps() -> u64 { 10_000_000_000 }
fn default_max_tx_size() -> usize { 16_384 }
fn default_budget_padding() -> (u64, u64) { (6, 5) }

impl ScooperExecution {
    /// If `scooper_secret_key_file` is set, read the file and populate
    /// `scooper_secret_key`. Call this once at startup.
    pub fn resolve_secret_key(&mut self) -> anyhow::Result<()> {
        if let Some(path) = &self.scooper_secret_key_file {
            let contents = std::fs::read_to_string(path)
                .with_context(|| format!("reading secret key file: {path}"))?;
            let trimmed = contents.trim();
            // Handle Cardano CLI skey JSON format: { "cborHex": "5820<hex>" }
            if trimmed.starts_with('{') {
                let json: serde_json::Value = serde_json::from_str(trimmed)
                    .with_context(|| format!("parsing skey JSON file: {path}"))?;
                let cbor_hex = json["cborHex"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("skey file missing cborHex field: {path}"))?;
                // Strip CBOR wrapping (5820 = 32-byte bytestring prefix)
                self.scooper_secret_key = cbor_hex
                    .strip_prefix("5820")
                    .unwrap_or(cbor_hex)
                    .to_string();
            } else {
                self.scooper_secret_key = trimmed.to_string();
            }
        }
        anyhow::ensure!(
            !self.scooper_secret_key.is_empty(),
            "scooper-secret-key or scooper-secret-key-file must be set"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ModuleScripts {
    pub constant_product: ScriptRefInfo,
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
        self.constraint.swap_offered()
            .expect("SundaeV4Order::swap_offered called on non-Swap constraint")
    }

    /// First entry of the Swap's min_received list. See
    /// [`Constraint::swap_min_received`].
    pub fn swap_min_received(&self) -> (&AssetClass, &BigInt) {
        self.constraint.swap_min_received()
            .expect("SundaeV4Order::swap_min_received called on non-Swap constraint")
    }

    /// Test-only constructor for a Swap-shaped order. `offer` is `(asset, qty)`
    /// where qty becomes both `original_offered` and `remaining_offered` (no
    /// partial-fill state). `min_received` becomes a single-entry list. Uses
    /// `unit` for `extension`; `budget` is the per-order tx-fee budget.
    #[cfg(test)]
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
                offer_qty.clone().to_plutus(),       // original_offered
                offer_qty.to_plutus(),               // remaining_offered
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
            budget,
            share_batcher: BigInt::from(0),
            config_token: Vec::new(),
            constraints,
            extension: unit,
        };
        let constraint = Constraint::from_plutus_constraint(&swap_data)
            .expect("test_swap_order: constraint should decode");
        SundaeV4Order { input, value, datum, constraint, slot }
    }
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct SundaeV4Settings {
    pub input: TransactionInput,
    pub value: crate::cardano_types::Value,
    pub datum: SettingsDatum,
    pub slot: u64,
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
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub starting_point: Point,
    pub execution: Option<ScooperExecution>,
    /// Optional blueprint for deriving scripts. Retained for serde round-trip.
    #[serde(default)]
    #[allow(dead_code)]
    pub blueprint: Option<crate::blueprint::Blueprint>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_v4_pool_datum() {
        // PoolDatum with 2 assets (ADA + token), total_lp=1000000, circ_lp=500, preminted=999500
        // identifier=0xdeadbeef, 1 action (tag=100, enabled=true, modules=[0xaa]),
        // module_state=[(0xaa, 0xbb)]
        let bytes = hex::decode(concat!(
            "d8799f",                           // Constr 0 (PoolDatum)
            "9f",                               // List: assets
            "9f9f4040ff00ff",                   // (("",""), 0) - ADA with 0 reserves
            "9f9f44010203044405060708ff01ff",    // ((0x01020304,0x05060708),1) - token
            "ff",
            "1a000f4240",                       // total_lp = 1_000_000
            "1901f4",                           // circulating_lp = 500
            "1a000f3e4c",                       // preminted_lp = 999_500
            "44deadbeef",                       // identifier
            "9f",                               // List: actions
            "d8799f1864d87a80",                 // ActionEntry { tag: 100, enabled: true,
            "9f41aaffff",                       // modules: [0xaa] }
            "ff",
            "9f",                               // List: module_state
            "9f41aa41bbff",                     // (0xaa, 0xbb)
            "ff",
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
            "1903e8",       // total_lp = 1000
            "1901f4",       // circulating_lp = 500
            "1901f4",       // preminted_lp = 500
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
            fee: Rational { num: BigInt::from(3), den: BigInt::from(1000) },
        };
        let cbor = minicbor::to_vec(&config.to_plutus()).unwrap();
        let hash = hex::encode(Hasher::<256>::hash(&cbor));
        eprintln!("CP CBOR: {}", hex::encode(&cbor));
        eprintln!("CP hash: {}", hash);
        // TS produces: d8799fd8799f031903e8ffff → hash 191f6d4b...
        assert_eq!(hex::encode(&cbor), "d8799fd8799f031903e8ffff");
        assert_eq!(hash, "191f6d4b97693d5268090e9d918bfad9e171e5699b155bdc9bd944e005891a5a");
    }

    #[test]
    fn test_decode_v4_order_datum_swap() {
        // OrderDatum with a Swap constraint (tag=2):
        //   offered = ADA (asset only)
        //   original_offered = remaining_offered = 5_000_000
        //   min_received = [(token, 1_000_000)]
        let ada = AssetClass { policy: vec![], token: vec![] };
        let token = AssetClass {
            policy: vec![0x01, 0x02, 0x03, 0x04],
            token: vec![0x05, 0x06, 0x07, 0x08],
        };
        let unit = PlutusData::Constr(pallas_primitives::Constr {
            tag: 121,
            any_constructor: None,
            fields: pallas_codec::utils::MaybeIndefArray::Def(vec![]),
        });
        let min_recv: Vec<(AssetClass, BigInt)> =
            vec![(token.clone(), BigInt::from(1_000_000))];
        let constraints = PlutusData::Constr(pallas_primitives::Constr {
            tag: 121 + 2, // Swap
            any_constructor: None,
            fields: pallas_codec::utils::MaybeIndefArray::Def(vec![
                ada.clone().to_plutus(),               // offered: AssetClass
                BigInt::from(5_000_000).to_plutus(),   // original_offered: Int
                BigInt::from(5_000_000).to_plutus(),   // remaining_offered: Int
                min_recv.to_plutus(),                  // min_received: List<(AssetClass, Int)>
            ]),
        });
        const SWAP_HASH: [u8; 28] = [0xAA; 28];
        let datum = OrderDatum {
            owner: Multisig::Signature(vec![0xaa; 28]),
            destination: Destination::SelfDestination,
            budget: BigInt::from(1_000_000),
            share_batcher: BigInt::from(50),
            config_token: vec![0xbb; 32],
            constraints: vec![(SWAP_HASH.to_vec(), constraints.clone())],
            extension: unit,
        };

        let pd = datum.clone().to_plutus();
        let decoded: OrderDatum = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(decoded.owner, Multisig::Signature(vec![0xaa; 28]));
        assert_eq!(decoded.destination, Destination::SelfDestination);
        assert_eq!(decoded.budget, BigInt::from(1_000_000));
        assert_eq!(decoded.share_batcher, BigInt::from(50));

        let inner = decoded
            .find_constraint_by_hash(&SWAP_HASH)
            .expect("decoded order should carry the test swap constraint");
        let parsed = Constraint::from_plutus_constraint(inner).unwrap();
        match parsed {
            Constraint::Swap { offered, original_offered, remaining_offered, min_received } => {
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
        // Round-trip: build a SettingsDatum with all 7 fields, encode, decode, compare.
        let datum = SettingsDatum {
            settings_admin: Multisig::Signature(vec![0xaa; 28]),
            treasury_admin: Multisig::Signature(vec![0xbb; 28]),
            treasury_address: vec![0xcc, 0xcc],
            authorized_scoopers: Some(vec![vec![0xdd; 28]]),
            order_modules: vec![
                (BigInt::from(2), vec![0xee; 28]), // tag 2 = Swap
            ],
            min_share_batcher: BigInt::from(50),
            extension: PlutusData::Constr(pallas_primitives::Constr {
                tag: 121,
                any_constructor: None,
                fields: pallas_codec::utils::MaybeIndefArray::Def(vec![]),
            }),
        };
        let pd = datum.clone().to_plutus();
        let decoded: SettingsDatum = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(decoded.treasury_address, vec![0xcc, 0xcc]);
        assert_eq!(decoded.authorized_scoopers.as_ref().unwrap().len(), 1);
        assert_eq!(decoded.order_modules.len(), 1);
        assert_eq!(decoded.order_modules[0].0, BigInt::from(2));
        assert_eq!(decoded.min_share_batcher, BigInt::from(50));
    }

    #[test]
    fn test_constant_sum_config_cbor_round_trip() {
        // ConstantSumConfig { prices: [1, 2], fee: 3/1000, bounty_k: 0/1 }
        let cfg = ConstantSumConfig {
            prices: vec![BigInt::from(1), BigInt::from(2)],
            fee: Rational { num: BigInt::from(3), den: BigInt::from(1000) },
            bounty_k: Rational { num: BigInt::from(0), den: BigInt::from(1) },
            waive_fee_on_claim: false,
        };
        let cbor = minicbor::to_vec(&cfg.clone().to_plutus()).unwrap();
        // Persisted byte shape used by sqlite tests in persistence::sqlite.
        // If this changes, update those test fixtures.
        // Trailing `d87980` = Constr 0 [] (False) for waive_fee_on_claim.
        assert_eq!(hex::encode(&cbor), "d8799f9f0102ffd8799f031903e8ffd8799f0001ffd87980ff");

        let pd: PlutusData = minicbor::decode(&cbor).unwrap();
        let decoded: ConstantSumConfig = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(decoded.prices, cfg.prices);
        assert_eq!(decoded.fee.num, cfg.fee.num);
        assert_eq!(decoded.fee.den, cfg.fee.den);
        assert_eq!(decoded.bounty_k.num, cfg.bounty_k.num);
        assert_eq!(decoded.bounty_k.den, cfg.bounty_k.den);
        assert!(!decoded.waive_fee_on_claim);
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
                (AssetClass { policy: vec![], token: vec![] }, BigInt::from(100)),
                (AssetClass { policy: vec![1], token: vec![2] }, BigInt::from(200)),
            ],
            total_lp: BigInt::from(1000),
            circulating_lp: BigInt::from(500),
            preminted_lp: BigInt::from(500),
            identifier: Ident::new(&[0xab]),
            actions: vec![],
            module_state: vec![],
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
            assets: vec![
                (AssetClass { policy: vec![0xaa], token: vec![0xbb] }, BigInt::from(100)),
            ],
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
            }.to_plutus(),
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
