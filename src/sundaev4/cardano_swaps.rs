//! Cardano-Swaps (the DeFi Kernel's p2p order book) one-way swaps as router
//! conversion edges.
//!
//! A one-way swap is a limit order held in its owner's own address — every
//! owner gets a distinct address sharing the swap script as payment
//! credential — carrying three non-transferable "beacon" tokens and an
//! inline datum that names the pair and the owner's limit price. **Anyone
//! may fill it.** The filler spends the order with the `Swap` redeemer,
//! takes some of the offered asset, deposits the asked asset at (or above)
//! the datum's price, and recreates the order at the same address with the
//! datum unchanged except `prev_input`.
//!
//! Unlike Indigo's order book there is no oracle, no price feed and no
//! withdrawal: the price is in the datum, so a fill is one spend, one
//! continuing output and one reference input.
//!
//! The rules below come from the validator source (`aiken/validators/
//! one_way_swap.ak`, `valid_swap` in `one_way_swap/utils.ak`) and are
//! regression-tested by replaying two mainnet fills and mutating them (see
//! `replay_tests`):
//!
//! 1. The input must hold exactly one of each of its three beacons.
//! 2. Exactly one output must sit at the same address with the datum
//!    byte-identical except `prev_input == Some(input_ref)`, no reference
//!    script, the same three beacons, and no extraneous assets.
//! 3. `offer_taken · price_num <= ask_given · price_den` — a plain integer
//!    cross-multiplication, so partial fills are free and there is no
//!    rounding slack to respect.
//! 4. `ask_given >= 0`: the asked asset may never be taken out.
//! 5. ADA may only be *deposited*, unless ADA is itself one side of the pair.
//! 6. If the datum sets an expiration, the transaction's upper validity
//!    bound must be set and at or below it.
//!
//! Beacon names are `sha2_256(serialise_data(ctor))` over a dedicated
//! `SwapAsset` type, which this module derives independently so an order
//! whose datum disagrees with its own assets is skipped rather than trusted.
//!
//! Everything is config-driven and closed by default: without
//! `execution.cardano-swaps` the scooper behaves exactly as before.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use pallas_codec::utils::CborWrap;
use pallas_primitives::{PlutusData, conway};
use serde::Deserialize;

use crate::bigint::BigInt;
use crate::cardano_types::{
    ADA_ASSET_CLASS, AssetClass, TransactionInput, TransactionOutput, Value,
};
use crate::sundaev4::conversions::ConversionEdge;

// ─── Config ─────────────────────────────────────────────────────────────────

/// The `execution.cardano-swaps` config section.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CardanoSwapsConfig {
    /// Master switch: nothing is indexed or routed unless true.
    #[serde(default)]
    pub enabled: bool,
    /// The one-way swap spending script hash, hex. Orders live at
    /// addresses using it as their payment credential.
    pub swap_script_hash: String,
    /// Its reference-script UTxO, "txid#index". Published by a third party
    /// (the cardano-reference-scripts protocol), so it could in principle
    /// be spent — the integration disables itself if the UTxO is missing
    /// rather than building a transaction that cannot be witnessed.
    pub swap_ref_utxo: String,
    /// The one-way beacon minting policy, hex. Used to recognise an
    /// order's beacons and to derive their expected names.
    pub beacon_policy: String,
    /// Lovelace a continuing output must retain when ADA is the offered
    /// asset. The validator pins the traded amounts; the ledger still
    /// needs the output to clear its own min-UTxO floor.
    #[serde(default = "default_min_keep")]
    pub min_keep_lovelace: u64,
    /// Filling resting orders against our own pools when no Sundae order
    /// is involved. Off unless present and enabled.
    #[serde(default)]
    pub foreign_fills: Option<ForeignFillConfig>,
}

fn default_min_keep() -> u64 {
    3_000_000
}

/// `execution.cardano-swaps.foreign-fills`: a pool swap whose counterparty
/// is a resting Cardano-Swaps order. The lovelace the order releases beyond
/// the pool's price lands on the pool as protocol surplus; the fill is only
/// made when that surplus, net of the transaction fee, clears a floor.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ForeignFillConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Net protocol revenue a fill must land (lovelace) after the exact
    /// transaction fee. Checked against the rebuilt transaction, not a
    /// projection. A fill that clears by one lovelace still costs a build,
    /// an evaluation and a race, so keep this above zero.
    #[serde(default = "default_min_revenue")]
    pub min_revenue_lovelace: u64,
    /// Fee projection used only to size and pre-screen a fill before it is
    /// built. The real fee replaces it for the floor check.
    #[serde(default = "default_fee_estimate")]
    pub fee_estimate_lovelace: u64,
}

fn default_min_revenue() -> u64 {
    1_000_000
}
fn default_fee_estimate() -> u64 {
    1_000_000
}

// ─── Runtime ────────────────────────────────────────────────────────────────

/// Config resolved into bytes. Any error disables the integration.
#[derive(Clone, Debug)]
pub struct CardanoSwapsRuntime {
    pub swap_hash: Vec<u8>,
    pub swap_ref: TransactionInput,
    pub beacon_policy: Vec<u8>,
    pub min_keep_lovelace: u64,
    pub foreign_fills: Option<ForeignFillConfig>,
}

fn parse_outref(s: &str) -> Result<TransactionInput> {
    let (txid, idx) =
        s.split_once('#').with_context(|| format!("outref {s:?} must be txid#index"))?;
    let txid = hex::decode(txid).with_context(|| format!("outref txid {txid:?}"))?;
    anyhow::ensure!(txid.len() == 32, "outref txid must be 32 bytes");
    let idx: u64 = idx.parse().with_context(|| format!("outref index {idx:?}"))?;
    let hash: [u8; 32] = txid.as_slice().try_into().expect("length checked");
    Ok(TransactionInput::new(hash.into(), idx))
}

fn parse_hash(label: &str, s: &str) -> Result<Vec<u8>> {
    let h = hex::decode(s).with_context(|| format!("{label}: not hex"))?;
    anyhow::ensure!(h.len() == 28, "{label}: must be 28 bytes");
    Ok(h)
}

impl CardanoSwapsRuntime {
    pub fn load(config: &CardanoSwapsConfig) -> Result<Self> {
        Ok(Self {
            swap_hash: parse_hash("swap-script-hash", &config.swap_script_hash)?,
            swap_ref: parse_outref(&config.swap_ref_utxo)?,
            beacon_policy: parse_hash("beacon-policy", &config.beacon_policy)?,
            min_keep_lovelace: config.min_keep_lovelace,
            foreign_fills: config.foreign_fills.clone().filter(|f| f.enabled),
        })
    }

    /// Reference UTxOs the indexer must track so the swap script is
    /// resolvable at build and eval time.
    pub fn static_ref_utxos(&self) -> Vec<TransactionInput> {
        vec![self.swap_ref.clone()]
    }
}

/// Load the runtime; any problem logs and disables the integration.
pub fn load_runtime(config: &Option<CardanoSwapsConfig>) -> Option<CardanoSwapsRuntime> {
    let cfg = config.as_ref()?;
    if !cfg.enabled {
        return None;
    }
    match CardanoSwapsRuntime::load(cfg) {
        Ok(rt) => Some(rt),
        Err(e) => {
            tracing::warn!(
                "cardano-swaps config present but unusable — integration disabled: {e:#}"
            );
            None
        }
    }
}

// ─── Beacon derivation ──────────────────────────────────────────────────────

/// Plutus `Data` CBOR for a bytestring (all our fields are ≤ 32 bytes, so
/// the 64-byte chunking rule never applies).
fn cbor_bytes(b: &[u8], out: &mut Vec<u8>) {
    if b.len() < 24 {
        out.push(0x40 + b.len() as u8);
    } else {
        out.push(0x58);
        out.push(b.len() as u8);
    }
    out.extend_from_slice(b);
}

/// Plutus `Data` CBOR for `Constr idx [bytestrings…]`: constructors 0..6
/// encode as CBOR tags 121..127 wrapping an indefinite-length array, which
/// is what `serialise_data` produces on chain.
fn cbor_constr(idx: u8, fields: &[&[u8]]) -> Vec<u8> {
    let mut out = vec![0xd8, 121 + idx, 0x9f];
    for f in fields {
        cbor_bytes(f, &mut out);
    }
    out.push(0xff);
    out
}

fn sha2_256(b: &[u8]) -> Vec<u8> {
    use sha2::Digest;
    sha2::Sha256::digest(b).to_vec()
}

/// `sha2_256(serialise_data(OfferAsset(policy, name)))` — `SwapAsset`
/// constructor 0.
pub fn offer_beacon(asset: &AssetClass) -> Vec<u8> {
    sha2_256(&cbor_constr(0, &[&asset.policy, &asset.token]))
}

/// `sha2_256(serialise_data(AskAsset(policy, name)))` — constructor 1.
pub fn ask_beacon(asset: &AssetClass) -> Vec<u8> {
    sha2_256(&cbor_constr(1, &[&asset.policy, &asset.token]))
}

/// `sha2_256(serialise_data(TradingPair(offer…, ask…)))` — constructor 2.
/// Directional: the offer always comes first, so ADA→TOKEN and TOKEN→ADA
/// have distinct pair beacons.
pub fn pair_beacon(offer: &AssetClass, ask: &AssetClass) -> Vec<u8> {
    sha2_256(&cbor_constr(
        2,
        &[&offer.policy, &offer.token, &ask.policy, &ask.token],
    ))
}

// ─── Order datum ────────────────────────────────────────────────────────────

/// The one-way `SwapDatum`: 11 fields, all of which the continuing output
/// must reproduce except `prev_input`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SwapDatum {
    pub beacon_id: Vec<u8>,
    pub pair_beacon: Vec<u8>,
    pub offer: AssetClass,
    pub offer_beacon: Vec<u8>,
    pub ask: AssetClass,
    pub ask_beacon: Vec<u8>,
    /// The owner's limit, ask per offer. Both parts are > 0 (the minting
    /// policy enforces it at creation).
    pub price_num: u64,
    pub price_den: u64,
    pub prev_input: Option<TransactionInput>,
    /// POSIX milliseconds, always on a 1-minute boundary when present.
    pub expiration: Option<u64>,
}

fn constr(idx: u64, fields: Vec<PlutusData>) -> PlutusData {
    let tag = if idx < 7 { 121 + idx } else { 1280 + (idx - 7) };
    PlutusData::Constr(pallas_primitives::Constr {
        tag,
        any_constructor: None,
        fields: pallas_primitives::MaybeIndefArray::Def(fields),
    })
}
fn pd_int(i: i128) -> PlutusData {
    PlutusData::BigInt(pallas_primitives::BigInt::Int((i as i64).into()))
}
fn pd_bytes(b: &[u8]) -> PlutusData {
    PlutusData::BoundedBytes(b.to_vec().into())
}

fn constr_index(c: &pallas_primitives::Constr<PlutusData>) -> Option<u64> {
    match c.tag {
        121..=127 => Some(c.tag - 121),
        1280..=1400 => Some(c.tag - 1280 + 7),
        102 => c.any_constructor,
        _ => None,
    }
}
fn as_constr(d: &PlutusData) -> Option<(u64, &[PlutusData])> {
    match d {
        PlutusData::Constr(c) => Some((constr_index(c)?, c.fields.as_ref())),
        _ => None,
    }
}
fn as_bytes(d: &PlutusData) -> Option<&[u8]> {
    match d {
        PlutusData::BoundedBytes(b) => Some(b.as_ref()),
        _ => None,
    }
}
fn as_u64(d: &PlutusData) -> Option<u64> {
    match d {
        PlutusData::BigInt(pallas_primitives::BigInt::Int(i)) => {
            let v: i128 = (*i).into();
            u64::try_from(v).ok()
        }
        _ => None,
    }
}

/// `Option<T>`: `Constr 0 [x]` = Some, `Constr 1 []` = None.
fn as_option(d: &PlutusData) -> Option<Option<&PlutusData>> {
    match as_constr(d)? {
        (0, f) => Some(Some(f.first()?)),
        (1, _) => Some(None),
        _ => None,
    }
}

fn parse_outref_data(d: &PlutusData) -> Option<TransactionInput> {
    let (0, f) = as_constr(d)? else { return None };
    let txid = as_bytes(f.first()?)?;
    if txid.len() != 32 {
        return None;
    }
    let idx = as_u64(f.get(1)?)?;
    let hash: [u8; 32] = txid.try_into().ok()?;
    Some(TransactionInput::new(hash.into(), idx))
}

impl SwapDatum {
    pub fn parse(d: &PlutusData) -> Option<Self> {
        let (0, f) = as_constr(d)? else { return None };
        if f.len() != 11 {
            return None;
        }
        let (price_num, price_den) = {
            let (0, r) = as_constr(&f[8])? else {
                return None;
            };
            (as_u64(r.first()?)?, as_u64(r.get(1)?)?)
        };
        if price_num == 0 || price_den == 0 {
            return None;
        }
        let prev_input = match as_option(&f[9])? {
            Some(x) => Some(parse_outref_data(x)?),
            None => None,
        };
        let expiration = match as_option(&f[10])? {
            Some(x) => Some(as_u64(x)?),
            None => None,
        };
        Some(Self {
            beacon_id: as_bytes(&f[0])?.to_vec(),
            pair_beacon: as_bytes(&f[1])?.to_vec(),
            offer: AssetClass {
                policy: as_bytes(&f[2])?.to_vec(),
                token: as_bytes(&f[3])?.to_vec(),
            },
            offer_beacon: as_bytes(&f[4])?.to_vec(),
            ask: AssetClass {
                policy: as_bytes(&f[5])?.to_vec(),
                token: as_bytes(&f[6])?.to_vec(),
            },
            ask_beacon: as_bytes(&f[7])?.to_vec(),
            price_num,
            price_den,
            prev_input,
            expiration,
        })
    }

    pub fn to_plutus(&self) -> PlutusData {
        let option = |x: Option<PlutusData>| match x {
            Some(v) => constr(0, vec![v]),
            None => constr(1, vec![]),
        };
        constr(
            0,
            vec![
                pd_bytes(&self.beacon_id),
                pd_bytes(&self.pair_beacon),
                pd_bytes(&self.offer.policy),
                pd_bytes(&self.offer.token),
                pd_bytes(&self.offer_beacon),
                pd_bytes(&self.ask.policy),
                pd_bytes(&self.ask.token),
                pd_bytes(&self.ask_beacon),
                constr(
                    0,
                    vec![
                        pd_int(self.price_num as i128),
                        pd_int(self.price_den as i128),
                    ],
                ),
                option(self.prev_input.as_ref().map(|i| {
                    constr(
                        0,
                        vec![
                            pd_bytes(i.0.transaction_id.as_ref()),
                            pd_int(i.0.index as i128),
                        ],
                    )
                })),
                option(self.expiration.map(|e| pd_int(e as i128))),
            ],
        )
    }

    /// Whether the datum's own beacon names agree with its assets and the
    /// configured policy. An order that fails this was mis-created and is
    /// its owner's problem: the validator would reject our fill, so it is
    /// skipped rather than trusted.
    pub fn beacons_are_consistent(&self, beacon_policy: &[u8]) -> bool {
        self.beacon_id == beacon_policy
            && self.offer != self.ask
            && self.pair_beacon == pair_beacon(&self.offer, &self.ask)
            && self.offer_beacon == offer_beacon(&self.offer)
            && self.ask_beacon == ask_beacon(&self.ask)
    }

    /// The three beacons, as asset classes under the beacon policy.
    pub fn beacon_assets(&self) -> [AssetClass; 3] {
        [&self.pair_beacon, &self.offer_beacon, &self.ask_beacon].map(|n| AssetClass {
            policy: self.beacon_id.clone(),
            token: n.clone(),
        })
    }
}

/// An indexed, unspent one-way swap order.
#[derive(Clone, Debug)]
pub struct SwapOrder {
    pub input: TransactionInput,
    /// Full address bytes: the swap script's payment credential plus the
    /// owner's staking credential.
    pub address: Vec<u8>,
    pub value: Value,
    pub datum: SwapDatum,
}

impl SwapOrder {
    pub fn from_output(input: TransactionInput, out: &TransactionOutput) -> Option<Self> {
        let crate::cardano_types::RawDatum::Inline(d) = &out.datum else {
            return None;
        };
        let datum = SwapDatum::parse(d)?;
        Some(Self {
            input,
            address: out.address.to_vec(),
            value: out.value.clone(),
            datum,
        })
    }

    /// Whether the UTxO actually holds the beacons its datum names. The
    /// validator checks this too, so a mismatch means an unfillable UTxO.
    pub fn holds_its_beacons(&self) -> bool {
        self.datum.beacon_assets().iter().all(|b| self.value.get(b) == BigInt::from(1))
    }
}

/// Everything the indexer tracks for the integration: just the open orders.
#[derive(Clone, Debug, Default)]
pub struct CardanoSwapsState {
    pub orders: BTreeMap<TransactionInput, SwapOrder>,
}

// ─── Fill math ──────────────────────────────────────────────────────────────

/// What the validator lets us take for `given` of the asked asset:
/// the largest `taken` with `taken · num <= given · den`.
pub fn offer_taken(given: u128, price_num: u64, price_den: u64) -> u128 {
    given * price_den as u128 / price_num as u128
}

/// The largest deposit worth making when only `avail` of the offered asset
/// can be taken. Giving more than this buys nothing.
pub fn max_ask_given(avail: u128, price_num: u64, price_den: u64) -> u128 {
    if avail == 0 {
        return 0;
    }
    // taken(given) = given·den/num, so given ≈ avail·num/den; trim the
    // flooring either way.
    let mut g = avail * price_num as u128 / price_den as u128 + 2;
    while g > 0 && offer_taken(g, price_num, price_den) > avail {
        g -= 1;
    }
    while offer_taken(g + 1, price_num, price_den) <= avail {
        g += 1;
    }
    g
}

// ─── Edges ──────────────────────────────────────────────────────────────────

/// How a planned leg maps back to the order it fills.
#[derive(Clone, Debug)]
pub struct SwapLeg {
    pub order: SwapOrder,
}

pub fn edge_key(order: &TransactionInput) -> String {
    format!("cswaps:{order}")
}

pub fn order_from_key(key: &str) -> Option<TransactionInput> {
    key.strip_prefix("cswaps:")?.parse().ok()
}

impl CardanoSwapsRuntime {
    /// How much of the offered asset a given order can part with.
    pub fn available_offer(&self, order: &SwapOrder) -> u128 {
        use num_traits::ToPrimitive;
        let held = order.value.get(&order.datum.offer).unwrap().to_u128().unwrap_or(0);
        if order.datum.offer == ADA_ASSET_CLASS {
            held.saturating_sub(self.min_keep_lovelace as u128)
        } else {
            held
        }
    }

    /// Routable edges for every fillable order. The edge's input is the
    /// asked asset (what we deposit) and its output the offered asset
    /// (what we take), at the datum's price with no fee — the price *is*
    /// the owner's limit, and our floored conversion math satisfies the
    /// validator's cross-multiplication exactly.
    pub fn edges(
        &self,
        state: &CardanoSwapsState,
        now_ms: u64,
    ) -> (Vec<ConversionEdge>, BTreeMap<String, SwapLeg>) {
        let mut edges = Vec::new();
        let mut legs = BTreeMap::new();
        for order in state.orders.values() {
            let d = &order.datum;
            if !d.beacons_are_consistent(&self.beacon_policy) || !order.holds_its_beacons() {
                tracing::debug!(order = %order.input, "cardano-swaps: inconsistent beacons, skipping");
                continue;
            }
            // An order that expires too soon cannot be filled inside a
            // sensible validity window.
            if let Some(exp) = d.expiration
                && exp <= now_ms
            {
                continue;
            }
            let avail = self.available_offer(order);
            let max_input = max_ask_given(avail, d.price_num, d.price_den);
            if max_input == 0 {
                continue;
            }
            let key = edge_key(&order.input);
            edges.push(ConversionEdge {
                key: key.clone(),
                from: d.ask.clone(),
                to: d.offer.clone(),
                rate_num: BigInt::from(d.price_den),
                rate_den: BigInt::from(d.price_num),
                fee_bps: 0,
                max_input: Some(BigInt::from(max_input as i128)),
            });
            legs.insert(
                key,
                SwapLeg {
                    order: order.clone(),
                },
            );
        }
        (edges, legs)
    }
}

/// An owned snapshot the scooper holds for one cycle.
#[derive(Clone, Debug)]
pub struct CardanoSwapsSnapshot {
    pub runtime: CardanoSwapsRuntime,
    pub state: CardanoSwapsState,
    pub legs: BTreeMap<String, SwapLeg>,
    pub slot_config: crate::sundaev4::types::SlotConfig,
}

impl CardanoSwapsSnapshot {
    pub fn edges(&self, now_ms: u64) -> Vec<ConversionEdge> {
        self.runtime.edges(&self.state, now_ms).0
    }

    pub fn context(&self) -> FillContext<'_> {
        FillContext {
            runtime: &self.runtime,
            legs: &self.legs,
            slot_config: self.slot_config.clone(),
        }
    }
}

/// What the builder needs to materialize planned legs.
pub struct FillContext<'a> {
    pub runtime: &'a CardanoSwapsRuntime,
    pub legs: &'a BTreeMap<String, SwapLeg>,
    pub slot_config: crate::sundaev4::types::SlotConfig,
}

// ─── Fill composition ───────────────────────────────────────────────────────

/// The transaction-level pieces of one fill.
#[derive(Clone, Debug)]
pub struct FillPieces {
    pub order_input: TransactionInput,
    pub order_address: Vec<u8>,
    pub order_value: Value,
    pub order_datum: PlutusData,
    pub continuing_output: conway::TransactionOutput,
    /// The swap script's reference UTxO.
    pub ref_inputs: Vec<TransactionInput>,
    /// Upper bound (POSIX ms) the transaction must respect, when the order
    /// sets an expiration.
    pub expiration_ms: Option<u64>,
}

impl FillPieces {
    /// The public `Swap` redeemer: `SwapRedeemer` constructor 2
    /// (`SpendWithMint`, `SpendWithStake`, `Swap`).
    pub fn spend_redeemer() -> PlutusData {
        constr(2, vec![])
    }
}

impl FillContext<'_> {
    /// Compose the pieces for a planned leg: `dx` of the asked asset goes
    /// in, `out` of the offered asset comes out. The amounts must be the
    /// ones the router computed from the same edge; recomputed and checked
    /// here against the validator's own rule.
    pub fn fill_pieces(&self, key: &str, dx: u128, out: u128) -> Result<FillPieces> {
        let leg = self.legs.get(key).with_context(|| format!("no cardano-swaps leg for {key}"))?;
        let order = &leg.order;
        let d = &order.datum;

        anyhow::ensure!(dx > 0 && out > 0, "leg {key}: empty fill");
        let expected = offer_taken(dx, d.price_num, d.price_den);
        anyhow::ensure!(
            out <= expected,
            "leg {key}: planned take {out} exceeds the price's {expected} for a {dx} deposit"
        );
        // The validator's exact rule, re-checked on the numbers we will
        // actually put on chain.
        anyhow::ensure!(
            out * d.price_num as u128 <= dx * d.price_den as u128,
            "leg {key}: offer_taken · price_num > ask_given · price_den"
        );

        let mut value = order.value.clone();
        let offer_held = value.get(&d.offer);
        anyhow::ensure!(
            offer_held >= BigInt::from(out as i128),
            "leg {key}: order holds less of the offered asset than the plan takes"
        );
        value.insert(&d.offer, offer_held - BigInt::from(out as i128));
        value.insert(&d.ask, value.get(&d.ask) + BigInt::from(dx as i128));

        // Asset purity: the output may hold only the three beacons, the
        // two traded assets and ADA. Drop a traded asset that reached zero
        // so the value encodes without an empty entry.
        let zero = BigInt::from(0);
        for class in [&d.offer, &d.ask] {
            if *class != ADA_ASSET_CLASS && value.get(class) == zero {
                if let Some(m) = value.0.get_mut(&class.policy) {
                    m.remove(&class.token);
                }
                if value.0.get(&class.policy).is_some_and(|m| m.is_empty()) {
                    value.0.remove(&class.policy);
                }
            }
        }
        anyhow::ensure!(
            value.get(&ADA_ASSET_CLASS) >= BigInt::from(self.runtime.min_keep_lovelace as i128),
            "leg {key}: continuing output would fall below the configured lovelace floor"
        );
        // ADA may only be deposited when it is not part of the pair.
        if d.offer != ADA_ASSET_CLASS && d.ask != ADA_ASSET_CLASS {
            anyhow::ensure!(
                value.get(&ADA_ASSET_CLASS) >= order.value.get(&ADA_ASSET_CLASS),
                "leg {key}: ADA may only be deposited for a token/token pair"
            );
        }

        let mut new_datum = d.clone();
        new_datum.prev_input = Some(order.input.clone());
        let continuing_output = conway::TransactionOutput::PostAlonzo(
            pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
                address: pallas_primitives::Bytes::from(order.address.clone()),
                value: crate::sundaev4::tx_builder::value_to_conway(&value)?,
                datum_option: Some(conway::PseudoDatumOption::Data(CborWrap(
                    new_datum.to_plutus(),
                ))),
                // Requirement: a continuing output may carry no reference
                // script.
                script_ref: None,
            },
        );

        Ok(FillPieces {
            order_input: order.input.clone(),
            order_address: order.address.clone(),
            order_value: order.value.clone(),
            order_datum: d.to_plutus(),
            continuing_output,
            ref_inputs: vec![self.runtime.swap_ref.clone()],
            expiration_ms: d.expiration,
        })
    }
}

// ─── Foreign fills: a pool swap serving a resting order ─────────────────────
//
// The pool's input asset is the order's offer (we take it from the order and
// put it in the pool); the pool's emission is the order's ask (we deposit it
// into the order). Exactly one side is ADA, and the lovelace left over on
// that side is the protocol's surplus:
//
//   offer == ADA: deposit everything the pool emitted; take as much ADA as
//                 the price allows; surplus = taken − pool dx − fee.
//   ask   == ADA: take exactly the pool's dx; deposit the least ADA the
//                 price demands; surplus = pool dy − deposited − fee.

/// The foreign order's exact amounts for a pool step of `dx` in and `dy`
/// out: `(take, deposit)` in the order's offer and ask assets.
pub fn foreign_amounts(
    datum: &SwapDatum,
    available_offer: u128,
    dx: u128,
    dy: u128,
) -> Result<(u128, u128)> {
    if datum.offer == ADA_ASSET_CLASS {
        let deposit = dy;
        let take = offer_taken(deposit, datum.price_num, datum.price_den).min(available_offer);
        anyhow::ensure!(
            take > 0,
            "the pool's emission buys nothing at the order's price"
        );
        Ok((take, deposit))
    } else if datum.ask == ADA_ASSET_CLASS {
        let take = dx;
        anyhow::ensure!(take <= available_offer, "take exceeds the order's offer");
        // Smallest deposit with take·num <= deposit·den.
        let deposit = (take * datum.price_num as u128).div_ceil(datum.price_den as u128);
        Ok((take, deposit))
    } else {
        anyhow::bail!("foreign fills need ADA on one side of the order");
    }
}

/// Argmax of an integer function on `[lo, hi]` that is unimodal up to
/// rounding ripple: a ternary search finds the hill, then the top edge
/// `hi` and a window around the hill are scanned exactly, since ceilings
/// in the price arithmetic make neighbouring points trade places.
fn argmax_unimodal(lo: u128, hi: u128, f: impl Fn(u128) -> i128) -> u128 {
    let (mut a, mut b) = (lo, hi);
    while b - a > 2 {
        let m1 = a + (b - a) / 3;
        let m2 = b - (b - a) / 3;
        if f(m1) < f(m2) {
            a = m1 + 1;
        } else {
            b = m2;
        }
    }
    let peak = (a..=b).max_by_key(|x| f(*x)).unwrap_or(a);
    let window = 2_000u128;
    let scan_lo = peak.saturating_sub(window).max(lo);
    let scan_hi = (peak + window).min(hi);
    (scan_lo..=scan_hi).chain(std::iter::once(hi)).max_by_key(|x| f(*x)).unwrap_or(peak)
}

/// A planned foreign fill: the pool step plus what it is expected to land.
#[derive(Clone, Debug)]
pub struct ForeignFillPlan {
    pub resolved: crate::sundaev4::batch::ResolvedForeign,
    /// Lovelace the fill releases before the transaction fee.
    pub gross_surplus: u64,
}

impl CardanoSwapsRuntime {
    /// Size a fill of `leg`'s order against `pool`, maximizing the lovelace
    /// released, and return it only if that clears the configured fee
    /// estimate plus revenue floor. The real fee is re-checked after the
    /// transaction is built.
    pub fn plan_foreign_fill(
        &self,
        pool: &crate::sundaev4::types::SundaeV4Pool,
        leg: &SwapLeg,
    ) -> Option<ForeignFillPlan> {
        use num_traits::ToPrimitive;
        let cfg = self.foreign_fills.as_ref()?;
        let d = &leg.order.datum;
        let assets = &pool.pool_datum.assets;
        if assets.len() != 2 {
            return None;
        }
        let input_idx = assets.iter().position(|(a, _)| *a == d.offer)?;
        let output_idx = assets.iter().position(|(a, _)| *a == d.ask)?;
        if d.offer != ADA_ASSET_CLASS && d.ask != ADA_ASSET_CLASS {
            return None;
        }
        let available = self.available_offer(&leg.order);
        if available == 0 {
            return None;
        }
        let dy_of = |dx: u128| -> u128 {
            crate::sundaev4::batch::compute_swap_result(
                &pool.pool_type,
                assets,
                &pool.pool_datum.total_lp,
                input_idx,
                output_idx,
                &BigInt::from(dx as i128),
            )
            .unwrap()
            .to_u128()
            .unwrap_or(0)
        };
        // Lovelace released by a pool step of `dx` (negative = a loss).
        let surplus_of = |dx: u128| -> i128 {
            if dx == 0 {
                return i128::MIN / 2;
            }
            let dy = dy_of(dx);
            if dy == 0 {
                return i128::MIN / 2;
            }
            match foreign_amounts(d, available, dx, dy) {
                Ok((take, deposit)) => {
                    if d.offer == ADA_ASSET_CLASS {
                        take as i128 - dx as i128
                    } else {
                        dy as i128 - deposit as i128
                    }
                }
                Err(_) => i128::MIN / 2,
            }
        };
        // Search bound: past the point where the order is exhausted, more
        // dx only costs. For an ADA offer that is the dx buying `available`
        // at the price; for a token offer it is `available` itself.
        let hi = if d.offer == ADA_ASSET_CLASS {
            available * d.price_num as u128 / d.price_den as u128 * 2 + 2
        } else {
            available
        };
        let dx = argmax_unimodal(1, hi.max(2), surplus_of);
        let gross = surplus_of(dx);
        if gross <= 0 {
            return None;
        }
        let gross = gross as u64;
        if gross < cfg.fee_estimate_lovelace + cfg.min_revenue_lovelace {
            tracing::debug!(
                order = %leg.order.input, pool = %pool.pool_datum.identifier,
                gross, "cardano-swaps: foreign fill below the projected floor",
            );
            return None;
        }
        Some(ForeignFillPlan {
            resolved: crate::sundaev4::batch::ResolvedForeign {
                key: edge_key(&leg.order.input),
                attribution: leg.order.input.clone(),
                input_idx,
                output_idx,
                dx: BigInt::from(dx as i128),
            },
            gross_surplus: gross,
        })
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod unit_tests {
    use super::*;

    fn usdm() -> AssetClass {
        AssetClass {
            policy: hex::decode("c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad")
                .unwrap(),
            token: hex::decode("0014df105553444d").unwrap(),
        }
    }

    /// The repo's golden vectors (aiken/lib/cardano_swaps/one_way_swap/
    /// utils.ak) plus the beacons in the live mainnet datums.
    #[test]
    fn beacon_derivation_matches_the_repo_and_the_chain() {
        assert_eq!(
            hex::encode(offer_beacon(&ADA_ASSET_CLASS)),
            "07d5f63e85046b83e1fc4102a7c19c3f1711c51984725e3b6cf195900947cebe"
        );
        assert_eq!(
            hex::encode(ask_beacon(&ADA_ASSET_CLASS)),
            "08bae3e35a3531a500149bd10d9b872621a41b4f6ba086920518220829370d2b"
        );
        // The repo's native-asset vector: pre-image
        // d8799f581c0011…aabb44444a4544ff.
        let djed = AssetClass {
            policy: hex::decode("00112233445566778899aabbccddeeff00112233445566778899aabb")
                .unwrap(),
            token: hex::decode("444a4544").unwrap(),
        };
        assert_eq!(
            hex::encode(offer_beacon(&djed)),
            "2dab55ec954afe698b6de75d81f0067fd503d1e4ecd49d593f8a3b9c9a963d4f"
        );
        // From the live ADA→USDM orders.
        assert_eq!(
            hex::encode(ask_beacon(&usdm())),
            "3e785839fd0f65a4689c51fa4f1dfe27066922ecfe66fa5ad8b2c5ea2ec0e055"
        );
        assert_eq!(
            hex::encode(pair_beacon(&ADA_ASSET_CLASS, &usdm())),
            "c35c0f3115aec28da91f69abd64195d2731a5e600af8e61ca5aab940916652b8"
        );
        // Directional: swapping the sides gives a different pair beacon.
        assert_ne!(
            pair_beacon(&ADA_ASSET_CLASS, &usdm()),
            pair_beacon(&usdm(), &ADA_ASSET_CLASS)
        );
    }

    #[test]
    fn fill_math_matches_the_two_mainnet_fills() {
        // ca27ff15…: 1000 ADA taken for 230 USDM at 23/100.
        assert_eq!(offer_taken(230_000_000, 23, 100), 1_000_000_000);
        // 9d8260ab…: 1 USDCx taken for 1.5 ADA at 3/2.
        assert_eq!(offer_taken(1_500_000, 3, 2), 1_000_000);
    }

    #[test]
    fn max_given_is_the_largest_useful_deposit() {
        for (num, den) in [(23u64, 100u64), (3, 2), (713, 3125), (1, 1), (7, 50)] {
            for avail in [0u128, 1, 999, 1_000_000, 50_000_000_000] {
                let g = max_ask_given(avail, num, den);
                assert!(
                    offer_taken(g, num, den) <= avail,
                    "{num}/{den} avail {avail}: given {g} takes too much"
                );
                if avail > 0 {
                    assert!(
                        offer_taken(g + 1, num, den) > avail,
                        "{num}/{den} avail {avail}: given {g} is not maximal"
                    );
                }
            }
        }
    }

    #[test]
    fn datum_round_trips() {
        let d = SwapDatum {
            beacon_id: vec![9; 28],
            pair_beacon: pair_beacon(&ADA_ASSET_CLASS, &usdm()),
            offer: ADA_ASSET_CLASS,
            offer_beacon: offer_beacon(&ADA_ASSET_CLASS),
            ask: usdm(),
            ask_beacon: ask_beacon(&usdm()),
            price_num: 23,
            price_den: 100,
            prev_input: Some(TransactionInput::new([4u8; 32].into(), 7)),
            expiration: Some(1_792_000_020_000),
        };
        assert_eq!(SwapDatum::parse(&d.to_plutus()).as_ref(), Some(&d));
        let mut none = d.clone();
        none.prev_input = None;
        none.expiration = None;
        assert_eq!(SwapDatum::parse(&none.to_plutus()).as_ref(), Some(&none));
        // A zero price part is rejected rather than divided by.
        let mut bad = d.clone();
        bad.price_num = 0;
        assert!(SwapDatum::parse(&bad.to_plutus()).is_none());
    }

    #[test]
    fn config_degrades_rather_than_panicking() {
        assert!(load_runtime(&None).is_none());
        let mut cfg = CardanoSwapsConfig {
            enabled: false,
            swap_script_hash: "e5a22e4c31db20bce1c8b081f8e4009683990a33157947d75030deb8".into(),
            swap_ref_utxo: format!("{}#0", "ab".repeat(32)),
            beacon_policy: "274765b4c626c28d18752176b59c0ff63db56b8305c1daa49c9879fe".into(),
            min_keep_lovelace: 3_000_000,
            foreign_fills: None,
        };
        assert!(
            load_runtime(&Some(cfg.clone())).is_none(),
            "disabled stays off"
        );
        cfg.enabled = true;
        assert!(load_runtime(&Some(cfg.clone())).is_some());
        cfg.swap_ref_utxo = "not-an-outref".into();
        assert!(load_runtime(&Some(cfg)).is_none());
    }
}

#[cfg(test)]
mod foreign_fill_tests {
    //! A resting Cardano-Swaps order filled against one of our pools with
    //! no Sundae order in the transaction: the pool's emission goes into the
    //! order, the lovelace the order releases beyond the pool's price lands
    //! on the pool as surplus, and every validator — ours and theirs — runs.

    use super::*;
    use crate::sundaev4::batch::{Batch, BatchOp, ScoopPlan};
    use crate::sundaev4::test_harness::{TestEnv, ada, make_pool, make_settings};
    use num_traits::ToPrimitive;

    const BLUEPRINT_PATH: &str = "test/fixtures/devnet-blueprint.json";

    fn usdm() -> AssetClass {
        AssetClass {
            policy: hex::decode("c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad")
                .unwrap(),
            token: hex::decode("0014df105553444d").unwrap(),
        }
    }
    fn usdcx() -> AssetClass {
        AssetClass {
            policy: hex::decode("1f3aec8bfe7ea4fe14c5f121e2a92e301afe414147860d557cac7e34")
                .unwrap(),
            token: hex::decode("5553444378").unwrap(),
        }
    }

    fn leg_for<'a>(snap: &'a CardanoSwapsSnapshot, offer: &AssetClass) -> &'a SwapLeg {
        snap.legs
            .values()
            .find(|l| l.order.datum.offer == *offer)
            .expect("fixture order with that offer")
    }

    fn plan_of(
        pool: &std::sync::Arc<crate::sundaev4::types::SundaeV4Pool>,
        fill: &ForeignFillPlan,
    ) -> ScoopPlan {
        ScoopPlan {
            batches: vec![Batch {
                pool: pool.clone(),
                pool_ident: pool.pool_datum.identifier.clone(),
                swaps: Vec::new(),
                continuations: Vec::new(),
                deposits: Vec::new(),
                withdraws: Vec::new(),
                zaps: Vec::new(),
                claims: Vec::new(),
                foreign: vec![fill.resolved.clone()],
                ops_order: vec![BatchOp::Foreign(0)],
                final_assets: pool.pool_datum.assets.clone(),
                final_total_lp: pool.pool_datum.total_lp.clone(),
            }],
            routes: Vec::new(),
            global_seq: Vec::new(),
            conversions: Vec::new(),
        }
    }

    fn lovelace_of(out: &conway::TransactionOutput) -> u64 {
        match out {
            conway::TransactionOutput::PostAlonzo(b) => match &b.value {
                conway::Value::Coin(c) => *c,
                conway::Value::Multiasset(c, _) => *c,
            },
            _ => 0,
        }
    }

    /// The order sells ADA at 0.23 USDM; a pool valuing ADA at 0.25 USDM
    /// buys it cheaper than it is worth. We take ADA from the order, feed
    /// the pool, and deposit the pool's USDM into the order.
    #[test]
    fn ada_offer_fill_lands_surplus_on_the_pool() {
        let mut env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let snap = env.enable_cardano_swaps().clone();
        // 400k ADA / 100k USDM: mid 0.25 USDM per ADA, deep enough that a
        // few hundred ADA barely moves it.
        let pool = make_pool(&env, 0xF1, ada(), 400_000_000_000, usdm(), 100_000_000_000);
        let leg = leg_for(&snap, &ada());
        let fill = snap
            .runtime
            .plan_foreign_fill(&pool, leg)
            .expect("the price gap makes the fill worth planning");
        assert!(fill.gross_surplus > 0, "{fill:?}");
        // The pool buys ADA for USDM; the order sells ADA for USDM.
        assert_eq!(pool.pool_datum.assets[fill.resolved.input_idx].0, ada());
        assert_eq!(pool.pool_datum.assets[fill.resolved.output_idx].0, usdm());

        let plan = plan_of(&pool, &fill);
        let settings = make_settings(&env, &env.scooper_keyhash());
        let (build, eval) = env
            .build_and_eval_plan(&plan, &settings, 200_000_000)
            .expect("pool, modules and the swap validator all accept the fill");

        // Exactly two spends: the pool and the foreign order. No Sundae order
        // validator ran, so there is no order withdrawal.
        let spends =
            eval.budgets.iter().filter(|(k, _)| k.tag == conway::RedeemerTag::Spend).count();
        assert_eq!(spends, 2, "{:?}", eval.budgets);
        let order_hash = env.exec.module_scripts.order.hash;
        assert!(
            !build
                .tx_body
                .withdrawals
                .as_ref()
                .unwrap()
                .iter()
                .any(|(acct, _)| acct[1..] == order_hash.as_ref()[..]),
            "no order-validator withdrawal in a foreign-only fill"
        );

        // The surplus landed: pool output 0 holds its declared reserve plus
        // the fee-netted release, and the number the builder reports is the
        // exact residual of value conservation.
        let surplus = build.foreign_surplus.expect("foreign-only fills report their surplus");
        assert!(surplus > 0);
        let pool_in = pool.value.get(&ada());
        let pool_out = lovelace_of(&build.tx_body.outputs[0]);
        let dx = fill.resolved.dx.clone().unwrap();
        assert_eq!(
            pool_out as i128,
            pool_in.unwrap().to_i128().unwrap() + dx.to_i128().unwrap() + surplus as i128,
            "pool output = reserve in + declared dx + landed surplus"
        );
        // Gross minus the real fee is what landed.
        assert_eq!(surplus + build.tx_body.fee, fill.gross_surplus);
        // The predicted pool the chain tracker will chain on carries it too.
        let (_, _, predicted) = &build.predicted_pools[0];
        assert_eq!(
            predicted.value.get(&ada()).unwrap().to_u64().unwrap(),
            pool_out
        );
    }

    /// The other direction: the order sells USDCx for 1.5 ADA each; a pool
    /// valuing USDCx at 10 ADA pays far more. We take USDCx from the order,
    /// sell it to the pool, and deposit the least ADA the order's price
    /// demands. (The order holds only 3 USDCx, so the pool's price has to be
    /// well above the order's for three units to cover a 1.5 ADA fee.)
    #[test]
    fn token_offer_fill_lands_surplus_on_the_pool() {
        let mut env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let snap = env.enable_cardano_swaps().clone();
        let pool = make_pool(
            &env,
            0xF2,
            ada(),
            1_000_000_000_000,
            usdcx(),
            100_000_000_000,
        );
        let leg = leg_for(&snap, &usdcx());
        let fill = snap.runtime.plan_foreign_fill(&pool, leg).expect("worth planning");
        // Essentially the whole 3 USDCx the order holds is worth taking at
        // this gap. Not exactly all of it: the deposit is ceil(dx·3/2), so
        // the last unit can cost a lovelace more than it earns, and the
        // planner stops a unit short rather than pay it.
        use num_traits::ToPrimitive;
        let dx = fill.resolved.dx.clone().unwrap().to_u128().unwrap();
        assert!((2_999_990..=3_000_000).contains(&dx), "dx {dx}");

        let plan = plan_of(&pool, &fill);
        let settings = make_settings(&env, &env.scooper_keyhash());
        let (build, eval) = env
            .build_and_eval_plan(&plan, &settings, 200_000_000)
            .expect("the token-side fill builds and validates");
        assert_eq!(
            eval.budgets.iter().filter(|(k, _)| k.tag == conway::RedeemerTag::Spend).count(),
            2
        );
        let surplus = build.foreign_surplus.expect("reported");
        assert_eq!(surplus + build.tx_body.fee, fill.gross_surplus);
        // The order received exactly ceil(dx · 3/2) lovelace, the least the
        // price demands.
        let cont = build
            .tx_body
            .outputs
            .iter()
            .find(|o| matches!(o, conway::TransactionOutput::PostAlonzo(b) if b.address[1..29] == snap.runtime.swap_hash[..]))
            .expect("continuing output");
        let before = leg.order.value.get(&ada()).unwrap().to_u64().unwrap();
        assert_eq!(
            lovelace_of(cont) as u128,
            before as u128 + (dx * 3).div_ceil(2)
        );
    }

    /// A fill whose gross release is positive but smaller than the real
    /// fee is refused by the builder itself, independently of the planner's
    /// screen: three USDCx at a 0.5 ADA edge cannot pay a 1.5 ADA fee.
    #[test]
    fn a_fill_that_cannot_cover_its_fee_is_refused_by_the_builder() {
        let mut env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let snap = env.enable_cardano_swaps().clone();
        // 2 ADA per USDCx against the order's 1.5: a positive but tiny edge.
        let pool = make_pool(&env, 0xF6, ada(), 200_000_000_000, usdcx(), 100_000_000_000);
        let leg = leg_for(&snap, &usdcx());
        let fill = snap
            .runtime
            .plan_foreign_fill(&pool, leg)
            .expect("the fixture's zero floor lets it through");
        assert!(fill.gross_surplus < 1_500_000, "{fill:?}");
        let plan = plan_of(&pool, &fill);
        let settings = make_settings(&env, &env.scooper_keyhash());
        let err = match env.build_and_eval_plan(&plan, &settings, 200_000_000) {
            Ok(_) => panic!("a fill that cannot pay its fee must not build"),
            Err(e) => e,
        };
        assert!(
            format!("{err:#}").contains("does not cover its own fee"),
            "{err:#}"
        );
    }

    /// A pool priced on the wrong side of the order yields nothing to take,
    /// and the planner says so rather than proposing a losing fill.
    #[test]
    fn a_fill_that_loses_is_not_planned() {
        let mut env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let snap = env.enable_cardano_swaps().clone();
        // Pool values ADA at 0.20 USDM; the order wants 0.23. Buying ADA at
        // 0.23 to sell at 0.20 loses.
        let pool = make_pool(&env, 0xF3, ada(), 500_000_000_000, usdm(), 100_000_000_000);
        assert!(snap.runtime.plan_foreign_fill(&pool, leg_for(&snap, &ada())).is_none());
    }

    #[test]
    fn the_projected_floor_screens_small_fills() {
        let mut env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let snap = env.enable_cardano_swaps().clone();
        let pool = make_pool(&env, 0xF4, ada(), 400_000_000_000, usdm(), 100_000_000_000);
        let leg = leg_for(&snap, &ada());
        let gross = snap.runtime.plan_foreign_fill(&pool, leg).expect("plannable").gross_surplus;
        let mut strict = snap.runtime.clone();
        strict.foreign_fills = Some(ForeignFillConfig {
            enabled: true,
            fee_estimate_lovelace: gross,
            min_revenue_lovelace: 1,
        });
        assert!(
            strict.plan_foreign_fill(&pool, leg).is_none(),
            "fee estimate + floor exceed the gross"
        );
    }

    #[test]
    fn foreign_fills_cannot_share_a_transaction_with_sundae_orders() {
        use crate::sundaev4::test_harness::make_basic_swap_order;
        let mut env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let snap = env.enable_cardano_swaps().clone();
        let pool = make_pool(&env, 0xF5, ada(), 400_000_000_000, usdm(), 100_000_000_000);
        let fill = snap.runtime.plan_foreign_fill(&pool, leg_for(&snap, &ada())).unwrap();
        let mut plan = plan_of(&pool, &fill);
        // Bolt a Sundae order onto the same pool batch.
        let order = make_basic_swap_order(usdm(), 1_000_000, ada(), 1_000_000, 1);
        plan.batches[0].swaps.push(crate::sundaev4::batch::ResolvedSwap {
            order,
            input_idx: 1,
            output_idx: 0,
            dx: BigInt::from(1_000_000i64),
            dy: BigInt::from(3_900_000i64),
            route: None,
        });
        plan.batches[0].ops_order.push(BatchOp::Swap(0));
        let settings = make_settings(&env, &env.scooper_keyhash());
        let err = match env.build_and_eval_plan(&plan, &settings, 200_000_000) {
            Ok(_) => panic!("a mixed plan must be refused"),
            Err(e) => e,
        };
        assert!(
            format!("{err:#}").contains("foreign fills must be the only ops"),
            "{err:#}"
        );
    }
}

#[cfg(test)]
mod composition_tests {
    //! Build a scoop that fills a Cardano-Swaps order from a Sundae order
    //! and run every validator — ours and theirs — through the evaluator.

    use std::collections::BTreeMap;

    use super::*;
    use crate::sundaev4::accumulator::Accumulator;
    use crate::sundaev4::router;
    use crate::sundaev4::test_harness::{TestEnv, ada, make_basic_swap_order, make_settings};

    const BLUEPRINT_PATH: &str = "test/fixtures/devnet-blueprint.json";

    fn usdm() -> AssetClass {
        AssetClass {
            policy: hex::decode("c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad")
                .unwrap(),
            token: hex::decode("0014df105553444d").unwrap(),
        }
    }
    fn usdcx() -> AssetClass {
        AssetClass {
            policy: hex::decode("1f3aec8bfe7ea4fe14c5f121e2a92e301afe414147860d557cac7e34")
                .unwrap(),
            token: hex::decode("5553444378").unwrap(),
        }
    }

    #[test]
    fn fixture_offers_both_directions_as_edges() {
        let mut env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let snap = env.enable_cardano_swaps().clone();
        let edges = snap.edges(0);
        assert_eq!(edges.len(), 2, "{edges:?}");
        for e in &edges {
            assert_eq!(e.fee_bps, 0, "the datum price is the whole limit");
            // One order offers ADA for USDM; the other offers USDCx for ADA.
            if e.to == ada() {
                assert_eq!(e.from, usdm());
                // The order holds 1002.788570 ADA and keeps 2.78, so
                // 1000.008570 ADA is takeable; at 23/100 the largest
                // useful deposit is 230.001971 USDM, and one more unit
                // would buy nothing.
                let cap = max_ask_given(1_000_008_570, 23, 100);
                assert_eq!(cap, 230_001_971);
                assert_eq!(e.max_input, Some(BigInt::from(cap as i128)));
            } else {
                assert_eq!((e.from.clone(), e.to.clone()), (ada(), usdcx()));
            }
        }
    }

    /// A Sundae order sells USDM for ADA; the ADA comes out of a resting
    /// Cardano-Swaps limit order rather than a pool.
    #[test]
    fn sundae_order_buys_ada_from_a_resting_swap() {
        let mut env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let snap = env.enable_cardano_swaps().clone();
        let edges: Vec<ConversionEdge> =
            snap.edges(0).into_iter().filter(|e| e.to == ada()).collect();
        assert_eq!(edges.len(), 1);

        let pool_map: BTreeMap<_, _> = BTreeMap::new();
        // Offer 23 USDM, want at least 99 ADA (the price gives 100).
        let order = make_basic_swap_order(usdm(), 23_000_000, ada(), 99_000_000, 1);
        let blend = router::find_blended_route(
            &pool_map,
            &edges,
            &usdm(),
            &ada(),
            order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        )
        .expect("a route through the resting swap exists");
        assert_eq!(blend.branches.len(), 1);

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum.try_add_blended_order(&order, &blend, &pool_map).expect("order accumulates");
        let plan = accum.into_plan();
        assert_eq!(plan.batches.len(), 0, "no pool is involved");
        assert_eq!(plan.conversions.len(), 1);
        assert!(plan.conversions[0].primary, "the fill owns the order");
        assert_eq!(
            plan.conversions[0].out,
            BigInt::from(offer_taken(23_000_000, 23, 100) as i128),
            "the router priced it exactly as the validator will check it"
        );

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (build, eval) = env
            .build_and_eval_plan(&plan, &settings, 200_000_000)
            .expect("the fill builds and both validators accept it");
        // Our order's spend plus the swap's spend; no mint, no withdrawal
        // beyond our own constraint ones.
        let spends =
            eval.budgets.iter().filter(|(k, _)| k.tag == conway::RedeemerTag::Spend).count();
        assert_eq!(spends, 2, "{:?}", eval.budgets);
        assert!(build.tx_body.mint.is_none(), "a fill mints nothing");
        // The 3473-byte swap script joins the tiered reference-script fee.
        assert!(build.total_ref_script_bytes >= 3473);
    }

    /// The other direction: a Sundae order sells ADA for USDCx, taking it
    /// out of the order that offers USDCx and asks ADA.
    #[test]
    fn sundae_order_buys_a_token_from_a_resting_swap() {
        let mut env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let snap = env.enable_cardano_swaps().clone();
        let edges: Vec<ConversionEdge> =
            snap.edges(0).into_iter().filter(|e| e.to == usdcx()).collect();
        assert_eq!(edges.len(), 1);

        let pool_map: BTreeMap<_, _> = BTreeMap::new();
        // Offer 3 ADA, want at least 1.9 USDCx (3/1.5 = 2 USDCx at the price).
        let order = make_basic_swap_order(ada(), 3_000_000, usdcx(), 1_900_000, 1);
        let blend = router::find_blended_route(
            &pool_map,
            &edges,
            &ada(),
            &usdcx(),
            order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        )
        .expect("a route through the resting swap exists");
        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum.try_add_blended_order(&order, &blend, &pool_map).expect("order accumulates");
        let plan = accum.into_plan();
        assert_eq!(plan.conversions[0].out, BigInt::from(2_000_000i64));

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (_, eval) = env
            .build_and_eval_plan(&plan, &settings, 200_000_000)
            .expect("the token-side fill builds and validates");
        let spends =
            eval.budgets.iter().filter(|(k, _)| k.tag == conway::RedeemerTag::Spend).count();
        assert_eq!(spends, 2, "{:?}", eval.budgets);
    }
}

#[cfg(test)]
pub(crate) mod replay_tests {
    //! Replay the real mainnet fills through the local evaluator, then
    //! mutate them to pin each rule. These are the ground truth the
    //! builder must satisfy.

    use std::collections::BTreeMap;

    use pallas_primitives::conway;
    use pallas_primitives::{ExUnits, PlutusData};
    use pallas_traverse::MultiEraTx;

    use super::*;
    use crate::cardano_types::{self, RawDatum};
    use crate::sundaev4::evaluator::{ScriptStore, evaluate_scoop_tx};
    use crate::sundaev4::script_context::{DatumOption, ResolvedTxOut};
    use crate::sundaev4::test_harness::PLUTUS_V3_COST_MODEL;
    use crate::sundaev4::types::SlotConfig;

    /// Pi's example: 1000 ADA taken from a 1002.79 ADA order for 230 USDM
    /// at price 23/100.
    pub const FILL_ADA_OFFER: &str = include_str!(
        "../../test/fixtures/cardano-swaps/ca27ff15d8e25f40b1376f3f458fbfdecb55708a859c8542dce6ebb433eae2d4.tx.hex"
    );
    /// The other direction: 1 USDCx taken for 1.5 ADA at price 3/2, so ADA
    /// is the asked asset and is deposited.
    pub const FILL_TOKEN_OFFER: &str = include_str!(
        "../../test/fixtures/cardano-swaps/9d8260abb0945d59a15d0a1bc6fe6e5fbdd37c74e75cb5479e6fa8d8a81c2dcd.tx.hex"
    );
    pub const PRODUCERS: &[&str] = &[
        include_str!(
            "../../test/fixtures/cardano-swaps/27f6db12d5123488ad4e3013ad5dbd81a6ee3d2c441eaf081aba9bd43e57a4be.tx.hex"
        ),
        include_str!(
            "../../test/fixtures/cardano-swaps/59830c3931ddcb5050ec7153760a71b44227a29a5193d7b5f79dbf1a0907da8a.tx.hex"
        ),
        include_str!(
            "../../test/fixtures/cardano-swaps/a2d7bd79632037b47581af17fa900cd8718758d46014b0804e1e23c14678d3e0.tx.hex"
        ),
        include_str!(
            "../../test/fixtures/cardano-swaps/c4a5f322683e8c76e0012c5a14dc47c1cd5c5062ecb603c316c01334559882fa.tx.hex"
        ),
        include_str!(
            "../../test/fixtures/cardano-swaps/e7ab2e2e12b5ae51110758e3b668b31cfa215187d24c388bb5e736b6338d2a07.tx.hex"
        ),
        include_str!(
            "../../test/fixtures/cardano-swaps/e86d0fb6635e1295e7414b3790da5a77acb5c3bc4ff69cd710e603f2085eb3e4.tx.hex"
        ),
        FILL_ADA_OFFER,
        FILL_TOKEN_OFFER,
    ];

    pub const SWAP_HASH: &str = "e5a22e4c31db20bce1c8b081f8e4009683990a33157947d75030deb8";
    pub const BEACON_POLICY: &str = "274765b4c626c28d18752176b59c0ff63db56b8305c1daa49c9879fe";

    pub fn mainnet_slots() -> SlotConfig {
        SlotConfig {
            zero_slot: 4_492_800,
            zero_time: 1_596_059_091_000,
            slot_length: 1000,
        }
    }

    pub fn utxo_set() -> BTreeMap<TransactionInput, TransactionOutput> {
        let mut set = BTreeMap::new();
        for hex_tx in PRODUCERS {
            let bytes = hex::decode(hex_tx.trim()).expect("fixture is hex");
            let tx = MultiEraTx::decode(&bytes).expect("fixture decodes");
            let hash = tx.hash();
            for (idx, out) in tx.outputs().iter().enumerate() {
                set.insert(
                    TransactionInput::new(hash, idx as u64),
                    cardano_types::convert_txo(out),
                );
            }
        }
        set
    }

    fn resolve(out: &TransactionOutput) -> ResolvedTxOut {
        let datum = match &out.datum {
            RawDatum::None => DatumOption::None,
            RawDatum::Inline(d) => DatumOption::InlineDatum(d.clone()),
            RawDatum::Hash(h) => DatumOption::DatumHash(*h),
        };
        let script_ref = out.script_ref.as_ref().map(|s| match s {
            cardano_types::ScriptRef::PlutusV3(script) => {
                let mut preimage = vec![0x03u8];
                preimage.extend_from_slice(script.as_ref());
                pallas_crypto::hash::Hasher::<224>::hash(&preimage)
            }
            other => panic!("unexpected ref script kind: {other:?}"),
        });
        ResolvedTxOut {
            address: out.address.to_vec(),
            value: out.value.clone(),
            datum,
            script_ref,
        }
    }

    pub struct Replay {
        pub body: conway::TransactionBody,
        pub redeemers: Vec<(conway::RedeemersKey, PlutusData, ExUnits)>,
        pub resolved_inputs: BTreeMap<TransactionInput, ResolvedTxOut>,
        pub resolved_refs: BTreeMap<TransactionInput, ResolvedTxOut>,
        pub scripts: ScriptStore,
    }

    pub fn replay(fill_hex: &str) -> Replay {
        let bytes = hex::decode(fill_hex.trim()).expect("fixture is hex");
        let tx: conway::Tx = minicbor::decode(&bytes).expect("conway tx decodes");
        let utxos = utxo_set();

        let mut resolved_inputs = BTreeMap::new();
        for i in tx.transaction_body.inputs.iter() {
            let key = TransactionInput::new(i.transaction_id, i.index);
            let out = utxos.get(&key).unwrap_or_else(|| panic!("input {key} not in fixtures"));
            resolved_inputs.insert(key, resolve(out));
        }
        let mut resolved_refs = BTreeMap::new();
        let mut ref_outputs = BTreeMap::new();
        if let Some(refs) = &tx.transaction_body.reference_inputs {
            for i in refs.iter() {
                let key = TransactionInput::new(i.transaction_id, i.index);
                let out =
                    utxos.get(&key).unwrap_or_else(|| panic!("ref input {key} not in fixtures"));
                resolved_refs.insert(key.clone(), resolve(out));
                ref_outputs.insert(key, out.clone());
            }
        }
        let scripts = ScriptStore::from_ref_utxos(&ref_outputs).expect("scripts load");
        let redeemers: Vec<(conway::RedeemersKey, PlutusData, ExUnits)> =
            match tx.transaction_witness_set.redeemer.as_ref().expect("fill has redeemers") {
                conway::Redeemers::List(list) => list
                    .iter()
                    .map(|r| {
                        (
                            conway::RedeemersKey {
                                tag: r.tag,
                                index: r.index,
                            },
                            r.data.clone(),
                            r.ex_units,
                        )
                    })
                    .collect(),
                conway::Redeemers::Map(map) => {
                    map.iter().map(|(k, v)| (k.clone(), v.data.clone(), v.ex_units)).collect()
                }
            };
        Replay {
            body: tx.transaction_body,
            redeemers,
            resolved_inputs,
            resolved_refs,
            scripts,
        }
    }

    impl Replay {
        pub fn evaluate(&self) -> anyhow::Result<Vec<(conway::RedeemersKey, ExUnits)>> {
            let body_cbor = minicbor::to_vec(&self.body).expect("body encodes");
            let tx_hash = pallas_crypto::hash::Hasher::<256>::hash(&body_cbor);
            let result = evaluate_scoop_tx(
                &self.body,
                &self.redeemers,
                &self.resolved_inputs,
                &self.resolved_refs,
                &self.scripts,
                PLUTUS_V3_COST_MODEL,
                None,
                tx_hash,
                &mainnet_slots(),
                None,
            )?;
            Ok(result.budgets)
        }
    }

    fn swap_output(body: &mut conway::TransactionBody) -> &mut conway::TransactionOutput {
        let hash = hex::decode(SWAP_HASH).unwrap();
        body.outputs
            .iter_mut()
            .find(|o| match o {
                conway::TransactionOutput::PostAlonzo(p) => p.address[1..29] == hash[..],
                _ => false,
            })
            .expect("continuing output present")
    }

    fn bump(out: &mut conway::TransactionOutput, class: &AssetClass, delta: i64) {
        let conway::TransactionOutput::PostAlonzo(p) = out else {
            panic!("post-alonzo")
        };
        let mut value = crate::sundaev4::tx_builder::conway_value_to_internal(&p.value);
        let current = value.get(class);
        value.insert(class, current + BigInt::from(delta));
        p.value = crate::sundaev4::tx_builder::value_to_conway(&value).expect("value encodes");
    }

    fn probe(
        fill_hex: &str,
        edit: impl FnOnce(&mut conway::TransactionBody),
    ) -> anyhow::Result<()> {
        let mut r = replay(fill_hex);
        edit(&mut r.body);
        r.evaluate().map(|_| ())
    }

    fn usdm() -> AssetClass {
        AssetClass {
            policy: hex::decode("c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad")
                .unwrap(),
            token: hex::decode("0014df105553444d").unwrap(),
        }
    }
    fn usdcx() -> AssetClass {
        AssetClass {
            policy: hex::decode("1f3aec8bfe7ea4fe14c5f121e2a92e301afe414147860d557cac7e34")
                .unwrap(),
            token: hex::decode("5553444378").unwrap(),
        }
    }

    #[test]
    fn mainnet_fills_evaluate() {
        for (label, fill) in [
            ("ada offer", FILL_ADA_OFFER),
            ("token offer", FILL_TOKEN_OFFER),
        ] {
            let r = replay(fill);
            let budgets = r.evaluate().unwrap_or_else(|e| panic!("{label}: {e:#}"));
            assert_eq!(
                budgets.len(),
                1,
                "{label}: one spend, no withdrawals, no mint"
            );
        }
    }

    #[test]
    fn the_price_is_a_floor_on_what_we_deposit() {
        // Depositing one more unit of the asked asset is always fine.
        probe(FILL_ADA_OFFER, |b| bump(swap_output(b), &usdm(), 1)).expect("overpaying is fine");
        // One less breaks the cross-multiplication.
        let err = probe(FILL_ADA_OFFER, |b| bump(swap_output(b), &usdm(), -1))
            .expect_err("underpaying must fail");
        assert!(
            format!("{err:#}").contains("offer_taken * price <= ask_given"),
            "{err:#}"
        );
    }

    #[test]
    fn taking_more_of_the_offer_than_the_price_allows_fails() {
        // Leaving the order one lovelace short = taking one more.
        let err = probe(FILL_ADA_OFFER, |b| {
            bump(swap_output(b), &ADA_ASSET_CLASS, -1)
        })
        .expect_err("taking more must fail");
        assert!(
            format!("{err:#}").contains("offer_taken * price <= ask_given"),
            "{err:#}"
        );
        // Taking less than the deposit entitles us to is allowed.
        probe(FILL_ADA_OFFER, |b| {
            bump(swap_output(b), &ADA_ASSET_CLASS, 1)
        })
        .expect("taking less is fine");
    }

    #[test]
    fn the_asked_asset_can_never_be_taken() {
        // The token-offer fill asks for ADA; draining ADA instead of
        // depositing it reverses the swap.
        let err = probe(FILL_TOKEN_OFFER, |b| {
            bump(swap_output(b), &ADA_ASSET_CLASS, -1_500_001)
        })
        .expect_err("reversing the swap must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("ask asset cannot be taken") || msg.contains("offer_taken"),
            "{msg}"
        );
    }

    #[test]
    fn extraneous_assets_are_rejected() {
        let err = probe(FILL_ADA_OFFER, |b| bump(swap_output(b), &usdcx(), 1))
            .expect_err("a third asset must fail");
        assert!(format!("{err:#}").contains("extraneous"), "{err:#}");
    }

    #[test]
    fn the_continuing_datum_must_name_the_spent_input() {
        let err = probe(FILL_ADA_OFFER, |b| {
            let out = swap_output(b);
            let conway::TransactionOutput::PostAlonzo(p) = out else {
                panic!()
            };
            let Some(conway::PseudoDatumOption::Data(d)) = &p.datum_option else {
                panic!()
            };
            let mut parsed = SwapDatum::parse(&d.0).expect("datum parses");
            // Point prev_input at something else.
            parsed.prev_input = Some(TransactionInput::new([0u8; 32].into(), 0));
            p.datum_option = Some(conway::PseudoDatumOption::Data(CborWrap(
                parsed.to_plutus(),
            )));
        })
        .expect_err("a wrong prev_input must fail");
        assert!(
            format!("{err:#}").contains("Corresponding swap output not found"),
            "{err:#}"
        );
    }

    /// Our own re-encoding of the datum must be byte-identical to the one
    /// the chain accepted — the validator compares datums with `==`, so a
    /// re-encoding that differs anywhere would never find its output.
    #[test]
    fn our_datum_encoding_is_accepted_by_the_validator() {
        probe(FILL_ADA_OFFER, |b| {
            let out = swap_output(b);
            let conway::TransactionOutput::PostAlonzo(p) = out else {
                panic!()
            };
            let Some(conway::PseudoDatumOption::Data(d)) = &p.datum_option else {
                panic!()
            };
            let parsed = SwapDatum::parse(&d.0).expect("datum parses");
            p.datum_option = Some(conway::PseudoDatumOption::Data(CborWrap(
                parsed.to_plutus(),
            )));
        })
        .expect("a datum we re-encoded ourselves still matches");
    }

    #[test]
    fn mainnet_orders_parse_and_their_beacons_check_out() {
        let utxos = utxo_set();
        let swap = hex::decode(SWAP_HASH).unwrap();
        let policy = hex::decode(BEACON_POLICY).unwrap();
        let mut seen = 0;
        for (k, out) in &utxos {
            if out.address.to_vec().get(1..29) != Some(&swap[..]) {
                continue;
            }
            let order = SwapOrder::from_output(k.clone(), out).expect("order parses");
            assert!(
                order.datum.beacons_are_consistent(&policy),
                "{k}: derived beacons disagree with the datum"
            );
            assert!(order.holds_its_beacons(), "{k}: UTxO lacks its own beacons");
            assert_eq!(
                order.datum.to_plutus(),
                match &out.datum {
                    RawDatum::Inline(d) => d.clone(),
                    _ => unreachable!(),
                },
                "{k}: re-encoding must be byte-identical"
            );
            seen += 1;
        }
        assert!(
            seen >= 3,
            "fixtures hold both orders and their continuations"
        );
    }

    /// The edges we would offer for the fixtures' orders, and the pieces we
    /// would build, reproduce the real fills.
    #[test]
    fn edges_and_pieces_reproduce_the_real_fills() {
        let utxos = utxo_set();
        let swap = hex::decode(SWAP_HASH).unwrap();
        let rt = CardanoSwapsRuntime {
            swap_hash: swap.clone(),
            swap_ref: TransactionInput::new([1u8; 32].into(), 0),
            beacon_policy: hex::decode(BEACON_POLICY).unwrap(),
            min_keep_lovelace: 2_780_000,
            foreign_fills: None,
        };
        // The two orders the real fills consumed.
        let mut state = CardanoSwapsState::default();
        for (k, out) in &utxos {
            if out.address.to_vec().get(1..29) != Some(&swap[..]) {
                continue;
            }
            if let Some(o) = SwapOrder::from_output(k.clone(), out)
                && o.datum.prev_input.is_none()
            {
                state.orders.insert(k.clone(), o);
            }
        }
        let (edges, legs) = rt.edges(&state, 0);
        assert!(edges.len() >= 2, "both directions are routable: {edges:?}");
        let ctx = FillContext {
            runtime: &rt,
            legs: &legs,
            slot_config: mainnet_slots(),
        };

        // ADA-offer order (price 23/100): deposit 230 USDM, take 1000 ADA.
        let ada_key = edges
            .iter()
            .find(|e| e.to == ADA_ASSET_CLASS && e.from == usdm())
            .map(|e| e.key.clone())
            .expect("an ADA-offering edge");
        let pieces = ctx.fill_pieces(&ada_key, 230_000_000, 1_000_000_000).expect("composes");
        let conway::TransactionOutput::PostAlonzo(p) = &pieces.continuing_output else {
            panic!()
        };
        let got = crate::sundaev4::tx_builder::conway_value_to_internal(&p.value);
        assert_eq!(got.get(&ADA_ASSET_CLASS), BigInt::from(2_788_570i64));
        assert_eq!(got.get(&usdm()), BigInt::from(230_000_000i64));

        // Token-offer order (price 3/2): deposit 1.5 ADA, take 1 USDCx.
        let tok_key = edges
            .iter()
            .find(|e| e.to == usdcx() && e.from == ADA_ASSET_CLASS)
            .map(|e| e.key.clone())
            .expect("a USDCx-offering edge");
        let pieces = ctx.fill_pieces(&tok_key, 1_500_000, 1_000_000).expect("composes");
        let conway::TransactionOutput::PostAlonzo(p) = &pieces.continuing_output else {
            panic!()
        };
        let got = crate::sundaev4::tx_builder::conway_value_to_internal(&p.value);
        assert_eq!(got.get(&ADA_ASSET_CLASS), BigInt::from(4_258_400i64));
        assert_eq!(got.get(&usdcx()), BigInt::from(2_000_000i64));

        // Taking more than the price allows is refused before we build.
        assert!(ctx.fill_pieces(&tok_key, 1_500_000, 1_000_001).is_err());
    }
}
