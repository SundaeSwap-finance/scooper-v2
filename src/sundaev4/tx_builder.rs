//! Builds a V4 scoop transaction using pallas 0.34 primitives.

use anyhow::{Context, Result, bail};
use pallas_addresses::{Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart};
use pallas_crypto::hash::Hasher;
use pallas_crypto::key::ed25519::{PublicKey, SecretKey, SecretKeyExtended, Signature};
use pallas_codec::utils::CborWrap;
use pallas_primitives::conway::{
    self, RedeemersKey, RedeemersValue, Redeemers, RedeemerTag, TransactionOutput, WitnessSet,
    VKeyWitness,
};
use pallas_primitives::{ExUnits, Hash, PositiveCoin, TransactionInput};
use plutus_parser::AsPlutus;

use crate::bigint::BigInt;
use crate::cardano_types::AssetClass;
use crate::sundaev3::{Credential, PlutusAddress, Referenced};
use crate::sundaev4::batch::{Batch, GlobalOp, ScoopPlan};
use crate::sundaev4::swap_math;
use crate::sundaev4::types::*;

type PallasBytes = pallas_primitives::Bytes;
type ConwayValue = conway::Value;

use std::collections::BTreeMap;
use crate::sundaev4::script_context::{ResolvedTxOut, DatumOption};

// Placeholder fee used for the first-pass build (before we know the real
// ex_units and tx_size). The scooper computes the exact required fee from
// `compute_tx_fee` after eval and passes it to the rebuild via the
// `fee_override` parameter, so this value never lands on chain.
//
// IMPORTANT: keep this CLOSE TO THE MINIMUM realistic fee, not an upper
// bound. The order validator's contract check is `each order's budget * n
// >= fee`, evaluated against THIS placeholder during first-pass eval. If
// the placeholder is too pessimistic, orders whose `budget * n` could
// satisfy the real fee fail the eval and abort the cycle. The minimum
// realistic scoop fee is ~1.5 ADA (1 pool, 1 order, current tracing-on
// contracts). Setting placeholder = 1.5 ADA means every order with budget
// >= 1.5 ADA passes any first-pass eval; the rebuild's higher fee_override
// is still checked against `budget * n` in the on-chain re-eval, which
// scales with n so it stays satisfied.
pub const TX_FEE: u64 = 1_500_000;

// Upper bound on the *real* fee any single scoop tx can have under our
// current cost model — used to size collateral selection so the
// collateral_return output stays above min_utxo even when the rebuild
// writes a fee much larger than `TX_FEE`. Observed real fees range
// 1.5–4M lovelace; 6M leaves comfortable headroom.
pub const MAX_REAL_TX_FEE: u64 = 6_000_000;

// Cardano protocol fee coefficients (preview/mainnet, stable for years).
// TODO: pull from `cardano.protocol.parameters` via Acropolis instead of
// hardcoding — same subscription planned for the cap-limit work.
const TX_FEE_PER_BYTE: u64 = 44;
const TX_FEE_FIXED: u64 = 155_381;
const PRICE_MEM_NUM: u64 = 577;
const PRICE_MEM_DEN: u64 = 10_000;
const PRICE_STEP_NUM: u64 = 721;
const PRICE_STEP_DEN: u64 = 10_000_000;

/// Conway-era reference-script fee parameters (preview/mainnet).
/// `min_fee_ref_script_cost_per_byte = 15`; tiered with a 6/5 growth factor
/// every `REF_SCRIPT_TIER_SIZE` bytes.
const REF_SCRIPT_BASE_FEE: u128 = 15;
const REF_SCRIPT_TIER_SIZE: u128 = 25_600;
const REF_SCRIPT_MUL_NUM: u128 = 6;
const REF_SCRIPT_MUL_DEN: u128 = 5;

/// Conway tiered reference-script fee.
///
/// Each `TIER_SIZE` bytes costs more than the previous tier:
///   tier 0: BASE per byte
///   tier 1: BASE * 6/5 per byte
///   tier 2: BASE * (6/5)^2 per byte
///   ...
/// Final result is `floor(total)`. Implemented with u128 rational arithmetic
/// (common denominator = `5^(n_tiers-1)`) to avoid float precision drift.
pub fn compute_ref_script_fee(total_ref_script_bytes: u64) -> u64 {
    if total_ref_script_bytes == 0 {
        return 0;
    }
    let mut chunks: Vec<u128> = Vec::new();
    let mut remaining = total_ref_script_bytes as u128;
    while remaining > 0 {
        let chunk = remaining.min(REF_SCRIPT_TIER_SIZE);
        chunks.push(chunk);
        remaining -= chunk;
    }
    let n = chunks.len() as u32;
    // sum_i chunks[i] * BASE * (6^i / 5^i)
    // = sum_i chunks[i] * BASE * 6^i * 5^(n-1-i)  / 5^(n-1)
    let mut total_num: u128 = 0;
    for (i, &chunk) in chunks.iter().enumerate() {
        let i = i as u32;
        let six_pow = REF_SCRIPT_MUL_NUM.pow(i);
        let five_pow_rem = REF_SCRIPT_MUL_DEN.pow(n - 1 - i);
        total_num += chunk * REF_SCRIPT_BASE_FEE * six_pow * five_pow_rem;
    }
    let total_den = REF_SCRIPT_MUL_DEN.pow(n - 1);
    (total_num / total_den) as u64
}

/// Compute the minimum protocol fee for a tx with the given size, total
/// ex_units, and total reference-script bytes, per the Conway fee formula:
///   fee = a*size + b
///       + ceil(priceMem*mem) + ceil(priceStep*cpu)
///       + tiered_ref_script_fee(ref_bytes)
pub fn compute_tx_fee(
    tx_size: u64,
    total_mem: u64,
    total_steps: u64,
    total_ref_script_bytes: u64,
) -> u64 {
    let size_fee = TX_FEE_PER_BYTE * tx_size + TX_FEE_FIXED;
    let mem_fee = (PRICE_MEM_NUM * total_mem).div_ceil(PRICE_MEM_DEN);
    let step_fee = (PRICE_STEP_NUM * total_steps).div_ceil(PRICE_STEP_DEN);
    let ref_fee = compute_ref_script_fee(total_ref_script_bytes);
    size_fee + mem_fee + step_fee + ref_fee
}
const POOL_MIN_ADA: u64 = 2_000_000;
pub const VALIDITY_RANGE: u64 = 180;

// Conway-era coinsPerUtxoByte (preview/mainnet, stable). Used to compute
// minUtxo for outputs whose datum size varies — e.g. pools post-governance
// upgrades that extend module_state or actions.
// TODO: pull from protocol parameters once Acropolis is wired.
const COINS_PER_UTXO_BYTE: u64 = 4310;

// Per Conway ledger spec: minUtxo = (160 + serializedOutputBytes) * coinsPerUtxoByte.
// The 160-byte constant covers the output's UTxO entry overhead (txid+ix on the
// reference side, plus header bytes).
fn compute_output_min_ada(output: &TransactionOutput) -> Result<u64> {
    let mut buf = Vec::new();
    minicbor::encode(output, &mut buf)
        .map_err(|e| anyhow::anyhow!("encode output for min_ada: {e}"))?;
    Ok((160u64 + buf.len() as u64) * COINS_PER_UTXO_BYTE)
}

// Floor for wallet-change outputs (ADA-only). 1 ADA comfortably exceeds
// Conway minUtxo (~858K lovelace) for an ADA-only vkey output.
const SCOOPER_CHANGE_MIN_ADA: u64 = 1_000_000;

use crate::sundaev3::Ident;

/// Result of building a multi-pool scoop transaction.
pub struct MultiPoolBuildResult {
    pub cbor: Vec<u8>,
    pub tx_hash: Hash<32>,
    pub tx_hash_hex: String,
    /// Total byte size of all reference scripts attached to this tx — used to
    /// compute the Conway-era ref-script fee component on the rebuild pass.
    pub total_ref_script_bytes: u64,
    pub tx_body: conway::PseudoTransactionBody<TransactionOutput>,
    pub resolved_inputs: BTreeMap<crate::cardano_types::TransactionInput, ResolvedTxOut>,
    pub resolved_ref_inputs: BTreeMap<crate::cardano_types::TransactionInput, ResolvedTxOut>,
    pub redeemers: Vec<(RedeemersKey, pallas_primitives::PlutusData, ExUnits)>,
    /// Predicted pool UTxOs after this tx settles (one per pool)
    pub predicted_pools: Vec<(Ident, crate::cardano_types::TransactionInput, SundaeV4Pool)>,
    /// The TTL used for this transaction
    pub ttl: u64,
    /// The scooper change output (index, value) when a funding UTxO was
    /// spent — the wallet UTxO this tx predicts into existence. Lets the
    /// next chained tx fund itself off this one instead of double-spending
    /// the confirmed funding UTxO an in-flight tx already consumed.
    pub wallet_change: Option<(u64, crate::cardano_types::Value)>,
}

/// Build a signed scoop transaction for M pools + N orders.
///
/// Generalizes the single-pool builder:
/// - Inputs: M pool inputs + N order inputs (all sorted together)
/// - Outputs: M pool outputs (indices 0..M-1), N fulfillment outputs (M..M+N-1)
/// - M vault spend redeemers (each with its own transcript)
/// - M CP/FS/fairness entries (one per pool)
/// - Fee split: TX_FEE / N total orders
pub fn build_multi_pool_scoop_tx(
    plan: &ScoopPlan,
    settings: &SundaeV4Settings,
    exec: &ScooperExecution,
    current_slot: u64,
    language_views: &[u8],
    collateral_utxo: &TransactionInput,
    collateral_value: &crate::cardano_types::Value,
    ex_units: Option<&[(RedeemersKey, ExUnits)]>,
    ref_utxo_outputs: &BTreeMap<crate::cardano_types::TransactionInput, crate::cardano_types::TransactionOutput>,
    fee_override: Option<u64>,
    // `order_configs`: OrderConfig settings entries keyed by token name —
    // resolves each order's `config_token` to its `required_constraints`
    // set so the right constraint-module withdrawals can be added (PR #11).
    order_configs: &BTreeMap<Vec<u8>, std::sync::Arc<crate::sundaev4::SundaeV4OrderConfig>>,
    // SignedStrategyExecutions (as decoded PlutusData) keyed by the order
    // input they authorize. Every order in the plan whose datum carries the
    // strategy_order constraint must have an entry; the strategy_order
    // withdrawal redeemer is the list of these in canonical input order.
    strategy_executions: &BTreeMap<crate::cardano_types::TransactionInput, pallas_primitives::PlutusData>,
    // Optional wallet UTxO that funds any min-ada gap on pool outputs (datum
    // growth from upgrades can push pool outputs above their current ada
    // buffer). When present, the input is included and net excess flows back
    // as a scooper change output. When None, build fails if any pool actually
    // needs a bump — so pass Some whenever the scooper has a suitable UTxO,
    // and None only as a "no bump expected" hint.
    funding_input: Option<(TransactionInput, &crate::cardano_types::Value)>,
    // Loaded Butane runtime — required iff plan.conversions contains
    // butane legs. None + butane legs = build error (the router must not
    // offer edges the builder can't compose; this is the backstop).
    butane: Option<&crate::sundaev4::butane::ButaneRuntime>,
) -> Result<MultiPoolBuildResult> {
    // First-pass builds use the TX_FEE upper bound; the rebuild passes the
    // exact fee computed from `compute_tx_fee(size, mem, cpu)`.
    let tx_fee = fee_override.unwrap_or(TX_FEE);
    let batches: &[Batch] = &plan.batches;
    let routes = &plan.routes;
    let m_pools = batches.len();
    let n_swap_orders: usize = batches.iter().map(|b| b.swaps.len()).sum();
    let n_deposit_orders: usize = batches.iter().map(|b| b.deposits.len()).sum();
    let n_withdraw_orders: usize = batches.iter().map(|b| b.withdraws.len()).sum();
    let n_claim_orders: usize = batches.iter().map(|b| b.claims.len()).sum();
    let n_conversion_orders: usize =
        plan.conversions.iter().filter(|c| c.primary).count();
    let n_orders: usize = n_swap_orders
        + n_deposit_orders
        + n_withdraw_orders
        + n_claim_orders
        + n_conversion_orders;
    // Pure-conversion scoops (ADA→ADAb mint orders) have zero pool batches:
    // the tx is order spend + mechanism pieces + fulfillment, no transcripts.
    if n_orders == 0 || (m_pools == 0 && plan.conversions.is_empty()) {
        bail!("no orders (or no batches and no conversions)");
    }

    // Synthesize a global_seq if the caller passed an empty one (legacy paths
    // that pass `&[Batch]` directly via test_harness wrap them in an empty
    // ScoopPlan). For non-routed batches, ordering doesn't cross pools, so
    // any consistent enumeration works.
    let synthesized_seq: Vec<GlobalOp>;
    let global_seq: &[GlobalOp] = if plan.global_seq.is_empty() {
        synthesized_seq = batches.iter().enumerate().flat_map(|(bi, b)| {
            (0..b.ops_order.len()).map(move |oi| GlobalOp { batch_idx: bi, op_idx: oi })
        }).collect();
        &synthesized_seq
    } else {
        &plan.global_seq
    };

    // ── Butane conversion legs → deposit pieces ────────────────────────────
    let butane_pieces: Vec<crate::sundaev4::butane::DepositPieces> = {
        use num_traits::ToPrimitive;
        let mut pieces = Vec::new();
        for leg in &plan.conversions {
            let Some(name) = leg
                .key
                .strip_prefix("butane:")
                .and_then(|s| s.strip_suffix(":mint"))
            else {
                bail!("unknown conversion mechanism for leg {}", leg.key);
            };
            let rt = butane.with_context(|| {
                format!("plan contains butane leg {} but no runtime is loaded", leg.key)
            })?;
            let dx = leg.dx.clone().unwrap().to_u64()
                .context("conversion dx doesn't fit u64")?;
            let out = leg.out.clone().unwrap().to_u64()
                .context("conversion out doesn't fit u64")?;
            // Network id from the scooper address config? Pot addresses are
            // testnet on preview; derive from settings address network bit.
            pieces.push(rt.deposit_pieces(name, dx, out, 0)?);
        }
        pieces
    };

    let sk = parse_secret_key(&exec.scooper_secret_key)?;
    let pk = sk.public_key();
    let pk_bytes: [u8; 32] = pk.as_ref().try_into().unwrap();
    let scooper_keyhash: Hash<28> = Hasher::<224>::hash(&pk_bytes);

    // ── Step 1: Streaming walk → per-pool transcripts + per-route outputs ──
    //
    // We walk `global_seq` in order, applying each op to its pool's running
    // state, computing dy at tx-time, and threading routed-order dy through
    // hops via per-route `RouteState`. This is the "current_inputs /
    // current_pool_state / current_outputs" model: pool state evolves per-op,
    // routed-order hop inputs cascade from previous hops' actual dy, and
    // routed-order final fulfillment amounts accumulate into `final_output`.
    //
    // Why this matters for CL: per-entry protocol_lp distribution bumps
    // `lp_before` for subsequent entries, which changes the validator's
    // expected dy. Recomputing here against the *bumped* running_total_lp
    // gives the validator-tight dy; for routed orders this dy propagates
    // through subsequent hops via the route's hop_input tracking, so the
    // user's fulfillment matches the actual cascade output.

    let void_pool_state = PoolState {
        assets: vec![],
        total_lp: BigInt::from(0),
        circulating_lp: BigInt::from(0),
        preminted_lp: BigInt::from(0),
    };

    struct PerPoolData {
        transcript: Vec<TranscriptEntry>,
        updated_datum: PoolDatum,
        /// Sum of LP minted by all deposits in this batch. Zero for swap-only
        /// batches. Used to drive the pool_mint policy's LP mint entry below.
        lp_minted: BigInt,
        /// Sum of LP burned by all withdraws in this batch. Zero unless there
        /// are withdraws. The pool_mint entry uses `lp_minted - lp_burned`.
        lp_burned: BigInt,
        /// Effective dy for each swap in this batch (used for direct, non-
        /// routed fulfillment). Routed-order fulfillment reads from
        /// `route_states[route_idx].final_output` instead.
        effective_swap_dys: Vec<BigInt>,
        /// Pool reserves at end of the transcript loop.
        final_assets_actual: Vec<(AssetClass, BigInt)>,
    }

    /// Per-route state threaded through the streaming walk.
    struct RouteState {
        /// Incoming amount for hop k (= sum of hop k-1's actual dys across
        /// all splits, except hop 0 which starts at the user's offered amount).
        hop_input: Vec<BigInt>,
        /// Tracks how much of `hop_input[k]` we've already allocated to splits
        /// 0..split_idx, so the last split absorbs the integer-division
        /// remainder.
        hop_allocated: Vec<BigInt>,
        /// Accumulated final-hop dy — what the user's fulfillment output pays.
        final_output: BigInt,
        final_output_asset: AssetClass,
    }

    // Initialize per-pool state.
    let mut per_pool_running_assets: Vec<Vec<(AssetClass, BigInt)>> = batches.iter()
        .map(|b| b.pool.pool_datum.assets.clone())
        .collect();
    let mut per_pool_running_total_lp: Vec<BigInt> = batches.iter()
        .map(|b| b.pool.pool_datum.total_lp.clone())
        .collect();
    let mut per_pool_running_circ_lp: Vec<BigInt> = batches.iter()
        .map(|b| b.pool.pool_datum.circulating_lp.clone())
        .collect();
    let mut per_pool_lp_minted: Vec<BigInt> = vec![BigInt::from(0); m_pools];
    let mut per_pool_lp_burned: Vec<BigInt> = vec![BigInt::from(0); m_pools];
    let mut per_pool_cum_gross_fb: Vec<BigInt> = vec![BigInt::from(0); m_pools];
    let mut per_pool_cum_protocol_lp: Vec<BigInt> = vec![BigInt::from(0); m_pools];
    let mut per_pool_transcripts: Vec<Vec<TranscriptEntry>> = vec![Vec::new(); m_pools];
    let mut per_pool_effective_swap_dys: Vec<Vec<BigInt>> = batches.iter()
        .map(|b| b.swaps.iter().map(|s| s.dy.clone()).collect())
        .collect();

    // Per-pool fee_split protocol_share. fee_split.Operate runs once per pool
    // and checks the cumulative protocol_lp captured across the transcript
    // matches `floor(total_fee * ps_num / ps_den)`. Using a global default
    // would produce the wrong protocol_lp for any pool with a non-default
    // share.
    //
    // CS pools now capture protocol revenue normally (post-SUN-101: the
    // `before_lp == after_lp` invariant was removed from cs_check's swap
    // path, so total_lp can grow each step like CP/CL).
    let per_pool_ps: Vec<(BigInt, BigInt)> = batches.iter().map(|batch| {
        batch.pool.fee_split_config.as_ref()
            .map(|c| (c.protocol_share.num.clone(), c.protocol_share.den.clone()))
            .unwrap_or_else(|| (
                BigInt::from(exec.protocol_share.0),
                BigInt::from(exec.protocol_share.1),
            ))
    }).collect();

    // Per-pool operation_tag for swap entries. CS dispatches on tag==3
    // (`tag_swap` in cs_check.ak); CP/CL infer from asset deltas.
    let per_pool_swap_tag: Vec<BigInt> = batches.iter().map(|b| match &b.pool.pool_type {
        PoolType::ConstantSum { .. } => BigInt::from(3),
        PoolType::ConstantProduct { .. } => BigInt::from(100),
        PoolType::ConcentratedLiquidity { .. } => BigInt::from(100),
    }).collect();

    // Initialize per-route state. hop_input[0] = order's offered amount
    // (entry-hop incoming). Subsequent hops start at 0 and accumulate dys.
    let mut route_states: Vec<RouteState> = routes.iter().map(|r| {
        let n_hops = r.hops.len();
        let mut hop_input = vec![BigInt::from(0); n_hops];
        hop_input[0] = r.order.swap_offered().1.clone();
        RouteState {
            hop_input,
            hop_allocated: vec![BigInt::from(0); n_hops],
            final_output: BigInt::from(0),
            final_output_asset: r.final_output_asset.clone(),
        }
    }).collect();

    // Conversion legs are invisible to the pool-op walk below; credit their
    // outputs to the route flow upfront (next hop's incoming, or the final
    // output when the conversion ends the route).
    for c in &plan.conversions {
        let rs = &mut route_states[c.route_idx];
        if c.hop_idx + 1 < rs.hop_input.len() {
            rs.hop_input[c.hop_idx + 1] = &rs.hop_input[c.hop_idx + 1] + &c.out;
        } else {
            rs.final_output = &rs.final_output + &c.out;
        }
    }

    // Diagnostic: dump initial pool state so the next ValueNotConservedUTxO can be
    // reconstructed offline. Keyed by short pool ident prefix.
    for (i, b) in batches.iter().enumerate() {
        tracing::debug!(
            walk = "init-pool",
            batch_idx = i,
            pool = %b.pool_ident,
            assets = ?b.pool.pool_datum.assets.iter().map(|(a, q)| {
                format!("{}={}", short_asset(a), q)
            }).collect::<Vec<_>>(),
            total_lp = %b.pool.pool_datum.total_lp,
            "streaming walk: initial pool state",
        );
    }
    for (r_idx, r) in routes.iter().enumerate() {
        tracing::debug!(
            walk = "init-route",
            route_idx = r_idx,
            order = %r.order.input,
            n_hops = r.hops.len(),
            offered = %r.order.swap_offered().1,
            final_output_asset = %short_asset(&r.final_output_asset),
            "streaming walk: route init",
        );
    }

    for g in global_seq {
        let batch_idx = g.batch_idx;
        let op_idx = g.op_idx;
        let batch = &batches[batch_idx];
        let op = &batch.ops_order[op_idx];
        let pool_type = batch.pool.pool_type.clone();

        let running_assets = &mut per_pool_running_assets[batch_idx];
        let running_total_lp = &mut per_pool_running_total_lp[batch_idx];
        let running_circ_lp = &mut per_pool_running_circ_lp[batch_idx];

        let prev_assets = running_assets.clone();

        // Claims carry a BountyClaim as operation_data; everything else
        // uses the void placeholder.
        let mut op_data_override: Option<pallas_primitives::PlutusData> = None;
        let (operation_tag, gross_fb) = match op {
            crate::sundaev4::batch::BatchOp::Claim(i) => {
                // Waived-mode CS bounty claim (cs_check tag 5): the reserve
                // vector moves by the resolved deltas (a pair-wise swap or a
                // multi-receive rebalance — same shape either way), with the
                // bounty named in operation_data; no fee retained, LP
                // untouched.
                let c = &batch.claims[*i];
                for (idx, delta) in c.pool_deltas.iter().enumerate() {
                    running_assets[idx].1 = &running_assets[idx].1 + delta;
                }
                op_data_override = Some(
                    crate::sundaev4::types::BountyClaim {
                        asset: batch.pool.pool_datum.assets[c.claim_idx].0.clone(),
                        amount: c.claim.clone(),
                    }
                    .to_plutus(),
                );
                tracing::info!(
                    walk = "op-claim",
                    batch_idx,
                    pool = %batch.pool_ident,
                    deltas = ?c.pool_deltas.iter().map(|d| d.to_string()).collect::<Vec<_>>(),
                    claim = %c.claim,
                    "streaming walk: claim op",
                );
                (BigInt::from(crate::sundaev4::types::TAG_CLAIM), BigInt::from(0))
            }
            crate::sundaev4::batch::BatchOp::Swap(i) => {
                let s = &batch.swaps[*i];
                // dx is fixed by the order (direct) or by the route's entry
                // split allocation (routed primary); both stored on `s.dx`.
                let dx = s.dx.clone();
                // dy: recompute fresh against the current pool state
                // (including any LP bumps from prior CL entries).
                let dy = crate::sundaev4::batch::compute_swap_result(
                    &pool_type, running_assets, running_total_lp,
                    s.input_idx, s.output_idx, &dx,
                );
                per_pool_effective_swap_dys[batch_idx][*i] = dy.clone();
                running_assets[s.input_idx].1 = &running_assets[s.input_idx].1 + &dx;
                running_assets[s.output_idx].1 = &running_assets[s.output_idx].1 - &dy;
                tracing::debug!(
                    walk = "op-swap",
                    batch_idx,
                    pool = %batch.pool_ident,
                    swap_idx = *i,
                    route = ?s.route.as_ref().map(|r| (r.route_idx, r.hop_idx, r.split_idx)),
                    dx = %dx, dy = %dy,
                    "streaming walk: swap op",
                );
                // If this is a routed-primary, thread dy → next hop's incoming
                // (or final_output if last hop).
                if let Some(rref) = &s.route {
                    let rs = &mut route_states[rref.route_idx];
                    let n_hops = rs.hop_input.len();
                    if rref.hop_idx + 1 < n_hops {
                        rs.hop_input[rref.hop_idx + 1] = &rs.hop_input[rref.hop_idx + 1] + &dy;
                    } else {
                        rs.final_output = &rs.final_output + &dy;
                    }
                }
                let fb = swap_math::compute_fee_budget(
                    &pool_type, &prev_assets, running_assets, running_total_lp,
                );
                (per_pool_swap_tag[batch_idx].clone(), fb)
            }
            crate::sundaev4::batch::BatchOp::Continuation(i) => {
                let c = &batch.continuations[*i];
                let rref = &c.route;
                let route = &routes[rref.route_idx];
                let hop = &route.hops[rref.hop_idx];
                let split_count = hop.split_input_props.len();
                // Entry-hop continuations: dx is the router's allocation
                // (stored on `c.dx` = `split.input_amount`). Later hops:
                // rescale against the actual incoming flow tracked in
                // route_state.hop_input[hop_idx]; the last split absorbs the
                // integer-division remainder so value is conserved exactly.
                let dx = if rref.hop_idx == 0 {
                    c.dx.clone()
                } else {
                    let rs = &mut route_states[rref.route_idx];
                    let incoming = rs.hop_input[rref.hop_idx].clone();
                    if split_count == 1 {
                        incoming
                    } else if rref.split_idx == split_count - 1 {
                        &incoming - &rs.hop_allocated[rref.hop_idx]
                    } else if num_traits::Signed::is_positive(&hop.hop_total_at_route_time) {
                        let proportional =
                            &incoming * &c.dx / &hop.hop_total_at_route_time;
                        rs.hop_allocated[rref.hop_idx] =
                            &rs.hop_allocated[rref.hop_idx] + &proportional;
                        proportional
                    } else {
                        c.dx.clone()
                    }
                };
                let dy = crate::sundaev4::batch::compute_swap_result(
                    &pool_type, running_assets, running_total_lp,
                    c.input_idx, c.output_idx, &dx,
                );
                running_assets[c.input_idx].1 = &running_assets[c.input_idx].1 + &dx;
                running_assets[c.output_idx].1 = &running_assets[c.output_idx].1 - &dy;
                tracing::debug!(
                    walk = "op-cont",
                    batch_idx,
                    pool = %batch.pool_ident,
                    cont_idx = *i,
                    route_idx = rref.route_idx,
                    hop_idx = rref.hop_idx,
                    split_idx = rref.split_idx,
                    dx = %dx, dy = %dy,
                    "streaming walk: continuation op",
                );
                let rs = &mut route_states[rref.route_idx];
                let n_hops = rs.hop_input.len();
                if rref.hop_idx + 1 < n_hops {
                    rs.hop_input[rref.hop_idx + 1] = &rs.hop_input[rref.hop_idx + 1] + &dy;
                } else {
                    rs.final_output = &rs.final_output + &dy;
                }
                let fb = swap_math::compute_fee_budget(
                    &pool_type, &prev_assets, running_assets, running_total_lp,
                );
                (per_pool_swap_tag[batch_idx].clone(), fb)
            }
            crate::sundaev4::batch::BatchOp::Deposit(i) => {
                let d = &batch.deposits[*i];
                for (idx, amt) in running_assets.iter_mut().enumerate() {
                    amt.1 = &amt.1 + &d.dx[idx];
                }
                *running_total_lp = &*running_total_lp + &d.lp_minted;
                *running_circ_lp = &*running_circ_lp + &d.lp_minted;
                per_pool_lp_minted[batch_idx] =
                    &per_pool_lp_minted[batch_idx] + &d.lp_minted;
                let dep_tag = match &pool_type {
                    PoolType::ConstantSum { .. } => BigInt::from(6),
                    PoolType::ConstantProduct { .. } => BigInt::from(100),
                    PoolType::ConcentratedLiquidity { .. } => BigInt::from(100),
                };
                (dep_tag, BigInt::from(0))
            }
            crate::sundaev4::batch::BatchOp::Withdraw(i) => {
                let w = &batch.withdraws[*i];
                for (idx, amt) in running_assets.iter_mut().enumerate() {
                    amt.1 = &amt.1 - &w.dy[idx];
                }
                *running_total_lp = &*running_total_lp - &w.lp_burned;
                *running_circ_lp = &*running_circ_lp - &w.lp_burned;
                per_pool_lp_burned[batch_idx] =
                    &per_pool_lp_burned[batch_idx] + &w.lp_burned;
                // CS pools dispatch per-tag (cs_check.ak: tag_swap=3,
                // tag_withdraw=4, tag_claim=5, tag_deposit=6). CP/CL infer
                // from asset deltas, so any sentinel tag works.
                let wd_tag = match &pool_type {
                    PoolType::ConstantProduct { .. } => BigInt::from(100),
                    PoolType::ConcentratedLiquidity { .. } => BigInt::from(100),
                    PoolType::ConstantSum { .. } => {
                        BigInt::from(crate::sundaev4::types::TAG_WITHDRAW)
                    }
                };
                (wd_tag, BigInt::from(0))
            }
        };

        // Per-entry protocol_lp share. fee_split.Operate's check is
        // *cumulative* per pool: protocol_lp = floor(total_gross * ps_num /
        // ps_den) where total_gross = sum of every entry's gross fee. A naive
        // per-entry floor (`floor(g_i * ps/...)`) loses rounding remainders
        // and the sum falls short of the cumulative floor. Instead, advance a
        // cumulative-floor target and let each entry's contribution be the
        // delta; the rounding "carry" naturally lands on whichever entry tips
        // the running product across the next ps_den boundary.
        // Pools with ps=(0, *) (= 0/N) leave LP untouched: target stays at
        // 0 and every entry contributes 0.
        let (ps_num_bi, ps_den_bi) = &per_pool_ps[batch_idx];
        let new_cum_gross_fb = &per_pool_cum_gross_fb[batch_idx] + &gross_fb;
        let new_cum_protocol_lp = &new_cum_gross_fb * ps_num_bi / ps_den_bi;
        let op_protocol_lp =
            &new_cum_protocol_lp - &per_pool_cum_protocol_lp[batch_idx];
        let submitted_fee_budget = &gross_fb - &op_protocol_lp;
        per_pool_cum_protocol_lp[batch_idx] = new_cum_protocol_lp;
        per_pool_cum_gross_fb[batch_idx] = new_cum_gross_fb;
        *running_total_lp = &*running_total_lp + &op_protocol_lp;

        per_pool_transcripts[batch_idx].push(TranscriptEntry {
            state_after: PoolState {
                assets: running_assets.clone(),
                total_lp: running_total_lp.clone(),
                circulating_lp: running_circ_lp.clone(),
                preminted_lp: batch.pool.pool_datum.preminted_lp.clone(),
            },
            fee_budget: submitted_fee_budget,
            operation_tag,
            operation_data: op_data_override
                .unwrap_or_else(|| void_pool_state.clone().to_plutus()),
        });
    }

    // End-of-walk summary: per-pool final state + per-route final_output.
    for (i, b) in batches.iter().enumerate() {
        let deltas: Vec<String> = per_pool_running_assets[i].iter()
            .zip(b.pool.pool_datum.assets.iter())
            .map(|((a, post), (_, pre))| {
                let delta = post - pre;
                format!("{}={:+}", short_asset(a), delta)
            }).collect();
        tracing::info!(
            walk = "final-pool",
            batch_idx = i,
            pool = %b.pool_ident,
            deltas = ?deltas,
            total_lp_delta = %(&per_pool_running_total_lp[i] - &b.pool.pool_datum.total_lp),
            "streaming walk: final pool state",
        );
    }
    for (r_idx, rs) in route_states.iter().enumerate() {
        tracing::info!(
            walk = "final-route",
            route_idx = r_idx,
            hop_input = ?rs.hop_input.iter().map(|v| v.to_string()).collect::<Vec<_>>(),
            final_output = %rs.final_output,
            final_output_asset = %short_asset(&rs.final_output_asset),
            "streaming walk: final route state",
        );
    }

    // Materialise PerPoolData from the running state.
    let mut per_pool: Vec<PerPoolData> = Vec::with_capacity(m_pools);
    for (i, batch) in batches.iter().enumerate() {
        let pool = &batch.pool;
        let final_total_lp = per_pool_running_total_lp[i].clone();
        let final_circ_lp = &pool.pool_datum.circulating_lp
            + &per_pool_lp_minted[i]
            - &per_pool_lp_burned[i];
        let final_assets_actual = per_pool_running_assets[i].clone();

        let updated_datum = PoolDatum {
            assets: final_assets_actual.clone(),
            total_lp: final_total_lp,
            circulating_lp: final_circ_lp,
            preminted_lp: pool.pool_datum.preminted_lp.clone(),
            identifier: pool.pool_datum.identifier.clone(),
            actions: pool.pool_datum.actions.clone(),
            module_state: pool.pool_datum.module_state.clone(),
        };

        per_pool.push(PerPoolData {
            transcript: std::mem::take(&mut per_pool_transcripts[i]),
            updated_datum,
            lp_minted: per_pool_lp_minted[i].clone(),
            lp_burned: per_pool_lp_burned[i].clone(),
            effective_swap_dys: std::mem::take(&mut per_pool_effective_swap_dys[i]),
            final_assets_actual,
        });
    }

    // ── Step 2: Collect all inputs and sort ─────────────────────────────────

    // All pool orefs
    let pool_orefs: Vec<TransactionInput> = batches.iter()
        .map(|b| b.pool.input.0.clone())
        .collect();

    // Flat list of all order inputs (swaps, deposits, and withdraws), in
    // batch-traversal order. Each entry carries enough info to look up its
    // backing Resolved* later for fulfillment-output construction.
    #[derive(Clone)]
    enum FlatOrderKind {
        Swap(usize),     // index into batch.swaps
        Deposit(usize),  // index into batch.deposits
        Withdraw(usize), // index into batch.withdraws
        Claim(usize),    // index into batch.claims
        /// Index into plan.conversions — a conversion leg that IS the
        /// order's primary op (pure-conversion orders; batch_idx unused).
        Conversion(usize),
    }
    #[derive(Clone)]
    struct FlatOrder {
        batch_idx: usize,
        kind: FlatOrderKind,
        order_ref: TransactionInput,
    }
    let flat_orders: Vec<FlatOrder> = batches.iter().enumerate().flat_map(|(bi, b)| {
        let swaps = b.swaps.iter().enumerate().map(move |(si, s)| FlatOrder {
            batch_idx: bi,
            kind: FlatOrderKind::Swap(si),
            order_ref: s.order.input.0.clone(),
        });
        let deps = b.deposits.iter().enumerate().map(move |(di, d)| FlatOrder {
            batch_idx: bi,
            kind: FlatOrderKind::Deposit(di),
            order_ref: d.order.input.0.clone(),
        });
        let wds = b.withdraws.iter().enumerate().map(move |(wi, w)| FlatOrder {
            batch_idx: bi,
            kind: FlatOrderKind::Withdraw(wi),
            order_ref: w.order.input.0.clone(),
        });
        let cls = b.claims.iter().enumerate().map(move |(ci, c)| FlatOrder {
            batch_idx: bi,
            kind: FlatOrderKind::Claim(ci),
            order_ref: c.order.input.0.clone(),
        });
        swaps.chain(deps).chain(wds).chain(cls)
    }).collect();
    let mut flat_orders = flat_orders;
    for (ci, c) in plan.conversions.iter().enumerate() {
        if c.primary {
            flat_orders.push(FlatOrder {
                batch_idx: 0,
                kind: FlatOrderKind::Conversion(ci),
                order_ref: c.order_input.0.clone(),
            });
        }
    }
    let flat_orders = flat_orders;
    let all_order_orefs: Vec<TransactionInput> =
        flat_orders.iter().map(|f| f.order_ref.clone()).collect();

    let funding_oref_opt = funding_input.as_ref().map(|(o, _)| o.clone());
    let funding_value_opt = funding_input.as_ref().map(|(_, v)| *v);

    let mut sorted_inputs: Vec<TransactionInput> = pool_orefs.iter()
        .chain(all_order_orefs.iter())
        .chain(funding_oref_opt.iter())
        .cloned()
        .collect();
    sorted_inputs.sort_by(|a, b| {
        a.transaction_id.cmp(&b.transaction_id).then(a.index.cmp(&b.index))
    });

    // Pool sorted indices (position of each pool in sorted_inputs)
    let pool_sorted_indices: Vec<usize> = pool_orefs.iter()
        .map(|oref| sorted_inputs.iter().position(|i| i == oref).unwrap())
        .collect();

    // Pool output indices: pools sorted by their input sort position
    // Output order: pools sorted by pool_sorted_indices
    let mut pool_output_order: Vec<usize> = (0..m_pools).collect();
    pool_output_order.sort_by_key(|&i| pool_sorted_indices[i]);

    // Map from batch index to pool output index
    let mut batch_to_pool_output: Vec<usize> = vec![0; m_pools];
    for (out_idx, &batch_idx) in pool_output_order.iter().enumerate() {
        batch_to_pool_output[batch_idx] = out_idx;
    }

    // Order sorted indices (position of each order in sorted_inputs)
    let order_sorted_indices: Vec<usize> = all_order_orefs.iter()
        .map(|oref| sorted_inputs.iter().position(|i| i == oref).unwrap())
        .collect();

    // Filtered order indices (position among order-script inputs only)
    let mut order_orefs_sorted = all_order_orefs.clone();
    order_orefs_sorted.sort_by(|a, b| {
        a.transaction_id.cmp(&b.transaction_id).then(a.index.cmp(&b.index))
    });

    let order_filtered_indices: Vec<u64> = all_order_orefs.iter()
        .map(|oref| {
            order_orefs_sorted.iter().position(|o| o == oref).unwrap() as u64
        })
        .collect();

    // ── Step 3: Build vault redeemers (M spend redeemers) ──────────────────

    let mut redeemer_info: Vec<(RedeemersKey, pallas_primitives::PlutusData, ExUnits)> = Vec::new();

    let lookup_eu = |key: &RedeemersKey| -> ExUnits {
        ex_units
            .and_then(|eus| eus.iter().find(|(k, _)| k == key).map(|(_, eu)| eu.clone()))
            .unwrap_or(ExUnits { mem: exec.max_tx_ex_mem / 10, steps: exec.max_tx_ex_steps / 10 })
    };

    let mut cp_entries: Vec<CPOperateEntry> = Vec::new();
    let mut cs_entries: Vec<CSOperateEntry> = Vec::new();
    let mut cl_entries: Vec<CLOperateEntry> = Vec::new();
    let mut fs_entries: Vec<FSOperateEntry> = Vec::new();
    let mut fairness_entries: Vec<FairnessOperateEntry> = Vec::new();

    for (batch_idx, batch) in batches.iter().enumerate() {
        let pool_oref = &pool_orefs[batch_idx];
        let pool_sorted_idx = pool_sorted_indices[batch_idx];
        let pool_output_idx = batch_to_pool_output[batch_idx];

        // Action.tag selects which entry from pool.actions to evaluate.
        // Different pools were initialised with different tags (some use 3,
        // others 100); we can't hardcode. Per-entry dispatch within a module
        // uses the transcript entry's `operation_tag` field, which is a
        // separate concept from this action tag.
        // TODO(audit): pools store their action catalogue inline on the datum,
        // so "first enabled" is brittle for any pool that ever registers
        // multiple actions. Audit feedback should move the catalogue into a
        // settings UTxO referenced from the datum.
        let action_tag = batch.pool.pool_datum.actions.iter()
            .find(|a| a.enabled)
            .map(|a| a.tag.clone())
            .ok_or_else(|| anyhow::anyhow!(
                "pool {} has no enabled action entry", batch.pool_ident
            ))?;
        let pool_redeemer = PoolRedeemer::Action {
            tag: action_tag,
            transcript: per_pool[batch_idx].transcript.clone(),
            pool_input_index: BigInt::from(pool_sorted_idx as u64),
            pool_output_index: BigInt::from(pool_output_idx as u64),
        };

        let pool_key = RedeemersKey { tag: RedeemerTag::Spend, index: pool_sorted_idx as u32 };
        redeemer_info.push((pool_key.clone(), pool_redeemer.to_plutus(), lookup_eu(&pool_key)));

        let pool_oref_plutus = OutputRef {
            transaction_id: pool_oref.transaction_id.to_vec(),
            output_index: pool_oref.index,
        };

        match &batch.pool.pool_type {
            PoolType::ConstantProduct { fee } => {
                let config = ConstantProductConfig { fee: fee.clone() };
                cp_entries.push(CPOperateEntry {
                    pool_oref: pool_oref_plutus.clone(),
                    config,
                });
            }
            PoolType::ConstantSum { prices, fee, bounty_k, waive_fee_on_claim } => {
                let cs_cfg = ConstantSumConfig {
                    prices: prices.clone(),
                    fee: fee.clone(),
                    bounty_k: bounty_k.clone(),
                    waive_fee_on_claim: *waive_fee_on_claim,
                };
                if let Some(cs_script) = exec.module_scripts.constant_sum.as_ref() {
                    let cs_cred = cs_script.hash.as_ref();
                    let stored = batch.pool.pool_datum.module_state.iter()
                        .find(|(cred, _)| cred.as_slice() == cs_cred)
                        .map(|(_, h)| hex::encode(h));
                    let pd = cs_cfg.clone().to_plutus();
                    let cbor = minicbor::to_vec(&pd).unwrap_or_default();
                    let expected = hex::encode(pallas_crypto::hash::Hasher::<256>::hash(&cbor));
                    tracing::info!(
                        pool = %batch.pool.pool_datum.identifier,
                        stored_cs_hash = ?stored,
                        expected_cs_hash = %expected,
                        cs_cbor = %hex::encode(&cbor),
                        "constant_sum hash diagnostic",
                    );
                }
                cs_entries.push(CSOperateEntry {
                    pool_oref: pool_oref_plutus.clone(),
                    config: cs_cfg,
                });
            }
            PoolType::ConcentratedLiquidity { sqrt_price_a, sqrt_price_b, fee } => {
                let cl_cfg = ConcentratedLiquidityConfig {
                    sqrt_price_a: sqrt_price_a.clone(),
                    sqrt_price_b: sqrt_price_b.clone(),
                    fee: fee.clone(),
                };
                if let Some(cl_script) = exec.module_scripts.concentrated_liquidity.as_ref() {
                    let cl_cred = cl_script.hash.as_ref();
                    let stored = batch.pool.pool_datum.module_state.iter()
                        .find(|(cred, _)| cred.as_slice() == cl_cred)
                        .map(|(_, h)| hex::encode(h));
                    let pd = cl_cfg.clone().to_plutus();
                    let cbor = minicbor::to_vec(&pd).unwrap_or_default();
                    let expected = hex::encode(pallas_crypto::hash::Hasher::<256>::hash(&cbor));
                    tracing::info!(
                        pool = %batch.pool.pool_datum.identifier,
                        stored_cl_hash = ?stored,
                        expected_cl_hash = %expected,
                        cl_cbor = %hex::encode(&cbor),
                        "concentrated_liquidity hash diagnostic",
                    );
                }
                cl_entries.push(CLOperateEntry {
                    pool_oref: pool_oref_plutus.clone(),
                    config: cl_cfg,
                });
            }
        }

        // Per-pool fee_split config from chain (recovered from the pool's
        // mint tx or a recent scoop). Falls back to the global protocol_share
        // from the scooper config, which only matches pools created with the
        // same default — non-default pools will fail validation if we hit
        // this branch.
        let fs_config = match &batch.pool.fee_split_config {
            Some(cfg) => cfg.clone(),
            None => {
                tracing::warn!(
                    pool = %batch.pool.pool_datum.identifier,
                    "no per-pool fee_split config; falling back to scooper-config default — \
                     scoop will fail if the pool was created with a non-default protocol_share",
                );
                FeeSplitConfig {
                    protocol_share: Rational {
                        num: BigInt::from(exec.protocol_share.0),
                        den: BigInt::from(exec.protocol_share.1),
                    },
                }
            }
        };

        fs_entries.push(FSOperateEntry {
            pool_oref: pool_oref_plutus,
            config: fs_config,
        });

        fairness_entries.push(FairnessOperateEntry {
            pool_oref: OutputRef {
                transaction_id: pool_oref.transaction_id.to_vec(),
                output_index: pool_oref.index,
            },
            pool_ident: batch.pool.pool_datum.identifier.clone(),
            scooper: scooper_keyhash.to_vec(),
        });
    }

    // ── Step 4: Build order redeemers (N spend redeemers) ──────────────────
    //
    // One Spend redeemer per order input — same shape (`Scoop { own_input_index }`)
    // regardless of whether the order is a Swap or Deposit. The order validator
    // dispatches on the constraint tag inside its withdraw handler.
    for idx in 0..n_orders {
        let order_key = RedeemersKey {
            tag: RedeemerTag::Spend,
            index: order_sorted_indices[idx] as u32,
        };
        let order_redeemer = OrderRedeemer::Scoop {
            own_input_index: order_sorted_indices[idx] as u64,
        };
        redeemer_info.push((order_key.clone(), order_redeemer.to_plutus(), lookup_eu(&order_key)));
    }

    // ── Step 5: Build order validator entries ───────────────────────────────
    // Contract iterates filtered order inputs and entries in lock-step; each
    // entry only carries output_index, which must be strictly increasing.

    let mut input_sorted_order: Vec<usize> = (0..n_orders).collect();
    input_sorted_order.sort_by_key(|&i| order_filtered_indices[i]);

    // Modular order constraints (PR #11): each order references an
    // OrderConfig settings entry by `config_token`. Build `configs[]` =
    // unique config_tokens used by orders in this batch, and set each
    // entry's `config_index` to its config's slot in that list. Each
    // config's `ref_index` is filled in further down, once the canonical
    // ref-input ordering is known.
    let order_config_token = |flat_idx: usize| -> Vec<u8> {
        let flat = &flat_orders[flat_idx];
        match &flat.kind {
            FlatOrderKind::Swap(i) => batches[flat.batch_idx].swaps[*i].order.datum.config_token.clone(),
            FlatOrderKind::Deposit(i) => batches[flat.batch_idx].deposits[*i].order.datum.config_token.clone(),
            FlatOrderKind::Withdraw(i) => batches[flat.batch_idx].withdraws[*i].order.datum.config_token.clone(),
            FlatOrderKind::Claim(i) => batches[flat.batch_idx].claims[*i].order.datum.config_token.clone(),
            FlatOrderKind::Conversion(i) => {
                plan.conversions[*i].order.datum.config_token.clone()
            }
        }
    };
    let flat_order_ref_and_datum = |flat_idx: usize| -> (&TransactionInput, &crate::sundaev4::OrderDatum) {
        let flat = &flat_orders[flat_idx];
        match &flat.kind {
            FlatOrderKind::Swap(i) => (&flat.order_ref, &batches[flat.batch_idx].swaps[*i].order.datum),
            FlatOrderKind::Deposit(i) => (&flat.order_ref, &batches[flat.batch_idx].deposits[*i].order.datum),
            FlatOrderKind::Withdraw(i) => (&flat.order_ref, &batches[flat.batch_idx].withdraws[*i].order.datum),
            FlatOrderKind::Claim(i) => (&flat.order_ref, &batches[flat.batch_idx].claims[*i].order.datum),
            FlatOrderKind::Conversion(i) => {
                (&flat.order_ref, &plan.conversions[*i].order.datum)
            }
        }
    };
    let mut unique_config_tokens: Vec<Vec<u8>> = Vec::new();
    let mut order_config_indices: Vec<u64> = Vec::with_capacity(n_orders);
    for &flat_idx in &input_sorted_order {
        let token = order_config_token(flat_idx);
        let idx = unique_config_tokens
            .iter()
            .position(|t| t == &token)
            .unwrap_or_else(|| {
                unique_config_tokens.push(token);
                unique_config_tokens.len() - 1
            });
        order_config_indices.push(idx as u64);
    }
    // OrderValidatorRedeemer.configs[i].ref_index needs the canonical
    // ref-input position of each OrderConfig's settings UTxO. We compute
    // it after all_ref_inputs is assembled below; for now stash just the
    // entries.
    let order_validator_entries: Vec<OrderValidatorEntry> = input_sorted_order
        .iter()
        .enumerate()
        .map(|(out_pos, _flat_idx)| OrderValidatorEntry {
            output_index: (m_pools + out_pos) as u64,
            config_index: order_config_indices[out_pos],
        })
        .collect();

    // Placeholder — the real OrderValidatorRedeemer is built further down
    // once `canonical_ref_order` is known. We just need a value here so
    // existing code that references `order_validator_redeemer` compiles.
    let order_validator_redeemer = OrderValidatorRedeemer {
        configs: Vec::new(),
        entries: order_validator_entries,
    };

    let has_cp = !cp_entries.is_empty();
    let has_cs = !cs_entries.is_empty();
    let has_cl = !cl_entries.is_empty();

    // pool_mint is only needed when the tx actually mints or burns LP tokens
    // (deposits/withdraws). Pure-swap batches grow the protocol_lp gap inside
    // each pool's datum but don't mint anything on chain — pool_lib's
    // check_lp_accounting compares `circulating + preminted` (not total_lp)
    // against `net_lp_minted`, so it's satisfied by 0 mint when only swaps
    // happen. The gap can be minted later by a separate "claim" tx. Skipping
    // pool_mint's ref script here drops ~5KB of ref_script_bytes per
    // pure-swap scoop, which on the tiered Conway fee saves real lovelace.
    let has_lp_mint_or_burn = {
        use num_traits::Zero;
        per_pool_lp_minted.iter()
            .zip(per_pool_lp_burned.iter())
            .any(|(m, b)| !(m - b).clone().unwrap().is_zero())
    };

    let fs_redeemer = FeeSplitRedeemer::Operate { entries: fs_entries };
    let fairness_redeemer = FairnessRedeemer::Operate { entries: fairness_entries };

    // ── Step 6: Reference inputs ───────────────────────────────────────────

    let mut all_ref_inputs: Vec<TransactionInput> = vec![
        exec.module_scripts.pool.ref_utxo.0.clone(),
        exec.module_scripts.order.ref_utxo.0.clone(),
        exec.module_scripts.fee_split.ref_utxo.0.clone(),
        exec.module_scripts.fairness.ref_utxo.0.clone(),
        exec.module_scripts.settings.ref_utxo.0.clone(),
    ];
    if has_lp_mint_or_burn {
        all_ref_inputs.push(exec.module_scripts.pool_mint.ref_utxo.0.clone());
    }
    if has_cp {
        all_ref_inputs.push(exec.module_scripts.constant_product.ref_utxo.0.clone());
    }
    if has_cs {
        if let Some(cs) = &exec.module_scripts.constant_sum {
            all_ref_inputs.push(cs.ref_utxo.0.clone());
        }
    }
    if has_cl {
        if let Some(cl) = &exec.module_scripts.concentrated_liquidity {
            all_ref_inputs.push(cl.ref_utxo.0.clone());
        }
    }
    // PR #11 modular order constraints. For each unique OrderConfig token
    // referenced by orders in this batch:
    //   1. resolve its settings UTxO from the indexer cache (add to ref inputs)
    //   2. union its `required_constraints` into `required_constraint_hashes`
    // Then add a script-ref for each required constraint module (so its
    // withdrawal can run) and emit the withdrawal itself further below.
    let mut unique_order_configs: Vec<(Vec<u8>, std::sync::Arc<crate::sundaev4::SundaeV4OrderConfig>)> =
        Vec::new();
    for token in &unique_config_tokens {
        if let Some(oc) = order_configs.get(token) {
            unique_order_configs.push((token.clone(), oc.clone()));
        } else if !token.is_empty() {
            tracing::warn!(
                token = %hex::encode(token),
                "scoop: order references unknown OrderConfig; tx will likely fail on chain",
            );
        }
    }
    let mut required_constraint_hashes: std::collections::BTreeSet<Vec<u8>> =
        std::collections::BTreeSet::new();
    for (_, oc) in &unique_order_configs {
        for h in &oc.config.required_constraints {
            required_constraint_hashes.insert(h.clone());
        }
    }
    let constraint_script_refs = |hash: &[u8]| -> Option<&ScriptRefInfo> {
        for slot in [
            &exec.module_scripts.swap_order,
            &exec.module_scripts.basic_order,
            &exec.module_scripts.route_order,
            &exec.module_scripts.fairness_order,
            &exec.module_scripts.strategy_order,
        ] {
            if let Some(sri) = slot {
                if sri.hash.as_ref() == hash {
                    return Some(sri);
                }
            }
        }
        None
    };
    for h in &required_constraint_hashes {
        if let Some(sri) = constraint_script_refs(h) {
            all_ref_inputs.push(sri.ref_utxo.0.clone());
        } else {
            tracing::warn!(
                hash = %hex::encode(h),
                "scoop: required constraint module has no configured script ref",
            );
        }
    }
    // Each OrderConfig's settings UTxO is a reference input — the base
    // order_validator's withdraw handler reads it as the `configs[i]` lookup
    // resolution.
    for (_, oc) in &unique_order_configs {
        all_ref_inputs.push(oc.input.0.clone());
    }
    all_ref_inputs.push(settings.input.0.clone());

    // Sort the reference inputs canonically — (txId bytes, output_index) —
    // and dedupe. The ledger treats reference_inputs as a Set and presents
    // them to scripts in exactly this order, so keeping the body in canonical
    // order makes the body, the on-chain ScriptContext, and every ref_index
    // we put in redeemers agree by construction.
    for p in &butane_pieces {
        all_ref_inputs.extend(p.ref_inputs.iter().map(|i| i.0.clone()));
    }
    all_ref_inputs.sort_by(|a, b| {
        a.transaction_id
            .cmp(&b.transaction_id)
            .then(a.index.cmp(&b.index))
    });
    all_ref_inputs.dedup();
    let canonical_ref_order: Vec<TransactionInput> = all_ref_inputs.clone();
    let canonical_index_of = |input: &TransactionInput| -> u64 {
        canonical_ref_order
            .iter()
            .position(|i| i == input)
            .expect("ref input must be in canonical order") as u64
    };

    let settings_input_index = canonical_index_of(&settings.input.0);
    // Re-resolve OrderValidatorConfig.ref_index now that we know the
    // canonical position of each OrderConfig settings UTxO.
    let order_validator_configs: Vec<OrderValidatorConfig> = unique_config_tokens
        .iter()
        .map(|token| {
            let ref_index = unique_order_configs
                .iter()
                .find(|(t, _)| t == token)
                .map(|(_, oc)| canonical_index_of(&oc.input.0))
                .unwrap_or(0);
            OrderValidatorConfig {
                ref_index,
                token: token.clone(),
            }
        })
        .collect();
    // Temp diagnostic — log canonical ref ordering vs the OrderConfig
    // resolutions so we can see if ref_index points at the right UTxO.
    tracing::info!("DBG canonical ref_inputs order:");
    for (i, r) in canonical_ref_order.iter().enumerate() {
        tracing::info!(
            "  [{}] {}#{}",
            i,
            hex::encode(r.transaction_id.as_ref()),
            r.index,
        );
    }
    for (token, oc) in &unique_order_configs {
        tracing::info!(
            "DBG OrderConfig token={} resolves to ref [{}] {}#{}",
            hex::encode(token),
            canonical_index_of(&oc.input.0),
            hex::encode(oc.input.0.transaction_id.as_ref()),
            oc.input.0.index,
        );
    }
    tracing::info!(
        "DBG settings.input canonical idx = {} (= settings_input_index)",
        settings_input_index,
    );
    let order_validator_redeemer = OrderValidatorRedeemer {
        configs: order_validator_configs,
        entries: order_validator_redeemer.entries,
    };
    // Compute the scooper's slot in `authorized_scoopers` for the
    // fairness_order constraint's redeemer (PR #11).
    let authorized_scooper_index: u64 = settings
        .datum
        .authorized_scoopers
        .as_ref()
        .and_then(|list| {
            list.iter()
                .position(|kh| kh.as_slice() == scooper_keyhash.as_ref())
        })
        .map(|i| i as u64)
        .unwrap_or(0);

    // Legacy compatibility helpers — kept for the unit-redeemer paths below
    // that don't care about the new per-class metadata.
    let has_swap_orders = n_swap_orders > 0;
    let _has_basic_orders = n_deposit_orders > 0 || n_withdraw_orders > 0;

    // ── Step 7: Build withdrawal map ───────────────────────────────────────

    fn reward_account(script_hash: &Hash<28>) -> PallasBytes {
        let mut account = vec![0xf0u8];
        account.extend_from_slice(script_hash.as_ref());
        PallasBytes::from(account)
    }

    // Always present: order, fee_split, fairness
    let mut withdrawals: Vec<(PallasBytes, pallas_primitives::PlutusData)> = vec![(
        reward_account(&exec.module_scripts.order.hash),
        order_validator_redeemer.to_plutus(),
    )];
    // fee_split and fairness are POOL action modules — the pool datum's
    // action entry demands them. A pool-less scoop (pure-conversion orders)
    // spends no pool, so nothing requires them and their empty-entry
    // redeemers would just burn budget (or fail).
    if m_pools > 0 {
        withdrawals.push((
            reward_account(&exec.module_scripts.fee_split.hash),
            fs_redeemer.to_plutus(),
        ));
        withdrawals.push((
            reward_account(&exec.module_scripts.fairness.hash),
            fairness_redeemer.to_plutus(),
        ));
    }

    // Conditionally add CP withdrawal
    if has_cp {
        let cp_redeemer = ConstantProductRedeemer::Operate { entries: cp_entries };
        withdrawals.push((
            reward_account(&exec.module_scripts.constant_product.hash),
            cp_redeemer.to_plutus(),
        ));
    }

    // Conditionally add CS withdrawal
    if has_cs {
        if let Some(cs_script) = &exec.module_scripts.constant_sum {
            let cs_redeemer = ConstantSumRedeemer::Operate { entries: cs_entries };
            withdrawals.push((
                reward_account(&cs_script.hash),
                cs_redeemer.to_plutus(),
            ));
        }
    }

    // Conditionally add CL withdrawal
    if has_cl {
        if let Some(cl_script) = &exec.module_scripts.concentrated_liquidity {
            let cl_redeemer = ConcentratedLiquidityRedeemer::Operate { entries: cl_entries };
            withdrawals.push((
                reward_account(&cl_script.hash),
                cl_redeemer.to_plutus(),
            ));
        }
    }

    // Modular order constraints (PR #11). For each constraint hash listed in
    // any orders' OrderConfig.required_constraints, add a withdrawal with the
    // shape that module expects:
    //   swap_order, basic_order → unit (Constr 0 [])
    //   route_order             → List<List<RouteStep>> (one inner list per
    //                              order carrying route_order)
    //   fairness_order          → { settings_input_index, authorized_scooper_index }
    //   strategy_order          → (deferred; SSE ingestion TBD)
    // Unknown / unconfigured constraint hashes are skipped — the on-chain
    // order_validator's withdraw handler will then fail, but the diagnostic
    // is clearer than a silent encoding mismatch.
    let unit_redeemer = || pallas_primitives::PlutusData::Constr(pallas_primitives::Constr {
        tag: 121,
        any_constructor: None,
        fields: pallas_codec::utils::MaybeIndefArray::Def(vec![]),
    });
    let classify_hash = |h: &[u8]| -> Option<&'static str> {
        let matches = |s: &Option<ScriptRefInfo>| s.as_ref().map_or(false, |si| si.hash.as_ref() == h);
        if matches(&exec.module_scripts.swap_order) { Some("swap_order") }
        else if matches(&exec.module_scripts.basic_order) { Some("basic_order") }
        else if matches(&exec.module_scripts.route_order) { Some("route_order") }
        else if matches(&exec.module_scripts.fairness_order) { Some("fairness_order") }
        else if matches(&exec.module_scripts.strategy_order) { Some("strategy_order") }
        else { None }
    };
    // Precompute the route_order redeemer if needed. It's a per-order
    // List<List<RouteStep>>, one inner list for EVERY base order entry — in
    // canonical input-sort order — because the on-chain `validate_route_entries`
    // walks entries/inputs/routes in lockstep and `expect`s a `route` for each
    // entry (see route.ak). Non-route orders get `[]`; a route order gets one
    // RouteStep = (pool_input_index, transcript_step_index) per pool it flows
    // through, in hop (flow) order.
    let route_order_hash = exec.module_scripts.route_order.as_ref().map(|s| s.hash.as_ref().to_vec());

    // Per route (indexed by RouteRef.route_idx), the ordered list of
    // (pool_input_index, transcript_step_index). We gate to serial routes
    // upstream (one split per hop), so sorting a route's ops by
    // (hop_idx, split_idx) yields the serial chain. `transcript_step_index` is
    // the op's position in its pool's `ops_order` (== the pool's transcript
    // index). `swap_op_idx` lets a direct (single-pool) swap emit its true
    // transcript index instead of a hardcoded 0.
    let mut per_route_keyed: Vec<Vec<((usize, usize), u64, u64)>> =
        vec![Vec::new(); routes.len()];
    let mut swap_op_idx: std::collections::HashMap<(usize, usize), u64> =
        std::collections::HashMap::new();
    for (bi, b) in batches.iter().enumerate() {
        let pool_input_idx = pool_sorted_indices[bi] as u64;
        for (op_idx, op) in b.ops_order.iter().enumerate() {
            let rr = match op {
                crate::sundaev4::batch::BatchOp::Swap(i) => {
                    swap_op_idx.insert((bi, *i), op_idx as u64);
                    b.swaps[*i].route.as_ref()
                }
                crate::sundaev4::batch::BatchOp::Continuation(i) => {
                    Some(&b.continuations[*i].route)
                }
                _ => None,
            };
            if let Some(rr) = rr {
                per_route_keyed[rr.route_idx].push((
                    (rr.hop_idx, rr.split_idx),
                    pool_input_idx,
                    op_idx as u64,
                ));
            }
        }
    }
    let per_route: Vec<Vec<(u64, u64)>> = per_route_keyed
        .into_iter()
        .map(|mut keyed| {
            keyed.sort_by_key(|(k, _, _)| *k);
            keyed.into_iter().map(|(_, pin, tsi)| (pin, tsi)).collect()
        })
        .collect();

    let route_redeemer = || -> pallas_primitives::PlutusData {
        // RouteStep = Constr 0 [pool_input_idx, transcript_step_idx].
        let make_step = |pin: u64, tsi: u64| -> pallas_primitives::PlutusData {
            pallas_primitives::PlutusData::Constr(pallas_primitives::Constr {
                tag: 121,
                any_constructor: None,
                fields: pallas_codec::utils::MaybeIndefArray::Def(vec![
                    pallas_primitives::PlutusData::BigInt(pallas_primitives::BigInt::Int(
                        (pin as i128).try_into().unwrap_or_else(|_| 0i64.into()),
                    )),
                    pallas_primitives::PlutusData::BigInt(pallas_primitives::BigInt::Int(
                        (tsi as i128).try_into().unwrap_or_else(|_| 0i64.into()),
                    )),
                ]),
            })
        };
        let route_lists: Vec<pallas_primitives::PlutusData> = input_sorted_order
            .iter()
            .map(|&flat_idx| {
                let is_route = route_order_hash
                    .as_ref()
                    .and_then(|route_hash| {
                        let oc = order_configs.get(&order_config_token(flat_idx))?;
                        oc.config
                            .required_constraints
                            .iter()
                            .any(|h| h == route_hash)
                            .then_some(())
                    })
                    .is_some();
                let steps: Vec<(u64, u64)> = if !is_route {
                    Vec::new()
                } else {
                    let flat = &flat_orders[flat_idx];
                    let bi = flat.batch_idx;
                    match &flat.kind {
                        FlatOrderKind::Swap(si) => match &batches[bi].swaps[*si].route {
                            // Routed order: full serial hop chain.
                            Some(rr) => per_route[rr.route_idx].clone(),
                            // Direct single-pool swap: one step at its pool.
                            None => vec![(
                                pool_sorted_indices[bi] as u64,
                                *swap_op_idx.get(&(bi, *si)).unwrap_or(&0),
                            )],
                        },
                        // Conversion-primary orders touch no pool: no
                        // steps to attest (and no pool to index — a pool-
                        // less tx panicked here).
                        FlatOrderKind::Conversion(_) => Vec::new(),
                        // Non-swap orders don't carry a route constraint in
                        // phase 1; emit a single step to stay 1:1 with entries.
                        _ if m_pools == 0 => Vec::new(),
                        _ => vec![(pool_sorted_indices[bi] as u64, 0)],
                    }
                };
                let step_data: Vec<pallas_primitives::PlutusData> =
                    steps.iter().map(|(pin, tsi)| make_step(*pin, *tsi)).collect();
                pallas_primitives::PlutusData::Array(
                    pallas_codec::utils::MaybeIndefArray::Indef(step_data),
                )
            })
            .collect();
        pallas_primitives::PlutusData::Array(pallas_codec::utils::MaybeIndefArray::Indef(route_lists))
    };
    for h in required_constraint_hashes.iter() {
        let class = classify_hash(h);
        let raw_hash: Hash<28> = match h.as_slice().try_into() {
            Ok(arr) => Hash::new(arr),
            Err(_) => continue,
        };
        let redeemer = match class {
            Some("swap_order") | Some("basic_order") => unit_redeemer(),
            Some("route_order") => route_redeemer(),
            Some("fairness_order") => {
                crate::sundaev4::types::FairnessOrderRedeemer {
                    settings_input_index,
                    authorized_scooper_index,
                }.to_plutus()
            }
            Some("strategy_order") => {
                // List<SignedStrategyExecution>, consumed positionally by the
                // validator as it walks order inputs (canonical order) whose
                // datum carries the strategy constraint — so: one SSE per
                // such order, in input_sorted_order.
                let mut sses: Vec<pallas_primitives::PlutusData> = Vec::new();
                for &flat_idx in &input_sorted_order {
                    let (oref, datum) = flat_order_ref_and_datum(flat_idx);
                    if datum.find_constraint_by_hash(h).is_none() {
                        continue;
                    }
                    match strategy_executions
                        .get(&crate::cardano_types::TransactionInput(oref.clone()))
                    {
                        Some(pd) => sses.push(pd.clone()),
                        None => bail!(
                            "strategy order {}#{} in plan without a signed execution",
                            hex::encode(oref.transaction_id.as_ref()),
                            oref.index,
                        ),
                    }
                }
                pallas_primitives::PlutusData::Array(
                    pallas_codec::utils::MaybeIndefArray::Indef(sses),
                )
            }
            None => {
                tracing::warn!(
                    hash = %hex::encode(h),
                    "scoop: required constraint hash didn't match any configured module",
                );
                continue;
            }
            _ => unreachable!(),
        };
        withdrawals.push((reward_account(&raw_hash), redeemer));
    }
    // `has_swap_orders` retained for tests that still gate on this flag.
    let _ = has_swap_orders;

    {
        // One withdrawal per butane validator regardless of leg count;
        // identical redeemers merge, conflicting ones (multiple synthetics
        // in one tx) are unsupported until the aux redeemer grows a list.
        let mut seen: std::collections::BTreeMap<Vec<u8>, pallas_primitives::PlutusData> =
            Default::default();
        for p in &butane_pieces {
            for (hash, redeemer, _version) in &p.withdrawals {
                match seen.get(hash) {
                    None => {
                        seen.insert(hash.clone(), redeemer.clone());
                    }
                    Some(prev) if prev == redeemer => {}
                    Some(_) => bail!(
                        "conflicting butane withdrawal redeemers (multiple \
                         synthetics in one tx isn't supported yet)"
                    ),
                }
            }
        }
        for (hash, redeemer) in seen {
            let h: pallas_primitives::Hash<28> = hash.as_slice().try_into()
                .expect("verified 28-byte script hash");
            withdrawals.push((reward_account(&h), redeemer));
        }
    }
    withdrawals.sort_by(|(a, _), (b, _)| a.cmp(b));

    // Withdrawal redeemers — one per withdrawal entry
    for (idx, (_, data)) in withdrawals.iter().enumerate() {
        let wd_key = RedeemersKey { tag: RedeemerTag::Reward, index: idx as u32 };
        redeemer_info.push((wd_key.clone(), data.clone(), lookup_eu(&wd_key)));
    }

    // ── Step 8: Build outputs ──────────────────────────────────────────────

    let pool_address = {
        let pool_addr = ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::Script(exec.module_scripts.pool.hash),
            ShelleyDelegationPart::Null,
        );
        PallasBytes::from(pool_addr.to_vec())
    };

    let mut outputs: Vec<TransactionOutput> = Vec::new();

    // Precompute per-batch net ADA delta for pool outputs.
    //
    // For ADA/TOKEN pools, ADA flows in two ways:
    //   - sell_outflow: dy for swaps/continuations where output is ADA (pool gives up ADA)
    //   - cont_buy_inflow: dx for CONTINUATIONS where input is ADA (pool receives ADA
    //     from routing — this ADA came from another pool's sell outflow)
    //
    // Pool output ADA delta: derived directly from the actual per-pool
    // final reserves we tracked in `per_pool[i].final_assets_actual`,
    // which reflects every applied op (including any recomputed CL dys
    // that diverge from the accumulator's projection). This is more
    // robust than summing per-op `dx`/`dy` aggregates that ignore the
    // recompute path.
    let ada_asset = AssetClass { policy: vec![], token: vec![] };
    let pool_ada_deltas: Vec<i64> = (0..batches.len()).map(|batch_idx| {
        use num_traits::ToPrimitive;
        let batch = &batches[batch_idx];
        let final_assets = &per_pool[batch_idx].final_assets_actual;
        let ada_idx = batch.pool.pool_datum.assets.iter().position(|(a, _)| {
            a.policy.is_empty() && a.token.is_empty()
        });
        match ada_idx {
            Some(idx) => {
                let initial: i64 = batch.pool.pool_datum.assets[idx].1
                    .clone().unwrap().to_i64().unwrap_or(0);
                let final_v: i64 = final_assets[idx].1
                    .clone().unwrap().to_i64().unwrap_or(0);
                final_v - initial
            }
            None => 0,
        }
    }).collect();

    // Pool outputs in pool_output_order (sorted by input position).
    // After construction we walk each pool output and bump its lovelace
    // to actual min-utxo if the current ada (preserved from the input)
    // falls short — datum growth from governance upgrades is the typical
    // cause. The shortfall is funded by the wallet `funding_input` and
    // any leftover flows back via the scooper change output below.
    let mut pool_output_bumps: Vec<u64> = vec![0; m_pools];
    for (pos, &batch_idx) in pool_output_order.iter().enumerate() {
        let batch = &batches[batch_idx];
        let pool_datum_pd = per_pool[batch_idx].updated_datum.clone().to_plutus();
        let pool_output_value = build_pool_output_value(
            &batch.pool, &per_pool[batch_idx].final_assets_actual, pool_ada_deltas[batch_idx],
        )?;
        let mut out = TransactionOutput::PostAlonzo(
            pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
                address: pool_address.clone(),
                value: pool_output_value,
                datum_option: Some(conway::PseudoDatumOption::Data(CborWrap(pool_datum_pd))),
                script_ref: None,
            },
        );
        // Reach the minUtxo by recomputing from the serialized output. Adding
        // lovelace can grow the CBOR by 1 byte (and therefore the requirement
        // by 4310 lovelace), so iterate until stable.
        for _ in 0..4 {
            let needed = compute_output_min_ada(&out)?;
            let TransactionOutput::PostAlonzo(ref body) = out else { unreachable!() };
            let current = match &body.value {
                ConwayValue::Coin(c) => *c,
                ConwayValue::Multiasset(c, _) => *c,
            };
            if current >= needed { break; }
            let bump = needed - current;
            pool_output_bumps[pos] += bump;
            let new_ada = current + bump;
            if let TransactionOutput::PostAlonzo(b) = &mut out {
                b.value = match &b.value {
                    ConwayValue::Coin(_) => ConwayValue::Coin(new_ada),
                    ConwayValue::Multiasset(_, ma) => ConwayValue::Multiasset(new_ada, ma.clone()),
                };
            }
        }
        outputs.push(out);
    }
    let total_pool_bump: u64 = pool_output_bumps.iter().sum();

    // Fee split across all orders
    let per_order_fee = tx_fee / n_orders as u64;
    let last_order_fee = tx_fee - per_order_fee * (n_orders as u64 - 1);

    // Fulfillment outputs in input-sorted order — one per order (Swap or
    // Deposit). The order validator iterates filtered order inputs and entries
    // in lockstep; entries' output_index values are computed from this same
    // sort, so the two stay aligned.
    // Lovelace the funding UTxO must cover beyond pool min-ada bumps:
    // fee contributions we waived (and output top-ups) to keep fulfillment
    // outputs above the ledger's min-UTxO — standing (Self) orders with
    // several assets and an inline datum need ~2.5M lovelace retained.
    let mut total_fulfillment_subsidy: u64 = 0;
    let mut fulfillment_order: Vec<usize> = (0..n_orders).collect();
    fulfillment_order.sort_by_key(|i| order_filtered_indices[*i]);

    for (out_pos, &fi) in fulfillment_order.iter().enumerate() {
        let fo_meta = &flat_orders[fi];
        let order = match &fo_meta.kind {
            FlatOrderKind::Swap(i) => &batches[fo_meta.batch_idx].swaps[*i].order,
            FlatOrderKind::Deposit(i) => &batches[fo_meta.batch_idx].deposits[*i].order,
            FlatOrderKind::Withdraw(i) => &batches[fo_meta.batch_idx].withdraws[*i].order,
            FlatOrderKind::Claim(i) => &batches[fo_meta.batch_idx].claims[*i].order,
            FlatOrderKind::Conversion(i) => &plan.conversions[*i].order,
        };
        // Partial fill detection (swap-module orders only): the fill size
        // is the route's hop-0 input (routed) or the resolved swap's dx
        // (direct). Below remaining_offered → continuation at the order
        // address with remaining decremented (swap.ak's continuation arm);
        // the received tokens accumulate ON the continuation.
        let swap_fill: Option<crate::bigint::BigInt> = match &fo_meta.kind {
            FlatOrderKind::Swap(i) => {
                let swap = &batches[fo_meta.batch_idx].swaps[*i];
                Some(match &swap.route {
                    Some(rref) => routes
                        .iter()
                        .enumerate()
                        .filter(|(_, r)| r.order.input == swap.order.input)
                        .fold(BigInt::from(0), |acc, (ri, _)| {
                            let hop0 = routes[ri]
                                .hops
                                .first()
                                .map(|h| h.hop_total_at_route_time.clone())
                                .unwrap_or_else(|| BigInt::from(0));
                            let _ = rref;
                            &acc + &hop0
                        }),
                    None => swap.dx.clone(),
                })
            }
            _ => None,
        };
        let partial_continuation: Option<pallas_primitives::PlutusData> = match &swap_fill {
            Some(fill) if fill < order.swap_offered().1 => {
                let (_, remaining) = order.swap_offered();
                let new_remaining = remaining - fill;
                let swap_hash = exec
                    .module_scripts
                    .swap_order
                    .as_ref()
                    .context("partial fill requires the swap_order module config")?
                    .hash;
                let datum = order
                    .datum
                    .with_swap_remaining(swap_hash.as_ref(), &new_remaining)?;
                Some(datum.to_plutus())
            }
            _ => None,
        };

        // Self destinations return the fulfillment to the order address with
        // the *identical* datum (check_destination's Self arm) — a standing
        // order that survives its own execution and can be executed again by
        // a fresh intent. Fixed destinations pay the given address, with the
        // destination's optional datum pinned inline when present. Partial
        // fills override both: the output IS the continuation.
        let (dest_address, dest_datum): (Vec<u8>, Option<pallas_primitives::PlutusData>) =
            if let Some(cont_datum) = &partial_continuation {
                let order_addr = ShelleyAddress::new(
                    Network::Testnet,
                    ShelleyPaymentPart::Script(exec.module_scripts.order.hash),
                    ShelleyDelegationPart::Null,
                );
                (order_addr.to_vec(), Some(cont_datum.clone()))
            } else {
            match &order.datum.destination {
                crate::sundaev4::Destination::SelfDestination => {
                    let order_addr = ShelleyAddress::new(
                        Network::Testnet,
                        ShelleyPaymentPart::Script(exec.module_scripts.order.hash),
                        ShelleyDelegationPart::Null,
                    );
                    (order_addr.to_vec(), Some(order.datum.clone().to_plutus()))
                }
                crate::sundaev4::Destination::Fixed(_, maybe_datum) => (
                    resolve_destination(&order.datum.destination, &order.datum.owner)?,
                    maybe_datum.clone(),
                ),
            }
            };

        let fee = if out_pos == n_orders - 1 { last_order_fee } else { per_order_fee };
        // Take the full per_order share. The contract's
        //   allowance = fee_share + share_batcher·(budget − fee_share)/10000
        // can drop below fee_share when an order's budget is small, and the
        // order_validator separately requires `budget·n >= tx_body.fee`
        // (line 139 of validators/order.ak). Capping our deduction without
        // also lowering tx_body.fee would break value conservation, and
        // lowering tx.fee involves a fixed-point computation — left as a
        // follow-up. For now we always take fee_share; orders that can't
        // afford it get caught by the budget-too-low filter at the scooper
        // and never reach this code.
        let actual_fee = fee;

        let fulfillment_value = match &fo_meta.kind {
            FlatOrderKind::Swap(i) => {
                let swap = &batches[fo_meta.batch_idx].swaps[*i];
                // Routed orders: fulfillment dy + output asset come from the
                // streaming walk's accumulated final-hop output (sum across
                // any splits of the last hop). Direct swaps use the dy we
                // recomputed against the pool's running state — that's
                // already the validator-tight bound after any CL lp bump.
                let (output_asset, dy_owned);
                let (output_asset_ref, dy_ref): (&AssetClass, &BigInt) = match &swap.route {
                    Some(rref) => {
                        // Blended orders own several routes (one per
                        // branch); the fulfillment carries the sum of their
                        // final outputs. Single-route orders sum over one.
                        let rs = &route_states[rref.route_idx];
                        dy_owned = routes
                            .iter()
                            .enumerate()
                            .filter(|(_, r)| r.order.input == swap.order.input)
                            .fold(BigInt::from(0), |acc, (ri, _)| {
                                &acc + &route_states[ri].final_output
                            });
                        (&rs.final_output_asset, &dy_owned)
                    }
                    None => {
                        output_asset =
                            batches[fo_meta.batch_idx].pool.pool_datum.assets[swap.output_idx].0.clone();
                        dy_owned = per_pool[fo_meta.batch_idx]
                            .effective_swap_dys[*i].clone();
                        (&output_asset, &dy_owned)
                    }
                };
                let (offer_asset, offer_amount) = swap.order.swap_offered();
                let spend_amount = swap_fill.as_ref().unwrap_or(offer_amount);
                // Contract fee cap for partials: fee ≤ allowance·fill/original.
                if spend_amount < offer_amount {
                    if let crate::sundaev4::Constraint::Swap { original_offered, .. } =
                        &swap.order.constraint
                    {
                        use num_traits::ToPrimitive;
                        let allowance = {
                            let fee_share = BigInt::from(actual_fee);
                            let share = &swap.order.datum.share_batcher;
                            let surplus = &swap.order.datum.budget - &fee_share;
                            &fee_share + &(&(share * &surplus) / &BigInt::from(10_000u64))
                        };
                        let cap = &(&allowance * spend_amount) / original_offered;
                        let cap_u64 = cap.clone().unwrap().to_u64().unwrap_or(0);
                        if actual_fee > cap_u64 {
                            bail!(
                                "partial fill fee {actual_fee} exceeds pro-rata cap \
                                 {cap_u64} (fill {spend_amount} of {original_offered})",
                            );
                        }
                    }
                }
                build_fulfillment_value_from_order(
                    &swap.order.value,
                    offer_asset,
                    spend_amount,
                    output_asset_ref,
                    dy_ref,
                    actual_fee,
                )?
            }
            FlatOrderKind::Deposit(i) => {
                let dep = &batches[fo_meta.batch_idx].deposits[*i];
                let lp_asset = pool_lp_asset(exec, &batches[fo_meta.batch_idx].pool)?;
                build_deposit_fulfillment_value(
                    &dep.order.value,
                    &batches[fo_meta.batch_idx].pool.pool_datum.assets,
                    &dep.dx,
                    &lp_asset,
                    &dep.lp_minted,
                    actual_fee,
                )?
            }
            FlatOrderKind::Withdraw(i) => {
                let wd = &batches[fo_meta.batch_idx].withdraws[*i];
                let lp_asset = pool_lp_asset(exec, &batches[fo_meta.batch_idx].pool)?;
                build_withdraw_fulfillment_value(
                    &wd.order.value,
                    &batches[fo_meta.batch_idx].pool.pool_datum.assets,
                    &wd.dy,
                    &lp_asset,
                    &wd.lp_burned,
                    actual_fee,
                )?
            }
            FlatOrderKind::Conversion(i) => {
                let c = &plan.conversions[*i];
                // The order's total output across its routes (a pure-
                // conversion order has one route whose final output was
                // seeded from the conversion legs).
                let total_out = routes
                    .iter()
                    .enumerate()
                    .filter(|(_, r)| r.order.input == c.order.input)
                    .fold(BigInt::from(0), |acc, (ri, _)| {
                        &acc + &route_states[ri].final_output
                    });
                // Total spent of the from-asset across the order's legs.
                let total_dx = plan
                    .conversions
                    .iter()
                    .filter(|l| l.order.input == c.order.input && l.from == c.from)
                    .fold(BigInt::from(0), |acc, l| &acc + &l.dx);
                build_fulfillment_value_from_order(
                    &c.order.value,
                    &c.from,
                    &total_dx,
                    &c.to,
                    &total_out,
                    actual_fee,
                )?
            }
            FlatOrderKind::Claim(i) => {
                // Fulfillment = order value moved by the NEGATED pool deltas
                // (what the pool gains, the order loses, and vice versa),
                // minus the fee share. Covers pair-wise and multi-receive
                // claims uniformly.
                let c = &batches[fo_meta.batch_idx].claims[*i];
                let moves: Vec<(&AssetClass, BigInt)> = batches[fo_meta.batch_idx]
                    .pool
                    .pool_datum
                    .assets
                    .iter()
                    .zip(c.pool_deltas.iter())
                    .map(|((asset, _), delta)| (asset, -delta.clone()))
                    .collect();
                build_fulfillment_value_with_moves(&c.order.value, &moves, actual_fee)?
            }
        };
        let mut out = TransactionOutput::PostAlonzo(
            pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
                address: PallasBytes::from(dest_address),
                value: fulfillment_value,
                datum_option: dest_datum
                    .map(|d| conway::PseudoDatumOption::Data(CborWrap(d))),
                script_ref: None,
            },
        );
        // The output must clear the ledger's min-UTxO on its own ada: the
        // scooper does NOT subsidise orders (its fee take only reimburses
        // the network fee, so any top-up is a direct loss — and a drain
        // vector via under-funded standing orders). Such orders are
        // unexecutable until re-funded; fail the build with a clear reason
        // so the matcher/status can surface it.
        {
            let needed = compute_output_min_ada(&out)?;
            let TransactionOutput::PostAlonzo(ref body) = out else { unreachable!() };
            let current = match &body.value {
                ConwayValue::Coin(c) => *c,
                ConwayValue::Multiasset(c, _) => *c,
            };
            if current < needed {
                bail!(
                    "fulfillment output for order {} retains {current} lovelace \
                     after its fee share but needs {needed} (min-UTxO): order is \
                     under-funded for execution",
                    hex::encode(fo_meta.order_ref.transaction_id.as_ref()),
                );
            }
        }
        let _ = &mut total_fulfillment_subsidy;
        outputs.push(out);
    }

    // ── Step 8.35: Butane pot outputs ──────────────────────────────────────
    for p in &butane_pieces {
        outputs.push(p.pot_output.clone());
    }

    // ── Step 8.4: Scooper change output for the funding UTxO ───────────────
    //
    // Only emitted when a `funding_input` was provided. Its ada covers any
    // min-ada bump we applied to pool outputs above; the remainder flows
    // here as a vkey-locked output back to the scooper's wallet, along
    // with any native tokens carried by the funding UTxO. If no funding
    // was provided but a bump was needed, bail — the scooper must retry
    // once a suitable UTxO is available.
    let total_funding_draw = total_pool_bump + total_fulfillment_subsidy;
    let mut wallet_change: Option<(u64, crate::cardano_types::Value)> = None;
    if funding_value_opt.is_none() && total_funding_draw > 0 {
        bail!(
            "min-ada support of {total_funding_draw} lovelace needed (pools \
             {total_pool_bump}, fulfillments {total_fulfillment_subsidy}) but \
             no funding UTxO was provided"
        );
    }
    if let Some(funding_value) = funding_value_opt {
        use num_traits::ToPrimitive;
        use pallas_primitives::NonEmptyKeyValuePairs;
        let ada_asset_local = AssetClass { policy: vec![], token: vec![] };
        let funding_ada = funding_value
            .get(&ada_asset_local)
            .unwrap()
            .to_u64()
            .context("funding UTxO ada doesn't fit u64")?;
        let change_ada = funding_ada.checked_sub(total_funding_draw)
            .with_context(|| format!(
                "funding UTxO ada ({funding_ada}) insufficient for min-ada support ({total_funding_draw})"
            ))?;
        if change_ada < SCOOPER_CHANGE_MIN_ADA {
            bail!(
                "scooper change ({change_ada} lovelace) below min UTxO; \
                 pick a larger funding UTxO"
            );
        }
        let scooper_change_addr = {
            let addr = ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Key(scooper_keyhash),
                ShelleyDelegationPart::Null,
            );
            PallasBytes::from(addr.to_vec())
        };
        // Mirror native tokens from funding into the change output.
        let mut multiasset_pairs: Vec<(Hash<28>, NonEmptyKeyValuePairs<PallasBytes, PositiveCoin>)> =
            Vec::new();
        for (policy_bytes, tokens) in &funding_value.0 {
            if policy_bytes.is_empty() {
                continue;
            }
            let policy_hash: Hash<28> = Hash::from(policy_bytes.as_slice());
            let mut token_pairs: Vec<(PallasBytes, PositiveCoin)> = Vec::new();
            for (name_bytes, qty) in tokens {
                let amt = qty.clone().unwrap().to_u64().unwrap_or(0);
                if let Ok(pc) = PositiveCoin::try_from(amt) {
                    token_pairs.push((PallasBytes::from(name_bytes.clone()), pc));
                }
            }
            if !token_pairs.is_empty() {
                multiasset_pairs.push((policy_hash, NonEmptyKeyValuePairs::Def(token_pairs)));
            }
        }
        let change_value = if multiasset_pairs.is_empty() {
            ConwayValue::Coin(change_ada)
        } else {
            ConwayValue::Multiasset(change_ada, NonEmptyKeyValuePairs::Def(multiasset_pairs))
        };
        outputs.push(TransactionOutput::PostAlonzo(
            pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
                address: scooper_change_addr,
                value: change_value,
                datum_option: None,
                script_ref: None,
            },
        ));
        // Non-fatal: warn if pallas wouldn't have accepted this change output
        // (e.g. funding UTxO carried a lot of tokens and 1 ADA isn't enough
        // for their minUtxo). Bail rather than ship an invalid tx.
        let needed_for_change =
            compute_output_min_ada(outputs.last().unwrap())?;
        if change_ada < needed_for_change {
            bail!(
                "scooper change ({change_ada} lovelace) below min UTxO for its size \
                 ({needed_for_change}); funding UTxO needs more ada or fewer tokens"
            );
        }
        let mut change_val = funding_value.clone();
        change_val.insert(
            &AssetClass { policy: vec![], token: vec![] },
            crate::bigint::BigInt::from(change_ada),
        );
        wallet_change = Some(((outputs.len() - 1) as u64, change_val));
    }

    // ── Step 8.5: LP mint/burn for deposits and withdraws ──────────────────
    //
    // Each pool with a non-zero net LP delta (`lp_minted - lp_burned`) emits
    // one entry under the pool_mint policy with asset name
    // `0014df10 ++ pool_ident`. Quantity is positive for net mint (deposit-
    // heavy) and negative for net burn (withdraw-heavy). The minting redeemer
    // is `PoolMintRedeemer::MintLP { pool_ident }` — the policy's `only_own_lp`
    // check doesn't constrain sign, so the same redeemer covers both.
    // The mint redeemer's index matches the policy's position in the sorted
    // mint map (always 0 since we only use pool_mint).
    let mint = {
        use pallas_primitives::{NonEmptyKeyValuePairs, NonZeroInt};
        use num_traits::{ToPrimitive, Zero};
        let mut asset_pairs: Vec<(PallasBytes, NonZeroInt)> = Vec::new();
        for (i, batch) in batches.iter().enumerate() {
            let net = &per_pool[i].lp_minted - &per_pool[i].lp_burned;
            if net.is_zero() { continue; }
            let qty: i64 = net.clone().unwrap()
                .to_i64()
                .context("net LP delta doesn't fit in i64")?;
            let mut name = vec![0x00, 0x14, 0xdf, 0x10];
            name.extend_from_slice(batch.pool.pool_datum.identifier.to_bytes());
            asset_pairs.push((
                PallasBytes::from(name),
                NonZeroInt::try_from(qty).expect("net LP delta nonzero"),
            ));
        }
        // Butane mint entries (one policy across all legs; aggregate).
        let mut butane_pairs: Vec<(PallasBytes, NonZeroInt)> = Vec::new();
        let mut butane_policy: Option<pallas_primitives::Hash<28>> = None;
        {
            let mut agg: std::collections::BTreeMap<Vec<u8>, i64> = Default::default();
            for p in &butane_pieces {
                butane_policy = Some(
                    p.mint_policy.as_slice().try_into().expect("28-byte policy"),
                );
                for (name, qty) in &p.mint_assets {
                    *agg.entry(name.clone()).or_default() += qty;
                }
            }
            for (name, qty) in agg {
                if qty != 0 {
                    butane_pairs.push((
                        PallasBytes::from(name),
                        NonZeroInt::try_from(qty).expect("nonzero"),
                    ));
                }
            }
        }

        if asset_pairs.is_empty() && butane_pairs.is_empty() {
            None
        } else {
            // Sort each policy's assets by name; policies sort in the map
            // below (mint redeemer indices follow sorted policy order).
            asset_pairs.sort_by(|a, b| {
                let av: Vec<u8> = a.0.clone().into();
                let bv: Vec<u8> = b.0.clone().into();
                av.cmp(&bv)
            });
            let policy = exec.module_scripts.pool_mint.hash;
            let mint_redeemer_data = if asset_pairs.is_empty() {
                None
            } else {
                // One Mint redeemer per minting policy. With only pool_mint
                // here, every entry shares it — but the contract reads
                // `pool_ident` from the redeemer, so all entries must target
                // the same pool. Today we restrict to a single pool with a
                // non-zero LP delta per tx.
                let pool_idents: Vec<_> = batches.iter().enumerate()
                    .filter(|(i, _)| {
                        !(&per_pool[*i].lp_minted - &per_pool[*i].lp_burned).is_zero()
                    })
                    .map(|(_, b)| b.pool.pool_datum.identifier.clone())
                    .collect();
                if pool_idents.len() != 1 {
                    anyhow::bail!(
                        "multi-pool LP mint/burn in one tx isn't supported by pool_mint \
                         (only_own_lp check rejects mixed lp_names); got {} pools",
                        pool_idents.len()
                    );
                }
                let r = PoolMintRedeemer::MintLP { pool_ident: pool_idents.into_iter().next().unwrap() };
                Some(r.to_plutus())
            };
            // Assemble the multi-policy mint map in sorted-policy order and
            // assign each policy's mint redeemer its map index.
            let mut policies: Vec<(pallas_primitives::Hash<28>, Vec<(PallasBytes, NonZeroInt)>, Option<pallas_primitives::PlutusData>)> = Vec::new();
            if !asset_pairs.is_empty() {
                policies.push((policy, asset_pairs, mint_redeemer_data));
            }
            if let (Some(bp), false) = (butane_policy, butane_pairs.is_empty()) {
                let butane_redeemer = butane_pieces
                    .first()
                    .map(|p| p.mint_redeemer.clone())
                    .expect("butane pairs imply pieces");
                policies.push((bp, butane_pairs, Some(butane_redeemer)));
            }
            policies.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
            let mut map_entries = Vec::new();
            for (idx, (pol, pairs, redeemer)) in policies.into_iter().enumerate() {
                if let Some(data) = redeemer {
                    let key = RedeemersKey { tag: RedeemerTag::Mint, index: idx as u32 };
                    redeemer_info.push((key.clone(), data, lookup_eu(&key)));
                }
                map_entries.push((pol, NonEmptyKeyValuePairs::Def(pairs)));
            }
            Some(NonEmptyKeyValuePairs::Def(map_entries))
        }
    };

    // ── Step 9: Assemble redeemer map ──────────────────────────────────────
    //
    // The ledger compares our `script_data_hash` against a hash it computes
    // from the canonical-ordered redeemer map (sorted by tag, then index).
    // If we emit entries in insertion order, the bytes don't match and Conway
    // rejects with ScriptIntegrityHashMismatch — so sort here.
    let mut redeemer_pairs: Vec<(RedeemersKey, RedeemersValue)> = redeemer_info
        .iter()
        .map(|(key, data, eu)| (key.clone(), RedeemersValue { data: data.clone(), ex_units: eu.clone() }))
        .collect();
    redeemer_pairs.sort_by_key(|(k, _)| (k.tag as u8, k.index));

    let redeemers =
        Redeemers::Map(pallas_primitives::NonEmptyKeyValuePairs::Def(redeemer_pairs));

    // ── Step 10: Compute script_data_hash ──────────────────────────────────
    //
    // Per Conway, script_data_hash = blake2b-256 of:
    //   encode(redeemers) || encode(datums)? || encode(language_views)
    // with datums omitted entirely when empty, and language_views encoded as
    // a canonical map (V1 wrapped in bytes, V2/V3 raw arrays). Pallas's
    // `ScriptData::hash` implements the canonical encoding exactly — match it
    // here so the value we put in tx_body equals what the ledger computes
    // while validating.
    let script_data_hash: Hash<32> = {
        let mut buf = Vec::new();
        minicbor::encode(&redeemers, &mut buf).expect("encode redeemers");
        // No attached datums (we only consume inline-datum UTxOs), so the
        // datums section is omitted — see ScriptData::hash in pallas-primitives.
        // Language views must cover exactly the languages this tx executes:
        // V3 always; V2 only when butane legs are present (three of the
        // deposit validators are V2). A V2 section on a V3-only tx — or the
        // reverse — makes the node's script integrity hash disagree.
        if butane_pieces.is_empty() {
            buf.extend_from_slice(language_views);
        } else {
            let v2 = exec.plutus_v2_cost_model.as_deref().with_context(|| {
                "butane legs present but plutus-v2-cost-model is not configured"
            })?;
            let multi = crate::sundaev4::submit::encode_language_views_multi(
                &exec.plutus_v3_cost_model,
                Some(v2),
            );
            buf.extend_from_slice(&multi);
        }
        Hasher::<256>::hash(&buf)
    };

    // ── Step 11: Assemble TransactionBody ──────────────────────────────────

    let ttl = current_slot + VALIDITY_RANGE;

    let body = conway::PseudoTransactionBody {
        inputs: sorted_inputs.into(),
        outputs,
        fee: tx_fee,
        ttl: Some(ttl),
        certificates: None,
        withdrawals: Some(pallas_primitives::NonEmptyKeyValuePairs::Def(
            withdrawals
                .into_iter()
                .map(|(account, _)| (account, 0u64))
                .collect(),
        )),
        auxiliary_data_hash: None,
        validity_interval_start: Some(current_slot),
        mint,
        script_data_hash: Some(script_data_hash),
        collateral: pallas_primitives::NonEmptySet::from_vec(vec![collateral_utxo.clone()]),
        required_signers: Some(
            pallas_primitives::NonEmptySet::from_vec(vec![scooper_keyhash]).unwrap(),
        ),
        network_id: None,
        collateral_return: Some(TransactionOutput::PostAlonzo(
            pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
                address: {
                    let scooper_addr = ShelleyAddress::new(
                        Network::Testnet,
                        ShelleyPaymentPart::Key(scooper_keyhash),
                        ShelleyDelegationPart::Null,
                    );
                    PallasBytes::from(scooper_addr.to_vec())
                },
                value: build_collateral_return_value(collateral_value, (tx_fee * 3).div_ceil(2))?,
                datum_option: None,
                script_ref: None,
            },
        )),
        total_collateral: Some((tx_fee * 3).div_ceil(2)),
        reference_inputs: pallas_primitives::NonEmptySet::from_vec(all_ref_inputs.clone()),
        voting_procedures: None,
        proposal_procedures: None,
        treasury_value: None,
        donation: None,
    };

    // ── Step 12: Sign ──────────────────────────────────────────────────────

    let body_cbor = minicbor::to_vec(&body).context("encode tx body")?;
    let body_hash: Hash<32> = Hasher::<256>::hash(&body_cbor);
    let signature = sk.sign(body_hash);

    let witness_set = WitnessSet {
        vkeywitness: Some(
            pallas_primitives::NonEmptySet::from_vec(vec![VKeyWitness {
                vkey: PallasBytes::from(pk_bytes.to_vec()),
                signature: PallasBytes::from(signature.as_ref().to_vec()),
            }])
            .unwrap(),
        ),
        native_script: None,
        bootstrap_witness: None,
        plutus_v1_script: None,
        plutus_data: None,
        redeemer: Some(redeemers),
        plutus_v2_script: None,
        plutus_v3_script: None,
    };

    let tx_hash_hex = hex::encode(body_hash);

    // ── Step 13: Build resolved inputs for evaluator ───────────────────────

    let mut resolved_inputs = BTreeMap::new();
    {
        // Conversion-primary orders live outside any pool batch.
        let order_addr_bytes = {
            let order_addr = ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Script(exec.module_scripts.order.hash),
                ShelleyDelegationPart::Null,
            );
            order_addr.to_vec()
        };
        for c in plan.conversions.iter().filter(|c| c.primary) {
            resolved_inputs.insert(c.order.input.clone(), ResolvedTxOut {
                address: order_addr_bytes.clone(),
                value: c.order.value.clone(),
                datum: DatumOption::InlineDatum(c.order.datum.clone().to_plutus()),
                script_ref: None,
            });
        }
    }
    for batch in batches {
        resolved_inputs.insert(batch.pool.input.clone(), ResolvedTxOut {
            address: pool_address.to_vec(),
            value: batch.pool.value.clone(),
            datum: DatumOption::InlineDatum(batch.pool.pool_datum.clone().to_plutus()),
            script_ref: None,
        });
        let order_addr_bytes = {
            let order_addr = ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Script(exec.module_scripts.order.hash),
                ShelleyDelegationPart::Null,
            );
            order_addr.to_vec()
        };
        for swap in &batch.swaps {
            resolved_inputs.insert(swap.order.input.clone(), ResolvedTxOut {
                address: order_addr_bytes.clone(),
                value: swap.order.value.clone(),
                datum: DatumOption::InlineDatum(swap.order.datum.clone().to_plutus()),
                script_ref: None,
            });
        }
        for dep in &batch.deposits {
            resolved_inputs.insert(dep.order.input.clone(), ResolvedTxOut {
                address: order_addr_bytes.clone(),
                value: dep.order.value.clone(),
                datum: DatumOption::InlineDatum(dep.order.datum.clone().to_plutus()),
                script_ref: None,
            });
        }
        for wd in &batch.withdraws {
            resolved_inputs.insert(wd.order.input.clone(), ResolvedTxOut {
                address: order_addr_bytes.clone(),
                value: wd.order.value.clone(),
                datum: DatumOption::InlineDatum(wd.order.datum.clone().to_plutus()),
                script_ref: None,
            });
        }
        for c in &batch.claims {
            resolved_inputs.insert(c.order.input.clone(), ResolvedTxOut {
                address: order_addr_bytes.clone(),
                value: c.order.value.clone(),
                datum: DatumOption::InlineDatum(c.order.datum.clone().to_plutus()),
                script_ref: None,
            });
        }
    }
    // Funding UTxO — vkey-locked, so it doesn't trigger a script during eval
    // but must still appear in resolved_inputs so the evaluator can resolve it.
    if let (Some(funding_oref), Some(funding_value)) = (funding_oref_opt.as_ref(), funding_value_opt) {
        let scooper_addr = ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::Key(scooper_keyhash),
            ShelleyDelegationPart::Null,
        );
        resolved_inputs.insert(
            crate::cardano_types::TransactionInput(funding_oref.clone()),
            ResolvedTxOut {
                address: scooper_addr.to_vec(),
                value: funding_value.clone(),
                datum: DatumOption::None,
                script_ref: None,
            },
        );
    }

    // Build resolved reference inputs
    let mut resolved_ref_inputs = BTreeMap::new();
    for ref_input_key in all_ref_inputs.iter() {
        let ref_input_ct = crate::cardano_types::TransactionInput(ref_input_key.clone());
        if let Some(txo) = ref_utxo_outputs.get(&ref_input_ct) {
            resolved_ref_inputs.insert(ref_input_ct, ResolvedTxOut {
                address: txo.address.to_vec(),
                value: txo.value.clone(),
                datum: match &txo.datum {
                    crate::cardano_types::RawDatum::None => DatumOption::None,
                    crate::cardano_types::RawDatum::Inline(d) => DatumOption::InlineDatum(d.clone()),
                    crate::cardano_types::RawDatum::Hash(h) => DatumOption::DatumHash(*h),
                },
                script_ref: txo.script_ref.as_ref().map(compute_script_ref_hash),
            });
        }
    }
    if !butane_pieces.is_empty() {
        if let Some(rt) = butane {
            for (input, output) in rt.resolved_ref_outputs() {
                if let conway::TransactionOutput::Legacy(body) = &output {
                    resolved_ref_inputs.insert(input, ResolvedTxOut {
                        address: body.address.to_vec(),
                        value: legacy_value_to_internal(&body.amount),
                        datum: match &body.datum_hash {
                            Some(h) => DatumOption::DatumHash(*h),
                            None => DatumOption::None,
                        },
                        script_ref: None,
                    });
                    continue;
                }
                let conway::TransactionOutput::PostAlonzo(body) = &output else {
                    continue;
                };
                resolved_ref_inputs.insert(input, ResolvedTxOut {
                    address: body.address.to_vec(),
                    value: conway_value_to_internal(&body.value),
                    datum: match &body.datum_option {
                        Some(conway::PseudoDatumOption::Data(d)) => {
                            let pd: pallas_primitives::PlutusData =
                                minicbor::decode(&minicbor::to_vec(&d.0).unwrap())
                                    .expect("re-decode inline datum");
                            DatumOption::InlineDatum(pd)
                        }
                        Some(conway::PseudoDatumOption::Hash(h)) => DatumOption::DatumHash(*h),
                        None => DatumOption::None,
                    },
                    script_ref: body.script_ref.as_ref().map(|sr| {
                        let (tag, bytes): (u8, &[u8]) = match &sr.0 {
                            conway::PseudoScript::PlutusV1Script(s) => (1, s.0.as_ref()),
                            conway::PseudoScript::PlutusV2Script(s) => (2, s.0.as_ref()),
                            conway::PseudoScript::PlutusV3Script(s) => (3, s.0.as_ref()),
                            conway::PseudoScript::NativeScript(_) => (0, &[]),
                        };
                        let mut pre = Vec::with_capacity(1 + bytes.len());
                        pre.push(tag);
                        pre.extend_from_slice(bytes);
                        Hasher::<224>::hash(&pre)
                    }),
                });
            }
        }
    }
    resolved_ref_inputs.insert(settings.input.clone(), ResolvedTxOut {
        address: {
            let settings_addr = ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Script(exec.module_scripts.settings.hash),
                ShelleyDelegationPart::Null,
            );
            settings_addr.to_vec()
        },
        value: settings.value.clone(),
        datum: DatumOption::InlineDatum(settings.datum.clone().to_plutus()),
        script_ref: None,
    });
    // OrderConfig settings entries — the order_validator's withdraw handler
    // resolves `config.ref_index` to one of these and checks its value
    // carries a token under settings_policy matching `config.token` and that
    // its inline datum decodes as `OrderConfig`.
    let settings_addr_bytes = {
        let settings_addr = ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::Script(exec.module_scripts.settings.hash),
            ShelleyDelegationPart::Null,
        );
        settings_addr.to_vec()
    };
    for (_, oc) in &unique_order_configs {
        resolved_ref_inputs.insert(oc.input.clone(), ResolvedTxOut {
            address: settings_addr_bytes.clone(),
            value: oc.value.clone(),
            datum: DatumOption::InlineDatum(oc.config.clone().to_plutus()),
            script_ref: None,
        });
    }

    // ── Step 14: Build predicted pool UTxOs ─────────────────────────────────

    let mut predicted_pools = Vec::with_capacity(m_pools);
    for (batch_idx, &out_idx) in pool_output_order.iter().enumerate() {
        let batch = &batches[out_idx];
        let predicted_input = crate::cardano_types::TransactionInput::new(body_hash, batch_idx as u64);
        let mut predicted_value = batch.pool.value.clone();
        for (asset, new_amount) in &per_pool[out_idx].final_assets_actual {
            if asset.policy.is_empty() && asset.token.is_empty() {
                continue; // ADA handled below
            }
            predicted_value.insert(asset, new_amount.clone());
        }
        // Set predicted ADA to match the actual pool output (input ADA + delta)
        {
            use num_traits::ToPrimitive;
            let pool_ada = batch.pool.value.get(&ada_asset).clone().unwrap().to_u64().unwrap_or(0);
            let predicted_ada = (pool_ada as i128 + pool_ada_deltas[out_idx] as i128)
                .max(POOL_MIN_ADA as i128) as u64;
            predicted_value.insert(&ada_asset, BigInt::from(predicted_ada as i64));
        }
        let predicted_pool = SundaeV4Pool {
            input: predicted_input.clone(),
            value: predicted_value,
            pool_datum: per_pool[out_idx].updated_datum.clone(),
            pool_type: batch.pool.pool_type.clone(),
            slot: current_slot,
            fee_split_config: batch.pool.fee_split_config.clone(),
        };
        predicted_pools.push((batch.pool_ident.clone(), predicted_input, predicted_pool));
    }

    let tx = conway::PseudoTx {
        transaction_body: body,
        transaction_witness_set: witness_set,
        success: true,
        auxiliary_data: pallas_primitives::Nullable::<conway::AuxiliaryData>::Null,
    };

    let tx_cbor = minicbor::to_vec(&tx).context("encode tx")?;

    // Sum the raw CBOR byte size of every reference-script attached to this
    // tx, for the Conway-era tiered ref-script fee component.
    let butane_ref_script_bytes: u64 = butane_pieces
        .first()
        .and_then(|_| butane)
        .map(|rt| {
            // Every butane ref-script UTxO rides in all_ref_inputs when
            // legs are present; their bytes pay the tiered Conway
            // ref-script fee like any other reference script.
            rt.scripts.values().map(|ds| ds.script_bytes.len() as u64).sum()
        })
        .unwrap_or(0);
    let sundae_ref_script_bytes: u64 = all_ref_inputs.iter()
        .filter_map(|input| {
            let ct_input = crate::cardano_types::TransactionInput(input.clone());
            let txo = ref_utxo_outputs.get(&ct_input)
                .or_else(|| {
                    if input == &settings.input.0 {
                        // settings UTxO isn't in ref_utxo_outputs; skip
                        None
                    } else {
                        None
                    }
                })?;
            match &txo.script_ref {
                Some(crate::cardano_types::ScriptRef::PlutusV1(s)) => Some(s.as_ref().len() as u64),
                Some(crate::cardano_types::ScriptRef::PlutusV2(s)) => Some(s.as_ref().len() as u64),
                Some(crate::cardano_types::ScriptRef::PlutusV3(s)) => Some(s.as_ref().len() as u64),
                Some(crate::cardano_types::ScriptRef::Native(_)) | None => None,
            }
        })
        .sum();
    let total_ref_script_bytes: u64 = butane_ref_script_bytes + sundae_ref_script_bytes;

    Ok(MultiPoolBuildResult {
        cbor: tx_cbor,
        tx_hash: body_hash,
        tx_hash_hex,
        total_ref_script_bytes,
        tx_body: tx.transaction_body,
        resolved_inputs,
        resolved_ref_inputs,
        redeemers: redeemer_info,
        predicted_pools,
        ttl,
        wallet_change,
    })
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Compute the script hash from a ScriptRef for the ScriptContext's TxOut encoding.
fn compute_script_ref_hash(script_ref: &crate::cardano_types::ScriptRef) -> Hash<28> {
    use pallas_crypto::hash::Hasher;
    match script_ref {
        crate::cardano_types::ScriptRef::Native(n) => {
            use pallas_traverse::ComputeHash;
            n.compute_hash()
        }
        crate::cardano_types::ScriptRef::PlutusV1(s) => {
            let cbor: &[u8] = s.as_ref();
            let mut preimage = Vec::with_capacity(1 + cbor.len());
            preimage.push(0x01);
            preimage.extend_from_slice(cbor);
            Hasher::<224>::hash(&preimage)
        }
        crate::cardano_types::ScriptRef::PlutusV2(s) => {
            let cbor: &[u8] = s.as_ref();
            let mut preimage = Vec::with_capacity(1 + cbor.len());
            preimage.push(0x02);
            preimage.extend_from_slice(cbor);
            Hasher::<224>::hash(&preimage)
        }
        crate::cardano_types::ScriptRef::PlutusV3(s) => {
            let cbor: &[u8] = s.as_ref();
            let mut preimage = Vec::with_capacity(1 + cbor.len());
            preimage.push(0x03);
            preimage.extend_from_slice(cbor);
            Hasher::<224>::hash(&preimage)
        }
    }
}

/// Either a 32-byte standard ed25519 key or a 64-byte Cardano-extended key.
/// Cardano HD-derived keys (BIP32 / CIP-1852) are always extended; the
/// standard form is only useful for one-off keys provided as a raw seed.
pub enum AnySecretKey {
    Standard(SecretKey),
    Extended(SecretKeyExtended),
}

impl AnySecretKey {
    pub fn public_key(&self) -> PublicKey {
        match self {
            AnySecretKey::Standard(k) => k.public_key(),
            AnySecretKey::Extended(k) => k.public_key(),
        }
    }
    pub fn sign(&self, msg: impl AsRef<[u8]>) -> Signature {
        match self {
            AnySecretKey::Standard(k) => k.sign(msg),
            AnySecretKey::Extended(k) => k.sign(msg),
        }
    }
}

fn parse_secret_key(key_str: &str) -> Result<AnySecretKey> {
    let hex_str = if key_str.trim_start().starts_with('{') {
        // Cardano JSON envelope: {"type":"...","cborHex":"5820<64hex>"}
        let envelope: serde_json::Value =
            serde_json::from_str(key_str).context("invalid signing key JSON envelope")?;
        let cbor_hex = envelope["cborHex"]
            .as_str()
            .context("missing cborHex field in signing key envelope")?;
        // Strip the CBOR prefix "5820" (bytes tag for 32-byte) or "5840" (64-byte).
        if let Some(s) = cbor_hex.strip_prefix("5820") {
            s.to_string()
        } else if let Some(s) = cbor_hex.strip_prefix("5840") {
            s.to_string()
        } else {
            anyhow::bail!("unexpected cborHex prefix (expected 5820 or 5840)")
        }
    } else {
        key_str.to_string()
    };
    let bytes = hex::decode(&hex_str).context("invalid secret key hex")?;
    match bytes.len() {
        32 => {
            let arr: [u8; 32] = bytes.try_into().unwrap();
            Ok(AnySecretKey::Standard(SecretKey::from(arr)))
        }
        64 => {
            let arr: [u8; 64] = bytes.try_into().unwrap();
            let ext = SecretKeyExtended::from_bytes(arr)
                .map_err(|e| anyhow::anyhow!("invalid extended ed25519 secret key: {e}"))?;
            Ok(AnySecretKey::Extended(ext))
        }
        n => anyhow::bail!("secret key must be 32 or 64 bytes, got {n}"),
    }
}

/// Build the pool output Value (pallas conway::Value) from the pool's existing
/// value with asset amounts adjusted to reflect the swap.
///
/// `ada_delta` is the net ADA change for this pool's output:
///   - Negative when ADA leaves the pool (TOKEN→ADA sell orders)
///   - Positive when ADA enters via routing continuations
///   - Zero for TOKEN/TOKEN pools or when flows cancel
///
/// Regular buy-order ADA (ADA→TOKEN) does NOT appear here because that ADA
/// stays in the fulfillment output.
fn build_pool_output_value(
    pool: &SundaeV4Pool,
    new_assets: &[(AssetClass, BigInt)],
    ada_delta: i64,
) -> Result<ConwayValue> {
    use num_traits::ToPrimitive;
    use pallas_primitives::NonEmptyKeyValuePairs;

    // Start from the pool's current ADA
    let ada_asset = AssetClass {
        policy: vec![],
        token: vec![],
    };
    let ada_amount = pool.value.get(&ada_asset);
    let mut lovelace = ada_amount
        .clone()
        .unwrap()
        .to_u64()
        .unwrap_or(POOL_MIN_ADA)
        .max(POOL_MIN_ADA);

    // Apply net ADA delta (positive = pool gains ADA, negative = pool loses ADA)
    lovelace = (lovelace as i128 + ada_delta as i128).max(POOL_MIN_ADA as i128) as u64;

    // Collect all native tokens from the pool's current value, then override
    // the pool asset amounts with new values
    let mut policy_map: std::collections::BTreeMap<
        Vec<u8>,
        std::collections::BTreeMap<Vec<u8>, u64>,
    > = std::collections::BTreeMap::new();

    // Copy all existing native tokens from the pool
    for (policy_bytes, tokens) in &pool.value.0 {
        if policy_bytes.is_empty() {
            continue; // skip ADA
        }
        for (token_bytes, qty) in tokens {
            use num_traits::ToPrimitive;
            let amt = qty.clone().unwrap().to_u64().unwrap_or(0);
            if amt > 0 {
                policy_map
                    .entry(policy_bytes.clone())
                    .or_default()
                    .insert(token_bytes.clone(), amt);
            }
        }
    }

    // Override the pool's tracked asset amounts
    for (asset, amount) in new_assets {
        if asset.policy.is_empty() && asset.token.is_empty() {
            continue; // ADA handled separately
        }
        use num_traits::ToPrimitive;
        let amt = amount.clone().unwrap().to_u64().unwrap_or(0);
        if amt > 0 {
            policy_map
                .entry(asset.policy.clone())
                .or_default()
                .insert(asset.token.clone(), amt);
        }
    }

    if policy_map.is_empty() {
        return Ok(ConwayValue::Coin(lovelace));
    }

    let multiasset_pairs: Vec<(Hash<28>, NonEmptyKeyValuePairs<PallasBytes, PositiveCoin>)> =
        policy_map
            .into_iter()
            .filter_map(|(policy, tokens)| {
                let policy_hash: Hash<28> = Hash::from(policy.as_slice());
                let token_pairs: Vec<(PallasBytes, PositiveCoin)> = tokens
                    .into_iter()
                    .filter_map(|(name, qty)| {
                        PositiveCoin::try_from(qty)
                            .ok()
                            .map(|pc| (PallasBytes::from(name), pc))
                    })
                    .collect();
                if token_pairs.is_empty() {
                    None
                } else {
                    Some((policy_hash, NonEmptyKeyValuePairs::Def(token_pairs)))
                }
            })
            .collect();

    if multiasset_pairs.is_empty() {
        return Ok(ConwayValue::Coin(lovelace));
    }

    Ok(ConwayValue::Multiasset(
        lovelace,
        NonEmptyKeyValuePairs::Def(multiasset_pairs),
    ))
}

/// Resolve a pool's LP-token AssetClass. Sundae's LP asset is minted under
/// the pool_mint policy with name `0014df10 ++ pool_ident` (CIP-67 label 222
/// for LP).
fn pool_lp_asset(
    exec: &ScooperExecution,
    pool: &SundaeV4Pool,
) -> Result<AssetClass> {
    let mut name = vec![0x00, 0x14, 0xdf, 0x10];
    name.extend_from_slice(pool.pool_datum.identifier.to_bytes());
    Ok(AssetClass {
        policy: exec.module_scripts.pool_mint.hash.as_ref().to_vec(),
        token: name,
    })
}

/// Build fulfillment output value for a Deposit order. Mirrors actions/order.ts'
/// deposit fulfillment: take the order's input value, subtract each `dx[i]`
/// (the per-asset contribution to the pool), subtract the scooper fee in ADA,
/// then add the freshly minted LP tokens. Any remaining offered asset (because
/// the user offered more than fit a proportional unit) stays in the output
/// as surplus.
fn build_deposit_fulfillment_value(
    order_value: &crate::cardano_types::Value,
    pool_assets: &[(AssetClass, BigInt)],
    dx: &[BigInt],
    lp_asset: &AssetClass,
    lp_minted: &BigInt,
    fee: u64,
) -> Result<ConwayValue> {
    use num_traits::ToPrimitive;
    use pallas_primitives::NonEmptyKeyValuePairs;

    let ada_asset = AssetClass { policy: vec![], token: vec![] };
    let mut result = order_value.clone();
    for (i, (asset, _)) in pool_assets.iter().enumerate() {
        let cur = result.get(asset);
        result.insert(asset, &cur - &dx[i]);
    }
    let cur_ada = result.get(&ada_asset);
    result.insert(&ada_asset, &cur_ada - &BigInt::from(fee as i64));
    let cur_lp = result.get(lp_asset);
    result.insert(lp_asset, &cur_lp + lp_minted);

    let lovelace = result.get(&ada_asset)
        .clone()
        .unwrap()
        .to_u64()
        .context("deposit fulfillment ADA doesn't fit in u64")?;
    let mut policy_map: std::collections::BTreeMap<
        Vec<u8>,
        std::collections::BTreeMap<Vec<u8>, u64>,
    > = std::collections::BTreeMap::new();
    for (policy, tokens) in &result.0 {
        if policy.is_empty() { continue; }
        for (token_name, qty) in tokens {
            let qty_u64 = qty.clone().unwrap().to_u64().unwrap_or(0);
            if qty_u64 > 0 {
                policy_map
                    .entry(policy.clone())
                    .or_default()
                    .insert(token_name.clone(), qty_u64);
            }
        }
    }
    if policy_map.is_empty() {
        return Ok(ConwayValue::Coin(lovelace));
    }
    let multiasset_pairs: Vec<_> = policy_map
        .into_iter()
        .map(|(policy, tokens)| {
            let policy_hash: Hash<28> = Hash::from(policy.as_slice());
            let token_pairs: Vec<_> = tokens.into_iter()
                .map(|(name, qty)| (
                    PallasBytes::from(name),
                    PositiveCoin::try_from(qty).unwrap(),
                ))
                .collect();
            (policy_hash, NonEmptyKeyValuePairs::Def(token_pairs))
        })
        .collect();
    Ok(ConwayValue::Multiasset(lovelace, NonEmptyKeyValuePairs::Def(multiasset_pairs)))
}

/// Build the fulfillment output value for a Withdraw order.
///
/// Starts from the order's input value (which contains the LP tokens the
/// user offered), burns `lp_burned` of the pool's LP asset, adds the per-
/// asset `dy[i]` reserves the user receives back, and subtracts the
/// scooper fee in ADA.
fn build_withdraw_fulfillment_value(
    order_value: &crate::cardano_types::Value,
    pool_assets: &[(AssetClass, BigInt)],
    dy: &[BigInt],
    lp_asset: &AssetClass,
    lp_burned: &BigInt,
    fee: u64,
) -> Result<ConwayValue> {
    use num_traits::ToPrimitive;
    use pallas_primitives::NonEmptyKeyValuePairs;

    let ada_asset = AssetClass { policy: vec![], token: vec![] };
    let mut result = order_value.clone();
    let cur_lp = result.get(lp_asset);
    result.insert(lp_asset, &cur_lp - lp_burned);
    for (i, (asset, _)) in pool_assets.iter().enumerate() {
        let cur = result.get(asset);
        result.insert(asset, &cur + &dy[i]);
    }
    let cur_ada = result.get(&ada_asset);
    result.insert(&ada_asset, &cur_ada - &BigInt::from(fee as i64));

    let lovelace = result.get(&ada_asset)
        .clone()
        .unwrap()
        .to_u64()
        .context("withdraw fulfillment ADA doesn't fit in u64")?;
    let mut policy_map: std::collections::BTreeMap<
        Vec<u8>,
        std::collections::BTreeMap<Vec<u8>, u64>,
    > = std::collections::BTreeMap::new();
    for (policy, tokens) in &result.0 {
        if policy.is_empty() { continue; }
        for (token_name, qty) in tokens {
            let qty_u64 = qty.clone().unwrap().to_u64().unwrap_or(0);
            if qty_u64 > 0 {
                policy_map
                    .entry(policy.clone())
                    .or_default()
                    .insert(token_name.clone(), qty_u64);
            }
        }
    }
    if policy_map.is_empty() {
        return Ok(ConwayValue::Coin(lovelace));
    }
    let multiasset_pairs: Vec<_> = policy_map
        .into_iter()
        .map(|(policy, tokens)| {
            let policy_hash: Hash<28> = Hash::from(policy.as_slice());
            let token_pairs: Vec<_> = tokens.into_iter()
                .map(|(name, qty)| (
                    PallasBytes::from(name),
                    PositiveCoin::try_from(qty).unwrap(),
                ))
                .collect();
            (policy_hash, NonEmptyKeyValuePairs::Def(token_pairs))
        })
        .collect();
    Ok(ConwayValue::Multiasset(lovelace, NonEmptyKeyValuePairs::Def(multiasset_pairs)))
}

/// Build fulfillment output value from first principles:
/// fulfillment = order_value - offer - fee + swap_result
/// Convert an internal Value to a ConwayValue, dropping zero/negative
/// token quantities and requiring the lovelace to fit u64.
fn value_to_conway(result: &crate::cardano_types::Value) -> Result<ConwayValue> {
    use num_traits::ToPrimitive;
    use pallas_primitives::NonEmptyKeyValuePairs;

    let ada_asset = AssetClass { policy: vec![], token: vec![] };
    // Convert to ConwayValue
    let lovelace = result.get(&ada_asset)
        .clone()
        .unwrap()
        .to_u64()
        .context("fulfillment ADA doesn't fit in u64")?;

    // Collect native tokens (skip ADA and any with zero/negative quantity)
    let mut policy_map: std::collections::BTreeMap<
        Vec<u8>,
        std::collections::BTreeMap<Vec<u8>, u64>,
    > = std::collections::BTreeMap::new();

    for (policy, tokens) in &result.0 {
        if policy.is_empty() {
            continue; // ADA handled above
        }
        for (token_name, qty) in tokens {
            let qty_u64 = qty.clone().unwrap().to_u64().unwrap_or(0);
            if qty_u64 > 0 {
                policy_map
                    .entry(policy.clone())
                    .or_default()
                    .insert(token_name.clone(), qty_u64);
            }
        }
    }

    if policy_map.is_empty() {
        return Ok(ConwayValue::Coin(lovelace));
    }

    let multiasset_pairs: Vec<_> = policy_map
        .into_iter()
        .map(|(policy, tokens)| {
            let policy_hash: Hash<28> = Hash::from(policy.as_slice());
            let token_pairs: Vec<_> = tokens
                .into_iter()
                .map(|(name, qty)| {
                    (
                        PallasBytes::from(name),
                        PositiveCoin::try_from(qty).unwrap(),
                    )
                })
                .collect();
            (policy_hash, NonEmptyKeyValuePairs::Def(token_pairs))
        })
        .collect();

    Ok(ConwayValue::Multiasset(
        lovelace,
        NonEmptyKeyValuePairs::Def(multiasset_pairs),
    ))
}

/// Fulfillment = order value + each signed move − fee. A generalization of
/// [`build_fulfillment_value_from_order`] for ops that touch several assets
/// at once (multi-receive claims).
fn build_fulfillment_value_with_moves(
    order_value: &crate::cardano_types::Value,
    moves: &[(&AssetClass, BigInt)],
    fee: u64,
) -> Result<ConwayValue> {
    let mut result = order_value.clone();
    for (asset, delta) in moves {
        let cur = result.get(asset);
        result.insert(asset, &cur + delta);
    }
    let ada_asset = AssetClass { policy: vec![], token: vec![] };
    let cur_ada = result.get(&ada_asset);
    result.insert(&ada_asset, &cur_ada - &BigInt::from(fee as i64));
    value_to_conway(&result)
}

fn build_fulfillment_value_from_order(
    order_value: &crate::cardano_types::Value,
    offer_asset: &AssetClass,
    offer_amount: &BigInt,
    output_asset: &AssetClass,
    dy: &BigInt,
    fee: u64,
) -> Result<ConwayValue> {
    let ada_asset = AssetClass { policy: vec![], token: vec![] };

    // Start with the order's input value as a working copy
    let mut result = order_value.clone();

    // Subtract the offered asset
    let cur_offer = result.get(offer_asset);
    result.insert(offer_asset, &cur_offer - offer_amount);

    // Subtract the protocol fee (always ADA)
    let cur_ada = result.get(&ada_asset);
    result.insert(&ada_asset, &cur_ada - &BigInt::from(fee as i64));

    // Add the swap result
    let cur_out = result.get(output_asset);
    result.insert(output_asset, &cur_out + dy);

    value_to_conway(&result)
}

/// Resolve the order destination to a raw address byte vector.
fn resolve_destination(dest: &Destination, owner: &crate::multisig::Multisig) -> Result<Vec<u8>> {
    match dest {
        Destination::Fixed(plutus_addr, _datum) => plutus_address_to_bytes(plutus_addr),
        Destination::SelfDestination => {
            // SelfDestination: send back to the owner's key hash
            match owner {
                crate::multisig::Multisig::Signature(keyhash_bytes) => {
                    let hash: Hash<28> = keyhash_bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| anyhow::anyhow!("owner keyhash not 28 bytes"))?;
                    let addr = ShelleyAddress::new(
                        Network::Testnet,
                        ShelleyPaymentPart::Key(hash),
                        ShelleyDelegationPart::Null,
                    );
                    Ok(addr.to_vec())
                }
                _ => bail!("SelfDestination with non-Signature owner not supported"),
            }
        }
    }
}

/// Build the collateral return output value, subtracting total_collateral
/// from the ADA portion. Native assets are passed through unchanged.
fn build_collateral_return_value(
    value: &crate::cardano_types::Value,
    total_collateral: u64,
) -> Result<ConwayValue> {
    use num_traits::ToPrimitive;
    use pallas_primitives::NonEmptyKeyValuePairs;

    let ada_asset = crate::cardano_types::AssetClass { policy: vec![], token: vec![] };
    let lovelace = value
        .get(&ada_asset)
        .clone()
        .unwrap()
        .to_u64()
        .context("ada amount doesn't fit u64")?;
    let return_lovelace = lovelace
        .checked_sub(total_collateral)
        .context("collateral UTxO doesn't have enough ADA")?;

    // Cardano requires collateral return output to meet min UTxO (~858K lovelace).
    // Bail early rather than letting the node reject with BabbageOutputTooSmallUTxO.
    const MIN_COLLATERAL_RETURN: u64 = 1_000_000;
    if return_lovelace < MIN_COLLATERAL_RETURN {
        bail!(
            "collateral return ({} lovelace) below min UTxO; need a larger collateral UTxO",
            return_lovelace
        );
    }

    let mut policy_pairs: Vec<(Hash<28>, NonEmptyKeyValuePairs<PallasBytes, PositiveCoin>)> =
        Vec::new();

    for (policy_bytes, tokens) in &value.0 {
        if policy_bytes.is_empty() {
            continue; // skip ADA
        }
        let policy_hash: Hash<28> = Hash::from(policy_bytes.as_slice());
        let mut token_pairs: Vec<(PallasBytes, PositiveCoin)> = Vec::new();
        for (name_bytes, qty) in tokens {
            let amount = qty.clone().unwrap().to_u64().unwrap_or(0);
            if let Ok(pc) = PositiveCoin::try_from(amount) {
                token_pairs.push((PallasBytes::from(name_bytes.clone()), pc));
            }
        }
        if !token_pairs.is_empty() {
            policy_pairs.push((policy_hash, NonEmptyKeyValuePairs::Def(token_pairs)));
        }
    }

    if policy_pairs.is_empty() {
        Ok(ConwayValue::Coin(return_lovelace))
    } else {
        Ok(ConwayValue::Multiasset(
            return_lovelace,
            NonEmptyKeyValuePairs::Def(policy_pairs),
        ))
    }
}

/// Convert a PlutusAddress to raw address bytes.
/// Short display for an asset class: "ADA" for the ada asset, otherwise
/// hex(policy[..4]).hex(token). Used only by diagnostic logging.
fn short_asset(a: &AssetClass) -> String {
    if a.policy.is_empty() && a.token.is_empty() {
        return "ADA".to_string();
    }
    let pol = hex::encode(&a.policy);
    let tk = hex::encode(&a.token);
    let pol_short: String = pol.chars().take(8).collect();
    format!("{}.{}", pol_short, tk)
}

fn plutus_address_to_bytes(addr: &PlutusAddress) -> Result<Vec<u8>> {
    let payment = match &addr.payment_credential {
        Credential::VerificationKey(hash) => ShelleyPaymentPart::Key(*hash),
        Credential::Script(hash) => ShelleyPaymentPart::Script(*hash),
    };

    let delegation = match &addr.stake_credential {
        Some(Referenced::Inline(cred)) => match cred {
            Credential::VerificationKey(hash) => ShelleyDelegationPart::Key(*hash),
            Credential::Script(hash) => ShelleyDelegationPart::Script(*hash),
        },
        _ => ShelleyDelegationPart::Null,
    };

    let shelley = ShelleyAddress::new(Network::Testnet, payment, delegation);
    Ok(shelley.to_vec())
}


/// Convert a conway ledger Value into the internal Value map form.
fn conway_value_to_internal(v: &ConwayValue) -> crate::cardano_types::Value {
    let mut out = crate::cardano_types::Value::default();
    let ada = AssetClass { policy: vec![], token: vec![] };
    match v {
        ConwayValue::Coin(c) => {
            out.insert(&ada, BigInt::from(*c));
        }
        ConwayValue::Multiasset(c, ma) => {
            out.insert(&ada, BigInt::from(*c));
            for (policy, tokens) in ma.iter() {
                for (name, qty) in tokens.iter() {
                    let asset = AssetClass {
                        policy: policy.as_ref().to_vec(),
                        token: name.to_vec(),
                    };
                    out.insert(&asset, BigInt::from(u64::from(*qty)));
                }
            }
        }
    }
    out
}


/// Legacy (pre-Babbage array-form) output values use signed coin maps.
fn legacy_value_to_internal(
    v: &pallas_primitives::alonzo::Value,
) -> crate::cardano_types::Value {
    let mut out = crate::cardano_types::Value::default();
    let ada = AssetClass { policy: vec![], token: vec![] };
    match v {
        pallas_primitives::alonzo::Value::Coin(c) => {
            out.insert(&ada, BigInt::from(*c));
        }
        pallas_primitives::alonzo::Value::Multiasset(c, ma) => {
            out.insert(&ada, BigInt::from(*c));
            for (policy, tokens) in ma.iter() {
                for (name, qty) in tokens.iter() {
                    let asset = AssetClass {
                        policy: policy.as_ref().to_vec(),
                        token: name.to_vec(),
                    };
                    out.insert(&asset, BigInt::from(*qty));
                }
            }
        }
    }
    out
}
