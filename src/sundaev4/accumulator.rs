//! Incremental multi-pool transaction accumulator.
//!
//! Instead of grouping orders by pool and building one tx per pool, this module
//! lets the scooper add orders one at a time. Each order is executed against its
//! pool's running state. After all orders are accumulated, `into_batches()`
//! produces `Vec<Batch>` ready for the multi-pool tx builder.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::bigint::BigInt;
use crate::cardano_types::AssetClass;
use crate::sundaev3::Ident;
use crate::sundaev4::batch::{
    self, Batch, BatchOp, ContinuationSwap, GlobalOp, ResolvedSwap, RouteHopInfo, RouteInfo,
    RouteRef, ScoopPlan,
};
use crate::sundaev4::router::RoutingPlan;
use crate::sundaev4::swap_math;
use crate::sundaev4::types::SundaeV4Pool;

/// Per-pool running state within a multi-pool tx being built incrementally.
///
/// The accumulator mirrors the tx-builder's streaming walk so that
/// CL swap math (which depends on `total_lp` via virtual reserves) gives the
/// *same* dy at accumulation time as at tx-build time. Without this, the
/// router would route against the accumulator's projection and the tx-builder
/// would compute different dys, leading to value-conservation failures.
///
/// Specifically: `running_total_lp` is incrementally bumped per swap entry
/// (cumulative-target trick on protocol_lp), and grown/shrunk by
/// deposit/withdraw LP deltas. Use this for both router queries and swap
/// math.
#[derive(Clone)]
pub struct PoolAccum {
    pub pool: Arc<SundaeV4Pool>,
    pub ident: Ident,
    pub running_assets: Vec<(AssetClass, BigInt)>,
    /// Live LP including per-entry protocol_lp bumps and deposit/withdraw
    /// effects. Mirrors tx_builder's running_total_lp at this point in the walk.
    pub running_total_lp: BigInt,
    pub running_circ_lp: BigInt,
    pub swaps: Vec<ResolvedSwap>,
    pub continuations: Vec<ContinuationSwap>,
    pub deposits: Vec<crate::sundaev4::batch::ResolvedDeposit>,
    pub withdraws: Vec<crate::sundaev4::batch::ResolvedWithdraw>,
    /// Cumulative gross fee_budget across all swap entries in this pool.
    /// Used together with `cum_protocol_lp` for the per-entry protocol_lp
    /// distribution (cumulative-target trick).
    cum_gross_fb: BigInt,
    /// Cumulative protocol_lp captured so far = floor(cum_gross_fb * ps_num / ps_den).
    cum_protocol_lp: BigInt,
    /// Per-pool protocol_share (num, den). Snapped at first-touch from the
    /// pool's fee_split_config (or global default if unset).
    ps: (BigInt, BigInt),
    /// Interleaved order of swaps, continuations, and deposits.
    ops_order: Vec<BatchOp>,
    /// The order input behind the most recently appended op. The deployed
    /// route constraint's `check_route_uniqueness` walks a tx's order inputs
    /// in canonical (TxOutRef-sorted) order and requires each pool's
    /// transcript step claims to be strictly increasing along that walk —
    /// which is only satisfiable when the orders behind a pool's ops appear
    /// in canonical order. `check_canonical_append` enforces it.
    last_order_input: Option<crate::cardano_types::TransactionInput>,
}

impl PoolAccum {
    /// Reject an op whose order sorts canonically before an order already in
    /// this pool's batch — the resulting transcript could never satisfy
    /// route.ak's strictly-increasing per-pool step claims. The rejected
    /// order isn't lost; it just goes in a later tx. Multiple ops from the
    /// SAME order (multi-step routes) compare equal and pass.
    fn check_canonical_append(
        &mut self,
        order_input: &crate::cardano_types::TransactionInput,
    ) -> Result<(), String> {
        if let Some(last) = &self.last_order_input {
            if last > order_input {
                return Err(format!(
                    "canonical-order violation on pool {}: order {} sorts before order {} already in the batch (check_route_uniqueness would fail on-chain)",
                    self.ident, order_input, last,
                ));
            }
        }
        self.last_order_input = Some(order_input.clone());
        Ok(())
    }
}

/// Incrementally-built multi-pool transaction state.
///
/// `routes` and `global_seq` track cross-pool data the tx_builder needs to
/// recompute dy at tx-time and thread it through multi-hop cascades:
/// - `routes[r]` describes route `r`'s hop structure / fulfillment asset.
/// - `global_seq` records ops in the order they were added across all pools;
///   this is the topological order the tx-time streaming walk consumes.
#[derive(Clone)]
pub struct Accumulator {
    pub pools: BTreeMap<Ident, PoolAccum>,
    routes: Vec<RouteInfo>,
    conversions: Vec<batch::PlannedConversion>,
    /// Order ops were added across all pools, stored as `(pool_ident, op_idx_in_pool)`.
    /// Resolved to `(batch_idx, op_idx)` in `into_plan` once batches are materialised.
    global_seq_raw: Vec<(Ident, usize)>,
    protocol_share: (u64, u64),
}

impl Accumulator {
    pub fn new(protocol_share: (u64, u64)) -> Self {
        Self {
            pools: BTreeMap::new(),
            routes: Vec::new(),
            conversions: Vec::new(),
            global_seq_raw: Vec::new(),
            protocol_share,
        }
    }

    /// Initialise a fresh per-pool state from the chain pool.
    fn fresh_pool_accum(
        &self,
        pool_ident: &Ident,
        effective_pool: &Arc<SundaeV4Pool>,
    ) -> PoolAccum {
        // CS pools capture protocol revenue like CP/CL post-SUN-101 (cs_check
        // no longer forbids LP growth on swap entries). Must stay in lockstep
        // with tx_builder's per_pool_ps or predicted pool state diverges from
        // the built tx.
        let ps = effective_pool
            .fee_split_config
            .as_ref()
            .map(|c| (c.protocol_share.num.clone(), c.protocol_share.den.clone()))
            .unwrap_or_else(|| (
                BigInt::from(self.protocol_share.0),
                BigInt::from(self.protocol_share.1),
            ));
        PoolAccum {
            pool: effective_pool.clone(),
            ident: pool_ident.clone(),
            running_assets: effective_pool.pool_datum.assets.clone(),
            running_total_lp: effective_pool.pool_datum.total_lp.clone(),
            running_circ_lp: effective_pool.pool_datum.circulating_lp.clone(),
            swaps: Vec::new(),
            continuations: Vec::new(),
            deposits: Vec::new(),
            withdraws: Vec::new(),
            cum_gross_fb: BigInt::from(0),
            cum_protocol_lp: BigInt::from(0),
            ps,
            ops_order: Vec::new(),
            last_order_input: None,
        }
    }

    /// Build a transient pool map reflecting the accumulator's running state.
    /// Pools the accumulator hasn't touched fall through to `fallback`. Use
    /// this for router queries so the router accounts for prior orders'
    /// depletion of pool reserves.
    pub fn current_pool_view(
        &self,
        fallback: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
    ) -> BTreeMap<Ident, Arc<SundaeV4Pool>> {
        let mut view: BTreeMap<Ident, Arc<SundaeV4Pool>> = fallback.clone();
        for (ident, accum) in &self.pools {
            let mut synthetic = (*accum.pool).clone();
            synthetic.pool_datum.assets = accum.running_assets.clone();
            synthetic.pool_datum.total_lp = accum.running_total_lp.clone();
            synthetic.pool_datum.circulating_lp = accum.running_circ_lp.clone();
            view.insert(ident.clone(), Arc::new(synthetic));
        }
        view
    }

    /// Try to add an order targeting `pool_ident`. If the pool hasn't been
    /// touched yet, `effective_pool` initializes its running state.
    ///
    /// Returns `Ok(())` if the order was successfully executed against the
    /// pool's running reserves, or `Err(reason)` if it couldn't execute.
    pub fn try_add_order(
        &mut self,
        order: &Arc<crate::sundaev4::types::SundaeV4Order>,
        pool_ident: &Ident,
        effective_pool: &Arc<SundaeV4Pool>,
    ) -> Result<(), String> {
        let fresh = self.fresh_pool_accum(pool_ident, effective_pool);
        let accum = self.pools.entry(pool_ident.clone()).or_insert(fresh);
        accum.check_canonical_append(&order.input)?;

        let swap = batch::try_execute_order(
            order,
            &accum.running_assets,
            &accum.running_total_lp,
            &effective_pool.pool_type,
        )?;

        // Capture reserves before update for fee budget computation
        let prev_assets = accum.running_assets.clone();

        // Update running reserves
        accum.running_assets[swap.input_idx].1 =
            &accum.running_assets[swap.input_idx].1 + &swap.dx;
        accum.running_assets[swap.output_idx].1 =
            &accum.running_assets[swap.output_idx].1 - &swap.dy;

        // Per-entry protocol_lp bump (cumulative-target trick — see tx_builder).
        // Keeps running_total_lp in sync with what tx_builder will see, so any
        // CL dys computed against the post-bump LP match between accumulator
        // and tx_builder. CS pools have ps=(0, _) by current design so this is
        // a no-op for them.
        let fb = swap_math::compute_fee_budget(
            &effective_pool.pool_type,
            &prev_assets,
            &accum.running_assets,
            &accum.running_total_lp,
        );
        accum.cum_gross_fb = &accum.cum_gross_fb + &fb;
        let new_cum_protocol_lp = &accum.cum_gross_fb * &accum.ps.0 / &accum.ps.1;
        let op_protocol_lp = &new_cum_protocol_lp - &accum.cum_protocol_lp;
        accum.cum_protocol_lp = new_cum_protocol_lp;
        accum.running_total_lp = &accum.running_total_lp + &op_protocol_lp;

        let swap_idx = accum.swaps.len();
        accum.swaps.push(swap);
        let op_idx = accum.ops_order.len();
        accum.ops_order.push(BatchOp::Swap(swap_idx));
        self.global_seq_raw.push((pool_ident.clone(), op_idx));
        Ok(())
    }

    /// Try to add a CP proportional Deposit order to the accumulator.
    ///
    /// Resolves the deposit against the pool's *running* reserves (so two
    /// deposits in the same scoop layer correctly even though the second
    /// reads post-first-deposit state). Updates running reserves with the
    /// user's per-asset contribution and bumps `initial_total_lp` by the
    /// minted LP — `initial_total_lp` is the running pre-protocol_lp total
    /// that the tx_builder reads to compute the final pool datum, so growing
    /// it here keeps protocol_share / state_after_total_lp accounting consistent.
    pub fn try_add_deposit(
        &mut self,
        order: &Arc<crate::sundaev4::types::SundaeV4Order>,
        pool_ident: &Ident,
        effective_pool: &Arc<SundaeV4Pool>,
    ) -> Result<(), String> {
        let fresh = self.fresh_pool_accum(pool_ident, effective_pool);
        let accum = self.pools.entry(pool_ident.clone()).or_insert(fresh);
        accum.check_canonical_append(&order.input)?;

        // Build a transient pool reflecting the accumulator's running reserves
        // so the resolver applies to the post-previous-ops state.
        let mut transient = (**effective_pool).clone();
        transient.pool_datum.assets = accum.running_assets.clone();
        transient.pool_datum.total_lp = accum.running_total_lp.clone();

        let deposit = batch::resolve_proportional_deposit(&transient, order)?;

        // Update running reserves: each asset i grows by dx[i].
        for (i, (_, amt)) in accum.running_assets.iter_mut().enumerate() {
            *amt = &*amt + &deposit.dx[i];
        }
        accum.running_total_lp = &accum.running_total_lp + &deposit.lp_minted;
        accum.running_circ_lp = &accum.running_circ_lp + &deposit.lp_minted;

        let dep_idx = accum.deposits.len();
        accum.deposits.push(deposit);
        let op_idx = accum.ops_order.len();
        accum.ops_order.push(BatchOp::Deposit(dep_idx));
        self.global_seq_raw.push((pool_ident.clone(), op_idx));
        Ok(())
    }

    /// Try to add a Withdraw order targeting `pool_ident`. Inverse of
    /// `try_add_deposit`: the user offers LP, the scooper burns it, and the
    /// pool's reserves shrink by `dy[i]`. The pool's `total_lp` shrinks by
    /// `lp_burned`.
    pub fn try_add_withdraw(
        &mut self,
        order: &Arc<crate::sundaev4::types::SundaeV4Order>,
        pool_ident: &Ident,
        effective_pool: &Arc<SundaeV4Pool>,
    ) -> Result<(), String> {
        let fresh = self.fresh_pool_accum(pool_ident, effective_pool);
        let accum = self.pools.entry(pool_ident.clone()).or_insert(fresh);
        accum.check_canonical_append(&order.input)?;

        let mut transient = (**effective_pool).clone();
        transient.pool_datum.assets = accum.running_assets.clone();
        transient.pool_datum.total_lp = accum.running_total_lp.clone();

        let withdraw = batch::resolve_proportional_withdraw(&transient, order)?;

        // Update running reserves: each asset i shrinks by dy[i].
        for (i, (_, amt)) in accum.running_assets.iter_mut().enumerate() {
            *amt = &*amt - &withdraw.dy[i];
        }
        accum.running_total_lp = &accum.running_total_lp - &withdraw.lp_burned;
        accum.running_circ_lp = &accum.running_circ_lp - &withdraw.lp_burned;

        let w_idx = accum.withdraws.len();
        accum.withdraws.push(withdraw);
        let op_idx = accum.ops_order.len();
        accum.ops_order.push(BatchOp::Withdraw(w_idx));
        self.global_seq_raw.push((pool_ident.clone(), op_idx));
        Ok(())
    }

    /// Try to add a routed order (multi-hop and/or split) to the accumulator.
    ///
    /// Records the route as `RouteInfo` for tx-build-time use. The entry-hop
    /// first split becomes a `ResolvedSwap` (which owns the order input).
    /// Every other split — entry-hop or later — becomes a `ContinuationSwap`.
    /// Both carry a `RouteRef { route_idx, hop_idx, split_idx }` so the
    /// tx-builder can thread dy through hops at build time.
    ///
    /// Fee budget computed here is router-projected; tx_builder recomputes
    /// fresh per-pool fee budgets during its streaming walk.
    pub fn try_add_routed_order(
        &mut self,
        order: &Arc<crate::sundaev4::types::SundaeV4Order>,
        route: &RoutingPlan,
        pools: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
    ) -> Result<(), String> {
        self.add_route_branch(order, route, pools, true, true)?;
        Ok(())
    }

    /// Add a blended (multi-branch) route for one order. Branch 0's entry
    /// split carries the order (the primary swap); every other split of
    /// every branch is a continuation. Branches are pool-disjoint by
    /// construction (see `router::find_blended_route`), so their walks
    /// don't interact. min_received is enforced on the SUM of the branch
    /// outputs — the fulfillment carries all of them to the destination.
    pub fn try_add_blended_order(
        &mut self,
        order: &Arc<crate::sundaev4::types::SundaeV4Order>,
        blend: &crate::sundaev4::router::BlendedRoute,
        pools: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
    ) -> Result<(), String> {
        // Snapshot for rollback: each branch commits incrementally.
        let saved_pools = self.pools.clone();
        let saved_routes = self.routes.len();
        let saved_seq = self.global_seq_raw.len();
        let saved_conversions = self.conversions.len();
        let mut total_out = crate::bigint::BigInt::from(0);
        let mut result = Ok(());
        // The primary (order-owning) op must sit on a pool: pick the first
        // branch whose entry split is a pool, not a conversion. Until the
        // uniform-ops refactor, a blend where EVERY branch opens with a
        // conversion can't carry an order.
        let primary_idx = blend
            .branches
            .iter()
            .position(|b| {
                b.hops
                    .first()
                    .and_then(|h| h.splits.first())
                    .map(|sp| {
                        !matches!(
                            sp.pool.view_type,
                            crate::sundaev4::router::PoolViewType::Conversion { .. }
                        )
                    })
                    .unwrap_or(false)
            })
            // No branch opens with a pool: pure-conversion order — branch 0
            // carries the primary, which lands on its first conversion leg
            // (see add_route_branch).
            .unwrap_or(0);
        for (b, branch) in blend.branches.iter().enumerate() {
            // Per-branch min checks are meaningless under blending — the
            // SUM is checked below.
            match self.add_route_branch(order, branch, pools, b == primary_idx, false) {
                Ok(out) => total_out = &total_out + &out,
                Err(e) => {
                    result = Err(format!("blended branch {b}: {e}"));
                    break;
                }
            }
        }
        if result.is_ok() {
            let (ask_asset, min_qty) = order.swap_min_received();
            let final_asset = self.routes.last().map(|r| r.final_output_asset.clone());
            if final_asset.as_ref() == Some(ask_asset) {
                let (_, remaining) = order.swap_offered();
                let total_in = blend
                    .branches
                    .iter()
                    .fold(crate::bigint::BigInt::from(0), |acc, b| &acc + &b.total_input);
                // Partial fills: the contract's exact pro-rata check,
                // received·original ≥ min·fill.
                let ok = if &total_in < remaining {
                    if let crate::sundaev4::Constraint::Swap { original_offered, .. } =
                        &order.constraint
                    {
                        &total_out * original_offered >= min_qty * &total_in
                    } else {
                        false
                    }
                } else {
                    &total_out >= min_qty
                };
                if !ok {
                    result = Err(format!(
                        "blended output {total_out} below min_received {min_qty} \
                         (fill {total_in} of {remaining})"
                    ));
                }
            }
        }
        if result.is_err() {
            self.pools = saved_pools;
            self.routes.truncate(saved_routes);
            self.global_seq_raw.truncate(saved_seq);
            self.conversions.truncate(saved_conversions);
        }
        result
    }

    /// Walk one route (one branch of a possibly-blended plan) into the
    /// accumulator. When `primary`, the entry hop's first split is the
    /// ResolvedSwap that owns the order and drives its fulfillment; the
    /// per-route min_received check only applies to primary single-branch
    /// adds (blended callers check the branch sum instead). Returns the
    /// route's final-hop output.
    fn add_route_branch(
        &mut self,
        order: &Arc<crate::sundaev4::types::SundaeV4Order>,
        route: &RoutingPlan,
        pools: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
        primary: bool,
        enforce_min: bool,
    ) -> Result<BigInt, String> {
        // Conversion legs are pushed onto self.conversions during the walk;
        // unlike pool state they aren't trial-buffered, so roll them back on
        // any failure — a leaked leg from a failed attempt produced
        // DUPLICATE butane withdrawals in a later successful tx (malformed:
        // duplicate map keys; validators read the wrong redeemer).
        let conv_mark = self.conversions.len();
        let result = self.add_route_branch_inner(order, route, pools, primary, enforce_min);
        if result.is_err() {
            self.conversions.truncate(conv_mark);
        }
        result
    }

    fn add_route_branch_inner(
        &mut self,
        order: &Arc<crate::sundaev4::types::SundaeV4Order>,
        route: &RoutingPlan,
        pools: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
        primary: bool,
        enforce_min: bool,
    ) -> Result<BigInt, String> {
        use num_traits::Signed;

        // Clone pool accums for trial execution; only committed on full success.
        let mut trial_pools = self.pools.clone();
        let mut trial_global_seq: Vec<(Ident, usize)> = Vec::new();

        // Reserve route_idx; populate `hops` as we walk.
        let route_idx = self.routes.len();
        let mut hops_info: Vec<RouteHopInfo> = Vec::with_capacity(route.hops.len());

        let mut final_output_asset: Option<AssetClass> = None;
        let mut final_output_amount = BigInt::from(0);
        // The order attaches to the FIRST pool split of the walk — routes
        // may open with conversion legs (ADA→ADAb mint first), which can't
        // carry an order (no pool transcript to anchor it).
        let mut primary_placed = false;

        // Track actual output from previous hop so subsequent hops use the
        // real dy (not the router's estimate).  This ensures ADA flows cancel
        // exactly across pools for routed orders.
        let mut prev_hop_output = BigInt::from(0);

        for (hop_idx, hop) in route.hops.iter().enumerate() {
            let is_entry_hop = hop_idx == 0;
            let mut this_hop_output = BigInt::from(0);

            // For multi-split non-entry hops, track allocated dx so the last
            // split absorbs the integer-division remainder.
            let mut allocated_dx = BigInt::from(0);
            let hop_total: BigInt = hop.splits.iter()
                .map(|s| s.input_amount.clone())
                .fold(BigInt::from(0), |a, b| &a + &b);

            hops_info.push(RouteHopInfo {
                split_input_props: hop.splits.iter().map(|s| s.input_amount.clone()).collect(),
                hop_total_at_route_time: hop_total.clone(),
            });

            for (split_idx, split) in hop.splits.iter().enumerate() {
                // Off-protocol conversion legs: no pool op — record the
                // planned leg and thread its output into the hop total. The
                // tx builder later composes the mechanism's pieces (pot
                // output, mint, withdrawals) from plan.conversions.
                if let crate::sundaev4::router::PoolViewType::Conversion {
                    rate_num, rate_den, key,
                } = &split.pool.view_type
                {
                    let dx = if is_entry_hop {
                        split.input_amount.clone()
                    } else if hop.splits.len() == 1 {
                        prev_hop_output.clone()
                    } else {
                        // Mixed pool+conversion multi-split non-entry hops:
                        // proportional like pools; keep it simple by using
                        // the router's allocation directly (conversions are
                        // linear, no re-quote drift).
                        split.input_amount.clone()
                    };
                    let fee_num = BigInt::from(split.pool.fee_num);
                    let fee_den = BigInt::from(split.pool.fee_den);
                    let dx_eff = &dx - &(&dx * &fee_num / &fee_den);
                    let out = &(&dx_eff * rate_num) / rate_den;
                    if !out.is_positive() {
                        return Err(format!("conversion {key} produced zero output"));
                    }
                    this_hop_output = &this_hop_output + &out;
                    if hop_idx == route.hops.len() - 1 {
                        final_output_asset = Some(hop.output_token.clone());
                        final_output_amount = &final_output_amount + &out;
                    }
                    self.conversions.push(batch::PlannedConversion {
                        key: key.clone(),
                        from: hop.input_token.clone(),
                        to: hop.output_token.clone(),
                        dx,
                        out,
                        order: order.clone(),
                        order_input: order.input.clone(),
                        route_idx,
                        hop_idx,
                        primary: false,
                    });
                    continue;
                }
                let pool_ident = &split.pool.ident;

                // Get effective pool — from trial state if already there, else from chain
                let effective_pool = match trial_pools.get(pool_ident) {
                    Some(accum) => accum.pool.clone(),
                    None => match pools.get(pool_ident) {
                        Some(p) => p.clone(),
                        None => return Err(format!("pool {} not found", pool_ident)),
                    },
                };

                // Initialize pool accum if not already present
                let fresh = self.fresh_pool_accum(pool_ident, &effective_pool);
                let accum = trial_pools.entry(pool_ident.clone()).or_insert(fresh);
                accum.check_canonical_append(&order.input)?;

                // Determine input/output direction for this pool
                let (input_idx, output_idx) = Self::find_direction_for_tokens_static(
                    &accum.running_assets,
                    &hop.input_token,
                    &hop.output_token,
                ).ok_or_else(|| format!("can't determine direction for pool {}", pool_ident))?;

                // For hop 0, use the router's split amount. For subsequent hops,
                // use the actual output from the previous hop (single-split) or
                // distribute proportionally (multi-split).
                let dx = if is_entry_hop {
                    split.input_amount.clone()
                } else if hop.splits.len() == 1 {
                    prev_hop_output.clone()
                } else if split_idx == hop.splits.len() - 1 {
                    // Last split absorbs the remainder to avoid integer-division
                    // rounding loss that would break value conservation.
                    &prev_hop_output - &allocated_dx
                } else {
                    // Proportional split, tracking allocated amount
                    let proportional = if hop_total.is_positive() {
                        &prev_hop_output * &split.input_amount / &hop_total
                    } else {
                        split.input_amount.clone()
                    };
                    allocated_dx = &allocated_dx + &proportional;
                    proportional
                };

                let dy = batch::compute_swap_result(
                    &effective_pool.pool_type,
                    &accum.running_assets,
                    &accum.running_total_lp,
                    input_idx,
                    output_idx,
                    &dx,
                );
                if !dy.is_positive() {
                    return Err(format!("zero output from pool {}", pool_ident));
                }

                // Capture reserves before update for fee budget computation
                let prev_assets = accum.running_assets.clone();

                // Update running reserves
                accum.running_assets[input_idx].1 =
                    &accum.running_assets[input_idx].1 + &dx;
                accum.running_assets[output_idx].1 =
                    &accum.running_assets[output_idx].1 - &dy;

                // Per-entry protocol_lp bump (mirrors tx_builder) so CL dy
                // computed for subsequent ops in this pool matches tx-time.
                let fb = swap_math::compute_fee_budget(
                    &effective_pool.pool_type,
                    &prev_assets,
                    &accum.running_assets,
                    &accum.running_total_lp,
                );
                accum.cum_gross_fb = &accum.cum_gross_fb + &fb;
                let new_cum_protocol_lp = &accum.cum_gross_fb * &accum.ps.0 / &accum.ps.1;
                let op_protocol_lp = &new_cum_protocol_lp - &accum.cum_protocol_lp;
                accum.cum_protocol_lp = new_cum_protocol_lp;
                accum.running_total_lp = &accum.running_total_lp + &op_protocol_lp;

                this_hop_output = &this_hop_output + &dy;

                // Track final output from last hop
                if hop_idx == route.hops.len() - 1 {
                    final_output_asset = Some(hop.output_token.clone());
                    final_output_amount = &final_output_amount + &dy;
                }

                let route_ref = RouteRef { route_idx, hop_idx, split_idx };

                // Entry-hop first split is the primary (owns order). All
                // others are continuations with route metadata for tx-time
                // cascade reconstruction.
                let op_idx_in_pool = accum.ops_order.len();
                if primary && !primary_placed {
                    primary_placed = true;
                    let idx = accum.swaps.len();
                    accum.swaps.push(ResolvedSwap {
                        order: order.clone(),
                        input_idx,
                        output_idx,
                        dx: dx.clone(),
                        dy: dy.clone(),
                        route: Some(route_ref),
                    });
                    accum.ops_order.push(BatchOp::Swap(idx));
                } else {
                    let idx = accum.continuations.len();
                    accum.continuations.push(ContinuationSwap {
                        input_idx,
                        output_idx,
                        dx: dx.clone(),
                        route: route_ref,
                    });
                    accum.ops_order.push(BatchOp::Continuation(idx));
                }
                trial_global_seq.push((pool_ident.clone(), op_idx_in_pool));
            }

            prev_hop_output = this_hop_output;
        }

        // Check min_received against the final routed output. Blended
        // branches are checked as a sum by the caller instead. Partial fills
        // (route input below remaining_offered) use the contract's exact
        // pro-rata cross-multiplication:
        //   received · original_offered ≥ min · offered_this_fill
        if enforce_min {
            let (ask_asset, min_qty) = order.swap_min_received();
            if final_output_asset.as_ref() == Some(ask_asset) {
                let (_, remaining) = order.swap_offered();
                let ok = if &route.total_input < remaining {
                    if let crate::sundaev4::Constraint::Swap {
                        original_offered, ..
                    } = &order.constraint
                    {
                        &final_output_amount * original_offered
                            >= min_qty * &route.total_input
                    } else {
                        false // partial fills only exist for swap constraints
                    }
                } else {
                    &final_output_amount >= min_qty
                };
                if !ok {
                    return Err(format!(
                        "routed output {} below min_received {} (fill {} of {})",
                        final_output_amount, min_qty, route.total_input, remaining
                    ));
                }
            }
        }

        if primary && !primary_placed {
            // Pure-conversion route: the order's first conversion leg is its
            // primary op — the tx builder spends the order, emits its
            // redeemers, and builds its fulfillment off that leg.
            let mine = self
                .conversions
                .iter_mut()
                .find(|c| c.route_idx == route_idx);
            match mine {
                Some(c) => c.primary = true,
                None => {
                    return Err(
                        "route has neither pool splits nor conversion legs".into(),
                    );
                }
            }
        }
        let route_info = RouteInfo {
            order: order.clone(),
            hops: hops_info,
            final_output_asset: final_output_asset
                .ok_or_else(|| "route has no hops".to_string())?,
        };

        // Commit: replace pool states + record route + append to global_seq
        self.pools = trial_pools;
        self.routes.push(route_info);
        self.global_seq_raw.extend(trial_global_seq);

        Ok(final_output_amount)
    }

    /// Find which pool asset indices correspond to the given input/output tokens.
    /// Static variant — usable while `trial_pools` holds a mutable borrow of `self.pools`.
    fn find_direction_for_tokens_static(
        running_assets: &[(AssetClass, BigInt)],
        input_token: &AssetClass,
        output_token: &AssetClass,
    ) -> Option<(usize, usize)> {
        let mut input_idx = None;
        let mut output_idx = None;
        for (i, (asset, _)) in running_assets.iter().enumerate() {
            if asset == input_token {
                input_idx = Some(i);
            }
            if asset == output_token {
                output_idx = Some(i);
            }
        }
        match (input_idx, output_idx) {
            (Some(i), Some(o)) if i != o => Some((i, o)),
            _ => None,
        }
    }

    /// Total number of orders across all pools (swaps + deposits + withdraws).
    pub fn order_count(&self) -> usize {
        self.pools.values().map(|a| a.swaps.len() + a.deposits.len() + a.withdraws.len()).sum()
    }

    /// Collect all order inputs across all accumulated pools.
    pub fn order_inputs(&self) -> Vec<&crate::cardano_types::TransactionInput> {
        self.pools
            .values()
            .flat_map(|p| {
                p.swaps.iter().map(|s| &s.order.input)
                    .chain(p.deposits.iter().map(|d| &d.order.input))
                    .chain(p.withdraws.iter().map(|w| &w.order.input))
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.pools.is_empty() || self.order_count() == 0
    }

    /// Convert accumulated state into a `ScoopPlan` for the tx builder.
    ///
    /// `ScoopPlan` carries per-pool `Batch`es plus the cross-pool metadata the
    /// tx builder needs to recompute dy at tx-time and thread it through
    /// multi-hop routes: `routes` (per-route hop structure + fulfillment asset)
    /// and `global_seq` (topological op order). Fee budget computed here is
    /// router-projected — tx_builder recomputes it fresh during its streaming
    /// walk.
    pub fn into_plan(self) -> ScoopPlan {
        let conversions = self.conversions.clone();
        let mut batches = Vec::new();
        // Map Ident → batch_idx so we can translate `global_seq_raw` (keyed by
        // pool_ident) into `GlobalOp { batch_idx, op_idx }`.
        let mut ident_to_batch_idx: BTreeMap<Ident, usize> = BTreeMap::new();

        for (ident, accum) in self.pools {
            if accum.swaps.is_empty()
                && accum.continuations.is_empty()
                && accum.deposits.is_empty()
                && accum.withdraws.is_empty()
            {
                continue;
            }

            // Accumulator already applied per-entry protocol_lp bumps as it
            // went, so `running_total_lp` is the final LP including all
            // protocol_lp captured for this pool. tx_builder computes its own
            // per-entry values during its streaming walk — this field is
            // informational (used by tests).
            let final_total_lp = accum.running_total_lp.clone();

            ident_to_batch_idx.insert(ident.clone(), batches.len());
            batches.push(Batch {
                pool: accum.pool,
                pool_ident: accum.ident,
                swaps: accum.swaps,
                continuations: accum.continuations,
                deposits: accum.deposits,
                withdraws: accum.withdraws,
                claims: Vec::new(),
                ops_order: accum.ops_order,
                final_assets: accum.running_assets,
                final_total_lp,
            });
        }

        // Resolve raw (Ident, op_idx) entries to (batch_idx, op_idx). Skip
        // entries pointing at filtered-out empty pools — none should exist
        // since global_seq is only populated when an op was actually pushed.
        let global_seq: Vec<GlobalOp> = self.global_seq_raw
            .iter()
            .filter_map(|(ident, op_idx)| {
                ident_to_batch_idx.get(ident).map(|&batch_idx| GlobalOp {
                    batch_idx,
                    op_idx: *op_idx,
                })
            })
            .collect();

        return ScoopPlan {
            batches,
            routes: self.routes,
            global_seq,
            conversions,
        };
    }

    /// Backwards-compatible: return only the batches, dropping route/global_seq
    /// metadata. Used by tests that don't drive the tx_builder.
    #[cfg(test)]
    pub fn into_batches(self) -> Vec<Batch> {
        self.into_plan().batches
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cardano_types::{TransactionInput, Value};
    use crate::multisig::Multisig;
    use crate::sundaev4::types::{Destination, PoolDatum, PoolType, Rational, SundaeV4Order};
    use pallas_codec::utils::MaybeIndefArray;

    fn ada() -> AssetClass {
        AssetClass { policy: vec![], token: vec![] }
    }

    fn token_a() -> AssetClass {
        AssetClass { policy: vec![0x01], token: vec![0x02] }
    }

    fn token_b() -> AssetClass {
        AssetClass { policy: vec![0x03], token: vec![0x04] }
    }

    fn unit_pd() -> pallas_primitives::PlutusData {
        pallas_primitives::PlutusData::Constr(pallas_primitives::Constr {
            tag: 121,
            any_constructor: None,
            fields: MaybeIndefArray::Def(vec![]),
        })
    }

    fn make_pool(ident_byte: u8, ada_reserve: i64, token: AssetClass, token_reserve: i64) -> Arc<SundaeV4Pool> {
        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(ada_reserve));
        value.insert(&token, BigInt::from(token_reserve));

        Arc::new(SundaeV4Pool {
            input: TransactionInput::new([ident_byte; 32].into(), 0),
            value,
            pool_datum: PoolDatum {
                assets: vec![
                    (ada(), BigInt::from(ada_reserve)),
                    (token, BigInt::from(token_reserve)),
                ],
                total_lp: BigInt::from(1_000_000),
                circulating_lp: BigInt::from(500_000),
                preminted_lp: BigInt::from(500_000),
                identifier: Ident::new(&[ident_byte]),
                actions: vec![],
                module_state: vec![],
            },
            pool_type: PoolType::ConstantProduct {
                fee: Rational { num: BigInt::from(3), den: BigInt::from(1000) },
            },
            slot: 100,
            fee_split_config: None,
        })
    }

    fn make_buy_order(ada_amount: i64, min_token: AssetClass, min_qty: i64, slot: u64) -> Arc<SundaeV4Order> {
        let offer_amount = ada_amount - 2_000_000; // subtract min UTxO
        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(ada_amount));

        Arc::new(SundaeV4Order::test_swap_order(
            TransactionInput::new([slot as u8; 32].into(), 0),
            value,
            Multisig::Signature(vec![0xaa; 28]),
            Destination::SelfDestination,
            (ada(), BigInt::from(offer_amount)),
            (min_token, BigInt::from(min_qty)),
            BigInt::from(1_500_000i64),
            slot,
        ))
    }

    #[test]
    fn test_single_order_accumulator() {
        let pool = make_pool(0xAA, 1_000_000_000, token_a(), 1_000_000_000);
        let ident = pool.pool_datum.identifier.clone();
        let order = make_buy_order(10_000_000, token_a(), 1, 1);

        let mut accum = Accumulator::new((1, 2));
        assert!(accum.is_empty());

        accum.try_add_order(&order, &ident, &pool).unwrap();
        assert_eq!(accum.order_count(), 1);
        assert!(!accum.is_empty());

        let batches = accum.into_batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].swaps.len(), 1);
        assert_eq!(batches[0].pool_ident, ident);
    }

    #[test]
    fn test_multi_order_same_pool() {
        let pool = make_pool(0xAA, 1_000_000_000, token_a(), 1_000_000_000);
        let ident = pool.pool_datum.identifier.clone();

        let mut accum = Accumulator::new((1, 2));
        for slot in 1..=3u64 {
            let order = make_buy_order(10_000_000, token_a(), 1, slot);
            accum.try_add_order(&order, &ident, &pool).unwrap();
        }

        assert_eq!(accum.order_count(), 3);
        let batches = accum.into_batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].swaps.len(), 3);
    }

    #[test]
    fn test_multi_pool_accumulator() {
        let pool_a = make_pool(0xAA, 1_000_000_000, token_a(), 1_000_000_000);
        let pool_b = make_pool(0xBB, 1_000_000_000, token_b(), 1_000_000_000);
        let ident_a = pool_a.pool_datum.identifier.clone();
        let ident_b = pool_b.pool_datum.identifier.clone();

        let mut accum = Accumulator::new((1, 2));

        let order1 = make_buy_order(10_000_000, token_a(), 1, 1);
        accum.try_add_order(&order1, &ident_a, &pool_a).unwrap();

        let order2 = make_buy_order(10_000_000, token_b(), 1, 2);
        accum.try_add_order(&order2, &ident_b, &pool_b).unwrap();

        assert_eq!(accum.order_count(), 2);
        let batches = accum.into_batches();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].swaps.len(), 1);
        assert_eq!(batches[1].swaps.len(), 1);
    }

    #[test]
    fn test_failed_order_doesnt_pollute() {
        let pool = make_pool(0xAA, 1_000_000_000, token_a(), 1_000_000_000);
        let ident = pool.pool_datum.identifier.clone();

        // Order that wants more tokens than the swap would produce
        let bad_order = make_buy_order(10_000_000, token_a(), 999_999_999, 1);

        let mut accum = Accumulator::new((1, 2));
        assert!(accum.try_add_order(&bad_order, &ident, &pool).is_err());
        assert!(accum.is_empty());
    }

    #[test]
    fn test_accumulator_matches_assemble_batch() {
        // Verify that the accumulator produces the same batch as assemble_batch
        // for a single pool with multiple orders
        use crate::sundaev4::batch::{assemble_batch, BatchLimits};

        let pool = make_pool(0xAA, 1_000_000_000, token_a(), 1_000_000_000);
        let ident = pool.pool_datum.identifier.clone();
        let fee = (3u64, 1000u64);
        let protocol_share = (1u64, 2u64);

        let orders: Vec<_> = (1..=3u64)
            .map(|slot| make_buy_order(10_000_000, token_a(), 1, slot))
            .collect();

        // Build via assemble_batch
        let batch_classic = assemble_batch(
            &pool, &orders, fee, protocol_share, &BatchLimits { max_orders: 30 },
        ).unwrap();

        // Build via accumulator (same order)
        let mut accum = Accumulator::new(protocol_share);
        for order in &orders {
            accum.try_add_order(order, &ident, &pool).unwrap();
        }
        let accum_batches = accum.into_batches();
        assert_eq!(accum_batches.len(), 1);
        let batch_accum = &accum_batches[0];

        // Same number of swaps
        assert_eq!(batch_classic.swaps.len(), batch_accum.swaps.len());
        // Same final assets
        assert_eq!(batch_classic.final_assets, batch_accum.final_assets);
        // Same final_total_lp
        assert_eq!(batch_classic.final_total_lp, batch_accum.final_total_lp);
    }
}
