//! Auto-router: finds optimal multi-hop and split routes through the pool graph.
//!
//! Port of `sundae-v4/test/emulator/src/router.ts`.
//! Supports CP and CS pools. Pure module (no IO) — all functions work on
//! immutable data.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use num_traits::Signed;

use crate::bigint::BigInt;
use crate::cardano_types::AssetClass;
use crate::sundaev3::Ident;
use crate::sundaev4::swap_math;
use crate::sundaev4::types::SundaeV4Pool;

// ─── Types ───────────────────────────────────────────────────────────────────

/// Pool-type-specific parameters for the router.
#[derive(Clone, Debug)]
pub enum PoolViewType {
    ConstantProduct,
    ConstantSum {
        /// Price of the input asset in this direction.
        price_in: BigInt,
        /// Price of the output asset in this direction.
        price_out: BigInt,
    },
    /// Concentrated liquidity. Stored in pool-positional `(a, b)` order
    /// plus a flag for swap direction so `pool_output` can dispatch to
    /// the right validator-matching branch of `cl_swap_result`. `lp` is
    /// the pool's current `total_lp` (CL swap math depends on LP via
    /// virtual reserves).
    ConcentratedLiquidity {
        is_a_input: bool,
        spa_num: BigInt,
        spa_den: BigInt,
        spb_num: BigInt,
        spb_den: BigInt,
        lp: BigInt,
    },
}

/// Lightweight pool view for the router (direction-aware).
#[derive(Clone, Debug)]
pub struct PoolView {
    pub ident: Ident,
    pub reserve_in: BigInt,
    pub reserve_out: BigInt,
    pub fee_num: u64,
    pub fee_den: u64,
    pub view_type: PoolViewType,
}

/// A single pool's contribution to a split.
#[derive(Clone, Debug)]
pub struct SplitEntry {
    pub pool: PoolView,
    pub input_amount: BigInt,
    pub output_amount: BigInt,
}

/// Result of a single hop (one token pair, possibly split across pools).
#[derive(Clone, Debug)]
pub struct HopResult {
    pub input_token: AssetClass,
    pub output_token: AssetClass,
    pub splits: Vec<SplitEntry>,
    pub total_output: BigInt,
}

/// Complete multi-hop routing plan.
#[derive(Clone, Debug)]
pub struct RoutingPlan {
    pub hops: Vec<HopResult>,
    /// Total input amount. Asserted in router tests.
    #[allow(dead_code)]
    pub total_input: BigInt,
    /// Total output across all hops. Asserted in router tests.
    #[allow(dead_code)]
    pub total_output: BigInt,
    /// Output from just using the single best direct pool (for comparison). Asserted in router tests.
    #[allow(dead_code)]
    pub naive_output: BigInt,
}

// ─── Swap Output ─────────────────────────────────────────────────────────────

/// Compute swap output for any pool type, capped at available reserves.
fn pool_output(pool: &PoolView, dx: &BigInt) -> BigInt {
    let raw = match &pool.view_type {
        PoolViewType::ConstantProduct => {
            swap_math::cp_swap_result(&pool.reserve_in, &pool.reserve_out, dx, pool.fee_num, pool.fee_den)
        }
        PoolViewType::ConstantSum { price_in, price_out } => {
            let fee_num = BigInt::from(pool.fee_num);
            let fee_den = BigInt::from(pool.fee_den);
            swap_math::cs_swap_result(dx, &[price_in.clone(), price_out.clone()], 0, 1, &fee_num, &fee_den)
        }
        PoolViewType::ConcentratedLiquidity {
            is_a_input, spa_num, spa_den, spb_num, spb_den, lp,
        } => {
            let fee_num = BigInt::from(pool.fee_num);
            let fee_den = BigInt::from(pool.fee_den);
            // pool.reserve_in / reserve_out are direction-oriented; map back
            // to pool-positional (a, b) using is_a_input.
            let (a, b) = if *is_a_input {
                (&pool.reserve_in, &pool.reserve_out)
            } else {
                (&pool.reserve_out, &pool.reserve_in)
            };
            swap_math::cl_swap_result(
                a, b, lp, dx, *is_a_input,
                spa_num, spa_den, spb_num, spb_den, &fee_num, &fee_den,
            )
        }
    };
    // Cap at reserve_out — can't withdraw more than the pool holds.
    // (CP naturally stays below reserves; CS can exceed them.)
    if raw > pool.reserve_out {
        pool.reserve_out.clone()
    } else {
        raw
    }
}

// ─── Marginal Price Functions ────────────────────────────────────────────────

fn scale() -> BigInt {
    // 10^18
    let mut s = BigInt::from(1i64);
    for _ in 0..18 {
        s = &s * &BigInt::from(10i64);
    }
    s
}

/// Marginal price at a given raw allocation for any pool type (scaled by SCALE).
///
/// CP: decreasing marginal — `fee_mult/fee_den * A * B * SCALE / (A + xEff)^2`
/// CS: constant marginal — `fee_mult * price_in * SCALE / (price_out * fee_den)`
fn marginal_at_allocation(pool: &PoolView, raw_allocated: &BigInt) -> BigInt {
    let fee_num = BigInt::from(pool.fee_num);
    let fee_den = BigInt::from(pool.fee_den);
    let fee_mult = &fee_den - &fee_num;

    match &pool.view_type {
        PoolViewType::ConstantProduct => {
            let x_eff = raw_allocated - &(raw_allocated * &fee_num / &fee_den);
            let denom = &pool.reserve_in + &x_eff;
            if !denom.is_positive() {
                return BigInt::from(0);
            }
            &fee_mult * &pool.reserve_in * &pool.reserve_out * &scale()
                / &(&fee_den * &denom * &denom)
        }
        PoolViewType::ConstantSum { price_in, price_out } => {
            // CS marginal is constant: dy/dx = price_in * fee_mult / (price_out * fee_den)
            let _ = raw_allocated;
            &fee_mult * price_in * &scale() / &(price_out * &fee_den)
        }
        PoolViewType::ConcentratedLiquidity {
            is_a_input, spa_num, spa_den, spb_num, spb_den, lp,
        } => {
            // Marginal dy/dx in CL = derivative of the validator formula:
            //   A→B:  dy = vb0·dva_eff / (spa_den·(va0+dva_eff))
            //         where va0 = a·spb_num + L·spb_den
            //               vb0 = b·spa_den + L·spa_num
            //               dva_eff = (fee_mult/fee_den)·dx·spb_num
            //         d(dy)/dx = (fm·vb0·va0·spb_num) / (fd·spa_den·va²)
            //   B→A:  symmetric with (spa↔spb), (a↔b), va↔vb
            let (a, b) = if *is_a_input {
                (&pool.reserve_in, &pool.reserve_out)
            } else {
                (&pool.reserve_out, &pool.reserve_in)
            };
            let va0 = &(a * spb_num) + &(lp * spb_den);
            let vb0 = &(b * spa_den) + &(lp * spa_num);
            let dx_eff = raw_allocated - &(raw_allocated * &fee_num / &fee_den);
            if *is_a_input {
                let va = &va0 + &(&dx_eff * spb_num);
                let denom = &fee_den * spa_den * &va * &va;
                if !denom.is_positive() {
                    return BigInt::from(0);
                }
                &fee_mult * &vb0 * &va0 * spb_num * &scale() / &denom
            } else {
                let vb = &vb0 + &(&dx_eff * spa_num);
                let denom = &fee_den * spb_num * &vb * &vb;
                if !denom.is_positive() {
                    return BigInt::from(0);
                }
                &fee_mult * &va0 * &vb0 * spa_num * &scale() / &denom
            }
        }
    }
}

// ─── Optimal Split via Bisection ─────────────────────────────────────────────

/// For a target marginal λ, compute how much raw input each pool absorbs.
///
/// CP pools: solve for the allocation that gives marginal = λ.
/// CS pools: constant marginal — absorb up to reserve limit if λ <= marginal,
/// otherwise 0.
fn allocations_for_lambda(pools: &[PoolView], lambda: &BigInt) -> Vec<BigInt> {
    if !lambda.is_positive() {
        return pools.iter().map(|_| BigInt::from(0)).collect();
    }

    let sc = scale();
    pools
        .iter()
        .map(|pool| {
            let fee_num = BigInt::from(pool.fee_num);
            let fee_den = BigInt::from(pool.fee_den);
            let fee_mult = &fee_den - &fee_num;

            match &pool.view_type {
                PoolViewType::ConstantProduct => {
                    // x_eff = isqrt(fee_mult * A * B * SCALE / (fee_den * lambda)) - A
                    let numerator = &fee_mult * &pool.reserve_in * &pool.reserve_out * &sc;
                    let denominator = &fee_den * lambda;
                    let x_eff = swap_math::isqrt(&(&numerator / &denominator)) - &pool.reserve_in;
                    if !x_eff.is_positive() {
                        return BigInt::from(0);
                    }
                    // Convert effective back to raw: raw = x_eff * fee_den / fee_mult
                    &x_eff * &fee_den / &fee_mult
                }
                PoolViewType::ConstantSum { price_in, price_out } => {
                    // CS marginal is constant. If lambda <= marginal, absorb
                    // everything up to what the reserve allows. Otherwise 0.
                    let cs_marginal = &fee_mult * price_in * &sc / &(price_out * &fee_den);
                    if lambda <= &cs_marginal {
                        // Can absorb up to the full output reserve
                        // max_raw = reserve_out * price_out * fee_den / (price_in * fee_mult)
                        &pool.reserve_out * price_out * &fee_den / &(price_in * &fee_mult)
                    } else {
                        BigInt::from(0)
                    }
                }
                PoolViewType::ConcentratedLiquidity {
                    is_a_input, spa_num, spa_den, spb_num, spb_den, lp,
                } => {
                    // Invert marginal = λ:
                    //   A→B: va² = (fm·vb0·va0·spb_num·SCALE) / (λ·fd·spa_den)
                    //   dva_eff = isqrt(va²) − va0
                    //   dx_eff  = dva_eff / spb_num
                    //   dx_raw  = dx_eff · fd / fm
                    //   B→A: symmetric (swap spa↔spb, va↔vb)
                    let (a, b) = if *is_a_input {
                        (&pool.reserve_in, &pool.reserve_out)
                    } else {
                        (&pool.reserve_out, &pool.reserve_in)
                    };
                    let va0 = &(a * spb_num) + &(lp * spb_den);
                    let vb0 = &(b * spa_den) + &(lp * spa_num);
                    let (numerator, denom, sp_input_num) = if *is_a_input {
                        (
                            &fee_mult * &vb0 * &va0 * spb_num * &sc,
                            &fee_den * spa_den * lambda,
                            spb_num,
                        )
                    } else {
                        (
                            &fee_mult * &va0 * &vb0 * spa_num * &sc,
                            &fee_den * spb_num * lambda,
                            spa_num,
                        )
                    };
                    if !denom.is_positive() || !sp_input_num.is_positive() {
                        return BigInt::from(0);
                    }
                    let v_target = swap_math::isqrt(&(&numerator / &denom));
                    let v0 = if *is_a_input { &va0 } else { &vb0 };
                    let dv_eff = &v_target - v0;
                    if !dv_eff.is_positive() {
                        return BigInt::from(0);
                    }
                    let dx_eff = &dv_eff / sp_input_num;
                    if !dx_eff.is_positive() {
                        return BigInt::from(0);
                    }
                    &dx_eff * &fee_den / &fee_mult
                }
            }
        })
        .collect()
}

/// Optimally split `total_input` across `pools` for the same token pair.
///
/// Returns `SplitEntry` for each pool with positive allocation.
pub fn optimize_split(pools: &[PoolView], total_input: &BigInt) -> Vec<SplitEntry> {
    if pools.is_empty() {
        return vec![];
    }
    if pools.len() == 1 {
        let out = pool_output(&pools[0], total_input);
        return vec![SplitEntry {
            pool: pools[0].clone(),
            input_amount: total_input.clone(),
            output_amount: out,
        }];
    }

    // Determine lambda search range
    let mut lambda_hi = BigInt::from(0);
    for pool in pools {
        let m = marginal_at_allocation(pool, &BigInt::from(0));
        if m > lambda_hi {
            lambda_hi = m;
        }
    }
    let mut lambda_lo = BigInt::from(1);

    // Evaluate single-pool baselines
    let mut best_allocs: Vec<BigInt> = vec![BigInt::from(0); pools.len()];
    let mut best_output = BigInt::from(0);

    for (i, pool) in pools.iter().enumerate() {
        let out = pool_output(pool, total_input);
        if out > best_output {
            best_output = out.clone();
            best_allocs = vec![BigInt::from(0); pools.len()];
            best_allocs[i] = total_input.clone();
        }
    }

    const MAX_ITER: usize = 200;

    for _iter in 0..MAX_ITER {
        let lambda_mid = &(&lambda_lo + &lambda_hi) / &BigInt::from(2);
        if !lambda_mid.is_positive() {
            break;
        }

        let mut allocs = allocations_for_lambda(pools, &lambda_mid);

        // Sum allocations
        let mut total_alloc: BigInt = allocs.iter().fold(BigInt::from(0), |a, b| &a + b);

        // Cap proportionally if exceeds total_input
        if &total_alloc > total_input {
            for a in allocs.iter_mut() {
                *a = &*a * total_input / &total_alloc;
            }
            total_alloc = allocs.iter().fold(BigInt::from(0), |a, b| &a + b);
        }

        // Compute total output for this allocation
        let mut candidate_output = BigInt::from(0);
        for (i, pool) in pools.iter().enumerate() {
            if allocs[i].is_positive() {
                candidate_output = &candidate_output + &pool_output(pool, &allocs[i]);
            }
        }

        if candidate_output > best_output {
            best_output = candidate_output;
            best_allocs = allocs;
        }

        if &total_alloc == total_input || &lambda_hi - &lambda_lo <= BigInt::from(1) {
            break;
        }

        if &total_alloc > total_input {
            lambda_lo = lambda_mid;
        } else {
            lambda_hi = lambda_mid;
        }
    }

    // Normalize allocations to sum exactly to total_input
    let alloc_sum: BigInt = best_allocs.iter().fold(BigInt::from(0), |a, b| &a + b);
    if &alloc_sum != total_input && alloc_sum.is_positive() {
        let mut scaled: Vec<BigInt> = best_allocs
            .iter()
            .map(|a| a * total_input / &alloc_sum)
            .collect();
        let new_sum: BigInt = scaled.iter().fold(BigInt::from(0), |a, b| &a + b);
        if &new_sum < total_input {
            // Add remainder to largest allocation
            let max_idx = scaled
                .iter()
                .enumerate()
                .max_by(|a, b| a.1.cmp(b.1))
                .map(|(i, _)| i)
                .unwrap_or(0);
            scaled[max_idx] = &scaled[max_idx] + &(total_input - &new_sum);
        }
        best_allocs = scaled;
    }

    // Build results
    let mut results = Vec::new();
    for (i, pool) in pools.iter().enumerate() {
        if best_allocs[i].is_positive() {
            let out = pool_output(pool, &best_allocs[i]);
            results.push(SplitEntry {
                pool: pool.clone(),
                input_amount: best_allocs[i].clone(),
                output_amount: out,
            });
        }
    }
    results
}

// ─── Graph + Path Finding ────────────────────────────────────────────────────

type PoolGraph = BTreeMap<AssetClass, BTreeMap<AssetClass, Vec<PoolView>>>;

/// Build a directed pool graph from on-chain pool state.
///
/// For CP pools with assets [A, B]: creates edges A→B and B→A.
/// For CS pools with N assets: creates edges for all (i, j) pairs.
fn build_graph(
    pools: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
) -> PoolGraph {
    use crate::sundaev4::types::PoolType;
    use num_traits::ToPrimitive;

    let mut graph: PoolGraph = BTreeMap::new();

    for (ident, pool) in pools {
        let assets = &pool.pool_datum.assets;

        let (fee_num, fee_den, view_type_fn): (u64, u64, Box<dyn Fn(usize, usize) -> PoolViewType>) = match &pool.pool_type {
            PoolType::ConstantProduct { fee } => {
                let fn_num = fee.num.clone().unwrap().to_u64().unwrap_or(0);
                let fn_den = fee.den.clone().unwrap().to_u64().unwrap_or(1);
                (fn_num, fn_den, Box::new(|_, _| PoolViewType::ConstantProduct))
            }
            PoolType::ConcentratedLiquidity { sqrt_price_a, sqrt_price_b, fee } => {
                let fn_num = fee.num.clone().unwrap().to_u64().unwrap_or(0);
                let fn_den = fee.den.clone().unwrap().to_u64().unwrap_or(1);
                let spa_num = sqrt_price_a.num.clone();
                let spa_den = sqrt_price_a.den.clone();
                let spb_num = sqrt_price_b.num.clone();
                let spb_den = sqrt_price_b.den.clone();
                let lp = pool.pool_datum.total_lp.clone();
                (fn_num, fn_den, Box::new(move |i, _| PoolViewType::ConcentratedLiquidity {
                    is_a_input: i == 0,
                    spa_num: spa_num.clone(),
                    spa_den: spa_den.clone(),
                    spb_num: spb_num.clone(),
                    spb_den: spb_den.clone(),
                    lp: lp.clone(),
                }))
            }
            PoolType::ConstantSum { prices, fee, .. } => {
                let fn_num = fee.num.clone().unwrap().to_u64().unwrap_or(0);
                let fn_den = fee.den.clone().unwrap().to_u64().unwrap_or(1);
                let prices = prices.clone();
                (fn_num, fn_den, Box::new(move |i, j| PoolViewType::ConstantSum {
                    price_in: prices[i].clone(),
                    price_out: prices[j].clone(),
                }))
            }
        };

        // Create edges for all (i, j) pairs
        for i in 0..assets.len() {
            for j in 0..assets.len() {
                if i == j { continue; }
                let (ref token_in, ref reserve_in) = assets[i];
                let (ref token_out, ref reserve_out) = assets[j];

                graph
                    .entry(token_in.clone())
                    .or_default()
                    .entry(token_out.clone())
                    .or_default()
                    .push(PoolView {
                        ident: ident.clone(),
                        reserve_in: reserve_in.clone(),
                        reserve_out: reserve_out.clone(),
                        fee_num,
                        fee_den,
                        view_type: view_type_fn(i, j),
                    });
            }
        }
    }

    graph
}

/// A hop in a path: from one token to another via one or more pools.
struct PathHop {
    token_in: AssetClass,
    token_out: AssetClass,
    pools: Vec<PoolView>,
}

/// Find all acyclic paths from source to dest (BFS, up to max_depth hops).
fn find_paths(
    graph: &PoolGraph,
    source: &AssetClass,
    dest: &AssetClass,
    max_depth: usize,
) -> Vec<Vec<PathHop>> {
    let mut results: Vec<Vec<PathHop>> = Vec::new();

    struct QueueEntry {
        current: AssetClass,
        path: Vec<(AssetClass, AssetClass, Vec<PoolView>)>,
        visited: Vec<AssetClass>,
    }

    let mut queue: VecDeque<QueueEntry> = VecDeque::new();
    queue.push_back(QueueEntry {
        current: source.clone(),
        path: vec![],
        visited: vec![source.clone()],
    });

    while let Some(entry) = queue.pop_front() {
        if entry.path.len() >= max_depth {
            continue;
        }

        if let Some(edges) = graph.get(&entry.current) {
            for (to_token, pools) in edges {
                if entry.visited.contains(to_token) {
                    continue;
                }

                let mut new_path = entry.path.clone();
                new_path.push((entry.current.clone(), to_token.clone(), pools.clone()));

                if to_token == dest {
                    let hops = new_path
                        .into_iter()
                        .map(|(tin, tout, p)| PathHop {
                            token_in: tin,
                            token_out: tout,
                            pools: p,
                        })
                        .collect();
                    results.push(hops);
                } else {
                    let mut new_visited = entry.visited.clone();
                    new_visited.push(to_token.clone());
                    queue.push_back(QueueEntry {
                        current: to_token.clone(),
                        path: new_path,
                        visited: new_visited,
                    });
                }
            }
        }
    }

    results
}

// ─── Path Evaluation ─────────────────────────────────────────────────────────

/// Evaluate a path: for each hop, split optimally among available pools.
fn evaluate_path(path: &[PathHop], input_amount: &BigInt) -> Vec<HopResult> {
    let mut results = Vec::new();
    let mut current_amount = input_amount.clone();

    for hop in path {
        let splits = if hop.pools.len() == 1 {
            let out = pool_output(&hop.pools[0], &current_amount);
            vec![SplitEntry {
                pool: hop.pools[0].clone(),
                input_amount: current_amount.clone(),
                output_amount: out,
            }]
        } else {
            optimize_split(&hop.pools, &current_amount)
        };

        let total_out: BigInt = splits.iter().fold(BigInt::from(0), |a, s| &a + &s.output_amount);

        results.push(HopResult {
            input_token: hop.token_in.clone(),
            output_token: hop.token_out.clone(),
            splits,
            total_output: total_out.clone(),
        });

        current_amount = total_out;
    }

    results
}

// ─── Main Router ─────────────────────────────────────────────────────────────

/// Find the optimal route from input_token to output_token.
///
/// Tries all paths up to 4 hops, evaluates each with optimal splitting,
/// and returns the best one. Also computes "naive" output (best single
/// direct pool, no multi-hop, no split).
pub fn find_optimal_route(
    pools: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
    input_token: &AssetClass,
    output_token: &AssetClass,
    amount: &BigInt,
) -> Option<RoutingPlan> {
    let graph = build_graph(pools);
    let paths = find_paths(&graph, input_token, output_token, 4);

    if paths.is_empty() {
        return None;
    }

    let mut best_plan: Option<RoutingPlan> = None;
    let mut best_output = BigInt::from(0);

    for path in &paths {
        let hops = evaluate_path(path, amount);
        let total_out = hops
            .last()
            .map(|h| h.total_output.clone())
            .unwrap_or_else(|| BigInt::from(0));

        if total_out > best_output {
            best_output = total_out.clone();
            best_plan = Some(RoutingPlan {
                hops,
                total_input: amount.clone(),
                total_output: total_out,
                naive_output: BigInt::from(0),
            });
        }
    }

    let mut plan = best_plan?;

    // Compute naive output: direct single-pool swap using best pool
    if let Some(edges) = graph.get(input_token) {
        if let Some(direct_pools) = edges.get(output_token) {
            for pool in direct_pools {
                let out = pool_output(pool, amount);
                if out > plan.naive_output {
                    plan.naive_output = out;
                }
            }
        }
    }

    Some(plan)
}

/// Check whether a route is "interesting" (multi-hop or split).
/// Returns true if the route has >1 hop or any hop has >1 split.
pub fn is_routed(plan: &RoutingPlan) -> bool {
    if plan.hops.len() > 1 {
        return true;
    }
    plan.hops.iter().any(|h| h.splits.len() > 1)
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cardano_types::{TransactionInput, Value};
    use crate::sundaev4::types::PoolDatum;

    fn ada() -> AssetClass {
        AssetClass { policy: vec![], token: vec![] }
    }

    fn token(id: u8) -> AssetClass {
        AssetClass { policy: vec![id], token: vec![id] }
    }

    fn make_pool(
        ident_byte: u8,
        asset_a: AssetClass,
        reserve_a: i64,
        asset_b: AssetClass,
        reserve_b: i64,
    ) -> (Ident, Arc<SundaeV4Pool>) {
        let mut value = Value::default();
        value.insert(&asset_a, BigInt::from(reserve_a));
        value.insert(&asset_b, BigInt::from(reserve_b));
        let ident = Ident::new(&[ident_byte]);

        let pool = Arc::new(SundaeV4Pool {
            input: TransactionInput::new([ident_byte; 32].into(), 0),
            value,
            pool_datum: PoolDatum {
                assets: vec![
                    (asset_a, BigInt::from(reserve_a)),
                    (asset_b, BigInt::from(reserve_b)),
                ],
                total_lp: BigInt::from(1_000_000),
                circulating_lp: BigInt::from(500_000),
                preminted_lp: BigInt::from(500_000),
                identifier: ident.clone(),
                actions: vec![],
                module_state: vec![],
            },
            pool_type: crate::sundaev4::types::PoolType::ConstantProduct {
                fee: crate::sundaev4::types::Rational {
                    num: BigInt::from(3),
                    den: BigInt::from(1000),
                },
            },
            slot: 100,
            fee_split_config: None,
        });

        (ident, pool)
    }

    /// Test 1: Split across 2 CP pools (matching TS test 3)
    #[test]
    fn test_split_two_cp_pools_small() {
        let mut pools = BTreeMap::new();
        let (id1, pool1) = make_pool(0x01, token(0xAA), 10_000_000, token(0xBB), 10_000_000);
        let (id2, pool2) = make_pool(0x02, token(0xAA), 1_000_000, token(0xBB), 1_000_000);
        pools.insert(id1, pool1);
        pools.insert(id2, pool2);

        let route = find_optimal_route(
            &pools,
            &token(0xAA),
            &token(0xBB),
            &BigInt::from(1000),
        );
        assert!(route.is_some());
        let plan = route.unwrap();
        // Small amount: split barely helps, expect ~996
        assert_eq!(plan.total_output, BigInt::from(996));
        assert_eq!(plan.naive_output, BigInt::from(996));
    }

    /// Test 2: Split across 2 CP pools with large amount (matching TS test 3)
    #[test]
    fn test_split_two_cp_pools_large() {
        let mut pools = BTreeMap::new();
        let (id1, pool1) = make_pool(0x01, token(0xAA), 10_000_000, token(0xBB), 10_000_000);
        let (id2, pool2) = make_pool(0x02, token(0xAA), 1_000_000, token(0xBB), 1_000_000);
        pools.insert(id1, pool1);
        pools.insert(id2, pool2);

        let route = find_optimal_route(
            &pools,
            &token(0xAA),
            &token(0xBB),
            &BigInt::from(1_000_000),
        );
        assert!(route.is_some());
        let plan = route.unwrap();
        // Large amount: splitting helps significantly
        // TS: split=914145, naive=906610
        assert_eq!(plan.total_output, BigInt::from(914145));
        assert_eq!(plan.naive_output, BigInt::from(906610));
    }

    /// Test 3: Multi-hop A→ADA→B
    #[test]
    fn test_multi_hop() {
        let mut pools = BTreeMap::new();
        let (id1, pool1) = make_pool(0x01, ada(), 1_000_000, token(0xAA), 1_000_000);
        let (id2, pool2) = make_pool(0x02, ada(), 1_000_000, token(0xBB), 1_000_000);
        pools.insert(id1, pool1);
        pools.insert(id2, pool2);

        // Swap TOKENA → TOKENB (no direct pool, must go via ADA)
        let route = find_optimal_route(
            &pools,
            &token(0xAA),
            &token(0xBB),
            &BigInt::from(10_000),
        );
        assert!(route.is_some());
        let plan = route.unwrap();
        assert_eq!(plan.hops.len(), 2);
        assert!(plan.total_output.is_positive());
        // No direct pool, so naive=0
        assert_eq!(plan.naive_output, BigInt::from(0));
        assert!(is_routed(&plan));
    }

    /// Test 4: Direct single pool — route should exist but not be "routed"
    #[test]
    fn test_direct_single_pool() {
        let mut pools = BTreeMap::new();
        let (id1, pool1) = make_pool(0x01, ada(), 1_000_000_000, token(0xAA), 1_000_000_000);
        pools.insert(id1, pool1);

        let route = find_optimal_route(
            &pools,
            &ada(),
            &token(0xAA),
            &BigInt::from(10_000_000),
        );
        assert!(route.is_some());
        let plan = route.unwrap();
        assert_eq!(plan.hops.len(), 1);
        assert_eq!(plan.hops[0].splits.len(), 1);
        assert!(!is_routed(&plan));
    }

    /// Test 5: Very large split (matching TS test 3: 5M input)
    #[test]
    fn test_split_very_large() {
        let mut pools = BTreeMap::new();
        let (id1, pool1) = make_pool(0x01, token(0xAA), 10_000_000, token(0xBB), 10_000_000);
        let (id2, pool2) = make_pool(0x02, token(0xAA), 1_000_000, token(0xBB), 1_000_000);
        pools.insert(id1, pool1);
        pools.insert(id2, pool2);

        let route = find_optimal_route(
            &pools,
            &token(0xAA),
            &token(0xBB),
            &BigInt::from(5_000_000),
        );
        assert!(route.is_some());
        let plan = route.unwrap();
        // TS: split=3430403, naive=3326659
        assert_eq!(plan.total_output, BigInt::from(3430403));
        assert_eq!(plan.naive_output, BigInt::from(3326659));
    }

    /// Test 6: TS test 2 equivalent — multi-hop A→B→C (CP only)
    #[test]
    fn test_multi_hop_three_pools() {
        let mut pools = BTreeMap::new();
        let (id1, pool1) = make_pool(0x01, token(0xAA), 1_000_000, token(0xBB), 1_000_000);
        let (id2, pool2) = make_pool(0x02, token(0xBB), 1_000_000, token(0xCC), 1_000_000);
        let (id3, pool3) = make_pool(0x03, token(0xCC), 1_000_000, token(0xDD), 1_000_000);
        pools.insert(id1, pool1);
        pools.insert(id2, pool2);
        pools.insert(id3, pool3);

        // A → D through 3 hops
        let route = find_optimal_route(
            &pools,
            &token(0xAA),
            &token(0xDD),
            &BigInt::from(1000),
        );
        assert!(route.is_some());
        let plan = route.unwrap();
        assert_eq!(plan.hops.len(), 3);
        // TS test 2 (all CP): A→B 1000→997, B→C 997→994, C→D 994→991
        // (TS uses CS for first hop which gives 997, our CP gives 996)
        // Verify positive output and 3 hops
        assert!(plan.total_output.is_positive());
        assert_eq!(plan.naive_output, BigInt::from(0)); // no direct pool
    }

    fn make_cs_pool(
        ident_byte: u8,
        assets: Vec<(AssetClass, i64)>,
        prices: Vec<i64>,
    ) -> (Ident, Arc<SundaeV4Pool>) {
        let mut value = Value::default();
        let mut datum_assets = Vec::new();
        for (asset, reserve) in &assets {
            value.insert(asset, BigInt::from(*reserve));
            datum_assets.push((asset.clone(), BigInt::from(*reserve)));
        }
        let ident = Ident::new(&[ident_byte]);

        let pool = Arc::new(SundaeV4Pool {
            input: TransactionInput::new([ident_byte; 32].into(), 0),
            value,
            pool_datum: PoolDatum {
                assets: datum_assets,
                total_lp: BigInt::from(1_000_000),
                circulating_lp: BigInt::from(500_000),
                preminted_lp: BigInt::from(500_000),
                identifier: ident.clone(),
                actions: vec![],
                module_state: vec![],
            },
            pool_type: crate::sundaev4::types::PoolType::ConstantSum {
                prices: prices.iter().map(|&p| BigInt::from(p)).collect(),
                fee: crate::sundaev4::types::Rational {
                    num: BigInt::from(3),
                    den: BigInt::from(1000),
                },
                bounty_k: crate::sundaev4::types::Rational {
                    num: BigInt::from(0),
                    den: BigInt::from(1),
                },
            },
            slot: 100,
            fee_split_config: None,
        });

        (ident, pool)
    }

    /// Test 9: Direct CS pool swap
    #[test]
    fn test_cs_direct_swap() {
        let mut pools = BTreeMap::new();
        let (id, pool) = make_cs_pool(
            0x01,
            vec![(token(0xAA), 1_000_000), (token(0xBB), 1_000_000)],
            vec![1, 1],
        );
        pools.insert(id, pool);

        let route = find_optimal_route(
            &pools,
            &token(0xAA),
            &token(0xBB),
            &BigInt::from(10_000),
        );
        assert!(route.is_some());
        let plan = route.unwrap();
        assert_eq!(plan.hops.len(), 1);
        // CS 1:1 with 3/1000 fee: dy = (10000*1 - floor(10000*1*3/1000)) / 1
        // = 10000 - 30 = 9970
        assert_eq!(plan.total_output, BigInt::from(9970));
    }

    /// Test 10: CS pool with asymmetric prices
    #[test]
    fn test_cs_asymmetric_prices() {
        let mut pools = BTreeMap::new();
        let (id, pool) = make_cs_pool(
            0x01,
            vec![(token(0xAA), 1_000_000), (token(0xBB), 2_000_000)],
            vec![2, 1],
        );
        pools.insert(id, pool);

        // Swap A→B: price_in=2, price_out=1
        // input_value = 1000 * 2 = 2000
        // v_increase = floor(2000 * 3 / 1000) = 6
        // dy = (2000 - 6) / 1 = 1994
        let route = find_optimal_route(
            &pools,
            &token(0xAA),
            &token(0xBB),
            &BigInt::from(1000),
        );
        assert!(route.is_some());
        let plan = route.unwrap();
        assert_eq!(plan.total_output, BigInt::from(1994));
    }

    /// Test 11: Split between CS and CP pools (same pair).
    /// CS has a better rate (prices [3,1] → 3x output) but limited reserves,
    /// so the router should use CS first and overflow to CP.
    #[test]
    fn test_split_cs_and_cp() {
        let mut pools = BTreeMap::new();
        // CS pool: prices [3,1] (3x rate), 100k reserve of token BB
        let (id1, pool1) = make_cs_pool(
            0x01,
            vec![(token(0xAA), 1_000_000), (token(0xBB), 100_000)],
            vec![3, 1],
        );
        // CP pool: large reserves (10M each)
        let (id2, pool2) = make_pool(0x02, token(0xAA), 10_000_000, token(0xBB), 10_000_000);
        pools.insert(id1, pool1);
        pools.insert(id2, pool2);

        // 100k input. CS marginal ≈ 2.991 >> CP marginal ≈ 0.997.
        // CS should absorb ~33.4k (exhausting its 100k BB reserve at 3:1), rest to CP.
        let route = find_optimal_route(
            &pools,
            &token(0xAA),
            &token(0xBB),
            &BigInt::from(100_000),
        );
        assert!(route.is_some());
        let plan = route.unwrap();
        assert_eq!(plan.hops.len(), 1);
        // The split should outperform CP-only.
        let cp_only = swap_math::cp_swap_result(
            &BigInt::from(10_000_000), &BigInt::from(10_000_000),
            &BigInt::from(100_000), 3, 1000,
        );
        assert!(plan.total_output > cp_only, "split should beat CP-only: {} vs {}", plan.total_output, cp_only);
        // Should actually split (use both pools)
        assert!(is_routed(&plan), "should split across CS and CP");
    }

    /// Test 12: CS multi-asset pool (3 assets) in router graph
    #[test]
    fn test_cs_3asset_routing() {
        let mut pools = BTreeMap::new();
        // 3-asset CS pool: A, B, C with prices [1, 2, 3]
        let (id, pool) = make_cs_pool(
            0x01,
            vec![(token(0xAA), 1_000_000), (token(0xBB), 1_000_000), (token(0xCC), 1_000_000)],
            vec![1, 2, 3],
        );
        pools.insert(id, pool);

        // Swap A→C: price_in=1, price_out=3
        // input_value = 300 * 1 = 300
        // v_increase = floor(300 * 3 / 1000) = 0
        // dy = (300 - 0) / 3 = 100
        let route = find_optimal_route(
            &pools,
            &token(0xAA),
            &token(0xCC),
            &BigInt::from(300),
        );
        assert!(route.is_some());
        let plan = route.unwrap();
        assert_eq!(plan.total_output, BigInt::from(100));
    }

    /// Test 13: Multi-hop through CS pool
    #[test]
    fn test_multi_hop_via_cs() {
        let mut pools = BTreeMap::new();
        // CP: A→ADA
        let (id1, pool1) = make_pool(0x01, ada(), 1_000_000, token(0xAA), 1_000_000);
        // CS: ADA→B (1:1 stablecoin-like)
        let (id2, pool2) = make_cs_pool(
            0x02,
            vec![(ada(), 1_000_000), (token(0xBB), 1_000_000)],
            vec![1, 1],
        );
        pools.insert(id1, pool1);
        pools.insert(id2, pool2);

        // Swap A → B via ADA (CP then CS)
        let route = find_optimal_route(
            &pools,
            &token(0xAA),
            &token(0xBB),
            &BigInt::from(10_000),
        );
        assert!(route.is_some());
        let plan = route.unwrap();
        assert_eq!(plan.hops.len(), 2);
        assert!(plan.total_output.is_positive());
        assert!(is_routed(&plan));
    }

    /// Test 14: CS marginal correctly prioritizes high-rate CS over CP
    #[test]
    fn test_cs_marginal_prioritization() {
        // CS pool with prices [2, 1]: marginal = 2 * 997/1000 ≈ 1.994
        // CP pool 1:1 with same reserves: marginal at 0 = 997/1000 ≈ 0.997
        // CS should be strongly preferred for small amounts.
        let mut pools = BTreeMap::new();
        let (id1, pool1) = make_cs_pool(
            0x01,
            vec![(token(0xAA), 1_000_000), (token(0xBB), 2_000_000)],
            vec![2, 1],
        );
        let (id2, pool2) = make_pool(0x02, token(0xAA), 1_000_000, token(0xBB), 1_000_000);
        pools.insert(id1, pool1);
        pools.insert(id2, pool2);

        let route = find_optimal_route(
            &pools,
            &token(0xAA),
            &token(0xBB),
            &BigInt::from(1000),
        );
        assert!(route.is_some());
        let plan = route.unwrap();
        // CS output: 1000*2 = 2000 input_value, v_increase=6, dy=1994
        // CP output: 1M*997/(1M+997) = 996
        // CS is much better, router should use CS
        assert_eq!(plan.total_output, BigInt::from(1994));
    }

    /// Test 7: No route possible
    #[test]
    fn test_no_route() {
        let mut pools = BTreeMap::new();
        let (id1, pool1) = make_pool(0x01, ada(), 1_000_000, token(0xAA), 1_000_000);
        pools.insert(id1, pool1);

        // Try to route between two tokens with no path
        let route = find_optimal_route(
            &pools,
            &token(0xBB),
            &token(0xCC),
            &BigInt::from(1000),
        );
        assert!(route.is_none());
    }

    /// Test 8: Empty pool set
    #[test]
    fn test_empty_pools() {
        let pools = BTreeMap::new();
        let route = find_optimal_route(
            &pools,
            &ada(),
            &token(0xAA),
            &BigInt::from(1000),
        );
        assert!(route.is_none());
    }
}
