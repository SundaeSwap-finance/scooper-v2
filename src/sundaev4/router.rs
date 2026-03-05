//! Auto-router: finds optimal multi-hop and split routes through the pool graph.
//!
//! Port of `sundae-v4/test/emulator/src/router.ts`, CP pools only.
//! Pure module (no IO) — all functions work on immutable data.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use num_traits::Signed;

use crate::bigint::BigInt;
use crate::cardano_types::AssetClass;
use crate::sundaev3::Ident;
use crate::sundaev4::swap_math;
use crate::sundaev4::types::SundaeV4Pool;

// ─── Types ───────────────────────────────────────────────────────────────────

/// Lightweight pool view for the router (direction-aware).
#[derive(Clone, Debug)]
pub struct PoolView {
    pub ident: Ident,
    pub reserve_in: BigInt,
    pub reserve_out: BigInt,
    pub fee_num: u64,
    pub fee_den: u64,
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
    #[allow(dead_code)]
    pub total_input: BigInt,
    #[allow(dead_code)]
    pub total_output: BigInt,
    /// Output from just using the single best direct pool (for comparison).
    #[allow(dead_code)]
    pub naive_output: BigInt,
}

// ─── Swap Output ─────────────────────────────────────────────────────────────

/// CP swap output: dy = B * dx_eff / (A + dx_eff)
fn cp_output(pool: &PoolView, dx: &BigInt) -> BigInt {
    swap_math::cp_swap_result(&pool.reserve_in, &pool.reserve_out, dx, pool.fee_num, pool.fee_den)
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

/// CP marginal at effective allocation x (raw input):
/// marginal_raw = (feeDen - feeNum)/feeDen * A * B * SCALE / (A + xEff)^2
fn cp_marginal_at_allocation(pool: &PoolView, raw_allocated: &BigInt) -> BigInt {
    let fee_num = BigInt::from(pool.fee_num);
    let fee_den = BigInt::from(pool.fee_den);
    let x_eff = raw_allocated - &(raw_allocated * &fee_num / &fee_den);
    let denom = &pool.reserve_in + &x_eff;
    if !denom.is_positive() {
        return BigInt::from(0);
    }
    let fee_mult = &fee_den - &fee_num;
    &fee_mult * &pool.reserve_in * &pool.reserve_out * &scale()
        / &(&fee_den * &denom * &denom)
}

// ─── Optimal Split via Bisection ─────────────────────────────────────────────

/// For a target marginal λ, compute how much raw input each CP pool absorbs.
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

            // x_eff = isqrt(fee_mult * A * B * SCALE / (fee_den * lambda)) - A
            let numerator = &fee_mult * &pool.reserve_in * &pool.reserve_out * &sc;
            let denominator = &fee_den * lambda;
            let x_eff = swap_math::isqrt(&(&numerator / &denominator)) - &pool.reserve_in;
            if !x_eff.is_positive() {
                return BigInt::from(0);
            }
            // Convert effective back to raw: raw = x_eff * fee_den / fee_mult
            &x_eff * &fee_den / &fee_mult
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
        let out = cp_output(&pools[0], total_input);
        return vec![SplitEntry {
            pool: pools[0].clone(),
            input_amount: total_input.clone(),
            output_amount: out,
        }];
    }

    // Determine lambda search range
    let mut lambda_hi = BigInt::from(0);
    for pool in pools {
        let m = cp_marginal_at_allocation(pool, &BigInt::from(0));
        if m > lambda_hi {
            lambda_hi = m;
        }
    }
    let mut lambda_lo = BigInt::from(1);

    // Evaluate single-pool baselines
    let mut best_allocs: Vec<BigInt> = vec![BigInt::from(0); pools.len()];
    let mut best_output = BigInt::from(0);

    for (i, pool) in pools.iter().enumerate() {
        let out = cp_output(pool, total_input);
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
                candidate_output = &candidate_output + &cp_output(pool, &allocs[i]);
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
            let out = cp_output(pool, &best_allocs[i]);
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
/// For each pool with assets [A, B], creates edges A→B and B→A.
fn build_graph(
    pools: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
    fee: (u64, u64),
) -> PoolGraph {
    let mut graph: PoolGraph = BTreeMap::new();

    for (ident, pool) in pools {
        if pool.pool_datum.assets.len() != 2 {
            continue;
        }
        let (ref token_a, ref reserve_a) = pool.pool_datum.assets[0];
        let (ref token_b, ref reserve_b) = pool.pool_datum.assets[1];

        // A→B direction
        graph
            .entry(token_a.clone())
            .or_default()
            .entry(token_b.clone())
            .or_default()
            .push(PoolView {
                ident: ident.clone(),
                reserve_in: reserve_a.clone(),
                reserve_out: reserve_b.clone(),
                fee_num: fee.0,
                fee_den: fee.1,
            });

        // B→A direction
        graph
            .entry(token_b.clone())
            .or_default()
            .entry(token_a.clone())
            .or_default()
            .push(PoolView {
                ident: ident.clone(),
                reserve_in: reserve_b.clone(),
                reserve_out: reserve_a.clone(),
                fee_num: fee.0,
                fee_den: fee.1,
            });
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
            let out = cp_output(&hop.pools[0], &current_amount);
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
    fee: (u64, u64),
    input_token: &AssetClass,
    output_token: &AssetClass,
    amount: &BigInt,
) -> Option<RoutingPlan> {
    let graph = build_graph(pools, fee);
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
                let out = cp_output(pool, amount);
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
            (3, 1000),
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
            (3, 1000),
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
            (3, 1000),
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
            (3, 1000),
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
            (3, 1000),
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
            (3, 1000),
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

    /// Test 7: No route possible
    #[test]
    fn test_no_route() {
        let mut pools = BTreeMap::new();
        let (id1, pool1) = make_pool(0x01, ada(), 1_000_000, token(0xAA), 1_000_000);
        pools.insert(id1, pool1);

        // Try to route between two tokens with no path
        let route = find_optimal_route(
            &pools,
            (3, 1000),
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
            (3, 1000),
            &ada(),
            &token(0xAA),
            &BigInt::from(1000),
        );
        assert!(route.is_none());
    }
}
