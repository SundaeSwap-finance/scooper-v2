//! Block-capacity benchmark: how many sundae-v4 orders a block can carry, a
//! Praos ranking block (RB) vs a Leios endorser block (EB), when every scoop tx
//! stays within today's per-tx limits.
//!
//! 1. Queue. A queue of orders is generated from the two parameters measured
//!    on the real DEX flow by `dataset_params.py` (`data/dataset-params.json`):
//!    each order touches 1-4 pools (`pools_per_order`), and each pool it
//!    touches was last touched about `d` orders earlier, `d` drawn from a
//!    log-normal fitted on the measured median and quartiles
//!    (`pool_reuse_distance`), see `draw_pools_per_order`. When no pool was last
//!    touched that far back, the order opens a new pool. The generated
//!    queue's own parameters are printed next to the targets.
//! 2. Market. Liquidity is infinite (the market can always provide): every
//!    pool is a copy of one large concentrated-liquidity pool of the
//!    route-quality market, hardcoded in `build_reference_pool` (built by
//!    `bench_common::cl_pool`, real CL module pipeline), all on the same
//!    A/B pair (ADA/NIGHT). An order is a basic swap of `ORDER_AMOUNT_A` A→B
//!    over the pools the queue gives it, routed by the production router
//!    restricted to those pools (`route_over_order_pools`: one hop split
//!    about evenly over them). Orders sell the pools' first
//!    asset (see `flipped_cl_pairs` in `route_quality.rs` for why that
//!    matters).
//! 3. Scoops. The queue is cut into scoop txs exactly as production does
//!    (`scooper.rs`, `run_v4_batch_cycle`): orders are accumulated in queue
//!    order up to `max_orders`, one checkpoint per prefix; the full batch is
//!    probed, then the largest prefix whose real tx (built and evaluated
//!    against the blueprint, ExUnits padded as in production) fits the per-tx
//!    limit is found by bisection (production's own code:
//!    `batch::largest_fitting_checkpoint`, `batch::ScoopTxCost`, and the
//!    production tx builder and evaluator). The next scoop starts from the rest of the
//!    queue, against the same pool state (infinite liquidity). A 1-order
//!    batch that does not fit is quarantined, as in production.
//! 4. Blocks. Scoops are packed in order into RBs and into EBs (bytes, padded
//!    mem and steps summed over the block's txs), and the orders per full
//!    block are reported. Several EB sizes can be given: the same scoops are
//!    packed for each, so their orders per block compare directly.
//!
//! The bench measures block capacity, not the scooper: a quarantined order or
//! an order the accumulator rejects makes the run invalid (exit code 1).
//!
//! Built as the `block-bench` example, which recompiles the crate's modules (see
//! `src/block_bench.rs`):
//!   cargo run --release --features bench --example block-bench -- --help

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use clap::Parser;
use rand::SeedableRng;
use rand::distr::Distribution;
use rand::distr::weighted::WeightedIndex;
use rand::rngs::StdRng;
use rand_distr::LogNormal;
use serde_json::Value as Json;

use crate::bench_common::{DEFAULT_BLUEPRINT, FEE_DEN, TxLimit, asset_class, cl_pool};
use crate::bigint::BigInt;
use crate::cardano_types::{ADA_ASSET_CLASS, AssetClass};
use crate::sundaev3::Ident;
use crate::sundaev4::accumulator::Accumulator;
use crate::sundaev4::batch::{BatchLimits, Fitness, ScoopTxCost, largest_fitting_checkpoint};
use crate::sundaev4::router::{RoutingLimits, RoutingPlan, find_blended_route};
use crate::sundaev4::test_harness::{TestEnv, make_basic_swap_order, make_settings};
use crate::sundaev4::{Rational, SundaeV4Order, SundaeV4Pool};

const BLOCK_CAPACITY_DATA: &str = "bench/block-capacity/data";

/// Size of every order, in units of the pools' asset A (lovelace: 100 ADA).
/// Irrelevant to the cost as long as it is small next to the pools (infinite
/// liquidity).
const ORDER_AMOUNT_A: i64 = 100_000_000;

/// Quantile 0.75 of the standard normal: a log-normal's quartiles are
/// exp(μ ± STANDARD_NORMAL_P75·σ).
const STANDARD_NORMAL_P75: f64 = 0.674_489_750_196_081_7;

// ─── Queue ───────────────────────────────────────────────────────────────────

/// The target queue parameters of `dataset-params.json`, and the
/// distributions the queue is drawn from.
struct QueueParams {
    /// (pool count, share of orders), by pool count.
    pools_per_order: Vec<(usize, f64)>,
    /// Draws an index into `pools_per_order`, weighted by the shares.
    pool_count_index: WeightedIndex<f64>,
    /// Target reuse distance quartiles: p25, median, p75.
    reuse_distance_quartiles: [f64; 3],
    /// Log-normal fitted on them.
    reuse_distance: LogNormal<f64>,
}

fn load_queue_params(path: &str) -> QueueParams {
    let contents = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("{path}: {e} (run `make -C bench/block-capacity`)"));
    let json: Json = serde_json::from_str(&contents).expect("dataset-params.json");
    let pools_per_order: Vec<(usize, f64)> = json["pools_per_order"]
        .as_object()
        .expect("pools_per_order")
        .iter()
        .map(|(pool_count, share)| {
            (
                pool_count.parse().expect("pool count"),
                share.as_f64().expect("share"),
            )
        })
        .collect();
    let pool_count_index = WeightedIndex::new(pools_per_order.iter().map(|(_, share)| share))
        .expect("pools_per_order shares: non-negative, not all zero");
    let reuse_distance_json = &json["pool_reuse_distance"];
    let quantile = |key: &str| reuse_distance_json[key].as_f64().expect(key);
    let [p25, median, p75] = [quantile("p25"), quantile("median"), quantile("p75")];
    let reuse_distance =
        LogNormal::new(median.ln(), (p75 / p25).ln() / (2.0 * STANDARD_NORMAL_P75))
            .expect("pool_reuse_distance quartiles: p25 ≤ p75");
    QueueParams {
        pools_per_order,
        pool_count_index,
        reuse_distance_quartiles: [p25, median, p75],
        reuse_distance,
    }
}

/// The last use of each pool (the last order that touched it), searchable by
/// recency. Pools are numbered in creation order.
#[derive(Default)]
struct PoolRecency {
    last_use_of_pool: Vec<usize>,
    /// The same as (last use, pool number), sorted by last use.
    pools_by_last_use: BTreeSet<(usize, usize)>,
}

impl PoolRecency {
    /// The pool for order `order_index` to touch, not in `excluded`.
    ///
    /// `min_reuse_distance` is how far back, at least, the pool's last use
    /// must be: the pick is the pool whose last use is the closest to
    /// `order_index - min_reuse_distance` without being after it, so its
    /// actual reuse distance is the smallest one ≥ `min_reuse_distance`. A new
    /// pool when there is none (or when that order is before the queue's
    /// start).
    fn pick_pool(
        &mut self,
        order_index: usize,
        min_reuse_distance: usize,
        excluded: &[usize],
    ) -> usize {
        let closest_pool =
            order_index.checked_sub(min_reuse_distance).and_then(|latest_allowed_last_use| {
                self.pools_by_last_use
                    .range(..=(latest_allowed_last_use, usize::MAX))
                    .rev()
                    .map(|&(_, pool)| pool)
                    .find(|pool| !excluded.contains(pool))
            });
        closest_pool.unwrap_or_else(|| {
            // Not in `pools_by_last_use` until `touch`.
            self.last_use_of_pool.push(order_index);
            self.last_use_of_pool.len() - 1
        })
    }

    /// Record `order_index` as the last use of `pools`.
    fn touch(&mut self, order_index: usize, pools: &[usize]) {
        for &pool in pools {
            // A no-op for a new pool, not in the set yet.
            self.pools_by_last_use.remove(&(self.last_use_of_pool[pool], pool));
            self.last_use_of_pool[pool] = order_index;
            self.pools_by_last_use.insert((order_index, pool));
        }
    }
}

/// Per order of a queue of `queue_length` orders matching `params`, the
/// numbers of the pools it touches; and the number of pools, numbered from 0.
///
/// Order `i` touches `k` pools, `k` drawn from `pools_per_order`. For each of
/// them, draw a minimum reuse distance `d` and take the pool whose last use is
/// the closest to order `i - d` without being after it (so its actual reuse
/// distance is `d`, or a bit more when no pool was last used exactly there);
/// when there is none, take a new pool. Drawing `d` and then requiring a pool
/// last used exactly `d` back, redrawing otherwise, would favour short
/// distances: the farther back, the likelier that order's pools were touched
/// again since.
fn draw_pools_per_order(
    params: &QueueParams,
    queue_length: usize,
    rng: &mut StdRng,
) -> (Vec<Vec<usize>>, usize) {
    let mut recency = PoolRecency::default();
    let pools_per_order = (0..queue_length)
        .map(|order_index| {
            let order_pool_count = params.pools_per_order[params.pool_count_index.sample(rng)].0;
            let pools = (0..order_pool_count).fold(
                Vec::with_capacity(order_pool_count),
                |mut pools_already_in_order, _| {
                    let min_reuse_distance =
                        params.reuse_distance.sample(rng).round().max(1.0) as usize;
                    let pool =
                        recency.pick_pool(order_index, min_reuse_distance, &pools_already_in_order);
                    pools_already_in_order.push(pool);
                    pools_already_in_order
                },
            );
            recency.touch(order_index, &pools);
            pools
        })
        .collect();
    (pools_per_order, recency.last_use_of_pool.len())
}

/// An order of the queue and the pools it touches.
struct QueuedOrder {
    order: Arc<SundaeV4Order>,
    pools: Vec<Arc<SundaeV4Pool>>,
}

/// A queue of `queue_length` orders matching `params` (see
/// `draw_pools_per_order`), each an A→B swap over its pools, copies of
/// `reference_pool`.
///
/// Orders get increasing slots: their UTxO refs increase too, which keeps the
/// per-pool canonical order of the accumulator.
fn generate_orders(
    env: &TestEnv,
    reference_pool: &ReferencePool,
    params: &QueueParams,
    queue_length: usize,
    rng: &mut StdRng,
) -> Vec<QueuedOrder> {
    let (pools_per_order, pool_count) = draw_pools_per_order(params, queue_length, rng);
    let pools: Vec<Arc<SundaeV4Pool>> = (0..pool_count)
        .map(|pool_number| copy_reference_pool(env, reference_pool, pool_number))
        .collect();
    pools_per_order
        .into_iter()
        .zip(1u64..)
        .map(|(order_pools, slot)| {
            let min_received = 1;
            QueuedOrder {
                order: make_basic_swap_order(
                    reference_pool.asset_a.clone(),
                    ORDER_AMOUNT_A,
                    reference_pool.asset_b.clone(),
                    min_received,
                    slot,
                ),
                pools: order_pools.iter().map(|&pool| pools[pool].clone()).collect(),
            }
        })
        .collect()
}

fn pool_ident(pool: &SundaeV4Pool) -> &Ident {
    &pool.pool_datum.identifier
}

/// The parameters of the generated queue, measured as `dataset_params.py`
/// does, next to their targets.
fn describe_queue_against_targets(orders: &[QueuedOrder], params: &QueueParams) -> String {
    let queue_length = orders.len() as f64;
    let mut orders_by_pool_count: BTreeMap<usize, usize> = BTreeMap::new();
    for order in orders {
        *orders_by_pool_count.entry(order.pools.len()).or_default() += 1;
    }
    let target_shares: BTreeMap<usize, f64> = params.pools_per_order.iter().copied().collect();
    let pools_per_order = orders_by_pool_count
        .iter()
        .map(|(pool_count, orders)| {
            format!(
                "{pool_count}: {:.2} % (target {:.2})",
                100.0 * *orders as f64 / queue_length,
                100.0 * target_shares.get(pool_count).copied().unwrap_or(0.0)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    let mut last_order_of_pool: BTreeMap<&Ident, usize> = BTreeMap::new();
    let (mut reuse_distances, mut pool_legs) = (Vec::new(), 0usize);
    for (order_index, order) in orders.iter().enumerate() {
        for pool in &order.pools {
            pool_legs += 1;
            let previous_use = last_order_of_pool.insert(pool_ident(pool), order_index);
            if let Some(previous_order) = previous_use {
                reuse_distances.push(order_index - previous_order);
            }
        }
    }
    reuse_distances.sort_unstable();
    let quantile = |q: f64| reuse_distances[(q * (reuse_distances.len() - 1) as f64) as usize];
    let new_pool_legs = pool_legs - reuse_distances.len();
    let [p25, median, p75] = params.reuse_distance_quartiles;
    format!(
        "{} orders, {} pools\n            pools per order   {pools_per_order}\n            \
         reuse distance    median {} (target {median}), p25 {} ({p25}), p75 {} ({p75}); \
         {:.2} % of pool legs open a new pool",
        orders.len(),
        last_order_of_pool.len(),
        quantile(0.5),
        quantile(0.25),
        quantile(0.75),
        100.0 * new_pool_legs as f64 / pool_legs as f64,
    )
}

// ─── Market ──────────────────────────────────────────────────────────────────

/// The pool every pool of the bench copies: `cl:ADA/NIGHT:+0` of
/// `route-quality/data/market-v4-extrapolated.json` (as of 2026-10-12), its
/// largest in-range ADA-first concentrated-liquidity pool. Hardcoded so that
/// regenerating that market does not silently change this bench's results.
/// What matters for the scoop cost: a CL pool, in range and deep enough to
/// stay so over the whole queue, with mainnet-sized integers.
///
/// Every order sells the pool's first asset A for its second asset B (see
/// `flipped_cl_pairs` in `route_quality.rs` for why the direction matters).
struct ReferencePool {
    asset_a: AssetClass,
    reserve_a: i64,
    asset_b: AssetClass,
    reserve_b: i64,
    liquidity: i64,
    /// The bounds of the pool's price range.
    sqrt_price_range: (Rational, Rational),
    fee: Rational,
}

fn build_reference_pool() -> ReferencePool {
    let rational = |num: u64, den: u64| Rational {
        num: BigInt::from(num),
        den: BigInt::from(den),
    };
    ReferencePool {
        asset_a: ADA_ASSET_CLASS,
        reserve_a: 13_484_290_140_505,
        // NIGHT.
        asset_b: asset_class("0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1fa.4e49474854"),
        reserve_b: 75_055_595_594_677,
        liquidity: 6_441_949_822_246_678,
        sqrt_price_range: (
            rational(2_347_618_107_038, 1_000_000_000_000),
            rational(2_370_978_066_542, 1_000_000_000_000),
        ),
        // 0.3 %.
        fee: rational(3 * FEE_DEN / 1000, FEE_DEN),
    }
}

/// A copy of `reference_pool`, its ident derived from `pool_number`.
fn copy_reference_pool(
    env: &TestEnv,
    reference_pool: &ReferencePool,
    pool_number: usize,
) -> Arc<SundaeV4Pool> {
    cl_pool(
        env,
        pool_number,
        (reference_pool.asset_a.clone(), reference_pool.reserve_a),
        (reference_pool.asset_b.clone(), reference_pool.reserve_b),
        reference_pool.liquidity,
        reference_pool.sqrt_price_range.clone(),
        reference_pool.fee.clone(),
    )
}

/// The order's route, as production dispatches it (`scooper.rs`,
/// `find_blended_route` then `as_single`), but over the order's pools alone:
/// they are copies of one pool on one pair, so the router finds a single hop
/// split (about) evenly across all of them. Anything else would change the
/// order's pool count, the queue's measured parameter: panic.
fn route_over_order_pools(
    order_pools: &[Arc<SundaeV4Pool>],
    offered: &AssetClass,
    wanted: &AssetClass,
) -> RoutingPlan {
    let order_market: BTreeMap<Ident, Arc<SundaeV4Pool>> =
        order_pools.iter().map(|pool| (pool_ident(pool).clone(), pool.clone())).collect();
    let blended_route = find_blended_route(
        &order_market,
        &[],
        offered,
        wanted,
        &BigInt::from(ORDER_AMOUNT_A),
        RoutingLimits::unlimited(),
    )
    .expect("a route A→B through the order's pools");
    let plan = blended_route.as_single().expect("a single-path route").clone();
    assert!(
        plan.hops.len() == 1 && plan.hops[0].splits.len() == order_pools.len(),
        "the router split the order over {:?} pools per hop, not its {}",
        plan.hops.iter().map(|hop| hop.splits.len()).collect::<Vec<_>>(),
        order_pools.len()
    );
    plan
}

// ─── Scoops ──────────────────────────────────────────────────────────────────

/// Resources of a tx or a block: bytes, padded mem, padded steps.
#[derive(Clone, Copy, Default)]
struct Cost {
    bytes: usize,
    mem: u64,
    steps: u64,
}

impl Cost {
    fn plus(self, other: Cost) -> Cost {
        Cost {
            bytes: self.bytes + other.bytes,
            mem: self.mem + other.mem,
            steps: self.steps + other.steps,
        }
    }

    /// The resources over `limit` (empty when it fits).
    fn resources_over(&self, limit: &TxLimit) -> Vec<&'static str> {
        let mut over = Vec::new();
        if self.bytes > limit.bytes {
            over.push("size (bytes)");
        }
        if self.mem > limit.mem {
            over.push("script memory");
        }
        if self.steps > limit.steps {
            over.push("script CPU steps");
        }
        over
    }
}

impl From<ScoopTxCost> for Cost {
    fn from(tx: ScoopTxCost) -> Self {
        Cost {
            bytes: tx.bytes,
            mem: tx.mem,
            steps: tx.steps,
        }
    }
}

/// Build and evaluate `batch`'s scoop tx with the production builder and
/// evaluator (`TestEnv::build_and_eval_plan`): its cost, padded as production
/// does (`ScoopTxCost`), or why it failed to build or evaluate.
fn build_and_cost_scoop_tx(
    env: &TestEnv,
    batch: &Accumulator,
    slot: u64,
) -> Result<ScoopTxCost, String> {
    let plan = batch.clone().into_plan();
    let settings = make_settings(env, &env.scooper_keyhash());
    let (built_tx, evaluation) =
        env.build_and_eval_plan(&plan, &settings, slot).map_err(|e| format!("build/eval: {e}"))?;
    Ok(ScoopTxCost::new(
        built_tx.cbor.len(),
        &evaluation,
        &env.exec,
    ))
}

/// Why a scoop holds no more orders.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum CutReason {
    MaxOrders,
    EndOfQueue,
    /// The next prefix exceeded these per-tx resources.
    OverTxLimit(String),
    /// The next prefix failed to build or evaluate.
    Bail,
}

impl std::fmt::Display for CutReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CutReason::MaxOrders => write!(f, "max_orders reached"),
            CutReason::EndOfQueue => write!(f, "end of queue"),
            CutReason::OverTxLimit(resources) => write!(f, "tx {resources}"),
            CutReason::Bail => write!(f, "next order fails to build/eval"),
        }
    }
}

struct Scoop {
    /// Queue index of its first order.
    first_order: usize,
    order_count: usize,
    pool_count: usize,
    cost: Cost,
    cut_reason: CutReason,
}

/// Orders the bench could not scoop, with why: the run is invalid if any.
#[derive(Default)]
struct UnscoopedOrders {
    /// 1-order batch that does not fit (production quarantines it).
    quarantined: Vec<(usize, String)>,
    /// Order the accumulator refuses (production leaves it in the queue).
    rejected: Vec<(usize, String)>,
}

impl UnscoopedOrders {
    fn is_empty(&self) -> bool {
        self.quarantined.is_empty() && self.rejected.is_empty()
    }
}

/// Cut the queue into scoops, as production does (see the module doc), under
/// the per-tx limits of `env.exec`.
fn cut_queue_into_scoops(
    env: &TestEnv,
    orders: &[QueuedOrder],
    routes: &[RoutingPlan],
    market: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
    max_orders: usize,
) -> (Vec<Scoop>, UnscoopedOrders) {
    let tx_limit = TxLimit {
        bytes: env.exec.max_tx_size,
        mem: env.exec.max_tx_ex_mem,
        steps: env.exec.max_tx_ex_steps,
    };
    // After every order's slot.
    let eval_slot = orders.len() as u64 + 1000;
    let mut pending: Vec<usize> = (0..orders.len()).collect();
    let (mut scoops, mut unscooped) = (Vec::new(), UnscoopedOrders::default());
    while !pending.is_empty() {
        let mut batch = Accumulator::new(env.exec.protocol_share);
        // checkpoints[i] holds batch_orders[..=i].
        let mut checkpoints: Vec<Accumulator> = Vec::new();
        let mut batch_orders: Vec<usize> = Vec::new();
        let mut done: BTreeSet<usize> = BTreeSet::new();
        let mut queue_exhausted = true;
        for &order in &pending {
            if batch.order_count() >= max_orders {
                queue_exhausted = false;
                break;
            }
            let mut candidate = batch.clone();
            match candidate.try_add_routed_order(&orders[order].order, &routes[order], market) {
                Ok(()) => {
                    batch = candidate;
                    checkpoints.push(batch.clone());
                    batch_orders.push(order);
                }
                // Production skips it and retries it next cycle; here it would
                // never succeed (same pools), so it is dropped and reported.
                Err(e) => {
                    unscooped.rejected.push((order, e));
                    done.insert(order);
                }
            }
        }
        if checkpoints.is_empty() {
            pending.retain(|order| !done.contains(order));
            continue;
        }

        // Production's search, every probe kept by checkpoint index.
        let full_batch = checkpoints.len() - 1;
        let mut probes: BTreeMap<usize, Result<ScoopTxCost, String>> = BTreeMap::new();
        let (largest_fitting, _last_bail) =
            largest_fitting_checkpoint(checkpoints.len(), |checkpoint| {
                let probe = build_and_cost_scoop_tx(env, &checkpoints[checkpoint], eval_slot);
                let fitness = match &probe {
                    Ok(cost) => cost.fitness(&env.exec),
                    Err(e) => Fitness::Bail(e.clone()),
                };
                probes.insert(checkpoint, probe);
                fitness
            });
        match largest_fitting {
            Some(last) => {
                let Some(Ok(cost)) = probes.get(&last) else {
                    unreachable!("largest_fitting is a fitting probe")
                };
                let cut_reason = match probes.get(&(last + 1)) {
                    None if last == full_batch && queue_exhausted => CutReason::EndOfQueue,
                    None => CutReason::MaxOrders,
                    // Built and evaluated but not the largest fitting: over budget.
                    Some(Ok(next_cost)) => CutReason::OverTxLimit(
                        Cost::from(*next_cost).resources_over(&tx_limit).join(" and "),
                    ),
                    Some(Err(_)) => CutReason::Bail,
                };
                scoops.push(Scoop {
                    first_order: batch_orders[0],
                    order_count: last + 1,
                    pool_count: checkpoints[last].pools.len(),
                    cost: Cost::from(*cost),
                    cut_reason,
                });
                done.extend(&batch_orders[..=last]);
            }
            None => {
                // Production quarantines the first order (build error or over
                // budget) or bails the cycle (eval error): either way it is
                // an order this bench cannot scoop.
                let reason = match probes.get(&0) {
                    Some(Ok(cost)) => format!(
                        "over the tx limit ({})",
                        Cost::from(*cost).resources_over(&tx_limit).join("+")
                    ),
                    Some(Err(e)) => e.clone(),
                    None => "not probed".to_string(),
                };
                unscooped.quarantined.push((batch_orders[0], reason));
                done.insert(batch_orders[0]);
            }
        }
        pending.retain(|order| !done.contains(order));
        eprint!(
            "\rscoops {}, orders left {}   ",
            scoops.len(),
            pending.len()
        );
    }
    eprint!("\r\x1b[K");
    (scoops, unscooped)
}

// ─── Blocks ──────────────────────────────────────────────────────────────────

/// Full blocks of one kind: scoops packed in queue order, a block is closed
/// when the next scoop would exceed one of its limits. The last, partial
/// block is not counted.
struct BlockPacking {
    /// Orders and scoops of each full block.
    orders_per_block: Vec<usize>,
    scoops_per_block: Vec<usize>,
    /// Which block resources the next scoop would have exceeded.
    blocks_by_limit: BTreeMap<String, usize>,
    /// Scoops and orders left in the last, partial block.
    partial_scoops: usize,
    partial_orders: usize,
}

fn pack_into_blocks(scoops: &[Scoop], block_limit: &TxLimit) -> BlockPacking {
    let mut packing = BlockPacking {
        orders_per_block: Vec::new(),
        scoops_per_block: Vec::new(),
        blocks_by_limit: BTreeMap::new(),
        partial_scoops: 0,
        partial_orders: 0,
    };
    let (mut block_cost, mut block_scoops, mut block_orders) = (Cost::default(), 0, 0);
    for scoop in scoops {
        let over = block_cost.plus(scoop.cost).resources_over(block_limit);
        if !over.is_empty() && block_scoops > 0 {
            packing.orders_per_block.push(block_orders);
            packing.scoops_per_block.push(block_scoops);
            *packing.blocks_by_limit.entry(format!("block {}", over.join(" and "))).or_default() +=
                1;
            (block_cost, block_scoops, block_orders) = (Cost::default(), 0, 0);
        }
        block_cost = block_cost.plus(scoop.cost);
        block_scoops += 1;
        block_orders += scoop.order_count;
    }
    packing.partial_scoops = block_scoops;
    packing.partial_orders = block_orders;
    packing
}

// ─── Report ──────────────────────────────────────────────────────────────────

/// `value` with an SI prefix (k, M, G, T).
fn format_si(value: f64) -> String {
    match value {
        v if v >= 1e12 => format!("{:.2}T", v / 1e12),
        v if v >= 1e9 => format!("{:.2}G", v / 1e9),
        v if v >= 1e6 => format!("{:.2}M", v / 1e6),
        v if v >= 1e3 => format!("{:.1}k", v / 1e3),
        v => format!("{v:.0}"),
    }
}

fn format_limit(limit: &TxLimit) -> String {
    format!(
        "{} B, {} mem, {} steps",
        format_si(limit.bytes as f64),
        format_si(limit.mem as f64),
        format_si(limit.steps as f64)
    )
}

/// Mean, median, min and max of a non-empty sample.
struct Spread {
    mean: f64,
    median: usize,
    min: usize,
    max: usize,
}

fn spread(sample: &[usize]) -> Spread {
    let mut sorted = sample.to_vec();
    sorted.sort_unstable();
    Spread {
        mean: sorted.iter().sum::<usize>() as f64 / sorted.len() as f64,
        median: sorted[sorted.len() / 2],
        min: sorted[0],
        max: sorted[sorted.len() - 1],
    }
}

fn format_spread(sample: &[usize]) -> String {
    let Spread { mean, median, min, max } = spread(sample);
    format!("mean {mean:.1}, median {median}, min {min}, max {max}")
}

/// "reason: n of total unit" per reason, most frequent first; just the
/// reason when there is only one.
fn format_reason_counts<R: std::fmt::Display>(
    by_reason: &BTreeMap<R, usize>,
    unit: &str,
) -> String {
    if let [(reason, _)] = by_reason.iter().collect::<Vec<_>>().as_slice() {
        return reason.to_string();
    }
    let total: usize = by_reason.values().sum();
    let mut most_frequent_first: Vec<_> = by_reason.iter().collect();
    most_frequent_first.sort_by(|a, b| b.1.cmp(a.1));
    most_frequent_first
        .iter()
        .map(|(reason, count)| format!("{reason}: {count} of {total} {unit}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Block-capacity benchmark: orders per RB vs per EB, scoops cut under the
/// per-tx limits. Paths are relative to the repo root.
#[derive(Parser)]
struct Args {
    /// Queue parameters, written by `bench/block-capacity/dataset_params.py`.
    #[arg(long = "params", default_value_t = format!("{BLOCK_CAPACITY_DATA}/dataset-params.json"))]
    params_path: String,
    /// Scooper-format blueprint the scoop txs are built and evaluated against.
    #[arg(long = "blueprint", default_value = DEFAULT_BLUEPRINT)]
    blueprint_path: String,
    /// Number of orders in the generated queue.
    #[arg(long = "orders", default_value_t = 20_000)]
    queue_length: usize,
    /// Seed of the queue generator.
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Max orders per scoop (production `BatchLimits`).
    #[arg(long, default_value_t = BatchLimits::default().max_orders)]
    max_orders: usize,
    /// Per-tx limit, `bytes,mem,steps` (default: mainnet, mem as in the scooper's evaluator).
    #[arg(long, default_value = "16384,14000000,10000000000")]
    tx_limit: TxLimit,
    /// Ranking block limit, `bytes,mem,steps` (default: current mainnet Praos block).
    #[arg(long, default_value = "90112,72000000,20000000000")]
    rb_limit: TxLimit,
    /// Endorser block limit, `bytes,mem,steps`; repeat the option to compare several EB
    /// sizes on the same scoops (default: CIP-164 Table 7, S_EB-tx 12 MB, scaled by 1/16,
    /// 1/8, 1/4, 1/2 and 1).
    #[arg(long = "eb-limit", default_values = DEFAULT_EB_LIMITS)]
    eb_limits: Vec<TxLimit>,
    /// Write one CSV row per scoop to this path.
    #[arg(long = "csv")]
    csv_path: Option<String>,
    /// Write one CSV row per block limit (the RB, then each EB) to this path.
    #[arg(long = "blocks-csv")]
    blocks_csv_path: Option<String>,
}

/// CIP-164 Table 7 (12 MB, 7G mem, 2T steps) scaled by 1/16, 1/8, 1/4, 1/2 and 1,
/// as `bytes,mem,steps`.
const DEFAULT_EB_LIMITS: [&str; 5] = [
    "750000,437500000,125000000000",
    "1500000,875000000,250000000000",
    "3000000,1750000000,500000000000",
    "6000000,3500000000,1000000000000",
    "12000000,7000000000,2000000000000",
];

pub fn main() {
    let args = Args::parse();
    let run_started_at = std::time::Instant::now();

    let params = load_queue_params(&args.params_path);
    let mut blueprint_env = TestEnv::from_blueprint_file(&args.blueprint_path);
    // The per-tx limits production checks scoop txs against.
    blueprint_env.exec.max_tx_size = args.tx_limit.bytes;
    blueprint_env.exec.max_tx_ex_mem = args.tx_limit.mem;
    blueprint_env.exec.max_tx_ex_steps = args.tx_limit.steps;
    let reference_pool = build_reference_pool();
    let orders = generate_orders(
        &blueprint_env,
        &reference_pool,
        &params,
        args.queue_length,
        &mut StdRng::seed_from_u64(args.seed),
    );
    let market: BTreeMap<Ident, Arc<SundaeV4Pool>> = orders
        .iter()
        .flat_map(|order| &order.pools)
        .map(|pool| (pool_ident(pool).clone(), pool.clone()))
        .collect();
    let routes: Vec<RoutingPlan> = orders
        .iter()
        .map(|order| {
            route_over_order_pools(
                &order.pools,
                &reference_pool.asset_a,
                &reference_pool.asset_b,
            )
        })
        .collect();

    let (scoops, unscooped_orders) =
        cut_queue_into_scoops(&blueprint_env, &orders, &routes, &market, args.max_orders);

    println!(
        "\nqueue       {}",
        describe_queue_against_targets(&orders, &params)
    );
    if !scoops.is_empty() {
        let scoop_count = scoops.len() as f64;
        let mean_over_scoops = |scoop_metric: fn(&Scoop) -> f64| {
            scoops.iter().map(scoop_metric).sum::<f64>() / scoop_count
        };
        // The last scoop ends with the queue: noise, like the partial block.
        let mut scoops_by_cut_reason: BTreeMap<CutReason, usize> = BTreeMap::new();
        for scoop in scoops.iter().filter(|scoop| scoop.cut_reason != CutReason::EndOfQueue) {
            *scoops_by_cut_reason.entry(scoop.cut_reason.clone()).or_default() += 1;
        }
        println!(
            "scoops      {} under {}\n            orders per scoop  {}\n            \
             pools per scoop   {}\n            mean tx           {} B, {} mem, {} steps\n            \
             limited by        {}",
            scoops.len(),
            format_limit(&args.tx_limit),
            format_spread(&scoops.iter().map(|scoop| scoop.order_count).collect::<Vec<_>>()),
            format_spread(&scoops.iter().map(|scoop| scoop.pool_count).collect::<Vec<_>>()),
            format_si(mean_over_scoops(|scoop| scoop.cost.bytes as f64)),
            format_si(mean_over_scoops(|scoop| scoop.cost.mem as f64)),
            format_si(mean_over_scoops(|scoop| scoop.cost.steps as f64)),
            format_reason_counts(&scoops_by_cut_reason, "scoops"),
        );
    }
    let mut blocks_csv_content = String::from(
        "block,bytes,mem,steps,full_blocks,orders_mean,orders_median,orders_min,orders_max,\
         scoops_mean,limited_by\n",
    );
    let block_limits = std::iter::once(("RB", &args.rb_limit))
        .chain(args.eb_limits.iter().map(|eb_limit| ("EB", eb_limit)));
    for (block_kind, block_limit) in block_limits {
        let block_packing = pack_into_blocks(&scoops, block_limit);
        let limit_columns = format!(
            "{block_kind},{},{},{}",
            block_limit.bytes, block_limit.mem, block_limit.steps
        );
        if block_packing.orders_per_block.is_empty() {
            println!(
                "{block_kind}          {}: queue too short to fill one block ({} scoops, {} orders so far)",
                format_limit(block_limit),
                block_packing.partial_scoops,
                block_packing.partial_orders
            );
            blocks_csv_content += &format!("{limit_columns},0,,,,,,\n");
            continue;
        }
        let orders_per_block = spread(&block_packing.orders_per_block);
        blocks_csv_content += &format!(
            "{limit_columns},{},{:.1},{},{},{},{:.1},\"{}\"\n",
            block_packing.orders_per_block.len(),
            orders_per_block.mean,
            orders_per_block.median,
            orders_per_block.min,
            orders_per_block.max,
            spread(&block_packing.scoops_per_block).mean,
            format_reason_counts(&block_packing.blocks_by_limit, "blocks"),
        );
        println!(
            "{block_kind}          {}: {} full blocks\n            orders per block  {}\n            \
             scoops per block  {}\n            limited by        {}",
            format_limit(block_limit),
            block_packing.orders_per_block.len(),
            format_spread(&block_packing.orders_per_block),
            format_spread(&block_packing.scoops_per_block),
            format_reason_counts(&block_packing.blocks_by_limit, "blocks"),
        );
    }
    println!(
        "\n  params     {}\n  scripts    {}\n  seed       {}\n  time       {:.0} s",
        args.params_path,
        args.blueprint_path,
        args.seed,
        run_started_at.elapsed().as_secs_f64()
    );

    if let Some(csv_path) = &args.csv_path {
        let mut csv_content =
            String::from("scoop,first_order,orders,pools,tx_bytes,mem_padded,steps_padded,cut\n");
        for (scoop_index, scoop) in scoops.iter().enumerate() {
            csv_content += &format!(
                "{scoop_index},{},{},{},{},{},{},{}\n",
                scoop.first_order,
                scoop.order_count,
                scoop.pool_count,
                scoop.cost.bytes,
                scoop.cost.mem,
                scoop.cost.steps,
                scoop.cut_reason
            );
        }
        std::fs::write(csv_path, csv_content).unwrap_or_else(|error| panic!("{csv_path}: {error}"));
        println!("  csv        {csv_path}");
    }
    if let Some(blocks_csv_path) = &args.blocks_csv_path {
        std::fs::write(blocks_csv_path, blocks_csv_content)
            .unwrap_or_else(|error| panic!("{blocks_csv_path}: {error}"));
        println!("  blocks csv {blocks_csv_path}");
    }

    // The bench measures blocks, not the scooper: any order left behind
    // invalidates the run.
    if !unscooped_orders.is_empty() {
        println!(
            "\nINVALID RUN: {} order(s) quarantined, {} rejected by the accumulator",
            unscooped_orders.quarantined.len(),
            unscooped_orders.rejected.len()
        );
        let mut unscooped_by_reason: BTreeMap<String, usize> = BTreeMap::new();
        for (order_index, reason) in
            unscooped_orders.quarantined.iter().chain(&unscooped_orders.rejected)
        {
            let pool_count = orders[*order_index].pools.len();
            let truncated_reason: String = reason.chars().take(160).collect();
            *unscooped_by_reason
                .entry(format!("{pool_count}-pool order: {truncated_reason}"))
                .or_default() += 1;
        }
        for (reason, order_count) in &unscooped_by_reason {
            println!("  ×{order_count}: {reason}");
        }
        std::process::exit(1);
    }
}
