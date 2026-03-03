//! Batch assembly for V4 scooping: groups orders by pool and applies greedy
//! selection to produce batches ready for the tx builder.
//!
//! This module is pure (no IO) — all functions work on immutable snapshots.

use std::collections::BTreeMap;
use std::sync::Arc;

use num_traits::Signed;

use crate::bigint::BigInt;
use crate::cardano_types::AssetClass;
use crate::sundaev3::Ident;
use crate::sundaev4::swap_math;
use crate::sundaev4::types::*;

/// Fixed ADA amount returned to the user in fulfillment outputs.
/// For ADA buy orders, the remaining order ADA (minus this and tx fee) goes to the pool.
pub const FULFILLMENT_BASE_ADA: u64 = 2_000_000;

/// A resolved swap with precomputed math.
#[derive(Clone)]
pub struct ResolvedSwap {
    pub order: Arc<SundaeV4Order>,
    pub input_idx: usize,
    pub output_idx: usize,
    pub dx: BigInt,
    pub dy: BigInt,
    /// For routed orders: the actual output asset and amount (from final hop).
    /// When None, fulfillment uses this swap's output_idx/dy directly.
    pub fulfillment_override: Option<FulfillmentOverride>,
}

/// Override for the fulfillment output of a routed order.
#[derive(Clone)]
pub struct FulfillmentOverride {
    pub output_asset: AssetClass,
    pub amount: BigInt,
}

/// Continuation swap from a routed order passing through this pool.
/// Generates a transcript entry but doesn't consume an order input.
#[derive(Clone)]
pub struct ContinuationSwap {
    pub input_idx: usize,
    pub output_idx: usize,
    pub dx: BigInt,
    pub dy: BigInt,
}

/// Identifies an operation in the batch's interleaved order.
#[derive(Clone, Debug)]
pub enum BatchOp {
    Swap(usize),
    Continuation(usize),
}

/// A complete batch for one pool, ready for the tx builder.
#[derive(Clone)]
pub struct Batch {
    pub pool: Arc<SundaeV4Pool>,
    pub pool_ident: Ident,
    pub swaps: Vec<ResolvedSwap>,
    pub continuations: Vec<ContinuationSwap>,
    /// The interleaved order of swaps and continuations as they were
    /// accumulated. Used by the tx_builder to build transcript entries
    /// with correct intermediate reserve states.
    pub ops_order: Vec<BatchOp>,
    pub final_assets: Vec<(AssetClass, BigInt)>,
    #[allow(dead_code)]
    pub final_total_lp: BigInt,
}

/// Safety cap on total orders per transaction.
///
/// With incremental accumulation, the natural constraint is execution unit
/// limits. This is a backstop to prevent runaway accumulation.
pub struct BatchLimits {
    pub max_orders: usize,
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self { max_orders: 30 }
    }
}

/// Group pending orders by their target pool.
///
/// Orders with `Structured` constraints are matched by their first step's
/// `pool_ident`. Orders with `Simple` constraints are matched by finding a
/// pool whose non-ADA asset appears in the order value.
pub fn group_orders_by_pool(
    orders: &[Arc<SundaeV4Order>],
    pools: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
) -> BTreeMap<Ident, Vec<Arc<SundaeV4Order>>> {
    let mut groups: BTreeMap<Ident, Vec<Arc<SundaeV4Order>>> = BTreeMap::new();

    for order in orders {
        let pool_ident = match &order.datum.constraints {
            OrderConstraints::Structured { steps } => {
                steps.first().map(|s| s.pool_ident.clone())
            }
            OrderConstraints::Simple { .. } => {
                find_pool_for_simple_order(order, pools)
            }
        };

        if let Some(ident) = pool_ident {
            if pools.contains_key(&ident) {
                groups.entry(ident).or_default().push(order.clone());
            }
        }
    }

    groups
}

/// Find a pool whose assets match the order's offer AND ask tokens.
///
/// For a pool to match, it must have BOTH:
/// - An asset matching the order's offer (present in the order's UTxO value), AND
/// - An asset matching the order's ask (from min_received constraints)
///
/// For ADA→token buy orders (no non-ADA in order value), the pool must have
/// the min_received asset and ADA as the other asset.
pub fn find_pool_for_simple_order(
    order: &SundaeV4Order,
    pools: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
) -> Option<Ident> {
    let min_received_assets: Vec<&AssetClass> =
        if let OrderConstraints::Simple { min_received } = &order.datum.constraints {
            min_received.iter().map(|(a, _)| a).collect()
        } else {
            vec![]
        };

    // Determine if the order's offer is a non-ADA token.
    // Order UTxOs always carry ~5M ADA as min-UTxO, so ADA > 2M alone doesn't
    // mean the order is offering ADA. If the order has ANY non-ADA token with
    // positive balance, the offer is that token, not ADA.
    let order_has_non_ada_tokens = order.value.0.iter().any(|(policy, tokens)| {
        !policy.is_empty() && tokens.values().any(|qty| qty.is_positive())
    });

    for (ident, pool) in pools {
        // Check if order's actual offer token is in pool's assets
        let has_offer = if order_has_non_ada_tokens {
            // Offer is a non-ADA token — only match pools containing that token
            pool.pool_datum.assets.iter().any(|(asset, _)| {
                !(asset.policy.is_empty() && asset.token.is_empty())
                    && order.value.get(asset).is_positive()
            })
        } else {
            // No non-ADA tokens → offering ADA (must have > 2M)
            let ada = AssetClass { policy: vec![], token: vec![] };
            order.value.get(&ada) > BigInt::from(2_000_000i64)
                && pool.pool_datum.assets.iter().any(|(a, _)| {
                    a.policy.is_empty() && a.token.is_empty()
                })
        };

        // Check if order's ask token is in pool's assets
        let has_ask = min_received_assets.is_empty()
            || min_received_assets.iter().all(|ask_asset| {
                if ask_asset.policy.is_empty() && ask_asset.token.is_empty() {
                    // Asking for ADA — pool should have an ADA pair
                    pool.pool_datum.assets.iter().any(|(a, _)| a.policy.is_empty() && a.token.is_empty())
                } else {
                    pool.pool_datum.assets.iter().any(|(a, _)| a == *ask_asset)
                }
            });

        if has_offer && has_ask {
            return Some(ident.clone());
        }
    }
    None
}

/// Greedy batch assembly: scan candidates oldest→newest, try to execute each
/// against running pool state. If a swap succeeds and satisfies min_received,
/// include it and restart from the beginning (a sell might enable an earlier buy).
///
/// Returns `None` if no orders can be executed.
#[allow(dead_code)]
pub fn assemble_batch(
    pool: &Arc<SundaeV4Pool>,
    candidates: &[Arc<SundaeV4Order>],
    fee: (u64, u64),
    protocol_share: (u64, u64),
    limits: &BatchLimits,
) -> Option<Batch> {
    if candidates.is_empty() {
        return None;
    }

    let mut running_assets = pool.pool_datum.assets.clone();
    let initial_total_lp = pool.pool_datum.total_lp.clone();

    let mut selected: Vec<ResolvedSwap> = Vec::new();
    let mut used: Vec<bool> = vec![false; candidates.len()];

    loop {
        let mut added_any = false;

        for (i, order) in candidates.iter().enumerate() {
            if used[i] || selected.len() >= limits.max_orders {
                continue;
            }

            if let Some(swap) = try_execute_order(
                order,
                &running_assets,
                &initial_total_lp,
                fee,
                protocol_share,
            ) {
                // Update running reserves
                let in_idx = swap.input_idx;
                let out_idx = swap.output_idx;
                running_assets[in_idx].1 = &running_assets[in_idx].1 + &swap.dx;
                running_assets[out_idx].1 = &running_assets[out_idx].1 - &swap.dy;

                used[i] = true;
                selected.push(swap);
                added_any = true;
            }
        }

        // If nothing was added this pass, or we've hit the limit, stop
        if !added_any || selected.len() >= limits.max_orders {
            break;
        }
    }

    if selected.is_empty() {
        return None;
    }

    // Compute final_total_lp using per-order fee_budgets with constant LP
    // (matching the tx_builder's per-order transcript approach).
    let mut total_fee_budget = BigInt::from(0);
    let mut replay_assets = pool.pool_datum.assets.clone();
    for swap in &selected {
        let prev_a = replay_assets[0].1.clone();
        let prev_b = replay_assets[1].1.clone();
        replay_assets[swap.input_idx].1 = &replay_assets[swap.input_idx].1 + &swap.dx;
        replay_assets[swap.output_idx].1 = &replay_assets[swap.output_idx].1 - &swap.dy;
        let fb = swap_math::cp_fee_budget(
            &prev_a, &prev_b,
            &replay_assets[0].1, &replay_assets[1].1,
            &initial_total_lp,
        );
        total_fee_budget = &total_fee_budget + &fb;
    }
    let total_protocol_lp = swap_math::compute_protocol_lp(
        &total_fee_budget, protocol_share.0, protocol_share.1,
    );
    let final_total_lp = &initial_total_lp + &total_protocol_lp;

    let ops_order: Vec<BatchOp> = (0..selected.len()).map(BatchOp::Swap).collect();
    Some(Batch {
        pool: pool.clone(),
        pool_ident: pool.pool_datum.identifier.clone(),
        swaps: selected,
        continuations: Vec::new(),
        ops_order,
        final_assets: running_assets,
        final_total_lp,
    })
}

/// Try to execute a single order against the current running pool state.
/// Returns a `ResolvedSwap` if the swap produces positive output and
/// satisfies min_received constraints, or `None` otherwise.
pub fn try_execute_order(
    order: &Arc<SundaeV4Order>,
    running_assets: &[(AssetClass, BigInt)],
    _running_total_lp: &BigInt,
    fee: (u64, u64),
    _protocol_share: (u64, u64),
) -> Option<ResolvedSwap> {
    let Some((input_idx, output_idx)) = detect_swap_direction_from_assets(order, running_assets) else {

        return None;
    };

    let reserve_in = &running_assets[input_idx].1;
    let reserve_out = &running_assets[output_idx].1;

    let offered_asset = &running_assets[input_idx].0;
    let raw_value = order.value.get(offered_asset);
    // For ADA buy orders, dx = order ADA minus the fulfillment base (which stays with the user).
    let dx = if offered_asset.policy.is_empty() && offered_asset.token.is_empty() {
        raw_value - BigInt::from(FULFILLMENT_BASE_ADA as i64)
    } else {
        raw_value
    };
    if !dx.is_positive() {
        return None;
    }

    let dy = swap_math::cp_swap_result(reserve_in, reserve_out, &dx, fee.0, fee.1);
    if !dy.is_positive() {
        return None;
    }

    // Check min_received constraint
    if !satisfies_min_received(order, &running_assets[output_idx].0, &dy) {
        return None;
    }

    Some(ResolvedSwap {
        order: order.clone(),
        input_idx,
        output_idx,
        dx,
        dy,
        fulfillment_override: None,
    })
}

/// Check whether an order can execute against the given pool state.
/// Returns a `ResolvedSwap` on success, or a descriptive error string explaining
/// why the order cannot execute.
pub fn check_order_executability(
    order: &Arc<SundaeV4Order>,
    pool_assets: &[(AssetClass, BigInt)],
    _total_lp: &BigInt,
    fee: (u64, u64),
    _protocol_share: (u64, u64),
) -> Result<ResolvedSwap, String> {
    let (input_idx, output_idx) = detect_swap_direction_from_assets(order, pool_assets)
        .ok_or_else(|| "no matching pool asset in order value".to_string())?;

    let reserve_in = &pool_assets[input_idx].1;
    let reserve_out = &pool_assets[output_idx].1;

    let offered_asset = &pool_assets[input_idx].0;
    let raw_value = order.value.get(offered_asset);
    let dx = if offered_asset.policy.is_empty() && offered_asset.token.is_empty() {
        raw_value - BigInt::from(FULFILLMENT_BASE_ADA as i64)
    } else {
        raw_value
    };
    if !dx.is_positive() {
        return Err("offered amount not positive".to_string());
    }

    let dy = swap_math::cp_swap_result(reserve_in, reserve_out, &dx, fee.0, fee.1);
    if !dy.is_positive() {
        return Err("swap output not positive".to_string());
    }

    // Check min_received constraint
    let output_asset = &pool_assets[output_idx].0;
    match &order.datum.constraints {
        OrderConstraints::Simple { min_received } => {
            for (asset, min_qty) in min_received {
                if asset == output_asset && &dy < min_qty {
                    return Err(format!("below min_received: got {dy}, need {min_qty}"));
                }
            }
            // Verify the pool actually provides the asked token
            if !min_received.is_empty()
                && !min_received.iter().any(|(asset, _)| asset == output_asset)
            {
                return Err("pool output asset doesn't match min_received token".to_string());
            }
        }
        OrderConstraints::Structured { .. } => {}
    }

    Ok(ResolvedSwap {
        order: order.clone(),
        input_idx,
        output_idx,
        dx,
        dy,
        fulfillment_override: None,
    })
}

/// Detect swap direction by checking which pool asset the order offers.
pub fn detect_swap_direction_from_assets(
    order: &SundaeV4Order,
    assets: &[(AssetClass, BigInt)],
) -> Option<(usize, usize)> {
    for (idx, (asset, _)) in assets.iter().enumerate() {
        if asset.policy.is_empty() && asset.token.is_empty() {
            continue;
        }
        if order.value.get(asset).is_positive() {
            let output_idx = if idx == 0 { 1 } else { 0 };
            return Some((idx, output_idx));
        }
    }
    // Fallback: offering ADA if it has more than min UTxO.
    // Find the actual ADA index rather than assuming it's at position 0.
    let ada = AssetClass { policy: vec![], token: vec![] };
    let ada_amount = order.value.get(&ada);
    if ada_amount > BigInt::from(2_000_000i64) {
        if let Some(ada_idx) = assets.iter().position(|(a, _)| a.policy.is_empty() && a.token.is_empty()) {
            let out_idx = if ada_idx == 0 { 1 } else { 0 };
            return Some((ada_idx, out_idx));
        }
    }
    None
}

/// Check if dy satisfies the order's min_received constraint.
fn satisfies_min_received(
    order: &SundaeV4Order,
    output_asset: &AssetClass,
    dy: &BigInt,
) -> bool {
    match &order.datum.constraints {
        OrderConstraints::Simple { min_received } => {
            for (asset, min_qty) in min_received {
                if asset == output_asset {
                    return dy >= min_qty;
                }
            }
            // min_received doesn't reference the output asset — this pool
            // can't satisfy the order (wrong token pair)
            min_received.is_empty()
        }
        OrderConstraints::Structured { .. } => {
            // Structured constraints don't have explicit min_received —
            // the constraint is enforced by the on-chain validator
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cardano_types::{TransactionInput, Value};
    use crate::multisig::Multisig;
    use pallas_codec::utils::MaybeIndefArray;

    fn ada() -> AssetClass {
        AssetClass { policy: vec![], token: vec![] }
    }

    fn token_a() -> AssetClass {
        AssetClass { policy: vec![0x01], token: vec![0x02] }
    }

    fn unit_pd() -> pallas_primitives::PlutusData {
        pallas_primitives::PlutusData::Constr(pallas_primitives::Constr {
            tag: 121,
            any_constructor: None,
            fields: MaybeIndefArray::Def(vec![]),
        })
    }

    fn make_pool(ada_reserve: i64, token_reserve: i64) -> Arc<SundaeV4Pool> {
        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(ada_reserve));
        value.insert(&token_a(), BigInt::from(token_reserve));

        Arc::new(SundaeV4Pool {
            input: TransactionInput::new([0xaa; 32].into(), 0),
            value,
            pool_datum: PoolDatum {
                assets: vec![
                    (ada(), BigInt::from(ada_reserve)),
                    (token_a(), BigInt::from(token_reserve)),
                ],
                total_lp: BigInt::from(1_000_000),
                circulating_lp: BigInt::from(500_000),
                preminted_lp: BigInt::from(500_000),
                identifier: Ident::new(&[0xde, 0xad]),
                actions: vec![],
                module_state: vec![],
            },
            slot: 100,
        })
    }

    fn make_buy_order(ada_amount: i64, min_token: i64, slot: u64) -> Arc<SundaeV4Order> {
        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(ada_amount));

        Arc::new(SundaeV4Order {
            input: TransactionInput::new([slot as u8; 32].into(), 0),
            value,
            datum: OrderDatum {
                owner: Multisig::Signature(vec![0xaa; 28]),
                destination: Destination::SelfDestination,
                constraints: OrderConstraints::Simple {
                    min_received: vec![(token_a(), BigInt::from(min_token))],
                },
                extension: unit_pd(),
            },
            slot,
        })
    }

    fn make_sell_order(token_amount: i64, min_ada: i64, slot: u64) -> Arc<SundaeV4Order> {
        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(2_000_000)); // min UTxO
        value.insert(&token_a(), BigInt::from(token_amount));

        Arc::new(SundaeV4Order {
            input: TransactionInput::new([slot as u8; 32].into(), 0),
            value,
            datum: OrderDatum {
                owner: Multisig::Signature(vec![0xaa; 28]),
                destination: Destination::SelfDestination,
                constraints: OrderConstraints::Simple {
                    min_received: vec![(ada(), BigInt::from(min_ada))],
                },
                extension: unit_pd(),
            },
            slot,
        })
    }

    #[test]
    fn test_single_order_batch() {
        let pool = make_pool(1_000_000_000, 1_000_000_000);
        let orders = vec![make_buy_order(10_000_000, 1, 1)];

        let batch = assemble_batch(&pool, &orders, (3, 1000), (1, 2), &BatchLimits::default());
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.swaps.len(), 1);
        assert!(batch.swaps[0].dy.is_positive());
    }

    #[test]
    fn test_multiple_orders_same_direction() {
        let pool = make_pool(1_000_000_000, 1_000_000_000);
        let orders = vec![
            make_buy_order(10_000_000, 1, 1),
            make_buy_order(20_000_000, 1, 2),
            make_buy_order(5_000_000, 1, 3),
        ];

        let batch = assemble_batch(&pool, &orders, (3, 1000), (1, 2), &BatchLimits::default());
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.swaps.len(), 3);
    }

    #[test]
    fn test_opposing_directions() {
        let pool = make_pool(1_000_000_000, 1_000_000_000);
        let orders = vec![
            make_buy_order(10_000_000, 1, 1),
            make_sell_order(5_000_000, 1, 2),
        ];

        let batch = assemble_batch(&pool, &orders, (3, 1000), (1, 2), &BatchLimits::default());
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.swaps.len(), 2);
    }

    #[test]
    fn test_min_received_filters_order() {
        let pool = make_pool(1_000_000_000, 1_000_000_000);
        // Order wants way more tokens than the swap would produce
        let orders = vec![make_buy_order(10_000_000, 999_999_999, 1)];

        let batch = assemble_batch(&pool, &orders, (3, 1000), (1, 2), &BatchLimits::default());
        assert!(batch.is_none());
    }

    #[test]
    fn test_max_orders_limit() {
        let pool = make_pool(1_000_000_000, 1_000_000_000);
        let orders: Vec<_> = (1..=20u64)
            .map(|i| make_buy_order(10_000_000, 1, i))
            .collect();

        let limits = BatchLimits { max_orders: 5 };
        let batch = assemble_batch(&pool, &orders, (3, 1000), (1, 2), &limits);
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().swaps.len(), 5);
    }

    #[test]
    fn test_empty_candidates() {
        let pool = make_pool(1_000_000, 1_000_000);
        let batch = assemble_batch(&pool, &[], (3, 1000), (1, 2), &BatchLimits::default());
        assert!(batch.is_none());
    }

    #[test]
    fn test_group_orders_by_pool() {
        let pool = make_pool(1_000_000_000, 1_000_000_000);
        let ident = pool.pool_datum.identifier.clone();
        let mut pools = BTreeMap::new();
        pools.insert(ident.clone(), pool);

        let orders = vec![
            make_buy_order(10_000_000, 1, 1),
            make_sell_order(5_000_000, 1, 2),
        ];

        let groups = group_orders_by_pool(&orders, &pools);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[&ident].len(), 2);
    }
}
