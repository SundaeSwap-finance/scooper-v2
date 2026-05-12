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
    /// Total LP after applying protocol share. Used in accumulator comparison tests.
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
/// Orders are matched by finding a pool whose assets include both the order's
/// offer token and min_received token.
pub fn group_orders_by_pool(
    orders: &[Arc<SundaeV4Order>],
    pools: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
) -> BTreeMap<Ident, Vec<Arc<SundaeV4Order>>> {
    let mut groups: BTreeMap<Ident, Vec<Arc<SundaeV4Order>>> = BTreeMap::new();

    for order in orders {
        if let Some(ident) = find_pool_for_simple_order(order, pools) {
            groups.entry(ident).or_default().push(order.clone());
        }
    }

    groups
}

/// Find a pool whose assets match the order's offer AND min_received tokens.
///
/// Phase B stance: scooper executes Swap orders as **full-consume** only. The
/// `remaining_offered` quantity from the order's Swap constraint is taken as
/// `dx`; one fulfillment output is produced at the order's destination
/// (matching the contract's `output.address != input.address` branch in
/// `validate_swap_order`). Partial fills — re-outputting to the same script
/// with a reduced `remaining_offered` and an unchanged `original_offered` /
/// `min_received` — are not yet supported. TODO(phase-B+): add a partial-fill
/// path so large limit-style orders can stream across multiple scoops.
pub fn find_pool_for_simple_order(
    order: &SundaeV4Order,
    pools: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
) -> Option<Ident> {
    let offer_asset = order.swap_offered().0;
    let ask_asset = order.swap_min_received().0;

    for (ident, pool) in pools {
        let has_offer = pool.pool_datum.assets.iter().any(|(a, _)| a == offer_asset);
        let has_ask = pool.pool_datum.assets.iter().any(|(a, _)| a == ask_asset);

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
/// Used by scoop_tests and accumulator comparison tests.
#[cfg(test)]
pub fn assemble_batch(
    pool: &Arc<SundaeV4Pool>,
    candidates: &[Arc<SundaeV4Order>],
    _fee: (u64, u64),
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
                &pool.pool_type,
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
        let prev_assets = replay_assets.clone();
        replay_assets[swap.input_idx].1 = &replay_assets[swap.input_idx].1 + &swap.dx;
        replay_assets[swap.output_idx].1 = &replay_assets[swap.output_idx].1 - &swap.dy;
        let fb = swap_math::compute_fee_budget(
            &pool.pool_type,
            &prev_assets,
            &replay_assets,
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
    pool_type: &PoolType,
) -> Option<ResolvedSwap> {
    let Some((input_idx, output_idx)) = detect_swap_direction(order, running_assets) else {
        return None;
    };

    // dx comes from the order's remaining_offered amount (partial fills not yet supported).
    let dx = order.swap_offered().1.clone();
    if !dx.is_positive() {
        return None;
    }

    let dy = compute_swap_result(pool_type, running_assets, input_idx, output_idx, &dx);
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

/// Dispatch swap result computation based on pool type.
pub fn compute_swap_result(
    pool_type: &PoolType,
    assets: &[(AssetClass, BigInt)],
    input_idx: usize,
    output_idx: usize,
    dx: &BigInt,
) -> BigInt {
    match pool_type {
        PoolType::ConstantProduct { fee } => {
            use num_traits::ToPrimitive;
            let fee_num = fee.num.clone().unwrap().to_u64().unwrap_or(0);
            let fee_den = fee.den.clone().unwrap().to_u64().unwrap_or(1);
            swap_math::cp_swap_result(
                &assets[input_idx].1,
                &assets[output_idx].1,
                dx,
                fee_num,
                fee_den,
            )
        }
        PoolType::ConstantSum { prices, fee, .. } => {
            swap_math::cs_swap_result(dx, prices, input_idx, output_idx, &fee.num, &fee.den)
        }
    }
}

/// Check whether an order can execute against the given pool state.
/// Returns a `ResolvedSwap` on success, or a descriptive error string explaining
/// why the order cannot execute.
pub fn check_order_executability(
    order: &Arc<SundaeV4Order>,
    pool_assets: &[(AssetClass, BigInt)],
    _total_lp: &BigInt,
    pool_type: &PoolType,
) -> Result<ResolvedSwap, String> {
    let (input_idx, output_idx) = detect_swap_direction(order, pool_assets)
        .ok_or_else(|| "no matching pool asset in order value".to_string())?;

    // dx comes from the order's remaining_offered amount.
    let dx = order.swap_offered().1.clone();
    if !dx.is_positive() {
        return Err("offered amount not positive".to_string());
    }

    let dy = compute_swap_result(pool_type, pool_assets, input_idx, output_idx, &dx);
    if !dy.is_positive() {
        return Err("swap output not positive".to_string());
    }

    // Check min_received
    let output_asset = &pool_assets[output_idx].0;
    let (ask_asset, min_qty) = order.swap_min_received();
    if ask_asset != output_asset {
        return Err("pool output asset doesn't match min_received token".to_string());
    }
    if &dy < min_qty {
        return Err(format!("below min_received: got {dy}, need {min_qty}"));
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

/// Detect swap direction from the order's explicit offer and min_received assets.
///
/// Works for N-asset pools: looks up both offer and min_received in the pool's
/// asset list, returning their indices.
pub fn detect_swap_direction(
    order: &SundaeV4Order,
    assets: &[(AssetClass, BigInt)],
) -> Option<(usize, usize)> {
    let offer_asset = order.swap_offered().0;
    let ask_asset = order.swap_min_received().0;
    let input_idx = assets.iter().position(|(a, _)| a == offer_asset)?;
    let output_idx = assets.iter().position(|(a, _)| a == ask_asset)?;
    if input_idx == output_idx {
        return None;
    }
    Some((input_idx, output_idx))
}

/// Check if dy satisfies the order's min_received constraint.
fn satisfies_min_received(
    order: &SundaeV4Order,
    output_asset: &AssetClass,
    dy: &BigInt,
) -> bool {
    let (ask_asset, min_qty) = order.swap_min_received();
    if ask_asset != output_asset {
        return false;
    }
    dy >= min_qty
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
            pool_type: PoolType::ConstantProduct {
                fee: Rational { num: BigInt::from(3), den: BigInt::from(1000) },
            },
            slot: 100,
            fee_split_config: None,
        })
    }

    fn make_buy_order(ada_amount: i64, min_token: i64, slot: u64) -> Arc<SundaeV4Order> {
        // ADA buy order: offering (ada_amount - 2M) ADA, want token_a
        let offer_amount = ada_amount - 2_000_000; // subtract min UTxO
        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(ada_amount));

        Arc::new(SundaeV4Order::test_swap_order(
            TransactionInput::new([slot as u8; 32].into(), 0),
            value,
            Multisig::Signature(vec![0xaa; 28]),
            Destination::SelfDestination,
            (ada(), BigInt::from(offer_amount)),
            (token_a(), BigInt::from(min_token)),
            BigInt::from(1_500_000i64),
            slot,
        ))
    }

    fn make_sell_order(token_amount: i64, min_ada: i64, slot: u64) -> Arc<SundaeV4Order> {
        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(2_000_000i64)); // min UTxO
        value.insert(&token_a(), BigInt::from(token_amount));

        Arc::new(SundaeV4Order::test_swap_order(
            TransactionInput::new([slot as u8; 32].into(), 0),
            value,
            Multisig::Signature(vec![0xaa; 28]),
            Destination::SelfDestination,
            (token_a(), BigInt::from(token_amount)),
            (ada(), BigInt::from(min_ada)),
            BigInt::from(1_500_000i64),
            slot,
        ))
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

    fn token_b() -> AssetClass {
        AssetClass { policy: vec![0x03], token: vec![0x04] }
    }

    fn token_c() -> AssetClass {
        AssetClass { policy: vec![0x05], token: vec![0x06] }
    }

    fn make_pool_3asset(
        ada_reserve: i64,
        token_a_reserve: i64,
        token_b_reserve: i64,
    ) -> Arc<SundaeV4Pool> {
        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(ada_reserve));
        value.insert(&token_a(), BigInt::from(token_a_reserve));
        value.insert(&token_b(), BigInt::from(token_b_reserve));

        Arc::new(SundaeV4Pool {
            input: TransactionInput::new([0xbb; 32].into(), 0),
            value,
            pool_datum: PoolDatum {
                assets: vec![
                    (ada(), BigInt::from(ada_reserve)),
                    (token_a(), BigInt::from(token_a_reserve)),
                    (token_b(), BigInt::from(token_b_reserve)),
                ],
                total_lp: BigInt::from(1_000_000),
                circulating_lp: BigInt::from(500_000),
                preminted_lp: BigInt::from(500_000),
                identifier: Ident::new(&[0xbe, 0xef]),
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

    fn make_pool_4asset(
        r0: i64, r1: i64, r2: i64, r3: i64,
    ) -> Arc<SundaeV4Pool> {
        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(r0));
        value.insert(&token_a(), BigInt::from(r1));
        value.insert(&token_b(), BigInt::from(r2));
        value.insert(&token_c(), BigInt::from(r3));

        Arc::new(SundaeV4Pool {
            input: TransactionInput::new([0xcc; 32].into(), 0),
            value,
            pool_datum: PoolDatum {
                assets: vec![
                    (ada(), BigInt::from(r0)),
                    (token_a(), BigInt::from(r1)),
                    (token_b(), BigInt::from(r2)),
                    (token_c(), BigInt::from(r3)),
                ],
                total_lp: BigInt::from(1_000_000),
                circulating_lp: BigInt::from(500_000),
                preminted_lp: BigInt::from(500_000),
                identifier: Ident::new(&[0xca, 0xfe]),
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

    /// Make an order swapping between arbitrary assets
    fn make_order(
        offer_asset: AssetClass,
        offer_amount: i64,
        ask_asset: AssetClass,
        min_ask: i64,
        slot: u64,
    ) -> Arc<SundaeV4Order> {
        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(2_000_000i64));
        value.insert(&offer_asset, BigInt::from(offer_amount));

        Arc::new(SundaeV4Order::test_swap_order(
            TransactionInput::new([slot as u8; 32].into(), 0),
            value,
            Multisig::Signature(vec![0xaa; 28]),
            Destination::SelfDestination,
            (offer_asset, BigInt::from(offer_amount)),
            (ask_asset, BigInt::from(min_ask)),
            BigInt::from(1_500_000i64),
            slot,
        ))
    }

    #[test]
    fn test_detect_swap_direction_3asset() {
        let pool = make_pool_3asset(1_000_000, 1_000_000, 1_000_000);
        // ADA→token_b: input_idx=0, output_idx=2
        let order = make_order(ada(), 10_000, token_b(), 1, 1);
        let dir = detect_swap_direction(&order, &pool.pool_datum.assets);
        assert_eq!(dir, Some((0, 2)));

        // token_a→token_b: input_idx=1, output_idx=2
        let order2 = make_order(token_a(), 5_000, token_b(), 1, 2);
        let dir2 = detect_swap_direction(&order2, &pool.pool_datum.assets);
        assert_eq!(dir2, Some((1, 2)));
    }

    #[test]
    fn test_detect_swap_direction_4asset() {
        let pool = make_pool_4asset(1_000_000, 1_000_000, 1_000_000, 1_000_000);
        // token_b→token_c: input_idx=2, output_idx=3
        let order = make_order(token_b(), 5_000, token_c(), 1, 1);
        let dir = detect_swap_direction(&order, &pool.pool_datum.assets);
        assert_eq!(dir, Some((2, 3)));
    }

    #[test]
    fn test_batch_assembly_3asset_cp() {
        let pool = make_pool_3asset(1_000_000_000, 1_000_000_000, 1_000_000_000);
        // Swap ADA→token_b (indices 0→2, skipping asset 1)
        let orders = vec![make_order(ada(), 10_000_000, token_b(), 1, 1)];
        let batch = assemble_batch(&pool, &orders, (3, 1000), (1, 2), &BatchLimits::default());
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.swaps.len(), 1);
        assert_eq!(batch.swaps[0].input_idx, 0);
        assert_eq!(batch.swaps[0].output_idx, 2);
        assert!(batch.swaps[0].dy.is_positive());
        // Final assets: asset[1] unchanged
        assert_eq!(batch.final_assets[1].1, BigInt::from(1_000_000_000i64));
    }

    #[test]
    fn test_batch_assembly_4asset_cp_mixed() {
        let pool = make_pool_4asset(1_000_000_000, 1_000_000_000, 1_000_000_000, 1_000_000_000);
        // Two orders using different pairs within the same 4-asset pool
        let orders = vec![
            make_order(ada(), 5_000_000, token_a(), 1, 1),     // 0→1
            make_order(token_b(), 5_000_000, token_c(), 1, 2), // 2→3
        ];
        let batch = assemble_batch(&pool, &orders, (3, 1000), (1, 2), &BatchLimits::default());
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.swaps.len(), 2);
        assert_eq!(batch.swaps[0].input_idx, 0);
        assert_eq!(batch.swaps[0].output_idx, 1);
        assert_eq!(batch.swaps[1].input_idx, 2);
        assert_eq!(batch.swaps[1].output_idx, 3);
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
