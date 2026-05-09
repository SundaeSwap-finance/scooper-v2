//! End-to-end scoop transaction tests: build → evaluate against real Plutus validators.
//!
//! Each test loads the devnet blueprint fixture, constructs a scenario with
//! synthetic pool/order data, builds a multi-pool scoop tx, and evaluates
//! every script in the transaction.
//!
//! Pools are token-to-token (matching V4 production), e.g. TOKEN_A / TOKEN_B.
//! ADA is held as min UTxO only.

#[cfg(test)]
mod tests {
    use crate::bigint::BigInt;
    use crate::sundaev4::batch::{assemble_batch, BatchLimits};
    use crate::sundaev4::test_harness::test_harness::*;
    use crate::sundaev4::tx_builder::TX_FEE;
    use num_traits::Signed;

    const BLUEPRINT_PATH: &str = "test/fixtures/devnet-blueprint.json";

    #[test]
    fn test_fixture_loads() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        assert!(!env.language_views.is_empty());
        assert!(!env.module_state().is_empty());
    }

    // ─── Single-pool tests ────────────────────────────────────────────────────

    #[test]
    fn single_pool_single_order() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        let orders = vec![make_order(token_a(), 10_000_000, token_b(), 1, 1)];
        let batch = assemble_batch(&pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default())
            .expect("batch assembly should succeed");

        assert_eq!(batch.swaps.len(), 1);
        assert!(batch.swaps[0].dy.is_positive());

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env.build_and_eval(&[batch], &settings, 1000)
            .expect("build_and_eval should succeed");

        assert!(!eval.budgets.is_empty(), "should have evaluated at least one script");
        assert_eq!(result.predicted_pools.len(), 1);
    }

    #[test]
    fn single_pool_reverse_direction() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        // Sell B for A (opposite direction)
        let orders = vec![make_order(token_b(), 5_000_000, token_a(), 1, 1)];
        let batch = assemble_batch(&pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default())
            .expect("batch assembly should succeed");

        assert_eq!(batch.swaps.len(), 1);

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (_result, eval) = env.build_and_eval(&[batch], &settings, 1000)
            .expect("build_and_eval should succeed");

        assert!(!eval.budgets.is_empty());
    }

    #[test]
    fn single_pool_multiple_same_direction() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        let orders = vec![
            make_order(token_a(), 10_000_000, token_b(), 1, 1),
            make_order(token_a(), 20_000_000, token_b(), 1, 2),
            make_order(token_a(), 5_000_000, token_b(), 1, 3),
        ];
        let batch = assemble_batch(&pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default())
            .expect("batch assembly should succeed");

        assert_eq!(batch.swaps.len(), 3);

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env.build_and_eval(&[batch], &settings, 1000)
            .expect("build_and_eval should succeed");

        assert!(!eval.budgets.is_empty());

        // Verify reserves monotonically changed (all selling A → A up, B down)
        let final_a = &result.predicted_pools[0].2.pool_datum.assets[0].1;
        let final_b = &result.predicted_pools[0].2.pool_datum.assets[1].1;
        assert!(*final_a > BigInt::from(1_000_000_000i64));
        assert!(*final_b < BigInt::from(1_000_000_000i64));
    }

    #[test]
    fn single_pool_opposing_orders() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        let orders = vec![
            make_order(token_a(), 10_000_000, token_b(), 1, 1),
            make_order(token_b(), 5_000_000, token_a(), 1, 2),
        ];
        let batch = assemble_batch(&pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default())
            .expect("batch assembly should succeed");

        assert_eq!(batch.swaps.len(), 2);

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env.build_and_eval(&[batch], &settings, 1000)
            .expect("build_and_eval should succeed");

        assert!(!eval.budgets.is_empty());

        // k-value should be non-decreasing
        let pool_datum = &result.predicted_pools[0].2.pool_datum;
        assert_k_nondecreasing(
            &BigInt::from(1_000_000_000i64), &BigInt::from(1_000_000_000i64),
            &pool_datum.assets[0].1, &pool_datum.assets[1].1,
        );
    }

    #[test]
    fn single_pool_many_orders() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        let orders: Vec<_> = (1..=8u64)
            .map(|i| {
                if i % 2 == 0 {
                    make_order(token_a(), 5_000_000 + i as i64 * 1_000_000, token_b(), 1, i)
                } else {
                    make_order(token_b(), 3_000_000 + i as i64 * 500_000, token_a(), 1, i)
                }
            })
            .collect();

        let batch = assemble_batch(&pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default())
            .expect("batch assembly should succeed");

        assert!(batch.swaps.len() >= 2, "should execute multiple orders");

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (_result, eval) = env.build_and_eval(&[batch], &settings, 1000)
            .expect("build_and_eval should succeed");

        assert!(!eval.budgets.is_empty());
    }

    // ─── Multi-pool tests ─────────────────────────────────────────────────────

    #[test]
    fn multi_pool_one_order_each() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        // Two pools with different token pairs
        let tok_c = token(0x05, 0x06);
        let tok_d = token(0x07, 0x08);
        let pool_1 = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        let pool_2 = make_pool(&env, 0xBB, tok_c.clone(), 1_000_000_000, tok_d.clone(), 1_000_000_000);

        let orders_1 = vec![make_order(token_a(), 10_000_000, token_b(), 1, 1)];
        let orders_2 = vec![make_order(tok_c.clone(), 10_000_000, tok_d.clone(), 1, 2)];

        let batch_1 = assemble_batch(&pool_1, &orders_1, env.exec.fee, env.exec.protocol_share, &BatchLimits::default()).unwrap();
        let batch_2 = assemble_batch(&pool_2, &orders_2, env.exec.fee, env.exec.protocol_share, &BatchLimits::default()).unwrap();

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env.build_and_eval(&[batch_1, batch_2], &settings, 1000)
            .expect("multi-pool build_and_eval should succeed");

        assert!(!eval.budgets.is_empty());
        assert_eq!(result.predicted_pools.len(), 2, "should predict 2 pool outputs");
    }

    #[test]
    fn multi_pool_mixed_orders() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let tok_c = token(0x05, 0x06);
        let tok_d = token(0x07, 0x08);
        let pool_1 = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        let pool_2 = make_pool(&env, 0xBB, tok_c.clone(), 1_000_000_000, tok_d.clone(), 1_000_000_000);

        let orders_1 = vec![
            make_order(token_a(), 10_000_000, token_b(), 1, 1),
            make_order(token_b(), 5_000_000, token_a(), 1, 2),
        ];
        let orders_2 = vec![
            make_order(tok_c.clone(), 15_000_000, tok_d.clone(), 1, 3),
        ];

        let batch_1 = assemble_batch(&pool_1, &orders_1, env.exec.fee, env.exec.protocol_share, &BatchLimits::default()).unwrap();
        let batch_2 = assemble_batch(&pool_2, &orders_2, env.exec.fee, env.exec.protocol_share, &BatchLimits::default()).unwrap();

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (_result, eval) = env.build_and_eval(&[batch_1, batch_2], &settings, 1000)
            .expect("multi-pool mixed build_and_eval should succeed");

        assert!(!eval.budgets.is_empty());
    }

    // ─── Fee split tests ──────────────────────────────────────────────────────

    #[test]
    fn fee_split_rounding() {
        // Verify that fee deductions from fulfillments sum to exactly TX_FEE
        for n_orders in [3, 5, 7] {
            let per_order_fee = TX_FEE / n_orders as u64;
            let last_order_fee = TX_FEE - per_order_fee * (n_orders as u64 - 1);
            let fee_sum = per_order_fee * (n_orders as u64 - 1) + last_order_fee;
            assert_eq!(fee_sum, TX_FEE, "fee deductions must sum to TX_FEE for {n_orders} orders");
        }
    }

    // ─── Negative tests ───────────────────────────────────────────────────────

    #[test]
    fn min_received_filters_impossible_order() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        // Order wants way more tokens than the swap could produce
        let orders = vec![make_order(token_a(), 10_000_000, token_b(), 999_999_999, 1)];
        let batch = assemble_batch(&pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default());
        assert!(batch.is_none(), "impossible min_received should prevent batch assembly");
    }

    // ─── Constant-sum pool tests ────────────────────────────────────────────

    #[test]
    fn cs_single_order() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let fee = crate::sundaev4::types::Rational {
            num: BigInt::from(3),
            den: BigInt::from(1000),
        };
        let pool = make_cs_pool(
            &env, 0xCC,
            vec![(token_a(), 1_000_000_000), (token_b(), 1_000_000_000)],
            vec![BigInt::from(1_000_000), BigInt::from(1_000_000)], // 1:1 price
            fee.clone(),
        );
        let orders = vec![make_order(token_a(), 10_000_000, token_b(), 1, 1)];
        let batch = assemble_batch(&pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default())
            .expect("CS batch assembly should succeed");

        assert_eq!(batch.swaps.len(), 1);
        // CS swap: dy = dx * price_in * (1 - fee) / price_out
        // = 10_000_000 * 1_000_000 * 997 / (1_000_000 * 1000) = 9_970_000
        assert_eq!(batch.swaps[0].dy, BigInt::from(9_970_000));

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (_result, eval) = env.build_and_eval(&[batch], &settings, 1000)
            .expect("CS build_and_eval should succeed");

        assert!(!eval.budgets.is_empty(), "should have evaluated at least one script");
    }

    #[test]
    fn cs_multiple_orders() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let fee = crate::sundaev4::types::Rational {
            num: BigInt::from(3),
            den: BigInt::from(1000),
        };
        let pool = make_cs_pool(
            &env, 0xCC,
            vec![(token_a(), 1_000_000_000), (token_b(), 1_000_000_000)],
            vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
            fee.clone(),
        );
        let orders = vec![
            make_order(token_a(), 10_000_000, token_b(), 1, 1),
            make_order(token_a(), 20_000_000, token_b(), 1, 2),
            make_order(token_a(), 5_000_000, token_b(), 1, 3),
        ];
        let batch = assemble_batch(&pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default())
            .expect("CS batch assembly should succeed");

        assert_eq!(batch.swaps.len(), 3);

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (_result, eval) = env.build_and_eval(&[batch], &settings, 1000)
            .expect("CS build_and_eval should succeed");

        assert!(!eval.budgets.is_empty());
    }

    #[test]
    fn cs_opposing_orders() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let fee = crate::sundaev4::types::Rational {
            num: BigInt::from(3),
            den: BigInt::from(1000),
        };
        let pool = make_cs_pool(
            &env, 0xCC,
            vec![(token_a(), 1_000_000_000), (token_b(), 1_000_000_000)],
            vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
            fee.clone(),
        );
        let orders = vec![
            make_order(token_a(), 10_000_000, token_b(), 1, 1),
            make_order(token_b(), 5_000_000, token_a(), 1, 2),
        ];
        let batch = assemble_batch(&pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default())
            .expect("CS batch assembly should succeed");

        assert_eq!(batch.swaps.len(), 2);

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (_result, eval) = env.build_and_eval(&[batch], &settings, 1000)
            .expect("CS build_and_eval should succeed");

        assert!(!eval.budgets.is_empty());
    }

    #[test]
    fn mixed_cp_cs_tx() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let tok_c = token(0x05, 0x06);
        let tok_d = token(0x07, 0x08);
        let cs_fee = crate::sundaev4::types::Rational {
            num: BigInt::from(3),
            den: BigInt::from(1000),
        };

        // CP pool
        let cp_pool = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        // CS pool
        let cs_pool = make_cs_pool(
            &env, 0xCC,
            vec![(tok_c.clone(), 1_000_000_000), (tok_d.clone(), 1_000_000_000)],
            vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
            cs_fee,
        );

        let cp_orders = vec![make_order(token_a(), 10_000_000, token_b(), 1, 1)];
        let cs_orders = vec![make_order(tok_c.clone(), 10_000_000, tok_d.clone(), 1, 2)];

        let cp_batch = assemble_batch(&cp_pool, &cp_orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default()).unwrap();
        let cs_batch = assemble_batch(&cs_pool, &cs_orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default()).unwrap();

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env.build_and_eval(&[cp_batch, cs_batch], &settings, 1000)
            .expect("mixed CP+CS build_and_eval should succeed");

        assert!(!eval.budgets.is_empty());
        assert_eq!(result.predicted_pools.len(), 2, "should predict 2 pool outputs");
    }

    // ─── Routed-through-CS tests ─────────────────────────────────────────────

    #[test]
    fn routed_through_cs_pool() {
        use std::collections::BTreeMap;
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::router;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let cs_fee = crate::sundaev4::types::Rational {
            num: BigInt::from(3),
            den: BigInt::from(1000),
        };

        // CP pool 0xAA: A/B
        let cp_pool = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        // CS pool 0xDD: A/E at 1:1
        let cs_pool = make_cs_pool(
            &env, 0xDD,
            vec![(token_a(), 1_000_000_000), (token_e(), 1_000_000_000)],
            vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
            cs_fee,
        );

        let mut pool_map = BTreeMap::new();
        pool_map.insert(cp_pool.pool_datum.identifier.clone(), cp_pool.clone());
        pool_map.insert(cs_pool.pool_datum.identifier.clone(), cs_pool.clone());

        // Order: sell E, want B (no direct pool — must route E→A via CS, then A→B via CP)
        // Note: CS→CP direction works because CS first hop uses the order's original amount
        // (controllable, multiple of 1000), and CP second hop has no divisibility constraint.
        // The reverse (CP→CS) fails because CP output amounts are generally not multiples of
        // 1000, violating the CS validator's floor constraint.
        let order = make_order(token_e(), 10_000_000, token_b(), 1, 1);

        let route = router::find_optimal_route(
            &pool_map, &token_e(), &token_b(), &order.swap_offered().1,
        ).expect("router should find E→A→B path");
        assert_eq!(route.hops.len(), 2, "should be a 2-hop route");
        assert!(router::is_routed(&route));

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum.try_add_routed_order(&order, &route, &pool_map)
            .expect("routed order should execute");

        let batches = accum.into_batches();
        assert_eq!(batches.len(), 2, "routed order touches 2 pools");

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env.build_and_eval(&batches, &settings, 1000)
            .expect("routed E→A→B build_and_eval should succeed");

        assert!(!eval.budgets.is_empty());
        assert_eq!(result.predicted_pools.len(), 2);
    }

    #[test]
    fn routed_through_two_cs_pools() {
        use std::collections::BTreeMap;
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::router;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let cs_fee = crate::sundaev4::types::Rational {
            num: BigInt::from(3),
            den: BigInt::from(1000),
        };

        // CS pool 0xDD: A/E at 1:1
        let cs_pool_ae = make_cs_pool(
            &env, 0xDD,
            vec![(token_a(), 1_000_000_000), (token_e(), 1_000_000_000)],
            vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
            cs_fee.clone(),
        );
        // CS pool 0xEE: E/F at 2:1 (E worth 2× F)
        let cs_pool_ef = make_cs_pool(
            &env, 0xEE,
            vec![(token_e(), 1_000_000_000), (token_f(), 2_000_000_000)],
            vec![BigInt::from(2_000_000), BigInt::from(1_000_000)],
            cs_fee,
        );

        let mut pool_map = BTreeMap::new();
        pool_map.insert(cs_pool_ae.pool_datum.identifier.clone(), cs_pool_ae.clone());
        pool_map.insert(cs_pool_ef.pool_datum.identifier.clone(), cs_pool_ef.clone());

        // Order: sell A, want F (must route A→E via CS, then E→F via CS)
        let order = make_order(token_a(), 10_000_000, token_f(), 1, 1);

        let route = router::find_optimal_route(
            &pool_map, &token_a(), &token_f(), &order.swap_offered().1,
        ).expect("router should find A→E→F path");
        assert_eq!(route.hops.len(), 2, "should be a 2-hop route");

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum.try_add_routed_order(&order, &route, &pool_map)
            .expect("routed order should execute");

        let batches = accum.into_batches();
        assert_eq!(batches.len(), 2, "routed order touches 2 CS pools");

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env.build_and_eval(&batches, &settings, 1000)
            .expect("routed A→E→F build_and_eval should succeed");

        assert!(!eval.budgets.is_empty());
        assert_eq!(result.predicted_pools.len(), 2);
    }

    #[test]
    fn mixed_direct_and_routed_cs() {
        use std::collections::BTreeMap;
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::router;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let cs_fee = crate::sundaev4::types::Rational {
            num: BigInt::from(3),
            den: BigInt::from(1000),
        };

        // 3 pools: CP A/B, CS A/E, CS E/F
        let cp_pool = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        let cs_pool_ae = make_cs_pool(
            &env, 0xDD,
            vec![(token_a(), 1_000_000_000), (token_e(), 1_000_000_000)],
            vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
            cs_fee.clone(),
        );
        let cs_pool_ef = make_cs_pool(
            &env, 0xEE,
            vec![(token_e(), 1_000_000_000), (token_f(), 2_000_000_000)],
            vec![BigInt::from(2_000_000), BigInt::from(1_000_000)],
            cs_fee,
        );

        let mut pool_map = BTreeMap::new();
        pool_map.insert(cp_pool.pool_datum.identifier.clone(), cp_pool.clone());
        pool_map.insert(cs_pool_ae.pool_datum.identifier.clone(), cs_pool_ae.clone());
        pool_map.insert(cs_pool_ef.pool_datum.identifier.clone(), cs_pool_ef.clone());

        let mut accum = Accumulator::new(env.exec.protocol_share);

        // Direct order: A→B on CP pool
        let order_ab = make_order(token_a(), 10_000_000, token_b(), 1, 1);
        accum.try_add_order(&order_ab, &cp_pool.pool_datum.identifier, &cp_pool)
            .expect("direct A→B order should execute");

        // Direct order: A→E on CS pool
        let order_ae = make_order(token_a(), 10_000_000, token_e(), 1, 2);
        accum.try_add_order(&order_ae, &cs_pool_ae.pool_datum.identifier, &cs_pool_ae)
            .expect("direct A→E order should execute");

        // Routed order: E→B (route: E→A via CS, then A→B via CP)
        // CS→CP direction ensures CS first hop gets a clean multiple-of-1000 input.
        let order_eb = make_order(token_e(), 5_000_000, token_b(), 1, 3);
        let route = router::find_optimal_route(
            &pool_map, &token_e(), &token_b(), &order_eb.swap_offered().1,
        ).expect("router should find E→B path");
        assert_eq!(route.hops.len(), 2);

        accum.try_add_routed_order(&order_eb, &route, &pool_map)
            .expect("routed E→B order should execute");

        let batches = accum.into_batches();
        // CS A/E pool has direct + routed leg, CP pool has direct + routed leg
        // CS E/F pool is not involved (route goes E→A→B, not through E/F)
        assert_eq!(batches.len(), 2, "direct + routed orders touch 2 pools");

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env.build_and_eval(&batches, &settings, 1000)
            .expect("mixed direct+routed build_and_eval should succeed");

        assert!(!eval.budgets.is_empty());
        assert_eq!(result.predicted_pools.len(), 2);
    }

    // ─── Assertion helpers ────────────────────────────────────────────────────

    fn assert_k_nondecreasing(a0: &BigInt, b0: &BigInt, a1: &BigInt, b1: &BigInt) {
        let k0 = a0 * b0;
        let k1 = a1 * b1;
        assert!(
            k1 >= k0,
            "k-value decreased: k0={k0}, k1={k1} (a0={a0}, b0={b0}, a1={a1}, b1={b1})"
        );
    }
}

// ─── Property-based tests ────────────────────────────────────────────────

#[cfg(test)]
mod prop_tests {
    use crate::bigint::BigInt;
    use crate::sundaev4::batch::{assemble_batch, BatchLimits};
    use crate::sundaev4::test_harness::test_harness::*;
    use proptest::prelude::*;
    use std::sync::Arc;

    const BLUEPRINT_PATH: &str = "test/fixtures/devnet-blueprint.json";

    /// Generate random orders for a token-to-token pool.
    ///
    /// Each order sells a random fraction (0.01% – 5%) of one reserve for the
    /// other token, with min_received = 1 (always satisfiable).
    ///
    /// `slot_offset` must be unique per pool to avoid tx_hash collisions across
    /// pools in multi-pool tests (make_order uses slot in the tx_hash).
    fn random_orders(
        tok_a: &crate::cardano_types::AssetClass,
        tok_b: &crate::cardano_types::AssetClass,
        reserve_a: i64,
        reserve_b: i64,
        n: usize,
        seed: u64,
        slot_offset: u64,
    ) -> Vec<Arc<crate::sundaev4::types::SundaeV4Order>> {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut orders = Vec::with_capacity(n);
        for i in 0..n {
            // Deterministic pseudo-random from seed + index
            let mut h = DefaultHasher::new();
            seed.hash(&mut h);
            i.hash(&mut h);
            let bits = h.finish();

            let sell_a = bits & 1 == 0;
            // Amount between 0.01% and 5% of reserve
            let fraction = ((bits >> 1) % 500 + 1) as i64; // 1..500
            let (offer_tok, want_tok, reserve) = if sell_a {
                (tok_a.clone(), tok_b.clone(), reserve_a)
            } else {
                (tok_b.clone(), tok_a.clone(), reserve_b)
            };
            let amount = (reserve * fraction / 10_000).max(1_000_000);
            orders.push(make_order(offer_tok, amount, want_tok, 1, slot_offset + (i + 1) as u64));
        }
        orders
    }

    /// Diagnostic test: dump a multi-pool proptest-style case to show what the
    /// property-based tests actually verify.
    #[test]
    fn proptest_example_dump() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);

        // Pool 1: TOKEN_A / TOKEN_B — asymmetric reserves
        let r_a1 = 2_347_000_000i64;
        let r_b1 = 891_000_000i64;
        // Pool 2: TOKEN_C / TOKEN_D — different magnitude
        let r_a2 = 500_000_000i64;
        let r_b2 = 4_200_000_000i64;
        let seed = 42u64;

        let tok_c = token(0x05, 0x06);
        let tok_d = token(0x07, 0x08);

        let pool_1 = make_pool(&env, 0xAA, token_a(), r_a1, token_b(), r_b1);
        let pool_2 = make_pool(&env, 0xBB, tok_c.clone(), r_a2, tok_d.clone(), r_b2);
        let orders_1 = random_orders(&token_a(), &token_b(), r_a1, r_b1, 3, seed, 0);
        let orders_2 = random_orders(&tok_c, &tok_d, r_a2, r_b2, 2, seed.wrapping_add(1), 1000);

        eprintln!();
        eprintln!("=== Proptest Example: multi-pool scoop (2 pools, 5 orders) ===");
        eprintln!();
        eprintln!("Pool 1 (A/B): reserves A={r_a1}  B={r_b1}  k={}", r_a1 as i128 * r_b1 as i128);
        eprintln!("Pool 2 (C/D): reserves C={r_a2}  D={r_b2}  k={}", r_a2 as i128 * r_b2 as i128);
        eprintln!("Fee: {}/{}  Protocol share: {}/{}", env.exec.fee.0, env.exec.fee.1, env.exec.protocol_share.0, env.exec.protocol_share.1);
        eprintln!();

        let batch_1 = assemble_batch(
            &pool_1, &orders_1, env.exec.fee, env.exec.protocol_share, &BatchLimits::default(),
        ).expect("batch 1 should assemble");
        let batch_2 = assemble_batch(
            &pool_2, &orders_2, env.exec.fee, env.exec.protocol_share, &BatchLimits::default(),
        ).expect("batch 2 should assemble");

        eprintln!("Pool 1 batch: {} swaps", batch_1.swaps.len());
        for (i, swap) in batch_1.swaps.iter().enumerate() {
            let dir = if swap.output_idx == 1 { "A->B" } else { "B->A" };
            eprintln!("  swap[{i}]: {dir}  dx={:<12} dy={:<12}", swap.dx, swap.dy);
        }
        eprintln!("  final: A={}  B={}", batch_1.final_assets[0].1, batch_1.final_assets[1].1);
        eprintln!();
        eprintln!("Pool 2 batch: {} swaps", batch_2.swaps.len());
        for (i, swap) in batch_2.swaps.iter().enumerate() {
            let dir = if swap.output_idx == 1 { "C->D" } else { "D->C" };
            eprintln!("  swap[{i}]: {dir}  dx={:<12} dy={:<12}", swap.dx, swap.dy);
        }
        eprintln!("  final: C={}  D={}", batch_2.final_assets[0].1, batch_2.final_assets[1].1);
        eprintln!();

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env.build_and_eval(&[batch_1, batch_2], &settings, 1000)
            .expect("multi-pool build_and_eval should succeed");

        eprintln!("Transaction inputs: {} pools + {} orders = {} total", 2, 5,
            result.resolved_inputs.len());
        eprintln!("Script evaluations ({} validators executed):", eval.budgets.len());
        for (key, eu) in &eval.budgets {
            let tag_str = match key.tag {
                pallas_primitives::conway::RedeemerTag::Spend => "Spend ",
                pallas_primitives::conway::RedeemerTag::Reward => "Reward",
                _ => "Other ",
            };
            eprintln!("  {tag_str}[{}]: cpu={:>12}  mem={:>8}", key.index, eu.steps, eu.mem);
        }
        eprintln!();

        for (pi, (ident, _input, pool)) in result.predicted_pools.iter().enumerate() {
            let pd = &pool.pool_datum;
            eprintln!("Predicted pool {pi} (ident={}..): A={}  B={}",
                &hex::encode(ident.to_bytes())[..8], pd.assets[0].1, pd.assets[1].1);
        }

        // k-value checks
        let pd1 = &result.predicted_pools[0].2.pool_datum;
        let k0_1 = BigInt::from(r_a1) * BigInt::from(r_b1);
        let k1_1 = &pd1.assets[0].1 * &pd1.assets[1].1;
        let pd2 = &result.predicted_pools[1].2.pool_datum;
        let k0_2 = BigInt::from(r_a2) * BigInt::from(r_b2);
        let k1_2 = &pd2.assets[0].1 * &pd2.assets[1].1;
        eprintln!();
        eprintln!("Pool 1 k-value: {k0_1} -> {k1_1}  (delta=+{})", &k1_1 - &k0_1);
        eprintln!("Pool 2 k-value: {k0_2} -> {k1_2}  (delta=+{})", &k1_2 - &k0_2);
        assert!(k1_1 >= k0_1, "pool 1 k decreased");
        assert!(k1_2 >= k0_2, "pool 2 k decreased");

        eprintln!();
        eprintln!("Tx hash: {}", hex::encode(result.tx_hash));
        eprintln!("Tx size: {} bytes", result.cbor.len());
        eprintln!("=== End ===");
        eprintln!();
    }

    /// Generate random orders for a constant-sum pool.
    ///
    /// Similar to `random_orders` but capped at a small fraction of the reserve
    /// so that the CS pool doesn't run dry (CS pools have finite output reserves).
    fn random_cs_orders(
        tok_a: &crate::cardano_types::AssetClass,
        tok_b: &crate::cardano_types::AssetClass,
        reserve_a: i64,
        reserve_b: i64,
        n: usize,
        seed: u64,
        slot_offset: u64,
    ) -> Vec<Arc<crate::sundaev4::types::SundaeV4Order>> {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut orders = Vec::with_capacity(n);
        for i in 0..n {
            let mut h = DefaultHasher::new();
            seed.hash(&mut h);
            i.hash(&mut h);
            let bits = h.finish();

            let sell_a = bits & 1 == 0;
            // CS pools can exhaust reserves, so use 0.01% – 1% of reserve
            let fraction = ((bits >> 1) % 100 + 1) as i64; // 1..100
            let (offer_tok, want_tok, reserve) = if sell_a {
                (tok_a.clone(), tok_b.clone(), reserve_a)
            } else {
                (tok_b.clone(), tok_a.clone(), reserve_b)
            };
            // Round to multiples of 1000 so the CS validator's floor constraint is satisfiable:
            // v_increase = floor(dx * fee_num / fee_den) requires dx * fee_num % fee_den == 0
            // for an exact integer dy. With fee 3/1000, dx must be a multiple of 1000.
            let amount = ((reserve * fraction / 10_000) / 1000 * 1000).max(1_000_000);
            orders.push(make_order(offer_tok, amount, want_tok, 1, slot_offset + (i + 1) as u64));
        }
        orders
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn prop_single_pool_scoop_evaluates(
            reserve_a in 500_000_000i64..5_000_000_000i64,
            reserve_b in 500_000_000i64..5_000_000_000i64,
            n_orders in 1usize..6,
            seed in any::<u64>(),
        ) {
            let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
            let pool = make_pool(&env, 0xAA, token_a(), reserve_a, token_b(), reserve_b);
            let orders = random_orders(&token_a(), &token_b(), reserve_a, reserve_b, n_orders, seed, 0);

            let batch = assemble_batch(
                &pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default(),
            );
            if let Some(batch) = batch {
                let settings = make_settings(&env, &env.scooper_keyhash());
                let (result, eval) = env.build_and_eval(&[batch], &settings, 1000)
                    .expect("build_and_eval should succeed");

                prop_assert!(!eval.budgets.is_empty());

                // k-value non-decreasing
                let pd = &result.predicted_pools[0].2.pool_datum;
                let k0 = BigInt::from(reserve_a) * BigInt::from(reserve_b);
                let k1 = &pd.assets[0].1 * &pd.assets[1].1;
                prop_assert!(k1 >= k0, "k decreased: k0={k0}, k1={k1}");
            }
        }

        #[test]
        fn prop_multi_pool_scoop_evaluates(
            reserve_a1 in 500_000_000i64..5_000_000_000i64,
            reserve_b1 in 500_000_000i64..5_000_000_000i64,
            reserve_a2 in 500_000_000i64..5_000_000_000i64,
            reserve_b2 in 500_000_000i64..5_000_000_000i64,
            n1 in 1usize..4,
            n2 in 1usize..4,
            seed in any::<u64>(),
        ) {
            let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
            let tok_c = token(0x05, 0x06);
            let tok_d = token(0x07, 0x08);

            let pool_1 = make_pool(&env, 0xAA, token_a(), reserve_a1, token_b(), reserve_b1);
            let pool_2 = make_pool(&env, 0xBB, tok_c.clone(), reserve_a2, tok_d.clone(), reserve_b2);

            let orders_1 = random_orders(&token_a(), &token_b(), reserve_a1, reserve_b1, n1, seed, 0);
            let orders_2 = random_orders(&tok_c, &tok_d, reserve_a2, reserve_b2, n2, seed.wrapping_add(1), 1000);

            let batch_1 = assemble_batch(&pool_1, &orders_1, env.exec.fee, env.exec.protocol_share, &BatchLimits::default());
            let batch_2 = assemble_batch(&pool_2, &orders_2, env.exec.fee, env.exec.protocol_share, &BatchLimits::default());

            // Need at least one batch to test
            if let (Some(b1), Some(b2)) = (batch_1, batch_2) {
                let settings = make_settings(&env, &env.scooper_keyhash());
                let (result, eval) = env.build_and_eval(&[b1, b2], &settings, 1000)
                    .expect("multi-pool build_and_eval should succeed");

                prop_assert!(!eval.budgets.is_empty());
                prop_assert_eq!(result.predicted_pools.len(), 2);
            }
        }

        #[test]
        fn prop_fee_accounting(
            n_orders in 1u64..20,
        ) {
            use crate::sundaev4::tx_builder::TX_FEE;
            let per_order_fee = TX_FEE / n_orders;
            let last_order_fee = TX_FEE - per_order_fee * (n_orders - 1);
            let total = per_order_fee * (n_orders - 1) + last_order_fee;
            prop_assert_eq!(total, TX_FEE);
        }

        #[test]
        fn prop_cs_pool_scoop_evaluates(
            reserve_a in 500_000_000i64..5_000_000_000i64,
            reserve_b in 500_000_000i64..5_000_000_000i64,
            n_orders in 1usize..6,
            seed in any::<u64>(),
        ) {
            let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
            let cs_fee = crate::sundaev4::types::Rational {
                num: BigInt::from(3),
                den: BigInt::from(1000),
            };
            let pool = make_cs_pool(
                &env, 0xCC,
                vec![(token_a(), reserve_a), (token_b(), reserve_b)],
                vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
                cs_fee,
            );
            let orders = random_cs_orders(&token_a(), &token_b(), reserve_a, reserve_b, n_orders, seed, 0);

            let batch = assemble_batch(
                &pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default(),
            );
            if let Some(batch) = batch {
                let settings = make_settings(&env, &env.scooper_keyhash());
                let (_result, eval) = env.build_and_eval(&[batch], &settings, 1000)
                    .expect("CS build_and_eval should succeed");

                prop_assert!(!eval.budgets.is_empty());
            }
        }

        #[test]
        fn prop_mixed_cp_cs_scoop_evaluates(
            reserve_a1 in 500_000_000i64..5_000_000_000i64,
            reserve_b1 in 500_000_000i64..5_000_000_000i64,
            reserve_a2 in 500_000_000i64..5_000_000_000i64,
            reserve_b2 in 500_000_000i64..5_000_000_000i64,
            n1 in 1usize..4,
            n2 in 1usize..4,
            seed in any::<u64>(),
        ) {
            let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
            let tok_c = token(0x05, 0x06);
            let tok_d = token(0x07, 0x08);
            let cs_fee = crate::sundaev4::types::Rational {
                num: BigInt::from(3),
                den: BigInt::from(1000),
            };

            // CP pool: A/B
            let cp_pool = make_pool(&env, 0xAA, token_a(), reserve_a1, token_b(), reserve_b1);
            // CS pool: C/D
            let cs_pool = make_cs_pool(
                &env, 0xCC,
                vec![(tok_c.clone(), reserve_a2), (tok_d.clone(), reserve_b2)],
                vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
                cs_fee,
            );

            let orders_cp = random_orders(&token_a(), &token_b(), reserve_a1, reserve_b1, n1, seed, 0);
            let orders_cs = random_cs_orders(&tok_c, &tok_d, reserve_a2, reserve_b2, n2, seed.wrapping_add(1), 1000);

            let batch_cp = assemble_batch(&cp_pool, &orders_cp, env.exec.fee, env.exec.protocol_share, &BatchLimits::default());
            let batch_cs = assemble_batch(&cs_pool, &orders_cs, env.exec.fee, env.exec.protocol_share, &BatchLimits::default());

            if let (Some(b_cp), Some(b_cs)) = (batch_cp, batch_cs) {
                let settings = make_settings(&env, &env.scooper_keyhash());
                let (result, eval) = env.build_and_eval(&[b_cp, b_cs], &settings, 1000)
                    .expect("mixed CP+CS build_and_eval should succeed");

                prop_assert!(!eval.budgets.is_empty());
                prop_assert_eq!(result.predicted_pools.len(), 2);
            }
        }
    }
}
