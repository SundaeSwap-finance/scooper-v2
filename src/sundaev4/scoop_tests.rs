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

    /// Reproduces the live router-parity divergence (order 84e3e4c8): the Go
    /// resolver blends the direct f46671fb leg with a multi-hop MNGO→tOKENA→MINT
    /// leg for 374G, but the scooper's find_blended_route single-hops (direct
    /// split across the MNGO/MINT pools) for only 362G. token_a=MINT, token_b=
    /// MNGO, token_e=tOKENA. Exact live reserves.
    #[test]
    fn repro_router_parity_84e3e4c8() {
        use std::collections::BTreeMap;
        use crate::sundaev4::router;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let mut pool_map = BTreeMap::new();
        for p in [
            // Direct MNGO/MINT pools (token_a=MINT, token_b=MNGO).
            make_cl_pool(&env, 0x46, token_a(), 349_594_488_343, token_b(), 53_852_218_245,
                6_765_584_440_427, 97, 100, 103, 100, 5, 10000), // f46671fb
            make_cl_pool(&env, 0x80, token_a(), 125_000_000_000, token_b(), 125_000_000_000,
                1_309_382_102_236, 90, 100, 110, 100, 5, 10000), // 80c8d105
            make_cl_pool(&env, 0xb5, token_a(), 75_100_000_000, token_b(), 74_899_865_896,
                495_795_726_371, 85, 100, 118, 100, 10, 10000),  // b56479f5
            make_pool(&env, 0xc2, token_a(), 125_000_000_000, token_b(), 125_000_000_000), // c21bd0b9 CP
            make_cl_pool(&env, 0x65, token_a(), 144_196_000_000, token_b(), 0,
                4_000_000_000_000, 1095, 1000, 1140, 1000, 30, 10000), // 65fe0a9e (value-losing)
            // Multi-hop legs: MNGO→tOKENA (3f94fbd6), tOKENA→MINT (ea58fe73).
            make_pool(&env, 0x3f, token_e(), 300_000_000_000, token_b(), 150_000_000_000), // tOKENA/MNGO
            make_pool(&env, 0xea, token_e(), 200_000_000_000, token_a(), 100_000_000_000), // tOKENA/MINT
            // ADA legs: MNGO→ADA (a22550f2), ADA→MINT (31bba660).
            make_pool(&env, 0xa2, token_f(), 62_500_000_000, token_b(), 25_000_000_000),   // ADA/MNGO
            make_pool(&env, 0x31, token_f(), 625_000_000_000, token_a(), 250_000_000_000), // ADA/MINT
        ] {
            pool_map.insert(p.pool_datum.identifier.clone(), p);
        }

        let order = make_order(token_b(), 400_000_000_000, token_a(), 1, 1);
        // Order budget: 5 ADA → max_pools=5, max_steps=10 (cost 1/0.5 ADA).
        let limits = router::RoutingLimits::from_budget(5_000_000, 1_000_000, 500_000);

        let blend = router::find_blended_route(
            &pool_map, &[], &token_b(), &token_a(), &order.swap_offered().1, limits,
        ).expect("route exists");
        eprintln!("=== blend: branches={} total_out={}", blend.branches.len(), blend.total_output);
        for (bi, br) in blend.branches.iter().enumerate() {
            for (hi, hop) in br.hops.iter().enumerate() {
                for sp in &hop.splits {
                    eprintln!("  b{bi} h{hi} pool={} in={} out={}",
                        hex::encode(&sp.pool.ident.to_bytes()[..1]), sp.input_amount, sp.output_amount);
                }
            }
        }
        let single = router::find_optimal_route(
            &pool_map, &[], &token_b(), &token_a(), &order.swap_offered().1, limits,
        ).expect("single route exists");
        eprintln!("=== find_optimal_route: hops={} total_out={}", single.hops.len(), single.total_output);

        // The full blend fits max_pools=5 (direct 1 + tOKENA 2 + ADA 2), so it
        // isn't pruned; it must beat the single-hop answer.
        assert!(blend.branches.len() >= 2, "expected a multi-branch blend at 5-pool budget");
        assert!(blend.total_output > single.total_output, "blend must beat single-hop");

        // TIGHT budget (3 pools): the full 5-pool blend now exceeds the budget.
        // Budget-aware pruning must find the best route that FITS 3 pools (direct
        // + ONE multi-hop leg) rather than collapsing to the single-hop fallback.
        let tight = router::RoutingLimits::from_budget(3_000_000, 1_000_000, 500_000);
        let tight_single = router::find_optimal_route(
            &pool_map, &[], &token_b(), &token_a(), &order.swap_offered().1, tight,
        ).expect("tight single exists");
        let tight_blend = router::find_blended_route(
            &pool_map, &[], &token_b(), &token_a(), &order.swap_offered().1, tight,
        ).expect("tight blend exists");
        let tpools: std::collections::BTreeSet<_> = tight_blend.branches.iter()
            .flat_map(|b| b.hops.iter()).flat_map(|h| h.splits.iter().map(|s| s.pool.ident.clone()))
            .collect();
        eprintln!("=== TIGHT(3): branches={} pools={} total_out={} (single={})",
            tight_blend.branches.len(), tpools.len(), tight_blend.total_output, tight_single.total_output);
        assert!(tpools.len() <= 3, "pruned blend must fit the 3-pool budget, used {}", tpools.len());
        assert!(tight_blend.total_output > tight_single.total_output,
            "budget-aware pruning must beat single-hop, not fall back to it");
    }

    /// Exact reproduction of the live quarantine (order 565ec5d3), a
    /// tOKENE→tOKENB swap of 400G against the live tOKENB/tOKENE v4 pool set.
    ///
    /// Root cause (confirmed against production): the order carries the
    /// ROUTE-ORDER constraint module (alongside swap + fairness), which forbids
    /// parallel blending. The optimizer's best answer is a 5-way split (fillable,
    /// output 364.7G ≥ the 363.3G floor), but the route module forces
    /// `collapse_to_serial`, which routes the WHOLE 400G through the single
    /// deepest pool (f46671fb). f46671fb's cl_max_dx ≈ 363.6G, so 400G over-drains
    /// (dy≈383G > 350G reserve). collapse_to_serial must REJECT that (→ order
    /// skipped) rather than emit an over-draining serial route. This test asserts
    /// both the blended path (fillable) and the collapse path (rejected) are safe.
    #[test]
    fn repro_565ec5d3_f46671fb_over_drain() {
        use std::collections::BTreeMap;
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::router;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        // token_a = tOKENB (asset A / idx0), token_b = tOKENE (asset B / idx1).
        // The FULL live tOKENB/tOKENE v4 pool set on preview (dumped from the
        // scooper at the time of the quarantine), so the router's path search
        // sees exactly what it saw in production.
        let mut pool_map = BTreeMap::new();
        for p in [
            // f46671fb — the tight pool that over-drained (350G B / 53.45G E).
            make_cl_pool(&env, 0x46, token_a(), 350_000_000_000, token_b(), 53_452_218_245,
                6_765_375_373_647, 97, 100, 103, 100, 5, 10000),
            // 80c8d105 — mid CL, wider range [0.9,1.1].
            make_cl_pool(&env, 0x80, token_a(), 125_000_000_000, token_b(), 125_000_000_000,
                1_309_382_102_236, 90, 100, 110, 100, 5, 10000),
            // b56479f5 — CL, range [0.85,1.18].
            make_cl_pool(&env, 0xb5, token_a(), 75_100_000_000, token_b(), 74_899_865_896,
                495_795_726_371, 85, 100, 118, 100, 10, 10000),
            // c21bd0b9 — constant-product 125G/125G.
            make_pool(&env, 0xc2, token_a(), 125_000_000_000, token_b(), 125_000_000_000),
            // 65fe0a9e — dormant one-sided CL (all B, 0 E, parked out of range).
            make_cl_pool(&env, 0x65, token_a(), 144_196_000_000, token_b(), 0,
                4_000_000_000_000, 1095, 1000, 1140, 1000, 30, 10000),
        ] {
            pool_map.insert(p.pool_datum.identifier.clone(), p);
        }

        let order = make_order(token_b(), 400_000_000_000, token_a(), 1, 1);

        // Exercise the PRODUCTION routing entrypoint (find_blended_route), the
        // same one scooper.rs dispatches swaps through — not find_optimal_route
        // directly. A single-branch blend (branches=1) is exactly what the live
        // quarantine produced.
        // Exercise the PRODUCTION routing entrypoint (find_blended_route). With
        // the value-preservation cap, the dormant bimodal pool (65fe0a9e, range
        // [1.199,1.30], spa>1) is excluded — a B-input swap into it is
        // value-losing (its fee_budget goes negative, which the pool contract
        // rejects). So the order must route cleanly through the healthy pools and
        // ACCUMULATE OK — no over-drain, no fee_budget<0 leg, no quarantine.
        let blend = router::find_blended_route(
            &pool_map, &[], &token_b(), &token_a(), &order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        )
        .expect("healthy pools can route the order");
        let mut accum = Accumulator::new(env.exec.protocol_share);
        let result = match blend.as_single() {
            Some(single) => accum.try_add_routed_order(&order, single, &pool_map),
            None => accum.try_add_blended_order(&order, &blend, &pool_map),
        };
        assert!(
            result.is_ok(),
            "value cap should let the order fill via the healthy pools, got: {result:?}"
        );
        // And the winning route must not touch the excluded bimodal pool.
        for br in &blend.branches {
            for hop in &br.hops {
                for sp in &hop.splits {
                    assert_ne!(
                        sp.pool.ident.to_bytes().first().copied(),
                        Some(0x65),
                        "route used the value-losing bimodal pool 65fe0a9e"
                    );
                }
            }
        }

        // The order in production carries the route-constraint MODULE, which
        // forbids parallel blending: scooper.rs collapses the blended split to a
        // serial single-split chain (collapse_to_serial), routing the WHOLE 400G
        // through the deepest single pool (f46671fb). f46671fb can only absorb
        // ~363.6G, so the collapse must be REJECTED (None) — never a route that
        // sends 400G through it and over-drains. This is the exact live path.
        if let Some(single) = router::find_optimal_route(
            &pool_map, &[], &token_b(), &token_a(), &order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        ) {
            let collapsed = router::collapse_to_serial(&single, &order.swap_offered().1);
            eprintln!("=== collapse_to_serial → {}",
                collapsed.as_ref().map(|c| format!("Some(in={}, out={})", c.total_input, c.total_output))
                    .unwrap_or_else(|| "None (skipped)".into()));
            if let Some(serial) = collapsed {
                let mut accum = Accumulator::new(env.exec.protocol_share);
                if let Err(e) = accum.try_add_routed_order(&order, &serial, &pool_map) {
                    assert!(
                        !e.contains("over-drain"),
                        "collapse_to_serial produced an over-draining serial route: {e}"
                    );
                }
            }
        }
        // (No route is also acceptable — the order genuinely can't fill via one
        // capped pool. What's NOT acceptable is a route that over-drains.)
    }

    /// REGRESSION (found by `router_route_never_over_drains`): a CL pool whose
    /// declared `total_lp` exceeds the liquidity its reserves actually support
    /// is value-losing on every SMALL swap — `fee_budget < 0` until the input
    /// out-earns the deficit — so its admissible inputs are a suffix, not a
    /// prefix. A single max-dx cap can't express that, and the old 64-step
    /// linear scan took its first sample at cap/64, past the crossing, so it
    /// reported the FULL reserve cap as value-preserving. The router then put
    /// a 466-unit dust allocation straight into the value-losing zone, and the
    /// accumulator (like the validator) rejected the leg. Such a pool has to be
    /// excluded from routing outright.
    #[test]
    fn underfunded_cl_pool_is_excluded_from_routing() {
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::{router, swap_math};
        use std::collections::BTreeMap;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let (a, b, lp) = (277_354_965_297i64, 374_991_445_735i64, 3_681_143_105_273i64);
        let (spa_n, spa_d, spb_n, spb_d) = (244i64, 69702i64, 6966i64, 64161i64);
        let big = |x: i64| BigInt::from(x);
        let (fee_n, fee_d) = (big(30), big(10000));

        // `cl_fee_budget` against lp_after = 0 is the liquidity the reserves
        // support: this fixture is ~81.3e9 (2.2%) short of its own books.
        let supported = swap_math::cl_fee_budget(
            &big(a), &big(b), &big(0), &big(spa_n), &big(spa_d), &big(spb_n), &big(spb_d));
        assert!(supported < big(lp), "fixture must be underfunded: {supported} vs {lp}");

        // Neither direction may report spare capacity.
        for is_a_input in [false, true] {
            if let Some(reserve_cap) = swap_math::cl_max_dx_for_reserve(
                &big(a), &big(b), &big(lp), is_a_input,
                &big(spa_n), &big(spa_d), &big(spb_n), &big(spb_d), &fee_n, &fee_d,
            ) {
                let vp = swap_math::cl_max_dx_value_preserving(
                    &big(a), &big(b), &big(lp), &reserve_cap, is_a_input,
                    &big(spa_n), &big(spa_d), &big(spb_n), &big(spb_d), &fee_n, &fee_d,
                );
                assert_eq!(
                    vp, big(0),
                    "underfunded pool must be excluded, got cap {vp} (is_a_input={is_a_input})"
                );
            }
        }

        // End to end: the router routes around it and the route accumulates.
        let cl = make_cl_pool(&env, 0x10, token_a(), a, token_b(), b, lp,
                              spa_n, spa_d, spb_n, spb_d, 30, 10000);
        let cp1 = make_pool(&env, 0x11, token_a(), 1_620_199_307_449, token_b(), 1_624_906_635_884);
        let cp2 = make_pool(&env, 0x12, token_a(), 898_826_200_404, token_b(), 450_988_088_115);
        let mut pool_map = BTreeMap::new();
        for p in [cl, cp1, cp2] {
            pool_map.insert(p.pool_datum.identifier.clone(), p);
        }
        let (offer, ask) = (token_b(), token_a());
        let order = make_order(offer.clone(), 1_158_376_366_982, ask.clone(), 1, 1);
        let blend = router::find_blended_route(
            &pool_map, &[], &offer, &ask, &order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        )
        .expect("the two healthy CP pools can fill this");
        let mut accum = Accumulator::new(env.exec.protocol_share);
        let result = match blend.as_single() {
            Some(single) => accum.try_add_routed_order(&order, single, &pool_map),
            None => accum.try_add_blended_order(&order, &blend, &pool_map),
        };
        assert!(result.is_ok(), "route must be fillable, got {:?}", result.err());
    }

    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// INTEGRATION INVARIANT: every route the router produces must be
        /// fillable — accumulating it must never over-drain a pool. This is the
        /// live quarantine class (the router handed the accumulator a leg — 400k
        /// into a tight CL pool — whose real dy blew past the pool's reserve). A
        /// failure here is a router↔accumulator disagreement, not a bad order.
        ///
        /// Pools are generated so they could actually exist on chain (see the
        /// CL branch below) — a failure that needs an impossible pool state is
        /// noise, not signal. Malformed pools get deterministic coverage in
        /// `underfunded_cl_pool_is_excluded_from_routing` instead, where the
        /// expectation is "excluded from routing", not "the route must fill".
        #[test]
        fn router_route_never_over_drains(
            specs in prop::collection::vec(
                (0u8..3u8,                            // 0=CP 1=CS 2=CL
                 1i64..2_000_000_000_000i64,          // reserve a (production scale)
                 1i64..2_000_000_000_000i64,          // reserve b
                 // CL total_lp as ‰ of the liquidity the reserves support.
                 // Weighted toward 1000‰ (zero accrued surplus) because that's
                 // both the state a pool is left in by a deposit/withdraw and
                 // the one with no fee cushion to mask a value-losing swap.
                 prop_oneof![3 => Just(1000i64), 7 => 1i64..=1000i64],
                 1i64..100_000i64, 1i64..100_000i64,  // sqrt_price_a num/den (incl. tight ranges)
                 1i64..100_000i64, 1i64..100_000i64), // sqrt_price_b num/den
                2..5usize,
            ),
            a_to_b in any::<bool>(),
            amount in 1i64..8_000_000_000_000i64, // spans oversized inputs that drain pools
        ) {
            use std::collections::BTreeMap;
            use crate::sundaev4::accumulator::Accumulator;
            use crate::sundaev4::types::Rational;
            use crate::sundaev4::{router, swap_math};
            use num_traits::ToPrimitive;

            let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
            let mut pool_map = BTreeMap::new();
            for (i, (curve, ra, rb, lp_permille, san, sad, sbn, sbd)) in specs.iter().enumerate() {
                let id = (0x10 + i) as u8;
                let pool = match *curve {
                    0 => make_pool(&env, id, token_a(), *ra, token_b(), *rb),
                    1 => make_cs_pool(
                        &env, id,
                        vec![(token_a(), *ra), (token_b(), *rb)],
                        vec![BigInt::from(1), BigInt::from(1)],
                        Rational { num: BigInt::from(30), den: BigInt::from(10000) },
                    ),
                    _ => {
                        // Order sqrt bounds so spa < spb; skip degenerate equal.
                        let (san, sad, sbn, sbd) = if san * sbd < sbn * sad {
                            (*san, *sad, *sbn, *sbd)
                        } else {
                            (*sbn, *sbd, *san, *sad)
                        };
                        if san * sbd == sbn * sad { continue; }
                        // A CL pool's total_lp is NOT free of its reserves. Every
                        // validator path pins it: a swap to
                        // `lp_after + fee_budget == L(reserves)` and a
                        // deposit/withdraw to `fee_budget == 0` plus a virtual
                        // invariant that rounds toward the pool — and fee_budget
                        // is never negative. So on chain `total_lp <= L`, the gap
                        // being fee income accrued since it was last claimed.
                        // Draw total_lp as a fraction of the supported liquidity
                        // so every generated pool is one that could exist: 1000‰
                        // is a pool right after a value-neutral op, lower values
                        // carry more accrued fees. (Drawing it independently, as
                        // this once did, produced pools already in DEFICIT at
                        // rest — `L < total_lp`, unreachable through any contract
                        // path — and the router "failures" that fell out of it
                        // were artifacts of that impossible state. The malformed
                        // case still has deterministic coverage in
                        // `underfunded_cl_pool_is_excluded_from_routing`.)
                        let supported = swap_math::cl_fee_budget(
                            &BigInt::from(*ra), &BigInt::from(*rb), &BigInt::from(0),
                            &BigInt::from(san), &BigInt::from(sad),
                            &BigInt::from(sbn), &BigInt::from(sbd),
                        );
                        // Tight ranges make L enormous (L ≫ reserves); clamping
                        // to i64 only lowers total_lp, which stays reachable.
                        let want = &(&supported * &BigInt::from(*lp_permille as u64))
                            / &BigInt::from(1000u64);
                        let ceiling = BigInt::from(i64::MAX);
                        let lp = if want > ceiling { ceiling } else { want }
                            .unwrap()
                            .to_i64()
                            .unwrap_or(i64::MAX)
                            .max(1);
                        let pool = make_cl_pool(&env, id, token_a(), *ra, token_b(), *rb, lp,
                                                san, sad, sbn, sbd, 30, 10000);
                        prop_assert!(
                            !swap_math::cl_fee_budget(
                                &BigInt::from(*ra), &BigInt::from(*rb), &BigInt::from(lp),
                                &BigInt::from(san), &BigInt::from(sad),
                                &BigInt::from(sbn), &BigInt::from(sbd),
                            ).is_negative(),
                            "generator built an unreachable pool (in deficit at rest): \
                             a={ra} b={rb} lp={lp} range={san}/{sad}..{sbn}/{sbd}"
                        );
                        pool
                    }
                };
                pool_map.insert(pool.pool_datum.identifier.clone(), pool);
            }

            if !pool_map.is_empty() {
                let (offer, ask) = if a_to_b { (token_a(), token_b()) } else { (token_b(), token_a()) };
                let order = make_order(offer.clone(), amount, ask.clone(), 1, 1);
                if let Some(blend) = router::find_blended_route(
                    &pool_map, &[], &offer, &ask, &order.swap_offered().1,
                    router::RoutingLimits::unlimited(),
                ) {
                    let mut accum = Accumulator::new(env.exec.protocol_share);
                    let result = match blend.as_single() {
                        Some(single) => accum.try_add_routed_order(&order, single, &pool_map),
                        None => accum.try_add_blended_order(&order, &blend, &pool_map),
                    };
                    if let Err(e) = result {
                        // Two router↔contract disagreements a route must never
                        // contain: an over-draining leg (dy > reserve) and a
                        // value-losing leg (fee_budget < 0, which the pool
                        // contract's check_lp_accounting rejects). The random
                        // sqrt bounds include range-above-1.0 (spa>1) CL pools,
                        // so this exercises the value-preservation cap.
                        prop_assert!(
                            !e.contains("over-drain") && !e.contains("lose value"),
                            "router produced an unfillable route: {} \
                             (amount={amount}, a_to_b={a_to_b}, n_pools={})",
                            e, pool_map.len()
                        );
                    }
                }
            }
        }
    }

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
    fn cs_floor_fill_unaligned_evaluates() {
        // The real shape the scooper refused on preview (order 4d0d9a9f…#0):
        // prices 4:5, fee 3/1000, dx = 100_300_903. The fill numerator leaves
        // remainder 2 mod 5, so no zero-remainder dy exists; the floor fill
        // pays 80_000_000 and the 2-value-unit crumb stays in the pool, which
        // the validator's one-out-unit fee window must accept. This evaluates
        // the actual scripts — the proof the window semantics are real.
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let fee = crate::sundaev4::types::Rational {
            num: BigInt::from(3),
            den: BigInt::from(1000),
        };
        let pool = make_cs_pool(
            &env, 0xCD,
            vec![(token_a(), 1_000_000_000), (token_b(), 1_000_000_000)],
            vec![BigInt::from(4), BigInt::from(5)],
            fee.clone(),
        );
        let orders = vec![make_order(token_a(), 100_300_903, token_b(), 80_000_000, 1)];
        let batch = assemble_batch(&pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default())
            .expect("unaligned CS batch assembly should succeed");

        assert_eq!(batch.swaps.len(), 1, "the unaligned order must be admitted");
        assert_eq!(batch.swaps[0].dy, BigInt::from(80_000_000));

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (_result, eval) = env.build_and_eval(&[batch], &settings, 1000)
            .expect("floor fill with a crumb should evaluate on-chain");
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
            &pool_map,
            &[], &token_e(), &token_b(), &order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        ).expect("router should find E→A→B path");
        assert_eq!(route.hops.len(), 2, "should be a 2-hop route");
        assert!(router::is_routed(&route));

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum.try_add_routed_order(&order, &route, &pool_map)
            .expect("routed order should execute");

        let plan = accum.into_plan();
        assert_eq!(plan.batches.len(), 2, "routed order touches 2 pools");

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env.build_and_eval_plan(&plan, &settings, 1000)
            .expect("routed E→A→B build_and_eval should succeed");

        assert!(!eval.budgets.is_empty());
        assert_eq!(result.predicted_pools.len(), 2);
    }

    /// Target-pinned CS deposit (cs_check tag 6) on COPRIME reserves — the
    /// exact case the old gcd-exact resolver could never fill (gcd = 1 makes
    /// "one proportional unit" the whole pool). The built scoop must
    /// evaluate cleanly against the real validators, proving the ceil-pinned
    /// deltas, floor-pinned LP, and the declared t in operation_data all
    /// match cs_check's brackets.
    #[test]
    fn cs_deposit_pinned_on_coprime_reserves_evaluates() {
        use crate::sundaev4::accumulator::Accumulator;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let cs_fee = crate::sundaev4::types::Rational {
            num: BigInt::from(3),
            den: BigInt::from(1000),
        };
        // Coprime reserves (both prime): gcd = 1.
        let pool = make_cs_pool(
            &env, 0xDD,
            vec![(token_a(), 1_000_000_007), (token_e(), 1_999_999_943)],
            vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
            cs_fee,
        );

        let lp_asset = crate::cardano_types::AssetClass {
            policy: env.exec.module_scripts.pool_mint.hash.to_vec(),
            token: {
                let mut t = vec![0x00, 0x14, 0xdf, 0x10];
                t.extend_from_slice(&[0xDD; 28]);
                t
            },
        };
        // Roughly proportional but deliberately unround amounts.
        let order = make_basic_deposit_order(
            vec![(token_a(), 10_000_019), (token_e(), 20_000_033)],
            lp_asset,
            1,
            1,
        );

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum
            .try_add_deposit(&order, &pool.pool_datum.identifier.clone(), &pool)
            .expect("pinned deposit should resolve on coprime reserves");

        let plan = accum.into_plan();
        assert_eq!(plan.batches.len(), 1);
        let dep = &plan.batches[0].deposits[0];
        assert!(dep.lp_minted.is_positive(), "deposit must mint LP");
        assert!(dep.target_delta_v.is_some(), "CS deposit must declare t");

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env
            .build_and_eval_plan(&plan, &settings, 1000)
            .expect("pinned CS deposit should evaluate against real validators");
        assert!(!eval.budgets.is_empty());
        assert_eq!(result.predicted_pools.len(), 1);
    }

    /// Target-pinned CS withdraw (cs_check tag 4) on the same coprime pool.
    #[test]
    fn cs_withdraw_pinned_evaluates() {
        use crate::sundaev4::accumulator::Accumulator;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let cs_fee = crate::sundaev4::types::Rational {
            num: BigInt::from(3),
            den: BigInt::from(1000),
        };
        let pool = make_cs_pool(
            &env, 0xDD,
            vec![(token_a(), 1_000_000_007), (token_e(), 1_999_999_943)],
            vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
            cs_fee,
        );
        // The harness pool premints all LP (circulating 0); a withdraw burns
        // circulating LP, so move some out of the premint — as if a depositor
        // holds it — keeping the datum and the pool value consistent.
        let pool = {
            let mut p = (*pool).clone();
            let circ = BigInt::from(50_000_000i64);
            p.pool_datum.circulating_lp = circ.clone();
            p.pool_datum.preminted_lp = &p.pool_datum.preminted_lp - &circ;
            let lp = crate::cardano_types::AssetClass {
                policy: env.exec.module_scripts.pool_mint.hash.to_vec(),
                token: {
                    let mut t = vec![0x00, 0x14, 0xdf, 0x10];
                    t.extend_from_slice(&[0xDD; 28]);
                    t
                },
            };
            let held = p.value.get(&lp);
            p.value.insert(&lp, &held - &circ);
            std::sync::Arc::new(p)
        };

        let lp_asset = crate::cardano_types::AssetClass {
            policy: env.exec.module_scripts.pool_mint.hash.to_vec(),
            token: {
                let mut t = vec![0x00, 0x14, 0xdf, 0x10];
                t.extend_from_slice(&[0xDD; 28]);
                t
            },
        };
        let order = make_basic_withdraw_order(
            lp_asset,
            5_000_017,
            vec![(token_a(), 1), (token_e(), 1)],
            1,
        );

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum
            .try_add_withdraw(&order, &pool.pool_datum.identifier.clone(), &pool)
            .expect("pinned withdraw should resolve");

        let plan = accum.into_plan();
        let wd = &plan.batches[0].withdraws[0];
        assert!(wd.lp_burned.is_positive());
        assert!(wd.target_delta_v.is_some(), "CS withdraw must declare t");
        assert!(wd.dy.iter().any(|q| q.is_positive()));

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env
            .build_and_eval_plan(&plan, &settings, 1000)
            .expect("pinned CS withdraw should evaluate against real validators");
        assert!(!eval.budgets.is_empty());
        assert_eq!(result.predicted_pools.len(), 1);
    }

    /// A single-sided CS deposit can never validate (cs_check disallows
    /// asymmetric deposits) — the resolver must refuse it with a message
    /// that says so, rather than quarantine-looping.
    #[test]
    fn cs_single_sided_deposit_rejected() {
        use crate::sundaev4::accumulator::Accumulator;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let cs_fee = crate::sundaev4::types::Rational {
            num: BigInt::from(3),
            den: BigInt::from(1000),
        };
        let pool = make_cs_pool(
            &env, 0xDD,
            vec![(token_a(), 1_000_000_007), (token_e(), 1_999_999_943)],
            vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
            cs_fee,
        );
        let lp_asset = crate::cardano_types::AssetClass {
            policy: env.exec.module_scripts.pool_mint.hash.to_vec(),
            token: {
                let mut t = vec![0x00, 0x14, 0xdf, 0x10];
                t.extend_from_slice(&[0xDD; 28]);
                t
            },
        };
        let order = make_basic_deposit_order(
            vec![(token_a(), 10_000_000)],
            lp_asset,
            1,
            1,
        );

        let mut accum = Accumulator::new(env.exec.protocol_share);
        let err = accum
            .try_add_deposit(&order, &pool.pool_datum.identifier.clone(), &pool)
            .expect_err("single-sided CS deposit must be rejected");
        assert!(
            err.contains("asymmetric"),
            "error should explain the on-chain rule, got: {err}"
        );
    }

    /// A route-module order whose constraint carries a non-empty pool
    /// whitelist must execute through a whitelisted pool even when a
    /// better-priced pool exists, and the resulting tx must satisfy
    /// route.ak's check_pool_whitelisted. Mirrors dispatch in scooper.rs,
    /// which filters the router's pool view by the parsed whitelist —
    /// preview orders b42626…/f3080db6… quarantine-looped because the
    /// router ignored the whitelist and picked the deeper pool.
    #[test]
    fn route_whitelist_restricts_routing() {
        use std::collections::BTreeMap;
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::router;
        use crate::sundaev4::parse_route_whitelist;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);

        // Two CP pools on the same pair: 0xAA is deep (better price for the
        // taker), 0xBB is thin.
        let deep = make_pool(&env, 0xAA, token_a(), 2_000_000_000, token_b(), 2_000_000_000);
        let thin = make_pool(&env, 0xBB, token_a(), 500_000_000, token_b(), 500_000_000);
        let mut pool_map = BTreeMap::new();
        pool_map.insert(deep.pool_datum.identifier.clone(), deep.clone());
        pool_map.insert(thin.pool_datum.identifier.clone(), thin.clone());

        let order = make_order(token_a(), 10_000_000, token_b(), 1, 1);
        let order = with_route_whitelist(order, &[thin.pool_datum.identifier.clone()]);

        // Sanity: unrestricted, the router prefers the deep pool — the
        // exact trap the preview orders fell into.
        let free = router::find_optimal_route(
            &pool_map,
            &[], &token_a(), &token_b(), &order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        ).expect("unrestricted route exists");
        assert_eq!(
            free.hops[0].splits[0].pool.ident,
            deep.pool_datum.identifier,
            "unrestricted router should pick the deep pool",
        );

        // Dispatch-equivalent: parse the whitelist off the order and filter
        // the pool view before routing.
        let route_hash = env.exec.module_scripts.route_order.as_ref().unwrap().hash.as_ref().to_vec();
        let wl = parse_route_whitelist(
            order.datum.find_constraint_by_hash(&route_hash).expect("order carries route module"),
        ).expect("whitelist parses");
        assert_eq!(wl, vec![thin.pool_datum.identifier.clone()]);
        let filtered: BTreeMap<_, _> = pool_map
            .iter()
            .filter(|(ident, _)| wl.contains(ident))
            .map(|(i, p)| (i.clone(), p.clone()))
            .collect();

        let route = router::find_optimal_route(
            &filtered,
            &[], &token_a(), &token_b(), &order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        ).expect("whitelisted route exists");
        assert_eq!(route.hops.len(), 1);
        assert_eq!(
            route.hops[0].splits[0].pool.ident,
            thin.pool_datum.identifier,
            "whitelist should force the thin pool",
        );

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum.try_add_routed_order(&order, &route, &filtered)
            .expect("whitelisted order should execute");
        let plan = accum.into_plan();

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (_result, eval) = env.build_and_eval_plan(&plan, &settings, 1000)
            .expect("whitelisted route must satisfy route.ak's check_pool_whitelisted");
        assert!(!eval.budgets.is_empty());
    }

    /// A swap expressed via the BASIC constraint module routes — and
    /// BLENDS across two disjoint paths — through the real validators.
    /// Pools: direct A/B (1B/1B) plus A/E and E/B (1B each), so the blend
    /// splits between the direct pool and the 2-hop path. The basic module
    /// checks only aggregate consumption and floors, so the branched
    /// execution must evaluate cleanly with no route module involved.
    #[test]
    fn basic_swap_blends_across_paths() {
        use std::collections::BTreeMap;
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::router;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let direct = make_pool(&env, 0xA1, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        let leg1 = make_pool(&env, 0xA2, token_a(), 1_000_000_000, token_e(), 1_000_000_000);
        let leg2 = make_pool(&env, 0xA3, token_e(), 1_000_000_000, token_b(), 1_000_000_000);

        let mut pool_map = BTreeMap::new();
        for p in [&direct, &leg1, &leg2] {
            pool_map.insert(p.pool_datum.identifier.clone(), (*p).clone());
        }

        let order = make_basic_swap_order(token_a(), 100_000_000, token_b(), 1, 1);
        assert!(
            matches!(order.constraint, crate::sundaev4::Constraint::Swap { .. }),
            "basic tag-2 order must present as a swap to the dispatcher",
        );

        let blend = router::find_blended_route(
            &pool_map,
            &[],
            &token_a(),
            &token_b(),
            &order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        )
        .expect("blend must exist");
        assert_eq!(blend.branches.len(), 2, "direct + 2-hop path should both carry flow");

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum
            .try_add_blended_order(&order, &blend, &pool_map)
            .expect("blended basic swap should accumulate");

        let plan = accum.into_plan();
        assert_eq!(plan.batches.len(), 3, "blend touches all three pools");

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env
            .build_and_eval_plan(&plan, &settings, 1000)
            .expect("blended basic swap build_and_eval should succeed");
        assert!(!eval.budgets.is_empty());
        assert_eq!(result.predicted_pools.len(), 3);
    }

    /// THE Butane integration proof: a basic order swapping ADA blends
    /// across a direct ADA/TOKEN pool and an ADAb/TOKEN pool reached
    /// through the real ADA→ADAb mint edge. The built tx contains the
    /// composed deposit (pot output, synthetic+treas mint, the four
    /// zero-withdrawals, params/registry refs) and every script — Sundae's
    /// V3 set plus Butane's mixed V2/V3 set — must evaluate. Skips when
    /// the deployment artifact isn't available.
    #[test]
    fn basic_swap_blends_through_butane_mint() {
        use std::collections::BTreeMap;
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::router;

        let mut env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        if !env.enable_butane() {
            eprintln!("skipping: butane artifact/config unavailable");
            return;
        }
        let rt = env.butane.as_ref().unwrap();
        let edges = rt.edges();
        let adab = rt.synthetic_asset("ADAb");

        // Shallow direct pool, deep ADAb pool: the blend sends real flow
        // through the mint.
        let direct = make_pool(&env, 0xB1, ada(), 500_000_000, token_b(), 500_000_000);
        let via = make_pool(&env, 0xB2, adab.clone(), 4_000_000_000, token_b(), 4_000_000_000);
        let mut pool_map = BTreeMap::new();
        for p in [&direct, &via] {
            pool_map.insert(p.pool_datum.identifier.clone(), (*p).clone());
        }

        let order = make_basic_swap_order(ada(), 100_000_000, token_b(), 1, 1);
        let blend = router::find_blended_route(
            &pool_map,
            &edges,
            &ada(),
            &token_b(),
            &order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        )
        .expect("blend exists");
        assert_eq!(blend.branches.len(), 2, "direct + via-ADAb branches");

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum
            .try_add_blended_order(&order, &blend, &pool_map)
            .expect("blended order with butane leg accumulates");
        let plan = accum.into_plan();
        assert_eq!(plan.conversions.len(), 1, "one ADAb mint leg");
        assert!(plan.conversions[0].key.contains("ADAb"));

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (build, eval) = env
            .build_and_eval_plan(&plan, &settings, 1000)
            .expect("blended butane scoop must build and evaluate");
        assert!(!eval.budgets.is_empty());
        // 2 pools + fulfillment + pot + change → at least 4 outputs.
        assert!(build.tx_body.outputs.len() >= 4);
    }

    /// Uniform ops: a PURE conversion order — a basic order offering ADA
    /// with an ADAb floor, no pools anywhere in the route. The conversion
    /// leg is the order's primary op: the tx spends the order, composes the
    /// Butane deposit, and pays the fulfillment, with zero pool batches and
    /// no pool modules. Users minting ADAb through Sundae order flow.
    #[test]
    fn basic_order_pure_adab_mint() {
        use std::collections::BTreeMap;
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::router;

        let mut env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        if !env.enable_butane() {
            eprintln!("skipping: butane artifact/config unavailable");
            return;
        }
        let rt = env.butane.as_ref().unwrap();
        let edges = rt.edges();
        let adab = rt.synthetic_asset("ADAb");

        let pool_map: BTreeMap<_, _> = BTreeMap::new();
        let order = make_basic_swap_order(ada(), 50_000_000, adab.clone(), 50_000_000, 1);

        let blend = router::find_blended_route(
            &pool_map,
            &edges,
            &ada(),
            &adab,
            &order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        )
        .expect("pure conversion route exists");
        assert_eq!(blend.branches.len(), 1);
        assert_eq!(blend.branches[0].hops.len(), 1, "single conversion hop");

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum
            .try_add_blended_order(&order, &blend, &pool_map)
            .expect("pure-conversion order accumulates");
        let plan = accum.into_plan();
        assert_eq!(plan.batches.len(), 0, "no pool batches");
        assert_eq!(plan.conversions.len(), 1);
        assert!(plan.conversions[0].primary, "the conversion owns the order");

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (build, eval) = env
            .build_and_eval_plan(&plan, &settings, 1000)
            .expect("pure mint order must build and evaluate");
        assert!(!eval.budgets.is_empty());
        // fulfillment (ADAb to the user) + pot + change.
        assert!(build.tx_body.outputs.len() >= 3);
    }

    /// Partial fill through the real validators: an order too large for
    /// the pool fills partially — the output is a CONTINUATION at the order
    /// address (same datum, remaining_offered decremented, received tokens
    /// accumulated on it), fee capped pro-rata, min checked by the
    /// contract's exact cross-multiplication.
    #[test]
    fn swap_partial_fill_continuation() {
        use std::collections::BTreeMap;
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::router;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = make_pool(&env, 0xC1, token_a(), 200_000_000, token_b(), 200_000_000);
        let mut pool_map = BTreeMap::new();
        pool_map.insert(pool.pool_datum.identifier.clone(), pool.clone());

        // 400M offered against a 200M pool; min 0.6/unit pro-rata. A full
        // fill averages ~0.33 — impossible; a 100M fill averages ~0.66 ✓.
        // Budget 8 ADA: a 25% fill's pro-rata fee cap (2 ADA) covers the
        // fee share — the user literally buys partial-fill granularity.
        let order = make_order_with_budget(token_a(), 400_000_000, token_b(), 240_000_000, 1, 8_000_000);
        let fill = BigInt::from(100_000_000u64);

        let route = router::find_optimal_route(
            &pool_map, &[], &token_a(), &token_b(), &fill,
            router::RoutingLimits::unlimited(),
        )
        .expect("partial route exists");

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum
            .try_add_routed_order(&order, &route, &pool_map)
            .expect("partial fill should pass the pro-rata min check");
        let plan = accum.into_plan();

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (build, eval) = env
            .build_and_eval_plan(&plan, &settings, 1000)
            .expect("partial fill must build and evaluate");
        assert!(!eval.budgets.is_empty());

        // The continuation output sits at the order address, carries the
        // leftover offer + the received tokens, and its datum decrements
        // remaining_offered by the fill.
        let order_addr = {
            use pallas_addresses::{ShelleyAddress, ShelleyPaymentPart, ShelleyDelegationPart, Network};
            ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Script(env.exec.module_scripts.order.hash),
                ShelleyDelegationPart::Null,
            )
            .to_vec()
        };
        let cont = build
            .tx_body
            .outputs
            .iter()
            .find_map(|o| match o {
                pallas_primitives::conway::TransactionOutput::PostAlonzo(b)
                    if b.address.to_vec() == order_addr =>
                {
                    Some(b)
                }
                _ => None,
            })
            .expect("continuation output at the order address");
        let datum_cbor = match &cont.datum_option {
            Some(pallas_primitives::conway::PseudoDatumOption::Data(d)) => {
                minicbor::to_vec(&d.0).unwrap()
            }
            _ => panic!("continuation must carry an inline datum"),
        };
        let datum: pallas_primitives::PlutusData = minicbor::decode(&datum_cbor).unwrap();
        use plutus_parser::AsPlutus;
        let parsed = <crate::sundaev4::OrderDatum as AsPlutus>::from_plutus(datum).unwrap();
        let c = crate::sundaev4::Constraint::from_order_datum(
            &parsed,
            env.exec.module_scripts.swap_order.as_ref().unwrap().hash.as_ref(),
            env.exec.module_scripts.basic_order.as_ref().unwrap().hash.as_ref(),
        )
        .unwrap();
        match c {
            crate::sundaev4::Constraint::Swap { original_offered, remaining_offered, .. } => {
                assert_eq!(original_offered, BigInt::from(400_000_000u64));
                assert_eq!(remaining_offered, BigInt::from(300_000_000u64));
            }
            other => panic!("expected swap constraint, got {other:?}"),
        }
    }

    /// Audit probe: can a swap-module order RECEIVING ADA fill at all?
    /// compute_fee_taken measures in_ada − out_ada; a destination that
    /// receives dy ADA drives it negative, and the module expects ≥ 0.
    #[test]
    fn swap_order_receiving_ada_full_fill() {
        use std::collections::BTreeMap;
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::router;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = make_pool(&env, 0xC9, ada(), 1_000_000_000, token_a(), 1_000_000_000);
        let mut pool_map = BTreeMap::new();
        pool_map.insert(pool.pool_datum.identifier.clone(), pool.clone());

        // Sell 10M tOKENA for ADA, easily satisfiable min.
        let order = make_order(token_a(), 10_000_000, ada(), 1_000_000, 1);
        let route = router::find_optimal_route(
            &pool_map, &[], &token_a(), &ada(), &order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        )
        .expect("route exists");
        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum.try_add_routed_order(&order, &route, &pool_map).expect("adds");
        let plan = accum.into_plan();
        let settings = make_settings(&env, &env.scooper_keyhash());
        // SUNDAE-2613 fixed the pre-launch fee_taken >= 0 bug: min_received
        // is measured GROSS of the fee on ADA legs, so selling a token FOR
        // ADA is a first-class fill now. This used to be the canary test
        // documenting the old contract bug.
        env.build_and_eval_plan(&plan, &settings, 1000)
            .expect("ADA-receiving full fill validates under SUNDAE-2613");
    }

    /// Single-pool basic swap: the degenerate case must also evaluate.
    #[test]
    fn basic_swap_direct() {
        use std::collections::BTreeMap;
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::router;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = make_pool(&env, 0xA4, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        let mut pool_map = BTreeMap::new();
        pool_map.insert(pool.pool_datum.identifier.clone(), pool.clone());

        let order = make_basic_swap_order(token_a(), 10_000_000, token_b(), 1, 1);
        let route = router::find_optimal_route(
            &pool_map,
            &[],
            &token_a(),
            &token_b(),
            &order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        )
        .expect("direct route");

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum
            .try_add_routed_order(&order, &route, &pool_map)
            .expect("basic swap should accumulate");
        let plan = accum.into_plan();
        let settings = make_settings(&env, &env.scooper_keyhash());
        let (_result, eval) = env
            .build_and_eval_plan(&plan, &settings, 1000)
            .expect("basic swap build_and_eval should succeed");
        assert!(!eval.budgets.is_empty());
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
            &pool_map,
            &[], &token_a(), &token_f(), &order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        ).expect("router should find A→E→F path");
        assert_eq!(route.hops.len(), 2, "should be a 2-hop route");

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum.try_add_routed_order(&order, &route, &pool_map)
            .expect("routed order should execute");

        let plan = accum.into_plan();
        assert_eq!(plan.batches.len(), 2, "routed order touches 2 CS pools");

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env.build_and_eval_plan(&plan, &settings, 1000)
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
            &pool_map,
            &[], &token_e(), &token_b(), &order_eb.swap_offered().1,
            router::RoutingLimits::unlimited(),
        ).expect("router should find E→B path");
        assert_eq!(route.hops.len(), 2);

        accum.try_add_routed_order(&order_eb, &route, &pool_map)
            .expect("routed E→B order should execute");

        let plan = accum.into_plan();
        // CS A/E pool has direct + routed leg, CP pool has direct + routed leg
        // CS E/F pool is not involved (route goes E→A→B, not through E/F)
        assert_eq!(plan.batches.len(), 2, "direct + routed orders touch 2 pools");

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env.build_and_eval_plan(&plan, &settings, 1000)
            .expect("mixed direct+routed build_and_eval should succeed");

        assert!(!eval.budgets.is_empty());
        assert_eq!(result.predicted_pools.len(), 2);
    }

    // ─── Batch-rule regression tests (devnet-discovered, migration notes) ────

    /// Rule 1+2 of the multi-order batch scoop rules: each route-bearing
    /// order claims its OWN transcript step, and per-pool transcript
    /// sequencing must match the canonical (TxOutRef-sorted) order of the
    /// orders it serves — route.ak's `check_route_uniqueness` walks order
    /// inputs canonically and demands strictly increasing step claims per
    /// pool. Orders arrive here in NON-canonical order (slots 3,1,2 — the
    /// harness embeds slot in the tx hash, so canonical order == slot
    /// order); the batch pipeline must still produce a valid transcript.
    #[test]
    fn batch_route_claims_follow_canonical_order() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        // Deliberately shuffled admission order vs canonical input order.
        let orders = vec![
            make_order(token_a(), 10_000_000, token_b(), 1, 3),
            make_order(token_a(), 20_000_000, token_b(), 1, 1),
            make_order(token_a(), 5_000_000, token_b(), 1, 2),
        ];
        let batch = assemble_batch(&pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default())
            .expect("batch assembly should succeed");
        assert_eq!(batch.swaps.len(), 3);

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (_result, eval) = env.build_and_eval(&[batch], &settings, 1000)
            .expect("shuffled-admission batch must satisfy check_route_uniqueness");
        assert!(!eval.budgets.is_empty());
    }

    /// The accumulator's canonical-append guard: a same-pool admission whose
    /// order sorts canonically before an order already in the batch must be
    /// rejected (it goes in the next tx) — dispatch sorts candidates
    /// canonically, but the confirmed/provisional boundary can still present
    /// them out of order. In canonical order both fit and the tx validates.
    #[test]
    fn accumulator_rejects_noncanonical_same_pool_admission() {
        use crate::sundaev4::accumulator::Accumulator;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        let ident = pool.pool_datum.identifier.clone();

        let early = make_order(token_a(), 20_000_000, token_b(), 1, 1);
        let late = make_order(token_a(), 10_000_000, token_b(), 1, 2);

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum.try_add_order(&late, &ident, &pool).expect("first admission succeeds");
        let err = accum.try_add_order(&early, &ident, &pool)
            .expect_err("out-of-canonical-order same-pool admission must be rejected");
        assert!(err.contains("canonical-order violation"), "unexpected error: {err}");

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum.try_add_order(&early, &ident, &pool).expect("canonical first");
        accum.try_add_order(&late, &ident, &pool).expect("canonical second");
        let plan = accum.into_plan();
        let settings = make_settings(&env, &env.scooper_keyhash());
        let (_result, eval) = env.build_and_eval_plan(&plan, &settings, 1000)
            .expect("canonical admission builds a valid tx");
        assert!(!eval.budgets.is_empty());
    }

    /// Rule 1: each route-bearing order claims its OWN transcript step. Two
    /// routed orders sharing BOTH pools of an E→A→B chain must claim steps
    /// 0 and 1 on each pool — "everyone points at step 0" fails as soon as
    /// a batch has two orders on one pool.
    #[test]
    fn batch_two_routed_orders_share_pools() {
        use std::collections::BTreeMap;
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::router;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let cs_fee = crate::sundaev4::types::Rational {
            num: BigInt::from(3),
            den: BigInt::from(1000),
        };
        let cp_pool = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        let cs_pool = make_cs_pool(
            &env, 0xDD,
            vec![(token_a(), 1_000_000_000), (token_e(), 1_000_000_000)],
            vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
            cs_fee,
        );
        let mut pool_map = BTreeMap::new();
        pool_map.insert(cp_pool.pool_datum.identifier.clone(), cp_pool.clone());
        pool_map.insert(cs_pool.pool_datum.identifier.clone(), cs_pool.clone());

        // No direct E/B pool: both orders must route E→A (CS) →B (CP).
        // Canonical order == slot order (the harness embeds slot in the tx
        // hash); add them canonically, as dispatch does.
        let orders = vec![
            make_order(token_e(), 10_000_000, token_b(), 1, 1),
            make_order(token_e(), 20_000_000, token_b(), 1, 2),
        ];

        let mut accum = Accumulator::new(env.exec.protocol_share);
        for order in &orders {
            let view = accum.current_pool_view(&pool_map);
            let route = router::find_optimal_route(
                &view,
                &[], &token_e(), &token_b(), &order.swap_offered().1,
                router::RoutingLimits::unlimited(),
            ).expect("router should find E→A→B path");
            assert_eq!(route.hops.len(), 2, "should be a 2-hop route");
            accum.try_add_routed_order(order, &route, &view)
                .expect("routed order should execute");
        }

        let plan = accum.into_plan();
        assert_eq!(plan.batches.len(), 2, "both orders share the same 2 pools");

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env.build_and_eval_plan(&plan, &settings, 1000)
            .expect("two routed orders on shared pools must claim distinct, increasing steps");
        assert!(!eval.budgets.is_empty());
        assert_eq!(result.predicted_pools.len(), 2);
    }

    /// Rule 3: per-step protocol-LP capture. fee_split.Operate requires
    /// every transcript entry's fee_budget >= 0 AND aggregate LP gap growth
    /// == floor(total_gross_fee × protocol_share) — so the protocol share
    /// must be captured per step via the cumulative-floor trick. (Deducting
    /// the whole share from the last entry goes negative once a batch has
    /// ≥3 similar swaps.) Reconstructs gross fees from the built transcript
    /// and asserts both bounds offline, on top of the on-chain evaluation.
    #[test]
    fn batch_protocol_capture_per_step() {
        use plutus_parser::AsPlutus;
        use crate::sundaev4::types::PoolRedeemer;

        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        let orders: Vec<_> = (1..=5u64)
            .map(|i| make_order(token_a(), 10_000_000, token_b(), 1, i))
            .collect();
        let batch = assemble_batch(&pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default())
            .expect("batch assembly should succeed");
        assert_eq!(batch.swaps.len(), 5);

        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, eval) = env.build_and_eval(&[batch], &settings, 1000)
            .expect("5-similar-swap batch must satisfy fee_split's per-entry budget bound");
        assert!(!eval.budgets.is_empty());

        let transcript = result.redeemers.iter()
            .find_map(|(_, pd, _)| match PoolRedeemer::from_plutus(pd.clone()) {
                Ok(PoolRedeemer::Action { transcript, .. }) => Some(transcript),
                _ => None,
            })
            .expect("tx carries a PoolRedeemer::Action");
        assert_eq!(transcript.len(), 5, "one transcript entry per swap");

        let (ps_num, ps_den) = (
            BigInt::from(env.exec.protocol_share.0),
            BigInt::from(env.exec.protocol_share.1),
        );
        let mut prev_lp = pool.pool_datum.total_lp.clone();
        let mut total_gross = BigInt::from(0);
        let mut total_growth = BigInt::from(0);
        for (i, entry) in transcript.iter().enumerate() {
            assert!(
                entry.fee_budget >= BigInt::from(0),
                "entry {i} fee_budget went negative: {} — protocol share must be captured per step",
                entry.fee_budget,
            );
            let growth = &entry.state_after.total_lp - &prev_lp;
            assert!(growth >= BigInt::from(0), "entry {i} shrank total_lp");
            total_gross = &total_gross + &entry.fee_budget + &growth;
            total_growth = &total_growth + &growth;
            prev_lp = entry.state_after.total_lp.clone();
        }
        assert!(total_gross > BigInt::from(0), "similar swaps should accrue fees");
        assert_eq!(
            total_growth,
            &total_gross * &ps_num / &ps_den,
            "cumulative capture must telescope to floor(total_gross_fee × protocol_share)",
        );
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


