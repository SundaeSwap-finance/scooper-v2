//! What a scoop costs, measured — the evidence behind
//! `cost-per-pool-lovelace` and `cost-per-step-lovelace`.
//!
//! `RoutingLimits::from_budget` divides an order's `max_per_execution` by
//! those two constants to cap how many pools and how many route steps the
//! order may buy. Set them too high and a legitimate multi-hop order is
//! never routed (preview, 2026-09: a 1 ADA budget against a 1 ADA per-pool
//! cost gave `max_pools = 1`, so a two-hop route was never considered).
//! Set them too low and the scooper executes orders whose fee does not
//! cover the block space they consume.
//!
//! This module measures the real quantities with the production transaction
//! builder and the real validator bytecode, through the same two passes the
//! scooper runs before it submits (`scooper.rs`):
//!
//!   1. build with default budgets and the `TX_FEE` placeholder;
//!   2. evaluate, pad every budget by `budget_padding` (5%);
//!   3. `compute_tx_fee(first-pass size, padded units, ref-script bytes)`;
//!   4. rebuild with the padded budgets and that fee.
//!
//! Three quantities are separated, because they are not the same thing:
//!
//! | quantity | held fixed | scenario |
//! | --- | --- | --- |
//! | one more order on a pool already in the tx | pools | `orders_on_one_pool` |
//! | one more pool holding the order count fixed | orders | `one_order_each` − `orders_on_one_pool` |
//! | one more hop for one order (a pool AND a step) | orders | routed scan |
//!
//! **This benchmark runs on untraced bytecode**
//! (`test/fixtures/devnet-blueprint-untraced.json`), not on the traced
//! fixture the eval tests use. The difference is not cosmetic: the traced
//! build costs about 4.5x the CPU and 2.7x the bytecode of the untraced
//! scripts a real deployment publishes, which would put the fee estimate out
//! by a factor. Measured on the traced fixture, one constant-product order
//! against one pool reports 5.56 G steps; the deployed preview scooper's own
//! submission logs show 1.22 G for the same shape, and the untraced fixture
//! reproduces that.
//!
//! The untraced fixture holds the same validator set with each script's
//! parameter hashes recomputed in dependency order (settingsMint -> poolMint
//! / order -> pool), so it is internally consistent without matching any
//! deployment. Regenerate it from a sundae-v4 checkout with
//! `bun run` over that repo's `blueprint.ts`, untraced, keeping the
//! reference `txIn`s of the traced fixture — see `docs/routing-costs.md`.
//!
//! Each row still reports `fee_no_ref` (the fee with the reference-script
//! component removed) beside `fee`, and the marginals are taken on
//! `fee_no_ref`: reference-script bytes do not change when a pool of a curve
//! already in the transaction is added, so they cancel in every marginal.
//!
//! Run the table:
//!
//! ```sh
//! cargo test cost_bench -- --ignored --nocapture
//! ```
//!
//! It is `#[ignore]`d because `test_harness::TEST_CTX` is a process-global
//! `OnceLock` holding the constraint hashes of the first blueprint any test
//! loads. Sharing a process with the traced-fixture tests would give one of
//! the two the other's hashes, so this test runs alone, under its own filter.

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use pallas_primitives::ExUnits;
    use pallas_primitives::conway::RedeemersKey;

    use crate::bigint::BigInt;
    use crate::cardano_types::AssetClass;
    use crate::sundaev4::batch::{Batch, BatchLimits, ScoopPlan, assemble_batch};
    use crate::sundaev4::evaluator::evaluate_scoop_tx;
    use crate::sundaev4::submit::encode_language_views;
    use crate::sundaev4::test_harness::*;
    use crate::sundaev4::tx_builder::{ValidityWindow, build_multi_pool_scoop_tx, compute_tx_fee};
    use crate::sundaev4::types::{Rational, SundaeV4Pool, SundaeV4Settings};

    const BLUEPRINT_PATH: &str = "test/fixtures/devnet-blueprint-untraced.json";
    /// The deployed Plutus V3 cost model. `test_harness::PLUTUS_V3_COST_MODEL`
    /// is a 295-entry pre-Plomin model: shorter than the 350 entries the
    /// chain runs, so every builtin past the truncation is charged with the
    /// wrong coefficient. It leaves memory about right and overstates CPU by
    /// nearly 4x, which is most of a scoop's fee. Read the real one instead.
    const COST_MODEL_CONFIG: &str = "config/preview-v4.json";
    const SLOT: u64 = 1000;
    /// Every order in the scan offers this much of the pool's first asset.
    const ORDER_SIZE: i64 = 10_000_000;
    /// Pool reserves. Deep enough that 30 orders of `ORDER_SIZE` move the
    /// price very little, so cost is measured on comparable trades.
    const RESERVE: i64 = 10_000_000_000;

    // ─── The three curves under test ──────────────────────────────────────

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Curve {
        Cp,
        Cs,
        Ss,
    }

    impl Curve {
        fn label(self) -> &'static str {
            match self {
                Curve::Cp => "cp",
                Curve::Cs => "cs",
                Curve::Ss => "ss",
            }
        }
    }

    fn fee_3_per_mille() -> Rational {
        Rational {
            num: BigInt::from(3),
            den: BigInt::from(1000),
        }
    }

    /// Pool `i` of a scan: its own asset pair, so each order routes to
    /// exactly one pool and nothing is shared between them.
    fn pair(i: usize) -> (AssetClass, AssetClass) {
        (
            token(0x80 + (2 * i) as u8, 0x01),
            token(0x80 + (2 * i + 1) as u8, 0x01),
        )
    }

    fn make_curve_pool(env: &TestEnv, curve: Curve, i: usize) -> Arc<SundaeV4Pool> {
        let ident = 0x40 + i as u8;
        let (ta, tb) = pair(i);
        match curve {
            Curve::Cp => make_pool(env, ident, ta, RESERVE, tb, RESERVE),
            Curve::Cs => make_cs_pool(
                env,
                ident,
                vec![(ta, RESERVE), (tb, RESERVE)],
                vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
                fee_3_per_mille(),
            ),
            Curve::Ss => make_ss_pool(
                env,
                ident,
                vec![(ta, RESERVE), (tb, RESERVE)],
                ss_config(200, fee_3_per_mille(), [1, 1]),
            ),
        }
    }

    /// `n_orders` orders against pool `i`, alternating direction so the pool
    /// does not drift far from its start price.
    fn orders_for(i: usize, n_orders: usize) -> Vec<Arc<crate::sundaev4::types::SundaeV4Order>> {
        let (ta, tb) = pair(i);
        (0..n_orders)
            .map(|j| {
                let slot = (i * 100 + j + 1) as u64;
                if j % 2 == 0 {
                    make_order(ta.clone(), ORDER_SIZE, tb.clone(), 1, slot)
                } else {
                    make_order(tb.clone(), ORDER_SIZE, ta.clone(), 1, slot)
                }
            })
            .collect()
    }

    fn batch_for(env: &TestEnv, curve: Curve, i: usize, n_orders: usize) -> Batch {
        let pool = make_curve_pool(env, curve, i);
        let orders = orders_for(i, n_orders);
        assemble_batch(
            &pool,
            &orders,
            env.exec.fee,
            env.exec.protocol_share,
            &BatchLimits { max_orders: 64 },
        )
        .unwrap_or_else(|| {
            panic!(
                "{} batch with {n_orders} orders did not assemble",
                curve.label()
            )
        })
    }

    /// The Plutus V3 cost model as the deployments carry it (preview and
    /// preprod ship the same array; mainnet's v4 config does not exist yet).
    fn deployed_v3_cost_model() -> Vec<i64> {
        let raw = std::fs::read_to_string(COST_MODEL_CONFIG)
            .unwrap_or_else(|e| panic!("{COST_MODEL_CONFIG}: {e}"));
        let cfg: serde_json::Value = serde_json::from_str(&raw).expect("config parses");
        fn find(v: &serde_json::Value) -> Option<&serde_json::Value> {
            match v {
                serde_json::Value::Object(m) => {
                    m.get("plutus-v3-cost-model").or_else(|| m.values().find_map(find))
                }
                _ => None,
            }
        }
        serde_json::from_value(find(&cfg).expect("config carries a V3 cost model").clone())
            .expect("cost model is an integer array")
    }

    // ─── Measurement ──────────────────────────────────────────────────────

    #[derive(Clone, Debug)]
    struct Row {
        label: String,
        pools: usize,
        orders: usize,
        /// Transcript entries across every pool — what the router counts as
        /// route "steps".
        steps: usize,
        /// Padded execution units: what the transaction declares and the
        /// block is charged for.
        mem: u64,
        cpu: u64,
        /// First-pass size, the one the fitness check and the fee use.
        size: usize,
        /// Size of the rebuilt transaction that would be submitted.
        final_size: usize,
        ref_bytes: u64,
        /// Production fee, traced fixture reference scripts included.
        fee: u64,
        /// Fee with the reference-script component removed.
        fee_no_ref: u64,
    }

    impl Row {
        fn mem_pct(&self, env: &TestEnv) -> f64 {
            self.mem as f64 / env.exec.max_tx_ex_mem as f64 * 100.0
        }
        fn cpu_pct(&self, env: &TestEnv) -> f64 {
            self.cpu as f64 / env.exec.max_tx_ex_steps as f64 * 100.0
        }
        fn size_pct(&self, env: &TestEnv) -> f64 {
            self.size as f64 / env.exec.max_tx_size as f64 * 100.0
        }
        /// Which protocol limit this transaction is closest to.
        fn binding(&self, env: &TestEnv) -> &'static str {
            let m = self.mem_pct(env);
            let c = self.cpu_pct(env);
            let s = self.size_pct(env);
            if m >= c && m >= s {
                "mem"
            } else if c >= s {
                "cpu"
            } else {
                "size"
            }
        }
    }

    /// Run the production two-pass build/eval/rebuild and record what it
    /// cost. `None` when the plan cannot be built or evaluated at all — the
    /// capacity scans read that as "this size no longer fits".
    fn measure(
        env: &TestEnv,
        settings: &SundaeV4Settings,
        label: &str,
        batches: Vec<Batch>,
    ) -> Option<Row> {
        measure_plan(
            env,
            settings,
            label,
            ScoopPlan {
                batches,
                routes: Vec::new(),
                global_seq: Vec::new(),
                conversions: Vec::new(),
            },
        )
    }

    fn measure_plan(
        env: &TestEnv,
        settings: &SundaeV4Settings,
        label: &str,
        plan: ScoopPlan,
    ) -> Option<Row> {
        let pools = plan.batches.len();
        // Distinct orders: a routed order owns one primary swap and a
        // continuation per later leg, so counting continuations here would
        // count one order several times.
        let orders: usize = plan
            .batches
            .iter()
            .map(|b| b.swaps.len() + b.deposits.len() + b.withdraws.len() + b.zaps.len())
            .sum();
        // Transcript entries, which is what the router calls a "step": a zap
        // emits two.
        let steps: usize = plan
            .batches
            .iter()
            .map(|b| {
                b.swaps.len()
                    + b.continuations.len()
                    + b.deposits.len()
                    + b.withdraws.len()
                    + 2 * b.zaps.len()
                    + b.claims.len()
            })
            .sum();

        let build = |ex: Option<&[(RedeemersKey, ExUnits)]>, fee: Option<u64>| {
            build_multi_pool_scoop_tx(
                &plan,
                settings,
                &env.exec,
                ValidityWindow::new(SLOT, SLOT),
                &env.language_views,
                &env.collateral_utxo,
                &env.collateral_value,
                ex,
                &env.ref_utxo_outputs,
                fee,
                &env.order_configs,
                &BTreeMap::new(),
                None,
                env.funding.as_ref().map(|(i, v)| (i.clone(), v)),
                env.butane.as_ref(),
            )
        };

        let first = build(None, None).ok()?;
        let eval = evaluate_scoop_tx(
            &first.tx_body,
            &first.redeemers,
            &first.resolved_inputs,
            &first.resolved_ref_inputs,
            &env.scripts,
            &env.exec.plutus_v3_cost_model,
            env.exec.plutus_v2_cost_model.as_deref(),
            first.tx_hash,
            &env.exec.slot_config,
            None,
        )
        .ok()?;

        // Same padding the scooper declares.
        let (pad_num, pad_den) = env.exec.budget_padding;
        let padded: Vec<(RedeemersKey, ExUnits)> = eval
            .budgets
            .iter()
            .map(|(k, eu)| {
                let mut p = *eu;
                p.mem = eu.mem * pad_num / pad_den;
                p.steps = eu.steps * pad_num / pad_den;
                (k.clone(), p)
            })
            .collect();
        let mem: u64 = padded.iter().map(|(_, eu)| eu.mem).sum();
        let cpu: u64 = padded.iter().map(|(_, eu)| eu.steps).sum();

        let size = first.cbor.len();
        let ref_bytes = first.total_ref_script_bytes;
        let fee = compute_tx_fee(size as u64, mem, cpu, ref_bytes) + 1000;
        let fee_no_ref = compute_tx_fee(size as u64, mem, cpu, 0) + 1000;
        let final_tx = build(Some(&padded), Some(fee)).ok()?;

        Some(Row {
            label: label.to_string(),
            pools,
            orders,
            steps,
            mem,
            cpu,
            size,
            final_size: final_tx.cbor.len(),
            ref_bytes,
            fee,
            fee_no_ref,
        })
    }

    /// The production fitness rule (`scooper.rs::within_limits`): padded
    /// units under the tx budgets and the first-pass size under the tx size
    /// limit.
    fn fits(env: &TestEnv, r: &Row) -> bool {
        r.mem <= env.exec.max_tx_ex_mem
            && r.cpu <= env.exec.max_tx_ex_steps
            && r.size <= env.exec.max_tx_size
    }

    fn print_header(title: &str) {
        println!("\n### {title}\n");
        println!(
            "| scenario | pools | orders | steps | mem | mem% | cpu | cpu% | size | final | size% | ref bytes | fee | fee_no_ref |"
        );
        println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    }

    fn print_row(env: &TestEnv, r: &Row) {
        println!(
            "| {} | {} | {} | {} | {} | {:.1}% | {} | {:.1}% | {} | {} | {:.1}% | {} | {} | {} |",
            r.label,
            r.pools,
            r.orders,
            r.steps,
            r.mem,
            r.mem_pct(env),
            r.cpu,
            r.cpu_pct(env),
            r.size,
            r.final_size,
            r.size_pct(env),
            r.ref_bytes,
            r.fee,
            r.fee_no_ref,
        );
    }

    /// Least-squares slope of `fee_no_ref` against `x`, in lovelace per unit.
    fn slope(rows: &[Row], x: impl Fn(&Row) -> f64) -> f64 {
        let n = rows.len() as f64;
        let xs: Vec<f64> = rows.iter().map(&x).collect();
        let ys: Vec<f64> = rows.iter().map(|r| r.fee_no_ref as f64).collect();
        let mx = xs.iter().sum::<f64>() / n;
        let my = ys.iter().sum::<f64>() / n;
        let num: f64 = xs.iter().zip(&ys).map(|(a, b)| (a - mx) * (b - my)).sum();
        let den: f64 = xs.iter().map(|a| (a - mx).powi(2)).sum();
        if den == 0.0 { 0.0 } else { num / den }
    }

    // ─── Scans ────────────────────────────────────────────────────────────

    /// `n` orders against ONE pool: isolates the cost of one more order (one
    /// more transcript entry, order input, fulfillment output and order
    /// redeemer) with no new pool.
    fn orders_on_one_pool(
        env: &TestEnv,
        settings: &SundaeV4Settings,
        curve: Curve,
        n: usize,
    ) -> Option<Row> {
        measure(
            env,
            settings,
            &format!("{} 1 pool x {n} orders", curve.label()),
            vec![batch_for(env, curve, 0, n)],
        )
    }

    /// `n` pools, one order each: same order count as `orders_on_one_pool(n)`,
    /// so the difference between the two is the cost of the extra pools
    /// alone (pool input, pool output, pool spend redeemer, per-pool module
    /// entries and the pool validator run).
    fn one_order_each(
        env: &TestEnv,
        settings: &SundaeV4Settings,
        curve: Curve,
        n: usize,
    ) -> Option<Row> {
        let batches: Vec<Batch> = (0..n).map(|i| batch_for(env, curve, i, 1)).collect();
        measure(
            env,
            settings,
            &format!("{} {n} pools x 1 order", curve.label()),
            batches,
        )
    }

    /// The two scans for one curve, up to `n_max`, plus the derived
    /// marginals. Returns (per-order lovelace, per-extra-pool lovelace).
    fn scan_curve(
        env: &TestEnv,
        settings: &SundaeV4Settings,
        curve: Curve,
        n_max: usize,
    ) -> (f64, f64) {
        print_header(&format!("{} — orders on one pool", curve.label()));
        let mut by_order: Vec<Row> = Vec::new();
        for n in 1..=n_max {
            let Some(r) = orders_on_one_pool(env, settings, curve, n) else {
                break;
            };
            print_row(env, &r);
            let fits = fits(env, &r);
            by_order.push(r);
            if !fits {
                break;
            }
        }

        print_header(&format!("{} — one order each, n pools", curve.label()));
        let mut by_pool: Vec<Row> = Vec::new();
        for n in 1..=n_max {
            let Some(r) = one_order_each(env, settings, curve, n) else {
                break;
            };
            print_row(env, &r);
            let fits = fits(env, &r);
            by_pool.push(r);
            if !fits {
                break;
            }
        }

        let per_order = slope(&by_order, |r| r.orders as f64);
        // Pure pool cost: at equal order counts, what the extra pools add.
        let mut pool_deltas: Vec<f64> = Vec::new();
        for p in &by_pool {
            if p.pools < 2 {
                continue;
            }
            if let Some(o) = by_order.iter().find(|o| o.orders == p.orders) {
                pool_deltas
                    .push((p.fee_no_ref as f64 - o.fee_no_ref as f64) / (p.pools - 1) as f64);
            }
        }
        let per_pool = if pool_deltas.is_empty() {
            0.0
        } else {
            pool_deltas.iter().sum::<f64>() / pool_deltas.len() as f64
        };

        println!(
            "\n**{}**: one more order on a pool already in the tx = **{:.0} lovelace**; \
             one more pool at a fixed order count = **{:.0} lovelace**; \
             one more pool carrying its own order = **{:.0} lovelace**. \
             (A routed hop is cheaper than that last figure — it adds a pool \
             and a transcript entry but no second order input or payout; the \
             routed scan below measures it.)",
            curve.label(),
            per_order,
            per_pool,
            per_order + per_pool,
        );
        (per_order, per_pool)
    }

    /// A `k`-hop chain: pools `(c0,c1), (c1,c2), … (c_{k-1},c_k)`, and one
    /// order paying `c0` for `c_k`. This is the shape the two constants
    /// gate — one order, `k` pools, `k` steps — so its cost is what a
    /// `k`-hop route has to be worth.
    fn routed_hops(
        env: &TestEnv,
        settings: &SundaeV4Settings,
        curve: Curve,
        k: usize,
    ) -> Option<Row> {
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::router;

        let chain = |i: usize| token(0xC0 + i as u8, 0x02);
        let mut pool_map: BTreeMap<crate::sundaev3::Ident, Arc<SundaeV4Pool>> = BTreeMap::new();
        for i in 0..k {
            let ident = 0x60 + i as u8;
            let (ta, tb) = (chain(i), chain(i + 1));
            let pool = match curve {
                Curve::Cp => make_pool(env, ident, ta, RESERVE, tb, RESERVE),
                Curve::Cs => make_cs_pool(
                    env,
                    ident,
                    vec![(ta, RESERVE), (tb, RESERVE)],
                    vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
                    fee_3_per_mille(),
                ),
                Curve::Ss => make_ss_pool(
                    env,
                    ident,
                    vec![(ta, RESERVE), (tb, RESERVE)],
                    ss_config(200, fee_3_per_mille(), [1, 1]),
                ),
            };
            pool_map.insert(pool.pool_datum.identifier.clone(), pool);
        }

        let order = make_order(chain(0), ORDER_SIZE, chain(k), 1, 900 + k as u64);
        let route = router::find_optimal_route(
            &pool_map,
            &[],
            &chain(0),
            &chain(k),
            order.swap_offered().1,
            router::RoutingLimits::unlimited(),
        )?;
        // Only the full chain reaches the output asset, so anything else is
        // a router bug rather than a cheaper route.
        assert_eq!(
            route.hops.len(),
            k,
            "{}: expected a {k}-hop route",
            curve.label()
        );

        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum.try_add_routed_order(&order, &route, &pool_map).ok()?;
        measure_plan(
            env,
            settings,
            &format!("{} 1 order through {k} hops", curve.label()),
            accum.into_plan(),
        )
    }

    // ─── The benchmark ────────────────────────────────────────────────────

    /// Measures every quantity behind the two routing constants and prints
    /// the table. The assertions are regression guards: they fail if a
    /// contract change moves a marginal far enough to invalidate the
    /// recommended constants (`docs/routing-costs.md`).
    #[test]
    #[ignore = "measurement, not a unit test: needs its own process for TEST_CTX \
                (cargo test cost_bench -- --ignored --nocapture)"]
    fn cost_bench_marginal_and_ceiling_table() {
        let mut env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        // Swap the harness's stale cost model for the deployed one, and
        // rebuild the language views so the script-data hash matches it.
        let v3 = deployed_v3_cost_model();
        assert_eq!(
            v3.len(),
            350,
            "expected the current 350-entry V3 cost model"
        );
        env.exec.plutus_v3_cost_model = v3.clone();
        env.language_views = encode_language_views(&v3);
        let env = env;
        let settings = make_settings(&env, &env.scooper_keyhash());

        // TEST_CTX is a process-global OnceLock: if a traced-fixture test
        // got there first, every order here would carry that fixture's
        // constraint hashes and the numbers would be meaningless.
        let ctx_basic = TEST_CTX
            .get()
            .and_then(|c| c.as_ref())
            .map(|c| c.basic_order.clone())
            .expect("blueprint sets the constraint context");
        assert_eq!(
            ctx_basic,
            env.exec.module_scripts.basic_order.as_ref().unwrap().hash.as_ref().to_vec(),
            "another fixture initialised TEST_CTX first — run this test alone: \
             cargo test cost_bench -- --ignored --nocapture",
        );

        println!(
            "\nprotocol limits: mem {}, cpu {}, size {} | padding {:?} | \
             fee = 44/byte + 155381 + 0.0577/mem + 0.0000721/step + tiered ref-script",
            env.exec.max_tx_ex_mem,
            env.exec.max_tx_ex_steps,
            env.exec.max_tx_size,
            env.exec.budget_padding,
        );

        let mut marginals: Vec<(Curve, f64, f64)> = Vec::new();
        for curve in [Curve::Cp, Curve::Cs, Curve::Ss] {
            let (per_order, per_pool) = scan_curve(&env, &settings, curve, 12);
            marginals.push((curve, per_order, per_pool));
        }

        // What the constants actually gate: one order through k hops.
        print_header("routed — one order through k hops");
        let mut routed: Vec<Row> = Vec::new();
        for curve in [Curve::Cp, Curve::Cs, Curve::Ss] {
            for k in 1..=4 {
                let Some(r) = routed_hops(&env, &settings, curve, k) else {
                    break;
                };
                print_row(&env, &r);
                let ok = fits(&env, &r);
                routed.push(r);
                if !ok {
                    break;
                }
            }
        }
        for curve in [Curve::Cp, Curve::Cs, Curve::Ss] {
            let rows: Vec<&Row> =
                routed.iter().filter(|r| r.label.starts_with(curve.label())).collect();
            if let (Some(one), Some(last)) = (rows.first(), rows.last())
                && last.pools > one.pools
            {
                println!(
                    "\n**{} routed**: one hop {} lovelace; {} hops {} lovelace; \
                     each extra hop **{:.0} lovelace**.",
                    curve.label(),
                    one.fee_no_ref,
                    last.pools,
                    last.fee_no_ref,
                    (last.fee_no_ref as f64 - one.fee_no_ref as f64)
                        / (last.pools - one.pools) as f64,
                );
            }
        }

        // A transaction that mixes curves pays each module's reference
        // script once — the one place where "another pool" is expensive.
        print_header("mixed curves, one pool and one order each");
        let mut mixed: Vec<Batch> = Vec::new();
        let mut mixed_rows: Vec<Row> = Vec::new();
        for (i, curve) in [Curve::Cp, Curve::Cs, Curve::Ss].into_iter().enumerate() {
            mixed.push(batch_for(&env, curve, i, 1));
            let label = match i {
                0 => "cp",
                1 => "cp+cs",
                _ => "cp+cs+ss",
            };
            if let Some(r) = measure(&env, &settings, label, mixed.clone()) {
                print_row(&env, &r);
                mixed_rows.push(r);
            }
        }
        if mixed_rows.len() == 3 {
            println!(
                "\nreference-script bytes: {} (cp) -> {} (+cs) -> {} (+ss); \
                 the fee those bytes carry: {} -> {} -> {} lovelace.",
                mixed_rows[0].ref_bytes,
                mixed_rows[1].ref_bytes,
                mixed_rows[2].ref_bytes,
                mixed_rows[0].fee - mixed_rows[0].fee_no_ref,
                mixed_rows[1].fee - mixed_rows[1].fee_no_ref,
                mixed_rows[2].fee - mixed_rows[2].fee_no_ref,
            );
        }

        // ── Capacity ceilings ────────────────────────────────────────────
        print_header("capacity — largest scoop that still fits");
        for curve in [Curve::Cp, Curve::Cs, Curve::Ss] {
            // Orders on one pool.
            let mut last: Option<Row> = None;
            for n in 1..=64 {
                match orders_on_one_pool(&env, &settings, curve, n) {
                    Some(r) if fits(&env, &r) => last = Some(r),
                    _ => break,
                }
            }
            if let Some(r) = last {
                println!(
                    "| {} orders/1 pool ceiling | {} | {} | {} | {:.1}% | {} | {:.1}% | {} | {:.1}% | {} | {} | binds on {} |",
                    curve.label(),
                    r.pools,
                    r.orders,
                    r.mem,
                    r.mem_pct(&env),
                    r.cpu,
                    r.cpu_pct(&env),
                    r.size,
                    r.size_pct(&env),
                    r.ref_bytes,
                    r.fee,
                    r.binding(&env),
                );
            }
            // Pools, one order each.
            let mut last: Option<Row> = None;
            for n in 1..=32 {
                match one_order_each(&env, &settings, curve, n) {
                    Some(r) if fits(&env, &r) => last = Some(r),
                    _ => break,
                }
            }
            if let Some(r) = last {
                println!(
                    "| {} pools/1 order each ceiling | {} | {} | {} | {:.1}% | {} | {:.1}% | {} | {:.1}% | {} | {} | binds on {} |",
                    curve.label(),
                    r.pools,
                    r.orders,
                    r.mem,
                    r.mem_pct(&env),
                    r.cpu,
                    r.cpu_pct(&env),
                    r.size,
                    r.size_pct(&env),
                    r.ref_bytes,
                    r.fee,
                    r.binding(&env),
                );
            }
        }

        // ── Regression guards ────────────────────────────────────────────
        //
        // These pin the two recommended constants to the measurement behind
        // them (docs/routing-costs.md): cost-per-pool 200 000 and
        // cost-per-step 100 000 lovelace. The bands leave ~30% headroom, so
        // ordinary cost-model drift passes and a contract change that moves
        // a marginal by a factor fails here instead of silently making
        // multi-hop orders unroutable or underpriced.
        const REC_COST_PER_POOL: f64 = 250_000.0;
        const REC_COST_PER_STEP: f64 = 125_000.0;
        for (curve, per_order, per_pool) in &marginals {
            assert!(
                *per_order <= REC_COST_PER_STEP,
                "{}: one more order now costs {per_order:.0} lovelace, more than the \
                 recommended cost-per-step of {REC_COST_PER_STEP:.0} — the constant no longer \
                 covers its own marginal; re-measure and update docs/routing-costs.md",
                curve.label(),
            );
            assert!(
                *per_pool <= REC_COST_PER_POOL,
                "{}: one more pool now costs {per_pool:.0} lovelace, more than the \
                 recommended cost-per-pool of {REC_COST_PER_POOL:.0} — re-measure and update \
                 docs/routing-costs.md",
                curve.label(),
            );
        }

        // The quantity the constants actually gate: what one more hop of a
        // routed order costs. `cost-per-pool` must stay at or above it, or
        // the order's budget under-buys the route it is charged for.
        let mut worst_hop = 0.0_f64;
        for curve in [Curve::Cp, Curve::Cs, Curve::Ss] {
            let rows: Vec<&Row> =
                routed.iter().filter(|r| r.label.starts_with(curve.label())).collect();
            if let (Some(one), Some(last)) = (rows.first(), rows.last())
                && last.pools > one.pools
            {
                let per_hop = (last.fee_no_ref as f64 - one.fee_no_ref as f64)
                    / (last.pools - one.pools) as f64;
                worst_hop = worst_hop.max(per_hop);
            }
        }
        assert!(
            worst_hop <= REC_COST_PER_POOL,
            "a routed hop on the most expensive curve now costs {worst_hop:.0} lovelace, \
             above the recommended cost-per-pool of {REC_COST_PER_POOL:.0} — raise the \
             constant and re-check the base_fee a multi-hop order needs \
             (docs/routing-costs.md)",
        );
        // The other direction: a constant far above the cost it prices makes
        // multi-hop orders unroutable for no reason, which is the failure
        // this whole measurement exists to prevent.
        assert!(
            worst_hop >= 0.4 * REC_COST_PER_POOL,
            "a routed hop now costs only {worst_hop:.0} lovelace against a cost-per-pool of \
             {REC_COST_PER_POOL:.0} — the constant is over-charging routes; re-measure and \
             lower it (docs/routing-costs.md)",
        );

        // Memory is the binding limit, not size or CPU. If that ever flips,
        // the capacity section's reasoning needs redoing.
        let biggest = routed.iter().max_by_key(|r| r.mem).expect("the routed scan produced rows");
        assert_eq!(
            biggest.binding(&env),
            "mem",
            "the binding protocol limit changed; redo the capacity section of \
             docs/routing-costs.md",
        );
    }
}
