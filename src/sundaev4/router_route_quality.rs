//! Effective price of a large A→Z swap vs the number of pools one scoop tx
//! may touch (K), on a real market snapshot, with the production router —
//! keeping only the routes whose real scoop tx fits the per-tx limits.
//!
//! Context: ouroboros-leios #1077 (per-tx limits). The market comes from
//! `LEIOS/bench-market/bench/data/market.json` (see its README): every
//! mainnet pool is read as a sundae-v4 pool, constant product or constant
//! sum, with its observed reserves and fee.
//!
//! For each (pair, size, K), the order is a basic swap (`swapIntent`) routed
//! by `find_blended_route` under `RoutingLimits { max_pools: K, max_steps: K }`,
//! accumulated, built into a real scoop tx against the devnet blueprint and
//! evaluated. The tx then fits:
//!   - today  if bytes ≤ 16 384 and padded ExUnits ≤ 14M mem / 10G steps;
//!   - #1077  if bytes ≤ 16 384 and padded ExUnits ≤ 7B mem / 2T steps.
//! As in production, a route whose tx fails to build or evaluate does not fit
//! (the evaluator caps every script at 14M / 10G, `evaluator.rs`).
//!
//! Ignored by default. Run with:
//!   cargo test --release route_quality_vs_k -- --ignored --nocapture
//! Env knobs (all optional):
//!   ROUTE_MARKET    path to market.json (default: ../../LEIOS/bench-market/bench/data/market.json)
//!   ROUTE_PAIRS     comma-separated A-Z tickers (default: ADA-USDM,ADA-NIGHT,ADA-SNEK,SNEK-USDM,NIGHT-USDCx)
//!   ROUTE_SIZES     comma-separated order sizes in USD (default: 10000,100000,500000,1000000)
//!   ROUTE_KS        comma-separated K values (default: 1,2,3,4,6,8,10,12,15,17,20,30)
//!   ROUTE_CSV       write one row per (pair, size, K) to this path

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use pallas_crypto::hash::Hasher;
use plutus_parser::AsPlutus;
use serde_json::Value as Json;

use super::accumulator::Accumulator;
use super::router::{BlendedRoute, RoutingLimits, find_blended_route};
use super::test_harness::{TestEnv, make_basic_swap_order, make_cs_pool, make_pool, make_settings};
use crate::bigint::BigInt;
use crate::cardano_types::{ADA_ASSET_CLASS, AssetClass};
use crate::sundaev3::Ident;
use crate::sundaev4::types::{ConstantProductConfig, PoolType, Rational, SundaeV4Pool};

const BLUEPRINT_PATH: &str = "test/fixtures/devnet-blueprint.json";
/// Fixed-point scale for fees and constant-sum prices.
const FEE_DEN: u64 = 1_000_000;
const PRICE_SCALE: f64 = 1e12;
/// Per-tx limits: bytes, mem, steps. Tx size is not relaxed by #1077.
const TODAY: (usize, u64, u64) = (16_384, 14_000_000, 10_000_000_000);
const LEIOS_1077: (usize, u64, u64) = (16_384, 7_000_000_000, 2_000_000_000_000);

struct Market {
    pools: BTreeMap<Ident, Arc<SundaeV4Pool>>,
    /// Reference price, lovelace per raw unit, by float asset id.
    price: BTreeMap<String, f64>,
    /// Ticker -> float asset id.
    asset_id: BTreeMap<String, String>,
    ada_usd: f64,
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_list<T: std::str::FromStr>(key: &str, default: &str) -> Vec<T>
where
    T::Err: std::fmt::Debug,
{
    env_or(key, default).split(',').map(|s| s.trim().parse().expect(key)).collect()
}

fn asset_class(id: &str) -> AssetClass {
    if id == "ada.lovelace" {
        ADA_ASSET_CLASS
    } else {
        id.parse().expect("asset id")
    }
}

fn reserve(v: &Json) -> i64 {
    v.as_i64().expect("reserve")
}

/// `make_pool` with the pool's own fee: the harness uses one global fee, but
/// the fee is part of the constant-product config whose hash sits in the
/// pool's `module_state`, so both must be rewritten together.
fn cp_pool(
    env: &TestEnv,
    ident_byte: u8,
    (a, ra): (AssetClass, i64),
    (b, rb): (AssetClass, i64),
    fee: Rational,
) -> Arc<SundaeV4Pool> {
    let mut pool = (*make_pool(env, ident_byte, a, ra, b, rb)).clone();
    let config = ConstantProductConfig { fee: fee.clone() };
    let hash = Hasher::<256>::hash(&minicbor::to_vec(config.to_plutus()).unwrap()).to_vec();
    pool.pool_datum.module_state[0].1 = hash;
    pool.pool_type = PoolType::ConstantProduct { fee };
    Arc::new(pool)
}

fn load_market(env: &TestEnv, path: &str) -> Market {
    let raw = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let d: Json = serde_json::from_str(&raw).expect("market.json");
    let price: BTreeMap<String, f64> = d["price"]
        .as_object()
        .expect("price")
        .iter()
        .map(|(k, v)| (k.clone(), v.as_f64().expect("price")))
        .collect();
    let asset_id = d["ticker"]
        .as_object()
        .expect("ticker")
        .iter()
        .map(|(id, t)| (t.as_str().expect("ticker").to_string(), id.clone()))
        .collect();

    let mut pools = BTreeMap::new();
    for (i, p) in d["pools"].as_array().expect("pools").iter().enumerate() {
        let (a, b) = (p["a"].as_str().unwrap(), p["b"].as_str().unwrap());
        let (ra, rb) = (reserve(&p["ra"]), reserve(&p["rb"]));
        let fee = Rational {
            num: BigInt::from((p["fee"].as_f64().expect("fee") * FEE_DEN as f64).round() as u64),
            den: BigInt::from(FEE_DEN),
        };
        // Ident byte 0 is avoided so every pool's input hash stays distinct.
        let ident_byte = u8::try_from(i + 1).expect("≤ 255 pools");
        let (ca, cb) = (asset_class(a), asset_class(b));
        let pool = match p["curve"].as_str().expect("curve") {
            "constant_product" => cp_pool(env, ident_byte, (ca, ra), (cb, rb), fee),
            "constant_sum" => {
                let prices = [a, b]
                    .iter()
                    .map(|t| BigInt::from((price[*t] * PRICE_SCALE).round() as u64))
                    .collect();
                make_cs_pool(env, ident_byte, vec![(ca, ra), (cb, rb)], prices, fee)
            }
            other => panic!("unknown curve {other}"),
        };
        pools.insert(pool.pool_datum.identifier.clone(), pool);
    }

    Market {
        pools,
        price,
        asset_id,
        ada_usd: d["ada_usd"].as_f64().expect("ada_usd"),
    }
}

/// Cost of the real scoop tx for one route.
enum TxCost {
    /// Built and evaluated: bytes, padded mem, padded steps.
    Eval(usize, u64, u64),
    /// Could not accumulate, build or evaluate (message kept for the summary).
    Failed(String),
}

impl TxCost {
    /// Same check as production (`scooper.rs`, `within_limits`).
    fn fits(&self, (bytes, mem, steps): (usize, u64, u64)) -> bool {
        match self {
            TxCost::Eval(b, m, s) => *b <= bytes && *m <= mem && *s <= steps,
            TxCost::Failed(_) => false,
        }
    }
}

fn tx_cost(
    env: &TestEnv,
    m: &Market,
    blend: &BlendedRoute,
    amount: i64,
    ca: &AssetClass,
    cz: &AssetClass,
) -> TxCost {
    let order = make_basic_swap_order(ca.clone(), amount, cz.clone(), 1, 1);
    let mut accum = Accumulator::new(env.exec.protocol_share);
    let added = match blend.as_single() {
        Some(single) => accum.try_add_routed_order(&order, single, &m.pools),
        None => accum.try_add_blended_order(&order, blend, &m.pools),
    };
    if let Err(e) = added {
        return TxCost::Failed(format!("accumulate: {e:?}"));
    }
    let plan = accum.into_plan();
    let settings = make_settings(env, &env.scooper_keyhash());
    match env.build_and_eval_plan(&plan, &settings, 1000) {
        Ok((build, r)) => {
            let (num, den) = env.exec.budget_padding;
            let mem: u64 = r.budgets.iter().map(|(_, eu)| eu.mem).sum();
            let steps: u64 = r.budgets.iter().map(|(_, eu)| eu.steps).sum();
            TxCost::Eval(build.cbor.len(), mem * num / den, steps * num / den)
        }
        // As in production (`Fitness::Bail`), a tx that fails to build or
        // evaluate does not fit, whatever the cause.
        Err(e) => TxCost::Failed(format!("build/eval: {e}")),
    }
}

/// One routed order at one K.
struct Point {
    loss: f64,
    pools: usize,
    branches: usize,
    max_hops: usize,
    cost: TxCost,
}

fn point(
    env: &TestEnv,
    m: &Market,
    blend: Option<BlendedRoute>,
    reference: f64,
    amount: i64,
    ca: &AssetClass,
    cz: &AssetClass,
) -> Option<Point> {
    let blend = blend?;
    let idents: BTreeSet<&Ident> = blend
        .branches
        .iter()
        .flat_map(|b| b.hops.iter())
        .flat_map(|h| h.splits.iter().map(|s| &s.pool.ident))
        .collect();
    Some(Point {
        loss: 1.0 - blend.total_output.to_f64().unwrap_or(0.0) / reference,
        pools: idents.len(),
        branches: blend.branches.len(),
        max_hops: blend.branches.iter().map(|b| b.hops.len()).max().unwrap_or(0),
        cost: tx_cost(env, m, &blend, amount, ca, cz),
    })
}

/// Lowest-loss point among `points` satisfying `keep`.
fn best<'a>(points: &[&'a Point], keep: impl Fn(&Point) -> bool) -> Option<&'a Point> {
    points.iter().copied().filter(|p| keep(p)).min_by(|a, b| a.loss.total_cmp(&b.loss))
}

/// Table cell for a best point: loss % and pools.
fn cell(p: Option<&Point>) -> String {
    match p {
        Some(p) => format!("{:>8} {:>5}", format!("{:.2} %", p.loss * 100.0), p.pools),
        None => format!("{:>8} {:>5}", "-", "-"),
    }
}

/// Route one order at every K and cost each route's tx. Returns the printed
/// table row, its CSV rows, and the failure messages seen.
///
/// The unlimited route is computed first: at any K ≥ the pools it uses, that
/// route fits the budget as is, so it is reused instead of re-running the
/// router (whose over-budget pruning re-solves the whole blend once per
/// dropped pool, the dominant cost at small K).
fn run_job(market_path: &str, ks: &[usize], pair: &str, size: f64) -> (String, String, Vec<String>) {
    let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
    let m = load_market(&env, market_path);
    let (ta, tz) = pair.split_once('-').expect("pair A-Z");
    let (a, z) = (&m.asset_id[ta], &m.asset_id[tz]);
    let (ca, cz) = (asset_class(a), asset_class(z));
    let amount = size / m.ada_usd * 1e6 / m.price[a];
    let reference = amount * m.price[a] / m.price[z];
    let amount_raw = BigInt::from(amount as u64);
    let amount = amount as i64;

    let route = |limits: RoutingLimits| {
        let blend = find_blended_route(&m.pools, &[], &ca, &cz, &amount_raw, limits);
        point(&env, &m, blend, reference, amount, &ca, &cz)
    };
    let unl = route(RoutingLimits::unlimited());
    let unl_pools = unl.as_ref().map_or(0, |p| p.pools);

    let mut fresh: Vec<Option<Point>> = Vec::new();
    let mut at_k: Vec<(usize, bool)> = Vec::new(); // (k, reuses unl)
    for &k in ks {
        if unl.is_some() && k >= unl_pools {
            at_k.push((k, true));
        } else {
            fresh.push(route(RoutingLimits {
                max_pools: k,
                max_steps: k,
            }));
            at_k.push((k, false));
        }
    }

    let (mut csv, mut errors) = (String::new(), Vec::new());
    let mut seen: Vec<&Point> = Vec::new();
    let mut fresh_iter = fresh.iter();
    for &(k, reused) in &at_k {
        let p = if reused { unl.as_ref() } else { fresh_iter.next().unwrap().as_ref() };
        if let Some(p) = p {
            seen.push(p);
            if let TxCost::Failed(e) = &p.cost {
                errors.push(e.clone());
            }
            let (bytes, mem, steps, eval) = match &p.cost {
                TxCost::Eval(b, m, s) => (*b, *m, *s, "ok"),
                TxCost::Failed(_) => (0, 0, 0, "failed"),
            };
            csv += &format!(
                "{pair},{size},{k},{},{},{},{},{bytes},{mem},{steps},{eval},{},{}\n",
                p.loss,
                p.pools,
                p.branches,
                p.max_hops,
                p.cost.fits(TODAY),
                p.cost.fits(LEIOS_1077),
            );
        }
    }
    if let Some(p) = unl.as_ref() {
        seen.push(p);
    }

    // Best route whose tx fits today, fits #1077, and best route at all (no
    // tx limit) among every route the router produced for this order.
    let today = best(&seen, |p| p.cost.fits(TODAY));
    let leios = best(&seen, |p| p.cost.fits(LEIOS_1077));
    let any = best(&seen, |_| true);
    let gain = match (today, leios) {
        (Some(t), Some(l)) => format!("{:>9.0} $", (t.loss - l.loss) * size),
        _ => format!("{:>11}", "n/a"),
    };
    let row = format!(
        "{:<12} {:>5.0}k | {} | {} | {} | {gain}",
        pair.replace('-', "→"),
        size / 1e3,
        cell(today),
        cell(leios),
        cell(any),
    );
    (row, csv, errors)
}

#[test]
#[ignore = "slow, needs market.json, run with --ignored --nocapture"]
fn route_quality_vs_k() {
    let market_path = env_or("ROUTE_MARKET", "../../LEIOS/bench-market/bench/data/market.json");
    let pairs: Vec<String> =
        env_list("ROUTE_PAIRS", "ADA-USDM,ADA-NIGHT,ADA-SNEK,SNEK-USDM,NIGHT-USDCx");
    let sizes: Vec<f64> = env_list("ROUTE_SIZES", "10000,100000,500000,1000000");
    let ks: Vec<usize> = env_list("ROUTE_KS", "1,2,3,4,6,8,10,12,15,17,20,30");
    let csv_path = std::env::var("ROUTE_CSV").ok();

    // One (pair, size) job per thread, each with its own harness and market.
    // A single progress counter is redrawn in place on stderr, then the tables
    // are printed in order.
    let jobs: Vec<(&String, f64)> =
        pairs.iter().flat_map(|p| sizes.iter().map(move |s| (p, *s))).collect();
    let t0 = std::time::Instant::now();
    let done = std::sync::atomic::AtomicUsize::new(0);
    let results: Vec<(String, String, Vec<String>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = jobs
            .iter()
            .map(|&(pair, size)| {
                let (market_path, ks, done, total) = (&market_path, &ks, &done, jobs.len());
                scope.spawn(move || {
                    let out = run_job(market_path, ks, pair, size);
                    let n = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    eprint!("\rrouting {n}/{total}");
                    out
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("job panicked")).collect()
    });
    eprint!("\r\x1b[K");

    let mut csv = String::from(
        "pair,size_usd,k,loss,pools_used,branches,max_hops,tx_bytes,mem_padded,steps_padded,eval,fits_today,fits_1077\n",
    );
    let mut errors: BTreeMap<String, usize> = BTreeMap::new();
    println!(
        "\n{:<12} {:>6} | {:>8} {:>5} | {:>8} {:>5} | {:>8} {:>5} | today→#1077",
        "pair", "size", "today", "pools", "#1077", "pools", "no limit", "pools"
    );
    for (i, ((pair, _), (row, rows_csv, errs))) in jobs.iter().zip(&results).enumerate() {
        if i > 0 && jobs[i - 1].0 != *pair {
            println!();
        }
        println!("{row}");
        csv += rows_csv;
        for e in errs {
            *errors.entry(e.chars().take(160).collect()).or_default() += 1;
        }
    }
    println!(
        "\nlowest loss vs ref price; today = tx ≤ 16 KB, 14M mem, 10G steps; \
         #1077 = tx ≤ 16 KB, 7B mem, 2T steps; no limit = best route regardless of tx. \
         Loss includes pool fees and price impact, not the Cardano tx fee. {:.0}s",
        t0.elapsed().as_secs_f64()
    );
    for (e, n) in &errors {
        println!("failed ×{n}: {e}");
    }

    if let Some(path) = csv_path {
        std::fs::write(&path, csv).unwrap_or_else(|e| panic!("{path}: {e}"));
        println!("csv: {path}");
    }
}
