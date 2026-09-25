//! Route-quality benchmark: how well a large A→Z swap executes on a sundae-v4
//! market, with the production router, as a function of how complex one scoop
//! tx may be (its size and ExUnits budget). See `bench/route-quality/README.md`.
//!
//! For each (pair, size), the order is a basic swap (`swapIntent`) routed by
//! `find_blended_route` under `RoutingLimits { max_pools: K, max_steps: K }`,
//! accumulated, built into a real scoop tx against the devnet blueprint and
//! evaluated. Routes are checked against two tx limits (bytes, padded mem,
//! padded steps): `mainnet` (default: the current mainnet limits) and `raised`
//! (default: a raised ExUnits budget, tx size unchanged). For each limit, the
//! largest K whose tx fits it is found by bisection (see `run_job`); the best
//! route with no tx limit is reported too. As in production, a route whose tx
//! fails to build or evaluate does not fit (the evaluator caps every script at
//! 14M / 10G, `evaluator.rs`).
//!
//! Built as the `route-bench` example, which recompiles the crate's modules
//! (see `src/route_bench.rs`):
//!   cargo run --release --features route-bench --example route-bench -- --help

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use clap::Parser;
use pallas_crypto::hash::Hasher;
use plutus_parser::AsPlutus;
use serde_json::Value as Json;

use crate::bigint::BigInt;
use crate::cardano_types::{ADA_ASSET_CLASS, AssetClass};
use crate::sundaev3::Ident;
use crate::sundaev4::accumulator::Accumulator;
use crate::sundaev4::router::{BlendedRoute, PoolViewType, RoutingLimits, find_blended_route};
use crate::sundaev4::test_harness::{
    TestEnv, make_basic_swap_order, make_cs_pool, make_pool, make_settings, pool_script_address,
};
use crate::sundaev4::{
    ActionEntry, ConcentratedLiquidityConfig, ConstantProductConfig, PoolDatum, PoolType, Rational,
    SundaeV4Pool, plutus_void,
};

const DATA: &str = "bench/route-quality/data";

/// Fixed-point scale for fees and constant-sum prices.
const FEE_DEN: u64 = 1_000_000;
const PRICE_SCALE: f64 = 1e12;

/// A tx limit: max tx size in bytes, max padded mem and steps.
#[derive(Clone)]
struct TxLimit {
    bytes: usize,
    mem: u64,
    steps: u64,
}

impl std::str::FromStr for TxLimit {
    type Err = String;

    /// `bytes,mem,steps`, e.g. `16384,14000000,10000000000`.
    fn from_str(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split(',').map(str::trim).collect();
        let [bytes, mem, steps] = parts[..] else {
            return Err(format!("expected bytes,mem,steps, got `{s}`"));
        };
        let num = |v: &str| v.parse::<u64>().map_err(|e| format!("`{v}`: {e}"));
        Ok(TxLimit {
            bytes: num(bytes)? as usize,
            mem: num(mem)?,
            steps: num(steps)?,
        })
    }
}

impl std::fmt::Display for TxLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let si = |n: u64| match n {
            n if n >= 1_000_000_000_000 => format!("{}T", n as f64 / 1e12),
            n if n >= 1_000_000_000 => format!("{}G", n as f64 / 1e9),
            n if n >= 1_000_000 => format!("{}M", n as f64 / 1e6),
            n => n.to_string(),
        };
        write!(
            f,
            "tx ≤ {} B, {} mem, {} steps",
            self.bytes,
            si(self.mem),
            si(self.steps)
        )
    }
}

struct Market {
    pools: BTreeMap<Ident, Arc<SundaeV4Pool>>,
    /// Reference price, lovelace per raw unit, by float asset id.
    price: BTreeMap<String, f64>,
    /// Ticker -> float asset id.
    asset_id: BTreeMap<String, String>,
    /// Token decimals, by float asset id: 10^decimals raw units per token.
    decimals: BTreeMap<String, u32>,
    /// Asset class -> ticker, and pool ident -> readable name (`--show`).
    ticker_of: BTreeMap<AssetClass, String>,
    name: BTreeMap<Ident, String>,
    ada_usd: f64,
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

/// A single-range concentrated-liquidity pool with the real CL module
/// pipeline (CL + fee split + fairness), so the scoop tx evaluates on chain.
/// The pool's `total_lp` is the CL liquidity L, taken as is from the market file:
/// `cl_market.py` writes exact rational sqrt prices and the exact L the integer
/// reserves support at them, so the pool is never "in deficit" for the router.
/// `make_cl_pool` is not used: its `module_state` is CP-shaped (routing-only
/// tests) and its one-byte ident caps a market at 255 pools; the ident here
/// is derived from the pool's index.
fn cl_pool(
    env: &TestEnv,
    index: usize,
    (a, ra): (AssetClass, i64),
    (b, rb): (AssetClass, i64),
    liquidity: i64,
    (sqrt_price_a, sqrt_price_b): (Rational, Rational),
    fee: Rational,
) -> Arc<SundaeV4Pool> {
    let config = ConcentratedLiquidityConfig {
        sqrt_price_a,
        sqrt_price_b,
        fee,
    };
    let liquidity = BigInt::from(liquidity);
    let cl_hash = env
        .exec
        .module_scripts
        .concentrated_liquidity
        .as_ref()
        .expect("blueprint includes concentratedLiquidity")
        .hash
        .to_vec();
    let mut module_state = env.module_state();
    module_state[0] = (
        cl_hash.clone(),
        Hasher::<256>::hash(&minicbor::to_vec(config.clone().to_plutus()).unwrap()).to_vec(),
    );
    let mut modules = env.action_modules();
    modules[0] = cl_hash;

    let index = u16::try_from(index).expect("≤ 65535 pools").to_be_bytes();
    let mut ident = vec![0xc1; 26];
    ident.extend_from_slice(&index);
    let policy = env.exec.module_scripts.pool_mint.hash.to_vec();
    let named = |prefix: [u8; 4]| AssetClass {
        policy: policy.clone(),
        token: [prefix.as_slice(), &ident].concat(),
    };
    let mut value = crate::cardano_types::Value::default();
    value.insert(&ADA_ASSET_CLASS, BigInt::from(50_000_000i64)); // min UTxO
    // A band out of range holds one asset only; zero quantities are not
    // valid multi-asset entries. `add`, not `insert` (which overwrites): when
    // ADA is one of the pool's assets its reserve comes on top of the min-UTxO
    // ADA, otherwise draining the band leaves the pool under POOL_MIN_ADA and
    // the tx builder tops it up from nowhere ("lovelace not conserved").
    for (asset, qty) in [(&a, ra), (&b, rb)] {
        if qty > 0 {
            value.add(asset, &BigInt::from(qty));
        }
    }
    value.insert(&named([0x00, 0x0d, 0xe1, 0x40]), BigInt::from(1i64)); // pool NFT
    value.insert(&named([0x00, 0x14, 0xdf, 0x10]), liquidity.clone()); // preminted LP
    let mut tx_hash = [0u8; 32];
    tx_hash[..2].copy_from_slice(&index);
    tx_hash[2] = 0xc1;

    Arc::new(SundaeV4Pool {
        input: crate::cardano_types::TransactionInput::new(tx_hash.into(), 0),
        address: pool_script_address(env, None),
        value,
        pool_datum: PoolDatum {
            assets: vec![(a, BigInt::from(ra)), (b, BigInt::from(rb))],
            total_lp: liquidity.clone(),
            circulating_lp: BigInt::from(0),
            preminted_lp: liquidity,
            identifier: Ident::new(&ident),
            actions: vec![ActionEntry {
                tag: BigInt::from(100),
                enabled: true,
                modules,
            }],
            module_state,
            min_surplus: BigInt::from(0),
            extension: plutus_void(),
        },
        pool_type: PoolType::ConcentratedLiquidity {
            sqrt_price_a: config.sqrt_price_a,
            sqrt_price_b: config.sqrt_price_b,
            fee: config.fee,
        },
        slot: 100,
        fee_split_config: None,
    })
}

/// WORKAROUND for a bug in the sundae-v4 concentrated-liquidity smart
/// contract. REMOVE (with its use in `load_market`) ONCE THE CONTRACT IS FIXED.
///
/// The bug: in the B-input branch of `sundae-v4/lib/modules/cl_check.ak`,
/// `let dvb_eff = dx_eff * spa_num` should be `spa_den`, so a swap that sells a
/// CL pool's SECOND asset has its input scaled by sqrt(pa). When the band's
/// lower price pa < 1 it pays out only ~sqrt(pa) of the fair output (a 96 %
/// loss on every SNEK sale of the CL market); when pa > 1 the pool would lose
/// value, and such a pool even makes the router reject the whole hop (it looks
/// overpriced during the split search, then is clamped to ~0 at the end, so
/// the hop comes out undersized). The router reproduces the contract's formula
/// on purpose.
///
/// The workaround: for each benchmarked trade, the CL pairs listed here are
/// loaded in the other order, (b, a, [1/pb, 1/pa], L), which is the same pool
/// (sundae-v4 does not constrain asset order), so that the trade's main paths
/// sell each pool's FIRST asset. Hard-coded per trade: one orientation cannot
/// serve every trade (ADA→SNEK needs ADA/SNEK, SNEK→USDM needs SNEK/ADA).
/// Secondary paths through other second-asset hops stay penalised or rejected.
/// For the measured flows this amounts to assuming the bug fixed. Only the
/// default trades are listed: a new trade may need its own entry (check its
/// legs with `--show`).
fn flipped_cl_pairs(trade: &str) -> &'static [(&'static str, &'static str)] {
    match trade {
        "SNEK-USDM" => &[
            ("ADA", "SNEK"),
            ("USDCx", "SNEK"),
            ("NIGHT", "SNEK"),
            ("USDM", "NIGHT"),
        ],
        "NIGHT-USDCx" => &[
            ("ADA", "NIGHT"),
            ("USDCx", "NIGHT"),
            ("USDM", "NIGHT"),
            ("USDCx", "USDM"),
            ("ADA", "SNEK"),
            ("USDCx", "SNEK"),
        ],
        _ => &[],
    }
}

/// `trade` (A-Z tickers) only selects the CL pairs to flip, see `flipped_cl_pairs`.
fn load_market(env: &TestEnv, path: &str, trade: &str) -> Market {
    let raw = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let d: Json = serde_json::from_str(&raw).expect("market-mainnet-selected.json");
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

    let decimals = d["decimals"]
        .as_object()
        .expect("decimals")
        .iter()
        .map(|(id, v)| (id.clone(), v.as_u64().expect("decimals") as u32))
        .collect();

    let ticker_of: BTreeMap<AssetClass, String> = d["ticker"]
        .as_object()
        .expect("ticker")
        .iter()
        .map(|(id, t)| (asset_class(id), t.as_str().expect("ticker").to_string()))
        .collect();
    let mut pools = BTreeMap::new();
    let mut name = BTreeMap::new();
    for (i, p) in d["pools"].as_array().expect("pools").iter().enumerate() {
        let (a, b) = (p["a"].as_str().unwrap(), p["b"].as_str().unwrap());
        let (ra, rb) = (reserve(&p["ra"]), reserve(&p["rb"]));
        let fee = Rational {
            num: BigInt::from((p["fee"].as_f64().expect("fee") * FEE_DEN as f64).round() as u64),
            den: BigInt::from(FEE_DEN),
        };
        // Ident byte 0 is avoided so every pool's input hash stays distinct.
        let ident_byte = || u8::try_from(i + 1).expect("≤ 255 CP/CS pools");
        let (ca, cb) = (asset_class(a), asset_class(b));
        let pool = match p["curve"].as_str().expect("curve") {
            "constant_product" => cp_pool(env, ident_byte(), (ca, ra), (cb, rb), fee),
            "concentrated_liquidity" => {
                // Exact rational sqrt prices `[num, den]`.
                let ratio = |k: &str, inverse: bool| {
                    let v = p[k].as_array().expect(k);
                    let (num, den) = (v[0].as_u64().expect(k), v[1].as_u64().expect(k));
                    let (num, den) = if inverse { (den, num) } else { (num, den) };
                    Rational {
                        num: BigInt::from(num),
                        den: BigInt::from(den),
                    }
                };
                let liquidity = reserve(&p["L"]);
                // WORKAROUND for the CL contract bug, see `flipped_cl_pairs`. The
                // mirror (b, a, [1/sqrt(pb), 1/sqrt(pa)]) uses the exact reciprocals,
                // which support exactly the same L.
                let tick = |t: &str| d["ticker"][t].as_str().expect("ticker");
                if flipped_cl_pairs(trade).contains(&(tick(a), tick(b))) {
                    let sqrt_prices = (ratio("sqrt_pb", true), ratio("sqrt_pa", true));
                    cl_pool(env, i, (cb, rb), (ca, ra), liquidity, sqrt_prices, fee)
                } else {
                    let sqrt_prices = (ratio("sqrt_pa", false), ratio("sqrt_pb", false));
                    cl_pool(env, i, (ca, ra), (cb, rb), liquidity, sqrt_prices, fee)
                }
            }
            "constant_sum" => {
                let prices = [a, b]
                    .iter()
                    .map(|t| BigInt::from((price[*t] * PRICE_SCALE).round() as u64))
                    .collect();
                make_cs_pool(env, ident_byte(), vec![(ca, ra), (cb, rb)], prices, fee)
            }
            other => panic!("unknown curve {other}"),
        };
        // Named after the pool's actual asset order (shows the flips).
        let assets = &pool.pool_datum.assets;
        let label = format!("{}/{}", ticker_of[&assets[0].0], ticker_of[&assets[1].0]);
        let label = match p["band"].as_i64() {
            Some(band) => format!("{label} band {band:+}"),
            None => format!("{label} {}", p["venue"].as_str().unwrap_or("")),
        };
        name.insert(pool.pool_datum.identifier.clone(), label);
        pools.insert(pool.pool_datum.identifier.clone(), pool);
    }

    Market {
        pools,
        price,
        asset_id,
        decimals,
        ticker_of,
        name,
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
    fn fits(&self, limit: &TxLimit) -> bool {
        match self {
            TxCost::Eval(b, m, s) => *b <= limit.bytes && *m <= limit.mem && *s <= limit.steps,
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
    /// Amount of Z received, raw units.
    output: f64,
    pools: usize,
    branches: usize,
    max_hops: usize,
    /// Every split of the route, one line each (only with `--show`).
    detail: String,
    cost: TxCost,
}

/// One line per split: hop, pool, input and output valued in ADA at the
/// reference prices, and the leg's rate vs reference (pool fee + price impact).
/// A CL leg that sells the pool's second asset is flagged: with the contract
/// bug (see `flipped_cl_pairs`) its rate is wrong.
fn describe(m: &Market, blend: &BlendedRoute) -> String {
    let ada = |amount: &BigInt, asset: &AssetClass| {
        let id = &m.asset_id[&m.ticker_of[asset]];
        amount.to_f64().unwrap_or(0.0) * m.price[id] / 1e6
    };
    let mut out = String::new();
    for (k, branch) in blend.branches.iter().enumerate() {
        out += &format!("    branch {k}\n");
        for hop in &branch.hops {
            let (tin, tout) = (
                &m.ticker_of[&hop.input_token],
                &m.ticker_of[&hop.output_token],
            );
            for split in &hop.splits {
                let flag = match split.pool.view_type {
                    PoolViewType::ConcentratedLiquidity {
                        is_a_input: false, ..
                    } => "  <-- sells the pool's 2nd asset",
                    _ => "",
                };
                let i = ada(&split.input_amount, &hop.input_token);
                let o = ada(&split.output_amount, &hop.output_token);
                out += &format!(
                    "      {:<12} {:<22} in {:>12.0} ADA  out {:>12.0} ADA  {:+7.2} %{flag}\n",
                    format!("{tin}→{tout}"),
                    m.name[&split.pool.ident],
                    i,
                    o,
                    (o / i - 1.0) * 100.0,
                );
            }
        }
    }
    out
}

fn point(
    env: &TestEnv,
    m: &Market,
    show: bool,
    blend: Option<BlendedRoute>,
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
        output: blend.total_output.to_f64().unwrap_or(0.0),
        pools: idents.len(),
        branches: blend.branches.len(),
        max_hops: blend.branches.iter().map(|b| b.hops.len()).max().unwrap_or(0),
        detail: if show {
            describe(m, &blend)
        } else {
            String::new()
        },
        cost: tx_cost(env, m, &blend, amount, ca, cz),
    })
}

/// Highest-output point among `points` satisfying `keep` (on a tie, the first).
fn best<'a>(points: &[&'a Point], keep: impl Fn(&Point) -> bool) -> Option<&'a Point> {
    points.iter().copied().filter(|p| keep(p)).min_by(|a, b| b.output.total_cmp(&a.output))
}

fn to_f64(n: &BigInt) -> f64 {
    n.to_f64().unwrap_or(f64::NAN)
}

/// Fee-free marginal rate of one pool, raw `to` per raw `from`, read from its
/// state alone (no reference price). `None` when the pool cannot pay `to`
/// (an out-of-range CL band holding only `from`) or its curve is not handled.
///
/// - constant product: `r_to / r_from`;
/// - constant sum: `p_from / p_to`, its own prices (`V = Σ p_i r_i`);
/// - concentrated liquidity: the ratio of the virtual reserves of the CP
///   invariant `(a + L/√pb)(b + L·√pa) = L²` (`PoolType::ConcentratedLiquidity`),
///   L being the pool's `total_lp`. For an out-of-range band it is the band's
///   edge price.
fn pool_mid(pool: &SundaeV4Pool, from: &AssetClass, to: &AssetClass) -> Option<f64> {
    let assets = &pool.pool_datum.assets;
    let i = assets.iter().position(|(c, _)| c == from)?;
    let j = assets.iter().position(|(c, _)| c == to)?;
    let (r_from, r_to) = (to_f64(&assets[i].1), to_f64(&assets[j].1));
    if r_to <= 0.0 {
        return None;
    }
    let ratio = |r: &Rational| to_f64(&r.num) / to_f64(&r.den);
    match &pool.pool_type {
        PoolType::ConstantProduct { .. } => (r_from > 0.0).then(|| r_to / r_from),
        PoolType::ConstantSum { prices, .. } => Some(to_f64(&prices[i]) / to_f64(&prices[j])),
        PoolType::ConcentratedLiquidity {
            sqrt_price_a,
            sqrt_price_b,
            ..
        } => {
            let l = to_f64(&pool.pool_datum.total_lp);
            let va = to_f64(&assets[0].1) + l / ratio(sqrt_price_b);
            let vb = to_f64(&assets[1].1) + l * ratio(sqrt_price_a);
            // Raw b per raw a, inverted when selling b.
            Some(if i == 0 { vb / va } else { va / vb })
        }
        _ => None,
    }
}

/// The market's fee-free A→Z mid, raw Z per raw A, from the pools' state alone.
/// Each directed pair of tokens gets the best mid among its pools, and each
/// acyclic path of at most `MID_MAX_HOPS` (the router's own depth cap, see
/// `find_blended_route`) the product of its pairs' mids. `best` is the highest
/// path mid, the benchmark; `worst` the lowest, to show how much paths
/// disagree (not at all on an arbitrage-free market).
struct Mid {
    best: f64,
    worst: f64,
}

const MID_MAX_HOPS: usize = 4;

fn market_mid(m: &Market, ca: &AssetClass, cz: &AssetClass) -> Option<Mid> {
    let mut edges: BTreeMap<(&AssetClass, &AssetClass), f64> = BTreeMap::new();
    for pool in m.pools.values() {
        for (x, _) in &pool.pool_datum.assets {
            for (y, _) in &pool.pool_datum.assets {
                if let Some(r) = (x != y).then(|| pool_mid(pool, x, y)).flatten() {
                    let e = edges.entry((x, y)).or_insert(r);
                    *e = e.max(r);
                }
            }
        }
    }
    fn walk<'a>(
        edges: &BTreeMap<(&'a AssetClass, &'a AssetClass), f64>,
        path: &mut Vec<&'a AssetClass>,
        rate: f64,
        dest: &AssetClass,
        out: &mut Vec<f64>,
    ) {
        let at = *path.last().unwrap();
        if at == dest {
            out.push(rate);
            return;
        }
        if path.len() > MID_MAX_HOPS {
            return;
        }
        for (&(x, y), &r) in edges {
            if x == at && !path.contains(&y) {
                path.push(y);
                walk(edges, path, rate * r, dest, out);
                path.pop();
            }
        }
    }
    let mut rates = Vec::new();
    walk(&edges, &mut vec![ca], 1.0, cz, &mut rates);
    let best = rates.iter().copied().fold(f64::NAN, f64::max);
    let worst = rates.iter().copied().fold(f64::NAN, f64::min);
    (!rates.is_empty()).then_some(Mid { best, worst })
}

/// Implementation shortfall of `p` vs the mid: Z received vs the same order
/// filled entirely at the mid, as a fraction (≤ 0: pool fees + price impact).
fn shortfall(p: &Point, amount: i64, mid: Option<&Mid>) -> f64 {
    mid.map_or(f64::NAN, |mid| p.output / (amount as f64 * mid.best) - 1.0)
}

/// Table cell for a best point: implementation shortfall vs the mid, in %,
/// and pools.
fn cell_is(p: Option<&Point>, amount: i64, mid: Option<&Mid>) -> String {
    match (p, mid) {
        (Some(p), Some(_)) => format!(
            "{:>8} {:>5}",
            format!("{:+.2} %", shortfall(p, amount, mid) * 100.0),
            p.pools
        ),
        (Some(p), None) => format!("{:>8} {:>5}", "-", p.pools),
        _ => format!("{:>8} {:>5}", "-", "-"),
    }
}

/// `x` with 6 significant digits.
fn sig6(x: f64) -> String {
    if !x.is_finite() || x == 0.0 {
        return format!("{x}");
    }
    let prec = (5 - x.abs().log10().floor() as i32).max(0) as usize;
    format!("{x:.prec$}")
}

/// Route one order, cost each route's tx, and find for each tx limit the
/// largest cap K whose route fits it. Returns the printed table row, its CSV
/// rows, and the failure messages seen.
///
/// The unlimited route is computed first and stored under the number of pools
/// it uses, U: at any K ≥ U that route fits the budget as is, so it is reused
/// instead of re-running the router (whose over-budget pruning re-solves the
/// whole blend once per dropped pool, the dominant cost at small K).
///
/// Per tx limit, K is bisected over [0, U], each probe routing at K and
/// building and evaluating the tx. "Fits" is not monotone in K (a small K may
/// find no route that fills the order), "overflows" (a route exists and its
/// tx exceeds the limit) is assumed to be: the search keeps the largest K
/// whose route does not overflow. If the route there is missing, no K fits.
fn run_job(
    blueprint_path: &str,
    market_path: &str,
    show: bool,
    (mainnet, raised): (&TxLimit, &TxLimit),
    pair: &str,
    size: f64,
) -> (String, String, Vec<String>, f64) {
    let env = TestEnv::from_blueprint_file(blueprint_path);
    let m = load_market(&env, market_path, pair);
    let (ta, tz) = pair.split_once('-').expect("pair A-Z");
    let (a, z) = (&m.asset_id[ta], &m.asset_id[tz]);
    let (ca, cz) = (asset_class(a), asset_class(z));
    let amount = size / m.ada_usd * 1e6 / m.price[a];
    let amount_raw = BigInt::from(amount as u64);
    let amount = amount as i64;

    let route = |limits: RoutingLimits| {
        let blend = find_blended_route(&m.pools, &[], &ca, &cz, &amount_raw, limits);
        point(&env, &m, show, blend, amount, &ca, &cz)
    };

    // Every route computed, by K; the unlimited one under U.
    let mut by_k: BTreeMap<usize, Option<Point>> = BTreeMap::new();
    let unl = route(RoutingLimits::unlimited());
    let unl_pools = unl.as_ref().map_or(0, |p| p.pools);
    if unl.is_some() {
        by_k.insert(unl_pools, unl);
    }
    // Route at cap K (cached), returns the key it is stored under.
    let at = |by_k: &mut BTreeMap<usize, Option<Point>>, k: usize| -> usize {
        let k = if unl_pools > 0 && k >= unl_pools {
            unl_pools
        } else {
            k
        };
        by_k.entry(k).or_insert_with(|| {
            route(RoutingLimits {
                max_pools: k,
                max_steps: k,
            })
        });
        k
    };
    for limit in [mainnet, raised] {
        let overflows = |p: &Option<Point>| p.as_ref().is_some_and(|p| !p.cost.fits(limit));
        if unl_pools == 0 || !overflows(&by_k[&unl_pools]) {
            continue;
        }
        // Invariant: K = lo does not overflow (K = 0 is no route), K = hi does.
        let (mut lo, mut hi) = (0, unl_pools);
        while hi - lo > 1 {
            let mid = (lo + hi) / 2;
            let k = at(&mut by_k, mid);
            if overflows(&by_k[&k]) {
                hi = mid;
            } else {
                lo = mid;
            }
        }
    }

    let mut errors = Vec::new();
    let mut seen: Vec<(usize, &Point)> = Vec::new();
    for (&k, p) in &by_k {
        if let Some(p) = p {
            seen.push((k, p));
            if let TxCost::Failed(e) = &p.cost {
                errors.push(e.clone());
            }
        }
    }

    // Best route whose tx fits the mainnet limit, the raised limit, and best
    // route at all (no tx limit) among every route the router produced.
    let points: Vec<&Point> = seen.iter().map(|&(_, p)| p).collect();
    let at_mainnet = best(&points, |p| p.cost.fits(mainnet));
    let at_raised = best(&points, |p| p.cost.fits(raised));
    let any = best(&points, |_| true);

    // One CSV row per K routed (the unlimited route under U): A spent and Z
    // received (in tokens), Z vs the no-limit route and vs the mid.
    let (dec_a, dec) = (m.decimals[a], m.decimals[z]);
    let mid = market_mid(&m, &ca, &cz);
    // Mid in tokens of Z per token of A.
    let mid_tokens = mid.as_ref().map_or(f64::NAN, |mid| {
        mid.best * 10f64.powi(dec_a as i32 - dec as i32)
    });
    let input = format!(
        "{:.prec$}",
        amount as f64 / 10f64.powi(dec_a as i32),
        prec = dec_a as usize
    );
    let mut csv = String::new();
    for &(k, p) in &seen {
        let (bytes, mem, steps, eval) = match &p.cost {
            TxCost::Eval(b, m, s) => (*b, *m, *s, "ok"),
            TxCost::Failed(_) => (0, 0, 0, "failed"),
        };
        let vs = any.map_or(f64::NAN, |a| p.output / a.output - 1.0);
        csv += &format!(
            "{pair},{size},{k},{input},{:.prec$},{mid_tokens},{vs},{},{},{},{},{bytes},{mem},{steps},{eval},{},{}\n",
            p.output / 10f64.powi(dec as i32),
            shortfall(p, amount, mid.as_ref()),
            p.pools,
            p.branches,
            p.max_hops,
            p.cost.fits(mainnet),
            p.cost.fits(raised),
            prec = dec as usize,
        );
    }
    // Extra Z received under the raised limit vs the mainnet one, valued in USD
    // at Z's reference price: an estimate, unlike the shortfalls.
    let gain_usd = match (at_mainnet, at_raised) {
        (Some(mp), Some(r)) => format!(
            "{:+.2} $",
            (r.output - mp.output) * m.price[z] / 1e6 * m.ada_usd
        ),
        _ => "n/a".to_string(),
    };
    let row = format!(
        "{:<12} {:>7} | {:>12} | {} | {} | {} | {gain_usd:>16}",
        pair.replace('-', "→"),
        format!("${:.0}k", size / 1e3),
        sig6(mid_tokens),
        cell_is(at_mainnet, amount, mid.as_ref()),
        cell_is(at_raised, amount, mid.as_ref()),
        cell_is(any, amount, mid.as_ref()),
    );
    let row = match by_k.get(&unl_pools).and_then(Option::as_ref) {
        Some(p) if !p.detail.is_empty() => format!("{row}\n  unlimited route:\n{}", p.detail),
        _ => row,
    };
    (
        row,
        csv,
        errors,
        mid.map_or(0.0, |mid| 1.0 - mid.worst / mid.best),
    )
}

/// Route-quality benchmark: trade execution vs max tx complexity. Paths are
/// relative to the repo root.
#[derive(Parser)]
struct Args {
    /// Market file (`market-v4-extrapolated.json`: projected v4 CL market; `market-mainnet-selected.json`:
    /// mainnet snapshot).
    #[arg(long, default_value_t = format!("{DATA}/market-v4-extrapolated.json"))]
    market: String,
    /// Scooper-format blueprint the scoop txs are built and evaluated against.
    #[arg(long, default_value_t = format!("{DATA}/devnet-blueprint-without-traces.json"))]
    blueprint: String,
    /// Trades, as comma-separated A-Z tickers.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "ADA-USDM,ADA-NIGHT,ADA-SNEK,SNEK-USDM,NIGHT-USDCx"
    )]
    pairs: Vec<String>,
    /// Order sizes in USD, comma-separated.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "500000,1000000,2000000,5000000"
    )]
    sizes: Vec<f64>,
    /// Lower tx limit, `bytes,mem,steps` (default: the current mainnet limits).
    #[arg(long, default_value = "16384,14000000,10000000000")]
    mainnet_limit: TxLimit,
    /// Higher tx limit, `bytes,mem,steps` (default: raised ExUnits, same tx size).
    #[arg(long, default_value = "16384,7000000000,2000000000000")]
    raised_limit: TxLimit,
    /// Write one CSV row per (pair, size, K) to this path.
    #[arg(long)]
    csv: Option<String>,
    /// Print every split of the unlimited route under its table row.
    #[arg(long)]
    show: bool,
}

pub fn main() {
    let args = Args::parse();
    let (blueprint_path, market_path, show) = (&args.blueprint, &args.market, args.show);
    let limits = (&args.mainnet_limit, &args.raised_limit);

    // One (pair, size) job per thread, each with its own harness and market.
    // A single progress counter is redrawn in place on stderr, then the tables
    // are printed in order.
    let jobs: Vec<(&String, f64)> =
        args.pairs.iter().flat_map(|p| args.sizes.iter().map(move |s| (p, *s))).collect();
    let t0 = std::time::Instant::now();
    let done = std::sync::atomic::AtomicUsize::new(0);
    let results: Vec<(String, String, Vec<String>, f64)> = std::thread::scope(|scope| {
        let handles: Vec<_> = jobs
            .iter()
            .map(|&(pair, size)| {
                let (done, total) = (&done, jobs.len());
                scope.spawn(move || {
                    let out = run_job(blueprint_path, market_path, show, limits, pair, size);
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
        "pair,size_usd,k,input,output,mid,vs_no_limit,shortfall,pools_used,branches,max_hops,tx_bytes,mem_padded,steps_padded,eval,fits_mainnet,fits_raised\n",
    );
    let mut errors: BTreeMap<String, usize> = BTreeMap::new();
    println!(
        "\n{:<12} {:>7} | {:>12} | {:>8} {:>5} | {:>8} {:>5} | {:>8} {:>5} | {:>16}",
        "pair",
        "size",
        "mid A→Z",
        "mainnet",
        "pools",
        "raised",
        "pools",
        "no limit",
        "pools",
        "mainnet→raised $"
    );
    for (i, ((pair, _), (row, rows_csv, errs, _))) in jobs.iter().zip(&results).enumerate() {
        if i > 0 && jobs[i - 1].0 != *pair {
            println!();
        }
        println!("{row}");
        csv += rows_csv;
        for e in errs {
            *errors.entry(e.chars().take(160).collect()).or_default() += 1;
        }
    }
    let mid_spread = results.iter().map(|r| r.3).fold(0.0, f64::max);
    println!(
        "
  %          implementation shortfall vs the mid: Z received / Z the same order would
             get entirely at the mid − 1. -0.50 % = 0.50 % less Z than at the mid, lost to
             pool fees and price impact. The Cardano tx fee is not counted.
  mid A→Z    market rate before fees and price impact, Z tokens per A token, read from
             the pools' state, over paths of ≤ {MID_MAX_HOPS} hops: all paths A→Z give the same
             mid within {:.1e}
  mainnet    best route whose tx fits {}
  raised     best route whose tx fits {}
  no limit   best route, whatever its tx
  pools      number of pools used by that route
  mainnet→raised $
             extra Z received under raised vs mainnet, valued in USD at Z's reference
             price: an ESTIMATE (the reference price is not exact), unlike the %

  market     {market_path}
  scripts    {blueprint_path}
  time       {:.0} s",
        mid_spread,
        args.mainnet_limit,
        args.raised_limit,
        t0.elapsed().as_secs_f64()
    );
    for (e, n) in &errors {
        println!("failed ×{n}: {e}");
    }

    if let Some(path) = &args.csv {
        std::fs::write(path, csv).unwrap_or_else(|e| panic!("{path}: {e}"));
        println!("csv: {path}");
    }
}
