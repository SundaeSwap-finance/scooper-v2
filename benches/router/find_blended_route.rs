//! Criterion benchmark of `find_blended_route` on a synthetic concentrated-
//! liquidity market, at the pool caps K the route-quality benchmark uses.
//!
//! The market reproduces the shape that makes routing slow on the projected v4
//! market (`bench/route-quality/data/market-v4-extrapolated.json`): a dense
//! token graph where every pair holds a ladder of adjacent single-range CL
//! bands. Unlimited, the blend spreads over many bands; under a small K it
//! overflows the budget, so the router prunes one pool and re-solves the whole
//! blend, many times over (one band replaced by its neighbour each time).
//!
//! Built as the `router` bench, which recompiles the crate's modules (see
//! `src/router_bench.rs`):
//!   cargo bench --bench router
//!   cargo bench --bench router -- 'K=4'           # one case
//!   cargo bench --bench router -- --save-baseline before
//!   cargo bench --bench router -- --baseline before

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group};

use crate::bigint::BigInt;
use crate::cardano_types::{AssetClass, TransactionInput, Value};
use crate::sundaev3::Ident;
use crate::sundaev4::router::{RoutingLimits, find_blended_route};
use crate::sundaev4::swap_math::cl_fee_budget;
use crate::sundaev4::{PoolDatum, PoolType, Rational, SundaeV4Pool, plutus_void};

/// Shape of the synthetic market. Every token has the same reference price
/// (1 raw unit = 1 raw unit), so every pool sits at price 1 and the market has
/// no arbitrage.
struct MarketShape {
    /// Tokens, all pairwise connected.
    tokens: usize,
    /// Bands on each side of the centre band, per pair (2·n + 1 bands).
    bands_per_side: i32,
    /// Relative width of a band (geometric).
    width: f64,
    /// Std of the Gaussian log-price spread of the pair's value over its bands.
    sigma: f64,
    /// Value of each pair, in raw units at price 1.
    pair_value: f64,
    /// Fee, in millionths.
    fee_ppm: u64,
}

/// Fixed-point denominator of the bands' sqrt prices.
const SQRT_DEN: u64 = 1_000_000_000_000;

fn token(i: usize) -> AssetClass {
    AssetClass {
        policy: vec![0xa0 + i as u8; 28],
        token: format!("T{i}").into_bytes(),
    }
}

/// One single-range CL pool: band `k` of the ladder, covering log-price
/// `[(k - 1/2)·h, (k + 1/2)·h]` around 1, holding `value` at price 1. Its
/// total_lp is the exact liquidity its integer reserves support (the formula
/// of `cl_fee_budget` with no LP after), as `cl_market.py` writes it, so the
/// router never sees the pool as in deficit.
fn cl_band(
    index: usize,
    a: &AssetClass,
    b: &AssetClass,
    k: i32,
    value: f64,
    shape: &MarketShape,
) -> SundaeV4Pool {
    let h = shape.width.ln_1p();
    let (pa, pb) = (((k as f64 - 0.5) * h).exp(), ((k as f64 + 0.5) * h).exp());
    let (sa, sb) = (pa.sqrt(), pb.sqrt());
    let sp = 1f64.clamp(sa, sb);
    // Reserves of liquidity 1 at price 1, then scaled to the band's value.
    let (ra1, rb1) = (1.0 / sp - 1.0 / sb, sp - sa);
    let l = value / (ra1 + rb1);
    let (ra, rb) = (
        BigInt::from((l * ra1) as u64),
        BigInt::from((l * rb1) as u64),
    );
    let sqrt_price = |s: f64| Rational {
        num: BigInt::from((s * SQRT_DEN as f64).round() as u64),
        den: BigInt::from(SQRT_DEN),
    };
    let (spa, spb) = (sqrt_price(sa), sqrt_price(sb));
    let zero = BigInt::from(0);
    let lp = cl_fee_budget(&ra, &rb, &zero, &spa.num, &spa.den, &spb.num, &spb.den);

    let mut ident = vec![0xc1; 26];
    ident.extend_from_slice(&u16::try_from(index).expect("≤ 65535 pools").to_be_bytes());
    let mut tx_hash = [0u8; 32];
    tx_hash[..28].copy_from_slice(&ident);
    SundaeV4Pool {
        input: TransactionInput::new(tx_hash.into(), 0),
        // Routing reads only the datum and the pool type.
        address: vec![],
        value: Value::default(),
        pool_datum: PoolDatum {
            assets: vec![(a.clone(), ra), (b.clone(), rb)],
            total_lp: lp.clone(),
            circulating_lp: BigInt::from(0),
            preminted_lp: lp,
            identifier: Ident::new(&ident),
            actions: vec![],
            module_state: vec![],
            min_surplus: BigInt::from(0),
            extension: plutus_void(),
        },
        pool_type: PoolType::ConcentratedLiquidity {
            sqrt_price_a: spa,
            sqrt_price_b: spb,
            fee: Rational {
                num: BigInt::from(shape.fee_ppm),
                den: BigInt::from(1_000_000u64),
            },
        },
        slot: 100,
        fee_split_config: None,
    }
}

/// Every pair (i < j) gets the ladder, oriented (T_i, T_j): a trade from T_0
/// up to T_last sells each pool's first asset on its main paths (see the CL
/// contract workaround in `bench/route-quality/route_quality.rs`).
fn market(shape: &MarketShape) -> BTreeMap<Ident, Arc<SundaeV4Pool>> {
    let h = shape.width.ln_1p();
    let ks = -shape.bands_per_side..=shape.bands_per_side;
    let mass = |k: i32| (-(k as f64 * h).powi(2) / (2.0 * shape.sigma.powi(2))).exp();
    let total: f64 = ks.clone().map(mass).sum();
    let mut pools = BTreeMap::new();
    for i in 0..shape.tokens {
        for j in i + 1..shape.tokens {
            for k in ks.clone() {
                let value = shape.pair_value * mass(k) / total;
                let pool = cl_band(pools.len(), &token(i), &token(j), k, value, shape);
                pools.insert(pool.pool_datum.identifier.clone(), Arc::new(pool));
            }
        }
    }
    pools
}

/// A market where the best paths on a small amount lack the depth for the
/// order: four T0→Tᵢ→T1 paths, each through two single cheap bands (0.05 %
/// fee), outrank the deep but costly T0/T1 ladder (3 % fee) and take every
/// branch slot, though together they hold far less than the order. The
/// router then gives the last slot to the T0/T1 path, the one that can absorb
/// the whole order, and water-fills again.
fn crowded_market() -> BTreeMap<Ident, Arc<SundaeV4Pool>> {
    let deep = MarketShape {
        tokens: 2,
        bands_per_side: 6,
        width: 0.02,
        sigma: 0.10,
        pair_value: 10e12,
        fee_ppm: 30_000,
    };
    let shallow = MarketShape {
        tokens: 2,
        bands_per_side: 0,
        width: 0.02,
        sigma: 0.10,
        pair_value: 0.4e12,
        fee_ppm: 500,
    };
    let mut pools = market(&deep);
    for i in 2..6 {
        for (a, b) in [(token(0), token(i)), (token(i), token(1))] {
            let pool = cl_band(pools.len(), &a, &b, 0, shallow.pair_value, &shallow);
            pools.insert(pool.pool_datum.identifier.clone(), Arc::new(pool));
        }
    }
    pools
}

fn bench_find_blended_route(c: &mut Criterion) {
    let shape = MarketShape {
        tokens: 4,
        bands_per_side: 6,
        width: 0.02,
        sigma: 0.10,
        pair_value: 10e12,
        fee_ppm: 3_000,
    };
    let pools = market(&shape);
    let (from, to) = (token(0), token(shape.tokens - 1));
    // 10 % of a pair's value: the unlimited blend spreads over 3 paths and
    // 6 bands; K = 3 or 4 sends the router through ~20 rounds of pruning.
    let amount = BigInt::from(1_000_000_000_000u64);

    let cases = [
        ("unlimited", RoutingLimits::unlimited()),
        (
            "K=15",
            RoutingLimits {
                max_pools: 15,
                max_steps: 15,
            },
        ),
        (
            "K=4",
            RoutingLimits {
                max_pools: 4,
                max_steps: 4,
            },
        ),
        (
            "K=3",
            RoutingLimits {
                max_pools: 3,
                max_steps: 3,
            },
        ),
    ];

    // What each case routes to, so a change of route shows next to a change
    // of time.
    eprintln!(
        "{} pools, T0 → T{}, amount {amount}",
        pools.len(),
        shape.tokens - 1
    );
    for (name, limits) in &cases {
        let t0 = std::time::Instant::now();
        let blend = find_blended_route(&pools, &[], &from, &to, &amount, *limits);
        let elapsed = t0.elapsed();
        let summary = match &blend {
            Some(b) => {
                let splits: usize =
                    b.branches.iter().flat_map(|r| &r.hops).map(|h| h.splits.len()).sum();
                format!(
                    "{} branches, {splits} splits, output {}",
                    b.branches.len(),
                    b.total_output
                )
            }
            None => "no route".to_string(),
        };
        eprintln!("  {name:<9} {elapsed:>8.2?}  {summary}");
    }

    let crowded = crowded_market();
    let t0 = std::time::Instant::now();
    let blend = find_blended_route(
        &crowded,
        &[],
        &token(0),
        &token(1),
        &amount,
        RoutingLimits::unlimited(),
    );
    let elapsed = t0.elapsed();
    let summary = match &blend {
        Some(b) => format!("{} branches, output {}", b.branches.len(), b.total_output),
        None => "no route".to_string(),
    };
    eprintln!("  {:<9} {elapsed:>8.2?}  {summary}", "crowded");

    let mut group = c.benchmark_group("find_blended_route");
    // The slow cases take seconds per call: criterion's minimum sample count.
    group.sample_size(10);
    for (name, limits) in cases {
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &limits,
            |bench, limits| {
                bench.iter(|| {
                    find_blended_route(
                        black_box(&pools),
                        &[],
                        &from,
                        &to,
                        black_box(&amount),
                        *limits,
                    )
                })
            },
        );
    }
    group.bench_function("crowded", |bench| {
        bench.iter(|| {
            find_blended_route(
                black_box(&crowded),
                &[],
                &token(0),
                &token(1),
                black_box(&amount),
                RoutingLimits::unlimited(),
            )
        })
    });
    group.finish();
}

criterion_group!(benches, bench_find_blended_route);
