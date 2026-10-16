//! Criterion benchmark of `find_blended_route` on a synthetic banded market,
//! at the pool caps K the route-quality benchmark uses. Ported from
//! scooper-v2 PR #99, whose market was a ladder of single-range CL pools per
//! pair; the scooper now prices a pair's ladder as one banded pool, so each
//! pair here is one `BandedConcentratedLiquidity` pool whose bands carry the
//! same Gaussian spread of value.
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
use crate::sundaev4::banded_math::{build_index, check_shape, find_witness};
use crate::sundaev4::router::{RoutingLimits, find_blended_route};
use crate::sundaev4::{BandSpec, BandedCLConfig, PoolDatum, PoolType, Rational, SundaeV4Pool, plutus_void};

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

fn sqrt_price(s: f64) -> Rational {
    Rational {
        num: BigInt::from((s * SQRT_DEN as f64).round() as u64),
        den: BigInt::from(SQRT_DEN),
    }
}

/// The pair's ladder: `2·n + 1` bands of geometric width around price 1, each
/// weighted by the Gaussian mass of its log-price (weights are integers, so
/// the mass is scaled to a thousand and floored at one). Band `k` covers
/// log-price `[(k - 1/2)·h, (k + 1/2)·h]`; `closing` is the last band's upper
/// edge.
fn ladder(shape: &MarketShape) -> BandedCLConfig {
    let h = shape.width.ln_1p();
    let ks: Vec<i32> = (-shape.bands_per_side..=shape.bands_per_side).collect();
    let mass = |k: i32| (-(k as f64 * h).powi(2) / (2.0 * shape.sigma.powi(2))).exp();
    let fee = Rational {
        num: BigInt::from(shape.fee_ppm),
        den: BigInt::from(1_000_000u64),
    };
    let bands: Vec<BandSpec> = ks
        .iter()
        .map(|&k| BandSpec {
            start: sqrt_price(((k as f64 - 0.5) * h).exp().sqrt()),
            weight: BigInt::from(((mass(k) * 1000.0) as u64).max(1)),
            curve: BigInt::from(0),
            fee_buy: fee.clone(),
            fee_sell: fee.clone(),
        })
        .collect();
    let weight_total = bands.iter().fold(BigInt::from(0), |acc, b| &acc + &b.weight);
    let last = *ks.last().expect("at least the centre band");
    let mut cfg = BandedCLConfig {
        bands,
        index: vec![],
        closing: sqrt_price(((last as f64 + 0.5) * h).exp().sqrt()),
        weight_total,
    };
    cfg.index = build_index(&cfg);
    check_shape(&cfg).expect("the synthetic ladder passes the module's shape checks");
    cfg
}

/// One banded pool for the pair (a, b), holding `value` split evenly at price
/// 1. The witness search must place the reserves on the ladder, as the
/// router's own ladder view will.
fn banded_pool(index: usize, a: &AssetClass, b: &AssetClass, shape: &MarketShape) -> SundaeV4Pool {
    let cfg = ladder(shape);
    let half = BigInt::from((shape.pair_value / 2.0) as u64);
    find_witness(&cfg, &half, &half).expect("balanced reserves sit on the ladder");
    let mut ident = vec![0xc1; 26];
    ident.extend_from_slice(&u16::try_from(index).expect("≤ 65535 pools").to_be_bytes());
    let mut tx_hash = [0u8; 32];
    tx_hash[..28].copy_from_slice(&ident);
    let lp = &half + &half;
    SundaeV4Pool {
        input: TransactionInput::new(tx_hash.into(), 0),
        // Routing reads only the datum and the pool type.
        address: vec![],
        value: Value::default(),
        pool_datum: PoolDatum {
            assets: vec![(a.clone(), half.clone()), (b.clone(), half)],
            total_lp: lp.clone(),
            circulating_lp: BigInt::from(0),
            preminted_lp: lp,
            identifier: Ident::new(&ident),
            actions: vec![],
            module_state: vec![],
            min_surplus: BigInt::from(0),
            extension: plutus_void(),
        },
        pool_type: PoolType::BandedConcentratedLiquidity { config: cfg },
        slot: 100,
        fee_split_config: None,
    }
}

/// Every pair (i < j) gets one banded pool, oriented (T_i, T_j).
fn market(shape: &MarketShape) -> BTreeMap<Ident, Arc<SundaeV4Pool>> {
    let mut pools = BTreeMap::new();
    for i in 0..shape.tokens {
        for j in i + 1..shape.tokens {
            add_pool(&mut pools, &token(i), &token(j), shape);
        }
    }
    pools
}

fn add_pool(
    pools: &mut BTreeMap<Ident, Arc<SundaeV4Pool>>,
    a: &AssetClass,
    b: &AssetClass,
    shape: &MarketShape,
) {
    let pool = banded_pool(pools.len(), a, b, shape);
    pools.insert(pool.pool_datum.identifier.clone(), Arc::new(pool));
}

/// The order `crowded` routes: 1e12 of T0 for T1, about 1.6 times what the
/// four thin paths can absorb together.
const CROWDED_AMOUNT: u64 = 1_000_000_000_000;

/// First pool index of `crowded_market`. The router caches a banded pool's
/// ladder by (ident, reserves), not by config: a `crowded` pool with the
/// ident and reserves of a pool of the main market, routed first, would be
/// priced with that pool's ladder (its 0.3 % fee instead of the deep pool's).
const CROWDED_FIRST_INDEX: usize = 1000;

/// A market where the best paths on a small amount lack the depth for the
/// order. Fee tiers only, every pool at price 1: a deep T0/T1 pool on the 1 %
/// tier, and four T0→Tᵢ→T1 paths through thin pools on the 0.05 % tier. On one
/// chunk the four thin paths outrank the deep one (rate 0.9927 vs 0.9899) and
/// take every branch slot, yet together they absorb only 0.62e12 of the 1e12
/// order: the router must give the deep path a slot back and water-fill a
/// second time (PR #102), the case this times. With the thin pools' sigma at
/// 0.10 instead of 0.05, the deep path keeps its slot and the case is gone.
fn crowded_market() -> BTreeMap<Ident, Arc<SundaeV4Pool>> {
    let deep = MarketShape {
        tokens: 2,
        bands_per_side: 6,
        width: 0.02,
        sigma: 0.10,
        pair_value: 10e12,
        fee_ppm: 10_000,
    };
    let thin = MarketShape {
        tokens: 2,
        bands_per_side: 6,
        width: 0.02,
        sigma: 0.05,
        pair_value: 0.3e12,
        fee_ppm: 500,
    };
    let mut pairs = vec![(token(0), token(1), &deep)];
    for intermediate in 2..6 {
        pairs.push((token(0), token(intermediate), &thin));
        pairs.push((token(intermediate), token(1), &thin));
    }
    let mut pools = BTreeMap::new();
    for (a, b, shape) in pairs {
        let pool = banded_pool(CROWDED_FIRST_INDEX + pools.len(), &a, &b, shape);
        pools.insert(pool.pool_datum.identifier.clone(), Arc::new(pool));
    }
    pools
}

fn summary(blend: &Option<crate::sundaev4::router::BlendedRoute>) -> String {
    match blend {
        Some(b) => {
            let splits: usize =
                b.branches.iter().flat_map(|r| &r.hops).map(|h| h.splits.len()).sum();
            format!("{} branches, {splits} splits, output {}", b.branches.len(), b.total_output)
        }
        None => "no route".to_string(),
    }
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
    // 10 % of a pair's value.
    let amount = BigInt::from(1_000_000_000_000u64);

    let cases = [
        ("unlimited", RoutingLimits::unlimited()),
        ("K=15", RoutingLimits { max_pools: 15, max_steps: 15 }),
        ("K=4", RoutingLimits { max_pools: 4, max_steps: 4 }),
        ("K=3", RoutingLimits { max_pools: 3, max_steps: 3 }),
    ];

    // What each case routes to, so a change of route shows next to a change
    // of time.
    eprintln!("{} pools, T0 → T{}, amount {amount}", pools.len(), shape.tokens - 1);
    for (name, limits) in &cases {
        let t0 = std::time::Instant::now();
        let blend = find_blended_route(&pools, &[], &from, &to, &amount, *limits);
        eprintln!("  {name:<9} {:>8.2?}  {}", t0.elapsed(), summary(&blend));
    }
    let crowded = crowded_market();
    let crowded_amount = BigInt::from(CROWDED_AMOUNT);
    eprintln!("{} pools, T0 → T1, amount {crowded_amount}", crowded.len());
    let t0 = std::time::Instant::now();
    let blend =
        find_blended_route(&crowded, &[], &token(0), &token(1), &crowded_amount, RoutingLimits::unlimited());
    eprintln!("  {:<9} {:>8.2?}  {}", "crowded", t0.elapsed(), summary(&blend));

    let mut group = c.benchmark_group("find_blended_route");
    // The slow cases take seconds per call: criterion's minimum sample count.
    group.sample_size(10);
    for (name, limits) in cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), &limits, |bench, limits| {
            bench.iter(|| {
                find_blended_route(black_box(&pools), &[], &from, &to, black_box(&amount), *limits)
            })
        });
    }
    group.bench_function("crowded", |bench| {
        bench.iter(|| {
            find_blended_route(
                black_box(&crowded),
                &[],
                &token(0),
                &token(1),
                black_box(&crowded_amount),
                RoutingLimits::unlimited(),
            )
        })
    });
    group.finish();
}

criterion_group!(benches, bench_find_blended_route);
