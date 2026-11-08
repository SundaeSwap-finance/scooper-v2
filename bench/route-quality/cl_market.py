"""Extrapolated sundae-v4 concentrated-liquidity market for the route-quality benchmark.

    python3 cl_market.py [--tvl-usd 350e6] [--volatile-sigma 0.10] [--stable-sigma 0.005]
                         [--cut 2.5] [--bands 3,4,5] [--spread-factor 2]
                         [--min-band-ada 10000] [--volatile-fee 0.003 --stable-fee 0.0005]
                         [--snapshot-fees]                      -> data/market-v4-extrapolated.json

Models what v4 will enable, not what is live today: Cardano TVL at 350M$, deployed in
concentrated-liquidity (CL) pools. In sundae-v4 a CL pool is a ladder of adjacent price
bands (banded CL, ADR-0014), whose bands the price walks through inside the pool, one
transcript step per band edge crossed. Each mainnet pool of the snapshot becomes one such
pool: today's fragmentation over venues is kept.

Built on top of the mainnet snapshot (data/market-mainnet-selected.json, see snapshot.py):
  1. STRUCTURE  same tokens, pools, reference prices: one v4 pool per snapshot pool.
                Fee = one tier per pair class (--volatile-fee / --stable-fee,
                Uniswap-v3-like standard tiers), since mainnet fees (e.g. Minswap v2
                dynamic fees up to 1.75 % on ADA/USDCx) say little about a CL market;
                --snapshot-fees keeps each pool's snapshot fee instead.
  2. TVL        the whole --tvl-usd is spread over the snapshot's pools, in proportion to
                their snapshot TVL.
  3. LADDER     every pool has its own band structure, drawn at random from the pool's
                id (so the file is reproducible): a band count among --bands (sundae-v4's
                guidance: "prefer 3 to 5 bands", docs/design/cl-range-strategy.md), a
                spread s = sigma times a factor log-uniform in [1/f, f] (f =
                --spread-factor), and bands of equal log-price width (geometric) over
                +-cut*s, shifted by a random fraction of a band: the price sits anywhere
                in band 0, not at its centre, as in block-capacity/dataset_params.py.
                Pools of one pair thus differ in band count, width and edges. Stable
                pairs (both assets stablecoins) use the stable sigma, the others the
                volatile one.
  4. SPREAD     a band's share of the pool's TVL (valued at the reference price) follows a
                Gaussian in log-price of std s around the reference price, i.e. the
                assumed spread of the pool's LP ranges. Bands below --min-band-ada
                dropped.

Each band is listed with its liquidity L and reserves at the reference price, Uniswap-v3
formulas (route_quality.rs gathers a ladder's bands, same `ladder`, into one banded pool,
L as each band's weight),
price P = raw units of b per raw unit of a (the sundae-v4 CL convention: virtual reserves
a + L/sqrt(pb), b + L*sqrt(pa), invariant L^2). All pools sit at the reference price, so the
market has no arbitrage by construction."""
import argparse, json, math, os, random
from pathlib import Path

HERE = Path(__file__).parent
STABLES = {"USDCx", "USDM", "DJED", "USDA", "iUSD"}

ap = argparse.ArgumentParser()
ap.add_argument("--snapshot", default=HERE / "data" / "market-mainnet-selected.json")
ap.add_argument("--out", default=HERE / "data" / "market-v4-extrapolated.json")
ap.add_argument("--tvl-usd", type=float, default=350e6)
ap.add_argument("--volatile-sigma", type=float, default=0.10, help="std of log-price spread")
ap.add_argument("--stable-sigma", type=float, default=0.005)
ap.add_argument("--cut", type=float, default=2.5, help="ladder extent, in sigmas")
ap.add_argument("--bands", type=lambda v: [int(n) for n in v.split(",")], default=[3, 4, 5],
                help="band counts a ladder draws from, comma-separated")
ap.add_argument("--spread-factor", type=float, default=2.0,
                help="a ladder's spread is sigma times a factor log-uniform in [1/f, f]")
ap.add_argument("--min-band-ada", type=float, default=10_000)
ap.add_argument("--volatile-fee", type=float, default=0.003)
ap.add_argument("--stable-fee", type=float, default=0.0005)
ap.add_argument("--snapshot-fees", action="store_true")
args = ap.parse_args()

snap = json.load(open(args.snapshot))
# Paths are recorded relative to this folder, so the committed file holds no local path.
rel = lambda p: os.path.relpath(Path(p).resolve(), HERE.resolve())
tick, price, ada_usd = snap["ticker"], snap["price"], snap["ada_usd"]
tvl_ada = args.tvl_usd / ada_usd

# 1. STRUCTURE + 2. TVL
snap_tvl = sum(p["tvl_ada"] for p in snap["pools"])


# The pool's sqrt prices are written as exact rationals num / SQRT_DEN, and its L is the
# exact liquidity its integer reserves support at those prices: the formula of the
# scooper's `swap_math::cl_fee_budget` (with no LP after), in integers. The file is then
# consistent as is: the scooper never sees a total_lp above what the reserves support
# (it would exclude the pool as "in deficit").
SQRT_DEN = 10**12


def supported_liquidity(ra, rb, spa_num, spa_den, spb_num, spb_den):
    c = spb_num * spa_den - spb_den * spa_num
    b_ = ra * spb_num * spa_num + rb * spb_den * spa_den
    a_ = ra * rb * spb_num * spa_den
    return (b_ + math.isqrt(b_ * b_ + 4 * a_ * c)) // (2 * c)


def within_saturation(ra, rb, spa_num, spa_den, spb_num, spb_den):
    """Caps an out-of-range band's one reserve at what the banded module allows.

    A full band holds at most its saturation for its liquidity L (`banded_math` a_sat /
    b_sat, rounded up), else no counter witnesses the reserves and the scooper rejects
    the pool. Truncating the float reserves can leave a raw unit above it."""
    L = supported_liquidity(ra, rb, spa_num, spa_den, spb_num, spb_den)
    d = spb_num * spa_den - spa_num * spb_den
    if rb == 0:
        ra = min(ra, -(-L * d // (spa_num * spb_num)))
    elif ra == 0:
        rb = min(rb, -(-L * d // (spa_den * spb_den)))
    return ra, rb


def band_value(L, P, pa, pb, a, b):
    """Reserves (raw a, raw b) and value (lovelace) of liquidity L on [pa, pb] at price P."""
    sp, sa, sb = math.sqrt(min(max(P, pa), pb)), math.sqrt(pa), math.sqrt(pb)
    ra, rb = L * (1 / sp - 1 / sb), L * (sp - sa)
    return ra, rb, ra * price[a] + rb * price[b]


pools, summary = [], []
for snap_pool in snap["pools"]:
    a, b = snap_pool["a"], snap_pool["b"]
    stable = tick[a] in STABLES and tick[b] in STABLES
    rng = random.Random(snap_pool["id"])
    sigma = args.stable_sigma if stable else args.volatile_sigma
    spread = sigma * args.spread_factor ** rng.uniform(-1, 1)
    band_count = rng.choice(args.bands)
    if args.snapshot_fees:
        fee, fee_source = snap_pool["fee"], "the snapshot pool's fee"
    else:
        fee = args.stable_fee if stable else args.volatile_fee
        fee_source = f"{'stable' if stable else 'volatile'} tier (parameter)"
    pool_tvl = tvl_ada * snap_pool["tvl_ada"] / snap_tvl
    P = price[a] / price[b]  # raw b per raw a

    # 3. LADDER: band k covers log-price [(k-1/2+shift)h, (k+1/2+shift)h] around P, k from
    # -(band_count // 2), band 0 holding the price
    h, lowest = 2 * args.cut * spread / band_count, -(band_count // 2)
    shift = rng.random() - 0.5
    edge = lambda k: (k + shift - 0.5) * h
    width = math.expm1(h)
    # 4. SPREAD: Gaussian mass of each band, renormalised over the ladder
    cdf = lambda x: 0.5 * (1 + math.erf(x / (spread * math.sqrt(2))))
    mass = {k: cdf(edge(k + 1)) - cdf(edge(k)) for k in range(lowest, lowest + band_count)}
    total = sum(mass.values())
    kept = 0
    for k, m in mass.items():
        value_ada = pool_tvl * m / total
        if value_ada < args.min_band_ada:
            continue
        pa, pb = P * math.exp(edge(k)), P * math.exp(edge(k + 1))
        _, _, v1 = band_value(1.0, P, pa, pb, a, b)  # lovelace per unit of L
        L = value_ada * 1e6 / v1
        ra, rb, _ = band_value(L, P, pa, pb, a, b)
        spa, spb = round(math.sqrt(pa) * SQRT_DEN), round(math.sqrt(pb) * SQRT_DEN)
        ra, rb = within_saturation(int(ra), int(rb), spa, SQRT_DEN, spb, SQRT_DEN)
        pools.append(dict(id=f"cl:{tick[a]}/{tick[b]}:{snap_pool['venue']}:{k:+d}",
                          ladder=snap_pool["id"], venue="sundae-v4-cl", a=a, b=b,
                          ra=ra, rb=rb, tvl_ada=value_ada, fee=fee,
                          fee_source=fee_source,
                          curve="concentrated_liquidity", band=k, pa=pa, pb=pb,
                          sqrt_pa=[spa, SQRT_DEN], sqrt_pb=[spb, SQRT_DEN],
                          L=supported_liquidity(ra, rb, spa, SQRT_DEN, spb, SQRT_DEN)))
        kept += 1
    summary.append((f"{tick[a]}/{tick[b]} {snap_pool['venue']}", "stable" if stable else "volatile",
                    pool_tvl, kept, width, fee, max(mass.values()) / total * pool_tvl))

print(f"CL market: {args.tvl_usd / 1e6:.0f}M$ = {tvl_ada / 1e6:.0f}M ADA (ADA ≈ {ada_usd:.4f} $) "
      f"over {len(summary)} pools, {len(pools)} bands")
print(f"  volatile: sigma {args.volatile_sigma:.1%}; stable: sigma {args.stable_sigma:.2%}; "
      f"ladders of {'/'.join(map(str, args.bands))} bands over ±{args.cut} spreads, spread = "
      f"sigma × [1/{args.spread_factor:g}, {args.spread_factor:g}], bands ≥ {args.min_band_ada:,.0f} ADA\n")
print(f"  {'pool':26} {'class':8} {'TVL (M$)':>9} {'bands':>5} {'width':>6} {'fee':>6} {'centre band (M$)':>17}")
for name, cls, tvl, kept, width, fee, centre in sorted(summary, key=lambda s: -s[2]):
    print(f"  {name:26} {cls:8} {tvl * ada_usd / 1e6:9.1f} {kept:5} {width:6.2%} {fee:6.2%} "
          f"{centre * ada_usd / 1e6:17.2f}")

json.dump(dict(
    source=dict(snapshot=rel(args.snapshot), snapshot_block=snap["source"]["as_of"]["height"]),
    params={k: rel(v) if isinstance(v, (Path, str)) else v for k, v in vars(args).items()},
    model="projected sundae-v4 CL market: one banded pool (ladder) per snapshot pool, "
          "each with its own random band count, spread and band edges, Gaussian "
          "log-price spread, all pools at the reference price",
    ada_usd=ada_usd, price=price, ticker=tick, decimals=snap["decimals"], pools=pools,
), open(args.out, "w"), indent=1)
print(f"-> {args.out}")
