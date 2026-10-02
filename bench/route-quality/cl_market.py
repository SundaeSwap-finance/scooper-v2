"""Extrapolated sundae-v4 concentrated-liquidity market for the route-quality benchmark.

    python3 cl_market.py [--tvl-usd 350e6] [--volatile-width 0.02 --volatile-sigma 0.10]
                         [--stable-width 0.001 --stable-sigma 0.005] [--cut 2.5]
                         [--min-band-ada 10000] [--volatile-fee 0.003 --stable-fee 0.0005]
                         [--snapshot-fees]                      -> data/market-v4-extrapolated.json

Models what v4 will enable, not what is live today: Cardano TVL at 350M$, deployed in
concentrated-liquidity (CL) pools. In sundae-v4 a CL pool has a single price range; every
distinct range is its own pool and the router splits an order across pools. Here each pair
gets a DISJOINT LADDER of adjacent price bands (one pool per band): an LP wanting a wider
range deposits into several adjacent bands. A large order therefore crosses one pool per
band it moves the price through, so K (pools per tx) bounds how far it can walk the book.

Built on top of the mainnet snapshot (data/market-mainnet-selected.json, see snapshot.py):
  1. STRUCTURE  same tokens, pairs, reference prices. Fee = one tier per pair class
                (--volatile-fee / --stable-fee, Uniswap-v3-like standard tiers), since
                mainnet fees (e.g. Minswap v2 dynamic fees up to 1.75 % on ADA/USDCx) say
                little about a CL market; --snapshot-fees uses the TVL-weighted median
                of the pair's snapshot fees instead.
  2. TVL        the whole --tvl-usd is spread over the snapshot's pairs, in proportion to
                their snapshot TVL.
  3. LADDER     per pair, bands of relative width `width` (geometric), one centred on the
                reference price, out to +-cut*sigma. Stable pairs (both assets stablecoins)
                use the stable width/sigma, the others the volatile ones.
  4. SPREAD     a band's share of the pair TVL (valued at the reference price) follows a
                Gaussian in log-price of std `sigma` around the reference price, i.e. the
                assumed "typical spread" of LP ranges. Bands below --min-band-ada dropped.

Each band -> one pool: liquidity L and reserves at the reference price, Uniswap-v3 formulas,
price P = raw units of b per raw unit of a (the sundae-v4 CL convention: virtual reserves
a + L/sqrt(pb), b + L*sqrt(pa), invariant L^2). All pools sit at the reference price, so the
market has no arbitrage by construction."""
import argparse, json, math, os
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).parent
STABLES = {"USDCx", "USDM", "DJED", "USDA", "iUSD"}

ap = argparse.ArgumentParser()
ap.add_argument("--snapshot", default=HERE / "data" / "market-mainnet-selected.json")
ap.add_argument("--out", default=HERE / "data" / "market-v4-extrapolated.json")
ap.add_argument("--tvl-usd", type=float, default=350e6)
ap.add_argument("--volatile-width", type=float, default=0.02, help="relative band width")
ap.add_argument("--volatile-sigma", type=float, default=0.10, help="std of log-price spread")
ap.add_argument("--stable-width", type=float, default=0.001)
ap.add_argument("--stable-sigma", type=float, default=0.005)
ap.add_argument("--cut", type=float, default=2.5, help="ladder extent, in sigmas")
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
pairs = defaultdict(list)
for p in snap["pools"]:
    pairs[(p["a"], p["b"])].append(p)
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


def band_value(L, P, pa, pb, a, b):
    """Reserves (raw a, raw b) and value (lovelace) of liquidity L on [pa, pb] at price P."""
    sp, sa, sb = math.sqrt(min(max(P, pa), pb)), math.sqrt(pa), math.sqrt(pb)
    ra, rb = L * (1 / sp - 1 / sb), L * (sp - sa)
    return ra, rb, ra * price[a] + rb * price[b]


pools, summary = [], []
for (a, b), group in pairs.items():
    stable = tick[a] in STABLES and tick[b] in STABLES
    width = args.stable_width if stable else args.volatile_width
    sigma = args.stable_sigma if stable else args.volatile_sigma
    if args.snapshot_fees:  # TVL-weighted median of the pair's snapshot fees
        acc, fees = 0.0, sorted((p["fee"], p["tvl_ada"]) for p in group)
        fee = next(f for f, w in fees if (acc := acc + w) >= sum(w for _, w in fees) / 2)
        fee_source = "TVL-weighted median of the pair's snapshot fees"
    else:
        fee = args.stable_fee if stable else args.volatile_fee
        fee_source = f"{'stable' if stable else 'volatile'} tier (parameter)"
    pair_tvl = tvl_ada * sum(p["tvl_ada"] for p in group) / snap_tvl
    P = price[a] / price[b]  # raw b per raw a

    # 3. LADDER: band k covers log-price [(k-1/2)h, (k+1/2)h] around P
    h = math.log1p(width)
    n = int(math.ceil(args.cut * sigma / h))
    # 4. SPREAD: Gaussian mass of each band, renormalised over the ladder
    cdf = lambda x: 0.5 * (1 + math.erf(x / (sigma * math.sqrt(2))))
    mass = {k: cdf((k + 0.5) * h) - cdf((k - 0.5) * h) for k in range(-n, n + 1)}
    total = sum(mass.values())
    kept = 0
    for k, m in mass.items():
        value_ada = pair_tvl * m / total
        if value_ada < args.min_band_ada:
            continue
        pa, pb = P * math.exp((k - 0.5) * h), P * math.exp((k + 0.5) * h)
        _, _, v1 = band_value(1.0, P, pa, pb, a, b)  # lovelace per unit of L
        L = value_ada * 1e6 / v1
        ra, rb, _ = band_value(L, P, pa, pb, a, b)
        ra, rb = int(ra), int(rb)
        spa, spb = round(math.sqrt(pa) * SQRT_DEN), round(math.sqrt(pb) * SQRT_DEN)
        pools.append(dict(id=f"cl:{tick[a]}/{tick[b]}:{k:+d}", venue="sundae-v4-cl", a=a, b=b,
                          ra=ra, rb=rb, tvl_ada=value_ada, fee=fee,
                          fee_source=fee_source,
                          curve="concentrated_liquidity", band=k, pa=pa, pb=pb,
                          sqrt_pa=[spa, SQRT_DEN], sqrt_pb=[spb, SQRT_DEN],
                          L=supported_liquidity(ra, rb, spa, SQRT_DEN, spb, SQRT_DEN)))
        kept += 1
    summary.append((tick[a] + "/" + tick[b], "stable" if stable else "volatile", pair_tvl,
                    kept, width, fee, max(mass.values()) / total * pair_tvl))

print(f"CL market: {args.tvl_usd / 1e6:.0f}M$ = {tvl_ada / 1e6:.0f}M ADA (ADA ≈ {ada_usd:.4f} $) "
      f"over {len(summary)} pairs, {len(pools)} pools")
print(f"  volatile: width {args.volatile_width:.2%}, sigma {args.volatile_sigma:.1%}; "
      f"stable: width {args.stable_width:.2%}, sigma {args.stable_sigma:.2%}; ladder ±{args.cut}σ, "
      f"bands ≥ {args.min_band_ada:,.0f} ADA\n")
print(f"  {'pair':12} {'class':8} {'TVL (M$)':>9} {'bands':>5} {'width':>6} {'fee':>6} {'centre band (M$)':>17}")
for name, cls, tvl, kept, width, fee, centre in sorted(summary, key=lambda s: -s[2]):
    print(f"  {name:12} {cls:8} {tvl * ada_usd / 1e6:9.1f} {kept:5} {width:6.2%} {fee:6.2%} "
          f"{centre * ada_usd / 1e6:17.2f}")

json.dump(dict(
    source=dict(snapshot=rel(args.snapshot), snapshot_block=snap["source"]["as_of"]["height"]),
    params={k: rel(v) if isinstance(v, (Path, str)) else v for k, v in vars(args).items()},
    model="projected sundae-v4 CL market: per pair a disjoint ladder of single-range CL pools, "
          "Gaussian log-price spread, all pools at the reference price",
    ada_usd=ada_usd, price=price, ticker=tick, decimals=snap["decimals"], pools=pools,
), open(args.out, "w"), indent=1)
print(f"-> {args.out}")
