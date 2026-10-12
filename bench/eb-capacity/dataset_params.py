"""Parameters of the block-capacity bench's order queue, measured on the real DEX flow.

    python3 dataset_params.py      (reads data/tape.csv) -> data/dataset-params.json

The bench does not model quantities: only the shape of an order matters, i.e. the pools it
touches and how many band edges it crosses in each. Three parameters describe a queue of
such orders:

  pools_per_order      share of orders touching 1, 2, 3, 4 pools (orders touching more
                       are capped to 4, see MAX_POOLS_PER_ORDER).
  pool_reuse_distance  for each pool an order touches, the distance in orders, along the
                       queue, back to the last order that touched the same pool. Given as
                       its median and quartiles (heavy-tailed: the mean is set by rare
                       pools and by the sample length, so it is not reported).
  bands_per_leg        share of pool legs walking 1, 2, 3+ bands of a banded pool, overall
                       and per pools_per_order.

The queue is the real sequence: txs in slot order, then by hash within a slot (float does
not give the position in the block).

From tape rows to orders:
  1. Per tx, net flow per asset over its rows (`buy` = the user receives the pool's base
     asset and gives the quote asset). An asset is "live" when its net flow exceeds
     LIVE_SHARE of its gross flow; the others are intermediate hops.
  2. A tx with no live input or no live output is an arbitrage cycle: dropped (not user
     orders).
  3. The rows of a tx are split into orders: rows sharing an intermediate asset form a
     path; paths with the same live endpoints form one order (a split over several paths
     or pools).
  4. Each row is mapped to its pool, rows of one order on the same pool merge into one
     leg (their inputs add up), and the order keeps its first 4 legs.
Limits: float nets each pool per tx, so several orders batched on one pool count as one;
two independent orders with the same input and output assets in one tx count as one.

Pool identity: one pool per pair, every venue's liquidity of a pair in one banded pool
(sundae-v4's ladder; route-quality's extrapolated v4 market: one ladder per pair). This is
an assumption: today's pools are fragmented over venues, and keying by venue gives about
22 % less block capacity. A pair is stable when both assets are in STABLECOINS, volatile
otherwise.

Heuristics shared with route-quality (snapshot.py, cl_market.py) are copied from there:
depth venues, one ladder per pair, stable pairs, sigma, fees.

Bands walked by a leg, from the leg's input amount (tape) and its pair's current
liquidity (float /v1/markets reserves, summed over the pair's pools on DEPTH_VENUES, one
snapshot cached in bench/cache/float-markets.json): the price move the amount causes on a
constant-product pool of that liquidity, 2 ln(1 + amount / reserve), cut into bands of
the bench's ladder (BANDS_PER_POOL bands over +-CUT sigma) at a uniform random offset.
This overstates the crossings of a banded pool, where concentrated liquidity moves the
price less: on purpose, crossings are overestimated rather than missed. The pool keeps
its fee (FEE) out of the amount. The distribution over the offset is computed exactly
(no sampling). Legs whose pair has no usable reserve walk 1 band and are counted."""
import csv, json, math, subprocess, sys, time
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).parent
TAPE = HERE / "data" / "tape.csv"
OUT = HERE / "data" / "dataset-params.json"
MARKETS = HERE.parent / "cache" / "float-markets.json"
LIVE_SHARE = 0.02
# One order alone in a sundae-v4 scoop fits ~4 pool legs under mainnet's per-tx limits
# (14M mem / 10G steps, steps bind: see bench/route-quality). An order touching more pools
# could never execute and the scooper would quarantine it; the bench measures block
# capacity, not the scooper, so such orders are capped: they keep their first 4 pools.
MAX_POOLS_PER_ORDER = 4
# Ladder: route-quality/cl_market.py's sigma (--*-sigma) and --cut, but BANDS_PER_POOL
# bands over the +-CUT sigma span (the block bench's pools: sundae-v4's "prefer 3 to 5
# bands"), i.e. bands 2 CUT sigma / BANDS_PER_POOL wide in log-price.
SIGMA = dict(volatile=0.10, stable=0.005)
CUT = 2.5
BANDS_PER_POOL = 5
# bands_per_leg buckets: 1 .. MAX_BANDS, the last one holding every longer walk. From the
# centre band a walk reaches the ladder's last band after BANDS_PER_POOL // 2 crossings.
MAX_BANDS = BANDS_PER_POOL // 2 + 1
# Heuristics shared with route-quality: keep them equal to their source, change both
# together.
# Venues whose pools give the depth: route-quality/snapshot.py's VENUE_CURVE (dropped there:
# sundae-v4, curve unknown; danogo, reserve ratios no CP prices; snek-fun, bonding curves;
# venues it has no curve for). Orders on the other venues stay in the queue and walk the
# pair's depth on these venues.
DEPTH_VENUES = {"minswap-v1", "minswap-v2", "sundae-v1", "sundae-v3", "cswap", "vyfi",
                "minswap-stable", "sundae-stable", "wingriders-v1", "wingriders-v2", "splash"}
# Fee of the ladder per pair class: route-quality/cl_market.py's --volatile-fee / --stable-fee.
FEE = dict(volatile=0.003, stable=0.0005)
# Stable pair = both assets stablecoins: route-quality/cl_market.py's STABLES.
STABLECOINS = {  # USDCx, USDM, DJED, USDA, iUSD (route-quality/snapshot.py's token set)
    "1f3aec8bfe7ea4fe14c5f121e2a92e301afe414147860d557cac7e34.5553444378",
    "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad.0014df105553444d",
    "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61.446a65644d6963726f555344",
    "fe7c786ab321f41c654ef6c1af7b3250a613c24e4213e0425a7ae456.55534441",
    "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880.69555344",
}



def curve_class(a, b):
    return "stable" if {a, b} <= STABLECOINS else "volatile"


def pool_key(a, b):
    # route-quality/cl_market.py: one ladder per pair, every venue's pools of it together.
    return tuple(sorted((a, b)))


def flows(rows):
    net, gross = defaultdict(int), defaultdict(int)
    for r in rows:
        base, quote = r["base_asset"], r["quote_asset"]
        b, q = int(r["base_amount"]), int(r["quote_amount"])
        sign = 1 if r["direction"] == "buy" else -1
        net[base] += sign * b
        net[quote] -= sign * q
        gross[base] += b
        gross[quote] += q
    return {a: v for a, v in net.items() if abs(v) > LIVE_SHARE * gross[a]}


def orders(rows, live):
    # Union-find over the rows of one tx.
    parent = list(range(len(rows)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(members):
        for i in members[1:]:
            parent[find(i)] = find(members[0])

    # Paths: rows chained by an intermediate asset.
    by_hop = defaultdict(list)
    for i, r in enumerate(rows):
        for asset in {r["base_asset"], r["quote_asset"]} - live.keys():
            by_hop[asset].append(i)
    for members in by_hop.values():
        union(members)
    # Orders: paths with the same live endpoints (one order split over several paths,
    # e.g. ADA->X direct plus ADA->USDCx->X; a pair split over pools is the 1-row case).
    endpoints = defaultdict(set)
    for i, r in enumerate(rows):
        endpoints[find(i)] |= {r["base_asset"], r["quote_asset"]} & live.keys()
    by_endpoints = defaultdict(list)
    for root, ends in endpoints.items():
        by_endpoints[frozenset(ends)].append(root)
    for roots in by_endpoints.values():
        union(roots)
    groups = defaultdict(list)
    for i, r in enumerate(rows):
        groups[find(i)].append(r)
    return list(groups.values())


def legs(order_rows):
    # One leg per pool key, in first-occurrence order: (key, curve class, input asset, dx).
    # The pool's input is what the user gives: the quote asset on a buy, the base asset on
    # a sell.
    by_key, cls = {}, {}
    for r in order_rows:
        key = pool_key(r["base_asset"], r["quote_asset"])
        cls[key] = curve_class(r["base_asset"], r["quote_asset"])
        asset, amount = ((r["quote_asset"], r["quote_amount"]) if r["direction"] == "buy"
                         else (r["base_asset"], r["base_amount"]))
        by_key.setdefault(key, Counter())[asset] += int(amount)
    return [(key, cls[key], *inputs.most_common(1)[0]) for key, inputs in by_key.items()]


# ---------------------------------------------------------------- reserves (float snapshot)

def curl(url, attempts=5):
    # Same retry policy as float_tape.py.
    for attempt in range(1, attempts + 1):
        try:
            out = subprocess.run(["curl", "-sf", "-m", "60", url], check=True, capture_output=True)
            return json.loads(out.stdout)
        except subprocess.CalledProcessError as e:
            if attempt == attempts:
                raise
            print(f"  curl exit {e.returncode}, retry {attempt}/{attempts - 1} ...", file=sys.stderr)
            time.sleep(2 ** attempt)


def reserves():
    # Every market, priced or not (route-quality/snapshot.py keeps the priced ones only).
    if not MARKETS.exists():
        MARKETS.parent.mkdir(parents=True, exist_ok=True)
        url, markets, meta = "https://api.float.sundae.fi/v1/markets?sort=liquidity&limit=100", [], None
        while url:
            res = curl(url)
            meta = meta or res["meta"]
            markets += res.get("markets") or []
            url = res.get("links", {}).get("next") if res.get("markets") else None
            print(f"\r  {MARKETS.name}: {len(markets)} markets", end="", file=sys.stderr)
            time.sleep(0.2)
        print(file=sys.stderr)
        json.dump(dict(meta=meta, markets=markets), open(MARKETS, "w"))
    snap = json.load(open(MARKETS))
    total = defaultdict(int)  # (pool key, asset) -> reserve, summed over the key's pools
    for m in snap["markets"]:
        if m["venue"] not in DEPTH_VENUES:
            continue
        a, b = m["base"]["asset"], m["quote"]["asset"]
        key = pool_key(a, b)
        total[key, a] += int(m["base"]["reserve"] or 0)
        total[key, b] += int(m["quote"]["reserve"] or 0)
    return snap["meta"], total


# ---------------------------------------------------------------- bands walked by a leg

def crossings_at_least(dx, x, cls):
    """[P(the leg crosses >= m band edges) for m = 1 .. MAX_BANDS - 1], over a band offset
    uniform within a band. The pool keeps its fee out of the input."""
    band_width = 2 * CUT * SIGMA[cls] / BANDS_PER_POOL
    move = 2 * math.log1p(dx * (1 - FEE[cls]) / x) / band_width
    return [min(max(move - (m - 1), 0.0), 1.0) for m in range(1, MAX_BANDS)]


def band_shares(at_least):
    p = [1.0] + at_least + [0.0]
    return [p[n] - p[n + 1] for n in range(MAX_BANDS)]  # walks 1 .. MAX_BANDS bands


# ---------------------------------------------------------------- run

# The tape is sorted by slot, tx hash, event: txs come out in queue order.
txs = defaultdict(list)
for r in csv.DictReader(open(TAPE)):
    txs[r["tx_hash"]].append(r)

queue, arbitrage = [], 0
for rows in txs.values():
    live = flows(rows)
    if len(rows) > 1 and (not any(v < 0 for v in live.values()) or not any(v > 0 for v in live.values())):
        arbitrage += 1
        continue
    queue += [legs(order_rows)[:MAX_POOLS_PER_ORDER] for order_rows in orders(rows, live)]

sizes = Counter(len(order) for order in queue)

last_seen, distances = {}, []
for i, order in enumerate(queue):
    pools = [key for key, _, _, _ in order]
    distances += [i - last_seen[p] for p in pools if p in last_seen]
    last_seen.update((p, i) for p in pools)
distances.sort()


def quantile(q):
    return distances[int(q * (len(distances) - 1))]


result = dict(
    source=dict(tape=TAPE.name, txs=len(txs), arbitrage_txs_dropped=arbitrage, orders=len(queue),
                pools=len(last_seen), live_share=LIVE_SHARE,
                max_pools_per_order=MAX_POOLS_PER_ORDER),
    pools_per_order={str(n): sizes[n] / len(queue) for n in sorted(sizes)},
    pool_reuse_distance=dict(median=quantile(0.5), p25=quantile(0.25), p75=quantile(0.75)),
)

meta, reserve = reserves()
overall, by_pools, no_reserve = [0.0] * MAX_BANDS, defaultdict(lambda: [0.0] * MAX_BANDS), 0
for order in queue:
    for key, cls, asset, dx in order:
        x = reserve.get((key, asset), 0)
        if x <= 0:
            no_reserve += 1
            shares = band_shares([0.0] * (MAX_BANDS - 1))
        else:
            shares = band_shares(crossings_at_least(dx, x, cls))
        for n, s in enumerate(shares):
            overall[n] += s
            by_pools[len(order)][n] += s
leg_count = sum(n * count for n, count in sizes.items())
label = lambda n: f"{n + 1}+" if n == MAX_BANDS - 1 else str(n + 1)
result["bands_per_leg"] = {label(n): s / leg_count for n, s in enumerate(overall)}
result["bands_per_leg_by_pools_per_order"] = {
    str(k): {label(n): s / sum(v) for n, s in enumerate(v)} for k, v in sorted(by_pools.items())}
result["source"] |= dict(
    reserves=dict(api="https://api.float.sundae.fi/v1/markets", **meta["as_of"]),
    legs=leg_count, legs_without_reserve=no_reserve,
    ladder=dict(sigma=SIGMA, cut_sigmas=CUT, bands=BANDS_PER_POOL))

json.dump(result, open(OUT, "w"), indent=2)
print(f"{OUT.name}: {len(queue)} orders from {len(txs)} txs "
      f"({arbitrage} arbitrage txs dropped), {len(last_seen)} pools", file=sys.stderr)
print("  pools_per_order      " + ", ".join(f"{n}: {100 * s:.2f} %" for n, s in result["pools_per_order"].items()),
      file=sys.stderr)
d = result["pool_reuse_distance"]
print(f"  pool_reuse_distance  median {d['median']} orders (p25 {d['p25']}, p75 {d['p75']})", file=sys.stderr)
print("  bands_per_leg        " + ", ".join(f"{n}: {100 * s:.3f} %" for n, s in result["bands_per_leg"].items()),
      f"({result['source']['legs_without_reserve']} of {result['source']['legs']} legs without reserve)",
      file=sys.stderr)
