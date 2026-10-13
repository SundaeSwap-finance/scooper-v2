"""Parameters of the block-capacity bench's order queue, measured on the real DEX flow.

    python3 dataset_params.py   (reads data/tape.csv)  -> data/dataset-params.json

The bench assumes the market can always provide (infinite liquidity): an order's size,
direction and the pools' state do not matter, only its shape, i.e. the set of pools it
touches. Pools are the real mainnet pools, one per venue (float market ids). Two
parameters describe a queue of such orders:

  pools_per_order      share of orders touching 1, 2, 3, 4 pools (orders touching more
                       are capped to 4, see MAX_POOLS_PER_ORDER).
  pool_reuse_distance  for each pool an order touches, the distance in orders, along the
                       queue, back to the last order that touched the same pool. Given as
                       its median and quartiles (heavy-tailed: the mean is set by rare
                       pools and by the sample length, so it is not reported).

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
     or pools). Its pool count is its number of rows. A tx bundling independent orders
     (batch, aggregator) thus yields several orders.
Limits: float nets each pool per tx, so several orders batched on one pool count as one;
two independent orders with the same input and output assets in one tx count as one."""
import csv, json, sys
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).parent
TAPE = HERE / "data" / "tape.csv"
OUT = HERE / "data" / "dataset-params.json"
LIVE_SHARE = 0.02
# One order alone in a sundae-v4 scoop fits ~4 pool legs under mainnet's per-tx limits
# (14M mem / 10G steps, steps bind: see bench/route-quality). An order touching more pools
# could never execute and the scooper would quarantine it; the bench measures block
# capacity, not the scooper, so such orders are capped: they keep their first 4 pools.
MAX_POOLS_PER_ORDER = 4


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
        groups[find(i)].append(r["market"])
    return list(groups.values())


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
    queue += [pools[:MAX_POOLS_PER_ORDER] for pools in orders(rows, live)]

sizes = Counter(len(pools) for pools in queue)

last_seen, distances = {}, []
for i, pools in enumerate(queue):
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
json.dump(result, open(OUT, "w"), indent=2)
print(f"{OUT.relative_to(HERE)}: {len(queue)} orders from {len(txs)} txs "
      f"({arbitrage} arbitrage txs dropped), {len(last_seen)} pools", file=sys.stderr)
print("  pools_per_order      " + ", ".join(f"{n}: {100 * s:.2f} %" for n, s in result["pools_per_order"].items()),
      file=sys.stderr)
d = result["pool_reuse_distance"]
print(f"  pool_reuse_distance  median {d['median']} orders (p25 {d['p25']}, p75 {d['p75']})", file=sys.stderr)
