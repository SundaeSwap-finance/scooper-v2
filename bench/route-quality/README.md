# Route-quality benchmark

How much better does a large swap execute when one scoop tx may be more complex, i.e.
touch more pools? This benchmark measures trade execution on a sundae-v4 market **as a
function of the maximum tx complexity** (tx size and ExUnits budget), **with the
production router and real scoop txs**.

For each trade A→Z and order size, the order (a basic `swapIntent`) is routed by
`find_blended_route` under a pool cap K, and every route is built into a real scoop tx
and evaluated against the v4 scripts. The benchmark then reports the best route whose tx
fits each of two limits, and the best route with no tx limit:

| limit | tx size | mem | steps | default |
|---|---|---|---|---|
| `mainnet` | 16 384 B | 14M | 10G | the current mainnet per-tx limits |
| `raised` | 16 384 B | 7G | 2T | a raised ExUnits budget, same tx size |
| no limit | ∞ | ∞ | ∞ | always reported |

Both limits are options (`--mainnet-limit`, `--raised-limit`, as `bytes,mem,steps`).
ExUnits are the evaluated budgets × `budget_padding` (21/20), as in `scooper.rs`. A route
whose tx fails to build or evaluate does not fit.

## Quick start

From this folder, the `Makefile` rebuilds what is out of date and runs the bench:

```sh
make help
make bench                               # default trades and sizes → results/market-v4-extrapolated.csv
```

Without make, from the repo root (options below):

```sh
cargo run --release --features route-bench --example route-bench -- --help
```

A full default run (5 trades × 4 sizes) takes about 7 minutes on a laptop.

## Options

The rust command accepts the following options:

| option | default | meaning |
|---|---|---|
| `--market` | `data/market-v4-extrapolated.json` | market file (see below) |
| `--blueprint` | `data/devnet-blueprint-without-traces.json` | scooper-format blueprint |
| `--pairs` | `ADA-USDM,ADA-NIGHT,ADA-SNEK,SNEK-USDM,NIGHT-USDCx` | trades, A-Z tickers |
| `--sizes` | `500000,1000000,2000000,5000000` | order sizes, USD |
| `--mainnet-limit` | `16384,14000000,10000000000` | lower tx limit, bytes,mem,steps |
| `--raised-limit` | `16384,7000000000,2000000000000` | higher tx limit, bytes,mem,steps |
| `--csv PATH` | none | one row per (pair, size, K) |
| `--show` | off | print every split of the unlimited route |

They're mapped in the Makefile with: `PAIRS`, `SIZES`, `MAINNET_LIMIT`, `RAISED_LIMIT`, `SHOW=1`.

Paths are relative to the repo root.

The router is capped with K: a route under cap K uses at most K pools and K splits. The
router itself knows nothing of tx limits, so K is how the bench asks it for a simpler tx.
For each (pair, size):

1. the router runs with no cap; its route uses U pools (and is reused for any K ≥ U);
2. for each tx limit, if that route does not fit, K is bisected over [0, U]: each probe
   routes at K, builds and evaluates the tx. The search keeps the largest K whose route
   does not *overflow* the limit (a route exists and its tx exceeds it). "Fits" itself
   is not monotone in K: a small K may find no route that fills the order at all. If
   the route at the K found is missing, no K fits that limit;
3. the table keeps, per limit, the best of all routes computed whose tx fits.

Bisection assumes "overflows" is monotone in K, which holds in practice (more pools,
bigger tx) but is not guaranteed: two routes with the same K can weigh differently.
Each probe below U is a full router search, the dominant cost: about log2(U) per limit.
To save probes, `K_HINTS` in `route_quality.rs` holds, per (pair, size, limit), the K the
bisection ended on in a previous run with the default market and limits (the largest K
that does not overflow, whether or not it has a route: the failed searches are the
expensive ones, so "no K fits" gets a hint too). The search starts there: probes at
K and K + 1 settle it when the hint still holds, otherwise the bisection goes on. A
stale or missing hint (other market, limits or sizes) costs probes, not correctness.
A run ends by printing the `K_HINTS` entries it contradicts or lacks, ready to paste.

## Reading the output

```
pair            size |      mid A→Z |  mainnet pools |   raised pools | no limit pools | mainnet→raised $
NIGHT→USDCx   $1000k |    0.0xxxxxx |  -x.xx %     4 |  -x.xx %     7 |  -x.xx %     7 |       +1102.91 $
```

The measure is the **implementation shortfall** (IS) of transaction cost analysis: what
the order received vs the same order filled entirely at the market's mid, its rate before
fees and price impact. It is a ratio of two amounts of Z: no USD and no reference price
enter it.

- **size**: order size in USD, converted to A at A's reference price. It only picks how
  much A is sold: a wrong reference price makes the size label approximate, not the
  shortfall, which is measured on the A actually sold.
- **mid A→Z**: the market's fee-free A→Z rate, in Z tokens per A token, read from the
  pools' state alone (`market_mid`). Each pool's mid is its fee-free marginal rate:
  `r_Z / r_A` for a constant-product pool, the ratio of its own prices for a constant-sum
  pool, and for a concentrated-liquidity pool the ratio of the virtual reserves of its
  invariant `(a + L/√pb)(b + L·√pa) = L²`. Each pair of tokens takes its best pool (a pool
  with no Z to pay is skipped), each acyclic path of at most 4 hops (the router's depth
  cap) the product of its pairs' mids, and the mid is the best path's. On
  `market-v4-extrapolated.json`, every pool sits at the reference prices (`cl_market.py`),
  so every path gives the same mid, `price[A] / price[Z]` up to rounding (≈ 1e-10); on an
  observed market, paths disagree. The line under the table gives the largest
  disagreement seen.
- **mainnet / raised / no limit**: among all routes produced (the unlimited one and the
  bisection probes), the best one whose tx fits that limit (any tx for no limit): IS,
  `Z_out / (A_in × mid) − 1` in % (≤ 0: pool fees + price impact), and the pools it uses.
  The gain of a limit over another is the difference of their IS. The Cardano tx fee is
  not included.
- **mainnet→raised $**: extra Z received under the raised limit vs the mainnet one, valued
  in USD at Z's reference price. Unlike the shortfalls, it is an **estimate**: the
  reference price and `ada_usd` are not exact. `n/a` when either limit has no route.
- **`-` under mainnet only**: no route fits the mainnet limit, so the order cannot be
  filled in one tx under it (the production scooper would quarantine it).
- **`-` everywhere**: the router found no route at all (see *Router limits*).
- **`failed ×n: …`**: build or evaluation errors, grouped.

`--show` prints the unlimited route under each row, one line per split: hop, pool (named
in its actual asset order, with its band), input and output in ADA at reference prices, and
the leg's rate. A concentrated-liquidity leg flagged `<-- sells the pool's 2nd asset` hit
the concentrated-liquidity contract bug (see `flipped_cl_pairs` in `route_quality.rs`)
and its rate is wrong.

### CSV

`pair, size_usd, k, input, output, mid, vs_no_limit, shortfall, pools_used, branches,
max_hops, tx_bytes, mem_padded, steps_padded, eval, fits_mainnet, fits_raised`, one row per
(pair, size, K routed: U and the bisection probes);
`input` is the amount of A spent (`size_usd` converted at A's reference price) and `output`
the amount of Z received, both in tokens; `mid` is the mid A→Z of the table, in Z tokens
per A token; `vs_no_limit` is `output / output_no_limit − 1` (a fraction, ≤ 0), against the
no-limit route of the table; `shortfall` is `output / (input × mid) − 1` (a fraction, ≤ 0);
`eval` is `ok` or `failed`.

## The markets

The bench runs on one market, `market-v4-extrapolated.json`: a hypothesis, built from an
observed market, `market-mainnet-selected.json`.

### `market-mainnet-selected.json`: the observed input

**Source.** Sundae's [float](https://float.sundae.fi) API (`api.float.sundae.fi/v1/markets`),
an indexer of Cardano DEX pools. It covers 13 venues (Minswap, SundaeSwap, WingRiders,
Splash, VyFi, CSwap, Danogo, Snek.fun) and gives, for every pool, its reserves read at one
single block and its value in ADA. The block and its time are recorded in the file. float
gives no fees: each pool's fee is read from its own venue (the venue's API, or the pool's
on-chain datum for CSwap). WingRiders v1 publishes none; those pools get the venue's most
common v2 fee for the same curve. Each pool's `fee_source` says where its fee comes from.

**Selection.** A pool is kept when:
- both of its assets are in a fixed set of 9 tokens: ADA; the 6 tokens with the highest
  30-day volume on float when the set was chosen (USDCx, USDM, NIGHT, USDA, SNEK, DJED);
  iUSD and MIN, added because they link tokens without going through ADA, which gives
  alternative paths;
- it holds at least 10 000 ADA of liquidity, to leave out dust pools;
- its curve is known.

The set favours routing structure (the most traded tokens and the pairs between them),
not coverage of the whole Cardano market.

**What the extrapolation takes from it.** The set of tokens and pairs, each pair's share
of the total observed liquidity, and one reference price per token (below). Individual
pools, their venues, curves and fees are not carried over.

**Reference price.** Each token gets one price: the liquidity-weighted median of the
prices of its constant-product pools against ADA. ADA's $ price comes from USDM, taken as
1 $. The extrapolated market is centred on these prices, and the bench
uses them only to size orders in $ (and to value the `--show` legs in ADA); losses are measured against the mid read from the pools.

### `market-v4-extrapolated.json`: the hypothetical market the bench runs on

Not observed: a scenario of what v4 could hold. It keeps the observed structure (tokens,
pairs, the relative size of each pair, reference prices) and assumes the rest:
- **TVL**: 350M$ in total, split between the pairs in proportion to their observed TVL;
- **pool type**: all liquidity in sundae-v4 concentrated-liquidity pools, where each price
  range is its own pool. Each pair gets adjacent price bands of 2 % (0.1 % for pairs of two
  stablecoins) around its reference price;
- **spread**: each band's share of the pair's liquidity follows a Gaussian in log-price
  around the price (σ = 10 %, 0.5 % for stablecoin pairs), an assumed typical spread of
  LP positions, not a measured one;
- **fees**: one per class, 0.30 % (volatile) or 0.05 % (stablecoin pairs), not the
  observed fees;
- **prices**: every pool sits at the reference price, so this market has no arbitrage.

Results on this market depend on these assumptions, mostly the band width and the total
TVL. All of them are options of `cl_market.py`.

## Limits

- Orders have `min_received = 1`: a degenerate route "fits" where a real order's
  `min_received` would reject it.
- The benchmark recompiles the crate's modules (`src/route_bench.rs` must list the same
  top-level modules as `main.rs`).

## Possible improvements to the production code

Limits of the production code found while running this bench. The bench measures the code
as it is.

- **Concentrated-liquidity contract, selling a pool's second asset**
  (`sundae-v4/lib/modules/cl_check.ak`, B-input branch: `dx_eff * spa_num` should be
  `spa_den`). The input is scaled by √pa: such a swap pays out only ~√pa of the fair
  amount when the band's lower price pa < 1, and is rejected when pa > 1. The router
  reproduces the formula on purpose. The bench works around it (`flipped_cl_pairs`).
- **The value-preserving cap is not applied during the split search**
  (`router.rs`, `allocations_for_lambda` caps a concentrated-liquidity pool at its
  reserve only; `pool_absorb_cap` is applied at the end). A pool whose value-preserving
  cap is far below its reserve, e.g. one selling its second asset with pa > 1, looks
  attractive during the search, gets most of the allocation, is then clamped to almost
  nothing, and the hop comes out short of its input: the whole path is rejected, even when
  the other pools of the pair could have absorbed it. Capping with `pool_absorb_cap`
  inside the search would avoid it.
- **The value-preserving cap is recomputed on every call.** `cl_max_dx_value_preserving`
  runs at least 64 `cl_fee_budget` evaluations (products of ~300-bit integers and a square
  root) each time, and nothing caches it per pool: ≈ 45 % of the routing time in a
  profile of this bench. It only depends on the pool, so it can be computed once per
  pool and direction.
- **Routes under a pool cap come from greedy pruning** (`find_blended_route`): the router
  takes its best uncapped route and removes the least valuable pool, re-solving the whole
  blend each time, until the cap is met. It is slow (one full re-solve per removed pool)
  and not optimal: a better route within the cap may exist.
