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
| `--ks` | `3,4,15` | pool caps to route under |
| `--mainnet-limit` | `16384,14000000,10000000000` | lower tx limit, bytes,mem,steps |
| `--raised-limit` | `16384,7000000000,2000000000000` | higher tx limit, bytes,mem,steps |
| `--csv PATH` | none | one row per (pair, size, K) |
| `--show` | off | print every split of the unlimited route |

They're mapped in the Makefile with: `PAIRS`, `SIZES`, `KS`, `MAINNET_LIMIT`, `RAISED_LIMIT`, `SHOW=1`.

Paths are relative to the repo root.

`--ks` lists the pool caps K the router is run with: a route under cap K uses at most K
pools (the router is also always run without a cap). For each tx limit, the table keeps
the best of these routes whose tx fits, so the list needs a K that fits each limit. With
the default limits, one scoop tx holds about 3-4 pools under `mainnet` (each pool costs
≈ 2G of the 10G steps) and about 15 under `raised` (each pool adds ≈ 1 KB of the 16 KB
tx size): hence the default `3,4,15`.

Keep the list short: every K smaller than the number of pools of the uncapped route makes
the router search again, and these searches are most of the run time.

## Reading the output

```
pair           size |  mainnet pools |   raised pools | no limit pools | mainnet→raised
NIGHT→USDCx   1000k |   1.13 %     4 |   0.92 %     8 |   0.92 %     8 |      2145 $
NIGHT→USDCx   2000k |        -     - |   1.26 %     9 |   1.26 %     9 |         n/a
```

- **loss**: `1 − output / (input at reference prices)`, i.e. pool fees + price impact.
  The Cardano tx fee is not included.
- **mainnet / raised**: the lowest loss among all routes produced (every K and the
  unlimited one) whose tx fits that limit, and the pools it uses.
- **no limit**: the lowest loss regardless of the tx.
- **mainnet→raised**: `(loss_mainnet − loss_raised) × size`, the gain in $.
- **`-` under mainnet only**: no route fits the mainnet limit, so the order cannot be
  filled in one tx under it (the production scooper would quarantine it).
- **`-` everywhere**: the router found no route at all (see *Router limits*).
- **`failed ×n: …`**: build or evaluation errors, grouped.

`--show` prints the unlimited route under each row, one line per split: hop, pool (named
in its actual asset order, with its band), input and output in $ at reference prices, and
the leg's rate. A concentrated-liquidity leg flagged `<-- sells the pool's 2nd asset` hit
the concentrated-liquidity contract bug (see `flipped_cl_pairs` in `route_quality.rs`)
and its rate is wrong.

### CSV

`pair, size_usd, k, loss, pools_used, branches, max_hops, tx_bytes, mem_padded,
steps_padded, eval, fits_mainnet, fits_raised`, one row per (pair, size, K); `eval` is `ok`
or `failed`. For example, the cost per pool of a scoop and the loss-vs-K curves:

```python
import pandas as pd
d = pd.read_csv("results/market-v4-extrapolated.csv")
print(d[d.eval == "ok"].groupby("pools_used")[["tx_bytes", "mem_padded", "steps_padded"]].median())
print(d.pivot_table(index=["pair", "size_usd"], columns="k", values="loss"))
```

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
- both of its assets are in a fixed set of 10 tokens: ADA; the 6 tokens with the highest
  30-day volume on float when the set was chosen (USDCx, USDM, NIGHT, USDA, SNEK, DJED);
  iUSD and MIN, added because they link tokens without going through ADA, which gives
  alternative paths; and the stablecoin USDr;
- it holds at least 10 000 ADA of liquidity, to leave out dust pools;
- its curve is known.

The set favours routing structure (the most traded tokens and the pairs between them),
not coverage of the whole Cardano market.

**What the extrapolation takes from it.** The set of tokens and pairs, each pair's share
of the total observed liquidity, and one reference price per token (below). Individual
pools, their venues, curves and fees are not carried over.

**Reference price.** Each token gets one price: the liquidity-weighted median of the
prices of its constant-product pools against ADA. ADA's $ price comes from USDM, taken as
1 $. USDr has no constant-product pool against ADA (its only pool is USDCx/USDr): it is
assumed at par with USDCx. The extrapolated market is centred on these prices, and the bench
uses them to size orders in $ and to measure losses.

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
