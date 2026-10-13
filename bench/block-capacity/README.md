# Block-capacity benchmark

How many sundae-v4 orders can a block carry, a Praos **ranking block (RB)** vs a Leios
**endorser block (EB)**, when every scoop tx stays within today's per-tx limits? The
benchmark generates an order queue shaped like the real Cardano DEX flow, cuts it into
scoop txs **with the production scooper's sizing code and real txs**, packs the scoops
into blocks and reports the orders per full block.

Pipeline:

1. **Tape** (`float_tape.py`): every DEX swap of the last 30 days, from Sundae's float API.
2. **Queue parameters** (`dataset_params.py`): two parameters measured on that tape.
3. **Queue** (`block_capacity.rs`): a queue of orders drawn from those parameters.
4. **Scoops**: the queue cut into scoop txs as production does.
5. **Blocks**: the scoops packed into RBs and into EBs.

## Quick start

From this folder, the `Makefile` rebuilds what is out of date and runs the bench:

```sh
make help
make                     # data/dataset-params.json (downloads the tape first if missing)
make bench               # run the bench → results/scoops.csv, results/blocks.csv
make bench ORDERS=2000   # quick run on a short queue
```

Without make, from the repo root (options below):

```sh
cargo run --release --features bench --example block-bench -- --help
```

A full default run (20 000 orders) takes about 5 minutes on a laptop; nearly all of it is
building and evaluating scoop txs.

## Options

| option | default | meaning |
|---|---|---|
| `--params` | `bench/block-capacity/data/dataset-params.json` | queue parameters (see below) |
| `--blueprint` | `bench/common/devnet-blueprint-without-traces.json` | scooper-format blueprint (shared by the benches) |
| `--orders` | `20000` | queue length |
| `--seed` | `1` | seed of the queue generator |
| `--max-orders` | `30` | max orders per scoop (production `BatchLimits`) |
| `--tx-limit` | `16384,14000000,10000000000` | per-tx limit, bytes,mem,steps (mainnet) |
| `--rb-limit` | `90112,72000000,20000000000` | ranking block limit, bytes,mem,steps (current mainnet Praos block) |
| `--eb-limit` | CIP-164 Table 7 (`12000000,7000000000,2000000000000`, S_EB-tx 12 MB) scaled by 1/16, 1/8, 1/4, 1/2 and 1 | endorser block limit, bytes,mem,steps; repeat it to compare several EB sizes (given ones replace the defaults) |
| `--csv PATH` | none | one row per scoop |
| `--blocks-csv PATH` | none | one row per block limit (the RB, then each EB) |

They're mapped in the `Makefile` with: `ORDERS`, `SEED`, `MAX_ORDERS`, `TX_LIMIT`,
`RB_LIMIT`, `EB_LIMITS` (space-separated list, e.g.
`EB_LIMITS="6000000,3500000000,1000000000000 12000000,7000000000,2000000000000"`).

Paths are relative to the repo root.

## The queue

The bench assumes the market can always provide (infinite liquidity): an order's size,
direction and the pools' state do not matter, only its **shape**, i.e. the set of pools
it touches. That is what decides the cost of a scoop: every pool it touches adds an input,
an output and script executions, so consecutive orders sharing pools make cheaper scoops.

### The tape: `data/tape.csv`

**Source.** Sundae's [float](https://float.sundae.fi) API (`api.float.sundae.fi/v1/trades`):
every swap of every venue, paged backwards from the newest trade (the anchor) over 30 days.
One row is the net swap of one pool in one tx: a routed order through three pools is three
rows sharing a tx hash. Pages are cached in `bench/cache/float-tape/` (not committed), so an
interrupted download resumes; `make refresh` takes a new anchor.

### The parameters: `data/dataset-params.json`

`dataset_params.py` turns the tape into the real sequence of user orders (txs in slot order,
then by hash within a slot): per tx, the assets with a net flow are the order's input and
output, the others are intermediate hops; arbitrage cycles (no net input or output) are
dropped; rows chained by intermediate assets form one order, so a tx bundling independent
orders yields several. Two parameters describe the queue:

- **`pools_per_order`**: share of orders touching 1, 2, 3 and 4 pools. Orders touching
  more are capped to their first 4 pools: one order alone fits about 4 pool legs under
  mainnet's per-tx limits, so a longer one could never execute.
- **`pool_reuse_distance`**: for each pool an order touches, the distance in orders back to
  the last order that touched the same pool, as its median and quartiles (heavy-tailed: the
  mean is set by rare pools and by the sample length).

### The generated queue

`block_capacity.rs` draws a queue of `--orders` orders from these two parameters
(`draw_pools_per_order`). For each order, its pool count is drawn from `pools_per_order`;
for each of its pools, a minimum reuse distance `d` is drawn from a log-normal fitted on the
median and quartiles, and the order takes the pool whose last use is the closest to `d`
orders back without being more recent. When no pool qualifies (start of the queue, or every
pool was touched since), the order opens a new pool. The realised distance is `d` or a bit
more; the run prints the generated queue's parameters next to the targets so the
approximation can be checked.

Every pool is a copy of one large ADA/NIGHT concentrated-liquidity pool of the
route-quality market (hardcoded in `build_reference_pool`, built with the real CL module
pipeline), so pools only differ by their identity. Every order is a basic swap of 100 ADA
for NIGHT over its pools, routed by the production router restricted to those pools: one
hop split about evenly over them.

## Scoops and blocks

**Scoops.** The queue is cut into scoop txs as production does (`scooper.rs`,
`run_v4_batch_cycle`): orders are accumulated in queue order up to `--max-orders`, one
checkpoint per prefix; the full batch is probed, then the largest prefix whose tx fits
`--tx-limit` is found by bisection. The search and the padded-cost check are production's
own code (`batch::largest_fitting_checkpoint`, `batch::ScoopTxCost`), and every probe is a
real tx built and evaluated against the blueprint by the production tx builder and
evaluator. The next scoop starts from the rest of the queue, against the same pool state
(infinite liquidity).

**Blocks.** Scoops are packed in queue order into RBs and, separately, into EBs: a block is
closed when the next scoop would exceed one of its limits (bytes, padded mem and steps
summed over its txs). The last, partial block is not counted. The scoops are cut once, then
packed for each EB size given: building and evaluating the scoop txs is nearly all of the
run time, packing is instant, and every EB size sees the same scoops.

## Reading the output

```
queue       20000 orders, … pools
            pools per order   1: … % (target …), 2: … % (target …), …
            reuse distance    median … (target 25), p25 … (7), p75 … (104); … % of pool legs open a new pool
scoops      … under 16.4k B, 14.00M mem, 10.00G steps
            orders per scoop  mean …, median …, min …, max …
            pools per scoop   mean …, median …, min …, max …
            mean tx           … B, … mem, … steps
            limited by        tx script memory: … of … scoops, …
RB          90.1k B, 72.00M mem, 20.00G steps: … full blocks
            orders per block  mean …, median …, min …, max …
            scoops per block  mean …, median …, min …, max …
            limited by        block script CPU steps
EB          750.0k B, 437.50M mem, 125.00G steps: … full blocks
            …
EB          …    (one entry per EB size)
```

- **queue**: the generated queue's parameters, measured as `dataset_params.py` does, next
  to their targets.
- **scoops**: per scoop, the orders and pools it holds, the mean tx cost (padded as
  production does), and why scoops stop growing: a per-tx resource (`tx script memory`,
  `tx script CPU steps`, `tx size (bytes)`), `max_orders reached`, or `next order fails to
  build/eval`. The last scoop, cut by the end of the queue, is left out.
- **RB / EB**: the number of full blocks, the **orders per block** (the result), the scoops
  per block, and the block resource that closes blocks.

A short queue may not fill one EB: the line then says so and gives what the partial block
holds. Lengthen the queue (`--orders`) to get several full EBs: with the default 20 000
orders, the largest default EB (Table 7) gets 7 full blocks.

The run is **invalid** (exit code 1, `INVALID RUN` with the reasons) when an order could
not be scooped at all: a 1-order scoop over the per-tx limit (production would quarantine
it), or an order the accumulator refuses. The bench measures block capacity, not the
scooper.

### CSV

`scoop, first_order, orders, pools, tx_bytes, mem_padded, steps_padded, cut`, one row per
scoop in queue order; `first_order` is the queue index of its first order, `mem_padded` and
`steps_padded` its ExUnits padded with `budget_padding`, `cut` why it holds no more orders.

`blocks.csv` (`--blocks-csv`): `block, bytes, mem, steps, full_blocks, orders_mean,
orders_median, orders_min, orders_max, scoops_mean, limited_by`, one row per block limit:
the RB, then each EB in the order given. A limit no full block reaches has `full_blocks` 0
and empty statistics.

## Limits

- **Infinite liquidity**: every scoop runs against the same pool state, all pools are
  copies of one pool on one pair, and orders have `min_received = 1`. Only the pool-sharing
  structure of the queue is real; trade sizes, prices and slippage are not modelled.
- **Queue shape**: float nets each pool per tx, so several orders batched on one pool count
  as one, and two independent orders with the same assets in one tx count as one. Orders
  touching more than 4 pools are capped.
- **Volume**: the queue keeps today's sequence. At k× the volume the same number of orders
  spans k× less time, and the reuse pattern between consecutive orders may not hold.
- **Scooper behaviour**: the bench keeps production's sizing (accumulation order,
  bisection, padding, limits) but not its failure handling (re-diagnosis of a 1-order
  failure, retries of refused orders, mempool chaining); such orders invalidate the run.
- **Blocks**: scoops only, packed greedily in queue order; no other txs, no block header or
  EB overhead beyond the limits given.
- The benchmark recompiles the crate's modules (`src/block_bench.rs` must list the same
  top-level modules as `main.rs`).
