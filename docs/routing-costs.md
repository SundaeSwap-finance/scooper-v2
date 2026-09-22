# What a scoop costs, and what the routing constants should be

`cost-per-pool-lovelace` and `cost-per-step-lovelace` cap how much route
fan-out an order may buy:

```rust
max_pools = max_per_execution / cost_per_pool
max_steps = max_per_execution / cost_per_step
```

A `k`-hop route touches `k` pools and takes `k` steps, so it is routable
only when `max_per_execution >= k * max(cost_per_pool, cost_per_step)`.

The deployed configs carry 1 000 000 and 500 000 lovelace. Those numbers
were never measured. This page measures them.

**Recommendation: `cost-per-pool-lovelace` 250 000, `cost-per-step-lovelace`
125 000.** The reasoning is below; the measurement is reproducible with

```sh
cargo test cost_bench -- --ignored --nocapture
```

## Method

`src/sundaev4/cost_bench.rs` builds scoops with the production transaction
builder and evaluates them against real validator bytecode, through the
same two passes `scooper.rs` runs before it submits: build with default
budgets, evaluate, pad every budget by `budget_padding` (5%), compute the
fee from the first-pass size and the padded units, rebuild. Every figure
below is what the scooper would declare and pay.

Two corrections were needed before the numbers meant anything.

1. **Untraced bytecode.** The committed fixture
   (`test/fixtures/devnet-blueprint.json`) is the traced build — right for
   eval diagnostics, wrong for cost. It costs about 4.5x the CPU and 2.7x
   the bytecode of what a deployment publishes. The benchmark uses
   `test/fixtures/devnet-blueprint-untraced.json` instead: the same
   validator set, untraced, with each script's parameter hashes recomputed
   in dependency order (settingsMint -> poolMint / order -> pool) so the set
   is internally consistent without matching any deployment.
2. **The real cost model.** `test_harness::PLUTUS_V3_COST_MODEL` is a
   295-entry pre-Plomin model; the chain runs 350 entries. Every builtin
   past the truncation is charged with another builtin's coefficient, which
   leaves memory about right and overstates CPU by nearly 4x. The benchmark
   reads the 350-entry model out of `config/preview-v4.json` (preprod ships
   the identical array; there is no v4 mainnet config yet) and rebuilds the
   language views to match.

Fee coefficients come from `tx_builder::compute_tx_fee`, the function the
scooper itself uses: 44 lovelace/byte, 155 381 fixed, 0.0577/memory unit,
0.0000721/step, plus the Conway tiered reference-script fee at 15
lovelace/byte.

**Validation against production.** One constant-sum order against one pool
measures memory 3 124 093, steps 1 105 756 404, size 2 309. The deployed
preview scooper's own submission log for that shape (`9b4df45a…`) reports
3 487 254 / 1 217 032 909 / 2 675 — within 10%, 14% and 14%. The remainder
is the fee constraint and its settings reference input, which the fixture
does not carry. Before the two corrections above the same row measured
5 558 585 476 steps, 4.5x the truth.

`fee` includes the reference-script component; `fee_no_ref` removes it.
Marginals are taken on `fee_no_ref`, because reference-script bytes do not
change when a pool of a curve already in the transaction is added — they
cancel in every marginal.

## The three marginals

They are different quantities and the two constants have to reflect that.

| what is added | held fixed | cp | cs | ss |
| --- | --- | --- | --- | --- |
| one more order on a pool already in the tx | pools | 103 495 | 104 308 | 107 790 |
| one more pool | order count | 162 638 | 173 426 | 173 130 |
| one more pool carrying its own order | — | 266 132 | 277 734 | 280 919 |
| **one more hop of a routed order** | orders (= 1) | **186 974** | **198 929** | **204 807** |

All figures in lovelace, `fee_no_ref`.

The last row is the one the constants gate. A routed hop adds a pool and a
transcript entry but no second order input and no second payout, so it
costs less than a pool that brings its own order. Reading the third row as
the hop cost — the obvious mistake — overstates it by about 37%.

## The scans

### One pool, n orders

| curve | orders | mem | mem% | cpu | cpu% | size | fee | fee_no_ref |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cp | 1 | 3 020 042 | 21.6% | 1 054 358 452 | 10.5% | 2 286 | 889 712 | 507 242 |
| cp | 5 | 7 260 246 | 51.9% | 2 472 334 796 | 24.7% | 3 510 | 1 290 464 | 907 994 |
| cp | 10 | 12 877 801 | 92.0% | 4 336 594 169 | 43.4% | 5 040 | 1 816 330 | 1 433 860 |
| cs | 1 | 3 124 093 | 22.3% | 1 105 756 404 | 11.1% | 2 309 | 940 196 | 517 964 |
| cs | 10 | 13 057 516 | 93.3% | 4 434 303 467 | 44.3% | 5 054 | 1 874 122 | 1 451 890 |
| ss | 1 | 3 189 990 | 22.8% | 1 126 944 175 | 11.3% | 2 357 | 973 305 | 525 405 |
| ss | 10 | 13 317 213 | 95.1% | 4 552 678 506 | 45.5% | 5 399 | 1 938 490 | 1 490 590 |

### n pools, one order each

| curve | pools | mem | mem% | cpu | cpu% | size | fee | fee_no_ref |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cp | 2 | 5 373 226 | 38.4% | 1 905 205 137 | 19.1% | 3 459 | 1 138 449 | 755 979 |
| cp | 5 | 13 303 458 | 95.0% | 4 890 052 659 | 48.9% | 6 978 | 1 966 066 | 1 583 596 |
| cs | 5 | 13 856 436 | 99.0% | 5 164 963 466 | 51.6% | 7 093 | 2 062 616 | 1 640 384 |
| ss | 4 | 11 180 067 | 79.9% | 4 111 388 701 | 41.1% | 6 089 | 1 813 719 | 1 365 819 |

### One order through k hops

| curve | hops | mem | mem% | cpu | cpu% | size | fee | fee_no_ref |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cp | 1 | 3 020 042 | 21.6% | 1 054 358 452 | 10.5% | 2 286 | 889 712 | 507 242 |
| cp | 2 | 4 590 902 | 32.8% | 1 634 451 598 | 16.3% | 3 316 | 1 067 495 | 685 025 |
| cp | 3 | 6 258 998 | 44.7% | 2 264 199 517 | 22.6% | 4 346 | 1 254 469 | 871 999 |
| cp | 4 | 8 024 325 | 57.3% | 2 943 602 207 | 29.4% | 5 376 | 1 450 633 | 1 068 163 |
| cs | 4 | 8 475 917 | 60.5% | 3 172 198 550 | 31.7% | 5 468 | 1 536 982 | 1 114 750 |
| ss | 2 | 4 926 991 | 35.2% | 1 780 732 646 | 17.8% | 3 458 | 1 169 112 | 721 212 |
| ss | 3 | 6 760 147 | 48.3% | 2 482 397 476 | 24.8% | 4 559 | 1 373 919 | 926 019 |
| ss | 4 | 8 689 463 | 62.1% | 3 231 938 666 | 32.3% | 5 660 | 1 587 727 | 1 139 827 |

## Capacity ceilings

The largest scoop that passes the production fitness rule (padded units
under the transaction budgets, first-pass size under the size limit), at
mainnet limits: memory 14 000 000, steps 10 000 000 000, size 16 384.

| shape | curve | largest that fits | mem | cpu | size | binds on |
| --- | --- | --- | --- | --- | --- | --- |
| orders on one pool | cp | 10 orders | 92.0% | 43.4% | 30.8% | **mem** |
| orders on one pool | cs | 10 orders | 93.3% | 44.3% | 30.8% | **mem** |
| orders on one pool | ss | 10 orders | 95.1% | 45.5% | 33.0% | **mem** |
| pools, one order each | cp | 5 pools | 95.0% | 48.9% | 42.6% | **mem** |
| pools, one order each | cs | 5 pools | 99.0% | 51.6% | 43.3% | **mem** |
| pools, one order each | ss | 4 pools | 79.9% | 41.1% | 37.2% | **mem** |

**Memory binds first, every time, and nothing else is close.** At the
memory ceiling CPU sits at 41–52% and size at 31–43%. Size never binds:
even 10 orders on one pool is under a third of the size limit. The old
intuition that size is the scarce resource is wrong for this contract set.

This also answers the rationing question. A scoop at its ceiling consumes
the whole 14 M transaction memory budget, which is about 23% of a 62 M
mainnet block. One routed hop is 1.6–1.9 M memory, roughly 12% of a
transaction budget and 2.8% of a block. A four-hop route for one order
uses 8.0–8.7 M, about 60% of a transaction.

Fan-out is bounded well before the constants matter for a well-funded
order: the router caps search depth at 4 hops (`find_optimal_route`) and
parallel branches at 4 (`find_blended_route`), and the dispatch loop binary
searches the batch down until it fits. The constants are therefore a
low-end gate — which is exactly why setting them too high is the dangerous
direction.

## Cost by curve

Stableswap is the most expensive curve, but only just: a routed hop costs
204 807 lovelace against constant product's 186 974, a 9.5% spread.
Constant sum sits between them at 198 929.

That spread is too small to justify per-curve constants, and a per-curve
constant could not be applied anyway — the router picks the pools *after*
the limits are computed from the order's budget. So **the constant is a
ceiling, not an average**: it is set from stableswap, the dearest curve, so
that no route is admitted whose cost the order's budget does not cover.
The cost of that choice is that a constant-product order is charged about
9% more than it consumes.

If a future curve is much more expensive than stableswap, this decision
needs revisiting: a single ceiling across a wide spread would price the
cheap curves out of multi-hop routing.

## Reference scripts: the one place an extra pool is expensive

Adding a pool of a curve already in the transaction adds no reference-script
bytes. Adding the *first* pool of a new curve adds that module's whole
script.

| transaction | ref bytes | fee those bytes carry |
| --- | --- | --- |
| constant product only | 25 498 | 382 470 |
| + a constant-sum pool | 31 079 | 482 622 |
| + a stableswap pool | 38 086 | 608 748 |

The first constant-sum pool costs 100 152 lovelace of reference-script fee
on top of its marginal; the first stableswap pool, 126 126. Both are
one-off per transaction, and both are already inside the measured
`fee` column.

This is worth knowing for batching policy — mixing three curves in one
scoop costs 0.61 ADA in reference scripts alone — but it does not belong in
the routing constants, which price marginal pools, not the first of a kind.

## Recommendation

| constant | current | recommended | basis |
| --- | --- | --- | --- |
| `cost-per-pool-lovelace` | 1 000 000 | **250 000** | worst-curve routed hop 204 807, plus 22% headroom |
| `cost-per-step-lovelace` | 500 000 | **125 000** | worst-curve order/step marginal 107 790, plus 16% headroom; half the pool constant |

Why these:

- **A pool is priced at what a hop costs**, because in every route a pool
  arrives with the step that uses it. 250 000 covers stableswap's 204 807
  with room for cost-model drift and contract growth.
- **A step is priced at what one more transcript entry costs**, 103–108 k
  measured. Keeping it at half the pool constant preserves the right
  ordering: a route can have more steps than pools (a hop split across
  pools, or a pool revisited on two hops), so `max_steps` must be the looser
  cap or it becomes the binding one for no reason.
- **Headroom is deliberately modest.** These constants ration nothing
  scarce at the top end — the router's own depth and branch caps do that —
  so the only real risk is setting them too high.

### What `base_fee` this demands

An order is routable at `k` hops when
`max_per_execution >= k * max(cost_per_pool, cost_per_step)`, and the fee
constraint requires `base_fee <= max_per_execution`. With the recommended
constants:

| route | minimum `max_per_execution` |
| --- | --- |
| direct (1 pool) | 250 000 (0.25 ADA) |
| 2 hops | 500 000 (0.50 ADA) |
| 3 hops | 750 000 (0.75 ADA) |
| 4 hops (the router's maximum) | 1 000 000 (1.00 ADA) |

The launch `base_fee` is 1.28 ADA, so a launch order reaches
`max_pools = 5` and `max_steps = 10`: every route the router can build is
affordable, with headroom.

**The current constants do not clear that bar.** At 1 000 000 per pool a
1.28 ADA order gets `max_pools = 1` and `max_steps = 2` — no multi-hop
route is ever considered, and dispatch logs "no route" with no error. That
is the preview failure, and it would ship to mainnet unchanged. Reaching
even two hops today would need a 2 ADA `base_fee`, and three hops 3 ADA.

### Not changed here

`config/preview-v4.json` and `config/preprod-v4.json` still carry
1 000 000 / 500 000. Changing a deployed config was out of scope for this
measurement.

## Re-running when the contracts change

```sh
cargo test cost_bench -- --ignored --nocapture
```

It is `#[ignore]`d because `test_harness::TEST_CTX` is a process-global
`OnceLock` holding the constraint hashes of the first blueprint any test
loads; sharing a process with the traced-fixture tests would give one of
the two the other's hashes. The test asserts it owns that context, so a
mis-run fails loudly rather than reporting wrong numbers.

The assertions pin each measured marginal under the constant that prices
it, and pin memory as the binding limit. A contract change that moves a
marginal past its constant fails the test with the number to put here.

To regenerate the untraced fixture after a contract change, compile the
validator set from a sundae-v4 checkout with `trace: false`, computing the
parameter hashes in dependency order (settingsMint from the boot UTxO;
settings, order, poolMint, fairness, fairnessOrder from settingsMint;
basicOrder, swapOrder, strategyOrder from order; the invariant modules from
poolMint; feeSplit from settingsMint and poolMint; pool from poolMint and
settingsMint; routeOrder from order and poolMint), and emit
`{validators: [{title, hash, compiledCode}], references: [{key, txIn}]}`
with the traced fixture's reference `txIn`s so only the bytecode differs.
