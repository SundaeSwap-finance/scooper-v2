# Stableswap pools

How the scooper recognises, prices and scoops a Sundae v4 `stableswap`
pool, and what an operator must do when the module is deployed.

The on-chain module is `validators/modules/stableswap.ak` in sundae-v4
(`lib/modules/ss_check.ak`, `lib/modules/ss_math.ak`; design in
`docs/stableswap.md` there). The scooper-side port is
`src/sundaev4/ss_math.rs`.

## Recognition

A pool is a stableswap pool when the first module of one of its enabled
`actions` entries is the stableswap module's script hash
(`detect_pool_type` in `src/sundaev4/indexer.rs`). The hash comes from
`module-scripts.stableswap` in the execution config. Without that entry the
scooper cannot classify the pool and falls back to constant-product, which
fails on chain; add the entry before the first stableswap pool exists.

## The config preimage

The pool datum's `module_state` holds only
`blake2b_256(serialise_data(StableSwapConfig))`. The scooper needs the
preimage for every scoop:

```
StableSwapConfig {
  linear_amplification: Int,
  fee: Rational,
  rates: List<Int>,             // one per pool asset, positionally aligned
  rate_manager: Option<MultisigScript>,
  monotone_rates: Bool,
  max_rate_step: Option<Rational>,
}
```

`rates` changes on chain. A manager-signed tag-7 transcript entry (rate
update) replaces it and the module rewrites the hash. The other fields
change only through an Upgrade. So, unlike the other modules, "known
config" means "hashes to the pool's current datum", not "present".

The scooper obtains the preimage from the pool's own transactions and
keeps it in the `module_configs` table:

1. The Create redeemer of the pool's mint transaction carries
   `initial_state`.
2. Every scoop's `Operate` redeemer carries the config as stored in the
   pool INPUT. When the pool's spend redeemer opens with a tag-7 entry, the
   scooper also derives the config with the updated `rates` (the config the
   OUTPUT datum hashes).
3. From those candidates it keeps the one whose hash the output datum's
   `module_state` slot names (`extract_ss_config_candidates`,
   `resolve_ss_config`). The row is upserted on every change.

At start, `recover_missing_module_configs` re-derives the config of any
stableswap pool whose stored config does not hash to its current datum
(walking the pool's transaction history newest-first). The same check gates
the bootstrap lookup.

A cached config that no longer matches the datum is not used. The pool is
classified with a placeholder that fails the tx builder's hash check, so
orders against it are skipped with a log line naming the mismatch rather
than built into a transaction the module rejects. A restart recovers it.

There is no operator override (`pool-configs`) for stableswap pools: a
static override goes stale on the first rate update, and the datum hash is
authoritative.

## Pricing

Every step is computed in `src/sundaev4/ss_math.rs`, a port of the Newton
reference in sundae-v4 `lib/tests/scenario.ak`. With `P = 10^12` and
`x = r_a · rate_a · P`, `y = r_b · rate_b · P`:

- `D` (`get_d_rated`) is the largest integer with
  `4A(x + y) + D ≥ 4AD + D³/(4xy)`.
- Swap (`swap_step`, tag 3): `raw` is the smallest scaled output that keeps
  the curve at `D_before`; gross output `floor(raw / (rate_out · P))`; fee
  `ceil(gross · fee)`; the trader receives `gross − fee`; the whole fee
  stays in the out reserve; `fee_budget = floor(lp · D_after / D_before) − lp`.
- Deposit (tag 6) and withdraw (tag 4) (`liquidity_step`): the entry
  declares `t` (the change in `D`); each reserve moves by
  `ceil(r_i · t / D_before)`; `total_lp` becomes
  `floor(lp · (D_before + t) / D_before)`; `fee_budget = 0`. A deposit
  takes the largest `t` the offer covers; a withdrawal takes the `t` that
  burns exactly the offered LP.
- A single-asset deposit fills as a zap: a tag-3 swap and then a tag-6
  deposit in one transcript.

`swap_step` runs the module's own two-sided checks (`exchange_invariant`,
`liquidity_invariant`) before it returns. A step that fails them is
unfillable and never reaches a transaction.

The router (`src/sundaev4/router.rs`, `PoolViewType::StableSwap`) prices a
stableswap edge with the same math. The split solver's inverse marginal has
no closed form on this curve, so it bisects on the analytic marginal
`|dy/dx| = (16A·x²y² + D³·y) / (16A·x²y² + D³·x)`; `D` is computed once per
graph build.

## Transaction shape

Per stableswap pool in a scoop:

- transcript entries carry `operation_tag` 3 / 4 / 6 with typed
  `operation_data`: `SwapStep { raw_swap_result, next_sum_invariant,
  attribution }` or `LiquidityStep { target_delta_d, next_sum_invariant,
  attribution }`. The module does not read `attribution`; the scooper
  writes the serving order's output reference there (the same encoding it
  stamps into `operation_data` on the other curves), or Void for a step
  that serves no order;
- `fee_budget` is split with fee_split's cumulative floor, as for every
  module;
- the stableswap module withdraws zero with
  `Operate { entries: [{ pool_oref, config, sum_invariant }] }`, where
  `sum_invariant` is `D` for the pool input's reserves at `config.rates`;
- the module's reference script is a reference input.

Before it builds, the tx builder checks that the held config hashes to the
pool input's `module_state` slot and refuses the batch if not.

The scooper never originates a tag-7 rate update. Rate updates are keeper
transactions (the sundae-v4 CLI's `update-rates` / `scoop-stableswap
--new-rates`); the scooper indexes them and follows the new rates.

## Operator checklist when the module is deployed

1. Publish the module's reference script and register its stake
   credential (sundae-v4 CLI `deploy-scripts --scope stableswap`,
   `register-stake`).
2. Add the script to the scooper's execution config:

   ```json
   "module-scripts": {
     "stableswap": {
       "hash": "<stableswap_module script hash>",
       "ref-utxo": "<tx hash>#<index>"
     }
   }
   ```

   Preview: `config/preview-v4.json` names the republished module — the
   build that carries the `attribution` field (sundae-v4 `3c2574d` or
   later) — at hash
   `9db7ce54fb25f4390a89bb715a022fe79a86b9a043aa22c31c27380a` and ref
   `849ff000d1b17e974f8024bf962e82d9317cbc28fc8be7010e52462f28ada689#0`.
   The superseded hash `a44e0058…` still backs the two older preview
   pools until their governance module-swap lands; a scooper on this
   config does not serve those two until then. Preprod: the ref UTxO and
   hash are set when the republished script is published there.
3. Restart the scooper. The indexer tracks the new reference UTxO, and
   startup recovery fetches the config of every stableswap pool that
   already exists.
4. Watch `scooper_orders_scooped_by_pool_type_total{pool_type="ss"}` and
   the dashboard's `SS` badge. A pool that logs
   `stableswap config preimage hash ... does not match the pool's
   module_state` needs a restart (recovery) or an investigation of a missed
   rate update.

Blueprint-driven deployments: the scooper's `Blueprint` loader maps a
validator titled `stableswap.withdraw`, `stableswap_module` or `stableswap`
(with a reference of the same key) to `module-scripts.stableswap`.

## Tests

- `src/sundaev4/ss_math.rs`: the Aiken vectors
  (`lib/tests/unit/ss_swap.ak`) and every transaction of the preview run
  in sundae-v4 `test/devnet/STABLESWAP.md` (pool `ba4a9cd4…`): create,
  swap, deposit, withdraw, rate update, swap at the new rates, and the
  config hash before and after the update.
- `src/sundaev4/scoop_tests.rs` (`ss_*`, `basic_swap_splits_across_cs_and_ss`):
  scoops built by the production builder and evaluated against the real
  `stableswap_module` bytecode in `test/fixtures/devnet-blueprint.json`
  (the module applied to the fixture's `pool_mint` policy).

Run: `cargo test ss_`.
