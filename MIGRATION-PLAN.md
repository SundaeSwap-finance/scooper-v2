# Scooper-v2 migration plan: post-audit sundae-v4 contracts

> **Status (2026-07-12):** D1 (fixture regen), workstream A (all shapes),
> B1 (fee-free semantics), and B2 (batch rules) are DONE — commits
> `2f7b4bb` + `45813fc` + B2, suite green at 188 passed / 0 failed against
> the real post-audit bytecode. SUNDAE-2613 canary flipped: ADA-receiving
> fills + partial-fill dispatch enabled. Multi-pool LP mint restriction
> lifted (SUN-102). **B2 found a real bug**: dispatch admitted orders in
> age (slot) order, but the route mandate requires per-pool transcript
> sequencing to follow canonical (TxOutRef) order — fixed by sorting
> candidates canonically, a canonical-append guard in the accumulator, and
> a build-time pre-flight in tx_builder; four regression tests pin all
> three batch rules. Remaining: the devnet load-test soak, C
> (claims/balance_fee generalization — hints currently gated to
> balance_fee == 0 pools), B3 fee-bearing mode (parked pending the
> fee-collection design decision). Note: `.cargo/config.toml` sets
> RUST_MIN_STACK=32MB — the UPLC evaluator overflows the default test
> stack on the traced post-audit validators.

Companion to `~/proj/sundae/sundae-v4/docs/offchain-migration-notes.md` (the
protocol-change catalog — read it first). This document maps each change onto
this codebase with file:line anchors, ordered by dependency. Reference
implementations for every tx shape live in the sundae-v4 devnet CLI
(`test/devnet/src/actions/order.ts` scoop builders, `kernel/scripts.ts`
parameterization), all devnet-verified against the real validators.

## What's already compatible (verify, don't rebuild)

The scooper is in better shape than expected — it's already on the
modular-constraints (PR #11) architecture:

- **OrderValidatorRedeemer** `{configs, entries}` with
  `{output_index, config_index}` entries and canonical ref-index resolution
  (tx_builder.rs:988–1221) — current shape. ✓
- **Route redeemer**: `List<List<RouteStep>>`, one route per order in input
  order, `[]` placeholders for non-route orders, per-order
  `transcript_step_index` from ops_order position (tx_builder.rs:1340–1437).
  This matches the Route Mandate; the devnet CLI had to be *fixed* to do
  this. **Verify** the ops_order position per pool is consistent with the
  canonical order-input walk (`check_route_uniqueness` demands strictly
  increasing step claims per pool along it) — add a test that scoops ≥3
  orders through one pool.
- **Per-step protocol-LP capture**: the cumulative-floor trick
  (tx_builder.rs:619–627) grows `total_lp` per step and keeps every entry's
  `fee_budget ≥ 0` — this is exactly what fee_split's per-entry
  `fee_budget >= 0` + exact aggregate pin require (the CLI had a
  deduct-from-last-entry bug here; the scooper doesn't). **Verify** the
  cumulative floor equals `floor(total_raw_fee × ps)` at the end (it does by
  construction — floor telescopes — but add the assertion to a test).
- **PoolRedeemer constructor indices** are unchanged (`Destroy` was appended
  at index 4; Action stays 3). Scooper's enum (types.rs:101–117) needs only
  the new `Destroy` variant for indexer tolerance.
- **Module Operate entry shapes** (`{pool_oref, config}`) unchanged.
- **Blueprint ingestion** (blueprint.rs:100–233) already matches the CLI's
  `emit-blueprint` camelCase titles; the devnet fixture
  (test/fixtures/devnet-blueprint.json) uses them.

## Workstream A — datum/redeemer shapes (mechanical, do first)

1. **OrderDatum** (types.rs:163–180): `budget`/`share_batcher` →
   `service_budget: BigInt` (mutable lifetime fee counter) +
   `max_per_execution: BigInt` (immutable per-scoop cap AND terminal
   settlement amount). Ripples: indexer parse (indexer.rs:1052), every fee
   site (workstream B), all test fixtures (`make_order`, scoop_tests.rs).
2. **SettingsDatum** (types.rs:460–473): drop `treasury_address`,
   `order_modules`, `min_share_batcher` (SUN-301). The constraint-decoding
   path that reads `order_modules` (indexer.rs:863–880) must instead
   dispatch on constraint *script hashes* from `module_scripts` config —
   the constraint list in OrderDatum is already `(hash, Data)` pairs, so tag
   dispatch by settings lookup is dead weight. Drop the
   `share_batcher < min_share_batcher` order-rejection filter
   (types.rs:470-ish comment; find call sites).
3. **ConstantSumConfig** (types.rs:528–540) + `PoolType::ConstantSum`
   (types.rs:504–517): `waive_fee_on_claim: bool` → `balance_fee: Rational`
   (SUN-310; 0 ≤ balance_fee ≤ fee; 0 = the old waiver). Update the CBOR
   round-trip test (types.rs:1442), CS Operate entry serialization, config
   extraction (indexer.rs:623–643), `pool_configs` operator-override schema
   (types.rs:798–812), and claims (workstream C).
4. **PoolMintRedeemer::MintLP** (types.rs:695–700): `pool_ident` →
   `pool_idents: Vec<Ident>` (SUN-102).
5. **Indexer tolerance for new redeemer fields** (indexer.rs:623–721): the
   Create-redeemer config extractors must accept the appended SUN-005 index
   fields — `constant_sum.Create{initial_state, pool_output_index}`,
   `fee_split.Create{config, pool_output_index, settings_ref_index,
   stake_list_ref_index}` — and the new `Destroy{entries}` constructor on
   every module redeemer (parse leniently: `initial_state`/`config` stays
   field 0 by contract). Same for `PoolRedeemer::Destroy` (index 4) and
   `pool_mint.CreatePool{seed_utxo, settings_ref_index}`.
6. **PoolConfig settings entries** now carry `module_params` +
   `mint_permission` — confirm the indexer ignores PoolConfig nodes (it
   appears to only parse SettingsDatum + OrderConfig); if any parser touches
   them, add the two fields.
7. **New types**: `FeeSettings {base_fee, fee_per_step, fee_destination:
   Destination}` (token-bound settings node) — needed by workstream B3
   only, but define with the shapes now.

## Workstream B — fee-system semantics (the core change)

The `tx_fee/n_orders + share_batcher surplus` model (tx_builder.rs:1620–1799)
is gone. Two modes, in order:

### B1. Fee-free mode (CURRENT deployment default — ship this first)

Per current direction, deployed OrderConfigs do NOT include the fee
constraint while the fee-collection design (excess-ADA-in-pool proposal) is
decided. In this mode the trade constraints alone bound the deduction:

- **Terminal (full) fill**: deduct exactly
  `min(max_per_execution, service_budget)` (`terminal_settlement`). The
  fill-ratio check on-chain adds this back to ADA-receiving legs
  (gross `min_received`) regardless of what's actually taken — so taking
  less just donates to the destination. This replaces `actual_fee =
  per_order_fee` (tx_builder.rs:1720–1731).
- **Partial fill (continuation)**: deduct `fee ≤ offered_this_fill ×
  service_budget / remaining_offered` (anti-micro boundary; pro-rata vs
  REMAINING, not original — the old cap at tx_builder.rs:1767–1788 uses
  `original_offered` and the `share_batcher` allowance: both wrong now),
  capped by `max_per_execution`. The continuation datum must carry
  `service_budget − fee` — everything else byte-identical except the
  order's own constraint entry (`remaining_offered`).
- **min_received is GROSS** for ADA-receiving legs (SUNDAE-2613): wherever
  fill-ratio/min-received feasibility is pre-checked (accumulator/validation),
  add `fee_deducted` back to the ADA delta first.
- **Economics rewire**: scooper compensation is now the deduction itself,
  fully decoupled from `tx_body.fee`. The `budget × n ≥ tx_fee` check and
  the placeholder-fee/rebuild dance (tx_builder.rs:37–42, 1620+) lose their
  order-side constraint; tx fee comes out of the scooper's take. New
  batch-admission filter: order's terminal settlement (or partial-fill
  deduction) ≥ the scooper's per-order cost target (reuse the `fee` config
  knob, redefined as min-compensation).
- **ADA-receiving basic orders are legal now** (`fee_taken >= 0` lower bound
  dropped on-chain) — remove any scooper-side rejection of
  sell-token-for-ADA basic orders.
- **Basic orders reject `Self` destinations** on-chain — filter them out at
  admission instead of building failing txs.

### B2. Batch-rule verification (cheap, do with B1) — DONE

The three devnet-discovered batch rules (migration notes, "Multi-order batch
scoop rules"): per-order route step claims, canonical per-pool ordering,
per-step protocol capture. Rules 1 and 3 were already satisfied; **rule 2
was NOT** — dispatch sorted candidates by `(provisional, slot)`, so two
same-pool orders whose hash order disagreed with their slot order built a
transcript `check_route_uniqueness` rejects (probe test reproduced the
on-chain `ExplicitErrorTerm` against real bytecode). Fixed in three layers:
canonical `(provisional, TxOutRef)` dispatch sort (scooper.rs),
`check_canonical_append` guard on every accumulator admission path
(accumulator.rs — catches the confirmed/provisional boundary), and a
build-time route-mandate pre-flight (tx_builder.rs). Regression tests in
scoop_tests.rs: `batch_route_claims_follow_canonical_order`,
`accumulator_rejects_noncanonical_same_pool_admission`,
`batch_two_routed_orders_share_pools`, `batch_protocol_capture_per_step`
(the last reconstructs gross fees from the built transcript and asserts
per-entry `fee_budget ≥ 0` + the telescoped aggregate floor).

### B3. Fee-bearing mode (flag-gated, LATER — pending fee-design decision)

When an order's OrderConfig requires the fee constraint: FeeSettings node
reference input (locate by unique token), fee-constraint withdrawal (Void
redeemer), continuation delta pinned to exactly
`order_fee = base_fee + fee_per_step × route_len`, inclusion gate
`order_fee ≤ max_per_execution`, and a datum-free output at
`fee_destination` covering `total_service_fee` (paying the FULL total forgoes
the ADR-0002 tx-fee netting; claiming the netting needs a fixed-point
fee iteration and minUTxO padding — the netting-vs-minUTxO interaction is
part of the open design discussion, don't build it yet). CLI reference:
`resolveFeeScoopContext`/`computeOrderFee` in actions/order.ts.

## Workstream C — CS claims (SUN-310)

claims.rs implements only the waived mode (= `balance_fee = 0`) with the old
cap_a. Rework to the generalized semantics (CLI reference: the claim solver
in actions/order.ts, devnet-verified):

1. Swap portion pinned at `balance_fee`: `v_increase = floor(dx·p_in ×
   bf_num/bf_den)` (waived mode falls out at bf=0).
2. `fee_budget` for the claim step = `floor(v_a_op × L / V_b) − L` computed
   on the OP-PORTION V (claim restored) — claim-independent. (Waived mode's
   `fee_budget = 0` falls out.) LP supply unchanged across the step.
3. **cap_a is GONE** (claim ≤ v_increase no longer applies).
4. **No-overshoot guard** (both solvers: `plan_waived_claim`,
   `plan_claim_meeting_floor`, `plan_rebalance_claim`): no traded asset may
   cross its pre-step balance point, measured against frozen `V_b`:
   - dx bound: `N·p_in·(r_in + dx) ≤ V_b`
   - claim bound: `N·p_out·(r_out − dy − claim) ≥ V_b`
   A full rebalance leaves zero claim headroom — sweep dx strictly inside
   the bound (the CLI caps at 90% of it).
5. `resolve_claim_shape` unchanged in spirit; the multi-asset rebalance
   path needs the guard applied per traded asset.

## Workstream D — config, blueprint, fixtures

1. **Regenerate test/fixtures/devnet-blueprint.json** from the new devnet
   deployment (`bun run cli emit-blueprint --profile devnet` in sundae-v4;
   a freshly deployed devnet with all post-audit scripts is running).
   This single step turns the whole scoop_tests suite into the migration
   verifier, since the harness evaluates against real bytecode.
2. **ModuleScripts** (types.rs:941–994): add optional `fee_constraint`
   entry; blueprint.rs mappings gain `(["fee_constraint", "feeConstraint"],
   "feeConstraint", "fee_constraint")`. (Governance/bond scripts are not
   scooper concerns.)
3. Config knob cleanup: `protocol-share` stays (fee_split preimages);
   `fee` becomes the min-compensation target (B1); add `fee-bearing`
   flag + FeeSettings token when B3 lands.
4. Devnet slot-config gotcha (from the CLI work): local yaci devnets have
   their own genesis — the scooper's `slot_config` is explicit config, so
   just document that devnet configs must match the running chain, and that
   upper validity bounds must stay inside the node's forecast horizon
   (~300 slots) or submission fails with `TimeTranslationPastHorizon`
   (VALIDITY_RANGE=180 slots at tx_builder.rs:122 is already safe).

## Workstream E — tests + end-to-end

1. Fixture shape updates across scoop_tests.rs/test_harness.rs
   (`make_order`, `make_cs_pool`, the 7a66bd76 rebalance regression —
   re-derive it under balance_fee semantics or mark it legacy).
2. New regression tests: ≥3-order single-pool batch (route uniqueness +
   per-step capture), partial fill with service_budget decrement, terminal
   settlement exactness, gross min_received on an ADA-receiving order,
   CS claim with balance_fee > 0 and the overshoot guard binding.
3. **Devnet soak**: the sundae-v4 devnet is deployed and hot; the CLI load
   test (`bun run test/devnet/src/load-test.ts`) generates hundreds of
   valid orders/minute and scrapes `SCOOPER_METRICS_URL` — point the
   migrated scooper at it and compare scooped-vs-submitted.

## Suggested order of attack

1. D1 (fixtures) + A (shapes) — gets it compiling against reality.
2. B1 (fee-free semantics) + B2 (batch-rule tests) — the scooper works on
   today's deployment.
3. E (test suite green, then devnet soak with the CLI load test).
4. C (claims/balance_fee) — needed only for CS bounty pools.
5. B3 (fee-bearing, flag-gated) — after the team's fee-collection decision.

Out of scope for the scooper: bond distribution (indexer/SDK concern),
governance, treasury harvest.
