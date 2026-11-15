# Cardano-Swaps order-book fills

[Cardano-Swaps](https://github.com/fallen-icarus/cardano-swaps) is the DeFi
Kernel's peer-to-peer order book. A one-way swap is a limit order resting in
its owner's **own** address — every owner gets a distinct address sharing the
swap script as payment credential — holding three non-transferable beacon
tokens and an inline datum that names the pair and the owner's limit price.

**Anyone may fill it.** The filler spends the order with the `Swap` redeemer,
takes some of the offered asset, deposits the asked asset at or above the
datum's price, and recreates the order at the same address. The scooper
treats each fillable order as a router conversion edge, so a Sundae order can
be filled out of the order book and a resting order can be filled against a
Sundae pool.

Everything is config-driven and closed by default. Without
`execution.cardano-swaps`, or when the swap script's reference UTxO is not
indexed, the scooper runs exactly as before and no edge reaches the router.

## Why this is the cheapest external edge we have

The limit price lives in the datum, so there is no oracle, no price feed, no
withdrawal and no validity-band coupling unless the order sets an expiration.
A fill is one spend, one continuing output and one reference input:

| | per fill |
|---|---|
| Script budget | ~336K memory, ~115M steps |
| Reference script | 3473 bytes |
| Observed mainnet fee | ~0.28 ADA for a lone fill |

The protocol's own benchmarks fit 29 fills in one transaction for about
2.13 ADA, so the marginal cost of an extra fill is roughly 0.05 ADA. That is
cheaper than one of our own pool steps.

## What the validator requires

Established from the validator source (`valid_swap` in
`aiken/lib/cardano_swaps/one_way_swap/utils.ak`) and regression-tested by
replaying two mainnet fills and mutating them (`src/sundaev4/cardano_swaps.rs`,
`replay_tests`): `ca27ff15…` takes 1000 ADA for 230 USDM at 23/100, and
`9d8260ab…` takes 1 USDCx for 1.5 ADA at 3/2, the direction where ADA is the
asked asset and gets deposited.

1. The input must hold exactly one of each of its three beacons.
2. Exactly one output must sit at the same address with the datum identical
   except `prev_input == Some(input_ref)`, carrying no reference script, the
   same three beacons, and no extraneous assets.
3. **The price check is one integer cross-multiplication:**
   `offer_taken · price_num <= ask_given · price_den`. Partial fills are free
   and there is no rounding slack to respect.
4. `ask_given >= 0`: the asked asset may never be taken out.
5. ADA may only be deposited, unless ADA is itself one side of the pair.
6. If the datum sets an expiration, the transaction's upper validity bound
   must be set and at or below it.

Rule 3 is why this integration needed no new arithmetic: with the edge's
input set to the asked asset, its output to the offered asset and its rate to
`(price_den, price_num)`, our existing floored conversion math satisfies the
validator's inequality exactly.

The price is a floor on what we deposit and a ceiling on what we take, so
depositing more or taking less is always accepted. Only the opposite fails.

## Beacons

Beacon names are `sha2_256(serialise_data(ctor))` over the protocol's
`SwapAsset` type: constructor 0 for the offer beacon, 1 for the ask beacon,
2 for the directional trading pair. ADA is `(#"", #"")` and needs no special
casing.

This module derives them independently, so an order whose datum disagrees
with its own assets is skipped rather than trusted — such a UTxO is its
owner's mistake and the validator would reject our fill. The derivation is
checked against the repo's golden vectors and against the live datums.

Deriving them also means we can compute the beacon for a pair we care about
and filter the book without parsing every datum, which matters once the book
is large.

## Configuration

```json
"execution": {
  "cardano-swaps": {
    "enabled": true,
    "swap-script-hash": "e5a22e4c31db20bce1c8b081f8e4009683990a33157947d75030deb8",
    "swap-ref-utxo": "e7ab2e2e12b5ae51110758e3b668b31cfa215187d24c388bb5e736b6338d2a07#0",
    "beacon-policy": "274765b4c626c28d18752176b59c0ff63db56b8305c1daa49c9879fe",
    "min-keep-lovelace": 3000000
  }
}
```

Those are protocol v2's mainnet hashes (TxPipe-audited 2026-09-21, Plutus V3).
Protocol v1 is a separate, legacy Plutus V2 deployment with its own hashes and
would need its own config stanza if we ever wanted it.

The reference-script UTxO is published by a **third party**, the
cardano-reference-scripts protocol, so it could in principle be spent. The
integration closes its edges when the UTxO is no longer indexed rather than
building a transaction that cannot be witnessed; publishing our own copy
would remove the dependency.

`min-keep-lovelace` is headroom, not a protocol rule: the validator pins the
traded amounts, but a continuing output whose offer is ADA still has to clear
the ledger's own min-UTxO floor.

## Filling a resting order against our own pool

With `foreign-fills` enabled, a cycle that sends out no Sundae order — none
pending, or none that could be batched — looks for a resting order whose price
is inside one of our pools' and fills it for the protocol. Sundae orders keep
priority: the attempt runs only after they have had their turn. The transaction has no Sundae order in it — the order it serves
is simply held at a different order contract:

```
inputs      our pool, the resting swap order
outputs     pool continuation (reserves + landed surplus), order continuation
redeemers   pool Action with one swap entry; the order's public Swap
withdrawals the pool's own modules, fee_split, fairness — no order validator,
            no fee constraint
```

The pool's emission goes into the order; the asset the order releases goes
into the pool; whichever side is ADA carries the difference. Exactly one side
must be ADA, because only lovelace above a pool's declared reserve is surplus
the treasury can sweep. Value conservation fixes the split, so the builder
lands the residual of `inputs − outputs − fee` on the pool output, and the
transaction pays for itself: the scooper fronts nothing.

Every pool family is eligible — constant product, constant sum, stableswap,
and banded concentrated liquidity — each sized by its own curve; a banded
ladder is walked through the router's cached view. A stableswap or banded
pool whose hash-pinned config the scooper has not indexed is skipped, as the
builder would refuse it. `min-pool-lovelace` applies to constant-product
pools only: their curve quotes a meaningless price when the declared ADA
reserve is a few lovelace, while the other families bound a step by their
own state.

Two gates decide whether a fill happens. The planner sizes the pool step to
maximize the lovelace released, skips pools in flight or carrying an
allowlist, and screens against `fee-estimate-lovelace + min-revenue-lovelace`
before anything is built. Then, after the transaction is rebuilt with its
exact fee, the landed residual must still clear `min-revenue-lovelace` or the
transaction is not submitted. The second check is the one that decides; the
first only avoids building losers.

Foreign fills never share a transaction with Sundae orders. A fee-bearing
scoop pins the designated pool's surplus delta exactly, so a fill's surplus
could not land there; keeping the shapes apart avoids depending on the
`==` → `>=` relaxation in `fee.ak`.

```json
"cardano-swaps": {
  "...": "...",
  "foreign-fills": {
    "enabled": true,
    "min-revenue-lovelace": 1000000,
    "fee-estimate-lovelace": 1000000,
    "min-pool-lovelace": 100000000
  }
}
```

## Scope today

Only **one-way** swaps. The two-way (liquidity swap) protocol quotes both
directions from a single UTxO and has never been used on chain, on either
version. It would mean two mutually exclusive edges per UTxO, which the
router cannot express yet.

Liquidity is the real constraint, not the code. As of 2026-10-14 the v2
one-way book held a single live order and had seen 16 fills ever, by two
wallets. We are building this partly to be one of those venues: the protocol
is explicit that a resting order is inert until an arbitrageur fills it, so a
second independent filler is what makes resting an order there reasonable.

## Open items

- **Mixing a foreign fill into a scoop with Sundae orders.** See
  [docs/fee-system.md](./fee-system.md): a fee-bearing scoop pins the
  *designated* pool's surplus delta exactly, so surplus from a fill can land
  on any other pool in the scoop but not on that one. Landing it on a single-pool scoop's designated pool needs the
  `==` → `>=` relaxation in `fee.ak`.
