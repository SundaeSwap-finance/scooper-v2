# Cardano-Swaps v2 script bytes

Exact bytes of the mainnet reference scripts, fetched from Koios
`/script_info` (creation tx `e7ab2e2e12b5ae51110758e3b668b31cfa215187d24c388bb5e736b6338d2a07`):

| file | hash | size |
|---|---|---|
| `one_way_swap.v3.cbor.hex` | `e5a22e4c31db20bce1c8b081f8e4009683990a33157947d75030deb8` | 3473 B |
| `one_way_beacon.v3.cbor.hex` | `274765b4c626c28d18752176b59c0ff63db56b8305c1daa49c9879fe` | 4098 B |

Both are the single-CBOR-wrapped Plutus V3 scripts. The beacon script is
already applied to the swap script's hash. The cardano-swaps repo at HEAD
compiles to different bytes (Aiken 1.1.22; `ef69e7b2…`), so these files, not
a fresh build, are what a testnet rehearsal must publish to exercise the
mainnet validator.
