# Running a mainnet v4 scooper

The steps are in the [operator guide](../docs/operators.md): the key, the
authorization, the config values to change, the wallet, the checks, upgrades
and recovery.

`mainnet-v4.json` is the layout that scooped on mainnet on 2026-09-19
(tx `ae7c7427…`, block 13,959,751). It needs a `cardano-node` on the same
host and nothing else: no Blockfrost, no Kupo. It has no `bootstrap` block,
so on an empty database the scooper replays the chain from `starting-point`
(slot 198093244, the block before the v4 protocol started).

This config is v4 only. If you also scoop v3 with the same binary, a v3 sync
from its `starting-point` replays two years of history, and a v3 Blockfrost
bootstrap walks more than 2,000 pools.
