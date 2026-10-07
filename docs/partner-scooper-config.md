# Running a scooper against the v4 launch deployment

Start from the example config for your network — `config/preview-v4.json` or
`config/preprod-v4.json` — and edit only the fields in the first section.
Everything else in those files describes the deployment itself and must match
it exactly.

Run the binary with `scooper-v2 --config <your-config.json>` (or
`cargo run --release -- --config …`). `deploy/scooper-v2.service` is an
example systemd unit.

## Fields you must set

| Field | What to set |
| --- | --- |
| `protocol.v4.execution.scooper-secret-key` or `scooper-secret-key-file` | Your Ed25519 payment secret key. The file form accepts a cardano-cli `.skey` JSON (`cborHex`) or raw 64-char hex. The blake2b-224 hash of this key's verification key must be in the on-chain settings `authorized_scoopers` list — SundaeSwap adds it for you. Prefer the file form; do not commit the key. |
| `protocol.bootstrap.project-id` | Your own Blockfrost project id for the network. Used once per start-up to hydrate pools, orders, settings and reference scripts. |
| `protocol.v4.execution.submit-url` | Where signed scoops are submitted: your Blockfrost `/tx/submit` URL (with your project id) or your own node's submit endpoint. |
| `acropolis.module.peer-network-interface.node-addresses` | `host:port` of a cardano-node you run, node-to-node protocol. The scooper follows the chain through this node. |
| `protocol.v4.mempool` (optional, preview example) | `socket-path` = your node's node-to-client IPC socket, `network-magic` = 2 (preview) / 1 (preprod), `execute: true`. With this section the scooper watches the mempool and chains scoops onto unconfirmed orders; without it the scooper still works, one confirmed batch at a time. |
| `persistence.sqlite.filename` | Local path for the scooper's state database. Delete it to force a clean re-sync from `starting-point`. |
| `server.public_address` | Bind address for the local status server. |

## Funding

The scooper pays each scoop's transaction fee from its own wallet and is paid
the protocol's scooper fee inside the same transaction. Fund the **enterprise
address** of your scooper key (no stake part) with test ADA before the first
run — 100 tADA is comfortable; steady state is roughly fee-neutral.

## Fields you must not change

These describe the deployed scripts and on-chain settings. A mismatch does
not degrade gracefully: scoops either never build or are rejected by the
node.

- `protocol.v4.pool-script-hash`, `order-script-hashes`,
  `settings-script-hash`, `settings-nft`, `pool-nft-policy`,
  `fee-settings-token`
- `protocol.v4.execution.module-scripts` — every module hash and its
  reference-script UTxO
- `protocol.v4.execution.fee`, `protocol-share`, `cost-per-pool-lovelace`,
  `cost-per-step-lovelace`, `max-tx-ex-mem`
- `protocol.v4.execution.plutus-v2-cost-model`, `plutus-v3-cost-model`
- `protocol.v4.execution.slot-config` (per network: preview
  `0 / 1666656000000`, preprod `86400 / 1655769600000`)
- `protocol.v4.starting-point` — the deployment block. Keep it: a later
  start misses the settings and reference-script UTxOs; the bootstrap
  fills state forward from here.
- `acropolis.global.startup.network-name` and the
  `mithril-snapshot-fetcher` section (per network)

When SundaeSwap publishes a new deployment, these values change together;
take the whole updated example rather than patching fields.

## Verifying it works

1. Start-up ends with `scooper synced with chain tip, batch processing
   enabled n_pools=<N> n_orders=<M>`. On preview, `n_pools` is currently 14.
2. Place any small order (or wait for organic flow). A healthy scooper logs
   `multi-pool scoop tx submitted tx_hash=…` within a few seconds of seeing
   an order, and the transaction confirms on-chain.
3. `no orders could be added to batch` with `skip_*` counters is normal when
   pending orders cannot be filled (for example, a minimum above the pool's
   fixed rate); it is not an authorization failure.
4. An unauthorized key fails at the node with a settings-related script
   error at submission — if you see that, confirm the hash of your key's
   verification key is in the settings datum's `authorized_scoopers`.
