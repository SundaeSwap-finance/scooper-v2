# Sundae Scooper v2

This repo contains the binary which runs v2 the Sundae Labs scooper, powering v4 of the SundaeSwap DEX. A "scooper" is a program run by a large set of operators to produce the "scoops" (transactions) that make swaps for users.

## Running

preview:

```sh
scooper-v2 --config config/preview.json
```
mainnet:
```sh
scooper-v2 --config config/mainnet.json
```

You can find prebuilt binaries in the releases of this project.

The binary takes one or more `--config` files. Each file is applied in order, and can override settings from other files. See the [./config](./config/) directory for example configurations. Any config value can be overridden by an environment variable: to override `persistence.sqlite.filename`, set `SCOOPER_V@_PERSISTENCE__SQLITE__FILENAME`.

The scooper is powered by the [Acropolis](https://github.com/input-output-hk/acropolis) rust library, which serves as a client to the Cardano network. Config for the different acropolis modules is described there. The required settings are
 - `acropolis.global.startup.network-name` (the Cardano network to connect to)
 - `acropolis.module.peer-network-interface.node-addresses` (an array with the address of at least one Cardano node).

State goes in SQLite at `persistence.sqlite.filename`, resolved against the working directory. Without a filename the whole index lives in memory and is rebuilt on every start. You should probably provide a filename.

A v4 config needs `protocol.v4.module-scripts`: every module script in the deployment, keyed by role (`pool`, `order`, `fee-split`, `fairness`, `pool-mint` and `settings` are required; the pool-type and order-constraint modules are optional). The indexer classifies pools and order constraints by these hashes whether or not this scooper executes, so they describe the deployment rather than the operator. Each entry's `ref-utxo` — where the script is published on chain — is only needed to build transactions, so it may be omitted when running as an indexer; the scooper refuses to start if `execution` is set and any declared module lacks one.

A v4 execution config needs these additional settings

| Config key | Needs |
| --- | --- |
| `protocol.v4.execution.butane.deployment-file` | A named Butane deployment file |
| `protocol.v4.mempool.socket-path` | A node's IPC socket (to read the mempool) |
| `protocol.v4.execution.scooper-secret-key-file` | The key file (prefer this over the inline key) |
| `server.tls_cert` / `tls_key` | Cert and key, if TLS is enabled |
| `server.public_address` | `-p 9998:9998` |

The server listens on `server.address` (`0.0.0.0:9999` by default) and serves `/dashboard`, `/status`, `/health`, `/metrics`, `/failures`, `/events` (SSE), `/pause`, `/resync-from-acropolis`, and per-protocol `/v3/…` and `/v4/…` listings of `pools`, `orders`, `spent-orders`, and `spent-pools`. Setting `server.public_address` opens a second listener carrying only the strategy-intent endpoints and `/health`.

### Docker

Images are published to `ghcr.io/sundaeswap-finance/scooper-v2`, tagged with the version.

To use the image, you must mount your configuration at `/app/config.json` and a writable data volume at `/app/data` (and point `persistence.sqlite.filename` into that dir). For example, to use our standard mainnet config:

```sh
docker run --rm \
  -p 9999:9999 \
  -v "$PWD/config/mainnet.json:/app/config.json:ro" \
  -v scooper-data:/app/data \
  -e SCOOPER_V2_PERSISTENCE__SQLITE__FILENAME=/app/data/scooper-v2.db \
  ghcr.io/sundaeswap-finance/scooper-v2:v0.6.0
```