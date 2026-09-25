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

The binary takes one or more `--config` files. Each file is applied in order, and can override settings from other files. See the [./config](./config/) directory for example configurations. Any config value can be overridden by an environment variable: to override `persistence.sqlite.filename`, set `SCOOPER_V2_PERSISTENCE__SQLITE__FILENAME`.

The scooper is powered by the [Acropolis](https://github.com/input-output-hk/acropolis) rust library, which serves as a client to the Cardano network. Config for the different acropolis modules is described there. The required settings are
 - `acropolis.global.startup.network-name` (the Cardano network to connect to)
 - `acropolis.module.peer-network-interface.node-addresses` (an array with the address of at least one Cardano node).

State goes in SQLite at `persistence.sqlite.filename`, resolved against the working directory. Without a filename the whole index lives in memory and is rebuilt on every start. You should probably provide a filename.

A v4 execution config needs these additional settings

| Config key | Needs |
| --- | --- |
| `protocol.v4.execution.butane.deployment-file` | A named Butane deployment file |
| `protocol.v4.mempool.socket-path` | A node's IPC socket (to read the mempool) |
| `protocol.v4.execution.scooper-secret-key-file` | The key file (prefer this over the inline key) |
| `server.tls_cert` / `tls_key` | Cert and key, if TLS is enabled |
| `server.public_address` | `-p 9998:9998` |

Pool families: constant product, constant sum, concentrated liquidity, and stableswap. Stableswap pools need `module-scripts.stableswap` and carry a config that changes on chain; see [docs/stableswap.md](./docs/stableswap.md) for how they are recognised, priced, and what to do when the module is deployed.

The server listens on `server.address` (`0.0.0.0:9999` by default) and serves `/dashboard`, `/status`, `/health`, `/metrics`, `/failures`, `/events` (SSE), `/pause`, `/resync-from-acropolis`, and per-protocol `/v3/…` and `/v4/…` listings of `pools`, `orders`, `spent-orders`, and `spent-pools`. Setting `server.public_address` opens a second listener carrying only the strategy-intent endpoints and `/health`.

### Pool allowlists

`protocol.v4.execution.pool-allowlists` closes named pools to everyone except listed credentials. Keys are pool idents and values list 28-byte key or script hashes, all in hex:

```json
{
  "protocol": { "v4": { "execution": { "pool-allowlists": {
    "3b809fd966274082c16ea6e670dd7c5d384a44a8989a70fd9fe4cc45": {
      "credentials": ["<key-or-script-hash>", "<key-or-script-hash>"]
    }
  } } } }
}
```

A pool that isn't listed is unrestricted. Your scooper serves an order against a restricted pool only when every credential that could act on the order is listed:

- **Owner:** nobody outside the list can satisfy the order's owner multisig. An owner that anyone satisfies (an empty `all`, a bare time bound, or an `any` with one unlisted branch) is denied.
- **Destination:** the payment credential the fill is paid to is listed. `self` destinations fall under the owner check.
- **Strategy orders:** the same owner rule applies to `auth`, and every entry in `final_destinations` must be listed.

A restricted pool is invisible to a denied order, including as a hop inside a longer route.

**The list binds only your scooper.** A denied order stays valid on chain, and any other authorized scooper can fill it. A restriction holds only if every authorized scooper runs the same list, so pool allowlists have to be coordinated across operators. If the settings datum authorizes any scooper (no `authorized_scoopers` list), anyone can fill what you decline. The scooper logs a warning at startup in that case.

The scooper refuses to start if an ident or credential isn't 28-byte hex, a pool is listed twice, or a pool entry has a field other than `credentials`. Hex case doesn't matter. A misspelled `pool-allowlists` key is ignored like any other unknown key, so check that the restriction is live:

- At startup, each listed pool logs `pool is allowlist-restricted`. A listed pool that isn't indexed logs a warning instead, usually because the ident is mistyped.
- `/v4/pools` marks restricted pools `"restricted": true`, and `/v4/pool/<ident>` lists denied orders as non-executable with the reason.
- `/v4/strategy-intents/<id>` reports `"state": "not-permitted"` for an intent that only a restricted pool could serve.
- The `scooper_restricted_pools` metric counts the configured pools.

[`config/preview-allowlist-test.json`](./config/preview-allowlist-test.json) is a preview overlay that closes one pool to an unreachable credential. Pass it as a second `--config` to watch every order on that pool get turned away.

### Pool blacklist

`protocol.v4.execution.blacklisted-pools` lists pool idents, in hex, that your scooper never scoops:

```json
{
  "protocol": { "v4": { "execution": { "blacklisted-pools": [
    "3b809fd966274082c16ea6e670dd7c5d384a44a8989a70fd9fe4cc45"
  ] } } }
}
```

A blacklisted pool is left out of every batch: swaps (including as a hop inside a longer route), deposits, withdrawals and constant-sum claims. Use it for a pool the scooper can't currently fulfill, such as one whose on-chain config it can't recover.

The scooper refuses to start if an entry isn't 28-byte hex or a pool is listed twice. Hex case doesn't matter. A misspelled `blacklisted-pools` key is ignored like any other unknown key, so check that the blacklist is live: at startup, each blacklisted pool logs `pool is blacklisted`. A blacklisted pool that isn't indexed logs a warning instead, usually because the ident is mistyped.

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