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

The server listens on `server.address` (`127.0.0.1:9999` by default) and serves `/dashboard`, `/status`, `/health`, `/metrics`, `/failures`, `/events` (SSE), `/pause`, `/resync-from-acropolis`, and per-protocol `/v3/…` and `/v4/…` listings of `pools`, `orders`, `spent-orders`, and `spent-pools`. Setting `server.public_address` opens a second listener carrying only the strategy-intent endpoints and `/health`.

`server.address` takes one address or a list, so a box can serve the
operational surface on loopback and on its tailnet address without serving it
anywhere else:

```json
"server": { "address": ["127.0.0.1:9999", "100.100.132.62:9999"] }
```

Addresses are listed, not discovered by interface name. A control surface that
finds where to listen can begin listening somewhere new when an interface
appears; one that is told cannot. A wildcard bind still works and now logs a
warning naming what it exposes.

### Public and private instances

The operational surface is unauthenticated. `/pause` stops scooping and
`/resync-from-acropolis` restarts the indexer, so anything that can reach
`server.address` can stop the scooper. It belongs on loopback and the tailnet,
never on a public interface.

When the strategy-intent endpoint is published, run two instances:

| | private | public |
| --- | --- | --- |
| `role` | `scooper` (default) | `observer` |
| signing key | yes | **refuses to start with one** |
| `server.address` | loopback + tailnet | loopback + tailnet |
| `server.public_address` | unset | `0.0.0.0:9998` |
| builds transactions | yes | no |
| `/pause`, `/resync-from-acropolis` | yes | 404 |

An observer indexes the chain, accepts strategy intents, validates them,
stores them and gossips them to `strategy_peers`. It holds no key and starts
no scooper loop. If a key is configured it refuses to start rather than run
with one loaded: a key on the public box is the thing the split exists to
prevent, and it should be loud.

Point the private instance at the public one with `strategy_peers` so intents
posted publicly reach the instance that can execute them.

### Provisioning the public observer

The CloudFormation template in `sundae-scooper-server` creates the instance,
its security group, the load balancer and the DNS name. It installs nothing.
These are the steps that turn a bare instance into the observer.

1. **Join the tailnet.** SSH in on the public address, install Tailscale and
   bring it up with `--advertise-tags=tag:scooper-public`. Then note the
   address it gets: `tailscale ip -4`.

2. **Install the binary.** Take the release matching the signer's version, so
   the two agree about everything they both parse. Put it at
   `~/scooper-v2/scooper-v2` with the unit from `deploy/scooper-v2.service`.

3. **Write the config.** Run the generator ON THE SIGNER, which is where the
   authoritative mainnet config lives, then copy the result across:

   ```
   python3 deploy/mk-observer-config.py --tailnet-ip <the address from step 1> \
     > observer-config.json
   ```

   It derives from the live file rather than from the example in this repo,
   because mainnet deploys with `--keep-config` and the two have drifted
   before. The six things it changes, and nothing else, are listed at the top
   of that script.

4. **Point the signer back.** The observer forwards accepted intents to the
   signer's tailnet address on 9998, so the signer needs
   `server.public_address` set to its own tailnet address. Not `0.0.0.0`:
   that surface should be reachable from the observer and from nowhere else.
   This is a config edit and a restart on the signer.

5. **Check it is actually gossiping.** Startup logs the peer list, and warns
   if an observer has none. An observer with no peers accepts intents and
   drops them, which nothing else would tell you.

Once it is up, the public address from the template can be removed. Because
it is declared in a `NetworkInterfaces` block that replaces the instance on
change, removing it means rebuilding the box. That is cheap here: an observer
holds no key and re-seeds from Blockfrost in minutes.

### First sync

`config/mainnet-v4.json` ships no `protocol.bootstrap` block, deliberately. On an
empty database the scooper replays the chain from `protocol.v4.starting-point`,
which reconstructs pools, orders, module configs and reference scripts straight
from the ledger. That replay is slower than a bootstrap and strictly more
complete, and it is the recovery path to reach for when a scooper's state is
wrong.

If you do configure a bootstrap source, the scooper checks it can enumerate
UTxOs by payment credential and refuses it otherwise:

> this bootstrap source cannot enumerate UTxOs by payment credential, so it
> cannot see orders at user-staked addresses (addr1z...). Seeding from it and
> then starting the indexer at the bootstrap tip would silently discard every
> pending order.

That is not hypothetical. A bootstrap blind to staked addresses seeds an
order book that looks healthy and is missing live orders, and the indexer then
starts at the bootstrap tip and never revisits them — the orders sit unfilled
with nothing in the logs to say why. Kupo (`/matches/{credential}/*`) and
Blockfrost (`/addresses/{credential}/utxos`) both qualify.

`--wipe-db` on a deploy therefore only makes sense with a bootstrap configured.
Without one it leaves the scooper to replay from the starting point, which is
correct but takes as long as the replay takes.

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