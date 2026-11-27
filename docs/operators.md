# Scooper operator guide

This guide takes a new operator from nothing to a scooper that fills orders
on SundaeSwap v4 mainnet. It also covers upgrades and recovery.

A scooper is a program that builds the transactions ("scoops") that execute
user orders against pools. Only keys on the protocol's authorized scooper
list can scoop.

## What you need

- A Linux x86_64 host.
- A synced `cardano-node` on the same host. The scooper uses its N2N port to
  follow the chain and its socket to submit and to read the mempool.
- The `scooper-v2` binary of the current release, from the
  [releases page](https://github.com/SundaeSwap-finance/scooper-v2/releases),
  or the Docker image (see [Docker](#docker)).
- About 70 ADA for the scooper wallet.

You do not need Blockfrost, Kupo or any other indexer.

## Get started

### 1. Make the signing key

Make the key on the scooper host and keep it there.

```sh
umask 077; mkdir -p keys
openssl genpkey -algorithm ed25519 -out keys/scooper.pem
openssl pkey -in keys/scooper.pem -outform DER | tail -c 32 | xxd -p -c 64 > keys/scooper.skey.hex
openssl pkey -in keys/scooper.pem -pubout -outform DER | tail -c 32 | xxd -p -c 64 > keys/scooper.vkey.hex
python3 -c 'import hashlib; print(hashlib.blake2b(bytes.fromhex(open("keys/scooper.vkey.hex").read().strip()), digest_size=28).hexdigest())'
```

The last line prints your scooper key hash.

### 2. Get the key authorized

Send the key hash to SundaeSwap. The secret key never leaves the host.
SundaeSwap adds the hash to the authorized scooper list on chain. Until then
your scooper follows the chain but its scoops are not valid.

### 3. Write the config

Copy [`config/mainnet-v4.json`](../config/mainnet-v4.json) to the host and
change these values. Leave every other value as it is.

| Key | Set it to |
| --- | --- |
| `protocol.v4.execution.scooper-secret-key-file` | the path of `keys/scooper.skey.hex` |
| `protocol.v4.mempool.socket-path` | the socket of your node |
| `acropolis.module.peer-network-interface.node-addresses` | the N2N `host:port` of your node |
| `acropolis.module.genesis-bootstrapper.byron-genesis-file` and `shelley-genesis-file` | the genesis files of your node |
| `protocol.v4.execution.submit-url` | a fallback that is used only if the socket submit fails. Leave the placeholder if you have no Blockfrost project. |

The user that runs the scooper must be able to connect to the node socket.
Run the node as the same user, or open the permissions of the socket.

The database is the file named by `persistence.sqlite.filename`, relative to
the working directory.

### 4. Start it

```sh
./scooper-v2 --config config.json
```

For a service, use the unit in
[`deploy/scooper-v2.service`](../deploy/scooper-v2.service). Change the user
and the three paths in it to yours.

On an empty database the scooper replays the chain from
`protocol.v4.starting-point`, the block before the v4 protocol started. From
the replay it learns the pools, the orders, the module configs, the reference
scripts, the settings and its own wallet. The replay time grows with the age
of the protocol. On preview, a replay of 7.0 million slots took 67 minutes on
2026-11-27.

### 5. Fund the wallet

The wallet is the enterprise address of your key. Build it with
`cardano-cli address build --payment-verification-key-file <vkey envelope> --mainnet`,
or ask SundaeSwap for it with your key hash.

Send it **at least three UTxOs of 20 ADA or more**. A scoop spends one wallet
UTxO for the fee and holds a second one as collateral. With one UTxO, the
next scoop waits until the first one is on chain.

Fund the wallet **after** the scooper has started its sync. The scooper
learns its UTxOs from the chain as it follows it.

### 6. Check that it works

The scooper serves its status on `server.address`, `127.0.0.1:9999` by
default.

| Check | Healthy |
| --- | --- |
| `curl -s 127.0.0.1:9999/health` | answers |
| `curl -s 127.0.0.1:9999/status` | under `v4`, `sync_pct` is 100 and `tip_slot` follows `network_tip_slot` |
| `http://127.0.0.1:9999/dashboard` | pools and pending orders are listed |
| `curl -s 127.0.0.1:9999/metrics \| grep scooper_orders_scooped_total` | goes up after your key is authorized and orders arrive |

The status surface has no authentication, and `/pause` stops scooping. Keep
`server.address` on loopback or on a private network. Never put it on a
public interface.

For alerts and a dashboard, see [`deploy/README.md`](../deploy/README.md).

## Upgrade to a new release

1. Read the release notes. They say if the config changes.
2. Stop the scooper.
3. Replace the binary.
4. If the notes name config changes, make them (see the next section).
5. Start the scooper and repeat the checks of step 6 above.

A release with no config change keeps its database.

## When the protocol adds a module

SundaeSwap can add a pool type or a new build of a module without a change
to the pool or order scripts. A pool names its modules by script hash. The
scooper looks each hash up in `protocol.v4.execution.module-scripts`. It does
not scoop a pool that names a hash that is not in that map, and it logs:

```
pool's enabled actions reference unknown module hash(es); the pool is not scooped
```

Your other pools are not affected. To scoop the new pools:

1. Take the new `module-scripts` entries from the announcement, or from
   `config/mainnet-v4.json` of the release that follows it.
2. Put them in your config. An entry is one `{ "hash", "ref-utxo" }` object,
   or a list of them when more than one build of a module is live:
   ```json
   "fee-split": [
     { "hash": "<the build your config has now>", "ref-utxo": "<...>" },
     { "hash": "<the new build>", "ref-utxo": "<...>" }
   ]
   ```
   Keep the old entry. Pools that were created with it still name it.
3. Stop the scooper, delete the database file, and start it. The scooper
   records a reference script when the replay passes its publication, so a
   new entry needs a new replay.

### Release 0.8.0: concentrated liquidity

Release 0.8.0 can scoop concentrated-liquidity pools. It needs two new
`module-scripts` entries on mainnet:

| Role | Hash |
| --- | --- |
| `banded-concentrated-liquidity` | `5c1ec4813a3d5575d521d03682a36b43b8122a4a99c4f2cdcd742194` |
| `fee-split`, second entry | `877b40ceda282cf35794c7fb1af72d00280719f8b39515c7c9b1564e` |

The two reference UTxOs do not exist until SundaeSwap publishes the scripts.
The announcement of the publication gives them. Until you add the entries,
0.8.0 runs with the config you have and scoops the pools it scooped before.

`concentrated-liquidity` is accepted as a name for the
`banded-concentrated-liquidity` role.

## Recover a scooper

If the state of a scooper is wrong, rebuild it from the chain:

1. Stop the scooper.
2. Move the database file away.
3. Start the scooper. It replays from `starting-point`.

Do not add a `protocol.bootstrap` block to make this faster unless you know
the source can list UTxOs by payment credential. The replay is slower and
complete. See [First sync](../README.md#first-sync) in the README.

## What log lines mean

| Log line | Meaning | What to do |
| --- | --- | --- |
| `pool's enabled actions reference unknown module hash(es)` | The pool names a module that your config does not list. | Add the module entry. See above. |
| `pool's scoop action is disabled` | The security council disabled the pool. | Nothing. The scooper scoops it again when it is enabled. |
| `try_add failed error=...` | One order cannot fill now, for the reason given: a price limit, an empty pool, a budget that is too small. | Nothing. The order waits or its owner cancels it. |
| `no orders could be added to batch` | No pending order can fill now. | Nothing. |
| `temporarily quarantining order: fails eval in isolation (suspected scooper bug)` | The scooper built a scoop that the scripts reject. | Report it to SundaeSwap with the order id. Other orders are not affected. |
| `permanently quarantining order` | The order can never be scooped as it was made. | Nothing. Its owner must cancel it. |

## Docker

Images are at `ghcr.io/sundaeswap-finance/scooper-v2`, tagged with the
version. Mount your config at `/app/config.json` and a writable volume at
`/app/data`, and point the database into that volume. The node socket and
the key file must be reachable in the container at the paths the config
names.

In a container, set `server.address` to `0.0.0.0:9999`, and publish the port
on the loopback address of the host only, as below.

```sh
docker run --rm \
  -p 127.0.0.1:9999:9999 \
  -v "$PWD/config.json:/app/config.json:ro" \
  -v scooper-data:/app/data \
  -e SCOOPER_V2_PERSISTENCE__SQLITE__FILENAME=/app/data/scooper-v2-mainnet.db \
  ghcr.io/sundaeswap-finance/scooper-v2:<version>
```

## More

The [README](../README.md) is the reference for the settings this guide does
not need: pool allowlists, the pool blacklist, rate guards, the public
observer instance, and the v3 protocol.
