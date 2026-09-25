# Running a mainnet v4 scooper

`mainnet-v4.json` is the layout that scooped on mainnet on 2026-09-19
(tx `ae7c7427…`, block 13,959,751). It needs a co-located `cardano-node` and
nothing else: no Blockfrost, no Kupo.

## How it gets its state

There is no `bootstrap` block. The scooper syncs from `starting-point`
(slot 198093244, the block before the v4 protocol boot) through the node's
N2N port. On a synced node that takes about a minute today and grows with
protocol age. It learns pools, module configs, reference scripts, settings and
your wallet UTxOs from the chain, so **fund the scooper wallet after the
scooper has started syncing**, or send the funds to yourself once so the
scooper sees the transaction.

Sync-from-`starting-point` only works if the database is empty. If you
change `starting-point` or the config's module scripts, delete
`scooper-v2-mainnet.db` before starting.

## Key

Generate the signing key on the scooper host and keep it there:

```sh
umask 077; mkdir -p keys
openssl genpkey -algorithm ed25519 -out keys/scooper.pem
openssl pkey -in keys/scooper.pem -outform DER | tail -c 32 | xxd -p -c 64 > keys/scooper.skey.hex
openssl pkey -in keys/scooper.pem -pubout -outform DER | tail -c 32 | xxd -p -c 64 > keys/scooper.vkey.hex
python3 -c 'import hashlib; print(hashlib.blake2b(bytes.fromhex(open("keys/scooper.vkey.hex").read().strip()), digest_size=28).hexdigest())'
```

The last line prints your scooper key hash. Send that hash to SundaeSwap to
be added to the authorized scooper list; the secret never leaves the host.
Point `scooper-secret-key-file` at `keys/scooper.skey.hex`.

## Wallet

The enterprise address for the key: `cardano-cli address build
--payment-verification-key-file <vkey envelope> --mainnet`, or ask us with
your key hash. Fund it with **at least three UTxOs of 20 ADA or more**. Each
scoop spends one wallet UTxO for fees and needs a second one of ≥ ~10.5 ADA
as collateral; while a scoop is in flight both are reserved, so a single
UTxO stalls the next scoop until the first confirms.

## Paths to replace

- `scooper-secret-key-file`
- `mempool.socket-path` — the node's socket. The scooper submits through it
  and watches the mempool. The user running the scooper must be able to
  connect to the socket (run the node as the same user, or open the socket's
  permissions).
- `genesis-bootstrapper.*-genesis-file` — the node's genesis files.
- `peer-network-interface.node-addresses` — your node's N2N `host:port`.
- `submit-url` — only a fallback if the socket submit fails; leave the
  placeholder if you have no Blockfrost key.

## v3

This config is v4 only. If you also scoop v3 with the same binary, be aware
that a v3 sync from its `starting-point` replays two years of history, and a
v3 Blockfrost bootstrap walks 2,000+ pools.
