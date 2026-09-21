# Getting scooper-v2 working on Musashi

Musashi is IOG's Leios prototype testnet. Its node runs a new ledger era —
**Dijkstra** — one past Conway, and its blocks are produced under Leios's
two-layer model rather than plain Praos. Neither of those things exist yet in
the crates scooper-v2's dependency chain used at the time this work started
(pallas 0.34, and the `input-output-hk/acropolis` revision scooper-v2 pinned).

This is a record of everything that had to change to get scooper-v2 running
end-to-end against a live Musashi node: build a scoop, submit it, and have it
land on-chain. Five separate changes were needed, of increasing specificity:

1. **Move to a pallas fork with Dijkstra ledger support**, and update every
   call site in scooper-v2 (and its `acropolis` dependency) that the new API
   shape broke. This is the bulk of the diff, but it's mechanical — a version
   migration, not new logic.
2. **Fix a real scooper-v2 bug the migration exposed**: reference scripts on
   every Dijkstra-era UTxO were silently discarded, which permanently emptied
   scooper-v2's on-chain script store.
3. **Patch a second gap the same fork doesn't cover**: the node's local
   transaction-submission protocol has no Dijkstra support in pallas-network
   at all, not even the ledger-primitive layer.
4. **Add Musashi as a runtime target**: new config files and a local Docker
   build, since Musashi isn't one of scooper-v2's existing named networks.
5. **Two remaining gaps, documented but not fixed** — one in pallas-network,
   one in acropolis — that don't block scooper-v2's own logic but do block
   it from being *fully* self-sufficient on this network. See the end of
   this document.

None of the local patches in steps 1–3 are upstreamed. They live in two
sibling clones (`../acropolis-patched`, `../pallas-patched`) wired in via
`[patch]` sections in `Cargo.toml`, so they build automatically but aren't
part of either upstream project.

---

## 1. Moving to a Dijkstra-aware pallas

### The dependency change

```diff
- pallas-addresses = "0.34"
- pallas-codec = "0.34"
- pallas-crypto = "0.34"
- pallas-primitives = "0.34"
- pallas-network = "0.34"
- pallas-traverse = "0.34"
+ pallas-addresses = { git = "https://github.com/geofflittle/pallas.git", branch = "feat/dijkstra-era-accessors" }
+ pallas-codec = { git = "https://github.com/geofflittle/pallas.git", branch = "feat/dijkstra-era-accessors" }
+ pallas-crypto = { git = "https://github.com/geofflittle/pallas.git", branch = "feat/dijkstra-era-accessors" }
+ pallas-primitives = { git = "...", branch = "...", features = ["unstable"] }
+ pallas-network = { git = "...", branch = "..." }
+ pallas-traverse = { git = "...", branch = "...", features = ["unstable"] }
```

`geofflittle/pallas`, branch `feat/dijkstra-era-accessors`, is a fork of
pallas 1.1.1 (itself a large jump from 0.34) with Dijkstra support added to
the **ledger-primitive and traversal layers** — `pallas-primitives`'
`dijkstra` module (blocks, transactions, transaction outputs, script refs)
and `pallas-traverse`'s era-neutral `MultiEra*` accessors. This is the layer
scooper-v2 actually needs for chain-sync, tx decoding, and tx building.
`unstable` is required to reach the Dijkstra types at all.

This one version jump — not anything Musashi-specific — was the source of
most of the diff below. Two knock-on dependency problems came with it:

- **`plutus-parser` pulls its own pallas-primitives.** It re-exports
  `pallas_primitives::PlutusData` as its own type, and its default feature
  pins pallas-primitives 0.34 from crates.io — a second, structurally
  identical but distinct copy of `PlutusData`, so nothing using both crates'
  versions would type-check. Fixed by switching to plutus-parser's
  `pallas-v1` feature (built for pallas 1.x) and adding a
  `[patch.crates-io]` redirecting its own pallas-primitives pull to the same
  fork:
  ```toml
  plutus-parser = { version = "0.6", default-features = false, features = ["derive", "pallas-v1"] }
  [patch.crates-io]
  pallas-primitives = { git = "https://github.com/geofflittle/pallas.git", branch = "feat/dijkstra-era-accessors", features = ["unstable"] }
  ```
- **minicbor version mismatch.** pallas-codec 1.x needs minicbor 0.26;
  scooper-v2 was on 0.25. Bumped directly — same duplicate-crate problem,
  one layer down.

### What broke in scooper-v2's own code, and why

Pallas 1.x changed several data shapes system-wide. None of this is
Musashi-specific — any consumer moving from 0.34 to 1.x hits the same
things — but every call site touching these types needed updating:

- **`Nullable<T>` → plain `Option<T>`** for most optional ledger fields.
- **`NonEmptyKeyValuePairs<K, V>` → plain `BTreeMap<K, V>`** for multiassets,
  redeemers, and similar maps.
- **`KeepRaw<'_, T>` wrapping became pervasive** — `TransactionOutput`,
  `DatumOption::Data`, a transaction's body/witness-set, `AuxiliaryData`, and
  script-context redeemers all moved from bare values to
  `KeepRaw<'_, T>`, a wrapper that preserves the original CBOR bytes for
  round-tripping. Building a *new* value from scratch (as scooper-v2 does
  when constructing a scoop tx) uses `.into()`, which produces an empty
  `raw` — safe, because `KeepRaw`'s `Encode` impl falls back to re-encoding
  the inner value whenever `raw_cbor()` is empty. Reading an *existing*
  value uses `Option::as_deref()` to match through the wrapper.
- **Renamed types**: `Pseudo*` types took their real names (e.g.
  `PseudoScript` → `ScriptRef`); `MintedDatumOption` → `DatumOption`;
  `.original_hash()` → `.hash()`.
- **"Widened" era-neutral accessors** (from upstream PR groundwork this fork
  builds on): `MultiEraTx::native_scripts()` and
  `MultiEraOutput::script_ref()` now return era-neutral
  `MultiEraNativeScript` / `MultiEraScriptRef` instead of a Conway-specific
  type, so a caller can in principle handle any era through one method.
- **`conway::TransactionBody`/`conway::Tx` stopped being generic over the
  output type** (`TransactionBody<'a, O>` → `TransactionBody<'a>`,
  hardcoded internally to `conway::TransactionOutput<'a>`) — scooper-v2's
  own `MultiPoolBuildResult.tx_body` field had to drop its now-invalid
  `TransactionOutput` type parameter.
- **`pallas_primitives::babbage::MintedPostAlonzoTransactionOutput`** (used
  throughout scooper-v2 to build outputs) turns out to have always been the
  wrong type in spirit — it parameterizes with `alonzo::Value`/
  `babbage::ScriptRef`, not `conway::Value`/`conway::ScriptRef`. Pallas 1.x
  stopped silently accepting it as a stand-in; every construction site now
  uses `conway::PostAlonzoTransactionOutput` directly.
- **`TxValidationError`** (pallas-network) changed from a raw-bytes newtype
  to a real enum — `src/mempool.rs`'s rejection-reason formatting switched
  from hex-dumping the bytes to `format!("{reason:?}")`.

These fixes touched, in order of size: `src/sundaev4/tx_builder.rs` (the
largest single file — every output/witness/mint construction site),
`src/sundaev4/butane.rs`, `src/cardano_types.rs`, `src/bootstrap.rs`,
`src/sundaev4/script_context.rs`, `src/sundaev4/evaluator.rs`,
`src/sundaev4/indexer.rs`, `src/sundaev3/indexer.rs`, `src/mempool.rs`, and
the test files (`scoop_tests.rs`, `test_harness.rs`). All 233 existing tests
pass afterward, including the UPLC script-evaluation suite — this was a
mechanical type migration, not a behavior change.

### The same migration, one layer down, in acropolis

`acropolis_common`/`acropolis_codec` (scooper-v2's chain-sync/indexing
dependency, from `input-output-hk/acropolis`) sit on the same pallas and
needed the identical class of fixes, applied in a local clone
(`../acropolis-patched`, pinned to upstream rev `2c0a32c`) and wired in via:

```toml
[patch."https://github.com/input-output-hk/acropolis.git"]
acropolis_common = { path = "../acropolis-patched/common" }
acropolis_module_block_unpacker = { path = "../acropolis-patched/modules/block_unpacker" }
acropolis_module_custom_indexer = { path = "../acropolis-patched/modules/custom_indexer" }
acropolis_module_genesis_bootstrapper = { path = "../acropolis-patched/modules/genesis_bootstrapper" }
acropolis_module_mithril_snapshot_fetcher = { path = "../acropolis-patched/modules/mithril_snapshot_fetcher" }
acropolis_module_peer_network_interface = { path = "../acropolis-patched/modules/peer_network_interface" }
```

Changes, by file:

| File | Change |
|---|---|
| `common/src/types.rs` | `Era::try_from(u8)`: era ≥ 6 now maps to `Era::Conway` instead of bailing. `acropolis_common::Era` has no real `Dijkstra` variant — this just stops it from rejecting Dijkstra-tagged blocks outright. Anything genuinely era-specific downstream still treats them as Conway. |
| `codec/src/utils.rs`, `certs.rs`, `governance.rs` | Same `Nullable`→`Option` fix as above, at each call site (`Relay`, `PoolMetadata`, `Anchor`, `GovActionId` fields). Deleted the now-unused generic `map_nullable`/`map_nullable_result` helpers. |
| `codec/src/script.rs` | `MultiEraRedeemer::tag()` now returns the era-neutral `MultiEraRedeemerTag` (was `conway::RedeemerTag`). `map_redeemer_tag` takes that type and returns `Result` — it errors on Dijkstra's new `Guarding` purpose, which `acropolis_common::RedeemerTag` has no variant for (out of scope; nothing in the Musashi deployment uses it). |
| `codec/src/tx.rs`, `codec/src/utxo.rs` | Switched to the widened `MultiEraNativeScript`/`MultiEraScriptRef` accessors. Dijkstra native scripts and Dijkstra reference scripts (including PlutusV4) have no `acropolis_common` representation and are skipped — out of scope, since Musashi's deployment is entirely PlutusV3. |
| `codec/src/witness.rs` | `ScriptNOfK`'s threshold is now `i64` in the ledger CDDL; `acropolis_common` still wants `u32` — clamped to `u32::MAX` on overflow/negative rather than panicking on untrusted chain data. |
| `modules/genesis_bootstrapper` | `pallas::ledger::configs` moved to `pallas::interop::hardano::configs`, gated behind a `hardano` feature (added to this crate's own `pallas` dependency). |
| `modules/mithril_snapshot_fetcher` | `pallas::storage::hardano` moved to `pallas::interop::hardano::storage` (aliased back to `hardano` on import, so the rest of the file needed no changes — the reported `[u8]`-unsized and `Point::slot_or_default` type-inference errors were cascading fallout from this one bad import, not separate bugs). |

---

## 2. The real bug: Dijkstra reference scripts were silently dropped

This is the one change in this list that's a genuine scooper-v2 defect, not
migration mechanics — and it was the hardest to find, because it never
threw an error. It just made every scoop attempt fail evaluation with
`"script ... not found in store"`, regardless of how the chain state was
bootstrapped.

`src/cardano_types.rs`'s `convert_script_ref` only handled
`MultiEraScriptRef::Conway`, returning `None` for anything else:

```rust
// before
fn convert_script_ref(script_ref: pallas_primitives::conway::MintedScriptRef) -> ScriptRef {
    match script_ref {
        MintedScriptRef::NativeScript(n) => ScriptRef::Native(n.unwrap()),
        MintedScriptRef::PlutusV1Script(s) => ScriptRef::PlutusV1(s),
        MintedScriptRef::PlutusV2Script(s) => ScriptRef::PlutusV2(s),
        MintedScriptRef::PlutusV3Script(s) => ScriptRef::PlutusV3(s),
    }
}
```

Once this was widened to the era-neutral `MultiEraScriptRef` (part of the
1.x migration above), the naive port just matched `Conway` and returned
`None` for everything else, including `Dijkstra`. **Every UTxO on Musashi is
Dijkstra-era.** So this meant scooper-v2's UPLC `ScriptStore` was
permanently empty no matter what — chain-sync correctly found and persisted
every module-script reference UTxO (confirmed directly against the sqlite
DB: right addresses, right slots, all present), but every one of them
decoded with `script_ref: None`.

Fix — handle the Dijkstra variant the same way as Conway:

```rust
fn convert_script_ref(script_ref: pallas_traverse::MultiEraScriptRef) -> Option<ScriptRef> {
    match script_ref {
        MultiEraScriptRef::Conway(r) => Some(match r.into_owned() { /* ... as before ... */ }),
        MultiEraScriptRef::Dijkstra(r) => match r.into_owned() {
            dijkstra::ScriptRef::NativeScript(_) => None, // distinct type, not needed here
            dijkstra::ScriptRef::PlutusV1Script(s) => Some(ScriptRef::PlutusV1(s)),
            dijkstra::ScriptRef::PlutusV2Script(s) => Some(ScriptRef::PlutusV2(s)),
            dijkstra::ScriptRef::PlutusV3Script(s) => Some(ScriptRef::PlutusV3(s)),
            dijkstra::ScriptRef::PlutusV4Script(_) => None, // no local representation
        },
        _ => None, // `MultiEraScriptRef` is non_exhaustive
    }
}
```

(`convert_datum` needed the parallel `MintedDatumOption` → `DatumOption`
rename as part of the same pass, and `convert_txo` switched from `.map()`
to `.and_then()` to match the new `Option`-returning signature.)

After this fix, scooper-v2 correctly evaluates and builds a fully valid
scoop transaction — confirmed with the correct swap math and script budgets
comfortably within limits (mem 20%, steps 9.8%, size 13.7% of the tx's
on-chain caps).

---

## 3. pallas-network has no Dijkstra support for local tx submission

Getting a built transaction *decoded correctly by scooper-v2's own node* —
submission response, not chain-sync — needed a second, separate patch,
because `geofflittle/pallas`'s Dijkstra work didn't touch
`pallas-network`'s client-facing mini-protocols at all.

`pallas-network/src/miniprotocols/localtxsubmission/protocol.rs`:

```rust
#[cbor(index_only)]
pub enum ShelleyBasedEra {
    Shelley = 1, Allegra = 2, Mary = 3, Alonzo = 4, Babbage = 5, Conway = 6,
}
```

This tags which ledger era a `RejectTx` response's `TxValidationError` came
from. A real Dijkstra `cardano-node` tags its era as index **7** — confirmed
independently by submitting the same transaction via
`cardano-cli dijkstra transaction submit` against the same node, whose own
error text reads `ShelleyTxValidationError ShelleyBasedEraDijkstra (...)`.
Since `TxValidationError`'s decoder reads `era` before `error` on the wire,
this was the very first thing decoded in *any* reject response — so every
local-node submission attempt failed with
`Error { err: UnknownVariant(7), pos: Some(4) }`, for both rejects (which
this era tag gates) and, transitively, made it impossible to tell whether an
accept had actually gone through.

Fixed in a second local clone (`../pallas-patched`, of
`geofflittle/pallas@feat/dijkstra-era-accessors`), wired in via:

```toml
[patch."https://github.com/geofflittle/pallas.git"]
pallas-addresses = { path = "../pallas-patched/pallas-addresses" }
pallas-codec = { path = "../pallas-patched/pallas-codec" }
pallas-crypto = { path = "../pallas-patched/pallas-crypto" }
pallas-primitives = { path = "../pallas-patched/pallas-primitives" }
pallas-network = { path = "../pallas-patched/pallas-network" }
pallas-traverse = { path = "../pallas-patched/pallas-traverse" }
pallas-hardano = { path = "../pallas-patched/pallas-hardano" }  # pulled in transitively
```

by adding one variant:

```rust
#[n(7)]
Dijkstra,
```

(`pallas-hardano/src/display/haskell_error.rs` has a second, JSON-only
mirror of the same enum — used for a Cardano submit-API-compatible error
format scooper-v2 doesn't call, but which still needs to compile as part of
the same dependency graph — and needed the identical one-line addition.)

This fix is verified correct and sufficient for the common case: after
adding it, a live submission now decodes cleanly through the era tag. A
`Message::AcceptTx` response carries no payload at all, so an accepted
submission was never actually blocked by this — only reading back a
*reject* reason was. **Deliberately not fixed**: `ApplyTxError`'s payload
(`Vec<ConwayLedgerFailure>`, decoded immediately after `era`) is a large,
Conway-shaped enum tree, and Dijkstra's real failure ADT has different
shapes here too (cardano-cli's own error text shows new top-level wrapper
constructors like `DijkstraUtxowFailure`). A rejected Dijkstra tx can still
fail to decode past the failure-detail payload — modeling that correctly
needs either raw wire bytes from a live rejection or cardano-ledger's actual
Dijkstra CDDL, neither of which is in hand. In practice this only matters
when a submission is rejected; every submission that reaches an *accepted*
outcome decodes cleanly.

---

## 4. Musashi as a runtime target

None of the above makes scooper-v2 aware Musashi exists — it's config and a
build, not code.

**`config/musashi/`** — Musashi's byron/shelley genesis files, copied from
the devnet's own config directory.

**`config/musashi.json`** — chain-sync config: network name `musashi`,
genesis bootstrapper pointing at the files above, peer-network-interface
node address.

**`config/musashi-v4.json`** — the full `protocol.v4` block, built from the
live deployment's own state file (`.state.musashi.json` in `sundae-v4`):
every module-script hash and reference UTxO, the settings NFT, a live-queried
350-entry PlutusV3 cost model (`cardano-cli query protocol-parameters`), a
slot-config matching the network's genesis, and the scooper's own signing
key.

**`config/musashi-container.json`** — an overlay applied only inside the
Docker container: sqlite path, and the peer-network-interface node address
rewritten to the container-network hostname.

**`Dockerfile.musashi-local`** — builds *inside* Docker (a natively-built
macOS binary doesn't run in the Linux container), using two extra build
contexts so the Dockerfile can see the sibling patched-dependency clones
that live outside the repo:

```
docker build -f Dockerfile.musashi-local \
  --build-context acropolis-patched=../acropolis-patched \
  --build-context pallas-patched=../pallas-patched \
  -t scooper-v2:musashi-local .
```

---

## What's still open

Two gaps remain, both outside scooper-v2 itself, and both left undone
deliberately rather than worked around blindly:

- **pallas-network's `LocalTxSubmission` reject-payload decoding** (§3
  above) — only the era tag is fixed; the nested Dijkstra failure-reason
  shapes aren't modeled. A related, not-yet-touched gap in the same
  territory: scooper-v2's mempool monitor (`LocalTxMonitor`, a different
  mini-protocol) logs `mempool tx failed to decode; skipping era=7` for
  transactions it observes in the node's mempool — same root cause, same
  missing Dijkstra support in pallas-network, different mini-protocol.
  Neither blocks scooper-v2's own build/submit/confirm loop; both would
  need to be fixed to give scooper-v2 full visibility into rejection
  reasons and live mempool contents on this network.

- **Leios input-block diffusion isn't implemented anywhere in this stack.**
  Leios splits block production into a tiny ranking block (73–74 bytes on
  Musashi, regardless of how many transactions it certifies — confirmed
  against blocks reporting `tx_count` in the thousands) plus separately
  diffused input blocks carrying the actual transaction bodies. acropolis's
  block-fetch client only speaks the traditional single-layer protocol: it
  decodes the ranking block fine (nothing crashes) but has no way to fetch
  the input blocks it references, so any transaction that lives only in an
  IB is invisible to chain-sync. On Musashi this affects a real fraction of
  background/load-test traffic, and — since scooper-v2 has no way to know
  in advance which of *its own* future transactions will land this way —
  it can silently make scooper-v2's tracked wallet or pool UTxOs stale.
  This isn't a bug in acropolis's chain-selection or rollback logic (both
  were inspected and look correct); it's a missing mini-protocol, and a
  materially larger body of work than anything else in this document.
