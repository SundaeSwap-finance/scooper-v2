//! Crate root of the `router` bench (`cargo bench --bench router`), the
//! router benchmark in `benches/router/`.
//!
//! The scooper is a binary-only crate (no `lib.rs`), so the bench cannot
//! import its modules: it recompiles them instead. This root must live in
//! `src/` (a module loaded through `#[path]` resolves its own submodules next
//! to its file, which would break `sundaev4/` and friends), and it must
//! declare the same top-level modules as `main.rs` — keep the two lists in
//! sync.

// Everything only `main.rs` uses is dead code here, and so are whole structs
// that the scooper builds but the bench never does: their fields'
// `#[expect(dead_code)]` go unfulfilled (the lint flags the struct instead).
// And cargo builds a bench with `cfg(test)` but, without the test harness,
// drops its `#[test]` fns: the test modules compile with their imports unused.
#![allow(dead_code, unfulfilled_lint_expectations, unused_imports)]

mod bigint;
mod blueprint;
mod bootstrap;
mod cardano_types;
mod cbor_guard;
mod config;
mod datum_lookup;
mod events;
mod historical_state;
mod instrumentation;
mod mempool;
mod metrics;
mod multisig;
mod persistence;
mod scooper;
mod server;
mod sundaev3;
mod sundaev4;

#[path = "../benches/router/find_blended_route.rs"]
mod find_blended_route;

criterion::criterion_main!(find_blended_route::benches);
