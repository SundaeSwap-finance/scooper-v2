//! Crate root of the `route-bench` example (feature `route-bench`), the
//! route-quality benchmark in `bench/route-quality/`.
//!
//! The scooper is a binary-only crate (no `lib.rs`), so the benchmark cannot
//! import its modules: it recompiles them instead. This root must live in
//! `src/` (a module loaded through `#[path]` resolves its own submodules next
//! to its file, which would break `sundaev4/` and friends), and it must
//! declare the same top-level modules as `main.rs` — keep the two lists in
//! sync. CI's `cargo clippy --all-targets --all-features` builds this target,
//! so a missing module shows up there.
//!
//!   cargo run --release --features route-bench --example route-bench -- --help

// Everything only `main.rs` uses is dead code here.
#![allow(dead_code)]

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

#[path = "../bench/route-quality/route_quality.rs"]
mod route_quality;

fn main() {
    route_quality::main();
}
