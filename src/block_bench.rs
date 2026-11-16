//! Crate root of the `block-bench` example (feature `bench`), the
//! block-capacity benchmark in `bench/block-capacity/`.
//!
//! Same layout as `route_bench.rs` (see there why the crate's modules are
//! recompiled, and keep the module list in sync with `main.rs`).
//!
//!   cargo run --release --features bench --example block-bench -- --help

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

#[path = "../bench/common/bench_common.rs"]
mod bench_common;

#[path = "../bench/block-capacity/block_capacity.rs"]
mod block_capacity;

fn main() {
    block_capacity::main();
}
