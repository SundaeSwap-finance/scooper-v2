# Benchmarks

Benchmarks of the scooper's sundae-v4 code against Cardano's ledger limits. Each one
lives in its own folder, with its own `Makefile` (`make help` lists its targets);
read its documentation before running it:

- [`route-quality/`](route-quality/README.md): how much better a large swap executes
  when one scoop tx may be more complex (bigger tx size and ExUnits budget).
- [`block-capacity/`](block-capacity/README.md): how many orders a Praos ranking block vs
  a Leios endorser block can carry when every scoop tx stays within today's per-tx limits.

Shared by both:

- `common/`: code shared by the two benches (`bench_common.rs`) and the scooper-format
  blueprint their scoop txs are built and evaluated against.
- `cache/`: raw API answers, downloaded on demand and not committed.

Both benches are examples built with the cargo feature `bench` (see `Cargo.toml`). They
are not the timing benchmarks of `cargo bench` (`src/router_bench.rs`).
