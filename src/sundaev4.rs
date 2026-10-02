pub mod access;
pub mod accumulator;
pub mod batch;
pub mod butane;
pub mod chain_tracker;
pub mod claims;
pub mod conversions;
pub mod evaluator;
mod indexer;
pub mod intents;
pub mod router;
pub mod script_context;
pub mod ss_math;
pub mod submit;
pub mod swap_math;
pub mod tx_builder;
mod types;

pub use indexer::*;
pub use types::*;

#[cfg(test)]
mod scoop_tests;
// Not actually dead: feature `route-bench` uses it. But features are
// crate-wide, so the scooper binary compiles it too, never calls it,
// and would trip `clippy --all-features -D warnings` in CI.
#[cfg(any(test, feature = "route-bench"))]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) mod test_harness;
