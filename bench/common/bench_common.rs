//! Code shared by the benchmarks in `bench/` (route-quality, block-capacity).
//!
//! Like the benchmarks themselves, it is compiled into each benchmark's
//! example through `#[path]` (see `src/route_bench.rs`), so it uses the
//! crate's modules as `crate::…`.

use std::sync::Arc;

use pallas_crypto::hash::Hasher;
use plutus_parser::AsPlutus;
use serde_json::Value as Json;

use crate::bigint::BigInt;
use crate::cardano_types::{ADA_ASSET_CLASS, AssetClass};
use crate::sundaev3::Ident;
use crate::sundaev4::test_harness::{TestEnv, pool_script_address};
use crate::sundaev4::{
    ActionEntry, ConcentratedLiquidityConfig, PoolDatum, PoolType, Rational, SundaeV4Pool,
    plutus_void,
};

/// Default scooper-format blueprint the benchmarks build and evaluate their
/// scoop txs against, relative to the repo root.
pub(crate) const DEFAULT_BLUEPRINT: &str = "bench/common/devnet-blueprint-without-traces.json";

/// Fixed-point scale for fees.
pub(crate) const FEE_DEN: u64 = 1_000_000;

/// A tx limit: max tx size in bytes, max padded mem and steps.
#[derive(Clone)]
pub(crate) struct TxLimit {
    pub(crate) bytes: usize,
    pub(crate) mem: u64,
    pub(crate) steps: u64,
}

impl std::str::FromStr for TxLimit {
    type Err = String;

    /// `bytes,mem,steps`, e.g. `16384,14000000,10000000000`.
    fn from_str(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split(',').map(str::trim).collect();
        let [bytes, mem, steps] = parts[..] else {
            return Err(format!("expected bytes,mem,steps, got `{s}`"));
        };
        let num = |v: &str| v.parse::<u64>().map_err(|e| format!("`{v}`: {e}"));
        Ok(TxLimit {
            bytes: num(bytes)? as usize,
            mem: num(mem)?,
            steps: num(steps)?,
        })
    }
}

impl std::fmt::Display for TxLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let si = |n: u64| match n {
            n if n >= 1_000_000_000_000 => format!("{}T", n as f64 / 1e12),
            n if n >= 1_000_000_000 => format!("{}G", n as f64 / 1e9),
            n if n >= 1_000_000 => format!("{}M", n as f64 / 1e6),
            n => n.to_string(),
        };
        write!(
            f,
            "tx ≤ {} B, {} mem, {} steps",
            self.bytes,
            si(self.mem),
            si(self.steps)
        )
    }
}

pub(crate) fn asset_class(id: &str) -> AssetClass {
    if id == "ada.lovelace" {
        ADA_ASSET_CLASS
    } else {
        id.parse().expect("asset id")
    }
}

pub(crate) fn reserve(v: &Json) -> i64 {
    v.as_i64().expect("reserve")
}

/// A single-range concentrated-liquidity pool with the real CL module
/// pipeline (CL + fee split + fairness), so the scoop tx evaluates on chain.
/// The pool's `total_lp` is the CL liquidity L, taken as is from the market file:
/// `cl_market.py` writes exact rational sqrt prices and the exact L the integer
/// reserves support at them, so the pool is never "in deficit" for the router.
/// `make_cl_pool` is not used: its `module_state` is CP-shaped (routing-only
/// tests) and its one-byte ident caps a market at 255 pools; the ident here
/// is derived from the pool's index.
pub(crate) fn cl_pool(
    env: &TestEnv,
    index: usize,
    (a, ra): (AssetClass, i64),
    (b, rb): (AssetClass, i64),
    liquidity: i64,
    (sqrt_price_a, sqrt_price_b): (Rational, Rational),
    fee: Rational,
) -> Arc<SundaeV4Pool> {
    let config = ConcentratedLiquidityConfig {
        sqrt_price_a,
        sqrt_price_b,
        fee,
    };
    let liquidity = BigInt::from(liquidity);
    let cl_hash = env
        .exec
        .module_scripts
        .concentrated_liquidity
        .as_ref()
        .expect("blueprint includes concentratedLiquidity")
        .hash
        .to_vec();
    let mut module_state = env.module_state();
    module_state[0] = (
        cl_hash.clone(),
        Hasher::<256>::hash(&minicbor::to_vec(config.clone().to_plutus()).unwrap()).to_vec(),
    );
    let mut modules = env.action_modules();
    modules[0] = cl_hash;

    let index = u16::try_from(index).expect("≤ 65535 pools").to_be_bytes();
    let mut ident = vec![0xc1; 26];
    ident.extend_from_slice(&index);
    let policy = env.exec.module_scripts.pool_mint.hash.to_vec();
    let named = |prefix: [u8; 4]| AssetClass {
        policy: policy.clone(),
        token: [prefix.as_slice(), &ident].concat(),
    };
    let mut value = crate::cardano_types::Value::default();
    value.insert(&ADA_ASSET_CLASS, BigInt::from(50_000_000i64)); // min UTxO
    // A band out of range holds one asset only; zero quantities are not
    // valid multi-asset entries. `add`, not `insert` (which overwrites): when
    // ADA is one of the pool's assets its reserve comes on top of the min-UTxO
    // ADA, otherwise draining the band leaves the pool under POOL_MIN_ADA and
    // the tx builder tops it up from nowhere ("lovelace not conserved").
    for (asset, qty) in [(&a, ra), (&b, rb)] {
        if qty > 0 {
            value.add(asset, &BigInt::from(qty));
        }
    }
    value.insert(&named([0x00, 0x0d, 0xe1, 0x40]), BigInt::from(1i64)); // pool NFT
    value.insert(&named([0x00, 0x14, 0xdf, 0x10]), liquidity.clone()); // preminted LP
    let mut tx_hash = [0u8; 32];
    tx_hash[..2].copy_from_slice(&index);
    tx_hash[2] = 0xc1;

    Arc::new(SundaeV4Pool {
        input: crate::cardano_types::TransactionInput::new(tx_hash.into(), 0),
        address: pool_script_address(env, None),
        value,
        pool_datum: PoolDatum {
            assets: vec![(a, BigInt::from(ra)), (b, BigInt::from(rb))],
            total_lp: liquidity.clone(),
            circulating_lp: BigInt::from(0),
            preminted_lp: liquidity,
            identifier: Ident::new(&ident),
            actions: vec![ActionEntry {
                tag: BigInt::from(100),
                enabled: true,
                modules,
            }],
            module_state,
            min_surplus: BigInt::from(0),
            extension: plutus_void(),
        },
        pool_type: PoolType::ConcentratedLiquidity {
            sqrt_price_a: config.sqrt_price_a,
            sqrt_price_b: config.sqrt_price_b,
            fee: config.fee,
        },
        slot: 100,
        fee_split_config: None,
    })
}
