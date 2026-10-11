//! Fail-closed guard for a stableswap pool whose `rates` track a
//! yield-bearing asset.
//!
//! The pool prices the yield-bearing asset by the `rates` in its config. A
//! keeper updates them from the asset's backing vault; between updates the
//! on-chain rate goes stale, and once the gap exceeds the swap fee a stale
//! rate is free money for whoever trades first. The keeper is a separate
//! process on a separate box, so it can die, and nothing on chain stops a
//! scoop against a stale rate.
//!
//! This guard derives the true rate from the vault itself, the same way the
//! keeper does (`test/devnet/ops/rate-keeper.mjs` in sundae-v4), compares it
//! with the pool's `rates`, and withholds the pool from scooping while the
//! drift is at or past `refuse_at` of the fee. The keeper acts at half the
//! fee, so a healthy keeper never trips this; a dead one trips it before
//! the drift turns profitable. It also withholds the pool when the vault
//! cannot be read at all: no reading means no basis to say the rate is
//! sound, and the cost of waiting is liveness on one pool, not LP value.
//!
//! The scooper never updates the rate itself. The rate manager's key stays
//! off this box by design.

use std::collections::BTreeMap;
use std::fmt;

use num_traits::Signed;
use pallas_addresses::Address;
use pallas_primitives::Hash;
use plutus_parser::AsPlutus;
use serde::Deserialize;

use crate::bigint::BigInt;
use crate::cardano_types::{AssetClass, TransactionInput, Value};
use crate::sundaev3::Ident;
use crate::sundaev4::types::{PoolType, SundaeV4Pool};

/// One guarded pool, as configured.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RateGuardConfig {
    /// The stableswap pool's ident, hex.
    pub pool: String,
    /// The vault address (bech32) whose UTxOs hold the backing and the
    /// `VaultDatumV1_1` the rate derives from. Matched by payment credential.
    pub vault_address: String,
    /// The backing asset held in the vault, `policy.name` hex.
    pub backing_asset: String,
    /// Position in the pool's assets of the yield-bearing asset, whose rate
    /// the keeper moves.
    pub share_asset_index: usize,
    /// Position of the asset the rate is quoted against. Default 0.
    #[serde(default)]
    pub base_asset_index: usize,
    /// Refuse to scoop once |drift| reaches this fraction of the swap fee.
    /// Default 1.0: the fee itself, where a stale rate turns profitable.
    /// The keeper acts at 0.5, so a live keeper never reaches this.
    #[serde(default = "default_refuse_at")]
    pub refuse_at: f64,
}

fn default_refuse_at() -> f64 {
    1.0
}

/// A guard with its strings resolved, built once at startup.
#[derive(Clone, Debug)]
pub struct RateGuard {
    pub pool: Ident,
    pub vault_payment_hash: Hash<28>,
    pub vault_address_bech32: String,
    pub backing: AssetClass,
    pub share_asset_index: usize,
    pub base_asset_index: usize,
    pub refuse_at: f64,
}

impl RateGuard {
    pub fn resolve(cfg: &RateGuardConfig) -> Result<Self, String> {
        let pool_bytes = hex::decode(&cfg.pool)
            .map_err(|e| format!("rate-guards: pool {} is not hex: {e}", cfg.pool))?;
        if pool_bytes.len() != 28 {
            return Err(format!(
                "rate-guards: pool {} is {} bytes, want 28",
                cfg.pool,
                pool_bytes.len()
            ));
        }
        let address = Address::from_bech32(&cfg.vault_address)
            .map_err(|e| format!("rate-guards: vault address {}: {e}", cfg.vault_address))?;
        let vault_payment_hash = match &address {
            Address::Shelley(s) => *s.payment().as_hash(),
            _ => {
                return Err(format!(
                    "rate-guards: vault address {} is not a Shelley address",
                    cfg.vault_address
                ));
            }
        };
        let (policy_hex, name_hex) = cfg.backing_asset.split_once('.').ok_or_else(|| {
            format!(
                "rate-guards: backing asset {} must be policy.name hex",
                cfg.backing_asset
            )
        })?;
        let backing = AssetClass {
            policy: hex::decode(policy_hex)
                .map_err(|e| format!("rate-guards: backing policy is not hex: {e}"))?,
            token: hex::decode(name_hex)
                .map_err(|e| format!("rate-guards: backing name is not hex: {e}"))?,
        };
        if cfg.share_asset_index == cfg.base_asset_index {
            return Err(format!(
                "rate-guards: pool {} names the same asset as share and base",
                cfg.pool
            ));
        }
        if !cfg.refuse_at.is_finite() || cfg.refuse_at <= 0.0 {
            return Err(format!(
                "rate-guards: pool {} refuse-at must be a positive fraction of the fee",
                cfg.pool
            ));
        }
        Ok(Self {
            pool: Ident::new(&pool_bytes),
            vault_payment_hash,
            vault_address_bech32: cfg.vault_address.clone(),
            backing,
            share_asset_index: cfg.share_asset_index,
            base_asset_index: cfg.base_asset_index,
            refuse_at: cfg.refuse_at,
        })
    }
}

/// `VaultDatumV1_1` from RealFi's staking vault: Constr 0 of four ints.
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct VaultDatumV1_1 {
    pub circulating_susdr: BigInt,
    pub pending_yield: BigInt,
    /// POSIX milliseconds.
    pub diffusion_start: BigInt,
    pub diffusion_end: BigInt,
}

/// A vault UTxO as the indexer tracks it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultUtxo {
    pub address: Vec<u8>,
    pub value: Value,
    /// Parsed when the UTxO carries a datum of the expected shape; the
    /// backing can sit on UTxOs without one.
    pub datum: Option<VaultDatumV1_1>,
}

/// Backing not yet diffused into the rate at POSIX-ms time `t`. Mirrors
/// `pending_remaining` in RealFi's `utilities.ak`, round-up included.
pub fn pending_remaining(v: &VaultDatumV1_1, t: &BigInt) -> BigInt {
    let zero = BigInt::from(0i64);
    if !v.pending_yield.is_positive() || *t >= v.diffusion_end {
        return zero;
    }
    if *t <= v.diffusion_start {
        return v.pending_yield.clone();
    }
    let span = v.diffusion_end.clone() - &v.diffusion_start;
    let remaining_span = v.diffusion_end.clone() - t;
    (v.pending_yield.clone() * remaining_span + span.clone() - BigInt::from(1i64)) / span
}

/// The true rate's numerator and denominator: settled backing over
/// circulating shares, both from the vault.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrueRate {
    pub settled: BigInt,
    pub circulating: BigInt,
}

impl TrueRate {
    pub fn to_f64(&self) -> Option<f64> {
        let den = self.circulating.to_f64()?;
        if den <= 0.0 {
            return None;
        }
        Some(self.settled.to_f64()? / den)
    }
}

/// Derive the true rate from the vault's UTxOs at POSIX-ms `now`.
///
/// The backing is summed over every UTxO at the vault; the datum is taken
/// from the one that carries it. Errors are returned, not swallowed: a
/// caller that cannot derive the rate must fail closed.
pub fn true_rate<'a>(
    vault_utxos: impl Iterator<Item = &'a VaultUtxo>,
    backing: &AssetClass,
    now_ms: &BigInt,
) -> Result<TrueRate, String> {
    let mut held = BigInt::from(0i64);
    let mut datum: Option<&VaultDatumV1_1> = None;
    let mut count = 0usize;
    for utxo in vault_utxos {
        count += 1;
        held += utxo.value.get(backing);
        if let Some(d) = &utxo.datum {
            if datum.is_some() {
                return Err("vault has more than one datum-bearing UTxO".to_string());
            }
            datum = Some(d);
        }
    }
    if count == 0 {
        return Err("no vault UTxO is known".to_string());
    }
    let v = datum.ok_or_else(|| "no vault UTxO carries the vault datum".to_string())?;
    if !held.is_positive() {
        return Err("vault holds none of the backing asset".to_string());
    }
    if !v.circulating_susdr.is_positive() {
        return Err("circulating shares are zero".to_string());
    }
    let pending = pending_remaining(v, now_ms);
    let settled = held - pending;
    if !settled.is_positive() {
        return Err("settled backing is not positive".to_string());
    }
    Ok(TrueRate {
        settled,
        circulating: v.circulating_susdr.clone(),
    })
}

/// What the guard decided for one pool in one cycle.
#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    pub pool: Ident,
    /// True rate over on-chain rate, minus one, in basis points. None when
    /// the rate could not be derived.
    pub drift_bp: Option<f64>,
    pub fee_bp: f64,
    pub refuse_bp: f64,
    pub blocked: bool,
    pub reason: Option<String>,
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.drift_bp, &self.reason) {
            (Some(d), None) => write!(
                f,
                "pool {} drift {d:.2}bp, refuses at {:.2}bp",
                self.pool, self.refuse_bp
            ),
            (Some(d), Some(r)) => write!(
                f,
                "pool {} drift {d:.2}bp, refuses at {:.2}bp: {r}",
                self.pool, self.refuse_bp
            ),
            (None, Some(r)) => write!(f, "pool {}: {r}", self.pool),
            (None, None) => write!(f, "pool {}", self.pool),
        }
    }
}

fn blocked(guard: &RateGuard, fee_bp: f64, reason: String) -> Verdict {
    Verdict {
        pool: guard.pool.clone(),
        drift_bp: None,
        fee_bp,
        refuse_bp: fee_bp * guard.refuse_at,
        blocked: true,
        reason: Some(reason),
    }
}

/// Judge one guarded pool. Any failure to read the pool or the vault is a
/// block: the guard exists for the case where the facts are not in hand.
pub fn evaluate(
    guard: &RateGuard,
    pool: Option<&SundaeV4Pool>,
    vault_utxos: &BTreeMap<TransactionInput, VaultUtxo>,
    now_ms: u64,
) -> Verdict {
    let Some(pool) = pool else {
        return blocked(guard, 0.0, "pool is not in the indexed state".to_string());
    };
    let PoolType::StableSwap { config } = &pool.pool_type else {
        return blocked(guard, 0.0, "pool is not a stableswap pool".to_string());
    };
    let fee = match (config.fee.num.to_f64(), config.fee.den.to_f64()) {
        (Some(n), Some(d)) if d > 0.0 => n / d,
        _ => return blocked(guard, 0.0, "pool fee is not a finite ratio".to_string()),
    };
    let fee_bp = fee * 1e4;
    let (Some(share), Some(base)) = (
        config.rates.get(guard.share_asset_index),
        config.rates.get(guard.base_asset_index),
    ) else {
        return blocked(
            guard,
            fee_bp,
            format!(
                "pool has {} rates; guard names indices {} and {}",
                config.rates.len(),
                guard.share_asset_index,
                guard.base_asset_index
            ),
        );
    };
    let onchain = match (share.to_f64(), base.to_f64()) {
        (Some(s), Some(b)) if b > 0.0 && s > 0.0 => s / b,
        _ => return blocked(guard, fee_bp, "pool rates are not positive".to_string()),
    };
    let at_vault = vault_utxos
        .values()
        .filter(|u| vault_payment_matches(&u.address, &guard.vault_payment_hash));
    let rate = match true_rate(at_vault, &guard.backing, &BigInt::from(now_ms)) {
        Ok(r) => r,
        Err(e) => return blocked(guard, fee_bp, e),
    };
    let Some(true_rate) = rate.to_f64() else {
        return blocked(guard, fee_bp, "true rate is not finite".to_string());
    };
    let drift = true_rate / onchain - 1.0;
    let drift_bp = drift * 1e4;
    let refuse_bp = fee_bp * guard.refuse_at;
    let is_blocked = drift_bp.abs() >= refuse_bp;
    Verdict {
        pool: guard.pool.clone(),
        drift_bp: Some(drift_bp),
        fee_bp,
        refuse_bp,
        blocked: is_blocked,
        reason: is_blocked.then(|| "stale rate: the keeper has not updated the pool".to_string()),
    }
}

/// Whether raw address bytes pay to `hash` (key or script).
pub fn vault_payment_matches(address: &[u8], hash: &Hash<28>) -> bool {
    match Address::from_bytes(address) {
        Ok(Address::Shelley(s)) => s.payment().as_hash() == hash,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sundaev4::types::{PoolDatum, Rational, StableSwapConfig};
    use pallas_addresses::{ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart};

    const VAULT: &str = "addr1wyh04daelcpkgahkuz2ld885w2y3wa27r7w82gzdeagsazgtwduhf";
    const POLICY: &str = "7d9e4a0ee1a3f5d5ff8159ea91a83310cf2795ee7a87170c7aea05ae";

    fn big(v: i128) -> BigInt {
        BigInt::from(v)
    }

    fn guard(refuse_at: f64) -> RateGuard {
        RateGuard::resolve(&RateGuardConfig {
            pool: "10aceb43bb0127e40b4696bb0e06174c830bec3f7ee6063b1d104546".into(),
            vault_address: VAULT.into(),
            backing_asset: format!("{POLICY}.55534472"),
            share_asset_index: 1,
            base_asset_index: 0,
            refuse_at,
        })
        .expect("guard resolves")
    }

    fn vault_address_bytes() -> Vec<u8> {
        Address::from_bech32(VAULT).unwrap().to_vec()
    }

    fn vault(held: i128, datum: Option<VaultDatumV1_1>) -> BTreeMap<TransactionInput, VaultUtxo> {
        let mut value = Value::default();
        value
            .0
            .entry(hex::decode(POLICY).unwrap())
            .or_default()
            .insert(hex::decode("55534472").unwrap(), big(held));
        let mut out = BTreeMap::new();
        out.insert(
            TransactionInput::new(Hash::new([1u8; 32]), 0),
            VaultUtxo {
                address: vault_address_bytes(),
                value,
                datum,
            },
        );
        out
    }

    fn datum(circulating: i128, pending: i128, start: i128, end: i128) -> VaultDatumV1_1 {
        VaultDatumV1_1 {
            circulating_susdr: big(circulating),
            pending_yield: big(pending),
            diffusion_start: big(start),
            diffusion_end: big(end),
        }
    }

    fn pool(rates: [i128; 2], fee: (i128, i128)) -> SundaeV4Pool {
        let ident = Ident::new(&[0x10u8; 28]);
        let asset = |name: &str| AssetClass {
            policy: hex::decode(POLICY).unwrap(),
            token: hex::decode(name).unwrap(),
        };
        SundaeV4Pool {
            input: TransactionInput::new(Hash::new([2u8; 32]), 0),
            address: ShelleyAddress::new(
                pallas_addresses::Network::Mainnet,
                ShelleyPaymentPart::Script(Hash::new([3u8; 28])),
                ShelleyDelegationPart::Null,
            )
            .to_vec(),
            value: Value::default(),
            pool_datum: PoolDatum {
                assets: vec![(asset("55534472"), big(1)), (asset("7355534472"), big(1))],
                total_lp: big(1),
                circulating_lp: big(1),
                preminted_lp: big(0),
                identifier: ident,
                actions: vec![],
                module_state: vec![],
                min_surplus: big(0),
                extension: plutus_parser::create_constr(0, vec![]),
            },
            pool_type: PoolType::StableSwap {
                config: StableSwapConfig {
                    linear_amplification: big(200),
                    fee: Rational {
                        num: big(fee.0),
                        den: big(fee.1),
                    },
                    rates: vec![big(rates[0]), big(rates[1])],
                    rate_manager: None,
                    monotone_rates: false,
                    max_rate_step: None,
                },
            },
            slot: 0,
            fee_split_config: None,
        }
    }

    #[test]
    fn pending_remaining_mirrors_the_contract_with_round_up() {
        let v = datum(1_000, 4_144, 1_000, 2_000);
        assert_eq!(pending_remaining(&v, &big(999)), big(4_144));
        assert_eq!(pending_remaining(&v, &big(1_000)), big(4_144));
        assert_eq!(pending_remaining(&v, &big(2_000)), big(0));
        assert_eq!(pending_remaining(&v, &big(5_000)), big(0));
        // Halfway: 4144 * 500 / 1000 = 2072 exactly.
        assert_eq!(pending_remaining(&v, &big(1_500)), big(2_072));
        // One third remaining: 4144 * 333 / 1000 = 1379.952 rounds UP to 1380.
        assert_eq!(pending_remaining(&v, &big(1_667)), big(1_380));
        let flat = datum(1_000, 0, 1_000, 2_000);
        assert_eq!(pending_remaining(&flat, &big(1_500)), big(0));
    }

    #[test]
    fn true_rate_is_settled_backing_over_circulating() {
        // The mainnet reading of 2026-10-11: held 2_591_309_469_571, pending
        // 4_144_000_000 with 755_573_699 undiffused, circulating
        // 2_582_937_002_070, rate 1.002948927.
        let start = 1_791_201_367_000i128;
        let end = 1_791_806_400_000i128;
        let now = end - (755_573_699 * (end - start) + 4_144_000_000 - 1) / 4_144_000_000;
        let v = datum(2_582_937_002_070, 4_144_000_000, start, end);
        let utxos = vault(2_591_309_469_571, Some(v));
        let rate = true_rate(utxos.values(), &guard(1.0).backing, &big(now)).unwrap();
        let r = rate.to_f64().unwrap();
        assert!((r - 1.002948927).abs() < 2e-9, "rate {r}");
    }

    #[test]
    fn holds_under_the_threshold_and_blocks_at_it() {
        // Fee 15bp. On-chain 1.002381. A true rate 6bp above holds; one 15bp
        // above blocks at refuse-at 1.0, while the keeper's own 0.5 would
        // already have acted at 7.5bp.
        let p = pool([1_000_000, 1_002_381], (15, 10_000));
        let on_chain = 1.002381f64;
        let at = |drift_bp: f64| {
            let circulating = 1_000_000_000_000i128;
            let settled = (circulating as f64 * on_chain * (1.0 + drift_bp / 1e4)).round() as i128;
            vault(settled, Some(datum(circulating, 0, 0, 0)))
        };
        let held = evaluate(&guard(1.0), Some(&p), &at(6.0), 0);
        assert!(!held.blocked, "{held}");
        assert!((held.drift_bp.unwrap() - 6.0).abs() < 0.01);
        assert!((held.refuse_bp - 15.0).abs() < 1e-9);
        let late = evaluate(&guard(1.0), Some(&p), &at(15.0), 0);
        assert!(late.blocked, "{late}");
        // Negative drift counts the same way: yield that went backwards is
        // arbitrage in the other direction.
        let backwards = evaluate(&guard(1.0), Some(&p), &at(-16.0), 0);
        assert!(backwards.blocked, "{backwards}");
        let strict = evaluate(&guard(0.5), Some(&p), &at(8.0), 0);
        assert!(strict.blocked, "{strict}");
    }

    #[test]
    fn fails_closed_without_the_facts() {
        let p = pool([1_000_000, 1_002_381], (15, 10_000));
        let none = evaluate(&guard(1.0), Some(&p), &BTreeMap::new(), 0);
        assert!(none.blocked);
        assert_eq!(none.drift_bp, None);
        let no_datum = evaluate(&guard(1.0), Some(&p), &vault(1_000, None), 0);
        assert!(no_datum.blocked);
        let no_pool = evaluate(&guard(1.0), None, &vault(1_000, Some(datum(1, 0, 0, 0))), 0);
        assert!(no_pool.blocked);
        // A UTxO at some other address does not count as the vault.
        let mut elsewhere = vault(1_000, Some(datum(1_000, 0, 0, 0)));
        for u in elsewhere.values_mut() {
            u.address = ShelleyAddress::new(
                pallas_addresses::Network::Mainnet,
                ShelleyPaymentPart::Key(Hash::new([9u8; 28])),
                ShelleyDelegationPart::Null,
            )
            .to_vec();
        }
        assert!(evaluate(&guard(1.0), Some(&p), &elsewhere, 0).blocked);
    }

    #[test]
    fn resolve_rejects_bad_config() {
        let base = RateGuardConfig {
            pool: "10aceb43bb0127e40b4696bb0e06174c830bec3f7ee6063b1d104546".into(),
            vault_address: VAULT.into(),
            backing_asset: format!("{POLICY}.55534472"),
            share_asset_index: 1,
            base_asset_index: 0,
            refuse_at: 1.0,
        };
        assert!(RateGuard::resolve(&base).is_ok());
        assert!(
            RateGuard::resolve(&RateGuardConfig {
                pool: "abcd".into(),
                ..base.clone()
            })
            .is_err()
        );
        assert!(
            RateGuard::resolve(&RateGuardConfig {
                vault_address: "nope".into(),
                ..base.clone()
            })
            .is_err()
        );
        assert!(
            RateGuard::resolve(&RateGuardConfig {
                backing_asset: "nodot".into(),
                ..base.clone()
            })
            .is_err()
        );
        assert!(
            RateGuard::resolve(&RateGuardConfig {
                share_asset_index: 0,
                ..base.clone()
            })
            .is_err()
        );
        assert!(
            RateGuard::resolve(&RateGuardConfig {
                refuse_at: 0.0,
                ..base.clone()
            })
            .is_err()
        );
    }
}
