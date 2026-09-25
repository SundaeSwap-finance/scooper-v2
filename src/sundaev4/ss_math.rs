//! Stableswap (Curve-style, two-asset) math in `BigInt`.
//!
//! Port of `lib/tests/scenario.ak` (`ss_get_d_rated`, `ss_get_raw_swap_rated`,
//! `ss_swap_rated`, `ss_fee_budget`) in sundae-v4 — the Newton twins of what
//! the on-chain module pins with `lib/modules/ss_math.ak`. The scooper
//! computes every value here; the module only checks that each supplied
//! integer is the unique one that satisfies the invariant
//!
//! ```text
//!   4A(x + y) + D = 4AD + D³ / (4xy)
//! ```
//!
//! on the rated, scaled reserves `x = r_a · rate_a · P`, `y = r_b · rate_b · P`
//! with `P = calc_precision = 10^12`. `D` and the raw swap result carry the
//! same scale. The integer rules below mirror `lib/modules/ss_check.ak`:
//!
//! - swap (tag 3): `raw` is the smallest scaled y-solution at `D_before`;
//!   `swap_result = floor(raw / (rate_out · P))`; `fee = ceil(swap_result · fee)`;
//!   the trader receives `swap_result − fee`; the whole fee stays in the out
//!   reserve; `fee_budget = floor(lp · D_after / D_before) − lp`.
//! - deposit (tag 6) / withdraw (tag 4): every reserve moves by
//!   `ceil(r_i · t / D_before)`; `total_lp` becomes
//!   `floor(lp · (D_before + t) / D_before)`; `fee_budget = 0`.
//! - rate update (tag 7): reserves and LP flat; `D` re-derived at the new
//!   rates; first transcript entry only. The scooper never originates one.
//!
//! Division follows Plutus `divideInteger` (floor toward −∞). That matters
//! only for the negative `t` of a withdrawal; every other quantity is positive.

use num_traits::{Signed, Zero};

use crate::bigint::BigInt;
use crate::sundaev4::types::{Rational, StableSwapConfig};

/// `ss_math.calc_precision`: scale applied to reserves before they enter the
/// invariant.
pub const CALC_PRECISION: u64 = 1_000_000_000_000;

/// Upper bound on a rate, on `A`, and on every rational component
/// (`ss_check.max_rate` = 2^64).
pub fn max_rate() -> BigInt {
    let two_32 = BigInt::from(1u64 << 32);
    &two_32 * &two_32
}

fn precision() -> BigInt {
    BigInt::from(CALC_PRECISION)
}

const NEWTON_MAX_ITERATIONS: usize = 255;

/// Plutus `divideInteger`: floor toward −∞.
pub fn floor_div(a: &BigInt, b: &BigInt) -> BigInt {
    assert!(!b.is_zero(), "floor_div by zero");
    let q = a / b;
    let r = a % b;
    if !r.is_zero() && (r.is_negative() != b.is_negative()) {
        q - &BigInt::from(1)
    } else {
        q
    }
}

/// `ceil(a / b)` with the same sign convention as [`floor_div`].
pub fn ceil_div(a: &BigInt, b: &BigInt) -> BigInt {
    -floor_div(&(-a), b)
}

// ─── Invariant polynomials (`ss_math.ak`) ───────────────────────────────────

/// `f(D)` from `liquidity_invariant`: `≤ 0` iff `D` is at or below the curve.
pub fn ss_f(x: &BigInt, y: &BigInt, amp: &BigInt, d: &BigInt) -> BigInt {
    let sixteen_axy = &(&BigInt::from(16) * amp) * &(x * y);
    &(&(&sixteen_axy * d) + &(&(d * d) * d))
        - &(&(&sixteen_axy * &(x + y)) + &(&(&BigInt::from(4) * &(x * y)) * d))
}

/// `g(y)` from `exchange_invariant`: `≥ 0` iff the pool keeps enough of the
/// taken asset.
pub fn ss_g(x: &BigInt, y: &BigInt, amp: &BigInt, d: &BigInt) -> BigInt {
    let four_xy = &BigInt::from(4) * &(x * y);
    let four_a = &BigInt::from(4) * amp;
    &(&(&four_xy * &(&(&four_a * &(x + y)) + d)) - &(&(&four_xy * &four_a) * d)) - &(&(d * d) * d)
}

/// `liquidity_invariant`: `D` is the largest integer with `f(D) ≤ 0`.
/// Scaled inputs.
pub fn liquidity_invariant(x: &BigInt, y: &BigInt, amp: &BigInt, d: &BigInt) -> bool {
    !ss_f(x, y, amp, d).is_positive() && ss_f(x, y, amp, &(d + &BigInt::from(1))).is_positive()
}

/// `exchange_invariant`: `raw` is the smallest scaled output such that the
/// post-swap taken reserve `old_takes − raw` still satisfies the curve at `d`.
pub fn exchange_invariant(
    new_gives: &BigInt,
    old_takes: &BigInt,
    raw: &BigInt,
    amp: &BigInt,
    d: &BigInt,
) -> bool {
    let y = old_takes - raw;
    !ss_g(new_gives, &y, amp, d).is_negative()
        && ss_g(new_gives, &(&y - &BigInt::from(1)), amp, d).is_negative()
}

// ─── D ──────────────────────────────────────────────────────────────────────

/// `D` for reserves `(x, y)` at rates `(px, py)` and amplification `amp`: the
/// largest integer that satisfies `liquidity_invariant`. Newton from
/// `D = x + y`, then a ±1 fix-up (`scenario.ss_get_d_rated`).
pub fn get_d_rated(
    amp: &BigInt,
    x: &BigInt,
    px: &BigInt,
    y: &BigInt,
    py: &BigInt,
) -> Result<BigInt, String> {
    let xs = &(x * px) * &precision();
    let ys = &(y * py) * &precision();
    let sum = &xs + &ys;
    if sum.is_zero() {
        return Ok(BigInt::from(0));
    }
    let ann = &BigInt::from(4) * amp;
    let one = BigInt::from(1);
    let mut d = sum.clone();
    for _ in 0..NEWTON_MAX_ITERATIONS {
        let d_p = floor_div(&(&(&d * &d) * &d), &(&BigInt::from(4) * &(&xs * &ys)));
        let d_next = floor_div(
            &(&(&(&ann * &sum) + &(&BigInt::from(2) * &d_p)) * &d),
            &(&(&(&ann - &one) * &d) + &(&BigInt::from(3) * &d_p)),
        );
        let diff = (&d_next - &d).abs();
        d = d_next;
        if diff <= one {
            return Ok(fix_d(&xs, &ys, amp, d));
        }
    }
    Err("ss get_d: reached max iterations".into())
}

/// [`get_d_rated`] at unit rates.
#[cfg(test)]
pub fn get_d(amp: &BigInt, x: &BigInt, y: &BigInt) -> Result<BigInt, String> {
    let one = BigInt::from(1);
    get_d_rated(amp, x, &one, y, &one)
}

fn fix_d(x: &BigInt, y: &BigInt, amp: &BigInt, d0: BigInt) -> BigInt {
    let one = BigInt::from(1);
    let mut d = d0;
    while ss_f(x, y, amp, &d).is_positive() {
        d = &d - &one;
    }
    while !ss_f(x, y, amp, &(&d + &one)).is_positive() {
        d = &d + &one;
    }
    d
}

// ─── Raw swap output ────────────────────────────────────────────────────────

/// The raw (scaled numeraire, pre-fee) swap output when the given reserve
/// becomes `in_after` (rate `p_in`) and the taken reserve starts at
/// `out_before` (rate `p_out`), at the pre-swap `d`. Newton on `y`, then a
/// walk to the smallest scaled `y` with `g(y) ≥ 0`
/// (`scenario.ss_get_raw_swap_rated`).
pub fn get_raw_swap_rated(
    amp: &BigInt,
    d: &BigInt,
    in_after: &BigInt,
    p_in: &BigInt,
    out_before: &BigInt,
    p_out: &BigInt,
) -> Result<BigInt, String> {
    let xs = &(in_after * p_in) * &precision();
    let ann = &BigInt::from(4) * amp;
    let two = BigInt::from(2);
    let one = BigInt::from(1);
    // n = 2: c = D³ / (4 · x · Ann), b = x + D / Ann — evaluated in the same
    // order as the Aiken source so intermediate floors match.
    let c = floor_div(&(&floor_div(&(d * d), &(&two * &xs)) * d), &(&two * &ann));
    let b = &xs + &floor_div(d, &ann);
    let mut y = d.clone();
    let mut converged = false;
    for _ in 0..NEWTON_MAX_ITERATIONS {
        let y_next = floor_div(&(&(&y * &y) + &c), &(&(&(&two * &y) + &b) - d));
        let diff = (&y_next - &y).abs();
        y = y_next;
        if diff <= one {
            converged = true;
            break;
        }
    }
    if !converged {
        return Err("ss get_y: reached max iterations".into());
    }
    while ss_g(&xs, &y, amp, d).is_negative() {
        y = &y + &one;
    }
    while !ss_g(&xs, &(&y - &one), amp, d).is_negative() {
        y = &y - &one;
    }
    Ok(&(&(out_before * p_out) * &precision()) - &y)
}

/// [`get_raw_swap_rated`] at unit rates.
#[cfg(test)]
pub fn get_raw_swap(
    amp: &BigInt,
    d: &BigInt,
    in_after: &BigInt,
    out_before: &BigInt,
) -> Result<BigInt, String> {
    let one = BigInt::from(1);
    get_raw_swap_rated(amp, d, in_after, &one, out_before, &one)
}

// ─── Fee budget ─────────────────────────────────────────────────────────────

/// LP-denominated fee budget of a swap: `floor(lp · d_after / d_before) − lp`.
pub fn fee_budget(d_before: &BigInt, d_after: &BigInt, lp: &BigInt) -> BigInt {
    &floor_div(&(lp * d_after), d_before) - lp
}

// ─── Pool parameters ────────────────────────────────────────────────────────

/// The config fields the step math reads. Built from a
/// [`StableSwapConfig`]; `rates` are the rates in force (after any leading
/// tag-7 update in the same transcript).
#[derive(Clone, Debug)]
pub struct SsParams {
    pub amp: BigInt,
    pub fee: Rational,
    pub rates: Vec<BigInt>,
}

impl SsParams {
    pub fn from_config(cfg: &StableSwapConfig) -> Self {
        SsParams {
            amp: cfg.linear_amplification.clone(),
            fee: cfg.fee.clone(),
            rates: cfg.rates.clone(),
        }
    }

    fn check_shape(&self, reserves: &[BigInt]) -> Result<(), String> {
        if reserves.len() != 2 || self.rates.len() != 2 {
            return Err("stableswap step builders take exactly two reserves and two rates".into());
        }
        Ok(())
    }

    /// `D` for `reserves` at this pool's rates.
    pub fn d_of(&self, reserves: &[BigInt]) -> Result<BigInt, String> {
        self.check_shape(reserves)?;
        get_d_rated(
            &self.amp,
            &reserves[0],
            &self.rates[0],
            &reserves[1],
            &self.rates[1],
        )
    }
}

// ─── Step builders ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SsSwapResult {
    /// `SwapStep.raw_swap_result`
    pub raw: BigInt,
    /// Gross output in token units, before the fee.
    pub swap_result: BigInt,
    /// `ceil(swap_result · fee)`, kept in the out reserve.
    pub fee: BigInt,
    /// What the trader receives.
    pub dy: BigInt,
    pub reserves_after: Vec<BigInt>,
    /// `SwapStep.next_sum_invariant`
    pub next_d: BigInt,
    pub d_before: BigInt,
    /// Gross LP fee budget of the step, before fee_split's protocol share.
    pub fee_budget: BigInt,
}

/// One tag-3 swap: `dx` of asset `in_idx` in, asset `1 − in_idx` out
/// (`scenario.ss_swap_rated` plus the budget). `d_before` may be passed when
/// the caller already holds `D` for `reserves`.
pub fn swap_step(
    p: &SsParams,
    reserves: &[BigInt],
    total_lp: &BigInt,
    in_idx: usize,
    dx: &BigInt,
    d_before: Option<&BigInt>,
) -> Result<SsSwapResult, String> {
    p.check_shape(reserves)?;
    if in_idx > 1 {
        return Err("in_idx must be 0 or 1".into());
    }
    if !dx.is_positive() {
        return Err("swap amount must be > 0".into());
    }
    let out_idx = 1 - in_idx;
    let d_before = match d_before {
        Some(d) => d.clone(),
        None => p.d_of(reserves)?,
    };
    let in_after = &reserves[in_idx] + dx;
    let out_before = &reserves[out_idx];
    let p_in = &p.rates[in_idx];
    let p_out = &p.rates[out_idx];
    let raw = get_raw_swap_rated(&p.amp, &d_before, &in_after, p_in, out_before, p_out)?;
    let swap_result = floor_div(&raw, &(p_out * &precision()));
    let fee = ceil_div(&(&swap_result * &p.fee.num), &p.fee.den);
    let dy = &swap_result - &fee;
    if !dy.is_positive() {
        return Err(format!("swap of {dx} yields no output"));
    }
    let out_after = out_before - &dy;
    if out_after.is_negative() {
        return Err("swap drains the out reserve".into());
    }
    let reserves_after = if in_idx == 0 {
        vec![in_after.clone(), out_after]
    } else {
        vec![out_after, in_after.clone()]
    };
    let next_d = p.d_of(&reserves_after)?;
    // Self-verification with the module's own two-sided checks. A step that
    // fails here would be rejected on chain; never let it reach a tx.
    let x_scaled = &(&in_after * p_in) * &precision();
    let out_scale = p_out * &precision();
    if !exchange_invariant(
        &x_scaled,
        &(out_before * &out_scale),
        &raw,
        &p.amp,
        &d_before,
    ) {
        return Err("ss swap_step: exchange_invariant self-check failed".into());
    }
    if !liquidity_invariant(
        &x_scaled,
        &(&reserves_after[out_idx] * &out_scale),
        &p.amp,
        &next_d,
    ) {
        return Err("ss swap_step: liquidity_invariant self-check failed".into());
    }
    let fee_budget = fee_budget(&d_before, &next_d, total_lp);
    if fee_budget.is_negative() {
        return Err("ss swap_step: negative fee budget".into());
    }
    Ok(SsSwapResult {
        raw,
        swap_result,
        fee,
        dy,
        reserves_after,
        next_d,
        d_before,
        fee_budget,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SsLiquidityResult {
    /// `LiquidityStep.target_delta_d` (> 0 deposit, < 0 withdraw)
    pub target_delta_d: BigInt,
    /// Per-asset reserve change, `ceil(r_i · t / D_before)`.
    pub deltas: Vec<BigInt>,
    pub reserves_after: Vec<BigInt>,
    pub lp_after: BigInt,
    /// `lp_after − total_lp`: minted (> 0) or burned (< 0).
    pub lp_delta: BigInt,
    /// `LiquidityStep.next_sum_invariant`
    pub next_d: BigInt,
    pub d_before: BigInt,
}

/// One target-pinned liquidity step (tag 6 for `t > 0`, tag 4 for `t < 0`):
/// every reserve moves by `ceil(r_i · t / D_before)` and `total_lp` becomes
/// `floor(lp · (D_before + t) / D_before)` (`ss_check.check_liquidity`).
pub fn liquidity_step(
    p: &SsParams,
    reserves: &[BigInt],
    total_lp: &BigInt,
    target_delta_d: &BigInt,
    d_before: Option<&BigInt>,
) -> Result<SsLiquidityResult, String> {
    p.check_shape(reserves)?;
    if target_delta_d.is_zero() {
        return Err("target_delta_d must be non-zero".into());
    }
    if target_delta_d.is_negative() && !total_lp.is_positive() {
        return Err("withdrawal from a pool with no LP supply".into());
    }
    let d_before = match d_before {
        Some(d) => d.clone(),
        None => p.d_of(reserves)?,
    };
    if !d_before.is_positive() {
        return Err("pool D is zero".into());
    }
    let deltas: Vec<BigInt> =
        reserves.iter().map(|r| ceil_div(&(r * target_delta_d), &d_before)).collect();
    let reserves_after: Vec<BigInt> =
        reserves.iter().zip(deltas.iter()).map(|(r, d)| r + d).collect();
    if reserves_after.iter().any(|r| r.is_negative()) {
        return Err("withdrawal exceeds the reserves".into());
    }
    let lp_after = floor_div(&(total_lp * &(&d_before + target_delta_d)), &d_before);
    if target_delta_d.is_positive() && &lp_after <= total_lp {
        return Err("deposit issues no LP: raise the target delta".into());
    }
    let next_d = p.d_of(&reserves_after)?;
    Ok(SsLiquidityResult {
        target_delta_d: target_delta_d.clone(),
        deltas,
        reserves_after,
        lp_after: lp_after.clone(),
        lp_delta: &lp_after - total_lp,
        next_d,
        d_before,
    })
}

/// The largest deposit target a depositor holding `have` (pool-ordered token
/// amounts) can cover: `t = min_i floor(have_i · D_before / r_i)`, so that
/// `ceil(r_i · t / D_before) ≤ have_i` for every asset.
pub fn deposit_target_for_holdings(
    p: &SsParams,
    reserves: &[BigInt],
    have: &[BigInt],
    d_before: Option<&BigInt>,
) -> Result<BigInt, String> {
    p.check_shape(reserves)?;
    if have.len() != reserves.len() {
        return Err("deposit basket not aligned with pool reserves".into());
    }
    let d_before = match d_before {
        Some(d) => d.clone(),
        None => p.d_of(reserves)?,
    };
    let mut t: Option<BigInt> = None;
    for (h, r) in have.iter().zip(reserves.iter()) {
        if r.is_zero() {
            continue;
        }
        let cap = floor_div(&(h * &d_before), r);
        t = Some(match t {
            None => cap,
            Some(prev) => {
                if cap < prev {
                    cap
                } else {
                    prev
                }
            }
        });
    }
    Ok(t.unwrap_or_else(|| BigInt::from(0)))
}

/// The withdrawal target that burns exactly `lp_burn` LP: the `t < 0` with
/// `floor(lp · (D_before + t) / D_before) = lp − lp_burn`. Smallest `|t|` in
/// the admissible range, verified by recomputation.
pub fn withdraw_target_for_lp(
    p: &SsParams,
    reserves: &[BigInt],
    total_lp: &BigInt,
    lp_burn: &BigInt,
    d_before: Option<&BigInt>,
) -> Result<BigInt, String> {
    p.check_shape(reserves)?;
    if !lp_burn.is_positive() || lp_burn > total_lp {
        return Err("lp_burn must be in (0, total_lp]".into());
    }
    let d_before = match d_before {
        Some(d) => d.clone(),
        None => p.d_of(reserves)?,
    };
    let want = total_lp - lp_burn;
    let mut t = ceil_div(&(&(-lp_burn) * &d_before), total_lp);
    let one = BigInt::from(1);
    // The bracket [−L·D/lp, (−L+1)·D/lp) has width D/lp ≥ 1 whenever lp ≤ D,
    // so the ceiling lands inside it; the loop guards the general case.
    for _ in 0..4 {
        let got = floor_div(&(total_lp * &(&d_before + &t)), &d_before);
        if got == want {
            return Ok(t);
        }
        t = if got > want { &t - &one } else { &t + &one };
    }
    Err(format!("no withdrawal target burns exactly {lp_burn} LP"))
}

/// The policy of a tag-7 update (`ss_check.rate_step_allowed`), stated on
/// the relative price `rate_1 / rate_0`: under `monotone` the ratio may not
/// fall, and under `max_step = s` it may move by at most the fraction `s`.
/// The scooper never originates a rate update; this is the reference for
/// tests and for tooling that does.
#[cfg(test)]
pub fn rate_step_allowed(
    old_rates: &[BigInt],
    new_rates: &[BigInt],
    monotone: bool,
    max_step: Option<&Rational>,
) -> bool {
    if old_rates.len() != 2 || new_rates.len() != 2 {
        return false;
    }
    let ratio_delta = &(&new_rates[1] * &old_rates[0]) - &(&old_rates[1] * &new_rates[0]);
    if monotone && ratio_delta.is_negative() {
        return false;
    }
    if let Some(s) = max_step {
        let delta = ratio_delta.abs();
        if &delta * &s.den > &(&old_rates[1] * &new_rates[0]) * &s.num {
            return false;
        }
    }
    true
}

/// Apply a tag-7 rate update off-chain: check the policy, then re-derive `D`
/// on the unchanged reserves at `new_rates`. Returns `(D_before, D_after)`.
/// Test/tooling reference; the scooper never originates a rate update.
#[cfg(test)]
pub fn rate_update_step(
    cfg: &StableSwapConfig,
    reserves: &[BigInt],
    new_rates: &[BigInt],
) -> Result<(BigInt, BigInt), String> {
    if new_rates.len() != 2 {
        return Err("stableswap needs exactly two rates".into());
    }
    let cap = max_rate();
    for r in new_rates {
        if !r.is_positive() || r > &cap {
            return Err(format!("rate {r} out of (0, 2^64]"));
        }
    }
    if !rate_step_allowed(
        &cfg.rates,
        new_rates,
        cfg.monotone_rates,
        cfg.max_rate_step.as_ref(),
    ) {
        return Err("rates violate the pool policy on rate_1 / rate_0".into());
    }
    let before = SsParams::from_config(cfg);
    let d_before = before.d_of(reserves)?;
    let after = SsParams {
        rates: new_rates.to_vec(),
        ..before
    };
    let d_after = after.d_of(reserves)?;
    Ok((d_before, d_after))
}

// ─── Config hashing ─────────────────────────────────────────────────────────

/// `module_state` slot value: `blake2b_256(serialise_data(config))`.
pub fn config_hash(cfg: &StableSwapConfig) -> Vec<u8> {
    use plutus_parser::AsPlutus;
    let cbor = minicbor::to_vec(cfg.clone().to_plutus()).expect("PlutusData encodes");
    pallas_crypto::hash::Hasher::<256>::hash(&cbor).to_vec()
}

/// True when `cfg` is the preimage of the stableswap slot in `datum`'s
/// `module_state`. The scooper prices and builds only against a config that
/// passes this check; a config that fails it is stale (a rate update this
/// scooper has not indexed) or wrong.
pub fn config_matches_datum(
    cfg: &StableSwapConfig,
    datum: &crate::sundaev4::types::PoolDatum,
    ss_module_hash: &[u8],
) -> bool {
    datum
        .module_state
        .iter()
        .find(|(cred, _)| cred.as_slice() == ss_module_hash)
        .is_some_and(|(_, stored)| *stored == config_hash(cfg))
}

/// The Create-time bounds of `validators/modules/stableswap.ak` that a
/// config the scooper holds must satisfy. A config that fails these did not
/// come from a real pool; refuse to price against it.
pub fn check_config(cfg: &StableSwapConfig) -> Result<(), String> {
    let cap = max_rate();
    if cfg.rates.len() != 2 {
        return Err("stableswap needs exactly two rates".into());
    }
    for r in &cfg.rates {
        if !r.is_positive() || r > &cap {
            return Err(format!("rate {r} out of (0, 2^64]"));
        }
    }
    let a = &cfg.linear_amplification;
    if !a.is_positive() || a > &cap {
        return Err(format!("linear_amplification {a} out of (0, 2^64]"));
    }
    let Rational { num, den } = &cfg.fee;
    if !den.is_positive() || num.is_negative() || num >= den || *num > cap || *den > cap {
        return Err(format!(
            "fee {num}/{den} must satisfy 0 <= num < den <= 2^64"
        ));
    }
    if let Some(s) = &cfg.max_rate_step {
        if !s.den.is_positive() || s.num.is_negative() || s.num > s.den || s.den > cap {
            return Err(format!(
                "max_rate_step {}/{} must satisfy 0 <= num <= den <= 2^64",
                s.num, s.den
            ));
        }
        for r in &cfg.rates {
            if r * &s.num < s.den {
                return Err(format!(
                    "rate {r} is too small for max_rate_step {}/{}",
                    s.num, s.den
                ));
            }
        }
    }
    Ok(())
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use num_traits::Num;

    fn big(s: &str) -> BigInt {
        BigInt::from_str_radix(s, 10).unwrap()
    }

    fn bi(i: i64) -> BigInt {
        BigInt::from(i)
    }

    fn unit_params(amp: i64, fee: (i64, i64)) -> SsParams {
        SsParams {
            amp: bi(amp),
            fee: Rational {
                num: bi(fee.0),
                den: bi(fee.1),
            },
            rates: vec![bi(1), bi(1)],
        }
    }

    /// The preview pool's config (`test/devnet/STABLESWAP.md`).
    fn preview_config(rates: (i64, i64)) -> StableSwapConfig {
        StableSwapConfig {
            linear_amplification: bi(200),
            fee: Rational {
                num: bi(25),
                den: bi(10000),
            },
            rates: vec![bi(rates.0), bi(rates.1)],
            rate_manager: Some(crate::multisig::Multisig::Signature(
                hex::decode("e2afcadc7b111be7b89f283e9facffbbc5292f40fd13d7613e639c35").unwrap(),
            )),
            monotone_rates: true,
            max_rate_step: Some(Rational {
                num: bi(1),
                den: bi(100),
            }),
        }
    }

    #[test]
    fn floor_and_ceil_div_follow_plutus() {
        assert_eq!(floor_div(&bi(7), &bi(2)), bi(3));
        assert_eq!(floor_div(&bi(-7), &bi(2)), bi(-4));
        assert_eq!(floor_div(&bi(7), &bi(-2)), bi(-4));
        assert_eq!(floor_div(&bi(-8), &bi(2)), bi(-4));
        assert_eq!(ceil_div(&bi(7), &bi(2)), bi(4));
        assert_eq!(ceil_div(&bi(-7), &bi(2)), bi(-3));
        assert_eq!(ceil_div(&bi(8), &bi(2)), bi(4));
    }

    // ── Aiken vectors (lib/tests/unit/ss_swap.ak) ──────────────────────────

    #[test]
    fn aiken_get_d_balanced() {
        let d = get_d(&bi(200), &bi(1_000_000_000), &bi(1_000_000_000)).unwrap();
        assert_eq!(d, big("2000000000000000000000"));
    }

    #[test]
    fn aiken_get_raw_swap_vectors() {
        let d = big("2000000000000000000000");
        let raw = get_raw_swap(&bi(200), &d, &bi(1_010_000_000), &bi(1_000_000_000)).unwrap();
        assert_eq!(raw, big("9999750604846068206"));
        let raw = get_raw_swap(&bi(200), &d, &bi(1_001_000_000), &bi(1_000_000_000)).unwrap();
        assert_eq!(raw, big("999997506238151489"));
    }

    #[test]
    fn aiken_get_d_after_small_swap() {
        let d = get_d(&bi(200), &bi(1_001_000_000), &bi(1_000_000_000 - 999_497)).unwrap();
        assert_eq!(d, big("2000000500507486765233"));
    }

    #[test]
    fn aiken_fixture_swap_1e7_at_3_per_mille() {
        let p = unit_params(200, (3, 1000));
        let reserves = vec![bi(1_000_000_000), bi(1_000_000_000)];
        let lp = big("2000000000");
        let s = swap_step(&p, &reserves, &lp, 0, &bi(10_000_000), None).unwrap();
        assert_eq!(s.raw, big("9999750604846068206"));
        assert_eq!(s.swap_result, bi(9_999_750));
        assert_eq!(s.fee, bi(30_000));
        assert_eq!(s.dy, bi(9_969_750));
        assert_eq!(
            s.reserves_after,
            vec![bi(1_010_000_000), bi(1_000_000_000 - 9_969_750)]
        );
        // Both integer checks the module runs hold on the step.
        let x = &s.reserves_after[0] * &precision();
        let y = &s.reserves_after[1] * &precision();
        assert!(liquidity_invariant(&x, &y, &bi(200), &s.next_d));
        assert!(exchange_invariant(
            &x,
            &(&bi(1_000_000_000) * &precision()),
            &s.raw,
            &bi(200),
            &s.d_before
        ));
        assert!(s.fee_budget.is_positive());
    }

    // ── Preview transactions (test/devnet/STABLESWAP.md, pool ba4a9cd4…) ──

    #[test]
    fn preview_create_d_and_initial_lp() {
        let cfg = preview_config((1_000_000, 1_000_000));
        let p = SsParams::from_config(&cfg);
        let d = p.d_of(&[bi(10_000_000_000), bi(10_000_000_000)]).unwrap();
        assert_eq!(d, big("20000000000000000000000000000"));
        assert_eq!(floor_div(&d, &precision()), big("20000000000000000"));
    }

    #[test]
    fn preview_step6_swap() {
        let cfg = preview_config((1_000_000, 1_000_000));
        let p = SsParams::from_config(&cfg);
        let reserves = vec![bi(10_000_000_000), bi(10_000_000_000)];
        let lp = big("20000000000000000");
        let s = swap_step(&p, &reserves, &lp, 0, &bi(100_000_000), None).unwrap();
        assert_eq!(s.raw, big("99997506048460682068311439"));
        assert_eq!(s.swap_result, bi(99_997_506));
        assert_eq!(s.fee, bi(249_994));
        assert_eq!(s.dy, bi(99_747_512));
        assert_eq!(
            s.reserves_after,
            vec![bi(10_100_000_000), bi(9_900_252_488)]
        );
        assert_eq!(s.next_d, big("20000250000311092943744940505"));
        assert_eq!(s.fee_budget, big("250000311092"));
        // fee_split at 1/5: total_lp after = 2e16 + floor(fb/5)
        let protocol = floor_div(&(&s.fee_budget * &bi(1)), &bi(5));
        assert_eq!(&lp + &protocol, big("20000050000062218"));
    }

    #[test]
    fn preview_step7_deposit() {
        let cfg = preview_config((1_000_000, 1_000_000));
        let p = SsParams::from_config(&cfg);
        let reserves = vec![bi(10_100_000_000), bi(9_900_252_488)];
        let lp = big("20000050000062218");
        let t = deposit_target_for_holdings(
            &p,
            &reserves,
            &[bi(1_000_000_000), bi(1_000_000_000)],
            None,
        )
        .unwrap();
        assert_eq!(t, big("1980222772308029004331182228"));
        let l = liquidity_step(&p, &reserves, &lp, &t, None).unwrap();
        assert_eq!(l.deltas, vec![bi(1_000_000_000), bi(980_223_019)]);
        assert_eq!(l.lp_delta, big("1980202970303189"));
        assert_eq!(
            l.reserves_after,
            vec![bi(11_100_000_000), bi(10_880_475_507)]
        );
        assert_eq!(l.lp_after, big("21980252970365407"));
        assert_eq!(l.next_d, big("21980472773005270229001643178"));
    }

    #[test]
    fn preview_step8_withdraw() {
        let cfg = preview_config((1_000_000, 1_000_000));
        let p = SsParams::from_config(&cfg);
        let reserves = vec![bi(11_100_000_000), bi(10_880_475_507)];
        let lp = big("21980252970365407");
        let t = withdraw_target_for_lp(&p, &reserves, &lp, &big("1000000000000000"), None).unwrap();
        assert_eq!(t, big("-1000010000005011751915692890"));
        let l = liquidity_step(&p, &reserves, &lp, &t, None).unwrap();
        assert_eq!(l.deltas, vec![bi(-504_998_737), bi(-495_011_386)]);
        assert_eq!(l.lp_delta, big("-1000000000000000"));
        assert_eq!(
            l.reserves_after,
            vec![bi(10_595_001_263), bi(10_385_464_121)]
        );
        assert_eq!(l.lp_after, big("20980252970365407"));
        assert_eq!(l.next_d, big("20980462774389413969059047428"));
    }

    #[test]
    fn preview_step9_rate_update() {
        let cfg = preview_config((1_000_000, 1_000_000));
        let reserves = vec![bi(10_595_001_263), bi(10_385_464_121)];
        let (d_before, d_after) =
            rate_update_step(&cfg, &reserves, &[bi(1_000_000), bi(1_001_000)]).unwrap();
        assert_eq!(d_before, big("20980462774389413969059047428"));
        assert_eq!(d_after, big("20990848491973490189010679074"));
        // Policy: the ratio may not fall, and may not move by more than 1%.
        assert!(rate_update_step(&cfg, &reserves, &[bi(1_000_000), bi(999_000)]).is_err());
        assert!(rate_update_step(&cfg, &reserves, &[bi(1_000_000), bi(1_020_000)]).is_err());
        assert!(rate_update_step(&cfg, &reserves, &[bi(1_000_000), bi(1_010_000)]).is_ok());
    }

    #[test]
    fn preview_step10_swap_at_new_rates() {
        let cfg = preview_config((1_000_000, 1_001_000));
        let p = SsParams::from_config(&cfg);
        let reserves = vec![bi(10_595_001_263), bi(10_385_464_121)];
        let lp = big("20980252970365407");
        let s = swap_step(&p, &reserves, &lp, 0, &bi(100_000_000), None).unwrap();
        assert_eq!(s.d_before, big("20990848491973490189010679074"));
        assert_eq!(s.raw, big("99992889147916277844231960"));
        assert_eq!(s.swap_result, bi(99_892_996));
        assert_eq!(s.fee, bi(249_733));
        assert_eq!(s.dy, bi(99_643_263));
        assert_eq!(
            s.reserves_after,
            vec![bi(10_695_001_263), bi(10_285_820_858)]
        );
        assert_eq!(s.next_d, big("20991098486829941094949407134"));
        assert_eq!(s.fee_budget, big("249868666892"));
        let protocol = floor_div(&s.fee_budget, &bi(5));
        assert_eq!(&lp + &protocol, big("20980302944098785"));
    }

    #[test]
    fn swap_in_reverse_direction_verifies() {
        let cfg = preview_config((1_000_000, 1_001_000));
        let p = SsParams::from_config(&cfg);
        let reserves = vec![bi(10_595_001_263), bi(10_385_464_121)];
        let lp = big("20980252970365407");
        let s = swap_step(&p, &reserves, &lp, 1, &bi(100_000_000), None).unwrap();
        assert_eq!(s.reserves_after[1], bi(10_485_464_121));
        assert!(s.dy.is_positive());
        // The yield asset is worth 0.1% more, so 100 sUSDrf buys more than
        // 100 USDrf minus the fee.
        assert!(s.dy > bi(99_747_512));
    }

    // ── Config hashing (cross-checked against the TS port's ssConfigHash) ─

    #[test]
    fn config_hash_matches_preview_module_state() {
        use plutus_parser::AsPlutus;
        let cfg = preview_config((1_000_000, 1_000_000));
        let cbor = minicbor::to_vec(cfg.clone().to_plutus()).unwrap();
        assert_eq!(
            hex::encode(&cbor),
            "d8799f18c8d8799f1819192710ff9f1a000f42401a000f4240ffd8799fd8799f581ce2afcadc7b111be7b89f283e9facffbbc5292f40fd13d7613e639c35ffffd87a80d8799fd8799f011864ffffff"
        );
        assert_eq!(
            hex::encode(config_hash(&cfg)),
            "a57ae7c9b0209b62d53184b4356329d26b1884b31a6d5a4936135196ed9218d3"
        );
        // After the tag-7 update on preview the pool's module_state slot holds:
        let after = preview_config((1_000_000, 1_001_000));
        assert_eq!(
            hex::encode(config_hash(&after)),
            "c4e995e4d21e29ce3a061b3503c732145e7741d7b830629fcd914105418421a7"
        );
        // Round trip.
        let decoded = StableSwapConfig::from_plutus(cfg.clone().to_plutus()).unwrap();
        assert_eq!(decoded, cfg);
    }

    #[test]
    fn config_hash_with_no_manager_and_no_cap() {
        use plutus_parser::AsPlutus;
        let cfg = StableSwapConfig {
            rate_manager: None,
            monotone_rates: false,
            max_rate_step: None,
            ..preview_config((1_000_000, 1_000_000))
        };
        let cbor = minicbor::to_vec(cfg.clone().to_plutus()).unwrap();
        assert_eq!(
            hex::encode(&cbor),
            "d8799f18c8d8799f1819192710ff9f1a000f42401a000f4240ffd87a80d87980d87a80ff"
        );
        assert_eq!(
            hex::encode(config_hash(&cfg)),
            "23e4b66de1ce4c26a16665541d7f7badc6ed1f5f8449de9423218437f0672ba7"
        );
    }

    #[test]
    fn check_config_enforces_create_bounds() {
        assert!(check_config(&preview_config((1_000_000, 1_000_000))).is_ok());
        let mut c = preview_config((1_000_000, 1_000_000));
        c.rates = vec![bi(1), bi(1)];
        // rate · num ≥ den fails for a 1% cap at rate 1.
        assert!(check_config(&c).is_err());
        let mut c = preview_config((1_000_000, 1_000_000));
        c.fee = Rational {
            num: bi(1),
            den: bi(1),
        };
        assert!(check_config(&c).is_err());
        let mut c = preview_config((1_000_000, 1_000_000));
        c.linear_amplification = bi(0);
        assert!(check_config(&c).is_err());
    }

    #[test]
    fn deposit_rounding_never_exceeds_holdings() {
        let p = unit_params(200, (3, 1000));
        let reserves = vec![bi(1_000_000_007), bi(1_999_999_943)];
        let have = vec![bi(10_000_019), bi(20_000_033)];
        let t = deposit_target_for_holdings(&p, &reserves, &have, None).unwrap();
        let l = liquidity_step(&p, &reserves, &big("3000000000"), &t, None).unwrap();
        for (d, h) in l.deltas.iter().zip(have.iter()) {
            assert!(d <= h, "delta {d} exceeds holding {h}");
        }
        assert!(l.lp_delta.is_positive());
        let x = &l.reserves_after[0] * &precision();
        let y = &l.reserves_after[1] * &precision();
        assert!(liquidity_invariant(&x, &y, &p.amp, &l.next_d));
    }

    #[test]
    fn withdraw_burns_exactly_the_requested_lp() {
        let p = unit_params(200, (3, 1000));
        let reserves = vec![bi(1_000_000_007), bi(1_999_999_943)];
        let lp = big("3000000000");
        let t = withdraw_target_for_lp(&p, &reserves, &lp, &bi(5_000_017), None).unwrap();
        let l = liquidity_step(&p, &reserves, &lp, &t, None).unwrap();
        assert_eq!(l.lp_delta, bi(-5_000_017));
        assert!(l.deltas.iter().all(|d| d.is_negative()));
    }
}
