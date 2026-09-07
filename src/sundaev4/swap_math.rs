use crate::bigint::BigInt;
use num_traits::{One, Signed, Zero};
use tracing::warn;

/// Integer square root via Newton's method (floor).
pub(crate) fn isqrt(n: &BigInt) -> BigInt {
    if n.is_negative() {
        panic!("isqrt of negative");
    }
    let two = BigInt::from(2);
    if *n < two {
        return n.clone();
    }
    let mut x = n.clone();
    let mut y = (&x + &BigInt::one()) / &two;
    while y < x {
        x = y.clone();
        y = (&x + n / &x) / &two;
    }
    x
}

/// Constant-product swap: dy = B * (dx - fee) / (A + (dx - fee))
pub fn cp_swap_result(
    reserve_in: &BigInt,
    reserve_out: &BigInt,
    dx: &BigInt,
    fee_num: u64,
    fee_den: u64,
) -> BigInt {
    let fee = dx * &BigInt::from(fee_num) / &BigInt::from(fee_den);
    let dx_eff = dx - &fee;
    reserve_out * &dx_eff / &(reserve_in + &dx_eff)
}

/// Fee budget: floor(lp_before * sqrt(k1/k0)) - lp_after
/// where k0 = a0*b0, k1 = a1*b1
pub fn cp_fee_budget(
    a0: &BigInt,
    b0: &BigInt,
    a1: &BigInt,
    b1: &BigInt,
    lp_before: &BigInt,
) -> BigInt {
    // isqrt(a1 * b1 * lp_before^2 / (a0 * b0)) - lp_before
    let numerator = a1 * b1 * lp_before * lp_before;
    let denominator = a0 * b0;
    if denominator.is_zero() {
        warn!(a0 = %a0, b0 = %b0, "cp_fee_budget: zero denominator (a0*b0=0)");
        return BigInt::from(0);
    }
    let quotient = &numerator / &denominator;
    if quotient.is_negative() {
        warn!(
            a0 = %a0, b0 = %b0, a1 = %a1, b1 = %b1, lp = %lp_before,
            "cp_fee_budget: negative quotient — pool reserves may be invalid"
        );
        return BigInt::from(0);
    }
    isqrt(&quotient) - lp_before
}

/// Constant-sum swap: dy such that v_increase = floor(input_value * fee_num / fee_den).
///
/// The on-chain CS validator checks that the pool value increase (v_increase)
/// is exactly floor(input_value * fee_num / fee_den). Working backwards:
///   v_increase = dx * prices[input] - dy * prices[output]
///   dy = (dx * prices[input] - v_increase) / prices[output]
///
/// Returns 0 if dy is not integer (swap impossible for this dx).
pub fn cs_swap_result(
    dx: &BigInt,
    prices: &[BigInt],
    input_idx: usize,
    output_idx: usize,
    fee_num: &BigInt,
    fee_den: &BigInt,
) -> BigInt {
    let input_value = dx * &prices[input_idx];
    let v_increase = &input_value * fee_num / fee_den;
    let numerator = &input_value - &v_increase;
    let price_out = &prices[output_idx];
    let rem = &numerator % price_out;
    if !rem.is_zero() {
        return BigInt::from(0); // swap impossible: dy not integer
    }
    &numerator / price_out
}

/// Largest constant-sum input `dx` whose output `dy` does not exceed the pool's
/// output reserve, so a fill can never drain the pool negative. Mirrors
/// [`cl_max_dx_for_reserve`]. Returns `None` if the fee makes the swap
/// ill-defined (`fee_den <= fee_num`) or the input price is non-positive.
///
/// `dy = (dx·p_in − floor(dx·p_in·fee_num/fee_den)) / p_out ≤ reserve_out`
///   ⟺  `numerator(dx) ≤ reserve_out·p_out`.
/// The fee floor makes the closed form a hair loose, so we clamp down until the
/// numerator actually fits (a couple of iterations at most).
pub fn cs_max_dx_for_reserve(
    reserve_out: &BigInt,
    prices: &[BigInt],
    input_idx: usize,
    output_idx: usize,
    fee_num: &BigInt,
    fee_den: &BigInt,
) -> Option<BigInt> {
    if !reserve_out.is_positive() {
        return Some(BigInt::from(0));
    }
    let p_in = &prices[input_idx];
    let p_out = &prices[output_idx];
    let fee_mult = fee_den - fee_num;
    if !fee_mult.is_positive() || !p_in.is_positive() {
        return None;
    }
    let target = reserve_out * p_out; // numerator(dx) must stay <= this
    let numerator = |dx: &BigInt| -> BigInt {
        let iv = dx * p_in;
        let v_inc = &iv * fee_num / fee_den;
        &iv - &v_inc
    };
    let mut dx_max = &(&target * fee_den) / &(p_in * &fee_mult);
    let one = BigInt::from(1);
    let mut guard = 0;
    while dx_max.is_positive() && numerator(&dx_max) > target {
        dx_max = &dx_max - &one;
        guard += 1;
        if guard > 8 {
            break;
        }
    }
    if !dx_max.is_positive() {
        return Some(BigInt::from(0));
    }
    Some(dx_max)
}

/// Concentrated-liquidity swap. The virtual-reserve formulas in the validator
/// aren't symmetric in (A,B): VA always uses spb, VB always uses spa, and the
/// denominators differ between A→B and B→A. Caller passes (a, b, lp) in pool-
/// positional order plus `is_a_input` to pick the direction.
///
/// VA = a·spb_num + L·spb_den, VB = b·spa_den + L·spa_num
///   A→B: dy = floor(VB · dVA_eff / ((VA + dVA_eff) · spa_den))   with dVA_eff = dx_eff·spb_num
///   B→A: dy = floor(VA · dVB_eff / ((VB + dVB_eff) · spb_num))   with dVB_eff = dx_eff·spa_num
pub fn cl_swap_result(
    a: &BigInt,
    b: &BigInt,
    lp: &BigInt,
    dx: &BigInt,
    is_a_input: bool,
    spa_num: &BigInt,
    spa_den: &BigInt,
    spb_num: &BigInt,
    spb_den: &BigInt,
    fee_num: &BigInt,
    fee_den: &BigInt,
) -> BigInt {
    let fee = dx * fee_num / fee_den;
    let dx_eff = dx - &fee;
    let va0 = &(a * spb_num) + &(lp * spb_den);
    let vb0 = &(b * spa_den) + &(lp * spa_num);
    if is_a_input {
        let dva_eff = &dx_eff * spb_num;
        let denom = &(&va0 + &dva_eff) * spa_den;
        if denom.is_zero() {
            warn!("cl_swap_result: zero denominator (A→B)");
            return BigInt::from(0);
        }
        &vb0 * &dva_eff / &denom
    } else {
        let dvb_eff = &dx_eff * spa_num;
        let denom = &(&vb0 + &dvb_eff) * spb_num;
        if denom.is_zero() {
            warn!("cl_swap_result: zero denominator (B→A)");
            return BigInt::from(0);
        }
        &va0 * &dvb_eff / &denom
    }
}

/// Maximum *raw* (pre-fee) dx that a CL pool can absorb without driving its
/// actual `reserve_out` below zero. Used by the router to cap allocations for
/// already-depleted pools — without this, CL virtual reserves can let the
/// formula produce dy > actual b, which fails value conservation at submit.
///
/// Returns `None` if the pool can't absorb any positive dx (e.g. reserve_out
/// already zero, or the pool is at its price boundary).
pub fn cl_max_dx_for_reserve(
    a: &BigInt,
    b: &BigInt,
    lp: &BigInt,
    is_a_input: bool,
    spa_num: &BigInt,
    spa_den: &BigInt,
    spb_num: &BigInt,
    spb_den: &BigInt,
    fee_num: &BigInt,
    fee_den: &BigInt,
) -> Option<BigInt> {
    let va0 = &(a * spb_num) + &(lp * spb_den);
    let vb0 = &(b * spa_den) + &(lp * spa_num);
    let fee_mult = fee_den - fee_num;
    if !fee_mult.is_positive() {
        return None;
    }
    if is_a_input {
        // dy = vb0·dva_eff / ((va0+dva_eff)·spa_den);  set dy = b:
        //   dva_eff_max = b·spa_den·va0 / (vb0 - b·spa_den) = b·spa_den·va0 / (L·spa_num)
        if !b.is_positive() {
            return Some(BigInt::from(0));
        }
        let denom = lp * spa_num;
        if !denom.is_positive() {
            return None;
        }
        let dva_eff_max = (b * spa_den * &va0) / &denom;
        if !dva_eff_max.is_positive() {
            return Some(BigInt::from(0));
        }
        let dx_eff_max = &dva_eff_max / spb_num;
        if !dx_eff_max.is_positive() {
            return Some(BigInt::from(0));
        }
        Some(&dx_eff_max * fee_den / &fee_mult)
    } else {
        if !a.is_positive() {
            return Some(BigInt::from(0));
        }
        let denom = lp * spb_den;
        if !denom.is_positive() {
            return None;
        }
        let dvb_eff_max = (a * spb_num * &vb0) / &denom;
        if !dvb_eff_max.is_positive() {
            return Some(BigInt::from(0));
        }
        let dx_eff_max = &dvb_eff_max / spa_num;
        if !dx_eff_max.is_positive() {
            return Some(BigInt::from(0));
        }
        Some(&dx_eff_max * fee_den / &fee_mult)
    }
}

/// Concentrated-liquidity fee budget via the quadratic formula.
///
/// `|C| = spb_num·spa_den − spb_den·spa_num`,
/// `B   = a1·spb_num·spa_num + b1·spb_den·spa_den`,
/// `A   = a1·b1·spb_num·spa_den`,
/// `fee_budget = floor((B + √(B² + 4·A·|C|)) / (2·|C|)) − lp_after`.
///
/// Returns 0 when `|C| ≤ 0` (degenerate range, treated as no fee).
pub fn cl_fee_budget(
    a1: &BigInt,
    b1: &BigInt,
    lp_after: &BigInt,
    spa_num: &BigInt,
    spa_den: &BigInt,
    spb_num: &BigInt,
    spb_den: &BigInt,
) -> BigInt {
    let abs_c = &(spb_num * spa_den) - &(spb_den * spa_num);
    if !abs_c.is_positive() {
        return BigInt::from(0);
    }
    let big_b = &(&(a1 * spb_num) * spa_num) + &(&(b1 * spb_den) * spa_den);
    let big_a = &(&(a1 * b1) * spb_num) * spa_den;
    let disc = &(&big_b * &big_b) + &(&(&BigInt::from(4) * &big_a) * &abs_c);
    let root = isqrt(&disc);
    &(&big_b + &root) / &(&BigInt::from(2) * &abs_c) - lp_after
}

/// The largest raw dx a CL pool can absorb while keeping the swap
/// **value-preserving** — i.e. `cl_fee_budget(after) >= 0`. A swap that drives
/// the fee budget negative makes the pool lose value, which the pool contract's
/// `check_lp_accounting` (`circulating_lp <= total_lp`) rejects. That happens
/// today for B-input swaps on any pool whose range floor sits above price 1.0,
/// because of a `spa_num`/`spa_den` scaling slip in the deployed B-input swap
/// (filed for audit — the fix is `cl_check.ak:103` `spa_num -> spa_den`). Until
/// that ships, this caps each CL pool at its value-preserving input so the
/// router never proposes a leg the validator would reject.
///
/// Binary-searches `[0, dx_reserve_cap]`. Returns `dx_reserve_cap` unchanged
/// when the whole range stays value-preserving (the common in-range case), so
/// healthy pools are unaffected; returns ~0 for a pool swapped in its
/// value-losing direction, effectively excluding it from that direction.
#[allow(clippy::too_many_arguments)]
pub fn cl_max_dx_value_preserving(
    a: &BigInt,
    b: &BigInt,
    lp: &BigInt,
    dx_reserve_cap: &BigInt,
    is_a_input: bool,
    spa_num: &BigInt,
    spa_den: &BigInt,
    spb_num: &BigInt,
    spb_den: &BigInt,
    fee_num: &BigInt,
    fee_den: &BigInt,
) -> BigInt {
    let fee_budget_after = |dx: &BigInt| -> BigInt {
        if !dx.is_positive() {
            return BigInt::from(0);
        }
        let dy = cl_swap_result(
            a, b, lp, dx, is_a_input, spa_num, spa_den, spb_num, spb_den, fee_num, fee_den,
        );
        // Reserves after the swap: the input side grows by dx (fee stays in the
        // pool), the output side shrinks by dy.
        let (a1, b1) = if is_a_input {
            (a + dx, b - &dy)
        } else {
            (a - &dy, b + dx)
        };
        cl_fee_budget(&a1, &b1, lp, spa_num, spa_den, spb_num, spb_den)
    };

    if !dx_reserve_cap.is_positive() {
        return BigInt::from(0);
    }
    // A pool that loses value on the SMALLEST possible swap is in deficit at
    // rest: its declared total_lp already exceeds the liquidity its reserves
    // support, so every dx below the deficit's break-even loses value and the
    // admissible set is a suffix, not a prefix. A single max-dx cap cannot
    // express that, so exclude the pool from this direction outright — which is
    // what we want anyway for a pool whose books don't balance. Without this
    // the scan below starts at cap/64 and never sees the deficit, handing back
    // the full reserve cap and letting the router drop a small allocation
    // straight into the value-losing zone (found by
    // `underfunded_cl_pool_is_excluded_from_routing`). One extra evaluation;
    // an on-curve pool answers `+` here and pays nothing further.
    if fee_budget_after(&BigInt::from(1)).is_negative() {
        return BigInt::from(0);
    }

    // Coarse-scan for the FIRST dx that loses value, then binary-refine within
    // the last value-preserving interval. `fee_budget` is monotonic in dx for an
    // on-curve pool (all-positive or all-negative), so the scan just confirms the
    // endpoint; but a malformed off-curve pool can be non-monotonic (value-losing
    // for a mid-range dx while its endpoints look fine), and a plain "check the
    // endpoint" fast-path would wrongly hand back the full reserve. Scanning from
    // 0 upward returns the largest cap whose ENTIRE prefix preserves value.
    const STEPS: u64 = 64;
    let steps = BigInt::from(STEPS);
    let mut last_ok = BigInt::from(0);
    let mut first_bad: Option<BigInt> = None;
    for i in 1..=STEPS {
        let dx = dx_reserve_cap * &BigInt::from(i) / &steps;
        if fee_budget_after(&dx).is_negative() {
            first_bad = Some(dx);
            break;
        }
        last_ok = dx;
    }
    let mut hi = match first_bad {
        None => return dx_reserve_cap.clone(), // whole range preserves value
        Some(bad) => bad,
    };
    let mut lo = last_ok;
    let one = BigInt::from(1);
    for _ in 0..128 {
        if &hi - &lo <= one {
            break;
        }
        let mid = &(&lo + &hi) / &BigInt::from(2);
        if fee_budget_after(&mid).is_negative() {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    lo
}

/// The CS fee-carve dock for one swap step (cs_check.check_swap, ADR-0013):
/// the bounty-obligation accrual the step must leave in the pool,
/// ⌈k·(Q_a·V_b − Q_b·V_a) / (k_den·n²·V_a·V_b)⌉ clamped at 0, where
/// Q = Σ_i (n·p_i·a_i − V)². Zero when the bounty is off or the swap
/// moves toward balance.
pub fn cs_dock(
    assets_before: &[BigInt],
    assets_after: &[BigInt],
    prices: &[BigInt],
    v_b: &BigInt,
    v_a: &BigInt,
    bounty_k: &super::types::Rational,
) -> BigInt {
    if bounty_k.num.is_zero() {
        return BigInt::from(0);
    }
    let n = BigInt::from(prices.len() as u64);
    let q_of = |assets: &[BigInt], v: &BigInt| -> BigInt {
        assets
            .iter()
            .zip(prices.iter())
            .fold(BigInt::from(0), |acc, (a, p)| {
                let dev = &(&(&n * p) * a) - v;
                &acc + &(&dev * &dev)
            })
    };
    let q_b = q_of(assets_before, v_b);
    let q_a = q_of(assets_after, v_a);
    let accrual_num = &bounty_k.num * &(&(&q_a * v_b) - &(&q_b * v_a));
    if !accrual_num.is_positive() {
        return BigInt::from(0);
    }
    let den = &(&(&bounty_k.den * &n) * &n) * &(v_a * v_b);
    (&(&accrual_num + &den) - &BigInt::from(1)) / &den
}

/// Fee budget for constant-sum pools (cs_check.check_swap bound 4, exact):
/// v_b = Σ(before_i·p_i), v_a = Σ(after_i·p_i),
/// fee_budget = floor((v_a − dock) · lp_before / v_b) − lp_before,
/// where `dock` is the bounty-obligation accrual (ADR-0013).
pub fn cs_fee_budget(
    assets_before: &[BigInt],
    assets_after: &[BigInt],
    lp_before: &BigInt,
    prices: &[BigInt],
    bounty_k: &super::types::Rational,
) -> BigInt {
    let v0: BigInt = assets_before
        .iter()
        .zip(prices.iter())
        .fold(BigInt::from(0), |acc, (a, p)| &acc + &(a * p));
    let v1: BigInt = assets_after
        .iter()
        .zip(prices.iter())
        .fold(BigInt::from(0), |acc, (a, p)| &acc + &(a * p));
    if v0.is_zero() {
        warn!("cs_fee_budget: zero denominator (v0=0)");
        return BigInt::from(0);
    }
    let dock = cs_dock(assets_before, assets_after, prices, &v0, &v1, bounty_k);
    &(&(&v1 - &dock) * lp_before) / &v0 - lp_before
}

/// Dispatch fee budget computation by pool type.
pub fn compute_fee_budget(
    pool_type: &super::types::PoolType,
    assets_before: &[(crate::cardano_types::AssetClass, BigInt)],
    assets_after: &[(crate::cardano_types::AssetClass, BigInt)],
    lp_before: &BigInt,
) -> BigInt {
    match pool_type {
        super::types::PoolType::ConstantProduct { .. } => {
            // CP pairwise: find which 2 assets changed (works for N-asset pools)
            let changed: Vec<usize> = (0..assets_before.len())
                .filter(|&i| assets_before[i].1 != assets_after[i].1)
                .collect();
            assert!(changed.len() == 2, "CP swap must change exactly 2 assets, got {}", changed.len());
            let (i, j) = (changed[0], changed[1]);
            cp_fee_budget(
                &assets_before[i].1, &assets_before[j].1,
                &assets_after[i].1, &assets_after[j].1,
                lp_before,
            )
        }
        super::types::PoolType::ConstantSum { prices, bounty_k, .. } => {
            let before: Vec<BigInt> = assets_before.iter().map(|(_, a)| a.clone()).collect();
            let after: Vec<BigInt> = assets_after.iter().map(|(_, a)| a.clone()).collect();
            cs_fee_budget(&before, &after, lp_before, prices, bounty_k)
        }
        super::types::PoolType::ConcentratedLiquidity { sqrt_price_a, sqrt_price_b, .. } => {
            // The CL fee budget is a pure function of the after-state and
            // the pool's sqrt-price bounds; the before-state determines
            // lp_before for callers that want the *delta* over a sequence
            // of swaps, but here we compute it as `formula − lp_after`
            // which already encodes both the achievable and tight bounds.
            cl_fee_budget(
                &assets_after[0].1, &assets_after[1].1, lp_before,
                &sqrt_price_a.num, &sqrt_price_a.den,
                &sqrt_price_b.num, &sqrt_price_b.den,
            )
        }
    }
}

/// Protocol LP = floor(fee_budget * ps_num / ps_den)
pub fn compute_protocol_lp(fee_budget: &BigInt, ps_num: u64, ps_den: u64) -> BigInt {
    fee_budget * &BigInt::from(ps_num) / &BigInt::from(ps_den)
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_traits::{Signed, Zero};
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4000))]

        /// INVARIANT: `cl_max_dx_for_reserve` must be a correct cap — every dx at
        /// or below it yields dy that does NOT exceed the output reserve. The
        /// router's `pool_can_absorb` trusts this to gate allocations; if it's
        /// wrong, the router green-lights a leg that drains the pool below zero
        /// (exactly the live quarantine we hit: dy=383k against a 350k reserve).
        #[test]
        fn cl_max_dx_never_over_drains(
            a in 1i64..2_000_000_000_000i64,
            b in 1i64..2_000_000_000_000i64,
            lp in 1i64..2_000_000_000_000i64,
            spa_num in 1i64..100_000i64,
            spa_den in 1i64..100_000i64,
            spb_num in 1i64..100_000i64,
            spb_den in 1i64..100_000i64,
            fee_num in 0i64..500i64,
            fee_den in 1_000i64..10_000i64,
            is_a_input in any::<bool>(),
            dx_permille in 0u64..=1000u64,
        ) {
            prop_assume!(fee_num < fee_den);
            // Order the two sqrt-price ratios so spa < spb (CL config invariant)
            // instead of rejecting — keeps the case yield high.
            let (spa_num, spa_den, spb_num, spb_den) =
                if spa_num * spb_den < spb_num * spa_den {
                    (spa_num, spa_den, spb_num, spb_den)
                } else {
                    (spb_num, spb_den, spa_num, spa_den)
                };
            prop_assume!(spa_num * spb_den < spb_num * spa_den); // skip exact-equal

            let a = BigInt::from(a);
            let b = BigInt::from(b);
            let lp = BigInt::from(lp);
            let spa_num = BigInt::from(spa_num);
            let spa_den = BigInt::from(spa_den);
            let spb_num = BigInt::from(spb_num);
            let spb_den = BigInt::from(spb_den);
            let fee_num = BigInt::from(fee_num);
            let fee_den = BigInt::from(fee_den);

            let max_dx = cl_max_dx_for_reserve(
                &a, &b, &lp, is_a_input,
                &spa_num, &spa_den, &spb_num, &spb_den, &fee_num, &fee_den,
            );
            if let Some(max_dx) = max_dx {
                if max_dx.is_positive() {
                    // Sample dx across [0, max_dx].
                    let dx = &max_dx * BigInt::from(dx_permille) / BigInt::from(1000u64);
                    if dx.is_positive() {
                        let dy = cl_swap_result(
                            &a, &b, &lp, &dx, is_a_input,
                            &spa_num, &spa_den, &spb_num, &spb_den, &fee_num, &fee_den,
                        );
                        let reserve_out = if is_a_input { &b } else { &a };
                        prop_assert!(
                            &dy <= reserve_out,
                            "CL over-drain within cap: dy={} > reserve_out={} \
                             (dx={}, max_dx={}, is_a_input={})",
                            dy, reserve_out, dx, max_dx, is_a_input
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_isqrt() {
        assert_eq!(isqrt(&BigInt::zero()), BigInt::zero());
        assert_eq!(isqrt(&BigInt::one()), BigInt::one());
        assert_eq!(isqrt(&BigInt::from(4)), BigInt::from(2));
        assert_eq!(isqrt(&BigInt::from(8)), BigInt::from(2));
        assert_eq!(isqrt(&BigInt::from(9)), BigInt::from(3));
        assert_eq!(isqrt(&BigInt::from(100)), BigInt::from(10));
    }

    #[test]
    fn test_cp_swap_result() {
        // Pool: 1_000_000 A, 1_000_000 B, fee 3/1000
        // Swap 10_000 A → B
        // fee = 10000 * 3 / 1000 = 30
        // dx_eff = 9970
        // dy = 1000000 * 9970 / (1000000 + 9970) = 9970000000 / 1009970 = 9871
        let dy = cp_swap_result(
            &BigInt::from(1_000_000),
            &BigInt::from(1_000_000),
            &BigInt::from(10_000),
            3,
            1000,
        );
        assert_eq!(dy, BigInt::from(9871));
    }

    #[test]
    fn test_cp_fee_budget() {
        // Before: 1M/1M, After: 1010000/990129, lp=1000000
        // k0 = 1e12, k1 = 1010000 * 990129
        // fee_budget = isqrt(k1 * lp^2 / k0) - lp
        let fb = cp_fee_budget(
            &BigInt::from(1_000_000),
            &BigInt::from(1_000_000),
            &BigInt::from(1_010_000),
            &BigInt::from(990_129),
            &BigInt::from(1_000_000),
        );
        // fee_budget should be small but positive
        assert!(fb > BigInt::zero());
    }

    #[test]
    fn test_compute_protocol_lp() {
        let fb = BigInt::from(100);
        let plp = compute_protocol_lp(&fb, 1, 2);
        assert_eq!(plp, BigInt::from(50));
    }

    #[test]
    fn test_cs_swap_result() {
        // Pool: prices [1_000_000, 1_000_000], fee 3/1000
        // Swap 10_000 of asset 0 → asset 1
        // input_value = 10_000 * 1_000_000 = 10_000_000_000
        // fee_mult = 1000 - 3 = 997
        // dy = 10_000_000_000 * 997 / (1_000_000 * 1000) = 9_970_000_000_000 / 1_000_000_000 = 9970
        let dy = cs_swap_result(
            &BigInt::from(10_000),
            &[BigInt::from(1_000_000), BigInt::from(1_000_000)],
            0,
            1,
            &BigInt::from(3),
            &BigInt::from(1000),
        );
        assert_eq!(dy, BigInt::from(9970));
    }

    #[test]
    fn test_cs_max_dx_for_reserve() {
        // Reproduces the preview wedge: pool USDr reserve = 622_977_567,
        // prices [1,1], fee 12/1000. A garbage order offered ~1.5e15 USDCx,
        // which cs_swap_result turned into a ~1.485e15 USDr `dy` — far beyond
        // the reserve — draining the pool negative (on-chain amt_after < 0).
        let prices = [BigInt::from(1), BigInt::from(1)];
        let reserve_out = BigInt::from(622_977_567u64);
        let (fee_num, fee_den) = (BigInt::from(12), BigInt::from(1000));

        // The huge order overshoots the reserve → must be capped/rejected.
        let huge = BigInt::from(1_503_764_146_505_130u64);
        let dy_huge = cs_swap_result(&huge, &prices, 0, 1, &fee_num, &fee_den);
        assert!(dy_huge > reserve_out, "precondition: huge fill overshoots reserve");

        let cap = cs_max_dx_for_reserve(&reserve_out, &prices, 0, 1, &fee_num, &fee_den)
            .expect("cap defined for a sane fee");
        assert_eq!(cap, BigInt::from(630_544_096u64));

        // At the cap, dy is exactly the reserve (amt_after == 0, allowed).
        assert_eq!(
            cs_swap_result(&cap, &prices, 0, 1, &fee_num, &fee_den),
            reserve_out
        );
        // One unit past the cap overshoots.
        let over = &cap + &BigInt::from(1);
        assert!(cs_swap_result(&over, &prices, 0, 1, &fee_num, &fee_den) > reserve_out);

        // Degenerate cases.
        assert_eq!(
            cs_max_dx_for_reserve(&BigInt::from(0), &prices, 0, 1, &fee_num, &fee_den),
            Some(BigInt::from(0))
        );
        assert_eq!(
            cs_max_dx_for_reserve(&reserve_out, &prices, 0, 1, &BigInt::from(1000), &BigInt::from(1000)),
            None // fee_den == fee_num
        );
    }

    #[test]
    fn test_cs_swap_result_different_prices() {
        // prices [2, 1], fee 0/1 (no fee)
        // Swap 100 of asset 0 → asset 1
        // input_value = 100 * 2 = 200
        // dy = 200 * 1 / (1 * 1) = 200
        let dy = cs_swap_result(
            &BigInt::from(100),
            &[BigInt::from(2), BigInt::from(1)],
            0,
            1,
            &BigInt::from(0),
            &BigInt::from(1),
        );
        assert_eq!(dy, BigInt::from(200));
    }

    #[test]
    fn test_cp_fee_budget_3asset_swap_0_2() {
        // 3-asset pool: swap assets [0] and [2], asset [1] unchanged
        // CP swap: reserve_in=1M, reserve_out=2M, dx=10000, fee=3/1000
        //   fee=30, dx_eff=9970, dy=2M*9970/(1M+9970)=19742
        // After: [1_010_000, 500_000, 1_980_258]  (k1 > k0 due to fee)
        use crate::cardano_types::AssetClass;
        let assets_before = vec![
            (AssetClass { policy: vec![], token: vec![] }, BigInt::from(1_000_000)),
            (AssetClass { policy: vec![1], token: vec![1] }, BigInt::from(500_000)),
            (AssetClass { policy: vec![2], token: vec![2] }, BigInt::from(2_000_000)),
        ];
        let assets_after = vec![
            (AssetClass { policy: vec![], token: vec![] }, BigInt::from(1_010_000)),
            (AssetClass { policy: vec![1], token: vec![1] }, BigInt::from(500_000)),
            (AssetClass { policy: vec![2], token: vec![2] }, BigInt::from(1_980_258)),
        ];
        let lp = BigInt::from(1_000_000);
        let pool_type = super::super::types::PoolType::ConstantProduct {
            fee: super::super::types::Rational { num: BigInt::from(3), den: BigInt::from(1000) },
        };
        let fb = super::compute_fee_budget(&pool_type, &assets_before, &assets_after, &lp);
        // Same as direct cp_fee_budget on just the changed pair
        let fb_direct = cp_fee_budget(
            &BigInt::from(1_000_000), &BigInt::from(2_000_000),
            &BigInt::from(1_010_000), &BigInt::from(1_980_258),
            &lp,
        );
        assert_eq!(fb, fb_direct);
        assert!(fb > BigInt::zero());
    }

    #[test]
    fn test_cp_fee_budget_4asset_swap_1_3() {
        // 4-asset pool: swap assets [1] and [3], assets [0] and [2] unchanged
        use crate::cardano_types::AssetClass;
        let assets_before = vec![
            (AssetClass { policy: vec![], token: vec![] }, BigInt::from(1_000_000)),
            (AssetClass { policy: vec![1], token: vec![1] }, BigInt::from(1_000_000)),
            (AssetClass { policy: vec![2], token: vec![2] }, BigInt::from(1_000_000)),
            (AssetClass { policy: vec![3], token: vec![3] }, BigInt::from(1_000_000)),
        ];
        let assets_after = vec![
            (AssetClass { policy: vec![], token: vec![] }, BigInt::from(1_000_000)),
            (AssetClass { policy: vec![1], token: vec![1] }, BigInt::from(1_010_000)),
            (AssetClass { policy: vec![2], token: vec![2] }, BigInt::from(1_000_000)),
            (AssetClass { policy: vec![3], token: vec![3] }, BigInt::from(990_129)),
        ];
        let lp = BigInt::from(1_000_000);
        let pool_type = super::super::types::PoolType::ConstantProduct {
            fee: super::super::types::Rational { num: BigInt::from(3), den: BigInt::from(1000) },
        };
        let fb = super::compute_fee_budget(&pool_type, &assets_before, &assets_after, &lp);
        // Should match direct call on just the [1],[3] pair
        let fb_direct = cp_fee_budget(
            &BigInt::from(1_000_000), &BigInt::from(1_000_000),
            &BigInt::from(1_010_000), &BigInt::from(990_129),
            &lp,
        );
        assert_eq!(fb, fb_direct);
        assert!(fb > BigInt::zero());
    }

    #[test]
    fn test_cs_swap_result_3asset() {
        // 3-asset CS pool: prices [1, 2, 3], fee 3/1000
        // Swap 300 of asset 0 → asset 2
        // input_value = 300 * 1 = 300
        // v_increase = floor(300 * 3 / 1000) = 0
        // numerator = 300 - 0 = 300
        // dy = 300 / 3 = 100
        let dy = cs_swap_result(
            &BigInt::from(300),
            &[BigInt::from(1), BigInt::from(2), BigInt::from(3)],
            0,
            2,
            &BigInt::from(3),
            &BigInt::from(1000),
        );
        assert_eq!(dy, BigInt::from(100));
    }

    #[test]
    fn test_cs_fee_budget_3asset() {
        // 3-asset CS pool: prices [1, 1, 1], lp 1M
        // Before: [1M, 1M, 1M], After: [1.01M, 1M, 990030]
        // v0 = 3M, v1 = 1_010_000 + 1_000_000 + 990_030 = 3_000_030
        // fee_budget = floor(3_000_030 * 1M / 3M) - 1M = 1_000_010 - 1_000_000 = 10
        let fb = cs_fee_budget(
            &[BigInt::from(1_000_000), BigInt::from(1_000_000), BigInt::from(1_000_000)],
            &[BigInt::from(1_010_000), BigInt::from(1_000_000), BigInt::from(990_030)],
            &BigInt::from(1_000_000),
            &[BigInt::from(1), BigInt::from(1), BigInt::from(1)],
            &crate::sundaev4::types::Rational { num: BigInt::from(0), den: BigInt::from(1) },
        );
        assert_eq!(fb, BigInt::from(10));
    }

    #[test]
    fn test_cs_fee_budget() {
        // Before: [1_000_000, 1_000_000], After: [1_010_000, 990_030]
        // prices: [1, 1], lp: 1_000_000
        // v0 = 1M + 1M = 2M, v1 = 1.01M + 990030 = 2_000_030
        // fee_budget = 2_000_030 * 1_000_000 / 2_000_000 - 1_000_000 = 15
        let fb = cs_fee_budget(
            &[BigInt::from(1_000_000), BigInt::from(1_000_000)],
            &[BigInt::from(1_010_000), BigInt::from(990_030)],
            &BigInt::from(1_000_000),
            &[BigInt::from(1), BigInt::from(1)],
            &crate::sundaev4::types::Rational { num: BigInt::from(0), den: BigInt::from(1) },
        );
        assert_eq!(fb, BigInt::from(15));
    }
}
