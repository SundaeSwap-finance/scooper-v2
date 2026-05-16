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

/// Fee budget for constant-sum pools:
/// v0 = Σ(before_i * prices_i), v1 = Σ(after_i * prices_i)
/// fee_budget = floor(v1 * lp_before / v0) - lp_before
pub fn cs_fee_budget(
    assets_before: &[BigInt],
    assets_after: &[BigInt],
    lp_before: &BigInt,
    prices: &[BigInt],
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
    &v1 * lp_before / &v0 - lp_before
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
        super::types::PoolType::ConstantSum { prices, .. } => {
            let before: Vec<BigInt> = assets_before.iter().map(|(_, a)| a.clone()).collect();
            let after: Vec<BigInt> = assets_after.iter().map(|(_, a)| a.clone()).collect();
            cs_fee_budget(&before, &after, lp_before, prices)
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
    use num_traits::Zero;

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
        );
        assert_eq!(fb, BigInt::from(15));
    }
}
