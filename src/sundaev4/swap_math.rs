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

/// Constant-sum swap: dy = floor((dx * prices[input_idx] * (fee_den - fee_num)) / (prices[output_idx] * fee_den))
pub fn cs_swap_result(
    dx: &BigInt,
    prices: &[BigInt],
    input_idx: usize,
    output_idx: usize,
    fee_num: &BigInt,
    fee_den: &BigInt,
) -> BigInt {
    let input_value = dx * &prices[input_idx];
    let fee_mult = fee_den - fee_num;
    &input_value * &fee_mult / &(&prices[output_idx] * fee_den)
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
            // CP uses pairwise reserves (always 2 assets)
            cp_fee_budget(
                &assets_before[0].1, &assets_before[1].1,
                &assets_after[0].1, &assets_after[1].1,
                lp_before,
            )
        }
        super::types::PoolType::ConstantSum { prices, .. } => {
            let before: Vec<BigInt> = assets_before.iter().map(|(_, a)| a.clone()).collect();
            let after: Vec<BigInt> = assets_after.iter().map(|(_, a)| a.clone()).collect();
            cs_fee_budget(&before, &after, lp_before, prices)
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
