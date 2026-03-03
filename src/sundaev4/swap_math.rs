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
}
