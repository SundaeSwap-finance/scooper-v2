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
    // Start just above √n: n < 2^bits, so 2^(bits/2 + 1) > √n. Newton's method
    // descends to ⌊√n⌋ from any start above it. (Ported from scooper-v2 PR #93:
    // starting at n spent ~150 big-integer divisions per call.)
    let bits = n.clone().unwrap().bits();
    let mut x = num_traits::pow(two.clone(), (bits / 2 + 1) as usize);
    let mut y = (&x + n / &x) / &two;
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

/// Constant-sum swap: the floor fill.
///
/// The on-chain CS validator (cs_check.ak, invariant 2) admits any pool value
/// increase in a window one output-unit wide:
///   floor(input_value·fee_num/fee_den) ≤ v_increase < floor(…) + p_out
/// so EVERY dx has a valid fill: pay out
///   dy = floor((input_value − floor(input_value·fee_num/fee_den)) / p_out)
/// and the sub-p_out division remainder stays with the pool inside
/// v_increase. (An earlier revision demanded the zero-remainder case only and
/// returned 0 otherwise — that stranded most wallet-built orders as "no
/// route" forever, e.g. preview order 4d0d9a9f…#0.)
pub fn cs_swap_result(
    dx: &BigInt,
    prices: &[BigInt],
    input_idx: usize,
    output_idx: usize,
    fee_num: &BigInt,
    fee_den: &BigInt,
) -> BigInt {
    let input_value = dx * &prices[input_idx];
    let v_increase_min = &input_value * fee_num / fee_den;
    let numerator = &input_value - &v_increase_min;
    let price_out = &prices[output_idx];
    &numerator / price_out
}

/// Largest constant-sum input `dx` whose output `dy` does not exceed the pool's
/// output reserve, so a fill can never drain the pool negative. Mirrors
/// the banded band cap. Returns `None` if the fee makes the swap
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
        assets.iter().zip(prices.iter()).fold(BigInt::from(0), |acc, (a, p)| {
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
    let v1: BigInt =
        assets_after.iter().zip(prices.iter()).fold(BigInt::from(0), |acc, (a, p)| &acc + &(a * p));
    if v0.is_zero() {
        warn!("cs_fee_budget: zero denominator (v0=0)");
        return BigInt::from(0);
    }
    let dock = cs_dock(assets_before, assets_after, prices, &v0, &v1, bounty_k);
    &(&(&v1 - &dock) * lp_before) / &v0 - lp_before
}

/// The stableswap step parameters for a pool's config.
pub fn ss_params(config: &super::types::StableSwapConfig) -> super::ss_math::SsParams {
    super::ss_math::SsParams::from_config(config)
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
            assert!(
                changed.len() == 2,
                "CP swap must change exactly 2 assets, got {}",
                changed.len()
            );
            let (i, j) = (changed[0], changed[1]);
            cp_fee_budget(
                &assets_before[i].1,
                &assets_before[j].1,
                &assets_after[i].1,
                &assets_after[j].1,
                lp_before,
            )
        }
        super::types::PoolType::ConstantSum {
            prices, bounty_k, ..
        } => {
            let before: Vec<BigInt> = assets_before.iter().map(|(_, a)| a.clone()).collect();
            let after: Vec<BigInt> = assets_after.iter().map(|(_, a)| a.clone()).collect();
            cs_fee_budget(&before, &after, lp_before, prices, bounty_k)
        }
        super::types::PoolType::StableSwap { config } => {
            // ss_check.check_swap bound 5: after_lp + fee_budget =
            // floor(before_lp · D_after / D_before). D is a function of the
            // rated reserves, so both sides derive from the asset lists.
            let p = super::ss_math::SsParams::from_config(config);
            let before: Vec<BigInt> = assets_before.iter().map(|(_, a)| a.clone()).collect();
            let after: Vec<BigInt> = assets_after.iter().map(|(_, a)| a.clone()).collect();
            match (p.d_of(&before), p.d_of(&after)) {
                (Ok(d_b), Ok(d_a)) if d_b.is_positive() => {
                    super::ss_math::fee_budget(&d_b, &d_a, lp_before)
                }
                _ => {
                    warn!("ss fee_budget: could not derive D for the step");
                    BigInt::from(0)
                }
            }
        }
        super::types::PoolType::BandedConcentratedLiquidity { config } => {
            // banded_cl_check.banded_swap step 9: `bp * X0 <= lp_before * X1
            // < (bp + 1) * X0` with `bp = lp_after + fee_budget`, on the two
            // ladder counters. A non-swap step must declare 0.
            if assets_before.len() != 2 || assets_after.len() != 2 {
                warn!("banded fee_budget: pool does not hold two assets");
                return BigInt::from(0);
            }
            let (a0, b0) = (&assets_before[0].1, &assets_before[1].1);
            let (a1, b1) = (&assets_after[0].1, &assets_after[1].1);
            let is_swap = (a1 > a0 && b1 < b0) || (a1 < a0 && b1 > b0);
            if !is_swap {
                return BigInt::from(0);
            }
            let (Some(before), Some(after)) = (
                super::banded_math::find_witness(config, a0, b0),
                super::banded_math::find_witness(config, a1, b1),
            ) else {
                warn!("banded fee_budget: no ladder witness for the step's reserves");
                return BigInt::from(0);
            };
            super::banded_math::fee_budget(lp_before, &before.x, &after.x, lp_before)
        }
    }
}

/// Protocol LP = floor(fee_budget * ps_num / ps_den). Used in tests; the
/// production walk computes the per-entry share in `tx_builder`.
#[cfg(test)]
pub fn compute_protocol_lp(fee_budget: &BigInt, ps_num: u64, ps_den: u64) -> BigInt {
    fee_budget * &BigInt::from(ps_num) / &BigInt::from(ps_den)
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_traits::{Signed, Zero};
    use proptest::prelude::*;

    proptest! {
        /// INVARIANT: `isqrt(n)` is the floor square root, `r² <= n < (r+1)²`, up to
        /// 512-bit inputs (the router calls it on ~300-bit products). Inputs mix
        /// random n with n right around a perfect square, where the answer steps up
        /// and an off-by-one would show: random n almost never land there.
        #[test]
        fn isqrt_is_floor_sqrt(n in prop_oneof![any_big(), near_square()]) {
            let r = isqrt(&n);
            let r1 = &r + &BigInt::from(1);
            prop_assert!(&r * &r <= n, "isqrt({}) = {}: r² > n", n, r);
            prop_assert!(&r1 * &r1 > n, "isqrt({}) = {}: (r+1)² <= n", n, r);
        }
    }

    /// Any n from 0 to 512 bits.
    fn any_big() -> impl Strategy<Value = BigInt> {
        proptest::collection::vec(any::<u64>(), 0..9).prop_map(|limbs| from_limbs(&limbs))
    }

    /// n around a perfect square k² (k ≥ 1, up to 256 bits): k²-1, k², k²+1, or
    /// k²+2k = (k+1)²-1, the last n whose root is still k.
    fn near_square() -> impl Strategy<Value = BigInt> {
        (proptest::collection::vec(any::<u64>(), 0..5), 0u8..4).prop_map(|(limbs, which)| {
            let k = from_limbs(&limbs) + BigInt::from(1);
            let square = &k * &k;
            match which {
                0 => &square - &BigInt::from(1),
                1 => square,
                2 => &square + &BigInt::from(1),
                _ => &square + &(&k * &BigInt::from(2)),
            }
        })
    }

    /// Big integer from 64-bit limbs, most significant first.
    fn from_limbs(limbs: &[u64]) -> BigInt {
        let base = BigInt::from(1i128 << 64);
        limbs.iter().fold(BigInt::zero(), |acc, limb| &(&acc * &base) + &BigInt::from(*limb))
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
        assert!(
            dy_huge > reserve_out,
            "precondition: huge fill overshoots reserve"
        );

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
            cs_max_dx_for_reserve(
                &reserve_out,
                &prices,
                0,
                1,
                &BigInt::from(1000),
                &BigInt::from(1000)
            ),
            None // fee_den == fee_num
        );
    }

    #[test]
    fn test_cs_swap_result_floor_fill_unaligned() {
        // The order the scooper refused for an hour on preview (4d0d9a9f…#0):
        // 100.300903 ADA → STRW at prices [4, 5], fee 3/1000. The numerator
        // (input_value − floor(fee)) = 400_000_002 leaves remainder 2 mod 5 —
        // no zero-remainder dy exists, but the validator's one-out-unit fee
        // window admits the floor fill with the crumb staying in the pool.
        let prices = [BigInt::from(4), BigInt::from(5)];
        let (fee_num, fee_den) = (BigInt::from(3), BigInt::from(1000));
        let dx = BigInt::from(100_300_903u64);
        let dy = cs_swap_result(&dx, &prices, 0, 1, &fee_num, &fee_den);
        assert_eq!(dy, BigInt::from(80_000_000u64));

        // The fill must land inside the validator's window:
        //   floor(iv·fee) ≤ v_increase < floor(iv·fee) + p_out
        let iv = &dx * &prices[0];
        let fee_floor = &iv * &fee_num / &fee_den;
        let v_increase = &iv - &(&dy * &prices[1]);
        assert!(v_increase >= fee_floor);
        assert!(v_increase < &fee_floor + &prices[1]);

        // And across a sweep of arbitrary dx, never a zero fill, always
        // in-window (the old code returned 0 for ~4 of 5 of these).
        for i in 1u64..500 {
            let dx = BigInt::from(999_983u64 * i);
            let dy = cs_swap_result(&dx, &prices, 0, 1, &fee_num, &fee_den);
            assert!(dy.is_positive(), "dx={dx} produced no fill");
            let iv = &dx * &prices[0];
            let fee_floor = &iv * &fee_num / &fee_den;
            let v_increase = &iv - &(&dy * &prices[1]);
            assert!(v_increase >= fee_floor, "dx={dx} underpays the fee");
            assert!(
                v_increase < &fee_floor + &prices[1],
                "dx={dx} overpays past the window"
            );
        }
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
            (
                AssetClass {
                    policy: vec![],
                    token: vec![],
                },
                BigInt::from(1_000_000),
            ),
            (
                AssetClass {
                    policy: vec![1],
                    token: vec![1],
                },
                BigInt::from(500_000),
            ),
            (
                AssetClass {
                    policy: vec![2],
                    token: vec![2],
                },
                BigInt::from(2_000_000),
            ),
        ];
        let assets_after = vec![
            (
                AssetClass {
                    policy: vec![],
                    token: vec![],
                },
                BigInt::from(1_010_000),
            ),
            (
                AssetClass {
                    policy: vec![1],
                    token: vec![1],
                },
                BigInt::from(500_000),
            ),
            (
                AssetClass {
                    policy: vec![2],
                    token: vec![2],
                },
                BigInt::from(1_980_258),
            ),
        ];
        let lp = BigInt::from(1_000_000);
        let pool_type = super::super::types::PoolType::ConstantProduct {
            fee: super::super::types::Rational {
                num: BigInt::from(3),
                den: BigInt::from(1000),
            },
        };
        let fb = super::compute_fee_budget(&pool_type, &assets_before, &assets_after, &lp);
        // Same as direct cp_fee_budget on just the changed pair
        let fb_direct = cp_fee_budget(
            &BigInt::from(1_000_000),
            &BigInt::from(2_000_000),
            &BigInt::from(1_010_000),
            &BigInt::from(1_980_258),
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
            (
                AssetClass {
                    policy: vec![],
                    token: vec![],
                },
                BigInt::from(1_000_000),
            ),
            (
                AssetClass {
                    policy: vec![1],
                    token: vec![1],
                },
                BigInt::from(1_000_000),
            ),
            (
                AssetClass {
                    policy: vec![2],
                    token: vec![2],
                },
                BigInt::from(1_000_000),
            ),
            (
                AssetClass {
                    policy: vec![3],
                    token: vec![3],
                },
                BigInt::from(1_000_000),
            ),
        ];
        let assets_after = vec![
            (
                AssetClass {
                    policy: vec![],
                    token: vec![],
                },
                BigInt::from(1_000_000),
            ),
            (
                AssetClass {
                    policy: vec![1],
                    token: vec![1],
                },
                BigInt::from(1_010_000),
            ),
            (
                AssetClass {
                    policy: vec![2],
                    token: vec![2],
                },
                BigInt::from(1_000_000),
            ),
            (
                AssetClass {
                    policy: vec![3],
                    token: vec![3],
                },
                BigInt::from(990_129),
            ),
        ];
        let lp = BigInt::from(1_000_000);
        let pool_type = super::super::types::PoolType::ConstantProduct {
            fee: super::super::types::Rational {
                num: BigInt::from(3),
                den: BigInt::from(1000),
            },
        };
        let fb = super::compute_fee_budget(&pool_type, &assets_before, &assets_after, &lp);
        // Should match direct call on just the [1],[3] pair
        let fb_direct = cp_fee_budget(
            &BigInt::from(1_000_000),
            &BigInt::from(1_000_000),
            &BigInt::from(1_010_000),
            &BigInt::from(990_129),
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
            &[
                BigInt::from(1_000_000),
                BigInt::from(1_000_000),
                BigInt::from(1_000_000),
            ],
            &[
                BigInt::from(1_010_000),
                BigInt::from(1_000_000),
                BigInt::from(990_030),
            ],
            &BigInt::from(1_000_000),
            &[BigInt::from(1), BigInt::from(1), BigInt::from(1)],
            &crate::sundaev4::types::Rational {
                num: BigInt::from(0),
                den: BigInt::from(1),
            },
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
            &crate::sundaev4::types::Rational {
                num: BigInt::from(0),
                den: BigInt::from(1),
            },
        );
        assert_eq!(fb, BigInt::from(15));
    }
}
