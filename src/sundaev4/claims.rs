//! Constant-sum rebalance bounty ("claim") math.
//!
//! Port of `lib/modules/cs_check.ak`'s tag_claim (5) validation, waived-fee
//! mode only (`waive_fee_on_claim = true`):
//!
//! - The entry's swap portion is value-neutral: `dy · p_out = dx · p_in`
//!   exactly, no fee retained (`v_increase = 0`, `fee_budget = 0`,
//!   `before_lp == after_lp`).
//! - The claim extracts `c` units of the claim asset on top, bounded by
//!   cap_b: `k_num · (V_a·Q_b − V_b·Q_a) ≥ c·p_claim · k_den · N² · V_a·V_b`
//!   where `V = Σ p_i·r_i` (pool value) and `Q = Σ (N·p_i·r_i − V)²`
//!   (squared imbalance), with the after-state measured on the *actual*
//!   post-claim reserves.
//! - The op portion must still be a real swap (`has_inc && has_dec`), so a
//!   claim always rides on a nonzero `dx`.
//!
//! Mirrors the reference implementation in
//! `sundae-v4/test/devnet/src/actions/order.ts` (`claimBounty`).

use crate::bigint::BigInt;
use crate::cardano_types::AssetClass;

/// A fully-resolved waived-mode claim step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimPlan {
    /// Swap portion: `dx` of the input asset in…
    pub dx: BigInt,
    /// …`dy = dx·p_in/p_out` of the output asset out (fee waived).
    pub dy: BigInt,
    /// Bounty extracted on top of `dy`, same asset as the swap output.
    pub claim: BigInt,
    /// Pool reserves after swap + claim, in pool asset order.
    pub final_assets: Vec<(AssetClass, BigInt)>,
}

/// `V = Σ p_i · r_i`.
pub fn compute_v(reserves: &[(AssetClass, BigInt)], prices: &[BigInt]) -> BigInt {
    reserves
        .iter()
        .zip(prices)
        .fold(BigInt::from(0), |acc, ((_, r), p)| acc + r * p)
}

/// `Q = Σ (N·p_i·r_i − V)²` — zero when perfectly balanced.
pub fn compute_q(reserves: &[(AssetClass, BigInt)], prices: &[BigInt], v: &BigInt) -> BigInt {
    let n = BigInt::from(reserves.len() as u64);
    reserves
        .iter()
        .zip(prices)
        .fold(BigInt::from(0), |acc, ((_, r), p)| {
            let d = &(&n * &(r * p)) - v;
            acc + &d * &d
        })
}

/// Plan a waived-fee claim entry against a CS pool.
///
/// `reserves`/`prices` are the pool's current state in pool asset order;
/// `in_idx`/`out_idx` pick the swap direction (input = what the order
/// offers, output = what it receives and claims); `dx` is the swap input.
/// Returns the largest claim cap_b admits (possibly finding that even the
/// swap alone is infeasible → `None`).
///
/// Waived-mode requirements enforced here:
/// - `bounty_k.num > 0` (claims disabled otherwise)
/// - `dy` divides exactly (`dx·p_in % p_out == 0`) and `dy ≤ reserve_out`
/// - claim ≤ post-swap reserve of the claim asset, and `V_a > 0`
pub fn plan_waived_claim(
    reserves: &[(AssetClass, BigInt)],
    prices: &[BigInt],
    bounty_k: (&BigInt, &BigInt),
    in_idx: usize,
    out_idx: usize,
    dx: &BigInt,
) -> Option<ClaimPlan> {
    use num_traits::{Signed, Zero};

    let (k_num, k_den) = bounty_k;
    if !k_num.is_positive() || in_idx == out_idx || !dx.is_positive() {
        return None;
    }
    let n = reserves.len();
    if prices.len() != n || in_idx >= n || out_idx >= n {
        return None;
    }
    let p_in = &prices[in_idx];
    let p_out = &prices[out_idx];

    // Value-neutral swap: dy·p_out == dx·p_in exactly.
    let input_value = dx * p_in;
    if !(&input_value % p_out).is_zero() {
        return None;
    }
    let dy = &input_value / p_out;
    if !dy.is_positive() || dy > reserves[out_idx].1 {
        return None;
    }

    // Post-swap ("op portion") reserves.
    let mut after_op: Vec<(AssetClass, BigInt)> = reserves.to_vec();
    after_op[in_idx].1 = &after_op[in_idx].1 + dx;
    after_op[out_idx].1 = &after_op[out_idx].1 - &dy;

    let v_b = compute_v(reserves, prices);
    let q_b = compute_q(reserves, prices, &v_b);
    let n_big = BigInt::from(n as u64);

    // cap_b check for a candidate claim `c` on the actual after-state.
    let passes = |c: &BigInt| -> bool {
        if !c.is_positive() || *c > after_op[out_idx].1 {
            return false;
        }
        let mut after_actual = after_op.clone();
        after_actual[out_idx].1 = &after_actual[out_idx].1 - c;
        let v_a = compute_v(&after_actual, prices);
        if !v_a.is_positive() {
            return false;
        }
        let q_a = compute_q(&after_actual, prices, &v_a);
        let c_value = c * p_out;
        let lhs = k_num * &(&(&v_a * &q_b) - &(&v_b * &q_a));
        let rhs = &(&(&c_value * k_den) * &(&n_big * &n_big)) * &(&v_a * &v_b);
        lhs >= rhs
    };

    // Binary search the largest admissible claim in [1, post-swap reserve].
    if !passes(&BigInt::from(1)) {
        return None;
    }
    let mut lo = BigInt::from(1);
    let mut hi = after_op[out_idx].1.clone();
    while &hi - &lo > BigInt::from(1) {
        let mid = &(&lo + &hi) / &BigInt::from(2);
        if passes(&mid) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    // `hi` may itself pass when the loop never tightened (reserve-bounded).
    let claim = if passes(&hi) { hi } else { lo };

    let mut final_assets = after_op;
    final_assets[out_idx].1 = &final_assets[out_idx].1 - &claim;

    Some(ClaimPlan { dx: dx.clone(), dy, claim, final_assets })
}

/// Resolved trade shape for a claim intent against a specific pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimShape {
    pub in_idx: usize,
    pub out_idx: usize,
    pub dx: crate::bigint::BigInt,
    /// The receive asset's floor from the execution's min_received.
    pub min_recv: crate::bigint::BigInt,
    /// How much of the receive asset the order already holds (counts toward
    /// the floor — min_received bounds the whole fulfillment output).
    pub already_held: crate::bigint::BigInt,
}

/// Resolve which trade a claim intent implies against `reserves`/`prices`.
///
/// Orders may hold several assets (a wallet's mixed holdings ride along
/// untouched into the fulfillment). The receive asset is the min_received
/// entry; the swap input is chosen as the order-held pool asset (≠ receive)
/// with the LARGEST positive deficit — the most rebalancing, and therefore
/// most claimable, direction. Explicit min_received pins on held assets cap
/// how much of them may be consumed.
pub fn resolve_claim_shape(
    order_value: &crate::cardano_types::Value,
    min_received: &[(AssetClass, BigInt)],
    reserves: &[(AssetClass, BigInt)],
    prices: &[BigInt],
) -> Option<ClaimShape> {
    use num_traits::Signed;

    let holding = |asset: &AssetClass| -> BigInt {
        order_value
            .0
            .get(&asset.policy)
            .and_then(|tokens| tokens.get(&asset.token))
            .cloned()
            .unwrap_or_else(|| BigInt::from(0))
    };
    let pin = |asset: &AssetClass| -> BigInt {
        min_received
            .iter()
            .find(|(a, _)| a == asset)
            .map(|(_, m)| m.clone())
            .unwrap_or_else(|| BigInt::from(0))
    };

    // Receive asset: the min_received entry naming a pool asset the order
    // isn't spending into the pool. Phase 2 shape: exactly one such entry.
    let mut receive: Option<(usize, BigInt)> = None;
    for (asset, amount) in min_received {
        let Some(idx) = reserves.iter().position(|(a, _)| a == asset) else {
            return None; // floor on a non-pool asset: can't be a claim target
        };
        // An entry can be a leftover pin (asset the order holds and might
        // spend) or the receive floor. Treat the entry with the largest
        // shortfall vs current holdings as the receive target.
        let short = amount - &holding(asset);
        if short.is_positive() {
            if receive.is_some() {
                return None; // multiple receive targets: not yet supported
            }
            receive = Some((idx, amount.clone()));
        }
    }
    let (out_idx, min_recv) = receive?;

    let n_big = BigInt::from(reserves.len() as u64);
    let v = compute_v(reserves, prices);

    // Swap input: order-held pool asset (≠ receive) with the largest
    // positive deficit.
    let mut best: Option<(usize, BigInt, BigInt)> = None; // (idx, dx, deficit)
    for (idx, (asset, reserve)) in reserves.iter().enumerate() {
        if idx == out_idx {
            continue;
        }
        let spendable = &holding(asset) - &pin(asset);
        if !spendable.is_positive() {
            continue;
        }
        let p = &prices[idx];
        let deficit = &(&v - &(&(&n_big * p) * reserve)) / &(&n_big * p);
        if !deficit.is_positive() {
            continue;
        }
        let dx = if spendable < deficit { spendable } else { deficit.clone() };
        let better = match &best {
            Some((_, _, bd)) => &deficit > bd,
            None => true,
        };
        if better {
            best = Some((idx, dx, deficit));
        }
    }
    let (in_idx, dx, _) = best?;
    let already_held = holding(&reserves[out_idx].0);

    Some(ClaimShape { in_idx, out_idx, dx, min_recv, already_held })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ac(tag: u8) -> AssetClass {
        AssetClass { policy: vec![tag; 28], token: vec![tag] }
    }

    fn pool(r: &[i64]) -> Vec<(AssetClass, BigInt)> {
        r.iter()
            .enumerate()
            .map(|(i, amt)| (ac(i as u8 + 1), BigInt::from(*amt)))
            .collect()
    }

    fn ones(n: usize) -> Vec<BigInt> {
        vec![BigInt::from(1); n]
    }

    /// Brute-force cap_b verifier — the exact contract inequality.
    fn contract_accepts(
        before: &[(AssetClass, BigInt)],
        prices: &[BigInt],
        k: (&BigInt, &BigInt),
        plan: &ClaimPlan,
        in_idx: usize,
        out_idx: usize,
    ) -> bool {
        use num_traits::Signed;
        // Reconstruct after_actual from the plan and re-check every clause
        // the validator checks in waived mode.
        let mut after_op = before.to_vec();
        after_op[in_idx].1 = &after_op[in_idx].1 + &plan.dx;
        after_op[out_idx].1 = &after_op[out_idx].1 - &plan.dy;
        // v_increase_op == 0 (value-neutral swap)
        let v_before = compute_v(before, prices);
        let v_op = compute_v(&after_op, prices);
        if v_op != v_before {
            return false;
        }
        let mut after_actual = after_op;
        after_actual[out_idx].1 = &after_actual[out_idx].1 - &plan.claim;
        if after_actual != plan.final_assets {
            return false;
        }
        let v_b = v_before;
        let q_b = compute_q(before, prices, &v_b);
        let v_a = compute_v(&after_actual, prices);
        if !v_a.is_positive() || !plan.claim.is_positive() {
            return false;
        }
        let q_a = compute_q(&after_actual, prices, &v_a);
        let n = BigInt::from(before.len() as u64);
        let c_value = &plan.claim * &prices[out_idx];
        let lhs = k.0 * &(&(&v_a * &q_b) - &(&v_b * &q_a));
        let rhs = &(&(&c_value * k.1) * &(&n * &n)) * &(&v_a * &v_b);
        lhs >= rhs
    }

    #[test]
    fn balanced_pool_pays_no_bounty() {
        // Perfectly balanced: Q_b = 0; a value-neutral swap + claim can only
        // increase imbalance, so cap_b can never admit a claim.
        let before = pool(&[1_000_000, 1_000_000, 1_000_000]);
        let prices = ones(3);
        let k = (BigInt::from(9), BigInt::from(4000));
        let plan = plan_waived_claim(&before, &prices, (&k.0, &k.1), 0, 1, &BigInt::from(10_000));
        assert!(plan.is_none());
    }

    #[test]
    fn imbalanced_pool_pays_bounty_and_contract_accepts() {
        // Asset 0 scarce, asset 1 surplus (like a pool after a big 1→0 swap).
        // Swapping 0-in/1-out rebalances; the claim rides on that.
        let before = pool(&[400_000_000, 800_000_000, 600_000_000]);
        let prices = ones(3);
        let k = (BigInt::from(9), BigInt::from(4000));
        let dx = BigInt::from(50_000_000);
        let plan = plan_waived_claim(&before, &prices, (&k.0, &k.1), 0, 1, &dx)
            .expect("imbalanced pool should admit a claim");
        assert!(plan.claim > BigInt::from(0));
        assert_eq!(plan.dy, dx); // 1:1 prices, fee waived
        assert!(contract_accepts(&before, &prices, (&k.0, &k.1), &plan, 0, 1));

        // Maximality: one more unit must fail the contract inequality.
        let mut greedy = plan.clone();
        greedy.claim = &plan.claim + &BigInt::from(1);
        greedy.final_assets[1].1 = &plan.final_assets[1].1 - &BigInt::from(1);
        assert!(!contract_accepts(&before, &prices, (&k.0, &k.1), &greedy, 0, 1));
    }

    #[test]
    fn wrong_direction_pays_nothing() {
        // Swapping the surplus asset IN makes imbalance worse; no claim.
        let before = pool(&[400_000_000, 800_000_000, 600_000_000]);
        let prices = ones(3);
        let k = (BigInt::from(9), BigInt::from(4000));
        let plan =
            plan_waived_claim(&before, &prices, (&k.0, &k.1), 1, 0, &BigInt::from(50_000_000));
        assert!(plan.is_none());
    }

    #[test]
    fn zero_k_disables_claims() {
        let before = pool(&[400_000_000, 800_000_000, 600_000_000]);
        let prices = ones(3);
        let k = (BigInt::from(0), BigInt::from(1));
        let plan =
            plan_waived_claim(&before, &prices, (&k.0, &k.1), 0, 1, &BigInt::from(50_000_000));
        assert!(plan.is_none());
    }
}
