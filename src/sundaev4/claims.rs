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

/// Result of searching for a claim that satisfies an intent's floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimSearch {
    pub plan: ClaimPlan,
    /// True when `plan.dy + plan.claim >= needed_out`; false means the plan
    /// is the best achievable total (for status reporting), not enough.
    pub meets_floor: bool,
}

fn gcd(mut a: BigInt, mut b: BigInt) -> BigInt {
    use num_traits::Zero;
    while !b.is_zero() {
        let r = &a % &b;
        a = b;
        b = r;
    }
    a
}

/// Find the smallest `dx ≤ spendable` whose value-neutral swap plus maximal
/// claim yields at least `needed_out` of the receive asset.
///
/// The swap portion is fee-waived and value-neutral (`dy·p_out = dx·p_in`),
/// so dx is NOT bounded by the pool's deficit of the input asset: the
/// contract admits any dx that leaves the pool no more imbalanced than it
/// started (cap_b with claim ≥ 1). The binding limits are the order's
/// spendable holdings, the pool's reserve of the receive asset, and cap_b
/// feasibility itself.
///
/// The total `dy + max_claim` grows with dx (dy 1:1 in value, the claim
/// shrinking only slowly), while the claim — the order's actual value
/// profit — shrinks. The smallest floor-meeting dx is therefore also the
/// most profitable one. When no dx meets the floor, returns the plan with
/// the highest total so callers can report how close the intent is.
pub fn plan_claim_meeting_floor(
    reserves: &[(AssetClass, BigInt)],
    prices: &[BigInt],
    bounty_k: (&BigInt, &BigInt),
    in_idx: usize,
    out_idx: usize,
    spendable: &BigInt,
    needed_out: &BigInt,
) -> Option<ClaimSearch> {
    use num_traits::Signed;

    if !spendable.is_positive() {
        return None;
    }
    let p_in = &prices[in_idx];
    let p_out = &prices[out_idx];

    // dy must divide exactly: dx must be a multiple of p_out/gcd(p_in,p_out).
    let step = p_out / &gcd(p_in.clone(), p_out.clone());
    let in_steps = |dx: &BigInt| -> BigInt { &(dx / &step) * &step };

    // Upper bound: spendable, and dy ≤ reserve_out.
    let dx_reserve_cap = &(&reserves[out_idx].1 * p_out) / p_in;
    let hi_raw = if spendable < &dx_reserve_cap { spendable.clone() } else { dx_reserve_cap };
    let mut hi = in_steps(&hi_raw);
    if !hi.is_positive() {
        return None;
    }

    let plan_at = |dx: &BigInt| -> Option<ClaimPlan> {
        plan_waived_claim(reserves, prices, bounty_k, in_idx, out_idx, dx)
    };
    let total = |p: &ClaimPlan| -> BigInt { &p.dy + &p.claim };

    // cap_b feasibility (claim ≥ 1) holds on an interval of dx: too small
    // and the rebalancing can't fund a 1-unit claim, too large (overshot
    // past mirror-imbalance) and the pool ends worse than it started. If
    // `hi` overshoots, walk the upper edge back by bisection using the
    // rebalancing point (the input-asset deficit) as a known-good anchor.
    if plan_at(&hi).is_none() {
        let v = compute_v(reserves, prices);
        let n_big = BigInt::from(reserves.len() as u64);
        let np = &n_big * p_in;
        let deficit = &(&v - &(&np * &reserves[in_idx].1)) / &np;
        let anchor = in_steps(&if deficit < hi { deficit } else { hi.clone() });
        if !anchor.is_positive() || plan_at(&anchor).is_none() {
            return None;
        }
        let mut lo = anchor;
        while &hi - &lo > step {
            let mid = in_steps(&(&(&lo + &hi) / &BigInt::from(2)));
            let mid = if mid <= lo { &lo + &step } else { mid };
            if plan_at(&mid).is_some() {
                lo = mid;
            } else {
                hi = &mid - &step;
            }
        }
        if plan_at(&hi).is_none() {
            hi = lo;
        }
    }
    let best = plan_at(&hi)?;
    if &total(&best) < needed_out {
        return Some(ClaimSearch { plan: best, meets_floor: false });
    }

    // Floor is reachable: bisect the smallest dx whose total meets it.
    // (Total is monotone in dx over the feasible range; infeasible small
    // dx counts as "too small".)
    let mut lo = BigInt::from(0);
    let mut hi_dx = hi;
    while &hi_dx - &lo > step {
        let mid = in_steps(&(&(&lo + &hi_dx) / &BigInt::from(2)));
        let mid = if mid <= lo { &lo + &step } else { mid };
        let meets = plan_at(&mid).map(|p| &total(&p) >= needed_out).unwrap_or(false);
        if meets {
            hi_dx = mid;
        } else {
            lo = mid;
        }
    }
    let plan = plan_at(&hi_dx)?;
    let meets_floor = &total(&plan) >= needed_out;
    Some(ClaimSearch { plan, meets_floor })
}

/// Resolved trade shape for a claim intent against a specific pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimShape {
    pub in_idx: usize,
    pub out_idx: usize,
    /// How much of the input asset the order may spend: holdings minus any
    /// explicit min_received pin. The dx actually used is chosen by
    /// `plan_claim_meeting_floor` — the deficit is only a direction signal,
    /// not a cap (overshooting it is legal while cap_b holds).
    pub spendable: crate::bigint::BigInt,
    /// The receive asset's floor from the execution's min_received.
    pub min_recv: crate::bigint::BigInt,
    /// How much of the receive asset the order already holds (counts toward
    /// the floor — min_received bounds the whole fulfillment output).
    pub already_held: crate::bigint::BigInt,
    /// Optional lovelace floor from an ADA min_received entry — the signer's
    /// cap on cumulative fee takes from a standing order. The fulfillment
    /// must retain at least this much ada.
    pub min_ada: Option<crate::bigint::BigInt>,
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
) -> Result<ClaimShape, &'static str> {
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

    // Partition min_received: pool assets act as receive floors / spend
    // pins; non-pool assets are "carry floors" — the fulfillment carries the
    // order's holdings through unchanged, so they're satisfiable iff already
    // held. An ADA entry is the signer's floor on retained lovelace (a cap
    // on cumulative fee takes), recorded for the caller to enforce.
    let mut receive: Option<(usize, BigInt)> = None;
    let mut min_ada: Option<BigInt> = None;
    for (asset, amount) in min_received {
        let Some(idx) = reserves.iter().position(|(a, _)| a == asset) else {
            if asset.policy.is_empty() && asset.token.is_empty() {
                min_ada = Some(amount.clone());
                continue;
            }
            if holding(asset) >= *amount {
                continue; // carried through untouched — floor already met
            }
            return Err(
                "min_received floors an asset this pool can't produce and the \
                 order doesn't hold enough of to carry through",
            );
        };
        // An entry can be a leftover pin (asset the order holds and might
        // spend) or the receive floor. Treat the entry with the largest
        // shortfall vs current holdings as the receive target.
        let short = amount - &holding(asset);
        if short.is_positive() {
            if receive.is_some() {
                return Err("multiple receive targets are not yet supported");
            }
            receive = Some((idx, amount.clone()));
        }
    }
    let Some((out_idx, min_recv)) = receive else {
        return Err(
            "no receive target: every min_received floor is already met by \
             the order's current holdings",
        );
    };

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
        let better = match &best {
            Some((_, _, bd)) => &deficit > bd,
            None => true,
        };
        if better {
            best = Some((idx, spendable, deficit));
        }
    }
    let Some((in_idx, spendable, _)) = best else {
        return Err(
            "no rebalancing input: the order holds no spendable pool asset \
             the pool is currently short of",
        );
    };
    let already_held = holding(&reserves[out_idx].0);

    Ok(ClaimShape { in_idx, out_idx, spendable, min_recv, already_held, min_ada })
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

    /// Regression: preview intent ce8b83b0… (2026-07-07). The pool was
    /// drained to a single unit of the input asset; the intent's floor
    /// needed ~5.3M more than the deficit-capped dx could deliver. The
    /// floor IS reachable — dx may overshoot the input-asset deficit
    /// because the waived swap is value-neutral; cap_b (not the deficit)
    /// is the real bound. The old `dx = min(spendable, deficit)` heuristic
    /// reported below-floor here.
    #[test]
    fn floor_meeting_dx_overshoots_the_deficit() {
        let reserves = pool(&[1, 1_252_230_053, 619_516_058]);
        let prices = ones(3);
        let k = (BigInt::from(9), BigInt::from(4000));
        let needed = BigInt::from(629_252_486u64);
        let spendable = BigInt::from(1_000_000_000_000u64);

        let search = plan_claim_meeting_floor(
            &reserves, &prices, (&k.0, &k.1), 0, 1, &spendable, &needed,
        )
        .expect("claim must be feasible");
        assert!(search.meets_floor, "floor is reachable by overshooting the deficit");
        let total = &search.plan.dy + &search.plan.claim;
        assert!(total >= needed);
        // Minimal dx: barely past the floor, not the full budget.
        assert!(search.plan.dx < BigInt::from(630_000_000u64), "dx = {}", search.plan.dx);
        assert!(
            search.plan.dx > BigInt::from(623_915_369u64),
            "dx must exceed the input-asset deficit (old cap): {}",
            search.plan.dx,
        );
        // The exact contract inequality accepts the plan.
        assert!(contract_accepts(&reserves, &prices, (&k.0, &k.1), &search.plan, 0, 1));

        // And when the floor is genuinely out of reach, the best plan is
        // reported without meets_floor.
        let too_much = BigInt::from(3_000_000_000u64);
        let search = plan_claim_meeting_floor(
            &reserves, &prices, (&k.0, &k.1), 0, 1, &spendable, &too_much,
        )
        .expect("still feasible");
        assert!(!search.meets_floor);
        assert!(contract_accepts(&reserves, &prices, (&k.0, &k.1), &search.plan, 0, 1));
    }
}
