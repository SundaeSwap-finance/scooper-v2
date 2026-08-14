//! Constant-sum rebalance bounty ("claim") math.
//!
//! Port of `lib/modules/cs_check.ak`'s tag_claim (5) validation, for any
//! `balance_fee` (SUN-310) — both the full waiver (`balance_fee = 0`) and the
//! fee-paying mode (`0 < balance_fee ≤ fee`):
//!
//! - The entry's swap portion ("op portion", claim restored) is a plain CS
//!   swap at the pool's `balance_fee` rate: `v_increase_op =
//!   floor(input_value_op · bf_num / bf_den)` and `dy · p_out = input_value_op
//!   − v_increase_op`. `balance_fee = 0` makes it value-neutral (`v_increase =
//!   0`, `fee_budget = 0`, `before_lp == after_lp`); a positive rate leaves
//!   `v_increase_op` of value in the pool and grows LP by `fee_budget =
//!   floor(before_lp · v_increase_op / V_b)`.
//! - The claim extracts `c` units of the claim asset on top, bounded by
//!   cap_b: `k_num · (V_a·Q_b − V_b·Q_a) ≥ c·p_claim · k_den · N² · V_a·V_b`
//!   where `V = Σ p_i·r_i` (pool value) and `Q = Σ (N·p_i·r_i − V)²`
//!   (squared imbalance), with the after-state measured on the *actual*
//!   post-claim reserves. cap_b is identical in both modes.
//! - The op portion must still be a real swap (`has_inc && has_dec`), so a
//!   claim always rides on a nonzero `dx`.
//! - No-overshoot guard (`no_flip`): no traded asset may cross its pre-step
//!   balance point, measured against the frozen pre-step value `V_b`. For
//!   every traded asset, `(N·p_i·r_b − V_b)·(N·p_i·r_a − V_b) ≥ 0`.
//!
//! Mirrors the reference implementation in
//! `sundae-v4/test/devnet/src/actions/order.ts` (`claimBounty`).

use crate::bigint::BigInt;
use crate::cardano_types::AssetClass;

/// The no-overshoot guard from `cs_check.ak::compute_q_pair`: every asset that
/// changed between `before` and `after` must stay on its side of the pre-step
/// balance point `N·p_i·r == V_b` (landing exactly on it is allowed).
/// Untouched assets are skipped (their check is a tautology against `V_b`).
fn no_flip(
    before: &[(AssetClass, BigInt)],
    after: &[(AssetClass, BigInt)],
    prices: &[BigInt],
    v_b: &BigInt,
) -> bool {
    use num_traits::Signed;
    let n = BigInt::from(before.len() as u64);
    before.iter().zip(after).zip(prices).all(|(((_, rb), (_, ra)), p)| {
        if rb == ra {
            return true;
        }
        let d_b = &(&(&n * p) * rb) - v_b;
        let d_a = &(&(&n * p) * ra) - v_b;
        !(&d_b * &d_a).is_negative()
    })
}

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

/// Plan a claim entry against a CS pool at the pool's `balance_fee` rate.
///
/// `reserves`/`prices` are the pool's current state in pool asset order;
/// `in_idx`/`out_idx` pick the swap direction (input = what the order
/// offers, output = what it receives and claims); `dx` is the swap input.
/// Returns the largest claim cap_b admits (possibly finding that even the
/// swap alone is infeasible → `None`).
///
/// Requirements enforced here (mirroring `cs_check.ak::check_swap_with_claim`):
/// - `bounty_k.num > 0` (claims disabled otherwise)
/// - op-portion `dy` from the `balance_fee`-rate CS swap divides exactly and
///   `dy ≤ reserve_out` (`balance_fee = 0` ⇒ the value-neutral waiver)
/// - claim ≤ post-swap reserve of the claim asset, and `V_a > 0`
/// - cap_b on the actual after-state, and the `no_flip` no-overshoot guard
pub fn plan_claim(
    reserves: &[(AssetClass, BigInt)],
    prices: &[BigInt],
    bounty_k: (&BigInt, &BigInt),
    balance_fee: (&BigInt, &BigInt),
    in_idx: usize,
    out_idx: usize,
    dx: &BigInt,
) -> Option<ClaimPlan> {
    use num_traits::Signed;

    let (k_num, k_den) = bounty_k;
    let (bf_num, bf_den) = balance_fee;
    if !k_num.is_positive() || in_idx == out_idx || !dx.is_positive() {
        return None;
    }
    let n = reserves.len();
    if prices.len() != n || in_idx >= n || out_idx >= n {
        return None;
    }
    let p_out = &prices[out_idx];

    // Op portion: a plain CS swap at balance_fee. `cs_swap_result` returns 0
    // when the resulting dy isn't integer (swap impossible for this dx). At
    // balance_fee = 0 this is the value-neutral waiver (dy·p_out = dx·p_in).
    let dy = super::swap_math::cs_swap_result(dx, prices, in_idx, out_idx, bf_num, bf_den);
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

    // cap_b + no_flip check for a candidate claim `c` on the actual after-state.
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
        if !no_flip(reserves, &after_actual, prices, &v_b) {
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
    balance_fee: (&BigInt, &BigInt),
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
        plan_claim(reserves, prices, bounty_k, balance_fee, in_idx, out_idx, dx)
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

/// A planned single-op rebalance claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalancePlan {
    /// Per pool asset: the delta applied to the POOL's reserve
    /// (positive = the order pays in, negative = the pool pays out).
    /// Net of the claim.
    pub deltas: Vec<BigInt>,
    /// Which pool asset carries the bounty claim, and how much.
    pub claim_idx: usize,
    pub claim: BigInt,
    /// Pool reserves after the op, in pool asset order.
    pub final_assets: Vec<(AssetClass, BigInt)>,
}

/// Plan a single-op multi-leg rebalance claim (CLI parity: sundae-v4 commit
/// 8ba6491). The order contributes ALL its pool-asset holdings and takes back
/// exactly `targets`; the pool moves to `before + held − target` per asset.
/// Any number of legs may move each way in the one op — several assets in,
/// several out, or both.
///
/// The declared bounty claim is `net_gain + the balance_fee the op portion
/// owes`, carried on one receive asset (its price must divide the claim
/// value and its reserve must cover it; deepest reserve preferred), where
/// `net_gain = Σ(target−held)·p` is what the order actually takes out of the
/// pool. On a waived pool (`balance_fee = 0`) that collapses to claim ==
/// net_gain, the value-neutral shape. On a fee-charging pool the order's
/// payout absorbs the fee — it takes back less than it puts in, so `net_gain`
/// may be zero or negative — exactly as `dy` absorbs it in the pair shape.
///
/// The op portion (claim restored) must move ≥1 reserve up and ≥1 down, and
/// cap_b must admit the declared claim on the aggregate imbalance
/// improvement. On a fee pool that makes rebalancing a net loss until the
/// imbalance is worth more than the fee — the neutral zone the fee creates.
pub fn plan_rebalance_claim(
    reserves: &[(AssetClass, BigInt)],
    prices: &[BigInt],
    bounty_k: (&BigInt, &BigInt),
    balance_fee: (&BigInt, &BigInt),
    held: &[BigInt],
    targets: &[BigInt],
) -> Result<RebalancePlan, &'static str> {
    use num_traits::Signed;

    let (k_num, k_den) = bounty_k;
    let (bf_num, bf_den) = balance_fee;
    if !k_num.is_positive() {
        return Err("claims are disabled on this pool (bounty_k ≤ 0)");
    }
    let n = reserves.len();
    if prices.len() != n || held.len() != n || targets.len() != n {
        return Err("rebalance shape arity mismatch");
    }

    let after: Vec<BigInt> = (0..n)
        .map(|i| &(&reserves[i].1 + &held[i]) - &targets[i])
        .collect();
    if after.iter().any(|a| a.is_negative()) {
        return Err("pool reserve would go negative (insufficient liquidity)");
    }

    // What the order takes out of the pool. Positive when it profits; zero or
    // negative when its payout is funding the pool's balance_fee.
    let net_gain: BigInt = (0..n)
        .map(|i| &(&targets[i] - &held[i]) * &prices[i])
        .fold(BigInt::from(0), |acc, d| acc + d);

    // Claim asset: a receive target, deepest reserve first. The claim VALUE is
    // the same whichever leg carries it; legs differ only in whether their
    // price divides it and their reserve covers it, so try them in turn.
    let mut candidates: Vec<usize> = (0..n).filter(|&i| targets[i] > held[i]).collect();
    candidates.sort_by(|&a, &b| reserves[b].1.cmp(&reserves[a].1));
    if candidates.is_empty() {
        return Err("rebalance has no receive leg to carry the bounty claim");
    }
    let mut solved: Option<(usize, BigInt, BigInt)> = None;
    let mut why = "no receive asset can carry the claim";
    for &idx in &candidates {
        match solve_claim_value(reserves, prices, balance_fee, &after, &net_gain, idx) {
            Ok((claim, claim_value)) => {
                solved = Some((idx, claim, claim_value));
                break;
            }
            Err(e) => why = e,
        }
    }
    let Some((claim_idx, claim, c_value)) = solved else {
        return Err(why);
    };

    // Op-portion shape: with the claim restored, ≥1 reserve up and ≥1 down.
    let has_inc = (0..n).any(|i| {
        let restored = if i == claim_idx { &after[i] + &claim } else { after[i].clone() };
        restored > reserves[i].1
    });
    let has_dec = (0..n).any(|i| {
        let restored = if i == claim_idx { &after[i] + &claim } else { after[i].clone() };
        restored < reserves[i].1
    });
    if !has_inc || !has_dec {
        return Err("rebalance op portion must move at least one reserve each way");
    }

    // cap_b on the aggregate imbalance improvement, against the DECLARED claim
    // (net gain plus the fee the op portion pays in) — that's the bounty the
    // pool is being asked to underwrite.
    let v_b = compute_v(reserves, prices);
    let q_b = compute_q(reserves, prices, &v_b);
    let after_assets: Vec<(AssetClass, BigInt)> = reserves
        .iter()
        .zip(after.iter())
        .map(|((a, _), amt)| (a.clone(), amt.clone()))
        .collect();
    let v_a = compute_v(&after_assets, prices);
    if !v_a.is_positive() {
        return Err("pool value after the rebalance would be non-positive");
    }
    let q_a = compute_q(&after_assets, prices, &v_a);
    let n_big = BigInt::from(n as u64);
    let lhs = k_num * &(&(&v_a * &q_b) - &(&v_b * &q_a));
    let rhs = &(&(&c_value * k_den) * &(&n_big * &n_big)) * &(&v_a * &v_b);
    if lhs < rhs {
        return Err(
            "the pool isn't imbalanced enough to fund the requested claim \
             (cap_b rejects the claim)",
        );
    }

    // No-overshoot guard on the actual after-state, per cs_check.
    if !no_flip(reserves, &after_assets, prices, &v_b) {
        return Err(
            "rebalance overshoots a traded asset past its pre-step balance \
             point (cs_check no-overshoot guard)",
        );
    }

    // Op-portion fee pin at balance_fee, re-checked on the assembled plan:
    // `v_increase_op == floor(input_value_op · bf)`. `solve_claim_value` sized
    // the claim to land exactly here (`v_increase_op = claim_value − net_gain`
    // by the held/target accounting identity), so this is verification rather
    // than a filter — but it is the clause the validator enforces, so no plan
    // leaves without passing it. At balance_fee = 0 it reduces to
    // v_increase_op == 0, which claim == net_gain satisfies by construction.
    let after_op: Vec<(AssetClass, BigInt)> = after_assets
        .iter()
        .enumerate()
        .map(|(i, (a, amt))| {
            (a.clone(), if i == claim_idx { amt + &claim } else { amt.clone() })
        })
        .collect();
    let input_value_op: BigInt = (0..n)
        .map(|i| &after_op[i].1 - &reserves[i].1)
        .zip(prices.iter())
        .filter(|(d, _)| d.is_positive())
        .map(|(d, p)| &d * p)
        .fold(BigInt::from(0), |acc, x| acc + x);
    let v_increase_op = &compute_v(&after_op, prices) - &v_b;
    if !(&(&v_increase_op * bf_den) <= &(&input_value_op * bf_num)) {
        return Err("rebalance op portion underpays the pool's balance_fee");
    }
    if !(&(&(&v_increase_op + &BigInt::from(1)) * bf_den) > &(&input_value_op * bf_num)) {
        return Err("rebalance op portion overpays the pool's balance_fee");
    }

    let deltas: Vec<BigInt> = (0..n).map(|i| &held[i] - &targets[i]).collect();
    Ok(RebalancePlan { deltas, claim_idx, claim, final_assets: after_assets })
}

/// Size the declared claim for one receive leg of a rebalance, returning
/// `(claim amount, claim value)`.
///
/// The validator pins the op portion's value increase to the pool's fee:
/// `v_increase_op == floor(input_value_op · balance_fee)`. The claim is what
/// the op portion hands back, so `v_increase_op = claim_value − net_gain`,
/// which fixes `claim_value = net_gain + fee`. The fee itself moves with the
/// claim once the claim asset's op-portion delta turns positive (a claim
/// larger than what the order receives of that asset), so iterate to the
/// least fixed point: `fee` is monotone in the claim and bounded by
/// `bf · input`, so it settles in a round or two. The caller re-checks the
/// pin on the assembled plan, which catches anything that didn't converge.
fn solve_claim_value(
    reserves: &[(AssetClass, BigInt)],
    prices: &[BigInt],
    balance_fee: (&BigInt, &BigInt),
    after: &[BigInt],
    net_gain: &BigInt,
    claim_idx: usize,
) -> Result<(BigInt, BigInt), &'static str> {
    use num_traits::{Signed, Zero};

    let price = &prices[claim_idx];
    let mut claim_value = net_gain.clone();
    for _ in 0..8 {
        let claim = if claim_value.is_positive() {
            &claim_value / price
        } else {
            BigInt::from(0)
        };
        let next = net_gain
            + &op_portion_fee(reserves, prices, after, claim_idx, &claim, balance_fee);
        if next == claim_value {
            break;
        }
        claim_value = next;
    }

    if !claim_value.is_positive() {
        return Err(
            "rebalance leaves no bounty to claim: the order's payout is short of \
             what balance_fee costs (raise its min_received targets)",
        );
    }
    if !(&claim_value % price).is_zero() {
        return Err(
            "no receive asset can carry the claim (its price must divide the \
             claim value and its reserve must cover it)",
        );
    }
    let claim = &claim_value / price;
    if reserves[claim_idx].1 < claim {
        return Err(
            "no receive asset can carry the claim (its price must divide the \
             claim value and its reserve must cover it)",
        );
    }
    Ok((claim, claim_value))
}

/// `floor(input_value_op · balance_fee)` — the value the op portion must leave
/// in the pool, measured on the op-portion state (claim restored).
fn op_portion_fee(
    reserves: &[(AssetClass, BigInt)],
    prices: &[BigInt],
    after: &[BigInt],
    claim_idx: usize,
    claim: &BigInt,
    balance_fee: (&BigInt, &BigInt),
) -> BigInt {
    use num_traits::Signed;

    let (bf_num, bf_den) = balance_fee;
    let input_value_op: BigInt = (0..reserves.len())
        .map(|i| {
            let after_op = if i == claim_idx { &after[i] + claim } else { after[i].clone() };
            &after_op - &reserves[i].1
        })
        .zip(prices.iter())
        .filter(|(d, _)| d.is_positive())
        .map(|(d, p)| &d * p)
        .fold(BigInt::from(0), |acc, x| acc + x);
    &(&input_value_op * bf_num) / bf_den
}

/// What kind of claim an intent's min_received implies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedShape {
    /// One receive target: a pair-wise claim (dx of one asset in, dy + bounty
    /// of another out), dx chosen by [`plan_claim_meeting_floor`].
    Pair(ClaimShape),
    /// Several receive targets, or several assets to offer: a single-op
    /// rebalance. min_received is the order's exact desired final holdings
    /// per pool asset (0 when unpinned — those holdings are forfeit to the
    /// pool), and its net value gain plus the pool's balance_fee is declared
    /// as one bounty claim. See [`plan_rebalance_claim`].
    Rebalance(RebalanceShape),
}

/// Inputs for a single-op multi-receive rebalance claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceShape {
    /// Order holdings per pool asset, in pool asset order.
    pub held: Vec<crate::bigint::BigInt>,
    /// Desired final holdings per pool asset (the min_received pin, 0 when
    /// absent), in pool asset order.
    pub targets: Vec<crate::bigint::BigInt>,
    /// Optional lovelace floor from an ADA min_received entry.
    pub min_ada: Option<crate::bigint::BigInt>,
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
/// The signed entries are per-asset DELTAS (destination output − order
/// input; SUN-310's `min_deltas`, bounded on-chain since SUN-109). This
/// resolver's math lives in absolute-holdings space, so the deltas are
/// re-based ONCE here — `floor = holding + delta` — and everything
/// downstream speaks absolute floors: a delta > 0 is a receive target
/// (floor above holdings), a delta ≤ 0 caps that asset's outflow (minimum
/// final holding), an absent asset is unconstrained.
///
/// Orders may hold several assets (a wallet's mixed holdings ride along
/// untouched into the fulfillment). The swap input is chosen as the
/// order-held pool asset (≠ receive) with the LARGEST positive deficit —
/// the most rebalancing, and therefore most claimable, direction.
pub fn resolve_claim_shape(
    order_value: &crate::cardano_types::Value,
    min_deltas: &[(AssetClass, BigInt)],
    reserves: &[(AssetClass, BigInt)],
    prices: &[BigInt],
) -> Result<ResolvedShape, &'static str> {
    use num_traits::Signed;

    let holding = |asset: &AssetClass| -> BigInt {
        order_value
            .0
            .get(&asset.policy)
            .and_then(|tokens| tokens.get(&asset.token))
            .cloned()
            .unwrap_or_else(|| BigInt::from(0))
    };
    // Re-base signed deltas to absolute final-holding floors, clamped at 0
    // (an outflow bound below the physical minimum is just "unconstrained").
    let floors: Vec<(AssetClass, BigInt)> = min_deltas
        .iter()
        .map(|(a, d)| {
            let f = &holding(a) + d;
            (a.clone(), if f.is_negative() { BigInt::from(0) } else { f })
        })
        .collect();
    // An asset with no signed entry is FROZEN on-chain (check_consumption:
    // assets present in the order input and not named in min_deltas must not
    // decrease), so its pin is its full holding — never 0/forfeit. Treating
    // missing as forfeit built plans the validator rejects and forced signers
    // to write explicit 0-deltas for every untouched asset.
    let pin = |asset: &AssetClass| -> BigInt {
        floors
            .iter()
            .find(|(a, _)| a == asset)
            .map(|(_, m)| m.clone())
            .unwrap_or_else(|| holding(asset))
    };

    // Partition the floors: pool assets act as receive floors / spend pins;
    // non-pool assets are "carry floors" — the fulfillment carries the
    // order's holdings through unchanged, so they're satisfiable iff the
    // delta was ≤ 0. The on-chain ADA check is GROSS (fee_deducted is added
    // back before the comparison), so an ADA delta ≤ 0 is vacuous when the
    // only ADA outflow is the fee — record no floor for it, or the caller's
    // fee cushion would decline every fill the chain accepts. A positive ADA
    // delta demands net inflow and keeps its re-based floor.
    let mut receives: Vec<(usize, BigInt)> = Vec::new();
    let mut min_ada: Option<BigInt> = None;
    for (asset, floor) in &floors {
        let Some(idx) = reserves.iter().position(|(a, _)| a == asset) else {
            if asset.policy.is_empty() && asset.token.is_empty() {
                if *floor > holding(asset) {
                    min_ada = Some(floor.clone());
                }
                continue;
            }
            if holding(asset) >= *floor {
                continue; // carried through untouched — floor already met
            }
            return Err(
                "min_deltas demands inflow of an asset this pool can't \
                 produce",
            );
        };
        // A floor above current holdings is a receive target; at-or-below
        // is an outflow cap handled via pin().
        let short = floor - &holding(asset);
        if short.is_positive() {
            receives.push((idx, floor.clone()));
        }
    }
    if receives.is_empty() {
        return Err(
            "no receive target: no min_deltas entry demands a positive delta",
        );
    }

    // Single-op rebalance when the trade can't be expressed as one pair:
    // several receive targets, or several offer-capable assets (pool assets
    // with spendable holdings beyond their pin). The re-based floors then act
    // as the exact desired final holdings vector — unmentioned pool assets
    // stay at their current holding (frozen, matching check_consumption).
    let receive_idxs: Vec<usize> = receives.iter().map(|(i, _)| *i).collect();
    let offer_capable = reserves
        .iter()
        .enumerate()
        .filter(|(i, (a, _))| {
            !receive_idxs.contains(i) && (&holding(a) - &pin(a)).is_positive()
        })
        .count();
    if receives.len() > 1 || offer_capable > 1 {
        let held: Vec<BigInt> = reserves.iter().map(|(a, _)| holding(a)).collect();
        let targets: Vec<BigInt> = reserves.iter().map(|(a, _)| pin(a)).collect();
        return Ok(ResolvedShape::Rebalance(RebalanceShape { held, targets, min_ada }));
    }
    let (out_idx, min_recv) = receives.into_iter().next().expect("len == 1");

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

    Ok(ResolvedShape::Pair(ClaimShape {
        in_idx,
        out_idx,
        spendable,
        min_recv,
        already_held,
        min_ada,
    }))
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

    /// Brute-force verifier — the exact `cs_check.ak::check_swap_with_claim`
    /// clauses (op-portion fee pin at balance_fee, shape, no_flip, cap_b).
    fn contract_accepts(
        before: &[(AssetClass, BigInt)],
        prices: &[BigInt],
        k: (&BigInt, &BigInt),
        bf: (&BigInt, &BigInt),
        plan: &ClaimPlan,
        in_idx: usize,
        out_idx: usize,
    ) -> bool {
        use num_traits::Signed;
        // Reconstruct the op portion (claim restored) from the plan.
        let mut after_op = before.to_vec();
        after_op[in_idx].1 = &after_op[in_idx].1 + &plan.dx;
        after_op[out_idx].1 = &after_op[out_idx].1 - &plan.dy;
        let v_before = compute_v(before, prices);
        let v_op = compute_v(&after_op, prices);
        // Op-portion shape: ≥1 up, ≥1 down.
        let has_inc = (0..before.len()).any(|i| after_op[i].1 > before[i].1);
        let has_dec = (0..before.len()).any(|i| after_op[i].1 < before[i].1);
        if !has_inc || !has_dec {
            return false;
        }
        // Fee-exact pin: v_increase_op == floor(input_value_op · bf).
        let mut input_value_op = BigInt::from(0);
        for i in 0..before.len() {
            let d = &after_op[i].1 - &before[i].1;
            if d.is_positive() {
                input_value_op = &input_value_op + &(&d * &prices[i]);
            }
        }
        let v_increase_op = &v_op - &v_before;
        if !(&(&v_increase_op * bf.1) <= &(&input_value_op * bf.0)) {
            return false;
        }
        if !(&(&(&v_increase_op + &BigInt::from(1)) * bf.1) > &(&input_value_op * bf.0)) {
            return false;
        }
        // Actual after-state (claim extracted) must match the plan.
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
        if !no_flip(before, &after_actual, prices, &v_b) {
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
        let plan = plan_claim(&before, &prices, (&k.0, &k.1), (&BigInt::from(0), &BigInt::from(1)), 0, 1, &BigInt::from(10_000));
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
        let plan = plan_claim(&before, &prices, (&k.0, &k.1), (&BigInt::from(0), &BigInt::from(1)), 0, 1, &dx)
            .expect("imbalanced pool should admit a claim");
        assert!(plan.claim > BigInt::from(0));
        assert_eq!(plan.dy, dx); // 1:1 prices, fee waived
        assert!(contract_accepts(&before, &prices, (&k.0, &k.1), (&BigInt::from(0), &BigInt::from(1)), &plan, 0, 1));

        // Maximality: one more unit must fail the contract inequality.
        let mut greedy = plan.clone();
        greedy.claim = &plan.claim + &BigInt::from(1);
        greedy.final_assets[1].1 = &plan.final_assets[1].1 - &BigInt::from(1);
        assert!(!contract_accepts(&before, &prices, (&k.0, &k.1), (&BigInt::from(0), &BigInt::from(1)), &greedy, 0, 1));
    }

    #[test]
    fn wrong_direction_pays_nothing() {
        // Swapping the surplus asset IN makes imbalance worse; no claim.
        let before = pool(&[400_000_000, 800_000_000, 600_000_000]);
        let prices = ones(3);
        let k = (BigInt::from(9), BigInt::from(4000));
        let plan =
            plan_claim(&before, &prices, (&k.0, &k.1), (&BigInt::from(0), &BigInt::from(1)), 1, 0, &BigInt::from(50_000_000));
        assert!(plan.is_none());
    }

    #[test]
    fn zero_k_disables_claims() {
        let before = pool(&[400_000_000, 800_000_000, 600_000_000]);
        let prices = ones(3);
        let k = (BigInt::from(0), BigInt::from(1));
        let plan =
            plan_claim(&before, &prices, (&k.0, &k.1), (&BigInt::from(0), &BigInt::from(1)), 0, 1, &BigInt::from(50_000_000));
        assert!(plan.is_none());
    }

    /// Brute-force verifier for a multi-leg plan — the same
    /// `cs_check.ak::check_swap_with_claim` clauses as [`contract_accepts`],
    /// reconstructed from an arbitrary reserve-delta vector instead of a
    /// single dx/dy pair.
    fn contract_accepts_rebalance(
        before: &[(AssetClass, BigInt)],
        prices: &[BigInt],
        k: (&BigInt, &BigInt),
        bf: (&BigInt, &BigInt),
        plan: &RebalancePlan,
    ) -> bool {
        use num_traits::Signed;
        let n = before.len();
        let after: Vec<(AssetClass, BigInt)> = (0..n)
            .map(|i| (before[i].0.clone(), &before[i].1 + &plan.deltas[i]))
            .collect();
        if after != plan.final_assets || after.iter().any(|(_, a)| a.is_negative()) {
            return false;
        }
        // Op portion: the claim restored to its asset.
        let after_op: Vec<(AssetClass, BigInt)> = after
            .iter()
            .enumerate()
            .map(|(i, (a, amt))| {
                (a.clone(), if i == plan.claim_idx { amt + &plan.claim } else { amt.clone() })
            })
            .collect();
        let has_inc = (0..n).any(|i| after_op[i].1 > before[i].1);
        let has_dec = (0..n).any(|i| after_op[i].1 < before[i].1);
        if !has_inc || !has_dec {
            return false;
        }
        // Fee-exact pin: v_increase_op == floor(input_value_op · bf).
        let mut input_value_op = BigInt::from(0);
        for i in 0..n {
            let d = &after_op[i].1 - &before[i].1;
            if d.is_positive() {
                input_value_op = &input_value_op + &(&d * &prices[i]);
            }
        }
        let v_b = compute_v(before, prices);
        let v_increase_op = &compute_v(&after_op, prices) - &v_b;
        if !(&(&v_increase_op * bf.1) <= &(&input_value_op * bf.0)) {
            return false;
        }
        if !(&(&(&v_increase_op + &BigInt::from(1)) * bf.1) > &(&input_value_op * bf.0)) {
            return false;
        }
        // Claim well-formedness, no-overshoot guard, cap_b.
        if !plan.claim.is_positive() || before[plan.claim_idx].1 < plan.claim {
            return false;
        }
        let v_a = compute_v(&after, prices);
        if !v_a.is_positive() || !no_flip(before, &after, prices, &v_b) {
            return false;
        }
        let q_b = compute_q(before, prices, &v_b);
        let q_a = compute_q(&after, prices, &v_a);
        let n_big = BigInt::from(n as u64);
        let c_value = &plan.claim * &prices[plan.claim_idx];
        let lhs = k.0 * &(&(&v_a * &q_b) - &(&v_b * &q_a));
        let rhs = &(&(&c_value * k.1) * &(&n_big * &n_big)) * &(&v_a * &v_b);
        lhs >= rhs
    }

    fn big(x: u64) -> BigInt {
        BigInt::from(x)
    }

    /// Multi-leg rebalances on a fee-charging pool: several legs in and/or
    /// several out in one op, with the order's payout absorbing balance_fee
    /// (exactly how `dy` absorbs it in the pair shape). The claim declared is
    /// `net_gain + fee`, which puts `v_increase_op` on the validator's pin —
    /// the shape itself is not what a fee pool rules out.
    #[test]
    fn fee_paying_multi_leg_rebalance_contract_accepts() {
        let prices = ones(3);
        let k = (big(75), big(100_000));
        let bf = (big(10), big(10_000)); // 0.1%
        // 10,000 in (6-decimal stables) ⇒ fee = 10.00.
        let fee = big(10_000_000);

        // One scarce asset in, a mix of the two abundant ones out.
        let before = pool(&[800_000_000_000, 1_200_000_000_000, 1_100_000_000_000]);
        let held = vec![big(10_000_000_000), big(0), big(0)];
        let targets = vec![big(0), big(4_995_500_000), big(4_995_500_000)];
        let plan = plan_rebalance_claim(
            &before, &prices, (&k.0, &k.1), (&bf.0, &bf.1), &held, &targets,
        )
        .expect("1-in/2-out rebalance is valid on a fee pool when the payout absorbs the fee");
        // The order nets −9.00: it pays 10.00 of fee and earns a 1.00 bounty.
        assert_eq!(plan.claim, big(1_000_000));
        assert_eq!(plan.claim_idx, 1, "deepest receive leg carries the claim");
        let net_gain: BigInt = (0..3)
            .map(|i| &(&targets[i] - &held[i]) * &prices[i])
            .fold(big(0), |acc, d| acc + d);
        assert_eq!(&net_gain + &fee, &plan.claim * &prices[plan.claim_idx]);
        assert!(contract_accepts_rebalance(&before, &prices, (&k.0, &k.1), (&bf.0, &bf.1), &plan));

        // Two scarce assets in, the abundant one out.
        let before = pool(&[800_000_000_000, 900_000_000_000, 1_400_000_000_000]);
        let held = vec![big(6_000_000_000), big(4_000_000_000), big(0)];
        let targets = vec![big(0), big(0), big(9_992_000_000)];
        let plan = plan_rebalance_claim(
            &before, &prices, (&k.0, &k.1), (&bf.0, &bf.1), &held, &targets,
        )
        .expect("2-in/1-out rebalance is valid on a fee pool too");
        assert_eq!(plan.claim, big(2_000_000));
        assert_eq!(plan.claim_idx, 2);
        assert!(contract_accepts_rebalance(&before, &prices, (&k.0, &k.1), (&bf.0, &bf.1), &plan));
    }

    /// The fee pool's neutral zone: near equilibrium cap_b admits less bounty
    /// than balance_fee costs, so a rebalance that asks to come out ahead is
    /// refused — by cap_b, on the declared claim, not by the shape.
    #[test]
    fn fee_pool_neutral_zone_is_enforced_by_cap_b() {
        let before = pool(&[800_000_000_000, 1_200_000_000_000, 1_100_000_000_000]);
        let prices = ones(3);
        let k = (big(75), big(100_000));
        let bf = (big(10), big(10_000));
        let held = vec![big(10_000_000_000), big(0), big(0)];

        // Asking for 1.00 of profit on top of the 10.00 fee needs an 11.00
        // claim — far past what this pool's imbalance underwrites.
        let greedy = vec![big(0), big(5_000_500_000), big(5_000_500_000)];
        let err = plan_rebalance_claim(
            &before, &prices, (&k.0, &k.1), (&bf.0, &bf.1), &held, &greedy,
        )
        .expect_err("profit on a fee pool must be refused near equilibrium");
        assert!(err.contains("cap_b"), "got: {err}");

        // Paying in more than the fee leaves nothing to declare as a bounty,
        // which the validator's `amount > 0` clause forbids.
        let underpaid = vec![big(0), big(4_990_000_000), big(4_990_000_000)];
        let err = plan_rebalance_claim(
            &before, &prices, (&k.0, &k.1), (&bf.0, &bf.1), &held, &underpaid,
        )
        .expect_err("a payout below fee-neutral has no claim to declare");
        assert!(err.contains("no bounty to claim"), "got: {err}");
    }

    /// A waived pool is unchanged: claim == the order's net value gain and the
    /// op portion stays value-neutral.
    #[test]
    fn waived_pool_rebalance_claims_exactly_the_net_gain() {
        let before = pool(&[800_000_000_000, 1_200_000_000_000, 1_100_000_000_000]);
        let prices = ones(3);
        let k = (big(75), big(100_000));
        let waived = (big(0), big(1));
        let held = vec![big(10_000_000_000), big(0), big(0)];
        let targets = vec![big(0), big(5_000_500_000), big(5_000_500_000)];
        let plan = plan_rebalance_claim(
            &before, &prices, (&k.0, &k.1), (&waived.0, &waived.1), &held, &targets,
        )
        .expect("waived pools admit the value-neutral rebalance as before");
        assert_eq!(plan.claim, big(1_000_000), "claim == net gain when the fee is waived");
        assert!(contract_accepts_rebalance(
            &before, &prices, (&k.0, &k.1), (&waived.0, &waived.1), &plan
        ));
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(3000))]

        /// INVARIANT: every plan we emit must satisfy the deployed validator.
        /// The fee-paying path sizes the claim by a fixed-point solve, so this
        /// is the guard that a solve which didn't converge — or a leg whose
        /// op-portion delta flipped sign under its own claim — can never leave
        /// as a plan the node would reject.
        ///
        /// Targets are drawn around the order's own holdings so the generator
        /// lands on fillable shapes (a uniform draw is almost always rejected
        /// before the claim math runs).
        #[test]
        fn emitted_rebalance_plans_always_satisfy_the_contract(
            r0 in 1_000_000i64..2_000_000_000i64,
            r1 in 1_000_000i64..2_000_000_000i64,
            r2 in 1_000_000i64..2_000_000_000i64,
            p0 in 1i64..4i64,
            p1 in 1i64..4i64,
            p2 in 1i64..4i64,
            h0 in 0i64..20_000_000i64,
            h1 in 0i64..20_000_000i64,
            h2 in 0i64..20_000_000i64,
            share0 in 0i64..100i64,
            share1 in 0i64..100i64,
            // How much of the pool's fee the payout gives up, plus a hair of
            // profit/loss on top — the band where a rebalance is fillable.
            fee_frac in 0i64..120i64,
            extra_bp in -5i64..6i64,
            k_num in 1i64..500i64,
            bf_num in 0i64..100i64,
        ) {
            let before = pool(&[r0, r1, r2]);
            let prices = vec![big(p0 as u64), big(p1 as u64), big(p2 as u64)];
            let k = (big(k_num as u64), big(100_000));
            let bf = (big(bf_num as u64), big(10_000));
            let held = vec![big(h0 as u64), big(h1 as u64), big(h2 as u64)];

            // Take back roughly what was paid in (plus `delta`), split across
            // the first two legs — the shape a rebalancing order actually has.
            let in_value = h0 * p0 + h1 * p1 + h2 * p2;
            let fee = in_value * bf_num / 10_000;
            let out_value =
                (in_value - fee * fee_frac / 100 + in_value * extra_bp / 100_000).max(0);
            let t0 = out_value * share0 / 100 / p0;
            let t1 = (out_value - t0 * p0).max(0) * share1 / 100 / p1;
            let t2 = (out_value - t0 * p0 - t1 * p1).max(0) / p2;
            let targets = vec![big(t0 as u64), big(t1 as u64), big(t2 as u64)];

            if let Ok(plan) = plan_rebalance_claim(
                &before, &prices, (&k.0, &k.1), (&bf.0, &bf.1), &held, &targets,
            ) {
                proptest::prop_assert!(
                    contract_accepts_rebalance(&before, &prices, (&k.0, &k.1), (&bf.0, &bf.1), &plan),
                    "planner emitted a plan the validator rejects: reserves={:?} \
                     prices={:?} held={:?} targets={:?} bf={}/10000 k={}/100000 \
                     claim={} idx={}",
                    [r0, r1, r2], [p0, p1, p2], [h0, h1, h2], [t0, t1, t2],
                    bf_num, k_num, plan.claim, plan.claim_idx,
                );
            }
        }
    }

    /// SUN-310 behaviour change: the manual multi-offer rebalance from preview
    /// scoop 7a66bd76… (2026-07-07, pre-SUN-310 contract) pushed USDr from a
    /// surplus (n·p·r > V_b) to a deficit — i.e. it *overshot* USDr's balance
    /// point. The pre-SUN-310 contract accepted that; the current contract's
    /// no-overshoot guard (commit dbf8997) rejects it. We assert the guard
    /// fires, so a scooper won't build a tx the deployed contract would fail.
    #[test]
    fn rebalance_overshoot_rejected_by_no_flip_sun310() {
        let reserves = pool(&[1, 1_252_230_053, 619_516_058]); // USDCx, USDr, USDM
        let prices = ones(3);
        let k = (BigInt::from(9), BigInt::from(4000));
        let held = vec![
            BigInt::from(1_000_002_662_691u64), // USDCx
            BigInt::from(1_000_003_130_080u64), // USDr
            BigInt::from(999_996_073_613u64),   // USDM
        ];
        let targets = vec![
            BigInt::from(999_378_747_322u64),   // USDCx pin
            BigInt::from(1_000_632_382_566u64), // USDr floor (receive)
            BigInt::from(999_991_674_301u64),   // USDM pin
        ];

        // Under the current (SUN-310) contract this shape overshoots USDr's
        // balance point (surplus → deficit) and must be rejected.
        let err = plan_rebalance_claim(
            &reserves, &prices, (&k.0, &k.1), (&BigInt::from(0), &BigInt::from(1)), &held, &targets,
        )
        .expect_err("no-overshoot guard must reject the pre-SUN-310 shape");
        assert!(err.contains("overshoot"), "got: {err}");

        // The shape resolver still routes this order to the rebalance path
        // (the guard is a planning-feasibility check, not a shape decision).
        let mut value = crate::cardano_types::Value::default();
        for (i, (asset, _)) in reserves.iter().enumerate() {
            value.insert(asset, held[i].clone());
        }
        let min_deltas: Vec<(AssetClass, BigInt)> = reserves
            .iter()
            .enumerate()
            .map(|(i, (a, _))| (a.clone(), &targets[i] - &held[i]))
            .collect();
        let shape = resolve_claim_shape(&value, &min_deltas, &reserves, &prices)
            .expect("shape must resolve");
        match shape {
            ResolvedShape::Rebalance(r) => {
                assert_eq!(r.held, held);
                assert_eq!(r.targets, targets);
            }
            ResolvedShape::Pair(_) => panic!("expected rebalance shape"),
        }
    }

    /// SUN-310: the no-overshoot guard caps a claim at the pre-step balance
    /// points. For the ce8b83b0… pool state (drained to 1 unit of the input
    /// asset), the receive asset (USDr) sits at reserve 1,252,230,053 with a
    /// balance point of 623,915,370, so a claim can extract at most
    /// 1,252,230,053 − 623,915,370 = 628,314,683 without pushing USDr past
    /// balance. A floor above that (629,252,486) is now genuinely unreachable
    /// — pre-SUN-310 it was reachable by overshooting, which the current
    /// contract forbids.
    #[test]
    fn no_flip_caps_claim_below_an_overshoot_floor() {
        let reserves = pool(&[1, 1_252_230_053, 619_516_058]);
        let prices = ones(3);
        let k = (BigInt::from(9), BigInt::from(4000));
        let spendable = BigInt::from(1_000_000_000_000u64);

        // A floor that only an overshoot could clear is now below-floor, and
        // the best plan the planner reports still satisfies the contract.
        let overshoot_floor = BigInt::from(629_252_486u64);
        let search = plan_claim_meeting_floor(
            &reserves, &prices, (&k.0, &k.1), (&BigInt::from(0), &BigInt::from(1)), 0, 1, &spendable, &overshoot_floor,
        )
        .expect("a best-effort plan is still feasible");
        assert!(!search.meets_floor, "no_flip caps the total below the overshoot floor");
        let total = &search.plan.dy + &search.plan.claim;
        assert!(total <= BigInt::from(628_314_683u64), "capped at the balance point: {total}");
        assert!(contract_accepts(&reserves, &prices, (&k.0, &k.1), (&BigInt::from(0), &BigInt::from(1)), &search.plan, 0, 1));

        // A floor at or under the cap is met, and the plan respects no_flip.
        let reachable_floor = BigInt::from(600_000_000u64);
        let search = plan_claim_meeting_floor(
            &reserves, &prices, (&k.0, &k.1), (&BigInt::from(0), &BigInt::from(1)), 0, 1, &spendable, &reachable_floor,
        )
        .expect("claim must be feasible");
        assert!(search.meets_floor, "a sub-cap floor is reachable");
        assert!(&search.plan.dy + &search.plan.claim >= reachable_floor);
        assert!(contract_accepts(&reserves, &prices, (&k.0, &k.1), (&BigInt::from(0), &BigInt::from(1)), &search.plan, 0, 1));
    }

    #[test]
    fn fee_paying_pool_claim_pays_balance_fee_and_contract_accepts() {
        // Non-waived pool (balance_fee = 10/10000, like preview c618676e…):
        // the op-portion swap must leave floor(input·bf) of value in the pool.
        // With prices [1,1,1], dy = dx − floor(dx·10/10000), so the pool keeps
        // the fee and the claim rides on the rebalancing on top.
        let before = pool(&[400_000_000, 800_000_000, 600_000_000]);
        let prices = ones(3);
        let k = (BigInt::from(9), BigInt::from(4000));
        let bf = (BigInt::from(10), BigInt::from(10000));
        let dx = BigInt::from(50_000_000);
        let plan = plan_claim(&before, &prices, (&k.0, &k.1), (&bf.0, &bf.1), 0, 1, &dx)
            .expect("imbalanced fee-paying pool should still admit a claim");
        assert!(plan.claim > BigInt::from(0));
        // dy is reduced by the balance_fee: v_increase_op = floor(50M·10/10000)
        // = 50_000, so dy = 50_000_000 − 50_000 = 49_950_000.
        assert_eq!(plan.dy, BigInt::from(49_950_000));
        // The exact contract clauses (incl. the balance_fee fee-pin) accept it.
        assert!(contract_accepts(&before, &prices, (&k.0, &k.1), (&bf.0, &bf.1), &plan, 0, 1));
        // Maximality: one more claim unit fails the contract check.
        let mut greedy = plan.clone();
        greedy.claim = &plan.claim + &BigInt::from(1);
        greedy.final_assets[1].1 = &plan.final_assets[1].1 - &BigInt::from(1);
        assert!(!contract_accepts(&before, &prices, (&k.0, &k.1), (&bf.0, &bf.1), &greedy, 0, 1));
    }

    #[test]
    fn fee_pin_rejects_a_waived_plan_on_a_fee_paying_pool() {
        // A value-neutral (waived) plan must NOT pass the contract check when
        // the pool charges balance_fee > 0: the op-portion underpays the fee.
        let before = pool(&[400_000_000, 800_000_000, 600_000_000]);
        let prices = ones(3);
        let k = (BigInt::from(9), BigInt::from(4000));
        let waived = plan_claim(
            &before, &prices, (&k.0, &k.1), (&BigInt::from(0), &BigInt::from(1)), 0, 1,
            &BigInt::from(50_000_000),
        )
        .expect("waived plan exists");
        // Same plan, judged against a fee-charging pool → rejected by the pin.
        let bf = (BigInt::from(10), BigInt::from(10000));
        assert!(!contract_accepts(&before, &prices, (&k.0, &k.1), (&bf.0, &bf.1), &waived, 0, 1));
    }

    /// min_deltas re-basing (SUN-310): entries are deltas over the order's
    /// holdings, not absolute floors. A +delta on an asset the order already
    /// holds plenty of is still a receive target (floor = holding + delta);
    /// −deltas cap outflow via the pin. This is the preprod claim shape that
    /// the absolute reading resolved to "no receive target".
    #[test]
    fn resolve_claim_shape_rebases_deltas_over_holdings() {
        let reserves = pool(&[3_250_000_000_000u64 as i64, 4_900_000_000_000, 6_850_000_000_000]);
        let prices = ones(3);
        let mut value = crate::cardano_types::Value::default();
        // Order already holds 10G of asset 1 (the receive target) and 10G of
        // asset 2 (the spend side); asset 3 unheld.
        value.insert(&reserves[0].0, BigInt::from(10_000_000_000u64));
        value.insert(&reserves[1].0, BigInt::from(10_000_000_000u64));
        let min_deltas = vec![
            (reserves[0].0.clone(), BigInt::from(1_500_000_000)),  // receive ≥ +1.5G
            (reserves[1].0.clone(), BigInt::from(-600_000_000)),   // spend ≤ 0.6G
        ];
        let shape = resolve_claim_shape(&value, &min_deltas, &reserves, &prices)
            .expect("delta mins must resolve");
        match shape {
            ResolvedShape::Pair(c) => {
                assert_eq!(c.in_idx, 1);
                assert_eq!(c.out_idx, 0);
                // Floor = holding + delta, counted gross of what's already held.
                assert_eq!(c.min_recv, BigInt::from(11_500_000_000u64));
                assert_eq!(c.already_held, BigInt::from(10_000_000_000u64));
                // Spendable = −delta, not the full holding.
                assert_eq!(c.spendable, BigInt::from(600_000_000));
            }
            other => panic!("expected Pair, got {other:?}"),
        }

        // No positive delta anywhere → nothing to receive.
        let no_positive = vec![(reserves[1].0.clone(), BigInt::from(-600_000_000))];
        let err = resolve_claim_shape(&value, &no_positive, &reserves, &prices)
            .expect_err("no positive delta → no receive target");
        assert!(err.contains("no receive target"), "got: {err}");
    }

    /// A held pool asset with NO signed entry is frozen (check_consumption),
    /// not forfeit: it must stay out of the trade entirely. Treating it as a
    /// 0-target forced the whole holding into the pool, turning a simple
    /// pair claim into a huge value donation the planner then rejected —
    /// which made signers write explicit 0-deltas for every untouched asset.
    #[test]
    fn resolve_claim_shape_freezes_unnamed_assets() {
        let reserves = pool(&[3_250_000_000_000u64 as i64, 4_900_000_000_000, 6_850_000_000_000]);
        let prices = ones(3);
        let mut value = crate::cardano_types::Value::default();
        value.insert(&reserves[0].0, BigInt::from(10_000_000_000u64)); // receive target
        value.insert(&reserves[1].0, BigInt::from(1_000_000_000u64));  // named spend
        value.insert(&reserves[2].0, BigInt::from(10_000_000_000u64)); // UNNAMED — frozen
        let min_deltas = vec![
            (reserves[0].0.clone(), BigInt::from(1_000_324_661)),
            (reserves[1].0.clone(), BigInt::from(-1_000_000_000)),
        ];
        let shape = resolve_claim_shape(&value, &min_deltas, &reserves, &prices)
            .expect("unnamed asset must not force a rebalance");
        match shape {
            ResolvedShape::Pair(c) => {
                assert_eq!(c.in_idx, 1);
                assert_eq!(c.out_idx, 0);
                assert_eq!(c.spendable, BigInt::from(1_000_000_000));
            }
            ResolvedShape::Rebalance(r) => {
                panic!("frozen asset was forfeited into a rebalance: {r:?}")
            }
        }
    }
}
