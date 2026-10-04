//! Banded concentrated liquidity: the scooper's half of
//! `lib/modules/banded_cl_check.ak` (sundae-v4).
//!
//! Every function here mirrors the module exactly, rounding included. Where
//! the two disagree the chain wins and the transaction fails, so the unit
//! tests pin the same vectors the Aiken tests use
//! (`lib/tests/unit/banded_cl_check.ak`, `banded_cl_cs.ak`).
//!
//! The shape: a ladder of `N` bands over sqrt-price, band `i` spanning
//! `[bands[i].start, bands[i+1].start)` and the last band ending at
//! `closing`. One integer counter `X` scales the whole ladder; band `i`'s
//! liquidity is `L_i = floor(w_i * X / W)`. The bands above the active one
//! hold only A, the bands below only B, in amounts fixed by `X`
//! ("saturation constants"); the active band holds the residual on its own
//! curve. The pool's `total_lp` IS `X` at Create and the two move together
//! on every proportional deposit, but `X` is re-derived from the reserves on
//! every spend, never stored.
//!
//! The rounding is not negotiable:
//!   - `L_k` floors          (a shallower curve buys less)
//!   - saturation ceils      (a bigger constant leaves a smaller residual)
//!   - saturation takes ONE division, never materialising `L_i` first.

use crate::bigint::BigInt;
use crate::sundaev4::types::{BandSpec, BandedCLConfig, PoolDatum, Rational};
use num_traits::{Signed, Zero};

/// `ceil(n / d)` for `n >= 0`, `d > 0`.
pub fn ceil_div(n: &BigInt, d: &BigInt) -> BigInt {
    &(&(n + d) - &BigInt::from(1)) / d
}

/// `D_i = p_i*q_{i-1} - p_{i-1}*q_i`: positive exactly when `lo < hi`.
pub fn delta(lo: &Rational, hi: &Rational) -> BigInt {
    &(&hi.num * &lo.den) - &(&lo.num * &hi.den)
}

/// Band `i`'s UPPER edge: the next band's start, or `closing` for the last.
pub fn upper_edge(cfg: &BandedCLConfig, i: usize) -> &Rational {
    cfg.bands.get(i + 1).map(|b| &b.start).unwrap_or(&cfg.closing)
}

/// The ladder evaluated at one witness `(x, k)`: `banded_cl_check.LadderState`.
#[derive(Debug, Clone)]
pub struct LadderState {
    /// `C_A(X, k)`: the A held by the bands ABOVE the active one.
    pub ca: BigInt,
    /// `C_B(X, k)`: the B held by the bands BELOW the active one.
    pub cb: BigInt,
    pub ca_next: BigInt,
    pub cb_next: BigInt,
    pub a_sat_k: BigInt,
    pub b_sat_k: BigInt,
    pub l_k: BigInt,
    pub l_k_next: BigInt,
    pub lo: Rational,
    pub hi: Rational,
    pub band: BandSpec,
}

/// `a_sat_i(x) = ceil(w_i * x * D_i / (W * p_{i-1} * p_i))`.
fn a_sat(cfg: &BandedCLConfig, x: &BigInt, i: usize) -> BigInt {
    let lo = &cfg.bands[i].start;
    let hi = upper_edge(cfg, i);
    let num = &(&cfg.bands[i].weight * &delta(lo, hi)) * x;
    let den = &(&cfg.weight_total * &lo.num) * &hi.num;
    ceil_div(&num, &den)
}

/// `b_sat_i(x) = ceil(w_i * x * D_i / (W * q_{i-1} * q_i))`.
fn b_sat(cfg: &BandedCLConfig, x: &BigInt, i: usize) -> BigInt {
    let lo = &cfg.bands[i].start;
    let hi = upper_edge(cfg, i);
    let num = &(&cfg.bands[i].weight * &delta(lo, hi)) * x;
    let den = &(&cfg.weight_total * &lo.den) * &hi.den;
    ceil_div(&num, &den)
}

/// One pass over the ladder at `(x, k)`. `Err` when `k` is out of range.
pub fn ladder_at(cfg: &BandedCLConfig, x: &BigInt, k: usize) -> Result<LadderState, String> {
    let n = cfg.bands.len();
    if k >= n {
        return Err(format!("band {k} out of range for a {n}-band ladder"));
    }
    let one = BigInt::from(1);
    let x1 = x + &one;
    let mut ca = BigInt::zero();
    let mut cb = BigInt::zero();
    let mut ca_next = BigInt::zero();
    let mut cb_next = BigInt::zero();
    for i in 0..n {
        if i < k {
            cb = &cb + &b_sat(cfg, x, i);
            cb_next = &cb_next + &b_sat(cfg, &x1, i);
        } else if i > k {
            ca = &ca + &a_sat(cfg, x, i);
            ca_next = &ca_next + &a_sat(cfg, &x1, i);
        }
    }
    let band = cfg.bands[k].clone();
    let lo = band.start.clone();
    let hi = upper_edge(cfg, k).clone();
    Ok(LadderState {
        a_sat_k: a_sat(cfg, x, k),
        b_sat_k: b_sat(cfg, x, k),
        // L_k rounds DOWN.
        l_k: &(&band.weight * x) / &cfg.weight_total,
        l_k_next: &(&band.weight * &x1) / &cfg.weight_total,
        ca,
        cb,
        ca_next,
        cb_next,
        lo,
        hi,
        band,
    })
}

/// `cl_check.f_at_raw`: the CL arc, cross-multiplied.
/// `(a*spb_num + l*spb_den) * (b*spa_den + l*spa_num) - l*l*spb_num*spa_den`.
pub fn f_at(a: &BigInt, b: &BigInt, l: &BigInt, lo: &Rational, hi: &Rational) -> BigInt {
    let va = &(a * &hi.num) + &(l * &hi.den);
    let vb = &(b * &lo.den) + &(l * &lo.num);
    &(&va * &vb) - &(&(&(l * l) * &hi.num) * &lo.den)
}

/// `G` on the active band's curve for residuals `(ra, rb)` at liquidity `l`.
pub fn g_of(st: &LadderState, ra: &BigInt, rb: &BigInt, l: &BigInt) -> BigInt {
    if st.band.curve.is_zero() {
        f_at(ra, rb, l, &st.lo, &st.hi)
    } else {
        let pn = &st.lo.num * &st.hi.num;
        let pd = &st.lo.den * &st.hi.den;
        &(&(ra * &pn) + &(rb * &pd)) - &(l * &delta(&st.lo, &st.hi))
    }
}

/// The band proof P2-P7 (`banded_cl_check.band_proof`). P1 (`x >= W`) is the
/// caller's. `tight` selects P7, which the chain evaluates unconditionally.
pub fn band_proof(a: &BigInt, b: &BigInt, st: &LadderState, tight: bool) -> bool {
    let ra = a - &st.ca;
    let rb = b - &st.cb;
    if ra.is_negative() || rb.is_negative() {
        return false; // P2, P3
    }
    if ra > st.a_sat_k || rb > st.b_sat_k {
        return false; // P4, P5
    }
    if g_of(st, &ra, &rb, &st.l_k).is_negative() {
        return false; // P6
    }
    if tight {
        let ra1 = a - &st.ca_next;
        let rb1 = b - &st.cb_next;
        return g_of(st, &ra1, &rb1, &st.l_k_next).is_negative(); // P7
    }
    true
}

/// The witness pair for a pair of reserves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Witness {
    pub x: BigInt,
    pub k: usize,
}

/// P1 and the full band proof at `(x, k)`.
pub fn is_witness(cfg: &BandedCLConfig, a: &BigInt, b: &BigInt, x: &BigInt, k: usize) -> bool {
    if x < &cfg.weight_total {
        return false;
    }
    match ladder_at(cfg, x, k) {
        Ok(st) => band_proof(a, b, &st, true),
        Err(_) => false,
    }
}

/// Find the witness `(X, k)` for a pool's reserves.
///
/// `G` is non-increasing in `X`, so `{X : G >= 0}` is a prefix and bisection
/// finds its end. Integer rounding can displace the witness by a few, so a
/// short scan follows. `None` when the reserves sit on no point of the
/// ladder; the chain would reject them too. Mirrors `banded-math.ts`.
pub fn find_witness(cfg: &BandedCLConfig, a: &BigInt, b: &BigInt) -> Option<Witness> {
    find_witness_scan(cfg, a, b, 400)
}

pub fn find_witness_scan(cfg: &BandedCLConfig, a: &BigInt, b: &BigInt, scan: u32) -> Option<Witness> {
    (0..cfg.bands.len()).find_map(|k| find_witness_in_band(cfg, a, b, k, scan))
}

/// The witness for `(a, b)` with band `k` active, if one exists. A state that
/// sits on a band edge can have a witness in both neighbouring bands; the
/// step planner uses this to continue in the band it is crossing into.
pub fn find_witness_in_band(cfg: &BandedCLConfig, a: &BigInt, b: &BigInt, k: usize, scan: u32) -> Option<Witness> {
    find_witness_in_band_from(cfg, a, b, k, scan, None)
}

/// `find_witness_in_band` seeded at `hint`: the counter only grows along a
/// swap, so the doubling search starts from the previous witness instead of
/// from 1. Falls back to the unseeded search when the hint does not satisfy
/// the sign and curve predicates.
pub fn find_witness_in_band_from(
    cfg: &BandedCLConfig,
    a: &BigInt,
    b: &BigInt,
    k: usize,
    scan: u32,
    hint: Option<&BigInt>,
) -> Option<Witness> {
    let one = BigInt::from(1);
    let two = BigInt::from(2);
    let cap = {
        let mut c = BigInt::from(1);
        for _ in 0..80 {
            c = &c * &two;
        }
        c
    };
    if let Some(h) = hint {
        let ok = |x: &BigInt| -> bool {
            match ladder_at(cfg, x, k) {
                Ok(st) => {
                    let ra = a - &st.ca;
                    let rb = b - &st.cb;
                    !ra.is_negative() && !rb.is_negative() && !g_of(&st, &ra, &rb, &st.l_k).is_negative()
                }
                Err(_) => false,
            }
        };
        if ok(h) {
            // Doubling from the hint, then bisection to the last ok x.
            let mut lo = h.clone();
            let mut span = one.clone();
            let mut hi = &lo + &span;
            while ok(&hi) {
                lo = hi;
                span = &span * &two;
                hi = h + &span;
                if hi >= cap {
                    break;
                }
            }
            while &lo + &one < hi {
                let m = &(&lo + &hi) / &two;
                if ok(&m) {
                    lo = m;
                } else {
                    hi = m;
                }
            }
            let sp = BigInt::from(scan as i64);
            let from = if &lo > &sp { &lo - &sp } else { one.clone() };
            let to = &lo + &sp;
            let mut x = to;
            while x >= from {
                if is_witness(cfg, a, b, &x, k) {
                    return Some(Witness { x, k });
                }
                x = &x - &one;
            }
        }
    }
    {
        let sign_ok = |x: &BigInt| -> bool {
            match ladder_at(cfg, x, k) {
                Ok(st) => !(a - &st.ca).is_negative() && !(b - &st.cb).is_negative(),
                Err(_) => false,
            }
        };
        let g_ok = |x: &BigInt| -> bool {
            match ladder_at(cfg, x, k) {
                Ok(st) => !g_of(&st, &(a - &st.ca), &(b - &st.cb), &st.l_k).is_negative(),
                Err(_) => false,
            }
        };
        let preds: [&dyn Fn(&BigInt) -> bool; 2] = [&sign_ok, &g_ok];
        let mut seeds: Vec<BigInt> = Vec::new();
        for ok in preds {
            if !ok(&one) {
                continue;
            }
            let mut lo = one.clone();
            let mut hi = two.clone();
            while ok(&hi) && hi < cap {
                lo = hi.clone();
                hi = &hi * &two;
            }
            if hi >= cap {
                continue;
            }
            while &lo + &one < hi {
                let m = &(&lo + &hi) / &two;
                if ok(&m) {
                    lo = m;
                } else {
                    hi = m;
                }
            }
            seeds.push(lo);
        }
        let span = BigInt::from(scan as i64);
        for seed in seeds {
            let from = if seed > span { &seed - &span } else { one.clone() };
            let to = &seed + &span;
            let mut x = from;
            while x <= to {
                if is_witness(cfg, a, b, &x, k) {
                    return Some(Witness { x, k });
                }
                x = &x + &one;
            }
        }
    }
    None
}

/// One in-band step of a swap plan: `dx` in, `dy` out, with the witnesses
/// the module sees before and after.
#[derive(Debug, Clone)]
pub struct SwapStepPlan {
    pub dx: BigInt,
    pub dy: BigInt,
    pub before: Witness,
    pub after: Witness,
}

/// The band a swap moves into when it drains the active band: selling A
/// takes the active band's B, so the ladder steps down; selling B steps up.
fn next_band(k: usize, is_a_input: bool, n: usize) -> Option<usize> {
    if is_a_input {
        k.checked_sub(1)
    } else if k + 1 < n {
        Some(k + 1)
    } else {
        None
    }
}

/// Plan a swap of `dx` as a sequence of in-band steps. Each step fills at
/// most what the active band can pay; a step that empties the band is
/// followed by one in the neighbouring band, whose witness is looked up
/// there first so the module's before-witness matches. Fails when the
/// ladder runs out before `dx` is consumed, or when no next-band witness
/// exists for an edge state.
pub fn swap_steps(
    cfg: &BandedCLConfig,
    a: &BigInt,
    b: &BigInt,
    is_a_input: bool,
    dx: &BigInt,
) -> Result<Vec<SwapStepPlan>, String> {
    swap_steps_from(cfg, a, b, is_a_input, dx, None)
}

/// `swap_steps` with the pool's current witness already known, so the first
/// step skips the unseeded search and every later step is seeded by the one
/// before it.
pub fn swap_steps_from(
    cfg: &BandedCLConfig,
    a: &BigInt,
    b: &BigInt,
    is_a_input: bool,
    dx: &BigInt,
    start: Option<&Witness>,
) -> Result<Vec<SwapStepPlan>, String> {
    if !dx.is_positive() {
        return Err("swap input is not positive".into());
    }
    let n = cfg.bands.len();
    let mut steps: Vec<SwapStepPlan> = Vec::new();
    let (mut ca, mut cb) = (a.clone(), b.clone());
    let mut remaining = dx.clone();
    let mut prefer: Option<usize> = start.map(|w| w.k);
    let mut hint: Option<BigInt> = start.map(|w| w.x.clone());
    let mut known: Option<Witness> = start.filter(|w| is_witness(cfg, a, b, &w.x, w.k)).cloned();
    // At most one step per band plus one retry at each edge.
    let mut guard = 2 * n + 2;
    while remaining.is_positive() {
        guard -= 1;
        if guard == 0 {
            return Err("banded swap plan did not converge".into());
        }
        let before = match known.take() {
            Some(w) => w,
            None => match prefer
                .and_then(|k| find_witness_in_band_from(cfg, &ca, &cb, k, 400, hint.as_ref()))
            {
                Some(w) => w,
                None => find_witness(cfg, &ca, &cb)
                    .ok_or_else(|| format!("no ladder witness for reserves ({ca}, {cb})"))?,
            },
        };
        let v = band_view(cfg, &ca, &cb, &before)?;
        let cap_dx = max_dx_in_band(&v, is_a_input);
        let step_dx = if remaining <= cap_dx { remaining.clone() } else { cap_dx };
        let dy = if step_dx.is_positive() { band_output(&v, is_a_input, &step_dx) } else { BigInt::zero() };
        if !dy.is_positive() {
            // This band pays nothing more: cross to the next one, once.
            let Some(k_next) = next_band(before.k, is_a_input, n) else {
                return Err(format!(
                    "swap of {dx} exhausts the ladder in band {} with {remaining} unfilled",
                    before.k
                ));
            };
            if prefer == Some(k_next) {
                return Err(format!(
                    "swap of {dx} cannot continue past band {}: no witness in band {k_next}                      for reserves ({ca}, {cb}), {remaining} unfilled",
                    before.k
                ));
            }
            prefer = Some(k_next);
            continue;
        }
        let (na, nb) = if is_a_input { (&ca + &step_dx, &cb - &dy) } else { (&ca - &dy, &cb + &step_dx) };
        let crossing = step_dx < remaining;
        let after = if crossing {
            next_band(before.k, is_a_input, n)
                .and_then(|k| find_witness_in_band_from(cfg, &na, &nb, k, 400, Some(&before.x)))
                .or_else(|| find_witness(cfg, &na, &nb))
        } else {
            find_witness_in_band_from(cfg, &na, &nb, before.k, 400, Some(&before.x))
                .or_else(|| find_witness(cfg, &na, &nb))
        }
        .ok_or_else(|| format!("no ladder witness for the after reserves ({na}, {nb})"))?;
        if after.x < before.x {
            return Err(format!(
                "step lowers the ladder counter ({} -> {}); the module requires fee_budget >= 0",
                before.x, after.x
            ));
        }
        remaining = &remaining - &step_dx;
        steps.push(SwapStepPlan { dx: step_dx, dy, before, after: after.clone() });
        ca = na;
        cb = nb;
        prefer = Some(after.k);
        hint = Some(after.x.clone());
        known = Some(after);
    }
    Ok(steps)
}

/// What the active band looks like at a witness: the residual reserves the
/// band prices against and its curve. The router builds its edge from this.
#[derive(Debug, Clone)]
pub struct BandView {
    /// Residual A and B inside the active band.
    pub ra: BigInt,
    pub rb: BigInt,
    /// `L_k`, the band's liquidity.
    pub l: BigInt,
    pub lo: Rational,
    pub hi: Rational,
    /// 0 = CL arc, 1 = constant-sum bin.
    pub curve: u8,
    pub fee_buy: Rational,
    pub fee_sell: Rational,
}

pub fn band_view(cfg: &BandedCLConfig, a: &BigInt, b: &BigInt, w: &Witness) -> Result<BandView, String> {
    let st = ladder_at(cfg, &w.x, w.k)?;
    Ok(BandView {
        ra: a - &st.ca,
        rb: b - &st.cb,
        l: st.l_k.clone(),
        lo: st.lo.clone(),
        hi: st.hi.clone(),
        curve: if st.band.curve.is_zero() { 0 } else { 1 },
        fee_buy: st.band.fee_buy.clone(),
        fee_sell: st.band.fee_sell.clone(),
    })
}

/// The fee rational a direction pays. Selling A (A is the input) pays
/// `fee_sell`; buying A (B is the input) pays `fee_buy`.
pub fn fee_for(v: &BandView, is_a_input: bool) -> &Rational {
    if is_a_input { &v.fee_sell } else { &v.fee_buy }
}

/// The output the active band pays for `dx` of the input asset, before the
/// band's capacity is considered. `banded_cl_check.banded_swap` step 8:
/// the CL output pair on the residuals' virtual reserves, or the CS bin's
/// pair. Both floor in the pool's favour.
pub fn band_output(v: &BandView, is_a_input: bool, dx: &BigInt) -> BigInt {
    let fee = fee_for(v, is_a_input);
    let fee_amt = &(dx * &fee.num) / &fee.den;
    let dx_eff = dx - &fee_amt;
    if !dx_eff.is_positive() {
        return BigInt::zero();
    }
    if v.curve == 0 {
        // Virtual reserves on the band's edges: VA = ra*hi.num + L*hi.den,
        // VB = rb*lo.den + L*lo.num.
        let va0 = &(&v.ra * &v.hi.num) + &(&v.l * &v.hi.den);
        let vb0 = &(&v.rb * &v.lo.den) + &(&v.l * &v.lo.num);
        if is_a_input {
            // dva_eff = dx_eff*spb_num; out*spa_den <= vb0*dva_eff/(va0+dva_eff)
            let dva_eff = &dx_eff * &v.hi.num;
            let denom = &(&va0 + &dva_eff) * &v.lo.den;
            if denom.is_zero() {
                return BigInt::zero();
            }
            &(&vb0 * &dva_eff) / &denom
        } else {
            // dvb_eff = dx_eff*spa_den (the corrected B-input scale);
            // out*spb_num <= va0*dvb_eff/(vb0+dvb_eff)
            let dvb_eff = &dx_eff * &v.lo.den;
            let denom = &(&vb0 + &dvb_eff) * &v.hi.num;
            if denom.is_zero() {
                return BigInt::zero();
            }
            &(&va0 * &dvb_eff) / &denom
        }
    } else {
        // The bin prices at the geometric mean of its edges, Pn/Pd.
        let pn = &v.lo.num * &v.hi.num;
        let pd = &v.lo.den * &v.hi.den;
        if is_a_input {
            // Selling A for B: out = floor(dx_eff * Pn / Pd)
            &(&dx_eff * &pn) / &pd
        } else {
            // Buying A with B: out = floor(dx_eff * Pd / Pn)
            &(&dx_eff * &pd) / &pn
        }
    }
}

/// The band's capacity in a direction: the residual of the output asset.
/// A step that would pay more leaves the band, which the module rejects
/// (`ra1 >= 0`, `rb1 >= 0`); it has to continue as another step.
pub fn band_capacity(v: &BandView, is_a_input: bool) -> &BigInt {
    if is_a_input { &v.rb } else { &v.ra }
}

/// The largest raw `dx` the active band fills in one step: the output stays
/// within `band_capacity`. Bisection on the monotone `band_output`.
pub fn max_dx_in_band(v: &BandView, is_a_input: bool) -> BigInt {
    let cap = band_capacity(v, is_a_input);
    if !cap.is_positive() {
        return BigInt::zero();
    }
    let one = BigInt::from(1);
    let two = BigInt::from(2);
    let fits = |dx: &BigInt| band_output(v, is_a_input, dx) <= *cap;
    // Find an upper bound that does not fit.
    let mut hi = one.clone();
    let mut guard = 0;
    while fits(&hi) {
        hi = &hi * &two;
        guard += 1;
        if guard > 200 {
            // The band cannot be exhausted at any input (a CL arc approaches
            // its capacity asymptotically); report the probe reached.
            return hi;
        }
    }
    let mut lo = BigInt::zero();
    while &lo + &one < hi {
        let m = &(&lo + &hi) / &two;
        if fits(&m) {
            lo = m;
        } else {
            hi = m;
        }
    }
    lo
}

/// `budget_pair_form3` solved for `bp`: `bp = floor(lp_before * x_after /
/// x_before)`, and the step's `fee_budget = bp - lp_after`. For a swap
/// `lp_after == lp_before`, so the budget is the LP the counter's growth is
/// worth.
/// The state a swap of `dx` leaves behind: the reserves and the band view
/// at the final step's after-witness. `None` when the ladder cannot absorb
/// `dx`.
pub fn view_after_from(
    cfg: &BandedCLConfig,
    a: &BigInt,
    b: &BigInt,
    is_a_input: bool,
    dx: &BigInt,
    start: Option<&Witness>,
) -> Option<(BigInt, BigInt, BandView)> {
    let steps = swap_steps_from(cfg, a, b, is_a_input, dx, start).ok()?;
    let last = steps.last()?;
    let (mut na, mut nb) = (a.clone(), b.clone());
    for st in &steps {
        if is_a_input {
            na = &na + &st.dx;
            nb = &nb - &st.dy;
        } else {
            nb = &nb + &st.dx;
            na = &na - &st.dy;
        }
    }
    let v = band_view(cfg, &na, &nb, &last.after).ok()?;
    Some((na, nb, v))
}

/// The largest input the whole ladder absorbs in one direction: the sum of
/// every band's capacity from the active band to the ladder's end.
pub fn ladder_capacity(cfg: &BandedCLConfig, a: &BigInt, b: &BigInt, is_a_input: bool) -> BigInt {
    ladder_capacity_from(cfg, a, b, is_a_input, None)
}

pub fn ladder_capacity_from(
    cfg: &BandedCLConfig,
    a: &BigInt,
    b: &BigInt,
    is_a_input: bool,
    start: Option<&Witness>,
) -> BigInt {
    let n = cfg.bands.len();
    let w = match start {
        Some(w) if is_witness(cfg, a, b, &w.x, w.k) => w.clone(),
        _ => match find_witness(cfg, a, b) {
            Some(w) => w,
            None => return BigInt::zero(),
        },
    };
    let (mut ca, mut cb) = (a.clone(), b.clone());
    let mut prefer = Some(w.k);
    let mut hint = w.x.clone();
    let mut total = BigInt::zero();
    for _ in 0..(2 * n + 2) {
        let Some(wk) = prefer
            .and_then(|k| find_witness_in_band_from(cfg, &ca, &cb, k, 400, Some(&hint)))
            .or_else(|| find_witness(cfg, &ca, &cb))
        else {
            break;
        };
        hint = wk.x.clone();
        let Ok(v) = band_view(cfg, &ca, &cb, &wk) else {
            break;
        };
        let cap_dx = max_dx_in_band(&v, is_a_input);
        let dy = if cap_dx.is_positive() { band_output(&v, is_a_input, &cap_dx) } else { BigInt::zero() };
        if dy.is_positive() {
            total = &total + &cap_dx;
            if is_a_input {
                ca = &ca + &cap_dx;
                cb = &cb - &dy;
            } else {
                cb = &cb + &cap_dx;
                ca = &ca - &dy;
            }
        }
        let Some(k_next) = next_band(wk.k, is_a_input, n) else {
            break;
        };
        if !dy.is_positive() && prefer == Some(k_next) {
            break;
        }
        prefer = Some(k_next);
    }
    total
}

pub fn fee_budget(lp_before: &BigInt, x_before: &BigInt, x_after: &BigInt, lp_after: &BigInt) -> BigInt {
    &(&(lp_before * x_after) / x_before) - lp_after
}

/// One in-band swap, fully resolved: the output, the after reserves, the
/// after witness and the fee budget the entry must declare.
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct BandedSwapResult {
    pub dy: BigInt,
    pub a_after: BigInt,
    pub b_after: BigInt,
    pub before: Witness,
    pub after: Witness,
    pub fee_budget: BigInt,
}

/// Resolve a swap of `dx` against the ladder at reserves `(a, b)`, total LP
/// `lp`. `Err` names why the step is unfillable: no witness for the input
/// reserves, the step would leave the band, no output, or the after reserves
/// have no witness.
#[cfg(test)]
pub fn swap_step(
    cfg: &BandedCLConfig,
    a: &BigInt,
    b: &BigInt,
    lp: &BigInt,
    is_a_input: bool,
    dx: &BigInt,
) -> Result<BandedSwapResult, String> {
    if !dx.is_positive() {
        return Err("swap input is not positive".into());
    }
    let before = find_witness(cfg, a, b)
        .ok_or_else(|| format!("no ladder witness for reserves ({a}, {b})"))?;
    let v = band_view(cfg, a, b, &before)?;
    let dy = band_output(&v, is_a_input, dx);
    if !dy.is_positive() {
        return Err(format!("swap of {dx} yields no output in band {}", before.k));
    }
    let cap = band_capacity(&v, is_a_input);
    if &dy > cap {
        return Err(format!(
            "swap of {dx} would take {dy} but band {} holds only {cap}; the step would cross \
             a band edge (max input in this band {})",
            before.k,
            max_dx_in_band(&v, is_a_input)
        ));
    }
    let (a_after, b_after) = if is_a_input { (a + dx, b - &dy) } else { (a - &dy, b + dx) };
    let after = find_witness(cfg, &a_after, &b_after).ok_or_else(|| {
        format!("no ladder witness for the after reserves ({a_after}, {b_after})")
    })?;
    let fb = fee_budget(lp, &before.x, &after.x, lp);
    if fb.is_negative() {
        return Err(format!(
            "swap lowers the ladder counter ({} -> {}); the module requires fee_budget >= 0",
            before.x, after.x
        ));
    }
    Ok(BandedSwapResult {
        dy,
        a_after,
        b_after,
        before,
        after,
        fee_budget: fb,
    })
}

// ─── Config hashing and shape ───────────────────────────────────────────────

/// `module_state` slot value: `blake2b_256(serialise_data(config))`.
pub fn config_hash(cfg: &BandedCLConfig) -> Vec<u8> {
    use plutus_parser::AsPlutus;
    let cbor = minicbor::to_vec(cfg.clone().to_plutus()).expect("PlutusData encodes");
    pallas_crypto::hash::Hasher::<256>::hash(&cbor).to_vec()
}

/// True when `cfg` is the preimage of the banded slot in `datum`'s
/// `module_state`.
pub fn config_matches_datum(cfg: &BandedCLConfig, datum: &PoolDatum, module_hash: &[u8]) -> bool {
    datum
        .module_state
        .iter()
        .find(|(cred, _)| cred.as_slice() == module_hash)
        .is_some_and(|(_, stored)| *stored == config_hash(cfg))
}

/// `2^64`, the bound on every rational component (V9).
fn rational_bound() -> BigInt {
    let mut c = BigInt::from(1);
    for _ in 0..64 {
        c = &c * &BigInt::from(2);
    }
    c
}

fn check_fee(f: &Rational, what: &str) -> Result<(), String> {
    let cap = rational_bound();
    if f.num.is_negative() || !f.den.is_positive() || f.num >= f.den || f.num > cap || f.den > cap {
        return Err(format!("{what} {}/{} must satisfy 0 <= num < den <= 2^64", f.num, f.den));
    }
    Ok(())
}

/// V1 to V9 of the spec: the Create-time shape checks
/// (`banded_cl_check.check_shape`). A config that fails these did not come
/// from a real pool.
pub fn check_shape(cfg: &BandedCLConfig) -> Result<(), String> {
    let cap = rational_bound();
    if cfg.bands.is_empty() {
        return Err("empty ladder".into());
    }
    let edge_ok = |r: &Rational| r.num.is_positive() && r.den.is_positive() && r.num <= cap && r.den <= cap;
    let mut wt = BigInt::zero();
    for (i, band) in cfg.bands.iter().enumerate() {
        if !edge_ok(&band.start) {
            return Err(format!("band {i}: start {}/{} out of (0, 2^64]", band.start.num, band.start.den));
        }
        if band.weight < BigInt::from(1) {
            return Err(format!("band {i}: weight {} < 1", band.weight));
        }
        if !(band.curve.is_zero() || band.curve == BigInt::from(1)) {
            return Err(format!("band {i}: unknown curve {}", band.curve));
        }
        check_fee(&band.fee_buy, &format!("band {i} fee_buy"))?;
        check_fee(&band.fee_sell, &format!("band {i} fee_sell"))?;
        let next = upper_edge(cfg, i);
        if !delta(&band.start, next).is_positive() {
            return Err(format!("band {i}: edges not strictly increasing"));
        }
        wt = &wt + &band.weight;
    }
    if !edge_ok(&cfg.closing) {
        return Err("closing edge out of (0, 2^64]".into());
    }
    if wt != cfg.weight_total {
        return Err(format!("weight_total {} != sum of weights {wt}", cfg.weight_total));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(n: i64, d: i64) -> Rational {
        Rational { num: BigInt::from(n), den: BigInt::from(d) }
    }

    fn band(n: i64, d: i64, curve: i64, fee: (i64, i64)) -> BandSpec {
        BandSpec {
            start: r(n, d),
            weight: BigInt::from(1),
            curve: BigInt::from(curve),
            fee_buy: r(fee.0, fee.1),
            fee_sell: r(fee.0, fee.1),
        }
    }

    /// The eight equal-weight arcs of `lib/tests/unit/banded_cl_check.ak`:
    /// sqrt-price 1.00 .. 1.08 in steps of 0.01, fee 0.3%.
    fn eq8() -> BandedCLConfig {
        BandedCLConfig {
            bands: (0..8).map(|i| band(1_000_000 + 10_000 * i, 1_000_000, 0, (3, 1000))).collect(),
            closing: r(1_080_000, 1_000_000),
            weight_total: BigInt::from(8),
        }
    }

    const RESERVE_A: i64 = 8_637_368;
    const RESERVE_B: i64 = 624_999;

    #[test]
    fn witness_matches_the_aiken_vector() {
        let cfg = eq8();
        let w = find_witness(&cfg, &BigInt::from(RESERVE_A), &BigInt::from(RESERVE_B)).unwrap();
        assert_eq!(w.x, BigInt::from(999_999_478i64));
        assert_eq!(w.k, 0);
        let st = ladder_at(&cfg, &w.x, w.k).unwrap();
        assert_eq!(st.ca, BigInt::from(8_021_634i64));
        assert_eq!(st.cb, BigInt::from(0));
        assert_eq!(st.l_k, BigInt::from(124_999_934i64));
        assert_eq!(st.a_sat_k, BigInt::from(1_237_624i64));
        assert!(band_proof(&BigInt::from(RESERVE_A), &BigInt::from(RESERVE_B), &st, true));
        // The far band at the counter a dishonest scooper would name.
        assert!(!is_witness(&cfg, &BigInt::from(RESERVE_A), &BigInt::from(RESERVE_B), &BigInt::from(71_428_000i64), 7));
    }

    /// `banded_swap_accepts_the_true_witness`: 624 B in, 616 A out, counter
    /// 999_999_478 -> 999_999_641, fee_budget 163 at total_lp 1e9.
    #[test]
    fn swap_matches_the_aiken_vector() {
        let cfg = eq8();
        let s = swap_step(
            &cfg,
            &BigInt::from(RESERVE_A),
            &BigInt::from(RESERVE_B),
            &BigInt::from(1_000_000_000i64),
            false,
            &BigInt::from(624),
        )
        .unwrap();
        assert_eq!(s.dy, BigInt::from(616));
        assert_eq!(s.before.x, BigInt::from(999_999_478i64));
        assert_eq!(s.a_after, BigInt::from(8_636_752i64));
        assert_eq!(s.b_after, BigInt::from(625_623i64));
        assert_eq!(s.after.x, BigInt::from(999_999_641i64));
        assert_eq!(s.after.k, 0);
        assert_eq!(s.fee_budget, BigInt::from(163));
    }

    /// The proportional deposit of `banded_walk_accepts_swap_then_deposit`:
    /// doubling the pool lands on counter 1_999_999_693.
    #[test]
    fn doubled_pool_has_the_aiken_counter() {
        let cfg = eq8();
        let w = find_witness(&cfg, &BigInt::from(17_273_504i64), &BigInt::from(1_251_246i64)).unwrap();
        assert_eq!(w.x, BigInt::from(1_999_999_693i64));
        assert_eq!(w.k, 0);
    }

    /// `lib/tests/unit/banded_cl_cs.ak`: one constant-sum bin over sqrt-price
    /// 2..3 prices at 6/1. Buying A with 1e6 B pays 166_166 A; selling 1e6 A
    /// pays 5_982_000 B. Fee budgets 3 and 18 at total_lp 1e6.
    #[test]
    fn cs_bin_matches_the_aiken_vectors() {
        let cfg = BandedCLConfig {
            bands: vec![band(2, 1, 1, (3, 1000))],
            closing: r(3, 1),
            weight_total: BigInt::from(1),
        };
        let (a, b) = (BigInt::from(41_666_666i64), BigInt::from(750_000_004i64));
        let lp = BigInt::from(1_000_000i64);
        let w = find_witness(&cfg, &a, &b).unwrap();
        assert_eq!(w.x, BigInt::from(1_000_000_000i64));

        let buy = swap_step(&cfg, &a, &b, &lp, false, &BigInt::from(1_000_000i64)).unwrap();
        assert_eq!(buy.dy, BigInt::from(166_166));
        assert_eq!(buy.a_after, BigInt::from(41_500_500i64));
        assert_eq!(buy.b_after, BigInt::from(751_000_004i64));
        assert_eq!(buy.after.x, BigInt::from(1_000_003_004i64));
        assert_eq!(buy.fee_budget, BigInt::from(3));

        let sell = swap_step(&cfg, &a, &b, &lp, true, &BigInt::from(1_000_000i64)).unwrap();
        assert_eq!(sell.dy, BigInt::from(5_982_000i64));
        assert_eq!(sell.a_after, BigInt::from(42_666_666i64));
        assert_eq!(sell.b_after, BigInt::from(744_018_004i64));
        assert_eq!(sell.after.x, BigInt::from(1_000_018_000i64));
        assert_eq!(sell.fee_budget, BigInt::from(18));
    }

    /// A mixed ladder (arc, bin, arc) with the price in the bin, from
    /// `banded_cl_cs.ak`: the witness is at band 1, counter 1_000_000_001.
    #[test]
    fn mixed_ladder_witness_is_in_the_bin() {
        let cfg = BandedCLConfig {
            bands: vec![band(1, 1, 0, (3, 1000)), band(2, 1, 1, (3, 1000)), band(3, 1, 0, (3, 1000))],
            closing: r(4, 1),
            weight_total: BigInt::from(3),
        };
        let w = find_witness(&cfg, &BigInt::from(41_666_666i64), &BigInt::from(583_333_339i64)).unwrap();
        assert_eq!(w.k, 1);
        assert_eq!(w.x, BigInt::from(1_000_000_001i64));
        assert!(check_shape(&cfg).is_ok());
    }

    #[test]
    fn a_step_that_crosses_the_band_edge_is_refused_and_capped() {
        let cfg = eq8();
        let (a, b) = (BigInt::from(RESERVE_A), BigInt::from(RESERVE_B));
        let lp = BigInt::from(1_000_000_000i64);
        // The whole B residual is 624_999; selling enough A to take it all
        // leaves the band.
        let err = swap_step(&cfg, &a, &b, &lp, true, &BigInt::from(10_000_000i64)).unwrap_err();
        assert!(err.contains("cross a band edge"), "{err}");
        let w = find_witness(&cfg, &a, &b).unwrap();
        let v = band_view(&cfg, &a, &b, &w).unwrap();
        let max = max_dx_in_band(&v, true);
        assert!(band_output(&v, true, &max) <= v.rb);
        assert!(band_output(&v, true, &(&max + &BigInt::from(1))) > v.rb);
        // The capped step itself resolves.
        assert!(swap_step(&cfg, &a, &b, &lp, true, &max).is_ok());
    }

    /// A B-input swap larger than band 0 can pay splits into steps: the
    /// first drains band 0's A, the next continues in band 1. Every step's
    /// before-witness is the previous step's after-witness, and the counter
    /// never falls, which is what the module's walk checks.
    #[test]
    fn a_swap_past_the_band_edge_plans_two_steps() {
        let cfg = eq8();
        let a = BigInt::from(RESERVE_A);
        let b = BigInt::from(RESERVE_B);
        let w = find_witness(&cfg, &a, &b).unwrap();
        let v = band_view(&cfg, &a, &b, &w).unwrap();
        let cap = max_dx_in_band(&v, false);
        let dx = &cap + &BigInt::from(50_000);
        let steps = swap_steps(&cfg, &a, &b, false, &dx).expect("plan should cross into band 1");
        assert_eq!(steps.len(), 2, "steps: {steps:?}");
        assert_eq!(steps[0].before.k, 0);
        assert_eq!(steps[0].dx, cap);
        assert_eq!(steps[1].before, steps[0].after, "witness chains across the edge");
        assert_eq!(steps[1].before.k, 1);
        assert_eq!(steps[1].dx, BigInt::from(50_000));
        assert!(steps[1].dy.is_positive());
        for st in &steps {
            assert!(st.after.x >= st.before.x, "counter falls in {st:?}");
        }
        let total_dx: BigInt = steps.iter().fold(BigInt::zero(), |acc, s| &acc + &s.dx);
        assert_eq!(total_dx, dx);
        // Each step passes the module's own checks on its before state.
        let (mut ca, mut cb) = (a.clone(), b.clone());
        for st in &steps {
            assert!(is_witness(&cfg, &ca, &cb, &st.before.x, st.before.k));
            ca = &ca - &st.dy;
            cb = &cb + &st.dx;
            assert!(is_witness(&cfg, &ca, &cb, &st.after.x, st.after.k));
        }
    }

    /// A single-band swap plans one step identical to `swap_step`.
    #[test]
    fn an_in_band_swap_plans_one_step() {
        let cfg = eq8();
        let a = BigInt::from(RESERVE_A);
        let b = BigInt::from(RESERVE_B);
        let steps = swap_steps(&cfg, &a, &b, false, &BigInt::from(624)).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].dy, BigInt::from(616));
        assert_eq!(steps[0].after.x, BigInt::from(999_999_641i64));
    }

    #[test]
    fn primitive_timings() {
        let cfg = eq8();
        let (a, b) = (BigInt::from(12_000_000i64), BigInt::from(2_400_000i64));
        let t = std::time::Instant::now();
        let w = find_witness(&cfg, &a, &b).unwrap();
        eprintln!("find_witness: {:?} -> {:?}", t.elapsed(), w);
        let v = band_view(&cfg, &a, &b, &w).unwrap();
        let t = std::time::Instant::now();
        let cap = max_dx_in_band(&v, false);
        eprintln!("max_dx_in_band: {:?} -> {cap}", t.elapsed());
        let t = std::time::Instant::now();
        let st = swap_steps(&cfg, &a, &b, false, &(&cap * &BigInt::from(3))).unwrap();
        eprintln!("swap_steps x3 bands: {:?} -> {} steps", t.elapsed(), st.len());
        let t = std::time::Instant::now();
        let lc = ladder_capacity(&cfg, &a, &b, false);
        eprintln!("ladder_capacity: {:?} -> {lc}", t.elapsed());
        let t = std::time::Instant::now();
        let _ = view_after_from(&cfg, &a, &b, false, &(&cap * &BigInt::from(3)), Some(&w));
        eprintln!("view_after: {:?}", t.elapsed());
    }

    #[test]
    fn shape_checks_reject_malformed_ladders() {
        let mut cfg = eq8();
        cfg.weight_total = BigInt::from(9);
        assert!(check_shape(&cfg).is_err());
        let mut cfg = eq8();
        cfg.bands[3].weight = BigInt::from(0);
        assert!(check_shape(&cfg).is_err());
        let mut cfg = eq8();
        cfg.closing = r(1, 1);
        assert!(check_shape(&cfg).is_err());
        assert!(check_shape(&eq8()).is_ok());
    }

    #[test]
    fn config_hash_is_stable_and_matches_a_datum_slot() {
        let cfg = eq8();
        let h = config_hash(&cfg);
        assert_eq!(h.len(), 32);
        assert_eq!(h, config_hash(&cfg.clone()));
    }
}
