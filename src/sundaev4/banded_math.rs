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
//!   - the PREFIX sums round up ONCE, not once per band: the config carries
//!     the cumulative coefficients in fixed point at `2^64` (the ladder
//!     index, spec section 2.3) and a spend multiplies by the counter and
//!     ceils the product. A per-band sum of ceilings is a DIFFERENT number
//!     and the chain will reject every transaction built from it.

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

/// `2^64`: the fixed-point scale of the ladder index, and the bound every
/// rational component of a config must respect (V9).
pub fn fixed_one() -> BigInt {
    let mut c = BigInt::from(1);
    for _ in 0..64 {
        c = &c * &BigInt::from(2);
    }
    c
}

/// Band `i`'s saturation coefficient on the A side: the part of `a_sat_i`
/// that does not depend on the counter, in fixed point and rounded UP.
///
///     a_sat_i = ceil(coeff_a_i * X / 2^64)   with the aggregate rounding
fn coeff_a(cfg: &BandedCLConfig, i: usize, scale: &BigInt) -> BigInt {
    let lo = &cfg.bands[i].start;
    let hi = upper_edge(cfg, i);
    let num = &(&cfg.bands[i].weight * &delta(lo, hi)) * scale;
    let den = &(&cfg.weight_total * &lo.num) * &hi.num;
    ceil_div(&num, &den)
}

/// The same on the B side.
fn coeff_b(cfg: &BandedCLConfig, i: usize, scale: &BigInt) -> BigInt {
    let lo = &cfg.bands[i].start;
    let hi = upper_edge(cfg, i);
    let num = &(&cfg.bands[i].weight * &delta(lo, hi)) * scale;
    let den = &(&cfg.weight_total * &lo.den) * &hi.den;
    ceil_div(&num, &den)
}

/// The ladder index: entry `k` is the pair of cumulative coefficients for
/// the bands strictly ABOVE and strictly BELOW band `k`.
///
/// `cfg.index` is ignored; this derives it from the bands, which is exactly
/// what `Create` pins (V13). Build a config with `with_index` rather than
/// calling this directly.
pub fn build_index(cfg: &BandedCLConfig) -> Vec<(BigInt, BigInt)> {
    let n = cfg.bands.len();
    let scale = fixed_one();
    let a: Vec<BigInt> = (0..n).map(|i| coeff_a(cfg, i, &scale)).collect();
    let b: Vec<BigInt> = (0..n).map(|i| coeff_b(cfg, i, &scale)).collect();
    let mut out = Vec::with_capacity(n);
    for k in 0..n {
        let mut ca = BigInt::zero();
        let mut cb = BigInt::zero();
        for i in 0..n {
            if i > k {
                ca = &ca + &a[i];
            } else if i < k {
                cb = &cb + &b[i];
            }
        }
        out.push((ca, cb));
    }
    out
}

/// A config with its index derived from its bands. Every constructor goes
/// through here: a config whose index does not match its bands fails
/// `Create` and bricks every spend of a pool that already has one.
pub fn with_index(
    bands: Vec<BandSpec>,
    closing: Rational,
    weight_total: BigInt,
) -> BandedCLConfig {
    let mut cfg = BandedCLConfig {
        bands,
        index: vec![],
        closing,
        weight_total,
    };
    cfg.index = build_index(&cfg);
    cfg
}

/// One pass over the ladder at `(x, k)`. `Err` when `k` is out of range.
pub fn ladder_at(cfg: &BandedCLConfig, x: &BigInt, k: usize) -> Result<LadderState, String> {
    let n = cfg.bands.len();
    if k >= n {
        return Err(format!("band {k} out of range for a {n}-band ladder"));
    }
    let one = BigInt::from(1);
    let x1 = x + &one;
    // The prefix sums come from the pinned index, rounded up ONCE after the
    // multiply by the counter. Summing `a_sat`/`b_sat` per band gives a
    // different number and the chain rejects it.
    let Some((ca_coeff, cb_coeff)) = cfg.index.get(k) else {
        return Err(format!(
            "ladder index has {} entries for a {n}-band ladder",
            cfg.index.len()
        ));
    };
    let scale = fixed_one();
    let ca = ceil_div(&(ca_coeff * x), &scale);
    let cb = ceil_div(&(cb_coeff * x), &scale);
    let ca_next = ceil_div(&(ca_coeff * &x1), &scale);
    let cb_next = ceil_div(&(cb_coeff * &x1), &scale);
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
    if let Some(hit) = memo_get(cfg, a, b) {
        note_memo_hit();
        return hit;
    }
    let t0 = std::time::Instant::now();
    let out = find_witness_inner(cfg, a, b);
    note_witness_time(t0.elapsed().as_secs_f64());
    memo_put(cfg, a, b, &out);
    out
}

// ── Memoising the witness ───────────────────────────────────────────────
//
// `find_witness` is a pure function of the ladder and the reserves, and a
// scoop asks it the same question many times over: the router quotes a
// pool, the splitter re-quotes it at the same fill, and
// `compute_fee_budget` asks for the before AND after witness of every step
// it prices. Measured on preview over 2h46m: 521,702 calls, about 800 per
// build attempt, 3,183s of CPU — 79% of everything the scooper computed.
//
// The memo NEVER goes stale and needs no invalidation. A pool's reserves
// change, but then `(a, b)` is a different key; the answer stored against
// the old key is still the right answer for the old key.
//
// It is keyed on the WHOLE config, unlike the seed cache above. A seed
// collision costs one failed narrow search. A memo collision would hand
// back the witness of a DIFFERENT ladder, which the caller has no way to
// detect.
const MEMO_LADDERS: usize = 64;
const MEMO_STATES: usize = 8192;

type MemoStates = BTreeMap<(BigInt, BigInt), Option<Witness>>;

thread_local! {
    static WITNESS_MEMO: RefCell<Vec<(BandedCLConfig, MemoStates)>> =
        const { RefCell::new(Vec::new()) };
    static MEMO_HITS: RefCell<u64> = const { RefCell::new(0) };
}

fn memo_get(cfg: &BandedCLConfig, a: &BigInt, b: &BigInt) -> Option<Option<Witness>> {
    WITNESS_MEMO.with(|m| {
        m.borrow()
            .iter()
            .find(|(c, _)| c == cfg)
            .and_then(|(_, states)| states.get(&(a.clone(), b.clone())).cloned())
    })
}

fn memo_put(cfg: &BandedCLConfig, a: &BigInt, b: &BigInt, out: &Option<Witness>) {
    WITNESS_MEMO.with(|m| {
        let mut m = m.borrow_mut();
        // Bounded in both dimensions. A scooper sees a few dozen ladders,
        // and each build adds states for its trial fills, so the state
        // table is the one that grows without a bound of its own. Dropping
        // either only costs cold searches.
        if m.len() > MEMO_LADDERS {
            m.clear();
        }
        let slot = match m.iter().position(|(c, _)| c == cfg) {
            Some(i) => i,
            None => {
                m.push((cfg.clone(), BTreeMap::new()));
                m.len() - 1
            }
        };
        let states = &mut m[slot].1;
        if states.len() >= MEMO_STATES {
            states.clear();
        }
        states.insert((a.clone(), b.clone()), out.clone());
    });
}

fn note_memo_hit() {
    MEMO_HITS.with(|h| *h.borrow_mut() += 1);
}

/// Take the memo hit count since the last drain. The scooper exports it
/// beside the search histogram: the histogram times the searches that ran,
/// this says how many never had to.
pub fn drain_witness_memo_hits() -> u64 {
    MEMO_HITS.with(|h| std::mem::take(&mut *h.borrow_mut()))
}

/// Drop the memo. Tests that measure a cold search need this; the scooper
/// never calls it.
#[cfg(test)]
pub fn clear_witness_memo() {
    WITNESS_MEMO.with(|m| m.borrow_mut().clear());
}

fn find_witness_inner(cfg: &BandedCLConfig, a: &BigInt, b: &BigInt) -> Option<Witness> {
    // Try to start from where this ladder was last seen before falling back
    // to the exhaustive search. See `seeded_find` for why this is safe.
    if let Some(w) = seeded_find(cfg, a, b) {
        return Some(w);
    }
    let found = find_witness_scan(cfg, a, b, 400);
    if let Some(w) = &found {
        remember_witness(cfg, w);
    }
    found
}

// ── Witness-search timing ───────────────────────────────────────────────
//
// This call cost 92-159ms cold and took the scooper from scooping every
// block to ninety seconds per order dispatch, and NOTHING measured it —
// the cost was found by hand-benchmarking after the stall. The seeded path
// is ~0.4ms, so the two differ by three orders of magnitude and "which one
// are we on" is the question an operator needs answered.
//
// Durations are buffered here rather than reported directly so this module
// keeps no dependency on the metrics layer; the scooper drains them into a
// histogram each cycle. The buffer is bounded: if nobody drains, old
// observations are dropped rather than growing without limit.
const WITNESS_TIME_CAP: usize = 4096;

thread_local! {
    static WITNESS_TIMES: RefCell<Vec<f64>> = const { RefCell::new(Vec::new()) };
}

fn note_witness_time(secs: f64) {
    WITNESS_TIMES.with(|v| {
        let mut v = v.borrow_mut();
        if v.len() >= WITNESS_TIME_CAP {
            v.clear();
        }
        v.push(secs);
    });
}

/// Take the witness-search durations recorded since the last drain.
pub fn drain_witness_times() -> Vec<f64> {
    WITNESS_TIMES.with(|v| std::mem::take(&mut *v.borrow_mut()))
}

// ── Seeding the witness search ──────────────────────────────────────────
//
// `find_witness_scan` walks every band, and for each runs a doubling
// search, a bisection and a scan of 800 counters, each of those a full
// band proof in bignums. Measured on the ladders the traffic simulator
// provisions: 159ms for a 16-band ladder, 92ms for an 8-band one. The
// router calls this once per banded pool per graph build and TWICE per
// step evaluation in `compute_fee_budget`, so a handful of banded pools on
// traded pairs takes the scooper from scooping every block to a minute and
// a half per order. That is not a tuning problem, it is the difference
// between working and not.
//
// Nearly all of that cost is bands that cannot hold the witness: searching
// only the right band costs 8ms, and starting from a known counter costs
// 0.35ms — a factor of 450.
//
// A scoop asks about the same ladder over and over, at states near each
// other: the pool as indexed, then the same pool after a trial fill, then
// after a slightly different trial fill. So remember where this ladder was
// last found and start there, trying that band and its two neighbours with
// a narrow scan.
//
// THIS CANNOT RETURN A WRONG WITNESS. Every candidate still has to pass
// the full band proof P1-P7 in `is_witness`, which is what defines a
// witness; the seed only decides where to look first. A stale or useless
// seed costs one failed narrow search and falls through to the exhaustive
// path. The scan width is likewise only a search bound: a hit at width 8
// satisfies exactly the same proof as a hit at width 400.
use std::cell::RefCell;
use std::collections::BTreeMap;

thread_local! {
    static LAST_WITNESS: RefCell<BTreeMap<LadderKey, Witness>> =
        const { RefCell::new(BTreeMap::new()) };
}

/// Cheap identity for a ladder. A collision costs a failed seed, nothing
/// more, so this does not need to be a hash of the whole config.
type LadderKey = (usize, String, String, String);

fn ladder_key(cfg: &BandedCLConfig) -> LadderKey {
    (
        cfg.bands.len(),
        cfg.weight_total.to_string(),
        cfg.closing.num.to_string(),
        cfg.bands.first().map(|b| b.start.num.to_string()).unwrap_or_default(),
    )
}

fn remember_witness(cfg: &BandedCLConfig, w: &Witness) {
    LAST_WITNESS.with(|m| {
        let mut m = m.borrow_mut();
        // Bounded: a scooper sees a few dozen ladders, and dropping the map
        // only costs cold searches.
        if m.len() > 256 {
            m.clear();
        }
        m.insert(ladder_key(cfg), w.clone());
    });
}

/// Drop the seed cache. Tests that measure a cold search need this; the
/// scooper never calls it.
#[cfg(test)]
pub fn clear_witness_seeds() {
    LAST_WITNESS.with(|m| m.borrow_mut().clear());
    clear_witness_memo();
}

fn seeded_find(cfg: &BandedCLConfig, a: &BigInt, b: &BigInt) -> Option<Witness> {
    let prev = LAST_WITNESS.with(|m| m.borrow().get(&ladder_key(cfg)).cloned())?;
    let n = cfg.bands.len();
    // The active band moves by one as a swap crosses an edge, so the last
    // band and its neighbours cover the overwhelmingly common case.
    let mut bands = vec![prev.k];
    if prev.k + 1 < n {
        bands.push(prev.k + 1);
    }
    if prev.k > 0 {
        bands.push(prev.k - 1);
    }
    for scan in [8u32, 64] {
        for &k in &bands {
            if let Some(w) = find_witness_in_band_from(cfg, a, b, k, scan, Some(&prev.x)) {
                remember_witness(cfg, &w);
                return Some(w);
            }
        }
    }
    None
}

pub fn find_witness_scan(cfg: &BandedCLConfig, a: &BigInt, b: &BigInt, scan: u32) -> Option<Witness> {
    let n = cfg.bands.len();
    let Some(range) = candidate_bands(cfg, a, b) else {
        return (0..n).find_map(|k| find_witness_in_band(cfg, a, b, k, scan));
    };
    // The bounds that chose these bands also bound the counter from
    // BELOW, and a band search that starts from a lower bound costs a
    // short doubling instead of a blind one from 1. Measured on the
    // eight-band bell ladder: 50us against 2.9ms for the same band.
    //
    // A wrong hint is safe. `find_witness_in_band_from` tests it against
    // the sign and curve predicates first and falls back to the unseeded
    // search when it does not hold, and every candidate counter still has
    // to pass the whole band proof in `is_witness`.
    if let Some(w) = range.clone().find_map(|k| {
        let hint = counter_floor(cfg, a, b, k);
        find_witness_in_band_from(cfg, a, b, k, scan, hint.as_ref())
    }) {
        return Some(w);
    }
    // The counter bounds are a hint, not a proof: they drop the rounding
    // slack in `ca`, `cb`, `a_sat` and `b_sat`. A miss costs the bands the
    // hint excluded, which is what an unpruned search pays anyway.
    (0..n)
        .filter(|k| !range.contains(k))
        .find_map(|k| find_witness_in_band(cfg, a, b, k, scan))
}

// ── Narrowing the band search ───────────────────────────────────────────
//
// `find_witness_scan` ran a doubling search, a bisection and a scan of 801
// counters for EVERY band, and all but one or two of those bands cannot
// hold the witness at all. On preview that search was 79% of the whole
// scooper's CPU.
//
// The reserves bound the counter from both sides, per band. Write
// `S_a_gt(k)` for the index entry that sums `coeff_a` over the bands above
// `k`, and `S_a_ge(k)` for the same sum including `k`:
//
//   P2 (`a >= C_A`)   gives   X <= a * 2^64 / S_a_gt(k)
//   P4 (`ra <= a_sat`) gives  X >= (a - 2) * 2^64 / S_a_ge(k)
//
// and the B side gives the mirror pair from P3 and P5. So each band admits
// the counter only inside one interval. `S_a_*` falls as `k` rises and
// `S_b_*` rises, so the A interval moves right with `k` and the B interval
// moves left. They overlap on one contiguous range of bands, and two
// binary searches find its ends.
//
// The `- 2` is the rounding slack, and it is why the bound is necessary
// rather than approximate. `C_A` is one `ceil_div`, worth up to 1 over the
// exact product. `a_sat_k = ceil(w*D*X / (W*p*p))` is a second, and the
// index coefficient it is compared against rounds up too, so it is worth
// another 1. `a <= S_a_ge(k) * X / 2^64 + 2` is therefore the tight
// inequality, and dropping the 2 is what let the search skip a band that
// really did hold a witness. `SLACK` carries a wider margin than the
// derivation needs; it costs at most a band or two of extra search.
//
// WITHOUT the slack this is not a pruning but a guess. The first version
// of this code omitted it, and on a two-band ladder the pruned search
// returned the witness of band 1 where the unpruned search returned
// band 0 — both valid, but a different counter, and so a different fee
// budget. `pruning_never_changes_the_witness` is the test that caught it.
//
// The comparisons are cross-multiplied, so no division enters the
// decision. The caller still falls back to the excluded bands when the
// pruned range yields nothing.
const SLACK: i64 = 4;

/// The bands whose counter interval overlaps, as a contiguous range.
/// `None` when the ladder index is not monotone, which means the caller
/// must search every band: the binary searches below assume monotonicity
/// and a malformed config would make them drop the right band.
fn candidate_bands(
    cfg: &BandedCLConfig,
    a: &BigInt,
    b: &BigInt,
) -> Option<std::ops::Range<usize>> {
    let n = cfg.bands.len();
    if n == 0 || cfg.index.len() != n {
        return None;
    }
    // `check_shape` guarantees this for a config the chain accepted. This
    // is cheap next to one band search, and `find_witness` is reachable
    // with a config nobody checked.
    for k in 1..n {
        if cfg.index[k].0 > cfg.index[k - 1].0 || cfg.index[k].1 < cfg.index[k - 1].1 {
            return None;
        }
    }
    let scale = fixed_one();
    let s_a_gt = |k: usize| cfg.index[k].0.clone();
    let s_a_ge = |k: usize| &cfg.index[k].0 + &coeff_a(cfg, k, &scale);
    let s_b_lt = |k: usize| cfg.index[k].1.clone();
    let s_b_le = |k: usize| &cfg.index[k].1 + &coeff_b(cfg, k, &scale);

    let slack = BigInt::from(SLACK);
    let a_slack = a - &slack;
    let b_slack = b - &slack;
    // The A interval starts at or below where the B interval ends. True on
    // a prefix of the bands, so the first failure is the range's end.
    let a_lo_under_b_hi = |k: usize| &a_slack * &s_b_lt(k) <= b * &s_a_ge(k);
    // The B interval starts at or below where the A interval ends. True on
    // a suffix, so the first success is the range's start.
    let b_lo_under_a_hi = |k: usize| &b_slack * &s_a_gt(k) <= a * &s_b_le(k);

    let lo = first_true(n, b_lo_under_a_hi);
    let hi = first_false(n, a_lo_under_b_hi);
    if lo >= hi {
        None
    } else {
        Some(lo..hi)
    }
}

/// A lower bound on band `k`'s ladder counter, from the same P4 and P5
/// inequalities `candidate_bands` uses:
///
///     X >= (a - SLACK) * 2^64 / S_a_ge(k)
///     X >= (b - SLACK) * 2^64 / S_b_le(k)
///
/// The larger of the two, and `None` when neither side gives a positive
/// one. This is a search start, not a claim about the witness: the band
/// search validates it and falls back when it does not hold.
fn counter_floor(cfg: &BandedCLConfig, a: &BigInt, b: &BigInt, k: usize) -> Option<BigInt> {
    let (ca_coeff, cb_coeff) = cfg.index.get(k)?;
    let scale = fixed_one();
    let slack = BigInt::from(SLACK);
    let floor_of = |num: &BigInt, den: &BigInt| -> Option<BigInt> {
        if !den.is_positive() {
            return None;
        }
        let n = num - &slack;
        if !n.is_positive() {
            return None;
        }
        Some(&(&n * &scale) / den)
    };
    let s_a_ge = ca_coeff + &coeff_a(cfg, k, &scale);
    let s_b_le = cb_coeff + &coeff_b(cfg, k, &scale);
    let from_a = floor_of(a, &s_a_ge);
    let from_b = floor_of(b, &s_b_le);
    let best = match (from_a, from_b) {
        (Some(x), Some(y)) => Some(if x > y { x } else { y }),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    };
    // P1: the counter is never below the total weight.
    best.map(|x| if x < cfg.weight_total { cfg.weight_total.clone() } else { x })
}

/// The least `k` in `0..n` where `pred` holds, for a `pred` that is false
/// then true. `n` when it never holds.
fn first_true(n: usize, pred: impl Fn(usize) -> bool) -> usize {
    let (mut lo, mut hi) = (0usize, n);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if pred(mid) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

/// The least `k` in `0..n` where `pred` fails, for a `pred` that is true
/// then false. `n` when it holds everywhere.
fn first_false(n: usize, pred: impl Fn(usize) -> bool) -> usize {
    first_true(n, |k| !pred(k))
}

/// `find_witness_scan` with the band pruning removed: every band, in
/// order. The reference the pruned search is tested against.
#[cfg(test)]
pub fn find_witness_scan_every_band(
    cfg: &BandedCLConfig,
    a: &BigInt,
    b: &BigInt,
    scan: u32,
) -> Option<Witness> {
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
            // `lo` is the largest x with non-negative residuals and G >= 0
            // (P2, P3, P6), and a witness is such an x whose successor
            // fails G (P7): the witness is `lo` itself unless P4/P5 push
            // it lower, and nothing above `lo` can be one. Scan DOWN from
            // `lo`. The scan used to start at `lo + scan` and walk down,
            // which was 400 failed band proofs per band before the hit —
            // most of a fill-curve build, and of every planner step that
            // crosses a band. The window above `lo` is kept as a backstop
            // in case the predicate is not monotone for some state.
            let sp = BigInt::from(scan as i64);
            let from = if &lo > &sp { &lo - &sp } else { one.clone() };
            let to = &lo + &sp;
            let mut x = lo.clone();
            while x >= from {
                if is_witness(cfg, a, b, &x, k) {
                    return Some(Witness { x, k });
                }
                x = &x - &one;
            }
            let mut x = &lo + &one;
            while x <= to {
                if is_witness(cfg, a, b, &x, k) {
                    return Some(Witness { x, k });
                }
                x = &x + &one;
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
pub fn next_band(k: usize, is_a_input: bool, n: usize) -> Option<usize> {
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
                    "swap of {dx} cannot be completed: band {} is the ladder's last in this \
                     direction and {remaining} of input would pay no output there",
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

/// `2^64`, the bound on every rational component (V9). The same number the
/// ladder index is scaled by.
fn rational_bound() -> BigInt {
    fixed_one()
}

fn check_fee(f: &Rational, what: &str) -> Result<(), String> {
    let cap = rational_bound();
    if f.num.is_negative() || !f.den.is_positive() || f.num >= f.den || f.num > cap || f.den > cap {
        return Err(format!("{what} {}/{} must satisfy 0 <= num < den <= 2^64", f.num, f.den));
    }
    Ok(())
}

/// V1 to V9 and V13 of the spec: the Create-time shape checks
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
    // V13: the index is exactly the cumulative coefficients of these bands.
    // A spend reads one entry of it instead of walking, so a wrong entry is
    // a wrong residual on every step of the pool's life.
    let expected = build_index(cfg);
    if cfg.index != expected {
        return Err(format!(
            "ladder index does not match the bands ({} entries, expected {})",
            cfg.index.len(),
            expected.len()
        ));
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
        with_index(
            (0..8).map(|i| band(1_000_000 + 10_000 * i, 1_000_000, 0, (3, 1000))).collect(),
            r(1_080_000, 1_000_000),
            BigInt::from(8),
        )
    }

    const RESERVE_A: i64 = 8_637_368;
    const RESERVE_B: i64 = 624_999;

    #[test]
    fn witness_matches_the_aiken_vector() {
        let cfg = eq8();
        let w = find_witness(&cfg, &BigInt::from(RESERVE_A), &BigInt::from(RESERVE_B)).unwrap();
        assert_eq!(w.x, BigInt::from(999_999_813i64));
        assert_eq!(w.k, 0);
        let st = ladder_at(&cfg, &w.x, w.k).unwrap();
        assert_eq!(st.ca, BigInt::from(8_021_634i64));
        assert_eq!(st.cb, BigInt::from(0));
        assert_eq!(st.l_k, BigInt::from(124_999_976i64));
        assert_eq!(st.a_sat_k, BigInt::from(1_237_624i64));
        assert!(band_proof(&BigInt::from(RESERVE_A), &BigInt::from(RESERVE_B), &st, true));
        // The far band at the counter a dishonest scooper would name.
        assert!(!is_witness(&cfg, &BigInt::from(RESERVE_A), &BigInt::from(RESERVE_B), &BigInt::from(71_428_457i64), 7));
    }

    /// `banded_swap_accepts_the_true_witness`: 624 B in, 616 A out, counter
    /// 999_999_813 -> 999_999_938, fee_budget 125 at total_lp 1e9.
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
        assert_eq!(s.before.x, BigInt::from(999_999_813i64));
        assert_eq!(s.a_after, BigInt::from(8_636_752i64));
        assert_eq!(s.b_after, BigInt::from(625_623i64));
        assert_eq!(s.after.x, BigInt::from(999_999_938i64));
        assert_eq!(s.after.k, 0);
        assert_eq!(s.fee_budget, BigInt::from(125));
    }

    /// The proportional deposit of `banded_walk_accepts_swap_then_deposit`:
    /// doubling the pool lands on counter 2_000_000_000.
    #[test]
    fn doubled_pool_has_the_aiken_counter() {
        let cfg = eq8();
        let w = find_witness(&cfg, &BigInt::from(17_273_504i64), &BigInt::from(1_251_246i64)).unwrap();
        assert_eq!(w.x, BigInt::from(2_000_000_000i64));
        assert_eq!(w.k, 0);
    }

    /// `lib/tests/unit/banded_cl_cs.ak`: one constant-sum bin over sqrt-price
    /// 2..3 prices at 6/1. Buying A with 1e6 B pays 166_166 A; selling 1e6 A
    /// pays 5_982_000 B. Fee budgets 3 and 18 at total_lp 1e6.
    #[test]
    fn cs_bin_matches_the_aiken_vectors() {
        let cfg = with_index(vec![band(2, 1, 1, (3, 1000))], r(3, 1), BigInt::from(1));
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
        let cfg = with_index(
            vec![band(1, 1, 0, (3, 1000)), band(2, 1, 1, (3, 1000)), band(3, 1, 0, (3, 1000))],
            r(4, 1),
            BigInt::from(3),
        );
        let w = find_witness(&cfg, &BigInt::from(41_666_666i64), &BigInt::from(583_333_339i64)).unwrap();
        assert_eq!(w.k, 1);
        assert_eq!(w.x, BigInt::from(1_000_000_001i64));
        assert!(check_shape(&cfg).is_ok());
    }

    /// V13: a config whose index does not match its bands is refused before
    /// it can reach a transaction. The chain rebuilds the index at `Create`
    /// and compares, so a wrong one bricks a pool for its whole life.
    #[test]
    fn check_shape_rejects_a_tampered_index() {
        let mut cfg = eq8();
        assert!(check_shape(&cfg).is_ok());
        // Halve one coefficient: that understates `C_A`, which inflates the
        // residual the active band pays out of -- the extracting direction.
        cfg.index[0].0 = &cfg.index[0].0 / &BigInt::from(2);
        let err = check_shape(&cfg).unwrap_err();
        assert!(err.contains("ladder index"), "{err}");

        let mut short = eq8();
        short.index.pop();
        assert!(check_shape(&short).is_err());
    }

    /// The direction argument of spec section 2.2, checked rather than
    /// asserted: the index form of the prefix sums never lands BELOW the
    /// exact sum, which is what keeps the residual from inflating, and it
    /// overshoots by at most one base unit per band plus one.
    ///
    /// It is NOT always at or below the per-band sum of ceilings. Each
    /// coefficient is itself rounded up, so at small counters the
    /// accumulated coefficient rounding can exceed what the per-band
    /// ceilings add: at `x = 1_000` on this ladder the index gives
    /// `cb = 1_251` where the per-band sum gives `1_250`. Both are above
    /// the exact value, so both are safe; the bounds are what differ, and
    /// the index's bound is the tighter one at any realistic counter.
    #[test]
    fn the_index_rounds_up_and_stays_within_its_bound() {
        let cfg = eq8();
        let n = BigInt::from(cfg.bands.len() as i64);
        for x in [1_000i64, 1_000_000, 999_999_813, 4_000_000_000] {
            let x = BigInt::from(x);
            for k in 0..cfg.bands.len() {
                let st = ladder_at(&cfg, &x, k).unwrap();
                // The truncating sum is at or below the exact value, and
                // the exact value is at or below it plus one per band.
                let mut floor_a = BigInt::zero();
                let mut floor_b = BigInt::zero();
                for i in 0..cfg.bands.len() {
                    let lo = &cfg.bands[i].start;
                    let hi = upper_edge(&cfg, i);
                    let num = &(&cfg.bands[i].weight * &delta(lo, hi)) * &x;
                    if i > k {
                        floor_a = &floor_a + &(&num / &(&(&cfg.weight_total * &lo.num) * &hi.num));
                    } else if i < k {
                        floor_b = &floor_b + &(&num / &(&(&cfg.weight_total * &lo.den) * &hi.den));
                    }
                }
                let one = BigInt::from(1);
                assert!(st.ca >= floor_a, "ca {} below the exact sum", st.ca);
                assert!(st.cb >= floor_b, "cb {} below the exact sum", st.cb);
                assert!(st.ca <= &(&floor_a + &n) + &one, "ca {} over its bound", st.ca);
                assert!(st.cb <= &(&floor_b + &n) + &one, "cb {} over its bound", st.cb);
            }
        }
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
        assert_eq!(steps[0].after.x, BigInt::from(999_999_938i64));
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

    /// Prints the vectors the Aiken adversarial tests use for the flow
    /// fixture (4 bands 1.00..1.04 at 1e6, counter 1e9, band 2).
    #[test]
    fn flow_fixture_vectors() {
        let r = |n: i64, d: i64| Rational { num: BigInt::from(n), den: BigInt::from(d) };
        let cfg = with_index(
            [1_000_000i64, 1_010_000, 1_020_000, 1_030_000]
                .iter()
                .map(|&n| BandSpec {
                    start: r(n, 1_000_000),
                    weight: BigInt::from(1),
                    curve: BigInt::from(0),
                    fee_buy: r(3, 1000),
                    fee_sell: r(3, 1000),
                })
                .collect(),
            r(1_040_000, 1_000_000),
            BigInt::from(4),
        );
        let (a, b) = (BigInt::from(4_689_396i64), BigInt::from(5_025_000i64));
        let w = find_witness(&cfg, &a, &b).unwrap();
        let v = band_view(&cfg, &a, &b, &w).unwrap();
        eprintln!("witness x={} k={} ra={} rb={} l={}", w.x, w.k, v.ra, v.rb, v.l);
        // B-input: the step takes A out of band 2's residual `ra`.
        let cap = max_dx_in_band(&v, false);
        let dy_cap = band_output(&v, false, &cap);
        let over = &cap + &BigInt::from(1);
        let dy_over = band_output(&v, false, &over);
        eprintln!("B-in cap dx={cap} dy={dy_cap}; over dx={over} dy={dy_over} (ra={})", v.ra);
        let capped = swap_step(&cfg, &a, &b, &BigInt::from(1_000_000i64), false, &cap).unwrap();
        eprintln!("cap step: dy={} after x={} k={} fb={}", capped.dy, capped.after.x, capped.after.k, capped.fee_budget);
        eprintln!("over as plan: {:?}", swap_steps(&cfg, &a, &b, false, &over).map(|s| s.len()));
        let one = swap_step(&cfg, &a, &b, &BigInt::from(1_000_000i64), false, &BigInt::from(5_000)).unwrap();
        eprintln!("flow vector: dy={} after x={} k={} fb={}", one.dy, one.after.x, one.after.k, one.fee_budget);
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

#[cfg(test)]
mod witness_cost {
    //! How long a witness search takes on the ladders the creamery
    //! provisions. Ignored by default; run with --ignored --nocapture.
    use super::*;
    use crate::sundaev4::types::BandSpec;

    fn ladder(n: i64, lo: i64, hi: i64, den: i64, weights: &[i64]) -> BandedCLConfig {
        let r = |num: i64, d: i64| Rational { num: BigInt::from(num), den: BigInt::from(d) };
        let step = (hi - lo) / n;
        let w_total: i64 = weights.iter().sum();
        with_index(
            (0..n)
                .map(|i| BandSpec {
                    start: r(lo + step * i, den),
                    weight: BigInt::from(weights[i as usize]),
                    curve: BigInt::from(0),
                    fee_buy: r(3, 1000),
                    fee_sell: r(3, 1000),
                })
                .collect(),
            r(hi, den),
            BigInt::from(w_total),
        )
    }

    #[test]
    #[ignore]
    fn time_find_witness() {
        // The creamery's shapes, at the counters it actually used.
        let cases: Vec<(&str, BandedCLConfig, BigInt)> = vec![
            (
                "wide 16 bands flat, X=1.45e12",
                ladder(16, 938_083_152, 1_058_300_524, 1_000_000_000, &[1; 16]),
                BigInt::from(1_452_199_123_434u64),
            ),
            (
                "tight 8 bands bell, X=8.03e13",
                ladder(8, 997_997_996, 1_001_998_004, 1_000_000_000,
                       &[2, 9, 40, 87, 87, 40, 9, 2]),
                BigInt::from(80_279_049_977_900u64),
            ),
        ];
        for (label, cfg, x) in cases {
            // Reserves at the ladder's midpoint: witness in a middle band,
            // which is the normal case and the one the router hits.
            let k = cfg.bands.len() / 2;
            let st = ladder_at(&cfg, &x, k).expect("state");
            let a = &st.ca + &st.a_sat_k;
            let b = &st.cb + &BigInt::from(0);
            clear_witness_seeds();
            let t = std::time::Instant::now();
            let w_cold = find_witness(&cfg, &a, &b);
            let dt = t.elapsed();
            println!(
                "{label}: find_witness -> {:?} in {:?} (cold)",
                w_cold.as_ref().map(|w| w.k),
                dt
            );

            let n = 20;
            let t = std::time::Instant::now();
            for _ in 0..n {
                clear_witness_seeds();
                let _ = find_witness(&cfg, &a, &b);
            }
            // `find_witness` prunes the band set, so this is the cold
            // PRUNED path, not a scan of every band. The PRUNE line below
            // is the one that compares the two.
            println!("  cold (no seed, pruned): {:?}", t.elapsed() / n);

            for scan in [400u32, 50, 8] {
                let t = std::time::Instant::now();
                for _ in 0..n {
                    let _ = find_witness_scan(&cfg, &a, &b, scan);
                }
                println!("    scan={scan:>3}: {:?}", t.elapsed() / n);
            }
            // One band, scanned wide. `k` is the ladder's midpoint, which
            // is NOT always the band holding the witness, so this is the
            // cost of searching ONE band whether it holds one or not.
            let holder = w_cold.as_ref().map(|w| w.k);
            let t = std::time::Instant::now();
            for _ in 0..n {
                let _ = find_witness_in_band(&cfg, &a, &b, k, 400);
            }
            println!(
                "    one band (k={k}, witness in {:?}), scan=400: {:?}",
                holder,
                t.elapsed() / n
            );
            // Seeded at the true counter, as a chained step would be.
            let t = std::time::Instant::now();
            for _ in 0..n {
                let _ = find_witness_in_band_from(&cfg, &a, &b, k, 8, Some(&x));
            }
            println!("    seeded at the known counter: {:?}", t.elapsed() / n);
            // What the router actually sees now: repeated calls on the same
            // ladder at nearby states, which is the shape of a split search.
            let _ = find_witness(&cfg, &a, &b);
            let t = std::time::Instant::now();
            let reps = 200;
            for i in 0..reps {
                let aa = &a + &BigInt::from(i as i64 * 7);
                let _ = find_witness(&cfg, &aa, &b);
            }
            println!("    WARM find_witness, nearby states: {:?}", t.elapsed() / reps);

            // ── What the two optimisations buy, separately ──────────────
            //
            // 1. Band pruning, on the cold path: pruned against every band.
            let kept = candidate_bands(&cfg, &a, &b).map(|r| r.len()).unwrap_or(cfg.bands.len());
            let t = std::time::Instant::now();
            for _ in 0..n {
                let _ = find_witness_scan_every_band(&cfg, &a, &b, 400);
            }
            let every = t.elapsed() / n;
            let t = std::time::Instant::now();
            for _ in 0..n {
                let _ = find_witness_scan(&cfg, &a, &b, 400);
            }
            let pruned = t.elapsed() / n;
            println!(
                "    PRUNE: {} of {} bands kept, {:?} -> {:?} ({:.1}x)",
                kept,
                cfg.bands.len(),
                every,
                pruned,
                every.as_secs_f64() / pruned.as_secs_f64().max(1e-12)
            );

            // 2. The memo, on the call pattern the router produces: one
            // state asked for repeatedly, which is what the splitter and
            // `compute_fee_budget` do.
            clear_witness_seeds();
            let _ = find_witness(&cfg, &a, &b);
            let reps = 2000;
            let t = std::time::Instant::now();
            for _ in 0..reps {
                let _ = find_witness(&cfg, &a, &b);
            }
            let memo = t.elapsed() / reps;
            let hits = drain_witness_memo_hits();
            // `hits` can exceed `reps`: the drain also collects hits from
            // the calls above that primed the memo.
            println!("    MEMO: repeat call {:?}, {hits} hits over {reps} repeats", memo);
        }
    }
}


#[cfg(test)]
mod witness_pruning_tests {
    //! The band pruning and the memo are both pure-function optimisations.
    //! Neither may change an answer. These compare them against the
    //! unpruned, unmemoised search.
    use super::*;
    use crate::sundaev4::types::BandSpec;

    fn r(num: i64, den: i64) -> Rational {
        Rational { num: BigInt::from(num), den: BigInt::from(den) }
    }

    /// An `n`-band ladder over `[lo, hi]/den` with the given weights and
    /// curves. Mirrors the creamery's archetypes.
    fn ladder(n: usize, lo: i64, hi: i64, den: i64, weights: &[i64], cs_at: Option<usize>) -> BandedCLConfig {
        let step = (hi - lo) / n as i64;
        let w_total: i64 = weights.iter().take(n).sum();
        with_index(
            (0..n)
                .map(|i| BandSpec {
                    start: r(lo + step * i as i64, den),
                    weight: BigInt::from(weights[i]),
                    curve: BigInt::from(if cs_at == Some(i) { 1 } else { 0 }),
                    fee_buy: r(3, 1000),
                    fee_sell: r(3, 1000),
                })
                .collect(),
            r(lo + step * n as i64, den),
            BigInt::from(w_total),
        )
    }

    /// The ladders the creamery provisions, by archetype.
    fn archetypes() -> Vec<(&'static str, BandedCLConfig)> {
        vec![
            ("tight 8 bell", ladder(8, 996_000_000, 1_004_000_000, 1_000_000_000, &[2, 9, 40, 87, 87, 40, 9, 2], None)),
            ("core 12 bell", ladder(12, 980_000_000, 1_020_000_000, 1_000_000_000, &[2, 5, 12, 28, 60, 96, 96, 60, 28, 12, 5, 2], None)),
            ("wide 16 flat", ladder(16, 880_000_000, 1_120_000_000, 1_000_000_000, &[1; 16], None)),
            ("tail 8 skewed", ladder(8, 550_000_000, 1_450_000_000, 1_000_000_000, &[96, 60, 28, 12, 5, 3, 2, 1], None)),
            ("split 12 bimodal", ladder(12, 920_000_000, 1_080_000_000, 1_000_000_000, &[40, 87, 40, 9, 2, 1, 1, 2, 9, 40, 87, 40], None)),
            ("pegged 10 with a cs bin", ladder(10, 990_000_000, 1_010_000_000, 1_000_000_000, &[4, 12, 30, 70, 96, 96, 70, 30, 12, 4], Some(4))),
            ("two bands", ladder(2, 900_000_000, 1_100_000_000, 1_000_000_000, &[1, 1], None)),
            ("one band", ladder(1, 950_000_000, 1_050_000_000, 1_000_000_000, &[1], None)),
        ]
    }

    /// Reserves that lie ON the ladder: pick a counter and a band, then
    /// read the saturated state back out. A state built this way has a
    /// witness, so a search that returns `None` for it is a defect.
    fn on_ladder(cfg: &BandedCLConfig, x: &BigInt, k: usize) -> Option<(BigInt, BigInt)> {
        let st = ladder_at(cfg, x, k).ok()?;
        // Mid-band: half of each side's saturation on top of the prefix.
        let two = BigInt::from(2);
        let a = &st.ca + &(&st.a_sat_k / &two);
        let b = &st.cb + &(&st.b_sat_k / &two);
        Some((a, b))
    }

    /// Compare the pruned search against the unpruned one over a sweep of
    /// ladders, counters, bands and small reserve offsets. The unpruned
    /// side costs 90-160ms per state in a debug build, so the always-run
    /// test takes a slice and the ignored one takes the lot.
    fn sweep(archs: &[(&str, BandedCLConfig)], counters: &[u64], deltas: &[(i64, i64)], band_step: usize) {
        let mut checked = 0usize;
        let mut pruned_none = 0usize;
        for (name, cfg) in archs {
            let n = cfg.bands.len();
            for cx in counters {
                let x = BigInt::from(*cx);
                if x < cfg.weight_total {
                    continue;
                }
                for k in (0..n).step_by(band_step) {
                    let Some((a, b)) = on_ladder(cfg, &x, k) else { continue };
                    // On-ladder states with a small offset, then states
                    // off the ladder. The pruned search must agree on
                    // both: a state with no witness is what sends it down
                    // the fallback path.
                    //
                    // Scaling BOTH reserves by the same factor does not
                    // leave the ladder, it only moves the counter, and
                    // scaling one of them moves the band. To leave the
                    // ladder the price has to go outside its range: P2
                    // bounds the counter above by `a`, so a small `a` with
                    // a large `b` asks for more B than any admissible
                    // counter can hold.
                    let mut probes: Vec<(BigInt, BigInt)> = deltas
                        .iter()
                        .map(|(da, db)| (&a + &BigInt::from(*da), &b + &BigInt::from(*db)))
                        .collect();
                    for (da, mb) in [(1000i64, 1000i64), (7, 100_000), (100_000, 7)] {
                        probes.push((&a / &BigInt::from(da), &b * &BigInt::from(mb)));
                    }
                    for (aa, bb) in probes {
                        if aa.is_negative() || bb.is_negative() {
                            continue;
                        }
                        clear_witness_seeds();
                        let want = find_witness_scan_every_band(cfg, &aa, &bb, 400);
                        clear_witness_seeds();
                        let got = find_witness_scan(cfg, &aa, &bb, 400);
                        assert_eq!(
                            got, want,
                            "{name}: pruned search disagrees at band {k}, X={x}, a={aa}, b={bb}"
                        );
                        checked += 1;
                        if want.is_none() {
                            pruned_none += 1;
                        }
                    }
                }
            }
        }
        assert!(checked > 20, "only {checked} states compared");
        // Reserves with NO witness are what send the pruned search down
        // its fallback path, and this sweep does not reach them: a ladder
        // admits every A:B ratio, because band 0 unsaturated holds only A
        // and the top band saturated holds only B. Scaling both reserves
        // moves the counter, scaling one moves the band, and a one unit
        // offset is absorbed by a neighbouring counter.
        // `find_states_with_no_witness` searched 1,600 small states on the
        // eight-arc ladder and found none.
        //
        // So the fallback is a safety net that this suite does not
        // exercise, and the slack in `candidate_bands` carries the
        // correctness on its own. Treat a change to that derivation as
        // unguarded by tests.
        assert!(
            checked - pruned_none > 10,
            "too few witnessed states: {}",
            checked - pruned_none
        );
        println!("  compared {checked} states, {pruned_none} with no witness");
    }

    #[test]
    fn pruning_never_changes_the_witness() {
        // Every band of three shapes, at one counter, with the offsets
        // that straddle a band edge.
        let archs = archetypes();
        let slice: Vec<(&str, BandedCLConfig)> = archs
            .iter()
            .filter(|(n, _)| matches!(*n, "tight 8 bell" | "pegged 10 with a cs bin" | "two bands"))
            .cloned()
            .collect();
        sweep(&slice, &[80_279_049_977_900], &[(0, 0), (1, 0), (0, -1)], 1);
    }

    #[test]
    #[ignore = "90-160ms per state in debug; run with --ignored"]
    fn pruning_never_changes_the_witness_exhaustively() {
        sweep(
            &archetypes(),
            &[1_000_000_000, 80_279_049_977_900, 1_452_199_123_434, 7_000_000_000_000_000, 123_456_789],
            &[(0, 0), (1, 0), (0, 1), (-1, 0), (0, -1), (7, -5)],
            1,
        );
    }

    #[test]
    fn the_memo_returns_what_a_cold_search_returns() {
        for (name, cfg) in archetypes() {
            let x = BigInt::from(80_279_049_977_900u64);
            if x < cfg.weight_total {
                continue;
            }
            for k in 0..cfg.bands.len() {
                let Some((a, b)) = on_ladder(&cfg, &x, k) else { continue };
                clear_witness_seeds();
                let cold = find_witness(&cfg, &a, &b);
                // Second call must come from the memo, and agree.
                let before = drain_witness_memo_hits();
                let warm = find_witness(&cfg, &a, &b);
                let hits = drain_witness_memo_hits();
                assert_eq!(cold, warm, "{name}: memo disagrees with the cold search at band {k}");
                assert_eq!(before, 0, "{name}: unexpected memo hit before the repeat");
                assert_eq!(hits, 1, "{name}: repeat did not come from the memo");
            }
        }
    }

    #[test]
    fn the_memo_does_not_confuse_two_ladders() {
        // Two ladders that a cheap fingerprint could conflate: same band
        // count, same weight_total, same first edge, different shape.
        let left = ladder(8, 990_000_000, 1_010_000_000, 1_000_000_000, &[1, 1, 1, 1, 1, 1, 1, 1], None);
        let right = ladder(8, 990_000_000, 1_030_000_000, 1_000_000_000, &[1, 1, 1, 1, 1, 1, 1, 1], None);
        assert_eq!(left.bands.len(), right.bands.len());
        assert_eq!(left.weight_total, right.weight_total);
        assert_eq!(left.bands[0].start, right.bands[0].start);
        assert_ne!(left, right, "the two ladders must differ");

        let x = BigInt::from(80_279_049_977_900u64);
        for k in 0..8 {
            let (Some((la, lb)), Some((ra, rb))) = (on_ladder(&left, &x, k), on_ladder(&right, &x, k)) else {
                continue;
            };
            clear_witness_seeds();
            let l_alone = find_witness(&left, &la, &lb);
            clear_witness_seeds();
            let r_alone = find_witness(&right, &ra, &rb);

            // Interleaved, through one memo.
            clear_witness_seeds();
            let l_mixed = find_witness(&left, &la, &lb);
            let r_mixed = find_witness(&right, &ra, &rb);
            let l_again = find_witness(&left, &la, &lb);
            assert_eq!(l_alone, l_mixed, "left changed when right shared the memo (band {k})");
            assert_eq!(r_alone, r_mixed, "right changed when left shared the memo (band {k})");
            assert_eq!(l_alone, l_again, "left changed on a memo hit (band {k})");
        }
    }

    /// The eight equal arcs of `lib/tests/unit/banded_cl_check.ak`:
    /// sqrt-price 1.00 .. 1.08 in steps of 0.01, weight 1, fee 0.3%.
    fn eq8() -> BandedCLConfig {
        with_index(
            (0..8)
                .map(|i| BandSpec {
                    start: r(100 + i, 100),
                    weight: BigInt::from(1),
                    curve: BigInt::from(0),
                    fee_buy: r(3, 1000),
                    fee_sell: r(3, 1000),
                })
                .collect(),
            r(108, 100),
            BigInt::from(8),
        )
    }

    #[test]
    fn the_memo_freezes_one_of_two_witnesses_at_a_band_edge() {
        // A state ON a band edge satisfies the band proof in BOTH
        // neighbouring bands, at DIFFERENT counters. `find_witness`
        // already picks between them by what the seed cache holds, so the
        // answer was never a function of the reserves alone. The memo
        // freezes the first answer instead, which narrows the variance
        // but does not remove it.
        //
        // This is NOT a correctness claim about which band is right. Both
        // pass P1-P7 and the chain accepts either. It is here so that a
        // change in the choice shows up as a failing test rather than as
        // a fee budget that disagrees with the transcript.
        let cfg = eq8();
        let (a, b) = (BigInt::from(5_618_482), BigInt::from(3_750_000));

        let admitted: Vec<Witness> = (0..cfg.bands.len())
            .filter_map(|k| find_witness_in_band(&cfg, &a, &b, k, 400))
            .collect();
        assert_eq!(
            admitted.len(),
            2,
            "the fixture must sit on an edge; admitted {admitted:?}"
        );
        assert_ne!(
            admitted[0].x, admitted[1].x,
            "the two witnesses must differ in the counter, or there is nothing to freeze"
        );

        // Cold, the search takes the lower band.
        clear_witness_seeds();
        let cold = find_witness(&cfg, &a, &b).expect("a witness exists");
        assert_eq!(cold, admitted[0], "a cold search no longer takes the first band");

        // The memo returns that same answer, and does not search again.
        let _ = drain_witness_memo_hits();
        let again = find_witness(&cfg, &a, &b).expect("a witness exists");
        assert_eq!(again, cold, "the memo changed the answer");
        assert_eq!(drain_witness_memo_hits(), 1, "the repeat searched again");

        // Seeded into the upper band, a search WITHOUT the memo takes the
        // upper one: the pre-existing seed dependence, recorded.
        clear_witness_seeds();
        let upper = find_witness_in_band(&cfg, &a, &b, admitted[1].k, 8)
            .expect("the upper band admits a witness");
        assert_eq!(upper, admitted[1]);
        assert_ne!(upper, cold, "the two paths must reach different witnesses");
    }

    #[test]
    #[ignore = "a search for fixtures, not a check; run with --ignored"]
    fn find_states_with_no_witness() {
        // Which reserves lie on no point of a ladder? Scaling does not
        // leave the ladder (it moves the counter) and neither does a one
        // unit offset (a neighbouring counter absorbs it), so the sweep
        // needs real fixtures. This prints them.
        let cfg = eq8();
        let mut found = 0usize;
        for a in (1u64..120).step_by(3) {
            for b in (1u64..120).step_by(3) {
                clear_witness_seeds();
                let (aa, bb) = (BigInt::from(a), BigInt::from(b));
                if find_witness_scan_every_band(&cfg, &aa, &bb, 400).is_none() {
                    if found < 12 {
                        println!("  no witness: a={a} b={b}");
                    }
                    found += 1;
                }
            }
        }
        println!("  {found} of {} probed states have no witness", (120 / 3) * (120 / 3));
    }

    #[test]
    fn pruning_survives_a_malformed_index() {
        // A config whose index does not match its bands must fall back to
        // the full scan, not drop bands by a broken binary search.
        let good = ladder(8, 990_000_000, 1_010_000_000, 1_000_000_000, &[2, 9, 40, 87, 87, 40, 9, 2], None);
        let x = BigInt::from(80_279_049_977_900u64);
        let (a, b) = on_ladder(&good, &x, 4).expect("state on the ladder");

        let mut scrambled = good.clone();
        scrambled.index.reverse();
        assert!(candidate_bands(&scrambled, &a, &b).is_none(), "a non-monotone index must not be pruned");

        let mut truncated = good.clone();
        truncated.index.pop();
        assert!(candidate_bands(&truncated, &a, &b).is_none(), "a short index must not be pruned");
    }

    #[test]
    fn pruning_reports_how_many_bands_it_keeps() {
        // Not a correctness claim. This is the number the optimisation is
        // for, printed so a change in it is visible.
        let x = BigInt::from(80_279_049_977_900u64);
        for (name, cfg) in archetypes() {
            if x < cfg.weight_total {
                continue;
            }
            let n = cfg.bands.len();
            let mut kept = Vec::new();
            for k in 0..n {
                let Some((a, b)) = on_ladder(&cfg, &x, k) else { continue };
                kept.push(candidate_bands(&cfg, &a, &b).map(|r| r.len()).unwrap_or(n));
            }
            if kept.is_empty() {
                continue;
            }
            let worst = *kept.iter().max().unwrap();
            let total: usize = kept.iter().sum();
            println!(
                "  {name}: {n} bands -> mean {:.1}, worst {worst}",
                total as f64 / kept.len() as f64
            );
            assert!(worst <= n, "{name}: pruning kept more bands than exist");
        }
    }
}
