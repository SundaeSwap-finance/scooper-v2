use std::collections::BTreeSet;
use std::fmt::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tokio::sync::Mutex;

use crate::scooper::QuarantineSnapshot;
use crate::sundaev3::{Ident, SundaeV3HistoricalState};
use crate::sundaev4::SundaeV4HistoricalState;

/// Reason a scoop batch failed to land. Used as a Prometheus label so
/// operators can triage `scooper_batches_failed_total` by cause without
/// grepping logs. Keep these names short and stable — they become part
/// of an external API.
#[derive(Copy, Clone, Debug)]
pub enum BatchFailureReason {
    /// `BadInputsUTxO` from the chain — another scooper got there first.
    RaceLost,
    /// Submit returned a non-BadInputsUTxO error (Blockfrost 4xx/5xx,
    /// validator failure, value-conservation, etc.).
    SubmitError,
    /// Local tx-build step failed before we even attempted submission.
    BuildError,
    /// Local UPLC eval failed (e.g. `ExplicitErrorTerm`). We treat this as a
    /// scooper bug — we built a tx that doesn't pass our own scripts. The
    /// cycle bails to avoid submitting txs we can't verify; the offending
    /// script context is dumped to /tmp for offline diagnosis.
    EvalError,
}

impl BatchFailureReason {
    fn label(self) -> &'static str {
        match self {
            Self::RaceLost => "race_lost",
            Self::SubmitError => "submit_error",
            Self::BuildError => "build_error",
            Self::EvalError => "eval_error",
        }
    }
}

/// Which pool family a counter sample is attributed to. Multi-pool
/// scoops increment each involved family's counter, so the sum of
/// `{pool_type=...}` samples can exceed the unlabeled tx count.
#[derive(Copy, Clone, Debug)]
pub enum PoolFamily {
    ConstantProduct,
    ConstantSum,
    ConcentratedLiquidity,
}

impl PoolFamily {
    fn label(self) -> &'static str {
        match self {
            Self::ConstantProduct => "cp",
            Self::ConstantSum => "cs",
            Self::ConcentratedLiquidity => "cl",
        }
    }
}

/// Lock-free histogram with fixed bucket boundaries, emitted in Prometheus
/// histogram exposition format. Each observation atomically increments the
/// bucket whose upper bound it falls in; at render time we cumulate.
///
/// Keep boundaries narrow and few — every bucket is a Prometheus time
/// series. Boundaries are upper bounds in seconds (matching Prometheus
/// convention; `+Inf` is appended implicitly).
pub struct Histogram {
    boundaries: &'static [f64],
    /// One bucket per boundary, plus one implicit `+Inf` bucket.
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    /// Sum of observations as microseconds. Tracked as integer to avoid
    /// float atomics; converted to seconds at render time.
    sum_micros: AtomicU64,
}

impl Histogram {
    pub fn new(boundaries: &'static [f64]) -> Self {
        let buckets = (0..=boundaries.len())
            .map(|_| AtomicU64::new(0))
            .collect();
        Self {
            boundaries,
            buckets,
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }

    /// Record an observation in seconds. Floors to microsecond precision in
    /// the sum (sufficient for latency histograms — Prometheus reports
    /// avg = sum/count which doesn't care about sub-µs precision).
    pub fn observe(&self, seconds: f64) {
        let bucket_idx = self
            .boundaries
            .iter()
            .position(|b| seconds <= *b)
            .unwrap_or(self.boundaries.len());
        self.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        let micros = (seconds * 1_000_000.0).max(0.0) as u64;
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
    }

    fn write(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        // Prometheus histograms expose cumulative counts: bucket{le="0.1"} is
        // the number of obs with value <= 0.1, INCLUDING those that fell in
        // smaller buckets. So accumulate as we walk.
        let mut cum: u64 = 0;
        for (i, b) in self.boundaries.iter().enumerate() {
            cum += self.buckets[i].load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{b}\"}} {cum}");
        }
        cum += self.buckets[self.boundaries.len()].load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cum}");
        let sum_secs = self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let _ = writeln!(out, "{name}_sum {sum_secs}");
        let _ = writeln!(out, "{name}_count {}", self.count.load(Ordering::Relaxed));
    }
}

/// Snapshot of in-flight transaction state, shared with the server for dashboard display.
#[derive(Clone, Default)]
pub struct InFlightSnapshot {
    pub pool_ids: Vec<String>,
    pub order_refs: Vec<String>,
}

/// Scooper-originated counters shared between the scooper task and the server.
pub struct Metrics {
    pub batches_submitted: AtomicU64,
    pub orders_scooped: AtomicU64,
    pub in_flight_txs: AtomicU64,
    // Per-reason failure buckets. Emitted as labeled samples on
    // `scooper_batches_failed_total{reason="..."}`. Use
    // `record_batch_failure(reason)` rather than touching these directly.
    failed_race_lost: AtomicU64,
    failed_submit_error: AtomicU64,
    failed_build_error: AtomicU64,
    failed_eval_error: AtomicU64,
    /// Per-pool-family counter of orders scooped. Incremented once per
    /// (scoop tx, distinct pool family) — i.e. a mixed-pool tx
    /// increments multiple families.
    scooped_cp: AtomicU64,
    scooped_cs: AtomicU64,
    scooped_cl: AtomicU64,
    /// Submit latency histogram in seconds. Buckets are tuned for
    /// Blockfrost: most submits land in 100ms-2s, tail past 5s is a
    /// sign of upstream trouble.
    pub submit_latency: Histogram,
    // Mempool monitor (phase 1, observation only). Counters of relevant
    // txs seen pre-block plus the mempool→block lead-time histogram — the
    // empirical basis for the chaining/batching work.
    pub mempool_txs_seen: AtomicU64,
    pub mempool_order_creates: AtomicU64,
    pub mempool_order_spends: AtomicU64,
    pub mempool_pool_spends: AtomicU64,
    pub mempool_confirmed: AtomicU64,
    pub mempool_evicted: AtomicU64,
    pub mempool_lead_time: Histogram,
    /// Process start instant — used to compute `scooper_uptime_seconds`
    /// so operators can spot crash loops without scraping systemd state.
    start_instant: Instant,
    in_flight_snapshot: std::sync::Mutex<InFlightSnapshot>,
    quarantine_snapshot: std::sync::Mutex<QuarantineSnapshot>,
}

const SUBMIT_LATENCY_BOUNDARIES: &[f64] = &[
    0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0,
];

/// Mempool lead time: how long before block inclusion the mempool showed us
/// a tx. Preview/mainnet blocks average 20s, so the interesting range is
/// 1-60s with a tail for txs that waited out several blocks.
const MEMPOOL_LEAD_BOUNDARIES: &[f64] = &[
    0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 40.0, 60.0, 120.0,
];

impl Metrics {
    pub fn new() -> Self {
        Self {
            batches_submitted: AtomicU64::new(0),
            orders_scooped: AtomicU64::new(0),
            in_flight_txs: AtomicU64::new(0),
            failed_race_lost: AtomicU64::new(0),
            failed_submit_error: AtomicU64::new(0),
            failed_build_error: AtomicU64::new(0),
            failed_eval_error: AtomicU64::new(0),
            scooped_cp: AtomicU64::new(0),
            scooped_cs: AtomicU64::new(0),
            scooped_cl: AtomicU64::new(0),
            submit_latency: Histogram::new(SUBMIT_LATENCY_BOUNDARIES),
            mempool_txs_seen: AtomicU64::new(0),
            mempool_order_creates: AtomicU64::new(0),
            mempool_order_spends: AtomicU64::new(0),
            mempool_pool_spends: AtomicU64::new(0),
            mempool_confirmed: AtomicU64::new(0),
            mempool_evicted: AtomicU64::new(0),
            mempool_lead_time: Histogram::new(MEMPOOL_LEAD_BOUNDARIES),
            start_instant: Instant::now(),
            in_flight_snapshot: std::sync::Mutex::new(InFlightSnapshot::default()),
            quarantine_snapshot: std::sync::Mutex::new(QuarantineSnapshot::default()),
        }
    }

    /// Record a batch failure for the given reason. Replaces direct access
    /// to the now-private per-reason atomics.
    pub fn record_batch_failure(&self, reason: BatchFailureReason) {
        let counter = match reason {
            BatchFailureReason::RaceLost => &self.failed_race_lost,
            BatchFailureReason::SubmitError => &self.failed_submit_error,
            BatchFailureReason::BuildError => &self.failed_build_error,
            BatchFailureReason::EvalError => &self.failed_eval_error,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Record `n` orders scooped against a particular pool family. Callers
    /// should bucket their batch's orders by family and call once per
    /// family with the per-family count.
    pub fn record_pool_family_orders(&self, family: PoolFamily, n: u64) {
        let counter = match family {
            PoolFamily::ConstantProduct => &self.scooped_cp,
            PoolFamily::ConstantSum => &self.scooped_cs,
            PoolFamily::ConcentratedLiquidity => &self.scooped_cl,
        };
        counter.fetch_add(n, Ordering::Relaxed);
    }

    /// Update the in-flight snapshot from current chain tracker state.
    pub fn update_in_flight(&self, pools: &[Ident], order_inputs: &BTreeSet<crate::cardano_types::TransactionInput>, tx_count: usize) {
        let snapshot = InFlightSnapshot {
            pool_ids: pools.iter().map(|id| hex::encode(id.to_bytes())).collect(),
            order_refs: order_inputs.iter().map(|i| i.to_string()).collect(),
        };
        self.in_flight_txs.store(tx_count as u64, Ordering::Relaxed);
        *self.in_flight_snapshot.lock().unwrap() = snapshot;
    }

    pub fn in_flight_snapshot(&self) -> InFlightSnapshot {
        self.in_flight_snapshot.lock().unwrap().clone()
    }

    /// Update the quarantine snapshot from the scooper's current state.
    pub fn update_quarantine(&self, snapshot: QuarantineSnapshot) {
        *self.quarantine_snapshot.lock().unwrap() = snapshot;
    }

    pub fn quarantine_snapshot(&self) -> QuarantineSnapshot {
        self.quarantine_snapshot.lock().unwrap().clone()
    }
}

fn write_gauge(out: &mut String, name: &str, help: &str, value: impl std::fmt::Display) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

fn write_counter(out: &mut String, name: &str, help: &str, value: impl std::fmt::Display) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} counter");
    let _ = writeln!(out, "{name} {value}");
}

pub async fn render_metrics(
    v3_state: &Option<Arc<Mutex<SundaeV3HistoricalState>>>,
    v4_state: &Option<Arc<Mutex<SundaeV4HistoricalState>>>,
    paused: &std::sync::atomic::AtomicBool,
    metrics: &Metrics,
) -> String {
    let mut out = String::with_capacity(2048);

    write_gauge(&mut out, "scooper_paused", "Whether the scooper is paused (0 or 1)", paused.load(Ordering::Relaxed) as u8);

    // V4 snapshot gauges
    if let Some(v4) = v4_state {
        let state = v4.lock().await.latest().into_owned();

        write_gauge(&mut out, "scooper_v4_tip_slot", "Last processed block slot", state.tip_slot);

        let network_tip = state.network_tip_slot.unwrap_or(0);
        write_gauge(&mut out, "scooper_v4_network_tip_slot", "Network tip slot from upstream node", network_tip);

        // sync_lag = chain tip − last processed. Cleaner alert target
        // than sync_pct (which loses precision at high tip values).
        // Zero or negative when caught up. We clamp to 0 so the gauge
        // type stays non-negative.
        let sync_lag = network_tip.saturating_sub(state.tip_slot);
        write_gauge(&mut out, "scooper_v4_sync_lag_slots", "Slots between our last processed block and the network tip", sync_lag);

        let sync_pct = if network_tip > 0 {
            state.tip_slot as f64 / network_tip as f64 * 100.0
        } else {
            0.0
        };
        write_gauge(&mut out, "scooper_v4_sync_pct", "Sync progress as percentage", format!("{sync_pct:.2}"));

        write_gauge(&mut out, "scooper_v4_pool_count", "Number of tracked V4 pools", state.pools.len());
        write_gauge(&mut out, "scooper_v4_order_count", "Number of pending V4 orders", state.orders.len());

        // Age of the oldest pending order, in seconds. Computed from
        // `order.slot` (first-seen slot at index time) against the
        // current chain tip. Lets ops alert on "we have pending work
        // that's been sitting around" without waiting for a 5-min rate
        // window — useful when a single stuck order is the symptom of
        // an upstream bug. Zero when no orders pending.
        let oldest_slot = state.orders.iter().map(|o| o.slot).min();
        let tip_for_age = if state.tip_slot > 0 { state.tip_slot } else { network_tip };
        let oldest_age_secs = match oldest_slot {
            Some(s) if tip_for_age > s => tip_for_age.saturating_sub(s),
            _ => 0,
        };
        write_gauge(
            &mut out,
            "scooper_v4_oldest_pending_order_age_seconds",
            "Age (in chain seconds, ≈ slots on Cardano mainnet) of the oldest pending order. 0 when no orders pending.",
            oldest_age_secs,
        );

        let wallet_lovelace: u64 = state.wallet_utxos.values()
            .map(|v| {
                let ada = crate::cardano_types::AssetClass { policy: vec![], token: vec![] };
                v.get(&ada).to_f64().unwrap_or(0.0) as u64
            })
            .sum();
        write_gauge(&mut out, "scooper_v4_wallet_lovelace", "Total lovelace in scooper wallet", wallet_lovelace);
        write_gauge(&mut out, "scooper_v4_wallet_utxo_count", "Number of wallet UTxOs", state.wallet_utxos.len());

        // Scoop stats per scooper
        let our_keyhash = &state.scoop_stats.our_keyhash;
        if !state.scoop_stats.scooper_totals.is_empty() {
            let _ = writeln!(out, "# HELP scooper_scoop_txs Total scoop transactions by scooper");
            let _ = writeln!(out, "# TYPE scooper_scoop_txs gauge");
            for total in &state.scoop_stats.scooper_totals {
                let is_ours = if !our_keyhash.is_empty() && total.scooper == *our_keyhash { "true" } else { "false" };
                let _ = writeln!(
                    out,
                    "scooper_scoop_txs{{scooper=\"{}\",is_ours=\"{}\"}} {}",
                    total.scooper, is_ours, total.scoop_txs
                );
            }

            let _ = writeln!(out, "# HELP scooper_orders_processed Total orders processed by scooper");
            let _ = writeln!(out, "# TYPE scooper_orders_processed gauge");
            for total in &state.scoop_stats.scooper_totals {
                let is_ours = if !our_keyhash.is_empty() && total.scooper == *our_keyhash { "true" } else { "false" };
                let _ = writeln!(
                    out,
                    "scooper_orders_processed{{scooper=\"{}\",is_ours=\"{}\"}} {}",
                    total.scooper, is_ours, total.orders_processed
                );
            }
        }
    }

    // V3 snapshot gauges
    if let Some(v3) = v3_state {
        let state = v3.lock().await.latest().into_owned();
        write_gauge(&mut out, "scooper_v3_pool_count", "Number of tracked V3 pools", state.pools.len());
        write_gauge(&mut out, "scooper_v3_order_count", "Number of pending V3 orders", state.orders.len());
    }

    // Scooper-originated counters
    write_counter(&mut out, "scooper_batches_submitted_total", "Total batches successfully submitted", metrics.batches_submitted.load(Ordering::Relaxed));
    write_counter(&mut out, "scooper_orders_scooped_total", "Total orders successfully scooped", metrics.orders_scooped.load(Ordering::Relaxed));
    write_gauge(&mut out, "scooper_in_flight_txs", "Number of in-flight transactions in chain tracker", metrics.in_flight_txs.load(Ordering::Relaxed));

    // Failure breakdown: one counter, multiple reason labels. Lets
    // operators alert on the dominant failure mode rather than a single
    // opaque rate. `race_lost` is expected to be non-zero at steady
    // state; sustained growth of `submit_error` or `build_error` is a
    // bug signal.
    let _ = writeln!(out, "# HELP scooper_batches_failed_total Total batches that failed to submit, by reason");
    let _ = writeln!(out, "# TYPE scooper_batches_failed_total counter");
    for (reason, value) in [
        (BatchFailureReason::RaceLost.label(),     metrics.failed_race_lost.load(Ordering::Relaxed)),
        (BatchFailureReason::SubmitError.label(),  metrics.failed_submit_error.load(Ordering::Relaxed)),
        (BatchFailureReason::BuildError.label(),   metrics.failed_build_error.load(Ordering::Relaxed)),
        (BatchFailureReason::EvalError.label(),    metrics.failed_eval_error.load(Ordering::Relaxed)),
    ] {
        let _ = writeln!(out, "scooper_batches_failed_total{{reason=\"{reason}\"}} {value}");
    }

    let q = metrics.quarantine_snapshot();
    write_gauge(&mut out, "scooper_quarantined_permanent", "Number of permanently quarantined orders", q.permanent.len());
    write_gauge(&mut out, "scooper_quarantined_temporary", "Number of temporarily quarantined orders", q.temporary.len());

    // Per-pool-family scoop counts. Sum across labels can exceed the
    // unlabeled `orders_scooped_total` since a mixed-pool tx counts
    // against each family it touched.
    let _ = writeln!(out, "# HELP scooper_orders_scooped_by_pool_type_total Orders scooped, by pool family");
    let _ = writeln!(out, "# TYPE scooper_orders_scooped_by_pool_type_total counter");
    for (family, value) in [
        (PoolFamily::ConstantProduct.label(),       metrics.scooped_cp.load(Ordering::Relaxed)),
        (PoolFamily::ConstantSum.label(),           metrics.scooped_cs.load(Ordering::Relaxed)),
        (PoolFamily::ConcentratedLiquidity.label(), metrics.scooped_cl.load(Ordering::Relaxed)),
    ] {
        let _ = writeln!(out, "scooper_orders_scooped_by_pool_type_total{{pool_type=\"{family}\"}} {value}");
    }

    // Mempool monitor (phase 1). All zero when the monitor is disabled.
    write_counter(&mut out, "scooper_mempool_txs_seen_total", "Transactions observed in the local node mempool", metrics.mempool_txs_seen.load(Ordering::Relaxed));
    write_counter(&mut out, "scooper_mempool_order_creates_total", "Order-address outputs observed in mempool txs", metrics.mempool_order_creates.load(Ordering::Relaxed));
    write_counter(&mut out, "scooper_mempool_order_spends_total", "Known order UTxOs spent by mempool txs", metrics.mempool_order_spends.load(Ordering::Relaxed));
    write_counter(&mut out, "scooper_mempool_pool_spends_total", "Known pool UTxOs spent by mempool txs", metrics.mempool_pool_spends.load(Ordering::Relaxed));
    write_counter(&mut out, "scooper_mempool_confirmed_total", "Mempool-seen txs later confirmed in a block", metrics.mempool_confirmed.load(Ordering::Relaxed));
    write_counter(&mut out, "scooper_mempool_evicted_total", "Mempool-seen txs that vanished without confirming", metrics.mempool_evicted.load(Ordering::Relaxed));
    metrics.mempool_lead_time.write(
        &mut out,
        "scooper_mempool_lead_time_seconds",
        "How far ahead of block inclusion the mempool showed us a relevant tx",
    );

    // Submit latency histogram.
    metrics.submit_latency.write(
        &mut out,
        "scooper_submit_latency_seconds",
        "Time spent in the chain-submit call (Blockfrost or equivalent)",
    );

    // Uptime since process start. Dropping near zero unexpectedly is the
    // canonical crash-loop signal.
    let uptime = metrics.start_instant.elapsed().as_secs();
    write_gauge(&mut out, "scooper_uptime_seconds", "Seconds since this scooper process started", uptime);

    out
}
