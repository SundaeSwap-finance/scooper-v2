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
}

impl BatchFailureReason {
    fn label(self) -> &'static str {
        match self {
            Self::RaceLost => "race_lost",
            Self::SubmitError => "submit_error",
            Self::BuildError => "build_error",
        }
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
    /// Process start instant — used to compute `scooper_uptime_seconds`
    /// so operators can spot crash loops without scraping systemd state.
    start_instant: Instant,
    in_flight_snapshot: std::sync::Mutex<InFlightSnapshot>,
    quarantine_snapshot: std::sync::Mutex<QuarantineSnapshot>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            batches_submitted: AtomicU64::new(0),
            orders_scooped: AtomicU64::new(0),
            in_flight_txs: AtomicU64::new(0),
            failed_race_lost: AtomicU64::new(0),
            failed_submit_error: AtomicU64::new(0),
            failed_build_error: AtomicU64::new(0),
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
        };
        counter.fetch_add(1, Ordering::Relaxed);
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
    ] {
        let _ = writeln!(out, "scooper_batches_failed_total{{reason=\"{reason}\"}} {value}");
    }

    let q = metrics.quarantine_snapshot();
    write_gauge(&mut out, "scooper_quarantined_permanent", "Number of permanently quarantined orders", q.permanent.len());
    write_gauge(&mut out, "scooper_quarantined_temporary", "Number of temporarily quarantined orders", q.temporary.len());

    // Uptime since process start. Dropping near zero unexpectedly is the
    // canonical crash-loop signal.
    let uptime = metrics.start_instant.elapsed().as_secs();
    write_gauge(&mut out, "scooper_uptime_seconds", "Seconds since this scooper process started", uptime);

    out
}
