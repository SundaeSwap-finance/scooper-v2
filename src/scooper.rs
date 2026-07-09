use std::{
    collections::BTreeMap,
    fs,
    io::{BufWriter, Write as _},
    path::PathBuf,
    sync::{Arc, atomic::{AtomicBool, Ordering}},
};

use anyhow::Result;
use serde::Serialize;
use tokio::{select, sync::Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use crate::{
    bigint::BigInt,
    cardano_types::TransactionInput,
    events::IndexEvent,
    metrics::Metrics,
    sundaev3::{
        Ident, PoolError, SingletonValue, SundaeV3HistoricalState, SundaeV3Order, SundaeV3Pool,
        ValueError, estimate_whether_in_range, validate_order_for_pool, validate_order_value,
    },
    sundaev4::{
        SundaeV4HistoricalState, ScooperExecution,
        accumulator::Accumulator,
        batch::{self, BatchLimits},
        chain_tracker::{ChainTracker, InFlightTx, PredictedPoolUtxo},
        router,
    },
};

/// How close (in slots) we need to be to the network tip to consider ourselves synced.
const SYNC_TOLERANCE_SLOTS: u64 = 10;

/// Log sync progress every N event batches to avoid log spam.
const SYNC_LOG_INTERVAL: u64 = 500;

/// Minimum ADA on the collateral return output (lovelace).
const MIN_COLLATERAL_RETURN: u64 = 1_500_000;

/// How many slots to temporarily quarantine orders after a BadInputsUTxO failure (~2 minutes).
const TEMP_QUARANTINE_SLOTS: u64 = 120;

/// Why an order is quarantined.
pub enum Quarantine {
    /// Structurally broken — single-order batch failed build/eval. Never retry.
    Permanent { reason: String },
    /// Likely already spent — retry after indexer catches up.
    Temporary { reason: String, until_slot: u64 },
}

pub struct Scooper {
    event_rx: tokio::sync::broadcast::Receiver<(u64, Vec<IndexEvent>)>,
    v3_state: Option<Arc<Mutex<SundaeV3HistoricalState>>>,
    v4_state: Option<Arc<Mutex<SundaeV4HistoricalState>>>,
    v4_execution: Option<ScooperExecution>,
    v4_language_views: Option<Vec<u8>>,
    v4_script_store: Option<crate::sundaev4::evaluator::ScriptStore>,
    v4_chain_tracker: ChainTracker,
    v4_batch_limits: BatchLimits,
    trace_directory: Option<PathBuf>,
    paused: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    /// Set to the tip slot when we lose a scoop race; skip batch cycles
    /// until the tip advances past this slot, giving the indexer time to
    /// process the competitor's block and remove spent UTxOs.
    backoff_until_after_slot: Option<u64>,
    /// Orders quarantined due to structural failure or suspected spent inputs.
    quarantine: BTreeMap<TransactionInput, Quarantine>,
    /// Posted strategy intents (shared with the admin server's ingest).
    v4_intents: Option<crate::sundaev4::intents::IntentServiceHandle>,
    /// Intent ids we've already logged a match for (log once, not per cycle).
    logged_intent_matches: std::collections::BTreeSet<Vec<u8>>,
}

impl Scooper {
    pub fn new(
        trace_directory: Option<PathBuf>,
        event_rx: tokio::sync::broadcast::Receiver<(u64, Vec<IndexEvent>)>,
        v3_state: Option<Arc<Mutex<SundaeV3HistoricalState>>>,
        v4_state: Option<Arc<Mutex<SundaeV4HistoricalState>>>,
        v4_execution: Option<ScooperExecution>,
        paused: Arc<AtomicBool>,
        metrics: Arc<Metrics>,
        v4_intents: Option<crate::sundaev4::intents::IntentServiceHandle>,
    ) -> Result<Self> {
        if let Some(dir) = &trace_directory {
            fs::create_dir_all(dir)?;
        }
        Ok(Self {
            event_rx,
            v3_state,
            v4_state,
            v4_execution,
            v4_language_views: None,
            v4_script_store: None,
            v4_chain_tracker: ChainTracker::new(),
            v4_batch_limits: BatchLimits::default(),
            trace_directory,
            paused,
            metrics,
            backoff_until_after_slot: None,
            quarantine: BTreeMap::new(),
            v4_intents,
            logged_intent_matches: std::collections::BTreeSet::new(),
        })
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        // Wait for the indexer to catch up to the chain tip before scooping.
        // The indexer updates the shared state (tip_slot, network_tip_slot)
        // as it processes blocks. We consume events (so the broadcast channel
        // doesn't overflow) and break once is_at_network_tip() reports true.
        info!("scooper waiting for indexer to reach chain tip");
        let mut sync_log_counter: u64 = 0;
        loop {
            // Drain buffered events, looking for TipAdvanced
            self.drain_events().await;

            if let Some(true) = self.is_at_network_tip().await {
                let (n_pools, n_orders) = if let Some(s) = &self.v4_state {
                    let state = s.lock().await;
                    let latest = state.latest();
                    (latest.pools.len(), latest.orders.len())
                } else {
                    (0, 0)
                };
                info!(n_pools, n_orders, "scooper synced with chain tip, batch processing enabled");
                break;
            }

            sync_log_counter += 1;
            if sync_log_counter % SYNC_LOG_INTERVAL == 1 {
                if let Some(s) = &self.v4_state {
                    let state = s.lock().await;
                    let latest = state.latest();
                    info!(
                        tip_slot = latest.tip_slot,
                        network_tip = latest.network_tip_slot.unwrap_or(0),
                        gap = latest.network_tip_slot.unwrap_or(0).saturating_sub(latest.tip_slot),
                        "waiting for indexer to catch up"
                    );
                }
            }

            // Wait for next event
            select! {
                _ = shutdown.cancelled() => { return; }
                res = self.event_rx.recv() => {
                    match res {
                        Ok((slot, events)) => {
                            self.process_events(slot, events).await;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            if n < 50 {
                                debug!("scooper lagged behind by {n} event batches");
                            } else {
                                warn!(n, "scooper lagged behind by {n} event batches — possible processing bottleneck");
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            return;
                        }
                    }
                }
            }
        }

        loop {
            // 1. Non-blocking drain of all pending events
            self.drain_events().await;

            // 2. Expire stale in-flight chains, prune quarantine & sync metrics
            if let Some(tip_slot) = self.current_tip_slot().await {
                self.v4_chain_tracker.expire_stale(tip_slot);
                self.prune_expired_quarantine(tip_slot);
            }
            self.sync_in_flight_metrics();
            self.sync_quarantine_metrics();

            // 3. Attempt batch cycle (skip if paused or backing off after lost race)
            let did_work = if self.paused.load(Ordering::Relaxed) {
                trace!("scooper paused, skipping batch cycle");
                false
            } else if let Some(backoff_slot) = self.backoff_until_after_slot {
                if let Some(tip) = self.current_tip_slot().await {
                    if tip > backoff_slot {
                        self.backoff_until_after_slot = None;
                        self.run_v4_batch_cycle().await
                    } else {
                        trace!(tip, backoff_slot, "waiting for tip to advance past lost-race slot");
                        false
                    }
                } else {
                    false
                }
            } else {
                self.run_v4_batch_cycle().await
            };

            // 4. If nothing to do, block on next event
            if !did_work {
                let (slot, events) = select! {
                    _ = shutdown.cancelled() => { break; }
                    res = self.event_rx.recv() => {
                        match res {
                            Ok(batch) => batch,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                if n < 50 {
                                    debug!("scooper lagged behind by {n} event batches");
                                } else {
                                    warn!(n, "scooper lagged behind by {n} event batches — possible processing bottleneck");
                                }
                                continue;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                break;
                            }
                        }
                    }
                };
                self.process_events(slot, events).await;
            }
        }
    }

    /// Drain all pending events from the broadcast channel (non-blocking).
    /// Ensures we have the latest state before building transactions.
    /// Returns the number of event batches drained.
    async fn drain_events(&mut self) -> usize {
        let mut count = 0;
        loop {
            match self.event_rx.try_recv() {
                Ok((slot, events)) => {
                    self.process_events(slot, events).await;
                    count += 1;
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                    if n < 50 {
                        debug!("scooper lagged behind by {n} event batches during drain");
                    } else {
                        warn!(n, "scooper lagged behind by {n} event batches during drain — possible processing bottleneck");
                    }
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            }
        }
        count
    }

    async fn current_tip_slot(&self) -> Option<u64> {
        match &self.v4_state {
            Some(s) => Some(s.lock().await.latest().tip_slot),
            None => None,
        }
    }

    /// Check whether an order is currently quarantined.
    fn is_quarantined(&self, input: &TransactionInput, current_slot: u64) -> bool {
        match self.quarantine.get(input) {
            Some(Quarantine::Permanent { .. }) => true,
            Some(Quarantine::Temporary { until_slot, .. }) => current_slot <= *until_slot,
            None => false,
        }
    }

    /// Remove expired temporary quarantine entries.
    fn prune_expired_quarantine(&mut self, current_slot: u64) {
        self.quarantine.retain(|_, q| match q {
            Quarantine::Permanent { .. } => true,
            Quarantine::Temporary { until_slot, .. } => current_slot <= *until_slot,
        });
    }

    /// Snapshot of quarantined order inputs for the metrics/API layer.
    pub fn quarantine_snapshot(&self) -> QuarantineSnapshot {
        let mut permanent = Vec::new();
        let mut temporary = Vec::new();
        for (input, q) in &self.quarantine {
            match q {
                Quarantine::Permanent { reason } => {
                    permanent.push(QuarantineEntry {
                        order: input.to_string(),
                        reason: reason.clone(),
                    });
                }
                Quarantine::Temporary { reason, until_slot } => {
                    temporary.push(QuarantineEntry {
                        order: input.to_string(),
                        reason: format!("{} (until slot {})", reason, until_slot),
                    });
                }
            }
        }
        QuarantineSnapshot { permanent, temporary }
    }

    /// Check if we've caught up with the network tip.
    /// Returns `Some(true)` if at tip, `Some(false)` if not, `None` if unknown.
    async fn is_at_network_tip(&self) -> Option<bool> {
        match &self.v4_state {
            Some(s) => {
                let state = s.lock().await;
                let latest = state.latest();
                match latest.network_tip_slot {
                    Some(network_tip) => {
                        let at_tip = latest.tip_slot + SYNC_TOLERANCE_SLOTS >= network_tip;
                        Some(at_tip)
                    }
                    None => None,
                }
            }
            None => None,
        }
    }

    async fn process_events(&mut self, slot: u64, events: Vec<IndexEvent>) {
        let mut updates: Vec<serde_json::Value> = vec![];

        // Get current v3 state snapshot for order validation (if v3 is configured)
        let v3_state = match &self.v3_state {
            Some(s) => Some(s.lock().await.latest().into_owned()),
            None => None,
        };

        for event in events {
            match event {
                IndexEvent::V3PoolCreated { id, pool } => {
                    let summary = pool_summary(&pool);
                    trace!(slot, pool = %id, "pool created");
                    updates.push(serde_json::to_value(PoolState {
                        slot,
                        pool: id,
                        action: PoolAction::Added { summary },
                    }).unwrap());
                }
                IndexEvent::V3PoolUpdated { id, pool, .. } => {
                    let summary = pool_summary(&pool);
                    trace!(slot, pool = %id, "pool updated");
                    updates.push(serde_json::to_value(PoolState {
                        slot,
                        pool: id,
                        action: PoolAction::Changed { summary },
                    }).unwrap());
                }
                IndexEvent::V3PoolRemoved { id, .. } => {
                    trace!(slot, pool = %id, "pool removed");
                    updates.push(serde_json::to_value(PoolState {
                        slot,
                        pool: id,
                        action: PoolAction::Removed,
                    }).unwrap());
                }
                IndexEvent::V3OrderCreated { order } => {
                    let pools = v3_state.as_ref().map(|s| &s.pools);
                    let validity = match pools {
                        Some(pools) => validate_order(&order, pools),
                        None => OrderValidity::Invalid {
                            reason: OrderInvalidReason::NoPools,
                        },
                    };
                    trace!(slot, order = %order.input, "order created");
                    updates.push(serde_json::to_value(OrderState {
                        slot,
                        order: order.input.clone(),
                        action: OrderAction::Added { valid: validity },
                    }).unwrap());
                }
                IndexEvent::V3OrderScooped { order, pool_id, .. } => {
                    trace!(slot, order = %order.input, pool = %pool_id, "order scooped");
                    updates.push(serde_json::to_value(OrderState {
                        slot,
                        order: order.input.clone(),
                        action: OrderAction::Scooped { pool_id },
                    }).unwrap());
                }
                IndexEvent::V3OrderCancelled { order, .. } => {
                    trace!(slot, order = %order.input, "order cancelled");
                    updates.push(serde_json::to_value(OrderState {
                        slot,
                        order: order.input.clone(),
                        action: OrderAction::Cancelled,
                    }).unwrap());
                }
                IndexEvent::V3SettingsUpdated { .. } => {
                    trace!(slot, "settings updated");
                }
                IndexEvent::V4PoolCreated { id, .. } => {
                    trace!(slot, pool = %id, "v4 pool created");
                }
                IndexEvent::V4PoolUpdated { id, pool, .. } => {
                    trace!(slot, pool = %id, "v4 pool updated");
                    // Check if this update settles one of our in-flight txs
                    if let Some(tx_hash) = self.v4_chain_tracker.find_settled_tx(&id, &pool.input) {
                        self.v4_chain_tracker.confirm_settlement(&id, &tx_hash);
                    } else if self.v4_chain_tracker.latest_predicted_pool(&id).is_some() {
                        // Pool was updated but doesn't match any of our
                        // in-flight txs — a competitor scooped, discard chain
                        // (cascade to related pools in multi-pool txs)
                        info!(pool = %id, "v4 pool updated by competitor, discarding chain");
                        self.v4_chain_tracker.discard_chain_and_related(&id);
                    }
                }
                IndexEvent::V4PoolRemoved { id, .. } => {
                    trace!(slot, pool = %id, "v4 pool removed");
                    self.v4_chain_tracker.discard_chain_and_related(&id);
                }
                IndexEvent::V4OrderCreated { order } => {
                    trace!(slot, order = %order.input, "v4 order created");
                    // No immediate action — batch cycle picks it up
                }
                IndexEvent::V4OrderScooped { order, pool_ids, .. } => {
                    trace!(slot, order = %order.input, pools = ?pool_ids, "v4 order scooped");
                    self.quarantine.remove(&order.input);
                }
                IndexEvent::V4OrderCancelled { order, .. } => {
                    trace!(slot, order = %order.input, "v4 order cancelled");
                    self.quarantine.remove(&order.input);
                }
                IndexEvent::V4SettingsUpdated { .. } => {
                    trace!(slot, "v4 settings updated");
                }
                IndexEvent::TipAdvanced { .. } => {
                    // Tip tracking is handled by the shared state;
                    // the scooper uses this event to unblock the sync loop.
                }
                IndexEvent::Rollback { to_slot } => {
                    trace!(to_slot, "rollback");
                    self.v4_chain_tracker.discard_all();
                }
            }
        }

        if !updates.is_empty() {
            if let Err(err) = self.write_updates(&updates) {
                warn!("could not log updates: {err:#}");
            }
        }
    }

    /// Run one cycle of incremental accumulation → build → evaluate → submit.
    ///
    /// Iterates candidate orders one at a time. For each: clone accumulator,
    /// try adding the order, build+evaluate, check limits. If within limits,
    /// accept the candidate; if over limits, submit the previous state.
    /// Sync the metrics in-flight snapshot from the chain tracker.
    fn sync_in_flight_metrics(&self) {
        let pools = self.v4_chain_tracker.in_flight_pools();
        let orders = self.v4_chain_tracker.in_flight_order_inputs();
        let tx_count = self.v4_chain_tracker.in_flight_tx_count();
        self.metrics.update_in_flight(&pools, &orders, tx_count);
    }

    /// Sync quarantine snapshot to the metrics layer for API/dashboard.
    fn sync_quarantine_metrics(&self) {
        self.metrics.update_quarantine(self.quarantine_snapshot());
    }

    ///
    /// This naturally supports multi-pool transactions when orders target
    /// different pools.
    async fn run_v4_batch_cycle(&mut self) -> bool {
        let exec = match &self.v4_execution {
            Some(e) => e.clone(),
            None => {
                trace!("v4 execution not configured, skipping batch cycle");
                return false;
            }
        };

        // Ensure language views are computed
        if self.v4_language_views.is_none() {
            let lv = crate::sundaev4::submit::encode_language_views(&exec.plutus_v3_cost_model);
            debug!(
                "computed PlutusV3 language views ({} bytes) from config cost model ({} params)",
                lv.len(),
                exec.plutus_v3_cost_model.len()
            );
            self.v4_language_views = Some(lv);
        }
        let language_views = self.v4_language_views.clone().unwrap();

        // Snapshot state
        let v4_state = match &self.v4_state {
            Some(s) => s.lock().await.latest().into_owned(),
            None => {
                trace!("v4 state not available, skipping batch cycle");
                return false;
            }
        };

        let settings = match &v4_state.settings {
            Some(s) => s.clone(),
            None => {
                debug!("v4 settings not yet loaded, skipping batch cycle");
                return false;
            }
        };

        // Use the network tip slot (actual chain tip) for the validity interval,
        // falling back to the last processed block slot.
        let current_slot = v4_state.network_tip_slot.unwrap_or(v4_state.tip_slot);

        // Select collateral. Must cover total_collateral (= final tx_fee * 1.5)
        // plus min UTxO on the collateral return output. We size against
        // `MAX_REAL_TX_FEE` (not the first-pass `TX_FEE` placeholder), since
        // the rebuild writes the real fee via fee_override — and a too-small
        // collateral input makes collateral_return drop below min_utxo.
        let ada_asset = crate::cardano_types::AssetClass { policy: vec![], token: vec![] };
        let min_collateral_ada =
            crate::sundaev4::tx_builder::MAX_REAL_TX_FEE * 3 / 2 + MIN_COLLATERAL_RETURN;
        let collateral = v4_state
            .wallet_utxos
            .iter()
            .find(|(_, v)| {
                use num_traits::ToPrimitive;
                v.get(&ada_asset).unwrap().to_u64().unwrap_or(0) >= min_collateral_ada
            });
        let (collateral_input, collateral_value) = match collateral {
            Some((input, value)) => (input.clone(), value.clone()),
            None => {
                warn!(
                    min_ada = min_collateral_ada,
                    wallet_utxos = v4_state.wallet_utxos.len(),
                    "no wallet UTxO with sufficient ADA for collateral"
                );
                return false;
            }
        };

        // Optional funding UTxO: covers any min-ada bump on pool outputs
        // (post-upgrades that grow datum size) and recycles the remainder
        // back as scooper change. Must be distinct from the collateral UTxO
        // and carry enough ada to cover a worst-case bump plus the change
        // floor. We pick the smallest UTxO that clears the threshold to
        // avoid tying up large balances. If none qualify, we still try to
        // build — pools whose datum didn't grow won't need a bump, so the
        // build can succeed without funding; pools that do need one will
        // fail with a clear "bump needed" error.
        const MIN_FUNDING_ADA: u64 = 2_500_000;
        let funding = v4_state
            .wallet_utxos
            .iter()
            .filter(|(i, _)| *i != &collateral_input)
            .filter(|(_, v)| {
                use num_traits::ToPrimitive;
                v.get(&ada_asset).unwrap().to_u64().unwrap_or(0) >= MIN_FUNDING_ADA
            })
            .min_by_key(|(_, v)| {
                use num_traits::ToPrimitive;
                v.get(&ada_asset).unwrap().to_u64().unwrap_or(0)
            });
        let funding_owned: Option<(TransactionInput, crate::cardano_types::Value)> =
            funding.map(|(i, v)| (i.clone(), v.clone()));
        if funding_owned.is_none() {
            debug!(
                min_ada = MIN_FUNDING_ADA,
                wallet_utxos = v4_state.wallet_utxos.len(),
                "no wallet UTxO available for funding; builds will fail if any pool needs a min-ada bump"
            );
        }

        // Filter orders: exclude in-flight and quarantined, sort oldest first
        let in_flight_inputs = self.v4_chain_tracker.in_flight_order_inputs();
        let in_flight_pools = self.v4_chain_tracker.in_flight_pools();
        let n_in_flight_orders = in_flight_inputs.len();
        let mut n_quarantined = 0u32;
        // Inputs to be permanently quarantined this pass — collected in the
        // filter chain (where we hold only `&self`) and applied below.
        let mut quarantine_budget_zero: Vec<TransactionInput> = Vec::new();
        let mut candidates: Vec<_> = v4_state
            .orders
            .iter()
            // Swap, (proportional) Deposit, and Withdraw are handled. Claim
            // isn't yet supported. Deposits that don't fit a proportional
            // unit against any indexed pool get filtered out in the matching
            // step below; CS withdraws fall out at resolve time.
            .filter(|o| matches!(
                o.constraint,
                crate::sundaev4::Constraint::Swap { .. }
                    | crate::sundaev4::Constraint::Deposit { .. }
                    | crate::sundaev4::Constraint::Withdraw { .. },
            ))
            .filter(|o| !in_flight_inputs.contains(&o.input))
            .filter(|o| {
                use num_traits::ToPrimitive;
                if self.is_quarantined(&o.input, current_slot) {
                    n_quarantined += 1;
                    return false;
                }
                // Orders with budget == 0 are structurally unscoopable:
                // order_validator enforces `budget * n >= tx_body.fee`
                // and `tx_body.fee >= chain_min_fee > 0`. Mark them for
                // permanent quarantine (applied below) so we stop
                // dispatching them every cycle — V4 orders don't TTL.
                let budget = o.datum.budget.clone().unwrap().to_u64().unwrap_or(0);
                if budget == 0 {
                    quarantine_budget_zero.push(o.input.clone());
                    n_quarantined += 1;
                    return false;
                }
                true
            })
            .cloned()
            .collect();

        // Strategy orders become scoopable when a posted intent authorizes
        // an execution. Synthesize a swap-shaped view of each matched order
        // so the ordinary routing/batching/fee pipeline handles it; the SSE
        // itself rides to the tx_builder as the strategy_order withdrawal
        // redeemer (in canonical input order).
        let mut strategy_executions: BTreeMap<
            TransactionInput,
            pallas_primitives::PlutusData,
        > = BTreeMap::new();
        // A claim-hinted intent that matched: executes as a dedicated
        // single-order plan, short-circuiting the normal accumulation cycle.
        let mut pending_claim_plan: Option<crate::sundaev4::batch::ScoopPlan> = None;
        if let Some(intents) = &self.v4_intents {
            use crate::sundaev4::intents;
            // The on-chain check is interval.includes(execution, tx_range):
            // the intent's window must contain the tx's whole validity range.
            let tx_start_ms = exec.slot_config.slot_to_posix_ms(current_slot);
            let tx_end_ms = exec
                .slot_config
                .slot_to_posix_ms(current_slot + crate::sundaev4::tx_builder::VALIDITY_RANGE);
            let store = intents.store.lock().await;
            let now = intents::now_ms();
            for order in v4_state.orders.iter() {
                if !matches!(order.constraint, crate::sundaev4::Constraint::Strategy { .. }) {
                    continue;
                }
                if in_flight_inputs.contains(&order.input)
                    || self.is_quarantined(&order.input, current_slot)
                {
                    continue;
                }
                let key = (
                    order.input.0.transaction_id.as_ref().to_vec(),
                    order.input.0.index,
                );
                for intent in store.valid_for_order(&key, now) {
                    if let Some(hint) = &intent.hint {
                        let crate::sundaev4::intents::ExecutionHint::Claim { pool: pool_hex } =
                            hint;
                        if pending_claim_plan.is_some()
                            || intent.sse.execution.final_destination.is_some()
                            || !intents::window_covers(&intent.sse, tx_start_ms, tx_end_ms)
                        {
                            continue;
                        }
                        match self.plan_claim_for_intent(
                            order, intent, pool_hex, &v4_state, &in_flight_pools, &exec,
                        ) {
                            Some((plan, sse_pd)) => {
                                info!(
                                    order = %order.input,
                                    intent = %hex::encode(&intent.intent_id),
                                    pool = %pool_hex,
                                    "claim intent matched; dispatching dedicated claim scoop",
                                );
                                strategy_executions.insert(order.input.clone(), sse_pd);
                                pending_claim_plan = Some(plan);
                                break;
                            }
                            None => continue,
                        }
                    }
                    if intent.sse.execution.final_destination.is_some() {
                        debug!(
                            order = %order.input,
                            "strategy intent picks a final destination; not yet supported",
                        );
                        continue;
                    }
                    if !intents::window_covers(&intent.sse, tx_start_ms, tx_end_ms) {
                        trace!(
                            order = %order.input,
                            "strategy intent window doesn't cover the tx validity range",
                        );
                        continue;
                    }
                    let Some(constraint) =
                        intents::synthesize_swap_constraint(order, &intent.sse)
                    else {
                        debug!(
                            order = %order.input,
                            "strategy intent shape not yet supported (ADA-only/multi-asset \
                             offer, or nothing to swap)",
                        );
                        continue;
                    };
                    let Ok(sse_pd) =
                        minicbor::decode::<pallas_primitives::PlutusData>(&intent.sse_cbor)
                    else {
                        continue;
                    };
                    if self.logged_intent_matches.insert(intent.intent_id.clone()) {
                        info!(
                            order = %order.input,
                            intent = %hex::encode(&intent.intent_id),
                            "strategy order matched with intent; dispatching as swap",
                        );
                    }
                    strategy_executions.insert(order.input.clone(), sse_pd);
                    candidates.push(Arc::new(crate::sundaev4::SundaeV4Order {
                        input: order.input.clone(),
                        value: order.value.clone(),
                        datum: order.datum.clone(),
                        constraint,
                        slot: order.slot,
                    }));
                    break;
                }
            }
        }
        candidates.sort_by_key(|o| o.slot);

        // Apply any permanent quarantines deferred from the filter chain
        // (we couldn't borrow `&mut self` while iterating).
        for input in quarantine_budget_zero {
            if !matches!(
                self.quarantine.get(&input),
                Some(Quarantine::Permanent { .. }),
            ) {
                warn!(
                    order = %input,
                    "permanently quarantining: budget == 0 (max_protocol_fee = 0); \
                     can never satisfy `budget * n >= tx.fee`",
                );
                self.quarantine.insert(
                    input,
                    Quarantine::Permanent { reason: "budget == 0".into() },
                );
            }
        }
        self.sync_quarantine_metrics();

        if candidates.is_empty() && pending_claim_plan.is_none() {
            if n_in_flight_orders > 0 || n_quarantined > 0 {
                debug!(
                    n_in_flight_orders,
                    n_quarantined,
                    in_flight_pools = ?in_flight_pools.iter().map(|i| i.to_string()).collect::<Vec<_>>(),
                    total_orders = v4_state.orders.len(),
                    "\u{23f3} all orders in-flight or quarantined, waiting"
                );
            }
            return false;
        }

        // Build script store once (cached across cycles)
        if self.v4_script_store.is_none() {
            match crate::sundaev4::evaluator::ScriptStore::from_ref_utxos(
                &v4_state.ref_utxo_outputs,
            ) {
                Ok(s) => {
                    self.v4_script_store = Some(s);
                }
                Err(e) => {
                    error!(error = %e, "failed to build script store from ref UTxOs");
                    return false;
                }
            }
        }
        let script_store = self.v4_script_store.as_ref().unwrap();

        // Claim-hinted intents execute as a dedicated single-order plan,
        // short-circuiting accumulation. Must sit AFTER the lazy script-store
        // initialisation above — a fresh process can match a claim on its
        // very first cycle.
        if pending_claim_plan.is_some() {
            let claim_plan = pending_claim_plan.take().unwrap();
            return self.build_and_submit_plan(
                claim_plan, &settings, &exec, &v4_state, &language_views,
                &collateral_input, &collateral_value, &funding_owned,
                &strategy_executions,
            ).await;
        }

        // ── Phase 1: Accumulate valid orders (cheap, no tx eval) ──────────
        //
        // Save checkpoints after each successful add so we can binary search
        // for the largest batch within execution limits.

        let mut accum = Accumulator::new(exec.protocol_share);
        let mut checkpoints: Vec<Accumulator> = Vec::new();

        // Base pool view for dispatch:
        //   1. Drop blacklisted pools (structurally unscoopable; e.g. CS pool
        //      with non-zero fee_split protocol_share that cs_check can never
        //      honour).
        //   2. Overlay chain-tracker predictions so the swap router and
        //      deposit/withdraw lookups both chain off our own in-flight txs
        //      rather than the stale on-chain UTxO that we've already spent.
        //      Without this overlay, the router would route against the
        //      pre-spend pool input and the resulting tx would race itself —
        //      node returns BadInputsUTxO since our own predecessor tx in the
        //      mempool already consumed that input.
        let pools_filtered: std::collections::BTreeMap<_, _> = v4_state.pools.iter()
            .filter(|(ident, _)| !exec.blacklisted_pools.contains(&hex::encode(ident.to_bytes())))
            .map(|(ident, pool)| {
                let effective = self.v4_chain_tracker
                    .latest_predicted_pool(ident)
                    .map(|p| p.pool.clone())
                    .unwrap_or_else(|| pool.clone());
                (ident.clone(), effective)
            })
            .collect();

        let mut skip_no_pool = 0u32;
        let mut skip_add_failed = 0u32;
        let mut skip_no_route = 0u32;
        let mut skip_route_failed = 0u32;

        let conversion_edges =
            crate::sundaev4::conversions::routable_edges(&exec.conversions);
        for order in &candidates {
            if accum.order_count() >= self.v4_batch_limits.max_orders {
                break;
            }

            // Dispatch by constraint kind. Deposit/Withdraw target a specific
            // pool identified by the LP token in the order's offered/
            // min_received list — direct match by ident.
            //
            // Swap orders are pool-agnostic on chain: the constraint just
            // says "give me ≥ min_received of asset B for X of asset A". Any
            // pool, or split across pools, that satisfies that is valid. So
            // we ALWAYS route swaps through the router — it picks the best
            // single-pool or multi-pool split. Falling back to "first pool
            // with both assets" picked a suboptimal pool and rejected the
            // order when a better pool existed.
            let mut candidate = accum.clone();
            let added = match &order.constraint {
                crate::sundaev4::Constraint::Deposit { .. } => {
                    let Some(pool_ident) = batch::find_pool_for_deposit_order(order, &pools_filtered) else {
                        tracing::info!(order = %order.input, "order dispatch: deposit, no pool match");
                        skip_no_pool += 1;
                        continue;
                    };
                    tracing::info!(order = %order.input, kind = "deposit", matched_pool = %pool_ident, "order dispatch");
                    let effective_pool = pick_effective_pool(&candidate, &self.v4_chain_tracker, &v4_state, &pool_ident);
                    let Some(effective_pool) = effective_pool else {
                        skip_no_pool += 1;
                        continue;
                    };
                    match candidate.try_add_deposit(order, &pool_ident, &effective_pool) {
                        Ok(_) => true,
                        Err(e) => {
                            skip_add_failed += 1;
                            tracing::info!(error = %e, order = %order.input, "try_add failed");
                            false
                        }
                    }
                }
                crate::sundaev4::Constraint::Withdraw { .. } => {
                    let Some(pool_ident) = batch::find_pool_for_withdraw_order(order, &pools_filtered) else {
                        tracing::info!(order = %order.input, "order dispatch: withdraw, no pool match");
                        skip_no_pool += 1;
                        continue;
                    };
                    tracing::info!(order = %order.input, kind = "withdraw", matched_pool = %pool_ident, "order dispatch");
                    let effective_pool = pick_effective_pool(&candidate, &self.v4_chain_tracker, &v4_state, &pool_ident);
                    let Some(effective_pool) = effective_pool else {
                        skip_no_pool += 1;
                        continue;
                    };
                    match candidate.try_add_withdraw(order, &pool_ident, &effective_pool) {
                        Ok(_) => true,
                        Err(e) => {
                            skip_add_failed += 1;
                            tracing::info!(error = %e, order = %order.input, "try_add failed");
                            false
                        }
                    }
                }
                _ => {
                    // Swap: route through the optimizer against the
                    // accumulator's *current* pool state, so prior orders'
                    // depletion is visible. The router then naturally splits
                    // across CL+non-CL pools when an earlier order has
                    // drained the optimal CL pool.
                    let (offer_asset, offer_amount) = order.swap_offered();
                    let (ask_asset, _) = order.swap_min_received();
                    if offer_asset == ask_asset {
                        continue;
                    }
                    let pool_view = candidate.current_pool_view(&pools_filtered);
                    // Per-order fan-out limits: the order's tx-fee budget
                    // buys it a number of pools and routing steps. Orders
                    // paying more get more elaborate routes.
                    let order_budget_lov: u64 = {
                        use num_traits::ToPrimitive;
                        order.datum.budget.clone().unwrap().to_u64().unwrap_or(0)
                    };
                    let limits = router::RoutingLimits::from_budget(
                        order_budget_lov,
                        exec.cost_per_pool_lovelace,
                        exec.cost_per_step_lovelace,
                    );
                    // Orders carrying the route constraint module are held
                    // to what the deployed module can validate: a linear
                    // full-flow chain. Plain swap-constraint orders may
                    // blend across parallel paths (min_received is their
                    // only on-chain output check).
                    let has_route_module = exec
                        .module_scripts
                        .route_order
                        .as_ref()
                        .map(|m| {
                            order.datum.constraints.iter().any(|(h, _)| {
                                h.as_slice() == m.hash.as_ref()
                            })
                        })
                        .unwrap_or(false);
                    let Some(blend) = router::find_blended_route(
                        &pool_view,
                        &conversion_edges,
                        offer_asset,
                        ask_asset,
                        offer_amount,
                        limits,
                    ) else {
                        tracing::info!(order = %order.input, "order dispatch: swap, no route");
                        skip_no_route += 1;
                        continue;
                    };
                    let blend = if has_route_module && blend.as_single().is_none() {
                        // Fall back to the best single path for route-
                        // constrained orders.
                        match router::find_optimal_route(
                            &pool_view,
                            &conversion_edges,
                            offer_asset,
                            ask_asset,
                            offer_amount,
                            limits,
                        ) {
                            Some(single) => crate::sundaev4::router::BlendedRoute {
                                total_input: single.total_input.clone(),
                                total_output: single.total_output.clone(),
                                branches: vec![single],
                            },
                            None => {
                                skip_no_route += 1;
                                continue;
                            }
                        }
                    } else {
                        blend
                    };
                    // The deployed route module validates only a strictly
                    // serial chain: each hop's positive delta must negate the
                    // next hop's negative delta (route_lib.check_intermediate_flow).
                    // A hop that fans out across parallel pools is unrepresentable
                    // in that redeemer, so hold route-constrained orders to a
                    // single-split-per-hop path. Plain swap-constraint orders are
                    // unaffected (min_received is their only on-chain check).
                    if has_route_module {
                        let serial = blend
                            .as_single()
                            .map(|p| p.hops.iter().all(|h| h.splits.len() == 1))
                            .unwrap_or(false);
                        if !serial {
                            tracing::info!(
                                order = %order.input,
                                "order dispatch: swap, route-module order has no serial single-split path; skipping",
                            );
                            skip_no_route += 1;
                            continue;
                        }
                    }
                    tracing::info!(
                        order = %order.input,
                        kind = "swap",
                        branches = blend.branches.len(),
                        hops_per_branch = ?blend
                            .branches
                            .iter()
                            .map(|b| b.hops.len())
                            .collect::<Vec<_>>(),
                        total_output = %blend.total_output,
                        "order dispatch",
                    );
                    let add_result = match blend.as_single() {
                        Some(single) => candidate.try_add_routed_order(order, single, &pool_view),
                        None => candidate.try_add_blended_order(order, &blend, &pool_view),
                    };
                    match add_result {
                        Ok(_) => true,
                        Err(e) => {
                            skip_route_failed += 1;
                            tracing::info!(error = %e, order = %order.input, "try_add_routed failed");
                            false
                        }
                    }
                }
            };

            // Only checkpoint when this order actually joined the accumulator.
            // Pushing on failure would seed checkpoints[0] with an empty state
            // (when the first candidate is rejected), which breaks the diag
            // path that expects checkpoints.first() to be the smallest viable
            // batch.
            if !added {
                continue;
            }
            accum = candidate;
            checkpoints.push(accum.clone());
        }

        if checkpoints.is_empty() {
            warn!(
                n_candidates = candidates.len(),
                skip_no_pool,
                skip_add_failed,
                skip_no_route,
                skip_route_failed,
                "no orders could be added to batch"
            );
            return false;
        }

        // ── Phase 2: Binary search for largest batch within limits ────────

        /// Three-state result of testing whether a candidate accumulator
        /// produces a viable tx. We never submit txs we can't evaluate, so
        /// eval failure is treated as a hard bail — not a "fall back to
        /// worst-case budget" hint. See the failure taxonomy in scoop().
        #[derive(Debug)]
        enum Fitness {
            Fits,
            Overbudget,
            Bail(String),
        }

        let within_limits = |accum: &Accumulator| -> Fitness {
            let plan = accum.clone().into_plan();
            let build = match crate::sundaev4::tx_builder::build_multi_pool_scoop_tx(
                &plan, &settings, &exec, current_slot, &language_views,
                &collateral_input.0, &collateral_value, None, &v4_state.ref_utxo_outputs,
                None, &v4_state.order_configs, &strategy_executions,
                funding_owned.as_ref().map(|(i, v)| (i.0.clone(), v)),
            ) {
                Ok(r) => r,
                Err(e) => {
                    // Build failures here mean the tx couldn't be assembled
                    // (e.g. tx_builder hit an internal invariant). Bail — it's
                    // a scooper bug, not a too-many-orders issue.
                    return Fitness::Bail(format!("build: {e}"));
                },
            };

            // Cheap pre-check: if the tx is already over the chain's size
            // limit, eval would just be wasted work (and noise — we'd dump
            // contexts for an obviously-too-big tx). Treat as Overbudget so
            // the binary search shrinks the batch.
            if build.cbor.len() > exec.max_tx_size {
                return Fitness::Overbudget;
            }

            let mut failure: Option<crate::sundaev4::evaluator::FailedScriptContext> = None;
            let r = match crate::sundaev4::evaluator::evaluate_scoop_tx(
                &build.tx_body,
                &build.redeemers,
                &build.resolved_inputs,
                &build.resolved_ref_inputs,
                self.v4_script_store.as_ref().unwrap(),
                &exec.plutus_v3_cost_model,
                build.tx_hash,
                &exec.slot_config,
                Some(&mut failure),
            ) {
                Ok(r) => r,
                Err(e) => {
                    // UPLC eval failure = scooper bug (we built a tx that
                    // doesn't pass our own scripts). Dump the context for
                    // offline diagnosis, then bail the entire scoop cycle —
                    // submitting on a guess could execute orders out of
                    // expected sequence.
                    if let Some(cap) = failure {
                        let ctx_dump = format!(
                            "/tmp/script-ctx-{}-{}-{:?}-{}.cbor",
                            build.tx_hash_hex,
                            hex::encode(cap.script_hash),
                            cap.redeemer_key.tag,
                            cap.redeemer_key.index,
                        );
                        let _ = std::fs::write(&ctx_dump, &cap.context_cbor);
                        let tx_dump = format!("/tmp/scoop-tx-{}.cbor", build.tx_hash_hex);
                        let _ = std::fs::write(&tx_dump, &build.cbor);
                    }
                    return Fitness::Bail(format!("eval: {e}"));
                }
            };

            let total_mem: u64 = r.budgets.iter().map(|(_, eu)| eu.mem).sum();
            let total_steps: u64 = r.budgets.iter().map(|(_, eu)| eu.steps).sum();
            let tx_size = build.cbor.len();

            let (pad_num, pad_den) = exec.budget_padding;
            let padded_mem = total_mem * pad_num / pad_den;
            let padded_steps = total_steps * pad_num / pad_den;

            let fits = padded_mem <= exec.max_tx_ex_mem
                && padded_steps <= exec.max_tx_ex_steps
                && tx_size <= exec.max_tx_size;
            if fits { Fitness::Fits } else { Fitness::Overbudget }
        };

        // Find the largest checkpoint (prefix of accumulated orders) that builds
        // and evaluates within limits. checkpoints[i] has (i+1) orders.
        //
        // Outcome handling per probe:
        // - Fits        → best = this index; try larger
        // - Overbudget  → too many ex-units / too big; try smaller
        // - Bail        → a build/eval failure was introduced by some order.
        //                 Rather than abort the whole cycle (which lets one bad
        //                 order wedge the queue for everyone), treat it like
        //                 Overbudget and search downward: this still scoops the
        //                 valid prefix and isolates the failing suffix. If even
        //                 the smallest (1-order) batch bails, that order is
        //                 quarantined below so it stops blocking the queue.
        let mut best: Option<usize> = None;
        let mut last_bail: Option<String> = None;

        let probe = |idx: usize| -> Fitness { within_limits(&checkpoints[idx]) };

        let top = checkpoints.len() - 1;
        // Quick check: try the full batch first (common case).
        match probe(top) {
            Fitness::Fits => best = Some(top),
            outcome => {
                if let Fitness::Bail(reason) = outcome {
                    last_bail = Some(reason);
                }
                // Full batch failed — binary search the smaller prefixes,
                // treating Overbudget and Bail alike as "go smaller".
                if top > 0 {
                    let (mut lo, mut hi) = (0usize, top - 1);
                    while lo <= hi {
                        let mid = lo + (hi - lo) / 2;
                        match probe(mid) {
                            Fitness::Fits => {
                                best = Some(mid);
                                lo = mid + 1;
                            }
                            Fitness::Overbudget => {
                                if mid == 0 { break; }
                                hi = mid - 1;
                            }
                            Fitness::Bail(reason) => {
                                last_bail = Some(reason);
                                if mid == 0 { break; }
                                hi = mid - 1;
                            }
                        }
                    }
                }
            }
        }

        // If we found a valid prefix but had to drop a failing suffix, note it.
        if let (Some(idx), Some(reason)) = (best, &last_bail) {
            if idx + 1 < checkpoints.len() {
                warn!(
                    kept = idx + 1,
                    dropped = checkpoints.len() - (idx + 1),
                    reason = %reason,
                    "isolated failing suffix from batch; scooping the valid prefix",
                );
            }
        }

        let had_successful_build = best.is_some();
        let accum = match best {
            Some(idx) => {
                let pools: Vec<_> = checkpoints[idx].pools.keys().map(|i| i.to_string()).collect();
                info!(
                    n_orders = checkpoints[idx].order_count(),
                    n_candidates = checkpoints.len(),
                    pools = ?pools,
                    "batch size determined"
                );
                checkpoints.into_iter().nth(idx).unwrap()
            }
            None => {
                // Re-run smallest batch (1 order) to capture the error at warn level
                let diag = checkpoints.first().unwrap();
                // Binary search returned None — re-run the smallest checkpoint
                // (1 order) to determine WHY. The taxonomy:
                //   - build error    → quarantine (structurally broken)
                //   - eval failure   → scooper bug; bail (don't quarantine)
                //   - over budget    → quarantine (one order can't fit; too fat)
                let diag_plan = diag.clone().into_plan();
                let (quarantine_reason, eval_bug_reason): (Option<String>, Option<String>) =
                    match crate::sundaev4::tx_builder::build_multi_pool_scoop_tx(
                    &diag_plan, &settings, &exec, current_slot, &language_views,
                    &collateral_input.0, &collateral_value, None, &v4_state.ref_utxo_outputs,
                    None, &v4_state.order_configs, &strategy_executions,
                    funding_owned.as_ref().map(|(i, v)| (i.0.clone(), v)),
                ) {
                    Err(e) => (Some(format!("build: {e}")), None),
                    Ok(build) => {
                        let mut failure: Option<crate::sundaev4::evaluator::FailedScriptContext> = None;
                        match crate::sundaev4::evaluator::evaluate_scoop_tx(
                            &build.tx_body, &build.redeemers, &build.resolved_inputs,
                            &build.resolved_ref_inputs, script_store, &exec.plutus_v3_cost_model,
                            build.tx_hash, &exec.slot_config,
                            Some(&mut failure),
                        ) {
                            Err(e) => {
                                // Eval failure on the minimal batch — definitely a
                                // scooper bug. Dump context + bail; don't penalise
                                // the order.
                                if let Some(cap) = failure {
                                    let ctx_dump = format!(
                                        "/tmp/script-ctx-{}-{}-{:?}-{}.cbor",
                                        build.tx_hash_hex,
                                        hex::encode(cap.script_hash),
                                        cap.redeemer_key.tag,
                                        cap.redeemer_key.index,
                                    );
                                    let _ = std::fs::write(&ctx_dump, &cap.context_cbor);
                                    let tx_dump = format!("/tmp/scoop-tx-{}.cbor", build.tx_hash_hex);
                                    let _ = std::fs::write(&tx_dump, &build.cbor);
                                }
                                (None, Some(format!("eval: {e}")))
                            }
                            Ok(r) => {
                                let total_mem: u64 = r.budgets.iter().map(|(_, eu)| eu.mem).sum();
                                let total_steps: u64 = r.budgets.iter().map(|(_, eu)| eu.steps).sum();
                                let (pad_num, pad_den) = exec.budget_padding;
                                (Some(format!(
                                    "over limits: mem={}/{}, steps={}/{}, size={}/{}",
                                    total_mem * pad_num / pad_den, exec.max_tx_ex_mem,
                                    total_steps * pad_num / pad_den, exec.max_tx_ex_steps,
                                    build.cbor.len(), exec.max_tx_size,
                                )), None)
                            }
                        }
                    }
                };

                if let Some(reason) = eval_bug_reason {
                    // The smallest (1-order) batch fails eval. It's likely a
                    // scooper bug specific to this order — but bailing every
                    // cycle lets it wedge the whole queue. Temporarily quarantine
                    // it (retried after ~TEMP_QUARANTINE_SLOTS, since the failure
                    // may be pool-state-dependent) so the rest of the queue keeps
                    // flowing, and keep the context dump for diagnosis.
                    let until_slot = current_slot + TEMP_QUARANTINE_SLOTS;
                    for input in diag.order_inputs() {
                        warn!(
                            order = %input, %reason, until_slot,
                            "temporarily quarantining order: fails eval in isolation \
                             (suspected scooper bug); context dumped to /tmp/script-ctx-*",
                        );
                        self.quarantine.insert(
                            input.clone(),
                            Quarantine::Temporary { reason: reason.clone(), until_slot },
                        );
                    }
                    self.sync_quarantine_metrics();
                    self.metrics.record_batch_failure(crate::metrics::BatchFailureReason::EvalError);
                    return false;
                }

                let reason = quarantine_reason.expect("either eval_bug or quarantine reason set");
                // Quarantine the offending order(s): they're truly structurally
                // unsound (build error) or too big to ever fit alone (over budget).
                let bad_inputs = diag.order_inputs();
                for input in bad_inputs {
                    warn!(order = %input, %reason, "permanently quarantining order");
                    self.quarantine.insert(input.clone(), Quarantine::Permanent {
                        reason: reason.clone(),
                    });
                }
                self.sync_quarantine_metrics();

                warn!(
                    n_candidates = checkpoints.len(),
                    reason,
                    "no valid batch size found within limits"
                );
                return false;
            }
        };

        // ── Submit the accumulated tx ──────────────────────────────────────

        if accum.is_empty() {
            debug!("accumulator empty after binary search");
            return false;
        }

        if !had_successful_build {
            debug!("no successful build during binary search");
            return false;
        }

        // Build → evaluate → submit. Shared with the claim path.
        let final_plan = accum.clone().into_plan();
        self.build_and_submit_plan(
            final_plan, &settings, &exec, &v4_state, &language_views,
            &collateral_input, &collateral_value, &funding_owned,
            &strategy_executions,
        ).await
    }

    /// Build the tx for `final_plan`, evaluate, compute the exact fee,
    /// rebuild, sign, submit, and record the outcome. Shared by the
    /// normal accumulation path and the dedicated claim path.
    #[allow(clippy::too_many_arguments)]
    async fn build_and_submit_plan(
        &mut self,
        final_plan: crate::sundaev4::batch::ScoopPlan,
        settings: &std::sync::Arc<crate::sundaev4::SundaeV4Settings>,
        exec: &ScooperExecution,
        v4_state: &crate::sundaev4::SundaeV4State,
        language_views: &[u8],
        collateral_input: &TransactionInput,
        collateral_value: &crate::cardano_types::Value,
        funding_owned: &Option<(TransactionInput, crate::cardano_types::Value)>,
        strategy_executions: &BTreeMap<TransactionInput, pallas_primitives::PlutusData>,
    ) -> bool {
        let n_orders: usize = final_plan.batches.iter()
            .map(|b| b.swaps.len() + b.deposits.len() + b.withdraws.len() + b.claims.len())
            .sum();
        let n_pools = final_plan.batches.len();
        let pool_idents: Vec<crate::sundaev3::Ident> =
            final_plan.batches.iter().map(|b| b.pool_ident.clone()).collect();
        let plan_order_inputs: Vec<TransactionInput> = final_plan.batches.iter()
            .flat_map(|b| {
                b.swaps.iter().map(|o| o.order.input.clone())
                    .chain(b.deposits.iter().map(|o| o.order.input.clone()))
                    .chain(b.withdraws.iter().map(|o| o.order.input.clone()))
                    .chain(b.claims.iter().map(|o| o.order.input.clone()))
            })
            .collect();
        // Refresh current_slot — the binary search phase may have taken many
        // seconds, so the slot captured at the start of the cycle could be stale.
        let current_slot = v4_state.network_tip_slot.unwrap_or(v4_state.tip_slot);

        // Build → evaluate → rebuild with exact budgets.

        let first_pass = match crate::sundaev4::tx_builder::build_multi_pool_scoop_tx(
            &final_plan, &settings, &exec, current_slot, language_views,
            &collateral_input.0, &collateral_value, None, &v4_state.ref_utxo_outputs,
            None, &v4_state.order_configs, &strategy_executions,
            funding_owned.as_ref().map(|(i, v)| (i.0.clone(), v)),
        ) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "final multi-pool tx build failed");
                // Under-funded orders can never execute (a UTxO's ada is
                // immutable), so quarantine permanently instead of retrying
                // every cycle.
                if e.to_string().contains("under-funded") {
                    for input in &plan_order_inputs {
                        self.quarantine.insert(
                            input.clone(),
                            Quarantine::Permanent {
                                reason: "under-funded: fulfillment can't retain min-UTxO".into(),
                            },
                        );
                    }
                    self.sync_quarantine_metrics();
                }
                return false;
            }
        };

        // Local eval. binary search already ran eval on this exact batch and
        // got Ok, so this should succeed too — anything else is a scooper bug
        // (race condition, builder non-determinism, etc.). Bail and dump on
        // failure rather than guessing a budget.
        let mut failure: Option<crate::sundaev4::evaluator::FailedScriptContext> = None;
        let padded_budgets: Vec<(pallas_primitives::conway::RedeemersKey, pallas_primitives::ExUnits)>
            = match crate::sundaev4::evaluator::evaluate_scoop_tx(
                &first_pass.tx_body,
                &first_pass.redeemers,
                &first_pass.resolved_inputs,
                &first_pass.resolved_ref_inputs,
                self.v4_script_store.as_ref().unwrap(),
                &exec.plutus_v3_cost_model,
                first_pass.tx_hash,
                &exec.slot_config,
                Some(&mut failure),
            ) {
                Ok(r) => {
                    // Inflate the evaluator's exact budgets by `budget_padding`
                    // before submitting. uplc-turbo's CEK step accounting
                    // doesn't always match cardano-node's exactly (we've seen
                    // ~0.04% under-estimates), and a node rejection for budget
                    // overrun aborts the whole scoop cycle — cheap insurance.
                    // Padding-vs-max-budget gating already happens in
                    // `within_limits` during the fitness binary search.
                    let (pad_num, pad_den) = exec.budget_padding;
                    r.budgets.iter().map(|(k, eu)| {
                        let mut padded = eu.clone();
                        padded.mem = eu.mem * pad_num / pad_den;
                        padded.steps = eu.steps * pad_num / pad_den;
                        (k.clone(), padded)
                    }).collect()
                }
                Err(e) => {
                    if let Some(cap) = failure {
                        let ctx_dump = format!(
                            "/tmp/script-ctx-{}-{}-{:?}-{}.cbor",
                            first_pass.tx_hash_hex,
                            hex::encode(cap.script_hash),
                            cap.redeemer_key.tag,
                            cap.redeemer_key.index,
                        );
                        let _ = std::fs::write(&ctx_dump, &cap.context_cbor);
                        let tx_dump = format!("/tmp/scoop-tx-{}.cbor", first_pass.tx_hash_hex);
                        let _ = std::fs::write(&tx_dump, &first_pass.cbor);
                    }
                    warn!(
                        error = %e,
                        tx_hash = %first_pass.tx_hash_hex,
                        "first_pass eval failed after binary-search Ok — likely a scooper bug; \
                         context dumped to /tmp/script-ctx-* — aborting scoop cycle",
                    );
                    self.metrics.record_batch_failure(crate::metrics::BatchFailureReason::EvalError);
                    return false;
                }
            };

        // Compute the exact protocol fee from the first-pass size and the
        // evaluated ex_units. The final rebuild changes per-order fee share
        // (and therefore output ADA values), but those values stay in the
        // same CBOR uint encoding bracket (5 bytes for amounts in the
        // hundreds of thousands to billions of lovelace), so final size
        // matches first_pass size to within 0–1 bytes. Add a small buffer
        // anyway in case the encoding nudges, since fee underpayment fails
        // the submit.
        let total_eval_mem: u64 = padded_budgets.iter().map(|(_, eu)| eu.mem).sum();
        let total_eval_steps: u64 = padded_budgets.iter().map(|(_, eu)| eu.steps).sum();
        let computed_fee = crate::sundaev4::tx_builder::compute_tx_fee(
            first_pass.cbor.len() as u64,
            total_eval_mem,
            total_eval_steps,
            first_pass.total_ref_script_bytes,
        ) + 1000; // +1000 lovelace buffer for any encoding-size jitter

        let final_tx = match crate::sundaev4::tx_builder::build_multi_pool_scoop_tx(
            &final_plan, &settings, &exec, current_slot, language_views,
            &collateral_input.0, &collateral_value, Some(&padded_budgets),
            &v4_state.ref_utxo_outputs,
            Some(computed_fee),
            &v4_state.order_configs,
            &strategy_executions,
            funding_owned.as_ref().map(|(i, v)| (i.0.clone(), v)),
        ) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "final multi-pool tx rebuild failed");
                self.metrics.record_batch_failure(crate::metrics::BatchFailureReason::BuildError);
                return false;
            }
        };



        let submitted_mem: u64 = padded_budgets.iter().map(|(_, eu)| eu.mem).sum();
        let submitted_steps: u64 = padded_budgets.iter().map(|(_, eu)| eu.steps).sum();
        let final_size = final_tx.cbor.len();
        info!(
            tx_hash = %final_tx.tx_hash_hex,
            n_orders,
            n_pools,
            pools = ?pool_idents.iter().map(|i| i.to_string()).collect::<Vec<_>>(),
            submitted_mem,
            mem_pct = format!("{:.1}%", submitted_mem as f64 / exec.max_tx_ex_mem as f64 * 100.0),
            submitted_steps,
            steps_pct = format!("{:.1}%", submitted_steps as f64 / exec.max_tx_ex_steps as f64 * 100.0),
            final_size,
            size_pct = format!("{:.1}%", final_size as f64 / exec.max_tx_size as f64 * 100.0),
            "multi-pool scoop tx built, submitting"
        );

        let submit_start = std::time::Instant::now();
        let submit_result = crate::sundaev4::submit::submit_tx(&exec.submit_url, &final_tx.cbor).await;
        self.metrics.submit_latency.observe(submit_start.elapsed().as_secs_f64());
        match submit_result {
            Ok(submitted_hash) => {
                info!(tx_hash = %submitted_hash, n_orders, n_pools, "multi-pool scoop tx submitted");
                self.metrics.batches_submitted.fetch_add(1, Ordering::Relaxed);
                self.metrics.orders_scooped.fetch_add(n_orders as u64, Ordering::Relaxed);

                // Persist the submitted tx CBOR for offline replay / diffing.
                let tx_dump = format!("/tmp/scoop-tx-{}.cbor", final_tx.tx_hash_hex);
                if let Err(e) = std::fs::write(&tx_dump, &final_tx.cbor) {
                    warn!(error = %e, path = %tx_dump, "failed to write tx CBOR dump");
                }

                // Per-pool-family attribution: a batch is a list of pools,
                // each typed. Count the orders against each family that
                // appeared in the batch (a mixed-pool tx increments
                // multiple families).
                use crate::sundaev4::PoolType;
                for batch in &final_plan.batches {
                    let family = match &batch.pool.pool_type {
                        PoolType::ConstantProduct { .. } => crate::metrics::PoolFamily::ConstantProduct,
                        PoolType::ConstantSum { .. } => crate::metrics::PoolFamily::ConstantSum,
                        PoolType::ConcentratedLiquidity { .. } => crate::metrics::PoolFamily::ConcentratedLiquidity,
                    };
                    let n = (batch.swaps.len() + batch.deposits.len() + batch.withdraws.len()) as u64;
                    self.metrics.record_pool_family_orders(family, n);
                }

                // Collect consumed orders from all batches. Swaps, deposits,
                // and withdraws all sit on real on-chain UTxOs and must be
                // tracked as in-flight so the next iteration doesn't re-attempt
                // them.
                let consumed_orders: Vec<_> = final_plan.batches.iter()
                    .flat_map(|b| {
                        b.swaps.iter().map(|s| s.order.clone())
                            .chain(b.deposits.iter().map(|d| d.order.clone()))
                            .chain(b.withdraws.iter().map(|w| w.order.clone()))
                    })
                    .collect();

                // Build predicted pools for chain tracker
                let predicted_pools: Vec<_> = final_tx.predicted_pools
                    .into_iter()
                    .map(|(ident, input, pool)| (ident, PredictedPoolUtxo {
                        input,
                        pool: Arc::new(pool),
                    }))
                    .collect();

                let in_flight = InFlightTx {
                    tx_hash: final_tx.tx_hash,
                    pool_idents: pool_idents.clone(),
                    consumed_orders,
                    predicted_pools,
                    ttl: final_tx.ttl,
                };
                self.v4_chain_tracker.record_submission(in_flight);
                self.sync_in_flight_metrics();
                true
            }
            Err(e) => {
                let msg = e.to_string();
                // Race-lost classifier covers both shapes the node uses when
                // our inputs were already spent:
                //   - BadInputsUTxO: somebody else's tx beat us to a pool/order
                //   - ConwayMempoolFailure "All inputs are spent. Transaction
                //     has probably already been included": our own previous
                //     submission was already accepted and we resubmitted
                //     (typically a state-lag artefact between submit and
                //     indexer-confirm). Both should be treated as RaceLost so
                //     we don't flag them as uplc-turbo divergence.
                let is_race_lost = msg.contains("BadInputsUTxO")
                    || msg.contains("ConwayMempoolFailure")
                    || msg.contains("All inputs are spent");
                let reason = if is_race_lost {
                    crate::metrics::BatchFailureReason::RaceLost
                } else {
                    crate::metrics::BatchFailureReason::SubmitError
                };
                self.metrics.record_batch_failure(reason);
                if matches!(reason, crate::metrics::BatchFailureReason::RaceLost) {
                    let pool_strs: Vec<String> = pool_idents.iter().map(|i| i.to_string()).collect();
                    info!(
                        tx_hash = %final_tx.tx_hash_hex,
                        pools = ?pool_strs,
                        "lost scoop race — pool or order UTxO already spent by another scooper"
                    );
                    // Parse bad inputs from the error and quarantine only those
                    let until_slot = current_slot + TEMP_QUARANTINE_SLOTS;
                    let bad_refs = parse_bad_inputs(&msg);
                    let order_inputs: Vec<&TransactionInput> = plan_order_inputs.iter().collect();
                    if bad_refs.is_empty() {
                        // Couldn't parse — quarantine all orders as fallback
                        info!(n_orders = order_inputs.len(), until_slot, "temporarily quarantining all batch orders (unparseable error)");
                        for input in &order_inputs {
                            self.quarantine.insert((*input).clone(), Quarantine::Temporary {
                                reason: "BadInputsUTxO (fallback)".into(),
                                until_slot,
                            });
                        }
                    } else {
                        // Only quarantine orders whose input appears in the bad inputs list
                        let mut n_quarantined = 0u32;
                        for input in &order_inputs {
                            if bad_refs.contains(&input.to_string()) {
                                self.quarantine.insert((*input).clone(), Quarantine::Temporary {
                                    reason: "BadInputsUTxO".into(),
                                    until_slot,
                                });
                                n_quarantined += 1;
                            }
                        }
                        info!(n_quarantined, n_bad_inputs = bad_refs.len(), until_slot, "temporarily quarantining spent orders");
                    }
                    self.sync_quarantine_metrics();
                } else {
                    // Non-race submit failures most often mean the node
                    // disagreed with our local eval on script budget. Dump
                    // the tx CBOR so we can hand it to the uplc-turbo team
                    // to diagnose the divergence.
                    let dump_path = format!("/tmp/submit-fail-{}.cbor", final_tx.tx_hash_hex);
                    if let Err(write_err) = std::fs::write(&dump_path, &final_tx.cbor) {
                        warn!(error = %write_err, path = %dump_path, "failed to write submit-fail tx dump");
                    }
                    error!(
                        error = %msg,
                        tx_hash = %final_tx.tx_hash_hex,
                        dump = %dump_path,
                        "multi-pool scoop tx submit failed — tx CBOR dumped for uplc-turbo diagnosis"
                    );
                }
                for ident in &pool_idents {
                    self.v4_chain_tracker.discard_chain_and_related(ident);
                }
                self.sync_in_flight_metrics();
                if let Some(tip) = self.current_tip_slot().await {
                    self.backoff_until_after_slot = Some(tip);
                }
                false
            }
        }
    }


    /// Resolve a claim-hinted intent into a dedicated single-order claim
    /// plan (waived-fee CS bounty, cs_check tag 5). Returns the plan plus
    /// the SSE PlutusData for the strategy_order withdrawal redeemer.
    ///
    /// Phase-1 shape requirements (all silently skipped otherwise):
    /// - hinted pool exists, is not in-flight, is CS with bounty enabled
    ///   (`bounty_k > 0`) and `waive_fee_on_claim = true`
    /// - the order offers exactly one non-ADA asset that's in the pool
    /// - the intent's receive asset is a different pool asset
    /// - the claim + swap output clears the intent's min_received floor
    fn plan_claim_for_intent(
        &self,
        order: &Arc<crate::sundaev4::SundaeV4Order>,
        intent: &crate::sundaev4::intents::StoredIntent,
        pool_hex: &str,
        v4_state: &crate::sundaev4::SundaeV4State,
        in_flight_pools: &[Ident],
        exec: &ScooperExecution,
    ) -> Option<(crate::sundaev4::batch::ScoopPlan, pallas_primitives::PlutusData)> {
        use crate::sundaev4::{batch, claims};
        use crate::sundaev4::PoolType;

        let (ident, pool) = v4_state
            .pools
            .iter()
            .find(|(id, _)| hex::encode(id.to_bytes()) == pool_hex)?;
        if in_flight_pools.contains(ident)
            || exec.blacklisted_pools.contains(&hex::encode(ident.to_bytes()))
        {
            return None;
        }
        let PoolType::ConstantSum { prices, bounty_k, waive_fee_on_claim, .. } =
            &pool.pool_type
        else {
            debug!(pool = %pool_hex, "claim hint targets a non-CS pool; skipping");
            return None;
        };
        if !waive_fee_on_claim {
            debug!(
                pool = %pool_hex,
                "claim hint targets a fee-paying-claims pool; only waived mode is supported",
            );
            return None;
        }

        let resolved_shape = match claims::resolve_claim_shape(
            &order.value,
            &intent.sse.execution.min_received,
            &pool.pool_datum.assets,
            prices,
        ) {
            Ok(shape) => shape,
            Err(reason) => {
                debug!(order = %order.input, reason, "claim intent shape unresolvable");
                return None;
            }
        };
        let min_ada_floor = match &resolved_shape {
            claims::ResolvedShape::Pair(s) => s.min_ada.clone(),
            claims::ResolvedShape::Rebalance(r) => r.min_ada.clone(),
        };
        if let Some(min_ada) = &min_ada_floor {
            use num_traits::ToPrimitive;
            let ada = crate::cardano_types::AssetClass { policy: vec![], token: vec![] };
            let order_ada = order.value.get(&ada).unwrap().to_u64().unwrap_or(0);
            let floor = min_ada.clone().unwrap().to_u64().unwrap_or(u64::MAX);
            // Conservative fee cushion — the exact fee is only known after
            // the build; the on-chain check is authoritative either way.
            const FEE_CUSHION: u64 = 2_500_000;
            if order_ada.saturating_sub(FEE_CUSHION) < floor {
                debug!(
                    order = %order.input,
                    order_ada, floor,
                    "claim intent's ada floor leaves no room for the fee share",
                );
                return None;
            }
        }

        let n_assets = pool.pool_datum.assets.len();
        let (resolved, final_assets) = match resolved_shape {
            claims::ResolvedShape::Pair(shape) => {
                let needed = &shape.min_recv - &shape.already_held;
                let search = claims::plan_claim_meeting_floor(
                    &pool.pool_datum.assets,
                    prices,
                    (&bounty_k.num, &bounty_k.den),
                    shape.in_idx,
                    shape.out_idx,
                    &shape.spendable,
                    &needed,
                )?;
                if !search.meets_floor {
                    debug!(
                        order = %order.input,
                        best_total = %(&search.plan.dy + &search.plan.claim),
                        needed = %needed,
                        "best achievable claim doesn't clear the intent's min_received floor",
                    );
                    return None;
                }
                let plan = search.plan;
                let mut deltas = vec![crate::bigint::BigInt::from(0); n_assets];
                deltas[shape.in_idx] = plan.dx.clone();
                deltas[shape.out_idx] = -(&plan.dy + &plan.claim);
                (
                    batch::ResolvedClaim {
                        order: order.clone(),
                        pool_deltas: deltas,
                        claim_idx: shape.out_idx,
                        claim: plan.claim.clone(),
                    },
                    plan.final_assets,
                )
            }
            claims::ResolvedShape::Rebalance(r) => {
                let plan = match claims::plan_rebalance_claim(
                    &pool.pool_datum.assets,
                    prices,
                    (&bounty_k.num, &bounty_k.den),
                    &r.held,
                    &r.targets,
                ) {
                    Ok(plan) => plan,
                    Err(reason) => {
                        debug!(
                            order = %order.input,
                            reason,
                            "rebalance claim not currently plannable",
                        );
                        return None;
                    }
                };
                (
                    batch::ResolvedClaim {
                        order: order.clone(),
                        pool_deltas: plan.deltas,
                        claim_idx: plan.claim_idx,
                        claim: plan.claim,
                    },
                    plan.final_assets,
                )
            }
        };

        let sse_pd: pallas_primitives::PlutusData =
            minicbor::decode(&intent.sse_cbor).ok()?;

        let claim_batch =
            batch::build_claim_batch(pool, order.clone(), resolved, final_assets);
        let scoop_plan = crate::sundaev4::batch::ScoopPlan {
            batches: vec![claim_batch],
            routes: Vec::new(),
            global_seq: vec![crate::sundaev4::batch::GlobalOp { batch_idx: 0, op_idx: 0 }],
        };
        Some((scoop_plan, sse_pd))
    }

    fn write_updates(&self, updates: &[serde_json::Value]) -> Result<()> {
        let date = chrono::Utc::now()
            .date_naive()
            .format("%Y-%m-%d")
            .to_string();
        let filename = format!("{date}.jsonl");
        let Some(dir) = self.trace_directory.as_ref() else {
            return Ok(());
        };
        let path = dir.join(filename);
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let mut file = BufWriter::new(file);
        for update in updates {
            serde_json::to_writer(&mut file, update)?;
            writeln!(&mut file)?;
        }
        Ok(())
    }
}

/// Best-effort extraction of bad input refs from a BadInputsUTxO error message.
///
/// The error string from submit is `"ogmios submit failed (status): {json}"`.
/// The JSON portion is the Ogmios error object with structure:
///   `{"code":...,"data":{"badInputs":["txhash#idx",...]}}`
/// Resolve the effective pool snapshot to scoop against — preferring an
/// already-accumulated state, then the chain tracker's in-flight prediction
/// (so we chain off our own pending tx), then the on-chain state.
fn pick_effective_pool(
    accum: &Accumulator,
    chain_tracker: &crate::sundaev4::chain_tracker::ChainTracker,
    v4_state: &crate::sundaev4::SundaeV4State,
    pool_ident: &Ident,
) -> Option<Arc<crate::sundaev4::SundaeV4Pool>> {
    if accum.pools.contains_key(pool_ident) {
        return Some(accum.pools[pool_ident].pool.clone());
    }
    if let Some(predicted) = chain_tracker.latest_predicted_pool(pool_ident) {
        return Some(predicted.pool.clone());
    }
    v4_state.pools.get(pool_ident).cloned()
}

fn parse_bad_inputs(msg: &str) -> std::collections::BTreeSet<String> {
    /// Minimal typed representation of the Ogmios error envelope.
    #[derive(serde::Deserialize)]
    struct OgmiosError {
        #[serde(default)]
        data: Option<OgmiosErrorData>,
    }

    #[derive(serde::Deserialize)]
    struct OgmiosErrorData {
        #[serde(default, alias = "badInputs")]
        bad_inputs: Vec<String>,
    }

    let json_str = msg.find('{').map(|i| &msg[i..]).unwrap_or("");
    match serde_json::from_str::<OgmiosError>(json_str) {
        Ok(err) => err.data
            .map(|d| d.bad_inputs.into_iter().collect())
            .unwrap_or_default(),
        Err(_) => std::collections::BTreeSet::new(),
    }
}

fn pool_summary(pool: &SundaeV3Pool) -> PoolSummary {
    let (asset_a, asset_b) = pool.pool_datum.assets.clone();
    let amount_a = pool.value.get(&asset_a);
    let amount_b = pool.value.get(&asset_b);
    PoolSummary {
        assets: (
            SingletonValue::new(asset_a, amount_a),
            SingletonValue::new(asset_b, amount_b),
        ),
        liquidity: pool.pool_datum.circulating_lp.clone(),
        protocol_fees: pool.pool_datum.protocol_fees.clone(),
    }
}

fn validate_order(
    order: &SundaeV3Order,
    pools: &BTreeMap<Ident, Arc<SundaeV3Pool>>,
) -> OrderValidity {
    if let Err(err) = validate_order_value(&order.datum, &order.value) {
        return OrderValidity::Invalid {
            reason: OrderInvalidReason::ValueError(err),
        };
    }
    let mut valid_pools = vec![];
    let mut errors = BTreeMap::new();
    for (ident, pool) in pools {
        if let Err(error) = validate_order_for_pool(&order.datum, &pool.pool_datum) {
            if matches!(error, PoolError::IdentMismatch) {
                continue;
            }
            errors.insert(ident.clone(), error);
        } else if let Err(error) =
            estimate_whether_in_range(&order.datum, &pool.pool_datum, &pool.value)
        {
            errors.insert(ident.clone(), error);
        } else {
            valid_pools.push(ident.clone());
        }
    }
    if !valid_pools.is_empty() {
        OrderValidity::Valid { pools: valid_pools }
    } else if !errors.is_empty() {
        OrderValidity::Invalid {
            reason: OrderInvalidReason::PoolErrors(errors),
        }
    } else {
        OrderValidity::Invalid {
            reason: OrderInvalidReason::NoPools,
        }
    }
}

#[derive(Serialize)]
struct PoolState {
    slot: u64,
    pool: Ident,
    action: PoolAction,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum PoolAction {
    Added {
        #[serde(flatten)]
        summary: PoolSummary,
    },
    Changed {
        #[serde(flatten)]
        summary: PoolSummary,
    },
    Removed,
}

#[derive(Serialize, PartialEq)]
struct PoolSummary {
    assets: (SingletonValue, SingletonValue),
    liquidity: BigInt,
    protocol_fees: BigInt,
}

#[derive(Serialize)]
struct OrderState {
    slot: u64,
    order: TransactionInput,
    action: OrderAction,
}
#[derive(Serialize)]
#[serde(tag = "type")]
enum OrderAction {
    Added {
        #[serde(flatten)]
        valid: OrderValidity,
    },
    Scooped {
        pool_id: Ident,
    },
    Cancelled,
}
#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "validity")]
enum OrderValidity {
    Valid { pools: Vec<Ident> },
    Invalid { reason: OrderInvalidReason },
}

/// Snapshot of quarantined orders, shared with the server for API/dashboard display.
#[derive(Clone, Default, Serialize)]
pub struct QuarantineSnapshot {
    pub permanent: Vec<QuarantineEntry>,
    pub temporary: Vec<QuarantineEntry>,
}

#[derive(Clone, Serialize)]
pub struct QuarantineEntry {
    pub order: String,
    pub reason: String,
}

#[derive(Debug, PartialEq, Serialize)]
enum OrderInvalidReason {
    NoPools,
    ValueError(ValueError),
    PoolErrors(BTreeMap<Ident, PoolError>),
}

