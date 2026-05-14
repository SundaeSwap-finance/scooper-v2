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
        let language_views = self.v4_language_views.as_ref().unwrap();

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

        // Select collateral — needs enough ADA for total_collateral (TX_FEE * 1.5)
        // plus min UTxO on the collateral return output.
        let ada_asset = crate::cardano_types::AssetClass { policy: vec![], token: vec![] };
        let min_collateral_ada = crate::sundaev4::tx_builder::TX_FEE * 3 / 2 + MIN_COLLATERAL_RETURN;
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

        if candidates.is_empty() {
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

        // ── Phase 1: Accumulate valid orders (cheap, no tx eval) ──────────
        //
        // Save checkpoints after each successful add so we can binary search
        // for the largest batch within execution limits.

        let mut accum = Accumulator::new(exec.protocol_share);
        let mut checkpoints: Vec<Accumulator> = Vec::new();

        let mut skip_no_pool = 0u32;
        let mut skip_add_failed = 0u32;
        let mut skip_no_route = 0u32;
        let mut skip_route_failed = 0u32;

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
                    let Some(pool_ident) = batch::find_pool_for_deposit_order(order, &v4_state.pools) else {
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
                    let Some(pool_ident) = batch::find_pool_for_withdraw_order(order, &v4_state.pools) else {
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
                    // Swap: route through the optimizer regardless of whether
                    // it ends up as single-pool or split.
                    let (offer_asset, offer_amount) = order.swap_offered();
                    let (ask_asset, _) = order.swap_min_received();
                    if offer_asset == ask_asset {
                        continue;
                    }
                    let Some(route) = router::find_optimal_route(
                        &v4_state.pools, offer_asset, ask_asset, offer_amount,
                    ) else {
                        tracing::info!(order = %order.input, "order dispatch: swap, no route");
                        skip_no_route += 1;
                        continue;
                    };
                    tracing::info!(
                        order = %order.input,
                        kind = "swap",
                        hops = route.hops.len(),
                        splits_per_hop = ?route.hops.iter().map(|h| h.splits.len()).collect::<Vec<_>>(),
                        total_output = %route.total_output,
                        "order dispatch",
                    );
                    match candidate.try_add_routed_order(order, &route, &v4_state.pools) {
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

        let within_limits = |accum: &Accumulator| -> bool {
            let batches = accum.clone().into_batches();
            let build = match crate::sundaev4::tx_builder::build_multi_pool_scoop_tx(
                &batches, &settings, &exec, current_slot, language_views,
                &collateral_input.0, &collateral_value, None, &v4_state.ref_utxo_outputs,
            ) {
                Ok(r) => r,
                Err(e) => {
                    debug!(error = %e, n_orders = accum.order_count(), "tx build failed");
                    return false;
                },
            };

            let eval = match crate::sundaev4::evaluator::evaluate_scoop_tx(
                &build.tx_body,
                &build.redeemers,
                &build.resolved_inputs,
                &build.resolved_ref_inputs,
                script_store,
                &exec.plutus_v3_cost_model,
                build.tx_hash,
                &exec.slot_config,
            ) {
                Ok(r) => Some(r),
                Err(e) => {
                    // Local eval can produce ExplicitErrorTerm where the chain
                    // would actually accept — particularly for Deposit txs the
                    // uplc-turbo bytecode path appears to diverge from the
                    // on-chain interpreter. Don't gate the binary search on
                    // it: pretend the budget is the worst case so the search
                    // still picks SOMETHING, and let the actual chain submit
                    // produce the authoritative verdict.
                    warn!(error = %e, n_orders = accum.order_count(), "local tx eval failed; submitting anyway with worst-case budget");
                    None
                },
            };

            let (total_mem, total_steps) = match &eval {
                Some(r) => (
                    r.budgets.iter().map(|(_, eu)| eu.mem).sum(),
                    r.budgets.iter().map(|(_, eu)| eu.steps).sum(),
                ),
                None => (exec.max_tx_ex_mem / 2, exec.max_tx_ex_steps / 2),
            };
            let tx_size = build.cbor.len();

            let (pad_num, pad_den) = exec.budget_padding;
            let padded_mem = total_mem * pad_num / pad_den;
            let padded_steps = total_steps * pad_num / pad_den;

            padded_mem <= exec.max_tx_ex_mem
                && padded_steps <= exec.max_tx_ex_steps
                && tx_size <= exec.max_tx_size
        };

        // Binary search: find the largest checkpoint index that's within limits.
        // checkpoints[i] has (i+1) orders.
        let mut lo: usize = 0;
        let mut hi: usize = checkpoints.len() - 1;
        let mut best: Option<usize> = None;

        // Quick check: try the full batch first (common case)
        if within_limits(&checkpoints[hi]) {
            best = Some(hi);
        } else {
            // Binary search between lo and hi
            while lo <= hi {
                let mid = lo + (hi - lo) / 2;
                if within_limits(&checkpoints[mid]) {
                    best = Some(mid);
                    lo = mid + 1;
                } else {
                    if mid == 0 { break; }
                    hi = mid - 1;
                }
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
                let diag_batches = diag.clone().into_batches();
                tracing::info!(
                    diag_batches_n = diag_batches.len(),
                    diag_order_count = diag.order_count(),
                    first_batch_swaps = diag_batches.first().map(|b| b.swaps.len()),
                    first_batch_deposits = diag_batches.first().map(|b| b.deposits.len()),
                    first_batch_withdraws = diag_batches.first().map(|b| b.withdraws.len()),
                    "diag rebuild input",
                );
                let reason = match crate::sundaev4::tx_builder::build_multi_pool_scoop_tx(
                    &diag_batches, &settings, &exec, current_slot, language_views,
                    &collateral_input.0, &collateral_value, None, &v4_state.ref_utxo_outputs,
                ) {
                    Err(e) => format!("build: {e}"),
                    Ok(build) => {
                        let dump_path = format!("/tmp/scoop-tx-{}.cbor", build.tx_hash_hex);
                        let _ = std::fs::write(&dump_path, &build.cbor);
                        info!(path = %dump_path, bytes = build.cbor.len(), tx_hash = %build.tx_hash_hex, "diag: dumped failing build CBOR");
                        match crate::sundaev4::evaluator::evaluate_scoop_tx(
                        &build.tx_body, &build.redeemers, &build.resolved_inputs,
                        &build.resolved_ref_inputs, script_store, &exec.plutus_v3_cost_model,
                        build.tx_hash, &exec.slot_config,
                    ) {
                        Err(e) => format!("eval: {e}"),
                        Ok(r) => {
                            let total_mem: u64 = r.budgets.iter().map(|(_, eu)| eu.mem).sum();
                            let total_steps: u64 = r.budgets.iter().map(|(_, eu)| eu.steps).sum();
                            let (pad_num, pad_den) = exec.budget_padding;
                            format!(
                                "over limits: mem={}/{}, steps={}/{}, size=n/a",
                                total_mem * pad_num / pad_den, exec.max_tx_ex_mem,
                                total_steps * pad_num / pad_den, exec.max_tx_ex_steps,
                            )
                        }
                        }
                    }
                };

                // Permanently quarantine the first order — it's structurally broken
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

        // Refresh current_slot — the binary search phase may have taken many
        // seconds, so the slot captured at the start of the cycle could be stale.
        let current_slot = v4_state.network_tip_slot.unwrap_or(v4_state.tip_slot);

        // Build → evaluate → rebuild with exact budgets.
        let final_batches = accum.clone().into_batches();

        let first_pass = match crate::sundaev4::tx_builder::build_multi_pool_scoop_tx(
            &final_batches, &settings, &exec, current_slot, language_views,
            &collateral_input.0, &collateral_value, None, &v4_state.ref_utxo_outputs,
        ) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "final multi-pool tx build failed");
                return false;
            }
        };

        // Local eval. If it fails (uplc-turbo divergence vs on-chain interp,
        // e.g. on deposits) fall back to a worst-case budget so we can still
        // submit and let the chain be authoritative. Production scoops should
        // pass eval; this just keeps the door open when uplc-turbo is wrong.
        let mut first_pass_eval_failed = false;
        let padded_budgets: Vec<(pallas_primitives::conway::RedeemersKey, pallas_primitives::ExUnits)>
            = match crate::sundaev4::evaluator::evaluate_scoop_tx(
                &first_pass.tx_body,
                &first_pass.redeemers,
                &first_pass.resolved_inputs,
                &first_pass.resolved_ref_inputs,
                script_store,
                &exec.plutus_v3_cost_model,
                first_pass.tx_hash,
                &exec.slot_config,
            ) {
                Ok(r) => {
                    // TODO: uplc-turbo underestimates the fairness script by
                    // ~12% vs the Cardano node evaluator. Apply 15% padding
                    // until the root cause is identified and fixed.
                    r.budgets.iter().map(|(k, eu)| {
                        (k.clone(), pallas_primitives::ExUnits {
                            mem: eu.mem + eu.mem / 7,
                            steps: eu.steps + eu.steps / 7,
                        })
                    }).collect()
                }
                Err(e) => {
                    first_pass_eval_failed = true;
                    warn!(error = %e, tx_hash = %first_pass.tx_hash_hex, "final multi-pool eval failed; submitting with worst-case budget");
                    // Empirical observation: typical multi-pool scoop redeemers
                    // run at ~750k mem / 280M steps each. Pick a generous-but-
                    // safe per-redeemer budget so the sum stays well under the
                    // tx limits. With N redeemers the total is N × (1M, 350M);
                    // for a 10-redeemer tx that's 10M mem / 3.5B steps, still
                    // comfortably under the 16.5M / 10B caps.
                    first_pass.redeemers.iter().map(|(k, _, _)| {
                        (k.clone(), pallas_primitives::ExUnits {
                            mem: 1_000_000,
                            steps: 350_000_000,
                        })
                    }).collect()
                }
            };

        let final_tx = match crate::sundaev4::tx_builder::build_multi_pool_scoop_tx(
            &final_batches, &settings, &exec, current_slot, language_views,
            &collateral_input.0, &collateral_value, Some(&padded_budgets),
            &v4_state.ref_utxo_outputs,
        ) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "final multi-pool tx rebuild failed");
                self.metrics.record_batch_failure(crate::metrics::BatchFailureReason::BuildError);
                return false;
            }
        };

        // If first-pass eval failed, re-run the evaluator on the rebuilt tx
        // (with worst-case budgets). The eval will likely fail again — but
        // the script context dump now corresponds to the tx hash we're
        // actually submitting on chain, which is what we want for diffing.
        if first_pass_eval_failed {
            let _ = crate::sundaev4::evaluator::evaluate_scoop_tx(
                &final_tx.tx_body,
                &final_tx.redeemers,
                &final_tx.resolved_inputs,
                &final_tx.resolved_ref_inputs,
                script_store,
                &exec.plutus_v3_cost_model,
                final_tx.tx_hash,
                &exec.slot_config,
            );
        }

        let n_orders = accum.order_count();
        let n_pools = accum.pools.len();
        let pool_idents: Vec<_> = accum.pools.keys().cloned().collect();

        info!(
            tx_hash = %final_tx.tx_hash_hex,
            n_orders,
            n_pools,
            pools = ?pool_idents.iter().map(|i| i.to_string()).collect::<Vec<_>>(),
            "multi-pool scoop tx built, submitting"
        );

        // Dump tx CBOR for offline analysis. Filename includes tx hash so
        // every attempt is preserved; cleanup is left to the operator.
        let dump_path = format!("/tmp/scoop-tx-{}.cbor", final_tx.tx_hash_hex);
        if let Err(e) = std::fs::write(&dump_path, &final_tx.cbor) {
            warn!(error = %e, path = %dump_path, "failed to write tx CBOR dump");
        } else {
            info!(path = %dump_path, bytes = final_tx.cbor.len(), "wrote tx CBOR dump");
        }

        let submit_start = std::time::Instant::now();
        let submit_result = crate::sundaev4::submit::submit_tx(&exec.submit_url, &final_tx.cbor).await;
        self.metrics.submit_latency.observe(submit_start.elapsed().as_secs_f64());
        match submit_result {
            Ok(submitted_hash) => {
                info!(tx_hash = %submitted_hash, n_orders, n_pools, "multi-pool scoop tx submitted");
                self.metrics.batches_submitted.fetch_add(1, Ordering::Relaxed);
                self.metrics.orders_scooped.fetch_add(n_orders as u64, Ordering::Relaxed);

                // Per-pool-family attribution: a batch is a list of pools,
                // each typed. Count the orders against each family that
                // appeared in the batch (a mixed-pool tx increments
                // multiple families).
                use crate::sundaev4::PoolType;
                for batch in &final_batches {
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
                let consumed_orders: Vec<_> = final_batches.iter()
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
                let reason = if msg.contains("BadInputsUTxO") {
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
                    let order_inputs = accum.order_inputs();
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
                    error!(error = %msg, tx_hash = %final_tx.tx_hash_hex, "multi-pool scoop tx submit failed");
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

