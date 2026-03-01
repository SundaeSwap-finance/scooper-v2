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
use tracing::{info, trace, warn};

use crate::{
    bigint::BigInt,
    cardano_types::TransactionInput,
    events::IndexEvent,
    sundaev3::{
        Ident, PoolError, SingletonValue, SundaeV3HistoricalState, SundaeV3Order, SundaeV3Pool,
        ValueError, estimate_whether_in_range, validate_order_for_pool, validate_order_value,
    },
    sundaev4::{
        SundaeV4HistoricalState, ScooperExecution,
        accumulator::Accumulator,
        batch::{self, BatchLimits},
        chain_tracker::{ChainTracker, InFlightTx, PredictedPoolUtxo},
        tx_builder::MultiPoolBuildResult,
    },
};

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
    /// Set to the tip slot when we lose a scoop race; skip batch cycles
    /// until the tip advances past this slot, giving the indexer time to
    /// process the competitor's block and remove spent UTxOs.
    backoff_until_after_slot: Option<u64>,
}

impl Scooper {
    pub fn new(
        trace_directory: Option<PathBuf>,
        event_rx: tokio::sync::broadcast::Receiver<(u64, Vec<IndexEvent>)>,
        v3_state: Option<Arc<Mutex<SundaeV3HistoricalState>>>,
        v4_state: Option<Arc<Mutex<SundaeV4HistoricalState>>>,
        v4_execution: Option<ScooperExecution>,
        paused: Arc<AtomicBool>,
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
            backoff_until_after_slot: None,
        })
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        // Wait for the indexer to catch up to the chain tip before scooping.
        // The indexer updates the shared state (tip_slot, network_tip_slot)
        // as it processes blocks. We poll the shared state while consuming
        // events so the broadcast channel doesn't overflow.
        info!("scooper waiting for indexer to reach chain tip");
        let mut sync_log_counter: u64 = 0;
        loop {
            // Drain buffered events, looking for TipAdvanced
            self.drain_events().await;

            if let Some(true) = self.is_at_network_tip().await {
                info!("scooper synced with chain tip, batch processing enabled");
                break;
            }

            sync_log_counter += 1;
            if sync_log_counter % 500 == 1 {
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

            // Wait for next event (TipAdvanced fires every block)
            select! {
                _ = shutdown.cancelled() => { return; }
                res = self.event_rx.recv() => {
                    match res {
                        Ok((slot, events)) => {
                            self.process_events(slot, events).await;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("scooper lagged behind by {n} event batches");
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

            // 2. Expire stale in-flight chains
            if let Some(tip_slot) = self.current_tip_slot().await {
                self.v4_chain_tracker.expire_stale(tip_slot);
            }

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
                                warn!("scooper lagged behind by {n} event batches");
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
                    warn!("scooper lagged behind by {n} event batches during drain");
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

    /// Check if we've caught up with the network tip.
    /// Returns `Some(true)` if at tip, `Some(false)` if not, `None` if unknown.
    async fn is_at_network_tip(&self) -> Option<bool> {
        match &self.v4_state {
            Some(s) => {
                let state = s.lock().await;
                let latest = state.latest();
                match latest.network_tip_slot {
                    Some(network_tip) => {
                        let at_tip = latest.tip_slot + 10 >= network_tip;
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
                }
                IndexEvent::V4OrderCancelled { order, .. } => {
                    trace!(slot, order = %order.input, "v4 order cancelled");
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

    /// Protocol-level limits for transaction building.
    const MAX_TX_EX_MEM: u64 = 14_000_000;
    const MAX_TX_EX_STEPS: u64 = 10_000_000_000;
    const MAX_TX_SIZE: usize = 16_384;

    /// Run one cycle of incremental accumulation → build → evaluate → submit.
    ///
    /// Iterates candidate orders one at a time. For each: clone accumulator,
    /// try adding the order, build+evaluate, check limits. If within limits,
    /// accept the candidate; if over limits, submit the previous state.
    ///
    /// This naturally supports multi-pool transactions when orders target
    /// different pools.
    async fn run_v4_batch_cycle(&mut self) -> bool {
        let exec = match &self.v4_execution {
            Some(e) => e.clone(),
            None => return false,
        };

        // Ensure language views are computed
        if self.v4_language_views.is_none() {
            let lv = crate::sundaev4::submit::encode_language_views(&exec.plutus_v3_cost_model);
            info!(
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
            None => return false,
        };

        let settings = match &v4_state.settings {
            Some(s) => s.clone(),
            None => return false,
        };

        // Use the network tip slot (actual chain tip) for the validity interval,
        // falling back to the last processed block slot.
        let current_slot = v4_state.network_tip_slot.unwrap_or(v4_state.tip_slot);

        // Select collateral
        let ada_asset = crate::cardano_types::AssetClass { policy: vec![], token: vec![] };
        let collateral = v4_state
            .wallet_utxos
            .iter()
            .find(|(_, v)| {
                use num_traits::ToPrimitive;
                v.get(&ada_asset).clone().unwrap().to_u64().unwrap_or(0) >= 5_000_000
            });
        let (collateral_input, collateral_value) = match collateral {
            Some((input, value)) => (input.clone(), value.clone()),
            None => return false,
        };

        // Filter orders: exclude in-flight ones, sort oldest first
        let in_flight_inputs = self.v4_chain_tracker.in_flight_order_inputs();
        let mut candidates: Vec<_> = v4_state
            .orders
            .iter()
            .filter(|o| !in_flight_inputs.contains(&o.input))
            .cloned()
            .collect();
        candidates.sort_by_key(|o| o.slot);

        if candidates.is_empty() {
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
                    warn!(error = %e, "failed to build script store from ref UTxOs");
                    return false;
                }
            }
        }
        let script_store = self.v4_script_store.as_ref().unwrap();

        // ── Incremental accumulation loop ──────────────────────────────────

        let mut accum = Accumulator::new(exec.fee, exec.protocol_share);
        let mut last_good: Option<(Accumulator, MultiPoolBuildResult)> = None;

        for order in &candidates {
            if accum.order_count() >= self.v4_batch_limits.max_orders {
                break;
            }

            // Match order to pool
            let pool_ident = match &order.datum.constraints {
                crate::sundaev4::OrderConstraints::Structured { steps } => {
                    steps.first().map(|s| s.pool_ident.clone())
                }
                crate::sundaev4::OrderConstraints::Simple { .. } => {
                    batch::find_pool_for_simple_order(order, &v4_state.pools)
                }
            };
            let Some(pool_ident) = pool_ident else { continue };

            // Get effective pool for this pool:
            // 1. If already in the accumulator, it uses its own running state
            // 2. If chain tracker has a predicted pool, use that
            // 3. Otherwise, use on-chain state
            let effective_pool = if accum.pools.contains_key(&pool_ident) {
                // Pool already in accumulator — the accum handles running state internally
                // We still need a reference pool for try_add_order's initial state
                // (but it's only used if the pool isn't already in the accum)
                accum.pools[&pool_ident].pool.clone()
            } else {
                match self.v4_chain_tracker.latest_predicted_pool(&pool_ident) {
                    Some(predicted) => predicted.pool.clone(),
                    None => match v4_state.pools.get(&pool_ident) {
                        Some(p) => p.clone(),
                        None => continue,
                    },
                }
            };

            // Clone + try add
            let mut candidate = accum.clone();
            if candidate.try_add_order(&order, &pool_ident, &effective_pool).is_err() {
                continue;
            }

            // Build + evaluate
            let batches = candidate.clone().into_batches();
            let build = match crate::sundaev4::tx_builder::build_multi_pool_scoop_tx(
                &batches, &settings, &exec, current_slot, language_views,
                &collateral_input.0, &collateral_value, None, &v4_state.ref_utxo_outputs,
            ) {
                Ok(r) => r,
                Err(e) => {
                    trace!(error = %e, order = %order.input, "incremental build failed, skipping order");
                    continue;
                }
            };

            let eval = match crate::sundaev4::evaluator::evaluate_scoop_tx(
                &build.tx_body,
                &build.redeemers,
                &build.resolved_inputs,
                &build.resolved_ref_inputs,
                script_store,
                &exec.plutus_v3_cost_model,
                build.tx_hash,
            ) {
                Ok(r) => r,
                Err(e) => {
                    trace!(error = %e, order = %order.input, "incremental eval failed, skipping order");
                    continue;
                }
            };

            // Check limits
            let total_mem: u64 = eval.budgets.iter().map(|(_, eu)| eu.mem).sum();
            let total_steps: u64 = eval.budgets.iter().map(|(_, eu)| eu.steps).sum();
            let tx_size = build.cbor.len();

            // Apply 20% safety margin to check whether padded values would exceed limits
            let padded_mem = total_mem * 6 / 5;
            let padded_steps = total_steps * 6 / 5;

            trace!(
                order = %order.input,
                n_orders = candidate.order_count(),
                mem = total_mem,
                steps = total_steps,
                tx_size,
                "incremental eval"
            );

            if padded_mem > Self::MAX_TX_EX_MEM
                || padded_steps > Self::MAX_TX_EX_STEPS
                || tx_size > Self::MAX_TX_SIZE
            {
                // Over limits — stop adding orders, submit previous state
                info!(
                    n_orders = candidate.order_count(),
                    mem = padded_mem,
                    steps = padded_steps,
                    tx_size,
                    "over limits, submitting previous accumulation"
                );
                break;
            }

            // Within limits — accept this candidate
            last_good = Some((accum.clone(), build));
            accum = candidate;
        }

        // ── Submit the accumulated tx ──────────────────────────────────────

        if accum.is_empty() {
            return false;
        }

        // Use last_good's build result for eval budgets, rebuild with padding
        let Some((_prev_accum, last_build)) = last_good else {
            return false;
        };

        // Re-evaluate the final accumulator state for accurate budgets
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

        let eval_result = match crate::sundaev4::evaluator::evaluate_scoop_tx(
            &first_pass.tx_body,
            &first_pass.redeemers,
            &first_pass.resolved_inputs,
            &first_pass.resolved_ref_inputs,
            script_store,
            &exec.plutus_v3_cost_model,
            first_pass.tx_hash,
        ) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, tx_hash = %first_pass.tx_hash_hex, "final multi-pool eval failed");
                return false;
            }
        };

        // Apply 20% safety margin
        let padded_budgets: Vec<_> = eval_result.budgets.iter().map(|(k, eu)| {
            (k.clone(), pallas_primitives::ExUnits {
                mem: eu.mem * 6 / 5,
                steps: eu.steps * 6 / 5,
            })
        }).collect();

        let final_tx = match crate::sundaev4::tx_builder::build_multi_pool_scoop_tx(
            &final_batches, &settings, &exec, current_slot, language_views,
            &collateral_input.0, &collateral_value, Some(&padded_budgets),
            &v4_state.ref_utxo_outputs,
        ) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "final multi-pool tx rebuild failed");
                return false;
            }
        };

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

        let _ = last_build; // was used for limit checking during accumulation

        match crate::sundaev4::submit::submit_tx(&exec.submit_url, &final_tx.cbor).await {
            Ok(submitted_hash) => {
                info!(tx_hash = %submitted_hash, n_orders, n_pools, "multi-pool scoop tx submitted");

                // Collect consumed orders from all batches
                let consumed_orders: Vec<_> = final_batches.iter()
                    .flat_map(|b| b.swaps.iter().map(|s| s.order.clone()))
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
                    tx_hash_hex: final_tx.tx_hash_hex,
                    pool_idents: pool_idents.clone(),
                    consumed_orders,
                    predicted_pools,
                    ttl: final_tx.ttl,
                    chain_index: pool_idents.iter()
                        .map(|i| self.v4_chain_tracker.next_chain_index(i))
                        .max()
                        .unwrap_or(0),
                };
                self.v4_chain_tracker.record_submission(in_flight);
                true
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("BadInputsUTxO") {
                    let pool_strs: Vec<String> = pool_idents.iter().map(|i| i.to_string()).collect();
                    info!(
                        tx_hash = %final_tx.tx_hash_hex,
                        pools = ?pool_strs,
                        "lost scoop race — pool or order UTxO already spent by another scooper"
                    );
                } else {
                    warn!(error = %msg, tx_hash = %final_tx.tx_hash_hex, "multi-pool scoop tx submit failed");
                }
                for ident in &pool_idents {
                    self.v4_chain_tracker.discard_chain_and_related(ident);
                }
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

#[derive(Debug, PartialEq, Serialize)]
enum OrderInvalidReason {
    NoPools,
    ValueError(ValueError),
    PoolErrors(BTreeMap<Ident, PoolError>),
}
