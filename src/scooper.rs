use std::{
    collections::BTreeMap,
    fs,
    io::{BufWriter, Write as _},
    path::PathBuf,
    sync::Arc,
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
        batch::{self, BatchLimits},
        chain_tracker::{ChainTracker, InFlightTx, PredictedPoolUtxo},
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
}

impl Scooper {
    pub fn new(
        trace_directory: Option<PathBuf>,
        event_rx: tokio::sync::broadcast::Receiver<(u64, Vec<IndexEvent>)>,
        v3_state: Option<Arc<Mutex<SundaeV3HistoricalState>>>,
        v4_state: Option<Arc<Mutex<SundaeV4HistoricalState>>>,
        v4_execution: Option<ScooperExecution>,
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

            // 3. Attempt batch cycle (build/eval/submit for each pool)
            let did_work = self.run_v4_batch_cycle().await;

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
                        info!(pool = %id, "v4 pool updated by competitor, discarding chain");
                        self.v4_chain_tracker.discard_chain(&id);
                    }
                }
                IndexEvent::V4PoolRemoved { id, .. } => {
                    trace!(slot, pool = %id, "v4 pool removed");
                    self.v4_chain_tracker.discard_chain(&id);
                }
                IndexEvent::V4OrderCreated { order } => {
                    trace!(slot, order = %order.input, "v4 order created");
                    // No immediate action — batch cycle picks it up
                }
                IndexEvent::V4OrderScooped { order, pool_id, .. } => {
                    trace!(slot, order = %order.input, pool = %pool_id, "v4 order scooped");
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

    /// Run one cycle of batch assembly → build → evaluate → submit for
    /// all pools with pending orders. Returns true if any batch was submitted.
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

        // Filter orders: exclude in-flight ones
        let in_flight_inputs = self.v4_chain_tracker.in_flight_order_inputs();
        let candidates: Vec<_> = v4_state
            .orders
            .iter()
            .filter(|o| !in_flight_inputs.contains(&o.input))
            .cloned()
            .collect();

        if candidates.is_empty() {
            return false;
        }

        // Group candidates by pool
        let groups = batch::group_orders_by_pool(&candidates, &v4_state.pools);
        if groups.is_empty() {
            return false;
        }

        let mut submitted_any = false;

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

        for (pool_ident, pool_orders) in &groups {
            // Get effective pool: use predicted pool from chain tracker if
            // we have an in-flight chain, otherwise use on-chain state
            let effective_pool = match self.v4_chain_tracker.latest_predicted_pool(pool_ident) {
                Some(predicted) => predicted.pool.clone(),
                None => match v4_state.pools.get(pool_ident) {
                    Some(p) => p.clone(),
                    None => continue,
                },
            };

            // Assemble batch
            let batch = match batch::assemble_batch(
                &effective_pool,
                pool_orders,
                exec.fee,
                exec.protocol_share,
                &self.v4_batch_limits,
            ) {
                Some(b) => b,
                None => {
                    trace!(
                        pool = %pool_ident,
                        n_candidates = pool_orders.len(),
                        "no executable orders for pool"
                    );
                    continue;
                }
            };

            let n_orders = batch.swaps.len();

            // Debug: log batch info
            info!(
                pool = %pool_ident,
                n_orders,
                a0 = %effective_pool.pool_datum.assets[0].1,
                b0 = %effective_pool.pool_datum.assets[1].1,
                a1 = %batch.final_assets[0].1,
                b1 = %batch.final_assets[1].1,
                lp_before = %effective_pool.pool_datum.total_lp,
                lp_after = %batch.final_total_lp,
                "assembling batch"
            );

            // Two-pass build → evaluate → rebuild → submit
            let first_pass = match crate::sundaev4::tx_builder::build_batch_scoop_tx(
                &batch, &settings, &exec, current_slot, language_views,
                &collateral_input.0, &collateral_value, None, &v4_state.ref_utxo_outputs,
            ) {
                Ok(r) => r,
                Err(e) => {
                    warn!(error = %e, pool = %pool_ident, n_orders, "v4 batch tx build failed (first pass)");
                    continue;
                }
            };

            let eval_result = match crate::sundaev4::evaluator::evaluate_scoop_tx(
                &first_pass.tx_body,
                &first_pass.redeemers,
                &first_pass.resolved_inputs,
                &first_pass.resolved_ref_inputs,
                &script_store,
                &exec.plutus_v3_cost_model,
                first_pass.tx_hash,
            ) {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        error = %e,
                        tx_hash = %first_pass.tx_hash_hex,
                        pool = %pool_ident,
                        "v4 batch local evaluation failed"
                    );
                    continue;
                }
            };

            // Apply 20% safety margin
            let padded_budgets: Vec<_> = eval_result.budgets.iter().map(|(k, eu)| {
                (k.clone(), pallas_primitives::ExUnits {
                    mem: eu.mem * 6 / 5,
                    steps: eu.steps * 6 / 5,
                })
            }).collect();

            let final_tx = match crate::sundaev4::tx_builder::build_batch_scoop_tx(
                &batch, &settings, &exec, current_slot, language_views,
                &collateral_input.0, &collateral_value, Some(&padded_budgets),
                &v4_state.ref_utxo_outputs,
            ) {
                Ok(r) => r,
                Err(e) => {
                    warn!(error = %e, pool = %pool_ident, "v4 batch tx build failed (second pass)");
                    continue;
                }
            };

            info!(
                tx_hash = %final_tx.tx_hash_hex,
                pool = %pool_ident,
                n_orders,
                "v4 batch scoop tx built, submitting"
            );

            match crate::sundaev4::submit::submit_tx(&exec.submit_url, &final_tx.cbor).await {
                Ok(submitted_hash) => {
                    info!(tx_hash = %submitted_hash, pool = %pool_ident, n_orders, "v4 batch scoop tx submitted");

                    // Record in chain tracker
                    let consumed_orders: Vec<_> = batch.swaps.iter()
                        .map(|s| s.order.clone())
                        .collect();

                    let (predicted_input, predicted_pool) = final_tx.predicted_pool;
                    let in_flight = InFlightTx {
                        tx_hash: final_tx.tx_hash,
                        tx_hash_hex: final_tx.tx_hash_hex,
                        pool_idents: vec![pool_ident.clone()],
                        consumed_orders,
                        predicted_pools: vec![(pool_ident.clone(), PredictedPoolUtxo {
                            input: predicted_input,
                            pool: Arc::new(predicted_pool),
                        })],
                        ttl: final_tx.ttl,
                        chain_index: self.v4_chain_tracker.next_chain_index(pool_ident),
                    };
                    self.v4_chain_tracker.record_submission(in_flight);
                    submitted_any = true;
                }
                Err(e) => {
                    warn!(error = %e, tx_hash = %final_tx.tx_hash_hex, pool = %pool_ident, "v4 batch scoop tx submit failed");
                    self.v4_chain_tracker.discard_chain(pool_ident);
                }
            }
        }

        submitted_any
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
