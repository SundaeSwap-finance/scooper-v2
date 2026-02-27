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
    sundaev4::{SundaeV4HistoricalState, SundaeV4Order, SundaeV4Pool, ScooperExecution},
};

pub struct Scooper {
    event_rx: tokio::sync::broadcast::Receiver<(u64, Vec<IndexEvent>)>,
    v3_state: Option<Arc<Mutex<SundaeV3HistoricalState>>>,
    v4_state: Option<Arc<Mutex<SundaeV4HistoricalState>>>,
    v4_execution: Option<ScooperExecution>,
    v4_language_views: Option<Vec<u8>>,
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
            trace_directory,
        })
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        loop {
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
                IndexEvent::V3PoolUpdated { id, pool } => {
                    let summary = pool_summary(&pool);
                    trace!(slot, pool = %id, "pool updated");
                    updates.push(serde_json::to_value(PoolState {
                        slot,
                        pool: id,
                        action: PoolAction::Changed { summary },
                    }).unwrap());
                }
                IndexEvent::V3PoolRemoved { id } => {
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
                IndexEvent::V3OrderScooped { order, pool_id } => {
                    trace!(slot, order = %order.input, pool = %pool_id, "order scooped");
                    updates.push(serde_json::to_value(OrderState {
                        slot,
                        order: order.input.clone(),
                        action: OrderAction::Scooped { pool_id },
                    }).unwrap());
                }
                IndexEvent::V3OrderCancelled { order } => {
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
                IndexEvent::V4PoolUpdated { id, .. } => {
                    trace!(slot, pool = %id, "v4 pool updated");
                }
                IndexEvent::V4PoolRemoved { id } => {
                    trace!(slot, pool = %id, "v4 pool removed");
                }
                IndexEvent::V4OrderCreated { order } => {
                    trace!(slot, order = %order.input, "v4 order created");
                    if let Some(exec) = self.v4_execution.clone() {
                        self.try_scoop_v4_order(&order, &exec, slot).await;
                    }
                }
                IndexEvent::V4OrderScooped { order, pool_id } => {
                    trace!(slot, order = %order.input, pool = %pool_id, "v4 order scooped");
                }
                IndexEvent::V4OrderCancelled { order } => {
                    trace!(slot, order = %order.input, "v4 order cancelled");
                }
                IndexEvent::V4SettingsUpdated { .. } => {
                    trace!(slot, "v4 settings updated");
                }
                IndexEvent::Rollback { to_slot } => {
                    trace!(to_slot, "rollback");
                }
            }
        }

        if !updates.is_empty() {
            if let Err(err) = self.write_updates(&updates) {
                warn!("could not log updates: {err:#}");
            }
        }
    }

    async fn try_scoop_v4_order(
        &mut self,
        order: &Arc<SundaeV4Order>,
        exec: &ScooperExecution,
        _event_slot: u64,
    ) {
        if self.v4_language_views.is_none() {
            let lv = crate::sundaev4::submit::encode_language_views(&exec.plutus_v3_cost_model);
            info!("computed PlutusV3 language views ({} bytes) from config cost model ({} params)", lv.len(), exec.plutus_v3_cost_model.len());
            self.v4_language_views = Some(lv);
        }
        let language_views = self.v4_language_views.as_ref().unwrap();

        let v4_state = match &self.v4_state {
            Some(s) => s.lock().await.latest().into_owned(),
            None => return,
        };

        let settings = match &v4_state.settings {
            Some(s) => s.clone(),
            None => {
                warn!("v4 scoop: no settings available");
                return;
            }
        };

        let current_slot = v4_state.tip_slot;

        // Select a collateral UTxO from tracked wallet UTxOs (>= 5 ADA)
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
            None => {
                warn!("v4 scoop: no suitable collateral UTxO found (need >= 5 ADA)");
                return;
            }
        };

        // Find a matching pool: look for a pool whose assets overlap with the order
        let pool = match self.find_matching_v4_pool(&v4_state.pools, order) {
            Some(p) => p,
            None => {
                trace!(order = %order.input, "v4 scoop: no matching pool found");
                return;
            }
        };

        // Two-pass build→evaluate→rebuild→submit
        // First pass: build with generous default ExUnits
        let first_pass = match crate::sundaev4::tx_builder::build_scoop_tx(
            &pool, order, &settings, exec, current_slot, language_views,
            &collateral_input.0, &collateral_value, None, &v4_state.ref_utxo_outputs,
        ) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, order = %order.input, "v4 scoop tx build failed (first pass)");
                return;
            }
        };

        // Evaluate locally to get realistic ExUnits
        let script_store = match crate::sundaev4::evaluator::ScriptStore::from_config(&[]) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "failed to build script store");
                return;
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
                warn!(error = %e, tx_hash = %first_pass.tx_hash_hex, "v4 local evaluation failed, submitting first pass");
                // Fall back to submitting the first pass with generous ExUnits
                match crate::sundaev4::submit::submit_tx(&exec.submit_url, &first_pass.cbor).await {
                    Ok(h) => info!(tx_hash = %h, "v4 scoop tx submitted (unevaluated)"),
                    Err(e) => warn!(error = %e, "v4 scoop tx submit failed"),
                }
                return;
            }
        };

        // Apply 20% safety margin to evaluated budgets
        let padded_budgets: Vec<_> = eval_result.budgets.iter().map(|(k, eu)| {
            (k.clone(), pallas_primitives::ExUnits {
                mem: eu.mem * 6 / 5,
                steps: eu.steps * 6 / 5,
            })
        }).collect();

        // Second pass: rebuild with realistic ExUnits
        let final_tx = match crate::sundaev4::tx_builder::build_scoop_tx(
            &pool, order, &settings, exec, current_slot, language_views,
            &collateral_input.0, &collateral_value, Some(&padded_budgets), &v4_state.ref_utxo_outputs,
        ) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, order = %order.input, "v4 scoop tx build failed (second pass)");
                return;
            }
        };

        info!(
            tx_hash = %final_tx.tx_hash_hex,
            pool = %pool.pool_datum.identifier,
            "v4 scoop tx built with evaluated ExUnits, submitting"
        );
        match crate::sundaev4::submit::submit_tx(&exec.submit_url, &final_tx.cbor).await {
            Ok(submitted_hash) => {
                info!(tx_hash = %submitted_hash, "v4 scoop tx submitted");
            }
            Err(e) => {
                warn!(error = %e, tx_hash = %final_tx.tx_hash_hex, "v4 scoop tx submit failed");
            }
        }
    }

    fn find_matching_v4_pool(
        &self,
        pools: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
        order: &SundaeV4Order,
    ) -> Option<Arc<SundaeV4Pool>> {
        // Match via structured constraints: the order specifies a pool_ident
        if let crate::sundaev4::OrderConstraints::Structured { steps } = &order.datum.constraints {
            if let Some(step) = steps.first() {
                return pools.get(&step.pool_ident).cloned();
            }
        }

        // For simple constraints: find a pool where the order's non-ADA token matches a pool asset
        for (_ident, pool) in pools {
            for (asset, _) in &pool.pool_datum.assets {
                if asset.policy.is_empty() && asset.token.is_empty() {
                    continue;
                }
                let amount = order.value.get(asset);
                if amount > BigInt::from(0) {
                    return Some(pool.clone());
                }
            }
        }
        None
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

