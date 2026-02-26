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
use tracing::{trace, warn};

use crate::{
    bigint::BigInt,
    cardano_types::TransactionInput,
    events::IndexEvent,
    sundaev3::{
        Ident, PoolError, SingletonValue, SundaeV3HistoricalState, SundaeV3Order, SundaeV3Pool,
        ValueError, estimate_whether_in_range, validate_order_for_pool, validate_order_value,
    },
};

pub struct Scooper {
    event_rx: tokio::sync::broadcast::Receiver<(u64, Vec<IndexEvent>)>,
    v3_state: Option<Arc<Mutex<SundaeV3HistoricalState>>>,
    trace_directory: Option<PathBuf>,
}

impl Scooper {
    pub fn new(
        trace_directory: Option<PathBuf>,
        event_rx: tokio::sync::broadcast::Receiver<(u64, Vec<IndexEvent>)>,
        v3_state: Option<Arc<Mutex<SundaeV3HistoricalState>>>,
    ) -> Result<Self> {
        if let Some(dir) = &trace_directory {
            fs::create_dir_all(dir)?;
        }
        Ok(Self {
            event_rx,
            v3_state,
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

    async fn process_events(&self, slot: u64, events: Vec<IndexEvent>) {
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
