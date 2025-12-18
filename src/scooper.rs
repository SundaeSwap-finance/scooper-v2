use std::{
    collections::BTreeMap,
    fs,
    io::{BufWriter, Write as _},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use serde::Serialize;
use tokio::{select, sync::watch};
use tokio_util::sync::CancellationToken;
use tracing::warn;

const LOG_DIR: &str = "logs";

use crate::{
    bigint::BigInt,
    cardano_types::TransactionInput,
    sundaev3::{
        Ident, PoolError, SingletonValue, SundaeV3Order, SundaeV3Pool, SundaeV3State,
        SundaeV3Update, ValueError, estimate_whether_in_range, validate_order_for_pool,
        validate_order_value,
    },
};

pub struct Scooper {
    sundaev3: watch::Receiver<SundaeV3Update>,
    pools: BTreeMap<Ident, PoolSummary>,
    orders: BTreeMap<TransactionInput, OrderValidity>,
}

impl Scooper {
    pub fn new(sundaev3: watch::Receiver<SundaeV3Update>) -> Result<Self> {
        fs::create_dir_all(LOG_DIR)?;
        Ok(Self {
            sundaev3,
            pools: BTreeMap::new(),
            orders: BTreeMap::new(),
        })
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        loop {
            select! {
                _ = shutdown.cancelled() => { break; }
                res = self.sundaev3.changed() => {
                    if res.is_err() {
                        break;
                    }
                }
            }

            // Sleep a bit to deduplicate updates to the state.
            tokio::time::sleep(Duration::from_millis(250)).await;

            let update = self.sundaev3.borrow_and_update().clone();
            // TODO: only "scoop" when we're at the head of the chain
            self.log_changes(update.slot, &update.state);
        }
    }

    fn log_changes(&mut self, slot: u64, state: &SundaeV3State) {
        self.log_orders(slot, state);
        self.log_pools(slot, state);
    }

    fn log_pools(&mut self, slot: u64, state: &SundaeV3State) {
        let mut new_pools = BTreeMap::new();
        for (ident, pool) in &state.pools {
            let (asset_a, asset_b) = pool.pool_datum.assets.clone();
            let amount_a = pool.value.get(&asset_a);
            let amount_b = pool.value.get(&asset_b);
            let summary = PoolSummary {
                assets: (
                    SingletonValue::new(asset_a, amount_a),
                    SingletonValue::new(asset_b, amount_b),
                ),
                liquidity: pool.pool_datum.circulating_lp.clone(),
                protocol_fees: pool.pool_datum.protocol_fees.clone(),
            };
            new_pools.insert(ident.clone(), summary);
        }

        let mut updates = vec![];
        for (ident, summary) in &new_pools {
            match self.pools.get(ident) {
                None => updates.push(PoolState {
                    slot,
                    pool: ident,
                    action: PoolAction::Added { summary },
                }),
                Some(old_summary) => {
                    if old_summary != summary {
                        updates.push(PoolState {
                            slot,
                            pool: ident,
                            action: PoolAction::Changed { summary },
                        });
                    }
                }
            }
        }
        for ident in self.pools.keys() {
            if !new_pools.contains_key(ident) {
                updates.push(PoolState {
                    slot,
                    pool: ident,
                    action: PoolAction::Removed,
                });
            }
        }

        if !updates.is_empty()
            && let Err(err) = self.write_updates(&updates)
        {
            warn!("could not log updates: {err:#}");
        }

        self.pools = new_pools;
    }

    fn log_orders(&mut self, slot: u64, state: &SundaeV3State) {
        let mut new_orders = BTreeMap::new();
        for order in &state.orders {
            let validity = self.validate_order(order, &state.pools);
            new_orders.insert(order.input.clone(), validity);
        }

        let mut updates = vec![];
        for (txo, validity) in &new_orders {
            match self.orders.get(txo) {
                None => updates.push(OrderState {
                    order: txo,
                    slot,
                    action: OrderAction::Added { valid: validity },
                }),
                Some(old_validity) => {
                    if self.validity_changed(old_validity, validity) {
                        updates.push(OrderState {
                            order: txo,
                            slot,
                            action: OrderAction::Changed { valid: validity },
                        });
                    }
                }
            }
        }
        for txo in self.orders.keys() {
            if !new_orders.contains_key(txo) {
                updates.push(OrderState {
                    order: txo,
                    slot,
                    action: OrderAction::Removed,
                });
            }
        }

        if !updates.is_empty()
            && let Err(err) = self.write_updates(&updates)
        {
            warn!("could not log updates: {err:#}");
        }

        self.orders = new_orders;
    }

    // Log if the order's valid state has changed, unless the change is just becuase the pool price changed
    fn validity_changed(&self, old: &OrderValidity, new: &OrderValidity) -> bool {
        match (old, new) {
            (
                OrderValidity::Invalid {
                    reason: OrderInvalidReason::PoolErrors(old_errors),
                },
                OrderValidity::Invalid {
                    reason: OrderInvalidReason::PoolErrors(new_errors),
                },
            ) => {
                if old_errors.len() != new_errors.len() {
                    return true;
                }
                for (ident, old_error) in old_errors {
                    let Some(new_error) = new_errors.get(ident) else {
                        return true;
                    };
                    let matching = match (old_error, new_error) {
                        (
                            PoolError::OutOfRange {
                                swap_price: old_price,
                                ..
                            },
                            PoolError::OutOfRange {
                                swap_price: new_price,
                                ..
                            },
                        ) => old_price != new_price,
                        (o, n) => o != n,
                    };
                    if !matching {
                        return true;
                    }
                }
                false
            }
            (o, n) => o != n,
        }
    }

    fn validate_order(
        &self,
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

    fn write_updates<T: Serialize>(&self, updates: &[T]) -> Result<()> {
        let date = chrono::Utc::now()
            .date_naive()
            .format("%Y-%m-%d")
            .to_string();
        let filename = format!("{date}.jsonl");
        let path: PathBuf = [LOG_DIR, &filename].iter().collect();
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

#[derive(Serialize)]
struct PoolState<'a> {
    slot: u64,
    pool: &'a Ident,
    action: PoolAction<'a>,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum PoolAction<'a> {
    Added {
        #[serde(flatten)]
        summary: &'a PoolSummary,
    },
    Changed {
        #[serde(flatten)]
        summary: &'a PoolSummary,
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
struct OrderState<'a> {
    slot: u64,
    order: &'a TransactionInput,
    action: OrderAction<'a>,
}
#[derive(Serialize)]
#[serde(tag = "type")]
enum OrderAction<'a> {
    Added {
        #[serde(flatten)]
        valid: &'a OrderValidity,
    },
    Changed {
        #[serde(flatten)]
        valid: &'a OrderValidity,
    },
    Removed,
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
