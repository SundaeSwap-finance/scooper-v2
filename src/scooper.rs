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
    cardano_types::TransactionInput,
    sundaev3::{
        Ident, PoolError, SundaeV3Order, SundaeV3Pool, SundaeV3State, SundaeV3Update, ValueError,
        estimate_whether_in_range, validate_order_for_pool, validate_order_value,
    },
};

pub struct Scooper {
    sundaev3: watch::Receiver<SundaeV3Update>,
    policy: Vec<u8>,
    orders: BTreeMap<TransactionInput, OrderValidity>,
}

impl Scooper {
    pub fn new(sundaev3: watch::Receiver<SundaeV3Update>, policy: &[u8]) -> Result<Self> {
        fs::create_dir_all(LOG_DIR)?;
        Ok(Self {
            sundaev3,
            policy: policy.to_vec(),
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
            self.log_orders(update.slot, &update.state);
        }
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
                    if old_validity != validity {
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

    fn validate_order(
        &self,
        order: &SundaeV3Order,
        pools: &BTreeMap<Ident, Arc<SundaeV3Pool>>,
    ) -> OrderValidity {
        if let Err(err) = validate_order_value(&order.datum, &order.output.value) {
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
                estimate_whether_in_range(&self.policy, &order.datum, &pool.pool_datum, &pool.value)
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

    fn write_updates(&self, updates: &[OrderState]) -> Result<()> {
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
