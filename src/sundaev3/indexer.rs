use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use acropolis_common::{BlockInfo, Point};
use acropolis_module_custom_indexer::chain_index::ChainIndex;
use anyhow::{Result, bail};
use async_trait::async_trait;
use pallas_addresses::Address;
use pallas_traverse::{Era, MultiEraOutput, MultiEraTx};
use tokio::sync::{Mutex, watch};
use tracing::{trace, warn};

use crate::{
    SundaeV3Protocol,
    cardano_types::{self, Datum, TransactionInput},
    historical_state::HistoricalState,
    persistence::{PersistedTxo, SundaeV3Dao, SundaeV3TxChanges},
    sundaev3::{Ident, SundaeV3Order, SundaeV3Pool},
};

#[derive(Debug, Clone, Default)]
pub struct SundaeV3State {
    pub pools: BTreeMap<Ident, Arc<SundaeV3Pool>>,
    pub orders: Vec<Arc<SundaeV3Order>>,
}

pub type SundaeV3HistoricalState = HistoricalState<SundaeV3State>;

#[derive(Clone, Debug, Default)]
pub struct SundaeV3Update {
    pub slot: u64,
    pub tip_slot: Option<u64>,
    pub state: SundaeV3State,
}
impl SundaeV3Update {
    #[allow(unused)]
    pub fn is_at_tip(&self) -> bool {
        self.tip_slot.is_some_and(|s| s <= self.slot)
    }
}

pub struct SundaeV3Indexer {
    state: Arc<Mutex<SundaeV3HistoricalState>>,
    broadcaster: watch::Sender<SundaeV3Update>,
    protocol: SundaeV3Protocol,
    rollback_limit: u64,
    dao: Box<dyn SundaeV3Dao>,
}

impl SundaeV3Indexer {
    pub fn new(
        state: Arc<Mutex<SundaeV3HistoricalState>>,
        broadcaster: watch::Sender<SundaeV3Update>,
        protocol: SundaeV3Protocol,
        rollback_limit: u64,
        dao: Box<dyn SundaeV3Dao>,
    ) -> Self {
        Self {
            state,
            broadcaster,
            protocol,
            rollback_limit,
            dao,
        }
    }

    pub async fn load(&mut self) -> Result<()> {
        let txos = self.dao.load_txos().await?;
        let mut slot = 0;
        let mut state = SundaeV3State::default();
        for txo in txos {
            let era = Era::try_from(txo.era)?;
            let parsed = MultiEraOutput::decode(era, &txo.txo)?;
            let output = cardano_types::convert_transaction_output(&parsed);
            slot = slot.max(txo.created_slot);
            match txo.txo_type.as_str() {
                "pool" => {
                    let Datum::ParsedPool(pool_datum) = output.datum else {
                        bail!("invalid pool datum");
                    };
                    state.pools.insert(
                        pool_datum.ident.clone(),
                        Arc::new(SundaeV3Pool {
                            input: txo.txo_id,
                            address: output.address,
                            value: output.value,
                            pool_datum,
                            slot: txo.created_slot,
                        }),
                    );
                }
                "order" => {
                    let Datum::ParsedOrder(datum) = &output.datum else {
                        bail!("invalid order datum");
                    };
                    state.orders.push(Arc::new(SundaeV3Order {
                        input: txo.txo_id,
                        datum: datum.clone(),
                        output,
                        slot: txo.created_slot,
                    }));
                }
                other => bail!("unrecognized txo type \"{other}\""),
            }
        }
        *self.state.lock().await.update_slot(slot)? = state.clone();
        self.broadcaster.send_replace(SundaeV3Update {
            slot,
            tip_slot: None,
            state,
        });
        Ok(())
    }
}

#[async_trait]
impl ChainIndex for SundaeV3Indexer {
    fn name(&self) -> String {
        "sundae-v3".to_string()
    }

    async fn handle_onchain_tx_bytes(&mut self, info: &BlockInfo, raw_tx: &[u8]) -> Result<()> {
        let tx = MultiEraTx::decode(raw_tx)?;
        let this_tx_hash = tx.hash();
        trace!("Ingesting tx: {}", hex::encode(this_tx_hash));
        let mut history = self.state.lock().await;

        let state = history.update_slot(info.slot)?;
        let mut changes = SundaeV3TxChanges::new(info.slot, info.number);

        let spent_inputs = tx
            .inputs()
            .into_iter()
            .map(|i| {
                TransactionInput(pallas_primitives::TransactionInput {
                    transaction_id: *i.hash(),
                    index: i.index(),
                })
            })
            .collect::<BTreeSet<_>>();
        state.pools.retain(|_, pool| {
            if spent_inputs.contains(&pool.input) {
                changes.spent_txos.push(pool.input.clone());
                false
            } else {
                true
            }
        });
        state.orders.retain(|order| {
            if spent_inputs.contains(&order.input) {
                changes.spent_txos.push(order.input.clone());
                false
            } else {
                true
            }
        });

        for (ix, output) in tx.outputs().iter().enumerate() {
            let address = output.address()?;
            if payment_hash_equals(&address, &self.protocol.pool_script_hash) {
                let this_input = TransactionInput(pallas_primitives::TransactionInput {
                    transaction_id: this_tx_hash,
                    index: ix as u64,
                });
                let tx_out = cardano_types::convert_transaction_output(output);
                if let Datum::ParsedPool(pd) = tx_out.datum {
                    changes.created_txos.push(PersistedTxo {
                        txo_id: this_input.clone(),
                        txo_type: "pool".to_string(),
                        created_slot: info.slot,
                        era: output.era().into(),
                        txo: output.encode(),
                    });

                    let pool_id = pd.ident.clone();
                    let pool_record = SundaeV3Pool {
                        input: this_input,
                        address: tx_out.address,
                        value: tx_out.value,
                        pool_datum: pd,
                        slot: info.slot,
                    };
                    state.pools.insert(pool_id, Arc::new(pool_record));
                }
            } else if payment_hash_equals(&address, &self.protocol.order_script_hash) {
                let this_input = TransactionInput(pallas_primitives::TransactionInput {
                    transaction_id: this_tx_hash,
                    index: ix as u64,
                });
                let tx_out = cardano_types::convert_transaction_output(output);
                if let Datum::ParsedOrder(od) = &tx_out.datum {
                    changes.created_txos.push(PersistedTxo {
                        txo_id: this_input.clone(),
                        txo_type: "order".to_string(),
                        created_slot: info.slot,
                        era: output.era().into(),
                        txo: output.encode(),
                    });

                    let datum = od.clone();
                    let order = SundaeV3Order {
                        input: this_input,
                        output: tx_out,
                        datum,
                        slot: info.slot,
                    };
                    state.orders.push(Arc::new(order));
                }
            }
        }

        if !changes.is_empty() {
            self.dao.apply_tx_changes(changes).await?;
            self.broadcaster.send_replace(SundaeV3Update {
                slot: info.slot,
                tip_slot: info.tip_slot,
                state: state.clone(),
            });
        }

        if history.prune_history(self.rollback_limit)
            && let Some(min_height) = info.number.checked_sub(self.rollback_limit)
        {
            self.dao.prune_txos(min_height).await?;
        }

        Ok(())
    }

    async fn handle_rollback(&mut self, point: &Point) -> Result<()> {
        match point {
            Point::Origin => {
                self.reset(point).await?;
            }
            Point::Specific { slot, .. } => {
                warn!("rolling back to {point}");
                let mut history = self.state.lock().await;
                history.rollback_to_slot(*slot);
            }
        }
        self.dao.rollback(point.slot()).await?;
        self.broadcaster.send_replace(SundaeV3Update {
            slot: point.slot(),
            tip_slot: None,
            state: self.state.lock().await.latest().into_owned(),
        });
        Ok(())
    }

    async fn reset(&mut self, point: &Point) -> Result<Point> {
        warn!("clearing all state and resetting to {point}");
        self.state.lock().await.rollback_to_origin();
        Ok(point.clone())
    }
}

fn payment_hash_equals(addr: &Address, hash: &[u8]) -> bool {
    if let Address::Shelley(s_addr) = addr {
        s_addr.payment().as_hash() == hash
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;

    use acropolis_common::{BlockHash, BlockIntent, BlockStatus, Era};
    use pallas_traverse::MultiEraBlock;

    struct NoOpSundaeV3Dao;

    #[async_trait]
    impl SundaeV3Dao for NoOpSundaeV3Dao {
        async fn apply_tx_changes(&self, changes: SundaeV3TxChanges) -> Result<()> {
            let _ = changes;
            Ok(())
        }
        async fn rollback(&self, slot: u64) -> Result<()> {
            let _ = slot;
            Ok(())
        }
        async fn load_txos(&self) -> Result<Vec<PersistedTxo>> {
            Ok(vec![])
        }
        async fn prune_txos(&self, min_height: u64) -> Result<()> {
            let _ = min_height;
            Ok(())
        }
    }

    async fn handle_block(indexer: &mut SundaeV3Indexer, block: MultiEraBlock<'_>) -> Result<()> {
        let info = BlockInfo {
            status: BlockStatus::Volatile,
            intent: BlockIntent::none(),
            slot: block.slot(),
            number: 0,
            hash: BlockHash::new(*block.hash()),
            epoch: 0,
            epoch_slot: 0,
            new_epoch: false,
            tip_slot: None,
            timestamp: 0,
            era: Era::Conway,
        };
        for tx in block.txs() {
            let raw_tx = tx.encode();
            indexer.handle_onchain_tx_bytes(&info, &raw_tx).await?
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_block() {
        let state = Arc::new(Mutex::new(SundaeV3HistoricalState::new()));
        let protocol_file = fs::File::open("testdata/protocol").unwrap();
        let protocol = serde_json::from_reader(protocol_file).unwrap();
        let mut indexer = SundaeV3Indexer::new(
            state.clone(),
            watch::Sender::default(),
            protocol,
            2160,
            Box::new(NoOpSundaeV3Dao),
        );
        let block_bytes = std::fs::read("testdata/scoop-pool.block").unwrap();
        let block = pallas_traverse::MultiEraBlock::decode(&block_bytes).unwrap();
        let ada_policy: Vec<u8> = vec![];
        let ada_token: Vec<u8> = vec![];
        let pool_policy: Vec<u8> = vec![
            68, 161, 235, 45, 159, 88, 173, 212, 235, 25, 50, 189, 0, 72, 230, 161, 148, 126, 133,
            227, 254, 79, 50, 149, 106, 17, 4, 20,
        ];
        let pool_token: Vec<u8> = vec![
            0, 13, 225, 64, 50, 196, 63, 9, 111, 160, 86, 38, 218, 30, 173, 147, 131, 121, 60, 205,
            123, 186, 106, 27, 37, 158, 119, 89, 119, 102, 174, 232,
        ];
        let coin_b_policy: Vec<u8> = vec![
            145, 212, 243, 130, 39, 63, 68, 47, 21, 233, 218, 72, 203, 35, 52, 155, 162, 117, 248,
            129, 142, 76, 122, 197, 209, 0, 74, 22,
        ];
        let coin_b_token: Vec<u8> = vec![77, 121, 85, 83, 68];
        handle_block(&mut indexer, block).await.unwrap();
        let mut index = state.lock().await.latest().into_owned();
        assert_eq!(index.pools.len(), 1);
        let first_pool = index.pools.first_entry().unwrap();
        let pool_value = &first_pool.get().value.0;
        assert_eq!(pool_value[&ada_policy][&ada_token], 6181255175);
        assert_eq!(pool_value[&pool_policy][&pool_token], 1);
        assert_eq!(pool_value[&coin_b_policy][&coin_b_token], 6397550387);
        assert_eq!(index.orders.len(), 0);
    }

    #[tokio::test]
    async fn test_rollback() {
        let state = Arc::new(Mutex::new(SundaeV3HistoricalState::new()));
        let protocol_file = fs::File::open("testdata/protocol").unwrap();
        let protocol = serde_json::from_reader(protocol_file).unwrap();
        let mut indexer = SundaeV3Indexer::new(
            state.clone(),
            watch::Sender::default(),
            protocol,
            2160,
            Box::new(NoOpSundaeV3Dao),
        );
        let block_bytes = std::fs::read("testdata/scoop-pool.block").unwrap();
        let block = pallas_traverse::MultiEraBlock::decode(&block_bytes).unwrap();
        let pool_id = Ident::new(
            &hex::decode("32c43f096fa05626da1ead9383793ccd7bba6a1b259e77597766aee8").unwrap(),
        );

        handle_block(&mut indexer, block.clone()).await.unwrap();
        {
            // The block contains a pool scoop, which results in a pool state being recorded.
            let index = state.lock().await.latest().into_owned();
            assert!(index.pools.contains_key(&pool_id));
        }

        let rollback_block_point = Point::Specific {
            slot: block.slot() - 1,
            hash: BlockHash::new([0; 32]),
        };

        indexer
            .handle_rollback(&rollback_block_point)
            .await
            .unwrap();
        {
            // After rollback, all record of this pool is gone
            let index = state.lock().await.latest().into_owned();
            assert!(!index.pools.contains_key(&pool_id));
        }
    }
}
