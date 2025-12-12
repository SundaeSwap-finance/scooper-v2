use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use acropolis_common::{BlockInfo, Point};
use acropolis_module_custom_indexer::chain_index::ChainIndex;
use anyhow::Result;
use async_trait::async_trait;
use pallas_addresses::Address;
use pallas_traverse::MultiEraTx;
use tokio::sync::{Mutex, watch};
use tracing::{trace, warn};

use crate::{
    SundaeV3Protocol,
    cardano_types::{self, Datum, TransactionInput},
    historical_state::HistoricalState,
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
}

impl SundaeV3Indexer {
    pub fn new(
        state: Arc<Mutex<SundaeV3HistoricalState>>,
        broadcaster: watch::Sender<SundaeV3Update>,
        protocol: SundaeV3Protocol,
    ) -> Self {
        Self {
            state,
            broadcaster,
            protocol,
        }
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
        let mut changed = false;

        for (ix, output) in tx.outputs().iter().enumerate() {
            let address = output.address()?;
            if payment_part_equal(&address, &self.protocol.pool_address) {
                let this_input = TransactionInput(pallas_primitives::TransactionInput {
                    transaction_id: this_tx_hash,
                    index: ix as u64,
                });
                let tx_out = cardano_types::convert_transaction_output(output);
                if let Datum::ParsedPool(pd) = tx_out.datum {
                    let pool_id = pd.ident.clone();
                    let pool_record = SundaeV3Pool {
                        input: this_input,
                        address: tx_out.address,
                        value: tx_out.value,
                        pool_datum: pd,
                        slot: info.slot,
                    };
                    state.pools.insert(pool_id, Arc::new(pool_record));
                    changed = true;
                }
            } else if payment_part_equal(&address, &self.protocol.order_address) {
                let this_input = TransactionInput(pallas_primitives::TransactionInput {
                    transaction_id: this_tx_hash,
                    index: ix as u64,
                });
                let tx_out = cardano_types::convert_transaction_output(output);
                if let Datum::ParsedOrder(od) = &tx_out.datum {
                    let datum = od.clone();
                    let order = SundaeV3Order {
                        input: this_input,
                        output: tx_out,
                        datum,
                        slot: info.slot,
                    };
                    state.orders.push(Arc::new(order));
                    changed = true;
                }
            }
        }

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
        let old_pool_count = state.pools.len();
        state
            .pools
            .retain(|_, pool| !spent_inputs.contains(&pool.input));
        if state.pools.len() != old_pool_count {
            changed = true;
        }
        let old_order_count = state.orders.len();
        state
            .orders
            .retain(|order| !spent_inputs.contains(&order.input));
        if state.orders.len() != old_order_count {
            changed = true;
        }

        if changed {
            self.broadcaster.send_replace(SundaeV3Update {
                slot: info.slot,
                tip_slot: info.tip_slot,
                state: state.clone(),
            });
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
        Ok(())
    }

    async fn reset(&mut self, point: &Point) -> Result<Point> {
        warn!("clearing all state and resetting to {point}");
        self.state.lock().await.rollback_to_origin();
        Ok(point.clone())
    }
}

fn payment_part_equal(a: &Address, b: &Address) -> bool {
    if let Address::Shelley(s_a) = a
        && let Address::Shelley(s_b) = b
    {
        return s_a.payment() == s_b.payment();
    }
    false
}

#[cfg(test)]
mod tests {
    use std::fs;

    use acropolis_common::{BlockHash, BlockIntent, BlockStatus, Era};
    use pallas_traverse::MultiEraBlock;

    use super::*;

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
        let state = Arc::new(Mutex::new(SundaeV3HistoricalState::new(2160)));
        let protocol_file = fs::File::open("testdata/protocol").unwrap();
        let protocol = serde_json::from_reader(protocol_file).unwrap();
        let mut indexer = SundaeV3Indexer::new(state.clone(), watch::Sender::default(), protocol);
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
        let state = Arc::new(Mutex::new(SundaeV3HistoricalState::new(2160)));
        let protocol_file = fs::File::open("testdata/protocol").unwrap();
        let protocol = serde_json::from_reader(protocol_file).unwrap();
        let mut indexer = SundaeV3Indexer::new(state.clone(), watch::Sender::default(), protocol);
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
