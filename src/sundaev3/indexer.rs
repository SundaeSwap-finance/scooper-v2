use std::{collections::BTreeMap, sync::Arc};

use acropolis_common::{BlockInfo, Point};
use acropolis_module_custom_indexer::chain_index::ChainIndex;
use anyhow::{Result, bail};
use async_trait::async_trait;
use num_traits::Signed;
use pallas_addresses::Address;
use pallas_primitives::conway::RedeemerTag;
use pallas_traverse::{Era, MultiEraOutput, MultiEraTx};
use plutus_parser::AsPlutus;
use tokio::sync::{Mutex, watch};
use tracing::{trace, warn};

use crate::{
    SundaeV3Protocol,
    cardano_types::{self, AssetClass, Datum, TransactionInput, TransactionOutput},
    historical_state::HistoricalState,
    persistence::{PersistedTxo, SundaeV3Dao, SundaeV3TxChanges},
    sundaev3::{
        Ident, OrderRedeemer, PoolDatum, SundaeV3Order, SundaeV3Pool, pool::ScoopedPool,
        validate_order,
    },
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

const CIP_67_ASSET_LABEL_222: &[u8] = &[0x00, 0x0d, 0xe1, 0x40];

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
                    let Some(pool_datum) = self.parse_pool(&output) else {
                        bail!("invalid pool datum");
                    };
                    state.pools.insert(
                        pool_datum.ident.clone(),
                        Arc::new(SundaeV3Pool {
                            input: txo.txo_id,
                            value: output.value,
                            pool_datum,
                            slot: txo.created_slot,
                        }),
                    );
                }
                "order" => {
                    let Datum::ParsedOrder(datum) = output.datum else {
                        bail!("invalid order datum");
                    };
                    state.orders.push(Arc::new(SundaeV3Order {
                        input: txo.txo_id,
                        datum,
                        value: output.value,
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

    fn parse_pool(&self, tx_out: &TransactionOutput) -> Option<PoolDatum> {
        let Datum::ParsedPool(pool_datum) = &tx_out.datum else {
            return None;
        };
        let mut asset_name = CIP_67_ASSET_LABEL_222.to_vec();
        asset_name.extend_from_slice(&pool_datum.ident);
        let nft_asset_id = AssetClass {
            policy: self.protocol.pool_script_hash.clone(),
            token: asset_name,
        };
        if tx_out.value.get(&nft_asset_id).is_positive() {
            Some(pool_datum.clone())
        } else {
            None
        }
    }

    fn parse_order_redeemer(&self, tx: &MultiEraTx, spend_index: usize) -> Option<OrderRedeemer> {
        let redeemers = tx.redeemers();
        let redeemer = redeemers
            .iter()
            .find(|r| r.tag() == RedeemerTag::Spend && r.index() == spend_index as u32)?;
        OrderRedeemer::from_plutus(redeemer.data().clone()).ok()
    }

    fn apply_order(&self, slot: u64, order: &SundaeV3Order, pool: Option<&mut ScoopedPool>) {
        let Some(pool) = pool else {
            warn!(slot, order = %order.input, "order was scooped in a TX without exactly one pool update");
            return;
        };
        match validate_order(&order.datum, &order.value, &pool.datum, &pool.value) {
            Ok(()) => pool.apply_order(order),
            Err(error) => {
                warn!(slot, order = %order.input, ident = %pool.datum.ident, "invalid order was scooped: {error:#}");
            }
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

        let mut updated_pools = BTreeMap::new();
        let mut new_orders = vec![];
        let mut changes = SundaeV3TxChanges::new(info.slot, info.number);

        // Find which pools and orders have been updated in this transaction.
        // Do not apply those updates to our new state yet.
        for (ix, output) in tx.outputs().iter().enumerate() {
            let address = output.address()?;
            if payment_hash_equals(&address, &self.protocol.pool_script_hash) {
                let this_input = TransactionInput(pallas_primitives::TransactionInput {
                    transaction_id: this_tx_hash,
                    index: ix as u64,
                });
                let tx_out = cardano_types::convert_transaction_output(output);
                if let Some(pd) = self.parse_pool(&tx_out) {
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
                        value: tx_out.value,
                        pool_datum: pd,
                        slot: info.slot,
                    };
                    updated_pools.insert(pool_id, Arc::new(pool_record));
                }
            } else if payment_hash_equals(&address, &self.protocol.order_script_hash) {
                let this_input = TransactionInput(pallas_primitives::TransactionInput {
                    transaction_id: this_tx_hash,
                    index: ix as u64,
                });
                let tx_out = cardano_types::convert_transaction_output(output);
                if let Datum::ParsedOrder(od) = tx_out.datum {
                    changes.created_txos.push(PersistedTxo {
                        txo_id: this_input.clone(),
                        txo_type: "order".to_string(),
                        created_slot: info.slot,
                        era: output.era().into(),
                        txo: output.encode(),
                    });

                    let order = SundaeV3Order {
                        input: this_input,
                        value: tx_out.value,
                        datum: od,
                        slot: info.slot,
                    };
                    new_orders.push(Arc::new(order));
                }
            }
        }

        let state = history.update_slot(info.slot)?;

        // Capture the state of the pool before any orders got scooped
        let mut scooped_pool = if updated_pools.len() == 1 {
            let ident = updated_pools.keys().next().unwrap();
            state.pools.get(ident).map(|pool| ScoopedPool::new(pool))
        } else {
            None
        };

        let mut spent_inputs = tx
            .inputs()
            .into_iter()
            .map(|i| TransactionInput::new(*i.hash(), i.index()))
            .collect::<Vec<_>>();
        spent_inputs.sort();

        // If any orders were scooped, apply them to the pool
        state.orders.retain(|order| {
            let Ok(spend_index) = spent_inputs.binary_search(&order.input) else {
                // not spent
                return true;
            };
            match self.parse_order_redeemer(&tx, spend_index) {
                Some(OrderRedeemer::Scoop) => {
                    self.apply_order(info.slot, order, scooped_pool.as_mut())
                }
                Some(OrderRedeemer::Cancel) => {}
                None => warn!(order = %order.input, "order spent without a valid redeemer!"),
            }
            changes.spent_txos.push(order.input.clone());
            false
        });

        // Remove spent pools
        state.pools.retain(|_, pool| {
            if spent_inputs.binary_search(&pool.input).is_ok() {
                changes.spent_txos.push(pool.input.clone());
                false
            } else {
                true
            }
        });

        // And apply new pools
        for (ident, pool) in updated_pools {
            if let Some(scooped_pool) = &scooped_pool {
                let expected_lp = &scooped_pool.datum.circulating_lp;
                let observed_lp = &pool.pool_datum.circulating_lp;
                if expected_lp != observed_lp {
                    warn!(ident = %pool.pool_datum.ident, %expected_lp, %observed_lp, "pool has incorrect liquidity");
                }

                let expected_value = &scooped_pool.value;
                let observed_value = &pool.value;
                if expected_value != observed_value {
                    warn!(ident = %pool.pool_datum.ident, %expected_value, %observed_value, "pool has incorrect value");
                }
            }
            state.pools.insert(ident, pool);
        }
        state.orders.append(&mut new_orders);

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
        self.dao.rollback(0).await?;
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
    use crate::bigint::BigInt;

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
        assert_eq!(
            pool_value[&ada_policy][&ada_token],
            BigInt::from(6181255175i128)
        );
        assert_eq!(pool_value[&pool_policy][&pool_token], BigInt::from(1));
        assert_eq!(
            pool_value[&coin_b_policy][&coin_b_token],
            BigInt::from(6397550387i128)
        );
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
