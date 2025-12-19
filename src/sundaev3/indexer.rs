use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

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
use tracing::{debug, trace, warn};

use crate::{
    SundaeV3Protocol,
    cardano_types::{self, AssetClass, Datum, TransactionInput, TransactionOutput},
    historical_state::HistoricalState,
    persistence::{PersistedTxo, SundaeV3Dao, SundaeV3TxChanges},
    sundaev3::{
        Ident, OrderRedeemer, PoolDatum, PoolRedeemer, SettingsDatum, SundaeV3Order, SundaeV3Pool,
        SundaeV3Settings, WrappedRedeemer, builder::ScoopBuilder, validate_order,
    },
};

#[derive(Debug, Clone, Default)]
pub struct SundaeV3State {
    pub pools: BTreeMap<Ident, Arc<SundaeV3Pool>>,
    pub orders: Vec<Arc<SundaeV3Order>>,
    pub settings: Option<Arc<SundaeV3Settings>>,
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
                "settings" => {
                    let Datum::ParsedSettings(datum) = output.datum else {
                        bail!("invalid settings datum");
                    };
                    state.settings = Some(Arc::new(SundaeV3Settings {
                        input: txo.txo_id,
                        datum,
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

    fn parse_settings(&self, tx_out: &TransactionOutput) -> Option<SettingsDatum> {
        let Datum::ParsedSettings(settings_datum) = &tx_out.datum else {
            return None;
        };
        if tx_out.value.get(&self.protocol.settings_nft).is_positive() {
            Some(settings_datum.clone())
        } else {
            None
        }
    }

    fn parse_redeemer<T: AsPlutus>(&self, tx: &MultiEraTx, spend_index: usize) -> Option<T> {
        let redeemers = tx.redeemers();
        let redeemer = redeemers
            .iter()
            .find(|r| r.tag() == RedeemerTag::Spend && r.index() == spend_index as u32)?;
        T::from_plutus(redeemer.data().clone()).ok()
    }

    fn apply_order(&self, slot: u64, order: &SundaeV3Order, scoop: &mut ScoopBuilder) {
        match validate_order(&order.datum, &order.value, &scoop.pool, &scoop.value) {
            Ok(()) => {
                if let Err(error) = scoop.apply_order(order) {
                    warn!(slot, order = %order.input, ident = %scoop.pool.ident, "could not apply order: {error:#}");
                }
            }
            Err(error) => {
                warn!(slot, order = %order.input, ident = %scoop.pool.ident, "invalid order was scooped: {error:#}");
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
        let slot = info.slot;
        let tx = MultiEraTx::decode(raw_tx)?;
        let this_tx_hash = tx.hash();
        trace!("Ingesting tx: {}", hex::encode(this_tx_hash));
        let mut history = self.state.lock().await;

        let mut updated_pools = BTreeMap::new();
        let mut new_orders = vec![];
        let mut new_settings = None;
        let mut changes = SundaeV3TxChanges::new(info.slot, info.number);

        // Find which pools and orders have been updated in this transaction.
        // Do not apply those updates to our new state yet.
        for (ix, output) in tx.outputs().iter().enumerate() {
            let address = output.address()?;
            if payment_hash_equals(&address, &self.protocol.pool_script_hash) {
                let this_input = TransactionInput::new(this_tx_hash, ix as u64);
                let tx_out = cardano_types::convert_transaction_output(output);
                if let Some(pd) = self.parse_pool(&tx_out) {
                    changes.created_txos.push(PersistedTxo {
                        txo_id: this_input.clone(),
                        txo_type: "pool".to_string(),
                        created_slot: slot,
                        era: output.era().into(),
                        txo: output.encode(),
                    });

                    let pool_id = pd.ident.clone();
                    let pool_record = SundaeV3Pool {
                        input: this_input,
                        value: tx_out.value,
                        pool_datum: pd,
                        slot,
                    };
                    updated_pools.insert(pool_id, Arc::new(pool_record));
                }
            } else if payment_hash_equals(&address, &self.protocol.order_script_hash) {
                let this_input = TransactionInput::new(this_tx_hash, ix as u64);
                let tx_out = cardano_types::convert_transaction_output(output);
                if let Datum::ParsedOrder(od) = tx_out.datum {
                    changes.created_txos.push(PersistedTxo {
                        txo_id: this_input.clone(),
                        txo_type: "order".to_string(),
                        created_slot: slot,
                        era: output.era().into(),
                        txo: output.encode(),
                    });

                    let order = SundaeV3Order {
                        input: this_input,
                        value: tx_out.value,
                        datum: od,
                        slot,
                    };
                    new_orders.push(Arc::new(order));
                }
            } else if payment_hash_equals(&address, &self.protocol.settings_script_hash) {
                let this_input = TransactionInput::new(this_tx_hash, ix as u64);
                let tx_out = cardano_types::convert_transaction_output(output);
                if let Some(sd) = self.parse_settings(&tx_out) {
                    changes.created_txos.push(PersistedTxo {
                        txo_id: this_input.clone(),
                        txo_type: "settings".to_string(),
                        created_slot: slot,
                        era: output.era().into(),
                        txo: output.encode(),
                    });
                    new_settings = Some(Arc::new(SundaeV3Settings {
                        input: this_input,
                        datum: sd,
                        slot,
                    }));
                }
            }
        }

        let state = history.update_slot(slot)?;

        let mut spent_inputs = tx
            .inputs()
            .into_iter()
            .map(|i| TransactionInput::new(*i.hash(), i.index()))
            .collect::<Vec<_>>();
        spent_inputs.sort();

        let mut scoops = vec![];

        // Remove spent pools. If they were spent to produce a scoop, track that.
        state.pools.retain(|ident, pool| {
            let Ok(spend_index) = spent_inputs.binary_search(&pool.input) else {
                // not spent
                return true;
            };
            match self.parse_redeemer(&tx, spend_index) {
                Some(WrappedRedeemer(PoolRedeemer::PoolScoop { input_order, .. })) => {
                    // TODO: validate scooper/SSEs
                    let mut orders = vec![];
                    for (index, _, _) in input_order {
                        orders.push(index as usize);
                    }
                    if let Some(settings) = state.settings.clone() {
                        scoops.push(Scoop {
                            builder: ScoopBuilder::new(pool, settings, orders.len()),
                            orders,
                        });
                    } else {
                        warn!(slot, %ident, "scoop attempted while we have no settings");
                    }
                }
                Some(WrappedRedeemer(PoolRedeemer::Manage)) => {
                    // pool's settings were updated, but no scoop was made
                }
                None => warn!(slot, %ident, "pool spent without a valid redeemer!"),
            }
            changes.spent_txos.push(pool.input.clone());
            false
        });

        let mut scooped_orders = BTreeSet::new();
        if scoops.len() > 1 {
            warn!(slot, tx = %tx.hash(), "one transaction contained multiple scoops");
        } else if let Some(mut scoop) = scoops.pop() {
            debug!(
                slot,
                "scooping pool: {}",
                serde_json::to_string(&scoop.builder.pool).unwrap()
            );
            debug!(
                slot,
                "scooping value: {}",
                serde_json::to_string(&scoop.builder.value).unwrap()
            );
            // Validate the scoop
            let ident = scoop.builder.pool.ident.clone();
            for order_index in scoop.orders {
                scooped_orders.insert(order_index);
                let Some(input) = spent_inputs.get(order_index) else {
                    warn!(slot, %ident, order_index, "invalid order index in scoop");
                    continue;
                };
                let Some(order) = state.orders.iter().find(|o| &o.input == input) else {
                    warn!(slot, %ident, %input, "unrecognized order in scoop");
                    continue;
                };
                debug!(slot, %ident, "applying order: {} {}", serde_json::to_string(&order.datum.action).unwrap(), serde_json::to_string(&order.value).unwrap());
                self.apply_order(slot, order, &mut scoop.builder);
            }
            if let Err(error) = scoop.builder.validate() {
                warn!(slot, %ident, "invalid scoop: {error:#}");
            }
            if let Some(final_pool) = updated_pools.get(&ident) {
                let expected_datum = &scoop.builder.pool.circulating_lp;
                let observed_datum = &final_pool.pool_datum.circulating_lp;
                if expected_datum != observed_datum {
                    warn!(slot, %ident, %expected_datum, %observed_datum, "pool has incorrect datum");
                }

                let expected_value = &scoop.builder.value;
                let observed_value = &final_pool.value;
                if expected_value != observed_value {
                    warn!(slot, %ident, %expected_value, %observed_value, "pool has incorrect value");
                }
            } else {
                warn!(slot, %ident, "scooped pool missing from outputs");
            }
        }

        // Remove spent orders from our state
        state.orders.retain(|order| {
            let Ok(spend_index) = spent_inputs.binary_search(&order.input) else {
                // not spent
                return true;
            };
            match self.parse_redeemer(&tx, spend_index) {
                Some(OrderRedeemer::Scoop) => {
                    if !scooped_orders.contains(&spend_index) {
                        warn!(slot, order = %order.input, spend_index, tx = %tx.hash(), "order had a Scoop redeemer but was not scooped");
                    }
                }
                Some(OrderRedeemer::Cancel) => {
                    if scooped_orders.contains(&spend_index) {
                        warn!(slot, order = %order.input, "order did not have a Scoop redeemer, but was scooped");
                    }
                }
                None => warn!(slot, order = %order.input, "order spent without a valid redeemer!"),
            }
            changes.spent_txos.push(order.input.clone());
            false
        });

        // remove old settings too
        if let Some(settings) = &state.settings
            && spent_inputs.contains(&settings.input)
        {
            changes.spent_txos.push(settings.input.clone());
            state.settings = None;
        }

        // And apply the new state
        state.pools.append(&mut updated_pools);
        state.orders.append(&mut new_orders);
        if let Some(settings) = new_settings {
            state.settings = Some(settings);
        }

        if !changes.is_empty() {
            self.dao.apply_tx_changes(changes).await?;
            self.broadcaster.send_replace(SundaeV3Update {
                slot,
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

struct Scoop {
    builder: ScoopBuilder,
    orders: Vec<usize>,
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
