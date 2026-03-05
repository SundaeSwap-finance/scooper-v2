use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use acropolis_common::{BlockInfo, Point};
use acropolis_module_custom_indexer::chain_index::ChainIndex;
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use num_traits::Signed;
use pallas_addresses::{Address, ScriptHash};
use pallas_crypto::hash::Hasher;
use pallas_primitives::conway::RedeemerTag;
use pallas_traverse::{Era, MultiEraOutput, MultiEraTx};
use plutus_parser::{AsPlutus, PlutusData};
use tokio::sync::{Mutex, broadcast, watch};
use tracing::{debug, trace, warn};

use crate::{
    cardano_types::{self, AssetClass, TransactionInput, TransactionOutput},
    datum_lookup::{DatumLookup, ScopedDatumLookup},
    events::{IndexEvent, InvalidOrder, SpentOrder, SpentOrderReason, SpentPool},
    historical_state::HistoricalState,
    persistence::{IndexerDao, PersistedDatum, PersistedTxo, SpentTxo, TxChanges},
    sundaev3::{
        Ident, OrderRedeemer, PoolDatum, PoolRedeemer, SettingsDatum, SignedStrategyExecution,
        SundaeV3Order, SundaeV3Pool, SundaeV3Protocol, SundaeV3Settings, WrappedRedeemer,
        builder::ScoopBuilder, validate_order,
    },
};

#[derive(Debug, Clone, Default)]
pub struct SundaeV3State {
    pub pools: BTreeMap<Ident, Arc<SundaeV3Pool>>,
    pub orders: Vec<Arc<SundaeV3Order>>,
    pub settings: Option<Arc<SundaeV3Settings>>,
    pub spent_orders: Vec<SpentOrder<SundaeV3Order>>,
    pub spent_pools: Vec<SpentPool<SundaeV3Pool>>,
    pub invalid_orders: Vec<InvalidOrder>,
    datums: DatumLookup,
}

pub type SundaeV3HistoricalState = HistoricalState<SundaeV3State>;

#[derive(Clone, Debug, Default)]
pub struct SundaeV3Update {
    pub slot: u64,
    pub tip_slot: Option<u64>,
}
impl SundaeV3Update {
    #[allow(unused)]
    pub fn is_at_tip(&self) -> bool {
        self.tip_slot.is_some_and(|s| s <= self.slot)
    }
}

use crate::cardano_types::{CIP_67_ASSET_LABEL_222, METADATA_DATUM_KEY};

pub struct SundaeV3Indexer {
    state: Arc<Mutex<SundaeV3HistoricalState>>,
    broadcaster: watch::Sender<SundaeV3Update>,
    event_tx: broadcast::Sender<(u64, Vec<IndexEvent>)>,
    protocol: SundaeV3Protocol,
    rollback_limit: u64,
    dao: Box<dyn IndexerDao>,
    /// The slot of the latest block loaded from DB. Blocks at or before this
    /// slot are skipped in handle_onchain_tx_bytes to avoid the
    /// "cannot update slot" error when the cursor lags behind the DB state.
    loaded_slot: u64,
}

impl SundaeV3Indexer {
    pub fn new(
        state: Arc<Mutex<SundaeV3HistoricalState>>,
        broadcaster: watch::Sender<SundaeV3Update>,
        event_tx: broadcast::Sender<(u64, Vec<IndexEvent>)>,
        protocol: SundaeV3Protocol,
        rollback_limit: u64,
        dao: Box<dyn IndexerDao>,
    ) -> Self {
        Self {
            state,
            broadcaster,
            event_tx,
            protocol,
            rollback_limit,
            dao,
            loaded_slot: 0,
        }
    }

    pub fn set_loaded_slot(&mut self, slot: u64) {
        self.loaded_slot = slot;
    }

    pub async fn load(&mut self) -> Result<()> {
        let txos = self.dao.load_txos().await?;
        let datums = self.dao.load_datums().await?;
        let mut slot = 0;
        let mut state = SundaeV3State::default();
        for datum in datums {
            let data = PlutusData::from_plutus_bytes(&datum.datum)
                .context("could not parse persisted datum")?;
            state
                .datums
                .add_metadata_datum((datum.datum.to_vec(), data));
        }

        for txo in txos {
            let era = Era::try_from(txo.era)?;
            let parsed = MultiEraOutput::decode(era, &txo.txo)?;
            let datum = match &txo.datum {
                Some(bytes) => {
                    let pd = minicbor::decode(bytes).context("could not parse persisted CBOR")?;
                    Some(pd)
                }
                None => None,
            };
            let datums = state.datums.for_persisted_txo(datum);
            let output = cardano_types::convert_txo(&parsed);
            slot = slot.max(txo.created_slot);
            match txo.txo_type.as_str() {
                "pool" => {
                    let Some(pool_datum) = self.parse_pool(&output, &datums) else {
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
                    match output.datum.try_parse(&datums) {
                        Ok(datum) => {
                            state.orders.push(Arc::new(SundaeV3Order {
                                input: txo.txo_id,
                                datum,
                                value: output.value,
                                slot: txo.created_slot,
                            }));
                        }
                        Err(reason) => {
                            warn!(slot = txo.created_slot, input = %txo.txo_id, "v3: invalid order datum on load: {reason}");
                            state.invalid_orders.push(InvalidOrder {
                                input: txo.txo_id,
                                slot: txo.created_slot,
                                reason,
                            });
                        }
                    }
                }
                "invalid_order" => {
                    match output.datum.try_parse(&datums) {
                        Ok(datum) => {
                            state.orders.push(Arc::new(SundaeV3Order {
                                input: txo.txo_id,
                                datum,
                                value: output.value,
                                slot: txo.created_slot,
                            }));
                        }
                        Err(reason) => {
                            state.invalid_orders.push(InvalidOrder {
                                input: txo.txo_id,
                                slot: txo.created_slot,
                                reason,
                            });
                        }
                    }
                }
                "settings" => {
                    let Some(datum) = output.datum.parse(&datums) else {
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

        // Recover spent orders/pools from DB
        let spent_since = slot.saturating_sub(self.rollback_limit);
        let spent_txos = self.dao.load_spent_txos(spent_since).await?;
        for stxo in spent_txos {
            let era = Era::try_from(stxo.txo.era)?;
            let parsed = MultiEraOutput::decode(era, &stxo.txo.txo)?;
            let datum = match &stxo.txo.datum {
                Some(bytes) => {
                    let pd = minicbor::decode(bytes).context("could not parse spent persisted CBOR")?;
                    Some(pd)
                }
                None => None,
            };
            let datums = state.datums.for_persisted_txo(datum);
            let output = cardano_types::convert_txo(&parsed);
            let tx_id = stxo.spent_tx_id.map(hex::encode).unwrap_or_default();
            match stxo.txo.txo_type.as_str() {
                "order" => {
                    if let Some(od) = output.datum.parse(&datums) {
                        state.spent_orders.push(SpentOrder {
                            order: Arc::new(SundaeV3Order {
                                input: stxo.txo.txo_id,
                                datum: od,
                                value: output.value,
                                slot: stxo.txo.created_slot,
                            }),
                            reason: SpentOrderReason::Unknown,
                            tx_id,
                            slot: stxo.spent_slot,
                        });
                    }
                }
                "pool" => {
                    if let Some(pd) = self.parse_pool(&output, &datums) {
                        state.spent_pools.push(SpentPool {
                            id: pd.ident.clone(),
                            old_pool: Arc::new(SundaeV3Pool {
                                input: stxo.txo.txo_id,
                                value: output.value,
                                pool_datum: pd,
                                slot: stxo.txo.created_slot,
                            }),
                            new_pool: None,
                            tx_id,
                            slot: stxo.spent_slot,
                        });
                    }
                }
                _ => {} // skip settings
            }
        }

        self.loaded_slot = slot;
        *self.state.lock().await.update_slot(slot)? = state.clone();
        self.broadcaster.send_replace(SundaeV3Update {
            slot,
            tip_slot: None,
        });
        Ok(())
    }

    fn extract_metadata_datums(&self, tx: &MultiEraTx) -> Vec<(Vec<u8>, PlutusData)> {
        use pallas_primitives::Metadatum;
        let metadata = tx.metadata();
        let Some(Metadatum::Map(kvps)) = metadata.find(METADATA_DATUM_KEY) else {
            return vec![];
        };
        let pairs: &Vec<_> = kvps;
        let mut result = vec![];
        'datum: for (_, val) in pairs {
            let Metadatum::Array(parts) = val else {
                continue 'datum;
            };
            let mut bytes = vec![];
            for part in parts {
                let Metadatum::Bytes(b) = part else {
                    continue 'datum;
                };
                bytes.extend_from_slice(b);
            }
            if let Ok(datum) = minicbor::decode(&bytes) {
                result.push((bytes, datum));
            }
        }
        result
    }

    fn parse_pool(
        &self,
        tx_out: &TransactionOutput,
        datums: &ScopedDatumLookup,
    ) -> Option<PoolDatum> {
        let pool_datum: PoolDatum = tx_out.datum.parse(datums)?;
        let mut asset_name = CIP_67_ASSET_LABEL_222.to_vec();
        asset_name.extend_from_slice(&pool_datum.ident);
        let nft_asset_id = AssetClass {
            policy: self.protocol.pool_script_hash.to_vec(),
            token: asset_name,
        };
        if tx_out.value.get(&nft_asset_id).is_positive() {
            Some(pool_datum)
        } else {
            None
        }
    }

    fn parse_settings(
        &self,
        tx_out: &TransactionOutput,
        datums: &ScopedDatumLookup,
    ) -> Option<SettingsDatum> {
        let settings_datum: SettingsDatum = tx_out.datum.parse(datums)?;
        if tx_out.value.get(&self.protocol.settings_nft).is_positive() {
            Some(settings_datum)
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

    fn apply_order(
        &self,
        slot: u64,
        order: &SundaeV3Order,
        sse: Option<SignedStrategyExecution>,
        scoop: &mut ScoopBuilder,
    ) {
        let action = match sse.as_ref() {
            Some(sse) => &sse.execution.details,
            None => &order.datum.action,
        };
        match validate_order(&order.datum, &order.value, &scoop.pool, &scoop.value) {
            Ok(()) => {
                if let Err(error) = scoop.apply_order(action, &order.value) {
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
        if info.slot <= self.loaded_slot {
            return Ok(());
        }
        let slot = info.slot;
        let tx = MultiEraTx::decode(raw_tx)?;
        let this_tx_hash = tx.hash();
        trace!("Ingesting tx: {}", hex::encode(this_tx_hash));
        let mut history = self.state.lock().await;

        let mut updated_pools = BTreeMap::new();
        let mut new_orders = vec![];
        let mut new_invalid_orders = vec![];
        let mut new_settings = None;
        let mut changes = TxChanges::new(info.slot, info.number);
        let mut events: Vec<IndexEvent> = vec![];

        let state = history.update_slot(slot)?;

        for new_datum in self.extract_metadata_datums(&tx) {
            let persisted = PersistedDatum {
                hash: Hasher::<256>::hash(&new_datum.0).to_vec(),
                datum: new_datum.0.clone(),
                created_slot: slot,
            };
            debug!(slot, hash = %hex::encode(&persisted.hash), "metadata datum spotted");
            changes.metadata_datums.push(persisted);
            state.datums.add_metadata_datum(new_datum);
        }

        let datums = state.datums.for_tx(&tx);

        // Find which pools and orders have been updated in this transaction.
        // Do not apply those updates to our new state yet.
        for (ix, output) in tx.outputs().iter().enumerate() {
            let address = output.address()?;
            if payment_hash_equals(&address, &self.protocol.pool_script_hash) {
                let this_input = TransactionInput::new(this_tx_hash, ix as u64);
                let tx_out = cardano_types::convert_txo(output);
                if let Some(pd) = self.parse_pool(&tx_out, &datums) {
                    changes.created_txos.push(PersistedTxo {
                        txo_id: this_input.clone(),
                        txo_type: "pool".to_string(),
                        created_slot: slot,
                        era: output.era().into(),
                        txo: output.encode(),
                        address: tx_out.address.to_vec(),
                        datum: tx_out.hashed_datum(&datums),
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
            } else if self
                .protocol
                .order_script_hashes
                .iter()
                .any(|hash| payment_hash_equals(&address, hash))
            {
                let this_input = TransactionInput::new(this_tx_hash, ix as u64);
                let tx_out = cardano_types::convert_txo(output);
                match tx_out.datum.try_parse(&datums) {
                    Ok(od) => {
                        changes.created_txos.push(PersistedTxo {
                            txo_id: this_input.clone(),
                            txo_type: "order".to_string(),
                            created_slot: slot,
                            era: output.era().into(),
                            txo: output.encode(),
                            address: tx_out.address.to_vec(),
                            datum: tx_out.hashed_datum(&datums),
                        });

                        let order = SundaeV3Order {
                            input: this_input,
                            value: tx_out.value,
                            datum: od,
                            slot,
                        };
                        new_orders.push(Arc::new(order));
                    }
                    Err(reason) => {
                        warn!(slot, input = %this_input, "v3: invalid order datum: {reason}");
                        changes.created_txos.push(PersistedTxo {
                            txo_id: this_input.clone(),
                            txo_type: "invalid_order".to_string(),
                            created_slot: slot,
                            era: output.era().into(),
                            txo: output.encode(),
                            address: tx_out.address.to_vec(),
                            datum: tx_out.hashed_datum(&datums),
                        });
                        new_invalid_orders.push(InvalidOrder {
                            input: this_input,
                            slot,
                            reason,
                        });
                    }
                }
            } else if payment_hash_equals(&address, &self.protocol.settings_script_hash) {
                let this_input = TransactionInput::new(this_tx_hash, ix as u64);
                let tx_out = cardano_types::convert_txo(output);
                if let Some(sd) = self.parse_settings(&tx_out, &datums) {
                    changes.created_txos.push(PersistedTxo {
                        txo_id: this_input.clone(),
                        txo_type: "settings".to_string(),
                        created_slot: slot,
                        era: output.era().into(),
                        txo: output.encode(),
                        address: tx_out.address.to_vec(),
                        datum: tx_out.hashed_datum(&datums),
                    });
                    new_settings = Some(Arc::new(SundaeV3Settings {
                        input: this_input,
                        datum: sd,
                        slot,
                    }));
                }
            }
        }

        let mut spent_inputs = tx
            .inputs()
            .into_iter()
            .map(|i| TransactionInput::new(*i.hash(), i.index()))
            .collect::<Vec<_>>();
        spent_inputs.sort();

        let tx_id_hex = hex::encode(this_tx_hash);
        let mut scoops = vec![];
        let mut removed_pool_ids: Vec<Ident> = vec![];

        // Remove spent pools. If they were spent to produce a scoop, track that.
        state.pools.retain(|ident, pool| {
            let Ok(spend_index) = spent_inputs.binary_search(&pool.input) else {
                // not spent
                return true;
            };
            // Record spent pool (new_pool filled in later if updated)
            state.spent_pools.push(SpentPool {
                id: ident.clone(),
                old_pool: pool.clone(),
                new_pool: None,
                tx_id: tx_id_hex.clone(),
                slot,
            });
            match self.parse_redeemer(&tx, spend_index) {
                Some(WrappedRedeemer(PoolRedeemer::PoolScoop { input_order, .. })) => {
                    // TODO: validate scooper/SSEs
                    let mut orders = vec![];
                    for (index, sse, _) in input_order {
                        orders.push((index as usize, sse));
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
                None => {
                    warn!(slot, %ident, "pool spent without a valid redeemer!");
                    removed_pool_ids.push(ident.clone());
                }
            }
            changes.spent_txos.push(SpentTxo {
                input: pool.input.clone(),
                spending_tx_id: this_tx_hash.to_vec(),
            });
            false
        });

        let mut scooped_orders = BTreeSet::new();
        let mut scoop_pool_id: Option<Ident> = None;
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
            scoop_pool_id = Some(ident.clone());
            for (order_index, sse) in scoop.orders {
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
                self.apply_order(slot, order, sse, &mut scoop.builder);
            }
            if let Err(error) = scoop.builder.validate() {
                warn!(slot, %ident, "invalid scoop: {error:#}");
            }
            if let Some(final_pool) = updated_pools.get(&ident) {
                let expected_datum = &scoop.builder.pool;
                let observed_datum = &final_pool.pool_datum;
                if expected_datum != observed_datum {
                    warn!(slot, %ident, expected_datum = serde_json::to_string(&expected_datum).unwrap(), observed_datum = serde_json::to_string(&observed_datum).unwrap(), "pool has incorrect datum");
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

        // Remove spent invalid orders
        state.invalid_orders.retain(|io| {
            if spent_inputs.binary_search(&io.input).is_ok() {
                changes.spent_txos.push(SpentTxo {
                    input: io.input.clone(),
                    spending_tx_id: this_tx_hash.to_vec(),
                });
                false
            } else {
                true
            }
        });

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
                    if let Some(pool_id) = &scoop_pool_id {
                        state.spent_orders.push(SpentOrder {
                            order: order.clone(),
                            reason: SpentOrderReason::Scooped { pool_ids: vec![pool_id.clone()], scooper: String::new() },
                            tx_id: tx_id_hex.clone(),
                            slot,
                        });
                        events.push(IndexEvent::V3OrderScooped {
                            order: order.clone(),
                            pool_id: pool_id.clone(),
                            tx_id: tx_id_hex.clone(),
                            scooper: String::new(),
                        });
                    }
                }
                Some(OrderRedeemer::Cancel) => {
                    if scooped_orders.contains(&spend_index) {
                        warn!(slot, order = %order.input, "order did not have a Scoop redeemer, but was scooped");
                    }
                    state.spent_orders.push(SpentOrder {
                        order: order.clone(),
                        reason: SpentOrderReason::Cancelled,
                        tx_id: tx_id_hex.clone(),
                        slot,
                    });
                    events.push(IndexEvent::V3OrderCancelled {
                        order: order.clone(),
                        tx_id: tx_id_hex.clone(),
                    });
                }
                None => warn!(slot, order = %order.input, "order spent without a valid redeemer!"),
            }
            changes.spent_txos.push(SpentTxo {
                input: order.input.clone(),
                spending_tx_id: this_tx_hash.to_vec(),
            });
            false
        });

        // Emit pool removed events for pools spent without scoop/manage
        for id in removed_pool_ids {
            events.push(IndexEvent::V3PoolRemoved { id, tx_id: tx_id_hex.clone() });
        }

        // remove old settings too
        if let Some(settings) = &state.settings
            && spent_inputs.contains(&settings.input)
        {
            changes.spent_txos.push(SpentTxo {
                input: settings.input.clone(),
                spending_tx_id: this_tx_hash.to_vec(),
            });
            state.settings = None;
        }

        // And apply the new state — emit events for new/updated pools
        for (id, pool) in &updated_pools {
            // Fill in new_pool on matching spent pool entries
            for sp in state.spent_pools.iter_mut().rev() {
                if sp.id == *id && sp.slot == slot && sp.new_pool.is_none() {
                    sp.new_pool = Some(pool.clone());
                    break;
                }
            }
            if state.pools.contains_key(id) {
                events.push(IndexEvent::V3PoolUpdated {
                    id: id.clone(),
                    pool: pool.clone(),
                    tx_id: tx_id_hex.clone(),
                });
            } else {
                events.push(IndexEvent::V3PoolCreated {
                    id: id.clone(),
                    pool: pool.clone(),
                });
            }
        }
        state.pools.append(&mut updated_pools);

        // Emit events for new orders
        for order in &new_orders {
            events.push(IndexEvent::V3OrderCreated {
                order: order.clone(),
            });
        }
        state.orders.append(&mut new_orders);
        state.invalid_orders.append(&mut new_invalid_orders);

        if let Some(settings) = new_settings {
            events.push(IndexEvent::V3SettingsUpdated {
                settings: settings.clone(),
            });
            state.settings = Some(settings);
        }

        if !changes.is_empty() {
            self.dao.apply_tx_changes(changes).await?;
            self.broadcaster.send_replace(SundaeV3Update {
                slot,
                tip_slot: info.tip_slot,
            });
        }

        if !events.is_empty() {
            let _ = self.event_tx.send((slot, events));
        }

        // Prune spent collections older than rollback window
        let cutoff = slot.saturating_sub(self.rollback_limit);
        state.spent_orders.retain(|s| s.slot >= cutoff);
        state.spent_pools.retain(|s| s.slot >= cutoff);
        state.invalid_orders.retain(|io| io.slot >= cutoff);

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
                let needs_reload = history.is_empty();
                drop(history);
                self.dao.rollback(*slot).await?;
                if needs_reload {
                    warn!("v3: history empty after rollback, rebuilding from DB");
                    self.load().await?;
                }
            }
        }
        let to_slot = point.slot();
        if matches!(point, Point::Origin) {
            self.dao.rollback(to_slot).await?;
        }
        self.broadcaster.send_replace(SundaeV3Update {
            slot: to_slot,
            tip_slot: None,
        });
        let _ = self
            .event_tx
            .send((to_slot, vec![IndexEvent::Rollback { to_slot }]));
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
    orders: Vec<(usize, Option<SignedStrategyExecution>)>,
}

fn payment_hash_equals(addr: &Address, hash: &ScriptHash) -> bool {
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
    use pallas_primitives::DatumHash;
    use pallas_traverse::MultiEraBlock;

    struct NoOpIndexerDao;

    #[async_trait]
    impl IndexerDao for NoOpIndexerDao {
        async fn apply_tx_changes(&self, changes: TxChanges) -> Result<()> {
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
        async fn load_spent_txos(&self, _since_slot: u64) -> Result<Vec<crate::persistence::SpentPersistedTxo>> {
            Ok(vec![])
        }
        async fn load_datums(&self) -> Result<Vec<PersistedDatum>> {
            Ok(vec![])
        }
        async fn prune_txos(&self, min_height: u64) -> Result<()> {
            let _ = min_height;
            Ok(())
        }
        async fn load_scoop_records(&self) -> Result<Vec<crate::persistence::ScoopRecord>> {
            Ok(vec![])
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
            is_new_era: false,
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
        let protocol_file = fs::File::open("testdata/protocol.json").unwrap();
        let protocol = serde_json::from_reader(protocol_file).unwrap();
        let (event_tx, _) = broadcast::channel(16);
        let mut indexer = SundaeV3Indexer::new(
            state.clone(),
            watch::Sender::default(),
            event_tx,
            protocol,
            2160,
            Box::new(NoOpIndexerDao),
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
        let protocol_file = fs::File::open("testdata/protocol.json").unwrap();
        let protocol = serde_json::from_reader(protocol_file).unwrap();
        let (event_tx, _) = broadcast::channel(16);
        let mut indexer = SundaeV3Indexer::new(
            state.clone(),
            watch::Sender::default(),
            event_tx,
            protocol,
            2160,
            Box::new(NoOpIndexerDao),
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

    #[tokio::test]
    async fn test_metadata_datums() {
        let state = Arc::new(Mutex::new(SundaeV3HistoricalState::new()));
        let protocol_file = fs::File::open("testdata/protocol.json").unwrap();
        let protocol = serde_json::from_reader(protocol_file).unwrap();
        let (event_tx, _) = broadcast::channel(16);
        let mut indexer = SundaeV3Indexer::new(
            state.clone(),
            watch::Sender::default(),
            event_tx,
            protocol,
            2160,
            Box::new(NoOpIndexerDao),
        );
        let block_bytes = std::fs::read("testdata/metadata.block").unwrap();
        let block = pallas_traverse::MultiEraBlock::decode(&block_bytes).unwrap();
        let datum_hash: DatumHash =
            "8ecfafddfa732227ba5b494183fd3150a4c8614656e6182f92c25ee2d1480019"
                .parse()
                .unwrap();
        handle_block(&mut indexer, block.clone()).await.unwrap();
        {
            let index = state.lock().await.latest().into_owned();
            assert!(index.datums.contains_metadata_datum(datum_hash));
        }
    }
}
