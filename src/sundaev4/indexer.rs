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
use tokio::sync::{Mutex, broadcast};
use tracing::{debug, trace, warn};

use crate::{
    cardano_types::{self, AssetClass, TransactionInput, TransactionOutput},
    datum_lookup::{DatumLookup, ScopedDatumLookup},
    events::IndexEvent,
    historical_state::HistoricalState,
    persistence::{IndexerDao, PersistedDatum, PersistedTxo, TxChanges},
    sundaev3::Ident,
    sundaev4::{
        OrderRedeemer, PoolDatum, SettingsDatum, SundaeV4Order, SundaeV4Pool,
        SundaeV4Protocol, SundaeV4Settings, VaultRedeemer,
    },
};

const CIP_67_ASSET_LABEL_222: &[u8] = &[0x00, 0x0d, 0xe1, 0x40];
const METADATA_DATUM_KEY: u64 = 103251;

#[derive(Debug, Clone, Default)]
pub struct SundaeV4State {
    pub pools: BTreeMap<Ident, Arc<SundaeV4Pool>>,
    pub orders: Vec<Arc<SundaeV4Order>>,
    pub settings: Option<Arc<SundaeV4Settings>>,
    datums: DatumLookup,
}

pub type SundaeV4HistoricalState = HistoricalState<SundaeV4State>;

pub struct SundaeV4Indexer {
    state: Arc<Mutex<SundaeV4HistoricalState>>,
    event_tx: broadcast::Sender<(u64, Vec<IndexEvent>)>,
    protocol: SundaeV4Protocol,
    rollback_limit: u64,
    dao: Box<dyn IndexerDao>,
}

impl SundaeV4Indexer {
    pub fn new(
        state: Arc<Mutex<SundaeV4HistoricalState>>,
        event_tx: broadcast::Sender<(u64, Vec<IndexEvent>)>,
        protocol: SundaeV4Protocol,
        rollback_limit: u64,
        dao: Box<dyn IndexerDao>,
    ) -> Self {
        Self {
            state,
            event_tx,
            protocol,
            rollback_limit,
            dao,
        }
    }

    pub async fn load(&mut self) -> Result<()> {
        let txos = self.dao.load_txos().await?;
        let datums = self.dao.load_datums().await?;
        let mut slot = 0;
        let mut state = SundaeV4State::default();
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
                        pool_datum.identifier.clone(),
                        Arc::new(SundaeV4Pool {
                            input: txo.txo_id,
                            value: output.value,
                            pool_datum,
                            slot: txo.created_slot,
                        }),
                    );
                }
                "order" => {
                    let Some(datum) = output.datum.parse(&datums) else {
                        bail!("invalid order datum");
                    };
                    state.orders.push(Arc::new(SundaeV4Order {
                        input: txo.txo_id,
                        datum,
                        value: output.value,
                        slot: txo.created_slot,
                    }));
                }
                "settings" => {
                    let Some(datum) = output.datum.parse(&datums) else {
                        bail!("invalid settings datum");
                    };
                    state.settings = Some(Arc::new(SundaeV4Settings {
                        input: txo.txo_id,
                        datum,
                        slot: txo.created_slot,
                    }));
                }
                other => bail!("unrecognized txo type \"{other}\""),
            }
        }
        *self.state.lock().await.update_slot(slot)? = state;
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
        // v4: pool NFT policy is separate from vault script hash
        let mut asset_name = CIP_67_ASSET_LABEL_222.to_vec();
        asset_name.extend_from_slice(&pool_datum.identifier);
        let nft_asset_id = AssetClass {
            policy: self.protocol.pool_nft_policy.to_vec(),
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
}

#[async_trait]
impl ChainIndex for SundaeV4Indexer {
    fn name(&self) -> String {
        "sundae-v4".to_string()
    }

    async fn handle_onchain_tx_bytes(&mut self, info: &BlockInfo, raw_tx: &[u8]) -> Result<()> {
        let slot = info.slot;
        let tx = MultiEraTx::decode(raw_tx)?;
        let this_tx_hash = tx.hash();
        trace!("v4: Ingesting tx: {}", hex::encode(this_tx_hash));
        let mut history = self.state.lock().await;

        let mut updated_pools = BTreeMap::new();
        let mut new_orders = vec![];
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
            debug!(slot, hash = %hex::encode(&persisted.hash), "v4: metadata datum spotted");
            changes.metadata_datums.push(persisted);
            state.datums.add_metadata_datum(new_datum);
        }

        let datums = state.datums.for_tx(&tx);

        // Scan outputs for vault/order/settings script hashes
        for (ix, output) in tx.outputs().iter().enumerate() {
            let address = output.address()?;
            if payment_hash_equals(&address, &self.protocol.vault_script_hash) {
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

                    let pool_id = pd.identifier.clone();
                    let pool_record = SundaeV4Pool {
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
                if let Some(od) = tx_out.datum.parse(&datums) {
                    changes.created_txos.push(PersistedTxo {
                        txo_id: this_input.clone(),
                        txo_type: "order".to_string(),
                        created_slot: slot,
                        era: output.era().into(),
                        txo: output.encode(),
                        address: tx_out.address.to_vec(),
                        datum: tx_out.hashed_datum(&datums),
                    });

                    let order = SundaeV4Order {
                        input: this_input,
                        value: tx_out.value,
                        datum: od,
                        slot,
                    };
                    new_orders.push(Arc::new(order));
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
                    new_settings = Some(Arc::new(SundaeV4Settings {
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

        let mut scooped_orders = BTreeSet::new();
        let mut scoop_pool_id: Option<Ident> = None;
        let mut removed_pool_ids: Vec<Ident> = vec![];

        // Remove spent pools. Track vault redeemer actions.
        state.pools.retain(|ident, pool| {
            let Ok(spend_index) = spent_inputs.binary_search(&pool.input) else {
                return true;
            };
            match self.parse_redeemer::<VaultRedeemer>(&tx, spend_index) {
                Some(VaultRedeemer::Action { .. }) => {
                    // Pool was scooped — we track the pool ident for order event attribution
                    scoop_pool_id = Some(ident.clone());
                }
                Some(VaultRedeemer::EscapeHatch { .. })
                | Some(VaultRedeemer::Upgrade)
                | Some(VaultRedeemer::EmergencyDisable { .. }) => {
                    // Non-scoop vault operation
                }
                None => {
                    warn!(slot, %ident, "v4: pool spent without a valid redeemer!");
                    removed_pool_ids.push(ident.clone());
                }
            }
            changes.spent_txos.push(pool.input.clone());
            false
        });

        // Remove spent orders
        state.orders.retain(|order| {
            let Ok(spend_index) = spent_inputs.binary_search(&order.input) else {
                return true;
            };
            match self.parse_redeemer::<OrderRedeemer>(&tx, spend_index) {
                Some(OrderRedeemer::Scoop { .. }) => {
                    scooped_orders.insert(spend_index);
                    if let Some(pool_id) = &scoop_pool_id {
                        events.push(IndexEvent::V4OrderScooped {
                            order: order.clone(),
                            pool_id: pool_id.clone(),
                        });
                    }
                }
                Some(OrderRedeemer::Cancel) => {
                    events.push(IndexEvent::V4OrderCancelled {
                        order: order.clone(),
                    });
                }
                None => {
                    warn!(slot, order = %order.input, "v4: order spent without a valid redeemer!");
                }
            }
            changes.spent_txos.push(order.input.clone());
            false
        });

        // Emit pool removed events
        for id in removed_pool_ids {
            events.push(IndexEvent::V4PoolRemoved { id });
        }

        // Remove old settings if spent
        if let Some(settings) = &state.settings
            && spent_inputs.contains(&settings.input)
        {
            changes.spent_txos.push(settings.input.clone());
            state.settings = None;
        }

        // Apply new pool state — emit events for new/updated pools
        for (id, pool) in &updated_pools {
            if state.pools.contains_key(id) {
                events.push(IndexEvent::V4PoolUpdated {
                    id: id.clone(),
                    pool: pool.clone(),
                });
            } else {
                events.push(IndexEvent::V4PoolCreated {
                    id: id.clone(),
                    pool: pool.clone(),
                });
            }
        }
        state.pools.append(&mut updated_pools);

        // Emit events for new orders
        for order in &new_orders {
            events.push(IndexEvent::V4OrderCreated {
                order: order.clone(),
            });
        }
        state.orders.append(&mut new_orders);

        if let Some(settings) = new_settings {
            events.push(IndexEvent::V4SettingsUpdated {
                settings: settings.clone(),
            });
            state.settings = Some(settings);
        }

        if !changes.is_empty() {
            self.dao.apply_tx_changes(changes).await?;
        }

        if !events.is_empty() {
            let _ = self.event_tx.send((slot, events));
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
                warn!("v4: rolling back to {point}");
                let mut history = self.state.lock().await;
                history.rollback_to_slot(*slot);
            }
        }
        let to_slot = point.slot();
        self.dao.rollback(to_slot).await?;
        let _ = self
            .event_tx
            .send((to_slot, vec![IndexEvent::Rollback { to_slot }]));
        Ok(())
    }

    async fn reset(&mut self, point: &Point) -> Result<Point> {
        warn!("v4: clearing all state and resetting to {point}");
        self.dao.rollback(0).await?;
        self.state.lock().await.rollback_to_origin();
        Ok(point.clone())
    }
}

fn payment_hash_equals(addr: &Address, hash: &ScriptHash) -> bool {
    if let Address::Shelley(s_addr) = addr {
        s_addr.payment().as_hash() == hash
    } else {
        false
    }
}
