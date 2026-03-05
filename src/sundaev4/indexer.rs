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
    events::{IndexEvent, InvalidOrder, ScoopRecordView, ScoopStats, ScooperTotal, SpentOrder, SpentOrderReason, SpentPool},
    historical_state::HistoricalState,
    persistence::{IndexerDao, PersistedDatum, PersistedTxo, ScoopRecord, SpentTxo, TxChanges},
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
    pub spent_orders: Vec<SpentOrder<SundaeV4Order>>,
    pub spent_pools: Vec<SpentPool<SundaeV4Pool>>,
    pub invalid_orders: Vec<InvalidOrder>,
    pub tip_slot: u64,
    /// The actual network tip slot, as reported by the upstream node.
    /// `None` until the upstream node reports it (Dolos may not always provide this).
    pub network_tip_slot: Option<u64>,
    pub wallet_utxos: BTreeMap<crate::cardano_types::TransactionInput, crate::cardano_types::Value>,
    pub ref_utxo_outputs: BTreeMap<crate::cardano_types::TransactionInput, crate::cardano_types::TransactionOutput>,
    pub scoop_stats: ScoopStats,
    datums: DatumLookup,
}

pub type SundaeV4HistoricalState = HistoricalState<SundaeV4State>;

pub struct SundaeV4Indexer {
    state: Arc<Mutex<SundaeV4HistoricalState>>,
    event_tx: broadcast::Sender<(u64, Vec<IndexEvent>)>,
    protocol: SundaeV4Protocol,
    rollback_limit: u64,
    dao: Box<dyn IndexerDao>,
    scooper_address: Option<Address>,
    scooper_keyhash: Option<pallas_primitives::Hash<28>>,
    ref_utxo_inputs: BTreeSet<crate::cardano_types::TransactionInput>,
    tip_event_counter: u64,
    /// The slot of the latest block loaded from DB. Blocks at or before this
    /// slot are skipped in handle_block/handle_onchain_tx_bytes to avoid the
    /// "cannot update slot" error when the cursor lags behind the DB state.
    loaded_slot: u64,
}

impl SundaeV4Indexer {
    pub fn new(
        state: Arc<Mutex<SundaeV4HistoricalState>>,
        event_tx: broadcast::Sender<(u64, Vec<IndexEvent>)>,
        protocol: SundaeV4Protocol,
        rollback_limit: u64,
        dao: Box<dyn IndexerDao>,
    ) -> Self {
        let scooper_address = protocol.execution.as_ref().and_then(|exec| {
            derive_scooper_pallas_address(&exec.scooper_secret_key).ok()
        });
        let scooper_keyhash = protocol.execution.as_ref().and_then(|exec| {
            derive_scooper_keyhash(&exec.scooper_secret_key).ok()
        });
        let ref_utxo_inputs = protocol
            .execution
            .as_ref()
            .map(|exec| {
                let scripts = &exec.module_scripts;
                [
                    &scripts.vault,
                    &scripts.order,
                    &scripts.constant_product,
                    &scripts.fee_split,
                    &scripts.fairness,
                    &scripts.pool_mint,
                    &scripts.settings,
                ]
                .iter()
                .map(|s| s.ref_utxo.clone())
                .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        Self {
            state,
            event_tx,
            protocol,
            rollback_limit,
            dao,
            scooper_address,
            scooper_keyhash,
            ref_utxo_inputs,
            tip_event_counter: 0,
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
                    let pool_type = self.detect_pool_type(&pool_datum);
                    state.pools.insert(
                        pool_datum.identifier.clone(),
                        Arc::new(SundaeV4Pool {
                            input: txo.txo_id,
                            value: output.value,
                            pool_datum,
                            pool_type,
                            slot: txo.created_slot,
                        }),
                    );
                }
                "order" => {
                    match output.datum.try_parse(&datums) {
                        Ok(datum) => {
                            state.orders.push(Arc::new(SundaeV4Order {
                                input: txo.txo_id,
                                datum,
                                value: output.value,
                                slot: txo.created_slot,
                            }));
                        }
                        Err(reason) => {
                            warn!(slot = txo.created_slot, input = %txo.txo_id, "v4: invalid order datum on load: {reason}");
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
                            state.orders.push(Arc::new(SundaeV4Order {
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
                    state.settings = Some(Arc::new(SundaeV4Settings {
                        input: txo.txo_id,
                        value: output.value,
                        datum,
                        slot: txo.created_slot,
                    }));
                }
                "wallet" => {
                    state.wallet_utxos.insert(txo.txo_id, output.value);
                }
                "ref" => {
                    state.ref_utxo_outputs.insert(txo.txo_id, output);
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
                            order: Arc::new(SundaeV4Order {
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
                        let pool_type = self.detect_pool_type(&pd);
                        state.spent_pools.push(SpentPool {
                            id: pd.identifier.clone(),
                            old_pool: Arc::new(SundaeV4Pool {
                                input: stxo.txo.txo_id,
                                value: output.value,
                                pool_datum: pd,
                                pool_type,
                                slot: stxo.txo.created_slot,
                            }),
                            new_pool: None,
                            tx_id,
                            slot: stxo.spent_slot,
                        });
                    }
                }
                _ => {} // skip wallet, ref, settings
            }
        }

        // Load scoop records and build stats
        let scoop_records = self.dao.load_scoop_records().await?;
        state.scoop_stats = build_scoop_stats(
            &scoop_records,
            self.scooper_keyhash.as_ref(),
        );

        state.tip_slot = slot;
        self.loaded_slot = slot;
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

    /// Detect the pool type from its datum's action modules.
    ///
    /// Matches the swap action's first module hash against known module script
    /// hashes from config. Defaults to ConstantProduct if no execution config
    /// is available or no match is found.
    fn detect_pool_type(&self, pool_datum: &PoolDatum) -> crate::sundaev4::types::PoolType {
        use crate::sundaev4::types::{PoolType, Rational};
        use crate::bigint::BigInt;

        let Some(exec) = &self.protocol.execution else {
            // No execution config: default to CP with 0/1 fee (won't be used for scooping)
            return PoolType::ConstantProduct {
                fee: Rational { num: BigInt::from(0), den: BigInt::from(1) },
            };
        };

        // Find the swap action (tag == 100, enabled)
        let swap_action = pool_datum.actions.iter().find(|a| {
            a.tag == BigInt::from(100) && a.enabled
        });

        let Some(action) = swap_action else {
            return PoolType::ConstantProduct {
                fee: Rational {
                    num: BigInt::from(exec.fee.0),
                    den: BigInt::from(exec.fee.1),
                },
            };
        };

        let first_module = action.modules.first();

        // Check if the first module matches the constant_sum script hash
        if let (Some(module_hash), Some(cs_script)) = (first_module, &exec.module_scripts.constant_sum) {
            if module_hash.as_slice() == cs_script.hash.as_ref() {
                // CS pool — look up config from pool_configs
                let ident_hex = hex::encode(pool_datum.identifier.to_bytes());
                if let Some(crate::sundaev4::types::PoolConfig::ConstantSum { prices, fee }) =
                    exec.pool_configs.get(&ident_hex)
                {
                    return PoolType::ConstantSum {
                        prices: prices.iter().map(|p| BigInt::from(*p)).collect(),
                        fee: Rational {
                            num: BigInt::from(fee.0),
                            den: BigInt::from(fee.1),
                        },
                    };
                }

                // No config found — use default prices (1:1) and global fee
                eprintln!("[WARN] CS pool {} has no pool-config entry, using defaults", ident_hex);
                return PoolType::ConstantSum {
                    prices: vec![BigInt::from(1); pool_datum.assets.len()],
                    fee: Rational {
                        num: BigInt::from(exec.fee.0),
                        den: BigInt::from(exec.fee.1),
                    },
                };
            }
        }

        // Default: constant product
        PoolType::ConstantProduct {
            fee: Rational {
                num: BigInt::from(exec.fee.0),
                den: BigInt::from(exec.fee.1),
            },
        }
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

    async fn handle_block(&mut self, info: &BlockInfo) -> Result<()> {
        if info.slot <= self.loaded_slot {
            // Block already covered by load(). Update network tip and emit
            // TipAdvanced so the scooper can detect sync progress, but skip
            // the state mutation that would fail with "cannot update slot".
            let mut history = self.state.lock().await;
            let state = history.update_slot(self.loaded_slot)?;
            if let Some(tip) = info.tip_slot {
                state.network_tip_slot = Some(tip);
            }
            self.tip_event_counter += 1;
            let at_tip = state.network_tip_slot
                .is_some_and(|net| self.loaded_slot + 10 >= net);
            if at_tip || self.tip_event_counter % 100 == 0 {
                let _ = self.event_tx.send((
                    info.slot,
                    vec![IndexEvent::TipAdvanced {
                        slot: info.slot,
                        network_tip_slot: info.tip_slot,
                    }],
                ));
            }
            return Ok(());
        }

        let mut history = self.state.lock().await;
        let state = history.update_slot(info.slot)?;
        state.tip_slot = info.slot;
        if let Some(tip) = info.tip_slot {
            state.network_tip_slot = Some(tip);
        }
        // Throttle tip events: every 100 blocks while syncing, every block once synced.
        self.tip_event_counter += 1;
        let at_tip = state.network_tip_slot
            .is_some_and(|net| info.slot + 10 >= net);
        if at_tip || self.tip_event_counter % 100 == 0 {
            let _ = self.event_tx.send((
                info.slot,
                vec![IndexEvent::TipAdvanced {
                    slot: info.slot,
                    network_tip_slot: info.tip_slot,
                }],
            ));
        }
        Ok(())
    }

    async fn handle_onchain_tx_bytes(&mut self, info: &BlockInfo, raw_tx: &[u8]) -> Result<()> {
        if info.slot <= self.loaded_slot {
            return Ok(());
        }
        let slot = info.slot;
        let tx = MultiEraTx::decode(raw_tx)?;
        let this_tx_hash = tx.hash();
        trace!("v4: Ingesting tx: {}", hex::encode(this_tx_hash));
        let mut history = self.state.lock().await;

        let mut updated_pools = BTreeMap::new();
        let mut new_orders = vec![];
        let mut new_invalid_orders = vec![];
        let mut new_settings = None;
        let mut changes = TxChanges::new(info.slot, info.number);
        let mut events: Vec<IndexEvent> = vec![];

        // handle_block already called update_slot for this slot;
        // this just retrieves the existing mutable reference.
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
                    let pool_type = self.detect_pool_type(&pd);
                    let pool_record = SundaeV4Pool {
                        input: this_input,
                        value: tx_out.value,
                        pool_datum: pd,
                        pool_type,
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

                        let order = SundaeV4Order {
                            input: this_input,
                            value: tx_out.value,
                            datum: od,
                            slot,
                        };
                        new_orders.push(Arc::new(order));
                    }
                    Err(reason) => {
                        warn!(slot, input = %this_input, "v4: invalid order datum: {reason}");
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
                    new_settings = Some(Arc::new(SundaeV4Settings {
                        input: this_input,
                        value: tx_out.value,
                        datum: sd,
                        slot,
                    }));
                }
            }

            // Track wallet UTxOs (scooper's own address)
            if let Some(scooper_addr) = &self.scooper_address {
                if address_equals(&address, scooper_addr) {
                    let this_input = TransactionInput::new(this_tx_hash, ix as u64);
                    let tx_out = cardano_types::convert_txo(output);
                    trace!(slot, utxo = %this_input, "v4: wallet UTxO spotted");
                    changes.created_txos.push(PersistedTxo {
                        txo_id: this_input.clone(),
                        txo_type: "wallet".to_string(),
                        created_slot: slot,
                        era: output.era().into(),
                        txo: output.encode(),
                        address: tx_out.address.to_vec(),
                        datum: None,
                    });
                    state.wallet_utxos.insert(this_input, tx_out.value);
                }
            }

            // Track reference UTxO outputs (for ScriptContext building)
            {
                let this_input = TransactionInput::new(this_tx_hash, ix as u64);
                if self.ref_utxo_inputs.contains(&this_input) {
                    let tx_out = cardano_types::convert_txo(output);
                    trace!(slot, utxo = %this_input, "v4: ref UTxO output spotted");
                    changes.created_txos.push(PersistedTxo {
                        txo_id: this_input.clone(),
                        txo_type: "ref".to_string(),
                        created_slot: slot,
                        era: output.era().into(),
                        txo: output.encode(),
                        address: tx_out.address.to_vec(),
                        datum: None,
                    });
                    state.ref_utxo_outputs.insert(this_input, tx_out);
                }
            }
        }

        let mut spent_inputs = tx
            .inputs()
            .into_iter()
            .map(|i| TransactionInput::new(*i.hash(), i.index()))
            .collect::<Vec<_>>();
        spent_inputs.sort();

        // Remove spent wallet UTxOs
        for input in &spent_inputs {
            if state.wallet_utxos.remove(input).is_some() {
                changes.spent_txos.push(SpentTxo {
                    input: input.clone(),
                    spending_tx_id: this_tx_hash.to_vec(),
                });
            }
        }

        let mut scooped_orders = BTreeSet::new();
        let mut scoop_pool_ids: Vec<Ident> = vec![];
        let mut removed_pool_ids: Vec<Ident> = vec![];
        // Track which pool idents existed before we remove spent UTxOs,
        // so we can distinguish V4PoolUpdated (scoop) from V4PoolCreated (new).
        let known_pool_idents: BTreeSet<Ident> = state.pools.keys().cloned().collect();

        // Extract scooper keyhash from tx's required_signers
        let req_signers = tx.required_signers();
        let signers: Vec<&pallas_primitives::Hash<28>> = req_signers.collect();
        let scooper_keyhash_bytes: Option<Vec<u8>> = signers.first().map(|h| h.to_vec());
        let scooper_hex = scooper_keyhash_bytes.as_ref()
            .map(|b| hex::encode(b))
            .unwrap_or_default();

        let tx_id_hex = hex::encode(this_tx_hash);

        // Remove spent pools. Track vault redeemer actions.
        state.pools.retain(|ident, pool| {
            let Ok(spend_index) = spent_inputs.binary_search(&pool.input) else {
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
            match self.parse_redeemer::<VaultRedeemer>(&tx, spend_index) {
                Some(VaultRedeemer::Action { .. }) => {
                    // Pool was scooped — collect all scooped pool idents
                    scoop_pool_ids.push(ident.clone());
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
            changes.spent_txos.push(SpentTxo {
                input: pool.input.clone(),
                spending_tx_id: this_tx_hash.to_vec(),
            });
            false
        });

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

        // Remove spent orders
        state.orders.retain(|order| {
            let Ok(spend_index) = spent_inputs.binary_search(&order.input) else {
                return true;
            };
            match self.parse_redeemer::<OrderRedeemer>(&tx, spend_index) {
                Some(OrderRedeemer::Scoop { .. }) => {
                    scooped_orders.insert(spend_index);
                    if !scoop_pool_ids.is_empty() {
                        state.spent_orders.push(SpentOrder {
                            order: order.clone(),
                            reason: SpentOrderReason::Scooped { pool_ids: scoop_pool_ids.clone(), scooper: scooper_hex.clone() },
                            tx_id: tx_id_hex.clone(),
                            slot,
                        });
                        events.push(IndexEvent::V4OrderScooped {
                            order: order.clone(),
                            pool_ids: scoop_pool_ids.clone(),
                            tx_id: tx_id_hex.clone(),
                            scooper: scooper_hex.clone(),
                        });
                    }
                }
                Some(OrderRedeemer::Cancel) => {
                    state.spent_orders.push(SpentOrder {
                        order: order.clone(),
                        reason: SpentOrderReason::Cancelled,
                        tx_id: tx_id_hex.clone(),
                        slot,
                    });
                    events.push(IndexEvent::V4OrderCancelled {
                        order: order.clone(),
                        tx_id: tx_id_hex.clone(),
                    });
                }
                None => {
                    warn!(slot, order = %order.input, "v4: order spent without a valid redeemer!");
                }
            }
            changes.spent_txos.push(SpentTxo {
                input: order.input.clone(),
                spending_tx_id: this_tx_hash.to_vec(),
            });
            false
        });

        // Record scoop if orders were scooped
        if !scooped_orders.is_empty() && !scoop_pool_ids.is_empty() {
            let n_orders = scooped_orders.len() as u32;
            let scooper_bytes = scooper_keyhash_bytes.clone().unwrap_or_default();

            for pool_id in &scoop_pool_ids {
                changes.scoop_records.push(ScoopRecord {
                    tx_id: this_tx_hash.to_vec(),
                    slot,
                    pool_id: pool_id.to_bytes().to_vec(),
                    n_orders,
                    scooper: scooper_bytes.clone(),
                });
            }

            // Update in-memory stats (once per tx, not per pool)
            let scooper_key = scooper_hex.clone();
            if let Some(total) = state.scoop_stats.scooper_totals.iter_mut().find(|t| t.scooper == scooper_key) {
                total.scoop_txs += 1;
                total.orders_processed += n_orders as u64;
            } else {
                state.scoop_stats.scooper_totals.push(ScooperTotal {
                    scooper: scooper_key,
                    scoop_txs: 1,
                    orders_processed: n_orders as u64,
                });
            }

            let pool_ids_hex: Vec<String> = scoop_pool_ids.iter()
                .map(|id| hex::encode(id.to_bytes()))
                .collect();
            state.scoop_stats.recent_scoops.insert(0, ScoopRecordView {
                tx_id: tx_id_hex.clone(),
                slot,
                pool_ids: pool_ids_hex,
                n_orders,
                scooper: scooper_hex.clone(),
            });
            state.scoop_stats.recent_scoops.truncate(50);
        }

        // Emit pool removed events
        for id in removed_pool_ids {
            events.push(IndexEvent::V4PoolRemoved { id, tx_id: tx_id_hex.clone() });
        }

        // Remove old settings if spent
        if let Some(settings) = &state.settings
            && spent_inputs.contains(&settings.input)
        {
            changes.spent_txos.push(SpentTxo {
                input: settings.input.clone(),
                spending_tx_id: this_tx_hash.to_vec(),
            });
            state.settings = None;
        }

        // Apply new pool state — emit events for new/updated pools.
        // Use known_pool_idents (captured before retain) so that scoops
        // (which remove then re-add the pool) emit Updated, not Created.
        for (id, pool) in &updated_pools {
            // Fill in new_pool on matching spent pool entries
            for sp in state.spent_pools.iter_mut().rev() {
                if sp.id == *id && sp.slot == slot && sp.new_pool.is_none() {
                    sp.new_pool = Some(pool.clone());
                    break;
                }
            }
            if known_pool_idents.contains(id) {
                events.push(IndexEvent::V4PoolUpdated {
                    id: id.clone(),
                    pool: pool.clone(),
                    tx_id: tx_id_hex.clone(),
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
        state.invalid_orders.append(&mut new_invalid_orders);

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
                warn!("v4: rolling back to {point}");
                let mut history = self.state.lock().await;
                history.rollback_to_slot(*slot);
                let needs_reload = history.is_empty();
                drop(history);
                self.dao.rollback(*slot).await?;
                if needs_reload {
                    warn!("v4: history empty after rollback, rebuilding from DB");
                    self.load().await?;
                }
            }
        }
        let to_slot = point.slot();
        if matches!(point, Point::Origin) {
            self.dao.rollback(to_slot).await?;
        }
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

fn derive_scooper_keyhash(secret_key_hex: &str) -> Result<pallas_primitives::Hash<28>> {
    use pallas_crypto::hash::Hasher;
    use pallas_crypto::key::ed25519::SecretKey;

    let bytes = hex::decode(secret_key_hex).context("invalid secret key hex")?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("secret key must be 32 bytes"))?;
    let sk = SecretKey::from(arr);
    let pk = sk.public_key();
    let pk_bytes: [u8; 32] = pk.as_ref().try_into().unwrap();
    Ok(Hasher::<224>::hash(&pk_bytes))
}

fn build_scoop_stats(
    records: &[ScoopRecord],
    our_keyhash: Option<&pallas_primitives::Hash<28>>,
) -> ScoopStats {
    use std::collections::BTreeMap;

    let our_hex = our_keyhash
        .map(|h| hex::encode(h.as_ref()))
        .unwrap_or_default();

    let mut totals: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    for r in records {
        let key = hex::encode(&r.scooper);
        let entry = totals.entry(key).or_default();
        entry.0 += 1;
        entry.1 += r.n_orders as u64;
    }

    let scooper_totals: Vec<ScooperTotal> = totals
        .into_iter()
        .map(|(scooper, (scoop_txs, orders_processed))| ScooperTotal {
            scooper,
            scoop_txs,
            orders_processed,
        })
        .collect();

    // DB records are per-pool; group by tx_id to collect all pool_ids per scoop tx.
    let mut seen_tx_ids = std::collections::BTreeSet::new();
    let mut recent_scoops: Vec<ScoopRecordView> = Vec::new();
    for r in records.iter().rev() {
        let tx_hex = hex::encode(&r.tx_id);
        let pool_hex = hex::encode(&r.pool_id);
        if let Some(existing) = recent_scoops.iter_mut().find(|s| s.tx_id == tx_hex) {
            if !existing.pool_ids.contains(&pool_hex) {
                existing.pool_ids.push(pool_hex);
            }
        } else if seen_tx_ids.insert(tx_hex.clone()) {
            recent_scoops.push(ScoopRecordView {
                tx_id: tx_hex,
                slot: r.slot,
                pool_ids: vec![pool_hex],
                n_orders: r.n_orders,
                scooper: hex::encode(&r.scooper),
            });
            if recent_scoops.len() >= 50 {
                break;
            }
        }
    }

    ScoopStats {
        our_keyhash: our_hex,
        scooper_totals,
        recent_scoops,
    }
}

pub(crate) fn derive_scooper_pallas_address(secret_key_hex: &str) -> Result<Address> {
    use pallas_addresses::{Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart};
    use pallas_crypto::hash::Hasher;
    use pallas_crypto::key::ed25519::SecretKey;
    use pallas_primitives::Hash;

    let bytes = hex::decode(secret_key_hex).context("invalid secret key hex")?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("secret key must be 32 bytes"))?;
    let sk = SecretKey::from(arr);
    let pk = sk.public_key();
    let pk_bytes: [u8; 32] = pk.as_ref().try_into().unwrap();
    let keyhash: Hash<28> = Hasher::<224>::hash(&pk_bytes);

    let shelley = ShelleyAddress::new(
        Network::Testnet,
        ShelleyPaymentPart::Key(keyhash),
        ShelleyDelegationPart::Null,
    );
    Ok(Address::from(shelley))
}

fn address_equals(a: &Address, b: &Address) -> bool {
    a.to_vec() == b.to_vec()
}

fn payment_hash_equals(addr: &Address, hash: &ScriptHash) -> bool {
    if let Address::Shelley(s_addr) = addr {
        s_addr.payment().as_hash() == hash
    } else {
        false
    }
}
