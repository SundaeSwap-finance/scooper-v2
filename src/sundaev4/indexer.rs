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
use tracing::{debug, info, trace, warn};

use crate::{
    cardano_types::{self, AssetClass, TransactionInput, TransactionOutput},
    datum_lookup::{DatumLookup, ScopedDatumLookup},
    events::{IndexEvent, InvalidOrder, ScoopRecordView, ScoopStats, ScooperTotal, SpentOrder, SpentOrderReason, SpentPool},
    historical_state::HistoricalState,
    persistence::{IndexerDao, PersistedDatum, PersistedModuleConfig, PersistedTxo, ScoopRecord, SpentTxo, TxChanges},
    sundaev3::Ident,
    sundaev4::{
        OrderRedeemer, PoolDatum, PoolRedeemer, SettingsDatum, SundaeV4Order, SundaeV4Pool,
        SundaeV4Protocol, SundaeV4Settings,
    },
};

use crate::cardano_types::{CIP_67_ASSET_LABEL_222, METADATA_DATUM_KEY};

#[derive(Debug, Clone, Default)]
pub struct SundaeV4State {
    pub pools: BTreeMap<Ident, Arc<SundaeV4Pool>>,
    pub orders: Vec<Arc<SundaeV4Order>>,
    pub settings: Option<Arc<SundaeV4Settings>>,
    /// OrderConfig settings entries, keyed by their token name (the asset
    /// name under `settings_mint` policy). Each order's `config_token`
    /// indexes into this map. Populated from `settings`-typed UTxOs whose
    /// datum decodes as `OrderConfig` (PR #11 modular order constraints).
    pub order_configs: BTreeMap<Vec<u8>, Arc<SundaeV4OrderConfig>>,
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

/// An OrderConfig settings entry. The order_validator's withdraw handler
/// resolves an order's `config_token` to this entry as a reference input,
/// then checks the order's constraints list matches `config.required_constraints`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SundaeV4OrderConfig {
    pub input: crate::cardano_types::TransactionInput,
    pub value: crate::cardano_types::Value,
    /// Token name (asset name under settings_mint policy) — duplicated here
    /// for convenience; equals the parent map key.
    #[serde(serialize_with = "v4_hex::bytes")]
    pub token_name: Vec<u8>,
    pub config: crate::sundaev4::types::OrderConfig,
    pub slot: u64,
}

mod v4_hex {
    use serde::Serializer;
    pub fn bytes<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(v))
    }
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
    /// Resolved per-module configs keyed by pool ident.
    ///
    /// Each pool's `module_state` stores `(module_script_hash, config_hash)`
    /// pairs; the scooper must re-send the actual config in every Operate
    /// redeemer. We hydrate from the `module_configs` DB table on load and
    /// from on-chain Create redeemers during sync.
    module_configs: Mutex<PoolModuleConfigCache>,
}

/// In-memory per-pool config cache, one map per module. Add a new field +
/// hydration arm in `load()` to support a new module.
#[derive(Default)]
pub struct PoolModuleConfigCache {
    pub cs: BTreeMap<Ident, crate::sundaev4::types::ConstantSumConfig>,
    pub cp: BTreeMap<Ident, crate::sundaev4::types::ConstantProductConfig>,
    pub cl: BTreeMap<Ident, crate::sundaev4::types::ConcentratedLiquidityConfig>,
    pub fee_split: BTreeMap<Ident, crate::sundaev4::types::FeeSplitConfig>,
}

impl SundaeV4Indexer {
    pub fn new(
        state: Arc<Mutex<SundaeV4HistoricalState>>,
        event_tx: broadcast::Sender<(u64, Vec<IndexEvent>)>,
        protocol: SundaeV4Protocol,
        rollback_limit: u64,
        dao: Box<dyn IndexerDao>,
    ) -> Self {
        info!(
            has_execution = protocol.execution.is_some(),
            fee = ?protocol.execution.as_ref().map(|e| e.fee),
            "V4 indexer created"
        );
        let scooper_address = protocol.execution.as_ref().and_then(|exec| {
            derive_scooper_pallas_address_with_stake(
                &exec.scooper_secret_key,
                exec.scooper_stake_keyhash.as_deref(),
            )
            .ok()
        });
        let scooper_keyhash = protocol.execution.as_ref().and_then(|exec| {
            derive_scooper_keyhash(&exec.scooper_secret_key).ok()
        });
        let ref_utxo_inputs = protocol
            .execution
            .as_ref()
            .map(|exec| {
                let scripts = &exec.module_scripts;
                let mut set = [
                    &scripts.pool,
                    &scripts.order,
                    &scripts.constant_product,
                    &scripts.fee_split,
                    &scripts.fairness,
                    &scripts.pool_mint,
                    &scripts.settings,
                ]
                .iter()
                .map(|s| s.ref_utxo.clone())
                .collect::<BTreeSet<_>>();
                if let Some(cs) = &scripts.constant_sum {
                    set.insert(cs.ref_utxo.clone());
                }
                if let Some(cl) = &scripts.concentrated_liquidity {
                    set.insert(cl.ref_utxo.clone());
                }
                if let Some(so) = &scripts.swap_order {
                    set.insert(so.ref_utxo.clone());
                }
                if let Some(bo) = &scripts.basic_order {
                    set.insert(bo.ref_utxo.clone());
                }
                if let Some(ro) = &scripts.route_order {
                    set.insert(ro.ref_utxo.clone());
                }
                if let Some(fo) = &scripts.fairness_order {
                    set.insert(fo.ref_utxo.clone());
                }
                if let Some(st) = &scripts.strategy_order {
                    set.insert(st.ref_utxo.clone());
                }
                set
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
            module_configs: Mutex::new(PoolModuleConfigCache::default()),
        }
    }

    pub fn set_loaded_slot(&mut self, slot: u64) {
        self.loaded_slot = slot;
    }

    /// (Re-)hydrate the per-module config cache from the DB's
    /// `module_configs` table, dispatching each entry by module hash.
    /// Entries whose module_hash matches no configured module are ignored.
    ///
    /// Called from `load()`, and again from main after a bootstrap: the
    /// bootstrap recovers configs from pool tx history and persists them
    /// AFTER `load()` has already hydrated against the pre-bootstrap DB.
    /// Without the second pass, the first live update of a bootstrapped
    /// pool re-classifies it with an empty cache and silently falls back
    /// to default configs (wrong module_state hash → on-chain eval fails).
    pub async fn rehydrate_module_configs(&self) -> Result<()> {
        use crate::sundaev4::types::{ConstantSumConfig, ConstantProductConfig, ConcentratedLiquidityConfig, FeeSplitConfig};
        let persisted_configs = self.dao.load_module_configs().await?;
        let cs_module_hash: Option<Vec<u8>> = self.protocol
            .execution
            .as_ref()
            .and_then(|e| e.module_scripts.constant_sum.as_ref())
            .map(|cs| cs.hash.as_ref().to_vec());
        let cp_module_hash: Option<Vec<u8>> = self.protocol
            .execution
            .as_ref()
            .map(|e| e.module_scripts.constant_product.hash.as_ref().to_vec());
        let cl_module_hash: Option<Vec<u8>> = self.protocol
            .execution
            .as_ref()
            .and_then(|e| e.module_scripts.concentrated_liquidity.as_ref())
            .map(|cl| cl.hash.as_ref().to_vec());
        let fs_module_hash: Option<Vec<u8>> = self.protocol
            .execution
            .as_ref()
            .map(|e| e.module_scripts.fee_split.hash.as_ref().to_vec());
        let mut cache = self.module_configs.lock().await;
        for cfg in persisted_configs {
            let pd = PlutusData::from_plutus_bytes(&cfg.config_cbor)
                .context("could not parse persisted module config CBOR")?;
            if Some(&cfg.module_hash) == cs_module_hash.as_ref() {
                let parsed = ConstantSumConfig::from_plutus(pd)
                    .context("could not parse persisted ConstantSumConfig")?;
                cache.cs.insert(Ident::new(&cfg.pool_id), parsed);
            } else if Some(&cfg.module_hash) == cp_module_hash.as_ref() {
                let parsed = ConstantProductConfig::from_plutus(pd)
                    .context("could not parse persisted ConstantProductConfig")?;
                cache.cp.insert(Ident::new(&cfg.pool_id), parsed);
            } else if Some(&cfg.module_hash) == cl_module_hash.as_ref() {
                let parsed = ConcentratedLiquidityConfig::from_plutus(pd)
                    .context("could not parse persisted ConcentratedLiquidityConfig")?;
                cache.cl.insert(Ident::new(&cfg.pool_id), parsed);
            } else if Some(&cfg.module_hash) == fs_module_hash.as_ref() {
                let parsed = FeeSplitConfig::from_plutus(pd)
                    .context("could not parse persisted FeeSplitConfig")?;
                cache.fee_split.insert(Ident::new(&cfg.pool_id), parsed);
            }
        }
        info!(
            cs = cache.cs.len(),
            cp = cache.cp.len(),
            cl = cache.cl.len(),
            fs = cache.fee_split.len(),
            "v4: hydrated per-module pool configs from DB",
        );
        Ok(())
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

        // Hydrate the per-module config cache from DB so detect_pool_type
        // calls below resolve correctly even though the original Create tx
        // is long out of the indexer's stream.
        self.rehydrate_module_configs().await?;

        // Constraint script hashes — used to find the right entry in the
        // order datum's `constraints: List<(hash, Data)>` list (PR #11).
        let swap_order_hash: Vec<u8> = self.protocol
            .execution
            .as_ref()
            .and_then(|e| e.module_scripts.swap_order.as_ref())
            .map(|s| s.hash.as_ref().to_vec())
            .unwrap_or_default();
        let basic_order_hash: Vec<u8> = self.protocol
            .execution
            .as_ref()
            .and_then(|e| e.module_scripts.basic_order.as_ref())
            .map(|s| s.hash.as_ref().to_vec())
            .unwrap_or_default();
        let strategy_order_hash: Vec<u8> = self.protocol
            .execution
            .as_ref()
            .and_then(|e| e.module_scripts.strategy_order.as_ref())
            .map(|s| s.hash.as_ref().to_vec())
            .unwrap_or_default();

        let cache = self.module_configs.lock().await;
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
                    let pool_type = self.detect_pool_type_with_cache(&pool_datum, &cache);
                    let fs_cfg = cache.fee_split.get(&pool_datum.identifier).cloned();
                    state.pools.insert(
                        pool_datum.identifier.clone(),
                        Arc::new(SundaeV4Pool {
                            input: txo.txo_id,
                            value: output.value,
                            pool_datum,
                            pool_type,
                            slot: txo.created_slot,
                            fee_split_config: fs_cfg,
                        }),
                    );
                }
                "order" => {
                    match output.datum.try_parse::<crate::sundaev4::OrderDatum>(&datums)
                        .and_then(|datum| {
                            crate::sundaev4::Constraint::from_order_datum_with_strategy(
                                &datum, &swap_order_hash, &basic_order_hash, &strategy_order_hash,
                            )
                                .map(|c| (datum, c))
                                .map_err(|e| format!("constraint decode: {e}"))
                        })
                    {
                        Ok((datum, constraint)) => {
                            state.orders.push(Arc::new(SundaeV4Order {
                                input: txo.txo_id,
                                datum,
                                constraint,
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
                    match output.datum.try_parse::<crate::sundaev4::OrderDatum>(&datums)
                        .and_then(|datum| {
                            crate::sundaev4::Constraint::from_order_datum_with_strategy(
                                &datum, &swap_order_hash, &basic_order_hash, &strategy_order_hash,
                            )
                                .map(|c| (datum, c))
                                .map_err(|e| format!("constraint decode: {e}"))
                        })
                    {
                        Ok((datum, constraint)) => {
                            state.orders.push(Arc::new(SundaeV4Order {
                                input: txo.txo_id,
                                datum,
                                constraint,
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
                "order_config" => {
                    // PR #11: OrderConfig settings entries are at the same
                    // address as the global SettingsDatum but carry a
                    // different (non-empty) token name and a 2-field
                    // `OrderConfig { label, required_constraints }` datum.
                    let token_name = output
                        .value
                        .0
                        .get(&self.protocol.settings_nft.policy)
                        .and_then(|tokens| {
                            tokens.iter().find_map(|(name, qty)| {
                                if !name.is_empty() && qty.is_positive() {
                                    Some(name.to_vec())
                                } else {
                                    None
                                }
                            })
                        });
                    let parsed: Option<crate::sundaev4::types::OrderConfig> =
                        output.datum.parse(&datums);
                    if let (Some(token_name), Some(oc)) = (token_name, parsed) {
                        state.order_configs.insert(
                            token_name.clone(),
                            Arc::new(SundaeV4OrderConfig {
                                input: txo.txo_id,
                                value: output.value,
                                token_name,
                                config: oc,
                                slot: txo.created_slot,
                            }),
                        );
                    } else {
                        warn!(input = %txo.txo_id, "v4: order_config txo could not be reparsed on load");
                    }
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
                    if let Some(od) = output.datum.parse::<crate::sundaev4::OrderDatum>(&datums) {
                        if let Ok(constraint) =
                            crate::sundaev4::Constraint::from_order_datum_with_strategy(
                                &od, &swap_order_hash, &basic_order_hash, &strategy_order_hash,
                            )
                        {
                            state.spent_orders.push(SpentOrder {
                                order: Arc::new(SundaeV4Order {
                                    input: stxo.txo.txo_id,
                                    datum: od,
                                    constraint,
                                    value: output.value,
                                    slot: stxo.txo.created_slot,
                                }),
                                reason: SpentOrderReason::Unknown,
                                tx_id,
                                slot: stxo.spent_slot,
                            });
                        }
                    }
                }
                "pool" => {
                    if let Some(pd) = self.parse_pool(&output, &datums) {
                        let pool_type = self.detect_pool_type_with_cache(&pd, &cache);
                        let fs_cfg = cache.fee_split.get(&pd.identifier).cloned();
                        state.spent_pools.push(SpentPool {
                            id: pd.identifier.clone(),
                            old_pool: Arc::new(SundaeV4Pool {
                                input: stxo.txo.txo_id,
                                value: output.value,
                                pool_datum: pd,
                                pool_type,
                                slot: stxo.txo.created_slot,
                                fee_split_config: fs_cfg,
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

    /// Detect the pool type from its datum's action modules, consulting the
    /// per-module config caches.
    fn detect_pool_type_with_cache(
        &self,
        pool_datum: &PoolDatum,
        cache: &PoolModuleConfigCache,
    ) -> crate::sundaev4::types::PoolType {
        let cs = cache.cs.get(&pool_datum.identifier);
        let cp = cache.cp.get(&pool_datum.identifier);
        let cl = cache.cl.get(&pool_datum.identifier);
        detect_pool_type(pool_datum, self.protocol.execution.as_ref(), cs, cp, cl)
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

    /// Try to parse a `settings`-typed UTxO as an `OrderConfig` settings entry.
    /// Returns `Some((token_name, OrderConfig))` when the UTxO carries a
    /// non-empty-name token under `settings_mint` policy AND its inline
    /// datum decodes as `OrderConfig`. The empty-name token is reserved for
    /// the global SettingsDatum entry.
    fn parse_order_config(
        &self,
        tx_out: &TransactionOutput,
        datums: &ScopedDatumLookup,
    ) -> Option<(Vec<u8>, crate::sundaev4::types::OrderConfig)> {
        let settings_policy = &self.protocol.settings_nft.policy;
        let token_name = tx_out
            .value
            .0
            .get(settings_policy)
            .and_then(|tokens| {
                tokens.iter().find_map(|(name, qty)| {
                    if !name.is_empty() && qty.is_positive() {
                        Some(name.to_vec())
                    } else {
                        None
                    }
                })
            })?;
        let order_config: crate::sundaev4::types::OrderConfig = tx_out.datum.parse(datums)?;
        Some((token_name, order_config))
    }

    fn parse_redeemer<T: AsPlutus>(&self, tx: &MultiEraTx, spend_index: usize) -> Option<T> {
        let redeemers = tx.redeemers();
        let redeemer = redeemers
            .iter()
            .find(|r| r.tag() == RedeemerTag::Spend && r.index() == spend_index as u32)?;
        T::from_plutus(redeemer.data().clone()).ok()
    }

    /// Try to extract a `ConstantSumConfig` from this tx's CS module Create redeemer.
    ///
    /// Returns `None` if: no CS module configured, no matching withdrawal in the tx,
    /// the redeemer isn't `Create`, or the initial_state can't be parsed.
    fn extract_cs_config_from_tx(
        &self,
        tx: &MultiEraTx,
    ) -> Option<crate::sundaev4::types::ConstantSumConfig> {
        let cs_hash = &self.protocol.execution.as_ref()?.module_scripts.constant_sum.as_ref()?.hash;
        extract_cs_config_from_tx(tx, cs_hash)
    }
}

/// Free-function form: try to extract a `ConstantSumConfig` from a tx's
/// CS module Create withdrawal redeemer. Used by both the live indexer and
/// the bootstrap path (which fetches a historical tx via Blockfrost).
pub fn extract_cs_config_from_tx(
    tx: &MultiEraTx,
    cs_script_hash: &ScriptHash,
) -> Option<crate::sundaev4::types::ConstantSumConfig> {
    use crate::sundaev4::types::{ConstantSumConfig, ConstantSumRedeemer};

    let mut account = vec![0xf0u8];
    account.extend_from_slice(cs_script_hash.as_ref());
    let sorted = tx.withdrawals_sorted_set();
    let wd_index = sorted.iter().position(|(k, _)| *k == account.as_slice())?;
    let redeemers = tx.redeemers();
    let redeemer = redeemers
        .iter()
        .find(|r| r.tag() == RedeemerTag::Reward && r.index() == wd_index as u32)?;
    let parsed: ConstantSumRedeemer = AsPlutus::from_plutus(redeemer.data().clone()).ok()?;
    match parsed {
        ConstantSumRedeemer::Create { initial_state, .. } => {
            ConstantSumConfig::from_plutus(initial_state).ok()
        }
        _ => None,
    }
}

/// Try to extract a `FeeSplitConfig` from a tx's fee_split module withdrawal
/// redeemer. The redeemer is `Create { config }` in the pool's mint tx and
/// `Operate { entries }` in every subsequent scoop. For Operate we accept the
/// Returns `Some(config)` only when the tx carries a `Create` redeemer for
/// the fee_split module — i.e. the pool's mint tx. Operate redeemers carry
/// one config entry per pool in a multi-pool scoop and are ambiguous without
/// a `pool_oref`, so this function intentionally returns `None` for them.
/// Use `extract_fee_split_config_for_pool_from_tx` when you have an oref.
pub fn extract_fee_split_config_from_tx(
    tx: &MultiEraTx,
    fs_script_hash: &ScriptHash,
) -> Option<crate::sundaev4::types::FeeSplitConfig> {
    use crate::sundaev4::types::FeeSplitRedeemer;

    let mut account = vec![0xf0u8];
    account.extend_from_slice(fs_script_hash.as_ref());
    let sorted = tx.withdrawals_sorted_set();
    let wd_index = sorted.iter().position(|(k, _)| *k == account.as_slice())?;
    let redeemers = tx.redeemers();
    let redeemer = redeemers
        .iter()
        .find(|r| r.tag() == RedeemerTag::Reward && r.index() == wd_index as u32)?;
    let parsed: FeeSplitRedeemer = AsPlutus::from_plutus(redeemer.data().clone()).ok()?;
    match parsed {
        FeeSplitRedeemer::Create { config, .. } => Some(config),
        _ => None,
    }
}

/// Same shape as `extract_fee_split_config_from_tx`: returns `Some(config)`
/// only for `Create` redeemers (mint tx). Operate's per-pool entries are
/// only safe when matched against a known `pool_oref`.
pub fn extract_cp_config_from_tx(
    tx: &MultiEraTx,
    cp_script_hash: &ScriptHash,
) -> Option<crate::sundaev4::types::ConstantProductConfig> {
    use crate::sundaev4::types::ConstantProductRedeemer;

    let mut account = vec![0xf0u8];
    account.extend_from_slice(cp_script_hash.as_ref());
    let sorted = tx.withdrawals_sorted_set();
    let wd_index = sorted.iter().position(|(k, _)| *k == account.as_slice())?;
    let redeemers = tx.redeemers();
    let redeemer = redeemers
        .iter()
        .find(|r| r.tag() == RedeemerTag::Reward && r.index() == wd_index as u32)?;
    let parsed: ConstantProductRedeemer = AsPlutus::from_plutus(redeemer.data().clone()).ok()?;
    match parsed {
        ConstantProductRedeemer::Create { initial_state } => Some(initial_state),
        _ => None,
    }
}

/// Try to extract a `ConcentratedLiquidityConfig` from a tx's CL module
/// withdrawal Create redeemer (the pool's mint tx). Operate redeemers
/// carry per-pool entries and need to be matched by `pool_oref`.
pub fn extract_cl_config_from_tx(
    tx: &MultiEraTx,
    cl_script_hash: &ScriptHash,
) -> Option<crate::sundaev4::types::ConcentratedLiquidityConfig> {
    use crate::sundaev4::types::ConcentratedLiquidityRedeemer;

    let mut account = vec![0xf0u8];
    account.extend_from_slice(cl_script_hash.as_ref());
    let sorted = tx.withdrawals_sorted_set();
    let wd_index = sorted.iter().position(|(k, _)| *k == account.as_slice())?;
    let redeemers = tx.redeemers();
    let redeemer = redeemers
        .iter()
        .find(|r| r.tag() == RedeemerTag::Reward && r.index() == wd_index as u32)?;
    let parsed: ConcentratedLiquidityRedeemer = AsPlutus::from_plutus(redeemer.data().clone()).ok()?;
    match parsed {
        ConcentratedLiquidityRedeemer::Create { initial_state } => Some(initial_state),
        _ => None,
    }
}

/// Like `extract_fee_split_config_from_tx`, but for an Operate redeemer with
/// multiple pools: returns the entry matching `pool_oref`.
#[allow(dead_code)]
pub fn extract_fee_split_config_for_pool_from_tx(
    tx: &MultiEraTx,
    fs_script_hash: &ScriptHash,
    pool_oref: &crate::sundaev4::types::OutputRef,
) -> Option<crate::sundaev4::types::FeeSplitConfig> {
    use crate::sundaev4::types::FeeSplitRedeemer;

    let mut account = vec![0xf0u8];
    account.extend_from_slice(fs_script_hash.as_ref());
    let sorted = tx.withdrawals_sorted_set();
    let wd_index = sorted.iter().position(|(k, _)| *k == account.as_slice())?;
    let redeemers = tx.redeemers();
    let redeemer = redeemers
        .iter()
        .find(|r| r.tag() == RedeemerTag::Reward && r.index() == wd_index as u32)?;
    let parsed: FeeSplitRedeemer = AsPlutus::from_plutus(redeemer.data().clone()).ok()?;
    match parsed {
        FeeSplitRedeemer::Create { config, .. } => Some(config),
        FeeSplitRedeemer::Operate { entries } => entries
            .into_iter()
            .find(|e| &e.pool_oref == pool_oref)
            .map(|e| e.config),
        FeeSplitRedeemer::Destroy { .. } => None,
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
        /// (token_name, OrderConfigEntry) — newly-discovered OrderConfig
        /// settings entries in this tx.
        let mut new_order_configs: Vec<(Vec<u8>, Arc<SundaeV4OrderConfig>)> = vec![];
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

        // Try to extract per-module configs from this tx's Create/Operate
        // redeemers (if any). For scoop txs without a Create redeemer for a
        // given module, these return None — harmless. We use the result to
        // populate per-pool config caches.
        let cs_config_from_tx = self.extract_cs_config_from_tx(&tx);
        let cp_config_from_tx = self
            .protocol
            .execution
            .as_ref()
            .and_then(|e| extract_cp_config_from_tx(&tx, &e.module_scripts.constant_product.hash));
        let cl_config_from_tx = self
            .protocol
            .execution
            .as_ref()
            .and_then(|e| e.module_scripts.concentrated_liquidity.as_ref())
            .and_then(|cl| extract_cl_config_from_tx(&tx, &cl.hash));
        let fs_config_from_tx = self
            .protocol
            .execution
            .as_ref()
            .and_then(|e| extract_fee_split_config_from_tx(&tx, &e.module_scripts.fee_split.hash));
        let swap_order_hash: Vec<u8> = self.protocol
            .execution
            .as_ref()
            .and_then(|e| e.module_scripts.swap_order.as_ref())
            .map(|s| s.hash.as_ref().to_vec())
            .unwrap_or_default();
        let basic_order_hash: Vec<u8> = self.protocol
            .execution
            .as_ref()
            .and_then(|e| e.module_scripts.basic_order.as_ref())
            .map(|s| s.hash.as_ref().to_vec())
            .unwrap_or_default();
        let strategy_order_hash: Vec<u8> = self.protocol
            .execution
            .as_ref()
            .and_then(|e| e.module_scripts.strategy_order.as_ref())
            .map(|s| s.hash.as_ref().to_vec())
            .unwrap_or_default();
        let cs_module_hash_bytes: Option<Vec<u8>> = self.protocol
            .execution
            .as_ref()
            .and_then(|e| e.module_scripts.constant_sum.as_ref())
            .map(|cs| cs.hash.as_ref().to_vec());
        let cp_module_hash_bytes: Option<Vec<u8>> = self.protocol
            .execution
            .as_ref()
            .map(|e| e.module_scripts.constant_product.hash.as_ref().to_vec());
        let cl_module_hash_bytes: Option<Vec<u8>> = self.protocol
            .execution
            .as_ref()
            .and_then(|e| e.module_scripts.concentrated_liquidity.as_ref())
            .map(|cl| cl.hash.as_ref().to_vec());
        let fs_module_hash_bytes: Option<Vec<u8>> = self.protocol
            .execution
            .as_ref()
            .map(|e| e.module_scripts.fee_split.hash.as_ref().to_vec());

        // Lock the persisted-config cache for the duration of output scanning:
        // pool outputs read from it, and Create writes a new entry.
        let mut module_cache = self.module_configs.lock().await;

        // Scan outputs for vault/order/settings script hashes
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

                    let pool_id = pd.identifier.clone();
                    // Resolution priority for the CS/CP branch inside detect_pool_type:
                    //   1. config-file override (consulted internally)
                    //   2. this tx's Create redeemer
                    //   3. previously-resolved persisted config (cache)
                    //   4. defaults
                    let resolved_cs = cs_config_from_tx
                        .as_ref()
                        .or_else(|| module_cache.cs.get(&pool_id));
                    let resolved_cp = cp_config_from_tx
                        .as_ref()
                        .or_else(|| module_cache.cp.get(&pool_id));
                    let resolved_cl = cl_config_from_tx
                        .as_ref()
                        .or_else(|| module_cache.cl.get(&pool_id));
                    let pool_type = detect_pool_type(
                        &pd,
                        self.protocol.execution.as_ref(),
                        resolved_cs,
                        resolved_cp,
                        resolved_cl,
                    );

                    // If we just learned this pool's CS config from a Create
                    // redeemer, persist it and remember it for future blocks.
                    if let (Some(cfg), crate::sundaev4::types::PoolType::ConstantSum { .. }) =
                        (cs_config_from_tx.as_ref(), &pool_type)
                    {
                        if !module_cache.cs.contains_key(&pool_id) {
                            let cbor = minicbor::to_vec(&cfg.clone().to_plutus())
                                .context("encode ConstantSumConfig CBOR")?;
                            changes.module_configs.push(PersistedModuleConfig {
                                pool_id: pool_id.to_bytes().to_vec(),
                                module_hash: cs_module_hash_bytes.clone()
                                    .expect("cs_module_hash present when CS pool detected"),
                                config_cbor: cbor,
                                created_slot: slot,
                            });
                            module_cache.cs.insert(pool_id.clone(), cfg.clone());
                            info!(
                                pool = %hex::encode(pool_id.to_bytes()),
                                "v4: persisted CS pool config from Create redeemer"
                            );
                        }
                    }
                    // Same for CP: persist on first sighting (Create or Operate).
                    if let (Some(cfg), crate::sundaev4::types::PoolType::ConstantProduct { .. }) =
                        (cp_config_from_tx.as_ref(), &pool_type)
                    {
                        if !module_cache.cp.contains_key(&pool_id) {
                            let cbor = minicbor::to_vec(&cfg.clone().to_plutus())
                                .context("encode ConstantProductConfig CBOR")?;
                            changes.module_configs.push(PersistedModuleConfig {
                                pool_id: pool_id.to_bytes().to_vec(),
                                module_hash: cp_module_hash_bytes.clone()
                                    .expect("cp_module_hash present when CP pool detected"),
                                config_cbor: cbor,
                                created_slot: slot,
                            });
                            module_cache.cp.insert(pool_id.clone(), cfg.clone());
                            info!(
                                pool = %hex::encode(pool_id.to_bytes()),
                                "v4: persisted CP pool config from redeemer"
                            );
                        }
                    }
                    // Same for CL: persist the spa/spb/fee from Create.
                    if let (Some(cfg), crate::sundaev4::types::PoolType::ConcentratedLiquidity { .. }) =
                        (cl_config_from_tx.as_ref(), &pool_type)
                    {
                        if !module_cache.cl.contains_key(&pool_id) {
                            let cbor = minicbor::to_vec(&cfg.clone().to_plutus())
                                .context("encode ConcentratedLiquidityConfig CBOR")?;
                            changes.module_configs.push(PersistedModuleConfig {
                                pool_id: pool_id.to_bytes().to_vec(),
                                module_hash: cl_module_hash_bytes.clone()
                                    .expect("cl_module_hash present when CL pool detected"),
                                config_cbor: cbor,
                                created_slot: slot,
                            });
                            module_cache.cl.insert(pool_id.clone(), cfg.clone());
                            info!(
                                pool = %hex::encode(pool_id.to_bytes()),
                                "v4: persisted CL pool config from Create redeemer"
                            );
                        }
                    }

                    // Same for fee_split: every pool has fee_split config in
                    // the mint tx (Create) and in every later scoop (Operate).
                    // Either form is usable; the first time we see it we
                    // persist and cache for future scoops.
                    if let (Some(cfg), Some(fs_hash)) =
                        (fs_config_from_tx.as_ref(), fs_module_hash_bytes.as_ref())
                    {
                        if !module_cache.fee_split.contains_key(&pool_id) {
                            let cbor = minicbor::to_vec(&cfg.clone().to_plutus())
                                .context("encode FeeSplitConfig CBOR")?;
                            changes.module_configs.push(PersistedModuleConfig {
                                pool_id: pool_id.to_bytes().to_vec(),
                                module_hash: fs_hash.clone(),
                                config_cbor: cbor,
                                created_slot: slot,
                            });
                            module_cache.fee_split.insert(pool_id.clone(), cfg.clone());
                            info!(
                                pool = %hex::encode(pool_id.to_bytes()),
                                "v4: persisted fee_split config from redeemer"
                            );
                        }
                    }

                    let fs_cfg = module_cache.fee_split.get(&pool_id).cloned();
                    let pool_record = SundaeV4Pool {
                        input: this_input,
                        value: tx_out.value,
                        pool_datum: pd,
                        pool_type,
                        slot,
                        fee_split_config: fs_cfg,
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
                match tx_out.datum.try_parse::<crate::sundaev4::OrderDatum>(&datums)
                    .and_then(|datum| {
                        crate::sundaev4::Constraint::from_order_datum_with_strategy(
                            &datum, &swap_order_hash, &basic_order_hash, &strategy_order_hash,
                        )
                            .map(|c| (datum, c))
                            .map_err(|e| format!("constraint decode: {e}"))
                    }) {
                    Ok((od, constraint)) => {
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
                            constraint,
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
                } else if let Some((token_name, oc)) = self.parse_order_config(&tx_out, &datums) {
                    changes.created_txos.push(PersistedTxo {
                        txo_id: this_input.clone(),
                        txo_type: "order_config".to_string(),
                        created_slot: slot,
                        era: output.era().into(),
                        txo: output.encode(),
                        address: tx_out.address.to_vec(),
                        datum: tx_out.hashed_datum(&datums),
                    });
                    new_order_configs.push((
                        token_name.clone(),
                        Arc::new(SundaeV4OrderConfig {
                            input: this_input,
                            value: tx_out.value,
                            token_name,
                            config: oc,
                            slot,
                        }),
                    ));
                }
            }

            // Track wallet UTxOs by payment credential — CIP-1852 wallets use
            // base addresses (payment + stake) but the scooper might be
            // configured with an enterprise address. Matching by payment
            // credential alone accepts any address form (enterprise, base,
            // pointer) whose payment key we hold.
            if let Some(scooper_kh) = &self.scooper_keyhash {
                if payment_hash_equals(&address, scooper_kh) {
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
            match self.parse_redeemer::<PoolRedeemer>(&tx, spend_index) {
                Some(PoolRedeemer::Action { .. }) => {
                    // Pool was scooped — collect all scooped pool idents
                    scoop_pool_ids.push(ident.clone());
                }
                Some(PoolRedeemer::EscapeHatch { .. })
                | Some(PoolRedeemer::Upgrade)
                | Some(PoolRedeemer::EmergencyDisable { .. })
                | Some(PoolRedeemer::Destroy) => {
                    // Non-scoop pool operation
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

        // Remove any OrderConfig entries whose UTxO was spent (admin updated
        // or burned). The replacement entry, if any, is detected via the
        // settings-script-hash output branch above and re-added.
        let mut spent_order_config_tokens: Vec<Vec<u8>> = Vec::new();
        for (token, oc) in state.order_configs.iter() {
            if spent_inputs.contains(&oc.input) {
                spent_order_config_tokens.push(token.clone());
                changes.spent_txos.push(SpentTxo {
                    input: oc.input.clone(),
                    spending_tx_id: this_tx_hash.to_vec(),
                });
            }
        }
        for token in spent_order_config_tokens {
            state.order_configs.remove(&token);
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

        for (token_name, oc) in new_order_configs.drain(..) {
            info!(
                token = %hex::encode(&token_name),
                constraints = oc.config.required_constraints.len(),
                "v4: new OrderConfig settings entry",
            );
            state.order_configs.insert(token_name, oc);
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

    let bytes = hex::decode(secret_key_hex).context("invalid secret key hex")?;
    let pk = match bytes.len() {
        32 => {
            use pallas_crypto::key::ed25519::SecretKey;
            let arr: [u8; 32] = bytes.try_into().unwrap();
            SecretKey::from(arr).public_key()
        }
        64 => {
            use pallas_crypto::key::ed25519::SecretKeyExtended;
            let arr: [u8; 64] = bytes.try_into().unwrap();
            SecretKeyExtended::from_bytes(arr)
                .map_err(|e| anyhow::anyhow!("invalid extended ed25519 key: {e}"))?
                .public_key()
        }
        n => anyhow::bail!("secret key must be 32 or 64 bytes, got {n}"),
    };
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
    derive_scooper_pallas_address_with_stake(secret_key_hex, None)
}

/// Build the scooper's Shelley address. If `stake_keyhash_hex` is `Some`, a
/// base address (payment + staking) is produced — required to match base
/// addresses used by CIP-1852 wallets. Without it, an enterprise (payment-
/// only) address is returned, which doesn't see funds at base addresses.
pub(crate) fn derive_scooper_pallas_address_with_stake(
    secret_key_hex: &str,
    stake_keyhash_hex: Option<&str>,
) -> Result<Address> {
    use pallas_addresses::{Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart};

    let keyhash = derive_scooper_keyhash(secret_key_hex)?;
    let delegation = match stake_keyhash_hex {
        Some(s) => {
            let bytes = hex::decode(s).context("invalid stake keyhash hex")?;
            let arr: [u8; 28] = bytes.try_into().map_err(|_| {
                anyhow::anyhow!("stake keyhash must be 28 bytes (got different length)")
            })?;
            ShelleyDelegationPart::Key(arr.into())
        }
        None => ShelleyDelegationPart::Null,
    };
    let shelley = ShelleyAddress::new(
        Network::Testnet,
        ShelleyPaymentPart::Key(keyhash),
        delegation,
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

/// Detect the pool type from a pool datum's action modules.
///
/// Matches the swap action's first module hash against known module script
/// hashes from config. Defaults to ConstantProduct if no execution config
/// is available or no match is found.
///
/// Used by both the indexer (during chain sync) and bootstrap (during initial load).
pub fn detect_pool_type(
    pool_datum: &PoolDatum,
    execution: Option<&crate::sundaev4::types::ScooperExecution>,
    cs_config_from_tx: Option<&crate::sundaev4::types::ConstantSumConfig>,
    cp_config_from_tx: Option<&crate::sundaev4::types::ConstantProductConfig>,
    cl_config_from_tx: Option<&crate::sundaev4::types::ConcentratedLiquidityConfig>,
) -> crate::sundaev4::types::PoolType {
    use crate::sundaev4::types::{PoolType, Rational};
    use crate::bigint::BigInt;

    let Some(exec) = execution else {
        return PoolType::ConstantProduct {
            fee: Rational { num: BigInt::from(0), den: BigInt::from(1) },
        };
    };

    // Look at every enabled action's first module and classify by which known
    // module hash it matches. CS and CP swap actions use different tags
    // (cs_check.ak's tag_swap=3 vs CP's tag=100), so we can't pre-pick by tag.
    let ident_hex = hex::encode(pool_datum.identifier.to_bytes());
    let cs_hash = exec.module_scripts.constant_sum.as_ref().map(|s| s.hash.as_ref().to_vec());
    let cl_hash = exec.module_scripts.concentrated_liquidity.as_ref().map(|s| s.hash.as_ref().to_vec());
    let cp_hash = exec.module_scripts.constant_product.hash.as_ref().to_vec();
    let mut matched_action: Option<&crate::sundaev4::types::ActionEntry> = None;
    let mut matched_kind: Option<&'static str> = None;
    for action in &pool_datum.actions {
        if !action.enabled { continue; }
        let Some(first) = action.modules.first() else { continue; };
        if first.as_slice() == cp_hash.as_slice() {
            matched_action = Some(action);
            matched_kind = Some("cp");
            break;
        }
        if let Some(h) = &cs_hash {
            if first.as_slice() == h.as_slice() {
                matched_action = Some(action);
                matched_kind = Some("cs");
                break;
            }
        }
        if let Some(h) = &cl_hash {
            if first.as_slice() == h.as_slice() {
                matched_action = Some(action);
                matched_kind = Some("cl");
                break;
            }
        }
    }
    if matched_action.is_none() {
        // Log the unknown first-module hashes so the operator can see why
        // detect_pool_type couldn't classify this pool. Until detect_pool_type
        // learns the corresponding module, scoops referencing this pool will
        // fail with `module: not found in module_state` on the wrong module.
        let unknowns: Vec<String> = pool_datum
            .actions
            .iter()
            .filter(|a| a.enabled)
            .filter_map(|a| a.modules.first().map(hex::encode))
            .collect();
        warn!(
            pool = %ident_hex,
            modules = ?unknowns,
            "pool's enabled actions reference unknown module hash(es); \
             falling back to ConstantProduct (likely wrong)",
        );
    }

    let _action = match matched_action {
        Some(a) => a,
        None => return PoolType::ConstantProduct {
            fee: cp_config_from_tx
                .map(|c| c.fee.clone())
                .unwrap_or_else(|| Rational {
                    num: BigInt::from(exec.fee.0),
                    den: BigInt::from(exec.fee.1),
                }),
        },
    };

    if matched_kind == Some("cs") {
        // Priority: config file override > on-chain Create redeemer > defaults
        if let Some(crate::sundaev4::types::PoolConfig::ConstantSum { prices, fee }) =
            exec.pool_configs.get(&ident_hex)
        {
            return PoolType::ConstantSum {
                prices: prices.iter().map(|p| BigInt::from(*p)).collect(),
                fee: Rational {
                    num: BigInt::from(fee.0),
                    den: BigInt::from(fee.1),
                },
                bounty_k: Rational { num: BigInt::from(0), den: BigInt::from(1) },
                // Bounty off => balance_fee unread; mirror the fee for hash
                // consistency with the CLI's create default.
                balance_fee: Rational {
                    num: BigInt::from(fee.0),
                    den: BigInt::from(fee.1),
                },
            };
        }

        if let Some(cs_config) = cs_config_from_tx {
            info!(pool = %ident_hex, "CS pool config extracted from Create redeemer");
            return PoolType::ConstantSum {
                prices: cs_config.prices.clone(),
                fee: cs_config.fee.clone(),
                bounty_k: cs_config.bounty_k.clone(),
                balance_fee: cs_config.balance_fee.clone(),
            };
        }

        // Loud on purpose: a CS pool's config is hash-pinned in its datum's
        // module_state, so guessed defaults only eval if the pool really was
        // created with them. Reaching this branch usually means the config
        // cache lost an entry (e.g. hydration ordering), not that defaults
        // are right.
        warn!(pool = %ident_hex, "CS pool has no pool-config entry, using defaults (likely wrong — scoops will fail eval if the pool was created with a different config)");
        return PoolType::ConstantSum {
            prices: vec![BigInt::from(1); pool_datum.assets.len()],
            fee: Rational {
                num: BigInt::from(exec.fee.0),
                den: BigInt::from(exec.fee.1),
            },
            bounty_k: Rational { num: BigInt::from(0), den: BigInt::from(1) },
            balance_fee: Rational {
                num: BigInt::from(exec.fee.0),
                den: BigInt::from(exec.fee.1),
            },
        };
    }

    if matched_kind == Some("cl") {
        // Priority: operator override > on-chain Create redeemer > defaults
        if let Some(crate::sundaev4::types::PoolConfig::ConcentratedLiquidity {
            sqrt_price_a, sqrt_price_b, fee,
        }) = exec.pool_configs.get(&ident_hex) {
            info!(pool = %ident_hex, "CL pool config from operator override");
            return PoolType::ConcentratedLiquidity {
                sqrt_price_a: Rational {
                    num: BigInt::from(sqrt_price_a.0),
                    den: BigInt::from(sqrt_price_a.1),
                },
                sqrt_price_b: Rational {
                    num: BigInt::from(sqrt_price_b.0),
                    den: BigInt::from(sqrt_price_b.1),
                },
                fee: Rational {
                    num: BigInt::from(fee.0),
                    den: BigInt::from(fee.1),
                },
            };
        }
        if let Some(cl_config) = cl_config_from_tx {
            info!(pool = %ident_hex, "CL pool config extracted from Create redeemer");
            return PoolType::ConcentratedLiquidity {
                sqrt_price_a: cl_config.sqrt_price_a.clone(),
                sqrt_price_b: cl_config.sqrt_price_b.clone(),
                fee: cl_config.fee.clone(),
            };
        }
        // CL pools require their sqrt-price bounds from the Create redeemer
        // (or a stored config); there's no sensible default. Fall back to
        // a tight range to avoid silent miscalculation — scoops against
        // this pool will fail with module-state hash mismatch, which is
        // the right loud failure.
        warn!(pool = %ident_hex, "CL pool but no Create-redeemer config recovered yet — \
            set a `concentrated-liquidity` pool-config override in scooper config to scoop this pool");
        return PoolType::ConcentratedLiquidity {
            sqrt_price_a: Rational { num: BigInt::from(1), den: BigInt::from(1) },
            sqrt_price_b: Rational { num: BigInt::from(1), den: BigInt::from(1) },
            fee: Rational {
                num: BigInt::from(exec.fee.0),
                den: BigInt::from(exec.fee.1),
            },
        };
    }

    // CP path: prefer the per-pool config (this tx's redeemer OR cached) over
    // the global default — different CP pools can have different fees, and
    // `verify_module_state` hashes the config we send.
    PoolType::ConstantProduct {
        fee: cp_config_from_tx
            .map(|c| c.fee.clone())
            .unwrap_or_else(|| Rational {
                num: BigInt::from(exec.fee.0),
                den: BigInt::from(exec.fee.1),
            }),
    }
}
