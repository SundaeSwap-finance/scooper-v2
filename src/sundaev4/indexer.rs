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
    events::{
        IndexEvent, InvalidOrder, ScoopRecordView, ScoopStats, ScooperTotal, SpentOrder,
        SpentOrderReason, SpentPool,
    },
    historical_state::HistoricalState,
    persistence::{
        IndexerDao, PersistedDatum, PersistedModuleConfig, PersistedTxo, ScoopRecord, SpentTxo,
        TxChanges,
    },
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
    /// The FeeSettings node (docs/fee-system.md): tracked when the config
    /// names its entry token. Required to scoop fee-bearing OrderConfigs.
    pub fee_settings: Option<Arc<crate::sundaev4::types::SundaeV4FeeSettings>>,
    pub spent_orders: Vec<SpentOrder<SundaeV4Order>>,
    pub spent_pools: Vec<SpentPool<SundaeV4Pool>>,
    pub invalid_orders: Vec<InvalidOrder>,
    pub tip_slot: u64,
    /// The actual network tip slot, as reported by the upstream node.
    /// `None` until the upstream node reports it (Dolos may not always provide this).
    pub network_tip_slot: Option<u64>,
    pub wallet_utxos: BTreeMap<crate::cardano_types::TransactionInput, crate::cardano_types::Value>,
    /// UTxOs at the vault addresses the rate guards name (see
    /// [`crate::sundaev4::rate_guard`]): the backing and the datum a
    /// guarded pool's true rate derives from. Tracked by payment credential.
    pub vault_utxos:
        BTreeMap<crate::cardano_types::TransactionInput, crate::sundaev4::rate_guard::VaultUtxo>,
    pub ref_utxo_outputs:
        BTreeMap<crate::cardano_types::TransactionInput, crate::cardano_types::TransactionOutput>,
    pub scoop_stats: ScoopStats,
    /// Open Cardano-Swaps one-way orders (tracked only when
    /// `execution.cardano-swaps` is enabled).
    pub cardano_swaps: crate::sundaev4::cardano_swaps::CardanoSwapsState,
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
    scooper_keyhash: Option<pallas_primitives::Hash<28>>,
    /// Payment credentials of the rate guards' vaults.
    vault_payment_hashes: Vec<pallas_primitives::Hash<28>>,
    ref_utxo_inputs: BTreeSet<crate::cardano_types::TransactionInput>,
    /// Loaded Cardano-Swaps runtime: which credential to track.
    cardano_swaps: Option<crate::sundaev4::cardano_swaps::CardanoSwapsRuntime>,
    tip_event_counter: u64,
    /// The slot of the latest block loaded from DB. Blocks STRICTLY before
    /// this slot are skipped in handle_block/handle_onchain_tx_bytes to
    /// avoid the "cannot update slot" error when the cursor lags behind the
    /// DB state.
    ///
    /// The block AT this slot is re-processed, and must be. `loaded_slot` is
    /// the highest slot with any row in the database, which a block that was
    /// only PARTLY applied also sets: the writes commit per transaction, so
    /// stopping the service partway through a block leaves the transactions
    /// already processed durable and the rest missing. Skipping that slot on
    /// restart drops those remaining transactions permanently, with no error
    /// and nothing to reconcile them later.
    ///
    /// This cost four hours of preview on 2026-10-07. Block 4733493 held
    /// three transactions; the service was stopped after the first; on
    /// restart the driver correctly re-delivered the block and this guard
    /// threw all three away. Transaction [2] spent two pool UTxOs, so the
    /// indexer kept two dead pool UTxOs forever, and the scooper rebuilt
    /// against one of them 125 times while 119 orders queued behind it.
    ///
    /// Re-processing the boundary block is safe: `update_slot` rejects only
    /// a slot strictly below the current one, and every insert on this path
    /// is `INSERT OR IGNORE`.
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
    /// Stableswap configs carry the pool's current `rates`; a tag-7 update
    /// replaces the entry (the persisted row is upserted).
    pub ss: BTreeMap<Ident, crate::sundaev4::types::StableSwapConfig>,
    /// Banded ladders: hash-pinned like stableswap, but fixed after Create.
    pub bcl: BTreeMap<Ident, crate::sundaev4::types::BandedCLConfig>,
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
        let scooper_keyhash = protocol
            .execution
            .as_ref()
            .and_then(|exec| derive_scooper_keyhash(&exec.scooper_secret_key).ok());
        let mut ref_utxo_inputs = protocol
            .execution
            .as_ref()
            .map(|exec| module_ref_utxos(&exec.module_scripts))
            .unwrap_or_default();
        // A guard that does not resolve is reported here and enforced in the
        // scooper, which blocks the pool it names; the indexer only needs the
        // vaults it can track.
        let vault_payment_hashes = protocol
            .execution
            .as_ref()
            .map(|exec| {
                exec.rate_guards
                    .iter()
                    .filter_map(
                        |cfg| match crate::sundaev4::rate_guard::RateGuard::resolve(cfg) {
                            Ok(guard) => Some(guard.vault_payment_hash),
                            Err(e) => {
                                warn!("v4: {e}");
                                None
                            }
                        },
                    )
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let cardano_swaps = protocol
            .execution
            .as_ref()
            .and_then(|exec| crate::sundaev4::cardano_swaps::load_runtime(&exec.cardano_swaps));
        if let Some(rt) = &cardano_swaps {
            // The swap script's reference UTxO rides the generic ref-UTxO
            // tracking, so the evaluator resolves it like any module script.
            ref_utxo_inputs.extend(rt.static_ref_utxos());
        }
        Self {
            state,
            event_tx,
            protocol,
            rollback_limit,
            dao,
            scooper_keyhash,
            vault_payment_hashes,
            ref_utxo_inputs,
            cardano_swaps,
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
        use crate::sundaev4::ModuleKind;
        use crate::sundaev4::types::{
            ConstantProductConfig, ConstantSumConfig, FeeSplitConfig, StableSwapConfig,
        };
        let persisted_configs = self.dao.load_module_configs().await?;
        let mut cache = self.module_configs.lock().await;
        for cfg in persisted_configs {
            let Some(kind) = self
                .protocol
                .execution
                .as_ref()
                .and_then(|e| e.module_scripts.kind_of(&cfg.module_hash))
            else {
                continue;
            };
            let pd = PlutusData::from_plutus_bytes(&cfg.config_cbor)
                .context("could not parse persisted module config CBOR")?;
            match kind {
                ModuleKind::ConstantSum => {
                    let parsed = ConstantSumConfig::from_plutus(pd)
                        .context("could not parse persisted ConstantSumConfig")?;
                    cache.cs.insert(Ident::new(&cfg.pool_id), parsed);
                }
                ModuleKind::ConstantProduct => {
                    let parsed = ConstantProductConfig::from_plutus(pd)
                        .context("could not parse persisted ConstantProductConfig")?;
                    cache.cp.insert(Ident::new(&cfg.pool_id), parsed);
                }
                ModuleKind::FeeSplit => {
                    let parsed = FeeSplitConfig::from_plutus(pd)
                        .context("could not parse persisted FeeSplitConfig")?;
                    cache.fee_split.insert(Ident::new(&cfg.pool_id), parsed);
                }
                ModuleKind::Stableswap => {
                    let parsed = StableSwapConfig::from_plutus(pd)
                        .context("could not parse persisted StableSwapConfig")?;
                    cache.ss.insert(Ident::new(&cfg.pool_id), parsed);
                }
                ModuleKind::ConcentratedLiquidity => {
                    let parsed = crate::sundaev4::types::BandedCLConfig::from_plutus(pd)
                        .context("could not parse persisted BandedCLConfig")?;
                    cache.bcl.insert(Ident::new(&cfg.pool_id), parsed);
                }
                _ => {}
            }
        }
        info!(
            cs = cache.cs.len(),
            cp = cache.cp.len(),
            ss = cache.ss.len(),
            bcl = cache.bcl.len(),
            fs = cache.fee_split.len(),
            "v4: hydrated per-module pool configs from DB",
        );
        Ok(())
    }

    /// Re-derive `pool_type` and `fee_split_config` for the given pools from
    /// the (freshly rehydrated) module-config cache.
    ///
    /// `load()` classifies every pool it finds in the DB against the cache as
    /// it stood then. When a config is recovered afterwards the cache is
    /// refreshed, but the pools already in memory keep the defaults they were
    /// classified with — and those are exactly the values the first scoop would
    /// build against. Recompute them in place.
    pub async fn reclassify_pools(
        &self,
        state: &Arc<Mutex<SundaeV4HistoricalState>>,
        idents: &[Ident],
    ) -> Result<()> {
        let cache = self.module_configs.lock().await;
        let mut locked = state.lock().await;
        let tip_slot = locked.latest().tip_slot;
        let s = locked.update_slot(tip_slot)?;
        let mut n = 0usize;
        for ident in idents {
            let Some(existing) = s.pools.get(ident) else {
                continue;
            };
            let mut pool = (**existing).clone();
            let ss = self.cached_ss_config(&pool.pool_datum, &cache);
            let bcl = self.cached_bcl_config(&pool.pool_datum, &cache);
            pool.pool_type = detect_pool_type(
                &pool.pool_datum,
                self.protocol.execution.as_ref(),
                cache.cs.get(ident),
                cache.cp.get(ident),
                ss.as_ref(),
                bcl.as_ref(),
            );
            pool.fee_split_config = cache.fee_split.get(ident).cloned();
            s.pools.insert(ident.clone(), Arc::new(pool));
            n += 1;
        }
        info!(
            pools = n,
            "v4: re-classified pools with recovered module configs"
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
            state.datums.add_metadata_datum((datum.datum.to_vec(), data));
        }

        // Hydrate the per-module config cache from DB so detect_pool_type
        // calls below resolve correctly even though the original Create tx
        // is long out of the indexer's stream.
        self.rehydrate_module_configs().await?;

        // Constraint script hashes — used to find the right entry in the
        // order datum's `constraints: List<(hash, Data)>` list (PR #11).
        let constraint_hashes: crate::sundaev4::ConstraintHashes = self
            .protocol
            .execution
            .as_ref()
            .map(|e| crate::sundaev4::ConstraintHashes::from_scripts(&e.module_scripts))
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
                            address: output.address.to_vec(),
                            value: output.value,
                            pool_datum,
                            pool_type,
                            slot: txo.created_slot,
                            fee_split_config: fs_cfg,
                        }),
                    );
                }
                "order" => {
                    match output.datum.try_parse::<crate::sundaev4::OrderDatum>(&datums).and_then(
                        |datum| {
                            crate::sundaev4::decode_order_constraint(&datum, &constraint_hashes)
                                .map(|c| (datum, c))
                        },
                    ) {
                        Ok((datum, constraint)) => {
                            state.orders.push(Arc::new(SundaeV4Order {
                                input: txo.txo_id,
                                address: output.address.to_vec(),
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
                    match output.datum.try_parse::<crate::sundaev4::OrderDatum>(&datums).and_then(
                        |datum| {
                            crate::sundaev4::decode_order_constraint(&datum, &constraint_hashes)
                                .map(|c| (datum, c))
                        },
                    ) {
                        Ok((datum, constraint)) => {
                            state.orders.push(Arc::new(SundaeV4Order {
                                input: txo.txo_id,
                                address: output.address.to_vec(),
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
                    let token_name =
                        output.value.0.get(&self.protocol.settings_nft.policy).and_then(|tokens| {
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
                "fee_settings" => {
                    let parsed: Option<crate::sundaev4::types::FeeSettingsDatum> =
                        output.datum.parse(&datums);
                    if let Some(fs) = parsed {
                        use num_traits::ToPrimitive;
                        if let Some(base_fee) = fs.base_fee.unwrap().to_u64() {
                            let token = self
                                .protocol
                                .fee_settings_token
                                .as_ref()
                                .and_then(|t| hex::decode(t).ok())
                                .unwrap_or_default();
                            state.fee_settings =
                                Some(Arc::new(crate::sundaev4::types::SundaeV4FeeSettings {
                                    input: txo.txo_id,
                                    token,
                                    base_fee,
                                    slot: txo.created_slot,
                                }));
                        }
                    } else {
                        warn!(input = %txo.txo_id, "v4: fee_settings txo could not be reparsed on load");
                    }
                }
                "wallet" => {
                    state.wallet_utxos.insert(txo.txo_id, output.value);
                }
                "vault" => {
                    let datum = output.datum.parse(&datums);
                    state.vault_utxos.insert(
                        txo.txo_id,
                        crate::sundaev4::rate_guard::VaultUtxo {
                            address: output.address.to_vec(),
                            value: output.value,
                            datum,
                        },
                    );
                }
                "ref" => {
                    state.ref_utxo_outputs.insert(txo.txo_id, output);
                }
                "cswaps_order" => {
                    match crate::sundaev4::cardano_swaps::SwapOrder::from_output(
                        txo.txo_id.clone(),
                        &output,
                    ) {
                        Some(order) => {
                            state.cardano_swaps.orders.insert(txo.txo_id, order);
                        }
                        None => {
                            warn!(input = %txo.txo_id, "v4: cardano-swaps order txo could not be reparsed on load")
                        }
                    }
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
                    let pd =
                        minicbor::decode(bytes).context("could not parse spent persisted CBOR")?;
                    Some(pd)
                }
                None => None,
            };
            let datums = state.datums.for_persisted_txo(datum);
            let output = cardano_types::convert_txo(&parsed);
            let tx_id = stxo.spent_tx_id.map(hex::encode).unwrap_or_default();
            match stxo.txo.txo_type.as_str() {
                "order" => {
                    if let Some(od) = output.datum.parse::<crate::sundaev4::OrderDatum>(&datums)
                        && let Ok(constraint) =
                            crate::sundaev4::Constraint::from_order_datum_with_strategy(
                                &od,
                                &constraint_hashes,
                            )
                    {
                        state.spent_orders.push(SpentOrder {
                            order: Arc::new(SundaeV4Order {
                                input: stxo.txo.txo_id,
                                address: output.address.to_vec(),
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
                "pool" => {
                    if let Some(pd) = self.parse_pool(&output, &datums) {
                        let pool_type = self.detect_pool_type_with_cache(&pd, &cache);
                        let fs_cfg = cache.fee_split.get(&pd.identifier).cloned();
                        state.spent_pools.push(SpentPool {
                            id: pd.identifier.clone(),
                            old_pool: Arc::new(SundaeV4Pool {
                                input: stxo.txo.txo_id,
                                address: output.address.to_vec(),
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
        state.scoop_stats = build_scoop_stats(&scoop_records, self.scooper_keyhash.as_ref());

        state.tip_slot = slot;
        self.loaded_slot = slot;
        let mut history = self.state.lock().await;
        history.rollback_to_origin();
        *history.update_slot(slot)? = state;
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
        let ss = self.cached_ss_config(pool_datum, cache);
        let bcl = self.cached_bcl_config(pool_datum, cache);
        detect_pool_type(
            pool_datum,
            self.protocol.execution.as_ref(),
            cs,
            cp,
            ss.as_ref(),
            bcl.as_ref(),
        )
    }

    /// The cached banded ladder for a pool, only when its hash is the one
    /// the datum's `module_state` names.
    fn cached_bcl_config(
        &self,
        pool_datum: &PoolDatum,
        cache: &PoolModuleConfigCache,
    ) -> Option<crate::sundaev4::types::BandedCLConfig> {
        let h = self.pool_invariant_hash(
            pool_datum,
            crate::sundaev4::ModuleKind::ConcentratedLiquidity,
        )?;
        resolve_bcl_config(
            pool_datum,
            h.as_ref(),
            &[],
            cache.bcl.get(&pool_datum.identifier),
        )
    }

    /// The cached stableswap config for a pool, only when its hash is the
    /// one the datum's `module_state` names. A cached config a rate update
    /// has left behind is not returned: pricing against it would build a tx
    /// the module rejects.
    fn cached_ss_config(
        &self,
        pool_datum: &PoolDatum,
        cache: &PoolModuleConfigCache,
    ) -> Option<crate::sundaev4::types::StableSwapConfig> {
        let ss_hash =
            self.pool_invariant_hash(pool_datum, crate::sundaev4::ModuleKind::Stableswap)?;
        resolve_ss_config(
            pool_datum,
            ss_hash.as_ref(),
            &[],
            cache.ss.get(&pool_datum.identifier),
        )
    }

    /// The hash of the pool's invariant module, when it has kind `kind`.
    fn pool_invariant_hash(
        &self,
        pool_datum: &PoolDatum,
        kind: crate::sundaev4::ModuleKind,
    ) -> Option<ScriptHash> {
        let (k, h) = self.protocol.execution.as_ref()?.module_scripts.pool_invariant(pool_datum)?;
        (k == kind).then_some(h)
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

    /// Try to parse a settings-address UTxO as the FeeSettings node: it must
    /// carry the configured fee-settings entry token under the settings
    /// policy and a `FeeSettings { base_fee }` datum.
    fn parse_fee_settings(
        &self,
        tx_out: &TransactionOutput,
        datums: &ScopedDatumLookup,
    ) -> Option<u64> {
        use num_traits::ToPrimitive;
        let want_token = hex::decode(self.protocol.fee_settings_token.as_ref()?).ok()?;
        let settings_policy = &self.protocol.settings_nft.policy;
        let has_token = tx_out
            .value
            .0
            .get(settings_policy)
            .map(|tokens| {
                tokens.iter().any(|(name, qty)| {
                    name.as_slice() == want_token.as_slice() && qty.is_positive()
                })
            })
            .unwrap_or(false);
        if !has_token {
            return None;
        }
        let fs: crate::sundaev4::types::FeeSettingsDatum = tx_out.datum.parse(datums)?;
        fs.base_fee.unwrap().to_u64()
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
        let token_name = tx_out.value.0.get(settings_policy).and_then(|tokens| {
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
}

/// The reference-script UTxOs the indexer must keep so the evaluator's
/// script store holds every module a scoop can invoke. Every module in the
/// config is tracked: a module missing here fails eval with
/// "script … not found in store" on every scoop that references it, which is
/// how the fee_constraint module went missing on mainnet.
pub fn module_ref_utxos(
    scripts: &crate::sundaev4::types::ModuleScripts,
) -> BTreeSet<crate::cardano_types::TransactionInput> {
    scripts.all_scripts().map(|s| s.ref_utxo.clone()).collect()
}

/// True when `reward_account` is the script reward account for `script_hash`
/// on any network. A reward account is one header byte (0xe0/0xe1 key,
/// 0xf0/0xf1 script; low nibble = network id) followed by the 28-byte
/// credential. Matching on the credential and the script nibble keeps the
/// extractors network-agnostic: the previous `0xf0 || hash` comparison was
/// the testnet form and never matched on mainnet, so every mainnet pool fell
/// back to default module configs.
pub fn is_script_reward_account(reward_account: &[u8], script_hash: &ScriptHash) -> bool {
    reward_account.len() == 29
        && reward_account[0] >> 4 == 0xf
        && &reward_account[1..] == script_hash.as_ref()
}

/// Index of `script_hash`'s withdrawal in the tx's canonical (sorted)
/// withdrawal set — the index a `Reward` redeemer refers to.
pub fn withdrawal_index_of_script(tx: &MultiEraTx, script_hash: &ScriptHash) -> Option<usize> {
    tx.withdrawals_sorted_set()
        .iter()
        .position(|(account, _)| is_script_reward_account(account, script_hash))
}

/// Free-function form: try to extract a `ConstantSumConfig` from a tx's
/// CS module Create withdrawal redeemer. Used by both the live indexer and
/// the bootstrap path (which fetches a historical tx via Blockfrost).
#[cfg(test)]
pub fn extract_cs_config_from_tx(
    tx: &MultiEraTx,
    cs_script_hash: &ScriptHash,
) -> Option<crate::sundaev4::types::ConstantSumConfig> {
    use crate::sundaev4::types::{ConstantSumConfig, ConstantSumRedeemer};

    let wd_index = withdrawal_index_of_script(tx, cs_script_hash)?;
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
#[cfg(test)]
pub fn extract_fee_split_config_from_tx(
    tx: &MultiEraTx,
    fs_script_hash: &ScriptHash,
) -> Option<crate::sundaev4::types::FeeSplitConfig> {
    use crate::sundaev4::types::FeeSplitRedeemer;

    let wd_index = withdrawal_index_of_script(tx, fs_script_hash)?;
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

/// The value of `datum`'s `module_state` slot for `module_hash`.
pub fn module_slot<'a>(datum: &'a PoolDatum, module_hash: &[u8]) -> Option<&'a [u8]> {
    datum
        .module_state
        .iter()
        .find(|(cred, _)| cred.as_slice() == module_hash)
        .map(|(_, v)| v.as_slice())
}

/// `blake2b_256(serialise_data(config))`: the value a module keeps in the
/// pool's `module_state` slot for its config. The same rule holds for
/// constant_product, constant_sum, fee_split, stableswap and CL.
pub fn module_config_hash<T: AsPlutus + Clone>(cfg: &T) -> Vec<u8> {
    let cbor = minicbor::to_vec(cfg.clone().to_plutus()).expect("PlutusData encodes");
    Hasher::<256>::hash(&cbor).to_vec()
}

/// True when `cfg` hashes to `datum`'s slot for `module_hash`.
pub fn config_hashes_to_slot<T: AsPlutus + Clone>(
    cfg: &T,
    datum: &PoolDatum,
    module_hash: &[u8],
) -> bool {
    module_slot(datum, module_hash).is_some_and(|slot| slot == module_config_hash(cfg).as_slice())
}

/// The data of the `Reward` redeemer for `script_hash`'s withdrawal.
fn reward_redeemer_data(tx: &MultiEraTx, script_hash: &ScriptHash) -> Option<PlutusData> {
    let wd_index = withdrawal_index_of_script(tx, script_hash)?;
    tx.redeemers()
        .iter()
        .find(|r| r.tag() == RedeemerTag::Reward && r.index() == wd_index as u32)
        .map(|r| r.data().clone())
}

/// Every `ConstantSumConfig` the CS withdrawal redeemer of `cs_script_hash`
/// in `tx` holds: a `Create`'s `initial_state` and each `Operate` entry's
/// `config`. An `Operate` entry can belong to any pool of the scoop, so the
/// caller must accept a candidate only when it hashes to the pool's slot.
pub fn extract_cs_config_candidates(
    tx: &MultiEraTx,
    cs_script_hash: &ScriptHash,
) -> Vec<crate::sundaev4::types::ConstantSumConfig> {
    use crate::sundaev4::types::{ConstantSumConfig, ConstantSumRedeemer};
    let Some(data) = reward_redeemer_data(tx, cs_script_hash) else {
        return vec![];
    };
    match ConstantSumRedeemer::from_plutus(data) {
        Ok(ConstantSumRedeemer::Create { initial_state, .. }) => {
            ConstantSumConfig::from_plutus(initial_state).into_iter().collect()
        }
        Ok(ConstantSumRedeemer::Operate { entries }) => {
            entries.into_iter().map(|e| e.config).collect()
        }
        _ => vec![],
    }
}

/// [`extract_cs_config_candidates`] for constant_product.
pub fn extract_cp_config_candidates(
    tx: &MultiEraTx,
    cp_script_hash: &ScriptHash,
) -> Vec<crate::sundaev4::types::ConstantProductConfig> {
    use crate::sundaev4::types::ConstantProductRedeemer;
    let Some(data) = reward_redeemer_data(tx, cp_script_hash) else {
        return vec![];
    };
    match ConstantProductRedeemer::from_plutus(data) {
        Ok(ConstantProductRedeemer::Create { initial_state }) => vec![initial_state],
        Ok(ConstantProductRedeemer::Operate { entries }) => {
            entries.into_iter().map(|e| e.config).collect()
        }
        _ => vec![],
    }
}

/// [`extract_cs_config_candidates`] for fee_split.
pub fn extract_fee_split_config_candidates(
    tx: &MultiEraTx,
    fs_script_hash: &ScriptHash,
) -> Vec<crate::sundaev4::types::FeeSplitConfig> {
    use crate::sundaev4::types::FeeSplitRedeemer;
    let Some(data) = reward_redeemer_data(tx, fs_script_hash) else {
        return vec![];
    };
    match FeeSplitRedeemer::from_plutus(data) {
        Ok(FeeSplitRedeemer::Create { config, .. }) => vec![config],
        Ok(FeeSplitRedeemer::Operate { entries }) => {
            entries.into_iter().map(|e| e.config).collect()
        }
        _ => vec![],
    }
}

/// The config of module kind `kind` for `datum` that `tx` holds, from the
/// withdrawal redeemer of ANY registered hash of that kind.
///
/// `current` is the hash of `kind` that the pool names now. The function
/// reads the redeemer of `current` first, then the redeemers of the other
/// registered hashes of `kind`. It accepts a candidate only when the
/// candidate hashes to the datum's slot for `current`.
///
/// Why every hash of the kind: a governor can move a pool to a new module
/// hash in place. The migration transaction rewrites the action map and the
/// `module_state` keys but carries no module redeemer. If the config type
/// did not change, the slot value did not change, and the config is only in
/// redeemers under the old hash. The slot check makes the wider search safe.
pub fn find_slot_config<T: AsPlutus + Clone>(
    tx: &MultiEraTx,
    datum: &PoolDatum,
    scripts: &crate::sundaev4::types::ModuleScripts,
    kind: crate::sundaev4::ModuleKind,
    current: &ScriptHash,
    extract: impl Fn(&MultiEraTx, &ScriptHash) -> Vec<T>,
) -> Option<T> {
    let expected = module_slot(datum, current.as_ref())?;
    scripts.hashes_of_kind_first(kind, current).iter().find_map(|h| {
        extract(tx, h).into_iter().find(|c| module_config_hash(c).as_slice() == expected)
    })
}

/// Every stableswap candidate `tx` holds under any registered stableswap
/// hash, `current` first. The caller selects by the slot hash.
pub fn extract_ss_config_candidates_any(
    tx: &MultiEraTx,
    scripts: &crate::sundaev4::types::ModuleScripts,
    current: &ScriptHash,
) -> Vec<crate::sundaev4::types::StableSwapConfig> {
    scripts
        .hashes_of_kind_first(crate::sundaev4::ModuleKind::Stableswap, current)
        .iter()
        .flat_map(|h| extract_ss_config_candidates(tx, h))
        .collect()
}

/// Every `StableSwapConfig` a tx vouches for, from its stableswap module
/// withdrawal redeemer:
/// - `Create { initial_state, .. }`: the initial config;
/// - `Operate { entries }`: each entry's `config` (the config as stored in
///   that pool's INPUT), and — when the entry's pool spend redeemer opens
///   with a tag-7 rate update — the same config with the updated `rates`
///   (the config the OUTPUT datum hashes).
///
/// Which candidate a pool output uses is decided by [`resolve_ss_config`]
/// against the datum's `module_state`; this function only enumerates.
pub fn extract_ss_config_candidates(
    tx: &MultiEraTx,
    ss_script_hash: &ScriptHash,
) -> Vec<crate::sundaev4::types::StableSwapConfig> {
    use crate::sundaev4::types::{
        PoolRedeemer, RateUpdate, StableSwapConfig, StableSwapRedeemer, TAG_UPDATE_RATES,
    };

    let Some(wd_index) = withdrawal_index_of_script(tx, ss_script_hash) else {
        return vec![];
    };
    let redeemers = tx.redeemers();
    let Some(redeemer) =
        redeemers.iter().find(|r| r.tag() == RedeemerTag::Reward && r.index() == wd_index as u32)
    else {
        return vec![];
    };
    let Ok(parsed) = StableSwapRedeemer::from_plutus(redeemer.data().clone()) else {
        return vec![];
    };
    match parsed {
        StableSwapRedeemer::Create { initial_state, .. } => vec![initial_state],
        StableSwapRedeemer::Operate { entries } => {
            // Spend redeemer indices refer to the canonical (sorted) input set.
            let mut inputs: Vec<(Vec<u8>, u64)> =
                tx.inputs().iter().map(|i| (i.hash().to_vec(), i.index())).collect();
            inputs.sort();
            let mut out = Vec::with_capacity(entries.len() * 2);
            for entry in entries {
                out.push(entry.config.clone());
                let key = (
                    entry.pool_oref.transaction_id.clone(),
                    entry.pool_oref.output_index,
                );
                let Some(spend_index) = inputs.iter().position(|k| *k == key) else {
                    continue;
                };
                let Some(spend) = redeemers
                    .iter()
                    .find(|r| r.tag() == RedeemerTag::Spend && r.index() == spend_index as u32)
                else {
                    continue;
                };
                let Ok(PoolRedeemer::Action { transcript, .. }) =
                    PoolRedeemer::from_plutus(spend.data().clone())
                else {
                    continue;
                };
                let Some(first) = transcript.first() else {
                    continue;
                };
                if first.operation_tag != crate::bigint::BigInt::from(TAG_UPDATE_RATES) {
                    continue;
                }
                let Ok(update) = RateUpdate::from_plutus(first.operation_data.clone()) else {
                    continue;
                };
                out.push(StableSwapConfig {
                    rates: update.rates,
                    ..entry.config
                });
            }
            out
        }
        StableSwapRedeemer::Destroy { .. } => vec![],
    }
}

/// Every banded ladder a transaction vouches for: a `Create`'s
/// `initial_state`, and each `Operate` entry's `config`.
///
/// The caller accepts a candidate only when it hashes to the pool datum's
/// `module_state` slot, so this function is deliberately permissive. It does
/// two things beyond reading the current module's own withdrawal redeemer:
///
///  1. It reads EVERY withdrawal redeemer in the transaction, not only the
///     one at `bcl_script_hash`. A pool created under an earlier banded
///     module hash keeps its ladder; the hash the config must match is in
///     the pool datum, not in the withdrawal.
///  2. It parses the pre-V13 redeemer form as well, and rebuilds the index
///     from the bands (`banded_math::with_index`). An index-swap governor
///     adds the index in place without writing the new config to any
///     redeemer, so the derived form is the only way to recover the config
///     of a migrated pool from chain history.
///  3. After each four-field candidate it adds the same ladder with
///     `ext = Void` (`banded_math::config_forms`). The preview governor
///     migration of 2026-11-14 wrote that five-field config's hash to the
///     pool's slot and the config itself to no redeemer.
///  4. It reads both `Operate` forms: the config table the module reads,
///     and the one-config-per-entry form of the pre-audit preview modules
///     (`BandedCLRedeemer::from_plutus_any_form`).
///
/// All four rules only ADD candidates. The hash pin rejects anything wrong.
pub fn extract_bcl_config_candidates(
    tx: &MultiEraTx,
    bcl_script_hash: &ScriptHash,
) -> Vec<crate::sundaev4::types::BandedCLConfig> {
    use crate::sundaev4::banded_math::with_index;
    use crate::sundaev4::types::{BandedCLRedeemer, LegacyBandedCLRedeemer};
    // Read the configured module's redeemer first, so its candidates come
    // first in the list; then every other withdrawal redeemer.
    let preferred = withdrawal_index_of_script(tx, bcl_script_hash).map(|i| i as u32);
    let mut redeemers: Vec<_> =
        tx.redeemers().into_iter().filter(|r| r.tag() == RedeemerTag::Reward).collect();
    redeemers.sort_by_key(|r| (Some(r.index()) != preferred, r.index()));

    let mut found = vec![];
    for redeemer in redeemers {
        let data = redeemer.data().clone();
        let out = &mut found;
        if let Ok(parsed) = BandedCLRedeemer::from_plutus_any_form(data.clone()) {
            match parsed {
                BandedCLRedeemer::Create { initial_state, .. } => out.push(initial_state),
                BandedCLRedeemer::Operate { entries } => {
                    out.extend(entries.into_iter().map(|e| e.config))
                }
                BandedCLRedeemer::Destroy { .. } => {}
            }
            continue;
        }
        let Ok(parsed) = LegacyBandedCLRedeemer::from_plutus(data) else {
            continue;
        };
        let rebuild = |c: crate::sundaev4::types::LegacyBandedCLConfig| {
            with_index(c.bands, c.closing, c.weight_total)
        };
        match parsed {
            LegacyBandedCLRedeemer::Create { initial_state, .. } => {
                out.push(rebuild(initial_state))
            }
            LegacyBandedCLRedeemer::Operate { entries } => {
                out.extend(entries.into_iter().map(|e| rebuild(e.config)))
            }
            LegacyBandedCLRedeemer::Destroy { .. } => {}
        }
    }
    found.iter().flat_map(crate::sundaev4::banded_math::config_forms).collect()
}

/// The banded ladder for `pool_datum`: the first of `candidates`, else
/// a form of `cached` (`banded_math::config_forms`), whose hash the datum's
/// banded `module_state` slot names, and which passes the module's
/// Create-time shape checks. `None` when nothing matches; the tx builder
/// then refuses the pool.
///
/// The cached config's `ext = Void` form is how a pool that a governor has
/// just migrated from the four-field config is recovered while indexing:
/// the migration transaction holds no CL redeemer.
pub fn resolve_bcl_config(
    pool_datum: &PoolDatum,
    bcl_module_hash: &[u8],
    candidates: &[crate::sundaev4::types::BandedCLConfig],
    cached: Option<&crate::sundaev4::types::BandedCLConfig>,
) -> Option<crate::sundaev4::types::BandedCLConfig> {
    use crate::sundaev4::banded_math::{check_shape, config_matches_datum};
    let accepted = |c: &crate::sundaev4::types::BandedCLConfig| -> bool {
        config_matches_datum(c, pool_datum, bcl_module_hash)
            && match check_shape(c) {
                Ok(()) => true,
                Err(e) => {
                    warn!(
                        pool = %hex::encode(pool_datum.identifier.to_bytes()),
                        "v4: banded config hashes to module_state but fails the module's shape checks: {e}"
                    );
                    false
                }
            }
    };
    if let Some(c) = candidates.iter().find(|c| accepted(c)) {
        return Some(c.clone());
    }
    cached.and_then(|c| crate::sundaev4::banded_math::config_forms(c).find(|c| accepted(c)))
}

/// The stableswap config for `pool_datum`: the first of `candidates`, else
/// `cached`, whose hash the datum's `module_state` slot for the stableswap
/// module names. `None` when the pool has no stableswap slot or nothing
/// matches — a cached config that no longer matches is stale (a rate update
/// this scooper did not index) and is reported, not returned.
pub fn resolve_ss_config(
    pool_datum: &PoolDatum,
    ss_module_hash: &[u8],
    candidates: &[crate::sundaev4::types::StableSwapConfig],
    cached: Option<&crate::sundaev4::types::StableSwapConfig>,
) -> Option<crate::sundaev4::types::StableSwapConfig> {
    use crate::sundaev4::ss_math::{check_config, config_matches_datum};
    // A config the module's Create bounds reject cannot have come from a
    // real pool; treat such a candidate as no match.
    let accepted = |c: &crate::sundaev4::types::StableSwapConfig| -> bool {
        config_matches_datum(c, pool_datum, ss_module_hash)
            && match check_config(c) {
                Ok(()) => true,
                Err(e) => {
                    warn!(
                        pool = %hex::encode(pool_datum.identifier.to_bytes()),
                        "v4: stableswap config hashes to module_state but fails the module's bounds: {e}"
                    );
                    false
                }
            }
    };
    if let Some(c) = candidates.iter().find(|c| accepted(c)) {
        return Some(c.clone());
    }
    let cached = cached?;
    if accepted(cached) {
        return Some(cached.clone());
    }
    warn!(
        pool = %hex::encode(pool_datum.identifier.to_bytes()),
        "v4: cached stableswap config does not hash to the pool's module_state — a rate update \
         was not indexed; scoops against this pool are refused until the config is recovered \
         (a restart recovers it from the pool's tx history)",
    );
    None
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

    let wd_index = withdrawal_index_of_script(tx, fs_script_hash)?;
    let redeemers = tx.redeemers();
    let redeemer = redeemers
        .iter()
        .find(|r| r.tag() == RedeemerTag::Reward && r.index() == wd_index as u32)?;
    let parsed: FeeSplitRedeemer = AsPlutus::from_plutus(redeemer.data().clone()).ok()?;
    match parsed {
        FeeSplitRedeemer::Create { config, .. } => Some(config),
        FeeSplitRedeemer::Operate { entries } => {
            entries.into_iter().find(|e| &e.pool_oref == pool_oref).map(|e| e.config)
        }
        FeeSplitRedeemer::Destroy { .. } => None,
    }
}

#[async_trait]
impl ChainIndex for SundaeV4Indexer {
    fn name(&self) -> String {
        "sundae-v4".to_string()
    }

    async fn handle_block(&mut self, info: &BlockInfo) -> Result<()> {
        if info.slot < self.loaded_slot {
            // Block already covered by load(). Update network tip and emit
            // TipAdvanced so the scooper can detect sync progress, but skip
            // the state mutation that would fail with "cannot update slot".
            let mut history = self.state.lock().await;
            let state = history.update_slot(self.loaded_slot)?;
            if let Some(tip) = info.tip_slot {
                state.network_tip_slot = Some(tip);
            }
            self.tip_event_counter += 1;
            let at_tip = state.network_tip_slot.is_some_and(|net| self.loaded_slot + 10 >= net);
            if at_tip || self.tip_event_counter.is_multiple_of(100) {
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
        let at_tip = state.network_tip_slot.is_some_and(|net| info.slot + 10 >= net);
        if at_tip || self.tip_event_counter.is_multiple_of(100) {
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
        if info.slot < self.loaded_slot {
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
        // (token_name, OrderConfigEntry) — newly-discovered OrderConfig
        // settings entries in this tx.
        let mut new_fee_settings: Option<Arc<crate::sundaev4::types::SundaeV4FeeSettings>> = None;
        let mut new_order_configs: Vec<(Vec<u8>, Arc<SundaeV4OrderConfig>)> = vec![];
        let mut changes = TxChanges::new(info.slot, info.number);
        let mut events: Vec<IndexEvent> = vec![];
        let mut cswaps_changed = false;

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

        let constraint_hashes: crate::sundaev4::ConstraintHashes = self
            .protocol
            .execution
            .as_ref()
            .map(|e| crate::sundaev4::ConstraintHashes::from_scripts(&e.module_scripts))
            .unwrap_or_default();

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
                    // Extract per-module configs from this tx's Create/Operate
                    // redeemers, for the module hashes THIS pool names. Two
                    // pools can name different hashes of one kind (for
                    // example two fee_split versions). For a scoop tx without
                    // a Create redeemer for a module, these return None.
                    use crate::sundaev4::ModuleKind;
                    let scripts = self.protocol.execution.as_ref().map(|e| &e.module_scripts);
                    let invariant = scripts.and_then(|m| m.pool_invariant(&pd));
                    let invariant_hash =
                        |kind: ModuleKind| invariant.filter(|(k, _)| *k == kind).map(|(_, h)| h);
                    let cs_module_hash = invariant_hash(ModuleKind::ConstantSum);
                    let cp_module_hash = invariant_hash(ModuleKind::ConstantProduct);
                    let ss_module_hash = invariant_hash(ModuleKind::Stableswap);
                    let bcl_module_hash = invariant_hash(ModuleKind::ConcentratedLiquidity);
                    let fs_module_hash =
                        scripts.and_then(|m| m.pool_module_of_kind(&pd, ModuleKind::FeeSplit));
                    // CS, CP and fee_split: a Create or Operate config
                    // under any registered hash of the kind, accepted only
                    // when it hashes to this output's slot
                    // (`find_slot_config`).
                    let slot_config = |kind: ModuleKind, h: Option<ScriptHash>| {
                        scripts.zip(h).map(|(m, h)| (m, h, kind))
                    };
                    let cs_config_from_tx = slot_config(ModuleKind::ConstantSum, cs_module_hash)
                        .and_then(|(m, h, k)| {
                            find_slot_config(&tx, &pd, m, k, &h, extract_cs_config_candidates)
                        });
                    let cp_config_from_tx =
                        slot_config(ModuleKind::ConstantProduct, cp_module_hash).and_then(
                            |(m, h, k)| {
                                find_slot_config(&tx, &pd, m, k, &h, extract_cp_config_candidates)
                            },
                        );
                    let fs_config_from_tx = slot_config(ModuleKind::FeeSplit, fs_module_hash)
                        .and_then(|(m, h, k)| {
                            find_slot_config(
                                &tx,
                                &pd,
                                m,
                                k,
                                &h,
                                extract_fee_split_config_candidates,
                            )
                        });
                    // Stableswap: every config this tx vouches for (Create
                    // initial_state, Operate entry configs, and the
                    // post-rate-update form of each), under any registered
                    // stableswap hash. The datum's module_state hash
                    // selects one below.
                    let ss_candidates_from_tx: Vec<crate::sundaev4::types::StableSwapConfig> =
                        scripts
                            .zip(ss_module_hash)
                            .map(|(m, h)| extract_ss_config_candidates_any(&tx, m, &h))
                            .unwrap_or_default();
                    // Banded: the ladder a Create pins or an Operate entry re-sends.
                    let bcl_candidates_from_tx: Vec<crate::sundaev4::types::BandedCLConfig> =
                        bcl_module_hash
                            .map(|h| extract_bcl_config_candidates(&tx, &h))
                            .unwrap_or_default();
                    let cs_module_hash_bytes = cs_module_hash.map(|h| h.as_ref().to_vec());
                    let cp_module_hash_bytes = cp_module_hash.map(|h| h.as_ref().to_vec());
                    let ss_module_hash_bytes = ss_module_hash.map(|h| h.as_ref().to_vec());
                    let bcl_module_hash_bytes = bcl_module_hash.map(|h| h.as_ref().to_vec());
                    let fs_module_hash_bytes = fs_module_hash.map(|h| h.as_ref().to_vec());
                    // Resolution priority for the CS/CP branch inside detect_pool_type:
                    //   1. config-file override (consulted internally)
                    //   2. this tx's Create or Operate redeemer (slot-checked)
                    //   3. previously-resolved persisted config (cache)
                    //   4. defaults
                    let resolved_cs =
                        cs_config_from_tx.as_ref().or_else(|| module_cache.cs.get(&pool_id));
                    let resolved_cp =
                        cp_config_from_tx.as_ref().or_else(|| module_cache.cp.get(&pool_id));
                    // Stableswap: the datum's module_state hash selects among
                    // this tx's candidates and the cache. A cached config a
                    // rate update has superseded is not used.
                    let resolved_ss = ss_module_hash_bytes.as_ref().and_then(|h| {
                        resolve_ss_config(
                            &pd,
                            h,
                            &ss_candidates_from_tx,
                            module_cache.ss.get(&pool_id),
                        )
                    });
                    let resolved_bcl = bcl_module_hash_bytes.as_ref().and_then(|h| {
                        resolve_bcl_config(
                            &pd,
                            h,
                            &bcl_candidates_from_tx,
                            module_cache.bcl.get(&pool_id),
                        )
                    });
                    let pool_type = detect_pool_type(
                        &pd,
                        self.protocol.execution.as_ref(),
                        resolved_cs,
                        resolved_cp,
                        resolved_ss.as_ref(),
                        resolved_bcl.as_ref(),
                    );

                    // Banded: persist on first sight. The ladder is fixed
                    // after Create, so a row never changes.
                    if let (Some(cfg), Some(bcl_hash)) =
                        (resolved_bcl.as_ref(), bcl_module_hash_bytes.as_ref())
                        && module_cache.bcl.get(&pool_id) != Some(cfg)
                    {
                        let cbor = minicbor::to_vec(cfg.clone().to_plutus())
                            .context("encode BandedCLConfig CBOR")?;
                        changes.module_configs.push(PersistedModuleConfig {
                            pool_id: pool_id.to_bytes().to_vec(),
                            module_hash: bcl_hash.clone(),
                            config_cbor: cbor,
                            created_slot: slot,
                        });
                        module_cache.bcl.insert(pool_id.clone(), cfg.clone());
                        info!(
                            pool = %hex::encode(pool_id.to_bytes()),
                            bands = cfg.bands.len(),
                            "v4: persisted banded pool config from redeemer"
                        );
                    }

                    // Stableswap: persist on first sight AND on change (a
                    // tag-7 rate update rewrites `rates`; the row is upserted).
                    if let (Some(cfg), Some(ss_hash)) =
                        (resolved_ss.as_ref(), ss_module_hash_bytes.as_ref())
                        && module_cache.ss.get(&pool_id) != Some(cfg)
                    {
                        let cbor = minicbor::to_vec(cfg.clone().to_plutus())
                            .context("encode StableSwapConfig CBOR")?;
                        changes.module_configs.push(PersistedModuleConfig {
                            pool_id: pool_id.to_bytes().to_vec(),
                            module_hash: ss_hash.clone(),
                            config_cbor: cbor,
                            created_slot: slot,
                        });
                        module_cache.ss.insert(pool_id.clone(), cfg.clone());
                        info!(
                            pool = %hex::encode(pool_id.to_bytes()),
                            rates = ?cfg.rates.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
                            "v4: persisted stableswap pool config from redeemer"
                        );
                    }

                    // If we just learned this pool's CS config from a Create
                    // redeemer, persist it and remember it for future blocks.
                    if let (Some(cfg), crate::sundaev4::types::PoolType::ConstantSum { .. }) =
                        (cs_config_from_tx.as_ref(), &pool_type)
                        && !module_cache.cs.contains_key(&pool_id)
                    {
                        let cbor = minicbor::to_vec(cfg.clone().to_plutus())
                            .context("encode ConstantSumConfig CBOR")?;
                        changes.module_configs.push(PersistedModuleConfig {
                            pool_id: pool_id.to_bytes().to_vec(),
                            module_hash: cs_module_hash_bytes
                                .clone()
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
                    // Same for CP: persist on first sighting (Create or Operate).
                    if let (Some(cfg), crate::sundaev4::types::PoolType::ConstantProduct { .. }) =
                        (cp_config_from_tx.as_ref(), &pool_type)
                        && !module_cache.cp.contains_key(&pool_id)
                    {
                        let cbor = minicbor::to_vec(cfg.clone().to_plutus())
                            .context("encode ConstantProductConfig CBOR")?;
                        changes.module_configs.push(PersistedModuleConfig {
                            pool_id: pool_id.to_bytes().to_vec(),
                            module_hash: cp_module_hash_bytes
                                .clone()
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

                    // Same for fee_split: every pool has fee_split config in
                    // the mint tx (Create) and in every later scoop (Operate).
                    // Either form is usable; the first time we see it we
                    // persist and cache for future scoops.
                    if let (Some(cfg), Some(fs_hash)) =
                        (fs_config_from_tx.as_ref(), fs_module_hash_bytes.as_ref())
                        && !module_cache.fee_split.contains_key(&pool_id)
                    {
                        let cbor = minicbor::to_vec(cfg.clone().to_plutus())
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

                    let fs_cfg = module_cache.fee_split.get(&pool_id).cloned();
                    let pool_record = SundaeV4Pool {
                        input: this_input,
                        address: address.to_vec(),
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
                match tx_out.datum.try_parse::<crate::sundaev4::OrderDatum>(&datums).and_then(
                    |datum| {
                        crate::sundaev4::decode_order_constraint(&datum, &constraint_hashes)
                            .map(|c| (datum, c))
                    },
                ) {
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
                            address: tx_out.address.to_vec(),
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
                } else if let Some(base_fee) = self.parse_fee_settings(&tx_out, &datums) {
                    changes.created_txos.push(PersistedTxo {
                        txo_id: this_input.clone(),
                        txo_type: "fee_settings".to_string(),
                        created_slot: slot,
                        era: output.era().into(),
                        txo: output.encode(),
                        address: tx_out.address.to_vec(),
                        datum: tx_out.hashed_datum(&datums),
                    });
                    new_fee_settings =
                        Some(Arc::new(crate::sundaev4::types::SundaeV4FeeSettings {
                            input: this_input,
                            token: self
                                .protocol
                                .fee_settings_token
                                .as_ref()
                                .and_then(|t| hex::decode(t).ok())
                                .unwrap_or_default(),
                            base_fee,
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

            // Cardano-Swaps orders: every owner has their own address, but
            // they all share the swap script as payment credential.
            if let Some(rt) = &self.cardano_swaps {
                let swap_hash: ScriptHash = rt.swap_hash.as_slice().into();
                if payment_hash_equals(&address, &swap_hash) {
                    let this_input = TransactionInput::new(this_tx_hash, ix as u64);
                    let tx_out = cardano_types::convert_txo(output);
                    match crate::sundaev4::cardano_swaps::SwapOrder::from_output(
                        this_input.clone(),
                        &tx_out,
                    ) {
                        Some(order) => {
                            trace!(slot, utxo = %this_input, "v4: cardano-swaps order spotted");
                            changes.created_txos.push(PersistedTxo {
                                txo_id: this_input.clone(),
                                txo_type: "cswaps_order".to_string(),
                                created_slot: slot,
                                era: output.era().into(),
                                txo: output.encode(),
                                address: tx_out.address.to_vec(),
                                datum: tx_out.hashed_datum(&datums),
                            });
                            state.cardano_swaps.orders.insert(this_input, order);
                            cswaps_changed = true;
                        }
                        None => {
                            debug!(slot, utxo = %this_input, "v4: cardano-swaps address output with an unparseable datum");
                        }
                    }
                }
            }

            // Track wallet UTxOs by payment credential — CIP-1852 wallets use
            // base addresses (payment + stake) but the scooper might be
            // configured with an enterprise address. Matching by payment
            // credential alone accepts any address form (enterprise, base,
            // pointer) whose payment key we hold.
            if let Some(scooper_kh) = &self.scooper_keyhash
                && payment_hash_equals(&address, scooper_kh)
            {
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

            // Track the rate guards' vault UTxOs: the backing asset and the
            // datum a guarded pool's true rate derives from. Matched by
            // payment credential like the wallet, so the vault's stake part
            // is irrelevant.
            if self.vault_payment_hashes.iter().any(|hash| payment_hash_equals(&address, hash)) {
                let this_input = TransactionInput::new(this_tx_hash, ix as u64);
                let tx_out = cardano_types::convert_txo(output);
                let datum = tx_out.datum.parse(&datums);
                trace!(slot, utxo = %this_input, has_datum = datum.is_some(), "v4: vault UTxO spotted");
                changes.created_txos.push(PersistedTxo {
                    txo_id: this_input.clone(),
                    txo_type: "vault".to_string(),
                    created_slot: slot,
                    era: output.era().into(),
                    txo: output.encode(),
                    address: tx_out.address.to_vec(),
                    datum: tx_out.hashed_datum(&datums),
                });
                state.vault_utxos.insert(
                    this_input,
                    crate::sundaev4::rate_guard::VaultUtxo {
                        address: tx_out.address.to_vec(),
                        value: tx_out.value,
                        datum,
                    },
                );
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

        // Remove spent wallet and vault UTxOs
        for input in &spent_inputs {
            if state.wallet_utxos.remove(input).is_some() {
                changes.spent_txos.push(SpentTxo {
                    input: input.clone(),
                    spending_tx_id: this_tx_hash.to_vec(),
                });
            }
            if state.vault_utxos.remove(input).is_some() {
                trace!(slot, utxo = %input, "v4: vault UTxO spent");
                changes.spent_txos.push(SpentTxo {
                    input: input.clone(),
                    spending_tx_id: this_tx_hash.to_vec(),
                });
            }
        }

        // Remove spent Cardano-Swaps orders. A fill spends the order and
        // recreates it, so both halves fire in the same transaction.
        if self.cardano_swaps.is_some() {
            for input in &spent_inputs {
                if state.cardano_swaps.orders.remove(input).is_some() {
                    cswaps_changed = true;
                    changes.spent_txos.push(SpentTxo {
                        input: input.clone(),
                        spending_tx_id: this_tx_hash.to_vec(),
                    });
                }
            }
        }
        if cswaps_changed {
            events.push(IndexEvent::CardanoSwapsOrderbookChanged {
                tx_id: hex::encode(this_tx_hash),
            });
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
        let scooper_hex = scooper_keyhash_bytes.as_ref().map(hex::encode).unwrap_or_default();

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
                            reason: SpentOrderReason::Scooped {
                                pool_ids: scoop_pool_ids.clone(),
                                scooper: scooper_hex.clone(),
                            },
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
            if let Some(total) =
                state.scoop_stats.scooper_totals.iter_mut().find(|t| t.scooper == scooper_key)
            {
                total.scoop_txs += 1;
                total.orders_processed += n_orders as u64;
            } else {
                state.scoop_stats.scooper_totals.push(ScooperTotal {
                    scooper: scooper_key,
                    scoop_txs: 1,
                    orders_processed: n_orders as u64,
                });
            }

            let pool_ids_hex: Vec<String> =
                scoop_pool_ids.iter().map(|id| hex::encode(id.to_bytes())).collect();
            state.scoop_stats.recent_scoops.insert(
                0,
                ScoopRecordView {
                    tx_id: tx_id_hex.clone(),
                    slot,
                    pool_ids: pool_ids_hex,
                    n_orders,
                    scooper: scooper_hex.clone(),
                },
            );
            state.scoop_stats.recent_scoops.truncate(50);
        }

        // Emit pool removed events
        for id in removed_pool_ids {
            events.push(IndexEvent::V4PoolRemoved {
                id,
                tx_id: tx_id_hex.clone(),
            });
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

        // Remove the FeeSettings node if spent (admin fee update); the
        // replacement is re-added via the settings-address branch above.
        if let Some(fs) = &state.fee_settings
            && spent_inputs.contains(&fs.input)
        {
            changes.spent_txos.push(SpentTxo {
                input: fs.input.clone(),
                spending_tx_id: this_tx_hash.to_vec(),
            });
            state.fee_settings = None;
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

        // The block at `loaded_slot` is re-processed on restart (a block
        // that was only partly applied must get the rest of its
        // transactions), so a transaction that was already applied finds
        // its orders already loaded. An output is created once on chain;
        // drop the ones the state already holds.
        if !new_orders.is_empty() || !new_invalid_orders.is_empty() {
            let held: std::collections::BTreeSet<&TransactionInput> = state
                .orders
                .iter()
                .map(|o| &o.input)
                .chain(state.invalid_orders.iter().map(|o| &o.input))
                .collect();
            new_orders.retain(|o| !held.contains(&o.input));
            new_invalid_orders.retain(|o| !held.contains(&o.input));
        }

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

        if let Some(fs) = new_fee_settings {
            info!(base_fee = fs.base_fee, "v4: FeeSettings node updated");
            state.fee_settings = Some(fs);
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

        // Invalid orders are NOT pruned by slot: an unparseable order stays
        // relevant for as long as its UTxO is unspent (spent ones are dropped
        // above when their input is consumed), and V4 orders never expire.
        // Bound the set by count instead so a spray of malformed orders can't
        // grow it without limit, keeping the newest by slot.
        if state.invalid_orders.len() > crate::config::INVALID_ORDER_CAP {
            state.invalid_orders.sort_by_key(|io| io.slot);
            let overflow = state.invalid_orders.len() - crate::config::INVALID_ORDER_CAP;
            state.invalid_orders.drain(..overflow);
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
        let _ = self.event_tx.send((to_slot, vec![IndexEvent::Rollback { to_slot }]));
        Ok(())
    }

    async fn reset(&mut self, point: &Point) -> Result<Point> {
        warn!("v4: clearing all state and resetting to {point}");
        self.dao.rollback(0).await?;
        self.state.lock().await.rollback_to_origin();
        self.loaded_slot = 0;
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

    let our_hex = our_keyhash.map(|h| hex::encode(h.as_ref())).unwrap_or_default();

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

/// Build the scooper's Shelley address. If `stake_keyhash_hex` is `Some`, a
/// base address (payment + staking) is produced — required to match base
/// addresses used by CIP-1852 wallets. Without it, an enterprise (payment-
/// only) address is returned, which doesn't see funds at base addresses.
pub(crate) fn derive_scooper_pallas_address_with_stake(
    secret_key_hex: &str,
    stake_keyhash_hex: Option<&str>,
    network: crate::sundaev4::AddressNetwork,
) -> Result<Address> {
    use pallas_addresses::{ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart};

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
        network.pallas(),
        ShelleyPaymentPart::Key(keyhash),
        delegation,
    );
    Ok(Address::from(shelley))
}

fn payment_hash_equals(addr: &Address, hash: &ScriptHash) -> bool {
    if let Address::Shelley(s_addr) = addr {
        s_addr.payment().as_hash() == hash
    } else {
        false
    }
}

/// `Ok` when every module config a scoop of `pool` sends hashes to the
/// pool datum's `module_state` slot for that module: the invariant config
/// from `pool_type`, and the fee_split config (the per-pool config, else the
/// scooper default that the tx builder falls back to).
///
/// A pool whose config bootstrap could not recover carries a guessed
/// default (`detect_pool_type`) or a placeholder. The module rejects a
/// config that does not hash to the slot, so the scooper must not scoop
/// such a pool. A guessed default passes only when it is the pool's real
/// config. The scooper drops a pool that fails this check before routing,
/// and the tx builder checks again before it builds.
pub fn module_configs_match_datum(
    pool: &SundaeV4Pool,
    exec: &crate::sundaev4::types::ScooperExecution,
) -> Result<(), String> {
    use crate::sundaev4::types::{
        ConstantProductConfig, ConstantSumConfig, FeeSplitConfig, PoolType, Rational,
    };
    let datum = &pool.pool_datum;
    let modules = exec.module_scripts.pool_modules(datum)?;
    let (kind, hash) = modules.invariant();
    let invariant_hash = match &pool.pool_type {
        PoolType::ConstantProduct { fee } => {
            module_config_hash(&ConstantProductConfig { fee: fee.clone() })
        }
        PoolType::ConstantSum {
            prices,
            fee,
            bounty_k,
            balance_fee,
        } => module_config_hash(&ConstantSumConfig {
            prices: prices.clone(),
            fee: fee.clone(),
            bounty_k: bounty_k.clone(),
            balance_fee: balance_fee.clone(),
        }),
        PoolType::StableSwap { config } => crate::sundaev4::ss_math::config_hash(config),
        PoolType::BandedConcentratedLiquidity { config } => {
            crate::sundaev4::banded_math::config_hash(config)
        }
    };
    let check = |module: &str, h: &ScriptHash, expected: &[u8]| -> Result<(), String> {
        match module_slot(datum, h.as_ref()) {
            Some(slot) if slot == expected => Ok(()),
            slot => Err(format!(
                "{module} config hash {} does not match the pool's module_state slot for {} \
                 ({}); the config is unknown or stale",
                hex::encode(expected),
                hex::encode(h),
                slot.map(hex::encode).unwrap_or_else(|| "absent".into()),
            )),
        }
    };
    check(&kind.to_string(), hash, &invariant_hash)?;
    if let Some(fs_hash) = modules.fee_split() {
        let cfg = pool.fee_split_config.clone().unwrap_or_else(|| FeeSplitConfig {
            protocol_share: Rational {
                num: crate::bigint::BigInt::from(exec.protocol_share.0),
                den: crate::bigint::BigInt::from(exec.protocol_share.1),
            },
        });
        check("fee_split", fs_hash, &module_config_hash(&cfg))?;
    }
    Ok(())
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
    ss_config: Option<&crate::sundaev4::types::StableSwapConfig>,
    bcl_config: Option<&crate::sundaev4::types::BandedCLConfig>,
) -> crate::sundaev4::types::PoolType {
    use crate::bigint::BigInt;
    use crate::sundaev4::types::{PoolType, Rational};

    let Some(exec) = execution else {
        return PoolType::ConstantProduct {
            fee: Rational {
                num: BigInt::from(0),
                den: BigInt::from(1),
            },
        };
    };

    // The scoop action is the first enabled action whose first module is a
    // configured invariant module. CS and CP swap actions use different tags
    // (cs_check.ak's tag_swap=3 vs CP's tag=100), so we can't pre-pick by
    // tag. The kind of that module's hash selects the pool type. Two hashes
    // of one kind (two versions of a module) classify the same way.
    use crate::sundaev4::ModuleKind;
    let ident_hex = hex::encode(pool_datum.identifier.to_bytes());
    let invariant = exec.module_scripts.pool_invariant(pool_datum);
    let matched_kind = invariant.map(|(k, _)| k);
    if matched_kind.is_none() {
        // Log the unknown first-module hashes so the operator can see why
        // detect_pool_type couldn't classify this pool. The scooper does not
        // scoop a pool whose modules it cannot resolve (see
        // `ModuleScripts::pool_modules`).
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
             the pool is not scooped (classified as ConstantProduct for display only)",
        );
    } else if let Err(e) = exec.module_scripts.pool_modules(pool_datum) {
        warn!(pool = %ident_hex, "pool's scoop action is not supported, the pool is not scooped: {e}");
    }

    if matched_kind.is_none() {
        return PoolType::ConstantProduct {
            fee: cp_config_from_tx.map(|c| c.fee.clone()).unwrap_or_else(|| Rational {
                num: BigInt::from(exec.fee.0),
                den: BigInt::from(exec.fee.1),
            }),
        };
    }

    if matched_kind == Some(ModuleKind::ConstantSum) {
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
                bounty_k: Rational {
                    num: BigInt::from(0),
                    den: BigInt::from(1),
                },
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
            bounty_k: Rational {
                num: BigInt::from(0),
                den: BigInt::from(1),
            },
            balance_fee: Rational {
                num: BigInt::from(exec.fee.0),
                den: BigInt::from(exec.fee.1),
            },
        };
    }

    if matched_kind == Some(ModuleKind::Stableswap) {
        if let Some(cfg) = ss_config {
            return PoolType::StableSwap {
                config: cfg.clone(),
            };
        }
        // No preimage that hashes to the datum's module_state slot. There is
        // no operator override for stableswap: a static override goes stale
        // on the first rate update, and the datum hash is authoritative. The
        // placeholder below cannot pass the tx builder's hash check, so the
        // pool is skipped loudly rather than mispriced.
        warn!(
            pool = %ident_hex,
            "stableswap pool has no config preimage matching its module_state (not yet indexed \
             from a Create/Operate redeemer, or a rate update was missed); scoops against it \
             are refused until it is recovered — a restart recovers from the pool's tx history"
        );
        return PoolType::StableSwap {
            config: crate::sundaev4::types::StableSwapConfig {
                linear_amplification: BigInt::from(1),
                fee: Rational {
                    num: BigInt::from(exec.fee.0),
                    den: BigInt::from(exec.fee.1),
                },
                rates: vec![BigInt::from(1); pool_datum.assets.len()],
                rate_manager: None,
                monotone_rates: false,
                max_rate_step: None,
            },
        };
    }

    if matched_kind == Some(ModuleKind::ConcentratedLiquidity) {
        // The module hashes five fields. A four-field config is the
        // pre-audit form: no implemented module reads it, so it cannot be
        // the config of a pool the scooper can scoop.
        if let Some(cfg) = bcl_config {
            if cfg.ext.is_some() {
                return PoolType::BandedConcentratedLiquidity {
                    config: cfg.clone(),
                };
            }
            warn!(pool = %ident_hex, "banded pool's config has four fields; it is refused");
        }
        // No preimage that hashes to the datum's module_state slot, and no
        // operator override: the ladder is hash-pinned and the module reads
        // it on every spend. The empty placeholder cannot pass the tx
        // builder's hash check, so the pool is skipped loudly.
        warn!(
            pool = %ident_hex,
            "banded pool has no config preimage matching its module_state (not yet indexed from \
             a Create/Operate redeemer); scoops against it are refused until it is recovered — \
             a restart recovers from the pool's tx history"
        );
        return PoolType::BandedConcentratedLiquidity {
            config: crate::sundaev4::types::BandedCLConfig {
                bands: vec![],
                index: vec![],
                closing: Rational {
                    num: BigInt::from(1),
                    den: BigInt::from(1),
                },
                weight_total: BigInt::from(0),
                ext: None,
            },
        };
    }

    // CP path: prefer the per-pool config (this tx's redeemer OR cached) over
    // the global default — different CP pools can have different fees, and
    // `verify_module_state` hashes the config we send.
    PoolType::ConstantProduct {
        fee: cp_config_from_tx.map(|c| c.fee.clone()).unwrap_or_else(|| Rational {
            num: BigInt::from(exec.fee.0),
            den: BigInt::from(exec.fee.1),
        }),
    }
}

#[cfg(test)]
mod mainnet_create_tx_tests {
    //! The mainnet SPRINKLES/JIMMIES pool's Create transaction
    //! (4b814760…, slot 198130613), fetched from Blockfrost. Its withdrawals
    //! are mainnet script reward accounts (header 0xf1). Until this fixture
    //! existed, the extractors compared against `0xf0 || hash` — the testnet
    //! form — matched nothing on mainnet, and every scooper fell back to
    //! default module configs that fail eval.
    use super::*;
    use crate::bigint::BigInt;
    use crate::sundaev4::types::Rational;

    const CREATE_TX_HEX: &str =
        include_str!("../../test/fixtures/mainnet-pool-create-4b814760.hex");
    const CS_HASH: &str = "4e1a435f8d55f26068150579c18964e58078082b899e6bb560be7cd5";
    const FEE_SPLIT_HASH: &str = "1351c687e6359b0030086fc7ddd6cd389f55f97faba3f531c5535688";

    fn create_tx() -> Vec<u8> {
        hex::decode(CREATE_TX_HEX.trim()).expect("fixture is hex")
    }

    fn ratio(num: i64, den: i64) -> Rational {
        Rational {
            num: BigInt::from(num),
            den: BigInt::from(den),
        }
    }

    #[test]
    fn mainnet_create_tx_yields_the_constant_sum_config() {
        let bytes = create_tx();
        let tx = MultiEraTx::decode(&bytes).expect("decodes");
        let cs_hash: ScriptHash = CS_HASH.parse().unwrap();
        let cfg = extract_cs_config_from_tx(&tx, &cs_hash)
            .expect("the Create redeemer carries the constant-sum config");
        assert_eq!(cfg.prices, vec![BigInt::from(1), BigInt::from(1)]);
        assert_eq!(cfg.fee, ratio(25, 10000));
        assert_eq!(cfg.bounty_k, ratio(25, 20000));
        assert_eq!(cfg.balance_fee, ratio(0, 1));
    }

    #[test]
    fn mainnet_create_tx_yields_the_fee_split_config() {
        let bytes = create_tx();
        let tx = MultiEraTx::decode(&bytes).expect("decodes");
        let fs_hash: ScriptHash = FEE_SPLIT_HASH.parse().unwrap();
        let cfg = extract_fee_split_config_from_tx(&tx, &fs_hash)
            .expect("the Create redeemer carries the fee_split config");
        assert_eq!(cfg.protocol_share, ratio(1, 5));
    }

    #[test]
    fn script_reward_account_matches_on_credential_and_script_nibble_only() {
        let hash: ScriptHash = CS_HASH.parse().unwrap();
        let mut mainnet = vec![0xf1u8];
        mainnet.extend_from_slice(hash.as_ref());
        let mut testnet = vec![0xf0u8];
        testnet.extend_from_slice(hash.as_ref());
        let mut key_account = vec![0xe1u8];
        key_account.extend_from_slice(hash.as_ref());
        let mut other_credential = hash.as_ref().to_vec();
        other_credential[0] ^= 0xff;
        let mut other_script = vec![0xf1u8];
        other_script.extend_from_slice(&other_credential);

        assert!(is_script_reward_account(&mainnet, &hash));
        assert!(is_script_reward_account(&testnet, &hash));
        assert!(
            !is_script_reward_account(&key_account, &hash),
            "a key credential is not the script"
        );
        assert!(!is_script_reward_account(&other_script, &hash));
        assert!(
            !is_script_reward_account(&mainnet[1..], &hash),
            "a bare credential has no header"
        );
    }

    #[test]
    fn every_configured_module_ref_utxo_is_tracked() {
        use crate::cardano_types::TransactionInput;
        use crate::sundaev4::types::{ModuleKind, ModuleScripts, ScriptRefInfo};
        let mut n = 0u8;
        let mut next = || {
            n += 1;
            ScriptRefInfo {
                hash: ScriptHash::new([n; 28]),
                ref_utxo: TransactionInput::new([n; 32].into(), 0),
                script_cbor: None,
            }
        };
        let mut roles: Vec<(String, Vec<ScriptRefInfo>)> =
            ["pool", "order", "pool-mint", "settings"]
                .into_iter()
                .map(|k| (k.to_string(), vec![next()]))
                .collect();
        for kind in ModuleKind::ALL {
            if kind == ModuleKind::Stableswap {
                continue;
            }
            roles.push((kind.config_key().to_string(), vec![next()]));
        }
        // A second fee_split version, as a CL deployment adds next to the
        // constant-sum one.
        roles.push(("fee-split".to_string(), vec![next()]));
        let scripts = ModuleScripts::from_role_map(roles).unwrap();
        let tracked = module_ref_utxos(&scripts);
        assert_eq!(
            tracked.len(),
            4 + 12 + 1,
            "four boot-fixed scripts, twelve module kinds, and a second fee_split"
        );
        for kind in [
            ModuleKind::ConcentratedLiquidity,
            ModuleKind::FeeConstraint,
            ModuleKind::FeeSplit,
        ] {
            for s in scripts.of_kind(kind) {
                assert!(
                    tracked.contains(&s.ref_utxo),
                    "{kind} is invoked by scoops and must be in the store"
                );
            }
        }
    }
}

#[cfg(test)]
mod preview_banded_recovery_tests {
    //! Preview pool 622cee5f…, Create transaction c601f694…. The pool was
    //! created under banded module 0977898c… with the pre-V13 config, and an
    //! index-swap governor later rewrote its `module_state` to the indexed
    //! config 361006908c…. The governor derives the new config on chain, so
    //! no redeemer anywhere holds it. Recovery must therefore read a
    //! withdrawal of a module hash it is not configured with, parse the old
    //! config form, and rebuild the index from the bands.
    use super::*;
    use crate::sundaev4::banded_math::config_hash;

    const CREATE_TX_HEX: &str =
        include_str!("../../test/fixtures/preview-banded-create-c601f694.hex");
    /// The banded module the scooper is configured with today.
    const NEW_BCL_HASH: &str = "33485bba2689d4b82b412fb683f07474b9c6c042778d3b9d2b691f18";
    /// The pool datum's banded `module_state` slot after the migration.
    const MIGRATED_STATE: &str = "361006908c76928585007cd24aeec231db3d571df4d04a76be5c6b8f0f4313c9";

    #[test]
    fn recovers_the_migrated_config_from_the_legacy_create_redeemer() {
        let cbor = hex::decode(CREATE_TX_HEX.trim()).expect("fixture is hex");
        let tx = MultiEraTx::decode(&cbor).expect("fixture decodes");
        let configured: ScriptHash = hex::decode(NEW_BCL_HASH).expect("hex").as_slice().into();

        let candidates = extract_bcl_config_candidates(&tx, &configured);
        assert!(
            !candidates.is_empty(),
            "no banded candidate recovered from the Create tx"
        );
        let want = hex::decode(MIGRATED_STATE).expect("hex");
        assert!(
            candidates.iter().any(|c| config_hash(c) == want),
            "no candidate hashes to the migrated module_state; got {:?}",
            candidates.iter().map(|c| hex::encode(config_hash(c))).collect::<Vec<_>>()
        );
    }
}

#[cfg(test)]
mod load_tests {
    use super::*;
    use crate::persistence::{PersistenceConfig, connect};

    /// The mainnet pool script and pool NFT policy, so the SPRINKLES/JIMMIES
    /// Create transaction fixture is recognised as a pool creation.
    fn protocol() -> SundaeV4Protocol {
        let hash = ScriptHash::from([0u8; 28]);
        SundaeV4Protocol {
            pool_script_hash: "21c9b7b965646069f9becebf8b4c3cedaf84c19b109eaa5980a14a26"
                .parse()
                .unwrap(),
            order_script_hashes: vec![],
            settings_script_hash: hash,
            settings_nft: AssetClass::from_pair((vec![], vec![])),
            pool_nft_policy: "d9d5985e0657933005d6eb953aeadad29aa8c36f17e493e2962584ec"
                .parse()
                .unwrap(),
            fee_settings_token: None,
            starting_point: Point::Origin,
            execution: None,
            mempool: None,
            blueprint: None,
            network: Default::default(),
        }
    }

    /// What `manager_loop` does on every (re)start: a fresh indexer on the
    /// shared state, loaded from the DB.
    async fn start(
        state: &Arc<Mutex<SundaeV4HistoricalState>>,
        persistence: &dyn crate::persistence::Persistence,
    ) -> Result<SundaeV4Indexer> {
        let (event_tx, _) = broadcast::channel(16);
        let mut indexer = SundaeV4Indexer::new(
            state.clone(),
            event_tx,
            protocol(),
            2160,
            persistence.indexer_dao("sundae_v4"),
        );
        indexer.load().await?;
        Ok(indexer)
    }

    /// A block with no protocol transaction: the node moves forward, the DB
    /// records nothing.
    fn empty_block(slot: u64) -> BlockInfo {
        BlockInfo {
            status: acropolis_common::BlockStatus::Volatile,
            intent: acropolis_common::BlockIntent::none(),
            slot,
            number: slot,
            hash: acropolis_common::BlockHash::new([0; 32]),
            epoch: 0,
            epoch_slot: 0,
            new_epoch: false,
            tip_slot: None,
            timestamp: 0,
            era: acropolis_common::Era::Conway,
            is_new_era: false,
        }
    }

    /// `/resync-from-acropolis` rebuilds the indexer from the DB, then
    /// reindexes from the starting point. A node that has followed the chain
    /// past its last pool must survive it and still know that pool afterwards.
    #[tokio::test]
    async fn resync_reloads_what_the_node_had_indexed() {
        // The block holding the mainnet SPRINKLES/JIMMIES pool creation.
        let pool_creation = hex::decode(
            include_str!("../../test/fixtures/mainnet-pool-create-4b814760.hex").trim(),
        )
        .unwrap();
        let block = empty_block(198130613);
        let persistence = connect(&PersistenceConfig::default()).await.unwrap();
        let state = Arc::new(Mutex::new(SundaeV4HistoricalState::new()));
        let mut node = start(&state, &*persistence).await.unwrap();
        node.handle_block(&block).await.unwrap();
        node.handle_onchain_tx_bytes(&block, &pool_creation).await.unwrap();
        node.handle_block(&empty_block(198130614)).await.unwrap();

        let mut node = start(&state, &*persistence).await.expect("resync must not crash the node");
        node.reset(&Point::Origin).await.unwrap();
        node.handle_block(&block).await.unwrap();
        node.handle_onchain_tx_bytes(&block, &pool_creation).await.unwrap();

        assert_eq!(
            state.lock().await.latest().pools.len(),
            1,
            "the resync lost the pool the node had indexed"
        );
    }
}

#[cfg(test)]
mod steelswap_quote_dump {
    //! Quotes for the Steelswap integration note, from the live preview
    //! ladder of pool 622cee5f… at reserves (5,953,003 tOKENA /
    //! 6,249,988 tOKENC). Ignored by default; run with --ignored.
    use super::*;
    use crate::bigint::BigInt;
    use crate::sundaev4::banded_math::{config_hash, find_witness, swap_steps};

    const CREATE_TX_HEX: &str =
        include_str!("../../test/fixtures/preview-banded-create-c601f694.hex");

    #[test]
    #[ignore]
    fn dump_quotes() {
        let cbor = hex::decode(CREATE_TX_HEX.trim()).unwrap();
        let tx = MultiEraTx::decode(&cbor).unwrap();
        let configured: ScriptHash =
            hex::decode("33485bba2689d4b82b412fb683f07474b9c6c042778d3b9d2b691f18")
                .unwrap()
                .as_slice()
                .into();
        let want = hex::decode("361006908c76928585007cd24aeec231db3d571df4d04a76be5c6b8f0f4313c9")
            .unwrap();
        let cfg = extract_bcl_config_candidates(&tx, &configured)
            .into_iter()
            .find(|c| config_hash(c) == want)
            .unwrap();
        let a = BigInt::from(5953003);
        let b = BigInt::from(6249988);
        println!("witness {:?}", find_witness(&cfg, &a, &b));
        for d in [100i64, 1000, 10000, 100000, 1000000, 5000000] {
            for a_in in [true, false] {
                let steps = swap_steps(&cfg, &a, &b, a_in, &BigInt::from(d));
                match steps {
                    Ok(s) => {
                        let out: BigInt = s.iter().fold(BigInt::from(0), |acc, st| &acc + &st.dy);
                        let spent: BigInt = s.iter().fold(BigInt::from(0), |acc, st| &acc + &st.dx);
                        let bands: Vec<usize> = s.iter().map(|st| st.before.k).collect();
                        println!(
                            "{d:>8} {} in -> out={out} spent={spent} steps={} bands={bands:?}",
                            if a_in { "A" } else { "B" },
                            s.len()
                        );
                    }
                    Err(e) => println!("{d:>8} {} in -> ERR {e}", if a_in { "A" } else { "B" }),
                }
            }
        }
    }
}

#[cfg(test)]
mod boundary_block_replay_tests {
    //! On restart, `load()` restores the state up to `loaded_slot`, and the
    //! driver re-delivers the block AT `loaded_slot` (70a2456: a block that
    //! was only partly applied must get the rest of its transactions). The
    //! transactions of that block that were already applied must not add
    //! their orders a second time.
    use super::*;
    use crate::bigint::BigInt;
    use crate::persistence::{PersistenceConfig, connect};
    use crate::sundaev4::accumulator::Accumulator;
    use crate::sundaev4::router;
    use crate::sundaev4::test_harness::*;

    const SLOT: u64 = 1_000;

    fn protocol(env: &TestEnv) -> SundaeV4Protocol {
        SundaeV4Protocol {
            pool_script_hash: env.exec.module_scripts.pool.hash,
            order_script_hashes: vec![env.exec.module_scripts.order.hash],
            settings_script_hash: env.exec.module_scripts.settings.hash,
            settings_nft: AssetClass::from_pair((vec![], vec![])),
            pool_nft_policy: env.exec.module_scripts.pool_mint.hash,
            fee_settings_token: None,
            starting_point: Point::Origin,
            execution: Some(env.exec.clone()),
            mempool: None,
            blueprint: None,
            network: Default::default(),
        }
    }

    fn block(slot: u64) -> BlockInfo {
        BlockInfo {
            status: acropolis_common::BlockStatus::Volatile,
            intent: acropolis_common::BlockIntent::none(),
            slot,
            number: slot,
            hash: acropolis_common::BlockHash::new([0; 32]),
            epoch: 0,
            epoch_slot: 0,
            new_epoch: false,
            tip_slot: None,
            timestamp: 0,
            era: acropolis_common::Era::Conway,
            is_new_era: false,
        }
    }

    /// A partial-fill scoop: its continuation output is a real order at the
    /// order address.
    fn tx_creating_an_order(env: &TestEnv) -> Vec<u8> {
        let pool = make_pool(env, 0xC1, token_a(), 200_000_000, token_b(), 200_000_000);
        let mut pool_map = BTreeMap::new();
        pool_map.insert(pool.pool_datum.identifier.clone(), pool.clone());
        let order =
            make_order_with_budget(token_a(), 400_000_000, token_b(), 240_000_000, 1, 8_000_000);
        let route = router::find_optimal_route(
            &pool_map,
            &[],
            &token_a(),
            &token_b(),
            &BigInt::from(100_000_000u64),
            router::RoutingLimits::unlimited(),
        )
        .expect("partial route exists");
        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum.try_add_routed_order(&order, &route, &pool_map).expect("partial fill adds");
        let plan = accum.into_plan();
        let settings = make_settings(env, &env.scooper_keyhash());
        let (build, _) = env.build_and_eval_plan(&plan, &settings, 1000).expect("builds");
        build.cbor
    }

    async fn start(
        env: &TestEnv,
        state: &Arc<Mutex<SundaeV4HistoricalState>>,
        persistence: &dyn crate::persistence::Persistence,
    ) -> SundaeV4Indexer {
        let (event_tx, _) = broadcast::channel(16);
        let mut indexer = SundaeV4Indexer::new(
            state.clone(),
            event_tx,
            protocol(env),
            2160,
            persistence.indexer_dao("sundae_v4"),
        );
        indexer.load().await.expect("load");
        indexer
    }

    #[tokio::test]
    async fn replaying_the_loaded_block_does_not_add_its_orders_twice() {
        let env = TestEnv::from_blueprint_file(PREVIEW_FIXTURE);
        let tx = tx_creating_an_order(&env);
        let persistence = connect(&PersistenceConfig::default()).await.unwrap();

        // First run: the block at SLOT is applied, then the service stops.
        let state = Arc::new(Mutex::new(SundaeV4HistoricalState::new()));
        let mut node = start(&env, &state, &*persistence).await;
        node.handle_block(&block(SLOT)).await.unwrap();
        node.handle_onchain_tx_bytes(&block(SLOT), &tx).await.unwrap();
        assert_eq!(
            state.lock().await.latest().orders.len(),
            1,
            "the tx creates one order"
        );

        // Restart: load() restores SLOT, and the driver re-delivers SLOT.
        let state = Arc::new(Mutex::new(SundaeV4HistoricalState::new()));
        let mut node = start(&env, &state, &*persistence).await;
        assert_eq!(
            state.lock().await.latest().orders.len(),
            1,
            "load restores the order"
        );
        node.handle_block(&block(SLOT)).await.unwrap();
        node.handle_onchain_tx_bytes(&block(SLOT), &tx).await.unwrap();

        let history = state.lock().await;
        let orders = &history.latest().orders;
        let mut inputs: Vec<_> = orders.iter().map(|o| o.input.clone()).collect();
        inputs.sort();
        inputs.dedup();
        assert_eq!(
            orders.len(),
            inputs.len(),
            "an order appears {} times after the replay",
            orders.len()
        );
        assert_eq!(orders.len(), 1);
    }
}
