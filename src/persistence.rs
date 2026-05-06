mod sqlite;

use std::{collections::HashMap, sync::Arc};

use acropolis_module_custom_indexer::cursor_store::{CursorEntry, CursorStore};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;

use crate::{
    cardano_types::TransactionInput,
    persistence::sqlite::{SqliteConfig, SqlitePersistence},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PersistenceConfig {
    Sqlite(SqliteConfig),
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self::Sqlite(SqliteConfig::default())
    }
}

pub trait Persistence: Send + Sync {
    fn indexer_dao(&self, namespace: &str) -> Box<dyn IndexerDao>;
    fn cursor_store(&self) -> CursorDao;
}

pub async fn connect(config: &PersistenceConfig) -> Result<Arc<dyn Persistence>> {
    Ok(match config {
        PersistenceConfig::Sqlite(sqlite) => Arc::new(SqlitePersistence::new(sqlite).await?),
    })
}

pub struct SpentTxo {
    pub input: TransactionInput,
    pub spending_tx_id: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ScoopRecord {
    pub tx_id: Vec<u8>,
    pub slot: u64,
    pub pool_id: Vec<u8>,
    pub n_orders: u32,
    pub scooper: Vec<u8>,
}

pub struct TxChanges {
    pub slot: u64,
    pub height: u64,
    pub created_txos: Vec<PersistedTxo>,
    pub spent_txos: Vec<SpentTxo>,
    pub metadata_datums: Vec<PersistedDatum>,
    pub scoop_records: Vec<ScoopRecord>,
    pub pool_configs: Vec<PersistedPoolConfig>,
}
impl TxChanges {
    pub fn new(slot: u64, height: u64) -> Self {
        Self {
            slot,
            height,
            created_txos: vec![],
            spent_txos: vec![],
            metadata_datums: vec![],
            scoop_records: vec![],
            pool_configs: vec![],
        }
    }
    pub fn is_empty(&self) -> bool {
        self.created_txos.is_empty()
            && self.spent_txos.is_empty()
            && self.metadata_datums.is_empty()
            && self.scoop_records.is_empty()
            && self.pool_configs.is_empty()
    }
}

#[async_trait]
pub trait IndexerDao: Send + Sync + 'static {
    async fn apply_tx_changes(&self, changes: TxChanges) -> Result<()>;
    async fn rollback(&self, slot: u64) -> Result<()>;
    async fn load_txos(&self) -> Result<Vec<PersistedTxo>>;
    async fn load_spent_txos(&self, since_slot: u64) -> Result<Vec<SpentPersistedTxo>>;
    async fn load_datums(&self) -> Result<Vec<PersistedDatum>>;
    async fn prune_txos(&self, min_height: u64) -> Result<()>;
    async fn load_scoop_records(&self) -> Result<Vec<ScoopRecord>>;
    async fn load_pool_configs(&self) -> Result<Vec<PersistedPoolConfig>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedTxo {
    pub txo_id: TransactionInput,
    pub txo_type: String,
    pub created_slot: u64,
    pub era: u16,
    pub txo: Vec<u8>,
    pub address: Vec<u8>,
    pub datum: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpentPersistedTxo {
    pub txo: PersistedTxo,
    pub spent_slot: u64,
    pub spent_tx_id: Option<Vec<u8>>,
}

/// CBOR-encoded pool module config keyed by pool identifier.
///
/// Used to persist CS pool configs (`prices`, `fee`) extracted from on-chain
/// Create redeemers, so the resolved type survives restarts where the original
/// Create tx is no longer in the indexer's stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedPoolConfig {
    pub pool_id: Vec<u8>,
    pub config_cbor: Vec<u8>,
    pub created_slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedDatum {
    pub hash: Vec<u8>,
    pub datum: Vec<u8>,
    pub created_slot: u64,
}

pub struct CursorDao(Box<dyn CursorDaoImpl>);

#[async_trait]
trait CursorDaoImpl: Send + Sync + 'static {
    async fn load(&self) -> Result<HashMap<String, CursorEntry>>;
    async fn save(&self, entries: &HashMap<String, CursorEntry>) -> Result<()>;
}

#[async_trait]
impl CursorStore for CursorDao {
    async fn load(&self) -> Result<HashMap<String, CursorEntry>> {
        self.0.load().await
    }

    async fn save(&self, entries: &HashMap<String, CursorEntry>) -> Result<()> {
        self.0.save(entries).await
    }
}
