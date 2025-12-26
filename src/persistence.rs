mod sqlite;

use std::{collections::HashMap, sync::Arc};

use acropolis_module_custom_indexer::cursor_store::{CursorEntry, CursorSaveError, CursorStore};
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
    fn sundae_v3_dao(&self) -> Box<dyn SundaeV3Dao>;
    fn cursor_store(&self) -> CursorDao;
}

pub async fn connect(config: &PersistenceConfig) -> Result<Arc<dyn Persistence>> {
    Ok(match config {
        PersistenceConfig::Sqlite(sqlite) => Arc::new(SqlitePersistence::new(sqlite).await?),
    })
}

pub struct SundaeV3TxChanges {
    pub slot: u64,
    pub height: u64,
    pub created_txos: Vec<PersistedTxo>,
    pub spent_txos: Vec<TransactionInput>,
    pub metadata_datums: Vec<PersistedDatum>,
}
impl SundaeV3TxChanges {
    pub fn new(slot: u64, height: u64) -> Self {
        Self {
            slot,
            height,
            created_txos: vec![],
            spent_txos: vec![],
            metadata_datums: vec![],
        }
    }
    pub fn is_empty(&self) -> bool {
        self.created_txos.is_empty()
            && self.spent_txos.is_empty()
            && self.metadata_datums.is_empty()
    }
}

#[async_trait]
pub trait SundaeV3Dao: Send + Sync + 'static {
    async fn apply_tx_changes(&self, changes: SundaeV3TxChanges) -> Result<()>;
    async fn rollback(&self, slot: u64) -> Result<()>;
    async fn load_txos(&self) -> Result<Vec<PersistedTxo>>;
    async fn load_datums(&self) -> Result<Vec<PersistedDatum>>;
    async fn prune_txos(&self, min_height: u64) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedTxo {
    pub txo_id: TransactionInput,
    pub txo_type: String,
    pub created_slot: u64,
    pub era: u16,
    pub txo: Vec<u8>,
    pub datum: Option<Vec<u8>>,
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
    async fn save(&self, entries: &HashMap<String, CursorEntry>) -> Result<(), CursorSaveError>;
}

impl CursorStore for CursorDao {
    async fn load(&self) -> Result<HashMap<String, CursorEntry>> {
        self.0.load().await
    }

    async fn save(&self, entries: &HashMap<String, CursorEntry>) -> Result<(), CursorSaveError> {
        self.0.save(entries).await
    }
}
