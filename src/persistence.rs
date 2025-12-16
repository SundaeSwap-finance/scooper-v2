mod sqlite;

use std::sync::Arc;

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
}
impl SundaeV3TxChanges {
    pub fn new(slot: u64, height: u64) -> Self {
        Self {
            slot,
            height,
            created_txos: vec![],
            spent_txos: vec![],
        }
    }
    pub fn is_empty(&self) -> bool {
        self.created_txos.is_empty() && self.spent_txos.is_empty()
    }
}

#[async_trait]
pub trait SundaeV3Dao: Send + Sync + 'static {
    async fn apply_tx_changes(&self, changes: SundaeV3TxChanges) -> Result<()>;
    async fn rollback(&self, slot: u64) -> Result<()>;
    async fn load_txos(&self) -> Result<Vec<PersistedTxo>>;
    async fn prune_txos(&self, min_height: u64) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedTxo {
    pub txo_id: TransactionInput,
    pub txo_type: String,
    pub created_slot: u64,
    pub era: u16,
    pub txo: Vec<u8>,
}
