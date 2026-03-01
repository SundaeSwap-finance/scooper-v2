use std::sync::Arc;

use serde::Serialize;

use crate::sundaev3::{Ident, SundaeV3Order, SundaeV3Pool, SundaeV3Settings};
use crate::sundaev4::{SundaeV4Order, SundaeV4Pool, SundaeV4Settings};

#[derive(Debug, Clone)]
pub enum IndexEvent {
    V3PoolCreated {
        id: Ident,
        pool: Arc<SundaeV3Pool>,
    },
    V3PoolUpdated {
        id: Ident,
        pool: Arc<SundaeV3Pool>,
        tx_id: String,
    },
    V3PoolRemoved {
        id: Ident,
        tx_id: String,
    },
    V3OrderCreated {
        order: Arc<SundaeV3Order>,
    },
    V3OrderScooped {
        order: Arc<SundaeV3Order>,
        pool_id: Ident,
        tx_id: String,
        scooper: String,
    },
    V3OrderCancelled {
        order: Arc<SundaeV3Order>,
        tx_id: String,
    },
    V3SettingsUpdated {
        settings: Arc<SundaeV3Settings>,
    },
    V4PoolCreated {
        id: Ident,
        pool: Arc<SundaeV4Pool>,
    },
    V4PoolUpdated {
        id: Ident,
        pool: Arc<SundaeV4Pool>,
        tx_id: String,
    },
    V4PoolRemoved {
        id: Ident,
        tx_id: String,
    },
    V4OrderCreated {
        order: Arc<SundaeV4Order>,
    },
    V4OrderScooped {
        order: Arc<SundaeV4Order>,
        pool_ids: Vec<Ident>,
        tx_id: String,
        scooper: String,
    },
    V4OrderCancelled {
        order: Arc<SundaeV4Order>,
        tx_id: String,
    },
    V4SettingsUpdated {
        settings: Arc<SundaeV4Settings>,
    },
    /// Emitted for every block (including empty ones) with current tip info.
    TipAdvanced {
        slot: u64,
        network_tip_slot: Option<u64>,
    },
    Rollback {
        to_slot: u64,
    },
}

#[derive(Debug, Serialize)]
pub struct SpentOrder<T> {
    pub order: Arc<T>,
    pub reason: SpentOrderReason,
    pub tx_id: String,
    pub slot: u64,
}

impl<T> Clone for SpentOrder<T> {
    fn clone(&self) -> Self {
        Self {
            order: self.order.clone(),
            reason: self.reason.clone(),
            tx_id: self.tx_id.clone(),
            slot: self.slot,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum SpentOrderReason {
    Scooped { pool_ids: Vec<Ident>, scooper: String },
    Cancelled,
    Unknown,
}

#[derive(Debug, Serialize)]
pub struct SpentPool<T> {
    pub id: Ident,
    pub old_pool: Arc<T>,
    pub new_pool: Option<Arc<T>>,
    pub tx_id: String,
    pub slot: u64,
}

impl<T> Clone for SpentPool<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            old_pool: self.old_pool.clone(),
            new_pool: self.new_pool.clone(),
            tx_id: self.tx_id.clone(),
            slot: self.slot,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ScoopStats {
    pub our_keyhash: String,
    pub scooper_totals: Vec<ScooperTotal>,
    pub recent_scoops: Vec<ScoopRecordView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScooperTotal {
    pub scooper: String,
    pub scoop_txs: u64,
    pub orders_processed: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScoopRecordView {
    pub tx_id: String,
    pub slot: u64,
    pub pool_ids: Vec<String>,
    pub n_orders: u32,
    pub scooper: String,
}
