use std::sync::Arc;

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
    },
    V3PoolRemoved {
        id: Ident,
    },
    V3OrderCreated {
        order: Arc<SundaeV3Order>,
    },
    V3OrderScooped {
        order: Arc<SundaeV3Order>,
        pool_id: Ident,
    },
    V3OrderCancelled {
        order: Arc<SundaeV3Order>,
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
    },
    V4PoolRemoved {
        id: Ident,
    },
    V4OrderCreated {
        order: Arc<SundaeV4Order>,
    },
    V4OrderScooped {
        order: Arc<SundaeV4Order>,
        pool_id: Ident,
    },
    V4OrderCancelled {
        order: Arc<SundaeV4Order>,
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
