use std::sync::Arc;

use crate::sundaev3::{Ident, SundaeV3Order, SundaeV3Pool, SundaeV3Settings};

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
    Rollback {
        to_slot: u64,
    },
}
