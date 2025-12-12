use std::{collections::BTreeMap, sync::Arc, time::Duration};

use tokio::sync::watch;

use crate::{
    cardano_types::TransactionInput,
    sundaev3::{
        Ident, PoolError, SundaeV3Order, SundaeV3Pool, SundaeV3State, SundaeV3Update, ValueError,
        estimate_whether_in_range, validate_order_for_pool, validate_order_value,
    },
};

pub struct Scooper {
    sundaev3: watch::Receiver<SundaeV3Update>,
    policy: Vec<u8>,
}

impl Scooper {
    pub fn new(sundaev3: watch::Receiver<SundaeV3Update>, policy: &[u8]) -> Self {
        Self {
            sundaev3,
            policy: policy.to_vec(),
        }
    }

    pub async fn run(mut self) {
        while self.sundaev3.changed().await.is_ok() {
            // Sleep a bit to deduplicate updates to the state.
            tokio::time::sleep(Duration::from_millis(250)).await;

            let update = self.sundaev3.borrow_and_update().clone();
            // TODO: only "scoop" when we're at the head of the chain
            self.log_orders(&update.state);
        }
    }

    fn log_orders(&self, state: &SundaeV3State) {
        let mut orders = vec![];
        for order in &state.orders {
            orders.push(OrderState {
                order: order.input.clone(),
                validity: self.validate_order(order, &state.pools),
            });
        }

        // TODO: log the orders
    }

    fn validate_order(
        &self,
        order: &SundaeV3Order,
        pools: &BTreeMap<Ident, Arc<SundaeV3Pool>>,
    ) -> OrderValidity {
        if let Err(err) = validate_order_value(&order.datum, &order.output.value) {
            return OrderValidity::ValueError(err);
        }
        let mut valid_pools = vec![];
        let mut errors = BTreeMap::new();
        for (ident, pool) in pools {
            if let Err(error) = validate_order_for_pool(&order.datum, &pool.pool_datum) {
                if matches!(error, PoolError::IdentMismatch) {
                    continue;
                }
                errors.insert(ident.clone(), error);
            } else if let Err(error) =
                estimate_whether_in_range(&self.policy, &order.datum, &pool.pool_datum, &pool.value)
            {
                errors.insert(ident.clone(), error);
            } else {
                valid_pools.push(ident.clone());
            }
        }
        if !valid_pools.is_empty() {
            OrderValidity::Valid(valid_pools)
        } else if !errors.is_empty() {
            OrderValidity::PoolErrors(errors)
        } else {
            OrderValidity::NoPools
        }
    }
}

#[expect(unused)]
struct OrderState {
    order: TransactionInput,
    validity: OrderValidity,
}
#[expect(unused)]
enum OrderValidity {
    Valid(Vec<Ident>),
    NoPools,
    ValueError(ValueError),
    PoolErrors(BTreeMap<Ident, PoolError>),
}
