use std::sync::Arc;

use num_traits::{ConstZero, Signed};

use crate::{
    bigint::BigInt,
    cardano_types::Value,
    sundaev3::{Order, PoolDatum, SundaeV3Order, SundaeV3Pool, SundaeV3Settings},
};

pub struct ScoopedPool {
    pub datum: PoolDatum,
    pub value: Value,
    #[expect(unused)]
    settings: Arc<SundaeV3Settings>,
}

impl ScoopedPool {
    pub fn new(pool: &SundaeV3Pool, settings: Arc<SundaeV3Settings>) -> Self {
        Self {
            datum: pool.pool_datum.clone(),
            value: pool.value.clone(),
            settings,
        }
    }

    pub fn apply_order(&mut self, order: &SundaeV3Order) {
        match &order.datum.action {
            Order::Strategy(_) => {}
            Order::Swap(gives, takes) => {
                // TODO: this is not taking fees into account
                self.value.add(&gives.asset_class(), &gives.amount);
                self.value.subtract(&takes.asset_class(), &takes.amount);
            }
            Order::Deposit((a, b)) => {
                // TODO: feeeeees
                let existing_a = self.value.get(&a.asset_class());
                let new_liquidity = if existing_a.is_positive() {
                    &a.amount * &self.datum.circulating_lp / existing_a
                } else {
                    BigInt::ZERO
                };
                self.datum.circulating_lp += new_liquidity;
                self.value.add(&a.asset_class(), &a.amount);
                self.value.add(&b.asset_class(), &b.amount);
            }
            Order::Withdrawal(lp) => {
                // TODO: feeeeeeeeeees
                let (asset_a, asset_b) = &self.datum.assets;
                let old_a = self.value.get(asset_a);
                let old_b = self.value.get(asset_b);
                let withdrawn_a = old_a * &lp.amount / &self.datum.circulating_lp;
                let withdrawn_b = old_b * &lp.amount / &self.datum.circulating_lp;
                self.value.subtract(asset_a, &withdrawn_a);
                self.value.subtract(asset_b, &withdrawn_b);
                self.datum.circulating_lp -= &lp.amount;
            }
            Order::Donation((a, b)) => {
                // TODO: even donations are subject to fees
                self.value.add(&a.asset_class(), &a.amount);
                self.value.add(&b.asset_class(), &b.amount);
            }
            Order::Record(_) => {
                // TODO: these also cost fees
            }
        }
    }
}
