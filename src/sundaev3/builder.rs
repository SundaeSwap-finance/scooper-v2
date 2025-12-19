use std::sync::Arc;

use num_traits::{ConstZero, Signed};
use thiserror::Error;

use crate::{
    bigint::BigInt,
    cardano_types::{ADA_ASSET_CLASS, Value},
    sundaev3::{Order, PoolDatum, SundaeV3Order, SundaeV3Pool, SundaeV3Settings},
};

pub struct ScoopBuilder {
    pub pool: PoolDatum,
    pub value: Value,
    settings: Arc<SundaeV3Settings>,
}

impl ScoopBuilder {
    pub fn new(pool: &SundaeV3Pool, settings: Arc<SundaeV3Settings>) -> Self {
        Self {
            pool: pool.pool_datum.clone(),
            value: pool.value.clone(),
            settings,
        }
    }

    pub fn apply_order(&mut self, order: &SundaeV3Order) -> Result<(), ApplyOrderError> {
        match &order.datum.action {
            Order::Strategy(_) => Ok(()),
            Order::Swap(given, taken) => {
                let (pool_gives, pool_takes, charge_per_10k) =
                    if given.asset_class() == self.pool.assets.0 {
                        // Give A, take B
                        let (pool_gives, pool_takes) = self.pool_values();
                        (pool_gives, pool_takes, &self.pool.bid_fees_per_10_thousand)
                    } else if given.asset_class() == self.pool.assets.1 {
                        // Give B, take A
                        let (pool_takes, pool_gives) = self.pool_values();
                        (pool_gives, pool_takes, &self.pool.ask_fees_per_10_thousand)
                    } else {
                        return Err(ApplyOrderError::CoinPairMismatch);
                    };

                let diff = BigInt::from(10_000) - charge_per_10k;
                let takes_num = &pool_takes * &given.amount * &diff;
                let takes_den = (&pool_gives * BigInt::from(10_000)) + (&given.amount * &diff);
                let takes = takes_num / takes_den;

                let order_give_num = &takes * &pool_gives * BigInt::from(10_000);

                let order_give_den = (&pool_takes - &takes) * &diff;
                let order_give_num = order_give_num / order_give_den;

                let order_give_plus_1 = &order_give_num + BigInt::from(1);
                let order_give_0 = order_give_num.clone();
                let order_give_minus_1 = order_give_num - BigInt::from(1);

                let order_give;
                if is_efficient(&pool_takes, &pool_gives, &order_give_plus_1, &diff)
                    && order_give_plus_1 <= given.amount
                {
                    order_give = order_give_plus_1;
                } else if is_efficient(&pool_takes, &pool_gives, &order_give_0, &diff) {
                    order_give = order_give_0;
                } else if is_efficient(&pool_takes, &pool_gives, &order_give_minus_1, &diff) {
                    order_give = order_give_minus_1;
                } else {
                    // couldn't find an efficient orderGive, do not apply
                    return Err(ApplyOrderError::NoEfficientOrderGive);
                }

                self.value.add(&given.asset_class(), &order_give);
                self.value.subtract(&taken.asset_class(), &takes);

                // TODO: base fee is only applied once per scoop
                let fees = &self.settings.datum.base_fee + &self.settings.datum.simple_fee;
                self.value.add(&ADA_ASSET_CLASS, &fees);
                Ok(())
            }
            Order::Deposit((a, b)) => {
                // TODO: feeeeees
                let existing_a = self.value.get(&a.asset_class());
                let new_liquidity = if existing_a.is_positive() {
                    &a.amount * &self.pool.circulating_lp / existing_a
                } else {
                    BigInt::ZERO
                };
                self.pool.circulating_lp += new_liquidity;
                self.value.add(&a.asset_class(), &a.amount);
                self.value.add(&b.asset_class(), &b.amount);
                Ok(())
            }
            Order::Withdrawal(lp) => {
                // TODO: feeeeeeeeeees
                let (asset_a, asset_b) = &self.pool.assets;
                let old_a = self.value.get(asset_a);
                let old_b = self.value.get(asset_b);
                let withdrawn_a = old_a * &lp.amount / &self.pool.circulating_lp;
                let withdrawn_b = old_b * &lp.amount / &self.pool.circulating_lp;
                self.value.subtract(asset_a, &withdrawn_a);
                self.value.subtract(asset_b, &withdrawn_b);
                self.pool.circulating_lp -= &lp.amount;
                Ok(())
            }
            Order::Donation((a, b)) => {
                // TODO: even donations are subject to fees
                self.value.add(&a.asset_class(), &a.amount);
                self.value.add(&b.asset_class(), &b.amount);
                Ok(())
            }
            Order::Record(_) => {
                // TODO: these also cost fees
                Ok(())
            }
        }
    }

    fn pool_values(&self) -> (BigInt, BigInt) {
        let asset_value = |asset| {
            let mut amount = self.value.get(asset);
            if asset == &ADA_ASSET_CLASS {
                amount -= &self.pool.protocol_fees;
            }
            amount
        };
        let (asset_a, asset_b) = &self.pool.assets;
        (asset_value(asset_a), asset_value(asset_b))
    }
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ApplyOrderError {
    #[error("couldn't find an efficient orderGive")]
    NoEfficientOrderGive,
    #[error("order coin pair does not match pool coin pair")]
    CoinPairMismatch,
}

fn is_efficient(
    pool_takes: &BigInt,
    pool_gives: &BigInt,
    order_give: &BigInt,
    diff: &BigInt,
) -> bool {
    // takesLess = (poolTakes * diff * orderGive - poolTakes * diff) / (poolGives * 10_000 + orderGive * diff - diff)
    let takes_less_num = (pool_takes * diff * order_give) - (pool_takes * diff);
    let takes_less_den = (pool_gives * BigInt::from(10_000)) + (order_give * diff) - diff;
    let takes_less = takes_less_num / takes_less_den;

    // takes = (poolTakes * diff * orderGive) / (poolGives * 10_000 + orderGive * diff)
    let takes_num = pool_takes * diff * order_give;
    let takes_den = (pool_gives * BigInt::from(10_000)) + (order_give * diff);
    let takes = takes_num / takes_den;

    takes_less < takes
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use pallas_primitives::Hash;

    use crate::{
        cardano_types::{ADA_ASSET_CLASS, AssetClass, TransactionInput},
        multisig::Multisig,
        sundaev3::{
            Credential, Destination, Ident, OrderDatum, PlutusAddress, SettingsDatum,
            SingletonValue, empty_cons,
        },
        value,
    };

    use super::*;

    fn build_order(action: Order, value: Value) -> SundaeV3Order {
        SundaeV3Order {
            input: TransactionInput::new(Hash::new([0; 32]), 0),
            value,
            datum: OrderDatum {
                ident: None,
                owner: Multisig::After(BigInt::ZERO),
                scoop_fee: BigInt::ZERO,
                destination: Destination::SelfDestination,
                action,
                extra: empty_cons(),
            },
            slot: 0,
        }
    }

    fn build_settings(base_fee: BigInt, simple_fee: BigInt) -> Arc<SundaeV3Settings> {
        Arc::new(SundaeV3Settings {
            input: TransactionInput::new(Hash::new([0; 32]), 0),
            datum: SettingsDatum {
                settings_admin: Multisig::After(BigInt::ZERO),
                metadata_admin: PlutusAddress {
                    payment_credential: Credential::Script(vec![]),
                    stake_credential: None,
                },
                treasury_admin: Multisig::After(BigInt::ZERO),
                treasury_address: PlutusAddress {
                    payment_credential: Credential::Script(vec![]),
                    stake_credential: None,
                },
                treasury_allowance: (BigInt::from(1), BigInt::from(10)),
                authorized_scoopers: None,
                authorized_staking_keys: vec![],
                base_fee,
                simple_fee,
                strategy_fee: BigInt::ZERO,
                pool_creation_fee: BigInt::ZERO,
                extensions: empty_cons(),
            },
            slot: 0,
        })
    }

    #[test]
    fn should_perform_simple_swap() {
        let sberry_asset_class = AssetClass::from_str(
            "99b071ce8580d6a3a11b4902145adb8bfd0d2a03935af8cf66403e15.534245525259",
        )
        .unwrap();
        let pool = SundaeV3Pool {
            input: TransactionInput::new(Hash::new([0; 32]), 0),
            value: value!(33_668_000, (&sberry_asset_class, 66_733_401)),
            pool_datum: PoolDatum {
                ident: Ident::new(&[]),
                assets: (ADA_ASSET_CLASS, sberry_asset_class.clone()),
                circulating_lp: BigInt::from(141_421),
                bid_fees_per_10_thousand: BigInt::from(30),
                ask_fees_per_10_thousand: BigInt::from(50),
                fee_manager: None,
                market_open: BigInt::ZERO,
                protocol_fees: BigInt::from(3_668_000),
            },
            slot: 0,
        };
        let order = build_order(
            Order::Swap(
                SingletonValue::new(ADA_ASSET_CLASS, BigInt::from(10_000_000)),
                SingletonValue::new(sberry_asset_class.clone(), BigInt::from(16_146_411)),
            ),
            value!(13_000_000),
        );

        let settings = build_settings(BigInt::from(332_000), BigInt::from(168_000));

        let mut scooped_pool = ScoopBuilder::new(&pool, settings);
        assert_eq!(scooped_pool.apply_order(&order), Ok(()));

        assert_eq!(
            scooped_pool.value,
            value!(44_168_000, (&sberry_asset_class, 50_087_617))
        );
    }
}
