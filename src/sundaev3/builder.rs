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
    expected_size: usize,
    actual_size: usize,
    pub settings: Arc<SundaeV3Settings>,
}

impl ScoopBuilder {
    pub fn new(pool: &SundaeV3Pool, settings: Arc<SundaeV3Settings>, size: usize) -> Self {
        Self {
            pool: pool.pool_datum.clone(),
            value: pool.value.clone(),
            expected_size: size,
            actual_size: 0,
            settings,
        }
    }

    pub fn apply_order(&mut self, order: &SundaeV3Order) -> Result<(), ApplyOrderError> {
        self.actual_size += 1;
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
                // our balance is too high, because takes is too low

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

                let fee = self.simple_fee();
                self.pool.protocol_fees += &fee;
                self.value.add(&ADA_ASSET_CLASS, &fee);
                Ok(())
            }
            Order::Deposit((a, b)) => {
                let fee = self.simple_fee();

                let quantity_a = &a.amount;
                let quantity_b = &b.amount;
                let (token_a, token_b) = self.pool_values();

                let mut actual_a = order.value.get(&a.asset_class());
                let actual_b = order.value.get(&b.asset_class());
                if a.asset_class() == ADA_ASSET_CLASS {
                    actual_a -= BigInt::from(2_000_000) + &fee;
                }

                let user_gives_a = quantity_a.min(&actual_a);
                if !user_gives_a.is_positive() {
                    return Err(ApplyOrderError::NegativeDeposit(user_gives_a.clone()));
                }
                let user_gives_b = quantity_b.min(&actual_b);

                let b_in_units_of_a = (user_gives_b * &token_a) / &token_b;
                let mut a_change = BigInt::ZERO;
                let mut b_change = BigInt::ZERO;

                if &b_in_units_of_a > user_gives_a {
                    let mut b_gives_minus_change = &token_b * user_gives_a;
                    b_gives_minus_change -= BigInt::from(1);
                    b_gives_minus_change /= &token_a;
                    b_gives_minus_change += BigInt::from(1);
                    b_change = user_gives_b - b_gives_minus_change;
                } else {
                    a_change = user_gives_a - b_in_units_of_a;
                }

                let actual_dep_a = user_gives_a - a_change;
                let actual_dep_b = user_gives_b - b_change;

                let new_liquidity = if quantity_a.is_positive() {
                    &actual_dep_a * &self.pool.circulating_lp / token_a
                } else {
                    BigInt::ZERO
                };

                if !new_liquidity.is_positive() {
                    return Err(ApplyOrderError::NoLiquidity);
                }

                self.pool.circulating_lp += new_liquidity;
                self.value.add(&a.asset_class(), &actual_dep_a);
                self.value.add(&b.asset_class(), &actual_dep_b);

                self.pool.protocol_fees += &fee;
                self.value.add(&ADA_ASSET_CLASS, &fee);
                Ok(())
            }
            Order::Withdrawal(lp) => {
                let (asset_a, asset_b) = &self.pool.assets;
                let (old_a, old_b) = self.pool_values();
                let withdrawn_a = old_a * &lp.amount / &self.pool.circulating_lp;
                let withdrawn_b = old_b * &lp.amount / &self.pool.circulating_lp;
                self.value.subtract(asset_a, &withdrawn_a);
                self.value.subtract(asset_b, &withdrawn_b);
                self.pool.circulating_lp -= &lp.amount;

                let fee = self.simple_fee();
                self.pool.protocol_fees += &fee;
                self.value.add(&ADA_ASSET_CLASS, &fee);
                Ok(())
            }
            Order::Donation((a, b)) => {
                self.value.add(&a.asset_class(), &a.amount);
                self.value.add(&b.asset_class(), &b.amount);

                let fee = self.simple_fee();
                self.pool.protocol_fees += &fee;
                self.value.add(&ADA_ASSET_CLASS, &fee);
                Ok(())
            }
            Order::Record(_) => {
                let fee = self.simple_fee();
                self.pool.protocol_fees += &fee;
                self.value.add(&ADA_ASSET_CLASS, &fee);
                Ok(())
            }
        }
    }

    pub fn validate(&self) -> Result<(), ScoopError> {
        if self.expected_size != self.actual_size {
            return Err(ScoopError::WrongOrderCount {
                expected: self.expected_size,
                actual: self.actual_size,
            });
        }
        Ok(())
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

    fn simple_fee(&self) -> BigInt {
        // The base fee is divided across every order in the scoop
        let size = BigInt::from(self.expected_size as i128);
        let amortized_base_fee = (&self.settings.datum.base_fee + &size - BigInt::from(1)) / &size;
        amortized_base_fee + &self.settings.datum.simple_fee
    }
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ApplyOrderError {
    #[error("couldn't find an efficient orderGive")]
    NoEfficientOrderGive,
    #[error("order coin pair does not match pool coin pair")]
    CoinPairMismatch,
    #[error("deposit would give {0} coin A")]
    NegativeDeposit(BigInt),
    #[error("would return 0 liquidity")]
    NoLiquidity,
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ScoopError {
    #[error("scoop has wrong order count (expected {expected}, saw {actual})")]
    WrongOrderCount { expected: usize, actual: usize },
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
        let settings = build_settings(BigInt::from(332_000), BigInt::from(168_000));

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

        let mut scooped_pool = ScoopBuilder::new(&pool, settings, 1);
        assert_eq!(scooped_pool.apply_order(&order), Ok(()));
        assert_eq!(scooped_pool.validate(), Ok(()));

        assert_eq!(
            scooped_pool.value,
            value!(44_168_000, (&sberry_asset_class, 50_087_617))
        );
    }

    #[test]
    fn should_scoop_two_orders() {
        let settings = build_settings(BigInt::from(332_000), BigInt::from(168_000));

        let sberry_asset_class = AssetClass::from_str(
            "99b071ce8580d6a3a11b4902145adb8bfd0d2a03935af8cf66403e15.534245525259",
        )
        .unwrap();
        let pool = SundaeV3Pool {
            input: TransactionInput::new(Hash::new([0; 32]), 0),
            value: value!(23_000_000, (&sberry_asset_class, 1_000)),
            pool_datum: PoolDatum {
                ident: Ident::new(&[]),
                assets: (ADA_ASSET_CLASS, sberry_asset_class.clone()),
                circulating_lp: BigInt::from(141_421),
                bid_fees_per_10_thousand: BigInt::from(30),
                ask_fees_per_10_thousand: BigInt::from(50),
                fee_manager: None,
                market_open: BigInt::ZERO,
                protocol_fees: BigInt::from(3_000_000),
            },
            slot: 0,
        };
        let donation = build_order(
            Order::Donation((
                SingletonValue::new(ADA_ASSET_CLASS, BigInt::ZERO),
                SingletonValue::new(sberry_asset_class.clone(), BigInt::from(99_999_000)),
            )),
            value!(3_100_000, (&sberry_asset_class, 99_999_000)),
        );
        let swap = build_order(
            Order::Swap(
                SingletonValue::new(ADA_ASSET_CLASS, BigInt::from(10_000_000)),
                SingletonValue::new(sberry_asset_class.clone(), BigInt::from(323)),
            ),
            value!(13_000_000),
        );

        let mut scooped_pool = ScoopBuilder::new(&pool, settings, 2);
        assert_eq!(scooped_pool.apply_order(&donation), Ok(()));
        assert_eq!(scooped_pool.apply_order(&swap), Ok(()));
        assert_eq!(scooped_pool.validate(), Ok(()));
        assert_eq!(
            scooped_pool.value,
            value!(33_668_000, (&sberry_asset_class, 66_733_401)),
        );
    }

    #[test]
    fn should_scoop_withdrawal() {
        let settings = build_settings(BigInt::from(332_000), BigInt::from(168_000));

        let test_asset_class = AssetClass::from_str(
            "99b071ce8580d6a3a11b4902145adb8bfd0d2a03935af8cf66403e15.54657374417373657433",
        )
        .unwrap();

        let lp_asset_class = AssetClass::from_str(
            "44a1eb2d9f58add4eb1932bd0048e6a1947e85e3fe4f32956a110414.0014df1070a5be631ece9fbb484c806a201aec847a362fa1e5d2783cd0df32b9",
        ).unwrap();

        let pool = SundaeV3Pool {
            input: TransactionInput::new(Hash::new([0; 32]), 0),
            value: value!(71_996_522, (&test_asset_class, 14_517)),
            pool_datum: PoolDatum {
                ident: Ident::new(&[]),
                assets: (ADA_ASSET_CLASS, test_asset_class.clone()),
                circulating_lp: BigInt::from(1_000_000),
                bid_fees_per_10_thousand: BigInt::from(100),
                ask_fees_per_10_thousand: BigInt::from(100),
                fee_manager: None,
                market_open: BigInt::from(49_762_568),
                protocol_fees: BigInt::from(2_004_001),
            },
            slot: 0,
        };
        let swap = build_order(
            Order::Swap(
                SingletonValue::new(test_asset_class.clone(), BigInt::from(10)),
                SingletonValue::new(ADA_ASSET_CLASS, BigInt::from(157)),
            ),
            value!(3_000_000, (&test_asset_class, 10)),
        );
        let withdrawal = build_order(
            Order::Withdrawal(SingletonValue::new(
                lp_asset_class.clone(),
                BigInt::from(1_000_000),
            )),
            value!(3_000_000, (&lp_asset_class, 1_000_000)),
        );

        let mut scooped_pool = ScoopBuilder::new(&pool, settings, 2);
        assert_eq!(scooped_pool.apply_order(&swap), Ok(()));
        assert_eq!(scooped_pool.apply_order(&withdrawal), Ok(()));
        assert_eq!(scooped_pool.validate(), Ok(()));
        assert_eq!(scooped_pool.value, value!(2_672_001),);
    }

    #[test]
    fn should_scoop_deposit() {
        let settings = build_settings(BigInt::from(332_000), BigInt::from(168_000));

        let rberry_asset_class = AssetClass::from_str(
            "99b071ce8580d6a3a11b4902145adb8bfd0d2a03935af8cf66403e15.524245525259",
        )
        .unwrap();
        let sberry_asset_class = AssetClass::from_str(
            "99b071ce8580d6a3a11b4902145adb8bfd0d2a03935af8cf66403e15.534245525259",
        )
        .unwrap();

        let pool = SundaeV3Pool {
            input: TransactionInput::new(Hash::new([0; 32]), 0),
            value: value!(
                96344040,
                (&rberry_asset_class, 152_640_608),
                (&sberry_asset_class, 66_301_789)
            ),
            pool_datum: PoolDatum {
                ident: Ident::new(&[]),
                assets: (rberry_asset_class.clone(), sberry_asset_class.clone()),
                circulating_lp: BigInt::from(97_000_000),
                bid_fees_per_10_thousand: BigInt::from(50),
                ask_fees_per_10_thousand: BigInt::from(30),
                fee_manager: None,
                market_open: BigInt::from(0),
                protocol_fees: BigInt::from(96_344_040),
            },
            slot: 0,
        };
        let deposit = build_order(
            Order::Deposit((
                SingletonValue::new(rberry_asset_class.clone(), BigInt::from(1_000_000)),
                SingletonValue::new(sberry_asset_class.clone(), BigInt::from(1_000_000)),
            )),
            value!(
                3_100_000,
                (&rberry_asset_class, 1_000_000),
                (&sberry_asset_class, 1_000_000)
            ),
        );

        let mut scooped_pool = ScoopBuilder::new(&pool, settings, 1);
        assert_eq!(scooped_pool.apply_order(&deposit), Ok(()));
        assert_eq!(scooped_pool.validate(), Ok(()));
        assert_eq!(
            scooped_pool.value,
            value!(
                96_844_040,
                (&rberry_asset_class, 153_640_608),
                (&sberry_asset_class, 66_736_155)
            ),
        );
    }

    #[test]
    fn should_scoop_other_deposit() {
        let settings = build_settings(BigInt::from(332_000), BigInt::from(168_000));

        let tindy_asset_class = AssetClass::from_str(
            "fa3eff2047fdf9293c5feef4dc85ce58097ea1c6da4845a351535183.74494e4459",
        )
        .unwrap();

        let pool = SundaeV3Pool {
            input: TransactionInput::new(Hash::new([0; 32]), 0),
            value: value!(20_000_000, (&tindy_asset_class, 20_000_000)),
            pool_datum: PoolDatum {
                ident: Ident::new(&[]),
                assets: (ADA_ASSET_CLASS, tindy_asset_class.clone()),
                circulating_lp: BigInt::from(20_000_000),
                bid_fees_per_10_thousand: BigInt::from(5),
                ask_fees_per_10_thousand: BigInt::from(5),
                fee_manager: None,
                market_open: BigInt::from(0),
                protocol_fees: BigInt::from(0),
            },
            slot: 0,
        };
        let deposit = build_order(
            Order::Deposit((
                SingletonValue::new(ADA_ASSET_CLASS, BigInt::from(99_999_998)),
                SingletonValue::new(tindy_asset_class.clone(), BigInt::from(20_510_929)),
            )),
            value!(102_499_998, (&tindy_asset_class, 20_510_929)),
        );

        let mut scooped_pool = ScoopBuilder::new(&pool, settings, 1);
        assert_eq!(scooped_pool.apply_order(&deposit), Ok(()));
        assert_eq!(scooped_pool.validate(), Ok(()));
        assert_eq!(
            scooped_pool.value,
            value!(41_010_929, (&tindy_asset_class, 40_510_929)),
        );
    }
}
