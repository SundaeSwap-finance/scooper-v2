use std::fmt;

use num_traits::{ConstZero, Signed};
use serde::Serialize;

use crate::{
    bigint::BigInt,
    cardano_types::{ADA_ASSET_CLASS, AssetClass, Value},
    sundaev3::{Order, OrderDatum, PoolDatum, SwapDirection, get_pool_price, swap_price},
};

const ADA_RIDER: i128 = 2000000;

pub enum ValidationError {
    ValueError(ValueError),
    PoolError(PoolError),
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::PoolError(e) => match e {
                PoolError::IdentMismatch => write!(f, "order ident does not match pool ident"),
                PoolError::CoinPairMismatch => {
                    write!(f, "order coin pair does not match pool coin pair")
                }
                PoolError::Empty => write!(f, "pool is empty"),
                PoolError::OutOfRange {
                    swap_price,
                    pool_price,
                } => {
                    write!(
                        f,
                        "order out of range (swap price {swap_price}, pool price {pool_price})"
                    )
                }
            },
            ValidationError::ValueError(e) => match e {
                ValueError::GivesZeroTokens => write!(f, "gives zero tokens"),
                ValueError::HasInsufficientTokens {
                    asset,
                    expected,
                    actual,
                } => {
                    write!(f, "has insufficient {asset} ({actual} < {expected})")
                }
            },
        }
    }
}

pub fn validate_order(
    order: &OrderDatum,
    value: &Value,
    pool: &PoolDatum,
    pool_value: &Value,
) -> Result<(), ValidationError> {
    validate_order_value(order, value).map_err(ValidationError::ValueError)?;
    validate_order_for_pool(order, pool).map_err(ValidationError::PoolError)?;
    estimate_whether_in_range(order, pool, pool_value).map_err(ValidationError::PoolError)?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub enum ValueError {
    GivesZeroTokens,
    HasInsufficientTokens {
        asset: AssetClass,
        expected: BigInt,
        actual: BigInt,
    },
}

pub fn validate_order_value(datum: &OrderDatum, value: &Value) -> Result<(), ValueError> {
    let scoop_fee = datum.scoop_fee.clone();
    match &datum.action {
        Order::Strategy(_) => Ok(()),
        Order::Swap(gives, _takes) => {
            let minimum_ada = BigInt::from(ADA_RIDER) + scoop_fee.clone();
            let gives_asset = gives.asset_class();
            let gives_ada = if gives_asset == ADA_ASSET_CLASS {
                gives.amount.clone()
            } else {
                BigInt::ZERO
            };

            if !gives.amount.is_positive() {
                return Err(ValueError::GivesZeroTokens);
            }

            let actual_ada = BigInt::from(value.get_asset_class(&ADA_ASSET_CLASS));
            let expected_ada = gives_ada + minimum_ada.clone();
            if actual_ada < expected_ada {
                return Err(ValueError::HasInsufficientTokens {
                    asset: ADA_ASSET_CLASS,
                    expected: expected_ada,
                    actual: actual_ada,
                });
            }

            let actual_amount_of_give_token = BigInt::from(value.get_asset_class(&gives_asset))
                - if gives_asset == ADA_ASSET_CLASS {
                    minimum_ada
                } else {
                    BigInt::ZERO
                };
            if actual_amount_of_give_token.is_negative() {
                return Err(ValueError::GivesZeroTokens);
            }

            if actual_amount_of_give_token < gives.amount {
                // This is an error in sundaedatum, even though the smart contract appears to allow it
                return Err(ValueError::HasInsufficientTokens {
                    asset: gives_asset,
                    expected: gives.amount.clone(),
                    actual: actual_amount_of_give_token,
                });
            }
            Ok(())
        }
        Order::Deposit((a, b)) => {
            let asset_a = a.asset_class();
            let asset_b = b.asset_class();
            let mut actual_a = BigInt::from(value.get_asset_class(&asset_a));
            if asset_a == ADA_ASSET_CLASS {
                let minimum = BigInt::from(ADA_RIDER) + scoop_fee.clone();
                if actual_a < minimum {
                    return Err(ValueError::HasInsufficientTokens {
                        asset: ADA_ASSET_CLASS,
                        expected: minimum,
                        actual: actual_a,
                    });
                }
                actual_a -= minimum;
            }
            let actual_b = BigInt::from(value.get_asset_class(&asset_b));

            if !actual_a.is_positive() || !actual_b.is_positive() {
                return Err(ValueError::GivesZeroTokens);
            }

            if actual_a < a.amount {
                return Err(ValueError::HasInsufficientTokens {
                    asset: asset_a,
                    expected: a.amount.clone(),
                    actual: actual_a,
                });
            }

            if actual_b < b.amount {
                return Err(ValueError::HasInsufficientTokens {
                    asset: asset_b,
                    expected: b.amount.clone(),
                    actual: actual_b,
                });
            }

            Ok(())
        }
        Order::Withdrawal(singleton) => {
            if !singleton.amount.is_positive() {
                return Err(ValueError::GivesZeroTokens);
            }
            let actual = BigInt::from(value.get_asset_class(&singleton.asset_class()));
            if actual < singleton.amount {
                return Err(ValueError::HasInsufficientTokens {
                    asset: singleton.asset_class(),
                    expected: singleton.amount.clone(),
                    actual,
                });
            }
            let expected = BigInt::from(ADA_RIDER) + scoop_fee;
            let actual = BigInt::from(value.get_asset_class(&ADA_ASSET_CLASS));
            if actual < expected {
                return Err(ValueError::HasInsufficientTokens {
                    asset: ADA_ASSET_CLASS,
                    expected,
                    actual,
                });
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[derive(Debug, PartialEq, Serialize)]
pub enum PoolError {
    IdentMismatch,
    CoinPairMismatch,
    Empty,
    OutOfRange { swap_price: f64, pool_price: f64 },
}

pub fn validate_order_for_pool(order: &OrderDatum, pool: &PoolDatum) -> Result<(), PoolError> {
    if let Some(i) = &order.ident
        && i != &pool.ident
    {
        return Err(PoolError::IdentMismatch);
    }
    match &order.action {
        Order::Swap(gives, takes) => {
            let give_coin = gives.asset_class();
            let take_coin = takes.asset_class();
            let matches_a_to_b = pool.assets.0 == give_coin && pool.assets.1 == take_coin;
            let matches_b_to_a = pool.assets.0 == take_coin && pool.assets.1 == give_coin;
            if !(matches_a_to_b || matches_b_to_a) {
                return Err(PoolError::CoinPairMismatch);
            }
            Ok(())
        }
        Order::Deposit((a, b)) => {
            let a_coin = a.asset_class();
            let b_coin = b.asset_class();
            let matches_a_to_b = pool.assets.0 == a_coin && pool.assets.1 == b_coin;
            let matches_b_to_a = pool.assets.0 == b_coin && pool.assets.1 == a_coin;
            if !(matches_a_to_b || matches_b_to_a) {
                return Err(PoolError::CoinPairMismatch);
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub fn estimate_whether_in_range(
    od: &OrderDatum,
    pd: &PoolDatum,
    pool_value: &Value,
) -> Result<(), PoolError> {
    let Some(pool_price) = get_pool_price(pd, pool_value) else {
        return Err(PoolError::Empty);
    };
    let Some(swap_price) = swap_price(od) else {
        return Ok(());
    };
    match swap_price {
        (SwapDirection::AtoB, swap_price) => {
            if pool_price <= swap_price {
                Ok(())
            } else {
                Err(PoolError::OutOfRange {
                    swap_price,
                    pool_price,
                })
            }
        }
        (SwapDirection::BtoA, swap_price) => {
            if pool_price >= (1.0 / swap_price) {
                Ok(())
            } else {
                Err(PoolError::OutOfRange {
                    swap_price: 1.0 / swap_price,
                    pool_price,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        cardano_types::{ADA_POLICY, ADA_TOKEN},
        multisig::Multisig,
        sundaev3::{Destination, SingletonValue, empty_cons},
        value,
    };

    use super::*;

    fn i64_to_bigint(i: i64) -> BigInt {
        BigInt::from(i)
    }

    struct ValidateAdaRBerrySwapTestCase {
        scoop_fee: i64,
        ada_offered: i64,
        rberry_offered: i64,
        actual_ada: i128,
        actual_rberry: i128,
    }

    fn test_validate_ada_rberry_swap_schema(
        test_case: ValidateAdaRBerrySwapTestCase,
    ) -> Result<(), ValueError> {
        let pkh = hex::decode("00").unwrap();
        let rberry_policy = vec![
            145, 212, 243, 130, 39, 63, 68, 47, 21, 233, 218, 72, 203, 35, 52, 155, 162, 117, 248,
            129, 142, 76, 122, 197, 209, 0, 74, 22,
        ];
        let rberry_token = vec![77, 121, 85, 83, 68];
        let order = OrderDatum {
            ident: None,
            owner: Multisig::Signature(pkh),
            scoop_fee: i64_to_bigint(test_case.scoop_fee),
            destination: Destination::SelfDestination,
            action: Order::Swap(
                SingletonValue {
                    policy: ADA_POLICY,
                    token: ADA_TOKEN,
                    amount: i64_to_bigint(test_case.ada_offered),
                },
                SingletonValue {
                    policy: rberry_policy.clone(),
                    token: rberry_token.clone(),
                    amount: i64_to_bigint(test_case.rberry_offered),
                },
            ),
            extra: empty_cons(),
        };
        let rberry_asset_class = AssetClass::from_pair((rberry_policy, rberry_token));
        let value = value![
            test_case.actual_ada,
            (&rberry_asset_class, test_case.actual_rberry)
        ];
        validate_order_value(&order, &value)
    }

    struct ValidateRBerrySBerrySwapTestCase {
        scoop_fee: i64,
        rberry_offered: i64,
        sberry_offered: i64,
        actual_ada: i128,
        actual_rberry: i128,
        actual_sberry: i128,
    }

    fn test_validate_rberry_sberry_swap_schema(
        test_case: ValidateRBerrySBerrySwapTestCase,
    ) -> Result<(), ValueError> {
        let pkh = hex::decode("00").unwrap();
        let rberry_policy = vec![
            145, 212, 243, 130, 39, 63, 68, 47, 21, 233, 218, 72, 203, 35, 52, 155, 162, 117, 248,
            129, 142, 76, 122, 197, 209, 0, 74, 22,
        ];
        let sberry_policy = rberry_policy.clone();
        let rberry_token = vec![77, 121, 85, 83, 68];
        let sberry_token = vec![77, 121, 85, 83, 69];
        let order = OrderDatum {
            ident: None,
            owner: Multisig::Signature(pkh),
            scoop_fee: i64_to_bigint(test_case.scoop_fee),
            destination: Destination::SelfDestination,
            action: Order::Swap(
                SingletonValue {
                    policy: rberry_policy.clone(),
                    token: rberry_token.clone(),
                    amount: i64_to_bigint(test_case.rberry_offered),
                },
                SingletonValue {
                    policy: sberry_policy.clone(),
                    token: sberry_token.clone(),
                    amount: i64_to_bigint(test_case.sberry_offered),
                },
            ),
            extra: empty_cons(),
        };
        let rberry_asset_class = AssetClass::from_pair((rberry_policy, rberry_token));
        let sberry_asset_class = AssetClass::from_pair((sberry_policy, sberry_token));
        let value = value![
            test_case.actual_ada,
            (&rberry_asset_class, test_case.actual_rberry),
            (&sberry_asset_class, test_case.actual_sberry)
        ];
        validate_order_value(&order, &value)
    }

    #[test]
    fn test_validate_ada_rberry_swap() {
        assert_eq!(
            test_validate_ada_rberry_swap_schema(ValidateAdaRBerrySwapTestCase {
                scoop_fee: 1_000_000,
                ada_offered: 1_000_000,
                rberry_offered: 1_000_000,
                actual_ada: 10_000_000,
                actual_rberry: 1_000_000,
            }),
            Ok(())
        )
    }

    // 3 ADA on the utxo is not sufficient because after deducting the 1 ADA
    // scoop fee and the 1 ADA offered the remaining amount is 1 ADA, less than
    // the 2 ADA rider value
    #[test]
    fn test_validate_ada_rberry_swap_insufficient_ada() {
        assert_eq!(
            test_validate_ada_rberry_swap_schema(ValidateAdaRBerrySwapTestCase {
                scoop_fee: 1_000_000,
                ada_offered: 1_000_000,
                rberry_offered: 1_000_000,
                actual_ada: 3_000_000,
                actual_rberry: 1_000_000,
            }),
            Err(ValueError::HasInsufficientTokens {
                asset: ADA_ASSET_CLASS,
                expected: BigInt::from(4_000_000),
                actual: BigInt::from(3_000_000)
            })
        )
    }

    #[test]
    fn test_validate_rberry_sberry_swap() {
        assert_eq!(
            test_validate_rberry_sberry_swap_schema(ValidateRBerrySBerrySwapTestCase {
                scoop_fee: 1_000_000,
                sberry_offered: 1_000_000,
                rberry_offered: 1_000_000,
                actual_ada: 3_000_000,
                actual_sberry: 10_000_000,
                actual_rberry: 1_000_000,
            }),
            Ok(())
        );
    }

    #[test]
    fn test_validate_rberry_sberry_gives_zero_tokens() {
        assert_eq!(
            test_validate_rberry_sberry_swap_schema(ValidateRBerrySBerrySwapTestCase {
                scoop_fee: 1_000_000,
                sberry_offered: 1_000_000,
                rberry_offered: -1_000_000,
                actual_ada: 3_000_000,
                actual_sberry: 10_000_000,
                actual_rberry: 1_000_000,
            }),
            Err(ValueError::GivesZeroTokens)
        );
    }
}
