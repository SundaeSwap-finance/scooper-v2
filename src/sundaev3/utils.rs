use num_traits::Signed;

use crate::{
    cardano_types::{ADA_ASSET_CLASS, AssetClass, Value},
    sundaev3::{Order, OrderDatum, PoolDatum},
};

pub fn get_pool_price(datum: &PoolDatum, value: &Value) -> Option<f64> {
    let (coin_a, coin_b) = &datum.assets;
    let mut quantity_a = value.get(coin_a);
    if coin_a == &ADA_ASSET_CLASS {
        quantity_a -= &datum.protocol_fees;
    }
    let quantity_b = value.get(coin_b);
    if quantity_a.is_negative() || !quantity_b.is_positive() {
        return None;
    }
    Some(quantity_a.to_f64()? / quantity_b.to_f64()?)
}

#[derive(Debug, PartialEq, Eq)]
pub enum SwapDirection {
    AtoB,
    BtoA,
}

// Get the marginal pool price for this swap. This figure being in agreement
// with the pool price does not guarantee that the order will succeed; for
// instance, swap fees and finite CPP liquidity will cause the takes to be lower
// than expected.
pub fn swap_price(order: &OrderDatum) -> Option<(SwapDirection, f64)> {
    match &order.action {
        Order::Swap(a, b) => {
            let gives = a.amount.clone();
            let takes = b.amount.clone();
            let coin_a = AssetClass::from_pair((a.policy.clone(), a.token.clone()));
            let coin_b = AssetClass::from_pair((b.policy.clone(), b.token.clone()));
            let mut price = gives.to_f64()? / takes.to_f64()?;
            if takes == 0.into() {
                price = f64::MAX;
            }
            if coin_a < coin_b {
                Some((SwapDirection::AtoB, price))
            } else {
                Some((SwapDirection::BtoA, price))
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::bigint::BigInt;
    use num_traits::ConstZero;

    use crate::{
        multisig::Multisig,
        sundaev3::{Destination, Ident, SingletonValue, empty_cons},
        value,
    };

    use super::*;

    fn rberry_asset_class() -> AssetClass {
        AssetClass {
            policy: vec![
                145, 212, 243, 130, 39, 63, 68, 47, 21, 233, 218, 72, 203, 35, 52, 155, 162, 117,
                248, 129, 142, 76, 122, 197, 209, 0, 74, 22,
            ],
            token: vec![77, 121, 85, 83, 68],
        }
    }

    fn i64_to_bigint(i: i64) -> BigInt {
        BigInt::from(i)
    }

    fn ada_rberry_pool_datum() -> PoolDatum {
        PoolDatum {
            ident: Ident::new(&[0x13, 0x37, 0x90, 0x01]),
            assets: (ADA_ASSET_CLASS, rberry_asset_class()),
            circulating_lp: BigInt::ZERO,
            bid_fees_per_10_thousand: BigInt::ZERO,
            ask_fees_per_10_thousand: BigInt::ZERO,
            fee_manager: None,
            market_open: BigInt::ZERO,
            protocol_fees: BigInt::from(3_000_000),
        }
    }

    #[test]
    fn test_pool_price_1() {
        let pool_datum = ada_rberry_pool_datum();
        let pool_value = value![103_000_000, (&rberry_asset_class(), 100_000_000)];
        let price = get_pool_price(&pool_datum, &pool_value);
        assert_eq!(price, Some(1.0));
    }

    #[test]
    fn test_pool_price_1_10() {
        let pool_datum = ada_rberry_pool_datum();
        let pool_value = value![103_000_000, (&rberry_asset_class(), 1_000_000_000)];
        let price = get_pool_price(&pool_datum, &pool_value);
        assert_eq!(price, Some(0.1));
    }

    #[test]
    fn test_swap_price_a_to_b() {
        let rberry_policy = vec![
            145, 212, 243, 130, 39, 63, 68, 47, 21, 233, 218, 72, 203, 35, 52, 155, 162, 117, 248,
            129, 142, 76, 122, 197, 209, 0, 74, 22,
        ];
        let rberry_token = vec![77, 121, 85, 83, 68];
        let sberry_token = vec![77, 121, 85, 83, 69];
        let od = OrderDatum {
            ident: Some(Ident::new(&[])),
            owner: Multisig::Signature(vec![]),
            scoop_fee: i64_to_bigint(1_280_000),
            destination: Destination::SelfDestination,
            action: Order::Swap(
                SingletonValue {
                    policy: rberry_policy.clone(),
                    token: rberry_token,
                    amount: i64_to_bigint(1_000_000),
                },
                SingletonValue {
                    policy: rberry_policy,
                    token: sberry_token,
                    amount: i64_to_bigint(10_000_000),
                },
            ),
            extra: empty_cons(),
        };
        let swap_price = swap_price(&od);
        assert_eq!(swap_price, Some((SwapDirection::AtoB, 0.1)));
    }

    #[test]
    fn test_swap_price_b_to_a() {
        let rberry_policy = vec![
            145, 212, 243, 130, 39, 63, 68, 47, 21, 233, 218, 72, 203, 35, 52, 155, 162, 117, 248,
            129, 142, 76, 122, 197, 209, 0, 74, 22,
        ];
        let rberry_token = vec![77, 121, 85, 83, 68];
        let sberry_token = vec![77, 121, 85, 83, 69];
        let od = OrderDatum {
            ident: Some(Ident::new(&[])),
            owner: Multisig::Signature(vec![]),
            scoop_fee: i64_to_bigint(1_280_000),
            destination: Destination::SelfDestination,
            action: Order::Swap(
                SingletonValue {
                    policy: rberry_policy.clone(),
                    token: sberry_token,
                    amount: i64_to_bigint(1_000_000),
                },
                SingletonValue {
                    policy: rberry_policy.clone(),
                    token: rberry_token,
                    amount: i64_to_bigint(10_000_000),
                },
            ),
            extra: empty_cons(),
        };
        let swap_price = swap_price(&od);
        assert_eq!(swap_price, Some((SwapDirection::BtoA, 0.1)));
    }
}
