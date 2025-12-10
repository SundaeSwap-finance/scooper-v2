use crate::{
    bigint::BigInt,
    cardano_types::{ADA_ASSET_CLASS, AssetClass, Value},
    sundaev3::{Order, OrderDatum},
};

pub fn get_pool_asset_pair(pool_policy: &[u8], v: &Value) -> Option<(AssetClass, AssetClass)> {
    let mut native_token_a = None;
    let mut native_token_b = None;
    let ada_policy: &[u8] = &[];
    for (policy, assets) in &v.0 {
        if policy == pool_policy {
            continue;
        }
        if policy == ada_policy {
            continue;
        }
        for asset in assets.keys() {
            if native_token_a.is_none() {
                native_token_a = Some(AssetClass {
                    policy: policy.clone(),
                    token: asset.clone(),
                });
            } else {
                native_token_b = Some(AssetClass {
                    policy: policy.clone(),
                    token: asset.clone(),
                });
            }
        }
    }
    match (native_token_a, native_token_b) {
        (Some(a), Some(b)) => Some((a, b)),
        (Some(a), None) => Some((ADA_ASSET_CLASS, a)),
        _ => None,
    }
}

pub fn get_pool_price(pool_policy: &[u8], v: &Value, rewards: &BigInt) -> Option<f64> {
    let (coin_a, coin_b) = get_pool_asset_pair(pool_policy, v)?;
    let mut quantity_a = BigInt::from(v.get_asset_class(&coin_a));
    if coin_a == ADA_ASSET_CLASS {
        if &quantity_a < rewards {
            return None;
        }
        quantity_a -= rewards;
    }
    let quantity_b = BigInt::from(v.get_asset_class(&coin_b));
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
    use crate::{
        multisig::Multisig,
        sundaev3::{AnyPlutusData, Destination, Ident, SingletonValue},
        value,
    };

    use super::*;

    fn i64_to_bigint(i: i64) -> BigInt {
        BigInt::from(i)
    }

    #[test]
    fn test_pool_price_1() {
        let rberry_policy = vec![
            145, 212, 243, 130, 39, 63, 68, 47, 21, 233, 218, 72, 203, 35, 52, 155, 162, 117, 248,
            129, 142, 76, 122, 197, 209, 0, 74, 22,
        ];
        let rberry_token = vec![77, 121, 85, 83, 68];
        let pool_policy = vec![0x09];
        let rberry_asset_class = AssetClass::from_pair((rberry_policy, rberry_token));
        let protocol_fees = 3_000_000;
        let pool_value = value![103_000_000, (&rberry_asset_class, 100_000_000)];
        let price = get_pool_price(&pool_policy, &pool_value, &BigInt::from(protocol_fees));
        assert_eq!(price, Some(1.0));
    }

    #[test]
    fn test_pool_price_1_10() {
        let rberry_policy = vec![
            145, 212, 243, 130, 39, 63, 68, 47, 21, 233, 218, 72, 203, 35, 52, 155, 162, 117, 248,
            129, 142, 76, 122, 197, 209, 0, 74, 22,
        ];
        let rberry_token = vec![77, 121, 85, 83, 68];
        let pool_policy = vec![0x09];
        let rberry_asset_class = AssetClass::from_pair((rberry_policy, rberry_token));
        let protocol_fees = 3_000_000;
        let pool_value = value![103_000_000, (&rberry_asset_class, 1_000_000_000)];
        let price = get_pool_price(&pool_policy, &pool_value, &BigInt::from(protocol_fees));
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
            extra: AnyPlutusData::empty_cons(),
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
            extra: AnyPlutusData::empty_cons(),
        };
        let swap_price = swap_price(&od);
        assert_eq!(swap_price, Some((SwapDirection::BtoA, 0.1)));
    }
}
