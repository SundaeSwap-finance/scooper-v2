#![allow(unused)]

use pallas_primitives::PlutusData;
use plutus_parser::AsPlutus;
use std::fmt;
use std::rc::Rc;

use crate::bigint::BigInt;
use crate::cardano_types::{ADA_ASSET_CLASS, ADA_POLICY, ADA_TOKEN, AssetClass, Value};
use crate::multisig::Multisig;
use crate::value;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Ident(Vec<u8>);

impl Ident {
    pub fn new(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }

    pub fn to_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

impl std::ops::Deref for Ident {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsPlutus for Ident {
    fn from_plutus(data: PlutusData) -> Result<Self, plutus_parser::DecodeError> {
        let bytes: Vec<u8> = AsPlutus::from_plutus(data)?;
        Ok(Ident(bytes))
    }

    fn to_plutus(self) -> PlutusData {
        self.0.to_plutus()
    }
}

#[derive(AsPlutus, Clone, PartialEq, Eq)]
pub struct PoolDatum {
    pub ident: Ident,
    pub assets: (AssetClass, AssetClass),
    pub circulating_lp: BigInt,
    pub bid_fees_per_10_thousand: BigInt,
    pub ask_fees_per_10_thousand: BigInt,
    pub fee_manager: Option<Multisig>,
    pub market_open: BigInt,
    pub protocol_fees: BigInt,
}

enum PlutusOption<T> {
    PlutusNone,
    PlutusSome(T),
}

fn plutus_option_to_option<T>(p: PlutusOption<T>) -> Option<T> {
    match p {
        PlutusOption::PlutusNone => None,
        PlutusOption::PlutusSome(x) => Some(x),
    }
}

#[derive(AsPlutus, Debug, PartialEq)]
pub enum PoolRedeemer {
    PoolScoop(PoolScoop),
    Manage,
}

#[derive(AsPlutus, Debug, PartialEq)]
pub struct SSEBytes(Vec<u8>);

// When constructing a pool scoop redeemer we don't construct SSEs because they will be
// retrieved from a database. So it's better to represent them here as raw bytes.
#[derive(AsPlutus, Debug, PartialEq)]
pub struct PoolScoop {
    signatory_index: BigInt,
    scooper_index: BigInt,
    input_order: Vec<(BigInt, Option<SSEBytes>, BigInt)>,
}

#[derive(AsPlutus, Debug, PartialEq)]
pub struct SignedStrategyExecution {
    execution: StrategyExecution,
    signature: Option<Vec<u8>>,
}

#[derive(AsPlutus, Debug, PartialEq, Eq)]
pub enum StrategyAuthorization {
    Signature(Vec<u8>),
    Script(Vec<u8>),
}

pub type SingletonValue = (Vec<u8>, Vec<u8>, BigInt);

#[derive(AsPlutus, Debug, PartialEq, Eq)]
pub enum Order {
    Strategy(StrategyAuthorization),
    Swap(SingletonValue, SingletonValue),
    Deposit((SingletonValue, SingletonValue)),
    Withdrawal(SingletonValue),
    Donation((SingletonValue, SingletonValue)),
    Record(AssetClass),
}

#[derive(AsPlutus, Debug, PartialEq, Eq)]
pub struct OrderDatum {
    pub ident: Option<Ident>,
    pub owner: Multisig,
    pub scoop_fee: BigInt,
    pub destination: Destination,
    pub action: Order,
    pub extra: AnyPlutusData,
}

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
            },
            ValidationError::ValueError(e) => match e {
                ValueError::GivesZeroTokens => write!(f, "gives zero tokens"),
                ValueError::HasInsufficientAda { expected, actual } => {
                    write!(f, "has insufficient ada ({actual} < {expected})")
                }
                ValueError::DeclaredExceedsActual { declared, actual } => {
                    write!(
                        f,
                        "offers value in excess of available funds ({actual} < {declared})"
                    )
                }
            },
        }
    }
}

pub fn validate_order(
    datum: &OrderDatum,
    value: &Value,
    pool: &PoolDatum,
) -> Result<(), ValidationError> {
    validate_order_value(datum, value).map_err(ValidationError::ValueError)?;
    validate_order_for_pool(datum, pool).map_err(ValidationError::PoolError)?;
    Ok(())
}

pub enum ValueError {
    GivesZeroTokens,
    HasInsufficientAda { expected: BigInt, actual: BigInt },
    DeclaredExceedsActual { declared: BigInt, actual: BigInt },
}

pub fn validate_order_value(datum: &OrderDatum, value: &Value) -> Result<(), ValueError> {
    let scoop_fee = datum.scoop_fee.clone();
    match &datum.action {
        Order::Strategy(_) => Ok(()),
        Order::Swap(a, b) => {
            let minimum_ada = BigInt::from(ADA_RIDER) + scoop_fee.clone();
            let gives = a.2.clone();
            let gives_asset = AssetClass::from_pair((a.0.clone(), a.1.clone()));
            let gives_ada = if gives_asset == ADA_ASSET_CLASS {
                gives.clone()
            } else {
                BigInt::from(0)
            };
            let actual_ada = BigInt::from(value.get_asset_class(&ADA_ASSET_CLASS));
            let expected_ada = gives_ada + minimum_ada.clone();
            if actual_ada < expected_ada {
                return Err(ValueError::HasInsufficientAda {
                    expected: expected_ada,
                    actual: actual_ada,
                });
            }

            let actual_amount_of_give_token = BigInt::from(value.get_asset_class(&gives_asset))
                - if gives_asset == ADA_ASSET_CLASS {
                    minimum_ada
                } else {
                    BigInt::from(0)
                };
            if actual_amount_of_give_token < BigInt::from(0) {
                return Err(ValueError::GivesZeroTokens);
            }

            if actual_amount_of_give_token < gives {
                // This is an error in sundaedatum, even though the smart contract appears to allow it
                return Err(ValueError::DeclaredExceedsActual {
                    declared: gives,
                    actual: actual_amount_of_give_token,
                });
            }
            Ok(())
        }
        Order::Deposit((a, b)) => {
            let gives_a = a.2.clone();
            let gives_b = b.2.clone();
            let asset_a = AssetClass::from_pair((a.0.clone(), a.1.clone()));
            let asset_b = AssetClass::from_pair((b.0.clone(), b.1.clone()));
            let mut actual_a = BigInt::from(value.get_asset_class(&asset_a));
            if asset_a == ADA_ASSET_CLASS {
                let minimum = BigInt::from(ADA_RIDER) + scoop_fee.clone();
                if actual_a < minimum {
                    return Err(ValueError::HasInsufficientAda {
                        expected: minimum,
                        actual: actual_a,
                    });
                }
                actual_a -= minimum;
            }
            let actual_b = BigInt::from(value.get_asset_class(&asset_b));

            let deposits_zero_tokens =
                actual_a == BigInt::from(0u64) && actual_b == BigInt::from(0u64);
            if !deposits_zero_tokens {
                return Err(ValueError::GivesZeroTokens);
            }
            Ok(())
        }
        Order::Withdrawal((policy, token, offered)) => {
            if offered == &BigInt::from(0) {
                return Err(ValueError::GivesZeroTokens);
            }
            let actual = BigInt::from(
                value.get_asset_class(&AssetClass::from_pair((policy.clone(), token.clone()))),
            );
            if offered > &actual {
                return Err(ValueError::DeclaredExceedsActual {
                    declared: offered.clone(),
                    actual,
                });
            }
            let expected = BigInt::from(ADA_RIDER) + scoop_fee;
            let actual = BigInt::from(value.get_asset_class(&ADA_ASSET_CLASS));
            if actual < expected {
                return Err(ValueError::HasInsufficientAda { expected, actual });
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub enum PoolError {
    IdentMismatch,
    CoinPairMismatch,
}

pub fn validate_order_for_pool(order: &OrderDatum, pool: &PoolDatum) -> Result<(), PoolError> {
    if let Some(i) = &order.ident
        && i != &pool.ident
    {
        return Err(PoolError::IdentMismatch);
    }
    match &order.action {
        Order::Swap(a, b) => {
            let give_coin = AssetClass::from_pair((a.0.clone(), a.1.clone()));
            let take_coin = AssetClass::from_pair((b.0.clone(), b.1.clone()));
            let matches_a_to_b = pool.assets.0 == give_coin && pool.assets.1 == take_coin;
            let matches_b_to_a = pool.assets.0 == take_coin && pool.assets.1 == give_coin;
            if !(matches_a_to_b || matches_b_to_a) {
                return Err(PoolError::CoinPairMismatch);
            }
            Ok(())
        }
        Order::Deposit((a, b)) => {
            let give_coin = AssetClass::from_pair((a.0.clone(), a.1.clone()));
            let take_coin = AssetClass::from_pair((b.0.clone(), b.1.clone()));
            let matches_a_to_b = pool.assets.0 == give_coin && pool.assets.1 == take_coin;
            let matches_b_to_a = pool.assets.0 == take_coin && pool.assets.1 == give_coin;
            if !(matches_a_to_b || matches_b_to_a) {
                return Err(PoolError::CoinPairMismatch);
            }
            Ok(())
        }
        _ => Ok(()),
    }
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
            let gives = a.2.clone();
            let takes = b.2.clone();
            let coin_a = AssetClass::from_pair((a.0.clone(), a.1.clone()));
            let coin_b = AssetClass::from_pair((b.0.clone(), b.1.clone()));
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

#[derive(AsPlutus, Debug, PartialEq, Eq)]
pub enum Destination {
    Fixed(PlutusAddress, AikenDatum),
    SelfDestination,
}

#[derive(AsPlutus, Debug, PartialEq, Eq)]
pub enum AikenDatum {
    NoDatum,
    DatumHash(Vec<u8>),
    InlineDatum(Vec<u8>),
}

#[derive(Debug, PartialEq, Eq)]
pub struct AnyPlutusData {
    inner: PlutusData,
}

impl AnyPlutusData {
    fn empty_cons() -> Self {
        Self {
            inner: PlutusData::Constr(pallas_primitives::Constr {
                tag: 121,
                any_constructor: None,
                fields: pallas_primitives::MaybeIndefArray::Def(vec![]),
            }),
        }
    }
}

impl AsPlutus for AnyPlutusData {
    fn from_plutus(data: PlutusData) -> Result<Self, plutus_parser::DecodeError> {
        Ok(AnyPlutusData { inner: data })
    }

    fn to_plutus(self) -> PlutusData {
        self.inner
    }
}

#[derive(AsPlutus, Debug, PartialEq, Eq)]
pub struct PlutusAddress {
    pub payment_credential: PaymentCredential,
    pub stake_credential: Option<StakeCredential>,
}

#[derive(AsPlutus, Debug, PartialEq, Eq)]
pub enum Credential {
    VerificationKey(VerificationKeyHash),
    Script(ScriptHash),
}

type VerificationKeyHash = Vec<u8>;
type ScriptHash = Vec<u8>;

#[derive(AsPlutus, Debug, PartialEq, Eq)]
pub enum Referenced<T: AsPlutus> {
    Inline(T),
    Pointer(StakePointer),
}

type PaymentCredential = Credential;
type StakeCredential = Referenced<Credential>;

#[derive(AsPlutus, Debug, PartialEq, Eq)]
pub struct StakePointer {
    pub slot_number: BigInt,
    pub transaction_index: BigInt,
    pub certificate_index: BigInt,
}

#[derive(AsPlutus, Debug, PartialEq)]
pub struct OutputReference {
    transaction_id: Vec<u8>,
    transaction_ix: u64,
}

#[derive(AsPlutus, Debug, PartialEq)]
pub enum ValidityBound {
    NegativeInfinity,
    Finite(BigInt),
    PositiveInfinity,
}

#[derive(AsPlutus, Debug, PartialEq)]
pub struct ValidityRange {
    validity_range_lower_bound: ValidityBound,
    validity_range_upper_bound: ValidityBound,
}

#[derive(AsPlutus, Debug, PartialEq)]
pub struct StrategyExecution {
    tx_ref: OutputReference,
    validity_range: ValidityRange,
    details: Order,
    extensions: AnyPlutusData,
}

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

#[derive(Clone, Eq, PartialEq)]
pub struct SundaeV3Pool {
    pub address: pallas_addresses::Address,
    pub value: Value,
    pub pool_datum: PoolDatum,
    pub slot: u64,
}

impl PartialOrd for SundaeV3Pool {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.slot.cmp(&other.slot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_singletonvalue() {
        let bytes = hex::decode("9f4100410102ff").unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let singleton: SingletonValue = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(singleton.2, BigInt::from(2));
    }

    #[test]
    fn test_decode_swap() {
        let bytes = hex::decode("d87a9f9f4100410102ff9f4103410405ffff").unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let order: Order = AsPlutus::from_plutus(pd).unwrap();
    }

    #[test]
    fn test_decode_orderdatum() {
        let od_bytes = hex::decode("d8799fd8799f581c99999999999999999999999999999999999999999999999999999999ffd8799f581c88888888888888888888888888888888888888888888888888888888ff0ad8799fd8799fd8799f581c77777777777777777777777777777777777777777777777777777777ffd87a80ffd87980ffd87a9f9f4100410102ff9f4103410405ffffd87980ff").unwrap();
        let order_pd: PlutusData = minicbor::decode(&od_bytes).unwrap();
        println!("{:?}", order_pd);
        let order: OrderDatum = AsPlutus::from_plutus(order_pd).unwrap();
        let expected_ident =
            hex::decode("99999999999999999999999999999999999999999999999999999999").unwrap();
        let expected_signature =
            hex::decode("88888888888888888888888888888888888888888888888888888888").unwrap();
        let expected_vkey =
            hex::decode("77777777777777777777777777777777777777777777777777777777").unwrap();
        assert_eq!(order.ident.unwrap().to_bytes(), expected_ident);
        assert_eq!(order.owner, Multisig::Signature(expected_signature));
        assert_eq!(order.scoop_fee, BigInt::from(10));
        assert_eq!(
            order.destination,
            Destination::Fixed(
                PlutusAddress {
                    payment_credential: Credential::VerificationKey(expected_vkey),
                    stake_credential: None,
                },
                AikenDatum::NoDatum,
            )
        );
        assert_eq!(
            order.action,
            Order::Swap(
                (vec![0], vec![1], BigInt::from(2)),
                (vec![3], vec![4], BigInt::from(5))
            )
        );
        assert_eq!(
            order.extra,
            AnyPlutusData {
                inner: PlutusData::Constr(pallas_primitives::Constr {
                    tag: 121,
                    any_constructor: None,
                    fields: pallas_primitives::MaybeIndefArray::Def(vec![]),
                })
            }
        );
    }

    #[test]
    fn test_decode_orderdatum_2() {
        let od_bytes = hex::decode("d8799fd8799f581c12d88c7f234493742d583c219101050b39e925d715a93060752d60d3ffd8799f581c621be66c7f488b22f66003fff0b7427c30f70da678c532b7233d85caff1a00138800d8799fd8799fd8799f581c1c1381a51312b9da9782b3f507af94bab78780f85196007fad5fbde3ffd8799fd8799fd8799f581c621be66c7f488b22f66003fff0b7427c30f70da678c532b7233d85caffffffffd8799fffffd87a9f9f581cac597ca62a32cab3f4766c8f9cd577e50ebb1d00383ec7fa3990b01646435241574a551a0002113eff9f40401a066b2bc2ffff43d87980ff").unwrap();
        let order_pd: PlutusData = minicbor::decode(&od_bytes).unwrap();
        let order: OrderDatum = AsPlutus::from_plutus(order_pd).unwrap();
        let expected_ident =
            hex::decode("12d88c7f234493742d583c219101050b39e925d715a93060752d60d3").unwrap();
        assert_eq!(order.ident.unwrap().to_bytes(), expected_ident);
    }

    #[test]
    fn test_decode_pooldatum() {
        let pd_bytes = hex::decode("d8799f581cba228444515fbefd2c8725338e49589f206c7f18a33e002b157aac3c9f9f4040ff9f581c99b071ce8580d6a3a11b4902145adb8bfd0d2a03935af8cf66403e1546534245525259ffff1a01c9c3801901f41901f4d8799fd87f9f581ce8dc0595c8d3a7e2c0323a11f5519c32d3b3fb7a994519e38b698b5dffff001a003d0900ff").unwrap();
        let pool_pd: PlutusData = minicbor::decode(&pd_bytes).unwrap();
        let pool: PoolDatum = AsPlutus::from_plutus(pool_pd).unwrap();
        let expected_ident =
            hex::decode("ba228444515fbefd2c8725338e49589f206c7f18a33e002b157aac3c").unwrap();
        assert_eq!(pool.ident.to_bytes(), expected_ident);
    }

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

    fn test_validate_ada_rberry_swap_schema(test_case: ValidateAdaRBerrySwapTestCase) -> bool {
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
                (ADA_POLICY, ADA_TOKEN, i64_to_bigint(test_case.ada_offered)),
                (
                    rberry_policy.clone(),
                    rberry_token.clone(),
                    i64_to_bigint(test_case.rberry_offered),
                ),
            ),
            extra: AnyPlutusData::empty_cons(),
        };
        let rberry_asset_class = AssetClass::from_pair((rberry_policy, rberry_token));
        let value = value![
            test_case.actual_ada,
            (&rberry_asset_class, test_case.actual_rberry)
        ];
        validate_order_value(&order, &value).is_ok()
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
    ) -> bool {
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
                (
                    rberry_policy.clone(),
                    rberry_policy.clone(),
                    i64_to_bigint(test_case.rberry_offered),
                ),
                (
                    sberry_policy.clone(),
                    sberry_token.clone(),
                    i64_to_bigint(test_case.sberry_offered),
                ),
            ),
            extra: AnyPlutusData::empty_cons(),
        };
        let rberry_asset_class = AssetClass::from_pair((rberry_policy, rberry_token));
        let sberry_asset_class = AssetClass::from_pair((sberry_policy, sberry_token));
        let value = value![
            test_case.actual_ada,
            (&rberry_asset_class, test_case.actual_rberry),
            (&sberry_asset_class, test_case.actual_sberry)
        ];
        validate_order_value(&order, &value).is_ok()
    }

    #[test]
    fn test_validate_ada_rberry_swap() {
        assert!(test_validate_ada_rberry_swap_schema(
            ValidateAdaRBerrySwapTestCase {
                scoop_fee: 1_000_000,
                ada_offered: 1_000_000,
                rberry_offered: 1_000_000,
                actual_ada: 10_000_000,
                actual_rberry: 1_000_000,
            }
        ))
    }

    // 3 ADA on the utxo is not sufficient because after deducting the 1 ADA
    // scoop fee and the 1 ADA offered the remaining amount is 1 ADA, less than
    // the 2 ADA rider value
    #[test]
    fn test_validate_ada_rberry_swap_insufficient_ada() {
        assert!(!test_validate_ada_rberry_swap_schema(
            ValidateAdaRBerrySwapTestCase {
                scoop_fee: 1_000_000,
                ada_offered: 1_000_000,
                rberry_offered: 1_000_000,
                actual_ada: 3_000_000,
                actual_rberry: 1_000_000,
            }
        ))
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
        let pd = PoolDatum {
            ident: Ident(vec![]),
            assets: (ADA_ASSET_CLASS, rberry_asset_class.clone()),
            circulating_lp: i64_to_bigint(0),
            bid_fees_per_10_thousand: i64_to_bigint(0),
            ask_fees_per_10_thousand: i64_to_bigint(0),
            fee_manager: None,
            market_open: i64_to_bigint(0),
            protocol_fees: i64_to_bigint(protocol_fees),
        };
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
        let pd = PoolDatum {
            ident: Ident(vec![]),
            assets: (ADA_ASSET_CLASS, rberry_asset_class.clone()),
            circulating_lp: i64_to_bigint(0),
            bid_fees_per_10_thousand: i64_to_bigint(0),
            ask_fees_per_10_thousand: i64_to_bigint(0),
            fee_manager: None,
            market_open: i64_to_bigint(0),
            protocol_fees: i64_to_bigint(protocol_fees),
        };
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
            ident: Some(Ident(vec![])),
            owner: Multisig::Signature(vec![]),
            scoop_fee: i64_to_bigint(1_280_000),
            destination: Destination::SelfDestination,
            action: Order::Swap(
                (
                    rberry_policy.clone(),
                    rberry_token,
                    i64_to_bigint(1_000_000),
                ),
                (rberry_policy, sberry_token, i64_to_bigint(10_000_000)),
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
            ident: Some(Ident(vec![])),
            owner: Multisig::Signature(vec![]),
            scoop_fee: i64_to_bigint(1_280_000),
            destination: Destination::SelfDestination,
            action: Order::Swap(
                (
                    rberry_policy.clone(),
                    sberry_token,
                    i64_to_bigint(1_000_000),
                ),
                (rberry_policy, rberry_token, i64_to_bigint(10_000_000)),
            ),
            extra: AnyPlutusData::empty_cons(),
        };
        let swap_price = swap_price(&od);
        assert_eq!(swap_price, Some((SwapDirection::BtoA, 0.1)));
    }
}
