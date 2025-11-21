use pallas_primitives::{BigInt, PlutusData};
use plutus_parser::AsPlutus;
use std::fmt;

use crate::cardano_types::{ADA_ASSET_CLASS, AssetClass, Value};
use crate::multisig::Multisig;

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

#[derive(AsPlutus, Clone)]
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

#[derive(AsPlutus, Debug, PartialEq)]
pub enum StrategyAuthorization {
    Signature(Vec<u8>),
    Script(Vec<u8>),
}

pub type SingletonValue = (Vec<u8>, Vec<u8>, BigInt);

#[derive(AsPlutus, Debug, PartialEq)]
pub enum Order {
    Strategy(StrategyAuthorization),
    Swap(SingletonValue, SingletonValue),
    Deposit((SingletonValue, SingletonValue)),
    Withdrawal(SingletonValue),
    Donation((SingletonValue, SingletonValue)),
    Record(AssetClass),
}

#[derive(AsPlutus, Debug, PartialEq)]
pub struct OrderDatum {
    pub ident: Option<Ident>,
    pub owner: Multisig,
    pub scoop_fee: BigInt,
    pub destination: Destination,
    pub action: Order,
    pub extra: AnyPlutusData,
}

#[derive(AsPlutus, Debug, PartialEq)]
pub enum Destination {
    Fixed(PlutusAddress, AikenDatum),
    SelfDestination,
}

#[derive(AsPlutus, Debug, PartialEq)]
pub enum AikenDatum {
    NoDatum,
    DatumHash(Vec<u8>),
    InlineDatum(Vec<u8>),
}

#[derive(Debug, PartialEq)]
pub struct AnyPlutusData {
    inner: PlutusData,
}

impl AsPlutus for AnyPlutusData {
    fn from_plutus(data: PlutusData) -> Result<Self, plutus_parser::DecodeError> {
        Ok(AnyPlutusData { inner: data })
    }

    fn to_plutus(self) -> PlutusData {
        self.inner
    }
}

//#[derive(AsPlutus, Debug, PartialEq)]
//pub struct FixedDestination {
//    pub address: PlutusAddress,
//    pub datum: AikenDatum,
//}

#[derive(AsPlutus, Debug, PartialEq)]
pub struct PlutusAddress {
    pub payment_credential: PaymentCredential,
    pub stake_credential: Option<StakeCredential>,
}

#[derive(AsPlutus, Debug, PartialEq)]
pub enum Credential {
    VerificationKey(VerificationKeyHash),
    Script(ScriptHash),
}

type VerificationKeyHash = Vec<u8>;
type ScriptHash = Vec<u8>;

#[derive(AsPlutus, Debug, PartialEq)]
pub enum Referenced<T: AsPlutus> {
    Inline(T),
    Pointer(StakePointer),
}

type PaymentCredential = Credential;
type StakeCredential = Referenced<Credential>;

#[derive(AsPlutus, Debug, PartialEq)]
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

//#[derive(AsPlutus, Debug, PartialEq)]
//pub enum PoolMintRedeemer {
//    MintLP(Ident),
//    CreatePool(CreatePool),
//    BurnPool(Ident),
//}
//
//#[derive(AsPlutus, Debug, PartialEq)]
//pub struct CreatePool {
//    assets: (AssetClass, AssetClass),
//    pool_output: BigInt,
//    metadata_output: BigInt,
//}
//
//#[derive(AsPlutus, Debug, PartialEq)]
//pub enum ManageRedeemer {
//    WithdrawFees(WithdrawFees),
//    UpdatePoolFees(BigInt),
//}
//
//#[derive(AsPlutus, Debug, PartialEq)]
//pub struct WithdrawFees {
//    amount: BigInt,
//    treasury_output: BigInt,
//    pool_input: BigInt,
//}

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

pub fn get_pool_price(pool_policy: &[u8], v: &Value) -> Option<f64> {
    let (coin_a, coin_b) = get_pool_asset_pair(pool_policy, v)?;
    let quantity_a = v.get_asset_class(&coin_a);
    let quantity_b = v.get_asset_class(&coin_b);
    Some((quantity_a as f64) / (quantity_b as f64))
}

pub struct SundaeV3Pool {
    pub address: pallas_addresses::Address,
    pub value: Value,
    pub pool_datum: PoolDatum,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_singletonvalue() {
        let bytes = hex::decode("9f4100410102ff").unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let singleton: SingletonValue = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(singleton.2, BigInt::Int(2.into()));
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
        assert_eq!(order.scoop_fee, BigInt::Int(10.into()));
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
                (vec![0], vec![1], BigInt::Int(2.into()),),
                (vec![3], vec![4], BigInt::Int(5.into()),)
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
}
