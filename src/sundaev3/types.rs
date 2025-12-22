use pallas_primitives::PlutusData;
use plutus_parser::AsPlutus;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use std::fmt;

use crate::bigint::BigInt;
use crate::cardano_types::{AssetClass, Rational, TransactionInput, Value, VerificationKey};
use crate::multisig::Multisig;

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SettingsDatum {
    pub settings_admin: Multisig,
    pub metadata_admin: PlutusAddress,
    pub treasury_admin: Multisig,
    pub treasury_address: PlutusAddress,
    pub treasury_allowance: Rational,
    pub authorized_scoopers: Option<Vec<VerificationKey>>,
    pub authorized_staking_keys: Vec<Credential>,
    pub base_fee: BigInt,
    pub simple_fee: BigInt,
    pub strategy_fee: BigInt,
    pub pool_creation_fee: BigInt,
    pub extensions: PlutusData,
}

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

impl serde::Serialize for Ident {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let hex_str = hex::encode(&**self);
        serializer.serialize_str(&hex_str)
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

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
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

#[derive(AsPlutus, Debug, PartialEq)]
pub enum PoolRedeemer {
    // When constructing a pool scoop redeemer we don't construct SSEs because they will be
    // retrieved from a database. So it's better to represent them here as raw bytes.
    PoolScoop {
        signatory_index: u64,
        scooper_index: u64,
        input_order: Vec<(u64, Option<SSEBytes>, BigInt)>,
    },
    Manage,
}

#[derive(AsPlutus, Debug, PartialEq)]
#[variant = 1]
pub struct WrappedRedeemer<T: AsPlutus>(pub T);

/// An order can be spent either to Scoop (execute) it, or to cancel it
#[derive(AsPlutus, Debug, PartialEq, Eq)]
pub enum OrderRedeemer {
    /// Execute the order
    Scoop,
    /// Or cancel/update it
    Cancel,
}

#[derive(AsPlutus, Debug, PartialEq)]
pub struct SSEBytes(Vec<u8>);

#[derive(AsPlutus, Debug, PartialEq)]
#[expect(unused)]
pub struct SignedStrategyExecution {
    execution: StrategyExecution,
    signature: Option<Vec<u8>>,
}

#[derive(Clone, AsPlutus, Debug, PartialEq, Eq)]
pub enum StrategyAuthorization {
    Signature(Vec<u8>),
    Script(Vec<u8>),
}

impl Serialize for StrategyAuthorization {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            StrategyAuthorization::Signature(bytes) => {
                let hex = hex::encode(bytes);
                let mut st = serializer.serialize_struct("StrategyAuthorization", 1)?;
                st.serialize_field("Signature", &hex)?;
                st.end()
            }
            StrategyAuthorization::Script(bytes) => {
                let hex = hex::encode(bytes);
                let mut st = serializer.serialize_struct("StrategyAuthorization", 1)?;
                st.serialize_field("Script", &hex)?;
                st.end()
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SingletonValue {
    pub policy: Vec<u8>,
    pub token: Vec<u8>,
    pub amount: BigInt,
}

impl SingletonValue {
    pub fn asset_class(&self) -> AssetClass {
        AssetClass {
            policy: self.policy.clone(),
            token: self.token.clone(),
        }
    }
    pub fn new(class: AssetClass, amount: BigInt) -> Self {
        Self {
            policy: class.policy,
            token: class.token,
            amount,
        }
    }
}

impl AsPlutus for SingletonValue {
    fn from_plutus(pd: PlutusData) -> Result<Self, plutus_parser::DecodeError> {
        let (policy, token, amount): (Vec<u8>, Vec<u8>, BigInt) = AsPlutus::from_plutus(pd)?;
        Ok(Self {
            policy,
            token,
            amount,
        })
    }

    fn to_plutus(self) -> PlutusData {
        (self.policy, self.token, self.amount).to_plutus()
    }
}

impl serde::Serialize for SingletonValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let key = if self.policy.is_empty() {
            "lovelace".to_string()
        } else {
            format!("{}.{}", hex::encode(&self.policy), hex::encode(&self.token))
        };

        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&key, &self.amount)?;
        map.end()
    }
}

#[derive(Clone, AsPlutus, Debug, PartialEq, Eq)]
pub enum Order {
    Strategy(StrategyAuthorization),
    Swap(SingletonValue, SingletonValue),
    Deposit((SingletonValue, SingletonValue)),
    Withdrawal(SingletonValue),
    Donation((SingletonValue, SingletonValue)),
    Record(AssetClass),
}

impl serde::Serialize for Order {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(1))?;

        match self {
            Order::Strategy(auth) => {
                map.serialize_entry("Strategy", auth)?;
            }

            Order::Swap(a, b) => {
                map.serialize_entry("Swap", &(a, b))?;
            }

            Order::Deposit((a, b)) => {
                map.serialize_entry("Deposit", &(a, b))?;
            }

            Order::Withdrawal(v) => {
                map.serialize_entry("Withdrawal", v)?;
            }

            Order::Donation((a, b)) => {
                map.serialize_entry("Donation", &(a, b))?;
            }

            Order::Record(asset_class) => {
                map.serialize_entry("Record", asset_class)?;
            }
        };

        map.end()
    }
}

#[derive(Clone, AsPlutus, Debug, PartialEq, Eq, serde::Serialize)]
pub struct OrderDatum {
    pub ident: Option<Ident>,
    pub owner: Multisig,
    pub scoop_fee: BigInt,
    pub destination: Destination,
    pub action: Order,
    pub extra: PlutusData,
}

#[derive(Clone, AsPlutus, Debug, PartialEq, Eq)]
pub enum Destination {
    Fixed(PlutusAddress, AikenDatum),
    SelfDestination,
}

impl serde::Serialize for Destination {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        match self {
            Destination::SelfDestination => serializer.serialize_str("self"),

            Destination::Fixed(addr, datum) => {
                let payment_hex = match &addr.payment_credential {
                    Credential::VerificationKey(vkh) => hex::encode(vkh.as_slice()),
                    Credential::Script(sh) => hex::encode(sh.as_slice()),
                };

                let stake_hex: Option<String> = match &addr.stake_credential {
                    Some(Referenced::Inline(Credential::VerificationKey(vkh))) => {
                        Some(hex::encode(vkh.as_slice()))
                    }
                    Some(Referenced::Inline(Credential::Script(sh))) => {
                        Some(hex::encode(sh.as_slice()))
                    }
                    _ => None,
                };

                let datum_hex: Option<String> = match datum {
                    AikenDatum::NoDatum => None,
                    AikenDatum::DatumHash(v) => Some(hex::encode(v)),
                    AikenDatum::InlineDatum(v) => Some(hex::encode(v.clone().to_plutus_bytes())),
                };

                let mut map = serializer.serialize_map(Some(2))?;

                map.serialize_entry(
                    "address",
                    &serde_json::json!({
                        "payment": payment_hex,
                        "stake": stake_hex
                    }),
                )?;

                map.serialize_entry("datum", &datum_hex)?;
                map.end()
            }
        }
    }
}

#[derive(Clone, AsPlutus, Debug, PartialEq, Eq)]
pub enum AikenDatum {
    NoDatum,
    DatumHash(Vec<u8>),
    InlineDatum(PlutusData),
}

#[cfg(test)]
pub fn empty_cons() -> PlutusData {
    PlutusData::Constr(pallas_primitives::Constr {
        tag: 121,
        any_constructor: None,
        fields: pallas_primitives::MaybeIndefArray::Def(vec![]),
    })
}

#[derive(Clone, AsPlutus, Debug, PartialEq, Eq, serde::Serialize)]
pub struct PlutusAddress {
    pub payment_credential: PaymentCredential,
    pub stake_credential: Option<StakeCredential>,
}

#[derive(Clone, AsPlutus, Debug, PartialEq, Eq, serde::Serialize)]
pub enum Credential {
    VerificationKey(VerificationKeyHash),
    Script(ScriptHash),
}

type VerificationKeyHash = Vec<u8>;
type ScriptHash = Vec<u8>;

#[derive(Clone, AsPlutus, Debug, PartialEq, Eq, serde::Serialize)]
pub enum Referenced<T: AsPlutus> {
    Inline(T),
    Pointer(StakePointer),
}

type PaymentCredential = Credential;
type StakeCredential = Referenced<Credential>;

#[derive(Clone, AsPlutus, Debug, PartialEq, Eq, serde::Serialize)]
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
    extensions: PlutusData,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize)]
pub struct SundaeV3Pool {
    pub input: TransactionInput,
    pub value: Value,
    pub pool_datum: PoolDatum,
    pub slot: u64,
}

impl PartialOrd for SundaeV3Pool {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.slot.cmp(&other.slot))
    }
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct SundaeV3Order {
    pub input: TransactionInput,
    pub value: Value,
    pub datum: OrderDatum,
    pub slot: u64,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct SundaeV3Settings {
    pub input: TransactionInput,
    pub datum: SettingsDatum,
    pub slot: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_singletonvalue() {
        let bytes = hex::decode("9f4100410102ff").unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let singleton: SingletonValue = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(singleton.amount, BigInt::from(2));
    }

    #[test]
    fn test_decode_swap() {
        let bytes = hex::decode("d87a9f9f4100410102ff9f4103410405ffff").unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let order: Order = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(
            order,
            Order::Swap(
                SingletonValue {
                    policy: vec![0x00],
                    token: vec![0x01],
                    amount: BigInt::from(2),
                },
                SingletonValue {
                    policy: vec![0x03],
                    token: vec![0x04],
                    amount: BigInt::from(5),
                }
            )
        );
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
                SingletonValue {
                    policy: vec![0],
                    token: vec![1],
                    amount: BigInt::from(2),
                },
                SingletonValue {
                    policy: vec![3],
                    token: vec![4],
                    amount: BigInt::from(5),
                }
            )
        );
        assert_eq!(order.extra, empty_cons());
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
    fn test_deocde_orderdatum_3() {
        let od_bytes = hex::decode("d8799fd8799f581c035002e600d25a96003ecd1746007f59bac2788355687d18c7927119ffd8799f581c121fd22e0b57ac206fefc763f8bfa0771919f5218b40691eea4514d0ff1a0007a120d8799fd8799fd87a9f581c73275b9e267fd927bfc14cf653d904d1538ad8869260ab638bf73f5cffd8799fd8799fd8799f581c045d47cac5067ce697478c11051deb935a152e0773a5d7430a11baa8ffffffffd87b9fd8799fd8799f581c121fd22e0b57ac206fefc763f8bfa0771919f5218b40691eea4514d0ff80ffffffd87b9f9f9f40401a0f1b6d96ff9f581c2fe3c3364b443194b10954771c95819b8d6ed464033c21f03f8facb544694254431903ddffffff43d87980ff").unwrap();
        let order_pd: PlutusData = minicbor::decode(&od_bytes).unwrap();
        let order: OrderDatum = AsPlutus::from_plutus(order_pd).unwrap();
        let expected_ident =
            hex::decode("035002e600d25a96003ecd1746007f59bac2788355687d18c7927119").unwrap();
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
