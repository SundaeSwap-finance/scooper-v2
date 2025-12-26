#![allow(unused)]

use anyhow::bail;
use num_traits::{ConstZero, Zero};
use pallas_addresses::Address;
use pallas_primitives::conway::{DatumOption, MintedDatumOption, NativeScript};
use pallas_primitives::{DatumHash, Hash, KeepRaw, PlutusData, PlutusScript};
use pallas_traverse::MultiEraOutput;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::str::FromStr;

use plutus_parser::AsPlutus;

use crate::bigint::BigInt;
use crate::datum_lookup::ScopedDatumLookup;
use crate::serde_compat::serialize_address;
use crate::sundaev3::{OrderDatum, PoolDatum, SettingsDatum};
pub type Bytes = Vec<u8>;

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub enum ScriptRef {
    Native(NativeScript),
    PlutusV1(PlutusScript<1>),
    PlutusV2(PlutusScript<2>),
    PlutusV3(PlutusScript<3>),
}

pub const ADA_POLICY: Vec<u8> = vec![];
pub const ADA_TOKEN: Vec<u8> = vec![];

pub const ADA_ASSET_CLASS: AssetClass = AssetClass {
    policy: ADA_POLICY,
    token: ADA_TOKEN,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AssetClass {
    pub policy: Vec<u8>,
    pub token: Vec<u8>,
}

impl FromStr for AssetClass {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "lovelace" {
            return Ok(ADA_ASSET_CLASS);
        }
        let Some((policy_hex, token_hex)) = s.split_once(".") else {
            bail!("no dot found");
        };
        let policy = hex::decode(policy_hex)?;
        let token = hex::decode(token_hex)?;
        Ok(AssetClass { policy, token })
    }
}

impl<'de> serde::Deserialize<'de> for AssetClass {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str = String::deserialize(deserializer)?;
        str.parse().map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for AssetClass {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if self.policy.is_empty() {
            return serializer.serialize_str("lovelace");
        }

        let policy_hex = hex::encode(&self.policy);
        let name_hex = hex::encode(&self.token);

        serializer.serialize_str(&format!("{}.{}", policy_hex, name_hex))
    }
}

impl AsPlutus for AssetClass {
    fn from_plutus(data: PlutusData) -> Result<Self, plutus_parser::DecodeError> {
        let (policy, token) = AsPlutus::from_plutus(data)?;
        Ok(AssetClass { policy, token })
    }

    fn to_plutus(self) -> PlutusData {
        let tuple = (self.policy, self.token);
        tuple.to_plutus()
    }
}

impl AssetClass {
    pub fn from_pair(pair: (Vec<u8>, Vec<u8>)) -> AssetClass {
        AssetClass {
            policy: pair.0,
            token: pair.1,
        }
    }
}

impl fmt::Display for AssetClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.policy.is_empty() {
            write!(f, "Ada")
        } else {
            write!(
                f,
                "{}.{}",
                hex::encode(&self.policy),
                hex::encode(&self.token)
            )
        }
    }
}

pub type Rational = (BigInt, BigInt);

pub type VerificationKey = Bytes;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Value(pub BTreeMap<Bytes, BTreeMap<Bytes, BigInt>>);

#[macro_export]
macro_rules! value {
    ( $ada:expr ) => {
        {
            let mut value = $crate::cardano_types::Value::new();
            value.insert(&$crate::cardano_types::ADA_ASSET_CLASS, BigInt::from($ada));
            value
        }
    };
    ( $ada:expr, $( $token:expr ),* ) => {
        {
            let mut value = $crate::cardano_types::Value::new();
            value.insert(&$crate::cardano_types::ADA_ASSET_CLASS, BigInt::from($ada));
            $(
                value.insert($token.0, BigInt::from($token.1));
            )*
            value
        }
    };
}

impl Value {
    pub fn new() -> Self {
        Value(BTreeMap::new())
    }

    pub fn get(&self, asset_class: &AssetClass) -> BigInt {
        if let Some(assets) = self.0.get(&asset_class.policy)
            && let Some(quantity) = assets.get(&asset_class.token)
        {
            return quantity.clone();
        }
        BigInt::ZERO
    }

    pub fn insert(&mut self, asset_class: &AssetClass, quantity: BigInt) {
        if quantity.is_zero() {
            self.delete(asset_class);
            return;
        }
        match self.0.get_mut(&asset_class.policy) {
            Some(tokens) => {
                tokens.insert(asset_class.token.clone(), quantity);
            }
            None => {
                let mut new_tokens = BTreeMap::new();
                new_tokens.insert(asset_class.token.clone(), quantity);
                self.0.insert(asset_class.policy.clone(), new_tokens);
            }
        }
    }

    pub fn delete(&mut self, asset_class: &AssetClass) {
        let Some(tokens) = self.0.get_mut(&asset_class.policy) else {
            return;
        };
        tokens.remove(&asset_class.token);
        if tokens.is_empty() {
            self.0.remove(&asset_class.policy);
        }
    }

    pub fn add(&mut self, asset_class: &AssetClass, quantity: &BigInt) {
        let new_amount = self.get(asset_class) + quantity;
        self.insert(asset_class, new_amount);
    }

    pub fn subtract(&mut self, asset_class: &AssetClass, quantity: &BigInt) {
        let new_amount = self.get(asset_class) - quantity;
        self.insert(asset_class, new_amount);
    }
}

impl serde::Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let outer = &self.0;

        let mut map = serializer.serialize_map(None)?;

        for (policy, inner) in outer {
            if policy.is_empty() {
                for qty in inner.values() {
                    map.serialize_entry("lovelace", qty)?;
                }
                continue;
            }

            let policy_hex = hex::encode(policy);

            for (token, qty) in inner {
                let token_hex = hex::encode(token);
                let key = format!("{}.{}", policy_hex, token_hex);
                map.serialize_entry(&key, qty)?;
            }
        }

        map.end()
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum RawDatum {
    None,
    Inline(PlutusData),
    Hash(DatumHash),
}

impl RawDatum {
    pub fn parse<T: AsPlutus>(&self, datums: &ScopedDatumLookup) -> Option<T> {
        T::from_plutus(self.plutus_data(datums)?.clone()).ok()
    }

    pub fn plutus_data<'a>(&'a self, datums: &'a ScopedDatumLookup<'a>) -> Option<&'a PlutusData> {
        match self {
            Self::None => None,
            Self::Inline(d) => Some(d),
            Self::Hash(h) => datums.lookup_datum(*h),
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct TransactionOutput {
    pub address: Address,
    pub value: Value,
    pub datum: RawDatum,
    pub script_ref: Option<ScriptRef>,
}

impl TransactionOutput {
    pub fn hashed_datum(&self, lookup: &ScopedDatumLookup) -> Option<Vec<u8>> {
        if let RawDatum::Hash(hash) = &self.datum {
            lookup.lookup_bytes(*hash)
        } else {
            None
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct TransactionInput(pub pallas_primitives::TransactionInput);
impl TransactionInput {
    pub fn new(transaction_id: Hash<32>, index: u64) -> Self {
        Self(pallas_primitives::TransactionInput {
            transaction_id,
            index,
        })
    }
}

impl serde::ser::Serialize for TransactionInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{}", self))
    }
}

impl fmt::Display for TransactionInput {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}#{}", hex::encode(self.0.transaction_id), self.0.index)
    }
}

fn convert_datum(datum: Option<MintedDatumOption>) -> RawDatum {
    match datum {
        None => RawDatum::None,
        Some(MintedDatumOption::Data(d)) => RawDatum::Inline(d.0.unwrap()),
        Some(MintedDatumOption::Hash(h)) => RawDatum::Hash(h),
    }
}

fn convert_value<'b>(value: pallas_traverse::MultiEraValue<'b>) -> Value {
    let mut result = BTreeMap::new();
    let mut ada_policy = BTreeMap::new();
    ada_policy.insert(vec![], value.coin().into());
    result.insert(vec![], ada_policy);
    for policy in value.assets() {
        let mut p_map = BTreeMap::new();
        let pol = policy.policy();
        for asset in policy.assets() {
            let tok = asset.name();
            p_map.insert(tok.to_vec(), BigInt::from(asset.any_coin()));
        }
        result.insert(pol.to_vec(), p_map);
    }
    Value(result)
}

fn convert_script_ref(script_ref: pallas_primitives::conway::MintedScriptRef) -> ScriptRef {
    match script_ref {
        pallas_primitives::conway::MintedScriptRef::NativeScript(n) => {
            ScriptRef::Native(n.unwrap())
        }
        pallas_primitives::conway::MintedScriptRef::PlutusV1Script(s) => ScriptRef::PlutusV1(s),
        pallas_primitives::conway::MintedScriptRef::PlutusV2Script(s) => ScriptRef::PlutusV2(s),
        pallas_primitives::conway::MintedScriptRef::PlutusV3Script(s) => ScriptRef::PlutusV3(s),
    }
}

pub fn convert_txo(output: &MultiEraOutput) -> TransactionOutput {
    let address = output.address().unwrap();
    let datum = convert_datum(output.datum());
    let value = convert_value(output.value());
    let script_ref = output.script_ref().map(convert_script_ref);
    TransactionOutput {
        address,
        datum,
        value,
        script_ref,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assetclass_ord() {
        let rberry = AssetClass::from_pair((vec![0x66, 0x67], vec![0x66, 0x66]));
        let sberry = AssetClass::from_pair((vec![0x66, 0x67], vec![0x66, 0x67]));
        let foobar = AssetClass::from_pair((vec![0x99, 0x99], vec![0x01, 0x01]));
        assert!(ADA_ASSET_CLASS < rberry);
        assert!(rberry < sberry);
        assert!(sberry < foobar);
    }
}
