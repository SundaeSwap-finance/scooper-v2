#![allow(unused)]

use pallas_addresses::Address;
use pallas_primitives::conway::{DatumOption, NativeScript};
use pallas_primitives::{PlutusData, PlutusScript};
use pallas_traverse::MultiEraOutput;

use std::collections::BTreeMap;
use std::fmt;

use plutus_parser::AsPlutus;

pub type Bytes = Vec<u8>;

#[derive(Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Value(pub BTreeMap<Bytes, BTreeMap<Bytes, i128>>);

#[macro_export]
macro_rules! value {
    ( $ada:expr, $( $token:expr ),* ) => {
        {
            let mut value = $crate::cardano_types::Value::new();
            value.insert(&$crate::cardano_types::ADA_ASSET_CLASS, $ada);
            $(
                value.insert($token.0, $token.1);
            )*
            value
        }
    };
}

impl Value {
    pub fn new() -> Self {
        Value(BTreeMap::new())
    }

    pub fn get_asset_class(&self, asset_class: &AssetClass) -> i128 {
        if let Some(assets) = self.0.get(&asset_class.policy)
            && let Some(quantity) = assets.get(&asset_class.token)
        {
            return *quantity;
        }
        0
    }

    pub fn insert(&mut self, asset_class: &AssetClass, quantity: i128) {
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
}

#[derive(PartialEq, Eq, Debug)]
pub enum Datum {
    None,
    Hash(Bytes),
    Data(Bytes),
}

// Would be convenient to parameterize this by the type of the decoded datum, with
// an 'Any' type that always succeeds at decoding and functions
//   TransactionOutput<T> -> TransactionOutput<Any>
//   TransactionOutput<Any> -> Result<TransactionOutput<T>, Error> where T: minicbor::Decode
#[derive(PartialEq, Eq, Debug)]
pub struct TransactionOutput {
    pub address: Address,
    pub value: Value,
    pub datum: Datum,
    pub script_ref: Option<ScriptRef>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct TransactionInput(pub pallas_primitives::TransactionInput);

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

pub fn convert_datum(datum: Option<DatumOption>) -> Datum {
    match datum {
        None => Datum::None,
        Some(DatumOption::Hash(h)) => Datum::Hash(h.to_vec()),
        Some(DatumOption::Data(d)) => Datum::Data(d.unwrap().raw_cbor().to_vec()),
    }
}

pub fn convert_value<'b>(value: pallas_traverse::MultiEraValue<'b>) -> Value {
    let mut result = BTreeMap::new();
    let mut ada_policy = BTreeMap::new();
    ada_policy.insert(vec![], value.coin().into());
    result.insert(vec![], ada_policy);
    for policy in value.assets() {
        let mut p_map = BTreeMap::new();
        let pol = policy.policy();
        for asset in policy.assets() {
            let tok = asset.name();
            p_map.insert(tok.to_vec(), asset.any_coin());
        }
        result.insert(pol.to_vec(), p_map);
    }
    Value(result)
}

pub fn convert_script_ref(script_ref: pallas_primitives::conway::ScriptRef) -> ScriptRef {
    match script_ref {
        pallas_primitives::conway::ScriptRef::NativeScript(n) => ScriptRef::Native(n.unwrap()),
        pallas_primitives::conway::ScriptRef::PlutusV1Script(s) => ScriptRef::PlutusV1(s),
        pallas_primitives::conway::ScriptRef::PlutusV2Script(s) => ScriptRef::PlutusV2(s),
        pallas_primitives::conway::ScriptRef::PlutusV3Script(s) => ScriptRef::PlutusV3(s),
    }
}

pub fn convert_transaction_output<'b>(output: &MultiEraOutput<'b>) -> TransactionOutput {
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
