use pallas_addresses::Address;
use plutus_parser::BigInt;
use serde::ser::SerializeMap;
use serde::{Deserializer, Serialize, Serializer, de, ser::Error};

use crate::cardano_types::AssetClass;
use crate::multisig::Multisig;
use crate::{cardano_types::Value, sundaev3::Ident};

struct AddressVisitor;

impl<'de> de::Visitor<'de> for AddressVisitor {
    type Value = Address;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a bech32-encoded address")
    }

    fn visit_str<E>(self, bech32: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Address::from_bech32(bech32).map_err(|e| E::custom(e.to_string()))
    }
}

pub fn deserialize_address<'de, D>(deserializer: D) -> Result<Address, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(AddressVisitor)
}

pub fn serialize_address<S>(
    addr: &pallas_addresses::Address,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let bech = addr
        .to_bech32()
        .map_err(|e| S::Error::custom(e.to_string()))?;

    serializer.serialize_str(&bech)
}

pub fn serialize_ident<S>(ident: &Ident, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let bytes = ident.to_bytes();
    let hex = hex::encode(bytes);
    serializer.serialize_str(&hex)
}

pub fn serialize_value<S>(value: &Value, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let outer = &value.0;

    let mut map = serializer.serialize_map(None)?;

    for (policy, inner_map) in outer {
        if policy.is_empty() {
            for qty in inner_map.values() {
                map.serialize_entry("lovelace", qty)?;
            }
            continue;
        }

        let policy_hex = hex::encode(policy);

        for (token, qty) in inner_map {
            let token_hex = hex::encode(token);

            let key = format!("{}.{}", policy_hex, token_hex);

            map.serialize_entry(&key, qty)?;
        }
    }

    map.end()
}

pub fn serialize_multisig<S>(value: &Option<Multisig>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(ms) => match ms {
            Multisig::Signature(bytes) => serializer.serialize_str(&hex::encode(bytes)),

            Multisig::Script(bytes) => serializer.serialize_str(&hex::encode(bytes)),

            Multisig::AllOf(list) => serializer.serialize_some(list),
            Multisig::AnyOf(list) => serializer.serialize_some(list),

            Multisig::AtLeast(n, list) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("at_least", n)?;
                map.serialize_entry("members", list)?;
                map.end()
            }

            Multisig::Before(n) => serializer.serialize_newtype_variant("Multisig", 0, "before", n),

            Multisig::After(n) => serializer.serialize_newtype_variant("Multisig", 1, "after", n),
        },

        None => serializer.serialize_none(),
    }
}

pub fn serialize_assets<S>(
    assets: &(AssetClass, AssetClass),
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    fn encode(ac: &AssetClass) -> String {
        if ac.policy.is_empty() {
            return "lovelace".to_string();
        }
        let policy = hex::encode(&ac.policy);
        let name = hex::encode(&ac.token);
        format!("{}.{}", policy, name)
    }

    let arr = [encode(&assets.0), encode(&assets.1)];
    arr.serialize(serializer)
}

pub fn serialize_plutus_bigint<S>(v: &BigInt, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match v {
        BigInt::Int(x) => serializer.serialize_i128(i128::from(x.0)),

        BigInt::BigUInt(bytes) | BigInt::BigNInt(bytes) => {
            let neg = matches!(v, BigInt::BigNInt(_));

            if bytes.len() > 16 {
                return Err(serde::ser::Error::custom("BigInt out of i128 range"));
            }

            let mut buf = [0u8; 16];
            let buf_len = buf.len();
            buf[buf_len - bytes.len()..].copy_from_slice(bytes);

            let mut n = i128::from_be_bytes(buf);
            if neg {
                n = -n;
            }
            serializer.serialize_i128(n)
        }
    }
}
