use pallas_addresses::Address;
use pallas_primitives::Fragment;
use plutus_parser::BigInt;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserializer, Serializer, de, ser::Error};

use crate::cardano_types::{AssetClass, TransactionInput, Value};
use crate::multisig::Multisig;
use crate::sundaev3::{
    AikenDatum, AnyPlutusData, Credential, Destination, Ident, Order, Referenced, SingletonValue,
};

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

pub fn serialize_plutus_bigint<S>(v: &BigInt, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let val = bigint_to_i128(v).map_err(serde::ser::Error::custom)?;
    serializer.serialize_i128(val)
}

pub fn bigint_to_i128(v: &BigInt) -> Result<i128, &'static str> {
    match v {
        BigInt::Int(x) => Ok(i128::from(x.0)),

        BigInt::BigUInt(bytes) | BigInt::BigNInt(bytes) => {
            let neg = matches!(v, BigInt::BigNInt(_));

            if bytes.len() > 16 {
                return Err("BigInt out of i128 range");
            }

            let mut buf = [0u8; 16];
            let offset = 16 - bytes.len();
            buf[offset..].copy_from_slice(bytes);

            let mut n = i128::from_be_bytes(buf);
            if neg {
                n = -n;
            }

            Ok(n)
        }
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

impl serde::Serialize for AnyPlutusData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let cbor = self
            .raw()
            .encode_fragment()
            .map_err(serde::ser::Error::custom)?;

        serializer.serialize_str(&hex::encode(cbor))
    }
}

impl serde::Serialize for Order {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let (key, value) = match self {
            Order::Strategy(auth) => ("Strategy", serde_json::Value::String(format!("{:?}", auth))),

            Order::Swap(a, b) => (
                "Swap",
                serde_json::Value::Array(vec![
                    serde_json::to_value(a).map_err(serde::ser::Error::custom)?,
                    serde_json::to_value(b).map_err(serde::ser::Error::custom)?,
                ]),
            ),

            Order::Deposit((a, b)) => (
                "Deposit",
                serde_json::Value::Array(vec![
                    serde_json::to_value(a).map_err(serde::ser::Error::custom)?,
                    serde_json::to_value(b).map_err(serde::ser::Error::custom)?,
                ]),
            ),

            Order::Withdrawal(v) => (
                "Withdrawal",
                serde_json::to_value(v).map_err(serde::ser::Error::custom)?,
            ),

            Order::Donation((a, b)) => (
                "Donation",
                serde_json::Value::Array(vec![
                    serde_json::to_value(a).map_err(serde::ser::Error::custom)?,
                    serde_json::to_value(b).map_err(serde::ser::Error::custom)?,
                ]),
            ),

            Order::Record(ac) => (
                "Record",
                serde_json::to_value(ac).map_err(serde::ser::Error::custom)?,
            ),
        };

        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(key, &value)?;
        map.end()
    }
}

impl serde::Serialize for SingletonValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        // Convert BigInt → i128
        let amount_i128 =
            crate::serde_compat::bigint_to_i128(&self.amount).map_err(serde::ser::Error::custom)?;

        // Format asset key
        let key = if self.policy.is_empty() {
            "lovelace".to_string()
        } else {
            format!("{}.{}", hex::encode(&self.policy), hex::encode(&self.name))
        };

        // Emit as: { "<asset>": amount }
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&key, &amount_i128)?;
        map.end()
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

impl serde::Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let outer = &self.0;

        // We do not know the exact number of map entries, because:
        // lovelace occupies 1 entry, and each asset is one entry.
        let mut map = serializer.serialize_map(None)?;

        for (policy, inner) in outer {
            if policy.is_empty() {
                // lovelace case
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

impl serde::Serialize for Multisig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Multisig::Signature(bytes) => serializer.serialize_str(&hex::encode(bytes)),

            Multisig::Script(bytes) => serializer.serialize_str(&hex::encode(bytes)),

            Multisig::AllOf(list) => {
                let mut seq = serializer.serialize_seq(Some(list.len()))?;
                for item in list {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }

            Multisig::AnyOf(list) => {
                let mut seq = serializer.serialize_seq(Some(list.len()))?;
                for item in list {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }

            Multisig::AtLeast(n, list) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("at_least", n)?;
                map.serialize_entry("members", list)?;
                map.end()
            }

            Multisig::Before(slot) => {
                let n =
                    crate::serde_compat::bigint_to_i128(slot).map_err(serde::ser::Error::custom)?;
                serializer.serialize_str(&format!("before:{n}"))
            }

            Multisig::After(slot) => {
                let n =
                    crate::serde_compat::bigint_to_i128(slot).map_err(serde::ser::Error::custom)?;
                serializer.serialize_str(&format!("after:{n}"))
            }
        }
    }
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
                    AikenDatum::InlineDatum(v) => Some(hex::encode(v)),
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

impl serde::Serialize for TransactionInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let tx_id_hex = hex::encode(self.0.transaction_id.as_ref());
        let s = format!("{}:{}", tx_id_hex, self.0.index);
        serializer.serialize_str(&s)
    }
}
