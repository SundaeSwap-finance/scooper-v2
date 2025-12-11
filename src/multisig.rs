use plutus_parser::AsPlutus;
use serde::{
    Serializer,
    ser::{SerializeMap, SerializeSeq},
};

use crate::bigint::BigInt;

#[derive(AsPlutus, Clone, Debug, PartialEq, Eq)]
pub enum Multisig {
    Signature(Vec<u8>),
    AllOf(Vec<Multisig>),
    AnyOf(Vec<Multisig>),
    AtLeast(BigInt, Vec<Multisig>),
    Before(BigInt),
    After(BigInt),
    Script(Vec<u8>),
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

            Multisig::Before(slot) => serializer.serialize_str(&format!("before:{slot}")),

            Multisig::After(slot) => serializer.serialize_str(&format!("after:{slot}")),
        }
    }
}
