use pallas_addresses::Address;
use serde::{Deserializer, de};

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
