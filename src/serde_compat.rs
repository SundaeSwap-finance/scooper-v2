use serde::{Serializer, ser::Error};

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
