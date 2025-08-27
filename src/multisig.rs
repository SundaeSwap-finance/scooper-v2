use minicbor::decode::{Decode, Decoder, Error};
use num_bigint::BigUint;

#[derive(Clone)]
pub enum Multisig {
    Signature(Vec<u8>),
    AllOf(Vec<Box<Multisig>>),
    AnyOf(Vec<Box<Multisig>>),
    AtLeast(BigUint, Vec<Box<Multisig>>),
    Before(BigUint),
    After(BigUint),
    Script(Vec<u8>),
}

impl<'b, C> Decode<'b, C> for Multisig {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, Error> {
        let tag = decoder.tag()?;
        match tag.as_u64() {
            121 => {
                let _ = decoder.array()?;
                let sig = decoder.bytes()?;
                let _break = decoder.skip()?;
                Ok(Multisig::Signature(sig.to_vec()))
            }
            122 => {
                todo!();
            }
            127 => {
                let _ = decoder.array()?;
                let script = decoder.bytes()?;
                let _break = decoder.skip()?;
                Ok(Multisig::Script(script.to_vec()))
            }
            _ => todo!(),
        }
    }
}
