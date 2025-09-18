use minicbor::decode::{Decode, Decoder, Error};
use num_bigint::BigUint;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Multisig {
    Signature(Vec<u8>),
    AllOf(Vec<Box<Multisig>>),
    AnyOf(Vec<Box<Multisig>>),
    AtLeast(i128, Vec<Box<Multisig>>),
    Before(i128),
    After(i128),
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
                let mut sigs = vec![];
                let _ = decoder.array()?;
                let count = decoder.array()?;
                if let Some(n) = count {
                    for _i in 0..n {
                        let ms = decoder.decode()?;
                        sigs.push(ms);
                    }
                } else {
                    loop {
                        let ty = decoder.datatype()?;
                        if ty == minicbor::data::Type::Break {
                            decoder.skip()?;
                            break;
                        } else {
                            let ms = decoder.decode()?;
                            sigs.push(ms);
                        }
                    }
                }
                Ok(Multisig::AllOf(sigs))
            }
            123 => {
                let mut sigs = vec![];
                let _ = decoder.array()?;
                let count = decoder.array()?;
                if let Some(n) = count {
                    for _i in 0..n {
                        let ms = decoder.decode()?;
                        sigs.push(ms);
                    }
                } else {
                    loop {
                        let ty = decoder.datatype()?;
                        if ty == minicbor::data::Type::Break {
                            decoder.skip()?;
                            break;
                        } else {
                            let ms = decoder.decode()?;
                            sigs.push(ms);
                        }
                    }
                }
                Ok(Multisig::AnyOf(sigs))
            }
            124 => {
                let mut sigs = vec![];
                let _ = decoder.array()?;
                let count = decoder.array()?;
                let at_least = decoder.int()?;
                if let Some(n) = count {
                    for _i in 0..n {
                        let ms = decoder.decode()?;
                        sigs.push(ms);
                    }
                } else {
                    loop {
                        let ty = decoder.datatype()?;
                        if ty == minicbor::data::Type::Break {
                            decoder.skip()?;
                            break;
                        } else {
                            let ms = decoder.decode()?;
                            sigs.push(ms);
                        }
                    }
                }
                Ok(Multisig::AtLeast(at_least.into(), sigs))
            }
            125 => {
                let _ = decoder.array()?;
                let before = decoder.int()?;
                Ok(Multisig::Before(before.into()))
            }
            126 => {
                let _ = decoder.array()?;
                let before = decoder.int()?;
                Ok(Multisig::After(before.into()))
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
