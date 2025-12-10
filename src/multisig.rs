use plutus_parser::AsPlutus;

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
