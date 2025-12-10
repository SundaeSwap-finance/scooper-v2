use pallas_primitives::BigInt;
use plutus_parser::AsPlutus;

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
