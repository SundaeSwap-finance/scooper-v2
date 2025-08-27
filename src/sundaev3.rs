use hex::encode;
use minicbor::decode::{Decoder};
use minicbor::Decode;
use num_bigint::BigInt;
use pallas_primitives::PlutusData;
use std::fmt;

use crate::multisig::Multisig;

#[derive(Clone, Decode, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ident(#[n(0)] Vec<u8>);

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

#[derive(Clone)]
pub struct AssetClass {
    policy: Vec<u8>,
    token: Vec<u8>,
}

// TODO: Use AsPlutus
// TODO: This code is imprecise because it uses skip assuming that a list break is present
impl<'b, C> minicbor::decode::Decode<'b, C> for AssetClass {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        let _ = decoder.array()?;
        let policy = decoder.bytes()?;
        let token = decoder.bytes()?;
        let _break = decoder.skip()?;
        Ok(AssetClass {
            policy: policy.to_vec(),
            token: token.to_vec(),
        })
    }
}

#[derive(Clone)]
pub struct PoolDatum {
    pub ident: Ident,
    pub assets: (AssetClass, AssetClass),
    pub circulating_lp: i128,
    pub bid_fees_per_10_thousand: i128,
    pub ask_fees_per_10_thousand: i128,
    pub fee_manager: Option<Multisig>,
    pub market_open: i128,
    pub protocol_fees: i128,
}

enum PlutusOption<T> {
    PlutusNone,
    PlutusSome(T),
}

fn plutus_option_to_option<T>(p: PlutusOption<T>) -> Option<T> {
    match p {
        PlutusOption::PlutusNone => None,
        PlutusOption::PlutusSome(x) => Some(x),
    }
}

// TODO: Use AsPlutus
// TODO: This code is imprecise because it uses skip assuming that a list break is present
impl<'b, C, T: Decode<'b, C>> minicbor::decode::Decode<'b, C> for PlutusOption<T> {
    fn decode(decoder: &mut Decoder<'b>, ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        let tag = decoder.tag()?;
        match tag.as_u64() {
            121 => {
                let _ = decoder.array()?;
                let content: T = T::decode(decoder, ctx)?;
                let _break = decoder.skip()?;
                Ok(PlutusOption::PlutusSome(content))
            }
            122 => {
                match decoder.array()? {
                    Some(0) => Ok(PlutusOption::PlutusNone),
                    None => Ok(PlutusOption::PlutusNone),
                    _ => Err(minicbor::decode::Error::message(format!("misformed plutus option None"))),
                }
            }
            _ => Err(minicbor::decode::Error::message(format!("misformed plutus option (wrong tag)")))
        }
    }
}

// TODO: Use AsPlutus
// TODO: This code is imprecise because it uses skip assuming that a list break is present
impl<'b, C> minicbor::decode::Decode<'b, C> for PoolDatum {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        let tag = decoder.tag()?;
        match tag.as_u64() {
            121 => {
                let _ = decoder.array()?;
                let ident = decoder.bytes()?;
                let _ = decoder.array()?;
                let asset_a = decoder.decode()?;
                let asset_b = decoder.decode()?;
                let _break = decoder.skip()?;
                let circulating_lp = decoder.int()?;
                let bid_fees_per_10_thousand = decoder.int()?;
                let ask_fees_per_10_thousand = decoder.int()?;
                let fee_manager = decoder.decode()?;
                let market_open = decoder.int()?;
                let protocol_fees = decoder.int()?;
                let _break = decoder.skip()?;
                Ok(PoolDatum {
                    ident: Ident(ident.to_vec()),
                    assets: (asset_a, asset_b),
                    circulating_lp: i128::from(circulating_lp),
                    bid_fees_per_10_thousand: i128::from(bid_fees_per_10_thousand),
                    ask_fees_per_10_thousand: i128::from(ask_fees_per_10_thousand),
                    fee_manager: plutus_option_to_option(fee_manager),
                    market_open: i128::from(market_open),
                    protocol_fees: i128::from(protocol_fees),
                })
            },
            x => {
                let m = format!("wrong wrapper tag {} for PoolDatum", x);
                return Err(minicbor::decode::Error::message(m));
            }
        }
    }
}

pub enum PoolRedeemer {
    PoolScoop(PoolScoop),
    Manage,
}

pub struct SSEBytes(Vec<u8>);

// When constructing a pool scoop redeemer we don't construct SSEs because they will be
// retrieved from a database. So it's better to represent them here as raw bytes.
pub struct PoolScoop {
    signatory_index: BigInt,
    scooper_index: BigInt,
    input_order: Vec<(BigInt, Option<SSEBytes>, BigInt)>,
}

pub struct SignedStrategyExecution {
    execution: StrategyExecution,
    signature: Option<Vec<u8>>,
}

pub enum StrategyAuthorization {
    Signature(Vec<u8>),
    Script(Vec<u8>),
}

pub struct SingletonValue {
    asset_class: AssetClass,
    quantity: BigInt,
}

pub enum Order {
    Strategy(StrategyAuthorization),
    Swap(SingletonValue, SingletonValue),
    Deposit((SingletonValue, SingletonValue)),
    Withdrawal(SingletonValue),
    Donation((SingletonValue, SingletonValue)),
    Record(AssetClass),
}

pub struct OutputReference {
    transaction_id: Vec<u8>,
    transaction_ix: u64,
}

pub enum ValidityBound {
    NegativeInfinity,
    Finite(BigInt),
    PositiveInfinity,
}

pub struct ValidityRange {
    validity_range_lower_bound: ValidityBound,
    validity_range_upper_bound: ValidityBound,
}

pub struct StrategyExecution {
    tx_ref: OutputReference,
    validity_range: ValidityRange,
    details: Order,
    extensions: PlutusData,
}

pub enum PoolMintRedeemer {
    MintLP(Ident),
    CreatePool(CreatePool),
    BurnPool(Ident),
}

pub struct CreatePool {
    assets: (AssetClass, AssetClass),
    pool_output: BigInt,
    metadata_output: BigInt,
}

pub enum ManageRedeemer {
    WithdrawFees(WithdrawFees),
    UpdatePoolFees(BigInt),
}

pub struct WithdrawFees {
    amount: BigInt,
    treasury_output: BigInt,
    pool_input: BigInt,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_pooldatum() {
        let pd_bytes = hex::decode("D8799F581CBA228444515FBEFD2C8725338E49589F206C7F18A33E002B157AAC3C9F9F4040FF9F581C99B071CE8580D6A3A11B4902145ADB8BFD0D2A03935AF8CF66403E1546534245525259FFFF1A01C9C3801901F41901F4D8799FD87F9F581CE8DC0595C8D3A7E2C0323A11F5519C32D3B3FB7A994519E38B698B5DFFFF001A003D0900FF").unwrap();
        let pd: PoolDatum = minicbor::decode(&pd_bytes).unwrap();
        let expected_ident = hex::decode("ba228444515fbefd2c8725338e49589f206c7f18a33e002b157aac3c").unwrap();
        assert_eq!(pd.ident.0, expected_ident);
    }
}
