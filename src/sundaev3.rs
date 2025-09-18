use hex::encode;
use minicbor::decode::{Decoder};
use minicbor::{Encode, Decode};
use num_bigint::BigInt;
use pallas_primitives::PlutusData;
use std::fmt;
use std::ops::Deref;

//use pallas_codec::utils::AnyCbor;
//use pallas_codec::minicbor;
//use pallas_codec::minicbor::{decode::{Decoder}, Decode};

use crate::multisig::Multisig;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Ident(Vec<u8>);

impl<'b, C> minicbor::decode::Decode<'b, C> for Ident {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        let b = decoder.bytes()?;
        Ok(Ident(b.to_vec()))
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

#[derive(Clone, Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub enum PoolRedeemer {
    PoolScoop(PoolScoop),
    Manage,
}

#[derive(Debug, PartialEq)]
pub struct SSEBytes(Vec<u8>);

// When constructing a pool scoop redeemer we don't construct SSEs because they will be
// retrieved from a database. So it's better to represent them here as raw bytes.
#[derive(Debug, PartialEq)]
pub struct PoolScoop {
    signatory_index: BigInt,
    scooper_index: BigInt,
    input_order: Vec<(BigInt, Option<SSEBytes>, BigInt)>,
}

#[derive(Debug, PartialEq)]
pub struct SignedStrategyExecution {
    execution: StrategyExecution,
    signature: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq)]
pub enum StrategyAuthorization {
    Signature(Vec<u8>),
    Script(Vec<u8>),
}

#[derive(Debug, PartialEq)]
pub struct SingletonValue {
    asset_class: AssetClass,
    quantity: i128,
}

#[derive(Debug, PartialEq)]
pub enum Order {
    Strategy(StrategyAuthorization),
    Swap(SingletonValue, SingletonValue),
    Deposit((SingletonValue, SingletonValue)),
    Withdrawal(SingletonValue),
    Donation((SingletonValue, SingletonValue)),
    Record(AssetClass),
}

impl <'b, C> minicbor::decode::Decode<'b, C> for StakePointer {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        todo!()
    }
}

impl <'b, C, T> minicbor::decode::Decode<'b, C> for Referenced<T> 
    where T: minicbor::decode::Decode<'b, ()> {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        let tag = decoder.tag()?;
        match tag.as_u64() {
            121 => {
                with_array(decoder, |d| {
                    let x: T = d.decode()?;
                    return Ok(Referenced::Inline(x))
                })
            },
            122 => {
                with_array(decoder, |d| {
                    let ptr = d.decode()?;
                    return Ok(Referenced::Pointer(ptr))
                })
            },
            _ => todo!()
        }
    }
}



impl <'b, C> minicbor::decode::Decode<'b, C> for Credential {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        let tag = decoder.tag()?;
        match tag.as_u64() {
            121 => {
                with_array(decoder, |d| {
                    let bytes: minicbor::bytes::ByteVec = d.decode()?;
                    return Ok(Credential::VerificationKey(bytes.to_vec()))
                })
            },
            122 => {
                with_array(decoder, |d| {
                    let bytes: minicbor::bytes::ByteVec = d.decode()?;
                    return Ok(Credential::Script(bytes.to_vec()))
                })
            },
            _ => todo!()
        }
    }
}



impl <'b, C> minicbor::decode::Decode<'b, C> for PlutusAddress {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        let tag = decoder.tag()?;
        match tag.as_u64() {
            121 => {
                with_array(decoder, |d| {
                    let payment_credential = d.decode()?;
                    let stake_credential: PlutusOption<StakeCredential> = d.decode()?;
                    return Ok(PlutusAddress {
                        payment_credential,
                        stake_credential: plutus_option_to_option(stake_credential),
                    })
                })
            },
            _ => todo!()
        }
    }
}

fn with_array<'b, T, F>(decoder: &mut Decoder<'b>, mut f: F) -> Result<T, minicbor::decode::Error> 
    where F: FnMut(&mut Decoder<'b>) -> Result<T, minicbor::decode::Error>
{
    match decoder.array()? {
        Some(_n) => {
            let result = f(decoder);
            result
        },
        None => {
            let result = f(decoder);
            let _ = decoder.skip()?;
            result
        }
    }
}

impl<'b, C> minicbor::decode::Decode<'b, C> for StrategyAuthorization {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        let tag = decoder.tag()?;
        match tag.as_u64() {
            121 => {
                with_array(decoder, |d| {
                    let sig = d.decode()?;
                    return Ok(StrategyAuthorization::Signature(sig));
                })
            }
            122 => {
                with_array(decoder, |d| {
                    let script = d.decode()?;
                    return Ok(StrategyAuthorization::Script(script));
                })
            }
            _ => todo!()
        }
    }
}

impl<'b, C> minicbor::decode::Decode<'b, C> for SingletonValue {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        with_array(decoder, |d| {
            let policy = d.bytes()?;
            let token = d.bytes()?;
            let quantity = d.int()?;
            Ok(SingletonValue {
                asset_class: AssetClass {
                    policy: policy.to_vec(),
                    token: token.to_vec(),
                },
                quantity: i128::from(quantity), 
            })
        })
    }
}


// TODO: Use AsPlutus
// TODO: This code is imprecise because it uses skip assuming that a list break is present
impl<'b, C> minicbor::decode::Decode<'b, C> for Order {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        let tag = decoder.tag()?;
        match tag.as_u64() {
            121 => {
                with_array(decoder, |d| {
                    let auth = d.decode()?;
                    return Ok(Order::Strategy(auth));
                })
            },
            122 => {
                with_array(decoder, |d| {
                    let give = d.decode()?;
                    let take = d.decode()?;
                    return Ok(Order::Swap(give, take));
                })
            },
            123 => {
                with_array(decoder, |d| {
                    let assets = d.decode()?;
                    return Ok(Order::Deposit(assets))
                })
            },
            124 => {
                with_array(decoder, |d| {
                    let amount = d.decode()?;
                    return Ok(Order::Withdrawal(amount))
                })
            },
            125 => {
                with_array(decoder, |d| {
                    let assets = d.decode()?;
                    return Ok(Order::Donation(assets))
                })
            },
            126 => {
                with_array(decoder, |d| {
                    let asset = with_array(d, |d2| {
                        let policy = d2.decode()?;
                        let token = d2.decode()?;
                        Ok(AssetClass {
                            policy,
                            token
                        })
                    })?;
                    return Ok(Order::Record(asset))
                })
            },

            _ => todo!()
        }
    }
}
 
#[derive(Debug, PartialEq)]
pub struct OrderDatum {
    pub ident: Option<Ident>,
    pub owner: Multisig,
    pub scoop_fee: i128,
    pub destination: Destination,
    pub action: Order,
    pub extra: AnyCbor,
}

#[derive(Debug, PartialEq)]
pub enum Destination {
    Fixed(FixedDestination),
    SelfDestination,
}

#[derive(Debug, PartialEq)]
pub enum AikenDatum {
    NoDatum,
    DatumHash(Vec<u8>),
    InlineDatum(Vec<u8>),
}

#[derive(Debug, PartialEq)]
pub struct FixedDestination {
    pub address: PlutusAddress,
    pub datum: AikenDatum,
}

impl<'b, C> minicbor::decode::Decode<'b, C> for FixedDestination {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        with_array(decoder, |d| {
            let address = d.decode()?;
            let datum = d.decode()?;
            return Ok(FixedDestination {
                address,
                datum,
            })
        })
    }
}

impl<'b, C> minicbor::decode::Decode<'b, C> for AikenDatum {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        match decoder.tag()?.as_u64() {
            121 => {
                let _ = decoder.skip()?;
                Ok(AikenDatum::NoDatum)
            }
            122 => {
                let _ = decoder.array()?;
                let dh = decoder.bytes()?;
                Ok(AikenDatum::DatumHash(dh.to_vec()))
            }
            123 => {
                let _ = decoder.array()?;
                let d = decoder.bytes()?;
                Ok(AikenDatum::InlineDatum(d.to_vec()))
            }
            _ => {
                Err(minicbor::decode::Error::message("wrong tag for destination"))
            }
        }
    }
}



impl<'b, C> minicbor::decode::Decode<'b, C> for Destination {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        match decoder.tag()?.as_u64() {
            121 => {
                let fixed = decoder.decode()?;
                Ok(Destination::Fixed(fixed))
            }
            122 => {
                let _ = decoder.skip()?;
                Ok(Destination::SelfDestination)
            }
            _ => {
                Err(minicbor::decode::Error::message("wrong tag for destination"))
            }
        }
    }
}



// AnyCbor copied from pallas_codec
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct AnyCbor {
    inner: Vec<u8>,
}

impl AnyCbor {
    pub fn raw_bytes(&self) -> &[u8] {
        &self.inner
    }

    pub fn unwrap(self) -> Vec<u8> {
        self.inner
    }

    pub fn from_encode<T>(other: T) -> Self
    where
        T: Encode<()>,
    {
        let inner = minicbor::to_vec(other).unwrap();
        Self { inner }
    }

    pub fn into_decode<T>(self) -> Result<T, minicbor::decode::Error>
    where
        for<'b> T: Decode<'b, ()>,
    {
        minicbor::decode(&self.inner)
    }
}

impl Deref for AnyCbor {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'b, C> minicbor::Decode<'b, C> for AnyCbor {
    fn decode(
        d: &mut minicbor::Decoder<'b>,
        _ctx: &mut C,
    ) -> Result<Self, minicbor::decode::Error> {
        let all = d.input();
        let start = d.position();
        d.skip()?;
        let end = d.position();

        Ok(Self {
            inner: Vec::from(&all[start..end]),
        })
    }
}

// TODO: Use AsPlutus
// TODO: This code is imprecise because it uses skip assuming that a list break is present
impl<'b, C> minicbor::decode::Decode<'b, C> for OrderDatum {
    fn decode(decoder: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        let tag = decoder.tag()?;
        match tag.as_u64() {
            121 => {
                let _ = decoder.array()?;
                let ident = decoder.decode()?;
                let owner = decoder.decode()?;
                let scoop_fee = decoder.int()?;
                let destination = decoder.decode()?;
                let action = decoder.decode()?;
                let extra = decoder.decode()?;
                let _break = decoder.skip()?;
                Ok(OrderDatum {
                    ident: plutus_option_to_option(ident),
                    owner,
                    scoop_fee: i128::from(scoop_fee),
                    destination,
                    action,
                    extra,
                })
            },
            x => {
                let m = format!("wrong wrapper tag {} for OrderDatum", x);
                return Err(minicbor::decode::Error::message(m));
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct PlutusAddress {
    pub payment_credential: PaymentCredential,
    pub stake_credential: Option<StakeCredential>,
}

#[derive(Debug, PartialEq)]
pub enum Credential {
    VerificationKey(VerificationKeyHash),
    Script(ScriptHash),
}

type VerificationKeyHash = Vec<u8>;
type ScriptHash = Vec<u8>;

#[derive(Debug, PartialEq)]
pub enum Referenced<T> {
    Inline(T),
    Pointer(StakePointer),
}

type PaymentCredential = Credential;
type StakeCredential = Referenced<Credential>;

#[derive(Debug, PartialEq)]
pub struct StakePointer {
    pub slot_number: i128,
    pub transaction_index: i128,
    pub certificate_index: i128,
}

#[derive(Debug, PartialEq)]
pub struct OutputReference {
    transaction_id: Vec<u8>,
    transaction_ix: u64,
}

#[derive(Debug, PartialEq)]
pub enum ValidityBound {
    NegativeInfinity,
    Finite(BigInt),
    PositiveInfinity,
}

#[derive(Debug, PartialEq)]
pub struct ValidityRange {
    validity_range_lower_bound: ValidityBound,
    validity_range_upper_bound: ValidityBound,
}

#[derive(Debug, PartialEq)]
pub struct StrategyExecution {
    tx_ref: OutputReference,
    validity_range: ValidityRange,
    details: Order,
    extensions: PlutusData,
}

#[derive(Debug, PartialEq)]
pub enum PoolMintRedeemer {
    MintLP(Ident),
    CreatePool(CreatePool),
    BurnPool(Ident),
}

#[derive(Debug, PartialEq)]
pub struct CreatePool {
    assets: (AssetClass, AssetClass),
    pool_output: BigInt,
    metadata_output: BigInt,
}

#[derive(Debug, PartialEq)]
pub enum ManageRedeemer {
    WithdrawFees(WithdrawFees),
    UpdatePoolFees(BigInt),
}

#[derive(Debug, PartialEq)]
pub struct WithdrawFees {
    amount: BigInt,
    treasury_output: BigInt,
    pool_input: BigInt,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_orderdatum() {
        let od_bytes = hex::decode("d8799fd8799f581c99999999999999999999999999999999999999999999999999999999ffd8799f581c88888888888888888888888888888888888888888888888888888888ff0ad8799fd8799fd8799f581c77777777777777777777777777777777777777777777777777777777ffd87a80ffd87980ffd87a9f9f4100410102ff9f4103410405ffffd87980ff").unwrap();
        let od: OrderDatum = minicbor::decode(&od_bytes).unwrap();
        let expected_ident = hex::decode("99999999999999999999999999999999999999999999999999999999").unwrap();
        let expected_signature = hex::decode("88888888888888888888888888888888888888888888888888888888").unwrap();
        let expected_vkey = hex::decode("77777777777777777777777777777777777777777777777777777777").unwrap();
        assert_eq!(od.ident.unwrap().0, expected_ident);
        assert_eq!(od.owner, Multisig::Signature(expected_signature));
        assert_eq!(od.scoop_fee, 10);
        assert_eq!(od.destination, Destination::Fixed(FixedDestination {
            address: PlutusAddress {
                payment_credential: Credential::VerificationKey(expected_vkey),
                stake_credential: None,
            },
            datum: AikenDatum::NoDatum,
        }));
        assert_eq!(od.action, Order::Swap(
            SingletonValue {
                asset_class: AssetClass {
                    policy: vec![0],
                    token: vec![1],
                },
                quantity: 2,
            },
            SingletonValue {
                asset_class: AssetClass {
                    policy: vec![3],
                    token: vec![4],
                },
                quantity: 5,
            }
        ));
        assert_eq!(od.extra, AnyCbor { inner: vec![0xd8, 0x79, 0x80] });
    }

    #[test]
    fn test_decode_orderdatum_2() {
        let od_bytes = hex::decode("d8799fd8799f581c12d88c7f234493742d583c219101050b39e925d715a93060752d60d3ffd8799f581c621be66c7f488b22f66003fff0b7427c30f70da678c532b7233d85caff1a00138800d8799fd8799fd8799f581c1c1381a51312b9da9782b3f507af94bab78780f85196007fad5fbde3ffd8799fd8799fd8799f581c621be66c7f488b22f66003fff0b7427c30f70da678c532b7233d85caffffffffd8799fffffd87a9f9f581cac597ca62a32cab3f4766c8f9cd577e50ebb1d00383ec7fa3990b01646435241574a551a0002113eff9f40401a066b2bc2ffff43d87980ff").unwrap();
        let od: OrderDatum = minicbor::decode(&od_bytes).unwrap();
        let expected_ident = hex::decode("12d88c7f234493742d583c219101050b39e925d715a93060752d60d3").unwrap();
        assert_eq!(od.ident.unwrap().0, expected_ident);
    }

    #[test]
    fn test_decode_pooldatum() {
        let pd_bytes = hex::decode("d8799f581cba228444515fbefd2c8725338e49589f206c7f18a33e002b157aac3c9f9f4040ff9f581c99b071ce8580d6a3a11b4902145adb8bfd0d2a03935af8cf66403e1546534245525259ffff1a01c9c3801901f41901f4d8799fd87f9f581ce8dc0595c8d3a7e2c0323a11f5519c32d3b3fb7a994519e38b698b5dffff001a003d0900ff").unwrap();
        let pd: PoolDatum = minicbor::decode(&pd_bytes).unwrap();
        let expected_ident = hex::decode("ba228444515fbefd2c8725338e49589f206c7f18a33e002b157aac3c").unwrap();
        assert_eq!(pd.ident.0, expected_ident);
    }
}
