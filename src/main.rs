use clap::Parser;
use num_bigint::BigInt;
use pallas_addresses::Address;
use pallas_network::facades::PeerClient;
use pallas_network::miniprotocols::chainsync::{HeaderContent, NextResponse};
use pallas_network::miniprotocols::Point;
use pallas_primitives::conway::{
    DatumOption, MintedScriptRef, NativeScript, PseudoDatumOption, PseudoScript,
};
use pallas_primitives::{KeepRaw, PlutusData, PlutusScript, TransactionInput};
use pallas_traverse::MultiEraOutput;
use std::collections::BTreeMap;

mod acropolis;
mod multisig;
mod sundaev3;

use sundaev3::{Ident, PoolDatum};

use crate::acropolis::core::Process;
use crate::acropolis::indexer::{ChainIndexer, ManagedChainIndex, ManagedIndex};

#[derive(clap::Parser, Debug)]
struct Args {
    #[arg(short, long)]
    addr: String,

    #[arg(short, long)]
    magic: u64,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, Debug)]
struct BlockHash(Vec<u8>);

const BLOCK_HASH_SIZE: usize = 32;

fn parse_block_hash(bh: &str) -> Result<BlockHash, String> {
    let bytes = hex::decode(bh).map_err(|e| e.to_string())?;
    if bytes.len() == BLOCK_HASH_SIZE {
        Ok(BlockHash(bytes.to_vec()))
    } else {
        Err(format!("Expected length {} for block hash, but got {}", BLOCK_HASH_SIZE, bytes.len()))
    }
}

#[derive(clap::Subcommand, Debug)]
enum Commands {
    SyncFromOrigin,
    SyncFromPoint {
        #[arg(short, long)]
        slot: u64,

        #[arg(short, long, value_parser=parse_block_hash)]
        block_hash: BlockHash,
    }
}

// Custom UTxO types
type Bytes = Vec<u8>;

enum ScriptRef {
    NativeScript(NativeScript),
    PlutusV1Script(PlutusScript<1>),
    PlutusV2Script(PlutusScript<2>),
    PlutusV3Script(PlutusScript<3>),
}

struct Value(BTreeMap<Bytes, BTreeMap<Bytes, i128>>);

enum Datum {
    None,
    Hash(Bytes),
    Data(Bytes),
}

// Would be convenient to parameterize this by the type of the decoded datum, with
// an 'Any' type that always succeeds at decoding and functions
//   TransactionOutput<T> -> TransactionOutput<Any>
//   TransactionOutput<Any> -> Result<TransactionOutput<T>, Error> where T: minicbor::Decode
struct TransactionOutput {
    address: Address,
    value: Value,
    datum: Datum,
    script_ref: Option<ScriptRef>,
}

fn convert_datum<'b>(datum: Option<PseudoDatumOption<KeepRaw<'_, PlutusData>>>) -> Datum {
    match datum {
        None => Datum::None,
        Some(PseudoDatumOption::Hash(h)) => Datum::Hash(h.to_vec()),
        Some(PseudoDatumOption::Data(d)) => Datum::Data(d.unwrap().raw_cbor().to_vec()),
    }
}

fn convert_value<'b>(value: pallas_traverse::MultiEraValue<'b>) -> Value {
    let mut result = BTreeMap::new();
    value.coin();
    for policy in value.assets() {
        let mut p_map = BTreeMap::new();
        let pol = policy.policy();
        for asset in policy.assets() {
            let tok = asset.name();
            p_map.insert(tok.to_vec(), asset.any_coin());
        }
        result.insert(pol.to_vec(), p_map);
    }
    Value(result)
}

fn convert_script_ref(script_ref: MintedScriptRef) -> ScriptRef {
    match script_ref {
        PseudoScript::NativeScript(n) => ScriptRef::NativeScript(n.unwrap()),
        PseudoScript::PlutusV1Script(s) => ScriptRef::PlutusV1Script(s),
        PseudoScript::PlutusV2Script(s) => ScriptRef::PlutusV2Script(s),
        PseudoScript::PlutusV3Script(s) => ScriptRef::PlutusV3Script(s),
    }
}

fn convert_transaction_output<'b>(output: &MultiEraOutput<'b>) -> TransactionOutput {
    let address = output.address().unwrap();
    let datum = convert_datum(output.datum());
    let value = convert_value(output.value());
    let script_ref = output.script_ref().map(convert_script_ref);
    TransactionOutput {
        address,
        datum,
        value,
        script_ref,
    }
}

struct SundaeV3Index {
    pools: BTreeMap<Ident, TransactionOutput>,
    orders: BTreeMap<Ident, TransactionOutput>,
}

#[derive(Default)]
struct PoolIndex {
    pools: BTreeMap<Ident, TransactionOutput>,
}
impl ManagedIndex for PoolIndex {
    fn name(&self) -> String {
        "pools".into()
    }
}
impl ManagedChainIndex for PoolIndex {
    async fn handle_onchain_tx(&mut self, info: &acropolis::core::BlockInfo, tx: &pallas_traverse::MultiEraTx<'_>) -> anyhow::Result<()> {
        for output in tx.outputs() {
            let p: TransactionOutput = convert_transaction_output(&output);
            let datum = match &p.datum {
                Datum::Data(d) => d,
                _ => { continue; }
            };
            if let Ok(pd) = minicbor::decode::<PoolDatum>(datum) {
                self.pools.insert(pd.ident.clone(), p);
            }
        }
        Ok(())
    }

    async fn handle_rollback(&mut self, info: &acropolis::core::BlockInfo) -> anyhow::Result<()> {
        todo!()
    }
}

#[derive(Default)]
struct OrderIndex {
    orders: BTreeMap<Ident, TransactionOutput>,
}
impl ManagedIndex for OrderIndex {
    fn name(&self) -> String {
        "orders".into()
    }
}
impl ManagedChainIndex for OrderIndex {
    async fn handle_onchain_tx(&mut self, info: &acropolis::core::BlockInfo, tx: &pallas_traverse::MultiEraTx<'_>) -> anyhow::Result<()> {
        todo!()
    }

    async fn handle_rollback(&mut self, info: &acropolis::core::BlockInfo) -> anyhow::Result<()> {
        todo!()
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let handle = tokio::spawn(async move {
        let mut indexer = ChainIndexer::new();
        let point = match args.command {
            Commands::SyncFromOrigin => Point::Origin,
            Commands::SyncFromPoint{ slot, block_hash } => Point::Specific(slot, block_hash.0)
        };
        indexer.add_index(PoolIndex::default(), point.clone(), false);
        indexer.add_index(OrderIndex::default(), point.clone(), false);
        
        let mut process = Process::create();
        process.register(indexer);
        process.run().await.unwrap();
    });
    let _ = handle.await;
}
