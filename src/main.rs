use async_trait::async_trait;
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
use pallas_traverse::{MultiEraOutput, OutputRef};
use std::collections::{BTreeMap, HashSet};

mod acropolis;
mod multisig;
mod sundaev3;

use sundaev3::{Ident, PoolDatum};

use crate::acropolis::core::Process;
use crate::acropolis::indexer::{ChainIndexer, InMemoryCursorStore, ManagedIndex};

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

struct PoolIndex {
    // Pretend this is something persistent like a database.
    pools: BTreeMap<Ident, TransactionOutput>,
}

impl PoolIndex {
    fn new() -> Self {
        Self { pools: BTreeMap::new() }
    }
}

// Managed indexes are written in an "event handler" style.
// They react to a stream of events, starting at a configured point on the chain.
// Each index can be somewhere different on-chain, so they should be granular.
#[async_trait]
impl ManagedIndex for PoolIndex {
    fn name(&self) -> String {
        "pools".into()
    }

    async fn handle_onchain_tx(&mut self, info: &acropolis::core::BlockInfo, tx: &pallas_traverse::MultiEraTx) -> anyhow::Result<()> {
        for output in tx.outputs() {
            let p: TransactionOutput = convert_transaction_output(&output);
            let datum = match &p.datum {
                Datum::Data(d) => d,
                _ => { continue; }
            };
            if let Ok(pd) = minicbor::decode::<PoolDatum>(datum) {
                // In reality, this would probably be updating a DB
                self.pools.insert(pd.ident.clone(), p);
            }
        }
        // This method is fallible; if it fails, the indexer will stop updating this index
        Ok(())
    }

    async fn handle_rollback(&mut self, info: &acropolis::core::BlockInfo) -> anyhow::Result<()> {
        todo!()
    }
}

struct OrderIndex {
    // Pretend this is something persistent like a database.
    orders: BTreeMap<Ident, TransactionOutput>,
}

impl OrderIndex {
    fn new() -> Self {
        Self { orders: BTreeMap::new() }
    }
}

#[async_trait]
impl ManagedIndex for OrderIndex {
    fn name(&self) -> String {
        "orders".into()
    }

    async fn handle_onchain_tx(&mut self, info: &acropolis::core::BlockInfo, tx: &pallas_traverse::MultiEraTx) -> anyhow::Result<()> {
        Ok(())
    }

    async fn handle_rollback(&mut self, info: &acropolis::core::BlockInfo) -> anyhow::Result<()> {
        todo!()
    }
}

struct WalletIndex {
    address: Address,
    utxos: Vec<(OutputRef, Value)>,
}
impl WalletIndex {
    fn new(address: Address) -> Self {
        Self {
            address,
            utxos: vec![],
        }
    }
}

#[async_trait]
impl ManagedIndex for WalletIndex {
    fn name(&self) -> String {
        "wallet".into()
    }

    async fn handle_onchain_tx(&mut self, info: &acropolis::core::BlockInfo, tx: &pallas_traverse::MultiEraTx) -> anyhow::Result<()> {
        let spent = tx.inputs().iter().map(|i| i.output_ref()).collect::<HashSet<_>>();
        self.utxos.retain(|u| !spent.contains(&u.0));
        for (out_idx, output) in tx.outputs().iter().enumerate() {
            if output.address().is_ok_and(|a| a == self.address) {
                let ref_ = OutputRef::new(tx.hash(), out_idx as u64);
                self.utxos.push((ref_, convert_value(output.value())));
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let handle = tokio::spawn(async move {
        let mut indexer = ChainIndexer::new(InMemoryCursorStore::new(vec![]));
        let point = match args.command {
            Commands::SyncFromOrigin => Point::Origin,
            Commands::SyncFromPoint{ slot, block_hash } => Point::Specific(slot, block_hash.0)
        };
        indexer.add_index(PoolIndex::new(), point.clone(), false);
        indexer.add_index(OrderIndex::new(), point.clone(), false);
        
        let mut process = Process::create();
        process.register(indexer);
        process.run().await.unwrap();
    });
    let _ = handle.await;
}
