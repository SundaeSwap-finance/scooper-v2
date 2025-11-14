use clap::Parser;
use pallas_addresses::Address;
use pallas_network::facades::PeerClient;
use pallas_network::miniprotocols::chainsync::{HeaderContent, NextResponse};
use pallas_network::miniprotocols::Point;
use pallas_primitives::conway::{
    DatumOption, NativeScript,
};
use pallas_primitives::{KeepRaw, PlutusData, PlutusScript, TransactionInput};
use pallas_traverse::MultiEraOutput;
use std::collections::{BTreeMap, HashSet};

mod multisig;
mod sundaev3;

use plutus_parser::AsPlutus;
use sundaev3::{Ident, OrderDatum, PoolDatum};

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

fn convert_datum<'b>(datum: Option<DatumOption>) -> Datum {
    match datum {
        None => Datum::None,
        Some(DatumOption::Hash(h)) => Datum::Hash(h.to_vec()),
        Some(DatumOption::Data(d)) => Datum::Data(d.unwrap().raw_cbor().to_vec()),
    }
}

fn convert_value<'b>(value: pallas_traverse::MultiEraValue<'b>) -> Value {
    let mut result = BTreeMap::new();
    let mut ada_policy = BTreeMap::new();
    ada_policy.insert(vec![], value.coin().into());
    result.insert(vec![], ada_policy);
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

fn convert_script_ref(script_ref: pallas_primitives::conway::ScriptRef) -> ScriptRef {
    match script_ref {
        pallas_primitives::conway::ScriptRef::NativeScript(n) => ScriptRef::NativeScript(n.unwrap()),
        pallas_primitives::conway::ScriptRef::PlutusV1Script(s) => ScriptRef::PlutusV1Script(s),
        pallas_primitives::conway::ScriptRef::PlutusV2Script(s) => ScriptRef::PlutusV2Script(s),
        pallas_primitives::conway::ScriptRef::PlutusV3Script(s) => ScriptRef::PlutusV3Script(s),
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
    orders: BTreeMap<Option<Ident>, (TransactionInput, TransactionOutput)>,
}

fn decode_header_point(header_content: &HeaderContent) -> Result<Point, pallas_traverse::Error> {
    let header = pallas_traverse::MultiEraHeader::decode(
        header_content.variant,
        header_content.byron_prefix.map(|x| x.0),
        &header_content.cbor,
    );
    header.map(|h| {
        let slot = h.slot();
        let header_hash = h.hash();
        Point::Specific(slot, header_hash.to_vec())
    })
}

fn summarize_protocol_state(index: &SundaeV3Index) {
    println!("Known pools:");
    let mut known_pool_ids = HashSet::new();
    for (ident, _p) in &index.pools {
        known_pool_ids.insert(ident);
        println!("  {:?}", ident);
        for (o_ident, o) in &index.orders {
            if Some(ident) == o_ident.as_ref() {
                println!("    {:?}", o.0);
            }
        }
    }

    println!("Orphan orders:");
    for (o_ident, o) in &index.orders {
        if let Some(oi) = o_ident {
            if !known_pool_ids.contains(oi) {
                println!("  {:?}", o.0);
            }
        } else {
            println!("  {:?}", o.0);
        }
    }
}

fn handle_block(index: &mut SundaeV3Index, block: pallas_traverse::MultiEraBlock) {
    if block.number() % 1000 == 0 {
        println!("Block height: {}", block.number());
    }
    for tx in block.txs() {
        let this_tx_hash = tx.hash();
        for (ix, output) in tx.outputs().iter().enumerate() {
            let this_input = TransactionInput {
                transaction_id: this_tx_hash,
                index: ix as u64,
            };
            // TODO: Don't need to convert every single utxo, we can inspect the address
            // and datum first to decide if we are interested
            // TODO: Get datums from the witness set to support hash datums
            let p: TransactionOutput = convert_transaction_output(&output);
            match p.datum {
                Datum::Data(ref inline) => {
                    let plutus_data: PlutusData = minicbor::decode(inline).unwrap();
                    let pd: Result<PoolDatum, _> = AsPlutus::from_plutus(plutus_data);
                    match pd {
                        Ok(pd) => {
                            println!("{}#{}: pool with datum {}",
                                hex::encode(this_tx_hash),
                                ix,
                                hex::encode(inline),
                            );
                            index.pools.insert(pd.ident.clone(), p);
                            summarize_protocol_state(index);
                            return;
                        }
                        _ => {}
                    }
                    let plutus_data: PlutusData = minicbor::decode(inline).unwrap();
                    let od: Result<OrderDatum, _> = AsPlutus::from_plutus(plutus_data);
                    match od {
                        Ok(od) => {
                            println!("{}#{}: order with datum {}",
                                hex::encode(this_tx_hash),
                                ix,
                                hex::encode(inline),
                            );
                            index.orders.insert(od.ident.clone(), (this_input, p));
                            summarize_protocol_state(index);
                            return;
                        }
                        _ => {}
                    }
                }
                Datum::None | Datum::Hash(_) => {}
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let handle = tokio::spawn(async move {
        let mut peer_client = PeerClient::connect(args.addr, args.magic).await.unwrap();
        let points = match args.command {
            Commands::SyncFromOrigin => vec![Point::Origin],
            Commands::SyncFromPoint{ slot, block_hash } => vec![Point::Specific(slot, block_hash.0)]
        };
        let intersect_result = peer_client.chainsync().find_intersect(points).await;
        println!("Intersect result {:?}", intersect_result);
        let mut index = SundaeV3Index {
            pools: BTreeMap::new(),
            orders: BTreeMap::new(),
        };
        loop {
            let resp = peer_client
                .chainsync()
                .request_or_await_next()
                .await
                .unwrap();
            match resp {
                NextResponse::RollForward(content, _tip) => {
                    let point = decode_header_point(&content).unwrap();
                    let resp = peer_client.blockfetch().fetch_single(point).await.unwrap();
                    match pallas_traverse::MultiEraBlock::decode(&resp) {
                        Ok(block) => handle_block(&mut index, block),
                        Err(e) => println!("Error decoding block: {:?}", e),
                    }
                }
                NextResponse::RollBackward(point, tip) => {
                    println!("RollBackward({:?}, {:?})", point, tip);
                }
                NextResponse::Await => {
                    println!("Await");
                }
            }
        }
    });
    let _ = handle.await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingest_block() {
        let mut index = SundaeV3Index {
            pools: BTreeMap::new(),
            orders: BTreeMap::new(),
        };
        let block_bytes = std::fs::read("testdata/scoop-pool.block").unwrap();
        let block = pallas_traverse::MultiEraBlock::decode(&block_bytes).unwrap();
        let ada_policy: Vec<u8> = vec![];
        let ada_token: Vec<u8> = vec![];
        let pool_policy: Vec<u8> = vec![68, 161, 235, 45, 159, 88, 173, 212, 235, 25, 50, 189, 0, 72, 230, 161, 148, 126, 133, 227, 254, 79, 50, 149, 106, 17, 4, 20];
        let pool_token: Vec<u8> = vec![0, 13, 225, 64, 50, 196, 63, 9, 111, 160, 86, 38, 218, 30, 173, 147, 131, 121, 60, 205, 123, 186, 106, 27, 37, 158, 119, 89, 119, 102, 174, 232];
        let coin_b_policy: Vec<u8> = vec![145, 212, 243, 130, 39, 63, 68, 47, 21, 233, 218, 72, 203, 35, 52, 155, 162, 117, 248, 129, 142, 76, 122, 197, 209, 0, 74, 22];
        let coin_b_token: Vec<u8> = vec![77, 121, 85, 83, 68];
        handle_block(&mut index, block);
        assert_eq!(index.pools.len(), 1);
        let first_pool = index.pools.first_entry().unwrap();
        let pool_value = &first_pool.get().value.0;
        assert_eq!(pool_value[&ada_policy][&ada_token], 6181255175);
        assert_eq!(pool_value[&pool_policy][&pool_token], 1);
        assert_eq!(pool_value[&coin_b_policy][&coin_b_token], 6397550387);
        assert_eq!(index.orders.len(), 0);
    }
}
