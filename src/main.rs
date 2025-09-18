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

mod multisig;
mod sundaev3;

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
    orders: BTreeMap<Ident, (TransactionInput, TransactionOutput)>,
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
    for (ident, _p) in &index.pools {
        println!("  {}", ident);
        for (o_ident, o) in &index.orders {
            if ident == o_ident {
                println!("    {:?}", o.0);
            }
        }
    }
}

fn handle_block(index: &mut SundaeV3Index, block: pallas_traverse::MultiEraBlock) {
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
                    let pd: Result<PoolDatum, _> = minicbor::decode(inline);
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
                    let od: Result<OrderDatum, _> = minicbor::decode(inline);
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
                    println!("raw block: {}", hex::encode(&resp));
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
    use pallas_traverse::MultiEraBlock;
    use pallas_primitives::babbage::{HeaderBody, MintedHeaderBody, MintedHeader, PseudoHeader};
    use pallas_primitives::conway::{MintedBlock};
    use pallas_primitives::{KeyValuePairs, MaybeIndefArray};

    fn make_block<'b>() -> MultiEraBlock<'b> {
        todo!()
    }

    #[test]
    fn test_do_stuff() {
        let mut index = SundaeV3Index {
            pools: BTreeMap::new(),
            orders: BTreeMap::new(),
        };
        let block = make_block();
            //.add_mint_pool_tx();
            //.add_list_order_tx();
        handle_block(&mut index, block);
        assert_eq!(index.pools.len(), 1);
    }
}
