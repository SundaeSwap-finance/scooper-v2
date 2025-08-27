use clap::Parser;
use pallas_network::facades::PeerClient;
use pallas_primitives::conway::{PseudoDatumOption};
use pallas_primitives::TransactionInput;
use pallas_network::miniprotocols::Point;
use pallas_network::miniprotocols::chainsync::{HeaderContent, NextResponse};
use pallas_traverse::{MultiEraOutput};
use std::collections::BTreeMap;

mod multisig;
mod sundaev3;

use sundaev3::{Ident, PoolDatum};


#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    addr: String,

    #[arg(short, long)]
    magic: u64,
}

// Hard to make lifetimes work here...

//#[derive(Clone)]
//struct UTxOWithDatum<'b, T> {
//    input: TransactionInput,
//    output: MultiEraOutput<'b>,
//    datum: T,
//}
//
//fn get_utxo_datum<'b, 'd, T>(output: MultiEraOutput<'b>) -> Option<T> where T: minicbor::Decode<'d, ()> {
//    let d = output.datum();
//    match d {
//        Some(PseudoDatumOption::Data(bytes)) => {
//            let datum: Result<T, _> = minicbor::decode(bytes.raw_cbor());
//            match datum {
//                Ok(datum) => Some(datum),
//                Err(_) => None,
//            }
//        }
//        _ => None
//    }
//}
//
//// TODO: Check tx witnesses for datum
//fn make_utxo_with_datum<'b, T>(input: TransactionInput, output: MultiEraOutput<'b>) -> Option<UTxOWithDatum<'b, T>> where T: minicbor::Decode<'b, ()> + Clone {
//    let d: Option<T> = get_utxo_datum(output.clone());
//    match d {
//        Some(d) => {
//            Some(UTxOWithDatum {
//                input,
//                output: output,
//                datum: d.clone(),
//            })
//        }
//        _ => None
//    }
//}

struct SundaeV3Index {
    //pools: BTreeMap<Ident, UTxOWithDatum<'b, PoolDatum>>,
    pools: BTreeMap<(), ()>,
    //orders: BTreeMap<Ident, Vec<UTxOWithDatum<'b, ()>>>,
    orders: BTreeMap<(), ()>,
}

fn decode_header_point(header_content: &HeaderContent) -> Result<Point, pallas_traverse::Error> {
    let header =
        pallas_traverse::MultiEraHeader::decode(
            header_content.variant,
            header_content.byron_prefix.map(|x| x.0),
            &header_content.cbor
        );
    header.map(|h| {
        let slot = h.slot();
        let header_hash = h.hash();
        Point::Specific(slot, header_hash.to_vec())
    })
}

fn handle_block(index: &mut SundaeV3Index, block: pallas_traverse::MultiEraBlock) {
    for body in block.txs() {
        let this_tx_hash = body.hash();
        for (ix, output) in body.outputs().iter().enumerate() {
            println!("Produced {:?}", output);
            let this_input = TransactionInput {
                transaction_id: this_tx_hash,
                index: ix as u64,
            };
            //let p: Option<UTxOWithDatum<PoolDatum>> =
            //    make_utxo_with_datum::<PoolDatum>(
            //        this_input,
            //        output.clone(),
            //    );
            //match p {
            //    Some(pool_utxo) => {
            //        index.pools.insert(pool_utxo.datum.ident.clone(), pool_utxo.clone());
            //    }
            //    _ => {}
            //}
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let handle = tokio::spawn(async move {
        let mut peer_client = PeerClient::connect(args.addr, args.magic).await.unwrap();
        let points = vec![
            Point::Specific(
                88547961,
                hex::decode("f5b08a0a1334f0a8c7fd0978ef7a8d44161962c90b3ac4f9267955e2d06a42fc").unwrap()
            )
        ];
        let intersect_result = peer_client.chainsync().find_intersect(points).await;
        println!("Intersect result {:?}", intersect_result);
        let mut index = SundaeV3Index {
            pools: BTreeMap::new(),
            orders: BTreeMap::new(),
        };
        loop {
            let resp = peer_client.chainsync().request_or_await_next().await.unwrap();
            match resp {
                NextResponse::RollForward(content, _tip) => {
                    let point = decode_header_point(&content).unwrap();
                    let resp = peer_client.blockfetch().fetch_single(point).await.unwrap();
                    match pallas_traverse::MultiEraBlock::decode(&resp) {
                        Ok(block) => handle_block(&mut index, block),
                        Err(e) => println!("Error decoding block: {:?}", e),
                    }
                },
                NextResponse::RollBackward(point, tip) => {
                    println!("RollBackward({:?}, {:?})", point, tip);
                },
                NextResponse::Await => {
                    println!("Await");
                }
            }
        }
    });
    let _ = handle.await;
}

// Might be helpful to define our own more ergonomic types for transaction outputs, to avoid
// lifetime issues:
//
//struct DatumOption {
//    Hash(DatumHash),
//    Data(PlutusData),
//}
//
//enum ScriptRef {
//    NativeScript(NativeScript),
//    PlutusV1Script(PlutusScript<1>),
//    PlutusV2Script(PlutusScript<2>),
//}
//
//struct TransactionOutput {
//    address: Address,
//    value: alonzo::Value,
//    datum_option: Option<DatumOption>,
//    script_ref: ScriptRef,
//}
//
//fn convert_transaction_output(output: MultiEraOutput<'b>) -> Result<TransactionOutput, String> {
//    let a = output.address()?;
//    let v = output.value()?;
//    TransactionOutput {
//         address: output.address().clone(),
//         value: convert_multi_era_value(v),
//
//}
