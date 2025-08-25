use clap::Parser;
use hex;
use pallas_codec::minicbor;
use pallas_crypto::hash::{Hash, Hasher};
use pallas_network::facades::PeerClient;
use pallas_network::miniprotocols::Point;
use pallas_network::miniprotocols::chainsync::{HeaderContent, NextResponse};
use pallas_primitives::alonzo;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    addr: String,

    #[arg(short, long)]
    magic: u64,
}

const HEADER_HASH_SIZE: usize = 32;
fn hash_header_content(header_content: &HeaderContent) -> Hash<HEADER_HASH_SIZE> {
    Hasher::<{ HEADER_HASH_SIZE * 8 }>::hash(&header_content.cbor)
}

fn decode_header_point(header_content: &HeaderContent) -> Result<Point, String> {
    let as_alonzo_header: Result<alonzo::Header, minicbor::decode::Error> =
        minicbor::decode(&header_content.cbor);
    if let Ok(alonzo_header) = as_alonzo_header {
        let slot = alonzo_header.header_body.slot;
        let header_hash = hash_header_content(header_content);
        return Ok(Point::Specific(slot, header_hash.to_vec()));
    }
    return Err("cannot decode header point".to_owned());
}

type BlockWrapper = (u16, alonzo::Block);

fn decode_alonzo_block(content: &[u8]) -> Option<alonzo::Block> {
    let as_alonzo_block: Result<BlockWrapper, minicbor::decode::Error> =
        minicbor::decode(content);
    match as_alonzo_block {
        Ok(b) => Some(b.1),
        Err(_) => None,
    }
}

fn handle_alonzo_block(block: alonzo::Block) {
    for body in block.transaction_bodies {
        for input in body.inputs {
            println!("Spent {:?}", input);
        }
        for output in body.outputs {
            println!("Produced {:?}", output);
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let handle = tokio::spawn(async move {
        let mut peer_client = PeerClient::connect(args.addr, args.magic).await.unwrap();
        let intersect_result = peer_client.chainsync().intersect_origin().await;
        println!("Intersect result {:?}", intersect_result);
        loop {
            let resp = peer_client.chainsync().request_or_await_next().await.unwrap();
            match resp {
                NextResponse::RollForward(content, tip) => {
                    let point = decode_header_point(&content).unwrap();
                    let resp = peer_client.blockfetch().fetch_single(point).await.unwrap();
                    if let Some(alonzo_block) = decode_alonzo_block(&resp) {
                        handle_alonzo_block(alonzo_block);
                    } else {
                        println!("RollForward({}, {:?})", hex::encode(resp), tip);
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
