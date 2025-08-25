use clap::Parser;
use pallas_network::facades::PeerClient;
use pallas_network::miniprotocols::Point;
use pallas_network::miniprotocols::chainsync::{HeaderContent, NextResponse};
use pallas_traverse;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    addr: String,

    #[arg(short, long)]
    magic: u64,
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

fn handle_block(block: pallas_traverse::MultiEraBlock) {
    for body in block.txs() {
        for input in body.inputs() {
            println!("Spent {:?}", input);
        }
        for output in body.outputs() {
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
                NextResponse::RollForward(content, _tip) => {
                    let point = decode_header_point(&content).unwrap();
                    let resp = peer_client.blockfetch().fetch_single(point).await.unwrap();
                    match pallas_traverse::MultiEraBlock::decode(&resp) {
                        Ok(block) => handle_block(block),
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
