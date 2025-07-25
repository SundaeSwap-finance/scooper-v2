use clap::Parser;
use pallas_network::facades::PeerClient;
use pallas_network::miniprotocols::{PROTOCOL_N2N_CHAIN_SYNC};
use pallas_network::multiplexer;
use pallas_network::miniprotocols::chainsync;
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    name: String,

    #[arg(short, long, default_value_t = 1)]
    count: u8,

    #[arg(short, long)]
    addr: String,

    #[arg(short, long)]
    magic: u64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    for _ in 0..args.count {
        println!("Hello {}!", args.name);
    }

    let handle = tokio::spawn(async move {
        let mut peer_client = PeerClient::connect(args.addr, args.magic).await.unwrap();
        let chainsync = peer_client.chainsync();
        let intersect_result = chainsync.intersect_origin().await;
        println!("Intersect result {:?}", intersect_result);
    });
    handle.await;
}
