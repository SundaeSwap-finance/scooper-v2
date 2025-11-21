use anyhow::{Result, bail};
use clap::Parser;
use pallas_network::facades::PeerClient;
use pallas_network::miniprotocols::Point;
use pallas_network::miniprotocols::chainsync::{HeaderContent, NextResponse};
use pallas_primitives::PlutusData;

use std::collections::{BTreeMap, HashSet};
use tracing::{Level, event};

use std::sync::Arc;
use std::sync::Mutex;

mod cardano_types;
mod multisig;
mod sundaev3;

use cardano_types::{Datum, TransactionInput, TransactionOutput};
use plutus_parser::AsPlutus;
use sundaev3::{Ident, OrderDatum, PoolDatum, SundaeV3Pool};

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::{Request, Response, body::Incoming as IncomingBody};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::net::TcpListener;

#[derive(clap::Parser, Clone, Debug)]
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

fn parse_block_hash(bh: &str) -> Result<BlockHash> {
    let bytes = hex::decode(bh)?;
    if bytes.len() == BLOCK_HASH_SIZE {
        Ok(BlockHash(bytes.to_vec()))
    } else {
        bail!(
            "Expected length {} for block hash, but got {}",
            BLOCK_HASH_SIZE,
            bytes.len()
        )
    }
}

#[derive(clap::Subcommand, Clone, Debug)]
enum Commands {
    SyncFromOrigin,
    SyncFromPoint {
        #[arg(short, long)]
        slot: u64,

        #[arg(short, long, value_parser=parse_block_hash)]
        block_hash: BlockHash,
    },
}

struct SundaeV3Index {
    pools: BTreeMap<Ident, SundaeV3Pool>,
    orders: BTreeMap<Option<Ident>, Vec<(TransactionInput, TransactionOutput)>>,
}

fn decode_header_point(header_content: &HeaderContent) -> Result<Point> {
    let header = pallas_traverse::MultiEraHeader::decode(
        header_content.variant,
        header_content.byron_prefix.map(|x| x.0),
        &header_content.cbor,
    )?;
    let slot = header.slot();
    let header_hash = header.hash();
    Ok(Point::Specific(slot, header_hash.to_vec()))
}

fn summarize_protocol_state(index: &SundaeV3Index) {
    println!("Known pools:");
    let mut known_pool_ids = HashSet::new();
    for (ident, p) in &index.pools {
        let pool_policy = match &p.address {
            pallas_addresses::Address::Shelley(a) => a.payment().as_hash(),
            _ => continue,
        };
        known_pool_ids.insert(ident);
        println!("  Pool ID: {}", ident);
        println!(
            "  Assets: ({}, {})",
            p.pool_datum.assets.0, p.pool_datum.assets.1,
        );
        if let Some(price) = sundaev3::get_pool_price(pool_policy.as_ref(), &p.value) {
            println!("  Price: {price}");
        } else {
            println!("  Price: N/A");
        }
        let i = Some(ident);
        let this_pool_orders = index.orders.get(&i.cloned());
        match this_pool_orders {
            Some(orders) => {
                if orders.is_empty() {
                    println!("    No orders");
                } else {
                    for o in orders {
                        println!("    Order: {}", o.0);
                    }
                }
            }
            None => {
                println!("    No orders");
            }
        }
    }

    println!("Orphan orders:");
    let orphan_orders = index.orders.get(&None);
    match orphan_orders {
        Some(orders) => {
            for o in orders {
                println!("  {}: {:?}", o.0, 0.1);
            }
        }
        None => {
            println!("  None");
        }
    }

    println!();
}

fn handle_block(index: &mut SundaeV3Index, block: pallas_traverse::MultiEraBlock) {
    if block.number().is_multiple_of(1000) {
        event!(Level::INFO, "Block height: {}", block.number());
    }
    for tx in block.txs() {
        let this_tx_hash = tx.hash();
        for (ix, output) in tx.outputs().iter().enumerate() {
            let this_input = TransactionInput(pallas_primitives::TransactionInput {
                transaction_id: this_tx_hash,
                index: ix as u64,
            });
            // TODO: Don't need to convert every single utxo, we can inspect the address
            // and datum first to decide if we are interested
            // TODO: Get datums from the witness set to support hash datums
            let tx_out: TransactionOutput = cardano_types::convert_transaction_output(output);
            match tx_out.datum {
                Datum::Data(ref inline) => {
                    let plutus_data: PlutusData = minicbor::decode(inline).unwrap();
                    let pd: Result<PoolDatum, _> = AsPlutus::from_plutus(plutus_data);
                    if let Ok(pd) = pd {
                        let pool_id = pd.ident.clone();
                        let pool_record = SundaeV3Pool {
                            address: tx_out.address,
                            value: tx_out.value,
                            pool_datum: pd,
                        };
                        index.pools.insert(pool_id, pool_record);

                        event!(Level::DEBUG, "{}", hex::encode(this_tx_hash));
                        summarize_protocol_state(index);
                        return;
                    }

                    let plutus_data: PlutusData = minicbor::decode(inline).unwrap();
                    let od: Result<OrderDatum, _> = AsPlutus::from_plutus(plutus_data);
                    if let Ok(od) = od {
                        let this_pool_orders = index.orders.entry(od.ident.clone()).or_default();
                        this_pool_orders.push((this_input, tx_out));

                        event!(Level::DEBUG, "{}", hex::encode(this_tx_hash));
                        summarize_protocol_state(index);
                        return;
                    }
                }
                Datum::None | Datum::Hash(_) => {}
            }
        }
    }
}

async fn do_scoops(
    args: Args,
    mut abort: tokio::sync::broadcast::Receiver<()>,
    index: Arc<Mutex<SundaeV3Index>>,
) {
    let mut peer_client = PeerClient::connect(args.addr, args.magic).await.unwrap();
    let points = match args.command {
        Commands::SyncFromOrigin => vec![Point::Origin],
        Commands::SyncFromPoint { slot, block_hash } => vec![Point::Specific(slot, block_hash.0)],
    };
    let intersect_result = peer_client.chainsync().find_intersect(points).await;
    event!(Level::DEBUG, "Intersect result {:?}", intersect_result);
    loop {
        if let Ok(()) = abort.try_recv() {
            return;
        }
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
                    Ok(block) => {
                        let mut index_lock = index.lock().unwrap();
                        handle_block(&mut index_lock, block)
                    }
                    Err(e) => event!(Level::DEBUG, "Error decoding block: {:?}", e),
                }
            }
            NextResponse::RollBackward(point, tip) => {
                event!(Level::DEBUG, "RollBackward({:?}, {:?})", point, tip);
            }
            NextResponse::Await => {
                event!(Level::DEBUG, "Await");
            }
        }
    }
}

struct AdminServer {
    index: Arc<Mutex<SundaeV3Index>>,
    kill_tx: tokio::sync::broadcast::Sender<()>,
}

impl hyper::service::Service<Request<IncomingBody>> for AdminServer {
    type Response = Response<Full<Bytes>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<IncomingBody>) -> Self::Future {
        let mk_response =
            |s: String| Ok(Response::builder().body(Full::new(Bytes::from(s))).unwrap());

        if let Some(pool_id) = req.uri().path().strip_prefix("/pool/") {
            let index_lock = self.index.lock().unwrap();
            let id_bytes = hex::decode(pool_id).unwrap();
            let ident = Ident::new(&id_bytes);
            if let Some(orders) = index_lock.orders.get(&Some(ident)) {
                let mut response = String::new();
                for (tx_in, _) in orders {
                    response += &format!("{tx_in}\n");
                }
                return Box::pin(async move { mk_response(response) });
            } else {
                return Box::pin(async move { mk_response("No such pool".into()) });
            }
        }

        let res = match req.uri().path() {
            "/resync-from-kupo" => {
                let _ = self.kill_tx.send(());
                mk_response("resync".into())
            }
            "/health" => mk_response("health".into()),
            "/pools" => {
                let mut response = String::new();
                let index_lock = self.index.lock().unwrap();
                for pool_id in index_lock.pools.keys() {
                    response += &format!("{pool_id}\n");
                }
                mk_response(response)
            }
            _ => mk_response("unknown".into()),
        };
        Box::pin(async { res })
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let (kill_tx, _) = tokio::sync::broadcast::channel(1);
    let kill_tx2 = kill_tx.clone();

    let index = Arc::new(Mutex::new(SundaeV3Index {
        pools: BTreeMap::new(),
        orders: BTreeMap::new(),
    }));
    let index2 = index.clone();

    // Manage restarting of the main scoop task in case we want to resync
    let manager_handle = tokio::spawn(async move {
        loop {
            let kill_rx2 = kill_tx.subscribe();
            let args2 = args.clone();
            let index2 = index.clone();
            index.lock().unwrap().pools.clear();
            index.lock().unwrap().orders.clear();
            let do_scoops_handle = tokio::spawn(async move {
                event!(Level::DEBUG, "Doing scoops");
                let _ = do_scoops(args2, kill_rx2, index2).await;
            });
            do_scoops_handle.await.unwrap();
        }
    });

    // HTTP server for admin controls:
    //   - running a health check
    //   - triggering a resync from kupo
    let admin_server_handle = tokio::spawn(async move {
        let addr = SocketAddr::from(([127, 0, 0, 1], 9999));
        let listener = TcpListener::bind(addr).await.unwrap();
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);
            let kill_tx = kill_tx2.clone();
            let index = index2.clone();
            tokio::task::spawn(async {
                let admin_server = AdminServer { index, kill_tx };
                if let Err(err) = http1::Builder::new()
                    .serve_connection(io, admin_server)
                    .await
                {
                    event!(Level::DEBUG, "Failed to serve connection: {:?}", err);
                }
            });
        }
    });

    tokio::try_join!(manager_handle, admin_server_handle).unwrap();
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
        let pool_policy: Vec<u8> = vec![
            68, 161, 235, 45, 159, 88, 173, 212, 235, 25, 50, 189, 0, 72, 230, 161, 148, 126, 133,
            227, 254, 79, 50, 149, 106, 17, 4, 20,
        ];
        let pool_token: Vec<u8> = vec![
            0, 13, 225, 64, 50, 196, 63, 9, 111, 160, 86, 38, 218, 30, 173, 147, 131, 121, 60, 205,
            123, 186, 106, 27, 37, 158, 119, 89, 119, 102, 174, 232,
        ];
        let coin_b_policy: Vec<u8> = vec![
            145, 212, 243, 130, 39, 63, 68, 47, 21, 233, 218, 72, 203, 35, 52, 155, 162, 117, 248,
            129, 142, 76, 122, 197, 209, 0, 74, 22,
        ];
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
