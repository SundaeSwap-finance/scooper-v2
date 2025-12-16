use acropolis_common::messages::Message;
use acropolis_common::{BlockHash, Point};
use acropolis_module_block_unpacker::BlockUnpacker;
use acropolis_module_custom_indexer::CustomIndexer;
use acropolis_module_custom_indexer::cursor_store::InMemoryCursorStore;
use acropolis_module_genesis_bootstrapper::GenesisBootstrapper;
use acropolis_module_peer_network_interface::PeerNetworkInterface;
use anyhow::{Result, anyhow};
use caryatid_process::Process;
use caryatid_sdk::module_registry::ModuleRegistry;
use clap::Parser;
use config::{Config, File};
use tokio::select;
use tokio::signal::ctrl_c;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tracing::{Level, event, info, warn};

mod bigint;
mod cardano_types;
mod historical_state;
mod multisig;
mod scooper;
mod serde_compat;
mod sundaev3;

use serde::{Deserialize, Serialize};

use cardano_types::TransactionInput;
use sundaev3::{Ident, validate_order};

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::{Request, Response, body::Incoming as IncomingBody};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::net::{TcpListener, TcpStream};

use crate::scooper::Scooper;
use crate::sundaev3::{
    PoolError, SundaeV3HistoricalState, SundaeV3Indexer, SundaeV3Update, ValidationError,
};

#[derive(Clone, Deserialize)]
struct SundaeV3Protocol {
    #[serde(with = "hex")]
    order_script_hash: Vec<u8>,
    #[serde(with = "hex")]
    pool_script_hash: Vec<u8>,
}

#[derive(clap::Parser, Clone, Debug)]
struct Args {
    #[arg(short, long)]
    protocol: PathBuf,

    #[command(subcommand)]
    command: Commands,

    #[arg(long, value_name = "PATH", default_value = "scooper.toml")]
    config: String,
}

const BLOCK_HASH_SIZE: usize = 32;

fn parse_block_hash(bh: &str) -> Result<BlockHash> {
    let bytes = hex::decode(bh)?;
    BlockHash::try_from(bytes).map_err(|v| {
        anyhow!(
            "invalid block hash length: expected {BLOCK_HASH_SIZE} bytes, got {} bytes",
            v.len()
        )
    })
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

#[derive(Clone)]
struct AdminServer {
    index: Arc<Mutex<SundaeV3HistoricalState>>,
    resync_tx: tokio::sync::broadcast::Sender<()>,
    protocol: SundaeV3Protocol,
}

impl hyper::service::Service<Request<IncomingBody>> for AdminServer {
    type Response = Response<Full<Bytes>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<IncomingBody>) -> Self::Future {
        let me = self.clone();
        Box::pin(async move {
            let s = me.do_call(req).await;
            Ok(Response::builder().body(Full::new(Bytes::from(s))).unwrap())
        })
    }
}

#[derive(Serialize)]
struct QueryPoolResponse<'a> {
    valid: Vec<&'a TransactionInput>,
    out_of_range: Vec<OrderOutOfRange<'a>>,
    unrecoverable: Vec<OrderUnrecoverable<'a>>,
}

#[derive(Serialize)]
struct OrderOutOfRange<'a> {
    order: &'a TransactionInput,
    reason: (f64, f64),
}

#[derive(Serialize)]
struct OrderUnrecoverable<'a> {
    order: &'a TransactionInput,
    reason: String,
}

impl AdminServer {
    async fn do_call(&self, req: Request<IncomingBody>) -> String {
        if let Some(pool_id) = req.uri().path().strip_prefix("/pool/") {
            let state = self.index.lock().await.latest().into_owned();
            let id_bytes = hex::decode(pool_id).unwrap();
            let ident = Ident::new(&id_bytes);
            let pool = match state.pools.get(&ident).cloned() {
                Some(p) => p,
                None => {
                    return "No such pool".into();
                }
            };
            let mut response = QueryPoolResponse {
                valid: vec![],
                out_of_range: vec![],
                unrecoverable: vec![],
            };
            for order in &state.orders {
                if order.datum.ident.as_ref() != Some(&ident) {
                    continue;
                }
                if let Err(err) = validate_order(
                    &order.datum,
                    &order.output.value,
                    &pool.pool_datum,
                    &pool.value,
                    &self.protocol.pool_script_hash,
                ) {
                    if let ValidationError::PoolError(PoolError::OutOfRange {
                        swap_price,
                        pool_price,
                    }) = err
                    {
                        response.out_of_range.push(OrderOutOfRange {
                            order: &order.input,
                            reason: (swap_price, pool_price),
                        });
                    } else {
                        response.unrecoverable.push(OrderUnrecoverable {
                            order: &order.input,
                            reason: err.to_string(),
                        });
                    }
                } else {
                    response.valid.push(&order.input);
                }
            }
            return serde_json::to_string(&response).unwrap();
        }

        match req.uri().path() {
            "/resync-from-acropolis" => {
                let _ = self.resync_tx.send(());
                "resync".into()
            }
            "/health" => "health".into(),
            "/pools" => {
                let state = self.index.lock().await.latest().into_owned();
                let mut json_map = serde_json::Map::new();

                for (ident, pool) in state.pools {
                    json_map.insert(
                        hex::encode(ident.to_bytes()),
                        serde_json::to_value(pool).unwrap(),
                    );
                }

                serde_json::to_string_pretty(&json_map).unwrap()
            }
            "/orders" => {
                let state = self.index.lock().await.latest().into_owned();

                let mut json_map = serde_json::Map::new();
                for order in &state.orders {
                    let hex = match order.datum.ident.as_ref() {
                        Some(id) => hex::encode(id.to_bytes()),
                        None => "null".to_string(),
                    };

                    match serde_json::to_value(order) {
                        Ok(val) => {
                            json_map.insert(hex, val);
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to serialize order {:?}: {}",
                                order.datum.ident,
                                e
                            );
                            continue;
                        }
                    }
                }

                serde_json::to_string_pretty(&json_map).unwrap()
            }
            _ => "unknown".into(),
        }
    }
}

#[tokio::main]
#[allow(unreachable_code)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    event!(Level::INFO, "Started scooper");
    let args = Args::parse();
    let scooper_config_file = args.config;
    let protocol_config_file = args.protocol;
    let default_start = match args.command {
        Commands::SyncFromOrigin => Point::Origin,
        Commands::SyncFromPoint { slot, block_hash } => Point::Specific {
            slot,
            hash: block_hash,
        },
    };

    let (resync_tx, _) = tokio::sync::broadcast::channel(1);
    let shutdown = CancellationToken::new();

    let protocol: SundaeV3Protocol = {
        let f = std::fs::File::open(protocol_config_file)?;
        serde_json::from_reader(f)?
    };

    const ROLLBACK_LIMIT: usize = 2160;
    let index = Arc::new(Mutex::new(SundaeV3HistoricalState::new(ROLLBACK_LIMIT)));
    let broadcaster = tokio::sync::watch::Sender::default();

    let manager_handle = tokio::spawn(manager_loop(
        index.clone(),
        resync_tx.clone(),
        broadcaster.clone(),
        scooper_config_file,
        protocol.clone(),
        default_start,
        shutdown.child_token(),
    ));
    let scooper_handle = tokio::spawn(
        Scooper::new(broadcaster.subscribe(), &protocol.pool_script_hash)?
            .run(shutdown.child_token()),
    );
    let admin_handle = tokio::spawn(admin_server(
        index.clone(),
        resync_tx,
        protocol,
        shutdown.child_token(),
    ));

    tokio::spawn(async move {
        let _ = ctrl_c().await;
        info!("shutdown requested");
        shutdown.cancel();
        let _ = ctrl_c().await;
        warn!("force shutdown requested");
        process::exit(0);
    });

    tokio::try_join!(manager_handle, scooper_handle, admin_handle)?;
    Ok(())
}

async fn manager_loop(
    index: Arc<Mutex<SundaeV3HistoricalState>>,
    resync_tx: tokio::sync::broadcast::Sender<()>,
    broadcaster: tokio::sync::watch::Sender<SundaeV3Update>,
    scooper_config_file: String,
    protocol: SundaeV3Protocol,
    default_start: Point,
    shutdown: CancellationToken,
) {
    let mut force_restart = false;
    loop {
        let index = index.clone();
        let mut resync_tx = resync_tx.subscribe();
        let scooper_config_file = scooper_config_file.clone();
        let protocol = protocol.clone();
        let default_start = default_start.clone();
        let broadcaster = broadcaster.clone();

        let config = Arc::new(
            Config::builder()
                .add_source(File::with_name("config/acropolis"))
                .add_source(File::with_name(&scooper_config_file))
                .build()
                .unwrap(),
        );

        let mut process = Process::<Message>::create(config).await;
        GenesisBootstrapper::register(&mut process);
        BlockUnpacker::register(&mut process);
        PeerNetworkInterface::register(&mut process);

        let indexer = Arc::new(CustomIndexer::new(InMemoryCursorStore::new()));
        process.register(indexer.clone());

        let v3_index = SundaeV3Indexer::new(index, broadcaster, protocol);

        indexer
            .add_index(v3_index, default_start, force_restart)
            .await
            .unwrap();

        match process.start().await {
            Ok(running_process) => {
                let shutting_down = select! {
                    res = resync_tx.recv() => res.is_err(),
                    _ = shutdown.cancelled() => true,
                };
                force_restart = true;

                info!("terminating acropolis process");
                match running_process.stop().await {
                    Ok(()) => info!("terminated acropolis process"),
                    Err(err) => warn!("could not terminate acropolis process: {err:#}"),
                }
                if shutting_down {
                    break;
                }
            }
            Err(err) => {
                warn!("could not start acropolis process: {err:#}");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        };

        warn!("Restarting Scooper indexer");
    }
}

async fn admin_server(
    index: Arc<Mutex<SundaeV3HistoricalState>>,
    resync_tx: tokio::sync::broadcast::Sender<()>,
    protocol: SundaeV3Protocol,
    shutdown: CancellationToken,
) {
    let addr = SocketAddr::from(([127, 0, 0, 1], 9999));
    let listener = TcpListener::bind(addr).await.unwrap();

    loop {
        let stream = select! {
            res = listener.accept() => res.unwrap().0,
            _ = shutdown.cancelled() => { break; }
        };

        let resync_tx = resync_tx.clone();
        let index = index.clone();
        let protocol = protocol.clone();

        let child = shutdown.child_token();
        tokio::task::spawn(async move {
            select! {
                _ = child.cancelled() => {},
                _ = handle_request(stream, index, resync_tx, protocol) => {}
            }
        });
    }
}

async fn handle_request(
    stream: TcpStream,
    index: Arc<Mutex<SundaeV3HistoricalState>>,
    resync_tx: tokio::sync::broadcast::Sender<()>,
    protocol: SundaeV3Protocol,
) {
    let io = TokioIo::new(stream);

    let admin_server = AdminServer {
        index,
        resync_tx,
        protocol,
    };
    if let Err(err) = http1::Builder::new()
        .serve_connection(io, admin_server)
        .await
    {
        event!(Level::DEBUG, "Failed to serve connection: {:?}", err);
    }
}
