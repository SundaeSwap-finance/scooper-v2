use acropolis_common::messages::Message;
use acropolis_module_block_unpacker::BlockUnpacker;
use acropolis_module_custom_indexer::CustomIndexer;
use acropolis_module_genesis_bootstrapper::GenesisBootstrapper;
use acropolis_module_mithril_snapshot_fetcher::MithrilSnapshotFetcher;
use acropolis_module_peer_network_interface::PeerNetworkInterface;
use anyhow::Result;
use caryatid_process::Process;
use caryatid_sdk::module_registry::ModuleRegistry;
use clap::Parser;
use tokio::select;
use tokio::signal::ctrl_c;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use std::process;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

mod bigint;
mod cardano_types;
mod config;
mod datum_lookup;
mod historical_state;
mod instrumentation;
mod multisig;
mod persistence;
mod scooper;
mod server;
mod sundaev3;

use crate::persistence::Persistence;
use crate::scooper::Scooper;
use crate::sundaev3::{SundaeV3HistoricalState, SundaeV3Indexer, SundaeV3Protocol, SundaeV3Update};

#[derive(clap::Parser, Clone, Debug)]
struct Args {
    #[arg(short, long)]
    config: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = config::load_config(&args.config)?;
    instrumentation::init(&config.log)?;
    info!("Started scooper");

    let (resync_tx, _) = tokio::sync::broadcast::channel(1);
    let shutdown = CancellationToken::new();

    let persistence = persistence::connect(&config.persistence).await?;

    let index = Arc::new(Mutex::new(SundaeV3HistoricalState::new()));
    let broadcaster = tokio::sync::watch::Sender::default();

    let manager_handle = tokio::spawn(manager_loop(
        index.clone(),
        resync_tx.clone(),
        broadcaster.clone(),
        config.acropolis_config()?,
        config.protocol.v3.clone(),
        persistence.clone(),
        shutdown.child_token(),
    ));
    let scooper_handle = tokio::spawn(
        Scooper::new(config.log.trace_directory.clone(), broadcaster.subscribe())?
            .run(shutdown.child_token()),
    );
    let server_handle = tokio::spawn(server::admin_server(
        config.server.clone(),
        index.clone(),
        resync_tx,
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

    tokio::try_join!(manager_handle, scooper_handle, server_handle)?;
    Ok(())
}

async fn manager_loop(
    index: Arc<Mutex<SundaeV3HistoricalState>>,
    resync_tx: tokio::sync::broadcast::Sender<()>,
    broadcaster: tokio::sync::watch::Sender<SundaeV3Update>,
    config: Arc<::config::Config>,
    protocol: SundaeV3Protocol,
    persistence: Arc<dyn Persistence>,
    shutdown: CancellationToken,
) {
    let mut force_restart = false;
    loop {
        let index = index.clone();
        let mut resync_tx = resync_tx.subscribe();
        let config = config.clone();
        let protocol = protocol.clone();
        let default_start = protocol.starting_point.clone();
        let broadcaster = broadcaster.clone();

        let mut process = Process::<Message>::create(config).await;
        GenesisBootstrapper::register(&mut process);
        MithrilSnapshotFetcher::register(&mut process);
        BlockUnpacker::register(&mut process);
        PeerNetworkInterface::register(&mut process);

        let indexer = Arc::new(CustomIndexer::new(persistence.cursor_store()));
        process.register(indexer.clone());

        let mut v3_index = SundaeV3Indexer::new(
            index,
            broadcaster,
            protocol,
            config::ROLLBACK_LIMIT,
            persistence.indexer_dao("sundae_v3"),
        );
        v3_index.load().await.unwrap();

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
