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
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tracing::{info, warn};

mod bigint;
mod blueprint;
mod bootstrap;
mod cardano_types;
mod config;
mod datum_lookup;
mod events;
mod historical_state;
mod instrumentation;
mod multisig;
mod persistence;
mod scooper;
mod server;
mod sundaev3;
mod sundaev4;

use crate::config::ProtocolConfig;
use crate::events::IndexEvent;
use crate::persistence::Persistence;
use crate::scooper::Scooper;
use crate::sundaev3::{SundaeV3HistoricalState, SundaeV3Indexer, SundaeV3Update};
use crate::sundaev4::{SundaeV4HistoricalState, SundaeV4Indexer};

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
    let (event_tx, _event_rx) = tokio::sync::broadcast::channel::<(u64, Vec<IndexEvent>)>(256);
    let shutdown = CancellationToken::new();

    let persistence = persistence::connect(&config.persistence).await?;

    let v3_state = config
        .protocol
        .v3
        .as_ref()
        .map(|_| Arc::new(Mutex::new(SundaeV3HistoricalState::new())));
    let v4_state = config
        .protocol
        .v4
        .as_ref()
        .map(|_| Arc::new(Mutex::new(SundaeV4HistoricalState::new())));
    let broadcaster = tokio::sync::watch::Sender::default();

    // Resolve the secret key file early so both manager_loop (bootstrap) and
    // scooper see the resolved key.
    let mut protocol = config.protocol.clone();
    if let Some(ref mut v4) = protocol.v4 {
        if let Some(ref mut exec) = v4.execution {
            exec.resolve_secret_key()
                .expect("failed to resolve scooper secret key");
        }
    }
    let v4_execution = protocol
        .v4
        .as_ref()
        .and_then(|v4| v4.execution.clone());
    let manager_handle = tokio::spawn(manager_loop(
        v3_state.clone(),
        v4_state.clone(),
        resync_tx.clone(),
        broadcaster.clone(),
        event_tx.clone(),
        config.acropolis_config()?,
        protocol,
        persistence.clone(),
        shutdown.child_token(),
    ));
    let v4_fee = v4_execution.as_ref().map(|e| e.fee);
    let v4_module_preimages = v4_execution
        .as_ref()
        .map(|e| server::compute_module_state_preimages(e.fee, e.protocol_share))
        .unwrap_or_default();
    let paused = Arc::new(AtomicBool::new(false));
    let scooper_handle = tokio::spawn(
        Scooper::new(
            config.log.trace_directory.clone(),
            event_tx.subscribe(),
            v3_state.clone(),
            v4_state.clone(),
            v4_execution,
            paused.clone(),
        )?
        .run(shutdown.child_token()),
    );
    let server_handle = tokio::spawn(server::admin_server(
        config.server.clone(),
        v3_state.clone(),
        v4_state.clone(),
        v4_fee,
        v4_module_preimages,
        resync_tx,
        event_tx.clone(),
        paused.clone(),
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
    v3_state: Option<Arc<Mutex<SundaeV3HistoricalState>>>,
    v4_state: Option<Arc<Mutex<SundaeV4HistoricalState>>>,
    resync_tx: tokio::sync::broadcast::Sender<()>,
    broadcaster: tokio::sync::watch::Sender<SundaeV3Update>,
    event_tx: tokio::sync::broadcast::Sender<(u64, Vec<IndexEvent>)>,
    config: Arc<::config::Config>,
    protocol: ProtocolConfig,
    persistence: Arc<dyn Persistence>,
    shutdown: CancellationToken,
) {
    let mut force_restart = false;
    loop {
        let v3_state = v3_state.clone();
        let v4_state = v4_state.clone();
        let mut resync_tx = resync_tx.subscribe();
        let config = config.clone();
        let protocol = protocol.clone();
        let broadcaster = broadcaster.clone();
        let event_tx = event_tx.clone();

        let mut process = Process::<Message>::create(config).await;
        GenesisBootstrapper::register(&mut process);
        MithrilSnapshotFetcher::register(&mut process);
        BlockUnpacker::register(&mut process);
        PeerNetworkInterface::register(&mut process);

        let indexer = Arc::new(CustomIndexer::new(persistence.cursor_store()));
        process.register(indexer.clone());

        // Load indexer state from DB, then optionally bootstrap from external source.
        let mut v3_index_and_config = None;
        if let (Some(v3_config), Some(v3_state)) = (&protocol.v3, &v3_state) {
            let mut v3_index = SundaeV3Indexer::new(
                v3_state.clone(),
                broadcaster.clone(),
                event_tx.clone(),
                v3_config.clone(),
                config::ROLLBACK_LIMIT,
                persistence.indexer_dao("sundae_v3"),
            );
            v3_index.load().await.unwrap();
            v3_index_and_config = Some((v3_index, v3_config));
        }

        let mut v4_index_and_config = None;
        if let (Some(v4_config), Some(v4_state)) = (&protocol.v4, &v4_state) {
            let mut v4_index = SundaeV4Indexer::new(
                v4_state.clone(),
                event_tx.clone(),
                v4_config.clone(),
                config::ROLLBACK_LIMIT,
                persistence.indexer_dao("sundae_v4"),
            );
            v4_index.load().await.unwrap();
            v4_index_and_config = Some((v4_index, v4_config));
        }

        // Bootstrap from Kupo/Blockfrost if configured and DB is empty.
        let mut bootstrap_point: Option<acropolis_common::Point> = None;
        if let Some(ref bootstrap_config) = protocol.bootstrap {
            let v3_empty = match &v3_state {
                Some(s) => s.lock().await.latest().pools.is_empty(),
                None => false,
            };
            let v4_empty = match &v4_state {
                Some(s) => s.lock().await.latest().pools.is_empty(),
                None => false,
            };
            if v3_empty || v4_empty {
                match bootstrap::run_bootstrap(
                    bootstrap_config,
                    protocol.v3.as_ref(),
                    protocol.v4.as_ref(),
                    &v3_state,
                    &v4_state,
                )
                .await
                {
                    Ok(result) => {
                        if !result.tip_hash.is_empty() {
                            let point_str =
                                format!("{}.{}", result.tip_slot, result.tip_hash);
                            match point_str.parse() {
                                Ok(point) => bootstrap_point = Some(point),
                                Err(e) => {
                                    warn!("Bootstrap: could not parse tip as Point: {e:#}")
                                }
                            }
                        }
                        // Set loaded_slot on both indexers so blocks before
                        // the bootstrap tip are skipped during chain sync.
                        if let Some((ref mut v3_index, _)) = v3_index_and_config {
                            v3_index.set_loaded_slot(result.tip_slot);
                        }
                        if let Some((ref mut v4_index, _)) = v4_index_and_config {
                            v4_index.set_loaded_slot(result.tip_slot);
                        }
                        if bootstrap_point.is_some() {
                            info!("Bootstrap succeeded, starting chain sync from tip");
                        } else {
                            info!("Bootstrap succeeded but no block hash available; using configured starting point");
                        }
                    }
                    Err(e) => {
                        warn!("Bootstrap failed, falling back to chain sync: {e:#}");
                    }
                }
            }
        }

        if let Some((v3_index, v3_config)) = v3_index_and_config {
            let start = bootstrap_point
                .clone()
                .unwrap_or_else(|| v3_config.starting_point.clone());
            indexer
                .add_index(v3_index, start, force_restart)
                .await
                .unwrap();
        }

        if let Some((v4_index, v4_config)) = v4_index_and_config {
            let start = bootstrap_point
                .clone()
                .unwrap_or_else(|| v4_config.starting_point.clone());
            indexer
                .add_index(v4_index, start, force_restart)
                .await
                .unwrap();
        }

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
