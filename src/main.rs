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
use tracing::{error, info, warn};

mod bigint;
mod blueprint;
mod bootstrap;
mod cardano_types;
mod config;
mod datum_lookup;
mod events;
mod historical_state;
mod instrumentation;
mod mempool;
mod metrics;
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
    info!(
        v3_enabled = config.protocol.v3.is_some(),
        v4_enabled = config.protocol.v4.is_some(),
        server_address = %config.server.address,
        "configuration loaded"
    );

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

    // Strategy intent service: ingest via the admin server, execution by the
    // scooper, hygiene via the prune loop below. Only meaningful with a v4
    // execution config (we can't validate intents without module hashes).
    let intents: Option<sundaev4::intents::IntentServiceHandle> = match &v4_execution {
        Some(exec) => Some(Arc::new(
            sundaev4::intents::IntentService::load(
                persistence.strategy_intent_dao(),
                exec.strategy_peers.clone(),
            )
            .await?,
        )),
        None => None,
    };
    if let (Some(intents), Some(v4_state)) = (intents.clone(), v4_state.clone()) {
        let shutdown = shutdown.child_token();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tick.tick() => {}
                }
                // Only trust "order not in state" as evidence of a spent
                // order once the indexer is at the network tip — before
                // that, the state is incomplete and pruning on it would
                // wipe intents that are still live.
                let (live_orders, spent_orders, at_tip) = {
                    let state = v4_state.lock().await;
                    let latest = state.latest();
                    let at_tip = latest
                        .network_tip_slot
                        .map(|net| latest.tip_slot + 10 >= net)
                        .unwrap_or(false);
                    let live: std::collections::BTreeSet<(Vec<u8>, u64)> = latest
                        .orders
                        .iter()
                        .map(|o| (o.input.0.transaction_id.as_ref().to_vec(), o.input.0.index))
                        .collect();
                    // Spending tx per recently-spent order — lets the intent
                    // status report "executed by tx T" (whoever scooped it).
                    let spent: std::collections::BTreeMap<(Vec<u8>, u64), Vec<u8>> = latest
                        .spent_orders
                        .iter()
                        .filter_map(|so| {
                            let tx = hex::decode(&so.tx_id).ok()?;
                            Some((
                                (
                                    so.order.input.0.transaction_id.as_ref().to_vec(),
                                    so.order.input.0.index,
                                ),
                                tx,
                            ))
                        })
                        .collect();
                    (live, spent, at_tip)
                };
                use sundaev4::intents::OrderDisposition;
                let result = intents
                    .prune(|key| {
                        if !at_tip || live_orders.contains(key) {
                            OrderDisposition::Live
                        } else {
                            OrderDisposition::Spent(spent_orders.get(key).cloned())
                        }
                    })
                    .await;
                if let Err(e) = result {
                    tracing::warn!("strategy intent prune failed: {e}");
                }
            }
        });
    }

    // Captured before `protocol` moves into manager_loop.
    let mempool_spawn = protocol.v4.as_ref().and_then(|v4| {
        v4.mempool.clone().map(|cfg| {
            (
                cfg,
                mempool::ProtocolWatch {
                    pool_script_hash: v4.pool_script_hash,
                    order_script_hashes: v4.order_script_hashes.clone(),
                },
            )
        })
    });
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
    let metrics = Arc::new(metrics::Metrics::new());
    // Mempool monitor (phase 1: observation only): mirrors the local node's
    // mempool and measures how far pre-block we see relevant txs. Config
    // absent → not spawned.
    if let (Some((mempool_cfg, watch)), Some(v4_state_ref)) = (mempool_spawn, v4_state.as_ref()) {
        tokio::spawn(mempool::run_mempool_monitor(
            mempool_cfg,
            watch,
            v4_state_ref.clone(),
            event_tx.subscribe(),
            metrics.clone(),
            shutdown.child_token(),
        ));
    }
    let scooper_handle = tokio::spawn(
        Scooper::new(
            config.log.trace_directory.clone(),
            event_tx.subscribe(),
            v3_state.clone(),
            v4_state.clone(),
            v4_execution,
            paused.clone(),
            metrics.clone(),
            intents.clone(),
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
        metrics.clone(),
        intents.clone(),
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

        // Warn if DB has data but starting-point is origin (common after devnet reset)
        if let Some((_, v4_config)) = &v4_index_and_config {
            if matches!(v4_config.starting_point, acropolis_common::Point::Origin) {
                if let Some(ref s) = v4_state {
                    let slot = s.lock().await.latest().tip_slot;
                    if slot > 0 {
                        warn!(
                            slot,
                            "DB contains v4 data at slot {slot} but starting-point is 'origin' \
                             — if the devnet was reset, delete the database file and restart"
                        );
                    }
                }
            }
        }
        if let Some((_, v3_config)) = &v3_index_and_config {
            if matches!(v3_config.starting_point, acropolis_common::Point::Origin) {
                if let Some(ref s) = v3_state {
                    let n_pools = s.lock().await.latest().pools.len();
                    if n_pools > 0 {
                        warn!(
                            n_pools,
                            "DB contains v3 data ({n_pools} pools) but starting-point is 'origin' \
                             — if the devnet was reset, delete the database file and restart"
                        );
                    }
                }
            }
        }

        // Bootstrap from Kupo/Blockfrost if configured and DB has no data.
        // We check tip_slot == 0 (no blocks indexed yet) rather than pools.is_empty(),
        // because a protocol can legitimately have 0 pools while still having
        // indexed blocks, settings, orders, etc.
        let mut bootstrap_point: Option<acropolis_common::Point> = None;
        if let Some(ref bootstrap_config) = protocol.bootstrap {
            let v3_needs_bootstrap = match &v3_state {
                Some(s) => s.lock().await.latest().pools.is_empty(),
                None => false,
            };
            let v4_needs_bootstrap = match &v4_state {
                Some(s) => s.lock().await.latest().tip_slot == 0,
                None => false,
            };
            if v3_needs_bootstrap || v4_needs_bootstrap {
                match bootstrap::run_bootstrap(
                    bootstrap_config,
                    if v3_needs_bootstrap { protocol.v3.as_ref() } else { None },
                    if v4_needs_bootstrap { protocol.v4.as_ref() } else { None },
                    &v3_state,
                    &v4_state,
                    &persistence,
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
                error!("could not start acropolis process: {err:#}");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        };

        warn!("restarting indexer — resync requested or connection lost");
    }
}
