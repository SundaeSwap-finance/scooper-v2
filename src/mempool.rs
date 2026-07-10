//! Mempool monitor — phase 1: observation only.
//!
//! Speaks the N2C LocalTxMonitor miniprotocol (via pallas-network) against a
//! co-located cardano-node socket, mirrors the mempool, classifies each
//! transaction against the v4 protocol's script hashes, and measures how far
//! ahead of block inclusion the mempool showed us each relevant tx by
//! correlating against the indexer's event stream. Emits logs and metrics
//! only — dispatch is deliberately not coupled yet. Phase 2 (chaining scoops
//! off unconfirmed orders, with cascade reset when a parent tx dies) will
//! consume this same feed.
//!
//! Protocol shape: `acquire` returns a mempool snapshot; `query_next_tx`
//! drains it; acquiring *again* blocks until the mempool has changed — so the
//! loop is push-shaped with no polling interval. Every tx the node hands us
//! has already passed full ledger validation against chain + mempool state.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pallas_traverse::MultiEraTx;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::events::IndexEvent;
use crate::sundaev4::SundaeV4HistoricalState;
use crate::metrics::Metrics;

/// Config lives at `protocol.v4.mempool`; absent = monitor disabled
/// (graceful degrade, matching the butane section's convention).
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MempoolMonitorConfig {
    /// Path to the local cardano-node's N2C unix socket.
    pub socket_path: String,
    /// Network magic for the N2C handshake (preview = 2, preprod = 1,
    /// mainnet = 764824073).
    pub network_magic: u64,
}

/// The script hashes the monitor classifies against. Derived from the v4
/// protocol config at spawn time.
#[derive(Clone, Debug)]
pub struct ProtocolWatch {
    pub pool_script_hash: pallas_addresses::ScriptHash,
    pub order_script_hashes: Vec<pallas_addresses::ScriptHash>,
}

/// What a mempool tx does to the protocol, as far as we can tell without
/// executing it. `order_spends`/`pool_spends` resolve inputs against the
/// indexer's current UTxO view, so a tx spending an order we've never seen
/// (e.g. created and consumed within the same mempool window) undercounts —
/// fine for observation, and phase 2's provisional store closes that gap.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TxClass {
    /// Outputs locked at an order validator address (order creations, or
    /// partial-fill continuations).
    pub order_creates: u32,
    /// Inputs that match a currently-known order UTxO (a scoop by someone,
    /// or a cancellation — distinguished by whether the tx also spends a pool).
    pub order_spends: u32,
    /// Inputs that match a currently-known pool UTxO (a scoop — ours or a
    /// competitor's).
    pub pool_spends: u32,
}

impl TxClass {
    pub fn relevant(&self) -> bool {
        self.order_creates > 0 || self.order_spends > 0 || self.pool_spends > 0
    }
}

/// Classify a transaction against the protocol's addresses and the current
/// known order/pool UTxO sets (each `(tx_hash, index)`).
pub fn classify_tx(
    tx: &MultiEraTx,
    watch: &ProtocolWatch,
    known_order_inputs: &BTreeSet<(Vec<u8>, u64)>,
    known_pool_inputs: &BTreeSet<(Vec<u8>, u64)>,
) -> TxClass {
    let mut class = TxClass::default();
    for output in tx.outputs() {
        let Ok(address) = output.address() else {
            continue;
        };
        let pallas_addresses::Address::Shelley(shelley) = address else {
            continue;
        };
        let payment = shelley.payment().as_hash();
        if watch.order_script_hashes.iter().any(|h| h == payment) {
            class.order_creates += 1;
        }
    }
    for input in tx.inputs() {
        let key = (input.hash().to_vec(), input.index());
        if known_order_inputs.contains(&key) {
            class.order_spends += 1;
        }
        if known_pool_inputs.contains(&key) {
            class.pool_spends += 1;
        }
    }
    class
}

struct SeenTx {
    seen_at: Instant,
    class: TxClass,
    /// Set when the tx disappears from the mempool without us having seen it
    /// in a block yet. Removal + no confirmation within the eviction window
    /// counts as evicted.
    gone_at: Option<Instant>,
}

type SeenMap = BTreeMap<Vec<u8>, SeenTx>;

/// How long after a tx vanishes from the mempool we keep waiting for its
/// block confirmation before counting it as evicted. Preview blocks average
/// 20s; five minutes is comfortably past any plausible indexer lag.
const EVICTION_WINDOW: Duration = Duration::from_secs(300);
/// Hard TTL on seen entries, so a wedged correlation stream can't leak.
const SEEN_TTL: Duration = Duration::from_secs(1800);

pub async fn run_mempool_monitor(
    cfg: MempoolMonitorConfig,
    watch: ProtocolWatch,
    v4_state: Arc<tokio::sync::Mutex<SundaeV4HistoricalState>>,
    events: tokio::sync::broadcast::Receiver<(u64, Vec<IndexEvent>)>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) {
    let seen: Arc<std::sync::Mutex<SeenMap>> = Arc::new(std::sync::Mutex::new(BTreeMap::new()));

    let correlate = correlate_confirmations(events, seen.clone(), metrics.clone());
    let monitor = monitor_loop(cfg, watch, v4_state, seen, metrics);

    tokio::select! {
        _ = shutdown.cancelled() => info!("mempool monitor shutting down"),
        _ = correlate => warn!("mempool confirmation correlator exited"),
        _ = monitor => warn!("mempool monitor loop exited"),
    }
}

/// Outer reconnect loop: the node restarting (or the socket perms changing)
/// must degrade to warnings, never take the scooper down.
async fn monitor_loop(
    cfg: MempoolMonitorConfig,
    watch: ProtocolWatch,
    v4_state: Arc<tokio::sync::Mutex<SundaeV4HistoricalState>>,
    seen: Arc<std::sync::Mutex<SeenMap>>,
    metrics: Arc<Metrics>,
) {
    loop {
        match pallas_network::facades::NodeClient::connect(&cfg.socket_path, cfg.network_magic)
            .await
        {
            Ok(mut client) => {
                info!(socket = %cfg.socket_path, "mempool monitor connected to node");
                if let Err(e) = watch_mempool(&mut client, &watch, &v4_state, &seen, &metrics).await
                {
                    warn!(error = %e, "mempool monitor session ended; reconnecting");
                }
                client.abort().await;
            }
            Err(e) => {
                warn!(error = %e, socket = %cfg.socket_path, "mempool monitor could not connect; retrying");
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn watch_mempool(
    client: &mut pallas_network::facades::NodeClient,
    watch: &ProtocolWatch,
    v4_state: &Arc<tokio::sync::Mutex<SundaeV4HistoricalState>>,
    seen: &Arc<std::sync::Mutex<SeenMap>>,
    metrics: &Arc<Metrics>,
) -> anyhow::Result<()> {
    let monitor = client.monitor();
    // Hashes currently in the node's mempool, as of the last snapshot.
    let mut in_mempool: BTreeSet<Vec<u8>> = BTreeSet::new();
    loop {
        // First call returns the current snapshot; subsequent calls block
        // until the mempool changes.
        monitor.acquire().await?;

        // The known-UTxO view refreshes per snapshot, not per tx: cheap, and
        // an order created earlier in this same snapshot won't be in it —
        // acceptable undercounting for phase 1.
        let (known_order_inputs, known_pool_inputs) = {
            let state = v4_state.lock().await;
            let latest = state.latest();
            let orders: BTreeSet<(Vec<u8>, u64)> = latest
                .orders
                .iter()
                .map(|o| (o.input.0.transaction_id.as_ref().to_vec(), o.input.0.index))
                .collect();
            let pools: BTreeSet<(Vec<u8>, u64)> = latest
                .pools
                .values()
                .map(|p| (p.input.0.transaction_id.as_ref().to_vec(), p.input.0.index))
                .collect();
            (orders, pools)
        };

        let mut current: BTreeSet<Vec<u8>> = BTreeSet::new();
        while let Some((era, body)) = monitor.query_next_tx().await? {
            let raw: &[u8] = body.0.as_ref();
            let tx = match MultiEraTx::decode(raw) {
                Ok(tx) => tx,
                Err(e) => {
                    debug!(era, error = %e, "mempool tx failed to decode; skipping");
                    continue;
                }
            };
            let hash = tx.hash().to_vec();
            current.insert(hash.clone());
            if in_mempool.contains(&hash) {
                continue;
            }
            metrics.mempool_txs_seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let class = classify_tx(&tx, watch, &known_order_inputs, &known_pool_inputs);
            if !class.relevant() {
                continue;
            }
            metrics
                .mempool_order_creates
                .fetch_add(class.order_creates as u64, std::sync::atomic::Ordering::Relaxed);
            metrics
                .mempool_order_spends
                .fetch_add(class.order_spends as u64, std::sync::atomic::Ordering::Relaxed);
            metrics
                .mempool_pool_spends
                .fetch_add(class.pool_spends as u64, std::sync::atomic::Ordering::Relaxed);
            info!(
                tx = %hex::encode(&hash),
                order_creates = class.order_creates,
                order_spends = class.order_spends,
                pool_spends = class.pool_spends,
                "mempool: relevant tx observed",
            );
            seen.lock().unwrap().insert(
                hash,
                SeenTx { seen_at: Instant::now(), class, gone_at: None },
            );
        }

        // Anything we knew that the fresh snapshot no longer contains was
        // either included in a block (the correlator will hear about it) or
        // evicted (TTL / replaced / conflict lost).
        let now = Instant::now();
        let mut seen_map = seen.lock().unwrap();
        for gone in in_mempool.difference(&current) {
            if let Some(entry) = seen_map.get_mut(gone) {
                entry.gone_at.get_or_insert(now);
            }
        }
        drop(seen_map);
        in_mempool = current;
    }
}

/// Match indexer events (block-confirmed order creations, pool updates,
/// scoops, cancellations) against mempool-seen txs to measure lead time, and
/// sweep out entries that vanished without confirming (evictions).
async fn correlate_confirmations(
    mut events: tokio::sync::broadcast::Receiver<(u64, Vec<IndexEvent>)>,
    seen: Arc<std::sync::Mutex<SeenMap>>,
    metrics: Arc<Metrics>,
) {
    let mut sweep = tokio::time::interval(Duration::from_secs(60));
    loop {
        tokio::select! {
            received = events.recv() => {
                let batch = match received {
                    Ok((_slot, events)) => events,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "mempool correlator lagged the event stream");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                };
                let mut confirmed: Vec<Vec<u8>> = Vec::new();
                for event in &batch {
                    match event {
                        IndexEvent::V4OrderCreated { order } => {
                            confirmed.push(order.input.0.transaction_id.as_ref().to_vec());
                        }
                        IndexEvent::V4PoolCreated { pool, .. }
                        | IndexEvent::V4PoolUpdated { pool, .. } => {
                            confirmed.push(pool.input.0.transaction_id.as_ref().to_vec());
                        }
                        IndexEvent::V4OrderScooped { tx_id, .. }
                        | IndexEvent::V4OrderCancelled { tx_id, .. } => {
                            if let Ok(hash) = hex::decode(tx_id) {
                                confirmed.push(hash);
                            }
                        }
                        _ => {}
                    }
                }
                let mut seen_map = seen.lock().unwrap();
                for hash in confirmed {
                    if let Some(entry) = seen_map.remove(&hash) {
                        let lead = entry.seen_at.elapsed();
                        metrics.mempool_confirmed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        metrics.mempool_lead_time.observe(lead.as_secs_f64());
                        info!(
                            tx = %hex::encode(&hash),
                            lead_secs = format!("{:.1}", lead.as_secs_f64()),
                            order_creates = entry.class.order_creates,
                            order_spends = entry.class.order_spends,
                            pool_spends = entry.class.pool_spends,
                            "mempool: tx confirmed in block; lead time measured",
                        );
                    }
                }
            }
            _ = sweep.tick() => {
                let now = Instant::now();
                let mut seen_map = seen.lock().unwrap();
                seen_map.retain(|hash, entry| {
                    let evicted = entry
                        .gone_at
                        .map(|gone| now.duration_since(gone) > EVICTION_WINDOW)
                        .unwrap_or(false);
                    let expired = now.duration_since(entry.seen_at) > SEEN_TTL;
                    if evicted || expired {
                        metrics.mempool_evicted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        info!(
                            tx = %hex::encode(hash),
                            "mempool: tx vanished without block confirmation (evicted or replaced)",
                        );
                        false
                    } else {
                        true
                    }
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scoop tx built by the real pipeline must classify as spending its
    /// pool + order inputs; a partial fill's continuation output must count
    /// as an order-address output.
    #[test]
    fn classify_scoop_tx() {
        use crate::sundaev4::batch::{assemble_batch, BatchLimits};
        use crate::sundaev4::test_harness::test_harness::*;

        let env = TestEnv::from_blueprint_file("test/fixtures/devnet-blueprint.json");
        let pool = make_pool(&env, 0xAA, token_a(), 1_000_000_000, token_b(), 1_000_000_000);
        let orders = vec![make_order(token_a(), 10_000_000, token_b(), 1, 1)];
        let batch = assemble_batch(&pool, &orders, env.exec.fee, env.exec.protocol_share, &BatchLimits::default())
            .expect("batch assembly should succeed");
        let settings = make_settings(&env, &env.scooper_keyhash());
        let (result, _eval) = env.build_and_eval(&[batch], &settings, 1000)
            .expect("build_and_eval should succeed");

        let tx = MultiEraTx::decode(&result.cbor).expect("scoop tx decodes");
        let watch = ProtocolWatch {
            pool_script_hash: env.exec.module_scripts.pool.hash,
            order_script_hashes: vec![env.exec.module_scripts.order.hash],
        };
        let order_inputs: BTreeSet<(Vec<u8>, u64)> = orders
            .iter()
            .map(|o| (o.input.0.transaction_id.as_ref().to_vec(), o.input.0.index))
            .collect();
        let pool_inputs: BTreeSet<(Vec<u8>, u64)> = [(
            pool.input.0.transaction_id.as_ref().to_vec(),
            pool.input.0.index,
        )]
        .into();

        let class = classify_tx(&tx, &watch, &order_inputs, &pool_inputs);
        assert_eq!(class.order_spends, 1, "scoop spends the order");
        assert_eq!(class.pool_spends, 1, "scoop spends the pool");
        assert!(class.relevant());

        // Unrelated tx view: same tx, but with no known UTxOs and different
        // watch hashes → nothing to see.
        let other_watch = ProtocolWatch {
            pool_script_hash: pallas_primitives::Hash::new([0xEE; 28]),
            order_script_hashes: vec![pallas_primitives::Hash::new([0xDD; 28])],
        };
        let class = classify_tx(&tx, &other_watch, &BTreeSet::new(), &BTreeSet::new());
        assert!(!class.relevant());
    }
}
