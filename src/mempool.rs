//! Mempool monitor + provisional order feed.
//!
//! Speaks the N2C LocalTxMonitor miniprotocol (via pallas-network) against a
//! co-located cardano-node socket, mirrors the mempool, classifies each
//! transaction against the v4 protocol's script hashes, and measures how far
//! ahead of block inclusion the mempool showed us each relevant tx by
//! correlating against the indexer's event stream.
//!
//! With `execute = true`, order outputs on mempool txs are parsed into full
//! [`SundaeV4Order`]s and published through a [`ProvisionalState`] shared
//! with the scooper, which dispatches scoops *chained on the unconfirmed
//! order tx*. The safety story: a chained tx whose parent never lands simply
//! never lands either — no on-chain cost — so reset is pure bookkeeping:
//! when a parent leaves the mempool its orders stop being dispatchable
//! immediately (either the confirmed copy takes over via the indexer, or the
//! eviction window expires and a [`IndexEvent::V4MempoolTxDropped`] tells the
//! scooper to cascade-discard chains built on it). Provisional *spends* are
//! tracked too, so orders being cancelled or scooped by someone else
//! in-mempool stop being dispatched before the block confirms it.
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
    /// When true, mempool-seen orders become dispatch candidates (chained
    /// execution) and provisional-parent scoops submit through the local
    /// node. False = observe only.
    #[serde(default)]
    pub execute: bool,
}

/// The script hashes the monitor classifies against. Derived from the v4
/// protocol config at spawn time.
#[derive(Clone, Debug)]
pub struct ProtocolWatch {
    pub pool_script_hash: pallas_addresses::ScriptHash,
    pub order_script_hashes: Vec<pallas_addresses::ScriptHash>,
    /// Constraint-module hashes for decoding order datums into dispatchable
    /// constraints (empty when execution isn't configured — parsing then
    /// classifies orders but can't produce candidates).
    pub swap_order_hash: Vec<u8>,
    pub basic_order_hash: Vec<u8>,
    pub strategy_order_hash: Vec<u8>,
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

// ────────────────────────────────────────────────────────────────────────────
// Provisional orders (phase 2: execution feed)
// ────────────────────────────────────────────────────────────────────────────

/// An order whose creating tx is still in the mempool.
#[derive(Clone)]
pub struct ProvisionalOrder {
    pub order: Arc<crate::sundaev4::SundaeV4Order>,
    /// Hash of the mempool tx that created this order UTxO.
    pub source_tx: Vec<u8>,
    /// True once the source tx has left the mempool. Not dispatchable in
    /// that window: either the confirmed copy is about to arrive via the
    /// indexer, or the tx was evicted and building on it is wasted work.
    pub gone: bool,
}

/// A pool state predicted by a FOREIGN unconfirmed tx (someone else's scoop
/// observed in the mempool). Dispatch chains on it instead of racing the
/// stale on-chain UTxO.
#[derive(Clone)]
pub struct ForeignPoolPrediction {
    pub pool: Arc<crate::sundaev4::SundaeV4Pool>,
    pub source_tx: Vec<u8>,
    /// The pool UTxO the foreign tx spent — used to detect conflicts with
    /// our own in-flight chains and to order chained predictions.
    pub spent_input: crate::cardano_types::TransactionInput,
    pub gone: bool,
}

/// Effects a single mempool tx had on the order book, tracked so that
/// confirmation or eviction of that tx can be undone as a unit.
#[derive(Clone, Default)]
struct TxEffects {
    created_orders: Vec<crate::cardano_types::TransactionInput>,
    spent_orders: Vec<crate::cardano_types::TransactionInput>,
    predicted_pools: Vec<crate::sundaev3::Ident>,
    /// True once the node's mempool no longer holds this tx. Mirrors the
    /// per-order/per-pool `gone` flags so a spend mark can be checked the
    /// same way — see [`ProvisionalState::spent_inputs`].
    gone: bool,
}

/// Shared between the mempool monitor (writer) and the scooper (reader).
/// All methods are quick and lock-free internally — callers hold the outer
/// `std::sync::Mutex` only for the duration of a call.
#[derive(Default)]
pub struct ProvisionalState {
    orders: BTreeMap<crate::cardano_types::TransactionInput, ProvisionalOrder>,
    /// Order inputs spent by some unconfirmed tx (cancellation, or a scoop —
    /// ours or a competitor's), keyed by order input → spender tx hash.
    spent: BTreeMap<crate::cardano_types::TransactionInput, Vec<u8>>,
    /// Latest predicted pool state per ident from mempool pool spends
    /// (includes our own scoops; the scooper filters those out against its
    /// in-flight set — its own chain tracker is authoritative for them).
    pools: BTreeMap<crate::sundaev3::Ident, ForeignPoolPrediction>,
    by_tx: BTreeMap<Vec<u8>, TxEffects>,
}

pub type SharedProvisional = Arc<std::sync::Mutex<ProvisionalState>>;

impl ProvisionalState {
    /// Record a mempool tx's effects. Idempotent per tx hash.
    pub fn note_tx(
        &mut self,
        tx_hash: Vec<u8>,
        created: Vec<(crate::cardano_types::TransactionInput, Arc<crate::sundaev4::SundaeV4Order>)>,
        spent: Vec<crate::cardano_types::TransactionInput>,
        pool_predictions: Vec<(
            crate::sundaev3::Ident,
            Arc<crate::sundaev4::SundaeV4Pool>,
            crate::cardano_types::TransactionInput,
        )>,
    ) {
        if self.by_tx.contains_key(&tx_hash) {
            return;
        }
        let mut effects = TxEffects::default();
        for (input, order) in created {
            effects.created_orders.push(input.clone());
            self.orders.insert(
                input,
                ProvisionalOrder { order, source_tx: tx_hash.clone(), gone: false },
            );
        }
        for input in spent {
            effects.spent_orders.push(input.clone());
            self.spent.insert(input, tx_hash.clone());
        }
        for (ident, pool, spent_input) in pool_predictions {
            effects.predicted_pools.push(ident.clone());
            // Last writer wins: mempool snapshots arrive in chain order, so
            // the newest prediction is the tip of that pool's mempool chain.
            self.pools.insert(
                ident,
                ForeignPoolPrediction {
                    pool,
                    source_tx: tx_hash.clone(),
                    spent_input,
                    gone: false,
                },
            );
        }
        self.by_tx.insert(tx_hash, effects);
    }

    /// The source tx left the mempool: suspend its orders immediately.
    /// (If it reappears — a re-add after a brief eviction — un-suspend.)
    pub fn set_gone(&mut self, tx_hash: &[u8], gone: bool) {
        // Split the borrow: `by_tx` is read while `orders`/`pools` are written.
        let Self { orders, pools, by_tx, .. } = self;
        let Some(effects) = by_tx.get_mut(tx_hash) else {
            return;
        };
        effects.gone = gone;
        for input in &effects.created_orders {
            if let Some(p) = orders.get_mut(input) {
                p.gone = gone;
            }
        }
        for ident in &effects.predicted_pools {
            if let Some(fp) = pools.get_mut(ident)
                && fp.source_tx.as_slice() == tx_hash
            {
                fp.gone = gone;
            }
        }
    }

    /// Remove a tx's effects entirely (confirmed in a block, or evicted).
    /// Returns the order inputs it had created, for logging.
    pub fn remove_tx(&mut self, tx_hash: &[u8]) -> Vec<crate::cardano_types::TransactionInput> {
        let Some(effects) = self.by_tx.remove(tx_hash) else {
            return Vec::new();
        };
        for input in &effects.created_orders {
            self.orders.remove(input);
        }
        for input in &effects.spent_orders {
            // Only clear the mark if WE set it (a later tx may have re-spent).
            if self.spent.get(input).map(|h| h.as_slice()) == Some(tx_hash) {
                self.spent.remove(input);
            }
        }
        for ident in &effects.predicted_pools {
            if self.pools.get(ident).map(|fp| fp.source_tx.as_slice()) == Some(tx_hash) {
                self.pools.remove(ident);
            }
        }
        effects.created_orders
    }

    /// Orders currently eligible for chained dispatch, with their parent tx.
    pub fn dispatchable_orders(
        &self,
    ) -> Vec<(Arc<crate::sundaev4::SundaeV4Order>, Vec<u8>)> {
        self.orders
            .values()
            .filter(|p| {
                !p.gone
                    && self
                        .spent
                        .get(&p.order.input)
                        .is_none_or(|tx_hash| !self.spend_is_live(tx_hash))
            })
            .map(|p| (p.order.clone(), p.source_tx.clone()))
            .collect()
    }

    /// Order inputs that some unconfirmed tx already spends — the scooper
    /// must not dispatch these even if they're still unspent on-chain.
    ///
    /// Spends by a tx the node's mempool no longer holds don't count. That
    /// tx either confirmed (the indexer is about to say so, and the order
    /// leaves the book anyway) or died, and in neither case is there
    /// anything left to conflict with. Waiting for [`EVICTION_WINDOW`] here
    /// instead cost order `f4a29320…#0` five minutes on preview
    /// (2026-07-30): our own scoop of it expired on TTL, left the mempool
    /// three seconds later, and the order still sat out of the candidate
    /// set until the eviction grace period ran out — while newer orders
    /// were scooped past it. That grace period exists to absorb *indexer*
    /// lag on confirmation, which is not a reason to keep blocking dispatch.
    pub fn spent_inputs(&self) -> BTreeSet<crate::cardano_types::TransactionInput> {
        self.spent
            .iter()
            .filter(|(_, tx_hash)| self.spend_is_live(tx_hash))
            .map(|(input, _)| input.clone())
            .collect()
    }

    /// Whether a spend recorded by `tx_hash` still blocks dispatch.
    fn spend_is_live(&self, tx_hash: &[u8]) -> bool {
        self.by_tx.get(tx_hash).map(|e| !e.gone).unwrap_or(false)
    }

    /// Inputs of currently-tracked provisional orders (for classification).
    pub fn order_inputs(&self) -> BTreeSet<(Vec<u8>, u64)> {
        self.orders
            .keys()
            .map(|i| (i.0.transaction_id.as_ref().to_vec(), i.0.index))
            .collect()
    }

    /// Live (not-gone) foreign pool predictions.
    pub fn foreign_pools(
        &self,
    ) -> Vec<(crate::sundaev3::Ident, ForeignPoolPrediction)> {
        self.pools
            .iter()
            .filter(|(_, fp)| !fp.gone)
            .map(|(i, fp)| (i.clone(), fp.clone()))
            .collect()
    }

    pub fn counts(&self) -> (usize, usize) {
        (self.orders.len(), self.spent.len())
    }
}

/// Parse the order-address outputs of a mempool tx into dispatchable orders.
/// Returns only outputs whose datum decodes into a known constraint shape —
/// anything else is logged and skipped (same tolerance as the indexer).
/// Datum resolution covers inline datums and the tx's own witness set;
/// metadata-posted datums are not resolvable here, and such orders simply
/// wait for block confirmation (the indexer handles them).
pub fn provisional_orders_from_tx(
    tx: &MultiEraTx,
    watch: &ProtocolWatch,
    slot: u64,
) -> Vec<(crate::cardano_types::TransactionInput, Arc<crate::sundaev4::SundaeV4Order>)> {
    use plutus_parser::AsPlutus;
    let tx_hash = tx.hash();
    let witness_datums: BTreeMap<pallas_primitives::DatumHash, pallas_primitives::PlutusData> = tx
        .plutus_data()
        .iter()
        .map(|d| {
            (
                pallas_crypto::hash::Hasher::<256>::hash(d.raw_cbor()),
                d.clone().unwrap(),
            )
        })
        .collect();
    let mut out = Vec::new();
    for (idx, output) in tx.outputs().iter().enumerate() {
        let Ok(address) = output.address() else { continue };
        let pallas_addresses::Address::Shelley(shelley) = address else {
            continue;
        };
        if !watch.order_script_hashes.iter().any(|h| h == shelley.payment().as_hash()) {
            continue;
        }
        let converted = crate::cardano_types::convert_txo(output);
        let datum_pd = match &converted.datum {
            crate::cardano_types::RawDatum::Inline(d) => d.clone(),
            crate::cardano_types::RawDatum::Hash(h) => match witness_datums.get(h) {
                Some(d) => d.clone(),
                None => {
                    debug!(tx = %hex::encode(tx_hash), idx, "mempool order datum by hash, not in witness set; deferring to confirmation");
                    continue;
                }
            },
            crate::cardano_types::RawDatum::None => continue,
        };
        let datum: crate::sundaev4::OrderDatum = match AsPlutus::from_plutus(datum_pd) {
            Ok(d) => d,
            Err(e) => {
                debug!(tx = %hex::encode(tx_hash), idx, error = %e, "mempool order output datum did not parse");
                continue;
            }
        };
        let constraint = match crate::sundaev4::Constraint::from_order_datum_with_strategy(
            &datum,
            &watch.swap_order_hash,
            &watch.basic_order_hash,
            &watch.strategy_order_hash,
        ) {
            Ok(c) => c,
            Err(e) => {
                debug!(tx = %hex::encode(tx_hash), idx, error = %e, "mempool order constraint did not decode");
                continue;
            }
        };
        let input = crate::cardano_types::TransactionInput::new(tx_hash, idx as u64);
        out.push((
            input.clone(),
            Arc::new(crate::sundaev4::SundaeV4Order {
                input,
                value: converted.value,
                datum,
                constraint,
                slot,
            }),
        ));
    }
    out
}

/// Parse the pool-address outputs of a mempool tx into predicted pool
/// states. `pool_context` maps known pool idents to their current
/// (input, pool) so the prediction can carry over pool_type/fee_split
/// (a scoop can't change a pool's module) and record which UTxO the
/// foreign tx spent. Pools we don't already know are skipped — a pool
/// *creation* in the mempool waits for its block.
pub fn pool_predictions_from_tx(
    tx: &MultiEraTx,
    watch: &ProtocolWatch,
    pool_context: &BTreeMap<
        crate::sundaev3::Ident,
        (crate::cardano_types::TransactionInput, Arc<crate::sundaev4::SundaeV4Pool>),
    >,
    slot: u64,
) -> Vec<(
    crate::sundaev3::Ident,
    Arc<crate::sundaev4::SundaeV4Pool>,
    crate::cardano_types::TransactionInput,
)> {
    use plutus_parser::AsPlutus;
    let tx_hash = tx.hash();
    let spent: BTreeSet<(Vec<u8>, u64)> = tx
        .inputs()
        .iter()
        .map(|i| (i.hash().to_vec(), i.index()))
        .collect();
    let mut out = Vec::new();
    for (idx, output) in tx.outputs().iter().enumerate() {
        let Ok(address) = output.address() else { continue };
        let pallas_addresses::Address::Shelley(shelley) = address else {
            continue;
        };
        if shelley.payment().as_hash() != &watch.pool_script_hash {
            continue;
        }
        let converted = crate::cardano_types::convert_txo(output);
        let datum_pd = match &converted.datum {
            crate::cardano_types::RawDatum::Inline(d) => d.clone(),
            _ => continue,
        };
        let pool_datum: crate::sundaev4::PoolDatum = match AsPlutus::from_plutus(datum_pd) {
            Ok(d) => d,
            Err(e) => {
                debug!(tx = %hex::encode(tx_hash), idx, error = %e, "mempool pool output datum did not parse");
                continue;
            }
        };
        let ident = pool_datum.identifier.clone();
        let Some((old_input, old_pool)) = pool_context.get(&ident) else {
            debug!(tx = %hex::encode(tx_hash), pool = %ident, "mempool pool output for unknown pool; deferring to confirmation");
            continue;
        };
        // Sanity: the tx must actually spend the pool UTxO we know about —
        // otherwise this is a chain we can't see the base of.
        if !spent.contains(&(old_input.0.transaction_id.as_ref().to_vec(), old_input.0.index)) {
            debug!(tx = %hex::encode(tx_hash), pool = %ident, "mempool pool output doesn't spend the known pool input; skipping");
            continue;
        }
        out.push((
            ident,
            Arc::new(crate::sundaev4::SundaeV4Pool {
                input: crate::cardano_types::TransactionInput::new(tx_hash, idx as u64),
                value: converted.value,
                pool_datum,
                pool_type: old_pool.pool_type.clone(),
                slot,
                fee_split_config: old_pool.fee_split_config.clone(),
            }),
            old_input.clone(),
        ));
    }
    out
}

/// Local-node submission failure, split so callers can tell an
/// authoritative ledger reject from an infrastructure problem: rejects must
/// not fall back to an external endpoint (the answer won't change), while a
/// dead socket should.
#[derive(Debug, thiserror::Error)]
pub enum NodeSubmitError {
    /// The node validated the tx against ledger + mempool state and said no.
    #[error("node rejected tx: {0}")]
    Rejected(String),
    /// Couldn't reach the node or the protocol errored.
    #[error("local tx submission failed: {0}")]
    Transport(String),
}

/// Submit a tx through the local node's N2C LocalTxSubmission. The default
/// submission path when a mempool config is present: ~12ms observed vs 1-2s
/// through an external endpoint, and chained txs' parents are guaranteed
/// visible to this node's mempool (we read them from there).
pub async fn submit_via_node(
    socket_path: &str,
    network_magic: u64,
    cbor: &[u8],
) -> Result<(), NodeSubmitError> {
    use pallas_network::miniprotocols::localtxsubmission::{EraTx, Response};
    const CONWAY_ERA: u16 = 6;
    let mut client = pallas_network::facades::NodeClient::connect(socket_path, network_magic)
        .await
        .map_err(|e| NodeSubmitError::Transport(e.to_string()))?;
    let result = client
        .submission()
        .submit_tx(EraTx(CONWAY_ERA, cbor.to_vec()))
        .await;
    client.abort().await;
    match result {
        Ok(Response::Accepted) => Ok(()),
        Ok(Response::Rejected(reason)) => {
            Err(NodeSubmitError::Rejected(hex::encode(&reason.0)))
        }
        Err(e) => Err(NodeSubmitError::Transport(e.to_string())),
    }
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
    event_tx: tokio::sync::broadcast::Sender<(u64, Vec<IndexEvent>)>,
    provisional: Option<SharedProvisional>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) {
    let seen: Arc<std::sync::Mutex<SeenMap>> = Arc::new(std::sync::Mutex::new(BTreeMap::new()));

    let correlate = correlate_confirmations(
        events,
        seen.clone(),
        provisional.clone(),
        event_tx.clone(),
        metrics.clone(),
    );
    let monitor = monitor_loop(cfg, watch, v4_state, seen, provisional, event_tx, metrics);

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
    provisional: Option<SharedProvisional>,
    event_tx: tokio::sync::broadcast::Sender<(u64, Vec<IndexEvent>)>,
    metrics: Arc<Metrics>,
) {
    loop {
        match pallas_network::facades::NodeClient::connect(&cfg.socket_path, cfg.network_magic)
            .await
        {
            Ok(mut client) => {
                info!(socket = %cfg.socket_path, "mempool monitor connected to node");
                metrics.mempool_connected.store(1, std::sync::atomic::Ordering::Relaxed);
                if let Err(e) = watch_mempool(
                    &mut client,
                    &watch,
                    &v4_state,
                    &seen,
                    provisional.as_ref(),
                    &event_tx,
                    &metrics,
                )
                .await
                {
                    warn!(error = %e, "mempool monitor session ended; reconnecting");
                }
                client.abort().await;
                metrics.mempool_connected.store(0, std::sync::atomic::Ordering::Relaxed);
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
    provisional: Option<&SharedProvisional>,
    event_tx: &tokio::sync::broadcast::Sender<(u64, Vec<IndexEvent>)>,
    metrics: &Arc<Metrics>,
) -> anyhow::Result<()> {
    let monitor = client.monitor();
    // Hashes currently in the node's mempool, as of the last snapshot.
    let mut in_mempool: BTreeSet<Vec<u8>> = BTreeSet::new();
    loop {
        // First call returns the current snapshot; subsequent calls block
        // until the mempool changes.
        monitor.acquire().await?;
        metrics.mempool_last_snapshot_unix.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            std::sync::atomic::Ordering::Relaxed,
        );

        // The known-UTxO view refreshes per snapshot, not per tx. Provisional
        // order inputs are folded in so that a tx spending an order that only
        // exists in the mempool (e.g. a cancellation racing us) still
        // classifies as an order spend.
        let mut new_events: Vec<IndexEvent> = Vec::new();
        let (known_order_inputs, known_pool_inputs, tip_slot) = {
            let state = v4_state.lock().await;
            let latest = state.latest();
            let mut orders: BTreeSet<(Vec<u8>, u64)> = latest
                .orders
                .iter()
                .map(|o| (o.input.0.transaction_id.as_ref().to_vec(), o.input.0.index))
                .collect();
            let mut pools: BTreeSet<(Vec<u8>, u64)> = latest
                .pools
                .values()
                .map(|p| (p.input.0.transaction_id.as_ref().to_vec(), p.input.0.index))
                .collect();
            if let Some(p) = provisional {
                let prov = p.lock().unwrap();
                orders.extend(prov.order_inputs());
                // Foreign predicted pool UTxOs: spends of them are chain
                // links we want to classify as pool spends too.
                for (_, fp) in prov.foreign_pools() {
                    pools.insert((
                        fp.pool.input.0.transaction_id.as_ref().to_vec(),
                        fp.pool.input.0.index,
                    ));
                }
            }
            (orders, pools, latest.network_tip_slot.unwrap_or(latest.tip_slot))
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
                // Re-appeared after a brief absence: resume dispatchability.
                if let Some(p) = provisional {
                    p.lock().unwrap().set_gone(&hash, false);
                }
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
            if let Some(p) = provisional {
                // Parse order outputs into dispatch candidates, record order
                // spends, and predict pool states — as one unit keyed by
                // this tx.
                let created = if class.order_creates > 0 {
                    provisional_orders_from_tx(&tx, watch, tip_slot)
                } else {
                    Vec::new()
                };
                let spent: Vec<crate::cardano_types::TransactionInput> = if class.order_spends > 0 {
                    tx.inputs()
                        .iter()
                        .filter(|i| {
                            known_order_inputs.contains(&(i.hash().to_vec(), i.index()))
                        })
                        .map(|i| crate::cardano_types::TransactionInput::new(*i.hash(), i.index()))
                        .collect()
                } else {
                    Vec::new()
                };
                let pool_predictions = if class.pool_spends > 0 {
                    // Context: confirmed pools overlaid with existing
                    // provisional predictions, so chains of foreign scoops
                    // resolve link by link.
                    let mut context: BTreeMap<
                        crate::sundaev3::Ident,
                        (crate::cardano_types::TransactionInput, Arc<crate::sundaev4::SundaeV4Pool>),
                    > = {
                        let state = v4_state.lock().await;
                        let latest = state.latest();
                        latest
                            .pools
                            .iter()
                            .map(|(i, pl)| (i.clone(), (pl.input.clone(), pl.clone())))
                            .collect()
                    };
                    for (ident, fp) in p.lock().unwrap().foreign_pools() {
                        context.insert(ident, (fp.pool.input.clone(), fp.pool.clone()));
                    }
                    pool_predictions_from_tx(&tx, watch, &context, tip_slot)
                } else {
                    Vec::new()
                };
                for (_, order) in &created {
                    info!(
                        order = %order.input,
                        source_tx = %hex::encode(&hash),
                        "mempool: provisional order candidate",
                    );
                    new_events.push(IndexEvent::V4MempoolOrderSeen { order: order.clone() });
                }
                for (ident, _, spent_input) in &pool_predictions {
                    info!(
                        pool = %ident,
                        source_tx = %hex::encode(&hash),
                        spent = %spent_input,
                        "mempool: pool spend predicted",
                    );
                }
                p.lock()
                    .unwrap()
                    .note_tx(hash.clone(), created, spent, pool_predictions);
            }
            seen.lock().unwrap().insert(
                hash,
                SeenTx { seen_at: Instant::now(), class, gone_at: None },
            );
        }

        // Anything we knew that the fresh snapshot no longer contains was
        // either included in a block (the correlator will hear about it) or
        // evicted (TTL / replaced / conflict lost). Suspend its provisional
        // orders right away — the confirmed copies arrive via the indexer if
        // it was a block.
        let now = Instant::now();
        let mut seen_map = seen.lock().unwrap();
        for gone in in_mempool.difference(&current) {
            if let Some(entry) = seen_map.get_mut(gone) {
                entry.gone_at.get_or_insert(now);
            }
            if let Some(p) = provisional {
                p.lock().unwrap().set_gone(gone, true);
            }
        }
        drop(seen_map);
        in_mempool = current;
        if !new_events.is_empty() {
            // Wake the scooper's dispatch loop; the provisional store is the
            // source of truth, the event is the doorbell.
            let _ = event_tx.send((tip_slot, new_events));
        }
    }
}

/// Match indexer events (block-confirmed order creations, pool updates,
/// scoops, cancellations) against mempool-seen txs to measure lead time, and
/// sweep out entries that vanished without confirming (evictions).
async fn correlate_confirmations(
    mut events: tokio::sync::broadcast::Receiver<(u64, Vec<IndexEvent>)>,
    seen: Arc<std::sync::Mutex<SeenMap>>,
    provisional: Option<SharedProvisional>,
    event_tx: tokio::sync::broadcast::Sender<(u64, Vec<IndexEvent>)>,
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
                    // Confirmed: the indexer now carries this tx's orders, so
                    // the provisional copies retire silently (dedupe).
                    if let Some(p) = &provisional {
                        p.lock().unwrap().remove_tx(&hash);
                    }
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
                let mut dropped: Vec<Vec<u8>> = Vec::new();
                {
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
                            dropped.push(hash.clone());
                            false
                        } else {
                            true
                        }
                    });
                }
                if let Some(p) = &provisional {
                    let mut drop_events: Vec<IndexEvent> = Vec::new();
                    {
                        let mut prov = p.lock().unwrap();
                        for hash in &dropped {
                            let removed = prov.remove_tx(hash);
                            if !removed.is_empty() {
                                warn!(
                                    tx = %hex::encode(hash),
                                    orders = removed.len(),
                                    "mempool: provisional parent evicted; cascading discard",
                                );
                                drop_events.push(IndexEvent::V4MempoolTxDropped {
                                    tx_hash: hash.clone(),
                                });
                            }
                        }
                    }
                    if !drop_events.is_empty() {
                        let _ = event_tx.send((0, drop_events));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provisional_store_lifecycle() {
        use crate::cardano_types::TransactionInput;
        fn dummy_order(tx_byte: u8, idx: u64) -> (TransactionInput, Arc<crate::sundaev4::SundaeV4Order>) {
            let input = TransactionInput::new([tx_byte; 32].into(), idx);
            let asset = crate::cardano_types::AssetClass { policy: vec![], token: vec![] };
            let order = crate::sundaev4::SundaeV4Order::test_swap_order(
                input.clone(),
                Default::default(),
                crate::multisig::Multisig::Signature(vec![0xAA; 28]),
                crate::sundaev4::Destination::SelfDestination,
                (asset.clone(), crate::bigint::BigInt::from(1)),
                (asset, crate::bigint::BigInt::from(1)),
                crate::bigint::BigInt::from(1_000_000),
                1,
            );
            (input, Arc::new(order))
        }
        let mut state = ProvisionalState::default();
        let parent_a = vec![0xA1; 32];
        let parent_b = vec![0xB2; 32];
        let (in_a, ord_a) = dummy_order(0xA1, 0);
        let (spent_target, _) = dummy_order(0x33, 0);

        state.note_tx(parent_a.clone(), vec![(in_a.clone(), ord_a)], vec![spent_target.clone()], vec![]);
        state.note_tx(parent_b.clone(), vec![], vec![in_a.clone()], vec![]);

        // Order from A is tracked but B spends it → not dispatchable.
        assert_eq!(state.dispatchable_orders().len(), 0);
        assert!(state.spent_inputs().contains(&spent_target));
        assert!(state.spent_inputs().contains(&in_a));

        // B evicted: its spend mark clears, A's order becomes dispatchable.
        state.remove_tx(&parent_b);
        let dispatchable = state.dispatchable_orders();
        assert_eq!(dispatchable.len(), 1);
        assert_eq!(dispatchable[0].1, parent_a);

        // A leaves the mempool: suspended immediately...
        state.set_gone(&parent_a, true);
        assert_eq!(state.dispatchable_orders().len(), 0);
        // ...and resumes if it reappears.
        state.set_gone(&parent_a, false);
        assert_eq!(state.dispatchable_orders().len(), 1);

        // A confirmed/evicted: everything it did unwinds.
        let removed = state.remove_tx(&parent_a);
        assert_eq!(removed, vec![in_a]);
        assert_eq!(state.counts(), (0, 0));
    }

    /// Replays the live stall (preview, 2026-07-30). Our scoop of an order
    /// expired on TTL and left the node's mempool, but the order stayed out
    /// of the candidate set until the 5-minute eviction grace period ran
    /// out — five minutes during which newer orders were scooped past it.
    #[test]
    fn a_spend_by_a_vanished_tx_stops_blocking_dispatch() {
        use crate::cardano_types::TransactionInput;
        let mut state = ProvisionalState::default();
        let our_scoop = vec![0x5C; 32];
        let order_input = TransactionInput::new([0xF4; 32].into(), 0);

        state.note_tx(our_scoop.clone(), vec![], vec![order_input.clone()], vec![]);
        assert!(
            state.spent_inputs().contains(&order_input),
            "while the node holds our scoop, re-dispatching would conflict",
        );

        // The node drops it (TTL passed, revalidated out on the next block).
        state.set_gone(&our_scoop, true);
        assert!(
            !state.spent_inputs().contains(&order_input),
            "nothing left to conflict with — the order is free now, not in 5 minutes",
        );

        // It reappears (brief absence, re-added): blocking resumes.
        state.set_gone(&our_scoop, false);
        assert!(state.spent_inputs().contains(&order_input));

        // And the eventual removal is still clean.
        state.remove_tx(&our_scoop);
        assert!(state.spent_inputs().is_empty());
    }

    /// The same release has to reach `dispatchable_orders`, which applies the
    /// spend check separately — a provisional order spent by a vanished tx is
    /// dispatchable again too.
    #[test]
    fn a_provisional_order_spent_by_a_vanished_tx_is_dispatchable_again() {
        use crate::cardano_types::TransactionInput;
        let input = TransactionInput::new([0xA1; 32].into(), 0);
        let asset = crate::cardano_types::AssetClass { policy: vec![], token: vec![] };
        let order = Arc::new(crate::sundaev4::SundaeV4Order::test_swap_order(
            input.clone(),
            Default::default(),
            crate::multisig::Multisig::Signature(vec![0xAA; 28]),
            crate::sundaev4::Destination::SelfDestination,
            (asset.clone(), crate::bigint::BigInt::from(1)),
            (asset, crate::bigint::BigInt::from(1)),
            crate::bigint::BigInt::from(1_000_000),
            1,
        ));

        let mut state = ProvisionalState::default();
        let creator = vec![0xA1; 32];
        let spender = vec![0xB2; 32];
        state.note_tx(creator.clone(), vec![(input.clone(), order)], vec![], vec![]);
        state.note_tx(spender.clone(), vec![], vec![input.clone()], vec![]);
        assert_eq!(state.dispatchable_orders().len(), 0);

        state.set_gone(&spender, true);
        let dispatchable = state.dispatchable_orders();
        assert_eq!(dispatchable.len(), 1);
        assert_eq!(dispatchable[0].1, creator);

        // The creating tx vanishing is a different matter: nothing to build on.
        state.set_gone(&creator, true);
        assert_eq!(state.dispatchable_orders().len(), 0);
    }

    #[test]
    fn foreign_pool_prediction_lifecycle() {
        use crate::cardano_types::TransactionInput;
        use crate::sundaev3::Ident;
        use crate::bigint::BigInt;
        use crate::sundaev4::{PoolDatum, PoolType, Rational};

        fn dummy_pool(ident: &Ident, tx_byte: u8) -> Arc<crate::sundaev4::SundaeV4Pool> {
            Arc::new(crate::sundaev4::SundaeV4Pool {
                input: TransactionInput::new([tx_byte; 32].into(), 0),
                value: Default::default(),
                pool_datum: PoolDatum {
                    assets: vec![],
                    total_lp: BigInt::from(0),
                    circulating_lp: BigInt::from(0),
                    preminted_lp: BigInt::from(0),
                    identifier: ident.clone(),
                    actions: vec![],
                    module_state: vec![],
                },
                pool_type: PoolType::ConstantProduct {
                    fee: Rational { num: BigInt::from(3), den: BigInt::from(1000) },
                },
                slot: 1,
                fee_split_config: None,
            })
        }

        let mut state = ProvisionalState::default();
        let ident = Ident::new(&[0x77]);
        let foreign_tx = vec![0xF0; 32];
        let base_input = TransactionInput::new([0x01; 32].into(), 0);
        let predicted = dummy_pool(&ident, 0xF0);

        state.note_tx(
            foreign_tx.clone(),
            vec![],
            vec![],
            vec![(ident.clone(), predicted, base_input.clone())],
        );
        let live = state.foreign_pools();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].0, ident);
        assert_eq!(live[0].1.spent_input, base_input);

        // A second foreign tx chains on the first: last writer wins.
        let foreign_tx2 = vec![0xF1; 32];
        let tip_input = live[0].1.pool.input.clone();
        state.note_tx(
            foreign_tx2.clone(),
            vec![],
            vec![],
            vec![(ident.clone(), dummy_pool(&ident, 0xF1), tip_input.clone())],
        );
        let live = state.foreign_pools();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].1.source_tx, foreign_tx2);
        assert_eq!(live[0].1.spent_input, tip_input);

        // Tip leaves the mempool → suspended; removal clears it. The first
        // tx's removal must NOT clear the entry (it's no longer the writer).
        state.set_gone(&foreign_tx2, true);
        assert!(state.foreign_pools().is_empty());
        state.remove_tx(&foreign_tx);
        state.set_gone(&foreign_tx2, false);
        assert_eq!(state.foreign_pools().len(), 1);
        state.remove_tx(&foreign_tx2);
        assert!(state.foreign_pools().is_empty());
    }

    /// A partial-fill scoop's continuation output is a REAL order-creating
    /// output at the order address with an inline datum — exactly the shape
    /// user order txs have in the mempool. Parsing it must yield a
    /// dispatchable order with the decremented remaining_offered.
    #[test]
    fn parse_provisional_order_from_tx() {
        use std::collections::BTreeMap as Map;
        use crate::sundaev4::accumulator::Accumulator;
        use crate::sundaev4::router;
        use crate::sundaev4::test_harness::test_harness::*;
        use crate::bigint::BigInt;

        let env = TestEnv::from_blueprint_file("test/fixtures/devnet-blueprint.json");
        let pool = make_pool(&env, 0xC1, token_a(), 200_000_000, token_b(), 200_000_000);
        let mut pool_map = Map::new();
        pool_map.insert(pool.pool_datum.identifier.clone(), pool.clone());
        let order = make_order_with_budget(token_a(), 400_000_000, token_b(), 240_000_000, 1, 8_000_000);
        let fill = BigInt::from(100_000_000u64);
        let route = router::find_optimal_route(
            &pool_map, &[], &token_a(), &token_b(), &fill,
            router::RoutingLimits::unlimited(),
        ).expect("partial route exists");
        let mut accum = Accumulator::new(env.exec.protocol_share);
        accum.try_add_routed_order(&order, &route, &pool_map).expect("partial fill adds");
        let plan = accum.into_plan();
        let settings = make_settings(&env, &env.scooper_keyhash());
        let (build, _eval) = env.build_and_eval_plan(&plan, &settings, 1000)
            .expect("partial fill builds");

        let tx = MultiEraTx::decode(&build.cbor).expect("tx decodes");
        let watch = ProtocolWatch {
            pool_script_hash: env.exec.module_scripts.pool.hash,
            order_script_hashes: vec![env.exec.module_scripts.order.hash],
            swap_order_hash: env.exec.module_scripts.swap_order.as_ref().unwrap().hash.as_ref().to_vec(),
            basic_order_hash: env.exec.module_scripts.basic_order.as_ref().unwrap().hash.as_ref().to_vec(),
            strategy_order_hash: env.exec.module_scripts.strategy_order.as_ref().map(|m| m.hash.as_ref().to_vec()).unwrap_or_default(),
        };
        let parsed = provisional_orders_from_tx(&tx, &watch, 42);
        assert_eq!(parsed.len(), 1, "continuation parses as one provisional order");
        let (input, cont) = &parsed[0];
        assert_eq!(input.0.transaction_id.as_ref(), tx.hash().as_ref());
        assert_eq!(cont.slot, 42);
        match &cont.constraint {
            crate::sundaev4::Constraint::Swap { remaining_offered, original_offered, .. } => {
                assert_eq!(*original_offered, BigInt::from(400_000_000u64));
                assert_eq!(*remaining_offered, BigInt::from(300_000_000u64));
            }
            other => panic!("expected swap constraint, got {other:?}"),
        }
    }

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
            swap_order_hash: Vec::new(),
            basic_order_hash: Vec::new(),
            strategy_order_hash: Vec::new(),
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
            swap_order_hash: Vec::new(),
            basic_order_hash: Vec::new(),
            strategy_order_hash: Vec::new(),
        };
        let class = classify_tx(&tx, &other_watch, &BTreeSet::new(), &BTreeSet::new());
        assert!(!class.relevant());
    }
}
