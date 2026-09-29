use std::{
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};

use http_body_util::{Either, Full};
use hyper::{
    Request, Response,
    body::{Bytes, Frame, Incoming as IncomingBody},
    server::conn::http1,
};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    select,
    sync::Mutex,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use std::collections::BTreeMap;

use crate::{
    cardano_types::TransactionInput,
    events::IndexEvent,
    metrics::Metrics,
    sundaev3::{Ident, PoolError, SundaeV3HistoricalState, ValidationError, validate_order},
    sundaev4::{SundaeV4HistoricalState, batch},
};

type V3State = Option<Arc<Mutex<SundaeV3HistoricalState>>>;
type V4State = Option<Arc<Mutex<SundaeV4HistoricalState>>>;

/// Module state preimage map: state_hash bytes → config CBOR bytes.
/// Computed at startup from the scooper's execution config.
pub type ModuleStatePreimages = BTreeMap<Vec<u8>, Vec<u8>>;

/// A reason dispatch skips a swap before pricing it. One variant per gate
/// the scooper applies; `reason()` is the operator-facing text and
/// `bucket()` the stable key the Protocol tab counts by.
enum SwapGate {
    ConfigMissing(String),
    Restricted,
    RouteBudget {
        per_exec: u64,
        cost_per_pool: u64,
        cost_per_step: u64,
    },
}

impl SwapGate {
    fn bucket(&self) -> &'static str {
        match self {
            SwapGate::ConfigMissing(_) => "config_missing",
            SwapGate::Restricted => "restricted",
            SwapGate::RouteBudget { .. } => "route_budget",
        }
    }

    fn reason(&self) -> String {
        match self {
            SwapGate::ConfigMissing(token) => format!(
                "references OrderConfig token {token} which is not on chain; \
                 the order cannot execute until that config is published"
            ),
            SwapGate::Restricted => "this pool is allowlist-restricted and this scooper does not \
                                     serve the order's credentials on it"
                .to_string(),
            SwapGate::RouteBudget {
                per_exec,
                cost_per_pool,
                cost_per_step,
            } => format!(
                "max_per_execution {per_exec} lovelace cannot fund any route \
                 (scooper prices {cost_per_pool} per pool / {cost_per_step} per step); \
                 cancel and re-place with a higher per-execution cap"
            ),
        }
    }
}

/// Build the preimage map from an execution config's fee and protocol_share.
pub fn compute_module_state_preimages(
    fee: (u64, u64),
    protocol_share: (u64, u64),
) -> ModuleStatePreimages {
    use crate::bigint::BigInt;
    use crate::sundaev4::{ConstantProductConfig, FeeSplitConfig, Rational};
    use pallas_crypto::hash::Hasher;
    use plutus_parser::AsPlutus;

    let configs = [
        minicbor::to_vec(
            ConstantProductConfig {
                fee: Rational {
                    num: BigInt::from(fee.0),
                    den: BigInt::from(fee.1),
                },
            }
            .to_plutus(),
        )
        .unwrap(),
        minicbor::to_vec(
            FeeSplitConfig {
                protocol_share: Rational {
                    num: BigInt::from(protocol_share.0),
                    den: BigInt::from(protocol_share.1),
                },
            }
            .to_plutus(),
        )
        .unwrap(),
    ];

    let mut map = BTreeMap::new();
    for cbor in configs {
        let hash = Hasher::<256>::hash(&cbor).to_vec();
        map.insert(hash, cbor);
    }
    map
}

// ── SSE streaming body ──────────────────────────────────────────────────

/// A streaming response body fed by an mpsc channel.
/// Each received `Bytes` chunk is emitted as an HTTP data frame.
pub struct SseBody {
    rx: tokio::sync::mpsc::Receiver<Bytes>,
}

impl hyper::body::Body for SseBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(chunk)) => Poll::Ready(Some(Ok(Frame::data(chunk)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

type ResponseBody = Either<Full<Bytes>, SseBody>;

#[derive(Clone, Debug, Deserialize)]
pub struct ServerConfig {
    pub address: SocketAddr,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    /// Optional second listener exposing only the PUBLIC surface: the
    /// strategy-intent endpoints and /health. Everything operational
    /// (dashboard, pause, resync, listings, metrics) stays private on
    /// `address`.
    #[serde(default)]
    pub public_address: Option<SocketAddr>,
    #[serde(default)]
    pub public_tls_cert: Option<String>,
    #[serde(default)]
    pub public_tls_key: Option<String>,
}

fn build_tls_acceptor(cert_path: &str, key_path: &str) -> anyhow::Result<TlsAcceptor> {
    use rustls_pemfile::{certs, private_key};
    use std::io::BufReader;
    use tokio_rustls::rustls;

    let cert_file = std::fs::File::open(cert_path)
        .map_err(|e| anyhow::anyhow!("failed to open TLS cert {cert_path}: {e}"))?;
    let key_file = std::fs::File::open(key_path)
        .map_err(|e| anyhow::anyhow!("failed to open TLS key {key_path}: {e}"))?;

    let certs: Vec<_> = certs(&mut BufReader::new(cert_file))
        .collect::<Result<_, _>>()
        .map_err(|e| anyhow::anyhow!("failed to parse TLS certs: {e}"))?;
    let key = private_key(&mut BufReader::new(key_file))
        .map_err(|e| anyhow::anyhow!("failed to parse TLS key: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {key_path}"))?;

    let config =
        rustls::ServerConfig::builder().with_no_client_auth().with_single_cert(certs, key)?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[allow(clippy::too_many_arguments)]
pub async fn admin_server(
    config: ServerConfig,
    network: String,
    v3_state: V3State,
    v4_state: V4State,
    v4_fee: Option<(u64, u64)>,
    v4_routing_costs: Option<(u64, u64)>,
    v4_pool_allowlists: Arc<crate::sundaev4::access::PoolAllowlists>,
    v4_module_preimages: ModuleStatePreimages,
    v4_protocol_share: Option<(u64, u64)>,
    v4_dao: Option<Arc<dyn crate::persistence::IndexerDao>>,
    resync_tx: tokio::sync::broadcast::Sender<()>,
    event_tx: tokio::sync::broadcast::Sender<(u64, Vec<IndexEvent>)>,
    paused: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    intents: Option<crate::sundaev4::intents::IntentServiceHandle>,
    shutdown: CancellationToken,
) {
    let base = AdminServer {
        visibility: Visibility::Private,
        network,
        v3_state,
        v4_state,
        v4_fee,
        v4_routing_costs,
        v4_pool_allowlists,
        v4_module_preimages: Arc::new(v4_module_preimages),
        v4_protocol_share,
        v4_dao,
        resync_tx,
        event_tx,
        paused,
        metrics,
        intents,
        remote_addr: None,
    };

    let mut listeners = Vec::new();

    let private_tls = match (&config.tls_cert, &config.tls_key) {
        (Some(cert), Some(key)) => {
            let acceptor = build_tls_acceptor(cert, key).expect("failed to initialize TLS");
            tracing::info!(cert = %cert, "TLS enabled for private admin server");
            Some(acceptor)
        }
        (None, None) => None,
        _ => panic!("tls_cert and tls_key must both be set or both be absent"),
    };
    listeners.push((config.address, private_tls, base.clone()));

    if let Some(public_addr) = config.public_address {
        let public_tls = match (&config.public_tls_cert, &config.public_tls_key) {
            (Some(cert), Some(key)) => {
                let acceptor =
                    build_tls_acceptor(cert, key).expect("failed to initialize public TLS");
                tracing::info!(cert = %cert, "TLS enabled for public server");
                Some(acceptor)
            }
            (None, None) => None,
            _ => panic!("public_tls_cert and public_tls_key must both be set or both be absent"),
        };
        let mut public = base.clone();
        public.visibility = Visibility::Public;
        tracing::info!(address = %public_addr, "public strategy-intent listener enabled");
        listeners.push((public_addr, public_tls, public));
    }

    let mut handles = Vec::new();
    for (addr, tls, server) in listeners {
        let shutdown = shutdown.child_token();
        handles.push(tokio::spawn(run_listener(addr, tls, server, shutdown)));
    }
    for h in handles {
        let _ = h.await;
    }
}

async fn run_listener(
    addr: SocketAddr,
    tls_acceptor: Option<TlsAcceptor>,
    server: AdminServer,
    shutdown: CancellationToken,
) {
    let listener = TcpListener::bind(addr).await.unwrap();
    loop {
        let (stream, peer) = select! {
            res = listener.accept() => match res {
                Ok(pair) => pair,
                Err(e) => { debug!("accept failed: {e}"); continue; }
            },
            _ = shutdown.cancelled() => { break; }
        };
        let mut server = server.clone();
        server.remote_addr = Some(peer);
        let tls_acceptor = tls_acceptor.clone();
        let child = shutdown.child_token();
        tokio::task::spawn(async move {
            if let Some(acceptor) = tls_acceptor {
                let tls_stream = match acceptor.accept(stream).await {
                    Ok(s) => s,
                    Err(e) => {
                        debug!("TLS handshake failed: {:?}", e);
                        return;
                    }
                };
                select! {
                    _ = child.cancelled() => {},
                    _ = serve_connection(tls_stream, server) => {}
                }
            } else {
                select! {
                    _ = child.cancelled() => {},
                    _ = serve_connection(stream, server) => {}
                }
            }
        });
    }
}

async fn serve_connection(
    stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
    server: AdminServer,
) {
    let io = TokioIo::new(stream);
    if let Err(err) = http1::Builder::new().serve_connection(io, server).await {
        debug!("Failed to serve connection: {:?}", err);
    }
}

/// Which route surface a listener serves.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Visibility {
    /// Full operational surface (dashboard, pause, listings, metrics, …).
    Private,
    /// Strategy-intent endpoints + /health only.
    Public,
}

#[derive(Clone)]
struct AdminServer {
    visibility: Visibility,
    /// Network name from config, e.g. "preview". Reported in `/status` so
    /// the dashboard can convert slots to wall-clock times.
    network: String,
    v3_state: V3State,
    v4_state: V4State,
    v4_fee: Option<(u64, u64)>,
    /// (cost_per_pool_lovelace, cost_per_step_lovelace) — the router's
    /// fan-out gating knobs, mirrored here so executability reporting
    /// agrees with what dispatch will actually do.
    v4_routing_costs: Option<(u64, u64)>,
    /// Per-pool trading allowlists, mirrored from the execution config so
    /// listings and intent probes agree with what dispatch will do.
    v4_pool_allowlists: Arc<crate::sundaev4::access::PoolAllowlists>,
    v4_module_preimages: Arc<ModuleStatePreimages>,
    /// The scooper's configured protocol share of pool fees, reported on the
    /// dashboard's Protocol tab. Pools may override it per pool on chain.
    v4_protocol_share: Option<(u64, u64)>,
    /// V4 indexer DAO, for scoop-record windows on the Protocol tab. The
    /// in-memory `ScoopStats` keep only the 50 most recent scoops.
    v4_dao: Option<Arc<dyn crate::persistence::IndexerDao>>,
    resync_tx: tokio::sync::broadcast::Sender<()>,
    event_tx: tokio::sync::broadcast::Sender<(u64, Vec<IndexEvent>)>,
    paused: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    intents: Option<crate::sundaev4::intents::IntentServiceHandle>,
    /// Peer address of the connection this (per-connection) clone serves.
    /// `None` only on the base template before a connection is accepted.
    remote_addr: Option<SocketAddr>,
}

impl hyper::service::Service<Request<IncomingBody>> for AdminServer {
    type Response = Response<ResponseBody>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<IncomingBody>) -> Self::Future {
        let me = self.clone();
        Box::pin(async move { Ok(me.do_call(req).await) })
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
    fn json_response(body: String) -> Response<ResponseBody> {
        Response::builder()
            .header("Content-Type", "application/json")
            .header("Access-Control-Allow-Origin", "*")
            .body(Either::Left(Full::new(Bytes::from(body))))
            .unwrap()
    }

    fn text_response(body: impl Into<String>) -> Response<ResponseBody> {
        Response::builder()
            .header("Access-Control-Allow-Origin", "*")
            .body(Either::Left(Full::new(Bytes::from(body.into()))))
            .unwrap()
    }

    fn error_response(
        status: hyper::StatusCode,
        message: impl Into<String>,
    ) -> Response<ResponseBody> {
        let body = serde_json::json!({ "error": message.into() }).to_string();
        Response::builder()
            .status(status)
            .header("Content-Type", "application/json")
            .header("Access-Control-Allow-Origin", "*")
            .body(Either::Left(Full::new(Bytes::from(body))))
            .unwrap()
    }

    /// POST /v4/strategy-intents
    ///
    /// Body: `{"signed_execution": "<SignedStrategyExecution cbor hex>",
    ///         "hint": {"type": "claim", "pool": "<ident hex>"}?}`.
    /// The signed CBOR travels verbatim — the Ed25519 signature covers the
    /// exact bytes of the execution, so no JSON re-encoding of it exists.
    async fn post_strategy_intent(self, req: Request<IncomingBody>) -> Response<ResponseBody> {
        use http_body_util::BodyExt;

        let Some(intents) = self.intents.clone() else {
            return Self::error_response(
                hyper::StatusCode::NOT_IMPLEMENTED,
                "strategy intents require a configured v4 execution",
            );
        };
        let Some(v4_state) = self.v4_state.clone() else {
            return Self::error_response(
                hyper::StatusCode::NOT_IMPLEMENTED,
                "v4 protocol not configured",
            );
        };

        // Bound the body read: a signed execution is small; 64KB is generous.
        //
        // The limit has to be enforced by the reader, not checked after it.
        // Collecting first and measuring afterwards buffers the whole body
        // whatever its size, so an unauthenticated post of a few gigabytes
        // was an out-of-memory kill rather than a 413. `Limited` stops at the
        // cap and returns an error instead.
        const MAX_BODY: usize = 64 * 1024;
        let limited = http_body_util::Limited::new(req.into_body(), MAX_BODY);
        let body = match limited.collect().await {
            Ok(b) => b.to_bytes(),
            // `Limited` reports the cap as a boxed error and gives no way to
            // tell it apart from a read failure, so both answer 413. A client
            // that lost its connection gets a slightly wrong code; a client
            // sending too much gets the right one.
            Err(e) => {
                return Self::error_response(
                    hyper::StatusCode::PAYLOAD_TOO_LARGE,
                    format!("body rejected (limit {MAX_BODY} bytes): {e}"),
                );
            }
        };

        #[derive(Deserialize)]
        struct PostIntentBody {
            signed_execution: String,
            #[serde(default)]
            hint: Option<crate::sundaev4::intents::ExecutionHint>,
        }
        let parsed: PostIntentBody = match serde_json::from_slice(&body) {
            Ok(p) => p,
            Err(e) => {
                return Self::error_response(
                    hyper::StatusCode::BAD_REQUEST,
                    format!("invalid JSON body: {e}"),
                );
            }
        };
        let sse_cbor = match hex::decode(parsed.signed_execution.trim()) {
            Ok(b) => b,
            Err(e) => {
                return Self::error_response(
                    hyper::StatusCode::BAD_REQUEST,
                    format!("signed_execution is not valid hex: {e}"),
                );
            }
        };

        // Snapshot current orders (and the malformed set) so validation doesn't
        // hold the state lock.
        let (orders, invalid_orders): (
            Vec<Arc<crate::sundaev4::SundaeV4Order>>,
            Vec<crate::events::InvalidOrder>,
        ) = {
            let state = v4_state.lock().await;
            let latest = state.latest();
            (latest.orders.clone(), latest.invalid_orders.clone())
        };
        let find_order = |key: &crate::sundaev4::intents::OrderKey| {
            orders
                .iter()
                .find(|o| {
                    o.input.0.transaction_id.as_ref() == key.0.as_slice()
                        && o.input.0.index == key.1
                })
                .cloned()
        };
        // When the target order was indexed but is unparseable, surface the
        // recorded reason instead of a bare "not found".
        let describe_invalid = |key: &crate::sundaev4::intents::OrderKey| {
            invalid_orders
                .iter()
                .find(|io| {
                    io.input.0.transaction_id.as_ref() == key.0.as_slice()
                        && io.input.0.index == key.1
                })
                .map(|io| io.reason.clone())
        };

        match intents.submit(sse_cbor, parsed.hint, find_order, describe_invalid).await {
            Ok(outcome) => Self::json_response(serde_json::to_string(&outcome).unwrap()),
            Err(e) => Self::error_response(hyper::StatusCode::BAD_REQUEST, format!("{e:#}")),
        }
    }

    /// GET /v4/strategy-intents — count only. Intent contents are trading
    /// strategy (front-runnable), so the listing exposes no details; use
    /// GET /v4/strategy-intents/<intent_id> with the id returned at POST
    /// time. The id is blake2b-256 of the signed execution bytes — a
    /// capability only the submitter can derive.
    async fn list_strategy_intents(self) -> Response<ResponseBody> {
        let Some(intents) = self.intents.clone() else {
            return Self::error_response(
                hyper::StatusCode::NOT_IMPLEMENTED,
                "strategy intents require a configured v4 execution",
            );
        };
        let store = intents.store.lock().await;
        Self::json_response(serde_json::json!({ "count": store.len() }).to_string())
    }

    /// GET /v4/strategy-intents/<intent_id hex> — status for one intent.
    /// Live intents also get a dispatchability probe against current pools.
    async fn strategy_intent_status(self, id_hex: &str) -> Response<ResponseBody> {
        use crate::sundaev4::intents::IntentLookup;

        let Some(intents) = self.intents.clone() else {
            return Self::error_response(
                hyper::StatusCode::NOT_IMPLEMENTED,
                "strategy intents require a configured v4 execution",
            );
        };
        let Ok(id) = hex::decode(id_hex.trim()) else {
            return Self::error_response(hyper::StatusCode::BAD_REQUEST, "intent id must be hex");
        };

        // Pool + order snapshots for the probe (before taking the store lock).
        let (pools, orders, order_configs) = match &self.v4_state {
            Some(v4) => {
                let state = v4.lock().await;
                let latest = state.latest();
                (
                    latest.pools.clone(),
                    latest.orders.clone(),
                    latest.order_configs.clone(),
                )
            }
            None => (Default::default(), Vec::new(), Default::default()),
        };

        let store = intents.store.lock().await;
        let body = match store.find(&id) {
            None => {
                return Self::error_response(hyper::StatusCode::NOT_FOUND, "unknown intent");
            }
            Some(IntentLookup::Terminal(t)) => serde_json::json!({
                "status": t.status,
                "tx_hash": t.tx_hash.as_ref().map(hex::encode),
            }),
            Some(IntentLookup::Live(i)) => {
                let key = crate::sundaev4::intents::order_key(&i.sse);
                let order = orders.iter().find(|o| {
                    o.input.0.transaction_id.as_ref() == key.0.as_slice()
                        && o.input.0.index == key.1
                });
                let visible = match order {
                    Some(o) => self.v4_pool_allowlists.retain_visible(
                        pools.clone(),
                        o,
                        o.constraint.strategy(),
                    ),
                    None => pools.clone(),
                };
                let probe: serde_json::Value = match (order, &i.hint) {
                    (None, _) => serde_json::json!("order-not-indexed"),
                    // The order validator's withdraw handler demands the
                    // order's OrderConfig as a reference input; when that
                    // token isn't on chain, no execution can succeed no
                    // matter what the swap/claim math says.
                    (Some(order), _) if !order_configs.contains_key(&order.datum.config_token) => {
                        serde_json::json!({
                            "state": "config-missing",
                            "config_token": hex::encode(&order.datum.config_token),
                            "detail": "the order references an OrderConfig token that \
                                       is not on chain; it cannot execute until that \
                                       config is published (or the order is re-placed \
                                       against an existing config)",
                        })
                    }
                    (Some(_), Some(crate::sundaev4::intents::ExecutionHint::Claim { pool }))
                        if !visible.keys().any(|id| &hex::encode(id.to_bytes()) == pool)
                            && pools.keys().any(|id| &hex::encode(id.to_bytes()) == pool) =>
                    {
                        serde_json::json!({
                            "state": "not-permitted",
                            "pool": pool,
                            "detail": "this scooper does not serve the order's credentials \
                                       on that pool",
                        })
                    }
                    (
                        Some(order),
                        Some(crate::sundaev4::intents::ExecutionHint::Claim { pool }),
                    ) => Self::probe_claim(order, i, pool, &visible),
                    (Some(order), None) => {
                        let probe = Self::probe_swap(order, i, &visible);
                        // A route that runs only through pools this order is
                        // barred from would otherwise read as "no-route" or
                        // "below-floor".
                        if visible.len() < pools.len()
                            && probe["state"] != "dispatchable"
                            && Self::probe_swap(order, i, &pools)["state"] == "dispatchable"
                        {
                            serde_json::json!({
                                "state": "not-permitted",
                                "detail": "a route exists, but only through pools where \
                                           this scooper does not serve the order's credentials",
                            })
                        } else {
                            probe
                        }
                    }
                };
                serde_json::json!({
                    "status": "pending",
                    "expiry_ms": i.expiry_ms,
                    "received_at_ms": i.received_at_ms,
                    "dispatch": probe,
                })
            }
        };
        Self::json_response(body.to_string())
    }

    /// Would this swap intent route right now? Reports the numbers behind
    /// the verdict: what's offered, the floor, the best achievable output,
    /// and the gap when it falls short.
    fn probe_swap(
        order: &crate::sundaev4::SundaeV4Order,
        intent: &crate::sundaev4::intents::StoredIntent,
        pools: &BTreeMap<crate::sundaev3::Ident, Arc<crate::sundaev4::SundaeV4Pool>>,
    ) -> serde_json::Value {
        use crate::sundaev4::{intents, router};
        let Some(constraint) = intents::synthesize_swap_constraint(order, &intent.sse) else {
            return serde_json::json!({
                "state": "shape-unsupported",
                "detail": "intent does not reduce to a single-asset swap \
                           against the order's holdings",
            });
        };
        let (Some((offered, amount)), Some((receive, floor))) =
            (constraint.swap_offered(), constraint.swap_min_received())
        else {
            return serde_json::json!({
                "state": "shape-unsupported",
                "detail": "synthesized constraint is missing an offered or \
                           min_received side",
            });
        };
        let mut body = serde_json::json!({
            "offered": { "asset": offered, "amount": amount.to_string() },
            "receive": { "asset": receive, "floor": floor.to_string() },
        });
        let obj = body.as_object_mut().unwrap();
        match router::find_optimal_route(
            pools,
            &[],
            offered,
            receive,
            amount,
            router::RoutingLimits::unlimited(),
        ) {
            Some(plan) if &plan.total_output >= floor => {
                obj.insert("state".into(), "dispatchable".into());
                obj.insert(
                    "expected_output".into(),
                    plan.total_output.to_string().into(),
                );
                obj.insert(
                    "surplus".into(),
                    (&plan.total_output - floor).to_string().into(),
                );
                obj.insert("hops".into(), plan.hops.len().into());
            }
            Some(plan) => {
                obj.insert("state".into(), "below-floor".into());
                obj.insert(
                    "expected_output".into(),
                    plan.total_output.to_string().into(),
                );
                obj.insert(
                    "shortfall".into(),
                    (floor - &plan.total_output).to_string().into(),
                );
                obj.insert(
                    "detail".into(),
                    "the best route's output is under the intent's \
                     min_received floor"
                        .into(),
                );
            }
            None => {
                obj.insert("state".into(), "no-route".into());
                obj.insert(
                    "detail".into(),
                    "no pool path currently connects the offered asset to \
                     the receive asset"
                        .into(),
                );
            }
        }
        body
    }

    /// Would this claim intent execute right now, and for how much?
    /// Reports the receive floor vs holdings, the achievable swap + bounty,
    /// the gap when short, and the pool's per-asset value deviation so the
    /// submitter can see which side the pool is off and by how much.
    fn probe_claim(
        order: &crate::sundaev4::SundaeV4Order,
        intent: &crate::sundaev4::intents::StoredIntent,
        pool_hex: &str,
        pools: &BTreeMap<crate::sundaev3::Ident, Arc<crate::sundaev4::SundaeV4Pool>>,
    ) -> serde_json::Value {
        use crate::bigint::BigInt;
        use crate::sundaev4::PoolType;
        use crate::sundaev4::claims;

        let Some(pool) =
            pools.iter().find(|(id, _)| hex::encode(id.to_bytes()) == pool_hex).map(|(_, p)| p)
        else {
            return serde_json::json!({
                "state": "pool-not-found",
                "pool": pool_hex,
                "detail": "the hinted pool ident is not in the indexed pool set",
            });
        };
        // SUN-310: claims work at any balance_fee (0 = full waiver); the op
        // portion pays balance_fee and the fee flows through the transcript.
        let PoolType::ConstantSum {
            prices,
            bounty_k,
            balance_fee,
            ..
        } = &pool.pool_type
        else {
            return serde_json::json!({
                "state": "pool-not-claimable",
                "pool": pool_hex,
                "detail": "claims are only supported against constant-sum pools",
            });
        };
        let assets = &pool.pool_datum.assets;

        // Per-asset value deviation: n·p_i·r_i − V. Positive means the pool
        // holds a surplus of that asset (a claim can drain it); negative
        // means a deficit (a claim must top it up). All in V's value units.
        let v = claims::compute_v(assets, prices);
        let n_big = BigInt::from(assets.len() as u64);
        let deviation: Vec<serde_json::Value> = assets
            .iter()
            .zip(prices.iter())
            .map(|((asset, reserve), price)| {
                serde_json::json!({
                    "asset": asset,
                    "value_deviation": (&(&(&n_big * price) * reserve) - &v).to_string(),
                })
            })
            .collect();

        let mut body = serde_json::json!({
            "pool": pool_hex,
            "pool_deviation": deviation,
        });
        let obj = body.as_object_mut().unwrap();

        let shape = match claims::resolve_claim_shape(
            &order.value,
            &intent.sse.execution.min_received,
            assets,
            prices,
        ) {
            Ok(claims::ResolvedShape::Pair(shape)) => shape,
            Ok(claims::ResolvedShape::Rebalance(r)) => {
                obj.insert("mode".into(), "rebalance".into());
                let legs: Vec<serde_json::Value> = assets
                    .iter()
                    .enumerate()
                    .map(|(i, (asset, _))| {
                        serde_json::json!({
                            "asset": asset,
                            "held": r.held[i].to_string(),
                            "target": r.targets[i].to_string(),
                        })
                    })
                    .collect();
                obj.insert("legs".into(), legs.into());
                match claims::plan_rebalance_claim(
                    assets,
                    prices,
                    (&bounty_k.num, &bounty_k.den),
                    (&balance_fee.num, &balance_fee.den),
                    &r.held,
                    &r.targets,
                ) {
                    Ok(plan) => {
                        obj.insert("state".into(), "claimable".into());
                        obj.insert(
                            "achievable".into(),
                            serde_json::json!({
                                "claim_asset": assets[plan.claim_idx].0,
                                "claim": plan.claim.to_string(),
                            }),
                        );
                    }
                    Err(reason) => {
                        // cap_b is a wait-and-see: the pool just isn't
                        // imbalanced enough yet (on a fee-charging pool, that
                        // is the neutral zone doing its job). A claim that
                        // can't be declared at all means the order's own
                        // targets don't leave room for the pool's fee — the
                        // signer has to move them, same as any below-floor.
                        let state = if reason.contains("cap_b") {
                            "awaiting-imbalance"
                        } else if reason.contains("no bounty to claim") {
                            "below-floor"
                        } else {
                            "shape-unsupported"
                        };
                        obj.insert("state".into(), state.into());
                        obj.insert("detail".into(), reason.into());
                    }
                }
                return body;
            }
            Err(reason) => {
                obj.insert("state".into(), "shape-unsupported".into());
                obj.insert("detail".into(), reason.into());
                return body;
            }
        };
        obj.insert("mode".into(), "pair".into());

        let needed = &shape.min_recv - &shape.already_held;
        obj.insert(
            "receive".into(),
            serde_json::json!({
                "asset": assets[shape.out_idx].0,
                "floor": shape.min_recv.to_string(),
                "already_held": shape.already_held.to_string(),
                "needed": needed.to_string(),
            }),
        );
        obj.insert(
            "spend".into(),
            serde_json::json!({
                "asset": assets[shape.in_idx].0,
                "spendable": shape.spendable.to_string(),
            }),
        );

        match claims::plan_claim_meeting_floor(
            assets,
            prices,
            (&bounty_k.num, &bounty_k.den),
            (&balance_fee.num, &balance_fee.den),
            shape.in_idx,
            shape.out_idx,
            &shape.spendable,
            &needed,
        ) {
            Some(search) => {
                let plan = &search.plan;
                let total = &plan.dy + &plan.claim;
                obj.insert(
                    "achievable".into(),
                    serde_json::json!({
                        "dx": plan.dx.to_string(),
                        "dy": plan.dy.to_string(),
                        "claim": plan.claim.to_string(),
                        "total": total.to_string(),
                    }),
                );
                if search.meets_floor {
                    obj.insert("state".into(), "claimable".into());
                } else {
                    obj.insert("state".into(), "below-floor".into());
                    obj.insert("shortfall".into(), (&needed - &total).to_string().into());
                    obj.insert(
                        "detail".into(),
                        "even spending the order's full input budget, the \
                         achievable swap + bounty is under the intent's floor"
                            .into(),
                    );
                }
            }
            None => {
                obj.insert("state".into(), "awaiting-imbalance".into());
                obj.insert(
                    "detail".into(),
                    "no positive bounty in this direction at current \
                     reserves — see pool_deviation for which side the pool \
                     is off"
                        .into(),
                );
            }
        }
        body
    }

    async fn do_call(self, req: Request<IncomingBody>) -> Response<ResponseBody> {
        let path = req.uri().path().to_string();

        if self.visibility == Visibility::Public
            && !(path.starts_with("/v4/strategy-intents") || path == "/health")
        {
            return Self::error_response(hyper::StatusCode::NOT_FOUND, "unknown path");
        }

        if path == "/v4/strategy-intents" {
            return match *req.method() {
                hyper::Method::POST => self.post_strategy_intent(req).await,
                hyper::Method::GET => self.list_strategy_intents().await,
                _ => Self::error_response(hyper::StatusCode::METHOD_NOT_ALLOWED, "use GET or POST"),
            };
        }
        if let Some(id_hex) = path.strip_prefix("/v4/strategy-intents/") {
            let id_hex = id_hex.to_string();
            return self.strategy_intent_status(&id_hex).await;
        }

        match path.as_str() {
            "/dashboard" => self.serve_dashboard(),
            "/events" => self.serve_sse(),
            "/failures" => Self::json_response(
                serde_json::to_string(&self.metrics.failures_snapshot()).unwrap(),
            ),
            "/status" => Self::json_response(self.serve_status().await),
            "/resync-from-acropolis" => {
                let _ = self.resync_tx.send(());
                Self::text_response("resync")
            }
            "/health" => Self::json_response(self.serve_health_stats().await),
            "/metrics" => self.serve_metrics().await,
            // POST toggles; GET just reports. A GET that mutated state let
            // any stray request (scanner, browser prefetch, curl typo)
            // silently stop all scooping — which once went unnoticed for
            // days because the toggle also didn't log.
            "/pause" => match *req.method() {
                hyper::Method::POST => {
                    let was_paused = self.paused.fetch_xor(true, Ordering::Relaxed);
                    let now_paused = !was_paused;
                    let remote =
                        self.remote_addr.map(|a| a.to_string()).unwrap_or_else(|| "unknown".into());
                    let forwarded_for = req
                        .headers()
                        .get("x-forwarded-for")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("-");
                    tracing::warn!(
                        %remote,
                        forwarded_for,
                        paused = now_paused,
                        "scooping {} via /pause",
                        if now_paused { "PAUSED" } else { "RESUMED" },
                    );
                    Self::json_response(
                        serde_json::to_string(&serde_json::json!({ "paused": now_paused }))
                            .unwrap(),
                    )
                }
                hyper::Method::GET => Self::json_response(
                    serde_json::to_string(&serde_json::json!({
                        "paused": self.paused.load(Ordering::Relaxed),
                    }))
                    .unwrap(),
                ),
                _ => Self::error_response(
                    hyper::StatusCode::METHOD_NOT_ALLOWED,
                    "POST to toggle, GET to read",
                ),
            },
            _ => {
                let body = self.route_protocol(&path).await;
                if body == "unknown" {
                    return Self::error_response(hyper::StatusCode::NOT_FOUND, "unknown path");
                }
                Self::json_response(body)
            }
        }
    }

    fn serve_dashboard(&self) -> Response<ResponseBody> {
        const HTML: &str = include_str!("../static/dashboard.html");
        Response::builder()
            .header("Content-Type", "text/html; charset=utf-8")
            .body(Either::Left(Full::new(Bytes::from(HTML))))
            .unwrap()
    }

    async fn serve_status(&self) -> String {
        let v3_info = if let Some(v3) = &self.v3_state {
            let state = v3.lock().await.latest().into_owned();
            serde_json::json!({
                "configured": true,
                "pool_count": state.pools.len(),
                "order_count": state.orders.len(),
            })
        } else {
            serde_json::json!({ "configured": false })
        };

        let strategy_intents = match &self.intents {
            Some(intents) => intents.store.lock().await.summary(),
            None => serde_json::Value::Null,
        };
        let in_flight = self.metrics.in_flight_snapshot();
        let quarantine = self.metrics.quarantine_snapshot();
        let ops = self.metrics.ops_snapshot();
        let mempool = serde_json::json!({
            "connected": self.metrics.mempool_connected.load(std::sync::atomic::Ordering::Relaxed) == 1,
            "last_snapshot_unix": self.metrics.mempool_last_snapshot_unix.load(std::sync::atomic::Ordering::Relaxed),
            "txs_seen": self.metrics.mempool_txs_seen.load(std::sync::atomic::Ordering::Relaxed),
            "confirmed": self.metrics.mempool_confirmed.load(std::sync::atomic::Ordering::Relaxed),
            "evicted": self.metrics.mempool_evicted.load(std::sync::atomic::Ordering::Relaxed),
            "node_rejects": self.metrics.node_rejects.load(std::sync::atomic::Ordering::Relaxed),
        });

        let v4_info = if let Some(v4) = &self.v4_state {
            let state = v4.lock().await.latest().into_owned();
            let sync_pct = match state.network_tip_slot {
                Some(net) if net > 0 => (state.tip_slot as f64 / net as f64) * 100.0,
                _ => 0.0,
            };
            serde_json::json!({
                "configured": true,
                "pool_count": state.pools.len(),
                "order_count": state.orders.len(),
                "tip_slot": state.tip_slot,
                "network_tip_slot": state.network_tip_slot,
                "sync_pct": sync_pct,
                "strategy_intents": strategy_intents,
                "in_flight_pools": in_flight.pool_ids,
                "in_flight_orders": in_flight.order_refs,
                "quarantine": quarantine,
                "ops": ops,
                "mempool": mempool,
            })
        } else {
            serde_json::json!({ "configured": false })
        };

        serde_json::to_string(&serde_json::json!({
            "network": self.network,
            "v3": v3_info,
            "v4": v4_info,
            "paused": self.paused.load(Ordering::Relaxed),
        }))
        .unwrap()
    }

    async fn serve_health_stats(&self) -> String {
        let Some(v4) = &self.v4_state else {
            return serde_json::to_string(&serde_json::json!({
                "error": "v4 indexer not configured"
            }))
            .unwrap();
        };
        let state = v4.lock().await.latest().into_owned();

        let sync_pct = match state.network_tip_slot {
            Some(net) if net > 0 => (state.tip_slot as f64 / net as f64) * 100.0,
            _ => 0.0,
        };

        let ada_balance: f64 = state
            .wallet_utxos
            .values()
            .map(|v| {
                let lovelace = v.get(&crate::cardano_types::AssetClass {
                    policy: vec![],
                    token: vec![],
                });
                lovelace.to_f64().unwrap_or(0.0) / 1_000_000.0
            })
            .sum();

        let orders_pending = state.orders.len();
        let stats = &state.scoop_stats;

        serde_json::to_string(&serde_json::json!({
            "sync": {
                "tip_slot": state.tip_slot,
                "network_tip_slot": state.network_tip_slot,
                "sync_pct": sync_pct,
            },
            "pools": {
                "count": state.pools.len(),
                "orders_pending": orders_pending,
            },
            "wallet": {
                "ada_balance": ada_balance,
                "utxo_count": state.wallet_utxos.len(),
            },
            "our_keyhash": stats.our_keyhash,
            "scooper_totals": stats.scooper_totals,
            "recent_scoops": stats.recent_scoops,
        }))
        .unwrap()
    }

    async fn serve_metrics(&self) -> Response<ResponseBody> {
        let body = crate::metrics::render_metrics(
            &self.v3_state,
            &self.v4_state,
            &self.paused,
            &self.metrics,
        )
        .await;
        Response::builder()
            .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            .header("Access-Control-Allow-Origin", "*")
            .body(Either::Left(Full::new(Bytes::from(body))))
            .unwrap()
    }

    fn serve_sse(&self) -> Response<ResponseBody> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(64);
        let mut event_rx = self.event_tx.subscribe();

        tokio::spawn(async move {
            // Heartbeat keeps intermediaries from silently killing the idle
            // connection AND gives the client a liveness signal: a dashboard
            // that hasn't heard anything for ~45s knows its EventSource is a
            // zombie and reconnects + reloads, instead of showing stale
            // state under a green dot.
            let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(15));
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                let received = tokio::select! {
                    r = event_rx.recv() => r,
                    _ = heartbeat.tick() => {
                        if tx.send(Bytes::from("event: heartbeat\ndata: {}\n\n")).await.is_err() {
                            return;
                        }
                        continue;
                    }
                };
                match received {
                    Ok((_slot, events)) => {
                        for event in events {
                            let (event_type, data) = format_sse_event(&event);
                            let msg = format!("event: {event_type}\ndata: {data}\n\n");
                            if tx.send(Bytes::from(msg)).await.is_err() {
                                return; // client disconnected
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        let msg = format!("event: error\ndata: {{\"lagged\":{n}}}\n\n");
                        if tx.send(Bytes::from(msg)).await.is_err() {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Response::builder()
            .header("Content-Type", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .header("Connection", "keep-alive")
            .header("Access-Control-Allow-Origin", "*")
            .body(Either::Right(SseBody { rx }))
            .unwrap()
    }

    async fn route_protocol(&self, path: &str) -> String {
        // /v4/... routes
        if let Some(v4_path) = path.strip_prefix("/v4") {
            return self.route_v4(v4_path).await;
        }

        // /v3/... routes and backward-compatible aliases (/ → /v3/)
        let v3_path = path.strip_prefix("/v3").unwrap_or(path);

        if let Some(pool_id) = v3_path.strip_prefix("/pool/") {
            return self.v3_query_pool(pool_id).await;
        }

        match v3_path {
            "/pools" => self.v3_list_pools().await,
            "/orders" => self.v3_list_orders().await,
            "/spent-orders" => self.v3_list_spent_orders().await,
            "/spent-pools" => self.v3_list_spent_pools().await,
            _ => "unknown".into(),
        }
    }

    async fn route_v4(&self, path: &str) -> String {
        if let Some(pool_id) = path.strip_prefix("/pool/") {
            return self.v4_query_pool(pool_id).await;
        }

        match path {
            "/pools" => self.v4_list_pools().await,
            "/orders" => self.v4_list_orders().await,
            "/spent-orders" => self.v4_list_spent_orders().await,
            "/spent-pools" => self.v4_list_spent_pools().await,
            "/protocol" => self.v4_protocol_stats().await,
            _ => "unknown".into(),
        }
    }

    async fn v3_query_pool(&self, pool_id: &str) -> String {
        let Some(v3) = &self.v3_state else {
            return "v3 indexer not configured".into();
        };
        let state = v3.lock().await.latest().into_owned();
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
            if let Err(err) =
                validate_order(&order.datum, &order.value, &pool.pool_datum, &pool.value)
            {
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
        serde_json::to_string(&response).unwrap()
    }

    async fn v3_list_pools(&self) -> String {
        let Some(v3) = &self.v3_state else {
            return "v3 indexer not configured".into();
        };
        let state = v3.lock().await.latest().into_owned();
        let mut json_map = serde_json::Map::new();

        for (ident, pool) in state.pools {
            json_map.insert(
                hex::encode(ident.to_bytes()),
                serde_json::to_value(pool).unwrap(),
            );
        }

        serde_json::to_string_pretty(&json_map).unwrap()
    }

    async fn v3_list_orders(&self) -> String {
        let Some(v3) = &self.v3_state else {
            return "v3 indexer not configured".into();
        };
        let state = v3.lock().await.latest().into_owned();

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
                    tracing::error!("Failed to serialize order {:?}: {}", order.datum.ident, e);
                    continue;
                }
            }
        }

        if !state.invalid_orders.is_empty() {
            json_map.insert(
                "invalid".to_string(),
                serde_json::to_value(&state.invalid_orders).unwrap(),
            );
        }

        serde_json::to_string_pretty(&json_map).unwrap()
    }

    async fn v3_list_spent_orders(&self) -> String {
        let Some(v3) = &self.v3_state else {
            return "v3 indexer not configured".into();
        };
        let state = v3.lock().await.latest().into_owned();
        serde_json::to_string(&state.spent_orders).unwrap()
    }

    async fn v3_list_spent_pools(&self) -> String {
        let Some(v3) = &self.v3_state else {
            return "v3 indexer not configured".into();
        };
        let state = v3.lock().await.latest().into_owned();
        serde_json::to_string(&state.spent_pools).unwrap()
    }

    // ── V4 helpers ──────────────────────────────────────────────────────

    /// Serialize a V4 pool to JSON, enriching `module_state` with preimage values.
    fn v4_pool_json(
        pool: &crate::sundaev4::SundaeV4Pool,
        preimages: &ModuleStatePreimages,
    ) -> serde_json::Value {
        let mut pool_json = serde_json::to_value(pool).unwrap_or_default();
        // A stableswap pool's config is per pool (and its rates move on
        // chain), so its preimage comes from the pool record, not the
        // startup map.
        let ss_preimage: Option<(Vec<u8>, Vec<u8>)> =
            if let crate::sundaev4::PoolType::StableSwap { config } = &pool.pool_type {
                use plutus_parser::AsPlutus;
                let cbor = minicbor::to_vec(config.clone().to_plutus()).unwrap_or_default();
                Some((
                    pallas_crypto::hash::Hasher::<256>::hash(&cbor).to_vec(),
                    cbor,
                ))
            } else {
                None
            };
        if let Some(obj) = pool_json.get_mut("pool_datum").and_then(|d| d.get_mut("module_state")) {
            let mut enriched = serde_json::Map::new();
            for (module_hash, state_bytes) in &pool.pool_datum.module_state {
                let state_hex = hex::encode(state_bytes);
                // Try to find a preimage whose blake2b-256 matches state_bytes
                let value_hex = preimages.get(state_bytes).map(hex::encode).or_else(|| {
                    ss_preimage
                        .as_ref()
                        .filter(|(h, _)| h == state_bytes)
                        .map(|(_, cbor)| hex::encode(cbor))
                });
                let mut entry = serde_json::Map::new();
                entry.insert("state".into(), state_hex.into());
                if let Some(v) = value_hex {
                    entry.insert("value".into(), v.into());
                }
                enriched.insert(hex::encode(module_hash), serde_json::Value::Object(entry));
            }
            *obj = serde_json::Value::Object(enriched);
        }
        pool_json
    }

    // ── V4 endpoints ─────────────────────────────────────────────────────

    async fn v4_query_pool(&self, pool_id: &str) -> String {
        let Some(v4) = &self.v4_state else {
            return "v4 indexer not configured".into();
        };
        let Some(_fee) = self.v4_fee else {
            return "v4 execution not configured (no fee)".into();
        };
        let state = v4.lock().await.latest().into_owned();
        let id_bytes = match hex::decode(pool_id) {
            Ok(b) => b,
            Err(_) => return "invalid hex in pool id".into(),
        };
        let ident = Ident::new(&id_bytes);
        let pool = match state.pools.get(&ident) {
            Some(p) => p.clone(),
            None => return "No such pool".into(),
        };

        let groups = batch::group_orders_by_pool(&state.orders, &state.pools);
        let candidates = groups.get(&ident).cloned().unwrap_or_default();

        let mut executable = Vec::new();
        let mut non_executable = Vec::new();

        for order in &candidates {
            if let Err(gate) = self.v4_swap_gate(&state, &ident, order) {
                non_executable.push(serde_json::json!({
                    "order": order.input.to_string(),
                    "reason": gate.reason(),
                }));
                continue;
            }
            match batch::check_order_executability(
                order,
                &pool.pool_datum.assets,
                &pool.pool_datum.total_lp,
                &pool.pool_type,
            ) {
                Ok(swap) => {
                    executable.push(serde_json::json!({
                        "order": order.input.to_string(),
                        "output_amount": swap.dy.to_string(),
                    }));
                }
                Err(reason) => {
                    non_executable.push(serde_json::json!({
                        "order": order.input.to_string(),
                        "reason": reason,
                    }));
                }
            }
        }

        let response = serde_json::json!({
            "pool": Self::v4_pool_json(&pool, &self.v4_module_preimages),
            "executable": executable,
            "non_executable": non_executable,
        });
        serde_json::to_string_pretty(&response).unwrap()
    }

    /// Why dispatch would skip a swap before even pricing it. Mirrors the
    /// scooper's own gates so the dashboard agrees with what dispatch does.
    fn v4_swap_gate(
        &self,
        state: &crate::sundaev4::SundaeV4State,
        ident: &Ident,
        order: &crate::sundaev4::SundaeV4Order,
    ) -> Result<(), SwapGate> {
        // An order naming an unindexed OrderConfig can never validate on chain.
        if !state.order_configs.contains_key(&order.datum.config_token) {
            return Err(SwapGate::ConfigMissing(hex::encode(
                &order.datum.config_token,
            )));
        }
        if !self.v4_pool_allowlists.permits(ident, order, order.constraint.strategy()) {
            return Err(SwapGate::Restricted);
        }
        // The router's fan-out gate: an order's max_per_execution buys its
        // route budget, and one that can't afford a single pool touch will
        // never dispatch, no matter how in-range the swap is. Without this
        // check the dashboard calls such orders executable while dispatch
        // logs "no route" every cycle (seen live with a max_per_execution of
        // 10,000 lovelace against a 1 ADA/pool routing cost).
        if let Some((cost_per_pool, cost_per_step)) = self.v4_routing_costs {
            use num_traits::ToPrimitive;
            let per_exec = order.datum.max_per_execution.clone().unwrap().to_u64().unwrap_or(0);
            let base_fee = state.fee_settings.as_ref().map(|f| f.base_fee).unwrap_or(0);
            let limits = crate::sundaev4::router::RoutingLimits::from_budget(
                per_exec,
                base_fee,
                cost_per_pool,
                cost_per_step,
            );
            if limits.max_pools < 1 || limits.max_steps < 1 {
                return Err(SwapGate::RouteBudget {
                    per_exec,
                    cost_per_pool,
                    cost_per_step,
                });
            }
        }
        Ok(())
    }

    /// Protocol-wide aggregates for the dashboard's Protocol tab: pools by
    /// type, reserves, accrued protocol equity, open orders and their
    /// executability, and scoop activity over recent windows. Everything
    /// here is derived from indexed state plus the scoop-record table; no
    /// prices are involved, the client prices assets itself.
    async fn v4_protocol_stats(&self) -> String {
        use crate::cardano_types::AssetClass;
        use crate::sundaev4::{Constraint, PoolType};
        use num_traits::{Signed, Zero};

        let Some(v4) = &self.v4_state else {
            return serde_json::to_string(&serde_json::json!({
                "error": "v4 indexer not configured"
            }))
            .unwrap();
        };
        let state = v4.lock().await.latest().into_owned();

        fn asset_list(map: BTreeMap<AssetClass, crate::bigint::BigInt>) -> Vec<serde_json::Value> {
            map.into_iter()
                .filter(|(_, amount)| amount.is_positive())
                .map(|(asset, amount)| {
                    serde_json::json!({ "asset": asset, "amount": amount.to_string() })
                })
                .collect()
        }
        fn add_to(
            map: &mut BTreeMap<AssetClass, crate::bigint::BigInt>,
            asset: &AssetClass,
            amount: crate::bigint::BigInt,
        ) {
            *map.entry(asset.clone()).or_insert_with(crate::bigint::BigInt::zero) += amount;
        }

        // ── Pools: counts by type, reserves, accrued protocol equity ──
        let mut by_type: BTreeMap<&str, u64> = BTreeMap::new();
        let mut tvl: BTreeMap<AssetClass, crate::bigint::BigInt> = BTreeMap::new();
        let mut equity_total: BTreeMap<AssetClass, crate::bigint::BigInt> = BTreeMap::new();
        let mut pool_rows = Vec::new();
        // Lovelace on each pool UTxO above its ADA reserve. Base fees are
        // delivered as an exact surplus on the designated pool (ADR-0010),
        // outside the datum's reserve accounting, so this is where base-fee
        // income accumulates. It also holds each pool's create-time ADA
        // buffer, which the datum does not record (only the min_surplus
        // floor), so it overstates fees collected by that deposit.
        let mut surplus_total = crate::bigint::BigInt::zero();
        let mut empty_pools = 0u64;
        let mut restricted_pools = 0u64;
        for (ident, pool) in &state.pools {
            let kind = match &pool.pool_type {
                PoolType::ConstantProduct { .. } => "cp",
                PoolType::ConstantSum { .. } => "cs",
                PoolType::ConcentratedLiquidity { .. } => "cl",
                PoolType::StableSwap { .. } => "ss",
            };
            *by_type.entry(kind).or_default() += 1;
            if self.v4_pool_allowlists.is_restricted(ident) {
                restricted_pools += 1;
            }
            let pd = &pool.pool_datum;
            if pd.assets.iter().any(|(_, r)| !r.is_positive()) {
                empty_pools += 1;
            }
            // The protocol's fee accrues as LP minted into total_lp but not
            // into circulation. Its share of every reserve is the accrued,
            // unclaimed protocol equity — the same figure the V4 pool table
            // shows per pool.
            let protocol_lp = pd.total_lp.clone() - &pd.circulating_lp;
            let mut equity: BTreeMap<AssetClass, crate::bigint::BigInt> = BTreeMap::new();
            for (asset, reserve) in &pd.assets {
                add_to(&mut tvl, asset, reserve.clone());
                if pd.total_lp.is_positive() && protocol_lp.is_positive() {
                    let share = reserve.clone() * &protocol_lp / &pd.total_lp;
                    add_to(&mut equity_total, asset, share.clone());
                    add_to(&mut equity, asset, share);
                }
            }
            let reserve_ada = pd
                .assets
                .iter()
                .find(|(a, _)| *a == crate::cardano_types::ADA_ASSET_CLASS)
                .map(|(_, r)| r.clone())
                .unwrap_or_else(crate::bigint::BigInt::zero);
            let surplus = pool.value.get(&crate::cardano_types::ADA_ASSET_CLASS) - &reserve_ada;
            surplus_total += &surplus;
            let protocol_share = pool
                .fee_split_config
                .as_ref()
                .map(|c| serde_json::to_value(&c.protocol_share).unwrap_or_default());
            pool_rows.push(serde_json::json!({
                "pool_id": hex::encode(ident.to_bytes()),
                "pool_type": kind,
                "reserves": asset_list(pd.assets.iter().cloned().collect()),
                "total_lp": pd.total_lp.to_string(),
                "protocol_lp": protocol_lp.to_string(),
                "protocol_equity": asset_list(equity),
                "surplus_lovelace": surplus.to_string(),
                "min_surplus": pd.min_surplus.to_string(),
                "protocol_share": protocol_share,
                "restricted": self.v4_pool_allowlists.is_restricted(ident),
            }));
        }

        // ── Orders: by kind, and swap executability against current pools ──
        let mut by_kind: BTreeMap<&str, u64> = BTreeMap::new();
        let mut oldest_slot: Option<u64> = None;
        for order in &state.orders {
            let kind = match &order.constraint {
                Constraint::Swap { .. } => "swap",
                Constraint::Deposit { .. } => "deposit",
                Constraint::Withdraw { .. } => "withdraw",
                Constraint::Claim { .. } => "claim",
                Constraint::Strategy { .. } => "strategy",
            };
            *by_kind.entry(kind).or_default() += 1;
            oldest_slot = Some(oldest_slot.map_or(order.slot, |s| s.min(order.slot)));
        }
        let groups = batch::group_orders_by_pool(&state.orders, &state.pools);
        let matched_swaps: u64 = groups.values().map(|v| v.len() as u64).sum();
        let unmatched_swaps =
            by_kind.get("swap").copied().unwrap_or(0).saturating_sub(matched_swaps);
        let mut executable = 0u64;
        let mut blocked: BTreeMap<&str, u64> = BTreeMap::new();
        let mut executable_volume: BTreeMap<AssetClass, crate::bigint::BigInt> = BTreeMap::new();
        let mut executable_output: BTreeMap<AssetClass, crate::bigint::BigInt> = BTreeMap::new();
        for (ident, candidates) in &groups {
            let Some(pool) = state.pools.get(ident) else {
                continue;
            };
            for order in candidates {
                if let Err(gate) = self.v4_swap_gate(&state, ident, order) {
                    *blocked.entry(gate.bucket()).or_default() += 1;
                    continue;
                }
                match batch::check_order_executability(
                    order,
                    &pool.pool_datum.assets,
                    &pool.pool_datum.total_lp,
                    &pool.pool_type,
                ) {
                    Ok(swap) => {
                        executable += 1;
                        let (offered, dx) = order.swap_offered();
                        add_to(&mut executable_volume, offered, dx.clone());
                        add_to(
                            &mut executable_output,
                            &pool.pool_datum.assets[swap.output_idx].0,
                            swap.dy.clone(),
                        );
                    }
                    Err(reason) => {
                        // Stable keys for check_order_executability's messages.
                        let bucket = if reason.starts_with("below min_received") {
                            "below_min_received"
                        } else if reason.starts_with("swap would take")
                            || reason.starts_with("swap of")
                            || reason.starts_with("stableswap")
                        {
                            "zero_output"
                        } else if reason.starts_with("no matching pool asset") {
                            "asset_mismatch"
                        } else {
                            "other"
                        };
                        *blocked.entry(bucket).or_default() += 1;
                    }
                }
            }
        }

        // ── Activity: scoop records over recent windows, from the DB ──
        // Slots are seconds on every post-Byron network, so a window in
        // seconds is a window in slots off the indexed tip.
        const WINDOWS: [(&str, u64); 3] = [("1h", 3_600), ("24h", 86_400), ("7d", 604_800)];
        let horizon = state.tip_slot.saturating_sub(WINDOWS[WINDOWS.len() - 1].1);
        let records = match &self.v4_dao {
            Some(dao) => dao.load_scoop_records_since(horizon).await.unwrap_or_else(|e| {
                tracing::warn!("protocol stats: scoop record window query failed: {e:#}");
                Vec::new()
            }),
            None => Vec::new(),
        };
        let windows: Vec<serde_json::Value> = WINDOWS
            .iter()
            .map(|(label, secs)| {
                let since = state.tip_slot.saturating_sub(*secs);
                let mut txs = std::collections::BTreeSet::new();
                let mut pools = std::collections::BTreeSet::new();
                let mut scoopers = std::collections::BTreeSet::new();
                let mut orders = 0u64;
                for r in records.iter().filter(|r| r.slot >= since) {
                    txs.insert(&r.tx_id);
                    pools.insert(&r.pool_id);
                    scoopers.insert(&r.scooper);
                    orders += r.n_orders as u64;
                }
                serde_json::json!({
                    "label": label,
                    "seconds": secs,
                    "scoop_txs": txs.len(),
                    "orders": orders,
                    "pools": pools.len(),
                    "scoopers": scoopers.len(),
                })
            })
            .collect();
        let stats = &state.scoop_stats;
        let all_time = serde_json::json!({
            "scoop_txs": stats.scooper_totals.iter().map(|t| t.scoop_txs).sum::<u64>(),
            "orders": stats.scooper_totals.iter().map(|t| t.orders_processed).sum::<u64>(),
            "scoopers": stats.scooper_totals.len(),
        });

        serde_json::to_string(&serde_json::json!({
            "tip_slot": state.tip_slot,
            "network_tip_slot": state.network_tip_slot,
            "pools": {
                "total": state.pools.len(),
                "by_type": by_type,
                "empty": empty_pools,
                "restricted": restricted_pools,
                "tvl": asset_list(tvl),
            },
            "protocol_equity": {
                "total": asset_list(equity_total),
                "surplus_lovelace": surplus_total.to_string(),
                "pools": pool_rows,
            },
            "orders": {
                "open": state.orders.len(),
                "by_kind": by_kind,
                "invalid": state.invalid_orders.len(),
                "oldest_slot": oldest_slot,
                "swaps": {
                    "matched": matched_swaps,
                    "unmatched": unmatched_swaps,
                    "executable": executable,
                    "blocked": blocked,
                    "executable_volume": asset_list(executable_volume),
                    "executable_output": asset_list(executable_output),
                },
            },
            "activity": {
                "windows": windows,
                "all_time": all_time,
                "records_available": self.v4_dao.is_some(),
            },
            "fees": {
                "scooper_base_fee_lovelace": state.fee_settings.as_ref().map(|f| f.base_fee),
                "protocol_share": self.v4_protocol_share,
            },
        }))
        .unwrap()
    }

    async fn v4_list_pools(&self) -> String {
        let Some(v4) = &self.v4_state else {
            return "v4 indexer not configured".into();
        };
        let state = v4.lock().await.latest().into_owned();
        let mut json_map = serde_json::Map::new();

        for (ident, pool) in &state.pools {
            let ident_hex = hex::encode(ident.to_bytes());
            let mut json = Self::v4_pool_json(pool, &self.v4_module_preimages);
            if self.v4_pool_allowlists.is_restricted(ident)
                && let Some(obj) = json.as_object_mut()
            {
                obj.insert("restricted".into(), true.into());
            }
            json_map.insert(ident_hex, json);
        }

        serde_json::to_string_pretty(&json_map).unwrap()
    }

    async fn v4_list_orders(&self) -> String {
        let Some(v4) = &self.v4_state else {
            return "v4 indexer not configured".into();
        };
        let state = v4.lock().await.latest().into_owned();
        let groups = batch::group_orders_by_pool(&state.orders, &state.pools);

        let mut json_map = serde_json::Map::new();

        for (ident, orders) in &groups {
            let order_vals: Vec<serde_json::Value> =
                orders.iter().filter_map(|o| serde_json::to_value(o.as_ref()).ok()).collect();
            json_map.insert(hex::encode(ident.to_bytes()), order_vals.into());
        }

        // Collect unmatched orders (those not in any group)
        let matched: std::collections::BTreeSet<&TransactionInput> =
            groups.values().flat_map(|orders| orders.iter().map(|o| &o.input)).collect();
        let unmatched: Vec<serde_json::Value> = state
            .orders
            .iter()
            .filter(|o| !matched.contains(&o.input))
            .filter_map(|o| serde_json::to_value(o.as_ref()).ok())
            .collect();
        if !unmatched.is_empty() {
            json_map.insert("unmatched".to_string(), unmatched.into());
        }

        if !state.invalid_orders.is_empty() {
            json_map.insert(
                "invalid".to_string(),
                serde_json::to_value(&state.invalid_orders).unwrap(),
            );
        }

        serde_json::to_string_pretty(&json_map).unwrap()
    }

    async fn v4_list_spent_orders(&self) -> String {
        let Some(v4) = &self.v4_state else {
            return "v4 indexer not configured".into();
        };
        let state = v4.lock().await.latest().into_owned();
        serde_json::to_string(&state.spent_orders).unwrap()
    }

    async fn v4_list_spent_pools(&self) -> String {
        let Some(v4) = &self.v4_state else {
            return "v4 indexer not configured".into();
        };
        let state = v4.lock().await.latest().into_owned();
        serde_json::to_string(&state.spent_pools).unwrap()
    }
}

/// Format an IndexEvent into an SSE event type name and JSON data string.
fn format_sse_event(event: &IndexEvent) -> (&'static str, String) {
    match event {
        IndexEvent::V4MempoolOrderSeen { order } => (
            "v4_mempool_order_seen",
            serde_json::json!({ "order": order.input.to_string() }).to_string(),
        ),
        IndexEvent::V4MempoolTxDropped { tx_hash } => (
            "v4_mempool_tx_dropped",
            serde_json::json!({ "tx_hash": hex::encode(tx_hash) }).to_string(),
        ),
        IndexEvent::V3PoolCreated { id, .. } => (
            "v3_pool_created",
            serde_json::json!({ "id": id.to_string() }).to_string(),
        ),
        IndexEvent::V3PoolUpdated { id, tx_id, .. } => (
            "v3_pool_updated",
            serde_json::json!({ "id": id.to_string(), "tx_id": tx_id }).to_string(),
        ),
        IndexEvent::V3PoolRemoved { id, tx_id } => (
            "v3_pool_removed",
            serde_json::json!({ "id": id.to_string(), "tx_id": tx_id }).to_string(),
        ),
        IndexEvent::V3OrderCreated { order } => (
            "v3_order_created",
            serde_json::json!({ "order": order.input.to_string() }).to_string(),
        ),
        IndexEvent::V3OrderScooped {
            order,
            pool_id,
            tx_id,
            scooper,
        } => (
            "v3_order_scooped",
            serde_json::json!({
                "order": order.input.to_string(),
                "pool_id": pool_id.to_string(),
                "tx_id": tx_id,
                "scooper": scooper,
            })
            .to_string(),
        ),
        IndexEvent::V3OrderCancelled { order, tx_id } => (
            "v3_order_cancelled",
            serde_json::json!({ "order": order.input.to_string(), "tx_id": tx_id }).to_string(),
        ),
        IndexEvent::V3SettingsUpdated { .. } => ("v3_settings_updated", "{}".to_string()),
        IndexEvent::V4PoolCreated { id, .. } => (
            "v4_pool_created",
            serde_json::json!({ "id": id.to_string() }).to_string(),
        ),
        IndexEvent::V4PoolUpdated { id, tx_id, .. } => (
            "v4_pool_updated",
            serde_json::json!({ "id": id.to_string(), "tx_id": tx_id }).to_string(),
        ),
        IndexEvent::V4PoolRemoved { id, tx_id } => (
            "v4_pool_removed",
            serde_json::json!({ "id": id.to_string(), "tx_id": tx_id }).to_string(),
        ),
        IndexEvent::V4OrderCreated { order } => (
            "v4_order_created",
            serde_json::json!({ "order": order.input.to_string() }).to_string(),
        ),
        IndexEvent::V4OrderScooped {
            order,
            pool_ids,
            tx_id,
            scooper,
        } => (
            "v4_order_scooped",
            serde_json::json!({
                "order": order.input.to_string(),
                "pool_ids": pool_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
                "tx_id": tx_id,
                "scooper": scooper,
            })
            .to_string(),
        ),
        IndexEvent::V4OrderCancelled { order, tx_id } => (
            "v4_order_cancelled",
            serde_json::json!({ "order": order.input.to_string(), "tx_id": tx_id }).to_string(),
        ),
        IndexEvent::V4SettingsUpdated { .. } => ("v4_settings_updated", "{}".to_string()),
        IndexEvent::TipAdvanced {
            slot,
            network_tip_slot,
        } => (
            "tip",
            serde_json::json!({
                "slot": slot,
                "network_tip_slot": network_tip_slot,
            })
            .to_string(),
        ),
        IndexEvent::Rollback { to_slot } => (
            "rollback",
            serde_json::json!({ "to_slot": to_slot }).to_string(),
        ),
    }
}

#[cfg(test)]
mod protocol_stats_tests {
    //! The Protocol tab's numbers come from one aggregate endpoint. This
    //! exercises it against a real pool and order from the test harness so
    //! the counts, reserves, equity and executability buckets line up with
    //! the state they were derived from.
    use super::*;
    use crate::sundaev4::test_harness::*;

    fn server_over(state: crate::sundaev4::SundaeV4State) -> AdminServer {
        let mut hist = SundaeV4HistoricalState::new();
        let slot = state.tip_slot;
        *hist.update_slot(slot).unwrap() = state;
        let (event_tx, _) = tokio::sync::broadcast::channel(1);
        let (resync_tx, _) = tokio::sync::broadcast::channel(1);
        AdminServer {
            visibility: Visibility::Private,
            network: "devnet".into(),
            v3_state: None,
            v4_state: Some(Arc::new(Mutex::new(hist))),
            v4_fee: Some((1, 1)),
            v4_routing_costs: None,
            v4_pool_allowlists: Arc::default(),
            v4_module_preimages: Arc::default(),
            v4_protocol_share: Some((1, 2)),
            v4_dao: None,
            resync_tx,
            event_tx,
            paused: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(Metrics::new()),
            intents: None,
            remote_addr: None,
        }
    }

    #[tokio::test]
    async fn aggregates_pools_orders_and_equity() {
        let env = TestEnv::from_blueprint_file("test/fixtures/devnet-blueprint.json");
        // 1000 A / 2000 B, with a quarter of the LP held by the protocol.
        let pool = make_pool(&env, 0xA1, token_a(), 1_000_000, token_b(), 2_000_000);
        let mut pool = (*pool).clone();
        pool.pool_datum.total_lp = 400.into();
        pool.pool_datum.circulating_lp = 300.into();
        // 7 ADA on the UTxO with no ADA reserve: all of it is surplus.
        pool.value.insert(&ada(), 7_000_000.into());
        let pool = Arc::new(pool);

        let mut state = crate::sundaev4::SundaeV4State::default();
        state.tip_slot = 5_000;
        state.pools.insert(pool.pool_datum.identifier.clone(), pool.clone());
        // One swap that fits, one that asks for more than the pool can pay,
        // one whose assets match no pool.
        state.orders.push(make_order(token_a(), 10_000, token_b(), 1, 100));
        state.orders.push(make_order(token_a(), 10_000, token_b(), 1_000_000_000, 200));
        state.orders.push(make_order(token_e(), 10_000, token_f(), 1, 300));
        // Index the harness's OrderConfigs so the config gate passes and the
        // pricing check is what decides.
        state.order_configs = env.order_configs.clone();

        let body = server_over(state).v4_protocol_stats().await;
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();

        assert_eq!(v["pools"]["total"], 1);
        assert_eq!(v["pools"]["by_type"]["cp"], 1);
        assert_eq!(v["pools"]["empty"], 0);
        let tvl = v["pools"]["tvl"].as_array().unwrap();
        assert_eq!(tvl.len(), 2);
        assert!(tvl.iter().any(|e| e["amount"] == "1000000"));
        assert!(tvl.iter().any(|e| e["amount"] == "2000000"));

        // Protocol equity = reserve × (400 − 300) / 400 = a quarter of each.
        let eq = v["protocol_equity"]["total"].as_array().unwrap();
        assert!(eq.iter().any(|e| e["amount"] == "250000"), "{eq:?}");
        assert!(eq.iter().any(|e| e["amount"] == "500000"), "{eq:?}");
        assert_eq!(v["protocol_equity"]["pools"][0]["protocol_lp"], "100");
        assert_eq!(
            v["protocol_equity"]["pools"][0]["surplus_lovelace"],
            "7000000"
        );
        assert_eq!(v["protocol_equity"]["surplus_lovelace"], "7000000");

        assert_eq!(v["orders"]["open"], 3);
        assert_eq!(v["orders"]["by_kind"]["swap"], 3);
        assert_eq!(v["orders"]["oldest_slot"], 100);
        let swaps = &v["orders"]["swaps"];
        assert_eq!(swaps["matched"], 2);
        assert_eq!(swaps["unmatched"], 1);
        assert_eq!(swaps["executable"], 1);
        assert_eq!(swaps["blocked"]["below_min_received"], 1);
        assert_eq!(swaps["executable_volume"][0]["amount"], "10000");

        assert_eq!(v["activity"]["records_available"], false);
        assert_eq!(v["fees"]["protocol_share"], serde_json::json!([1, 2]));
    }

    #[tokio::test]
    async fn unindexed_order_config_is_counted_not_priced() {
        let env = TestEnv::from_blueprint_file("test/fixtures/devnet-blueprint.json");
        let pool = make_pool(&env, 0xA2, token_a(), 1_000_000, token_b(), 2_000_000);
        let mut state = crate::sundaev4::SundaeV4State::default();
        state.pools.insert(pool.pool_datum.identifier.clone(), pool);
        state.orders.push(make_order(token_a(), 10_000, token_b(), 1, 100));

        let body = server_over(state).v4_protocol_stats().await;
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["orders"]["swaps"]["executable"], 0);
        assert_eq!(v["orders"]["swaps"]["blocked"]["config_missing"], 1);
    }
}
