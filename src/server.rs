use std::{net::SocketAddr, pin::Pin, sync::Arc, sync::atomic::{AtomicBool, Ordering}, task::{Context, Poll}};

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

/// Build the preimage map from an execution config's fee and protocol_share.
pub fn compute_module_state_preimages(
    fee: (u64, u64),
    protocol_share: (u64, u64),
) -> ModuleStatePreimages {
    use pallas_crypto::hash::Hasher;
    use plutus_parser::AsPlutus;
    use crate::sundaev4::{ConstantProductConfig, FeeSplitConfig, Rational};
    use crate::bigint::BigInt;

    let configs = [
        minicbor::to_vec(
            &ConstantProductConfig {
                fee: Rational { num: BigInt::from(fee.0), den: BigInt::from(fee.1) },
            }.to_plutus(),
        ).unwrap(),
        minicbor::to_vec(
            &FeeSplitConfig {
                protocol_share: Rational { num: BigInt::from(protocol_share.0), den: BigInt::from(protocol_share.1) },
            }.to_plutus(),
        ).unwrap(),
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

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

pub async fn admin_server(
    config: ServerConfig,
    v3_state: V3State,
    v4_state: V4State,
    v4_fee: Option<(u64, u64)>,
    v4_module_preimages: ModuleStatePreimages,
    resync_tx: tokio::sync::broadcast::Sender<()>,
    event_tx: tokio::sync::broadcast::Sender<(u64, Vec<IndexEvent>)>,
    paused: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    intents: Option<crate::sundaev4::intents::IntentServiceHandle>,
    shutdown: CancellationToken,
) {
    let v4_module_preimages = Arc::new(v4_module_preimages);

    let tls_acceptor = match (&config.tls_cert, &config.tls_key) {
        (Some(cert), Some(key)) => {
            let acceptor = build_tls_acceptor(cert, key)
                .expect("failed to initialize TLS");
            tracing::info!(cert = %cert, "TLS enabled for admin server");
            Some(acceptor)
        }
        (None, None) => {
            tracing::info!("Admin server running without TLS");
            None
        }
        _ => panic!("tls_cert and tls_key must both be set or both be absent"),
    };

    let listener = TcpListener::bind(config.address).await.unwrap();

    loop {
        let stream = select! {
            res = listener.accept() => res.unwrap().0,
            _ = shutdown.cancelled() => { break; }
        };

        let resync_tx = resync_tx.clone();
        let event_tx = event_tx.clone();
        let v3_state = v3_state.clone();
        let v4_state = v4_state.clone();
        let v4_module_preimages = v4_module_preimages.clone();
        let paused = paused.clone();
        let metrics = metrics.clone();
        let intents = intents.clone();
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
                    _ = handle_request(tls_stream, v3_state, v4_state, v4_fee, v4_module_preimages, resync_tx, event_tx, paused, metrics, intents) => {}
                }
            } else {
                select! {
                    _ = child.cancelled() => {},
                    _ = handle_request(stream, v3_state, v4_state, v4_fee, v4_module_preimages, resync_tx, event_tx, paused, metrics, intents) => {}
                }
            }
        });
    }
}

async fn handle_request(
    stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
    v3_state: V3State,
    v4_state: V4State,
    v4_fee: Option<(u64, u64)>,
    v4_module_preimages: Arc<ModuleStatePreimages>,
    resync_tx: tokio::sync::broadcast::Sender<()>,
    event_tx: tokio::sync::broadcast::Sender<(u64, Vec<IndexEvent>)>,
    paused: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    intents: Option<crate::sundaev4::intents::IntentServiceHandle>,
) {
    let io = TokioIo::new(stream);

    let admin_server = AdminServer {
        v3_state,
        v4_state,
        v4_fee,
        v4_module_preimages,
        resync_tx,
        event_tx,
        paused,
        metrics,
        intents,
    };
    if let Err(err) = http1::Builder::new()
        .serve_connection(io, admin_server)
        .await
    {
        debug!("Failed to serve connection: {:?}", err);
    }
}

#[derive(Clone)]
struct AdminServer {
    v3_state: V3State,
    v4_state: V4State,
    v4_fee: Option<(u64, u64)>,
    v4_module_preimages: Arc<ModuleStatePreimages>,
    resync_tx: tokio::sync::broadcast::Sender<()>,
    event_tx: tokio::sync::broadcast::Sender<(u64, Vec<IndexEvent>)>,
    paused: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    intents: Option<crate::sundaev4::intents::IntentServiceHandle>,
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

    fn error_response(status: hyper::StatusCode, message: impl Into<String>) -> Response<ResponseBody> {
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

        // Bound the body read: an SSE is small; 64KB is generous.
        const MAX_BODY: usize = 64 * 1024;
        let body = match req.into_body().collect().await {
            Ok(b) => b.to_bytes(),
            Err(e) => {
                return Self::error_response(
                    hyper::StatusCode::BAD_REQUEST,
                    format!("failed to read body: {e}"),
                );
            }
        };
        if body.len() > MAX_BODY {
            return Self::error_response(hyper::StatusCode::PAYLOAD_TOO_LARGE, "body too large");
        }

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

        // Snapshot current orders so validation doesn't hold the state lock.
        let orders: Vec<Arc<crate::sundaev4::SundaeV4Order>> = {
            let state = v4_state.lock().await;
            state.latest().orders.clone()
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

        match intents.submit(sse_cbor, parsed.hint, find_order).await {
            Ok(outcome) => Self::json_response(serde_json::to_string(&outcome).unwrap()),
            Err(e) => Self::error_response(hyper::StatusCode::BAD_REQUEST, format!("{e:#}")),
        }
    }

    /// GET /v4/strategy-intents — observability listing.
    async fn list_strategy_intents(self) -> Response<ResponseBody> {
        let Some(intents) = self.intents.clone() else {
            return Self::error_response(
                hyper::StatusCode::NOT_IMPLEMENTED,
                "strategy intents require a configured v4 execution",
            );
        };
        let store = intents.store.lock().await;
        let now = crate::sundaev4::intents::now_ms();
        let listing: Vec<serde_json::Value> = store
            .all()
            .map(|i| {
                serde_json::json!({
                    "intent_id": hex::encode(&i.intent_id),
                    "order": format!(
                        "{}#{}",
                        hex::encode(&i.sse.execution.order_ref.transaction_id),
                        i.sse.execution.order_ref.output_index,
                    ),
                    "hint": i.hint,
                    "expiry_ms": i.expiry_ms,
                    "expired": i.expiry_ms <= now,
                    "received_at_ms": i.received_at_ms,
                    "min_received": i.sse.execution.min_received,
                })
            })
            .collect();
        Self::json_response(
            serde_json::json!({ "count": store.len(), "intents": listing }).to_string(),
        )
    }

    async fn do_call(self, req: Request<IncomingBody>) -> Response<ResponseBody> {
        let path = req.uri().path().to_string();

        if path == "/v4/strategy-intents" {
            return match *req.method() {
                hyper::Method::POST => self.post_strategy_intent(req).await,
                hyper::Method::GET => self.list_strategy_intents().await,
                _ => Self::error_response(
                    hyper::StatusCode::METHOD_NOT_ALLOWED,
                    "use GET or POST",
                ),
            };
        }

        match path.as_str() {
            "/dashboard" => self.serve_dashboard(),
            "/events" => self.serve_sse(),
            "/status" => Self::json_response(self.serve_status().await),
            "/resync-from-acropolis" => {
                let _ = self.resync_tx.send(());
                Self::text_response("resync")
            }
            "/health" => Self::json_response(self.serve_health_stats().await),
            "/metrics" => self.serve_metrics().await,
            "/pause" => {
                let was_paused = self.paused.fetch_xor(true, Ordering::Relaxed);
                let now_paused = !was_paused;
                Self::json_response(
                    serde_json::to_string(&serde_json::json!({ "paused": now_paused })).unwrap(),
                )
            }
            _ => Self::json_response(self.route_protocol(&path).await),
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

        let in_flight = self.metrics.in_flight_snapshot();
        let quarantine = self.metrics.quarantine_snapshot();

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
                "in_flight_pools": in_flight.pool_ids,
                "in_flight_orders": in_flight.order_refs,
                "quarantine": quarantine,
            })
        } else {
            serde_json::json!({ "configured": false })
        };

        serde_json::to_string(&serde_json::json!({
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
            })).unwrap();
        };
        let state = v4.lock().await.latest().into_owned();

        let sync_pct = match state.network_tip_slot {
            Some(net) if net > 0 => (state.tip_slot as f64 / net as f64) * 100.0,
            _ => 0.0,
        };

        let ada_balance: f64 = state.wallet_utxos.values()
            .map(|v| {
                let lovelace = v.get(&crate::cardano_types::AssetClass { policy: vec![], token: vec![] });
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
        })).unwrap()
    }

    async fn serve_metrics(&self) -> Response<ResponseBody> {
        let body = crate::metrics::render_metrics(
            &self.v3_state,
            &self.v4_state,
            &self.paused,
            &self.metrics,
        ).await;
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
            loop {
                match event_rx.recv().await {
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
                        let msg = format!(
                            "event: error\ndata: {{\"lagged\":{n}}}\n\n"
                        );
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
        let v3_path = path
            .strip_prefix("/v3")
            .or_else(|| Some(path))
            .unwrap();

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
                    tracing::error!(
                        "Failed to serialize order {:?}: {}",
                        order.datum.ident,
                        e
                    );
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
        if let Some(obj) = pool_json.get_mut("pool_datum").and_then(|d| d.get_mut("module_state")) {
            let mut enriched = serde_json::Map::new();
            for (module_hash, state_bytes) in &pool.pool_datum.module_state {
                let state_hex = hex::encode(state_bytes);
                // Try to find a preimage whose blake2b-256 matches state_bytes
                let value_hex = preimages.get(state_bytes).map(hex::encode);
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

    async fn v4_list_pools(&self) -> String {
        let Some(v4) = &self.v4_state else {
            return "v4 indexer not configured".into();
        };
        let state = v4.lock().await.latest().into_owned();
        let mut json_map = serde_json::Map::new();

        for (ident, pool) in &state.pools {
            json_map.insert(
                hex::encode(ident.to_bytes()),
                Self::v4_pool_json(pool, &self.v4_module_preimages),
            );
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
            let order_vals: Vec<serde_json::Value> = orders
                .iter()
                .filter_map(|o| serde_json::to_value(o.as_ref()).ok())
                .collect();
            json_map.insert(hex::encode(ident.to_bytes()), order_vals.into());
        }

        // Collect unmatched orders (those not in any group)
        let matched: std::collections::BTreeSet<&TransactionInput> = groups
            .values()
            .flat_map(|orders| orders.iter().map(|o| &o.input))
            .collect();
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
        IndexEvent::V3OrderScooped { order, pool_id, tx_id, scooper } => (
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
        IndexEvent::V3SettingsUpdated { .. } => (
            "v3_settings_updated",
            "{}".to_string(),
        ),
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
        IndexEvent::V4OrderScooped { order, pool_ids, tx_id, scooper } => (
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
        IndexEvent::V4SettingsUpdated { .. } => (
            "v4_settings_updated",
            "{}".to_string(),
        ),
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
