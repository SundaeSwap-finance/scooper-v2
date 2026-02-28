use std::{net::SocketAddr, pin::Pin, sync::Arc, task::{Context, Poll}};

use http_body_util::{Either, Full};
use hyper::{
    Request, Response,
    body::{Bytes, Frame, Incoming as IncomingBody},
    server::conn::http1,
};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    select,
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use std::collections::BTreeMap;

use crate::{
    cardano_types::TransactionInput,
    events::IndexEvent,
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
}

pub async fn admin_server(
    config: ServerConfig,
    v3_state: V3State,
    v4_state: V4State,
    v4_fee: Option<(u64, u64)>,
    v4_module_preimages: ModuleStatePreimages,
    resync_tx: tokio::sync::broadcast::Sender<()>,
    event_tx: tokio::sync::broadcast::Sender<(u64, Vec<IndexEvent>)>,
    shutdown: CancellationToken,
) {
    let v4_module_preimages = Arc::new(v4_module_preimages);
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

        let child = shutdown.child_token();
        tokio::task::spawn(async move {
            select! {
                _ = child.cancelled() => {},
                _ = handle_request(stream, v3_state, v4_state, v4_fee, v4_module_preimages, resync_tx, event_tx) => {}
            }
        });
    }
}

async fn handle_request(
    stream: TcpStream,
    v3_state: V3State,
    v4_state: V4State,
    v4_fee: Option<(u64, u64)>,
    v4_module_preimages: Arc<ModuleStatePreimages>,
    resync_tx: tokio::sync::broadcast::Sender<()>,
    event_tx: tokio::sync::broadcast::Sender<(u64, Vec<IndexEvent>)>,
) {
    let io = TokioIo::new(stream);

    let admin_server = AdminServer {
        v3_state,
        v4_state,
        v4_fee,
        v4_module_preimages,
        resync_tx,
        event_tx,
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

    async fn do_call(self, req: Request<IncomingBody>) -> Response<ResponseBody> {
        let path = req.uri().path();

        match path {
            "/dashboard" => self.serve_dashboard(),
            "/events" => self.serve_sse(),
            "/status" => Self::json_response(self.serve_status().await),
            "/resync-from-acropolis" => {
                let _ = self.resync_tx.send(());
                Self::text_response("resync")
            }
            "/health" => Self::text_response("health"),
            _ => Self::json_response(self.route_protocol(path).await),
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
            })
        } else {
            serde_json::json!({ "configured": false })
        };

        serde_json::to_string(&serde_json::json!({
            "v3": v3_info,
            "v4": v4_info,
        }))
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
        let Some(fee) = self.v4_fee else {
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

        // protocol_share is not in the fee config — use (0,1) as neutral default
        // since check_order_executability ignores it (it only affects LP math, not swap result)
        let protocol_share = (0u64, 1u64);

        for order in &candidates {
            match batch::check_order_executability(
                order,
                &pool.pool_datum.assets,
                &pool.pool_datum.total_lp,
                fee,
                protocol_share,
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
        IndexEvent::V3OrderScooped { order, pool_id, tx_id } => (
            "v3_order_scooped",
            serde_json::json!({
                "order": order.input.to_string(),
                "pool_id": pool_id.to_string(),
                "tx_id": tx_id,
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
        IndexEvent::V4OrderScooped { order, pool_id, tx_id } => (
            "v4_order_scooped",
            serde_json::json!({
                "order": order.input.to_string(),
                "pool_id": pool_id.to_string(),
                "tx_id": tx_id,
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
