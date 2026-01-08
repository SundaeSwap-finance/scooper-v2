use std::{net::SocketAddr, pin::Pin, sync::Arc};

use http_body_util::Full;
use hyper::{
    Request, Response,
    body::{Bytes, Incoming as IncomingBody},
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

use crate::{
    cardano_types::TransactionInput,
    sundaev3::{Ident, PoolError, SundaeV3HistoricalState, ValidationError, validate_order},
};

#[derive(Clone, Debug, Deserialize)]
pub struct ServerConfig {
    pub address: SocketAddr,
}

pub async fn admin_server(
    config: ServerConfig,
    index: Arc<Mutex<SundaeV3HistoricalState>>,
    resync_tx: tokio::sync::broadcast::Sender<()>,
    shutdown: CancellationToken,
) {
    let listener = TcpListener::bind(config.address).await.unwrap();

    loop {
        let stream = select! {
            res = listener.accept() => res.unwrap().0,
            _ = shutdown.cancelled() => { break; }
        };

        let resync_tx = resync_tx.clone();
        let index = index.clone();

        let child = shutdown.child_token();
        tokio::task::spawn(async move {
            select! {
                _ = child.cancelled() => {},
                _ = handle_request(stream, index, resync_tx) => {}
            }
        });
    }
}

async fn handle_request(
    stream: TcpStream,
    index: Arc<Mutex<SundaeV3HistoricalState>>,
    resync_tx: tokio::sync::broadcast::Sender<()>,
) {
    let io = TokioIo::new(stream);

    let admin_server = AdminServer { index, resync_tx };
    if let Err(err) = http1::Builder::new()
        .serve_connection(io, admin_server)
        .await
    {
        debug!("Failed to serve connection: {:?}", err);
    }
}

#[derive(Clone)]
struct AdminServer {
    index: Arc<Mutex<SundaeV3HistoricalState>>,
    resync_tx: tokio::sync::broadcast::Sender<()>,
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
