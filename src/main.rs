use anyhow::{Result, bail};
use async_trait::async_trait;
use clap::Parser;
use pallas_network::miniprotocols::Point;
use pallas_primitives::PlutusData;
use pallas_traverse::MultiEraTx;
use tokio::sync::Mutex;

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use tracing::{Level, event, warn};

use std::sync::Arc;

mod acropolis;
mod cardano_types;
mod multisig;
mod serde_compat;
mod sundaev3;

use serde::Deserialize;

use cardano_types::{Datum, TransactionInput, TransactionOutput};
use pallas_addresses::Address;
use plutus_parser::AsPlutus;
use sundaev3::{Ident, OrderDatum, PoolDatum, SundaeV3Pool, validate_order};

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::{Request, Response, body::Incoming as IncomingBody};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::net::TcpListener;

use crate::acropolis::{BlockInfo, Indexer, ManagedIndex};

#[derive(Deserialize)]
struct SundaeV3Protocol {
    #[serde(deserialize_with = "serde_compat::deserialize_address")]
    order_address: Address,
    #[serde(deserialize_with = "serde_compat::deserialize_address")]
    pool_address: Address,
}

#[derive(clap::Parser, Clone, Debug)]
struct Args {
    #[arg(short, long)]
    addr: String,

    #[arg(short, long)]
    magic: u64,

    #[arg(short, long)]
    protocol: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, Debug)]
struct BlockHash(Vec<u8>);

const BLOCK_HASH_SIZE: usize = 32;

fn parse_block_hash(bh: &str) -> Result<BlockHash> {
    let bytes = hex::decode(bh)?;
    if bytes.len() == BLOCK_HASH_SIZE {
        Ok(BlockHash(bytes.to_vec()))
    } else {
        bail!(
            "Expected length {} for block hash, but got {}",
            BLOCK_HASH_SIZE,
            bytes.len()
        )
    }
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

#[derive(PartialEq, Eq)]
struct SundaeV3Order {
    input: TransactionInput,
    #[allow(unused)]
    output: TransactionOutput,
    slot: u64,
    spent_slot: Option<u64>,
}

impl PartialOrd for SundaeV3Order {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.slot.cmp(&other.slot))
    }
}

struct SortedVec<T> {
    contents: Vec<T>,
}

impl<T> SortedVec<T>
where
    T: PartialOrd,
{
    fn new() -> Self {
        SortedVec { contents: vec![] }
    }

    fn insert(&mut self, elem: T) {
        if self.contents.is_empty() {
            self.contents.push(elem);
        } else {
            for i in 0..self.contents.len() {
                if self.contents[i] > elem {
                    self.contents.insert(i, elem);
                    return;
                }
            }
            self.contents.insert(self.contents.len(), elem);
        }
    }

    fn retain<F>(&mut self, condition: F)
    where
        F: FnMut(&T) -> bool,
    {
        self.contents.retain(condition);
    }
}

impl<T> Default for SortedVec<T>
where
    T: PartialOrd,
{
    fn default() -> Self {
        SortedVec::new()
    }
}

struct SundaeV3PoolOrders {
    orders: SortedVec<SundaeV3Order>,
    unrecoverable_orders: SortedVec<SundaeV3Order>,
}

impl Default for SundaeV3PoolOrders {
    fn default() -> Self {
        SundaeV3PoolOrders {
            orders: SortedVec { contents: vec![] },
            unrecoverable_orders: SortedVec { contents: vec![] },
        }
    }
}

impl SundaeV3PoolOrders {
    fn insert(&mut self, order: SundaeV3Order) {
        self.orders.insert(order)
    }

    fn insert_unrecoverable(&mut self, order: SundaeV3Order) {
        self.unrecoverable_orders.insert(order)
    }

    fn rollback(&mut self, slot: u64) {
        self.orders.retain(|o| o.slot <= slot);
    }

    #[allow(unused)]
    fn iter<'a>(&'a mut self) -> std::slice::Iter<'a, SundaeV3Order> {
        self.orders.contents.iter()
    }

    fn iter_mut<'a>(&'a mut self) -> std::slice::IterMut<'a, SundaeV3Order> {
        self.orders.contents.iter_mut()
    }

    fn spend(&mut self, slot: u64, this_input: &TransactionInput) {
        for order in self.orders.contents.iter_mut() {
            if &order.input == this_input {
                order.spent_slot = Some(slot);
            }
        }
        for order in self.unrecoverable_orders.contents.iter_mut() {
            if &order.input == this_input {
                order.spent_slot = Some(slot);
            }
        }
    }
}

impl IntoIterator for SundaeV3PoolOrders {
    type Item = SundaeV3Order;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.orders.contents.into_iter()
    }
}

#[derive(Default)]
struct SundaeV3PoolStates {
    states: SortedVec<SundaeV3Pool>,
}

impl SundaeV3PoolStates {
    fn latest(&self) -> &SundaeV3Pool {
        self.states.contents.last().unwrap()
    }

    fn insert(&mut self, pool: SundaeV3Pool) {
        self.states.insert(pool)
    }

    fn rollback(&mut self, slot: u64) {
        self.states.retain(|state| state.slot <= slot);
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.states.contents.is_empty()
    }
}

struct SundaeV3Index {
    pools: BTreeMap<Ident, SundaeV3PoolStates>,
    orders: BTreeMap<Option<Ident>, SundaeV3PoolOrders>,
    // TODO: When pruning old orders marked as spent from memory db, also remove the entry here
    order_to_pool: BTreeMap<TransactionInput, Ident>,
}

impl SundaeV3Index {
    fn new() -> Self {
        Self {
            pools: BTreeMap::new(),
            orders: BTreeMap::new(),
            order_to_pool: BTreeMap::new(),
        }
    }
}

struct SundaeV3Indexer {
    state: Arc<Mutex<SundaeV3Index>>,
    protocol: SundaeV3Protocol,
}

fn payment_part_equal(a: &Address, b: &Address) -> bool {
    if let Address::Shelley(s_a) = a
        && let Address::Shelley(s_b) = b
    {
        return s_a.payment() == s_b.payment();
    }
    false
}

#[async_trait]
impl ManagedIndex for SundaeV3Indexer {
    fn name(&self) -> String {
        "sundae-v3".to_string()
    }

    async fn handle_onchain_tx(&mut self, info: &BlockInfo, tx: &MultiEraTx) -> Result<()> {
        let this_tx_hash = tx.hash();
        let mut index = self.state.lock().await;
        for (ix, output) in tx.outputs().iter().enumerate() {
            let address = output.address()?;
            if payment_part_equal(&address, &self.protocol.pool_address) {
                let tx_out: TransactionOutput = cardano_types::convert_transaction_output(output);
                match tx_out.datum {
                    Datum::Data(ref inline) => {
                        let plutus_data: PlutusData = minicbor::decode(inline).unwrap();
                        let pd: Result<PoolDatum, _> = AsPlutus::from_plutus(plutus_data);
                        if let Ok(pd) = pd {
                            let pool_id = pd.ident.clone();
                            let pool_record = SundaeV3Pool {
                                address: tx_out.address,
                                value: tx_out.value,
                                pool_datum: pd,
                                slot: info.slot,
                            };
                            let this_pool = index.pools.entry(pool_id).or_default();
                            this_pool.insert(pool_record);

                            event!(Level::DEBUG, "{}", hex::encode(this_tx_hash));
                            return Ok(());
                        }
                    }
                    Datum::None | Datum::Hash(_) => {}
                }
            } else if payment_part_equal(&address, &self.protocol.order_address) {
                let this_input = TransactionInput(pallas_primitives::TransactionInput {
                    transaction_id: this_tx_hash,
                    index: ix as u64,
                });
                let this_input_ref = format!("{}#{}", hex::encode(this_tx_hash), ix);
                let tx_out: TransactionOutput = cardano_types::convert_transaction_output(output);
                match tx_out.datum {
                    Datum::Data(ref inline) => {
                        let plutus_data: PlutusData = minicbor::decode(inline).unwrap();
                        let od: Result<OrderDatum, _> = AsPlutus::from_plutus(plutus_data);
                        if let Ok(od) = od {
                            if let Some(ref ident) = od.ident {
                                if let Some(pool) = index.pools.get(ident) {
                                    let current_pool = pool.latest();
                                    let order_value_ok = validate_order(
                                        &od,
                                        &tx_out.value,
                                        &current_pool.pool_datum,
                                    );

                                    let this_pool_orders =
                                        index.orders.entry(od.ident.clone()).or_default();

                                    if let Err(e) = order_value_ok {
                                        event!(
                                            Level::DEBUG,
                                            "Order {} was rejected: {}",
                                            this_input_ref,
                                            e
                                        );
                                        this_pool_orders.insert_unrecoverable(SundaeV3Order {
                                            input: this_input.clone(),
                                            output: tx_out,
                                            slot: info.slot,
                                            spent_slot: None,
                                        });

                                        index.order_to_pool.insert(this_input, ident.clone());

                                        return Ok(());
                                    }

                                    this_pool_orders.insert(SundaeV3Order {
                                        input: this_input.clone(),
                                        output: tx_out,
                                        slot: info.slot,
                                        spent_slot: None,
                                    });

                                    index.order_to_pool.insert(this_input, ident.clone());

                                    event!(Level::DEBUG, "Added order {} to index", this_input_ref);
                                    return Ok(());
                                } else {
                                    event!(
                                        Level::WARN,
                                        "Order {} was listed for an unknown pool {}",
                                        this_input_ref,
                                        ident,
                                    );
                                }
                            } else {
                                // Order is free
                                event!(
                                    Level::WARN,
                                    "Order {} was listed for no pool",
                                    this_input_ref,
                                )
                            }
                        }
                    }
                    Datum::None | Datum::Hash(_) => {}
                }
            }
        }

        for tx_in in tx.inputs() {
            let this_input = TransactionInput(pallas_primitives::TransactionInput {
                transaction_id: *tx_in.hash(),
                index: tx_in.index(),
            });
            if let Some(pool_ident) = index.order_to_pool.get(&this_input).cloned()
                && let Some(pool_orders) = index.orders.get_mut(&Some(pool_ident.clone()))
            {
                pool_orders.spend(info.slot, &this_input);
                index.order_to_pool.remove(&this_input);
            }
        }
        Ok(())
    }

    async fn handle_rollback(&mut self, info: &BlockInfo) -> Result<()> {
        warn!("rolling back to {}:{}", info.slot, info.hash);
        let mut index = self.state.lock().await;
        for pool in index.pools.values_mut() {
            pool.rollback(info.slot);
        }
        for pool_orders in index.orders.values_mut() {
            pool_orders.rollback(info.slot);
        }
        Ok(())
    }
}

async fn do_scoops(
    args: Args,
    mut abort: tokio::sync::broadcast::Receiver<()>,
    index: Arc<Mutex<SundaeV3Index>>,
) -> Result<()> {
    let start = match args.command {
        Commands::SyncFromOrigin => Point::Origin,
        Commands::SyncFromPoint { slot, block_hash } => Point::Specific(slot, block_hash.0),
    };

    let protocol_file = fs::File::open(args.protocol)?;
    let protocol: SundaeV3Protocol = serde_json::from_reader(protocol_file)?;
    let index = SundaeV3Indexer {
        state: index,
        protocol,
    };

    let mut indexer = Indexer::new(&args.addr, args.magic);
    indexer.add_index(index, start, false);
    tokio::select! {
        res = indexer.run() => {
            res
        }
        _ = abort.recv() => {
            Ok(())
        }
    }
}

#[derive(Clone)]
struct AdminServer {
    index: Arc<Mutex<SundaeV3Index>>,
    kill_tx: tokio::sync::broadcast::Sender<()>,
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

impl AdminServer {
    async fn do_call(&self, req: Request<IncomingBody>) -> String {
        if let Some(pool_id) = req.uri().path().strip_prefix("/pool/") {
            let index_lock = self.index.lock().await;
            let id_bytes = hex::decode(pool_id).unwrap();
            let ident = Ident::new(&id_bytes);
            if let Some(orders) = index_lock.orders.get(&Some(ident)) {
                let mut response = String::new();
                for order in &orders.orders.contents {
                    response += &format!("{}\n", order.input);
                }
                return response;
            } else {
                return "No such pool".into();
            }
        }

        match req.uri().path() {
            "/resync-from-kupo" => {
                let _ = self.kill_tx.send(());
                "resync".into()
            }
            "/health" => "health".into(),
            "/pools" => {
                let mut response = String::new();
                let index_lock = self.index.lock().await;
                for pool_id in index_lock.pools.keys() {
                    response += &format!("{pool_id}\n");
                }
                response
            }
            _ => "unknown".into(),
        }
    }
}

#[tokio::main]
#[allow(unreachable_code)]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let (kill_tx, _) = tokio::sync::broadcast::channel(1);
    let kill_tx2 = kill_tx.clone();

    let index = Arc::new(Mutex::new(SundaeV3Index::new()));
    let index2 = index.clone();

    // Manage restarting of the main scoop task in case we want to resync
    let manager_handle = tokio::spawn(async move {
        loop {
            let kill_rx2 = kill_tx.subscribe();
            let args2 = args.clone();
            {
                let mut lock = index.lock().await;
                lock.pools.clear();
                lock.orders.clear();
            }
            let index2 = index.clone();
            let do_scoops_handle = tokio::spawn(async move {
                event!(Level::DEBUG, "Doing scoops");
                match do_scoops(args2, kill_rx2, index2).await {
                    Ok(()) => {}
                    Err(e) => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                        println!("Scooper thread died: {}", e);
                    }
                }
            });
            do_scoops_handle.await.unwrap();
        }
    });

    // HTTP server for admin controls:
    //   - running a health check
    //   - triggering a resync from kupo
    let admin_server_handle = tokio::spawn(async move {
        let addr = SocketAddr::from(([127, 0, 0, 1], 9999));
        let listener = TcpListener::bind(addr).await.unwrap();
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);
            let kill_tx = kill_tx2.clone();
            let index = index2.clone();
            tokio::task::spawn(async {
                let admin_server = AdminServer { index, kill_tx };
                if let Err(err) = http1::Builder::new()
                    .serve_connection(io, admin_server)
                    .await
                {
                    event!(Level::DEBUG, "Failed to serve connection: {:?}", err);
                }
            });
        }
    });

    tokio::try_join!(manager_handle, admin_server_handle).unwrap();
}

#[cfg(test)]
mod tests {
    use pallas_codec::utils::Int;
    use pallas_traverse::MultiEraBlock;

    use super::*;

    async fn handle_block(indexer: &mut SundaeV3Indexer, block: MultiEraBlock<'_>) -> Result<()> {
        let info = BlockInfo {
            slot: block.slot(),
            hash: acropolis::BlockHash::new(*block.hash()),
        };
        for tx in block.txs() {
            indexer.handle_onchain_tx(&info, &tx).await?
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_block() {
        let state = Arc::new(Mutex::new(SundaeV3Index::new()));
        let protocol_file = fs::File::open("testdata/protocol").unwrap();
        let protocol = serde_json::from_reader(protocol_file).unwrap();
        let mut indexer = SundaeV3Indexer {
            state: state.clone(),
            protocol,
        };
        let block_bytes = std::fs::read("testdata/scoop-pool.block").unwrap();
        let block = pallas_traverse::MultiEraBlock::decode(&block_bytes).unwrap();
        let ada_policy: Vec<u8> = vec![];
        let ada_token: Vec<u8> = vec![];
        let pool_policy: Vec<u8> = vec![
            68, 161, 235, 45, 159, 88, 173, 212, 235, 25, 50, 189, 0, 72, 230, 161, 148, 126, 133,
            227, 254, 79, 50, 149, 106, 17, 4, 20,
        ];
        let pool_token: Vec<u8> = vec![
            0, 13, 225, 64, 50, 196, 63, 9, 111, 160, 86, 38, 218, 30, 173, 147, 131, 121, 60, 205,
            123, 186, 106, 27, 37, 158, 119, 89, 119, 102, 174, 232,
        ];
        let coin_b_policy: Vec<u8> = vec![
            145, 212, 243, 130, 39, 63, 68, 47, 21, 233, 218, 72, 203, 35, 52, 155, 162, 117, 248,
            129, 142, 76, 122, 197, 209, 0, 74, 22,
        ];
        let coin_b_token: Vec<u8> = vec![77, 121, 85, 83, 68];
        handle_block(&mut indexer, block).await.unwrap();
        let mut index = state.lock().await;
        assert_eq!(index.pools.len(), 1);
        let first_pool = index.pools.first_entry().unwrap();
        let pool_value = &first_pool.get().latest().value.0;
        assert_eq!(pool_value[&ada_policy][&ada_token], 6181255175);
        assert_eq!(pool_value[&pool_policy][&pool_token], 1);
        assert_eq!(pool_value[&coin_b_policy][&coin_b_token], 6397550387);
        assert_eq!(index.orders.len(), 0);
    }

    #[tokio::test]
    async fn test_rollback() {
        let state = Arc::new(Mutex::new(SundaeV3Index::new()));
        let protocol_file = fs::File::open("testdata/protocol").unwrap();
        let protocol = serde_json::from_reader(protocol_file).unwrap();
        let mut indexer = SundaeV3Indexer {
            state: state.clone(),
            protocol,
        };
        let block_bytes = std::fs::read("testdata/scoop-pool.block").unwrap();
        let block = pallas_traverse::MultiEraBlock::decode(&block_bytes).unwrap();
        let pool_id = Ident::new(
            &hex::decode("32c43f096fa05626da1ead9383793ccd7bba6a1b259e77597766aee8").unwrap(),
        );

        handle_block(&mut indexer, block.clone()).await.unwrap();
        {
            // The block contains a pool scoop, which results in a pool state being recorded.
            let index = state.lock().await;
            let pool = index.pools.get(&pool_id).unwrap();
            let pool_states_empty = pool.is_empty();
            assert!(!pool_states_empty);
        }

        let rollback_block_info = BlockInfo {
            slot: block.slot() - 1,
            hash: acropolis::BlockHash::new([0; 32]),
        };

        indexer.handle_rollback(&rollback_block_info).await.unwrap();
        {
            // After rollback, the states for this pool have been deleted,
            // though the map entry for the pool still exists.
            let index = state.lock().await;
            let pool = index.pools.get(&pool_id).unwrap();
            let pool_states_empty = pool.is_empty();
            assert!(pool_states_empty);
        }
    }

    fn make_lovelace_value(lovelace: i128) -> cardano_types::Value {
        let mut m = BTreeMap::new();
        let mut lovelace_quantity = BTreeMap::new();
        lovelace_quantity.insert(vec![], lovelace);
        m.insert(vec![], lovelace_quantity);
        cardano_types::Value(m)
    }

    #[tokio::test]
    async fn pools_maintains_sorted() {
        let payment_cred_1 = [0; 28];
        let mut address_1 = [0; 29];
        address_1[0] = 0x60;
        address_1[1..].clone_from_slice(&payment_cred_1);

        let address_1 = pallas_addresses::Address::from_bytes(&address_1).unwrap();
        let value_1 = make_lovelace_value(1000000);
        let pool_datum_1 = PoolDatum {
            ident: Ident::new(&[]),
            assets: (
                cardano_types::AssetClass {
                    policy: vec![],
                    token: vec![],
                },
                cardano_types::AssetClass {
                    policy: vec![],
                    token: vec![],
                },
            ),
            circulating_lp: pallas_primitives::BigInt::Int(Int::from(0)),
            ask_fees_per_10_thousand: pallas_primitives::BigInt::Int(Int::from(0)),
            bid_fees_per_10_thousand: pallas_primitives::BigInt::Int(Int::from(0)),
            fee_manager: None,
            market_open: pallas_primitives::BigInt::Int(Int::from(0)),
            protocol_fees: pallas_primitives::BigInt::Int(Int::from(0)),
        };
        let pool = |s| SundaeV3Pool {
            address: address_1.clone(),
            value: value_1.clone(),
            pool_datum: pool_datum_1.clone(),
            slot: s,
        };
        let mut pools = SundaeV3PoolStates {
            states: SortedVec { contents: vec![] },
        };
        pools.insert(pool(1));
        pools.insert(pool(0));
        let latest_pool = pools.latest();
        assert_eq!(latest_pool.slot, 1);

        pools.insert(pool(2));
        let latest_pool = pools.latest();
        assert_eq!(latest_pool.slot, 2);
    }
}
