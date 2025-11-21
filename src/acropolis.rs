use std::fmt::Display;

use anyhow::Result;
use async_trait::async_trait;
use pallas_network::{
    facades::PeerClient,
    miniprotocols::{
        Point,
        chainsync::{self, NextResponse},
    },
};
use pallas_traverse::{MultiEraBlock, MultiEraTx};
use tracing::{Level, event, warn};

pub struct BlockInfo {
    pub slot: u64,
    pub hash: BlockHash,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct BlockHash([u8; 32]);

impl BlockHash {
    pub fn new(value: [u8; 32]) -> Self {
        Self(value)
    }
}

impl Display for BlockHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl TryFrom<Vec<u8>> for BlockHash {
    type Error = Vec<u8>;
    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        Ok(Self(value.try_into()?))
    }
}

fn decode_header_point(header_content: &chainsync::HeaderContent) -> Result<Point> {
    let header = pallas_traverse::MultiEraHeader::decode(
        header_content.variant,
        header_content.byron_prefix.map(|x| x.0),
        &header_content.cbor,
    )?;
    let slot = header.slot();
    let header_hash = header.hash();
    Ok(Point::Specific(slot, header_hash.to_vec()))
}
pub struct Indexer {
    address: String,
    magic: u64,
    indexes: Vec<Box<dyn ManagedIndex>>,
    start: Point,
}

impl Indexer {
    pub fn new(address: &str, magic: u64) -> Self {
        Self {
            address: address.to_string(),
            magic,
            indexes: vec![],
            start: Point::Origin,
        }
    }

    pub fn add_index<M: ManagedIndex>(&mut self, index: M, start: Point, force_restart: bool) {
        let _ = force_restart;
        self.indexes.push(Box::new(index));
        self.start = start;
    }

    pub async fn run(mut self) -> Result<()> {
        let mut peer_client = PeerClient::connect(self.address, self.magic).await?;
        let intersect_result = peer_client
            .chainsync()
            .find_intersect(vec![self.start.clone()])
            .await;
        event!(Level::DEBUG, "Intersect result {:?}", intersect_result);
        loop {
            let resp = peer_client.chainsync().request_or_await_next().await?;
            match resp {
                NextResponse::RollForward(content, _tip) => {
                    let point = decode_header_point(&content)?;
                    let resp = peer_client.blockfetch().fetch_single(point).await?;
                    let block = MultiEraBlock::decode(&resp)?;
                    let info = BlockInfo {
                        slot: block.slot(),
                        hash: BlockHash::new(*block.hash()),
                    };
                    if block.number().is_multiple_of(1000) {
                        event!(Level::INFO, "Block height: {}", block.number());
                    }
                    for tx in block.txs() {
                        for index in &mut self.indexes {
                            index
                                .handle_onchain_tx(&info, &tx)
                                .await
                                .inspect_err(|e| warn!("error from {}: {:#}", index.name(), e))?;
                        }
                    }
                }
                NextResponse::RollBackward(point, tip) => {
                    event!(Level::DEBUG, "RollBackward({:?}, {:?})", point, tip);
                    let info = match point {
                        Point::Origin => BlockInfo {
                            slot: 0,
                            hash: Default::default(),
                        },
                        Point::Specific(slot, hash) => BlockInfo {
                            slot,
                            hash: hash.try_into().unwrap(),
                        },
                    };
                    for index in &mut self.indexes {
                        index.handle_rollback(&info).await?;
                    }
                }
                NextResponse::Await => {
                    event!(Level::DEBUG, "Await");
                }
            }
        }
    }
}

#[async_trait]
pub trait ManagedIndex: Send + Sync + 'static {
    fn name(&self) -> String;

    // Called when a new TX has arrived on-chain.
    async fn handle_onchain_tx(&mut self, info: &BlockInfo, tx: &MultiEraTx) -> Result<()> {
        // This method can update a database, or a mutex-locked in-memory map, or publish messages to the rest of the system,
        // or whatever. It's async and it's allowed to be long-running.
        let _ = (info, tx);
        // These indexes are fallible. If an index fails, we'll stop updating it, but keep running other indexes.
        // Could make sense to build a retry in too, in case the issue is just a fallible DB
        Ok(())
    }

    // Called when a block has rolled back.
    async fn handle_rollback(&mut self, info: &BlockInfo) -> Result<()> {
        let _ = info;
        Ok(())
    }
}
