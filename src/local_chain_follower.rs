//! Chain follow over the local node's N2C socket, for chains with Leios
//! endorser blocks.
//!
//! On a Leios chain a ranking block certifies an endorser block that carries
//! the transactions; the ranking block itself carries none of them. Node to
//! node chainsync + blockfetch (acropolis' `peer-network-interface`) hands out
//! the bare ranking block, so every transaction in an endorser block — scoops,
//! orders, pool updates — never reaches the indexer, and the scooper goes stale
//! the first time one of its own scoops lands in one (its next scoop spends
//! inputs that are already gone: `AllInputsAreSpent` / `BadInputsUTxO`).
//!
//! A node resolves certified endorser blocks for its LOCAL clients: N2C
//! chainsync delivers each ranking block with the endorsed transactions
//! inlined (Dolos stores the same shape and refuses to relay it over N2N for
//! that reason). This module follows that stream and publishes exactly what
//! `peer-network-interface` publishes in direct mode — `BlockAvailable` per
//! roll-forward and `StateTransition::Rollback` per roll-back, on the same
//! topics, honouring the same dynamic `FindIntersect` sync commands — so
//! nothing downstream changes.
//!
//! Enabled by a `module.local-chain-follower` section in the acropolis config,
//! in place of `peer-network-interface` (see `main.rs`):
//!
//! ```json
//! "local-chain-follower": { "socket-path": "/data/node.socket", "network-magic": 164 }
//! ```

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use acropolis_common::{
    BlockHash, BlockInfo, BlockIntent, BlockStatus, Era,
    commands::chain_sync::ChainSyncCommand,
    genesis_values::GenesisValues,
    messages::{CardanoMessage, Command, Message, RawBlockMessage, StateTransitionMessage},
};
use anyhow::{Context as _, Result, bail};
use caryatid_sdk::{Context, Subscription, module};
use config::Config;
use pallas_network::facades::NodeClient;
use pallas_network::miniprotocols::{Point, chainsync};
use pallas_traverse::MultiEraBlock;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// How many recently published blocks to remember, to describe a rollback
/// target (slot, number, era) the way `peer-network-interface` does.
const RECENT_BLOCKS: usize = 2160;

/// Delay before reconnecting after the node socket drops.
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct FollowerConfig {
    /// The local cardano-node's N2C unix socket.
    socket_path: String,
    /// Network magic for the N2C handshake.
    network_magic: u64,
    #[serde(default = "default_block_topic")]
    block_topic: String,
    #[serde(default = "default_genesis_completion_topic")]
    genesis_completion_topic: String,
    #[serde(default = "default_sync_command_topic")]
    sync_command_topic: String,
}

// The same defaults as peer-network-interface's config.default.toml.
fn default_block_topic() -> String {
    "cardano.block.available".to_string()
}
fn default_genesis_completion_topic() -> String {
    "cardano.sequence.bootstrapped".to_string()
}
fn default_sync_command_topic() -> String {
    "cardano.sync.command".to_string()
}

#[module(
    message_type(Message),
    name = "local-chain-follower",
    description = "Chain follower over the local node's N2C socket (resolves Leios endorser blocks)"
)]
pub struct LocalChainFollower;

impl LocalChainFollower {
    pub async fn init(&self, context: Arc<Context<Message>>, config: Arc<Config>) -> Result<()> {
        let cfg: FollowerConfig =
            config.as_ref().clone().try_deserialize().context("local-chain-follower config")?;
        let mut genesis_subscription = context.subscribe(&cfg.genesis_completion_topic).await?;
        let mut command_subscription = context.subscribe(&cfg.sync_command_topic).await?;

        context.clone().run(async move {
            let genesis = match wait_genesis_completion(&mut genesis_subscription).await {
                Ok(g) => g,
                Err(e) => {
                    error!("local-chain-follower: no genesis values: {e:#}");
                    return;
                }
            };
            // Dynamic sync point: the custom indexer names where to start.
            let start = match wait_find_intersect(&mut command_subscription).await {
                Ok(p) => p,
                Err(e) => {
                    error!("local-chain-follower: sync command never received: {e:#}");
                    return;
                }
            };
            if let Point::Specific(slot, _) = &start {
                let (epoch, _) = genesis.slot_to_epoch(*slot);
                info!(slot, epoch, socket = %cfg.socket_path, "local-chain-follower: starting sync");
            }

            // Later FindIntersect commands move the sync point at runtime.
            let (point_tx, point_rx) = mpsc::channel(16);
            context.run(forward_commands(command_subscription, point_tx));

            let mut sink = BlockSink {
                context: context.clone(),
                topic: cfg.block_topic.clone(),
                genesis,
                last_epoch: None,
                era: None,
                rolled_back: false,
                recent: VecDeque::with_capacity(RECENT_BLOCKS),
            };
            follow(&cfg, start, point_rx, &mut sink).await;
        });

        Ok(())
    }
}

/// Reconnect loop: the node restarting must degrade to warnings and a resume
/// from the last published block, never a gap.
async fn follow(
    cfg: &FollowerConfig,
    start: Point,
    mut point_rx: mpsc::Receiver<Point>,
    sink: &mut BlockSink,
) {
    let mut sync_point = start;
    loop {
        match NodeClient::connect(&cfg.socket_path, cfg.network_magic).await {
            Ok(mut client) => {
                info!(socket = %cfg.socket_path, "local-chain-follower: connected to node");
                match follow_session(&mut client, &mut sync_point, &mut point_rx, sink).await {
                    Ok(()) => info!("local-chain-follower: sync point changed; re-intersecting"),
                    Err(e) => {
                        warn!(error = %e, "local-chain-follower: session ended; reconnecting")
                    }
                }
                client.abort().await;
            }
            Err(e) => {
                warn!(error = %e, socket = %cfg.socket_path, "local-chain-follower: could not connect; retrying");
            }
        }
        // A sync command that arrived while disconnected wins over the resume point.
        while let Ok(p) = point_rx.try_recv() {
            sync_point = p;
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

/// One chainsync session from `sync_point`. Returns Ok(()) when a new sync
/// command arrives (the caller re-intersects there); `sync_point` tracks the
/// last block published so a reconnect resumes without a gap.
async fn follow_session(
    client: &mut NodeClient,
    sync_point: &mut Point,
    point_rx: &mut mpsc::Receiver<Point>,
    sink: &mut BlockSink,
) -> Result<()> {
    let cs = client.chainsync();
    let (found, _tip) =
        cs.find_intersect(vec![sync_point.clone()]).await.context("find_intersect")?;
    if found.is_none() && !matches!(sync_point, Point::Origin) {
        bail!("node has no intersection at {:?}", sync_point);
    }
    // The first reply after an intersection is a roll-back to the intersection
    // itself: not a rollback of anything already published.
    let mut skip_rollback_to = Some(sync_point.clone());

    loop {
        let next = tokio::select! {
            p = point_rx.recv() => {
                if let Some(p) = p {
                    *sync_point = p;
                }
                return Ok(());
            }
            next = cs.request_or_await_next() => next.context("chainsync next")?,
        };
        match next {
            chainsync::NextResponse::RollForward(content, tip) => {
                let block = MultiEraBlock::decode(&content).context("decode block")?;
                sink.announce_roll_forward(&block, &content, Some(&tip.0)).await?;
                *sync_point = Point::Specific(block.slot(), block.hash().to_vec());
                skip_rollback_to = None;
            }
            chainsync::NextResponse::RollBackward(point, tip) => {
                if skip_rollback_to.take().is_some_and(|p| p == point) {
                    continue;
                }
                sink.announce_roll_backward(&point, Some(&tip.0)).await?;
                *sync_point = point;
            }
            chainsync::NextResponse::Await => {}
        }
    }
}

async fn forward_commands(
    mut subscription: Box<dyn Subscription<Message>>,
    tx: mpsc::Sender<Point>,
) {
    while let Ok((_, msg)) = subscription.read().await {
        if let Message::Command(Command::ChainSync(ChainSyncCommand::FindIntersect(p))) =
            msg.as_ref()
            && tx.send(to_pallas_point(p)).await.is_err()
        {
            return;
        }
    }
    error!("local-chain-follower: sync command subscription closed");
}

async fn wait_genesis_completion(
    sub: &mut Box<dyn Subscription<Message>>,
) -> Result<GenesisValues> {
    let (_, message) = sub.read().await?;
    match message.as_ref() {
        Message::Cardano((_, CardanoMessage::GenesisComplete(complete))) => {
            Ok(complete.values.clone())
        }
        msg => bail!("unexpected message in genesis completion topic: {msg:?}"),
    }
}

async fn wait_find_intersect(sub: &mut Box<dyn Subscription<Message>>) -> Result<Point> {
    loop {
        let (_, message) = sub.read().await?;
        if let Message::Command(Command::ChainSync(ChainSyncCommand::FindIntersect(p))) =
            message.as_ref()
        {
            return Ok(to_pallas_point(p));
        }
    }
}

fn to_pallas_point(p: &acropolis_common::Point) -> Point {
    match p {
        acropolis_common::Point::Origin => Point::Origin,
        acropolis_common::Point::Specific { hash, slot } => Point::Specific(*slot, hash.to_vec()),
    }
}

/// The acropolis era of a block, as `peer-network-interface` derives it from
/// the N2N header variant (Byron 0 … Conway 6, Dijkstra 7). pallas numbers
/// eras by their block CBOR tag (Byron 1 … Dijkstra 8), one higher.
fn acropolis_era(block: &MultiEraBlock) -> Result<Era> {
    acropolis_era_of(block.era())
}

fn acropolis_era_of(era: pallas_traverse::Era) -> Result<Era> {
    let tag = u16::from(era);
    let variant = u8::try_from(tag.saturating_sub(1)).context("era tag")?;
    Era::try_from(variant)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn era_matches_the_n2n_header_variant() {
        // peer-network-interface: Era::try_from(header variant), Byron 0 … Conway 6.
        assert_eq!(
            acropolis_era_of(pallas_traverse::Era::Byron).unwrap(),
            Era::try_from(0u8).unwrap()
        );
        assert_eq!(
            acropolis_era_of(pallas_traverse::Era::Shelley).unwrap(),
            Era::try_from(1u8).unwrap()
        );
        assert_eq!(
            acropolis_era_of(pallas_traverse::Era::Conway).unwrap(),
            Era::try_from(6u8).unwrap()
        );
    }

    #[test]
    fn dijkstra_maps_like_the_patched_header_variant() {
        // Musashi's Dijkstra blocks: variant 7, accepted by acropolis-patched.
        assert_eq!(
            acropolis_era_of(pallas_traverse::Era::Dijkstra).ok(),
            Era::try_from(7u8).ok()
        );
    }
}

#[derive(Clone)]
struct RecentBlock {
    slot: u64,
    hash: Vec<u8>,
    number: u64,
    era: Era,
}

/// Mirrors peer-network-interface's BlockSink (direct mode).
struct BlockSink {
    context: Arc<Context<Message>>,
    topic: String,
    genesis: GenesisValues,
    last_epoch: Option<u64>,
    era: Option<Era>,
    rolled_back: bool,
    recent: VecDeque<RecentBlock>,
}

impl BlockSink {
    async fn announce_roll_forward(
        &mut self,
        block: &MultiEraBlock<'_>,
        raw: &[u8],
        tip: Option<&Point>,
    ) -> Result<()> {
        let era = acropolis_era(block)?;
        let hash: [u8; 32] = *block.hash();
        let info = self.make_block_info(block.slot(), block.number(), hash, era, tip);
        let raw_block = RawBlockMessage {
            header: block.header().cbor().to_vec(),
            // The full era-tagged block, as blockfetch delivers it and as
            // block_unpacker decodes it (MultiEraBlock::decode).
            body: raw.to_vec(),
        };
        let message = Arc::new(Message::Cardano((
            info,
            CardanoMessage::BlockAvailable(raw_block),
        )));
        self.context.publish(&self.topic, message).await?;
        self.rolled_back = false;

        if self.recent.len() == RECENT_BLOCKS {
            self.recent.pop_front();
        }
        self.recent.push_back(RecentBlock {
            slot: block.slot(),
            hash: hash.to_vec(),
            number: block.number(),
            era,
        });
        Ok(())
    }

    async fn announce_roll_backward(&mut self, point: &Point, tip: Option<&Point>) -> Result<()> {
        let (slot, hash) = match point {
            Point::Specific(slot, hash) => (*slot, hash.clone()),
            Point::Origin => bail!("rollback to origin is not supported"),
        };
        // Forget everything after the rollback point, and describe the point
        // from what was published for it.
        while self.recent.back().is_some_and(|b| b.slot > slot) {
            self.recent.pop_back();
        }
        let known = self.recent.back().filter(|b| b.slot == slot && b.hash == hash).cloned();
        let (number, era) = match known {
            Some(b) => (b.number, b.era),
            None => {
                warn!(
                    slot,
                    "local-chain-follower: rollback beyond remembered blocks"
                );
                (0, self.era.unwrap_or(Era::Conway))
            }
        };
        let hash: [u8; 32] = hash.as_slice().try_into().context("rollback point hash")?;
        self.rolled_back = true;
        let info = self.make_block_info(slot, number, hash, era, tip);
        let point = acropolis_common::Point::Specific {
            hash: info.hash,
            slot: info.slot,
        };
        let message = Arc::new(Message::Cardano((
            info,
            CardanoMessage::StateTransition(StateTransitionMessage::Rollback(point)),
        )));
        self.context.publish(&self.topic, message).await
    }

    fn make_block_info(
        &mut self,
        slot: u64,
        number: u64,
        hash: [u8; 32],
        era: Era,
        tip: Option<&Point>,
    ) -> BlockInfo {
        let (epoch, epoch_slot) = self.genesis.slot_to_epoch(slot);
        let new_epoch = self.last_epoch != Some(epoch);
        self.last_epoch = Some(epoch);
        let is_new_era = self.era != Some(era);
        self.era = Some(era);
        let timestamp = self.genesis.slot_to_timestamp(slot);
        BlockInfo {
            status: if self.rolled_back {
                BlockStatus::RolledBack
            } else {
                BlockStatus::Volatile
            },
            intent: BlockIntent::ValidateAndApply,
            slot,
            number,
            hash: BlockHash::new(hash),
            epoch,
            epoch_slot,
            new_epoch,
            is_new_era,
            tip_slot: tip.map(|p| p.slot_or_default()),
            timestamp,
            era,
        }
    }
}
