// Everything in this file is a mock interface for what Acropolis can provide.
use std::sync::Arc;

use anyhow::Result;

pub mod core {
    #![allow(unused)]
    use anyhow::bail;
    use async_trait::async_trait;
    use pallas_network::miniprotocols::Point;
    use tokio::task;

    use crate::BlockHash;

    use super::*;

    pub struct Process {
        modules: Vec<Box<dyn Module>>
    }

    impl Process {
        pub fn create() -> Self {
            Self { modules: vec![] }
        }
        pub fn register<T: Module>(&mut self, module: T) {
            self.modules.push(Box::new(module));
        }
        pub async fn run(self) -> Result<()> {
            let context = Arc::new(Context {});
            for mut module in self.modules {
                module.init(context.clone()).await?;
            }
            Ok(())
        }
    }

    pub enum AcropolisMessage {
        SyncFrom(Point),
        NewTx(BlockInfo, Vec<u8>),
        Rollback(BlockInfo),
    }

    pub struct Subscription {}
    impl Subscription {
        pub async fn read(&mut self) -> Result<AcropolisMessage> {
            let tx = "84a300d90102818258203e28562ebb6f9a777b28f154877c960f9ad5f850e05cf1d761f481c0390badc7000182a300581d6003c0b0797dd49a8549986c3c21c910b75cbe3a6d4e1318872a96763a011a002625a0028201d8185822d87a9f581cc279a3fb3b4e62bbc78e288783b58045d4ae82a18867d8352d02775aff82583900c279a3fb3b4e62bbc78e288783b58045d4ae82a18867d8352d02775a121fd22e0b57ac206fefc763f8bfa0771919f5218b40691eea4514d0821b0000003625048409a1581c45df5f274b8950b512b08d10656864958659c4ecf3ffad092ef63024a144555344720c021a00029ac5a0f5f6";
            let tx = hex::decode(tx).unwrap();
            Ok(AcropolisMessage::NewTx(BlockInfo { slot: 0, hash: BlockHash(vec![]) }, tx))
        }
    }

    pub struct Context {
    
    }

    impl Context {
        pub fn run<T, F>(&self, func: F) -> task::JoinHandle<T>
        where 
            T: Send + 'static,
            F: Future<Output = T> + Send + 'static,
        {
            tokio::spawn(func)
        }

        pub async fn publish(&self, topic: &str, message: AcropolisMessage) -> Result<()> {
            Ok(())
        }

        pub async fn subscribe(&self, topic: &str) -> Result<Subscription> {
            Ok(Subscription {  })
        }
    }

    pub struct BlockInfo {
        pub slot: u64,
        pub hash: BlockHash,
    }

    #[async_trait]
    pub trait Module: Send + Sync + 'static {
        fn name(&self) -> String;
        async fn init(&mut self, context: Arc<Context>) -> Result<()>;
    }
}

pub mod indexer {
    use std::{cmp::Ordering, collections::HashMap};

    use async_trait::async_trait;
    use futures::{StreamExt, stream::FuturesUnordered};
    use pallas_network::miniprotocols::Point;
    use pallas_traverse::MultiEraTx;

    use crate::acropolis::core::{AcropolisMessage, BlockInfo, Module};

    use super::*;

    #[derive(Debug, Clone)]
    pub struct Cursor {
        pub name: String,
        pub point: Point,
    }

    pub trait CursorStore: Send + Sync + 'static {
        fn load(&self) -> impl Future<Output = Result<Vec<Cursor>>> + Send;
        fn save(&mut self, cursors: &[Cursor]) -> impl Future<Output = Result<()>> + Send;
    }

    pub struct InMemoryCursorStore {
        cursors: Vec<Cursor>,
    }
    impl InMemoryCursorStore {
        pub fn new(cursors: Vec<Cursor>) -> Self {
            Self { cursors }
        }
    }
    impl CursorStore for InMemoryCursorStore {
        async fn load(&self) -> Result<Vec<Cursor>> {
            Ok(self.cursors.clone())
        }

        async fn save(&mut self, cursors: &[Cursor]) -> Result<()> {
            self.cursors = cursors.to_vec();
            Ok(())
        }
    }

    #[async_trait]
    pub trait ManagedIndex: Send + Sync + 'static {
        fn name(&self) -> String;

        async fn handle_onchain_tx(&mut self, info: &BlockInfo, tx: &MultiEraTx) -> Result<()> {
            let _ = (info, tx);
            Ok(())
        }
        async fn handle_rollback(&mut self, info: &BlockInfo) -> Result<()> {
            let _ = info;
            Ok(())
        }
    }

    struct IndexWrapper {
        name: String,
        index: Box<dyn ManagedIndex>,
        tip: Point,
        force_restart: bool,
    }

    pub struct ChainIndexer<CS: CursorStore> {
        indexes: HashMap<String, IndexWrapper>,
        cursor_store: Option<CS>,
    }

    impl<CS: CursorStore> ChainIndexer<CS> {
        pub fn new(cursors: CS) -> Self {
            Self {
                indexes: HashMap::new(),
                cursor_store: Some(cursors),
            }
        }

        /// Begin managing an index.
        /// If the index doesn't already exist, it will start indexing from `start`.
        /// If it does, it will start from wherever it began before, unless `force_restart` is passed.
        pub fn add_index<M: ManagedIndex>(&mut self, index: M, start: Point, force_restart: bool) {
            let name = index.name();
            if self.indexes.insert(name.clone(), IndexWrapper {
                name,
                index: Box::new(index),
                tip: start,
                force_restart,
            }).is_some() {
                panic!("adding same index twice");
            }
        }
    }

    #[async_trait]
    impl<CS: CursorStore> Module for ChainIndexer<CS> {
        fn name(&self) -> String {
            "chain-indexer".into()
        }

        async fn init(&mut self, context: Arc<core::Context>) -> Result<()> {
            let mut blocks = context.subscribe("blocks").await?;
            let mut indexes = std::mem::take(&mut self.indexes);
            let mut cursor_store = self.cursor_store.take().unwrap();

            context.clone().run(async move {
                let should_fetch_cursors = indexes.values().any(|i| !i.force_restart);
                if should_fetch_cursors {
                    let cursors = cursor_store.load().await.expect("could not fetch cursors");
                    for cursor in cursors {
                        let Some(index) = indexes.get_mut(&cursor.name) else {
                            continue;
                        };
                        if !index.force_restart {
                            index.tip = cursor.point;
                        }
                    }
                }
                let first_point = indexes.values().map(|i| &i.tip).min_by(|l, r| compare_points(&l, &r)).cloned().unwrap_or(Point::Origin);
                context.publish("sync-from", AcropolisMessage::SyncFrom(first_point)).await.expect("could not start sync");
                while let Ok(message) = blocks.read().await {
                    match message {
                        AcropolisMessage::NewTx(info, tx) => {
                            let tx = MultiEraTx::decode(&tx).expect("invalid tx");
                            let tx = &tx;
                            let at = Point::Specific(info.slot, info.hash.0.to_vec());
                            process_message(indexes.values_mut(), at, |x| x.handle_onchain_tx(&info, tx)).await;
                        }
                        AcropolisMessage::Rollback(info) => {
                            let at = Point::Specific(info.slot, info.hash.0.to_vec());
                            process_message(indexes.values_mut(), at, |x| x.handle_rollback(&info)).await;
                        }
                        _ => {}
                    }
                    let cursors = indexes.values().map(|i| Cursor { name: i.name.clone(), point: i.tip.clone() }).collect::<Vec<_>>();
                    cursor_store.save(&cursors).await.expect("couldn't save cursors");
                }
            });
            Ok(())
        }
    }

    async fn process_message<'a, F, Fut>(indexes: impl Iterator<Item = &'a mut IndexWrapper>, at: Point, f: F)
    where
        F: Fn(&'a mut dyn ManagedIndex) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let mut fut = FuturesUnordered::new();
        for index in indexes {
            let this_is_next = index.tip.slot_or_default() == at.slot_or_default().saturating_sub(1);
            if this_is_next {
                fut.push(async {
                    match f(index.index.as_mut()).await {
                        Ok(()) => {
                            index.tip = at.clone();
                        }
                        Err(e) => {
                            eprintln!("index {} failed at {:?}: {e:#}", index.name, index.tip);
                        }
                    }
                });
            }
        }
        while fut.next().await.is_some() {
        }
    }

    fn compare_points<'a, 'b>(lhs: &'a Point, rhs: &'b Point) -> Ordering {
        lhs.slot_or_default().cmp(&rhs.slot_or_default())
    }
}