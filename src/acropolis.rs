// Everything in this file is a mock interface for what Acropolis can provide.
#![allow(unused)]
use std::sync::Arc;

use anyhow::Result;


pub mod core {
    use crate::BlockHash;

    use super::*;

    pub struct Process {

    }

    impl Process {
        pub fn create() -> Self {
            Self {}
        }
        pub fn register<T: Module>(&mut self, module: T) {}
        pub async fn run(&self) -> Result<()> {
            Ok(())
        }
    }

    pub enum Message {

    }

    pub struct Context {

    }

    pub struct BlockInfo {
        pub slot: u8,
        pub hash: BlockHash,
    }

    pub trait Module {
        fn name() -> String;
        async fn init(&self, context: Arc<Context>) -> Result<()>;
    }
}

pub mod indexer {
    use std::collections::HashMap;

    use pallas_network::miniprotocols::Point;
    use pallas_traverse::MultiEraTx;

    use crate::acropolis::core::{BlockInfo, Module};

    use super::*;

    pub trait ManagedIndex: Send + 'static {
        fn name(&self) -> String;
    }

    pub trait ManagedChainIndex: ManagedIndex {
        async fn handle_onchain_tx(&mut self, info: &BlockInfo, tx: &MultiEraTx) -> Result<()>;
        async fn handle_rollback(&mut self, info: &BlockInfo) -> Result<()>;
    }

    pub struct ChainIndexer {
        indexes: HashMap<String, Box<dyn ManagedIndex>>,
    }

    impl ChainIndexer {
        pub fn new() -> Self {
            Self {
                indexes: HashMap::new(),
            }
        }

        pub fn add_index<M: ManagedIndex>(&mut self, index: M, start: Point, force_restart: bool) {
            self.indexes.insert(index.name(), Box::new(index));
        }
    }

    impl Module for ChainIndexer {
        fn name() -> String {
            "chain-indexer".into()
        }

        async fn init(&self, context: Arc<core::Context>) -> Result<()> {
            Ok(())
        }
    }
}