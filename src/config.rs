use std::sync::Arc;

use anyhow::Result;
use config::{Config, Environment, File};
use serde::Deserialize;

use crate::{
    bootstrap::BootstrapConfig, instrumentation::LogConfig, persistence::PersistenceConfig,
    server::ServerConfig, sundaev3::SundaeV3Protocol, sundaev4::SundaeV4Protocol,
};

pub const ROLLBACK_LIMIT: u64 = 2160;

/// Maximum number of malformed (unparseable) order UTxOs retained for API
/// reporting. Unlike spent-order/pool history (which ages out by slot), an
/// invalid order stays relevant for as long as its UTxO is unspent on chain
/// — V4 orders never expire, so a slot window would drop still-live records
/// (and bootstrap reloads them anyway, ignoring age). We therefore bound the
/// set by count, keeping the newest entries, so it can't grow without bound
/// under a spray of malformed orders while still answering "why is this order
/// malformed?" for anything recent.
pub const INVALID_ORDER_CAP: usize = 10_000;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub log: LogConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    pub protocol: ProtocolConfig,
    pub server: ServerConfig,
    #[serde(default)]
    pub acropolis: config::Map<String, config::Value>,
}
impl AppConfig {
    pub fn acropolis_config(&self) -> Result<Arc<Config>> {
        let config = Config::builder()
            .add_source(LiteralSource(self.acropolis.clone()))
            .build()?;
        Ok(Arc::new(config))
    }

    /// The Cardano network we're indexing ("mainnet", "preprod", "preview",
    /// or a custom name like "devnet"). Same key the genesis bootstrapper
    /// reads, with the same default; surfaced over the API so clients can map
    /// slots to wall-clock times.
    pub fn network_name(&self) -> String {
        self.acropolis_config()
            .ok()
            .and_then(|c| c.get_string("global.startup.network-name").ok())
            .unwrap_or_else(|| "mainnet".to_string())
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProtocolConfig {
    pub v3: Option<SundaeV3Protocol>,
    pub v4: Option<SundaeV4Protocol>,
    #[serde(default)]
    pub bootstrap: Option<BootstrapConfig>,
}

pub fn load_config<S: AsRef<str>>(config_files: impl IntoIterator<Item = S>) -> Result<AppConfig> {
    let mut builder = Config::builder().add_source(File::from_str(
        include_str!("../config/default.json"),
        config::FileFormat::Json,
    ));
    for config_file in config_files {
        builder = builder.add_source(File::with_name(config_file.as_ref()));
    }
    let config = builder
        .add_source(Environment::with_prefix("SCOOPER_V2"))
        .build()?;
    Ok(config.try_deserialize()?)
}

#[derive(Debug, Clone)]
struct LiteralSource(config::Map<String, config::Value>);
impl config::Source for LiteralSource {
    fn clone_into_box(&self) -> Box<dyn config::Source + Send + Sync> {
        Box::new((*self).clone())
    }

    fn collect(&self) -> Result<config::Map<String, config::Value>, config::ConfigError> {
        Ok(self.0.clone())
    }
}
