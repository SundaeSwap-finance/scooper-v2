use std::path::Path;

use anyhow::Result;
use config::{Config, File};
use serde::Deserialize;

use crate::persistence::PersistenceConfig;

pub const ROLLBACK_LIMIT: u64 = 2160;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub persistence: PersistenceConfig,
}

pub fn load_config(config_path: &Path) -> Result<Config> {
    Ok(Config::builder()
        .add_source(File::with_name("config/acropolis"))
        .add_source(File::with_name(&config_path.to_string_lossy()))
        .build()?)
}

pub fn use_mithril(cfg: &Config) -> bool {
    cfg.get_string("global.startup.method")
        .map(|m| m == "mithril")
        .unwrap_or(false)
}
