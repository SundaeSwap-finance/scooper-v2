use std::sync::Arc;

use anyhow::Result;
use config::{Config, Environment, File};
use serde::Deserialize;

use crate::{
    bootstrap::BootstrapConfig, instrumentation::LogConfig, persistence::PersistenceConfig,
    server::ServerConfig, sundaev3::SundaeV3Protocol, sundaev4::SundaeV4Protocol,
};

pub const ROLLBACK_LIMIT: u64 = 2160;

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
