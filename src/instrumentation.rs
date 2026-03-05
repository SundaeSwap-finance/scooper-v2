use std::path::PathBuf;

use anyhow::Result;
use serde::Deserialize;
use tracing::Level;
use tracing_subscriber::{
    Layer, Registry, filter::Targets, fmt, layer::SubscriberExt, util::SubscriberInitExt,
};

#[derive(Debug, Deserialize, Default, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Compact,
    Json,
}

#[serde_with::serde_as]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LogConfig {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub level: Level,
    pub trace_directory: Option<PathBuf>,
    #[serde(default)]
    pub format: LogFormat,
}

pub fn init(config: &LogConfig) -> Result<()> {
    let filter = Targets::new()
        .with_default(Level::INFO)
        .with_target("scooper_v2", config.level);

    match config.format {
        LogFormat::Compact => {
            Registry::default()
                .with(fmt::layer().compact().with_filter(filter))
                .init();
        }
        LogFormat::Json => {
            Registry::default()
                .with(fmt::layer().json().with_filter(filter))
                .init();
        }
    }

    Ok(())
}
