use anyhow::Result;
use serde::Deserialize;
use tracing::Level;
use tracing_subscriber::{
    Layer, Registry, filter::Targets, fmt, layer::SubscriberExt, util::SubscriberInitExt,
};

#[serde_with::serde_as]
#[derive(Debug, Deserialize)]
pub struct LogConfig {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub level: Level,
}

pub fn init(config: &LogConfig) -> Result<()> {
    let filter = Targets::new()
        .with_default(Level::INFO)
        .with_target("scooper_v2", config.level);

    Registry::default()
        .with(fmt::layer().compact().with_filter(filter))
        .init();

    Ok(())
}
