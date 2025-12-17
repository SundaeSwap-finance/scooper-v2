use std::sync::Arc;

use config::{Config, File};

const ACROPOLIS_CONFIG: &str = "config/acropolis";

pub fn make_config(scooper_config_file: &str) -> Result<(Arc<Config>, bool), config::ConfigError> {
    let cfg = Config::builder()
        .add_source(File::with_name(ACROPOLIS_CONFIG))
        .add_source(File::with_name(scooper_config_file))
        .build()?;

    let use_mithril = cfg
        .get_string("global.startup.method")
        .map(|m| m == "mithril")
        .unwrap_or(false);

    Ok((Arc::new(cfg), use_mithril))
}
