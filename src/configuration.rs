use std::sync::Arc;

use config::{Config, ConfigBuilder, File, builder::BuilderState};

const ACROPOLIS_CONFIG: &str = "config/acropolis";

pub fn make_config(scooper_config_file: &str) -> Result<(Arc<Config>, bool), config::ConfigError> {
    let mut builder = Config::builder()
        .add_source(File::with_name(ACROPOLIS_CONFIG))
        .add_source(File::with_name(scooper_config_file));

    let temp = builder.clone().build().unwrap();
    let use_mithril = temp.get_bool("use-mithril").unwrap_or(false);
    let network = temp
        .get_string("network")
        .expect("Network must be set in scooper.toml");

    if use_mithril {
        builder = configure_mithril(builder)?;
    }

    builder = configure_network(builder, network)?;

    Ok((Arc::new(builder.build()?), use_mithril))
}

fn configure_mithril<St>(
    mut builder: ConfigBuilder<St>,
) -> Result<ConfigBuilder<St>, config::ConfigError>
where
    St: BuilderState,
{
    builder = builder.set_override("global.startup.method", "mithril")?;
    builder = builder.set_override("module.peer-network-interface.sync-point", "snapshot")?;
    Ok(builder)
}

fn configure_network<St>(
    mut builder: ConfigBuilder<St>,
    network: String,
) -> Result<ConfigBuilder<St>, config::ConfigError>
where
    St: BuilderState,
{
    match network.as_str() {
        "mainnet" => {
            builder =
                builder.set_override("module.peer-network-interface.magic-number", 764824073)?;
            builder =
                builder.set_override("module.genesis-bootstrapper.network-name", "mainnet")?;
            Ok(builder)
        }
        "preview" => {
            builder = builder.set_override("module.peer-network-interface.magic-number", 2)?;
            builder =
                builder.set_override("module.genesis-bootstrapper.network-name", "preview")?;
            Ok(builder)
        }
        other => Err(config::ConfigError::Message(format!(
            "Invalid network selection: `{}` (expected `mainnet` or `preview`)",
            other
        ))),
    }
}
