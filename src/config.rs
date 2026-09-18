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
        let config = Config::builder().add_source(LiteralSource(self.acropolis.clone())).build()?;
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
    // `SCOOPER_V2_PERSISTENCE__SQLITE__FILENAME` overrides
    // `persistence.sqlite.filename`. Pinning prefix_separator keeps the prefix
    // `SCOOPER_V2_`; config-rs would otherwise take it from `separator`.
    let config = builder
        .add_source(Environment::with_prefix("SCOOPER_V2").prefix_separator("_").separator("__"))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The scooper follows one configured node. Peer sharing defaults to `true`
    /// upstream, which makes it dial peers it learned from that node — including,
    /// on a host that also runs a node for another network, a peer that refuses
    /// the handshake on a network-magic mismatch. `config/default.json` turns it
    /// off; this test keeps it off for every environment config.
    #[test]
    fn peer_sharing_is_disabled_for_every_environment() {
        for file in [
            "config/preview-v4.json",
            "config/preprod-v4.json",
            "config/mainnet.json",
        ] {
            let config = load_config([file]).expect("config loads");
            let acropolis = config.acropolis_config().expect("acropolis config builds");
            let enabled = acropolis
                .get_bool("module.peer-network-interface.peer-sharing-enabled")
                .unwrap_or(true);
            assert!(!enabled, "{file}: peer sharing must be disabled");
        }
    }

    /// Module hashes describe the deployment, not the operator. An
    /// execution-free config still has to classify order constraints and pool
    /// types — when they lived under `execution`, every order on such a node
    /// decoded as invalid and every pool as zero-fee constant product.
    #[test]
    fn module_hashes_survive_a_config_with_no_execution() {
        let config = load_config(["config/mainnet.json"]).expect("config loads");
        let v4 = config.protocol.v4.expect("mainnet configures v4");
        assert!(
            v4.execution.is_none(),
            "this test needs an indexer-only config"
        );
        assert!(!v4.module_scripts.swap_order_hash().is_empty());
        assert!(!v4.module_scripts.basic_order_hash().is_empty());
        assert!(v4.module_scripts.constant_product.is_some());
        v4.check_execution_ref_utxos().expect("no execution, so no ref-utxo requirement");
    }

    /// An executor that can't reference a module it declares would fail every
    /// scoop at build time, so startup refuses it.
    #[test]
    fn execution_without_a_ref_utxo_is_rejected() {
        let mut config = load_config(["config/preprod-v4.json"]).expect("config loads");
        let v4 = config.protocol.v4.as_mut().expect("preprod configures v4");
        assert!(v4.execution.is_some());
        v4.check_execution_ref_utxos().expect("preprod ships every ref-utxo");
        v4.module_scripts.fairness.ref_utxo = None;
        let err = v4.check_execution_ref_utxos().expect_err("missing ref-utxo must be caught");
        assert!(err.contains("fairness"), "{err}");
    }
}
