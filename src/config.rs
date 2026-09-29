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

/// What an instance is for.
///
/// The public-facing box must not be able to sign. Making that a role the
/// binary enforces, rather than only the absence of a key file, means a key
/// that arrives on that box by accident stops it starting instead of
/// quietly arming it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeRole {
    /// Indexes, executes orders and signs transactions. The default, so an
    /// existing config keeps the behaviour it had.
    #[default]
    Scooper,
    /// Indexes, accepts strategy intents, gossips them to peers, and does
    /// nothing else. Holds no key, builds no transaction, and does not serve
    /// the controls that stop or restart the indexer.
    Observer,
}

impl NodeRole {
    pub fn is_observer(self) -> bool {
        matches!(self, Self::Observer)
    }
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub role: NodeRole,
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
            "config/mainnet-v4.json",
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

    /// The preview allowlist overlay loads through config-rs and restricts the
    /// pool it names.
    #[test]
    fn preview_allowlist_overlay_restricts_its_pool() {
        let config = load_config([
            "config/preview-v4.json",
            "config/preview-allowlist-test.json",
        ])
        .expect("config loads");
        let exec = config.protocol.v4.and_then(|v4| v4.execution).expect("v4 execution config");
        let pool = crate::sundaev3::Ident::new(
            &hex::decode("3b809fd966274082c16ea6e670dd7c5d384a44a8989a70fd9fe4cc45").unwrap(),
        );
        assert!(exec.pool_allowlists.is_restricted(&pool));
        assert_eq!(exec.pool_allowlists.0.len(), 1);
    }

    /// An uppercase blacklist entry loads through config-rs and still
    /// blacklists the pool.
    #[test]
    fn uppercase_blacklist_entry_still_blacklists_the_pool() {
        let pool = crate::sundaev3::Ident::new(&[0xab; 28]);
        let overlay = serde_json::json!({
            "protocol": { "v4": { "execution": { "blacklisted-pools": ["AB".repeat(28)] } } }
        });
        let path = std::env::temp_dir().join(format!(
            "scooper-blacklist-test-{}.json",
            std::process::id()
        ));
        std::fs::write(&path, overlay.to_string()).unwrap();
        let config = load_config(["config/preview-v4.json", path.to_str().unwrap()]);
        let _ = std::fs::remove_file(&path);
        let exec = config.expect("config loads").protocol.v4.and_then(|v4| v4.execution);
        assert!(exec.expect("v4 execution config").blacklisted_pools.contains(&pool));
    }
}

#[cfg(test)]
mod role_tests {
    use super::NodeRole;

    #[derive(serde::Deserialize)]
    struct Holder {
        #[serde(default)]
        role: NodeRole,
    }

    fn role_of(json: &str) -> NodeRole {
        serde_json::from_str::<Holder>(json).unwrap().role
    }

    #[test]
    fn the_default_is_the_scooper_it_has_always_been() {
        assert_eq!(role_of("{}"), NodeRole::Scooper);
        assert!(!role_of("{}").is_observer());
    }

    #[test]
    fn observer_is_spelled_in_kebab_case() {
        assert_eq!(role_of(r#"{"role":"observer"}"#), NodeRole::Observer);
        assert!(role_of(r#"{"role":"observer"}"#).is_observer());
    }

    #[test]
    fn an_unrecognised_role_is_refused_rather_than_defaulted() {
        // A typo must not quietly produce a scooper. It would then demand a
        // signing key and fail to start, which is the safe direction, but
        // the error should name the role rather than the missing key.
        for bad in [
            r#"{"role":"Observer"}"#,
            r#"{"role":"observe"}"#,
            r#"{"role":""}"#,
        ] {
            assert!(
                serde_json::from_str::<Holder>(bad).is_err(),
                "{bad} must not parse"
            );
        }
    }
}
