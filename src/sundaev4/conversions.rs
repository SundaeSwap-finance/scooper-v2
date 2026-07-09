//! Off-protocol conversion edges for the router.
//!
//! A conversion is a linear-rate asset transformation that isn't a Sundae
//! pool: minting Butane's ADAb by locking ADA (a PSM-style par window),
//! staking USDr for sUSDr, borrowing against collateral on a lending
//! protocol, etc. The router treats each as a constant-marginal edge —
//! `out = (in − fee(in)) · rate_num / rate_den`, no price impact — so path
//! finding can discover e.g. ADA → ADAb → NIGHT and the split optimizer can
//! weigh the edge against real pools' marginal rates.
//!
//! Routing (this module + a `PoolViewType::Conversion` view) is generic; how
//! a leg is *materialized* into the scoop transaction is per-mechanism. Only
//! mechanisms whose tx composition is actually wired may be offered to the
//! router — an edge the builder can't realize would stall every order routed
//! through it — hence `is_executable` below.

use serde::Deserialize;

use crate::bigint::BigInt;
use crate::cardano_types::AssetClass;

/// A configured conversion edge (one direction; configure the reverse as its
/// own edge with its own rate/fee — e.g. Butane's ADAb mint is free while
/// the burn direction pays a bps fee and is capped by pot capacity).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ConversionEdgeConfig {
    /// How the leg is realized in the transaction. Determines whether the
    /// edge is executable at all (see [`ConversionEdgeConfig::is_executable`]).
    pub mechanism: ConversionMechanism,
    /// Unique key for this edge, e.g. "butane:ADAb:mint". Used for logging,
    /// plan attribution, and the synthetic router ident.
    pub key: String,
    /// Input asset, "policy.token" hex ("" or "lovelace" for ADA).
    pub from: String,
    /// Output asset, same format.
    pub to: String,
    /// out = in · rate-num / rate-den (before fee).
    pub rate_num: u64,
    pub rate_den: u64,
    /// Fee in basis points, taken on the input side.
    #[serde(default)]
    pub fee_bps: u64,
    /// Max input the edge can absorb (e.g. burn capacity limited by pot
    /// supply). None = unlimited (mint direction).
    #[serde(default)]
    pub max_input: Option<u64>,
    /// Off by default: an operator opts each edge in explicitly.
    #[serde(default)]
    pub enabled: bool,
}

/// The tx-level realization of a conversion leg.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ConversionMechanism {
    /// Butane v2 "underlying" deposit: lock the underlying asset in a fresh
    /// pot UTxO, mint the synthetic at the params ratio. Contention-free
    /// (params UTxO is a reference input; every deposit creates a new pot).
    /// Tx composition NOT yet implemented — carried here so preview configs
    /// can already describe the deployment while the builder lands.
    ButaneUnderlyingDeposit {
        /// The synthetic's name on the mint policy, e.g. "ADAb".
        synthetic: String,
        /// Butane's state script hash (pot address payment credential), hex.
        state_script_hash: String,
        /// Butane's mint policy hash (also the pot address staking
        /// credential), hex.
        mint_policy_hash: String,
    },
}

/// A resolved, routable edge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversionEdge {
    pub key: String,
    pub from: AssetClass,
    pub to: AssetClass,
    pub rate_num: BigInt,
    pub rate_den: BigInt,
    /// Input-side fee as (num, den) over 10_000.
    pub fee_bps: u64,
    pub max_input: Option<BigInt>,
}

fn parse_asset(s: &str) -> Result<AssetClass, String> {
    if s.is_empty() || s == "lovelace" {
        return Ok(AssetClass { policy: vec![], token: vec![] });
    }
    let (policy, token) = s
        .split_once('.')
        .ok_or_else(|| format!("asset {s:?} must be \"policy.token\" hex or \"lovelace\""))?;
    Ok(AssetClass {
        policy: hex::decode(policy).map_err(|e| format!("asset policy {policy:?}: {e}"))?,
        token: hex::decode(token).map_err(|e| format!("asset token {token:?}: {e}"))?,
    })
}

impl ConversionEdgeConfig {
    /// Whether the tx builder can realize this edge today. Routing must
    /// never offer an edge the builder can't compose.
    pub fn is_executable(&self) -> bool {
        match &self.mechanism {
            // TODO(butane): flips to true when deposit+mint composition
            // lands in tx_builder (needs the preview registry outref and
            // params UTxO discovery).
            ConversionMechanism::ButaneUnderlyingDeposit { .. } => false,
        }
    }

    /// Resolve into a routable edge. Errors on malformed assets or rates.
    pub fn resolve(&self) -> Result<ConversionEdge, String> {
        if self.rate_num == 0 || self.rate_den == 0 {
            return Err(format!("conversion {}: rate must be positive", self.key));
        }
        if self.fee_bps >= 10_000 {
            return Err(format!("conversion {}: fee_bps must be < 10000", self.key));
        }
        Ok(ConversionEdge {
            key: self.key.clone(),
            from: parse_asset(&self.from)?,
            to: parse_asset(&self.to)?,
            rate_num: BigInt::from(self.rate_num),
            rate_den: BigInt::from(self.rate_den),
            fee_bps: self.fee_bps,
            max_input: self.max_input.map(BigInt::from),
        })
    }
}

/// Resolve the edges the router may use: enabled AND executable.
pub fn routable_edges(configs: &[ConversionEdgeConfig]) -> Vec<ConversionEdge> {
    configs
        .iter()
        .filter(|c| c.enabled)
        .filter(|c| {
            let ok = c.is_executable();
            if !ok {
                tracing::warn!(
                    key = %c.key,
                    "conversion edge enabled in config but its mechanism's tx \
                     composition isn't implemented yet — ignoring",
                );
            }
            ok
        })
        .filter_map(|c| match c.resolve() {
            Ok(e) => Some(e),
            Err(err) => {
                tracing::error!(key = %c.key, err, "invalid conversion edge config");
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(fee_bps: u64) -> ConversionEdgeConfig {
        ConversionEdgeConfig {
            mechanism: ConversionMechanism::ButaneUnderlyingDeposit {
                synthetic: "ADAb".into(),
                state_script_hash: "5b".repeat(28),
                mint_policy_hash: "1a".repeat(28),
            },
            key: "butane:ADAb:mint".into(),
            from: "lovelace".into(),
            to: format!("{}.{}", "1a".repeat(28), hex::encode(b"ADAb")),
            rate_num: 1,
            rate_den: 1,
            fee_bps,
            max_input: None,
            enabled: true,
        }
    }

    #[test]
    fn resolves_ada_and_rates() {
        let e = cfg(0).resolve().unwrap();
        assert!(e.from.policy.is_empty());
        assert_eq!(e.to.token, b"ADAb".to_vec());
        assert_eq!(e.rate_num, BigInt::from(1));
        assert!(cfg(10_000).resolve().is_err());
    }

    #[test]
    fn unimplemented_mechanisms_are_not_routable() {
        // Butane composition isn't wired yet: enabled or not, the edge must
        // not reach the router.
        let edges = routable_edges(&[cfg(0)]);
        assert!(edges.is_empty());
    }
}
