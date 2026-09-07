use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use num_traits::Signed;
use pallas_addresses::{Address, Network, ScriptHash, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart};
use pallas_codec::utils::CborWrap;
use pallas_primitives::{Bytes as PallasBytes, Hash, NonEmptyKeyValuePairs, PositiveCoin};
use pallas_primitives::conway;
use plutus_parser::{AsPlutus, PlutusData};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::{
    bigint::BigInt,
    cardano_types::{AssetClass, TransactionInput, Value},
    events::InvalidOrder,
    persistence::{IndexerDao, PersistedTxo, TxChanges},
    sundaev3::{self, Ident, SundaeV3HistoricalState, SundaeV3Protocol},
    sundaev4::{self, SundaeV4HistoricalState, SundaeV4Protocol},
};

use crate::cardano_types::CIP_67_ASSET_LABEL_222;

// ──────────────────────────────────────────────────────────────────────────────
// Configuration
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "source", rename_all = "kebab-case")]
pub enum BootstrapConfig {
    Kupo { url: String },
    Blockfrost {
        url: String,
        #[serde(rename = "project-id")]
        project_id: String,
    },
}

// ──────────────────────────────────────────────────────────────────────────────
// Internal types
// ──────────────────────────────────────────────────────────────────────────────

struct FetchedUtxo {
    tx_hash: [u8; 32],
    output_index: u64,
    value: Value,
    datum_cbor: Option<Vec<u8>>,
    slot: u64,
}

pub struct BootstrapResult {
    pub tip_slot: u64,
    pub tip_hash: String,
}

// ──────────────────────────────────────────────────────────────────────────────
// Encoding helpers for bootstrap persistence
// ──────────────────────────────────────────────────────────────────────────────

/// Convert internal `Value` to pallas Conway-era value for encoding bootstrap UTxOs.
fn value_to_conway(value: &Value) -> conway::Value {
    use num_traits::ToPrimitive;
    let ada_asset = AssetClass { policy: vec![], token: vec![] };
    let lovelace = value.get(&ada_asset).clone().unwrap().to_u64().unwrap_or(0);

    let mut policy_map: std::collections::BTreeMap<Vec<u8>, std::collections::BTreeMap<Vec<u8>, u64>> =
        std::collections::BTreeMap::new();
    for (policy_bytes, tokens) in &value.0 {
        if policy_bytes.is_empty() { continue; }
        for (token_bytes, qty) in tokens {
            let amt = qty.clone().unwrap().to_u64().unwrap_or(0);
            if amt > 0 {
                policy_map.entry(policy_bytes.clone()).or_default().insert(token_bytes.clone(), amt);
            }
        }
    }

    if policy_map.is_empty() {
        return conway::Value::Coin(lovelace);
    }

    let multiasset_pairs: Vec<_> = policy_map.into_iter().filter_map(|(policy, tokens)| {
        let policy_hash: Hash<28> = Hash::from(policy.as_slice());
        let token_pairs: Vec<_> = tokens.into_iter()
            .filter_map(|(name, qty)| PositiveCoin::try_from(qty).ok().map(|pc| (PallasBytes::from(name), pc)))
            .collect();
        if token_pairs.is_empty() { None }
        else { Some((policy_hash, NonEmptyKeyValuePairs::Def(token_pairs))) }
    }).collect();

    if multiasset_pairs.is_empty() {
        conway::Value::Coin(lovelace)
    } else {
        conway::Value::Multiasset(lovelace, NonEmptyKeyValuePairs::Def(multiasset_pairs))
    }
}

/// Encode a bootstrap UTxO as Conway-era PostAlonzoTransactionOutput bytes for DB persistence.
fn encode_bootstrap_utxo(
    address_bytes: &[u8],
    value: &Value,
    datum_cbor: Option<&[u8]>,
) -> Vec<u8> {
    let pallas_value = value_to_conway(value);
    let datum_option = datum_cbor.map(|cbor| {
        let pd: conway::PlutusData = minicbor::decode(cbor).expect("invalid datum CBOR in bootstrap");
        conway::PseudoDatumOption::Data(CborWrap(pd))
    });
    let txo = conway::TransactionOutput::PostAlonzo(
        pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
            address: PallasBytes::from(address_bytes.to_vec()),
            value: pallas_value,
            datum_option,
            script_ref: None,
        },
    );
    minicbor::to_vec(&txo).expect("infallible encoding")
}

// ──────────────────────────────────────────────────────────────────────────────
// Provider trait
// ──────────────────────────────────────────────────────────────────────────────

#[async_trait]
trait BootstrapProvider {
    async fn fetch_script_utxos(&self, script_hash: &ScriptHash) -> Result<Vec<FetchedUtxo>>;
    async fn fetch_address_utxos(&self, address: &str) -> Result<Vec<FetchedUtxo>>;
    async fn fetch_datum(&self, datum_hash: &str) -> Result<Vec<u8>>;
    async fn fetch_tip(&self) -> Result<(u64, String)>;

    /// Fetch pool UTxOs by discovering addresses that hold NFTs under the given policy.
    /// Pools often sit at addresses with staking credentials, so a simple script-hash-to-address
    /// lookup (with null staking) misses them. This method enumerates NFT holders instead.
    /// Default: falls back to fetch_script_utxos (works for Kupo's wildcard matching).
    async fn fetch_pool_utxos_by_nft(
        &self,
        _nft_policy: &ScriptHash,
        pool_script_hash: &ScriptHash,
    ) -> Result<Vec<FetchedUtxo>> {
        self.fetch_script_utxos(pool_script_hash).await
    }

    /// Fetch a PlutusV3 script's CBOR by its hash.
    /// Returns the raw script CBOR bytes (single-wrapped: CBOR bytestring containing FLAT UPLC).
    async fn fetch_script_cbor(&self, script_hash: &str) -> Result<Vec<u8>>;

    /// Fetch the CBOR of the transaction that first minted the given asset.
    ///
    /// Used to recover the original Create-redeemer payload for pools that were
    /// created before the scooper started (e.g. CS pools whose `prices`/`fee`
    /// only live in the Create withdrawal redeemer, not in the persistent datum).
    /// `asset_unit` is the Blockfrost asset id: hex(policy) ++ hex(asset_name).
    async fn fetch_first_mint_tx_cbor(&self, asset_unit: &str) -> Result<Vec<u8>> {
        let _ = asset_unit;
        anyhow::bail!(
            "first-mint tx lookup is not supported by this bootstrap provider; \
             use a Blockfrost source to bootstrap CS pools"
        )
    }

    /// Fetch tx hashes that involved `asset_unit`, newest first, paginated.
    /// Returns up to `page_size` results per call; `page` is 1-indexed.
    async fn fetch_asset_tx_hashes_desc(
        &self,
        _asset_unit: &str,
        _page: u32,
        _page_size: u32,
    ) -> Result<Vec<String>> {
        anyhow::bail!(
            "asset tx history lookup is not supported by this bootstrap provider; \
             use a Blockfrost source to recover per-pool module configs"
        )
    }

    /// Fetch a single transaction's CBOR by hash.
    async fn fetch_tx_cbor(&self, _tx_hash: &str) -> Result<Vec<u8>> {
        anyhow::bail!(
            "tx CBOR lookup is not supported by this bootstrap provider; \
             use a Blockfrost source to recover per-pool module configs"
        )
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Kupo provider
// ──────────────────────────────────────────────────────────────────────────────

struct KupoProvider {
    client: reqwest::Client,
    url: String,
}

impl KupoProvider {
    fn new(url: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.trim_end_matches('/').to_string(),
        }
    }
}

#[derive(Deserialize)]
struct KupoUtxo {
    transaction_id: String,
    output_index: u64,
    value: KupoValue,
    datum_hash: Option<String>,
    datum_type: Option<String>,
    datum: Option<String>,
    created_at: KupoSlotRef,
}

#[derive(Deserialize)]
struct KupoValue {
    coins: u64,
    #[serde(default)]
    assets: serde_json::Map<String, serde_json::Value>,
}

#[derive(Deserialize)]
struct KupoSlotRef {
    slot_no: u64,
}

#[derive(Deserialize)]
struct KupoDatumResponse {
    datum: String,
}

#[derive(Deserialize)]
struct KupoHealth {
    most_recent_checkpoint: Option<KupoSlotRef>,
    most_recent_node_tip: Option<KupoSlotRef>,
}

#[derive(Deserialize)]
struct KupoCheckpoint {
    slot_no: u64,
    header_hash: String,
}

fn parse_kupo_value(kv: &KupoValue) -> Value {
    let mut value = Value::default();
    let ada = AssetClass {
        policy: vec![],
        token: vec![],
    };
    value.insert(&ada, BigInt::from(kv.coins));
    for (asset_key, qty) in &kv.assets {
        // asset_key format: "policy_hex.asset_name_hex"
        let Some((policy_hex, name_hex)) = asset_key.split_once('.') else {
            continue;
        };
        let Ok(policy) = hex::decode(policy_hex) else {
            continue;
        };
        let Ok(token) = hex::decode(name_hex) else {
            continue;
        };
        let quantity = match qty {
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    BigInt::from(i)
                } else {
                    continue;
                }
            }
            _ => continue,
        };
        value.insert(&AssetClass { policy, token }, quantity);
    }
    value
}

#[async_trait]
impl BootstrapProvider for KupoProvider {
    async fn fetch_script_utxos(&self, script_hash: &ScriptHash) -> Result<Vec<FetchedUtxo>> {
        let hash_hex = hex::encode(script_hash.as_ref());
        let url = format!("{}/matches/{}/*?unspent", self.url, hash_hex);
        let resp: Vec<KupoUtxo> = self
            .client
            .get(&url)
            .send()
            .await
            .context("kupo: fetch script UTxOs")?
            .error_for_status()
            .context("kupo: script UTxOs status")?
            .json()
            .await
            .context("kupo: parse script UTxOs")?;

        let mut result = Vec::with_capacity(resp.len());
        for utxo in resp {
            let tx_bytes = hex::decode(&utxo.transaction_id)
                .context("kupo: invalid tx hash hex")?;
            let tx_hash: [u8; 32] = tx_bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("kupo: tx hash not 32 bytes"))?;

            let value = parse_kupo_value(&utxo.value);

            // Resolve datum: inline datums have datum_type="inline" and datum is the CBOR hex.
            // Hash datums have datum_type="hash" and datum_hash is set.
            let datum_cbor = if utxo.datum_type.as_deref() == Some("inline") {
                utxo.datum
                    .as_deref()
                    .and_then(|d| hex::decode(d).ok())
            } else if let Some(ref dh) = utxo.datum_hash {
                match self.fetch_datum(dh).await {
                    Ok(bytes) => Some(bytes),
                    Err(e) => {
                        warn!(datum_hash = %dh, "kupo: could not fetch datum: {e:#}");
                        None
                    }
                }
            } else {
                None
            };

            result.push(FetchedUtxo {
                tx_hash,
                output_index: utxo.output_index,
                value,
                datum_cbor,
                slot: utxo.created_at.slot_no,
            });
        }
        Ok(result)
    }

    async fn fetch_address_utxos(&self, address: &str) -> Result<Vec<FetchedUtxo>> {
        let url = format!("{}/matches/{}?unspent", self.url, address);
        let resp: Vec<KupoUtxo> = self
            .client
            .get(&url)
            .send()
            .await
            .context("kupo: fetch address UTxOs")?
            .error_for_status()
            .context("kupo: address UTxOs status")?
            .json()
            .await
            .context("kupo: parse address UTxOs")?;

        let mut result = Vec::with_capacity(resp.len());
        for utxo in resp {
            let tx_bytes = hex::decode(&utxo.transaction_id)
                .context("kupo: invalid tx hash hex")?;
            let tx_hash: [u8; 32] = tx_bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("kupo: tx hash not 32 bytes"))?;

            result.push(FetchedUtxo {
                tx_hash,
                output_index: utxo.output_index,
                value: parse_kupo_value(&utxo.value),
                datum_cbor: None, // wallet UTxOs don't need datums
                slot: utxo.created_at.slot_no,
            });
        }
        Ok(result)
    }

    async fn fetch_datum(&self, datum_hash: &str) -> Result<Vec<u8>> {
        let url = format!("{}/datums/{}", self.url, datum_hash);
        let resp: KupoDatumResponse = self
            .client
            .get(&url)
            .send()
            .await
            .context("kupo: fetch datum")?
            .error_for_status()
            .context("kupo: datum status")?
            .json()
            .await
            .context("kupo: parse datum")?;
        hex::decode(&resp.datum).context("kupo: invalid datum hex")
    }

    async fn fetch_tip(&self) -> Result<(u64, String)> {
        // Fetch the most recent checkpoint which includes the block header hash.
        let cp_url = format!("{}/checkpoints", self.url);
        if let Ok(resp) = self.client.get(&cp_url).send().await {
            if let Ok(checkpoints) = resp.json::<Vec<KupoCheckpoint>>().await {
                if let Some(cp) = checkpoints.last() {
                    return Ok((cp.slot_no, cp.header_hash.clone()));
                }
            }
        }
        // Fallback to health endpoint (no block hash available).
        let url = format!("{}/health", self.url);
        let resp: KupoHealth = self
            .client
            .get(&url)
            .send()
            .await
            .context("kupo: fetch health")?
            .error_for_status()
            .context("kupo: health status")?
            .json()
            .await
            .context("kupo: parse health")?;
        let slot = resp
            .most_recent_node_tip
            .or(resp.most_recent_checkpoint)
            .map(|s| s.slot_no)
            .unwrap_or(0);
        Ok((slot, String::new()))
    }

    async fn fetch_script_cbor(&self, script_hash: &str) -> Result<Vec<u8>> {
        // Kupo stores scripts alongside datums — try /scripts/{hash}
        let url = format!("{}/scripts/{}", self.url, script_hash);
        let resp: serde_json::Value = self
            .client
            .get(&url)
            .send()
            .await
            .context("kupo: fetch script")?
            .error_for_status()
            .context("kupo: script status")?
            .json()
            .await
            .context("kupo: parse script")?;
        let script_hex = resp["script"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("kupo: no script field for {script_hash}"))?;
        hex::decode(script_hex).context("kupo: invalid script hex")
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Blockfrost provider
// ──────────────────────────────────────────────────────────────────────────────

struct BlockfrostProvider {
    client: reqwest::Client,
    url: String,
    project_id: String,
    network: Network,
}

impl BlockfrostProvider {
    fn new(url: &str, project_id: &str) -> Self {
        let network = if url.contains("mainnet") {
            Network::Mainnet
        } else {
            Network::Testnet
        };
        Self {
            client: reqwest::Client::new(),
            url: url.trim_end_matches('/').to_string(),
            project_id: project_id.to_string(),
            network,
        }
    }

    fn script_address(&self, script_hash: &ScriptHash) -> String {
        let shelley = ShelleyAddress::new(
            self.network,
            ShelleyPaymentPart::Script(*script_hash),
            ShelleyDelegationPart::Null,
        );
        let addr = Address::from(shelley);
        addr.to_bech32().unwrap_or_default()
    }
}

#[derive(Deserialize)]
struct BlockfrostUtxo {
    tx_hash: String,
    tx_index: u64,
    amount: Vec<BlockfrostAmount>,
    data_hash: Option<String>,
    inline_datum: Option<serde_json::Value>,
    block: Option<String>,
}

#[derive(Deserialize)]
struct BlockfrostAmount {
    unit: String,
    quantity: String,
}

#[derive(Deserialize)]
struct BlockfrostPolicyAsset {
    asset: String,
}

#[derive(Deserialize)]
struct BlockfrostAssetAddress {
    address: String,
}

#[derive(Deserialize)]
struct BlockfrostDatumCbor {
    cbor: String,
}

#[derive(Deserialize)]
struct BlockfrostBlock {
    slot: Option<u64>,
    hash: Option<String>,
}

#[derive(Deserialize)]
struct BlockfrostBlockForUtxo {
    slot: Option<u64>,
}

#[derive(Deserialize)]
struct BlockfrostAssetHistory {
    tx_hash: String,
    action: String, // "minted" | "burned"
}

#[derive(Deserialize)]
struct BlockfrostTxCbor {
    cbor: String,
}

#[derive(Deserialize)]
struct BlockfrostAssetTx {
    tx_hash: String,
}

fn parse_blockfrost_value(amounts: &[BlockfrostAmount]) -> Value {
    let mut value = Value::default();
    for a in amounts {
        let quantity: i64 = a.quantity.parse().unwrap_or(0);
        if a.unit == "lovelace" {
            value.insert(
                &AssetClass {
                    policy: vec![],
                    token: vec![],
                },
                BigInt::from(quantity),
            );
        } else {
            // unit format: policy_hex ++ asset_name_hex (56 chars policy + rest asset name)
            if a.unit.len() < 56 {
                continue;
            }
            let (policy_hex, name_hex) = a.unit.split_at(56);
            let Ok(policy) = hex::decode(policy_hex) else {
                continue;
            };
            let Ok(token) = hex::decode(name_hex) else {
                continue;
            };
            value.insert(&AssetClass { policy, token }, BigInt::from(quantity));
        }
    }
    value
}

#[async_trait]
impl BootstrapProvider for BlockfrostProvider {
    async fn fetch_script_utxos(&self, script_hash: &ScriptHash) -> Result<Vec<FetchedUtxo>> {
        let bech32 = self.script_address(script_hash);
        self.fetch_address_utxos(&bech32).await
    }

    async fn fetch_address_utxos(&self, address: &str) -> Result<Vec<FetchedUtxo>> {
        let mut all_utxos = Vec::new();
        let mut page = 1u32;
        loop {
            let url = format!(
                "{}/addresses/{}/utxos?page={}&count=100",
                self.url, address, page
            );
            let resp = self
                .client
                .get(&url)
                .header("project_id", &self.project_id)
                .send()
                .await
                .context("blockfrost: fetch UTxOs")?;

            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                break;
            }
            let utxos: Vec<BlockfrostUtxo> = resp
                .error_for_status()
                .context("blockfrost: UTxOs status")?
                .json()
                .await
                .context("blockfrost: parse UTxOs")?;

            let batch_len = utxos.len();
            debug!("blockfrost: fetched page {page} ({batch_len} UTxOs, {} total so far)", all_utxos.len() + batch_len);
            for utxo in utxos {
                let tx_bytes =
                    hex::decode(&utxo.tx_hash).context("blockfrost: invalid tx hash hex")?;
                let tx_hash: [u8; 32] = tx_bytes
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("blockfrost: tx hash not 32 bytes"))?;

                let value = parse_blockfrost_value(&utxo.amount);

                // Resolve datum
                let datum_cbor = if utxo.inline_datum.is_some() {
                    // Blockfrost returns inline datum as JSON; we need the CBOR.
                    // Fall back to the datum CBOR endpoint using data_hash.
                    if let Some(ref dh) = utxo.data_hash {
                        match self.fetch_datum(dh).await {
                            Ok(bytes) => Some(bytes),
                            Err(e) => {
                                warn!(datum_hash = %dh, "blockfrost: could not fetch inline datum CBOR: {e:#}");
                                None
                            }
                        }
                    } else {
                        None
                    }
                } else if let Some(ref dh) = utxo.data_hash {
                    match self.fetch_datum(dh).await {
                        Ok(bytes) => Some(bytes),
                        Err(e) => {
                            warn!(datum_hash = %dh, "blockfrost: could not fetch datum: {e:#}");
                            None
                        }
                    }
                } else {
                    None
                };

                // Get slot from block hash
                let slot = if let Some(ref block_hash) = utxo.block {
                    let block_url = format!("{}/blocks/{}", self.url, block_hash);
                    match self
                        .client
                        .get(&block_url)
                        .header("project_id", &self.project_id)
                        .send()
                        .await
                    {
                        Ok(resp) => resp
                            .json::<BlockfrostBlockForUtxo>()
                            .await
                            .ok()
                            .and_then(|b| b.slot)
                            .unwrap_or(0),
                        Err(_) => 0,
                    }
                } else {
                    0
                };

                all_utxos.push(FetchedUtxo {
                    tx_hash,
                    output_index: utxo.tx_index,
                    value,
                    datum_cbor,
                    slot,
                });
            }

            if batch_len < 100 {
                break;
            }
            page += 1;
        }
        Ok(all_utxos)
    }

    async fn fetch_datum(&self, datum_hash: &str) -> Result<Vec<u8>> {
        let url = format!("{}/scripts/datum/{}/cbor", self.url, datum_hash);
        let resp: BlockfrostDatumCbor = self
            .client
            .get(&url)
            .header("project_id", &self.project_id)
            .send()
            .await
            .context("blockfrost: fetch datum")?
            .error_for_status()
            .context("blockfrost: datum status")?
            .json()
            .await
            .context("blockfrost: parse datum")?;
        hex::decode(&resp.cbor).context("blockfrost: invalid datum CBOR hex")
    }

    async fn fetch_tip(&self) -> Result<(u64, String)> {
        let url = format!("{}/blocks/latest", self.url);
        let resp: BlockfrostBlock = self
            .client
            .get(&url)
            .header("project_id", &self.project_id)
            .send()
            .await
            .context("blockfrost: fetch latest block")?
            .error_for_status()
            .context("blockfrost: latest block status")?
            .json()
            .await
            .context("blockfrost: parse latest block")?;
        Ok((resp.slot.unwrap_or(0), resp.hash.unwrap_or_default()))
    }

    async fn fetch_pool_utxos_by_nft(
        &self,
        nft_policy: &ScriptHash,
        _pool_script_hash: &ScriptHash,
    ) -> Result<Vec<FetchedUtxo>> {
        let policy_hex = hex::encode(nft_policy.as_ref());
        let cip67_label = "000de140";

        // 1. List all assets under the NFT policy, paginated.
        let mut nft_assets: Vec<String> = Vec::new();
        let mut page = 1u32;
        loop {
            let url = format!(
                "{}/assets/policy/{}?page={}&count=100",
                self.url, policy_hex, page
            );
            let resp = self
                .client
                .get(&url)
                .header("project_id", &self.project_id)
                .send()
                .await
                .context("blockfrost: fetch policy assets")?;
            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                break;
            }
            let assets: Vec<BlockfrostPolicyAsset> = resp
                .error_for_status()
                .context("blockfrost: policy assets status")?
                .json()
                .await
                .context("blockfrost: parse policy assets")?;
            let batch_len = assets.len();
            for a in assets {
                // Filter for CIP-67 label 222 NFTs (token name starts with 000de140).
                let token_hex = &a.asset[policy_hex.len()..];
                if token_hex.starts_with(cip67_label) {
                    nft_assets.push(a.asset);
                }
            }
            if batch_len < 100 {
                break;
            }
            page += 1;
        }
        info!(
            nft_count = nft_assets.len(),
            "blockfrost: discovered pool NFTs under policy {policy_hex}"
        );

        // 2. For each NFT, find the address holding it.
        let mut addresses = std::collections::BTreeSet::new();
        for asset in &nft_assets {
            let url = format!("{}/assets/{}/addresses", self.url, asset);
            let resp = self
                .client
                .get(&url)
                .header("project_id", &self.project_id)
                .send()
                .await;
            if let Ok(resp) = resp {
                if let Ok(holders) = resp.json::<Vec<BlockfrostAssetAddress>>().await {
                    for h in holders {
                        addresses.insert(h.address);
                    }
                }
            }
        }
        info!(
            address_count = addresses.len(),
            "blockfrost: discovered pool addresses"
        );

        // 3. Fetch UTxOs at each unique address.
        let mut all_utxos = Vec::new();
        for addr in &addresses {
            match self.fetch_address_utxos(addr).await {
                Ok(utxos) => all_utxos.extend(utxos),
                Err(e) => warn!(address = %addr, "blockfrost: could not fetch pool UTxOs: {e:#}"),
            }
        }
        Ok(all_utxos)
    }

    async fn fetch_script_cbor(&self, script_hash: &str) -> Result<Vec<u8>> {
        let url = format!("{}/scripts/{}/cbor", self.url, script_hash);
        let resp: serde_json::Value = self
            .client
            .get(&url)
            .header("project_id", &self.project_id)
            .send()
            .await
            .context("blockfrost: fetch script cbor")?
            .error_for_status()
            .context("blockfrost: script cbor status")?
            .json()
            .await
            .context("blockfrost: parse script cbor")?;
        let cbor_hex = resp["cbor"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("blockfrost: no cbor field for script {script_hash}"))?;
        hex::decode(cbor_hex).context("blockfrost: invalid script CBOR hex")
    }

    async fn fetch_first_mint_tx_cbor(&self, asset_unit: &str) -> Result<Vec<u8>> {
        // Page 1, ascending — the earliest history record for the asset is the mint tx.
        let history_url = format!(
            "{}/assets/{}/history?order=asc&page=1&count=1",
            self.url, asset_unit
        );
        let history: Vec<BlockfrostAssetHistory> = self
            .client
            .get(&history_url)
            .header("project_id", &self.project_id)
            .send()
            .await
            .context("blockfrost: fetch asset history")?
            .error_for_status()
            .context("blockfrost: asset history status")?
            .json()
            .await
            .context("blockfrost: parse asset history")?;
        let first = history.into_iter().next().ok_or_else(|| {
            anyhow::anyhow!("blockfrost: no history for asset {asset_unit}")
        })?;
        if first.action != "minted" {
            anyhow::bail!(
                "blockfrost: first history entry for {asset_unit} is {}, not 'minted'",
                first.action
            );
        }

        self.fetch_tx_cbor(&first.tx_hash).await
    }

    async fn fetch_asset_tx_hashes_desc(
        &self,
        asset_unit: &str,
        page: u32,
        page_size: u32,
    ) -> Result<Vec<String>> {
        let url = format!(
            "{}/assets/{}/transactions?order=desc&page={page}&count={page_size}",
            self.url, asset_unit,
        );
        let rows: Vec<BlockfrostAssetTx> = self
            .client
            .get(&url)
            .header("project_id", &self.project_id)
            .send()
            .await
            .context("blockfrost: fetch asset transactions")?
            .error_for_status()
            .context("blockfrost: asset transactions status")?
            .json()
            .await
            .context("blockfrost: parse asset transactions")?;
        Ok(rows.into_iter().map(|r| r.tx_hash).collect())
    }

    async fn fetch_tx_cbor(&self, tx_hash: &str) -> Result<Vec<u8>> {
        let tx_url = format!("{}/txs/{}/cbor", self.url, tx_hash);
        let resp: BlockfrostTxCbor = self
            .client
            .get(&tx_url)
            .header("project_id", &self.project_id)
            .send()
            .await
            .context("blockfrost: fetch tx cbor")?
            .error_for_status()
            .context("blockfrost: tx cbor status")?
            .json()
            .await
            .context("blockfrost: parse tx cbor")?;
        hex::decode(&resp.cbor).context("blockfrost: invalid tx CBOR hex")
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Orchestration
// ──────────────────────────────────────────────────────────────────────────────

pub async fn run_bootstrap(
    config: &BootstrapConfig,
    v3_protocol: Option<&SundaeV3Protocol>,
    v4_protocol: Option<&SundaeV4Protocol>,
    v3_state: &Option<Arc<Mutex<SundaeV3HistoricalState>>>,
    v4_state: &Option<Arc<Mutex<SundaeV4HistoricalState>>>,
    persistence: &Arc<dyn crate::persistence::Persistence>,
) -> Result<BootstrapResult> {
    let provider: Box<dyn BootstrapProvider + Send + Sync> = match config {
        BootstrapConfig::Kupo { url } => Box::new(KupoProvider::new(url)),
        BootstrapConfig::Blockfrost { url, project_id } => {
            Box::new(BlockfrostProvider::new(url, project_id))
        }
    };

    let (tip_slot, tip_hash) = provider.fetch_tip().await?;
    info!(tip_slot, tip_hash = %tip_hash, "bootstrap: fetched chain tip");

    if let (Some(proto), Some(state)) = (v3_protocol, v3_state) {
        bootstrap_v3(&*provider, proto, state, tip_slot).await?;
    }

    if let (Some(proto), Some(state)) = (v4_protocol, v4_state) {
        let dao = persistence.indexer_dao("sundae_v4");
        bootstrap_v4(&*provider, proto, state, tip_slot, &*dao).await?;
    }

    Ok(BootstrapResult { tip_slot, tip_hash })
}

async fn bootstrap_v3(
    provider: &(dyn BootstrapProvider + Send + Sync),
    protocol: &SundaeV3Protocol,
    state: &Arc<Mutex<SundaeV3HistoricalState>>,
    tip_slot: u64,
) -> Result<()> {
    info!("bootstrap: fetching V3 pools...");
    let pool_utxos = provider
        .fetch_pool_utxos_by_nft(&protocol.pool_script_hash, &protocol.pool_script_hash)
        .await
        .context("bootstrap v3: fetch pool UTxOs")?;

    let mut pools = std::collections::BTreeMap::new();
    for utxo in &pool_utxos {
        let Some(ref cbor) = utxo.datum_cbor else {
            continue;
        };
        let Ok(data) = PlutusData::from_plutus_bytes(cbor) else {
            warn!(
                tx_hash = %hex::encode(utxo.tx_hash),
                "bootstrap v3: could not decode pool datum CBOR"
            );
            continue;
        };
        let Ok(pool_datum) = sundaev3::PoolDatum::from_plutus(data) else {
            warn!(
                tx_hash = %hex::encode(utxo.tx_hash),
                "bootstrap v3: could not parse pool datum"
            );
            continue;
        };
        // Verify pool NFT
        let mut asset_name = CIP_67_ASSET_LABEL_222.to_vec();
        asset_name.extend_from_slice(&pool_datum.ident);
        let nft = AssetClass {
            policy: protocol.pool_script_hash.to_vec(),
            token: asset_name,
        };
        if !utxo.value.get(&nft).is_positive() {
            continue;
        }
        let input = TransactionInput::new(utxo.tx_hash.into(), utxo.output_index);
        pools.insert(
            pool_datum.ident.clone(),
            Arc::new(sundaev3::SundaeV3Pool {
                input,
                value: utxo.value.clone(),
                pool_datum,
                slot: utxo.slot,
            }),
        );
    }

    info!("bootstrap: fetching V3 orders...");
    let mut orders = Vec::new();
    let mut invalid_orders = Vec::new();
    for script_hash in &protocol.order_script_hashes {
        let order_utxos = provider
            .fetch_script_utxos(script_hash)
            .await
            .context("bootstrap v3: fetch order UTxOs")?;
        let n_utxos = order_utxos.len();
        info!("bootstrap: processing {n_utxos} V3 order UTxOs...");
        for (i, utxo) in order_utxos.iter().enumerate() {
            if (i + 1) % 100 == 0 || i + 1 == n_utxos {
                if i + 1 == n_utxos {
                    info!("bootstrap: parsed {}/{n_utxos} V3 orders ({} valid, {} invalid)", i + 1, orders.len(), invalid_orders.len());
                } else {
                    debug!("bootstrap: parsed {}/{n_utxos} V3 orders ({} valid, {} invalid)", i + 1, orders.len(), invalid_orders.len());
                }
            }
            let Some(ref cbor) = utxo.datum_cbor else {
                continue;
            };
            let input = TransactionInput::new(utxo.tx_hash.into(), utxo.output_index);
            match PlutusData::from_plutus_bytes(cbor)
                .map_err(|e| format!("{e}"))
                .and_then(|data| {
                    sundaev3::OrderDatum::from_plutus(data).map_err(|e| format!("{e}"))
                }) {
                Ok(datum) => {
                    orders.push(Arc::new(sundaev3::SundaeV3Order {
                        input,
                        value: utxo.value.clone(),
                        datum,
                        slot: utxo.slot,
                    }));
                }
                Err(reason) => {
                    warn!(input = %input, "bootstrap v3: invalid order datum: {reason}");
                    invalid_orders.push(InvalidOrder {
                        input,
                        slot: utxo.slot,
                        reason,
                    });
                }
            }
        }
    }

    info!("bootstrap: fetching V3 settings...");
    let settings_utxos = provider
        .fetch_script_utxos(&protocol.settings_script_hash)
        .await
        .context("bootstrap v3: fetch settings UTxOs")?;
    let mut settings = None;
    for utxo in &settings_utxos {
        // Check for settings NFT
        if !utxo.value.get(&protocol.settings_nft).is_positive() {
            continue;
        }
        let Some(ref cbor) = utxo.datum_cbor else {
            continue;
        };
        let Ok(data) = PlutusData::from_plutus_bytes(cbor) else {
            continue;
        };
        let Ok(datum) = sundaev3::SettingsDatum::from_plutus(data) else {
            continue;
        };
        let input = TransactionInput::new(utxo.tx_hash.into(), utxo.output_index);
        settings = Some(Arc::new(sundaev3::SundaeV3Settings {
            input,
            datum,
            slot: utxo.slot,
        }));
        break;
    }

    let n_pools = pools.len();
    let n_orders = orders.len();

    // Populate state
    {
        let mut locked = state.lock().await;
        let s = locked.update_slot(tip_slot)?;
        s.pools = pools;
        s.orders = orders;
        s.invalid_orders = invalid_orders;
        s.settings = settings;
    }

    info!(pools = n_pools, orders = n_orders, "V3 bootstrap complete");
    Ok(())
}

/// True iff this pool is constant-sum *and* we don't already know its config —
/// neither from `pool-configs` in the operator file nor from `cs_configs`
/// (preloaded persisted entries plus any we've recovered earlier in this pass).
fn needs_cs_lookup(
    pool_datum: &sundaev4::PoolDatum,
    execution: Option<&sundaev4::ScooperExecution>,
    cs_configs: &std::collections::BTreeMap<Ident, sundaev4::ConstantSumConfig>,
) -> bool {
    let Some(exec) = execution else {
        return false;
    };
    let Some(cs_script) = exec.module_scripts.constant_sum.as_ref() else {
        return false;
    };
    // CS swap actions use tag=3 (cs_check.ak's tag_swap), not tag=100. Match by
    // module hash across every enabled action instead — the tag isn't a
    // reliable discriminator between CS and CP.
    let is_cs = pool_datum.actions.iter().any(|a| {
        a.enabled
            && a.modules
                .first()
                .is_some_and(|h| h.as_slice() == cs_script.hash.as_ref())
    });
    if !is_cs {
        return false;
    }
    // It's CS. Skip lookup if we already have an answer from any source.
    let ident_hex = hex::encode(pool_datum.identifier.to_bytes());
    if exec.pool_configs.contains_key(&ident_hex) {
        return false;
    }
    if cs_configs.contains_key(&pool_datum.identifier) {
        return false;
    }
    true
}

/// True iff this pool is constant-product *and* we don't already know its
/// config. Same logic as `needs_cs_lookup` but matched against the CP module
/// hash.
fn needs_cp_lookup(
    pool_datum: &sundaev4::PoolDatum,
    execution: Option<&sundaev4::ScooperExecution>,
    cp_configs: &std::collections::BTreeMap<Ident, sundaev4::ConstantProductConfig>,
) -> bool {
    let Some(exec) = execution else { return false; };
    let Some(cp) = exec.module_scripts.constant_product.as_ref() else { return false; };
    let cp_hash = cp.hash;
    let is_cp = pool_datum.actions.iter().any(|a| {
        a.enabled
            && a.modules
                .first()
                .is_some_and(|h| h.as_slice() == cp_hash.as_ref())
    });
    if !is_cp { return false; }
    if cp_configs.contains_key(&pool_datum.identifier) { return false; }
    true
}

/// True iff this pool is concentrated-liquidity *and* we don't already know
/// its config (the spa/spb bounds + fee).
fn needs_cl_lookup(
    pool_datum: &sundaev4::PoolDatum,
    execution: Option<&sundaev4::ScooperExecution>,
    cl_configs: &std::collections::BTreeMap<Ident, sundaev4::ConcentratedLiquidityConfig>,
) -> bool {
    let Some(exec) = execution else { return false; };
    let Some(cl_script) = exec.module_scripts.concentrated_liquidity.as_ref() else {
        return false;
    };
    let is_cl = pool_datum.actions.iter().any(|a| {
        a.enabled
            && a.modules
                .first()
                .is_some_and(|h| h.as_slice() == cl_script.hash.as_ref())
    });
    if !is_cl { return false; }
    if cl_configs.contains_key(&pool_datum.identifier) { return false; }
    true
}

/// Per-pool module configs we try to recover during bootstrap. Each field is
/// `None` when either the module isn't configured for the protocol or we
/// couldn't find its config in the pool's tx history.
#[derive(Default, Debug)]
struct RecoveredPoolConfigs {
    cs: Option<sundaev4::ConstantSumConfig>,
    cp: Option<sundaev4::ConstantProductConfig>,
    cl: Option<sundaev4::ConcentratedLiquidityConfig>,
    fee_split: Option<sundaev4::FeeSplitConfig>,
}

/// Recover all module configs for a pool from its on-chain tx history.
///
/// Strategy:
/// 1. Fetch the pool's mint tx (first ever tx involving the pool NFT) and
///    extract every module config present there — this is the cheap, one-call
///    path for modules whose config only appears at Create.
/// 2. If any module's config is still missing, walk back through the pool's
///    asset tx history newest-first and extract from each tx. The first
///    occurrence we encounter (newest-first ⇒ latest in time) wins, so any
///    later Operate-form config update overrides the Create config.
/// 3. Stop as soon as every needed module is covered, or once we walk past
///    the first tx already inspected in step 1.
async fn lookup_pool_module_configs(
    provider: &(dyn BootstrapProvider + Send + Sync),
    protocol: &SundaeV4Protocol,
    ident: &Ident,
    need_cs: bool,
    need_cp: bool,
    need_cl: bool,
) -> Result<RecoveredPoolConfigs> {
    let exec = protocol
        .execution
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no execution config for module config lookup"))?;
    let cs_hash = exec.module_scripts.constant_sum.as_ref().map(|s| s.hash);
    let cl_hash = exec.module_scripts.concentrated_liquidity.as_ref().map(|s| s.hash);
    let cp_hash = exec.module_scripts.constant_product.as_ref().map(|s| s.hash);
    let fs_hash = exec.module_scripts.fee_split.hash;

    let mut asset_name = CIP_67_ASSET_LABEL_222.to_vec();
    asset_name.extend_from_slice(ident.to_bytes());
    let asset_unit = format!(
        "{}{}",
        hex::encode(protocol.pool_nft_policy.as_ref()),
        hex::encode(&asset_name)
    );

    let mut out = RecoveredPoolConfigs::default();
    let want_cs = need_cs && cs_hash.is_some();
    let want_cp = need_cp;
    let want_cl = need_cl && cl_hash.is_some();

    // Step 1: first tx (mint). Pool Create + per-module Create withdrawals are
    // here, so for static configs this single call is enough.
    debug!(asset = %asset_unit, "bootstrap v4: fetching first (mint) tx for module configs");
    let first_cbor = provider.fetch_first_mint_tx_cbor(&asset_unit).await?;
    let first_tx = pallas_traverse::MultiEraTx::decode(&first_cbor)
        .context("decode first tx CBOR")?;
    let first_tx_hash = hex::encode(first_tx.hash());
    if want_cs {
        if let Some(h) = cs_hash.as_ref() {
            out.cs = sundaev4::extract_cs_config_from_tx(&first_tx, h);
        }
    }
    if want_cp {
        if let Some(h) = cp_hash.as_ref() {
            out.cp = sundaev4::extract_cp_config_from_tx(&first_tx, h);
        }
    }
    if want_cl {
        if let Some(h) = cl_hash.as_ref() {
            out.cl = sundaev4::extract_cl_config_from_tx(&first_tx, h);
        }
    }
    out.fee_split = sundaev4::extract_fee_split_config_from_tx(&first_tx, &fs_hash);

    let still_missing = |c: &RecoveredPoolConfigs| {
        (want_cs && c.cs.is_none())
            || (want_cp && c.cp.is_none())
            || (want_cl && c.cl.is_none())
            || c.fee_split.is_none()
    };
    if !still_missing(&out) {
        return Ok(out);
    }

    // Step 2: walk back from the latest tx. For each module not yet covered,
    // the first occurrence we find here is the most recent config in time.
    let page_size: u32 = 100;
    'pages: for page in 1u32.. {
        let tx_hashes = provider
            .fetch_asset_tx_hashes_desc(&asset_unit, page, page_size)
            .await?;
        if tx_hashes.is_empty() {
            break;
        }
        let n = tx_hashes.len();
        for tx_hash in tx_hashes {
            // We already inspected the first tx in step 1; if we've walked all
            // the way back to it, there's nothing earlier and we should stop.
            if tx_hash == first_tx_hash {
                break 'pages;
            }
            let cbor = provider.fetch_tx_cbor(&tx_hash).await?;
            let tx = pallas_traverse::MultiEraTx::decode(&cbor)
                .context("decode walk-back tx CBOR")?;
            if want_cs && out.cs.is_none() {
                if let Some(h) = cs_hash.as_ref() {
                    out.cs = sundaev4::extract_cs_config_from_tx(&tx, h);
                }
            }
            if want_cp && out.cp.is_none() {
                if let Some(h) = cp_hash.as_ref() {
                    out.cp = sundaev4::extract_cp_config_from_tx(&tx, h);
                }
            }
            if want_cl && out.cl.is_none() {
                if let Some(h) = cl_hash.as_ref() {
                    out.cl = sundaev4::extract_cl_config_from_tx(&tx, h);
                }
            }
            if out.fee_split.is_none() {
                out.fee_split = sundaev4::extract_fee_split_config_from_tx(&tx, &fs_hash);
            }
            if !still_missing(&out) {
                break 'pages;
            }
        }
        if (n as u32) < page_size {
            break;
        }
    }

    Ok(out)
}

async fn bootstrap_v4(
    provider: &(dyn BootstrapProvider + Send + Sync),
    protocol: &SundaeV4Protocol,
    state: &Arc<Mutex<SundaeV4HistoricalState>>,
    tip_slot: u64,
    dao: &dyn IndexerDao,
) -> Result<()> {
    info!("bootstrap: fetching V4 pools...");
    let pool_utxos = provider
        .fetch_pool_utxos_by_nft(&protocol.pool_nft_policy, &protocol.pool_script_hash)
        .await
        .context("bootstrap v4: fetch pool UTxOs")?;

    // Pre-load any persisted per-module configs so we don't re-fetch them via
    // Blockfrost on every bootstrap. New entries discovered in this pass get
    // appended below. We split the flat persisted rows by module_hash into
    // per-module maps so callers can look up by pool ident.
    let cs_module_hash: Option<Vec<u8>> = protocol
        .execution
        .as_ref()
        .and_then(|e| e.module_scripts.constant_sum.as_ref())
        .map(|cs| cs.hash.as_ref().to_vec());
    let cp_module_hash: Option<Vec<u8>> = protocol
        .execution
        .as_ref()
        .and_then(|e| e.module_scripts.constant_product.as_ref())
        .map(|cp| cp.hash.as_ref().to_vec());
    let cl_module_hash: Option<Vec<u8>> = protocol
        .execution
        .as_ref()
        .and_then(|e| e.module_scripts.concentrated_liquidity.as_ref())
        .map(|cl| cl.hash.as_ref().to_vec());
    let fs_module_hash: Option<Vec<u8>> = protocol
        .execution
        .as_ref()
        .map(|e| e.module_scripts.fee_split.hash.as_ref().to_vec());

    let persisted_configs = dao
        .load_module_configs()
        .await
        .context("bootstrap v4: load persisted module configs")?;
    let mut cs_configs: std::collections::BTreeMap<
        Ident,
        sundaev4::ConstantSumConfig,
    > = std::collections::BTreeMap::new();
    let mut cp_configs: std::collections::BTreeMap<
        Ident,
        sundaev4::ConstantProductConfig,
    > = std::collections::BTreeMap::new();
    let mut cl_configs: std::collections::BTreeMap<
        Ident,
        sundaev4::ConcentratedLiquidityConfig,
    > = std::collections::BTreeMap::new();
    let mut fs_configs: std::collections::BTreeMap<
        Ident,
        sundaev4::FeeSplitConfig,
    > = std::collections::BTreeMap::new();
    for cfg in persisted_configs {
        let pd = PlutusData::from_plutus_bytes(&cfg.config_cbor)
            .context("bootstrap v4: persisted module config CBOR malformed")?;
        if Some(&cfg.module_hash) == cs_module_hash.as_ref() {
            let parsed = sundaev4::ConstantSumConfig::from_plutus(pd)
                .context("bootstrap v4: persisted CS config decode failed")?;
            cs_configs.insert(Ident::new(&cfg.pool_id), parsed);
        } else if Some(&cfg.module_hash) == cp_module_hash.as_ref() {
            let parsed = sundaev4::ConstantProductConfig::from_plutus(pd)
                .context("bootstrap v4: persisted CP config decode failed")?;
            cp_configs.insert(Ident::new(&cfg.pool_id), parsed);
        } else if Some(&cfg.module_hash) == cl_module_hash.as_ref() {
            let parsed = sundaev4::ConcentratedLiquidityConfig::from_plutus(pd)
                .context("bootstrap v4: persisted CL config decode failed")?;
            cl_configs.insert(Ident::new(&cfg.pool_id), parsed);
        } else if Some(&cfg.module_hash) == fs_module_hash.as_ref() {
            let parsed = sundaev4::FeeSplitConfig::from_plutus(pd)
                .context("bootstrap v4: persisted FS config decode failed")?;
            fs_configs.insert(Ident::new(&cfg.pool_id), parsed);
        }
        // Unknown module hashes are ignored; they may belong to modules not
        // configured in this protocol.
    }
    let preloaded_cs = cs_configs.len();
    let preloaded_cp = cp_configs.len();
    let preloaded_cl = cl_configs.len();
    let preloaded_fs = fs_configs.len();
    if preloaded_cs + preloaded_cp + preloaded_cl + preloaded_fs > 0 {
        info!(
            cs = preloaded_cs,
            cp = preloaded_cp,
            cl = preloaded_cl,
            fs = preloaded_fs,
            "bootstrap v4: hydrated per-module pool configs from DB",
        );
    }
    let mut new_persisted_configs: Vec<crate::persistence::PersistedModuleConfig> = Vec::new();

    let mut pools = std::collections::BTreeMap::new();
    for utxo in &pool_utxos {
        let Some(ref cbor) = utxo.datum_cbor else {
            continue;
        };
        let Ok(data) = PlutusData::from_plutus_bytes(cbor) else {
            warn!(
                tx_hash = %hex::encode(utxo.tx_hash),
                "bootstrap v4: could not decode pool datum CBOR"
            );
            continue;
        };
        let Ok(pool_datum) = sundaev4::PoolDatum::from_plutus(data) else {
            warn!(
                tx_hash = %hex::encode(utxo.tx_hash),
                "bootstrap v4: could not parse pool datum"
            );
            continue;
        };
        // Verify pool NFT
        let mut asset_name = CIP_67_ASSET_LABEL_222.to_vec();
        asset_name.extend_from_slice(&pool_datum.identifier);
        let nft = AssetClass {
            policy: protocol.pool_nft_policy.to_vec(),
            token: asset_name,
        };
        if !utxo.value.get(&nft).is_positive() {
            continue;
        }
        let input = TransactionInput::new(utxo.tx_hash.into(), utxo.output_index);

        // Recover any per-module configs we don't yet have for this pool. One
        // call walks the pool's tx history (first tx → walk back from latest)
        // and gathers every module config it can find.
        let need_cs = needs_cs_lookup(&pool_datum, protocol.execution.as_ref(), &cs_configs);
        let need_cp = needs_cp_lookup(&pool_datum, protocol.execution.as_ref(), &cp_configs);
        let need_cl = needs_cl_lookup(&pool_datum, protocol.execution.as_ref(), &cl_configs);
        let need_fs = !fs_configs.contains_key(&pool_datum.identifier);
        if need_cs || need_cp || need_cl || need_fs {
            let recovered = lookup_pool_module_configs(
                provider,
                protocol,
                &pool_datum.identifier,
                need_cs,
                need_cp,
                need_cl,
            )
            .await
            .with_context(|| {
                format!(
                    "bootstrap v4: failed to recover module configs for pool {}",
                    hex::encode(pool_datum.identifier.to_bytes())
                )
            })?;

            if need_cs {
                let cs_cfg = recovered.cs.ok_or_else(|| {
                    anyhow::anyhow!(
                        "bootstrap v4: pool {} is CS but no CS Create redeemer found in tx history",
                        hex::encode(pool_datum.identifier.to_bytes())
                    )
                })?;
                let cbor = minicbor::to_vec(&cs_cfg.clone().to_plutus())
                    .context("bootstrap v4: encode ConstantSumConfig CBOR")?;
                new_persisted_configs.push(crate::persistence::PersistedModuleConfig {
                    pool_id: pool_datum.identifier.to_bytes().to_vec(),
                    module_hash: cs_module_hash.clone().expect("cs_module_hash known when need_cs"),
                    config_cbor: cbor,
                    created_slot: utxo.slot,
                });
                cs_configs.insert(pool_datum.identifier.clone(), cs_cfg);
                info!(
                    pool = %hex::encode(pool_datum.identifier.to_bytes()),
                    "bootstrap v4: recovered CS pool config"
                );
            }
            if need_cp {
                if let Some(cp_cfg) = recovered.cp {
                    let cbor = minicbor::to_vec(&cp_cfg.clone().to_plutus())
                        .context("bootstrap v4: encode ConstantProductConfig CBOR")?;
                    new_persisted_configs.push(crate::persistence::PersistedModuleConfig {
                        pool_id: pool_datum.identifier.to_bytes().to_vec(),
                        module_hash: cp_module_hash.clone().expect("cp_module_hash known when need_cp"),
                        config_cbor: cbor,
                        created_slot: utxo.slot,
                    });
                    cp_configs.insert(pool_datum.identifier.clone(), cp_cfg);
                    info!(
                        pool = %hex::encode(pool_datum.identifier.to_bytes()),
                        "bootstrap v4: recovered CP pool config"
                    );
                } else {
                    warn!(
                        pool = %hex::encode(pool_datum.identifier.to_bytes()),
                        "bootstrap v4: no CP config found in pool's tx history — scoop will fall back to defaults"
                    );
                }
            }
            if need_cl {
                if let Some(cl_cfg) = recovered.cl {
                    let cbor = minicbor::to_vec(&cl_cfg.clone().to_plutus())
                        .context("bootstrap v4: encode ConcentratedLiquidityConfig CBOR")?;
                    new_persisted_configs.push(crate::persistence::PersistedModuleConfig {
                        pool_id: pool_datum.identifier.to_bytes().to_vec(),
                        module_hash: cl_module_hash.clone().expect("cl_module_hash known when need_cl"),
                        config_cbor: cbor,
                        created_slot: utxo.slot,
                    });
                    cl_configs.insert(pool_datum.identifier.clone(), cl_cfg);
                    info!(
                        pool = %hex::encode(pool_datum.identifier.to_bytes()),
                        "bootstrap v4: recovered CL pool config"
                    );
                } else {
                    warn!(
                        pool = %hex::encode(pool_datum.identifier.to_bytes()),
                        "bootstrap v4: CL pool but no Create config found in tx history — scoops will fail"
                    );
                }
            }
            if need_fs {
                if let Some(fs_cfg) = recovered.fee_split {
                    let cbor = minicbor::to_vec(&fs_cfg.clone().to_plutus())
                        .context("bootstrap v4: encode FeeSplitConfig CBOR")?;
                    new_persisted_configs.push(crate::persistence::PersistedModuleConfig {
                        pool_id: pool_datum.identifier.to_bytes().to_vec(),
                        module_hash: fs_module_hash.clone().expect("fs_module_hash known when execution present"),
                        config_cbor: cbor,
                        created_slot: utxo.slot,
                    });
                    fs_configs.insert(pool_datum.identifier.clone(), fs_cfg);
                    info!(
                        pool = %hex::encode(pool_datum.identifier.to_bytes()),
                        "bootstrap v4: recovered fee_split pool config"
                    );
                } else {
                    warn!(
                        pool = %hex::encode(pool_datum.identifier.to_bytes()),
                        "bootstrap v4: no fee_split config found in pool's tx history — scoop will fall back to defaults"
                    );
                }
            }
        }

        let resolved_cs = cs_configs.get(&pool_datum.identifier);
        let resolved_cp = cp_configs.get(&pool_datum.identifier);
        let resolved_cl = cl_configs.get(&pool_datum.identifier);
        let pool_type = crate::sundaev4::detect_pool_type(
            &pool_datum,
            protocol.execution.as_ref(),
            resolved_cs,
            resolved_cp,
            resolved_cl,
        );
        let fs_cfg = fs_configs.get(&pool_datum.identifier).cloned();
        pools.insert(
            pool_datum.identifier.clone(),
            Arc::new(sundaev4::SundaeV4Pool {
                input,
                value: utxo.value.clone(),
                pool_datum,
                pool_type,
                slot: utxo.slot,
                fee_split_config: fs_cfg,
            }),
        );
    }

    info!("bootstrap: fetching V4 orders...");
    let mut orders = Vec::new();
    let mut invalid_orders = Vec::new();
    for script_hash in &protocol.order_script_hashes {
        let order_utxos = provider
            .fetch_script_utxos(script_hash)
            .await
            .context("bootstrap v4: fetch order UTxOs")?;
        let n_utxos = order_utxos.len();
        info!("bootstrap: processing {n_utxos} V4 order UTxOs...");
        for (i, utxo) in order_utxos.iter().enumerate() {
            if (i + 1) % 100 == 0 || i + 1 == n_utxos {
                if i + 1 == n_utxos {
                    info!("bootstrap: parsed {}/{n_utxos} V4 orders ({} valid, {} invalid)", i + 1, orders.len(), invalid_orders.len());
                } else {
                    debug!("bootstrap: parsed {}/{n_utxos} V4 orders ({} valid, {} invalid)", i + 1, orders.len(), invalid_orders.len());
                }
            }
            let Some(ref cbor) = utxo.datum_cbor else {
                continue;
            };
            let input = TransactionInput::new(utxo.tx_hash.into(), utxo.output_index);
            let swap_order_hash: Vec<u8> = protocol
                .execution
                .as_ref()
                .and_then(|e| e.module_scripts.swap_order.as_ref())
                .map(|s| s.hash.as_ref().to_vec())
                .unwrap_or_default();
            let basic_order_hash: Vec<u8> = protocol
                .execution
                .as_ref()
                .and_then(|e| e.module_scripts.basic_order.as_ref())
                .map(|s| s.hash.as_ref().to_vec())
                .unwrap_or_default();
            let strategy_order_hash: Vec<u8> = protocol
                .execution
                .as_ref()
                .and_then(|e| e.module_scripts.strategy_order.as_ref())
                .map(|s| s.hash.as_ref().to_vec())
                .unwrap_or_default();
            match PlutusData::from_plutus_bytes(cbor)
                .map_err(|e| format!("{e}"))
                .and_then(|data| {
                    sundaev4::OrderDatum::from_plutus(data).map_err(|e| format!("{e}"))
                })
                .and_then(|datum| {
                    sundaev4::decode_order_constraint(
                        &datum, &swap_order_hash, &basic_order_hash, &strategy_order_hash,
                    )
                        .map(|constraint| (datum, constraint))
                }) {
                Ok((datum, constraint)) => {
                    orders.push(Arc::new(sundaev4::SundaeV4Order {
                        input,
                        value: utxo.value.clone(),
                        datum,
                        constraint,
                        slot: utxo.slot,
                    }));
                }
                Err(reason) => {
                    warn!(input = %input, "bootstrap v4: invalid order datum: {reason}");
                    invalid_orders.push(InvalidOrder {
                        input,
                        slot: utxo.slot,
                        reason,
                    });
                }
            }
        }
    }

    info!("bootstrap: fetching V4 settings...");
    let settings_utxos = provider
        .fetch_script_utxos(&protocol.settings_script_hash)
        .await
        .context("bootstrap v4: fetch settings UTxOs")?;
    let mut settings = None;
    let mut fee_settings: Option<Arc<sundaev4::SundaeV4FeeSettings>> = None;
    let fee_token: Option<Vec<u8>> = protocol
        .fee_settings_token
        .as_ref()
        .and_then(|t| hex::decode(t).ok());
    let mut order_configs: std::collections::BTreeMap<Vec<u8>, Arc<sundaev4::SundaeV4OrderConfig>> =
        std::collections::BTreeMap::new();
    for utxo in &settings_utxos {
        let Some(ref cbor) = utxo.datum_cbor else { continue };
        let Ok(data) = PlutusData::from_plutus_bytes(cbor) else { continue };
        if utxo.value.get(&protocol.settings_nft).is_positive() {
            // Global settings entry — empty-name token under settings_mint.
            if let Ok(datum) = sundaev4::SettingsDatum::from_plutus(data) {
                let input = TransactionInput::new(utxo.tx_hash.into(), utxo.output_index);
                settings = Some(Arc::new(sundaev4::SundaeV4Settings {
                    input,
                    value: utxo.value.clone(),
                    datum,
                    slot: utxo.slot,
                }));
            }
            continue;
        }
        // The FeeSettings node (docs/fee-system.md): matched by its
        // configured entry token; datum is `FeeSettings { base_fee }`.
        if let Some(want) = &fee_token {
            let has_token = utxo
                .value
                .0
                .get(&protocol.settings_nft.policy)
                .map(|tokens| {
                    tokens
                        .iter()
                        .any(|(name, qty)| name.as_slice() == want.as_slice() && qty.is_positive())
                })
                .unwrap_or(false);
            if has_token {
                if let Ok(fs) = sundaev4::FeeSettingsDatum::from_plutus(data.clone()) {
                    use num_traits::ToPrimitive;
                    if let Some(base_fee) = fs.base_fee.unwrap().to_u64() {
                        let input = TransactionInput::new(utxo.tx_hash.into(), utxo.output_index);
                        info!(base_fee, "bootstrap v4: hydrated FeeSettings node");
                        fee_settings = Some(Arc::new(sundaev4::SundaeV4FeeSettings {
                            input,
                            token: want.clone(),
                            base_fee,
                            slot: utxo.slot,
                        }));
                    }
                }
                continue;
            }
        }
        // Non-global settings entry: try OrderConfig (PR #11). Other shapes
        // (e.g. PoolConfig minted by mint-pool-config) are ignored — the
        // scooper doesn't consume them directly.
        let token_name = utxo
            .value
            .0
            .get(&protocol.settings_nft.policy)
            .and_then(|tokens| {
                tokens.iter().find_map(|(name, qty)| {
                    if !name.is_empty() && qty.is_positive() {
                        Some(name.clone())
                    } else {
                        None
                    }
                })
            });
        let parsed = sundaev4::OrderConfig::from_plutus(data).ok();
        if let (Some(token_name), Some(oc)) = (token_name, parsed) {
            let input = TransactionInput::new(utxo.tx_hash.into(), utxo.output_index);
            order_configs.insert(
                token_name.clone(),
                Arc::new(sundaev4::SundaeV4OrderConfig {
                    input,
                    value: utxo.value.clone(),
                    token_name,
                    config: oc,
                    slot: utxo.slot,
                }),
            );
        }
    }
    info!(count = order_configs.len(), "bootstrap v4: hydrated OrderConfig settings entries");

    // Fetch wallet UTxOs if execution is configured. Probe both the
    // enterprise address (payment-only) and, if a stake keyhash is configured,
    // the base address (payment + staking). CIP-1852 wallets fund the base
    // form, so we must check it explicitly.
    let mut wallet_utxos = std::collections::BTreeMap::new();
    let mut scooper_addr_bytes: Vec<u8> = Vec::new();
    if let Some(ref exec) = protocol.execution {
        let mut candidates: Vec<pallas_addresses::Address> = Vec::new();
        match sundaev4::derive_scooper_pallas_address_with_stake(
            &exec.scooper_secret_key,
            None,
        ) {
            Err(e) => warn!("bootstrap v4: could not derive enterprise address: {e:#}"),
            Ok(addr) => {
                scooper_addr_bytes = addr.to_vec();
                candidates.push(addr);
            }
        }
        if let Some(ref stake_kh) = exec.scooper_stake_keyhash {
            match sundaev4::derive_scooper_pallas_address_with_stake(
                &exec.scooper_secret_key,
                Some(stake_kh),
            ) {
                Err(e) => warn!("bootstrap v4: could not derive base address: {e:#}"),
                Ok(addr) => {
                    // Prefer the base form's bytes for the in-memory wallet
                    // records — that's what's on chain for these UTxOs.
                    scooper_addr_bytes = addr.to_vec();
                    candidates.push(addr);
                }
            }
        }
        for addr in &candidates {
            let addr_bech32 = addr.to_bech32().unwrap_or_default();
            info!("bootstrap: fetching V4 wallet UTxOs at {addr_bech32}...");
            match provider.fetch_address_utxos(&addr_bech32).await {
                Ok(utxos) => {
                    for utxo in &utxos {
                        let input =
                            TransactionInput::new(utxo.tx_hash.into(), utxo.output_index);
                        wallet_utxos.insert(input, utxo.value.clone());
                    }
                }
                Err(e) => {
                    warn!("bootstrap v4: could not fetch wallet UTxOs at {addr_bech32}: {e:#}");
                }
            }
        }
    }

    // Fetch reference script UTxOs if execution is configured.
    // The scooper needs these to build the ScriptStore for tx evaluation.
    let mut ref_utxo_outputs = std::collections::BTreeMap::new();
    if let Some(ref exec) = protocol.execution {
        info!("bootstrap: fetching V4 reference script UTxOs...");
        let scripts = &exec.module_scripts;
        let mut all_refs: Vec<&crate::sundaev4::ScriptRefInfo> = vec![
            &scripts.fee_split,
            &scripts.fairness,
            &scripts.pool,
            &scripts.order,
            &scripts.pool_mint,
            &scripts.settings,
        ];
        if let Some(ref cp) = scripts.constant_product {
            all_refs.push(cp);
        }
        if let Some(ref cs) = scripts.constant_sum {
            all_refs.push(cs);
        }
        if let Some(ref cl) = scripts.concentrated_liquidity {
            all_refs.push(cl);
        }
        if let Some(ref so) = scripts.swap_order {
            all_refs.push(so);
        }
        if let Some(ref bo) = scripts.basic_order {
            all_refs.push(bo);
        }
        if let Some(ref ro) = scripts.route_order {
            all_refs.push(ro);
        }
        if let Some(ref fo) = scripts.fairness_order {
            all_refs.push(fo);
        }
        if let Some(ref so) = scripts.strategy_order {
            all_refs.push(so);
        }
        if let Some(ref fc) = scripts.fee_constraint {
            all_refs.push(fc);
        }
        for script_ref in all_refs {
            let hash_hex = hex::encode(script_ref.hash.as_ref());
            match provider.fetch_script_cbor(&hash_hex).await {
                Ok(script_cbor) => {
                    // Blockfrost returns the *outer* CBOR (the full script encoding).
                    // We need to build a PlutusV3Script from the inner bytes.
                    // The API returns: CBOR-wrapped script bytes, same as what
                    // pallas stores in PlutusV3Script.
                    let script = pallas_primitives::PlutusScript::<3>(script_cbor.into());
                    // Dummy address — only the script_ref field matters.
                    let dummy_addr = pallas_addresses::Address::Shelley(
                        pallas_addresses::ShelleyAddress::new(
                            pallas_addresses::Network::Testnet,
                            pallas_addresses::ShelleyPaymentPart::Key(
                                pallas_primitives::Hash::new([0u8; 28]),
                            ),
                            pallas_addresses::ShelleyDelegationPart::Null,
                        ),
                    );
                    let txo = crate::cardano_types::TransactionOutput {
                        address: dummy_addr,
                        value: crate::cardano_types::Value::default(),
                        datum: crate::cardano_types::RawDatum::None,
                        script_ref: Some(crate::cardano_types::ScriptRef::PlutusV3(script)),
                    };
                    ref_utxo_outputs.insert(script_ref.ref_utxo.clone(), txo);
                }
                Err(e) => {
                    warn!(hash = %hash_hex, "bootstrap v4: could not fetch script CBOR: {e:#}");
                }
            }
        }
        info!(count = ref_utxo_outputs.len(), "bootstrap: fetched reference scripts");
    }

    let n_pools = pools.len();
    let n_orders = orders.len();
    let n_wallet = wallet_utxos.len();
    let n_refs = ref_utxo_outputs.len();

    // Persist bootstrap state to DB so subsequent restarts skip bootstrap.
    {
        let mut persisted_txos: Vec<PersistedTxo> = Vec::new();

        let pool_addr = ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::Script(protocol.pool_script_hash),
            ShelleyDelegationPart::Null,
        ).to_vec();
        for (_, pool) in &pools {
            let datum_bytes = pool.pool_datum.clone().to_plutus_bytes();
            persisted_txos.push(PersistedTxo {
                txo_id: pool.input.clone(),
                txo_type: "pool".to_string(),
                created_slot: tip_slot,
                era: 7,
                txo: encode_bootstrap_utxo(&pool_addr, &pool.value, Some(&datum_bytes)),
                address: pool_addr.clone(),
                datum: None,
            });
        }

        let order_addr = protocol.order_script_hashes.first().map(|h| {
            ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Script(*h),
                ShelleyDelegationPart::Null,
            ).to_vec()
        }).unwrap_or_default();
        for order in &orders {
            let datum_bytes = order.datum.clone().to_plutus_bytes();
            persisted_txos.push(PersistedTxo {
                txo_id: order.input.clone(),
                txo_type: "order".to_string(),
                created_slot: tip_slot,
                era: 7,
                txo: encode_bootstrap_utxo(&order_addr, &order.value, Some(&datum_bytes)),
                address: order_addr.clone(),
                datum: None,
            });
        }

        if let Some(ref s) = settings {
            let settings_addr = ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Script(protocol.settings_script_hash),
                ShelleyDelegationPart::Null,
            ).to_vec();
            let datum_bytes = s.datum.clone().to_plutus_bytes();
            persisted_txos.push(PersistedTxo {
                txo_id: s.input.clone(),
                txo_type: "settings".to_string(),
                created_slot: tip_slot,
                era: 7,
                txo: encode_bootstrap_utxo(&settings_addr, &s.value, Some(&datum_bytes)),
                address: settings_addr,
                datum: None,
            });
        }

        // OrderConfig settings entries must survive restarts: the live
        // indexer persists them as "order_config" txos, and load() rebuilds
        // state.order_configs from those rows — without this, a restart
        // after bootstrap silently drops every config and all scoops fail
        // the order validator's config resolution.
        for oc in order_configs.values() {
            let settings_addr = ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Script(protocol.settings_script_hash),
                ShelleyDelegationPart::Null,
            ).to_vec();
            let datum_bytes = oc.config.clone().to_plutus_bytes();
            persisted_txos.push(PersistedTxo {
                txo_id: oc.input.clone(),
                txo_type: "order_config".to_string(),
                created_slot: tip_slot,
                era: 7,
                txo: encode_bootstrap_utxo(&settings_addr, &oc.value, Some(&datum_bytes)),
                address: settings_addr,
                datum: None,
            });
        }

        // The FeeSettings node must survive restarts the same way (the
        // loader's "fee_settings" arm re-parses it from the persisted txo).
        if let Some(fs) = &fee_settings {
            let settings_addr = ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Script(protocol.settings_script_hash),
                ShelleyDelegationPart::Null,
            ).to_vec();
            let datum_bytes = sundaev4::FeeSettingsDatum {
                base_fee: crate::bigint::BigInt::from(fs.base_fee),
            }.to_plutus_bytes();
            let mut value = crate::cardano_types::Value::default();
            if let Some(want) = &fee_token {
                value.0.entry(protocol.settings_nft.policy.clone())
                    .or_default()
                    .insert(want.clone().into(), crate::bigint::BigInt::from(1u64));
            }
            persisted_txos.push(PersistedTxo {
                txo_id: fs.input.clone(),
                txo_type: "fee_settings".to_string(),
                created_slot: tip_slot,
                era: 7,
                txo: encode_bootstrap_utxo(&settings_addr, &value, Some(&datum_bytes)),
                address: settings_addr,
                datum: None,
            });
        }

        for (input, value) in &wallet_utxos {
            persisted_txos.push(PersistedTxo {
                txo_id: input.clone(),
                txo_type: "wallet".to_string(),
                created_slot: tip_slot,
                era: 7,
                txo: encode_bootstrap_utxo(&scooper_addr_bytes, value, None),
                address: scooper_addr_bytes.clone(),
                datum: None,
            });
        }

        let dummy_addr = ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::Key(Hash::new([0u8; 28])),
            ShelleyDelegationPart::Null,
        ).to_vec();
        for (input, output) in &ref_utxo_outputs {
            let txo_bytes = match &output.script_ref {
                Some(crate::cardano_types::ScriptRef::PlutusV3(script)) => {
                    let txo = conway::TransactionOutput::PostAlonzo(
                        pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
                            address: PallasBytes::from(dummy_addr.clone()),
                            value: conway::Value::Coin(2_000_000),
                            datum_option: None,
                            script_ref: Some(CborWrap(conway::PseudoScript::PlutusV3Script(script.clone()))),
                        },
                    );
                    minicbor::to_vec(&txo).expect("infallible encoding")
                }
                _ => encode_bootstrap_utxo(&dummy_addr, &output.value, None),
            };
            persisted_txos.push(PersistedTxo {
                txo_id: input.clone(),
                txo_type: "ref".to_string(),
                created_slot: tip_slot,
                era: 7,
                txo: txo_bytes,
                address: dummy_addr.clone(),
                datum: None,
            });
        }

        if !persisted_txos.is_empty() {
            let n = persisted_txos.len();
            dao.apply_tx_changes(TxChanges {
                slot: tip_slot,
                height: 0,
                created_txos: persisted_txos,
                spent_txos: vec![],
                metadata_datums: vec![],
                scoop_records: vec![],
                module_configs: new_persisted_configs.clone(),
            }).await?;
            info!(
                txos = n,
                module_configs = new_persisted_configs.len(),
                "bootstrap: persisted V4 state to DB"
            );
        }
    }

    // Populate in-memory state
    {
        let mut locked = state.lock().await;
        let s = locked.update_slot(tip_slot)?;
        s.pools = pools;
        s.orders = orders;
        // Bound the malformed-order set by count (newest by slot), matching the
        // live indexer's cap so it can't grow without limit from a spray of
        // unparseable orders.
        invalid_orders.sort_by_key(|io| io.slot);
        if invalid_orders.len() > crate::config::INVALID_ORDER_CAP {
            let overflow = invalid_orders.len() - crate::config::INVALID_ORDER_CAP;
            invalid_orders.drain(..overflow);
        }
        s.invalid_orders = invalid_orders;
        s.settings = settings;
        s.fee_settings = fee_settings;
        s.order_configs = order_configs;
        s.wallet_utxos = wallet_utxos;
        s.ref_utxo_outputs = ref_utxo_outputs;
        s.network_tip_slot = Some(tip_slot);
        s.tip_slot = tip_slot;
    }

    info!(
        pools = n_pools,
        orders = n_orders,
        wallet_utxos = n_wallet,
        ref_scripts = n_refs,
        "V4 bootstrap complete"
    );
    Ok(())
}

#[cfg(test)]
mod live_smoke_tests {
    //! Network-bound smoke tests against preview Blockfrost.
    //!
    //! Each test is `#[ignore]`'d so `cargo test` stays hermetic. Run with:
    //!     cargo test --bin scooper-v2 live_smoke -- --ignored --nocapture
    //!
    //! These verify the bootstrap lookup endpoints return what our
    //! deserializers expect, and that we can recover a CS pool's config
    //! end-to-end. If preview state changes (e.g. the asset id below is
    //! retired), update the constants — these are diagnostic, not regression
    //! gates.
    use super::*;
    use pallas_traverse::MultiEraTx;

    const PREVIEW_BLOCKFROST: &str = "https://cardano-preview.blockfrost.io/api/v0";
    const PREVIEW_PROJECT_ID: &str = "previewUJJvqX2v9TOOAis8dZWiuyTPfJxJIKgH";
    const PREVIEW_POOL_NFT_POLICY_HEX: &str =
        "9a30124e1071263f8f1b5da9f39436c3e80fab3a7bf7260af7682ad1";
    const PREVIEW_CS_MODULE_HASH_HEX: &str =
        "1eb851777361b9a2de1ed6a8a9c6efe510667cdae4cb3d741ac9d4da";

    #[tokio::test]
    #[ignore]
    async fn live_blockfrost_endpoints_match_struct_shapes() -> Result<()> {
        let provider = BlockfrostProvider::new(PREVIEW_BLOCKFROST, PREVIEW_PROJECT_ID);
        // Pick any pool NFT under the preview policy and verify we can walk
        // the asset-history -> tx-cbor pipeline without serde failures.
        let policy_hash: ScriptHash = PREVIEW_POOL_NFT_POLICY_HEX.parse()?;
        let pool_utxos = provider
            .fetch_pool_utxos_by_nft(&policy_hash, &policy_hash)
            .await?;
        let utxo = pool_utxos
            .iter()
            .find(|u| u.datum_cbor.is_some())
            .expect("preview must have at least one pool with a datum");
        let pool_datum = sundaev4::PoolDatum::from_plutus(
            PlutusData::from_plutus_bytes(utxo.datum_cbor.as_ref().unwrap())?,
        )?;
        let mut asset_name = CIP_67_ASSET_LABEL_222.to_vec();
        asset_name.extend_from_slice(pool_datum.identifier.to_bytes());
        let asset_unit = format!(
            "{}{}",
            PREVIEW_POOL_NFT_POLICY_HEX,
            hex::encode(&asset_name)
        );

        let tx_cbor = provider.fetch_first_mint_tx_cbor(&asset_unit).await?;
        let _tx = MultiEraTx::decode(&tx_cbor)?;
        eprintln!(
            "decoded Create tx for pool {} ({} bytes)",
            pool_datum.identifier,
            tx_cbor.len()
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn live_recover_cs_config_for_any_cs_pool() -> Result<()> {
        let provider = BlockfrostProvider::new(PREVIEW_BLOCKFROST, PREVIEW_PROJECT_ID);
        let policy_hash: ScriptHash = PREVIEW_POOL_NFT_POLICY_HEX.parse()?;
        let cs_hash: ScriptHash = PREVIEW_CS_MODULE_HASH_HEX.parse()?;
        let pool_utxos = provider
            .fetch_pool_utxos_by_nft(&policy_hash, &policy_hash)
            .await?;
        let mut found_cs = false;
        for utxo in &pool_utxos {
            let Some(ref cbor) = utxo.datum_cbor else { continue; };
            let Ok(pd) = PlutusData::from_plutus_bytes(cbor) else { continue; };
            let Ok(pool_datum) = sundaev4::PoolDatum::from_plutus(pd) else { continue; };
            // Detect "is this a CS pool" via the swap action's first module hash.
            let is_cs = pool_datum
                .actions
                .iter()
                .find(|a| a.tag == BigInt::from(100) && a.enabled)
                .and_then(|a| a.modules.first())
                .map(|h| h.as_slice() == cs_hash.as_ref())
                .unwrap_or(false);
            if !is_cs {
                continue;
            }
            let mut asset_name = CIP_67_ASSET_LABEL_222.to_vec();
            asset_name.extend_from_slice(pool_datum.identifier.to_bytes());
            let asset_unit = format!(
                "{}{}",
                PREVIEW_POOL_NFT_POLICY_HEX,
                hex::encode(&asset_name)
            );
            let tx_cbor = provider.fetch_first_mint_tx_cbor(&asset_unit).await?;
            let tx = MultiEraTx::decode(&tx_cbor)?;
            let cfg = sundaev4::extract_cs_config_from_tx(&tx, &cs_hash)
                .expect("Create redeemer should be parseable");
            eprintln!(
                "recovered CS config for {}: prices={:?} fee={}/{}",
                pool_datum.identifier, cfg.prices, cfg.fee.num, cfg.fee.den
            );
            found_cs = true;
        }
        if !found_cs {
            eprintln!("NOTE: no CS pools currently on preview — skipping deep verification");
        }
        Ok(())
    }
}
