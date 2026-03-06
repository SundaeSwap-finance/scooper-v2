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
    sundaev3::{self, SundaeV3HistoricalState, SundaeV3Protocol},
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

async fn bootstrap_v4(
    provider: &(dyn BootstrapProvider + Send + Sync),
    protocol: &SundaeV4Protocol,
    state: &Arc<Mutex<SundaeV4HistoricalState>>,
    tip_slot: u64,
    dao: &dyn IndexerDao,
) -> Result<()> {
    info!("bootstrap: fetching V4 pools...");
    let pool_utxos = provider
        .fetch_pool_utxos_by_nft(&protocol.pool_nft_policy, &protocol.vault_script_hash)
        .await
        .context("bootstrap v4: fetch pool UTxOs")?;

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
        let pool_type = crate::sundaev4::detect_pool_type(
            &pool_datum,
            protocol.execution.as_ref(),
        );
        pools.insert(
            pool_datum.identifier.clone(),
            Arc::new(sundaev4::SundaeV4Pool {
                input,
                value: utxo.value.clone(),
                pool_datum,
                pool_type,
                slot: utxo.slot,
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
            match PlutusData::from_plutus_bytes(cbor)
                .map_err(|e| format!("{e}"))
                .and_then(|data| {
                    sundaev4::SimpleOrderDatum::from_plutus(data).map_err(|e| format!("{e}"))
                }) {
                Ok(datum) => {
                    orders.push(Arc::new(sundaev4::SundaeV4Order {
                        input,
                        value: utxo.value.clone(),
                        datum,
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
    for utxo in &settings_utxos {
        if !utxo.value.get(&protocol.settings_nft).is_positive() {
            continue;
        }
        let Some(ref cbor) = utxo.datum_cbor else {
            continue;
        };
        let Ok(data) = PlutusData::from_plutus_bytes(cbor) else {
            continue;
        };
        let Ok(datum) = sundaev4::SettingsDatum::from_plutus(data) else {
            continue;
        };
        let input = TransactionInput::new(utxo.tx_hash.into(), utxo.output_index);
        settings = Some(Arc::new(sundaev4::SundaeV4Settings {
            input,
            value: utxo.value.clone(),
            datum,
            slot: utxo.slot,
        }));
        break;
    }

    // Fetch wallet UTxOs if execution is configured
    let mut wallet_utxos = std::collections::BTreeMap::new();
    let mut scooper_addr_bytes: Vec<u8> = Vec::new();
    if let Some(ref exec) = protocol.execution {
        match sundaev4::derive_scooper_pallas_address(&exec.scooper_secret_key) {
            Err(e) => {
                warn!("bootstrap v4: could not derive scooper address: {e:#}");
            }
            Ok(addr) => {
                scooper_addr_bytes = addr.to_vec();
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
                        warn!("bootstrap v4: could not fetch wallet UTxOs: {e:#}");
                    }
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
            &scripts.constant_product,
            &scripts.fee_split,
            &scripts.fairness,
            &scripts.vault,
            &scripts.order,
            &scripts.pool_mint,
            &scripts.settings,
        ];
        if let Some(ref cs) = scripts.constant_sum {
            all_refs.push(cs);
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

        let vault_addr = ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::Script(protocol.vault_script_hash),
            ShelleyDelegationPart::Null,
        ).to_vec();
        for (_, pool) in &pools {
            let datum_bytes = pool.pool_datum.clone().to_plutus_bytes();
            persisted_txos.push(PersistedTxo {
                txo_id: pool.input.clone(),
                txo_type: "pool".to_string(),
                created_slot: tip_slot,
                era: 7,
                txo: encode_bootstrap_utxo(&vault_addr, &pool.value, Some(&datum_bytes)),
                address: vault_addr.clone(),
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
            }).await?;
            info!(txos = n, "bootstrap: persisted V4 state to DB");
        }
    }

    // Populate in-memory state
    {
        let mut locked = state.lock().await;
        let s = locked.update_slot(tip_slot)?;
        s.pools = pools;
        s.orders = orders;
        s.invalid_orders = invalid_orders;
        s.settings = settings;
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
