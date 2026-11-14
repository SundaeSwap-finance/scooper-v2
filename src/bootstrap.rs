use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use num_traits::Signed;
use pallas_addresses::{ScriptHash, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart};
use pallas_codec::utils::CborWrap;
use pallas_primitives::conway;
use pallas_primitives::{Bytes as PallasBytes, Hash, NonEmptyKeyValuePairs, PositiveCoin};
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
use crate::sundaev4::ModuleKind;

// ──────────────────────────────────────────────────────────────────────────────
// Configuration
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "source", rename_all = "kebab-case")]
pub enum BootstrapConfig {
    Kupo {
        url: String,
    },
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
    /// Bech32 address holding the UTxO.
    address: String,
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
    let ada_asset = AssetClass {
        policy: vec![],
        token: vec![],
    };
    let lovelace = value.get(&ada_asset).clone().unwrap().to_u64().unwrap_or(0);

    let mut policy_map: std::collections::BTreeMap<
        Vec<u8>,
        std::collections::BTreeMap<Vec<u8>, u64>,
    > = std::collections::BTreeMap::new();
    for (policy_bytes, tokens) in &value.0 {
        if policy_bytes.is_empty() {
            continue;
        }
        for (token_bytes, qty) in tokens {
            let amt = qty.clone().unwrap().to_u64().unwrap_or(0);
            if amt > 0 {
                policy_map
                    .entry(policy_bytes.clone())
                    .or_default()
                    .insert(token_bytes.clone(), amt);
            }
        }
    }

    if policy_map.is_empty() {
        return conway::Value::Coin(lovelace);
    }

    let multiasset_pairs: Vec<_> = policy_map
        .into_iter()
        .filter_map(|(policy, tokens)| {
            let policy_hash: Hash<28> = Hash::from(policy.as_slice());
            let token_pairs: Vec<_> = tokens
                .into_iter()
                .filter_map(|(name, qty)| {
                    PositiveCoin::try_from(qty).ok().map(|pc| (PallasBytes::from(name), pc))
                })
                .collect();
            if token_pairs.is_empty() {
                None
            } else {
                Some((policy_hash, NonEmptyKeyValuePairs::Def(token_pairs)))
            }
        })
        .collect();

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
        let pd: conway::PlutusData =
            minicbor::decode(cbor).expect("invalid datum CBOR in bootstrap");
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
    /// One transaction output by its outpoint, as the chain holds it:
    /// address, value, datum. `Ok(None)` when the provider cannot answer.
    async fn fetch_tx_output(&self, tx_hash: &[u8; 32], index: u64) -> Result<Option<FetchedUtxo>>;
    async fn fetch_address_utxos(&self, address: &str) -> Result<Vec<FetchedUtxo>>;
    async fn fetch_tip(&self) -> Result<(u64, String)>;

    /// Whether this provider can enumerate a script's UTxOs by PAYMENT
    /// CREDENTIAL, i.e. find them at addresses carrying any staking part.
    ///
    /// Users attach their own stake credential to an order, so real orders
    /// sit at `addr1z...`, not at the enterprise form `addr1w...`. A provider
    /// that only matches one full address sees none of them and reports a
    /// healthy "0 order UTxOs".
    ///
    /// This matters because bootstrap does two things: it seeds state, and it
    /// then tells the indexer to start at the bootstrap tip. The second part
    /// discards all history before that tip. A provider that cannot enumerate
    /// by credential has not seen the orders, so skipping the history that
    /// contains them loses every pending order, with no error anywhere.
    /// `run_bootstrap` refuses rather than do that. The caller already falls
    /// back to a full replay from the configured starting point, which reads
    /// orders from the ledger and needs no provider at all.
    ///
    /// Kupo matches `{credential}/*`. Blockfrost accepts a bare payment
    /// credential (CIP-5 `script1...` / `addr_vkh1...`) wherever it accepts
    /// an address, and `/addresses/{credential}/utxos` returns the UTxOs at
    /// every address with that payment part. Both answer true.
    fn enumerates_by_credential(&self) -> bool {
        true
    }

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
    /// Datum by hash; `Ok(None)` when Kupo does not know it, an error otherwise.
    async fn fetch_datum_opt(&self, datum_hash: &str) -> Result<Option<Vec<u8>>> {
        let url = format!("{}/datums/{}", self.url, datum_hash);
        let resp = self.client.get(&url).send().await.context("kupo: fetch datum")?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp: KupoDatumResponse = resp
            .error_for_status()
            .context("kupo: datum status")?
            .json()
            .await
            .context("kupo: parse datum")?;
        hex::decode(&resp.datum).context("kupo: invalid datum hex").map(Some)
    }

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
    address: String,
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
    async fn fetch_tx_output(
        &self,
        _tx_hash: &[u8; 32],
        _index: u64,
    ) -> Result<Option<FetchedUtxo>> {
        Ok(None)
    }
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
            let tx_bytes =
                hex::decode(&utxo.transaction_id).context("kupo: invalid tx hash hex")?;
            let tx_hash: [u8; 32] =
                tx_bytes.try_into().map_err(|_| anyhow::anyhow!("kupo: tx hash not 32 bytes"))?;

            let value = parse_kupo_value(&utxo.value);

            // Resolve datum: inline datums have datum_type="inline" and datum is the CBOR hex.
            // Hash datums have datum_type="hash" and datum_hash is set.
            // A fetch failure is an error, not a missing datum: the bootstrap
            // skips a pool whose datum is None, and a snapshot persisted
            // without that pool is never repaired. Only "Kupo has no such
            // datum" (404) is a missing datum.
            let datum_cbor = if utxo.datum_type.as_deref() == Some("inline") {
                utxo.datum.as_deref().and_then(|d| hex::decode(d).ok())
            } else if let Some(ref dh) = utxo.datum_hash {
                self.fetch_datum_opt(dh).await.with_context(|| {
                    format!(
                        "kupo: datum {dh} for {}#{}",
                        utxo.transaction_id, utxo.output_index
                    )
                })?
            } else {
                None
            };

            result.push(FetchedUtxo {
                tx_hash,
                output_index: utxo.output_index,
                address: utxo.address,
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
            let tx_bytes =
                hex::decode(&utxo.transaction_id).context("kupo: invalid tx hash hex")?;
            let tx_hash: [u8; 32] =
                tx_bytes.try_into().map_err(|_| anyhow::anyhow!("kupo: tx hash not 32 bytes"))?;

            result.push(FetchedUtxo {
                tx_hash,
                output_index: utxo.output_index,
                address: utxo.address,
                value: parse_kupo_value(&utxo.value),
                datum_cbor: None, // wallet UTxOs don't need datums
                slot: utxo.created_at.slot_no,
            });
        }
        Ok(result)
    }

    async fn fetch_tip(&self) -> Result<(u64, String)> {
        // Fetch the most recent checkpoint which includes the block header hash.
        let cp_url = format!("{}/checkpoints", self.url);
        if let Ok(resp) = self.client.get(&cp_url).send().await
            && let Ok(checkpoints) = resp.json::<Vec<KupoCheckpoint>>().await
            && let Some(cp) = checkpoints.last()
        {
            return Ok((cp.slot_no, cp.header_hash.clone()));
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

/// Blockfrost rate-limits per project as a token bucket: a burst of 500
/// requests, refilled at 10 per second, and HTTP 429 beyond that. The
/// provider keeps the same bucket locally so a bootstrap spends the burst
/// and then settles at the refill rate instead of tripping 429 in a loop.
const BLOCKFROST_BURST: f64 = 500.0;
const BLOCKFROST_REFILL_PER_SEC: f64 = 10.0;
/// Attempts per request before a network error or 5xx becomes an error.
/// 429 is not counted: it clears within seconds and is retried until it does.
const BLOCKFROST_MAX_ATTEMPTS: u32 = 6;
/// Longest single back-off between attempts.
const BLOCKFROST_MAX_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(30);

/// Local mirror of Blockfrost's token bucket.
struct RateBucket {
    tokens: f64,
    refilled_at: tokio::time::Instant,
}

impl RateBucket {
    fn full(now: tokio::time::Instant) -> Self {
        Self {
            tokens: BLOCKFROST_BURST,
            refilled_at: now,
        }
    }

    /// Credit the refill since the last call, then take one token if there is
    /// one. Returns how long to wait for the next token otherwise.
    fn take(&mut self, now: tokio::time::Instant) -> Result<(), std::time::Duration> {
        let elapsed = now.saturating_duration_since(self.refilled_at).as_secs_f64();
        self.tokens = (self.tokens + elapsed * BLOCKFROST_REFILL_PER_SEC).min(BLOCKFROST_BURST);
        self.refilled_at = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return Ok(());
        }
        Err(std::time::Duration::from_secs_f64(
            (1.0 - self.tokens) / BLOCKFROST_REFILL_PER_SEC,
        ))
    }

    /// The server said we are over: our mirror is ahead of it, so empty it.
    fn drain(&mut self) {
        self.tokens = 0.0;
    }
}

/// How long to wait before attempt `attempt + 1`: the server's `Retry-After`
/// when it sends one, else 1s doubling per attempt, capped.
fn blockfrost_retry_delay(attempt: u32, retry_after_secs: Option<u64>) -> std::time::Duration {
    let backoff = std::time::Duration::from_secs(1u64 << attempt.min(10));
    let wanted = match retry_after_secs {
        Some(secs) => backoff.max(std::time::Duration::from_secs(secs)),
        None => backoff,
    };
    wanted.min(BLOCKFROST_MAX_RETRY_DELAY)
}

/// Blockfrost serves `inline_datum` on a UTxO as the datum's CBOR in hex.
/// Anything else (null, an object) yields None and the caller falls back to
/// the datum endpoint.
fn blockfrost_inline_datum_cbor(inline_datum: &serde_json::Value) -> Option<Vec<u8>> {
    inline_datum.as_str().and_then(|hex_str| hex::decode(hex_str).ok())
}

struct BlockfrostProvider {
    client: reqwest::Client,
    url: String,
    project_id: String,
    bucket: Mutex<RateBucket>,
    /// Block hash → slot, so a page of UTxOs from the same block costs one call.
    block_slots: Mutex<std::collections::HashMap<String, u64>>,
}

impl BlockfrostProvider {
    fn new(url: &str, project_id: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.trim_end_matches('/').to_string(),
            project_id: project_id.to_string(),
            bucket: Mutex::new(RateBucket::full(tokio::time::Instant::now())),
            block_slots: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Take one token from the bucket, sleeping until one is available.
    /// Callers awaiting concurrently are serialised by the lock.
    async fn throttle(&self) {
        let mut bucket = self.bucket.lock().await;
        loop {
            match bucket.take(tokio::time::Instant::now()) {
                Ok(()) => return,
                Err(wait) => tokio::time::sleep(wait).await,
            }
        }
    }

    /// One paced GET with the project id. A 429 drains the local bucket and
    /// is retried until it clears; a network error or 5xx is retried with
    /// back-off up to BLOCKFROST_MAX_ATTEMPTS. Every other status is returned
    /// as-is for the caller to interpret.
    async fn get(&self, url: &str) -> Result<reqwest::Response> {
        let mut failures = 0u32;
        let mut rate_limited = 0u32;
        loop {
            self.throttle().await;
            let sent = self.client.get(url).header("project_id", &self.project_id).send().await;
            let (status, retry_after) = match sent {
                Ok(resp) if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS => {
                    self.bucket.lock().await.drain();
                    rate_limited += 1;
                    let retry_after = resp
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok());
                    let delay = blockfrost_retry_delay(rate_limited.min(4) - 1, retry_after);
                    warn!(
                        rate_limited,
                        delay_ms = delay.as_millis() as u64,
                        "blockfrost: 429 Too Many Requests; backing off and retrying {url}"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                Ok(resp) if resp.status().is_server_error() => (resp.status().to_string(), None),
                Ok(resp) => return Ok(resp),
                Err(e) => (format!("{e:#}"), None),
            };
            failures += 1;
            if failures >= BLOCKFROST_MAX_ATTEMPTS {
                anyhow::bail!("blockfrost: {status} after {failures} attempts: GET {url}");
            }
            let delay = blockfrost_retry_delay(failures - 1, retry_after);
            warn!(
                attempt = failures,
                delay_ms = delay.as_millis() as u64,
                "blockfrost: {status}; retrying {url}"
            );
            tokio::time::sleep(delay).await;
        }
    }

    /// Datum CBOR by hash; `Ok(None)` when Blockfrost has no such datum
    /// (a hash-only output whose datum was never posted), an error otherwise.
    async fn fetch_datum_opt(&self, datum_hash: &str) -> Result<Option<Vec<u8>>> {
        let url = format!("{}/scripts/datum/{}/cbor", self.url, datum_hash);
        let resp = self.get(&url).await.context("blockfrost: fetch datum")?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp: BlockfrostDatumCbor = resp
            .error_for_status()
            .context("blockfrost: datum status")?
            .json()
            .await
            .context("blockfrost: parse datum")?;
        hex::decode(&resp.cbor).context("blockfrost: invalid datum CBOR hex").map(Some)
    }

    /// Slot of a block by hash, cached for the life of the provider.
    async fn block_slot(&self, block_hash: &str) -> Result<u64> {
        if let Some(slot) = self.block_slots.lock().await.get(block_hash) {
            return Ok(*slot);
        }
        let block_url = format!("{}/blocks/{}", self.url, block_hash);
        let block: BlockfrostBlockForUtxo = self
            .get(&block_url)
            .await?
            .error_for_status()
            .context("blockfrost: block status")?
            .json()
            .await
            .context("blockfrost: parse block")?;
        let slot = block.slot.unwrap_or(0);
        self.block_slots.lock().await.insert(block_hash.to_string(), slot);
        Ok(slot)
    }

    /// The CIP-5 bech32 form of a script payment credential (`script1...`).
    ///
    /// Blockfrost accepts this wherever it accepts a full address, and the
    /// UTxO query then covers every address with this payment part, whatever
    /// staking part the user attached. Querying the enterprise address
    /// (`addr1w...`) instead would match only outputs with no staking part,
    /// which is none of the UI's orders.
    fn script_credential(script_hash: &ScriptHash) -> String {
        bech32::encode::<bech32::Bech32>(
            bech32::Hrp::parse_unchecked("script"),
            script_hash.as_ref(),
        )
        .expect("28-byte script hash is well within the bech32 limit")
    }
}

#[derive(Deserialize)]
struct BlockfrostUtxo {
    /// The full address holding the output. Present on current Blockfrost;
    /// only there does a credential query learn the address the order was
    /// actually paid to.
    address: Option<String>,
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
    async fn fetch_tx_output(&self, tx_hash: &[u8; 32], index: u64) -> Result<Option<FetchedUtxo>> {
        #[derive(Deserialize)]
        struct TxUtxos {
            outputs: Vec<TxOutput>,
        }
        #[derive(Deserialize)]
        struct TxOutput {
            address: String,
            output_index: u64,
            amount: Vec<BlockfrostAmount>,
            data_hash: Option<String>,
            inline_datum: Option<serde_json::Value>,
        }
        let hash_hex = hex::encode(tx_hash);
        let url = format!("{}/txs/{}/utxos", self.url, hash_hex);
        let resp = self.get(&url).await.context("blockfrost: fetch tx utxos")?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let tx: TxUtxos = resp
            .error_for_status()
            .context("blockfrost: tx utxos status")?
            .json()
            .await
            .context("blockfrost: parse tx utxos")?;
        let Some(o) = tx.outputs.into_iter().find(|o| o.output_index == index) else {
            return Ok(None);
        };
        let inline = o.inline_datum.as_ref().and_then(blockfrost_inline_datum_cbor);
        let datum_cbor = match (inline, &o.data_hash) {
            (Some(bytes), _) => Some(bytes),
            (None, Some(dh)) => self.fetch_datum_opt(dh).await?,
            (None, None) => None,
        };
        Ok(Some(FetchedUtxo {
            tx_hash: *tx_hash,
            output_index: index,
            address: o.address,
            value: parse_blockfrost_value(&o.amount),
            datum_cbor,
            slot: 0,
        }))
    }
    /// Queries by payment credential, so UTxOs at every staking variant of
    /// the script address are returned. Each row's own `address` field says
    /// which variant.
    async fn fetch_script_utxos(&self, script_hash: &ScriptHash) -> Result<Vec<FetchedUtxo>> {
        self.fetch_address_utxos(&Self::script_credential(script_hash)).await
    }

    async fn fetch_address_utxos(&self, address: &str) -> Result<Vec<FetchedUtxo>> {
        let mut all_utxos = Vec::new();
        let mut page = 1u32;
        loop {
            let url = format!(
                "{}/addresses/{}/utxos?page={}&count=100",
                self.url, address, page
            );
            let resp = self.get(&url).await.context("blockfrost: fetch UTxOs")?;

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
            debug!(
                "blockfrost: fetched page {page} ({batch_len} UTxOs, {} total so far)",
                all_utxos.len() + batch_len
            );
            for utxo in utxos {
                let tx_bytes =
                    hex::decode(&utxo.tx_hash).context("blockfrost: invalid tx hash hex")?;
                let tx_hash: [u8; 32] = tx_bytes
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("blockfrost: tx hash not 32 bytes"))?;

                let value = parse_blockfrost_value(&utxo.amount);

                // Resolve the datum. The UTxO row carries an inline datum as
                // CBOR hex, so that costs nothing; only hash-only outputs need
                // the datum endpoint. A fetch failure is an error, not a
                // missing datum: bootstrap_v3/v4 skip a pool whose datum is
                // None, and a snapshot persisted without that pool is never
                // repaired.
                let inline = utxo.inline_datum.as_ref().and_then(blockfrost_inline_datum_cbor);
                let datum_cbor = match (inline, &utxo.data_hash) {
                    (Some(bytes), _) => Some(bytes),
                    (None, Some(dh)) => self.fetch_datum_opt(dh).await.with_context(|| {
                        format!(
                            "blockfrost: datum {dh} for {}#{}",
                            utxo.tx_hash, utxo.tx_index
                        )
                    })?,
                    (None, None) => None,
                };

                let slot = match utxo.block {
                    Some(ref block_hash) => self.block_slot(block_hash).await?,
                    None => 0,
                };

                all_utxos.push(FetchedUtxo {
                    tx_hash,
                    output_index: utxo.tx_index,
                    // `address` may be a bare credential; the row carries the
                    // real address, which downstream parses as bech32.
                    address: utxo.address.unwrap_or_else(|| address.to_string()),
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

    async fn fetch_tip(&self) -> Result<(u64, String)> {
        let url = format!("{}/blocks/latest", self.url);
        let resp: BlockfrostBlock = self
            .get(&url)
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
            let resp = self.get(&url).await.context("blockfrost: fetch policy assets")?;
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
            let resp = self.get(&url).await;
            if let Ok(resp) = resp
                && let Ok(holders) = resp.json::<Vec<BlockfrostAssetAddress>>().await
            {
                for h in holders {
                    addresses.insert(h.address);
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
            .get(&url)
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
            .get(&history_url)
            .await
            .context("blockfrost: fetch asset history")?
            .error_for_status()
            .context("blockfrost: asset history status")?
            .json()
            .await
            .context("blockfrost: parse asset history")?;
        let first = history
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("blockfrost: no history for asset {asset_unit}"))?;
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
            .get(&url)
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
            .get(&tx_url)
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

fn make_provider(config: &BootstrapConfig) -> Box<dyn BootstrapProvider + Send + Sync> {
    match config {
        BootstrapConfig::Kupo { url } => Box::new(KupoProvider::new(url)),
        BootstrapConfig::Blockfrost { url, project_id } => {
            Box::new(BlockfrostProvider::new(url, project_id))
        }
    }
}

/// Which of a pool's module configs are still unknown: (cs, cp, cl, fee_split).
///
/// Pure, so the decision that gates a chain lookup can be tested without one.
fn missing_module_configs(
    pool_datum: &sundaev4::PoolDatum,
    execution: Option<&sundaev4::ScooperExecution>,
    cs_configs: &std::collections::BTreeMap<Ident, sundaev4::ConstantSumConfig>,
    cp_configs: &std::collections::BTreeMap<Ident, sundaev4::ConstantProductConfig>,
    fs_configs: &std::collections::BTreeMap<Ident, sundaev4::FeeSplitConfig>,
    ss_configs: &std::collections::BTreeMap<Ident, sundaev4::StableSwapConfig>,
    bcl_configs: &std::collections::BTreeMap<Ident, sundaev4::BandedCLConfig>,
) -> (bool, bool, bool, bool, bool) {
    (
        needs_cs_lookup(pool_datum, execution, cs_configs),
        needs_cp_lookup(pool_datum, execution, cp_configs),
        pool_config_hashes(pool_datum, execution).fs.is_some()
            && !fs_configs.contains_key(&pool_datum.identifier),
        needs_ss_lookup(pool_datum, execution, ss_configs),
        needs_bcl_lookup(pool_datum, execution, bcl_configs),
    )
}

/// Recover, from chain, the module config of every known pool that the DB does
/// not yet have one for. Runs on every start, whatever the DB looks like.
///
/// A pool's constant-sum config is hash-pinned in its datum and lives only in
/// its Create redeemer. Until now the only code that recovered it ran inside
/// `run_bootstrap`, and `run_bootstrap` runs only when `tip_slot == 0` — a DB
/// that has never indexed a block. So a scooper that started once with a bad
/// config, or was stopped mid-sync, or simply joined after a pool was created,
/// carried a tip and never bootstrapped again: the config was unrecoverable,
/// the indexer fell back to guessed defaults, and every scoop against that pool
/// failed eval. Seen in the wild on the first third-party mainnet scooper.
///
/// Returns the idents whose configs changed, so the caller can re-classify the
/// pools already loaded into memory with the old defaults.
pub async fn recover_missing_module_configs(
    config: &BootstrapConfig,
    protocol: &SundaeV4Protocol,
    state: &Arc<Mutex<SundaeV4HistoricalState>>,
    persistence: &Arc<dyn crate::persistence::Persistence>,
) -> Result<Vec<Ident>> {
    let Some(exec) = protocol.execution.as_ref() else {
        return Ok(vec![]);
    };
    let dao = persistence.indexer_dao("sundae_v4");

    // What the DB already knows.
    let rows = dao.load_module_configs().await.context("recover: load persisted module configs")?;
    let mut known = KnownConfigs::from_rows(rows, Some(exec));

    // The pools we currently track, snapshotted so the lock is not held across
    // network calls.
    let pools: Vec<(sundaev4::PoolDatum, u64)> = {
        let locked = state.lock().await;
        let latest = locked.latest();
        latest.pools.values().map(|p| (p.pool_datum.clone(), p.slot)).collect()
    };

    let mut provider: Option<Box<dyn BootstrapProvider + Send + Sync>> = None;
    let mut persisted: Vec<crate::persistence::PersistedModuleConfig> = Vec::new();
    let mut changed: Vec<Ident> = Vec::new();
    let mut max_slot = 0u64;

    for (pool_datum, slot) in pools {
        let (a, b, c, d, e) = missing_module_configs(
            &pool_datum,
            Some(exec),
            &known.cs,
            &known.cp,
            &known.fs,
            &known.ss,
            &known.bcl,
        );
        if !(a || b || c || d || e) {
            continue;
        }
        let provider = provider.get_or_insert_with(|| make_provider(config));
        // Per pool: a failure is a WARN inside, never an abort.
        let rows = recover_pool_configs(&**provider, protocol, &pool_datum, slot, &mut known).await;
        if !rows.is_empty() {
            changed.push(pool_datum.identifier.clone());
            max_slot = max_slot.max(slot);
            persisted.extend(rows);
        }
    }

    if !persisted.is_empty() {
        // Upsert: the table keeps the earliest created_slot per (pool, module).
        let mut changes = crate::persistence::TxChanges::new(max_slot, 0);
        changes.module_configs = persisted;
        dao.apply_tx_changes(changes).await.context("recover: persist recovered module configs")?;
        info!(
            pools = changed.len(),
            "v4: persisted recovered module configs"
        );
    }
    Ok(changed)
}

pub async fn run_bootstrap(
    config: &BootstrapConfig,
    v3_protocol: Option<&SundaeV3Protocol>,
    v4_protocol: Option<&SundaeV4Protocol>,
    v3_state: &Option<Arc<Mutex<SundaeV3HistoricalState>>>,
    v4_state: &Option<Arc<Mutex<SundaeV4HistoricalState>>>,
    persistence: &Arc<dyn crate::persistence::Persistence>,
) -> Result<BootstrapResult> {
    let provider = make_provider(config);

    // Refuse to seed-and-skip with a provider that cannot see staked order
    // addresses. Returning Err here is deliberate: the caller logs it and
    // falls through to a chain replay from the configured starting point,
    // which reconstructs orders, pools, module configs and reference scripts
    // straight from the ledger. That replay is the bedrock recovery path and
    // it is strictly more complete than any bootstrap - bootstrap only ever
    // buys the time the replay would take.
    anyhow::ensure!(
        provider.enumerates_by_credential(),
        "this bootstrap source cannot enumerate UTxOs by payment credential, so it cannot \
         see orders at user-staked addresses (addr1z...). Seeding from it and then starting \
         the indexer at the bootstrap tip would silently discard every pending order. \
         Falling back to a full chain replay from the configured starting-point."
    );

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
                    info!(
                        "bootstrap: parsed {}/{n_utxos} V3 orders ({} valid, {} invalid)",
                        i + 1,
                        orders.len(),
                        invalid_orders.len()
                    );
                } else {
                    debug!(
                        "bootstrap: parsed {}/{n_utxos} V3 orders ({} valid, {} invalid)",
                        i + 1,
                        orders.len(),
                        invalid_orders.len()
                    );
                }
            }
            let Some(ref cbor) = utxo.datum_cbor else {
                continue;
            };
            let input = TransactionInput::new(utxo.tx_hash.into(), utxo.output_index);
            match PlutusData::from_plutus_bytes(cbor).map_err(|e| format!("{e}")).and_then(|data| {
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

/// The module hashes one pool names, for each kind whose per-pool config
/// the scooper recovers. Two pools can name different hashes of one kind.
#[derive(Clone, Copy, Debug, Default)]
struct PoolConfigHashes {
    cs: Option<pallas_addresses::ScriptHash>,
    cp: Option<pallas_addresses::ScriptHash>,
    ss: Option<pallas_addresses::ScriptHash>,
    bcl: Option<pallas_addresses::ScriptHash>,
    fs: Option<pallas_addresses::ScriptHash>,
}

fn pool_config_hashes(
    pool_datum: &sundaev4::PoolDatum,
    execution: Option<&sundaev4::ScooperExecution>,
) -> PoolConfigHashes {
    let Some(exec) = execution else {
        return PoolConfigHashes::default();
    };
    let ms = &exec.module_scripts;
    let invariant = ms.pool_invariant(pool_datum);
    let of = |kind: ModuleKind| invariant.filter(|(k, _)| *k == kind).map(|(_, h)| h);
    PoolConfigHashes {
        cs: of(ModuleKind::ConstantSum),
        cp: of(ModuleKind::ConstantProduct),
        ss: of(ModuleKind::Stableswap),
        bcl: of(ModuleKind::ConcentratedLiquidity),
        fs: ms.pool_module_of_kind(pool_datum, ModuleKind::FeeSplit),
    }
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
    // CS swap actions use tag=3 (cs_check.ak's tag_swap), not tag=100. The
    // pool's invariant module hash, not the tag, says it is CS.
    if pool_config_hashes(pool_datum, execution).cs.is_none() {
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

/// True iff this pool is stableswap *and* the config we hold (if any) does
/// not hash to the datum's `module_state` slot. Unlike the other modules the
/// config changes on chain (tag-7 rate updates), so "known" means "hashes to
/// the current datum", not "present". There is no operator override.
fn needs_ss_lookup(
    pool_datum: &sundaev4::PoolDatum,
    execution: Option<&sundaev4::ScooperExecution>,
    ss_configs: &std::collections::BTreeMap<Ident, sundaev4::StableSwapConfig>,
) -> bool {
    let Some(ss_hash) = pool_config_hashes(pool_datum, execution).ss else {
        return false;
    };
    !ss_configs.get(&pool_datum.identifier).is_some_and(|cfg| {
        crate::sundaev4::ss_math::config_matches_datum(cfg, pool_datum, ss_hash.as_ref())
    })
}

/// True iff this pool is banded *and* the config we hold (if any) does not
/// hash to the datum's `module_state` slot. There is no operator override.
fn needs_bcl_lookup(
    pool_datum: &sundaev4::PoolDatum,
    execution: Option<&sundaev4::ScooperExecution>,
    bcl_configs: &std::collections::BTreeMap<Ident, sundaev4::BandedCLConfig>,
) -> bool {
    let Some(h) = pool_config_hashes(pool_datum, execution).bcl else {
        return false;
    };
    // A form of the held config counts: a four-field config also covers a
    // pool a governor migrated to the five-field form (`config_forms`).
    !bcl_configs.get(&pool_datum.identifier).is_some_and(|cfg| {
        crate::sundaev4::banded_math::matching_form(cfg, pool_datum, h.as_ref()).is_some()
    })
}

/// True iff this pool is constant-product *and* we don't already know its
/// config. Same logic as `needs_cs_lookup` but matched against the CP module
/// hash.
fn needs_cp_lookup(
    pool_datum: &sundaev4::PoolDatum,
    execution: Option<&sundaev4::ScooperExecution>,
    cp_configs: &std::collections::BTreeMap<Ident, sundaev4::ConstantProductConfig>,
) -> bool {
    pool_config_hashes(pool_datum, execution).cp.is_some()
        && !cp_configs.contains_key(&pool_datum.identifier)
}

/// The per-pool module configs the scooper knows, by kind.
#[derive(Default)]
struct KnownConfigs {
    cs: std::collections::BTreeMap<Ident, sundaev4::ConstantSumConfig>,
    cp: std::collections::BTreeMap<Ident, sundaev4::ConstantProductConfig>,
    fs: std::collections::BTreeMap<Ident, sundaev4::FeeSplitConfig>,
    ss: std::collections::BTreeMap<Ident, sundaev4::StableSwapConfig>,
    bcl: std::collections::BTreeMap<Ident, sundaev4::BandedCLConfig>,
}

impl KnownConfigs {
    /// Split persisted rows into per-kind maps. A row of a hash the config
    /// does not register is ignored. A row that does not decode is skipped
    /// with a WARN: one bad row must not stop the protocol bootstrap, and
    /// its pool is then recovered from chain like a pool with no row.
    fn from_rows(
        rows: Vec<crate::persistence::PersistedModuleConfig>,
        execution: Option<&sundaev4::ScooperExecution>,
    ) -> Self {
        let mut known = KnownConfigs::default();
        let Some(exec) = execution else {
            return known;
        };
        for row in rows {
            let Some(kind) = exec.module_scripts.kind_of(&row.module_hash) else {
                continue;
            };
            let ident = Ident::new(&row.pool_id);
            let parsed: Result<()> = (|| {
                let pd = PlutusData::from_plutus_bytes(&row.config_cbor)
                    .context("persisted module config CBOR malformed")?;
                match kind {
                    ModuleKind::ConstantSum => {
                        known
                            .cs
                            .insert(ident.clone(), sundaev4::ConstantSumConfig::from_plutus(pd)?);
                    }
                    ModuleKind::ConstantProduct => {
                        known.cp.insert(
                            ident.clone(),
                            sundaev4::ConstantProductConfig::from_plutus(pd)?,
                        );
                    }
                    ModuleKind::FeeSplit => {
                        known.fs.insert(ident.clone(), sundaev4::FeeSplitConfig::from_plutus(pd)?);
                    }
                    ModuleKind::Stableswap => {
                        known
                            .ss
                            .insert(ident.clone(), sundaev4::StableSwapConfig::from_plutus(pd)?);
                    }
                    ModuleKind::ConcentratedLiquidity => {
                        known.bcl.insert(ident.clone(), sundaev4::BandedCLConfig::from_plutus(pd)?);
                    }
                    _ => {}
                }
                Ok(())
            })();
            if let Err(e) = parsed {
                warn!(
                    pool = %hex::encode(&row.pool_id),
                    module = %hex::encode(&row.module_hash),
                    "v4: persisted {kind} config does not decode; ignoring the row: {e:#}"
                );
            }
        }
        known
    }
}

/// Recover from chain the module configs of one pool that `known` does not
/// hold. Adds what it finds to `known` and returns the rows to persist.
///
/// This function never fails. A Blockfrost error or a config that no
/// redeemer holds is logged as a WARN that names the pool (and the module),
/// and the pool is left without that config. The other pools continue. A
/// pool without its config is not scooped: `module_configs_match_datum`
/// refuses it, because no guessed default hashes to its slot unless the
/// default is the pool's real config.
async fn recover_pool_configs(
    provider: &(dyn BootstrapProvider + Send + Sync),
    protocol: &SundaeV4Protocol,
    pool_datum: &sundaev4::PoolDatum,
    slot: u64,
    known: &mut KnownConfigs,
) -> Vec<crate::persistence::PersistedModuleConfig> {
    let execution = protocol.execution.as_ref();
    let ident = &pool_datum.identifier;
    let ident_hex = hex::encode(ident.to_bytes());
    let (need_cs, need_cp, need_fs, need_ss, need_bcl) = missing_module_configs(
        pool_datum, execution, &known.cs, &known.cp, &known.fs, &known.ss, &known.bcl,
    );
    if !(need_cs || need_cp || need_fs || need_ss || need_bcl) {
        return vec![];
    }
    info!(pool = %ident_hex, need_cs, need_cp, need_fs, need_ss, need_bcl, "v4: pool is missing module config(s); recovering from chain");
    let hashes = pool_config_hashes(pool_datum, execution);
    let wanted = PoolConfigHashes {
        cs: hashes.cs.filter(|_| need_cs),
        cp: hashes.cp.filter(|_| need_cp),
        ss: hashes.ss.filter(|_| need_ss),
        bcl: hashes.bcl.filter(|_| need_bcl),
        fs: hashes.fs.filter(|_| need_fs),
    };
    let recovered = match lookup_pool_module_configs(provider, protocol, pool_datum, wanted).await {
        Ok(r) => r,
        Err(e) => {
            warn!(
                pool = %ident_hex,
                "v4: module config recovery failed for this pool; skipping it (not scooped until \
                 a later scoop or restart recovers its config): {e:#}"
            );
            return vec![];
        }
    };

    let mut rows = vec![];
    let mut push = |module: &str, hash: Option<pallas_addresses::ScriptHash>, pd: PlutusData| {
        let Some(hash) = hash else {
            return;
        };
        match minicbor::to_vec(pd) {
            Ok(cbor) => {
                info!(pool = %ident_hex, module, "v4: recovered {module} config");
                rows.push(crate::persistence::PersistedModuleConfig {
                    pool_id: ident.to_bytes().to_vec(),
                    module_hash: hash.as_ref().to_vec(),
                    config_cbor: cbor,
                    created_slot: slot,
                });
            }
            Err(e) => warn!(pool = %ident_hex, module, "v4: encode recovered config: {e}"),
        }
    };
    let not_found = |module: &str, hash: Option<pallas_addresses::ScriptHash>| {
        warn!(
            pool = %ident_hex,
            module,
            module_hash = %hash.map(hex::encode).unwrap_or_default(),
            "v4: no redeemer in the pool's tx history, under any registered hash of this \
             module's kind, hashes to the pool's module_state slot; the pool is skipped and is \
             not scooped until a later scoop or recovery supplies the config"
        );
    };
    match recovered.cs {
        Some(cfg) => {
            push("constant_sum", wanted.cs, cfg.clone().to_plutus());
            known.cs.insert(ident.clone(), cfg);
        }
        None if need_cs => not_found("constant_sum", wanted.cs),
        None => {}
    }
    match recovered.cp {
        Some(cfg) => {
            push("constant_product", wanted.cp, cfg.clone().to_plutus());
            known.cp.insert(ident.clone(), cfg);
        }
        None if need_cp => not_found("constant_product", wanted.cp),
        None => {}
    }
    match recovered.ss {
        Some(cfg) => {
            push("stableswap", wanted.ss, cfg.clone().to_plutus());
            known.ss.insert(ident.clone(), cfg);
        }
        None if need_ss => not_found("stableswap", wanted.ss),
        None => {}
    }
    match recovered.bcl {
        Some(cfg) => {
            push(
                "concentrated_liquidity",
                wanted.bcl,
                cfg.clone().to_plutus(),
            );
            known.bcl.insert(ident.clone(), cfg);
        }
        None if need_bcl => not_found("concentrated_liquidity", wanted.bcl),
        None => {}
    }
    match recovered.fee_split {
        Some(cfg) => {
            push("fee_split", wanted.fs, cfg.clone().to_plutus());
            known.fs.insert(ident.clone(), cfg);
        }
        None if need_fs => not_found("fee_split", wanted.fs),
        None => {}
    }
    rows
}

/// Per-pool module configs we try to recover during bootstrap. Each field is
/// `None` when either the module isn't configured for the protocol or we
/// couldn't find its config in the pool's tx history.
#[derive(Default, Debug)]
struct RecoveredPoolConfigs {
    cs: Option<sundaev4::ConstantSumConfig>,
    cp: Option<sundaev4::ConstantProductConfig>,
    /// The stableswap config whose hash the pool's current datum names.
    ss: Option<sundaev4::StableSwapConfig>,
    /// The banded ladder whose hash the pool's current datum names.
    bcl: Option<sundaev4::BandedCLConfig>,
    fee_split: Option<sundaev4::FeeSplitConfig>,
}

impl RecoveredPoolConfigs {
    /// True while a wanted config (a `Some` field of `wanted`) is not found.
    fn missing(&self, wanted: &PoolConfigHashes) -> bool {
        (wanted.cs.is_some() && self.cs.is_none())
            || (wanted.cp.is_some() && self.cp.is_none())
            || (wanted.ss.is_some() && self.ss.is_none())
            || (wanted.bcl.is_some() && self.bcl.is_none())
            || (wanted.fs.is_some() && self.fee_split.is_none())
    }

    /// Fill each wanted, still-missing config from `tx`.
    ///
    /// Every rule is the same: read the redeemers of every registered hash
    /// of the module's kind (the pool's current hash first), and accept a
    /// candidate only when it hashes to the pool datum's slot for the
    /// current hash. A `Create` and an `Operate` entry both qualify. The
    /// slot hash pins the config value, so the newest match and the oldest
    /// match are the same config.
    fn fill_from(
        &mut self,
        tx: &pallas_traverse::MultiEraTx,
        pool_datum: &sundaev4::PoolDatum,
        scripts: &sundaev4::ModuleScripts,
        wanted: &PoolConfigHashes,
    ) {
        use sundaev4::{
            config_hashes_to_slot, extract_bcl_config_candidates, extract_cp_config_candidates,
            extract_cs_config_candidates, extract_fee_split_config_candidates,
            extract_ss_config_candidates_any, find_slot_config,
        };
        if self.cs.is_none()
            && let Some(h) = wanted.cs
        {
            self.cs = find_slot_config(
                tx,
                pool_datum,
                scripts,
                ModuleKind::ConstantSum,
                &h,
                extract_cs_config_candidates,
            );
        }
        if self.cp.is_none()
            && let Some(h) = wanted.cp
        {
            self.cp = find_slot_config(
                tx,
                pool_datum,
                scripts,
                ModuleKind::ConstantProduct,
                &h,
                extract_cp_config_candidates,
            );
        }
        if self.fee_split.is_none()
            && let Some(h) = wanted.fs
        {
            self.fee_split = find_slot_config(
                tx,
                pool_datum,
                scripts,
                ModuleKind::FeeSplit,
                &h,
                extract_fee_split_config_candidates,
            );
        }
        if self.ss.is_none()
            && let Some(h) = wanted.ss
        {
            self.ss = extract_ss_config_candidates_any(tx, scripts, &h)
                .into_iter()
                .find(|c| config_hashes_to_slot(c, pool_datum, h.as_ref()));
        }
        if self.bcl.is_none()
            && let Some(h) = wanted.bcl
        {
            // `extract_bcl_config_candidates` already reads every withdrawal
            // redeemer of the tx and adds the `ext = Void` form.
            self.bcl = extract_bcl_config_candidates(tx, &h)
                .into_iter()
                .find(|c| config_hashes_to_slot(c, pool_datum, h.as_ref()));
        }
    }
}

/// Recover the wanted module configs of one pool from its on-chain tx
/// history. A `Some` field of `wanted` is the hash of that kind that the
/// pool names now; a `None` field is not looked up.
///
/// Strategy:
/// 1. Fetch the pool's mint tx (first ever tx involving the pool NFT) and
///    extract every wanted config from it. For a pool that no governor has
///    migrated, this one call is enough.
/// 2. If any wanted config is still missing, walk back through the pool's
///    asset tx history newest-first and extract from each tx.
/// 3. Stop when every wanted config is found, when the walk reaches the
///    mint tx, or when the history ends.
///
/// Bound: each tx costs one `fetch_tx_cbor`, each 100 txs one page request.
/// A pool whose configs a recent scoop re-sends (every scoop re-sends them
/// in its `Operate` entries) stops after the first such tx. The worst case
/// is a pool whose config is in no redeemer: the walk reads the pool's whole
/// history and returns `None` for that config.
async fn lookup_pool_module_configs(
    provider: &(dyn BootstrapProvider + Send + Sync),
    protocol: &SundaeV4Protocol,
    pool_datum: &sundaev4::PoolDatum,
    wanted: PoolConfigHashes,
) -> Result<RecoveredPoolConfigs> {
    let mut out = RecoveredPoolConfigs::default();
    let Some(exec) = protocol.execution.as_ref() else {
        return Ok(out);
    };
    let scripts = &exec.module_scripts;
    if !out.missing(&wanted) {
        return Ok(out);
    }

    let mut asset_name = CIP_67_ASSET_LABEL_222.to_vec();
    asset_name.extend_from_slice(pool_datum.identifier.to_bytes());
    let asset_unit = format!(
        "{}{}",
        hex::encode(protocol.pool_nft_policy.as_ref()),
        hex::encode(&asset_name)
    );

    // Step 1: first tx (mint).
    debug!(asset = %asset_unit, "bootstrap v4: fetching first (mint) tx for module configs");
    let first_cbor = provider.fetch_first_mint_tx_cbor(&asset_unit).await?;
    let first_tx =
        pallas_traverse::MultiEraTx::decode(&first_cbor).context("decode first tx CBOR")?;
    let first_tx_hash = hex::encode(first_tx.hash());
    out.fill_from(&first_tx, pool_datum, scripts, &wanted);
    if !out.missing(&wanted) {
        return Ok(out);
    }

    // Step 2: walk back from the latest tx.
    let page_size: u32 = 100;
    let mut walked = 0usize;
    #[derive(Default)]
    struct Settled {
        cs: bool,
        cp: bool,
        ss: bool,
        bcl: bool,
        fs: bool,
    }
    let mut settled = Settled::default();
    'pages: for page in 1u32.. {
        let tx_hashes = provider.fetch_asset_tx_hashes_desc(&asset_unit, page, page_size).await?;
        if tx_hashes.is_empty() {
            break;
        }
        let n = tx_hashes.len();
        for tx_hash in tx_hashes {
            // We already inspected the first tx in step 1.
            if tx_hash == first_tx_hash {
                break 'pages;
            }
            let cbor = provider.fetch_tx_cbor(&tx_hash).await?;
            let tx =
                pallas_traverse::MultiEraTx::decode(&cbor).context("decode walk-back tx CBOR")?;
            walked += 1;
            out.fill_from(&tx, pool_datum, scripts, &wanted);
            if !out.missing(&wanted) {
                break 'pages;
            }
            // Every Operate entry re-sends the config in force, so the newest
            // tx that carries a withdrawal of the module's kind (under any
            // registered hash) settles that config: either it matched above,
            // or no candidate hashes to the slot and no older tx will do
            // better. Walking on from here is one fetch per tx through the
            // pool's whole history (7050069).
            let bears = |kind: ModuleKind| {
                scripts.modules.of_kind(kind).any(|e| {
                    sundaev4::withdrawal_index_of_script(&tx, &e.hash).is_some()
                })
            };
            for (kind, want, found, flag) in [
                (ModuleKind::ConstantSum, wanted.cs.is_some(), out.cs.is_some(), &mut settled.cs),
                (ModuleKind::ConstantProduct, wanted.cp.is_some(), out.cp.is_some(), &mut settled.cp),
                (ModuleKind::Stableswap, wanted.ss.is_some(), out.ss.is_some(), &mut settled.ss),
                (ModuleKind::ConcentratedLiquidity, wanted.bcl.is_some(), out.bcl.is_some(), &mut settled.bcl),
                (ModuleKind::FeeSplit, wanted.fs.is_some(), out.fee_split.is_some(), &mut settled.fs),
            ] {
                if want && !found && !*flag && bears(kind) {
                    *flag = true;
                    warn!(
                        asset = %asset_unit,
                        tx = %tx_hash,
                        ?kind,
                        "bootstrap v4: the newest tx bearing the module's withdrawal carries no config \
                         hashing to the pool's module_state; no older tx will, so this config is \
                         given up",
                    );
                }
            }
            let done = |want: bool, found: bool, flag: bool| !want || found || flag;
            if done(wanted.cs.is_some(), out.cs.is_some(), settled.cs)
                && done(wanted.cp.is_some(), out.cp.is_some(), settled.cp)
                && done(wanted.ss.is_some(), out.ss.is_some(), settled.ss)
                && done(wanted.bcl.is_some(), out.bcl.is_some(), settled.bcl)
                && done(wanted.fs.is_some(), out.fee_split.is_some(), settled.fs)
            {
                break 'pages;
            }
        }
        if (n as u32) < page_size {
            break;
        }
    }
    debug!(asset = %asset_unit, walked, missing = out.missing(&wanted), "bootstrap v4: config walk done");

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
    let persisted_configs =
        dao.load_module_configs().await.context("bootstrap v4: load persisted module configs")?;
    let mut known = KnownConfigs::from_rows(persisted_configs, protocol.execution.as_ref());
    if known.cs.len() + known.cp.len() + known.fs.len() > 0 {
        info!(
            cs = known.cs.len(),
            cp = known.cp.len(),
            fs = known.fs.len(),
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

        // Recover any per-module configs we don't yet have for this pool.
        // Per pool: a failure is a WARN naming the pool, never an abort of
        // the protocol bootstrap (`recover_pool_configs`).
        new_persisted_configs.extend(
            recover_pool_configs(provider, protocol, &pool_datum, utxo.slot, &mut known).await,
        );
        let hashes = pool_config_hashes(&pool_datum, protocol.execution.as_ref());
        let ss_module_hash = hashes.ss.map(|h| h.as_ref().to_vec());
        let bcl_module_hash = hashes.bcl.map(|h| h.as_ref().to_vec());

        let resolved_cs = known.cs.get(&pool_datum.identifier);
        let resolved_cp = known.cp.get(&pool_datum.identifier);
        // Only a config that hashes to the current datum is handed on.
        let resolved_ss = known.ss.get(&pool_datum.identifier).filter(|cfg| {
            ss_module_hash.as_ref().is_some_and(|h| {
                crate::sundaev4::ss_math::config_matches_datum(cfg, &pool_datum, h)
            })
        });
        let resolved_bcl = known.bcl.get(&pool_datum.identifier).and_then(|cfg| {
            bcl_module_hash
                .as_ref()
                .and_then(|h| crate::sundaev4::banded_math::matching_form(cfg, &pool_datum, h))
        });
        let pool_type = crate::sundaev4::detect_pool_type(
            &pool_datum,
            protocol.execution.as_ref(),
            resolved_cs,
            resolved_cp,
            resolved_ss,
            resolved_bcl.as_ref(),
        );
        let fs_cfg = known.fs.get(&pool_datum.identifier).cloned();
        // One pool's bad address skips that pool, not the bootstrap.
        let address = match pallas_addresses::Address::from_bech32(&utxo.address) {
            Ok(a) => a.to_vec(),
            Err(e) => {
                warn!(
                    pool = %hex::encode(pool_datum.identifier.to_bytes()),
                    address = %utxo.address,
                    "bootstrap v4: pool address does not parse; skipping the pool: {e}"
                );
                continue;
            }
        };
        pools.insert(
            pool_datum.identifier.clone(),
            Arc::new(sundaev4::SundaeV4Pool {
                input,
                address,
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
                    info!(
                        "bootstrap: parsed {}/{n_utxos} V4 orders ({} valid, {} invalid)",
                        i + 1,
                        orders.len(),
                        invalid_orders.len()
                    );
                } else {
                    debug!(
                        "bootstrap: parsed {}/{n_utxos} V4 orders ({} valid, {} invalid)",
                        i + 1,
                        orders.len(),
                        invalid_orders.len()
                    );
                }
            }
            let Some(ref cbor) = utxo.datum_cbor else {
                continue;
            };
            let input = TransactionInput::new(utxo.tx_hash.into(), utxo.output_index);
            let constraint_hashes: sundaev4::ConstraintHashes = protocol
                .execution
                .as_ref()
                .map(|e| sundaev4::ConstraintHashes::from_scripts(&e.module_scripts))
                .unwrap_or_default();
            match PlutusData::from_plutus_bytes(cbor)
                .map_err(|e| format!("{e}"))
                .and_then(|data| {
                    sundaev4::OrderDatum::from_plutus(data).map_err(|e| format!("{e}"))
                })
                .and_then(|datum| {
                    sundaev4::decode_order_constraint(&datum, &constraint_hashes)
                        .map(|constraint| (datum, constraint))
                }) {
                Ok((datum, constraint)) => {
                    orders.push(Arc::new(sundaev4::SundaeV4Order {
                        input,
                        address: pallas_addresses::Address::from_bech32(&utxo.address)
                            .map(|a| a.to_vec())
                            .unwrap_or_default(),
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
    let fee_token: Option<Vec<u8>> =
        protocol.fee_settings_token.as_ref().and_then(|t| hex::decode(t).ok());
    let mut order_configs: std::collections::BTreeMap<Vec<u8>, Arc<sundaev4::SundaeV4OrderConfig>> =
        std::collections::BTreeMap::new();
    for utxo in &settings_utxos {
        let Some(ref cbor) = utxo.datum_cbor else {
            continue;
        };
        let Ok(data) = PlutusData::from_plutus_bytes(cbor) else {
            continue;
        };
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
        let token_name = utxo.value.0.get(&protocol.settings_nft.policy).and_then(|tokens| {
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
    info!(
        count = order_configs.len(),
        "bootstrap v4: hydrated OrderConfig settings entries"
    );

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
            exec.network,
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
                exec.network,
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
                        let input = TransactionInput::new(utxo.tx_hash.into(), utxo.output_index);
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
        let all_refs: Vec<&crate::sundaev4::ScriptRefInfo> = scripts.all_scripts().collect();
        for script_ref in all_refs {
            let hash_hex = hex::encode(script_ref.hash.as_ref());
            match provider.fetch_script_cbor(&hash_hex).await {
                Ok(script_cbor) => {
                    // Blockfrost returns the *outer* CBOR (the full script encoding).
                    // We need to build a PlutusV3Script from the inner bytes.
                    // The API returns: CBOR-wrapped script bytes, same as what
                    // pallas stores in PlutusV3Script.
                    let script = pallas_primitives::PlutusScript::<3>(script_cbor.into());
                    // The output as the chain holds it. This used to be a
                    // placeholder (zero payment key, no stake part, 2 ADA)
                    // on the theory that only script_ref matters — but the
                    // output is a reference input in every scoop's
                    // ScriptContext, and a script that walks reference
                    // inputs reads its address and value. On preview
                    // 2026-10-08 the node's context and ours differed in
                    // exactly those fields for every module reference
                    // input, and nothing else.
                    let real = match provider
                        .fetch_tx_output(
                            script_ref
                                .ref_utxo
                                .0
                                .transaction_id
                                .as_ref()
                                .try_into()
                                .expect("32-byte tx id"),
                            script_ref.ref_utxo.0.index,
                        )
                        .await
                    {
                        Ok(Some(u)) => Some(u),
                        Ok(None) => None,
                        Err(e) => {
                            warn!(error = %e, ref_utxo = %script_ref.ref_utxo, "bootstrap: could not fetch module ref UTxO output; using a placeholder");
                            None
                        }
                    };
                    // An address that does not parse falls back to the
                    // placeholder, as a failed fetch does, instead of
                    // failing the whole bootstrap for one reference UTxO.
                    let real = real.and_then(|u| {
                        match pallas_addresses::Address::from_bech32(&u.address) {
                            Ok(a) => Some((a, u.value)),
                            Err(e) => {
                                warn!(ref_utxo = %script_ref.ref_utxo, address = %u.address, "bootstrap: ref UTxO address does not parse; using a placeholder: {e}");
                                None
                            }
                        }
                    });
                    let (address, value) = match real {
                        Some(av) => av,
                        None => (
                            pallas_addresses::Address::Shelley(
                                pallas_addresses::ShelleyAddress::new(
                                    protocol.network.pallas(),
                                    pallas_addresses::ShelleyPaymentPart::Key(
                                        pallas_primitives::Hash::new([0u8; 28]),
                                    ),
                                    pallas_addresses::ShelleyDelegationPart::Null,
                                ),
                            ),
                            crate::cardano_types::Value::default(),
                        ),
                    };
                    let txo = crate::cardano_types::TransactionOutput {
                        address,
                        value,
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
        info!(
            count = ref_utxo_outputs.len(),
            "bootstrap: fetched reference scripts"
        );
    }

    // Cardano-Swaps: the swap script's reference UTxO rides the ref set, and
    // the orders resting at the swap script are seeded like our own orders.
    // Both are persisted so a restart reloads them; the indexer then keeps
    // them current from the bootstrap tip on.
    let mut cswaps_orders = std::collections::BTreeMap::new();
    let mut cswaps_persisted: Vec<PersistedTxo> = Vec::new();
    if let Some(rt) = protocol
        .execution
        .as_ref()
        .and_then(|e| crate::sundaev4::cardano_swaps::load_runtime(&e.cardano_swaps))
    {
        let hash_hex = hex::encode(&rt.swap_hash);
        match provider.fetch_script_cbor(&hash_hex).await {
            Ok(script_cbor) => {
                let script = pallas_primitives::PlutusScript::<3>(script_cbor.into());
                let dummy_addr =
                    pallas_addresses::Address::Shelley(pallas_addresses::ShelleyAddress::new(
                        protocol.network.pallas(),
                        pallas_addresses::ShelleyPaymentPart::Key(pallas_primitives::Hash::new(
                            [0u8; 28],
                        )),
                        pallas_addresses::ShelleyDelegationPart::Null,
                    ));
                ref_utxo_outputs.insert(
                    rt.swap_ref.clone(),
                    crate::cardano_types::TransactionOutput {
                        address: dummy_addr,
                        value: crate::cardano_types::Value::default(),
                        datum: crate::cardano_types::RawDatum::None,
                        script_ref: Some(crate::cardano_types::ScriptRef::PlutusV3(script)),
                    },
                );
            }
            Err(e) => {
                warn!(hash = %hash_hex, "bootstrap v4: could not fetch cardano-swaps script CBOR: {e:#}")
            }
        }
        let swap_hash: [u8; 28] = rt.swap_hash.as_slice().try_into().expect("28-byte script hash");
        match provider.fetch_script_utxos(&ScriptHash::from(swap_hash)).await {
            Ok(utxos) => {
                for utxo in &utxos {
                    let input = TransactionInput::new(utxo.tx_hash.into(), utxo.output_index);
                    let addr_bytes = pallas_addresses::Address::from_bech32(&utxo.address)
                        .map(|a| a.to_vec())
                        .unwrap_or_default();
                    let bytes =
                        encode_bootstrap_utxo(&addr_bytes, &utxo.value, utxo.datum_cbor.as_deref());
                    let Ok(parsed) = pallas_traverse::MultiEraOutput::decode(
                        pallas_traverse::Era::Conway,
                        &bytes,
                    ) else {
                        continue;
                    };
                    let tx_out = crate::cardano_types::convert_txo(&parsed);
                    let Some(order) = crate::sundaev4::cardano_swaps::SwapOrder::from_output(
                        input.clone(),
                        &tx_out,
                    ) else {
                        debug!(input = %input, "bootstrap v4: cardano-swaps UTxO with an unparseable datum");
                        continue;
                    };
                    cswaps_persisted.push(PersistedTxo {
                        txo_id: input.clone(),
                        txo_type: "cswaps_order".to_string(),
                        created_slot: tip_slot,
                        era: 7,
                        txo: bytes,
                        address: addr_bytes,
                        datum: None,
                    });
                    cswaps_orders.insert(input, order);
                }
                info!(
                    utxos = utxos.len(),
                    orders = cswaps_orders.len(),
                    "bootstrap: fetched cardano-swaps orders"
                );
            }
            Err(e) => warn!("bootstrap v4: could not fetch cardano-swaps order UTxOs: {e:#}"),
        }
    }

    let n_pools = pools.len();
    let n_orders = orders.len();
    let n_wallet = wallet_utxos.len();
    let n_refs = ref_utxo_outputs.len();

    // Persist bootstrap state to DB so subsequent restarts skip bootstrap.
    {
        let mut persisted_txos: Vec<PersistedTxo> = Vec::new();

        for pool in pools.values() {
            let datum_bytes = pool.pool_datum.clone().to_plutus_bytes();
            persisted_txos.push(PersistedTxo {
                txo_id: pool.input.clone(),
                txo_type: "pool".to_string(),
                created_slot: tip_slot,
                era: 7,
                txo: encode_bootstrap_utxo(&pool.address, &pool.value, Some(&datum_bytes)),
                address: pool.address.clone(),
                datum: None,
            });
        }

        let order_addr = protocol
            .order_script_hashes
            .first()
            .map(|h| {
                ShelleyAddress::new(
                    protocol.network.pallas(),
                    ShelleyPaymentPart::Script(*h),
                    ShelleyDelegationPart::Null,
                )
                .to_vec()
            })
            .unwrap_or_default();
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
                protocol.network.pallas(),
                ShelleyPaymentPart::Script(protocol.settings_script_hash),
                ShelleyDelegationPart::Null,
            )
            .to_vec();
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
                protocol.network.pallas(),
                ShelleyPaymentPart::Script(protocol.settings_script_hash),
                ShelleyDelegationPart::Null,
            )
            .to_vec();
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
                protocol.network.pallas(),
                ShelleyPaymentPart::Script(protocol.settings_script_hash),
                ShelleyDelegationPart::Null,
            )
            .to_vec();
            let datum_bytes = sundaev4::FeeSettingsDatum {
                base_fee: crate::bigint::BigInt::from(fs.base_fee),
            }
            .to_plutus_bytes();
            let mut value = crate::cardano_types::Value::default();
            if let Some(want) = &fee_token {
                value
                    .0
                    .entry(protocol.settings_nft.policy.clone())
                    .or_default()
                    .insert(want.clone(), crate::bigint::BigInt::from(1u64));
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

        for (input, output) in &ref_utxo_outputs {
            let txo_bytes = match &output.script_ref {
                Some(crate::cardano_types::ScriptRef::PlutusV3(script)) => {
                    let txo = conway::TransactionOutput::PostAlonzo(
                        pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
                            address: PallasBytes::from(output.address.to_vec()),
                            value: value_to_conway(&output.value),
                            datum_option: None,
                            script_ref: Some(CborWrap(conway::PseudoScript::PlutusV3Script(
                                script.clone(),
                            ))),
                        },
                    );
                    minicbor::to_vec(&txo).expect("infallible encoding")
                }
                _ => encode_bootstrap_utxo(&output.address.to_vec(), &output.value, None),
            };
            persisted_txos.push(PersistedTxo {
                txo_id: input.clone(),
                txo_type: "ref".to_string(),
                created_slot: tip_slot,
                era: 7,
                txo: txo_bytes,
                address: output.address.to_vec(),
                datum: None,
            });
        }

        persisted_txos.extend(cswaps_persisted);

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
            })
            .await?;
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
        s.cardano_swaps.orders = cswaps_orders;
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
mod credential_enumeration_tests {
    //! Bootstrap seeds state and then starts the indexer at the bootstrap
    //! tip, discarding every block before it. That is only safe when the
    //! provider could actually see everything worth keeping.
    //!
    //! On 2026-09-26 a mainnet bootstrap reported "0 V4 order UTxOs" and a
    //! clean start while a live user order sat unspent on chain. Blockfrost
    //! had been asked for `addr1w...` (the enterprise form of the order
    //! script) while the order sat at `addr1z...`, because users attach their
    //! own stake credential. The order became invisible: bootstrap had not
    //! seen it, and the history containing it was skipped.
    //!
    //! The fix is to ask Blockfrost for the bare payment credential
    //! (`script1...`), which its `/addresses` endpoints accept and which
    //! matches every staking variant.
    use super::*;

    /// Preview V4 order script; the bech32 was checked against a live
    /// `/addresses/{credential}/utxos` call that returned an order at
    /// `addr_test1z...` where the enterprise address returned nothing.
    #[test]
    fn script_credential_is_cip5_bech32() {
        let hash: ScriptHash =
            "8a7ecdafb3605ddf761751b391c20482b7ef42ca01575d135d484c2b".parse().unwrap();
        assert_eq!(
            BlockfrostProvider::script_credential(&hash),
            "script13flvmtanvpwa7ash2xeerssys2m77sk2q9t46y6afpxzknmwa3q"
        );
    }

    #[test]
    fn blockfrost_enumerates_by_credential() {
        let p = BlockfrostProvider::new("https://example.invalid/api/v0", "tok");
        assert!(p.enumerates_by_credential());
    }

    /// The row's own address must win over the queried string: the queried
    /// string is a credential, and `bootstrap_v4` parses the address as bech32.
    #[test]
    fn utxo_row_carries_its_full_address() {
        let row: BlockfrostUtxo = serde_json::from_str(
            r#"{"address":"addr_test1zz98and0kds9mhmkzagm8ywzqjpt0m6zegq4whgnt4yyc2cpjqw6nx3y36p6r8z825sahmmdkdn7mq9md9qnqu3l8fnqlmqq02",
                "tx_hash":"2595ce1b13ab0000000000000000000000000000000000000000000000000000",
                "tx_index":0,"output_index":0,"amount":[],"block":null,
                "data_hash":null,"inline_datum":null,"reference_script_hash":null}"#,
        )
        .unwrap();
        assert!(row.address.unwrap().starts_with("addr_test1z"));
    }

    #[test]
    fn kupo_enumerates_by_credential() {
        // Kupo matches `{credential}/*`, so any staking part is covered.
        let p = KupoProvider::new("https://example.invalid");
        assert!(p.enumerates_by_credential());
    }

    /// The guard judges the *config the operator actually writes*, not just
    /// a provider object.
    #[test]
    fn both_bootstrap_configs_pass_the_guard() {
        let cfg = BootstrapConfig::Blockfrost {
            url: "https://example.invalid/api/v0".to_string(),
            project_id: "tok".to_string(),
        };
        assert!(make_provider(&cfg).enumerates_by_credential());

        let cfg = BootstrapConfig::Kupo {
            url: "https://example.invalid".to_string(),
        };
        assert!(make_provider(&cfg).enumerates_by_credential());
    }
}

#[cfg(test)]
mod blockfrost_pacing_tests {
    //! A fresh free-tier Blockfrost key answers >10 req/s with 429. Before
    //! these guards, a mainnet bootstrap with v3 enabled hit that within
    //! seconds, logged "could not fetch inline datum CBOR", dropped the pool,
    //! and persisted a snapshot that could never regain it.
    use super::*;
    use std::time::Duration;

    #[test]
    fn retry_delay_doubles_from_one_second_and_caps() {
        assert_eq!(blockfrost_retry_delay(0, None), Duration::from_secs(1));
        assert_eq!(blockfrost_retry_delay(1, None), Duration::from_secs(2));
        assert_eq!(blockfrost_retry_delay(3, None), Duration::from_secs(8));
        assert_eq!(blockfrost_retry_delay(9, None), BLOCKFROST_MAX_RETRY_DELAY);
        assert_eq!(blockfrost_retry_delay(40, None), BLOCKFROST_MAX_RETRY_DELAY);
    }

    #[test]
    fn retry_delay_honours_a_longer_retry_after_but_not_a_shorter_one() {
        assert_eq!(blockfrost_retry_delay(0, Some(5)), Duration::from_secs(5));
        assert_eq!(blockfrost_retry_delay(3, Some(2)), Duration::from_secs(8));
        assert_eq!(
            blockfrost_retry_delay(0, Some(600)),
            BLOCKFROST_MAX_RETRY_DELAY
        );
    }

    #[test]
    fn inline_datum_is_cbor_hex_not_json() {
        // The mainnet SPRINKLES/JIMMIES pool datum starts like this on the
        // /addresses/{addr}/utxos row; reading it here saves one call per UTxO.
        let hex_str = serde_json::Value::String("d8799f581c9f91de10ff".to_string());
        assert_eq!(
            blockfrost_inline_datum_cbor(&hex_str).as_deref(),
            Some(&hex::decode("d8799f581c9f91de10ff").unwrap()[..])
        );
        assert_eq!(blockfrost_inline_datum_cbor(&serde_json::Value::Null), None);
        assert_eq!(
            blockfrost_inline_datum_cbor(&serde_json::json!({"constructor": 0})),
            None
        );
        assert_eq!(
            blockfrost_inline_datum_cbor(&serde_json::Value::String("zz".into())),
            None
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bucket_spends_the_burst_then_settles_at_the_refill_rate() {
        let provider = BlockfrostProvider::new("https://cardano-mainnet.blockfrost.io/api/v0", "x");
        let start = tokio::time::Instant::now();
        for _ in 0..500 {
            provider.throttle().await;
        }
        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "the burst costs no waiting"
        );
        for _ in 0..20 {
            provider.throttle().await;
        }
        // 20 more tokens at 10/s: 2s, give or take float rounding.
        let waited = start.elapsed();
        assert!(
            waited >= Duration::from_millis(1990) && waited <= Duration::from_millis(2010),
            "{waited:?}"
        );
    }

    #[test]
    fn a_429_drains_the_bucket_so_the_next_call_waits_a_full_token() {
        let t0 = tokio::time::Instant::now();
        let mut bucket = RateBucket::full(t0);
        assert!(bucket.take(t0).is_ok());
        bucket.drain();
        let wait = bucket.take(t0).unwrap_err();
        assert!(
            wait >= Duration::from_millis(99) && wait <= Duration::from_millis(101),
            "{wait:?}"
        );
        // Half a second later, five tokens have refilled.
        let t1 = t0 + Duration::from_millis(500);
        for _ in 0..5 {
            assert!(bucket.take(t1).is_ok());
        }
        assert!(bucket.take(t1).is_err());
    }

    #[test]
    fn bucket_never_holds_more_than_the_burst() {
        let t0 = tokio::time::Instant::now();
        let mut bucket = RateBucket::full(t0);
        let later = t0 + Duration::from_secs(3600);
        for _ in 0..(BLOCKFROST_BURST as usize) {
            assert!(bucket.take(later).is_ok());
        }
        assert!(bucket.take(later).is_err());
    }
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

    /// The credential query must surface orders at user-staked addresses.
    /// At the time of writing preview holds one V4 order at `addr_test1z...`
    /// and none at the enterprise address.
    #[tokio::test]
    #[ignore]
    async fn live_order_query_by_credential_sees_staked_addresses() -> Result<()> {
        let provider = BlockfrostProvider::new(PREVIEW_BLOCKFROST, PREVIEW_PROJECT_ID);
        let order_hash: ScriptHash =
            "8a7ecdafb3605ddf761751b391c20482b7ef42ca01575d135d484c2b".parse()?;
        let utxos = provider.fetch_script_utxos(&order_hash).await?;
        assert!(
            !utxos.is_empty(),
            "preview should hold at least one V4 order"
        );
        for u in &utxos {
            let addr = pallas_addresses::Address::from_bech32(&u.address)
                .with_context(|| format!("row address {}", u.address))?;
            let pallas_addresses::Address::Shelley(sh) = addr else {
                anyhow::bail!("order at non-Shelley address {}", u.address);
            };
            assert_eq!(sh.payment().as_hash(), &order_hash);
            eprintln!(
                "{}#{} at {}",
                hex::encode(u.tx_hash),
                u.output_index,
                u.address
            );
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn live_blockfrost_endpoints_match_struct_shapes() -> Result<()> {
        let provider = BlockfrostProvider::new(PREVIEW_BLOCKFROST, PREVIEW_PROJECT_ID);
        // Pick any pool NFT under the preview policy and verify we can walk
        // the asset-history -> tx-cbor pipeline without serde failures.
        let policy_hash: ScriptHash = PREVIEW_POOL_NFT_POLICY_HEX.parse()?;
        let pool_utxos = provider.fetch_pool_utxos_by_nft(&policy_hash, &policy_hash).await?;
        let utxo = pool_utxos
            .iter()
            .find(|u| u.datum_cbor.is_some())
            .expect("preview must have at least one pool with a datum");
        let pool_datum = sundaev4::PoolDatum::from_plutus(PlutusData::from_plutus_bytes(
            utxo.datum_cbor.as_ref().unwrap(),
        )?)?;
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
        let pool_utxos = provider.fetch_pool_utxos_by_nft(&policy_hash, &policy_hash).await?;
        let mut found_cs = false;
        for utxo in &pool_utxos {
            let Some(ref cbor) = utxo.datum_cbor else {
                continue;
            };
            let Ok(pd) = PlutusData::from_plutus_bytes(cbor) else {
                continue;
            };
            let Ok(pool_datum) = sundaev4::PoolDatum::from_plutus(pd) else {
                continue;
            };
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

#[cfg(test)]
mod recover_missing_configs_tests {
    //! The decision that gates chain recovery of a pool's module configs.
    //! Until this pass existed, that recovery ran only inside a first-ever
    //! bootstrap (`tip_slot == 0`), so any DB carrying a tip could never learn
    //! a config it lacked. These pin the decision itself; the network walk is
    //! covered by the `#[ignore]`d live smoke tests.

    use super::missing_module_configs;
    use crate::bigint::BigInt;
    use crate::sundaev4::test_harness::{TestEnv, make_cs_pool, token_a, token_b};
    use crate::sundaev4::{ConstantSumConfig, PoolConfig, PoolType, Rational, SundaeV4Pool};
    use std::collections::BTreeMap;

    const BLUEPRINT_PATH: &str = "test/fixtures/preview-launch-candidate-blueprint.json";

    fn cs_pool(env: &TestEnv) -> std::sync::Arc<SundaeV4Pool> {
        make_cs_pool(
            env,
            0xCC,
            vec![(token_a(), 1_000_000_000), (token_b(), 1_000_000_000)],
            vec![BigInt::from(1_000_000), BigInt::from(1_000_000)],
            Rational {
                num: BigInt::from(3),
                den: BigInt::from(1000),
            },
        )
    }

    /// A constant-sum pool nothing knows about needs its CS and fee-split
    /// configs recovered — and must not ask for CP, whose lookup would
    /// come back empty and log noise for a pool that never had them.
    #[test]
    fn unknown_cs_pool_asks_for_cs_and_fee_split_only() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = cs_pool(&env);
        let (cs, cp, fs, ss, _bcl) = missing_module_configs(
            &pool.pool_datum,
            Some(&env.exec),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert_eq!((cs, cp, fs, ss), (true, false, true, false));
    }

    /// A stableswap pool asks for its config until the cached config hashes
    /// to the datum's module_state; a cached config a rate update has left
    /// behind asks again (the rates changed on chain, the hash with them).
    #[test]
    fn stableswap_lookup_is_gated_by_the_datum_hash() {
        use crate::sundaev4::test_harness::{make_ss_pool, ss_config, token_a, token_b};
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let cfg = ss_config(
            200,
            Rational {
                num: BigInt::from(3),
                den: BigInt::from(1000),
            },
            [1, 1],
        );
        let pool = make_ss_pool(
            &env,
            0x5A,
            vec![(token_a(), 1_000_000_000), (token_b(), 1_000_000_000)],
            cfg.clone(),
        );
        let (_, _, _, ss, _) = missing_module_configs(
            &pool.pool_datum,
            Some(&env.exec),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert!(ss, "an unknown stableswap config must be looked up");

        let mut ss_configs = BTreeMap::new();
        ss_configs.insert(pool.pool_datum.identifier.clone(), cfg.clone());
        let (_, _, _, ss, _) = missing_module_configs(
            &pool.pool_datum,
            Some(&env.exec),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &ss_configs,
            &BTreeMap::new(),
        );
        assert!(
            !ss,
            "a cached config that hashes to the datum suppresses the lookup"
        );

        let mut stale = cfg.clone();
        stale.rates = vec![BigInt::from(1), BigInt::from(2)];
        ss_configs.insert(pool.pool_datum.identifier.clone(), stale);
        let (_, _, _, ss, _) = missing_module_configs(
            &pool.pool_datum,
            Some(&env.exec),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &ss_configs,
            &BTreeMap::new(),
        );
        assert!(
            ss,
            "a cached config with other rates must be looked up again"
        );
    }

    /// Once the cache has the pool's CS config, recovery stops asking for it.
    /// This is the state every start after a successful recovery should be in,
    /// so it must be cheap: no network call at all.
    #[test]
    fn known_cs_config_is_not_requested_again() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = cs_pool(&env);
        let mut cs_configs = BTreeMap::new();
        if let PoolType::ConstantSum {
            prices,
            fee,
            bounty_k,
            balance_fee,
        } = pool.pool_type.clone()
        {
            cs_configs.insert(
                pool.pool_datum.identifier.clone(),
                ConstantSumConfig {
                    prices,
                    fee,
                    bounty_k,
                    balance_fee,
                },
            );
        } else {
            panic!("make_cs_pool must yield a constant-sum pool type");
        }
        let (cs, _, _, _, _) = missing_module_configs(
            &pool.pool_datum,
            Some(&env.exec),
            &cs_configs,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert!(!cs, "a cached CS config must suppress the chain lookup");
    }

    /// An operator `pool-configs` override also suppresses the lookup, as it
    /// always has. (Whether that override can *express* a given pool is a
    /// separate problem: it forces bounty_k to 0/1.)
    #[test]
    fn operator_override_suppresses_cs_lookup() {
        let env = TestEnv::from_blueprint_file(BLUEPRINT_PATH);
        let pool = cs_pool(&env);
        let mut exec = env.exec.clone();
        exec.pool_configs.insert(
            hex::encode(pool.pool_datum.identifier.to_bytes()),
            PoolConfig::ConstantSum {
                prices: vec![1_000_000, 1_000_000],
                fee: (3, 1000),
            },
        );
        let (cs, _, _, _, _) = missing_module_configs(
            &pool.pool_datum,
            Some(&exec),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert!(!cs, "an operator override must suppress the chain lookup");
    }
}

#[cfg(test)]
mod migrated_pool_bootstrap_tests {
    //! Bootstrap from the chain tip after a governor moved pools to new
    //! module hashes in place (preview, 2026-11-14). The migration tx
    //! carries no module redeemer, so each pool's config is only in
    //! redeemers under the OLD hash. These tests run the bootstrap code
    //! against a mock provider that serves hand-built transactions.
    use super::*;
    use crate::bigint::BigInt;
    use crate::sundaev4::test_harness::{
        Revision, TestEnv, bcl_eq8_config_at, make_bcl_pool, make_cs_pool, token_a, token_b,
    };
    use crate::sundaev4::{
        BandedCLRedeemer, CSOperateEntry, ConstantSumConfig, ConstantSumRedeemer, FeeSplitConfig,
        FeeSplitRedeemer, OutputRef, PoolType, Rational, ScriptRefInfo, SundaeV4Pool,
    };
    use pallas_primitives::conway::{RedeemerTag, Redeemers, RedeemersKey, RedeemersValue};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A fee_split hash with no code: the "old" fee_split of a migrated
    /// pool. Recovery only reads its redeemers; nothing evaluates it.
    const OLD_FS: [u8; 28] = [0xF5; 28];

    fn ratio(n: i64, d: i64) -> Rational {
        Rational {
            num: BigInt::from(n),
            den: BigInt::from(d),
        }
    }

    /// The preview launch-candidate fixture (current CS and CL revision 2),
    /// the revision-1 modules (old CS 95e2c69a, old CL 33485bba), and a
    /// second, old fee_split hash.
    fn env() -> TestEnv {
        let mut env = TestEnv::preview_both_revisions();
        env.exec
            .module_scripts
            .modules
            .insert(
                ModuleKind::FeeSplit,
                ScriptRefInfo {
                    hash: ScriptHash::new(OLD_FS),
                    ref_utxo: crate::cardano_types::TransactionInput::new([0xF5; 32].into(), 0),
                    script_cbor: None,
                },
            )
            .unwrap();
        env
    }

    fn hash_of(env: &TestEnv, kind: ModuleKind, revision: Revision) -> ScriptHash {
        env.exec.module_scripts.first_of_revision(kind, revision).expect("registered").hash
    }

    fn old_cs(env: &TestEnv) -> ScriptHash {
        let current = env.exec.module_scripts.first_of(ModuleKind::ConstantSum).unwrap().hash;
        env.exec
            .module_scripts
            .of_kind(ModuleKind::ConstantSum)
            .map(|s| s.hash)
            .find(|h| *h != current)
            .expect("the revision-1 modules add a second constant_sum")
    }

    fn current_fs(env: &TestEnv) -> ScriptHash {
        env.exec.module_scripts.first_of(ModuleKind::FeeSplit).unwrap().hash
    }

    /// The CS pool `make_cs_pool` builds, and the config its slot pins.
    fn cs_pool(env: &TestEnv, ident: u8) -> (Arc<SundaeV4Pool>, ConstantSumConfig) {
        let prices = vec![BigInt::from(1_000_000), BigInt::from(1_000_000)];
        let fee = ratio(3, 1000);
        let pool = make_cs_pool(
            env,
            ident,
            vec![(token_a(), 1_000_000_000), (token_b(), 1_000_000_000)],
            prices.clone(),
            fee.clone(),
        );
        let cfg = ConstantSumConfig {
            prices,
            fee,
            bounty_k: ratio(0, 1),
            balance_fee: ratio(0, 1),
        };
        (pool, cfg)
    }

    fn default_fs(env: &TestEnv) -> FeeSplitConfig {
        FeeSplitConfig {
            protocol_share: ratio(
                env.exec.protocol_share.0 as i64,
                env.exec.protocol_share.1 as i64,
            ),
        }
    }

    fn cs_create(cfg: &ConstantSumConfig) -> PlutusData {
        ConstantSumRedeemer::Create {
            initial_state: cfg.clone().to_plutus(),
            pool_output_index: 0,
        }
        .to_plutus()
    }

    fn fs_create(cfg: &FeeSplitConfig) -> PlutusData {
        FeeSplitRedeemer::Create {
            config: cfg.clone(),
            pool_output_index: 0,
            settings_ref_index: 0,
            stake_list_ref_index: 0,
        }
        .to_plutus()
    }

    /// A Conway tx whose only content that matters is its withdrawals and
    /// their `Reward` redeemers. `seed` makes the tx hash distinct.
    fn withdrawal_tx(seed: u8, redeemers: Vec<(ScriptHash, PlutusData)>) -> (String, Vec<u8>) {
        let mut accounts: Vec<(Vec<u8>, PlutusData)> = redeemers
            .into_iter()
            .map(|(h, d)| {
                let mut a = vec![0xf0u8];
                a.extend_from_slice(h.as_ref());
                (a, d)
            })
            .collect();
        accounts.sort_by(|a, b| a.0.cmp(&b.0));
        let withdrawals = (!accounts.is_empty()).then(|| {
            NonEmptyKeyValuePairs::Def(
                accounts.iter().map(|(a, _)| (PallasBytes::from(a.clone()), 0u64)).collect(),
            )
        });
        let redeemer = (!accounts.is_empty()).then(|| {
            Redeemers::Map(NonEmptyKeyValuePairs::Def(
                accounts
                    .iter()
                    .enumerate()
                    .map(|(i, (_, d))| {
                        (
                            RedeemersKey {
                                tag: RedeemerTag::Reward,
                                index: i as u32,
                            },
                            RedeemersValue {
                                data: d.clone(),
                                ex_units: pallas_primitives::ExUnits { mem: 0, steps: 0 },
                            },
                        )
                    })
                    .collect(),
            ))
        });
        let body: conway::TransactionBody = conway::PseudoTransactionBody {
            inputs: vec![pallas_primitives::TransactionInput {
                transaction_id: Hash::new([seed; 32]),
                index: 0,
            }]
            .into(),
            outputs: vec![],
            fee: 0,
            ttl: None,
            certificates: None,
            withdrawals,
            auxiliary_data_hash: None,
            validity_interval_start: None,
            mint: None,
            script_data_hash: None,
            collateral: None,
            required_signers: None,
            network_id: None,
            collateral_return: None,
            total_collateral: None,
            reference_inputs: None,
            voting_procedures: None,
            proposal_procedures: None,
            treasury_value: None,
            donation: None,
        };
        let tx = conway::PseudoTx {
            transaction_body: body,
            transaction_witness_set: conway::WitnessSet {
                vkeywitness: None,
                native_script: None,
                bootstrap_witness: None,
                plutus_v1_script: None,
                plutus_data: None,
                redeemer,
                plutus_v2_script: None,
                plutus_v3_script: None,
            },
            success: true,
            auxiliary_data: pallas_primitives::Nullable::<conway::AuxiliaryData>::Null,
        };
        let cbor = minicbor::to_vec(&tx).unwrap();
        let hash = hex::encode(pallas_traverse::MultiEraTx::decode(&cbor).unwrap().hash());
        (hash, cbor)
    }

    /// Serves pools, each pool's mint tx and tx history (newest first), and
    /// tx CBOR by hash. Counts the history requests.
    #[derive(Default)]
    struct MockProvider {
        pools: Vec<FetchedUtxo>,
        mint: BTreeMap<String, String>,
        history: BTreeMap<String, Vec<String>>,
        txs: BTreeMap<String, Vec<u8>>,
        /// Asset units whose mint-tx request fails, as a Blockfrost error.
        broken: Vec<String>,
        tx_fetches: AtomicUsize,
        page_fetches: AtomicUsize,
    }

    impl MockProvider {
        fn add_tx(&mut self, tx: (String, Vec<u8>)) -> String {
            self.txs.insert(tx.0.clone(), tx.1);
            tx.0
        }

        /// Add `pool` with `mint` as its mint tx and `newer` (newest first)
        /// as the rest of its history.
        fn add_pool(
            &mut self,
            policy: &ScriptHash,
            pool: &SundaeV4Pool,
            mint: (String, Vec<u8>),
            newer: Vec<(String, Vec<u8>)>,
        ) {
            let unit = asset_unit(policy, pool);
            let mint = self.add_tx(mint);
            let mut hist: Vec<String> = newer.into_iter().map(|t| self.add_tx(t)).collect();
            hist.push(mint.clone());
            self.mint.insert(unit.clone(), mint);
            self.history.insert(unit, hist);
            let mut tx_hash = [0u8; 32];
            tx_hash[..28].copy_from_slice(pool.pool_datum.identifier.to_bytes());
            self.pools.push(FetchedUtxo {
                tx_hash,
                output_index: 0,
                address: pallas_addresses::Address::from_bytes(&pool.address)
                    .unwrap()
                    .to_bech32()
                    .unwrap(),
                value: pool.value.clone(),
                datum_cbor: Some(pool.pool_datum.clone().to_plutus_bytes()),
                slot: 100,
            });
        }
    }

    fn asset_unit(policy: &ScriptHash, pool: &SundaeV4Pool) -> String {
        let mut name = CIP_67_ASSET_LABEL_222.to_vec();
        name.extend_from_slice(pool.pool_datum.identifier.to_bytes());
        format!("{}{}", hex::encode(policy.as_ref()), hex::encode(name))
    }

    #[async_trait]
    impl BootstrapProvider for MockProvider {
        async fn fetch_script_utxos(&self, _h: &ScriptHash) -> Result<Vec<FetchedUtxo>> {
            Ok(vec![])
        }
        async fn fetch_tx_output(&self, _t: &[u8; 32], _i: u64) -> Result<Option<FetchedUtxo>> {
            Ok(None)
        }
        async fn fetch_address_utxos(&self, _a: &str) -> Result<Vec<FetchedUtxo>> {
            Ok(vec![])
        }
        async fn fetch_tip(&self) -> Result<(u64, String)> {
            Ok((1000, "00".repeat(32)))
        }
        async fn fetch_pool_utxos_by_nft(
            &self,
            _p: &ScriptHash,
            _s: &ScriptHash,
        ) -> Result<Vec<FetchedUtxo>> {
            Ok(self
                .pools
                .iter()
                .map(|u| FetchedUtxo {
                    tx_hash: u.tx_hash,
                    output_index: u.output_index,
                    address: u.address.clone(),
                    value: u.value.clone(),
                    datum_cbor: u.datum_cbor.clone(),
                    slot: u.slot,
                })
                .collect())
        }
        async fn fetch_script_cbor(&self, _h: &str) -> Result<Vec<u8>> {
            anyhow::bail!("the mock serves no scripts")
        }
        async fn fetch_first_mint_tx_cbor(&self, unit: &str) -> Result<Vec<u8>> {
            if self.broken.iter().any(|b| b == unit) {
                anyhow::bail!("blockfrost: 500 Internal Server Error (mock)");
            }
            let h = self.mint.get(unit).context("no mint tx")?;
            Ok(self.txs[h].clone())
        }
        async fn fetch_asset_tx_hashes_desc(
            &self,
            unit: &str,
            page: u32,
            page_size: u32,
        ) -> Result<Vec<String>> {
            self.page_fetches.fetch_add(1, Ordering::SeqCst);
            let all = self.history.get(unit).cloned().unwrap_or_default();
            let start = ((page - 1) * page_size) as usize;
            Ok(all.into_iter().skip(start).take(page_size as usize).collect())
        }
        async fn fetch_tx_cbor(&self, h: &str) -> Result<Vec<u8>> {
            self.tx_fetches.fetch_add(1, Ordering::SeqCst);
            self.txs.get(h).cloned().context("unknown tx")
        }
    }

    fn protocol(env: &TestEnv) -> SundaeV4Protocol {
        SundaeV4Protocol {
            pool_script_hash: env.exec.module_scripts.pool.hash,
            order_script_hashes: vec![],
            settings_script_hash: env.exec.module_scripts.settings.hash,
            settings_nft: crate::cardano_types::AssetClass {
                policy: vec![0; 28],
                token: vec![],
            },
            pool_nft_policy: env.exec.module_scripts.pool_mint.hash,
            fee_settings_token: None,
            starting_point: acropolis_common::Point::Origin,
            execution: Some(env.exec.clone()),
            mempool: None,
            blueprint: None,
            network: Default::default(),
        }
    }

    /// (a) A CS pool whose datum names the new CS hash recovers its config
    /// from the Create redeemer under the old CS hash.
    #[tokio::test]
    async fn cs_config_recovers_through_the_old_hash() {
        let env = env();
        let protocol = protocol(&env);
        let (pool, cfg) = cs_pool(&env, 0xA1);
        let mut mock = MockProvider::default();
        let mint = withdrawal_tx(
            1,
            vec![
                (old_cs(&env), cs_create(&cfg)),
                (current_fs(&env), fs_create(&default_fs(&env))),
            ],
        );
        mock.add_pool(&protocol.pool_nft_policy, &pool, mint, vec![]);

        let mut known = KnownConfigs::default();
        let rows = recover_pool_configs(&mock, &protocol, &pool.pool_datum, 100, &mut known).await;
        assert_eq!(known.cs.get(&pool.pool_datum.identifier), Some(&cfg));
        assert_eq!(
            known.fs.get(&pool.pool_datum.identifier),
            Some(&default_fs(&env))
        );
        let current_cs = env.exec.module_scripts.first_of(ModuleKind::ConstantSum).unwrap().hash;
        assert!(
            rows.iter().any(|r| r.module_hash == current_cs.as_ref()),
            "the row is stored under the hash the pool names now"
        );
        assert_eq!(
            mock.tx_fetches.load(Ordering::SeqCst),
            0,
            "the mint tx was enough"
        );
    }

    /// The old-hash fallback still requires the slot hash: a Create under
    /// the old hash with another config is not accepted.
    #[tokio::test]
    async fn an_old_hash_config_that_misses_the_slot_is_refused() {
        let env = env();
        let protocol = protocol(&env);
        let (pool, mut cfg) = cs_pool(&env, 0xA2);
        cfg.fee = ratio(5, 1000);
        let mut mock = MockProvider::default();
        let mint = withdrawal_tx(2, vec![(old_cs(&env), cs_create(&cfg))]);
        mock.add_pool(&protocol.pool_nft_policy, &pool, mint, vec![]);
        let mut known = KnownConfigs::default();
        recover_pool_configs(&mock, &protocol, &pool.pool_datum, 100, &mut known).await;
        assert!(known.cs.is_empty());
    }

    /// (b) The fee_split config recovers from the Create redeemer under the
    /// old fee_split hash.
    #[tokio::test]
    async fn fee_split_config_recovers_through_the_old_hash() {
        let env = env();
        let protocol = protocol(&env);
        let (pool, cfg) = cs_pool(&env, 0xB1);
        let mut mock = MockProvider::default();
        let current_cs = env.exec.module_scripts.first_of(ModuleKind::ConstantSum).unwrap().hash;
        let mint = withdrawal_tx(
            3,
            vec![
                (current_cs, cs_create(&cfg)),
                (ScriptHash::new(OLD_FS), fs_create(&default_fs(&env))),
            ],
        );
        mock.add_pool(&protocol.pool_nft_policy, &pool, mint, vec![]);
        let mut known = KnownConfigs::default();
        recover_pool_configs(&mock, &protocol, &pool.pool_datum, 100, &mut known).await;
        assert_eq!(
            known.fs.get(&pool.pool_datum.identifier),
            Some(&default_fs(&env))
        );
        assert_eq!(known.cs.get(&pool.pool_datum.identifier), Some(&cfg));
    }

    /// The walk stops at the first tx that holds a matching config. An
    /// Operate entry counts: the slot pins the value, not the pool.
    #[tokio::test]
    async fn the_walk_stops_at_the_first_matching_operate() {
        let env = env();
        let protocol = protocol(&env);
        let (pool, cfg) = cs_pool(&env, 0xB2);
        let mut mock = MockProvider::default();
        let operate = ConstantSumRedeemer::Operate {
            entries: vec![CSOperateEntry {
                pool_oref: OutputRef {
                    transaction_id: vec![9; 32],
                    output_index: 0,
                },
                config: cfg.clone(),
            }],
        }
        .to_plutus();
        let fs_operate = FeeSplitRedeemer::Operate {
            entries: vec![crate::sundaev4::FSOperateEntry {
                pool_oref: OutputRef {
                    transaction_id: vec![9; 32],
                    output_index: 0,
                },
                config: default_fs(&env),
            }],
        }
        .to_plutus();
        let newer = vec![
            withdrawal_tx(10, vec![]),
            withdrawal_tx(
                11,
                vec![
                    (old_cs(&env), operate),
                    (ScriptHash::new(OLD_FS), fs_operate),
                ],
            ),
            withdrawal_tx(12, vec![]),
            withdrawal_tx(13, vec![]),
        ];
        mock.add_pool(
            &protocol.pool_nft_policy,
            &pool,
            withdrawal_tx(14, vec![]),
            newer,
        );
        let mut known = KnownConfigs::default();
        recover_pool_configs(&mock, &protocol, &pool.pool_datum, 100, &mut known).await;
        assert_eq!(known.cs.get(&pool.pool_datum.identifier), Some(&cfg));
        assert_eq!(
            known.fs.get(&pool.pool_datum.identifier),
            Some(&default_fs(&env))
        );
        assert_eq!(
            mock.tx_fetches.load(Ordering::SeqCst),
            2,
            "two txs read, then stop"
        );
        assert_eq!(mock.page_fetches.load(Ordering::SeqCst), 1);
    }

    /// The acceptance test. Bootstrap with an empty DB against five pools:
    ///  1. CS on the new CS and fee_split hashes, Creates under the old ones;
    ///  2. CL revision 1 migrated to revision 2 (slot = the `ext = Void`
    ///     form), Create under the revision-1 hash with four fields;
    ///  3. a CS pool on the new hash whose config is in no redeemer;
    ///  4. a CS pool whose mint-tx request fails (a Blockfrost error);
    ///  5. a CS pool that no governor touched.
    /// Bootstrap completes. Pools 1, 2 and 5 are scoopable; 3 and 4 are in
    /// state but not scoopable.
    #[tokio::test]
    async fn bootstrap_completes_after_an_in_place_migration() -> Result<()> {
        let env = env();
        let protocol = protocol(&env);
        let policy = protocol.pool_nft_policy;
        let mut mock = MockProvider::default();

        let (p1, cfg1) = cs_pool(&env, 0xC1);
        mock.add_pool(
            &policy,
            &p1,
            withdrawal_tx(
                21,
                vec![
                    (old_cs(&env), cs_create(&cfg1)),
                    (ScriptHash::new(OLD_FS), fs_create(&default_fs(&env))),
                ],
            ),
            vec![withdrawal_tx(22, vec![])],
        );

        let four = bcl_eq8_config_at(ratio(3, 1000), Revision(1));
        let five = bcl_eq8_config_at(ratio(3, 1000), Revision(2));
        let p2 = make_bcl_pool(
            &env,
            0xC2,
            vec![(token_a(), 1_000_000), (token_b(), 1_000_000)],
            five.clone(),
        );
        let r1 = hash_of(&env, ModuleKind::ConcentratedLiquidity, Revision(1));
        let r2 = hash_of(&env, ModuleKind::ConcentratedLiquidity, Revision(2));
        assert_eq!(p2.pool_datum.actions[0].modules[0], r2.as_ref().to_vec());
        let cl_create = BandedCLRedeemer::Create {
            initial_state: four.clone(),
            pool_output_index: 0,
            initial_band: 0,
        }
        .to_plutus();
        mock.add_pool(
            &policy,
            &p2,
            withdrawal_tx(
                31,
                vec![
                    (r1, cl_create),
                    (current_fs(&env), fs_create(&default_fs(&env))),
                ],
            ),
            vec![withdrawal_tx(32, vec![])],
        );

        let (p3, _) = cs_pool(&env, 0xC3);
        mock.add_pool(
            &policy,
            &p3,
            withdrawal_tx(41, vec![]),
            vec![withdrawal_tx(42, vec![]), withdrawal_tx(43, vec![])],
        );

        let (p4, cfg4) = cs_pool(&env, 0xC4);
        mock.add_pool(
            &policy,
            &p4,
            withdrawal_tx(51, vec![(old_cs(&env), cs_create(&cfg4))]),
            vec![],
        );
        mock.broken.push(asset_unit(&policy, &p4));

        let (p5, cfg5) = cs_pool(&env, 0xC5);
        let current_cs = env.exec.module_scripts.first_of(ModuleKind::ConstantSum).unwrap().hash;
        mock.add_pool(
            &policy,
            &p5,
            withdrawal_tx(
                61,
                vec![
                    (current_cs, cs_create(&cfg5)),
                    (current_fs(&env), fs_create(&default_fs(&env))),
                ],
            ),
            vec![],
        );

        let persistence = crate::persistence::connect(&Default::default()).await?;
        let dao = persistence.indexer_dao("sundae_v4");
        let state = Arc::new(Mutex::new(SundaeV4HistoricalState::new()));
        bootstrap_v4(&mock, &protocol, &state, 1000, &*dao).await?;

        let locked = state.lock().await;
        let pools = &locked.latest().pools;
        assert_eq!(pools.len(), 5, "every pool is in state");
        let scoopable = |p: &SundaeV4Pool| {
            let pool = &pools[&p.pool_datum.identifier];
            env.exec.module_scripts.pool_modules(&pool.pool_datum).is_ok()
                && crate::sundaev4::module_configs_match_datum(pool, &env.exec).is_ok()
        };
        assert!(scoopable(&p1), "migrated CS pool");
        assert!(scoopable(&p2), "CL pool migrated to revision 2");
        assert!(!scoopable(&p3), "a CS pool with no config is not scooped");
        assert!(
            !scoopable(&p4),
            "a CS pool whose lookup failed is not scooped"
        );
        assert!(scoopable(&p5), "an untouched CS pool");

        match &pools[&p2.pool_datum.identifier].pool_type {
            PoolType::BandedConcentratedLiquidity { config } => {
                assert_eq!(config.rules.revision, Revision(2));
                assert_eq!(
                    crate::sundaev4::banded_math::config_hash(config),
                    crate::sundaev4::banded_math::config_hash(&five)
                );
            }
            other => panic!("expected CL, got {other:?}"),
        }
        drop(locked);

        // The recovered configs are persisted under the hashes the pools
        // name now, so a restart needs no walk.
        let rows = dao.load_module_configs().await?;
        let has = |p: &SundaeV4Pool, h: &ScriptHash| {
            rows.iter().any(|r| {
                r.pool_id == p.pool_datum.identifier.to_bytes() && r.module_hash == h.as_ref()
            })
        };
        assert!(has(&p1, &current_cs) && has(&p1, &current_fs(&env)));
        assert!(has(&p2, &r2));
        assert!(
            !rows.iter().any(|r| r.pool_id == p3.pool_datum.identifier.to_bytes()
                && r.module_hash == current_cs.as_ref())
        );
        Ok(())
    }
}
