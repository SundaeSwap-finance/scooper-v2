//! Network interactions: submit transactions via cardano-submit-api or ogmios.

use anyhow::{Context, Result, bail};

/// Submit a CBOR-encoded signed transaction.
///
/// Automatically detects the backend:
/// - URLs containing `/api/submit/tx` use cardano-submit-api (raw CBOR POST)
/// - All other URLs use Ogmios JSON-RPC (`submitTransaction`)
///
/// Returns the transaction hash on success.
pub async fn submit_tx(url: &str, cbor: &[u8]) -> Result<String> {
    if url.contains("/api/submit/tx") {
        submit_cardano_api(url, cbor).await
    } else {
        submit_ogmios(url, cbor).await
    }
}

async fn submit_cardano_api(url: &str, cbor: &[u8]) -> Result<String> {
    let client = reqwest::Client::new();
    let resp = client
        .post(url)
        .header("Content-Type", "application/cbor")
        .body(cbor.to_vec())
        .send()
        .await
        .context("submit request failed")?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();

    if status.is_success() {
        // cardano-submit-api returns the tx hash as a JSON string
        let hash = body.trim().trim_matches('"').to_string();
        Ok(hash)
    } else {
        bail!("submit failed ({}): {}", status, body);
    }
}

async fn submit_ogmios(url: &str, cbor: &[u8]) -> Result<String> {
    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "submitTransaction",
        "params": {
            "transaction": { "cbor": hex::encode(cbor) }
        },
        "id": 1
    });
    let resp = client
        .post(url)
        .json(&body)
        .send()
        .await
        .context("ogmios submit request failed")?;

    let status = resp.status();
    let json: serde_json::Value = resp
        .json()
        .await
        .context("ogmios response not valid JSON")?;

    if let Some(result) = json.get("result") {
        let hash = result
            .get("transaction")
            .and_then(|t| t.get("id"))
            .and_then(|id| id.as_str())
            .unwrap_or("unknown");
        Ok(hash.to_string())
    } else {
        let error = json
            .get("error")
            .map(|e| e.to_string())
            .unwrap_or_default();
        bail!("ogmios submit failed ({}): {}", status, error);
    }
}

/// Evaluate a transaction via Ogmios `evaluateTransaction`.
///
/// Returns Ok with the evaluation result JSON on success, or Err with details
/// on failure. Note: Ogmios cannot see unconfirmed (mempool) UTxOs, so this
/// will fail for chained transactions consuming predicted outputs.
#[allow(dead_code)]
pub async fn evaluate_tx_ogmios(url: &str, cbor: &[u8]) -> Result<serde_json::Value> {
    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "evaluateTransaction",
        "params": {
            "transaction": { "cbor": hex::encode(cbor) }
        },
        "id": 1
    });
    let resp = client
        .post(url)
        .json(&body)
        .send()
        .await
        .context("ogmios evaluate request failed")?;

    let json: serde_json::Value = resp
        .json()
        .await
        .context("ogmios evaluate response not valid JSON")?;

    if let Some(result) = json.get("result") {
        Ok(result.clone())
    } else {
        let error = json
            .get("error")
            .map(|e| e.to_string())
            .unwrap_or_default();
        bail!("ogmios evaluate failed: {}", error);
    }
}

/// Encode PlutusV3 cost model parameters as CBOR language views: `{2: [params...]}`.
pub fn encode_language_views(v3_params: &[i64]) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut enc = minicbor::Encoder::new(&mut buf);
        enc.map(1).unwrap();
        enc.u32(2).unwrap(); // PlutusV3 = language 2
        enc.array(v3_params.len() as u64).unwrap();
        for &p in v3_params {
            enc.i64(p).unwrap();
        }
    }
    buf
}
