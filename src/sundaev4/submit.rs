//! Network interactions: submit transactions via cardano-submit-api,
//! Blockfrost, or ogmios.

use anyhow::{Context, Result, bail};

/// Submit a CBOR-encoded signed transaction.
///
/// Backend selection from the URL:
/// - host contains `blockfrost.io` → Blockfrost `POST /tx/submit` with
///   `project_id` header taken from the URL's `project_id` query parameter
/// - URL contains `/api/submit/tx` → cardano-submit-api (raw CBOR POST)
/// - otherwise → Ogmios JSON-RPC (`submitTransaction`)
///
/// Returns the transaction hash on success.
pub async fn submit_tx(url: &str, cbor: &[u8]) -> Result<String> {
    if url.contains("blockfrost.io") {
        submit_blockfrost(url, cbor).await
    } else if url.contains("/api/submit/tx") {
        submit_cardano_api(url, cbor).await
    } else {
        submit_ogmios(url, cbor).await
    }
}

async fn submit_blockfrost(url: &str, cbor: &[u8]) -> Result<String> {
    // Pull `project_id` out of the URL query, then strip it before POSTing —
    // Blockfrost takes it as a header, not a query param.
    let parsed = url::Url::parse(url).context("invalid blockfrost submit URL")?;
    let project_id = parsed
        .query_pairs()
        .find(|(k, _)| k == "project_id")
        .map(|(_, v)| v.into_owned())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "blockfrost submit URL must include ?project_id=<token>; got `{url}`"
            )
        })?;
    let mut clean = parsed.clone();
    clean.set_query(None);
    let endpoint = clean.as_str().trim_end_matches('/').to_string();
    let submit_url = if endpoint.ends_with("/tx/submit") {
        endpoint
    } else {
        format!("{}/tx/submit", endpoint)
    };

    let client = reqwest::Client::new();
    let resp = client
        .post(&submit_url)
        .header("project_id", project_id)
        .header("Content-Type", "application/cbor")
        .body(cbor.to_vec())
        .send()
        .await
        .context("blockfrost submit request failed")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if status.is_success() {
        // Blockfrost returns the tx hash as a quoted JSON string.
        Ok(body.trim().trim_matches('"').to_string())
    } else {
        bail!("blockfrost submit failed ({}): {}", status, body);
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

/// Encode PlutusV3 cost model parameters as CBOR language views: `{2: [params...]}`.
pub fn encode_language_views(v3_params: &[i64]) -> Vec<u8> {
    encode_language_views_multi(v3_params, None)
}

/// Language views for the script integrity hash. Keys ascend (PlutusV2 = 1,
/// PlutusV3 = 2); V2 and V3 both use the plain definite encoding (only V1
/// has the legacy double-bagged quirk, and we never execute V1).
pub fn encode_language_views_multi(v3_params: &[i64], v2_params: Option<&[i64]>) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut enc = minicbor::Encoder::new(&mut buf);
        let n = 1 + v2_params.is_some() as u64;
        enc.map(n).unwrap();
        if let Some(v2) = v2_params {
            enc.u32(1).unwrap(); // PlutusV2 = language 1
            enc.array(v2.len() as u64).unwrap();
            for &p in v2 {
                enc.i64(p).unwrap();
            }
        }
        enc.u32(2).unwrap(); // PlutusV3 = language 2
        enc.array(v3_params.len() as u64).unwrap();
        for &p in v3_params {
            enc.i64(p).unwrap();
        }
    }
    buf
}
