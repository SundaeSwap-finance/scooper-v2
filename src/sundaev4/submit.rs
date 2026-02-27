//! Network interactions: submit transactions, fetch protocol parameters.

use anyhow::{Context, Result, bail};

/// Submit a CBOR-encoded signed transaction.
///
/// Returns the transaction hash on success.
pub async fn submit_tx(url: &str, cbor: &[u8]) -> Result<String> {
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

/// Fetch PlutusV3 cost model parameters from an Ogmios endpoint and encode
/// them as CBOR "language views" for use in the `script_data_hash` computation.
///
/// Returns the CBOR-encoded `{2: [param1, param2, ...]}` bytes.
pub async fn fetch_language_views(ogmios_url: &str) -> Result<Vec<u8>> {
    let client = reqwest::Client::new();
    let resp = client
        .post(ogmios_url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "queryLedgerState/protocolParameters",
            "id": 1
        }))
        .send()
        .await
        .context("ogmios request failed")?;

    let json: serde_json::Value = resp
        .json()
        .await
        .context("ogmios response parse failed")?;

    let params = json["result"]["plutusCostModels"]["plutus:v3"]
        .as_array()
        .context("missing plutus:v3 cost model in protocol parameters")?;

    let v3_params: Vec<i64> = params
        .iter()
        .map(|v| v.as_i64().context("cost model param not i64"))
        .collect::<Result<_>>()?;

    Ok(encode_language_views(&v3_params))
}

/// Encode PlutusV3 cost model parameters as CBOR language views: `{2: [params...]}`.
fn encode_language_views(v3_params: &[i64]) -> Vec<u8> {
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



/// Evaluate a transaction via Ogmios to get script execution costs and trace output.
///
/// Returns the raw JSON response on success, or an error with trace messages on failure.
pub async fn evaluate_tx(ogmios_url: &str, cbor: &[u8]) -> Result<serde_json::Value> {
    let client = reqwest::Client::new();
    let cbor_hex = hex::encode(cbor);
    let resp = client
        .post(ogmios_url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "evaluateTransaction",
            "params": { "transaction": { "cbor": cbor_hex } },
            "id": 1
        }))
        .send()
        .await
        .context("ogmios evaluate request failed")?;

    let json: serde_json::Value = resp
        .json()
        .await
        .context("ogmios evaluate response parse failed")?;

    if let Some(err) = json.get("error") {
        bail!("evaluate failed: {}", serde_json::to_string_pretty(err).unwrap_or_default());
    }

    Ok(json)
}

