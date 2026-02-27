//! Network interactions: submit transactions via cardano-submit-api.

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
