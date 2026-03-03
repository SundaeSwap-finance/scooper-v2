//! Protocol blueprint types matching the SundaeSwap GraphQL API format.
//!
//! A blueprint describes a set of validators (with their script hashes and
//! compiled code) and their on-chain reference UTxOs. This is protocol-agnostic
//! and can represent V3, V4, stableswap, or future protocol versions.

use anyhow::{Context, Result};
use pallas_crypto::hash::Hasher;
use pallas_primitives::Hash;

use crate::sundaev4::ScriptRefInfo;

/// A protocol blueprint — validators and their on-chain reference UTxOs.
/// Mirrors the format from the SundaeSwap GraphQL API.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Blueprint {
    pub validators: Vec<Validator>,
    #[serde(default)]
    pub references: Vec<Reference>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Validator {
    pub title: String,
    pub hash: String,
    #[serde(default)]
    pub compiled_code: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Reference {
    pub key: String,
    pub tx_in: TxIn,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct TxIn {
    pub hash: String,
    pub index: u64,
}

impl Blueprint {
    /// Find a validator by title substring (e.g. "vault" matches "vault.pool_vault.spend").
    #[allow(dead_code)]
    pub fn find_validator(&self, title: &str) -> Option<&Validator> {
        self.validators.iter().find(|v| v.title.contains(title))
    }

    /// Find a reference by key.
    #[allow(dead_code)]
    pub fn find_reference(&self, key: &str) -> Option<&Reference> {
        self.references.iter().find(|r| r.key == key)
    }

    /// Combine a validator hash + reference txIn into a `ScriptRefInfo`.
    ///
    /// Looks up the validator by `validator_title` (substring match) and the
    /// reference by `ref_key` (exact match), then constructs a `ScriptRefInfo`.
    #[allow(dead_code)]
    pub fn script_ref_info(&self, validator_title: &str, ref_key: &str) -> Result<ScriptRefInfo> {
        let validator = self
            .find_validator(validator_title)
            .with_context(|| format!("no validator matching '{validator_title}'"))?;

        let reference = self
            .find_reference(ref_key)
            .with_context(|| format!("no reference with key '{ref_key}'"))?;

        let hash_bytes = hex::decode(&validator.hash)
            .with_context(|| format!("invalid hex in validator hash '{}'", validator.hash))?;
        let script_hash: Hash<28> = hash_bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("validator hash not 28 bytes"))?;

        let tx_hash_bytes = hex::decode(&reference.tx_in.hash)
            .with_context(|| format!("invalid hex in ref txIn hash '{}'", reference.tx_in.hash))?;
        let tx_hash: Hash<32> = tx_hash_bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("tx hash not 32 bytes"))?;

        let ref_utxo = crate::cardano_types::TransactionInput::new(tx_hash, reference.tx_in.index);

        Ok(ScriptRefInfo {
            hash: script_hash,
            ref_utxo,
            script_cbor: validator.compiled_code.clone(),
        })
    }

    /// Derive `ModuleScripts` from this blueprint using the standard V4 validator
    /// title → field mapping.
    ///
    /// Returns `Err` if any required validator or reference is missing.
    #[allow(dead_code)]
    pub fn to_v4_module_scripts(&self) -> Result<crate::sundaev4::ModuleScripts> {
        use crate::sundaev4::ModuleScripts;

        // (validator title pattern, reference key, field name for error messages)
        let mappings: &[(&[&str], &str, &str)] = &[
            (&["constant_product", "constant-product", "constantProduct"], "constantProduct", "constant_product"),
            (&["fee_split", "fee-split", "feeSplit"], "feeSplit", "fee_split"),
            (&["fairness"], "fairness", "fairness"),
            (&["vault"], "vault", "vault"),
            (&["order"], "order", "order"),
            (&["pool_mint", "pool-mint", "poolMint"], "poolMint", "pool_mint"),
            (&["settings_mint", "settings-mint", "settings", "settingsMint"], "settings", "settings"),
        ];

        fn find_by_patterns<'a>(bp: &'a Blueprint, patterns: &[&str]) -> Option<&'a Validator> {
            for pattern in patterns {
                if let Some(v) = bp.find_validator(pattern) {
                    return Some(v);
                }
            }
            None
        }

        fn find_ref_by_patterns<'a>(bp: &'a Blueprint, patterns: &[&str]) -> Option<&'a Reference> {
            for pattern in patterns {
                if let Some(r) = bp.find_reference(pattern) {
                    return Some(r);
                }
            }
            None
        }

        fn make_info(bp: &Blueprint, patterns: &[&str], ref_key: &str, field_name: &str) -> Result<ScriptRefInfo> {
            let validator = find_by_patterns(bp, patterns)
                .with_context(|| format!("no validator matching patterns for '{field_name}'"))?;

            // Try ref_key first, then all validator title patterns
            let reference = bp.find_reference(ref_key)
                .or_else(|| find_ref_by_patterns(bp, patterns))
                .with_context(|| format!("no reference for '{field_name}'"))?;

            let hash_bytes = hex::decode(&validator.hash)
                .with_context(|| format!("invalid hex in '{field_name}' hash"))?;
            let script_hash: Hash<28> = hash_bytes
                .as_slice()
                .try_into()
                .map_err(|_| anyhow::anyhow!("'{field_name}' hash not 28 bytes"))?;

            let tx_hash_bytes = hex::decode(&reference.tx_in.hash)
                .with_context(|| format!("invalid hex in '{field_name}' ref tx hash"))?;
            let tx_hash: Hash<32> = tx_hash_bytes
                .as_slice()
                .try_into()
                .map_err(|_| anyhow::anyhow!("'{field_name}' ref tx hash not 32 bytes"))?;

            let ref_utxo = crate::cardano_types::TransactionInput::new(tx_hash, reference.tx_in.index);

            Ok(ScriptRefInfo {
                hash: script_hash,
                ref_utxo,
                script_cbor: validator.compiled_code.clone(),
            })
        }

        Ok(ModuleScripts {
            constant_product: make_info(self, mappings[0].0, mappings[0].1, mappings[0].2)?,
            fee_split: make_info(self, mappings[1].0, mappings[1].1, mappings[1].2)?,
            fairness: make_info(self, mappings[2].0, mappings[2].1, mappings[2].2)?,
            vault: make_info(self, mappings[3].0, mappings[3].1, mappings[3].2)?,
            order: make_info(self, mappings[4].0, mappings[4].1, mappings[4].2)?,
            pool_mint: make_info(self, mappings[5].0, mappings[5].1, mappings[5].2)?,
            settings: make_info(self, mappings[6].0, mappings[6].1, mappings[6].2)?,
        })
    }
}

impl Validator {
    /// Compute the PlutusV3 script hash from compiled code.
    ///
    /// PlutusV3 hash = blake2b_224(0x03 || script_cbor)
    /// where script_cbor is the hex-decoded `compiled_code`.
    #[allow(dead_code)]
    pub fn compute_hash(&self) -> Result<Hash<28>> {
        let code = self
            .compiled_code
            .as_ref()
            .context("validator has no compiled_code")?;
        let script_cbor = hex::decode(code).context("invalid hex in compiled_code")?;
        let mut preimage = Vec::with_capacity(1 + script_cbor.len());
        preimage.push(0x03);
        preimage.extend_from_slice(&script_cbor);
        Ok(Hasher::<224>::hash(&preimage))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_blueprint() -> Blueprint {
        serde_json::from_str(r#"{
            "validators": [
                { "title": "vault.pool_vault.spend", "hash": "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd" },
                { "title": "order.order.spend", "hash": "11223344aabbccdd11223344aabbccdd11223344aabbccdd11223344" }
            ],
            "references": [
                { "key": "vault", "txIn": { "hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "index": 0 } },
                { "key": "order", "txIn": { "hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "index": 1 } }
            ]
        }"#).unwrap()
    }

    #[test]
    fn test_find_validator() {
        let bp = sample_blueprint();
        assert!(bp.find_validator("vault").is_some());
        assert!(bp.find_validator("order").is_some());
        assert!(bp.find_validator("missing").is_none());
    }

    #[test]
    fn test_find_reference() {
        let bp = sample_blueprint();
        assert!(bp.find_reference("vault").is_some());
        assert!(bp.find_reference("missing").is_none());
    }

    #[test]
    fn test_script_ref_info() {
        let bp = sample_blueprint();
        let info = bp.script_ref_info("vault", "vault").unwrap();
        assert_eq!(hex::encode(info.hash), "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd");
        assert_eq!(info.ref_utxo.0.index, 0);
    }

    #[test]
    fn test_deserialize_camel_case() {
        let json = r#"{
            "validators": [
                { "title": "test", "hash": "aabb", "compiledCode": "deadbeef" }
            ],
            "references": [
                { "key": "test", "txIn": { "hash": "ccdd", "index": 2 } }
            ]
        }"#;
        let bp: Blueprint = serde_json::from_str(json).unwrap();
        assert_eq!(bp.validators[0].compiled_code.as_deref(), Some("deadbeef"));
        assert_eq!(bp.references[0].tx_in.index, 2);
    }
}
