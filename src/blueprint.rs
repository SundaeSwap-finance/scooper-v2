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

        // (validator title pattern, reference key, field name for error messages).
        // Patterns are substring-matched against the plutus.json validator title,
        // so use the most specific suffix (e.g. `_module`, `_validator`) to avoid
        // collisions: `pool` would match `pool_mint` and `pool_validator`; `order`
        // would match `order_validator`, `basic_order_module`, and `swap_order_module`.
        let mappings: &[(&[&str], &str, &str)] = &[
            (&["constant_product_module", "constant_product", "constantProduct"], "constantProduct", "constant_product"),
            (&["fee_split_module", "fee_split", "feeSplit"], "feeSplit", "fee_split"),
            (&["fairness_module", "fairness"], "fairness", "fairness"),
            (&["pool_validator", "pool", "vault"], "pool", "pool"),
            (&["order_validator", "order"], "order", "order"),
            (&["pool_mint", "poolMint"], "poolMint", "pool_mint"),
            (&["settings_validator", "settings"], "settings", "settings"),
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

        // Try to find constant_sum (optional — not all blueprints include it)
        let constant_sum = make_info(
            self,
            &["constant_sum_module", "constant_sum", "constantSum"],
            "constantSum",
            "constant_sum",
        ).ok();

        // Try to find swap_order (optional — pre-redesign blueprints lack it)
        let swap_order = make_info(
            self,
            &["swap_order_module", "swap_order", "swapOrder"],
            "swapOrder",
            "swap_order",
        ).ok();
        // Same for basic_order (handles Deposit/Withdraw/Claim).
        let basic_order = make_info(
            self,
            &["basic_order_module", "basic_order", "basicOrder"],
            "basicOrder",
            "basic_order",
        ).ok();
        // Concentrated liquidity module (optional — only present when CL pools exist).
        let concentrated_liquidity = make_info(
            self,
            &["concentrated_liquidity_module", "concentrated_liquidity", "concentratedLiquidity"],
            "concentratedLiquidity",
            "concentrated_liquidity",
        ).ok();

        Ok(ModuleScripts {
            constant_product: make_info(self, mappings[0].0, mappings[0].1, mappings[0].2)?,
            fee_split: make_info(self, mappings[1].0, mappings[1].1, mappings[1].2)?,
            fairness: make_info(self, mappings[2].0, mappings[2].1, mappings[2].2)?,
            pool: make_info(self, mappings[3].0, mappings[3].1, mappings[3].2)?,
            order: make_info(self, mappings[4].0, mappings[4].1, mappings[4].2)?,
            pool_mint: make_info(self, mappings[5].0, mappings[5].1, mappings[5].2)?,
            settings: make_info(self, mappings[6].0, mappings[6].1, mappings[6].2)?,
            constant_sum,
            concentrated_liquidity,
            swap_order,
            basic_order,
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

    /// Smoke test against the deployed preview blueprint. Verifies that the
    /// title-pattern mappings in `to_v4_module_scripts` resolve every required
    /// validator + ref-UTxO from the post-redesign contract titles
    /// (`pool.pool_validator.spend`, `modules/constant_product.constant_product_module.withdraw`,
    /// `constraints/swap_order.swap_order_module.withdraw`, etc.). Skipped if
    /// the file is missing so CI on a fresh checkout doesn't fail.
    #[test]
    fn test_load_preview_blueprint() {
        let path = "/home/pi/proj/sundae/sundae-v4/preview-blueprint.json";
        let Ok(data) = std::fs::read_to_string(path) else {
            eprintln!("skipping: {path} not present");
            return;
        };
        let bp: Blueprint = serde_json::from_str(&data)
            .expect("preview blueprint should parse");
        let modules = bp.to_v4_module_scripts()
            .expect("to_v4_module_scripts should resolve every required validator");
        // Pool validator hash should be 28 bytes.
        assert_eq!(modules.pool.hash.as_slice().len(), 28);
        assert_eq!(modules.order.hash.as_slice().len(), 28);
        assert_eq!(modules.settings.hash.as_slice().len(), 28);
        assert_eq!(modules.pool_mint.hash.as_slice().len(), 28);
        assert!(modules.constant_product.script_cbor.is_some());
        assert!(modules.constant_sum.is_some(), "constant_sum optional but present in preview");
        assert!(modules.swap_order.is_some(), "swap_order required for new order dispatch");
    }

    /// Verifies that `config/preview-v4.json` deserializes into the scooper's
    /// `SundaeV4Protocol` end-to-end and that the hashes in it agree with the
    /// blueprint at `~/proj/sundae/sundae-v4/preview-blueprint.json`. Catches
    /// drift between the two without running any chain ops.
    #[test]
    fn test_preview_v4_config_matches_blueprint() {
        // Skip if blueprint is absent (fresh checkout / non-deploy machine).
        let bp_path = "/home/pi/proj/sundae/sundae-v4/preview-blueprint.json";
        let Ok(bp_data) = std::fs::read_to_string(bp_path) else {
            eprintln!("skipping: {bp_path} not present");
            return;
        };
        let bp: Blueprint = serde_json::from_str(&bp_data).expect("blueprint should parse");

        // Parse the config file via the scooper's full config layer, then pull
        // out the v4 protocol section.
        let cfg_text = std::fs::read_to_string("config/preview-v4.json")
            .expect("config/preview-v4.json should exist");
        let cfg_value: serde_json::Value =
            serde_json::from_str(&cfg_text).expect("config should parse as JSON");
        let v4_value = &cfg_value["protocol"]["v4"];
        let v4: crate::sundaev4::SundaeV4Protocol =
            serde_json::from_value(v4_value.clone()).expect("v4 protocol should deserialize");

        // pool/order/settings/pool_nft script hashes must match the blueprint.
        let bp_modules = bp.to_v4_module_scripts().expect("blueprint modules");
        assert_eq!(
            v4.pool_script_hash, bp_modules.pool.hash,
            "pool script hash drift",
        );
        assert_eq!(
            v4.order_script_hashes[0], bp_modules.order.hash,
            "order script hash drift",
        );
        assert_eq!(
            v4.settings_script_hash, bp_modules.settings.hash,
            "settings script hash drift",
        );
        assert_eq!(
            v4.pool_nft_policy, bp_modules.pool_mint.hash,
            "pool_nft_policy must equal pool_mint validator hash",
        );

        // settings_nft policy must equal the settings_mint validator hash.
        let settings_mint_hash =
            bp.find_validator("settings_mint").expect("settings_mint validator").hash.clone();
        assert_eq!(hex::encode(v4.settings_nft.policy), settings_mint_hash);
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
