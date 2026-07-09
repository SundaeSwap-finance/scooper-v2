//! Butane v2 "underlying" integration: mint synthetics (ADAb) by locking
//! the underlying asset, composed directly into scoop transactions as a
//! router conversion edge.
//!
//! Everything here is config-driven and degrades gracefully: if the
//! `butane` section is absent, incomplete, or its deployment artifact can't
//! be loaded/verified, the scooper runs exactly as before — no edges are
//! offered to the router. Nothing is hardcoded to a deployment.
//!
//! Mechanics (from the protocol-simulation fixture dissection, 2026-07-08):
//! a deposit spends only user funds and creates a fresh "pot" UTxO at
//! `spend`-script address (staking cred = mint policy) holding the locked
//! underlying + one freshly-minted `treas` marker, with inline datum
//! `Constr 6 [synthetic_name, credit]`. The synthetic mints at the params
//! ratio (no fee, no oracle), authorized by four zero-withdrawal validators
//! plus the mint policy (redeemer: int 0). The per-synthetic params UTxO is
//! a read-only reference input; deposits are contention-free.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use pallas_codec::utils::CborWrap;
use pallas_primitives::conway;
use serde::Deserialize;

use crate::bigint::BigInt;
use crate::cardano_types::{AssetClass, TransactionInput};
use crate::sundaev4::conversions::ConversionEdge;

// ─── Config ─────────────────────────────────────────────────────────────────

/// The `execution.butane` config section.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ButaneConfig {
    /// Path to the deployment artifact (e.g.
    /// `butane-v2.deployment.preview.json`): per-validator `[outref,
    /// output]` CBOR entries carrying the ref-script UTxOs and bytecode.
    pub deployment_file: String,
    /// Expected script hashes by deployment role, hex. Verified against the
    /// recompiled hashes of the artifact's bytecode — a mismatch disables
    /// the integration rather than composing txs that can't validate.
    /// Required roles: `spend`, `mint`, `synthetics`, `synthetics-aux`,
    /// `external-underlying`, `upgradable`.
    pub script_hashes: BTreeMap<String, String>,
    /// The registry UTxO the upgradable validator's redeemer points at,
    /// "txid#index".
    pub registry_utxo: String,
    /// Hex CBOR of the registry UTxO's resolved output — needed to build
    /// script contexts during local eval (the scooper doesn't index Butane
    /// state). Absent = deposits can't be evaluated, edges stay closed.
    #[serde(default)]
    pub registry_utxo_cbor: Option<String>,
    /// Synthetics the router may mint via the underlying window.
    #[serde(default)]
    pub synthetics: Vec<ButaneSyntheticConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ButaneSyntheticConfig {
    /// Asset name under the mint policy, e.g. "ADAb".
    pub name: String,
    /// The live `p_<name>` params UTxO, "txid#index" (referenced read-only
    /// by every deposit). Must be updated if governance replaces it.
    pub params_utxo: String,
    /// Hex CBOR of the params UTxO's resolved output (see registry-utxo-cbor).
    #[serde(default)]
    pub params_utxo_cbor: Option<String>,
    /// Mint ratio: minted = deposited · num / den. Must match the params
    /// datum's underlying entry for the deposit asset (ADA) — the contract
    /// is the arbiter; this drives routing estimates and the built amounts.
    pub rate: (u64, u64),
    /// Optional cap on lovelace routed through the mint per tx.
    #[serde(default)]
    pub max_input: Option<u64>,
    #[serde(default)]
    pub enabled: bool,
}

const REQUIRED_ROLES: &[&str] = &[
    "spend",
    "mint",
    "synthetics",
    "synthetics-aux",
    "external-underlying",
    "upgradable",
];

// ─── Deployment artifact ────────────────────────────────────────────────────

/// One deployed validator: its ref-script UTxO and bytecode.
#[derive(Clone, Debug)]
pub struct DeployedScript {
    pub hash: Vec<u8>,
    pub ref_input: TransactionInput,
    /// The resolved ref UTxO output (address/value/script), needed so the
    /// evaluator can resolve the ref input without a chain query.
    pub ref_output: conway::TransactionOutput,
    /// Plutus language version (1/2/3) — drives the hash preimage prefix,
    /// the redeemer cost model, and the script_data_hash language views.
    pub plutus_version: u8,
    /// Raw Plutus script bytes (the `59…` wrapped form).
    pub script_bytes: Vec<u8>,
}

/// Loaded, verified runtime state for the integration.
#[derive(Clone, Debug)]
pub struct ButaneRuntime {
    /// Keyed by config role name (see `REQUIRED_ROLES`).
    pub scripts: BTreeMap<String, DeployedScript>,
    pub registry_utxo: TransactionInput,
    pub synthetics: Vec<ButaneSyntheticConfig>,
    /// Registry + params UTxOs resolved from config CBOR (for local eval).
    extra_resolved: Vec<(TransactionInput, conway::TransactionOutput)>,
}

fn parse_outref(s: &str) -> Result<TransactionInput> {
    let (txid, idx) = s
        .split_once('#')
        .with_context(|| format!("outref {s:?} must be txid#index"))?;
    let txid = hex::decode(txid).with_context(|| format!("outref txid {txid:?}"))?;
    anyhow::ensure!(txid.len() == 32, "outref txid must be 32 bytes");
    let idx: u64 = idx.parse().with_context(|| format!("outref index {idx:?}"))?;
    let hash: [u8; 32] = txid.as_slice().try_into().expect("length checked");
    Ok(TransactionInput::new(hash.into(), idx))
}

impl ButaneRuntime {
    /// Load and verify the deployment artifact against the configured
    /// hashes. Any error here should be treated as "integration disabled",
    /// not fatal — see [`load_runtime`].
    pub fn load(config: &ButaneConfig) -> Result<Self> {
        let raw = std::fs::read_to_string(&config.deployment_file)
            .with_context(|| format!("read {}", config.deployment_file))?;
        let artifact: serde_json::Value =
            serde_json::from_str(&raw).context("parse deployment artifact")?;
        let deployments = artifact
            .get("deployments")
            .and_then(|d| d.as_object())
            .context("artifact missing `deployments` object")?;

        // Config role → artifact key. The artifact uses camelCase names.
        let artifact_key = |role: &str| -> String {
            match role {
                "synthetics-aux" => "syntheticsAux".into(),
                "external-underlying" => "externalUnderlying".into(),
                other => other.into(),
            }
        };

        let mut scripts = BTreeMap::new();
        for role in REQUIRED_ROLES {
            let expected_hex = config
                .script_hashes
                .get(*role)
                .with_context(|| format!("script-hashes missing role {role:?}"))?;
            let expected =
                hex::decode(expected_hex).with_context(|| format!("hash for {role:?}"))?;
            let entry_hex = deployments
                .get(&artifact_key(role))
                .and_then(|v| v.as_str())
                .with_context(|| format!("artifact missing deployment for {role:?}"))?;
            let entry = hex::decode(entry_hex)
                .with_context(|| format!("deployment entry for {role:?} not hex"))?;
            let (raw_input, ref_output): (
                pallas_primitives::TransactionInput,
                conway::TransactionOutput,
            ) = minicbor::decode(&entry)
                .map_err(|e| anyhow::anyhow!("decode deployment entry {role}: {e}"))?;
            let ref_input =
                TransactionInput::new(raw_input.transaction_id, raw_input.index);

            let conway::TransactionOutput::PostAlonzo(ref body) = ref_output else {
                bail!("deployment {role}: expected post-alonzo output");
            };
            let Some(script_ref) = &body.script_ref else {
                bail!("deployment {role}: output carries no reference script");
            };
            let (plutus_version, script_bytes): (u8, Vec<u8>) = match &script_ref.0 {
                conway::PseudoScript::PlutusV1Script(s) => (1, s.0.to_vec()),
                conway::PseudoScript::PlutusV2Script(s) => (2, s.0.to_vec()),
                conway::PseudoScript::PlutusV3Script(s) => (3, s.0.to_vec()),
                conway::PseudoScript::NativeScript(_) => {
                    bail!("deployment {role}: native scripts unsupported")
                }
            };

            // Verify: script hash = blake2b-224(language_tag || bytes).
            let mut preimage = Vec::with_capacity(1 + script_bytes.len());
            preimage.push(plutus_version);
            preimage.extend_from_slice(&script_bytes);
            let actual = pallas_crypto::hash::Hasher::<224>::hash(&preimage);
            if actual.as_ref() != expected.as_slice() {
                bail!(
                    "deployment {role}: bytecode hashes to {} but config says {expected_hex} \
                     — wrong artifact or stale config",
                    hex::encode(actual),
                );
            }

            scripts.insert(
                role.to_string(),
                DeployedScript {
                    hash: expected,
                    ref_input,
                    ref_output: ref_output.clone(),
                    plutus_version,
                    script_bytes,
                },
            );
        }

        let registry_utxo = parse_outref(&config.registry_utxo)?;
        let mut extra_resolved = Vec::new();
        let mut decode_output = |label: &str, hex_cbor: &str| -> Result<conway::TransactionOutput> {
            let bytes = hex::decode(hex_cbor)
                .with_context(|| format!("{label} output cbor not hex"))?;
            minicbor::decode(&bytes)
                .map_err(|e| anyhow::anyhow!("decode {label} output: {e}"))
        };
        if let Some(cbor) = &config.registry_utxo_cbor {
            extra_resolved.push((registry_utxo.clone(), decode_output("registry", cbor)?));
        }
        for synth in &config.synthetics {
            if let Some(cbor) = &synth.params_utxo_cbor {
                extra_resolved.push((
                    parse_outref(&synth.params_utxo)?,
                    decode_output(&format!("p_{}", synth.name), cbor)?,
                ));
            }
        }
        Ok(Self {
            scripts,
            registry_utxo,
            synthetics: config.synthetics.clone(),
            extra_resolved,
        })
    }

    pub fn script(&self, role: &str) -> &DeployedScript {
        self.scripts.get(role).expect("verified at load")
    }

    /// The synthetic's AssetClass under the mint policy.
    pub fn synthetic_asset(&self, name: &str) -> AssetClass {
        AssetClass {
            policy: self.script("mint").hash.clone(),
            token: name.as_bytes().to_vec(),
        }
    }

    /// Conversion edges the router may use: enabled synthetics with a
    /// params UTxO configured. ADA → synthetic at the configured rate,
    /// no fee (mint direction is fee-less; only withdraws pay).
    pub fn edges(&self) -> Vec<ConversionEdge> {
        self.synthetics
            .iter()
            .filter(|s| s.enabled)
            .map(|s| ConversionEdge {
                key: format!("butane:{}:mint", s.name),
                from: AssetClass { policy: vec![], token: vec![] },
                to: self.synthetic_asset(&s.name),
                rate_num: BigInt::from(s.rate.0),
                rate_den: BigInt::from(s.rate.1),
                fee_bps: 0,
                max_input: s.max_input.map(BigInt::from),
            })
            .collect()
    }

    pub fn synthetic_config(&self, name: &str) -> Option<&ButaneSyntheticConfig> {
        self.synthetics.iter().find(|s| s.name == name)
    }

    /// Every butane reference input with a resolvable output, for the local
    /// evaluator's script contexts: the deployment ref-script UTxOs (from
    /// the artifact) plus the registry and per-synthetic params UTxOs (from
    /// config CBOR). Synthetics without params CBOR are skipped — their
    /// deposits fail eval with a missing-resolution error rather than a
    /// wrong context.
    pub fn resolved_ref_outputs(
        &self,
    ) -> Vec<(TransactionInput, conway::TransactionOutput)> {
        let mut out: Vec<(TransactionInput, conway::TransactionOutput)> = self
            .scripts
            .values()
            .map(|ds| (ds.ref_input.clone(), ds.ref_output.clone()))
            .collect();
        out.extend(self.extra_resolved.iter().cloned());
        out
    }
}

// ─── Deposit composition ────────────────────────────────────────────────────

/// The tx-level pieces of one underlying deposit, ready for the tx builder
/// to merge: a pot output, the mint entries under the butane policy, the
/// four zero-withdrawals (with each validator's Plutus version, for
/// language views), the mint redeemer, and the reference inputs.
#[derive(Clone, Debug)]
pub struct DepositPieces {
    pub pot_output: conway::TransactionOutput,
    /// (asset name, amount) under the butane mint policy.
    pub mint_assets: Vec<(Vec<u8>, i64)>,
    pub mint_policy: Vec<u8>,
    /// Mint redeemer for the butane policy: int 0.
    pub mint_redeemer: pallas_primitives::PlutusData,
    /// (script hash, redeemer, plutus version) per zero-withdrawal.
    pub withdrawals: Vec<(Vec<u8>, pallas_primitives::PlutusData, u8)>,
    /// params UTxO + registry UTxO + executed validators' ref-script UTxOs.
    pub ref_inputs: Vec<TransactionInput>,
}

fn constr(tag_idx: u64, fields: Vec<pallas_primitives::PlutusData>) -> pallas_primitives::PlutusData {
    let tag = if tag_idx < 7 { 121 + tag_idx } else { 1280 + (tag_idx - 7) };
    pallas_primitives::PlutusData::Constr(pallas_primitives::Constr {
        tag,
        any_constructor: None,
        fields: pallas_primitives::MaybeIndefArray::Def(fields),
    })
}

impl ButaneRuntime {
    /// Compose the pieces for depositing `deposit_lovelace` and minting
    /// `minted` of `synthetic`. Network id 0 = testnet.
    pub fn deposit_pieces(
        &self,
        synthetic: &str,
        deposit_lovelace: u64,
        minted: u64,
        network_id: u8,
    ) -> Result<DepositPieces> {
        use pallas_primitives::PlutusData;
        let cfg = self
            .synthetic_config(synthetic)
            .with_context(|| format!("synthetic {synthetic:?} not configured"))?;
        let spend = self.script("spend");
        let mint = self.script("mint");
        let synthetics = self.script("synthetics");
        let synthetics_aux = self.script("synthetics-aux");
        let underlying = self.script("external-underlying");
        let upgradable = self.script("upgradable");

        // Pot address: base address, script payment (spend) + script staking
        // (mint policy). Header type 0b0011 | network.
        let mut addr = Vec::with_capacity(57);
        addr.push(0x30 | (network_id & 0x0f));
        addr.extend_from_slice(&spend.hash);
        addr.extend_from_slice(&mint.hash);

        // Pot value: the locked lovelace + 1 treas marker (minted here).
        let treas: (Vec<u8>, i64) = (b"treas".to_vec(), 1);
        let pot_value = conway::Value::Multiasset(
            deposit_lovelace,
            pallas_primitives::NonEmptyKeyValuePairs::Def(vec![(
                pallas_crypto::hash::Hash::<28>::from(mint.hash.as_slice()),
                pallas_primitives::NonEmptyKeyValuePairs::Def(vec![(
                    pallas_primitives::Bytes::from(treas.0.clone()),
                    pallas_primitives::PositiveCoin::try_from(1i64 as u64).unwrap(),
                )]),
            )]),
        );
        // Pot datum: Constr 6 [synthetic name, credit].
        let datum = constr(6, vec![
            PlutusData::BoundedBytes(synthetic.as_bytes().to_vec().into()),
            PlutusData::BigInt(pallas_primitives::BigInt::Int((minted as i64).into())),
        ]);
        let pot_output = conway::TransactionOutput::PostAlonzo(
            pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
                address: pallas_primitives::Bytes::from(addr),
                value: pot_value,
                datum_option: Some(conway::PseudoDatumOption::Data(
                    CborWrap(datum),
                )),
                script_ref: None,
            },
        );

        // Withdraw redeemers (fixture dissection, seq 16):
        //   synthetics     → Constr 1 []
        //   syntheticsAux  → Constr 16 [synthetic name]
        //   underlying     → bytes(syntheticsAux hash)
        //   upgradable     → Constr 0 [Constr 0 [registry txid], registry idx]
        let withdrawals = vec![
            (synthetics.hash.clone(), constr(1, vec![]), synthetics.plutus_version),
            (
                synthetics_aux.hash.clone(),
                constr(16, vec![PlutusData::BoundedBytes(
                    synthetic.as_bytes().to_vec().into(),
                )]),
                synthetics_aux.plutus_version,
            ),
            (
                underlying.hash.clone(),
                PlutusData::BoundedBytes(synthetics_aux.hash.clone().into()),
                underlying.plutus_version,
            ),
            (
                upgradable.hash.clone(),
                constr(0, vec![
                    constr(0, vec![PlutusData::BoundedBytes(
                        self.registry_utxo.0.transaction_id.as_ref().to_vec().into(),
                    )]),
                    PlutusData::BigInt(pallas_primitives::BigInt::Int(
                        (self.registry_utxo.0.index as i64).into(),
                    )),
                ]),
                upgradable.plutus_version,
            ),
        ];

        let params_utxo = parse_outref(&cfg.params_utxo)?;
        let mut ref_inputs = vec![
            params_utxo,
            self.registry_utxo.clone(),
            mint.ref_input.clone(),
            synthetics.ref_input.clone(),
            synthetics_aux.ref_input.clone(),
            underlying.ref_input.clone(),
            upgradable.ref_input.clone(),
        ];
        ref_inputs.dedup();

        Ok(DepositPieces {
            pot_output,
            mint_assets: vec![(synthetic.as_bytes().to_vec(), minted as i64), treas],
            mint_policy: mint.hash.clone(),
            mint_redeemer: PlutusData::BigInt(pallas_primitives::BigInt::Int(0.into())),
            withdrawals,
            ref_inputs,
        })
    }
}

/// Load the runtime from optional config, degrading to None (with a clear
/// warning) on any problem — a scooper with a broken butane section must
/// still scoop.
pub fn load_runtime(config: &Option<ButaneConfig>) -> Option<ButaneRuntime> {
    let config = config.as_ref()?;
    match ButaneRuntime::load(config) {
        Ok(rt) => {
            tracing::info!(
                synthetics = ?rt.synthetics.iter().map(|s| (&s.name, s.enabled)).collect::<Vec<_>>(),
                "butane integration loaded and verified",
            );
            Some(rt)
        }
        Err(e) => {
            tracing::warn!(
                "butane config present but unusable — integration disabled: {e:#}"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PREVIEW_ARTIFACT: &str =
        "/home/pi/Downloads/butane-v2.deployment.preview.json";

    fn preview_config() -> ButaneConfig {
        let hashes: &[(&str, &str)] = &[
            ("spend", "b132270a7949dc354385295c99e90483d95f393dc604e9383111ce79"),
            ("mint", "84de43f8ae128d33d1c0e04ace1b275f76e45e375d884d8fda36a5e2"),
            ("synthetics", "04156db9a24fefa2091ba48ab0cd64e3bf90f9c83028b9906e3d670b"),
            ("synthetics-aux", "c0fde399e58e6d422fab65aa7e66d2174026ee616cc511b17355d387"),
            ("external-underlying", "75a2e23edd8f7d1c55d62a72f7e326216e420100753da83231daf255"),
            ("upgradable", "8137010c1908095a4e484ed91b4f8ff222cb5968afc802258a24acde"),
        ];
        ButaneConfig {
            deployment_file: PREVIEW_ARTIFACT.into(),
            script_hashes: hashes
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            registry_utxo: format!("{}#0", "00".repeat(32)),
            registry_utxo_cbor: None,
            synthetics: vec![ButaneSyntheticConfig {
                name: "ADAb".into(),
                params_utxo: format!("{}#0", "11".repeat(32)),
                params_utxo_cbor: None,
                rate: (1, 1),
                max_input: None,
                enabled: true,
            }],
        }
    }

    /// Parses the real preview artifact and verifies every required
    /// validator's bytecode hashes to the values the Butane team published.
    /// Skips silently when the artifact isn't on this machine.
    #[test]
    fn preview_artifact_hashes_verify() {
        if !std::path::Path::new(PREVIEW_ARTIFACT).exists() {
            eprintln!("skipping: {PREVIEW_ARTIFACT} not present");
            return;
        }
        let rt = ButaneRuntime::load(&preview_config())
            .expect("artifact should load and verify");
        assert_eq!(rt.scripts.len(), REQUIRED_ROLES.len());
        // The mint policy drives the synthetic asset id.
        let adab = rt.synthetic_asset("ADAb");
        assert_eq!(hex::encode(&adab.policy), preview_config().script_hashes["mint"]);
        assert_eq!(adab.token, b"ADAb".to_vec());
        // Edges resolve for the enabled synthetic.
        let edges = rt.edges();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].key, "butane:ADAb:mint");
        assert_eq!(edges[0].fee_bps, 0);
    }

    /// A wrong hash must disable the integration loudly, not compose
    /// unvalidatable txs.
    #[test]
    fn hash_mismatch_fails_load() {
        if !std::path::Path::new(PREVIEW_ARTIFACT).exists() {
            eprintln!("skipping: {PREVIEW_ARTIFACT} not present");
            return;
        }
        let mut cfg = preview_config();
        cfg.script_hashes
            .insert("mint".into(), "ab".repeat(28));
        assert!(ButaneRuntime::load(&cfg).is_err());
    }

    /// Deposit pieces match the fixture dissection's tx anatomy.
    #[test]
    fn deposit_pieces_shape() {
        if !std::path::Path::new(PREVIEW_ARTIFACT).exists() {
            eprintln!("skipping: {PREVIEW_ARTIFACT} not present");
            return;
        }
        let rt = ButaneRuntime::load(&preview_config()).unwrap();
        let p = rt.deposit_pieces("ADAb", 40_000_000, 40_000_000, 0).unwrap();

        // Pot address: type-3 base (script/script), spend + mint creds.
        let conway::TransactionOutput::PostAlonzo(body) = &p.pot_output else {
            panic!("expected post-alonzo pot output");
        };
        let addr: Vec<u8> = body.address.clone().into();
        assert_eq!(addr[0], 0x30);
        assert_eq!(&addr[1..29], rt.script("spend").hash.as_slice());
        assert_eq!(&addr[29..57], rt.script("mint").hash.as_slice());

        // Mint: synthetic + treas under the mint policy, redeemer int 0.
        assert_eq!(p.mint_policy, rt.script("mint").hash);
        assert_eq!(p.mint_assets, vec![(b"ADAb".to_vec(), 40_000_000i64), (b"treas".to_vec(), 1)]);

        // Four withdrawals with the on-chain language mix (V3/V3/V3/V2).
        assert_eq!(p.withdrawals.len(), 4);
        let versions: Vec<u8> = p.withdrawals.iter().map(|(_, _, v)| *v).collect();
        assert_eq!(versions, vec![3, 3, 3, 2], "synthetics/aux/underlying V3, upgradable V2");

        // Ref inputs: params, registry, and 5 distinct ref-script UTxOs.
        assert_eq!(p.ref_inputs.len(), 7);
    }

    /// The committed preview config must round-trip through the loader:
    /// artifact hashes verify, registry + params CBOR decode. Catches config
    /// drift before a deploy does.
    #[test]
    fn preview_repo_config_loads() {
        let raw = std::fs::read_to_string("config/preview-v4.json")
            .expect("repo preview config");
        let cfg: serde_json::Value = serde_json::from_str(&raw).unwrap();
        fn find_butane(v: &serde_json::Value) -> Option<&serde_json::Value> {
            match v {
                serde_json::Value::Object(m) => m
                    .get("butane")
                    .or_else(|| m.values().find_map(find_butane)),
                _ => None,
            }
        }
        let butane = find_butane(&cfg).expect("butane section in preview config");
        let mut parsed: ButaneConfig =
            serde_json::from_value(butane.clone()).expect("butane config parses");
        // The artifact path in the config is relative to the deploy dir;
        // point it at the repo copy for the test.
        parsed.deployment_file = "config/butane-v2.deployment.preview.json".into();
        let rt = ButaneRuntime::load(&parsed).expect("runtime loads from repo config");
        assert_eq!(rt.edges().len(), 1, "ADAb edge enabled");
        // Registry + params resolved outputs both present (scripts' 5-6 +2).
        let resolved = rt.resolved_ref_outputs();
        assert!(
            resolved.len() >= REQUIRED_ROLES.len() + 2,
            "expected ref-script + registry + params outputs, got {}",
            resolved.len(),
        );
    }

    /// Absent config degrades to None without complaint.
    #[test]
    fn absent_config_degrades() {
        assert!(load_runtime(&None).is_none());
        let mut cfg = preview_config();
        cfg.deployment_file = "/nonexistent/path.json".into();
        assert!(load_runtime(&Some(cfg)).is_none());
    }
}
