//! Local UPLC evaluation using uplc-turbo.
//!
//! Evaluates each script in a scoop transaction to determine actual ExUnits
//! consumed, enabling realistic budgeting instead of over-provisioning.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use pallas_crypto::hash::Hasher;
use pallas_primitives::conway::{self, RedeemersKey, RedeemerTag, TransactionOutput};
use pallas_primitives::{ExUnits, Hash, PlutusData};
use tracing::warn;

use crate::cardano_types;
use crate::sundaev4::script_context::{
    self, Credential, DatumOption, OutputReference, ResolvedTxOut, ScriptPurpose,
};

/// Decoded FLAT script bytes keyed by script hash.
pub struct ScriptStore {
    scripts: BTreeMap<Hash<28>, Vec<u8>>,
}

impl ScriptStore {
    /// Build a ScriptStore from reference UTxO outputs.
    ///
    /// Extracts PlutusV3 scripts from `script_ref` fields, computes their
    /// script hashes, and CBOR-unwraps to get FLAT-encoded UPLC bytes.
    ///
    /// Pallas decodes the outer CBOR bytestring of `PlutusScript`, but Cardano
    /// scripts are double-wrapped: `bytes(bytes(FLAT))`. So `as_ref()` gives us
    /// the inner CBOR bytestring (used for hashing), and we unwrap once more to
    /// get the raw FLAT bytes for the evaluator.
    pub fn from_ref_utxos(
        ref_utxo_outputs: &BTreeMap<cardano_types::TransactionInput, cardano_types::TransactionOutput>,
    ) -> Result<Self> {
        let mut store = BTreeMap::new();
        for (_input, txo) in ref_utxo_outputs {
            if let Some(cardano_types::ScriptRef::PlutusV3(script)) = &txo.script_ref {
                let script_cbor: &[u8] = script.as_ref();
                // PlutusV3 script hash = blake2b_224(0x03 || script_cbor)
                let mut preimage = Vec::with_capacity(1 + script_cbor.len());
                preimage.push(0x03);
                preimage.extend_from_slice(script_cbor);
                let hash: Hash<28> = Hasher::<224>::hash(&preimage);
                // CBOR unwrap: handles both definite and indefinite-length bytestrings
                let flat_bytes = cbor_unwrap_bytes(script_cbor)
                    .with_context(|| format!("CBOR unwrap failed for script {}", hex::encode(hash)))?;
                store.insert(hash, flat_bytes);
            }
        }
        Ok(ScriptStore { scripts: store })
    }

    /// Build a ScriptStore from a Blueprint's compiled code.
    ///
    /// For each validator with `compiled_code`, hex-decodes the script CBOR,
    /// computes the PlutusV3 hash, and CBOR-unwraps to FLAT bytes.
    /// This bypasses the need to index reference UTxOs from the chain.
    /// Build from blueprint compiled code. Used by test harness.
    #[cfg(test)]
    pub fn from_blueprint(blueprint: &crate::blueprint::Blueprint) -> Result<Self> {
        let mut store = BTreeMap::new();
        for validator in &blueprint.validators {
            if let Some(code_hex) = &validator.compiled_code {
                let script_cbor = hex::decode(code_hex)
                    .with_context(|| format!("invalid hex in compiled_code for '{}'", validator.title))?;
                // PlutusV3 script hash = blake2b_224(0x03 || script_cbor)
                let mut preimage = Vec::with_capacity(1 + script_cbor.len());
                preimage.push(0x03);
                preimage.extend_from_slice(&script_cbor);
                let hash: Hash<28> = Hasher::<224>::hash(&preimage);
                // CBOR unwrap to get FLAT bytes
                let flat_bytes = cbor_unwrap_bytes(&script_cbor)
                    .with_context(|| format!("CBOR unwrap failed for '{}' ({})", validator.title, hex::encode(hash)))?;
                store.insert(hash, flat_bytes);
            }
        }
        Ok(ScriptStore { scripts: store })
    }

    pub fn get(&self, hash: &Hash<28>) -> Option<&[u8]> {
        self.scripts.get(hash).map(|v| v.as_slice())
    }
}

/// Unwrap a CBOR bytestring, handling both definite and indefinite-length encoding.
///
/// Large Cardano scripts are often encoded as indefinite-length (chunked) CBOR
/// bytestrings which `minicbor::decode::<Vec<u8>>` doesn't handle.
fn cbor_unwrap_bytes(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = minicbor::Decoder::new(data);
    match decoder.datatype()? {
        minicbor::data::Type::Bytes => Ok(decoder.bytes()?.to_vec()),
        minicbor::data::Type::BytesIndef => {
            let mut result = Vec::new();
            for chunk in decoder.bytes_iter()? {
                result.extend_from_slice(chunk?);
            }
            Ok(result)
        }
        t => bail!("expected CBOR bytes, got {:?}", t),
    }
}

/// Result of evaluating all scripts in a transaction.
pub struct EvalResult {
    pub budgets: Vec<(RedeemersKey, ExUnits)>,
}

/// Information about a script eval failure that the caller can persist to
/// disk later (after confirming the tx made it on chain) for offline
/// comparison against the chain's view of the same context. Populated by
/// `evaluate_scoop_tx` via its optional `failure_capture` out-param.
pub struct FailedScriptContext {
    pub script_hash: Hash<28>,
    pub redeemer_key: RedeemersKey,
    pub context_cbor: Vec<u8>,
    pub error: String,
}

/// Evaluate all scripts in a scoop transaction locally.
///
/// For each redeemer, builds the appropriate ScriptContext, looks up the script,
/// applies CIP-0069 convention (single argument for V3), and evaluates.
pub fn evaluate_scoop_tx(
    tx_body: &conway::PseudoTransactionBody<TransactionOutput>,
    redeemers: &[(RedeemersKey, PlutusData, ExUnits)],
    resolved_inputs: &BTreeMap<cardano_types::TransactionInput, ResolvedTxOut>,
    resolved_ref_inputs: &BTreeMap<cardano_types::TransactionInput, ResolvedTxOut>,
    scripts: &ScriptStore,
    cost_model: &[i64],
    tx_hash: Hash<32>,
    slot_config: &crate::sundaev4::types::SlotConfig,
    failure_capture: Option<&mut Option<FailedScriptContext>>,
) -> Result<EvalResult> {
    use uplc_turbo::arena::Arena;
    use uplc_turbo::binder::DeBruijn;
    use uplc_turbo::data::PlutusData as UplcPlutusData;
    use uplc_turbo::machine::{ExBudget, PlutusVersion};
    use uplc_turbo::term::Term;

    let redeemer_pairs: Vec<(RedeemersKey, PlutusData)> = redeemers
        .iter()
        .map(|(k, d, _)| (k.clone(), d.clone()))
        .collect();

    let mut budgets = Vec::new();

    for (key, redeemer_data, _ex_units) in redeemers {
        // Determine which script to run and build the ScriptPurpose
        let (script_hash, purpose) =
            resolve_script_and_purpose(key, tx_body, resolved_inputs)?;

        // Look up FLAT script bytes
        let flat_bytes = scripts
            .get(&script_hash)
            .with_context(|| format!("script {} not found in store", hex::encode(script_hash)))?;

        // Build ScriptContext CBOR
        let context_cbor = script_context::build_script_context(
            tx_body,
            &redeemer_pairs,
            resolved_inputs,
            resolved_ref_inputs,
            tx_hash,
            &purpose,
            redeemer_data,
            slot_config,
        );

        // Evaluate in a fresh arena (16MB stack not needed for single-threaded)
        let arena = Arena::new();

        // Decode FLAT script
        let program = uplc_turbo::flat::decode::<DeBruijn>(&arena, flat_bytes)
            .map_err(|e| anyhow::anyhow!("FLAT decode failed for {}: {e}", hex::encode(script_hash)))?;

        // Decode ScriptContext as uplc-turbo PlutusData
        let context_pd = UplcPlutusData::from_cbor(&arena, &context_cbor)
            .map_err(|e| anyhow::anyhow!("context CBOR decode failed: {e}"))?;

        // CIP-0069: V3 validators receive a single argument (the ScriptContext)
        let applied = program.apply(&arena, Term::data(&arena, context_pd));

        // Evaluate with cost model and maximum budget
        let budget = ExBudget {
            cpu: 10_000_000_000,
            mem: 14_000_000,
        };
        let result = applied.eval_with_params(&arena, PlutusVersion::V3, cost_model, budget);

        match result.term {
            Ok(_) => {
                let consumed = ExUnits {
                    mem: result.info.consumed_budget.mem.max(0) as u64,
                    steps: result.info.consumed_budget.cpu.max(0) as u64,
                };
                budgets.push((key.clone(), consumed));
            }
            Err(e) => {
                // Log any trace output
                warn!(script = %hex::encode(script_hash), n_logs = result.info.logs.len(), "script eval failed; emitting any traces");
                for log in &result.info.logs {
                    warn!(script = %hex::encode(script_hash), "trace: {log}");
                }
                let err_msg = format!(
                    "script {} ({:?}[{}]) evaluation failed: {e:?}",
                    hex::encode(script_hash),
                    key.tag,
                    key.index,
                );
                // Stash the context bytes for the caller to persist *only* if
                // the tx ends up on chain. Writing here unconditionally would
                // spam /tmp with binary-search candidates that never submit.
                if let Some(slot) = failure_capture {
                    *slot = Some(FailedScriptContext {
                        script_hash,
                        redeemer_key: key.clone(),
                        context_cbor: context_cbor.clone(),
                        error: err_msg.clone(),
                    });
                }
                bail!(err_msg);
            }
        }
    }

    Ok(EvalResult { budgets })
}

/// Determine which script hash to evaluate and build the ScriptPurpose.
fn resolve_script_and_purpose(
    key: &RedeemersKey,
    tx_body: &conway::PseudoTransactionBody<TransactionOutput>,
    resolved_inputs: &BTreeMap<cardano_types::TransactionInput, ResolvedTxOut>,
) -> Result<(Hash<28>, ScriptPurpose)> {
    match key.tag {
        RedeemerTag::Spend => {
            // Find the input at this sorted index
            let mut sorted_inputs: Vec<_> = tx_body.inputs.iter().cloned().collect();
            sorted_inputs.sort_by(|a, b| {
                a.transaction_id
                    .cmp(&b.transaction_id)
                    .then(a.index.cmp(&b.index))
            });
            let input = sorted_inputs
                .get(key.index as usize)
                .context("spend redeemer index out of bounds")?;

            let input_key =
                cardano_types::TransactionInput::new(input.transaction_id, input.index);
            let resolved = resolved_inputs
                .get(&input_key)
                .context("resolved input not found for spend")?;

            // Extract script hash from the address
            let script_hash = extract_payment_script_hash(&resolved.address)
                .context("spend input address is not a script address")?;

            // Extract datum for spending purpose
            let datum = match &resolved.datum {
                DatumOption::InlineDatum(d) => Some(d.clone()),
                DatumOption::DatumHash(_) => None, // would need to look up
                DatumOption::None => None,
            };

            let oref = OutputReference {
                tx_hash: input.transaction_id,
                index: input.index,
            };
            Ok((script_hash, ScriptPurpose::Spending(oref, datum)))
        }
        RedeemerTag::Mint => {
            let mint = tx_body
                .mint
                .as_ref()
                .context("mint redeemer but no mint in tx body")?;
            let mut policies: Vec<Hash<28>> = mint.iter().map(|(p, _)| *p).collect();
            policies.sort();
            let policy = policies
                .get(key.index as usize)
                .copied()
                .context("mint redeemer index out of bounds")?;
            Ok((policy, ScriptPurpose::Minting(policy)))
        }
        RedeemerTag::Reward => {
            // Find the withdrawal at this sorted index
            let withdrawals = tx_body
                .withdrawals
                .as_ref()
                .context("reward redeemer but no withdrawals")?;
            let sorted_accounts: Vec<_> = withdrawals.iter().map(|(a, _)| a.clone()).collect();
            let account = sorted_accounts
                .get(key.index as usize)
                .context("reward redeemer index out of bounds")?;

            if account.len() != 29 {
                bail!("invalid reward account length: {}", account.len());
            }
            let hash_bytes: [u8; 28] = account[1..29]
                .try_into()
                .map_err(|_| anyhow::anyhow!("reward account hash not 28 bytes"))?;
            let script_hash: Hash<28> = hash_bytes.into();

            let header = account[0];
            let cred = if header & 0x10 != 0 {
                Credential::Script(script_hash)
            } else {
                Credential::PubKey(script_hash)
            };

            Ok((script_hash, ScriptPurpose::Rewarding(cred)))
        }
        _ => bail!("unsupported redeemer tag {:?}", key.tag),
    }
}

/// Extract the script hash from a Shelley script address.
fn extract_payment_script_hash(address_bytes: &[u8]) -> Option<Hash<28>> {
    use pallas_addresses::Address;

    let addr = Address::from_bytes(address_bytes).ok()?;
    match addr {
        Address::Shelley(shelley) => match shelley.payment() {
            pallas_addresses::ShelleyPaymentPart::Script(h) => Some(*h),
            _ => None,
        },
        _ => None,
    }
}
