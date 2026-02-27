//! Builds a V4 scoop transaction using pallas 0.34 primitives.
//!
//! MVP: 1 constant-product pool, 1 swap order, devnet only.

use anyhow::{Context, Result, bail};
use pallas_addresses::{Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart};
use pallas_crypto::hash::Hasher;
use pallas_crypto::key::ed25519::SecretKey;
use pallas_codec::utils::CborWrap;
use pallas_primitives::conway::{
    self, RedeemersKey, RedeemersValue, Redeemers, RedeemerTag, TransactionOutput, WitnessSet,
    VKeyWitness,
};
use pallas_primitives::{ExUnits, Hash, PositiveCoin, TransactionInput};
use plutus_parser::AsPlutus;

use crate::bigint::BigInt;
use crate::cardano_types::AssetClass;
use crate::sundaev3::{Credential, PlutusAddress, Referenced};
use crate::sundaev4::swap_math;
use crate::sundaev4::types::*;

type PallasBytes = pallas_primitives::Bytes;
type ConwayValue = conway::Value;

use std::collections::BTreeMap;
use crate::sundaev4::script_context::{ResolvedTxOut, DatumOption};

/// Per-redeemer ExUnits budget (tx max / 10 so 6 redeemers fit comfortably).
const EX_MEM: u64 = 14_000_000 / 10;
const EX_STEPS: u64 = 10_000_000_000 / 10;
const TX_FEE: u64 = 2_000_000;
const FULFILLMENT_ADA: u64 = 2_000_000;
const POOL_MIN_ADA: u64 = 50_000_000;
const VALIDITY_RANGE: u64 = 60;

/// Result of building a scoop transaction, containing everything needed
/// for local evaluation and the final signed CBOR.
pub struct BuildResult {
    pub cbor: Vec<u8>,
    pub tx_hash: Hash<32>,
    pub tx_hash_hex: String,
    pub tx_body: conway::PseudoTransactionBody<TransactionOutput>,
    pub resolved_inputs: BTreeMap<crate::cardano_types::TransactionInput, ResolvedTxOut>,
    pub resolved_ref_inputs: BTreeMap<crate::cardano_types::TransactionInput, ResolvedTxOut>,
    pub redeemers: Vec<(RedeemersKey, pallas_primitives::PlutusData, ExUnits)>,
}

/// Build a signed scoop transaction for 1 pool + 1 order.
///
/// If `ex_units` is `Some`, uses those per-redeemer budgets. Otherwise uses generous defaults.
pub fn build_scoop_tx(
    pool: &SundaeV4Pool,
    order: &SundaeV4Order,
    settings: &SundaeV4Settings,
    exec: &ScooperExecution,
    current_slot: u64,
    language_views: &[u8],
    collateral_utxo: &TransactionInput,
    collateral_value: &crate::cardano_types::Value,
    ex_units: Option<&[(RedeemersKey, ExUnits)]>,
    ref_utxo_outputs: &BTreeMap<crate::cardano_types::TransactionInput, crate::cardano_types::TransactionOutput>,
) -> Result<BuildResult> {
    let sk = parse_secret_key(&exec.scooper_secret_key)?;
    let pk = sk.public_key();
    let pk_bytes: [u8; 32] = pk.as_ref().try_into().unwrap();
    let scooper_keyhash: Hash<28> = Hasher::<224>::hash(&pk_bytes);

    // ── Step 1: Compute swap ────────────────────────────────────────────────

    let (input_idx, output_idx) = detect_swap_direction(order, &pool.pool_datum)?;
    let reserve_in = &pool.pool_datum.assets[input_idx].1;
    let reserve_out = &pool.pool_datum.assets[output_idx].1;

    // dx = amount of offered asset in the order UTxO
    let offered_asset = &pool.pool_datum.assets[input_idx].0;
    let dx = order.value.get(offered_asset);
    if dx <= BigInt::from(0) {
        bail!("order has no amount of the offered asset");
    }

    let dy = swap_math::cp_swap_result(
        reserve_in,
        reserve_out,
        &dx,
        exec.fee.0,
        exec.fee.1,
    );

    let a0 = &pool.pool_datum.assets[0].1;
    let b0 = &pool.pool_datum.assets[1].1;
    let a1 = if input_idx == 0 { a0 + &dx } else { a0 - &dy };
    let b1 = if input_idx == 0 { b0 - &dy } else { b0 + &dx };
    let lp_before = &pool.pool_datum.total_lp;

    let fee_budget = swap_math::cp_fee_budget(a0, b0, &a1, &b1, lp_before);
    let protocol_lp = swap_math::compute_protocol_lp(
        &fee_budget,
        exec.protocol_share.0,
        exec.protocol_share.1,
    );

    // ── Step 2: Build updated PoolDatum ─────────────────────────────────────

    let new_assets = pool
        .pool_datum
        .assets
        .iter()
        .enumerate()
        .map(|(idx, (ac, _))| {
            let new_amt = if idx == input_idx {
                &pool.pool_datum.assets[idx].1 + &dx
            } else if idx == output_idx {
                &pool.pool_datum.assets[idx].1 - &dy
            } else {
                pool.pool_datum.assets[idx].1.clone()
            };
            (ac.clone(), new_amt)
        })
        .collect::<Vec<_>>();

    let new_total_lp = lp_before + &protocol_lp;

    let updated_pool_datum = PoolDatum {
        assets: new_assets.clone(),
        total_lp: new_total_lp.clone(),
        circulating_lp: pool.pool_datum.circulating_lp.clone(),
        preminted_lp: pool.pool_datum.preminted_lp.clone(),
        identifier: pool.pool_datum.identifier.clone(),
        actions: pool.pool_datum.actions.clone(),
        module_state: pool.pool_datum.module_state.clone(),
    };

    // ── Step 3: Determine canonical input order ─────────────────────────────

    let pool_oref = &pool.input.0;
    let order_oref = &order.input.0;
    let mut sorted_inputs = vec![pool_oref.clone(), order_oref.clone()];
    sorted_inputs.sort_by(|a, b| {
        a.transaction_id
            .cmp(&b.transaction_id)
            .then(a.index.cmp(&b.index))
    });

    let pool_sorted_idx = sorted_inputs
        .iter()
        .position(|i| i == pool_oref)
        .unwrap();
    let order_sorted_idx = sorted_inputs
        .iter()
        .position(|i| i == order_oref)
        .unwrap();

    // ── Step 4: Build redeemers ─────────────────────────────────────────────

    let void_vault_state = VaultState {
        assets: vec![],
        total_lp: BigInt::from(0),
        circulating_lp: BigInt::from(0),
        preminted_lp: BigInt::from(0),
    };

    let state_after = VaultState {
        assets: new_assets.clone(),
        total_lp: new_total_lp.clone(),
        circulating_lp: pool.pool_datum.circulating_lp.clone(),
        preminted_lp: pool.pool_datum.preminted_lp.clone(),
    };

    let transcript_entry = TranscriptEntry {
        state_after,
        fee_budget: &fee_budget - &protocol_lp,
        operation_tag: BigInt::from(100),
        operation_data: void_vault_state.to_plutus(),
    };

    let vault_redeemer = VaultRedeemer::Action {
        tag: BigInt::from(100),
        transcript: vec![transcript_entry],
        pool_input_index: BigInt::from(pool_sorted_idx as u64),
        pool_output_index: BigInt::from(0u64),
    };

    let order_redeemer = OrderRedeemer::Scoop {
        own_input_index: order_sorted_idx as u64,
    };

    // Withdrawal redeemers
    let pool_oref_plutus = OutputRef {
        transaction_id: pool_oref.transaction_id.to_vec(),
        output_index: pool_oref.index,
    };

    // input_index is the index into the *filtered* list of order-script inputs
    // (not the full sorted tx inputs). With 1 order, this is always 0.
    let order_filtered_idx = 0u64;
    let order_validator_redeemer = OrderValidatorRedeemer {
        entries: vec![OrderValidatorEntry {
            input_index: order_filtered_idx,
            output_index: 1, // pool output is 0, fulfillment is 1
        }],
    };

    let cp_redeemer = ConstantProductRedeemer::Operate {
        entries: vec![CPOperateEntry {
            vault_oref: pool_oref_plutus.clone(),
            config: ConstantProductConfig {
                fee: Rational {
                    num: BigInt::from(exec.fee.0),
                    den: BigInt::from(exec.fee.1),
                },
            },
        }],
    };

    let fs_redeemer = FeeSplitRedeemer::Operate {
        entries: vec![FSOperateEntry {
            vault_oref: pool_oref_plutus,
            config: FeeSplitConfig {
                protocol_share: Rational {
                    num: BigInt::from(exec.protocol_share.0),
                    den: BigInt::from(exec.protocol_share.1),
                },
            },
        }],
    };

    let fairness_redeemer = FairnessRedeemer::Operate {
        entries: vec![FairnessOperateEntry {
            pool_ident: pool.pool_datum.identifier.clone(),
            scooper: scooper_keyhash.to_vec(),
        }],
    };

    // ── Step 5: Build reference inputs ──────────────────────────────────────

    let ref_inputs: Vec<TransactionInput> = [
        &exec.module_scripts.vault,
        &exec.module_scripts.order,
        &exec.module_scripts.constant_product,
        &exec.module_scripts.fee_split,
        &exec.module_scripts.fairness,
        &exec.module_scripts.pool_mint,
        &exec.module_scripts.settings,
    ]
    .iter()
    .map(|s| s.ref_utxo.0.clone())
    .collect();

    // Also include the settings UTxO as a reference input
    let mut all_ref_inputs = ref_inputs;
    all_ref_inputs.push(settings.input.0.clone());

    // ── Step 6: Build withdrawal map ────────────────────────────────────────
    // Withdrawals keyed by reward account (e_network + script_hash)
    // For V3 scripts on testnet: reward account = 0xf0 + script_hash

    fn reward_account(script_hash: &Hash<28>) -> PallasBytes {
        let mut account = vec![0xf0u8]; // testnet script reward
        account.extend_from_slice(script_hash.as_ref());
        PallasBytes::from(account)
    }

    let withdrawals = vec![
        (
            reward_account(&exec.module_scripts.order.hash),
            order_validator_redeemer.to_plutus(),
        ),
        (
            reward_account(&exec.module_scripts.constant_product.hash),
            cp_redeemer.to_plutus(),
        ),
        (
            reward_account(&exec.module_scripts.fee_split.hash),
            fs_redeemer.to_plutus(),
        ),
        (
            reward_account(&exec.module_scripts.fairness.hash),
            fairness_redeemer.to_plutus(),
        ),
    ];

    // Sort withdrawals by reward account for canonical ordering
    let mut sorted_withdrawals = withdrawals;
    sorted_withdrawals.sort_by(|(a, _), (b, _)| a.cmp(b));

    // ── Step 7: Build outputs ───────────────────────────────────────────────

    // Pool output: same address, updated datum, adjusted value
    let pool_address = match &pool.input.0.transaction_id {
        _ => {
            // Reconstruct vault address from script hash
            let vault_addr = ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Script(exec.module_scripts.vault.hash),
                ShelleyDelegationPart::Null,
            );
            PallasBytes::from(vault_addr.to_vec())
        }
    };

    let pool_datum_pd = updated_pool_datum.to_plutus();

    let pool_output_value = build_pool_output_value(pool, &new_assets)?;
    let pool_output = TransactionOutput::PostAlonzo(
        pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
            address: pool_address.clone(),
            value: pool_output_value,
            datum_option: Some(conway::PseudoDatumOption::Data(CborWrap(pool_datum_pd))),
            script_ref: None,
        },
    );

    // Fulfillment output: send dy of output asset to destination.
    // The ADA for the fulfillment comes from the order's ADA minus the tx fee.
    let dest_address = resolve_destination(&order.datum.destination, &order.datum.owner)?;
    let output_asset = &pool.pool_datum.assets[output_idx].0;
    let ada_asset = AssetClass { policy: vec![], token: vec![] };
    let order_ada = {
        use num_traits::ToPrimitive;
        order.value.get(&ada_asset).clone().unwrap().to_u64().unwrap_or(0)
    };
    let fulfillment_ada = order_ada.saturating_sub(TX_FEE);
    let fulfillment_value = build_fulfillment_value(output_asset, &dy, fulfillment_ada)?;
    let fulfillment_output = TransactionOutput::PostAlonzo(
        pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
            address: PallasBytes::from(dest_address),
            value: fulfillment_value,
            datum_option: None,
            script_ref: None,
        },
    );

    let outputs = vec![pool_output, fulfillment_output];

    // ── Step 8: Build redeemer map ──────────────────────────────────────────

    // Build withdrawal index map: sorted position in withdrawals
    let sorted_withdrawal_accounts: Vec<PallasBytes> =
        sorted_withdrawals.iter().map(|(a, _)| a.clone()).collect();

    fn withdrawal_index(accounts: &[PallasBytes], account: &PallasBytes) -> u32 {
        accounts.iter().position(|a| a == account).unwrap() as u32
    }

    let order_wd_account = reward_account(&exec.module_scripts.order.hash);
    let cp_wd_account = reward_account(&exec.module_scripts.constant_product.hash);
    let fs_wd_account = reward_account(&exec.module_scripts.fee_split.hash);
    let fair_wd_account = reward_account(&exec.module_scripts.fairness.hash);

    // Helper: look up ExUnits from provided map or use defaults
    let lookup_eu = |key: &RedeemersKey| -> ExUnits {
        ex_units
            .and_then(|eus| eus.iter().find(|(k, _)| k == key).map(|(_, eu)| eu.clone()))
            .unwrap_or(ExUnits { mem: EX_MEM, steps: EX_STEPS })
    };

    let vault_key = RedeemersKey { tag: RedeemerTag::Spend, index: pool_sorted_idx as u32 };
    let order_key = RedeemersKey { tag: RedeemerTag::Spend, index: order_sorted_idx as u32 };
    let order_wd_key = RedeemersKey { tag: RedeemerTag::Reward, index: withdrawal_index(&sorted_withdrawal_accounts, &order_wd_account) };
    let cp_wd_key = RedeemersKey { tag: RedeemerTag::Reward, index: withdrawal_index(&sorted_withdrawal_accounts, &cp_wd_account) };
    let fs_wd_key = RedeemersKey { tag: RedeemerTag::Reward, index: withdrawal_index(&sorted_withdrawal_accounts, &fs_wd_account) };
    let fair_wd_key = RedeemersKey { tag: RedeemerTag::Reward, index: withdrawal_index(&sorted_withdrawal_accounts, &fair_wd_account) };

    let vault_redeemer_pd = vault_redeemer.to_plutus();
    let order_redeemer_pd = order_redeemer.to_plutus();
    let order_wd_data = sorted_withdrawals.iter().find(|(a, _)| *a == order_wd_account).unwrap().1.clone();
    let cp_wd_data = sorted_withdrawals.iter().find(|(a, _)| *a == cp_wd_account).unwrap().1.clone();
    let fs_wd_data = sorted_withdrawals.iter().find(|(a, _)| *a == fs_wd_account).unwrap().1.clone();
    let fair_wd_data = sorted_withdrawals.iter().find(|(a, _)| *a == fair_wd_account).unwrap().1.clone();

    // Collect redeemer info for BuildResult
    let redeemer_info: Vec<(RedeemersKey, pallas_primitives::PlutusData, ExUnits)> = vec![
        (vault_key.clone(), vault_redeemer_pd.clone(), lookup_eu(&vault_key)),
        (order_key.clone(), order_redeemer_pd.clone(), lookup_eu(&order_key)),
        (order_wd_key.clone(), order_wd_data.clone(), lookup_eu(&order_wd_key)),
        (cp_wd_key.clone(), cp_wd_data.clone(), lookup_eu(&cp_wd_key)),
        (fs_wd_key.clone(), fs_wd_data.clone(), lookup_eu(&fs_wd_key)),
        (fair_wd_key.clone(), fair_wd_data.clone(), lookup_eu(&fair_wd_key)),
    ];

    let redeemer_pairs: Vec<(RedeemersKey, RedeemersValue)> = redeemer_info
        .iter()
        .map(|(key, data, eu)| (key.clone(), RedeemersValue { data: data.clone(), ex_units: eu.clone() }))
        .collect();

    let redeemers =
        Redeemers::Map(pallas_primitives::NonEmptyKeyValuePairs::Def(redeemer_pairs));

    // ── Step 9: Compute script_data_hash ────────────────────────────────────

    let redeemers_cbor = minicbor::to_vec(&redeemers).context("encode redeemers")?;
    // No datums in witness set → omit datums from hash entirely (per Alonzo spec)
    let mut hasher = Hasher::<256>::new();
    hasher.input(&redeemers_cbor);
    hasher.input(language_views);
    let script_data_hash: Hash<32> = hasher.finalize();

    // ── Step 10: Assemble TransactionBody ───────────────────────────────────

    let body = conway::PseudoTransactionBody {
        inputs: sorted_inputs.into(),
        outputs,
        fee: TX_FEE,
        ttl: Some(current_slot + VALIDITY_RANGE),
        certificates: None,
        withdrawals: Some(pallas_primitives::NonEmptyKeyValuePairs::Def(
            sorted_withdrawals
                .into_iter()
                .map(|(account, _)| (account, 0u64))
                .collect(),
        )),
        auxiliary_data_hash: None,
        validity_interval_start: Some(current_slot),
        mint: None,
        script_data_hash: Some(script_data_hash),
        collateral: pallas_primitives::NonEmptySet::from_vec(vec![collateral_utxo.clone()]),
        required_signers: Some(
            pallas_primitives::NonEmptySet::from_vec(vec![scooper_keyhash]).unwrap(),
        ),
        network_id: None,
        collateral_return: Some(TransactionOutput::PostAlonzo(
            pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
                address: {
                    let scooper_addr = ShelleyAddress::new(
                        Network::Testnet,
                        ShelleyPaymentPart::Key(scooper_keyhash),
                        ShelleyDelegationPart::Null,
                    );
                    PallasBytes::from(scooper_addr.to_vec())
                },
                value: build_collateral_return_value(collateral_value, TX_FEE * 3 / 2)?,
                datum_option: None,
                script_ref: None,
            },
        )),
        total_collateral: Some(TX_FEE * 3 / 2), // 150% of fee
        reference_inputs: pallas_primitives::NonEmptySet::from_vec(all_ref_inputs.clone()),
        voting_procedures: None,
        proposal_procedures: None,
        treasury_value: None,
        donation: None,
    };

    // ── Step 11: Sign ───────────────────────────────────────────────────────

    let body_cbor = minicbor::to_vec(&body).context("encode tx body")?;
    let body_hash: Hash<32> = Hasher::<256>::hash(&body_cbor);
    let signature = sk.sign(body_hash);

    let witness_set = WitnessSet {
        vkeywitness: Some(
            pallas_primitives::NonEmptySet::from_vec(vec![VKeyWitness {
                vkey: PallasBytes::from(pk_bytes.to_vec()),
                signature: PallasBytes::from(signature.as_ref().to_vec()),
            }])
            .unwrap(),
        ),
        native_script: None,
        bootstrap_witness: None,
        plutus_v1_script: None,
        plutus_data: None,
        redeemer: Some(redeemers),
        plutus_v2_script: None,
        plutus_v3_script: None,
    };

    let tx_hash_hex = hex::encode(body_hash);

    // Build resolved inputs for the evaluator (before body is consumed)
    let mut resolved_inputs = BTreeMap::new();
    // Pool input
    resolved_inputs.insert(pool.input.clone(), ResolvedTxOut {
        address: pool_address.to_vec(),
        value: pool.value.clone(),
        datum: DatumOption::InlineDatum(pool.pool_datum.clone().to_plutus()),
        script_ref: None,
    });
    // Order input
    resolved_inputs.insert(order.input.clone(), ResolvedTxOut {
        address: {
            // Reconstruct order script address
            let order_addr = ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Script(exec.module_scripts.order.hash),
                ShelleyDelegationPart::Null,
            );
            order_addr.to_vec()
        },
        value: order.value.clone(),
        datum: DatumOption::InlineDatum(order.datum.clone().to_plutus()),
        script_ref: None,
    });

    // Build resolved reference inputs
    let mut resolved_ref_inputs = BTreeMap::new();
    for ref_input_key in all_ref_inputs.iter() {
        let ref_input_ct = crate::cardano_types::TransactionInput(ref_input_key.clone());
        if let Some(txo) = ref_utxo_outputs.get(&ref_input_ct) {
            resolved_ref_inputs.insert(ref_input_ct, ResolvedTxOut {
                address: txo.address.to_vec(),
                value: txo.value.clone(),
                datum: match &txo.datum {
                    crate::cardano_types::RawDatum::None => DatumOption::None,
                    crate::cardano_types::RawDatum::Inline(d) => DatumOption::InlineDatum(d.clone()),
                    crate::cardano_types::RawDatum::Hash(h) => DatumOption::DatumHash(*h),
                },
                script_ref: None,
            });
        }
    }
    // Settings UTxO as a reference input (last in all_ref_inputs)
    resolved_ref_inputs.insert(settings.input.clone(), ResolvedTxOut {
        address: {
            let settings_addr = ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Script(exec.module_scripts.settings.hash),
                ShelleyDelegationPart::Null,
            );
            settings_addr.to_vec()
        },
        value: crate::cardano_types::Value::default(),
        datum: DatumOption::InlineDatum(settings.datum.clone().to_plutus()),
        script_ref: None,
    });

    let tx = conway::PseudoTx {
        transaction_body: body,
        transaction_witness_set: witness_set,
        success: true,
        auxiliary_data: pallas_primitives::Nullable::<conway::AuxiliaryData>::Null,
    };

    let tx_cbor = minicbor::to_vec(&tx).context("encode tx")?;

    Ok(BuildResult {
        cbor: tx_cbor,
        tx_hash: body_hash,
        tx_hash_hex,
        tx_body: tx.transaction_body,
        resolved_inputs,
        resolved_ref_inputs,
        redeemers: redeemer_info,
    })
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn parse_secret_key(hex_str: &str) -> Result<SecretKey> {
    let bytes = hex::decode(hex_str).context("invalid secret key hex")?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("secret key must be 32 bytes"))?;
    Ok(SecretKey::from(arr))
}

/// Detect which pool asset the order is offering by checking the order UTxO value.
fn detect_swap_direction(
    order: &SundaeV4Order,
    pool_datum: &PoolDatum,
) -> Result<(usize, usize)> {
    for (idx, (asset, _)) in pool_datum.assets.iter().enumerate() {
        // Skip lovelace — it's always present for min UTxO
        if asset.policy.is_empty() && asset.token.is_empty() {
            continue;
        }
        let amount = order.value.get(asset);
        if amount > BigInt::from(0) {
            let output_idx = if idx == 0 { 1 } else { 0 };
            return Ok((idx, output_idx));
        }
    }
    // Fallback: if the order has only ADA beyond min-utxo, it's offering ADA (asset 0)
    let ada = AssetClass {
        policy: vec![],
        token: vec![],
    };
    let ada_amount = order.value.get(&ada);
    if ada_amount > BigInt::from(FULFILLMENT_ADA as i64) {
        return Ok((0, 1));
    }
    bail!("cannot determine swap direction from order value")
}

/// Build the pool output Value (pallas conway::Value) from the pool's existing
/// value with asset amounts adjusted to reflect the swap.
fn build_pool_output_value(
    pool: &SundaeV4Pool,
    new_assets: &[(AssetClass, BigInt)],
) -> Result<ConwayValue> {
    use num_traits::ToPrimitive;
    use pallas_primitives::NonEmptyKeyValuePairs;

    // Start from ADA
    let ada_asset = AssetClass {
        policy: vec![],
        token: vec![],
    };
    let ada_amount = pool.value.get(&ada_asset);
    let lovelace = ada_amount
        .clone()
        .unwrap()
        .to_u64()
        .unwrap_or(POOL_MIN_ADA)
        .max(POOL_MIN_ADA);

    // Collect all native tokens from the pool's current value, then override
    // the pool asset amounts with new values
    let mut policy_map: std::collections::BTreeMap<
        Vec<u8>,
        std::collections::BTreeMap<Vec<u8>, u64>,
    > = std::collections::BTreeMap::new();

    // Copy all existing native tokens from the pool
    for (policy_bytes, tokens) in &pool.value.0 {
        if policy_bytes.is_empty() {
            continue; // skip ADA
        }
        for (token_bytes, qty) in tokens {
            use num_traits::ToPrimitive;
            let amt = qty.clone().unwrap().to_u64().unwrap_or(0);
            if amt > 0 {
                policy_map
                    .entry(policy_bytes.clone())
                    .or_default()
                    .insert(token_bytes.clone(), amt);
            }
        }
    }

    // Override the pool's tracked asset amounts
    for (asset, amount) in new_assets {
        if asset.policy.is_empty() && asset.token.is_empty() {
            continue; // ADA handled separately
        }
        use num_traits::ToPrimitive;
        let amt = amount.clone().unwrap().to_u64().unwrap_or(0);
        if amt > 0 {
            policy_map
                .entry(asset.policy.clone())
                .or_default()
                .insert(asset.token.clone(), amt);
        }
    }

    if policy_map.is_empty() {
        return Ok(ConwayValue::Coin(lovelace));
    }

    let multiasset_pairs: Vec<(Hash<28>, NonEmptyKeyValuePairs<PallasBytes, PositiveCoin>)> =
        policy_map
            .into_iter()
            .filter_map(|(policy, tokens)| {
                let policy_hash: Hash<28> = Hash::from(policy.as_slice());
                let token_pairs: Vec<(PallasBytes, PositiveCoin)> = tokens
                    .into_iter()
                    .filter_map(|(name, qty)| {
                        PositiveCoin::try_from(qty)
                            .ok()
                            .map(|pc| (PallasBytes::from(name), pc))
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
        return Ok(ConwayValue::Coin(lovelace));
    }

    Ok(ConwayValue::Multiasset(
        lovelace,
        NonEmptyKeyValuePairs::Def(multiasset_pairs),
    ))
}

/// Build fulfillment output value: ADA + dy of the output asset.
fn build_fulfillment_value(output_asset: &AssetClass, dy: &BigInt, ada: u64) -> Result<ConwayValue> {
    use num_traits::ToPrimitive;
    use pallas_primitives::NonEmptyKeyValuePairs;

    let dy_u64 = dy
        .clone()
        .unwrap()
        .to_u64()
        .context("dy doesn't fit in u64")?;

    if output_asset.policy.is_empty() && output_asset.token.is_empty() {
        // Output is ADA — add dy to the fulfillment ADA
        return Ok(ConwayValue::Coin(ada + dy_u64));
    }

    let policy_hash: Hash<28> = Hash::from(output_asset.policy.as_slice());
    let positive_dy = PositiveCoin::try_from(dy_u64)
        .map_err(|_| anyhow::anyhow!("dy is zero"))?;

    let token_pairs = NonEmptyKeyValuePairs::Def(vec![(
        PallasBytes::from(output_asset.token.clone()),
        positive_dy,
    )]);

    Ok(ConwayValue::Multiasset(
        ada,
        NonEmptyKeyValuePairs::Def(vec![(policy_hash, token_pairs)]),
    ))
}

/// Resolve the order destination to a raw address byte vector.
fn resolve_destination(dest: &Destination, owner: &crate::multisig::Multisig) -> Result<Vec<u8>> {
    match dest {
        Destination::Fixed(plutus_addr, _datum) => plutus_address_to_bytes(plutus_addr),
        Destination::SelfDestination => {
            // SelfDestination: send back to the owner's key hash
            match owner {
                crate::multisig::Multisig::Signature(keyhash_bytes) => {
                    let hash: Hash<28> = keyhash_bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| anyhow::anyhow!("owner keyhash not 28 bytes"))?;
                    let addr = ShelleyAddress::new(
                        Network::Testnet,
                        ShelleyPaymentPart::Key(hash),
                        ShelleyDelegationPart::Null,
                    );
                    Ok(addr.to_vec())
                }
                _ => bail!("SelfDestination with non-Signature owner not supported"),
            }
        }
    }
}

/// Build the collateral return output value, subtracting total_collateral
/// from the ADA portion. Native assets are passed through unchanged.
fn build_collateral_return_value(
    value: &crate::cardano_types::Value,
    total_collateral: u64,
) -> Result<ConwayValue> {
    use num_traits::ToPrimitive;
    use pallas_primitives::NonEmptyKeyValuePairs;

    let ada_asset = crate::cardano_types::AssetClass { policy: vec![], token: vec![] };
    let lovelace = value
        .get(&ada_asset)
        .clone()
        .unwrap()
        .to_u64()
        .context("ada amount doesn't fit u64")?;
    let return_lovelace = lovelace
        .checked_sub(total_collateral)
        .context("collateral UTxO doesn't have enough ADA")?;

    let mut policy_pairs: Vec<(Hash<28>, NonEmptyKeyValuePairs<PallasBytes, PositiveCoin>)> =
        Vec::new();

    for (policy_bytes, tokens) in &value.0 {
        if policy_bytes.is_empty() {
            continue; // skip ADA
        }
        let policy_hash: Hash<28> = Hash::from(policy_bytes.as_slice());
        let mut token_pairs: Vec<(PallasBytes, PositiveCoin)> = Vec::new();
        for (name_bytes, qty) in tokens {
            let amount = qty.clone().unwrap().to_u64().unwrap_or(0);
            if let Ok(pc) = PositiveCoin::try_from(amount) {
                token_pairs.push((PallasBytes::from(name_bytes.clone()), pc));
            }
        }
        if !token_pairs.is_empty() {
            policy_pairs.push((policy_hash, NonEmptyKeyValuePairs::Def(token_pairs)));
        }
    }

    if policy_pairs.is_empty() {
        Ok(ConwayValue::Coin(return_lovelace))
    } else {
        Ok(ConwayValue::Multiasset(
            return_lovelace,
            NonEmptyKeyValuePairs::Def(policy_pairs),
        ))
    }
}

/// Convert a PlutusAddress to raw address bytes.
fn plutus_address_to_bytes(addr: &PlutusAddress) -> Result<Vec<u8>> {
    let payment = match &addr.payment_credential {
        Credential::VerificationKey(hash) => ShelleyPaymentPart::Key(*hash),
        Credential::Script(hash) => ShelleyPaymentPart::Script(*hash),
    };

    let delegation = match &addr.stake_credential {
        Some(Referenced::Inline(cred)) => match cred {
            Credential::VerificationKey(hash) => ShelleyDelegationPart::Key(*hash),
            Credential::Script(hash) => ShelleyDelegationPart::Script(*hash),
        },
        _ => ShelleyDelegationPart::Null,
    };

    let shelley = ShelleyAddress::new(Network::Testnet, payment, delegation);
    Ok(shelley.to_vec())
}
