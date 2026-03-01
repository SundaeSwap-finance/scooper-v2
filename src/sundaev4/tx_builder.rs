//! Builds a V4 scoop transaction using pallas 0.34 primitives.

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
use crate::sundaev4::batch::Batch;
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
const POOL_MIN_ADA: u64 = 50_000_000;
const VALIDITY_RANGE: u64 = 60;

/// Result of building a batch scoop transaction, with predicted pool UTxO for chaining.
pub struct BatchBuildResult {
    pub cbor: Vec<u8>,
    pub tx_hash: Hash<32>,
    pub tx_hash_hex: String,
    pub tx_body: conway::PseudoTransactionBody<TransactionOutput>,
    pub resolved_inputs: BTreeMap<crate::cardano_types::TransactionInput, ResolvedTxOut>,
    pub resolved_ref_inputs: BTreeMap<crate::cardano_types::TransactionInput, ResolvedTxOut>,
    pub redeemers: Vec<(RedeemersKey, pallas_primitives::PlutusData, ExUnits)>,
    /// Predicted pool UTxO after this tx settles: (input_ref, updated_pool)
    pub predicted_pool: (crate::cardano_types::TransactionInput, SundaeV4Pool),
    /// The TTL used for this transaction
    pub ttl: u64,
}

/// Build a signed scoop transaction for 1 pool + N orders (a Batch).
///
/// Handles multiple inputs, N fulfillment outputs, cumulative transcript
/// entries, and per-order redeemers. Returns a `BatchBuildResult` with the
/// predicted pool UTxO for transaction chaining.
pub fn build_batch_scoop_tx(
    batch: &Batch,
    settings: &SundaeV4Settings,
    exec: &ScooperExecution,
    current_slot: u64,
    language_views: &[u8],
    collateral_utxo: &TransactionInput,
    collateral_value: &crate::cardano_types::Value,
    ex_units: Option<&[(RedeemersKey, ExUnits)]>,
    ref_utxo_outputs: &BTreeMap<crate::cardano_types::TransactionInput, crate::cardano_types::TransactionOutput>,
) -> Result<BatchBuildResult> {
    let pool = &batch.pool;
    let swaps = &batch.swaps;
    let n_orders = swaps.len();
    if n_orders == 0 {
        bail!("batch has no swaps");
    }

    let sk = parse_secret_key(&exec.scooper_secret_key)?;
    let pk = sk.public_key();
    let pk_bytes: [u8; 32] = pk.as_ref().try_into().unwrap();
    let scooper_keyhash: Hash<28> = Hasher::<224>::hash(&pk_bytes);

    // ── Step 1: Build per-order transcript entries ─────────────────────────
    //
    // Each order gets its own transcript entry (matching the reference impl).
    // - state_after.assets: cumulative reserves after this order
    // - state_after.total_lp: unchanged from initial (except last entry)
    // - fee_budget: per-order CP headroom from intermediate reserves
    //
    // Protocol LP is computed once from the total fee budget across all
    // orders and applied only to the last entry.

    let initial_total_lp = pool.pool_datum.total_lp.clone();
    let mut running_assets = pool.pool_datum.assets.clone();
    let mut total_fee_budget = BigInt::from(0);

    let void_vault_state = VaultState {
        assets: vec![],
        total_lp: BigInt::from(0),
        circulating_lp: BigInt::from(0),
        preminted_lp: BigInt::from(0),
    };

    let mut transcript_entries: Vec<TranscriptEntry> = Vec::new();

    for swap in swaps {
        let prev_a = running_assets[0].1.clone();
        let prev_b = running_assets[1].1.clone();

        running_assets[swap.input_idx].1 = &running_assets[swap.input_idx].1 + &swap.dx;
        running_assets[swap.output_idx].1 = &running_assets[swap.output_idx].1 - &swap.dy;

        let fee_budget = swap_math::cp_fee_budget(
            &prev_a, &prev_b,
            &running_assets[0].1, &running_assets[1].1,
            &initial_total_lp,
        );
        total_fee_budget = &total_fee_budget + &fee_budget;

        transcript_entries.push(TranscriptEntry {
            state_after: VaultState {
                assets: running_assets.clone(),
                total_lp: initial_total_lp.clone(),
                circulating_lp: pool.pool_datum.circulating_lp.clone(),
                preminted_lp: pool.pool_datum.preminted_lp.clone(),
            },
            fee_budget,
            operation_tag: BigInt::from(100),
            operation_data: void_vault_state.clone().to_plutus(),
        });
    }

    // Apply protocol LP to last entry only
    let protocol_lp = swap_math::compute_protocol_lp(
        &total_fee_budget, exec.protocol_share.0, exec.protocol_share.1,
    );
    let final_total_lp = &initial_total_lp + &protocol_lp;

    if let Some(last) = transcript_entries.last_mut() {
        last.fee_budget = &last.fee_budget - &protocol_lp;
        last.state_after.total_lp = final_total_lp.clone();
    }

    // ── Step 2: Build updated PoolDatum ─────────────────────────────────────

    let updated_pool_datum = PoolDatum {
        assets: batch.final_assets.clone(),
        total_lp: final_total_lp.clone(),
        circulating_lp: pool.pool_datum.circulating_lp.clone(),
        preminted_lp: pool.pool_datum.preminted_lp.clone(),
        identifier: pool.pool_datum.identifier.clone(),
        actions: pool.pool_datum.actions.clone(),
        module_state: pool.pool_datum.module_state.clone(),
    };

    // ── Step 3: Determine canonical input order ─────────────────────────────

    let pool_oref = &pool.input.0;
    let mut all_input_orefs: Vec<TransactionInput> = vec![pool_oref.clone()];
    for swap in swaps {
        all_input_orefs.push(swap.order.input.0.clone());
    }

    let mut sorted_inputs = all_input_orefs.clone();
    sorted_inputs.sort_by(|a, b| {
        a.transaction_id
            .cmp(&b.transaction_id)
            .then(a.index.cmp(&b.index))
    });

    let pool_sorted_idx = sorted_inputs
        .iter()
        .position(|i| i == pool_oref)
        .unwrap();

    // Each order's position in sorted inputs
    let order_sorted_indices: Vec<usize> = swaps
        .iter()
        .map(|swap| {
            sorted_inputs
                .iter()
                .position(|i| i == &swap.order.input.0)
                .unwrap()
        })
        .collect();

    // Filtered order indices: position among order-script inputs only
    // (excluding pool input). The on-chain OrderValidator expects these.
    let mut order_orefs_sorted: Vec<TransactionInput> = swaps
        .iter()
        .map(|s| s.order.input.0.clone())
        .collect();
    order_orefs_sorted.sort_by(|a, b| {
        a.transaction_id.cmp(&b.transaction_id).then(a.index.cmp(&b.index))
    });

    // For each swap (in batch order), find its position in the filtered
    // order-only sorted list
    let order_filtered_indices: Vec<u64> = swaps
        .iter()
        .map(|swap| {
            order_orefs_sorted
                .iter()
                .position(|o| o == &swap.order.input.0)
                .unwrap() as u64
        })
        .collect();

    // ── Step 4: Build redeemers ─────────────────────────────────────────────

    let vault_redeemer = VaultRedeemer::Action {
        tag: BigInt::from(100),
        transcript: transcript_entries,
        pool_input_index: BigInt::from(pool_sorted_idx as u64),
        pool_output_index: BigInt::from(0u64),
    };

    let pool_oref_plutus = OutputRef {
        transaction_id: pool_oref.transaction_id.to_vec(),
        output_index: pool_oref.index,
    };

    // The on-chain validator iterates both inputs and outputs in ascending
    // order using skip-based traversal, so entries MUST be sorted by
    // input_index AND output_index must also be ascending. We achieve this
    // by building fulfillment outputs in input-sorted order (see below).
    let mut input_sorted_order: Vec<usize> = (0..n_orders).collect();
    input_sorted_order.sort_by_key(|&i| order_filtered_indices[i]);

    let order_validator_entries: Vec<OrderValidatorEntry> = input_sorted_order
        .iter()
        .enumerate()
        .map(|(out_pos, &batch_idx)| OrderValidatorEntry {
            input_index: order_filtered_indices[batch_idx],
            output_index: (1 + out_pos) as u64,
        })
        .collect();

    let order_validator_redeemer = OrderValidatorRedeemer {
        entries: order_validator_entries,
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

    let mut all_ref_inputs = ref_inputs;
    all_ref_inputs.push(settings.input.0.clone());

    // ── Step 6: Build withdrawal map ────────────────────────────────────────

    fn reward_account(script_hash: &Hash<28>) -> PallasBytes {
        let mut account = vec![0xf0u8];
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

    let mut sorted_withdrawals = withdrawals;
    sorted_withdrawals.sort_by(|(a, _), (b, _)| a.cmp(b));

    // ── Step 7: Build outputs ───────────────────────────────────────────────

    let pool_address = {
        let vault_addr = ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::Script(exec.module_scripts.vault.hash),
            ShelleyDelegationPart::Null,
        );
        PallasBytes::from(vault_addr.to_vec())
    };

    let pool_datum_pd = updated_pool_datum.clone().to_plutus();
    let pool_output_value = build_pool_output_value(pool, &batch.final_assets)?;
    let pool_output = TransactionOutput::PostAlonzo(
        pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
            address: pool_address.clone(),
            value: pool_output_value,
            datum_option: Some(conway::PseudoDatumOption::Data(CborWrap(pool_datum_pd))),
            script_ref: None,
        },
    );

    let mut outputs = vec![pool_output];

    // Fee split: divide TX_FEE across all orders, last order absorbs remainder
    let per_order_fee = TX_FEE / n_orders as u64;
    let last_order_fee = TX_FEE - per_order_fee * (n_orders as u64 - 1);

    // Build fulfillment outputs in input-sorted order so output indices
    // ascend together with input indices (required by on-chain validator).
    let ada_asset = AssetClass { policy: vec![], token: vec![] };
    for (out_pos, &batch_idx) in input_sorted_order.iter().enumerate() {
        let swap = &swaps[batch_idx];
        let dest_address = resolve_destination(
            &swap.order.datum.destination,
            &swap.order.datum.owner,
        )?;
        let output_asset = &pool.pool_datum.assets[swap.output_idx].0;
        let order_ada = {
            use num_traits::ToPrimitive;
            swap.order.value.get(&ada_asset).clone().unwrap().to_u64().unwrap_or(0)
        };
        let fee = if out_pos == n_orders - 1 { last_order_fee } else { per_order_fee };
        let fulfillment_ada = order_ada.saturating_sub(fee);
        let fulfillment_value = build_fulfillment_value(output_asset, &swap.dy, fulfillment_ada)?;
        outputs.push(TransactionOutput::PostAlonzo(
            pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
                address: PallasBytes::from(dest_address),
                value: fulfillment_value,
                datum_option: None,
                script_ref: None,
            },
        ));
    }

    // ── Step 8: Build redeemer map ──────────────────────────────────────────

    let sorted_withdrawal_accounts: Vec<PallasBytes> =
        sorted_withdrawals.iter().map(|(a, _)| a.clone()).collect();

    fn withdrawal_index(accounts: &[PallasBytes], account: &PallasBytes) -> u32 {
        accounts.iter().position(|a| a == account).unwrap() as u32
    }

    let order_wd_account = reward_account(&exec.module_scripts.order.hash);
    let cp_wd_account = reward_account(&exec.module_scripts.constant_product.hash);
    let fs_wd_account = reward_account(&exec.module_scripts.fee_split.hash);
    let fair_wd_account = reward_account(&exec.module_scripts.fairness.hash);

    let lookup_eu = |key: &RedeemersKey| -> ExUnits {
        ex_units
            .and_then(|eus| eus.iter().find(|(k, _)| k == key).map(|(_, eu)| eu.clone()))
            .unwrap_or(ExUnits { mem: EX_MEM, steps: EX_STEPS })
    };

    let vault_key = RedeemersKey { tag: RedeemerTag::Spend, index: pool_sorted_idx as u32 };
    let vault_redeemer_pd = vault_redeemer.to_plutus();

    let order_wd_key = RedeemersKey { tag: RedeemerTag::Reward, index: withdrawal_index(&sorted_withdrawal_accounts, &order_wd_account) };
    let cp_wd_key = RedeemersKey { tag: RedeemerTag::Reward, index: withdrawal_index(&sorted_withdrawal_accounts, &cp_wd_account) };
    let fs_wd_key = RedeemersKey { tag: RedeemerTag::Reward, index: withdrawal_index(&sorted_withdrawal_accounts, &fs_wd_account) };
    let fair_wd_key = RedeemersKey { tag: RedeemerTag::Reward, index: withdrawal_index(&sorted_withdrawal_accounts, &fair_wd_account) };

    let order_wd_data = sorted_withdrawals.iter().find(|(a, _)| *a == order_wd_account).unwrap().1.clone();
    let cp_wd_data = sorted_withdrawals.iter().find(|(a, _)| *a == cp_wd_account).unwrap().1.clone();
    let fs_wd_data = sorted_withdrawals.iter().find(|(a, _)| *a == fs_wd_account).unwrap().1.clone();
    let fair_wd_data = sorted_withdrawals.iter().find(|(a, _)| *a == fair_wd_account).unwrap().1.clone();

    let mut redeemer_info: Vec<(RedeemersKey, pallas_primitives::PlutusData, ExUnits)> = vec![
        (vault_key.clone(), vault_redeemer_pd, lookup_eu(&vault_key)),
    ];

    // N order spend redeemers
    for (i, swap) in swaps.iter().enumerate() {
        let _ = swap;
        let order_key = RedeemersKey { tag: RedeemerTag::Spend, index: order_sorted_indices[i] as u32 };
        let order_redeemer = OrderRedeemer::Scoop {
            own_input_index: order_sorted_indices[i] as u64,
        };
        redeemer_info.push((order_key.clone(), order_redeemer.to_plutus(), lookup_eu(&order_key)));
    }

    // Withdrawal redeemers
    redeemer_info.push((order_wd_key.clone(), order_wd_data, lookup_eu(&order_wd_key)));
    redeemer_info.push((cp_wd_key.clone(), cp_wd_data, lookup_eu(&cp_wd_key)));
    redeemer_info.push((fs_wd_key.clone(), fs_wd_data, lookup_eu(&fs_wd_key)));
    redeemer_info.push((fair_wd_key.clone(), fair_wd_data, lookup_eu(&fair_wd_key)));

    let redeemer_pairs: Vec<(RedeemersKey, RedeemersValue)> = redeemer_info
        .iter()
        .map(|(key, data, eu)| (key.clone(), RedeemersValue { data: data.clone(), ex_units: eu.clone() }))
        .collect();

    let redeemers =
        Redeemers::Map(pallas_primitives::NonEmptyKeyValuePairs::Def(redeemer_pairs));

    // ── Step 9: Compute script_data_hash ────────────────────────────────────

    let redeemers_cbor = minicbor::to_vec(&redeemers).context("encode redeemers")?;
    let mut hasher = Hasher::<256>::new();
    hasher.input(&redeemers_cbor);
    hasher.input(language_views);
    let script_data_hash: Hash<32> = hasher.finalize();

    // ── Step 10: Assemble TransactionBody ───────────────────────────────────

    let ttl = current_slot + VALIDITY_RANGE;

    let body = conway::PseudoTransactionBody {
        inputs: sorted_inputs.into(),
        outputs,
        fee: TX_FEE,
        ttl: Some(ttl),
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
        total_collateral: Some(TX_FEE * 3 / 2),
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

    // Build resolved inputs for the evaluator
    let mut resolved_inputs = BTreeMap::new();
    resolved_inputs.insert(pool.input.clone(), ResolvedTxOut {
        address: pool_address.to_vec(),
        value: pool.value.clone(),
        datum: DatumOption::InlineDatum(pool.pool_datum.clone().to_plutus()),
        script_ref: None,
    });
    for swap in swaps {
        resolved_inputs.insert(swap.order.input.clone(), ResolvedTxOut {
            address: {
                let order_addr = ShelleyAddress::new(
                    Network::Testnet,
                    ShelleyPaymentPart::Script(exec.module_scripts.order.hash),
                    ShelleyDelegationPart::Null,
                );
                order_addr.to_vec()
            },
            value: swap.order.value.clone(),
            datum: DatumOption::InlineDatum(swap.order.datum.clone().to_plutus()),
            script_ref: None,
        });
    }

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
    resolved_ref_inputs.insert(settings.input.clone(), ResolvedTxOut {
        address: {
            let settings_addr = ShelleyAddress::new(
                Network::Testnet,
                ShelleyPaymentPart::Script(exec.module_scripts.settings.hash),
                ShelleyDelegationPart::Null,
            );
            settings_addr.to_vec()
        },
        value: settings.value.clone(),
        datum: DatumOption::InlineDatum(settings.datum.clone().to_plutus()),
        script_ref: None,
    });

    // Build predicted pool UTxO for chaining
    let predicted_pool_input = crate::cardano_types::TransactionInput::new(body_hash, 0);
    let mut predicted_pool_value = pool.value.clone();
    for (asset, new_amount) in &batch.final_assets {
        predicted_pool_value.insert(asset, new_amount.clone());
    }
    let predicted_pool = SundaeV4Pool {
        input: predicted_pool_input.clone(),
        value: predicted_pool_value,
        pool_datum: updated_pool_datum,
        slot: current_slot,
    };

    let tx = conway::PseudoTx {
        transaction_body: body,
        transaction_witness_set: witness_set,
        success: true,
        auxiliary_data: pallas_primitives::Nullable::<conway::AuxiliaryData>::Null,
    };

    let tx_cbor = minicbor::to_vec(&tx).context("encode tx")?;

    Ok(BatchBuildResult {
        cbor: tx_cbor,
        tx_hash: body_hash,
        tx_hash_hex,
        tx_body: tx.transaction_body,
        resolved_inputs,
        resolved_ref_inputs,
        redeemers: redeemer_info,
        predicted_pool: (predicted_pool_input, predicted_pool),
        ttl,
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
