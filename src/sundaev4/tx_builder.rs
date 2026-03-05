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
pub const TX_FEE: u64 = 3_000_000;
const POOL_MIN_ADA: u64 = 2_000_000;
const VALIDITY_RANGE: u64 = 180;

use crate::sundaev3::Ident;

/// Result of building a multi-pool scoop transaction.
pub struct MultiPoolBuildResult {
    pub cbor: Vec<u8>,
    pub tx_hash: Hash<32>,
    pub tx_hash_hex: String,
    pub tx_body: conway::PseudoTransactionBody<TransactionOutput>,
    pub resolved_inputs: BTreeMap<crate::cardano_types::TransactionInput, ResolvedTxOut>,
    pub resolved_ref_inputs: BTreeMap<crate::cardano_types::TransactionInput, ResolvedTxOut>,
    pub redeemers: Vec<(RedeemersKey, pallas_primitives::PlutusData, ExUnits)>,
    /// Predicted pool UTxOs after this tx settles (one per pool)
    pub predicted_pools: Vec<(Ident, crate::cardano_types::TransactionInput, SundaeV4Pool)>,
    /// The TTL used for this transaction
    pub ttl: u64,
}

/// Build a signed scoop transaction for M pools + N orders.
///
/// Generalizes the single-pool builder:
/// - Inputs: M pool inputs + N order inputs (all sorted together)
/// - Outputs: M pool outputs (indices 0..M-1), N fulfillment outputs (M..M+N-1)
/// - M vault spend redeemers (each with its own transcript)
/// - M CP/FS/fairness entries (one per pool)
/// - Fee split: TX_FEE / N total orders
pub fn build_multi_pool_scoop_tx(
    batches: &[Batch],
    settings: &SundaeV4Settings,
    exec: &ScooperExecution,
    current_slot: u64,
    language_views: &[u8],
    collateral_utxo: &TransactionInput,
    collateral_value: &crate::cardano_types::Value,
    ex_units: Option<&[(RedeemersKey, ExUnits)]>,
    ref_utxo_outputs: &BTreeMap<crate::cardano_types::TransactionInput, crate::cardano_types::TransactionOutput>,
) -> Result<MultiPoolBuildResult> {
    let m_pools = batches.len();
    let n_orders: usize = batches.iter().map(|b| b.swaps.len()).sum();
    if m_pools == 0 || n_orders == 0 {
        bail!("no batches or no swaps");
    }

    let sk = parse_secret_key(&exec.scooper_secret_key)?;
    let pk = sk.public_key();
    let pk_bytes: [u8; 32] = pk.as_ref().try_into().unwrap();
    let scooper_keyhash: Hash<28> = Hasher::<224>::hash(&pk_bytes);

    // ── Step 1: Per-pool transcript + updated datums ───────────────────────

    let void_vault_state = VaultState {
        assets: vec![],
        total_lp: BigInt::from(0),
        circulating_lp: BigInt::from(0),
        preminted_lp: BigInt::from(0),
    };

    struct PerPoolData {
        transcript: Vec<TranscriptEntry>,
        updated_datum: PoolDatum,
    }

    let mut per_pool: Vec<PerPoolData> = Vec::with_capacity(m_pools);

    for batch in batches {
        let pool = &batch.pool;
        let initial_total_lp = pool.pool_datum.total_lp.clone();
        let mut running_assets = pool.pool_datum.assets.clone();
        let mut total_fee_budget = BigInt::from(0);
        let mut transcript_entries: Vec<TranscriptEntry> = Vec::new();

        // Process operations in the interleaved order they were accumulated.
        // This is critical: replaying swaps-then-continuations would produce
        // wrong intermediate reserves when routed orders interleave.
        for op in &batch.ops_order {
            let (input_idx, output_idx, dx, dy) = match op {
                crate::sundaev4::batch::BatchOp::Swap(i) => {
                    let s = &batch.swaps[*i];
                    (s.input_idx, s.output_idx, &s.dx, &s.dy)
                }
                crate::sundaev4::batch::BatchOp::Continuation(i) => {
                    let c = &batch.continuations[*i];
                    (c.input_idx, c.output_idx, &c.dx, &c.dy)
                }
            };

            let prev_assets = running_assets.clone();

            running_assets[input_idx].1 = &running_assets[input_idx].1 + dx;
            running_assets[output_idx].1 = &running_assets[output_idx].1 - dy;

            let fee_budget = swap_math::compute_fee_budget(
                &batch.pool.pool_type,
                &prev_assets,
                &running_assets,
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

        let protocol_lp = swap_math::compute_protocol_lp(
            &total_fee_budget, exec.protocol_share.0, exec.protocol_share.1,
        );
        let final_total_lp = &initial_total_lp + &protocol_lp;

        if let Some(last) = transcript_entries.last_mut() {
            last.fee_budget = &last.fee_budget - &protocol_lp;
            last.state_after.total_lp = final_total_lp.clone();
        }

        let updated_datum = PoolDatum {
            assets: batch.final_assets.clone(),
            total_lp: final_total_lp.clone(),
            circulating_lp: pool.pool_datum.circulating_lp.clone(),
            preminted_lp: pool.pool_datum.preminted_lp.clone(),
            identifier: pool.pool_datum.identifier.clone(),
            actions: pool.pool_datum.actions.clone(),
            module_state: pool.pool_datum.module_state.clone(),
        };

        per_pool.push(PerPoolData {
            transcript: transcript_entries,
            updated_datum,
        });
    }

    // ── Step 2: Collect all inputs and sort ─────────────────────────────────

    // All pool orefs
    let pool_orefs: Vec<TransactionInput> = batches.iter()
        .map(|b| b.pool.input.0.clone())
        .collect();

    // All order orefs (flat list across all batches)
    let all_order_orefs: Vec<TransactionInput> = batches.iter()
        .flat_map(|b| b.swaps.iter().map(|s| s.order.input.0.clone()))
        .collect();

    let mut sorted_inputs: Vec<TransactionInput> = pool_orefs.iter()
        .chain(all_order_orefs.iter())
        .cloned()
        .collect();
    sorted_inputs.sort_by(|a, b| {
        a.transaction_id.cmp(&b.transaction_id).then(a.index.cmp(&b.index))
    });

    // Pool sorted indices (position of each pool in sorted_inputs)
    let pool_sorted_indices: Vec<usize> = pool_orefs.iter()
        .map(|oref| sorted_inputs.iter().position(|i| i == oref).unwrap())
        .collect();

    // Pool output indices: pools sorted by their input sort position
    // Output order: pools sorted by pool_sorted_indices
    let mut pool_output_order: Vec<usize> = (0..m_pools).collect();
    pool_output_order.sort_by_key(|&i| pool_sorted_indices[i]);

    // Map from batch index to pool output index
    let mut batch_to_pool_output: Vec<usize> = vec![0; m_pools];
    for (out_idx, &batch_idx) in pool_output_order.iter().enumerate() {
        batch_to_pool_output[batch_idx] = out_idx;
    }

    // Order sorted indices (position of each order in sorted_inputs)
    let order_sorted_indices: Vec<usize> = all_order_orefs.iter()
        .map(|oref| sorted_inputs.iter().position(|i| i == oref).unwrap())
        .collect();

    // Filtered order indices (position among order-script inputs only)
    let mut order_orefs_sorted = all_order_orefs.clone();
    order_orefs_sorted.sort_by(|a, b| {
        a.transaction_id.cmp(&b.transaction_id).then(a.index.cmp(&b.index))
    });

    let order_filtered_indices: Vec<u64> = all_order_orefs.iter()
        .map(|oref| {
            order_orefs_sorted.iter().position(|o| o == oref).unwrap() as u64
        })
        .collect();

    // ── Step 3: Build vault redeemers (M spend redeemers) ──────────────────

    let mut redeemer_info: Vec<(RedeemersKey, pallas_primitives::PlutusData, ExUnits)> = Vec::new();

    let lookup_eu = |key: &RedeemersKey| -> ExUnits {
        ex_units
            .and_then(|eus| eus.iter().find(|(k, _)| k == key).map(|(_, eu)| eu.clone()))
            .unwrap_or(ExUnits { mem: EX_MEM, steps: EX_STEPS })
    };

    let mut cp_entries: Vec<CPOperateEntry> = Vec::new();
    let mut cs_entries: Vec<CSOperateEntry> = Vec::new();
    let mut fs_entries: Vec<FSOperateEntry> = Vec::new();
    let mut fairness_entries: Vec<FairnessOperateEntry> = Vec::new();

    for (batch_idx, batch) in batches.iter().enumerate() {
        let pool_oref = &pool_orefs[batch_idx];
        let pool_sorted_idx = pool_sorted_indices[batch_idx];
        let pool_output_idx = batch_to_pool_output[batch_idx];

        let vault_redeemer = VaultRedeemer::Action {
            tag: BigInt::from(100),
            transcript: per_pool[batch_idx].transcript.clone(),
            pool_input_index: BigInt::from(pool_sorted_idx as u64),
            pool_output_index: BigInt::from(pool_output_idx as u64),
        };

        let vault_key = RedeemersKey { tag: RedeemerTag::Spend, index: pool_sorted_idx as u32 };
        redeemer_info.push((vault_key.clone(), vault_redeemer.to_plutus(), lookup_eu(&vault_key)));

        let pool_oref_plutus = OutputRef {
            transaction_id: pool_oref.transaction_id.to_vec(),
            output_index: pool_oref.index,
        };

        match &batch.pool.pool_type {
            PoolType::ConstantProduct { fee } => {
                cp_entries.push(CPOperateEntry {
                    vault_oref: pool_oref_plutus.clone(),
                    config: ConstantProductConfig {
                        fee: fee.clone(),
                    },
                });
            }
            PoolType::ConstantSum { prices, fee } => {
                cs_entries.push(CSOperateEntry {
                    vault_oref: pool_oref_plutus.clone(),
                    config: ConstantSumConfig {
                        prices: prices.clone(),
                        fee: fee.clone(),
                    },
                });
            }
        }

        fs_entries.push(FSOperateEntry {
            vault_oref: pool_oref_plutus,
            config: FeeSplitConfig {
                protocol_share: Rational {
                    num: BigInt::from(exec.protocol_share.0),
                    den: BigInt::from(exec.protocol_share.1),
                },
            },
        });

        fairness_entries.push(FairnessOperateEntry {
            pool_ident: batch.pool.pool_datum.identifier.clone(),
            scooper: scooper_keyhash.to_vec(),
        });
    }

    // ── Step 4: Build order redeemers (N spend redeemers) ──────────────────

    // Build a flat list of all swaps with their global order index
    let mut global_order_idx = 0usize;
    for batch in batches {
        for _swap in &batch.swaps {
            let idx = global_order_idx;
            let order_key = RedeemersKey {
                tag: RedeemerTag::Spend,
                index: order_sorted_indices[idx] as u32,
            };
            let order_redeemer = OrderRedeemer::Scoop {
                own_input_index: order_sorted_indices[idx] as u64,
            };
            redeemer_info.push((order_key.clone(), order_redeemer.to_plutus(), lookup_eu(&order_key)));
            global_order_idx += 1;
        }
    }

    // ── Step 5: Build order validator entries ───────────────────────────────
    // Sorted by input_index, output at m_pools + out_pos

    let mut input_sorted_order: Vec<usize> = (0..n_orders).collect();
    input_sorted_order.sort_by_key(|&i| order_filtered_indices[i]);

    let order_validator_entries: Vec<OrderValidatorEntry> = input_sorted_order
        .iter()
        .enumerate()
        .map(|(out_pos, &flat_idx)| OrderValidatorEntry {
            input_index: order_filtered_indices[flat_idx],
            output_index: (m_pools + out_pos) as u64,
        })
        .collect();

    let order_validator_redeemer = OrderValidatorRedeemer {
        entries: order_validator_entries,
    };

    let has_cp = !cp_entries.is_empty();
    let has_cs = !cs_entries.is_empty();

    let fs_redeemer = FeeSplitRedeemer::Operate { entries: fs_entries };
    let fairness_redeemer = FairnessRedeemer::Operate { entries: fairness_entries };

    // ── Step 6: Reference inputs ───────────────────────────────────────────

    let mut all_ref_inputs: Vec<TransactionInput> = vec![
        exec.module_scripts.vault.ref_utxo.0.clone(),
        exec.module_scripts.order.ref_utxo.0.clone(),
        exec.module_scripts.fee_split.ref_utxo.0.clone(),
        exec.module_scripts.fairness.ref_utxo.0.clone(),
        exec.module_scripts.pool_mint.ref_utxo.0.clone(),
        exec.module_scripts.settings.ref_utxo.0.clone(),
    ];
    if has_cp {
        all_ref_inputs.push(exec.module_scripts.constant_product.ref_utxo.0.clone());
    }
    if has_cs {
        if let Some(cs) = &exec.module_scripts.constant_sum {
            all_ref_inputs.push(cs.ref_utxo.0.clone());
        }
    }
    all_ref_inputs.push(settings.input.0.clone());

    // ── Step 7: Build withdrawal map ───────────────────────────────────────

    fn reward_account(script_hash: &Hash<28>) -> PallasBytes {
        let mut account = vec![0xf0u8];
        account.extend_from_slice(script_hash.as_ref());
        PallasBytes::from(account)
    }

    // Always present: order, fee_split, fairness
    let mut withdrawals: Vec<(PallasBytes, pallas_primitives::PlutusData)> = vec![
        (
            reward_account(&exec.module_scripts.order.hash),
            order_validator_redeemer.to_plutus(),
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

    // Conditionally add CP withdrawal
    if has_cp {
        let cp_redeemer = ConstantProductRedeemer::Operate { entries: cp_entries };
        withdrawals.push((
            reward_account(&exec.module_scripts.constant_product.hash),
            cp_redeemer.to_plutus(),
        ));
    }

    // Conditionally add CS withdrawal
    if has_cs {
        if let Some(cs_script) = &exec.module_scripts.constant_sum {
            let cs_redeemer = ConstantSumRedeemer::Operate { entries: cs_entries };
            withdrawals.push((
                reward_account(&cs_script.hash),
                cs_redeemer.to_plutus(),
            ));
        }
    }

    withdrawals.sort_by(|(a, _), (b, _)| a.cmp(b));

    // Withdrawal redeemers — one per withdrawal entry
    for (idx, (_, data)) in withdrawals.iter().enumerate() {
        let wd_key = RedeemersKey { tag: RedeemerTag::Reward, index: idx as u32 };
        redeemer_info.push((wd_key.clone(), data.clone(), lookup_eu(&wd_key)));
    }

    // ── Step 8: Build outputs ──────────────────────────────────────────────

    let pool_address = {
        let vault_addr = ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::Script(exec.module_scripts.vault.hash),
            ShelleyDelegationPart::Null,
        );
        PallasBytes::from(vault_addr.to_vec())
    };

    let mut outputs: Vec<TransactionOutput> = Vec::new();

    // Precompute per-batch net ADA delta for pool outputs.
    //
    // For ADA/TOKEN pools, ADA flows in two ways:
    //   - sell_outflow: dy for swaps/continuations where output is ADA (pool gives up ADA)
    //   - cont_buy_inflow: dx for CONTINUATIONS where input is ADA (pool receives ADA
    //     from routing — this ADA came from another pool's sell outflow)
    //
    // Pool output ADA delta: buy orders add ADA to the pool, sell orders remove it.
    let ada_asset = AssetClass { policy: vec![], token: vec![] };
    let pool_ada_deltas: Vec<i64> = batches.iter().map(|batch| {
        use num_traits::ToPrimitive;

        // ADA flowing OUT of the pool (sell orders where output is ADA)
        let sell_outflow: i64 = batch.swaps.iter()
            .filter(|s| {
                let out = &batch.pool.pool_datum.assets[s.output_idx].0;
                out.policy.is_empty() && out.token.is_empty()
            })
            .map(|s| s.dy.clone().unwrap().to_i64().unwrap_or(0))
            .sum::<i64>()
            + batch.continuations.iter()
            .filter(|c| {
                let out = &batch.pool.pool_datum.assets[c.output_idx].0;
                out.policy.is_empty() && out.token.is_empty()
            })
            .map(|c| c.dy.clone().unwrap().to_i64().unwrap_or(0))
            .sum::<i64>();

        // ADA flowing INTO the pool (buy orders where input is ADA)
        let buy_inflow: i64 = batch.swaps.iter()
            .filter(|s| {
                let inp = &batch.pool.pool_datum.assets[s.input_idx].0;
                inp.policy.is_empty() && inp.token.is_empty()
            })
            .map(|s| s.dx.clone().unwrap().to_i64().unwrap_or(0))
            .sum::<i64>()
            + batch.continuations.iter()
            .filter(|c| {
                let inp = &batch.pool.pool_datum.assets[c.input_idx].0;
                inp.policy.is_empty() && inp.token.is_empty()
            })
            .map(|c| c.dx.clone().unwrap().to_i64().unwrap_or(0))
            .sum::<i64>();

        buy_inflow - sell_outflow
    }).collect();

    // Pool outputs in pool_output_order (sorted by input position)
    for &batch_idx in &pool_output_order {
        let batch = &batches[batch_idx];
        let pool_datum_pd = per_pool[batch_idx].updated_datum.clone().to_plutus();
        let pool_output_value = build_pool_output_value(
            &batch.pool, &batch.final_assets, pool_ada_deltas[batch_idx],
        )?;
        outputs.push(TransactionOutput::PostAlonzo(
            pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
                address: pool_address.clone(),
                value: pool_output_value,
                datum_option: Some(conway::PseudoDatumOption::Data(CborWrap(pool_datum_pd))),
                script_ref: None,
            },
        ));
    }

    // Fee split across all orders
    let per_order_fee = TX_FEE / n_orders as u64;
    let last_order_fee = TX_FEE - per_order_fee * (n_orders as u64 - 1);

    // Fulfillment outputs in input-sorted order
    // Build a flat list of (order_filtered_index, batch_idx, swap_idx_in_batch)
    struct FlatSwap<'a> {
        batch_idx: usize,
        swap: &'a crate::sundaev4::batch::ResolvedSwap,
        filtered_idx: u64,
    }

    let mut flat_swaps: Vec<FlatSwap> = Vec::with_capacity(n_orders);
    let mut gi = 0usize;
    for (bi, batch) in batches.iter().enumerate() {
        for swap in &batch.swaps {
            flat_swaps.push(FlatSwap {
                batch_idx: bi,
                swap,
                filtered_idx: order_filtered_indices[gi],
            });
            gi += 1;
        }
    }

    // Sort by filtered input index for ascending output order
    let mut fulfillment_order: Vec<usize> = (0..n_orders).collect();
    fulfillment_order.sort_by_key(|&i| flat_swaps[i].filtered_idx);

    for (out_pos, &flat_idx) in fulfillment_order.iter().enumerate() {
        let fs = &flat_swaps[flat_idx];
        let swap = fs.swap;
        let batch = &batches[fs.batch_idx];
        let dest_address = resolve_destination(
            &swap.order.datum.destination,
            &swap.order.datum.owner,
        )?;

        // Use fulfillment override if present (routed orders), otherwise use
        // the direct swap output.
        let (output_asset, dy) = if let Some(fo) = &swap.fulfillment_override {
            (&fo.output_asset, &fo.amount)
        } else {
            (&batch.pool.pool_datum.assets[swap.output_idx].0, &swap.dy)
        };

        let fee = if out_pos == n_orders - 1 { last_order_fee } else { per_order_fee };

        // Compute fulfillment from first principles:
        // fulfillment = order_value - offer - fee + swap_result
        let (offer_asset, offer_amount) = &swap.order.datum.offer;
        let actual_fee = {
            use num_traits::ToPrimitive;
            let max_fee = swap.order.datum.max_protocol_fee.clone().unwrap().to_u64().unwrap_or(fee);
            fee.min(max_fee)
        };
        let fulfillment_value = build_fulfillment_value_from_order(
            &swap.order.value,
            offer_asset,
            offer_amount,
            output_asset,
            dy,
            actual_fee,
        )?;
        outputs.push(TransactionOutput::PostAlonzo(
            pallas_primitives::babbage::PseudoPostAlonzoTransactionOutput {
                address: PallasBytes::from(dest_address),
                value: fulfillment_value,
                datum_option: None,
                script_ref: None,
            },
        ));
    }

    // ── Step 9: Assemble redeemer map ──────────────────────────────────────

    let redeemer_pairs: Vec<(RedeemersKey, RedeemersValue)> = redeemer_info
        .iter()
        .map(|(key, data, eu)| (key.clone(), RedeemersValue { data: data.clone(), ex_units: eu.clone() }))
        .collect();

    let redeemers =
        Redeemers::Map(pallas_primitives::NonEmptyKeyValuePairs::Def(redeemer_pairs));

    // ── Step 10: Compute script_data_hash ──────────────────────────────────

    let redeemers_cbor = minicbor::to_vec(&redeemers).context("encode redeemers")?;
    let mut hasher = Hasher::<256>::new();
    hasher.input(&redeemers_cbor);
    hasher.input(language_views);
    let script_data_hash: Hash<32> = hasher.finalize();

    // ── Step 11: Assemble TransactionBody ──────────────────────────────────

    let ttl = current_slot + VALIDITY_RANGE;

    let body = conway::PseudoTransactionBody {
        inputs: sorted_inputs.into(),
        outputs,
        fee: TX_FEE,
        ttl: Some(ttl),
        certificates: None,
        withdrawals: Some(pallas_primitives::NonEmptyKeyValuePairs::Def(
            withdrawals
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

    // ── Step 12: Sign ──────────────────────────────────────────────────────

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

    // ── Step 13: Build resolved inputs for evaluator ───────────────────────

    let mut resolved_inputs = BTreeMap::new();
    for batch in batches {
        resolved_inputs.insert(batch.pool.input.clone(), ResolvedTxOut {
            address: pool_address.to_vec(),
            value: batch.pool.value.clone(),
            datum: DatumOption::InlineDatum(batch.pool.pool_datum.clone().to_plutus()),
            script_ref: None,
        });
        for swap in &batch.swaps {
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
                script_ref: txo.script_ref.as_ref().map(compute_script_ref_hash),
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

    // ── Step 14: Build predicted pool UTxOs ─────────────────────────────────

    let mut predicted_pools = Vec::with_capacity(m_pools);
    for (batch_idx, &out_idx) in pool_output_order.iter().enumerate() {
        let batch = &batches[out_idx];
        let predicted_input = crate::cardano_types::TransactionInput::new(body_hash, batch_idx as u64);
        let mut predicted_value = batch.pool.value.clone();
        for (asset, new_amount) in &batch.final_assets {
            if asset.policy.is_empty() && asset.token.is_empty() {
                continue; // ADA handled below
            }
            predicted_value.insert(asset, new_amount.clone());
        }
        // Set predicted ADA to match the actual pool output (input ADA + delta)
        {
            use num_traits::ToPrimitive;
            let pool_ada = batch.pool.value.get(&ada_asset).clone().unwrap().to_u64().unwrap_or(0);
            let predicted_ada = (pool_ada as i128 + pool_ada_deltas[out_idx] as i128)
                .max(POOL_MIN_ADA as i128) as u64;
            predicted_value.insert(&ada_asset, BigInt::from(predicted_ada as i64));
        }
        let predicted_pool = SundaeV4Pool {
            input: predicted_input.clone(),
            value: predicted_value,
            pool_datum: per_pool[out_idx].updated_datum.clone(),
            pool_type: batch.pool.pool_type.clone(),
            slot: current_slot,
        };
        predicted_pools.push((batch.pool_ident.clone(), predicted_input, predicted_pool));
    }

    let tx = conway::PseudoTx {
        transaction_body: body,
        transaction_witness_set: witness_set,
        success: true,
        auxiliary_data: pallas_primitives::Nullable::<conway::AuxiliaryData>::Null,
    };

    let tx_cbor = minicbor::to_vec(&tx).context("encode tx")?;

    Ok(MultiPoolBuildResult {
        cbor: tx_cbor,
        tx_hash: body_hash,
        tx_hash_hex,
        tx_body: tx.transaction_body,
        resolved_inputs,
        resolved_ref_inputs,
        redeemers: redeemer_info,
        predicted_pools,
        ttl,
    })
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Compute the script hash from a ScriptRef for the ScriptContext's TxOut encoding.
fn compute_script_ref_hash(script_ref: &crate::cardano_types::ScriptRef) -> Hash<28> {
    use pallas_crypto::hash::Hasher;
    match script_ref {
        crate::cardano_types::ScriptRef::Native(n) => {
            use pallas_traverse::ComputeHash;
            n.compute_hash()
        }
        crate::cardano_types::ScriptRef::PlutusV1(s) => {
            let cbor: &[u8] = s.as_ref();
            let mut preimage = Vec::with_capacity(1 + cbor.len());
            preimage.push(0x01);
            preimage.extend_from_slice(cbor);
            Hasher::<224>::hash(&preimage)
        }
        crate::cardano_types::ScriptRef::PlutusV2(s) => {
            let cbor: &[u8] = s.as_ref();
            let mut preimage = Vec::with_capacity(1 + cbor.len());
            preimage.push(0x02);
            preimage.extend_from_slice(cbor);
            Hasher::<224>::hash(&preimage)
        }
        crate::cardano_types::ScriptRef::PlutusV3(s) => {
            let cbor: &[u8] = s.as_ref();
            let mut preimage = Vec::with_capacity(1 + cbor.len());
            preimage.push(0x03);
            preimage.extend_from_slice(cbor);
            Hasher::<224>::hash(&preimage)
        }
    }
}

fn parse_secret_key(key_str: &str) -> Result<SecretKey> {
    let hex_str = if key_str.trim_start().starts_with('{') {
        // Cardano JSON envelope: {"type":"...","cborHex":"5820<64hex>"}
        let envelope: serde_json::Value =
            serde_json::from_str(key_str).context("invalid signing key JSON envelope")?;
        let cbor_hex = envelope["cborHex"]
            .as_str()
            .context("missing cborHex field in signing key envelope")?;
        // Strip the CBOR prefix "5820" (bytes tag for 32-byte bytestring)
        cbor_hex
            .strip_prefix("5820")
            .context("unexpected cborHex prefix (expected 5820)")?
            .to_string()
    } else {
        key_str.to_string()
    };
    let bytes = hex::decode(&hex_str).context("invalid secret key hex")?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("secret key must be 32 bytes"))?;
    Ok(SecretKey::from(arr))
}

/// Build the pool output Value (pallas conway::Value) from the pool's existing
/// value with asset amounts adjusted to reflect the swap.
///
/// `ada_delta` is the net ADA change for this pool's output:
///   - Negative when ADA leaves the pool (TOKEN→ADA sell orders)
///   - Positive when ADA enters via routing continuations
///   - Zero for TOKEN/TOKEN pools or when flows cancel
///
/// Regular buy-order ADA (ADA→TOKEN) does NOT appear here because that ADA
/// stays in the fulfillment output.
fn build_pool_output_value(
    pool: &SundaeV4Pool,
    new_assets: &[(AssetClass, BigInt)],
    ada_delta: i64,
) -> Result<ConwayValue> {
    use num_traits::ToPrimitive;
    use pallas_primitives::NonEmptyKeyValuePairs;

    // Start from the pool's current ADA
    let ada_asset = AssetClass {
        policy: vec![],
        token: vec![],
    };
    let ada_amount = pool.value.get(&ada_asset);
    let mut lovelace = ada_amount
        .clone()
        .unwrap()
        .to_u64()
        .unwrap_or(POOL_MIN_ADA)
        .max(POOL_MIN_ADA);

    // Apply net ADA delta (positive = pool gains ADA, negative = pool loses ADA)
    lovelace = (lovelace as i128 + ada_delta as i128).max(POOL_MIN_ADA as i128) as u64;

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

/// Build fulfillment output value from first principles:
/// fulfillment = order_value - offer - fee + swap_result
fn build_fulfillment_value_from_order(
    order_value: &crate::cardano_types::Value,
    offer_asset: &AssetClass,
    offer_amount: &BigInt,
    output_asset: &AssetClass,
    dy: &BigInt,
    fee: u64,
) -> Result<ConwayValue> {
    use num_traits::ToPrimitive;
    use pallas_primitives::NonEmptyKeyValuePairs;

    let ada_asset = AssetClass { policy: vec![], token: vec![] };

    // Start with the order's input value as a working copy
    let mut result = order_value.clone();

    // Subtract the offered asset
    let cur_offer = result.get(offer_asset);
    result.insert(offer_asset, &cur_offer - offer_amount);

    // Subtract the protocol fee (always ADA)
    let cur_ada = result.get(&ada_asset);
    result.insert(&ada_asset, &cur_ada - &BigInt::from(fee as i64));

    // Add the swap result
    let cur_out = result.get(output_asset);
    result.insert(output_asset, &cur_out + dy);

    // Convert to ConwayValue
    let lovelace = result.get(&ada_asset)
        .clone()
        .unwrap()
        .to_u64()
        .context("fulfillment ADA doesn't fit in u64")?;

    // Collect native tokens (skip ADA and any with zero/negative quantity)
    let mut policy_map: std::collections::BTreeMap<
        Vec<u8>,
        std::collections::BTreeMap<Vec<u8>, u64>,
    > = std::collections::BTreeMap::new();

    for (policy, tokens) in &result.0 {
        if policy.is_empty() {
            continue; // ADA handled above
        }
        for (token_name, qty) in tokens {
            let qty_u64 = qty.clone().unwrap().to_u64().unwrap_or(0);
            if qty_u64 > 0 {
                policy_map
                    .entry(policy.clone())
                    .or_default()
                    .insert(token_name.clone(), qty_u64);
            }
        }
    }

    if policy_map.is_empty() {
        return Ok(ConwayValue::Coin(lovelace));
    }

    let multiasset_pairs: Vec<_> = policy_map
        .into_iter()
        .map(|(policy, tokens)| {
            let policy_hash: Hash<28> = Hash::from(policy.as_slice());
            let token_pairs: Vec<_> = tokens
                .into_iter()
                .map(|(name, qty)| {
                    (
                        PallasBytes::from(name),
                        PositiveCoin::try_from(qty).unwrap(),
                    )
                })
                .collect();
            (policy_hash, NonEmptyKeyValuePairs::Def(token_pairs))
        })
        .collect();

    Ok(ConwayValue::Multiasset(
        lovelace,
        NonEmptyKeyValuePairs::Def(multiasset_pairs),
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

    // Cardano requires collateral return output to meet min UTxO (~858K lovelace).
    // Bail early rather than letting the node reject with BabbageOutputTooSmallUTxO.
    const MIN_COLLATERAL_RETURN: u64 = 1_000_000;
    if return_lovelace < MIN_COLLATERAL_RETURN {
        bail!(
            "collateral return ({} lovelace) below min UTxO; need a larger collateral UTxO",
            return_lovelace
        );
    }

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
