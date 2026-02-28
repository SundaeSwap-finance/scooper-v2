//! Build PlutusV3 ScriptContext as CBOR-encoded PlutusData.
//!
//! Follows CIP-0069: all V3 validators receive a single UPLC argument
//! containing the full ScriptContext.
//!
//! ```text
//! ScriptContext = Constr(0, [TxInfo, Redeemer, ScriptInfo])
//! ```

use std::collections::BTreeMap;

use pallas_codec::utils::{KeyValuePairs, MaybeIndefArray};
use pallas_primitives::conway::{self, RedeemersKey, RedeemerTag, TransactionOutput};
use pallas_primitives::{Hash, PlutusData};

use crate::cardano_types;

/// Resolved transaction output data for building TxInInfo.
pub struct ResolvedTxOut {
    pub address: Vec<u8>,
    pub value: cardano_types::Value,
    pub datum: DatumOption,
    pub script_ref: Option<Hash<28>>,
}

/// Datum attached to a TxOut (for ScriptContext purposes).
pub enum DatumOption {
    None,
    DatumHash(Hash<32>),
    InlineDatum(PlutusData),
}

/// Which script purpose this evaluation is for.
pub enum ScriptPurpose {
    /// Spending a script UTxO: (output_reference, optional datum)
    Spending(OutputReference, Option<PlutusData>),
    /// Rewarding from a staking credential (withdraw-zero pattern)
    Rewarding(Credential),
}

/// OutputReference as PlutusData: Constr(0, [Constr(0, [tx_hash]), index])
#[derive(Clone)]
pub struct OutputReference {
    pub tx_hash: Hash<32>,
    pub index: u64,
}

/// Credential for reward addresses.
#[derive(Clone)]
pub enum Credential {
    PubKey(Hash<28>),
    Script(Hash<28>),
}

/// Build a complete PlutusV3 ScriptContext and return it as CBOR bytes.
///
/// The ScriptContext is: `Constr(0, [tx_info, redeemer, script_info])`
pub fn build_script_context(
    tx_body: &conway::PseudoTransactionBody<TransactionOutput>,
    redeemers: &[(RedeemersKey, PlutusData)],
    resolved_inputs: &BTreeMap<cardano_types::TransactionInput, ResolvedTxOut>,
    resolved_ref_inputs: &BTreeMap<cardano_types::TransactionInput, ResolvedTxOut>,
    tx_hash: Hash<32>,
    script_purpose: &ScriptPurpose,
    redeemer_data: &PlutusData,
) -> Vec<u8> {
    let tx_info = build_tx_info(tx_body, redeemers, resolved_inputs, resolved_ref_inputs, tx_hash);
    let script_info = build_script_info(script_purpose);

    let context = constr(0, vec![tx_info, redeemer_data.clone(), script_info]);
    minicbor::to_vec(&context).expect("CBOR encode ScriptContext")
}

// ──────────────────────────────────────────────────────────────────────────────
// TxInfo builder
// ──────────────────────────────────────────────────────────────────────────────

/// TxInfo = Constr(0, [inputs, ref_inputs, outputs, fee, mint, tx_certs,
///   wdrl, valid_range, signatories, redeemers, data, id,
///   votes, proposal_procedures, current_treasury, treasury_donation])
fn build_tx_info(
    tx_body: &conway::PseudoTransactionBody<TransactionOutput>,
    redeemers: &[(RedeemersKey, PlutusData)],
    resolved_inputs: &BTreeMap<cardano_types::TransactionInput, ResolvedTxOut>,
    resolved_ref_inputs: &BTreeMap<cardano_types::TransactionInput, ResolvedTxOut>,
    tx_hash: Hash<32>,
) -> PlutusData {
    // inputs: sorted list of TxInInfo
    let input_vec: Vec<_> = tx_body.inputs.iter().cloned().collect();
    let inputs = encode_tx_in_info_list(&input_vec, resolved_inputs);

    // reference_inputs
    let ref_inputs = match &tx_body.reference_inputs {
        Some(ref_set) => {
            let ref_vec: Vec<_> = ref_set.iter().cloned().collect();
            encode_tx_in_info_list(&ref_vec, resolved_ref_inputs)
        }
        None => pd_array(vec![]),
    };

    // outputs: list of TxOut
    let outputs = pd_array(
        tx_body
            .outputs
            .iter()
            .map(encode_tx_out)
            .collect(),
    );

    // fee: integer
    let fee = pd_int(tx_body.fee as i64);

    // mint: empty Value (no minting in scoop txs)
    let mint = encode_empty_value();

    // tx_certs: empty list
    let tx_certs = pd_array(vec![]);

    // wdrl: Map<Credential, Integer>
    let wdrl = match &tx_body.withdrawals {
        Some(kvps) => {
            let pairs: Vec<(PlutusData, PlutusData)> = kvps
                .iter()
                .map(|(account, coin)| {
                    let cred = encode_reward_account_credential(account);
                    let amount = pd_int(*coin as i64);
                    (cred, amount)
                })
                .collect();
            pd_map(pairs)
        }
        None => pd_map(vec![]),
    };

    // valid_range: Interval
    let valid_range = encode_validity_range(
        tx_body.validity_interval_start,
        tx_body.ttl,
    );

    // signatories: list of PubKeyHash
    let signatories = match &tx_body.required_signers {
        Some(signers) => pd_array(
            signers
                .iter()
                .map(|hash| PlutusData::BoundedBytes(hash.to_vec().into()))
                .collect(),
        ),
        None => pd_array(vec![]),
    };

    // redeemers: Map<ScriptPurpose, Redeemer>
    let redeemers_pd = encode_redeemers_map(redeemers, tx_body);

    // data: Map<DatumHash, Datum> (inline datums don't appear here)
    let data = pd_map(vec![]);

    // id: tx body hash
    let id = constr(0, vec![PlutusData::BoundedBytes(tx_hash.to_vec().into())]);

    // votes: empty map
    let votes = pd_map(vec![]);

    // proposal_procedures: empty list
    let proposal_procedures = pd_array(vec![]);

    // current_treasury: None
    let current_treasury = constr(1, vec![]);

    // treasury_donation: None
    let treasury_donation = constr(1, vec![]);

    constr(
        0,
        vec![
            inputs,
            ref_inputs,
            outputs,
            fee,
            mint,
            tx_certs,
            wdrl,
            valid_range,
            signatories,
            redeemers_pd,
            data,
            id,
            votes,
            proposal_procedures,
            current_treasury,
            treasury_donation,
        ],
    )
}

// ──────────────────────────────────────────────────────────────────────────────
// ScriptInfo
// ──────────────────────────────────────────────────────────────────────────────

fn build_script_info(purpose: &ScriptPurpose) -> PlutusData {
    match purpose {
        ScriptPurpose::Spending(oref, datum) => {
            let oref_pd = encode_output_reference(oref);
            let datum_option = match datum {
                Some(d) => constr(0, vec![d.clone()]), // Some(datum)
                None => constr(1, vec![]),              // None
            };
            constr(1, vec![oref_pd, datum_option])
        }
        ScriptPurpose::Rewarding(cred) => {
            let cred_pd = encode_credential(cred);
            constr(2, vec![cred_pd])
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Encoding helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Shorthand for PlutusData::Array with MaybeIndefArray.
fn pd_array(items: Vec<PlutusData>) -> PlutusData {
    PlutusData::Array(MaybeIndefArray::Def(items))
}

/// Shorthand for PlutusData::Map with KeyValuePairs.
fn pd_map(pairs: Vec<(PlutusData, PlutusData)>) -> PlutusData {
    PlutusData::Map(KeyValuePairs::Def(pairs))
}

/// Shorthand for a PlutusData integer.
fn pd_int(n: i64) -> PlutusData {
    PlutusData::BigInt(pallas_primitives::BigInt::Int(n.into()))
}

/// Create a Constr PlutusData node.
fn constr(variant: u64, fields: Vec<PlutusData>) -> PlutusData {
    PlutusData::Constr(pallas_primitives::Constr {
        tag: constr_tag(variant),
        any_constructor: if variant > 6 { Some(variant) } else { None },
        fields: MaybeIndefArray::Def(fields),
    })
}

/// Convert variant index to CBOR tag.
fn constr_tag(variant: u64) -> u64 {
    match variant {
        0..=6 => 121 + variant,
        _ => 102, // general tag, uses any_constructor
    }
}

fn encode_output_reference(oref: &OutputReference) -> PlutusData {
    // PlutusV3: TxId is de-newtyped, so OutputReference = Constr(0, [bytes, int])
    let tx_id = PlutusData::BoundedBytes(oref.tx_hash.to_vec().into());
    let index = pd_int(oref.index as i64);
    constr(0, vec![tx_id, index])
}

fn encode_credential(cred: &Credential) -> PlutusData {
    match cred {
        Credential::PubKey(hash) => {
            constr(0, vec![PlutusData::BoundedBytes(hash.to_vec().into())])
        }
        Credential::Script(hash) => {
            constr(1, vec![PlutusData::BoundedBytes(hash.to_vec().into())])
        }
    }
}

/// Encode a list of transaction inputs with their resolved outputs as TxInInfo.
fn encode_tx_in_info_list(
    inputs: &[pallas_primitives::TransactionInput],
    resolved: &BTreeMap<cardano_types::TransactionInput, ResolvedTxOut>,
) -> PlutusData {
    let mut sorted = inputs.to_vec();
    sorted.sort_by(|a, b| {
        a.transaction_id
            .cmp(&b.transaction_id)
            .then(a.index.cmp(&b.index))
    });
    pd_array(
        sorted
            .iter()
            .map(|input| {
                let key = cardano_types::TransactionInput::new(input.transaction_id, input.index);
                let resolved_out = resolved.get(&key);
                encode_tx_in_info(input, resolved_out)
            })
            .collect(),
    )
}

/// TxInInfo = Constr(0, [OutputReference, TxOut])
fn encode_tx_in_info(
    input: &pallas_primitives::TransactionInput,
    resolved: Option<&ResolvedTxOut>,
) -> PlutusData {
    let oref = encode_output_reference(&OutputReference {
        tx_hash: input.transaction_id,
        index: input.index,
    });
    let txout = match resolved {
        Some(r) => encode_resolved_tx_out(r),
        None => {
            // Fallback: empty TxOut (shouldn't happen in practice)
            constr(0, vec![
                encode_address_bytes(&[]),
                encode_empty_value(),
                constr(1, vec![]), // NoOutputDatum
                constr(1, vec![]), // None script_ref
            ])
        }
    };
    constr(0, vec![oref, txout])
}

/// Encode a ResolvedTxOut as PlutusData TxOut.
/// TxOut = Constr(0, [address, value, datum_option, maybe_script_ref])
fn encode_resolved_tx_out(txo: &ResolvedTxOut) -> PlutusData {
    let address = encode_address_bytes(&txo.address);
    let value = encode_value(&txo.value);
    let datum_option = match &txo.datum {
        DatumOption::None => constr(1, vec![]),      // NoOutputDatum
        DatumOption::DatumHash(h) => {
            constr(1, vec![PlutusData::BoundedBytes(h.to_vec().into())])  // OutputDatumHash
        }
        DatumOption::InlineDatum(d) => {
            constr(2, vec![d.clone()])  // OutputDatum (inline)
        }
    };
    let script_ref = match &txo.script_ref {
        Some(hash) => constr(0, vec![PlutusData::BoundedBytes(hash.to_vec().into())]),
        None => constr(1, vec![]),
    };
    constr(0, vec![address, value, datum_option, script_ref])
}

/// Encode a pallas TransactionOutput as PlutusData TxOut.
fn encode_tx_out(output: &TransactionOutput) -> PlutusData {
    match output {
        TransactionOutput::Legacy(o) => {
            let address = encode_address_bytes(&o.address);
            // Legacy outputs use alonzo Value (u64 coin only for our purposes)
            let value = {
                let lovelace = match &o.amount {
                    pallas_primitives::alonzo::Value::Coin(c) => *c,
                    pallas_primitives::alonzo::Value::Multiasset(c, _) => *c,
                };
                let ada_policy = PlutusData::BoundedBytes(vec![].into());
                let ada_token = PlutusData::BoundedBytes(vec![].into());
                pd_map(vec![(ada_policy, pd_map(vec![(ada_token, pd_int(lovelace as i64))]))])
            };
            let datum_option = constr(1, vec![]); // NoOutputDatum
            let script_ref = constr(1, vec![]);   // None
            constr(0, vec![address, value, datum_option, script_ref])
        }
        TransactionOutput::PostAlonzo(o) => {
            let address = encode_address_bytes(&o.address);
            let value = encode_conway_value(&o.value);
            let datum_option = match &o.datum_option {
                None => constr(1, vec![]),  // NoOutputDatum
                Some(conway::PseudoDatumOption::Hash(h)) => {
                    constr(1, vec![PlutusData::BoundedBytes(h.to_vec().into())])
                }
                Some(conway::PseudoDatumOption::Data(d)) => {
                    constr(2, vec![d.0.clone()])
                }
            };
            let script_ref = constr(1, vec![]); // None (we don't encode script_ref in outputs)
            constr(0, vec![address, value, datum_option, script_ref])
        }
    }
}

/// Encode an address from raw bytes into PlutusData Address.
/// Address = Constr(0, [payment_credential, staking_credential_option])
fn encode_address_bytes(raw: &[u8]) -> PlutusData {
    use pallas_addresses::Address;

    let Ok(addr) = Address::from_bytes(raw) else {
        // Fallback: encode as raw bytes
        return constr(0, vec![
            constr(0, vec![PlutusData::BoundedBytes(raw.to_vec().into())]),
            constr(1, vec![]),
        ]);
    };

    match addr {
        Address::Shelley(shelley) => {
            let payment = match shelley.payment() {
                pallas_addresses::ShelleyPaymentPart::Key(h) => {
                    constr(0, vec![PlutusData::BoundedBytes(h.to_vec().into())])
                }
                pallas_addresses::ShelleyPaymentPart::Script(h) => {
                    constr(1, vec![PlutusData::BoundedBytes(h.to_vec().into())])
                }
            };
            let staking = match shelley.delegation() {
                pallas_addresses::ShelleyDelegationPart::Key(h) => {
                    // Some(Inline(PubKeyCredential))
                    constr(0, vec![constr(0, vec![
                        constr(0, vec![PlutusData::BoundedBytes(h.to_vec().into())])
                    ])])
                }
                pallas_addresses::ShelleyDelegationPart::Script(h) => {
                    // Some(Inline(ScriptCredential))
                    constr(0, vec![constr(0, vec![
                        constr(1, vec![PlutusData::BoundedBytes(h.to_vec().into())])
                    ])])
                }
                pallas_addresses::ShelleyDelegationPart::Null
                | pallas_addresses::ShelleyDelegationPart::Pointer(_) => {
                    constr(1, vec![]) // None
                }
            };
            constr(0, vec![payment, staking])
        }
        _ => {
            // Non-Shelley address: encode payment as raw bytes
            constr(0, vec![
                constr(0, vec![PlutusData::BoundedBytes(raw.to_vec().into())]),
                constr(1, vec![]),
            ])
        }
    }
}

/// Encode our Value type as PlutusData: Map<CurrencySymbol, Map<TokenName, Int>>
fn encode_value(value: &cardano_types::Value) -> PlutusData {
    let mut outer: Vec<(PlutusData, PlutusData)> = Vec::new();

    for (policy, tokens) in &value.0 {
        let policy_pd = PlutusData::BoundedBytes(policy.clone().into());
        let mut inner: Vec<(PlutusData, PlutusData)> = Vec::new();
        for (token, qty) in tokens {
            let token_pd = PlutusData::BoundedBytes(token.clone().into());
            let qty_pd = bigint_to_plutus(qty);
            inner.push((token_pd, qty_pd));
        }
        outer.push((policy_pd, pd_map(inner)));
    }

    pd_map(outer)
}

/// Encode a conway::Value as PlutusData.
fn encode_conway_value(value: &conway::Value) -> PlutusData {
    match value {
        conway::Value::Coin(lovelace) => {
            let ada_policy = PlutusData::BoundedBytes(vec![].into());
            let ada_token = PlutusData::BoundedBytes(vec![].into());
            let ada_qty = pd_int(*lovelace as i64);
            pd_map(vec![(ada_policy, pd_map(vec![(ada_token, ada_qty)]))])
        }
        conway::Value::Multiasset(lovelace, assets) => {
            let mut outer: Vec<(PlutusData, PlutusData)> = Vec::new();

            // ADA entry
            let ada_policy = PlutusData::BoundedBytes(vec![].into());
            let ada_token = PlutusData::BoundedBytes(vec![].into());
            let ada_qty = pd_int(*lovelace as i64);
            outer.push((ada_policy, pd_map(vec![(ada_token, ada_qty)])));

            // Native tokens
            for (policy_hash, tokens) in assets.iter() {
                let policy_pd = PlutusData::BoundedBytes(policy_hash.to_vec().into());
                let mut inner: Vec<(PlutusData, PlutusData)> = Vec::new();
                for (token_name, qty) in tokens.iter() {
                    let token_pd = PlutusData::BoundedBytes(token_name.to_vec().into());
                    let qty_pd = pd_int(u64::from(*qty) as i64);
                    inner.push((token_pd, qty_pd));
                }
                outer.push((policy_pd, pd_map(inner)));
            }

            pd_map(outer)
        }
    }
}

fn encode_empty_value() -> PlutusData {
    pd_map(vec![])
}

/// Encode a validity range as Interval PlutusData.
/// Interval = Constr(0, [LowerBound, UpperBound])
/// LowerBound = Constr(0, [Bound, Closure])
/// UpperBound = Constr(0, [Bound, Closure])
/// Bound::Finite(x) = Constr(1, [x])
/// Bound::NegInf = Constr(0, [])
/// Bound::PosInf = Constr(2, [])
/// Closure (True) = Constr(1, [])
/// Closure (False) = Constr(0, [])
fn encode_validity_range(start: Option<u64>, ttl: Option<u64>) -> PlutusData {
    let lower = match start {
        Some(s) => {
            let finite = constr(1, vec![pd_int(s as i64)]);
            constr(0, vec![finite, constr(1, vec![])]) // Closed (True)
        }
        None => {
            let neg_inf = constr(0, vec![]);
            constr(0, vec![neg_inf, constr(1, vec![])]) // True
        }
    };

    let upper = match ttl {
        Some(t) => {
            let finite = constr(1, vec![pd_int(t as i64)]);
            constr(0, vec![finite, constr(1, vec![])]) // Closed (True)
        }
        None => {
            let pos_inf = constr(2, vec![]);
            constr(0, vec![pos_inf, constr(1, vec![])]) // True
        }
    };

    constr(0, vec![lower, upper])
}

/// Encode the redeemers map: Map<ScriptPurpose, Redeemer>
fn encode_redeemers_map(
    redeemers: &[(RedeemersKey, PlutusData)],
    tx_body: &conway::PseudoTransactionBody<TransactionOutput>,
) -> PlutusData {
    let mut pairs: Vec<(PlutusData, PlutusData)> = Vec::new();

    for (key, data) in redeemers {
        let purpose = encode_redeemer_purpose(key, tx_body);
        pairs.push((purpose, data.clone()));
    }

    pd_map(pairs)
}

/// Map a RedeemersKey to its ScriptPurpose PlutusData.
fn encode_redeemer_purpose(
    key: &RedeemersKey,
    tx_body: &conway::PseudoTransactionBody<TransactionOutput>,
) -> PlutusData {
    match key.tag {
        RedeemerTag::Spend => {
            // Look up the input at this index
            let mut sorted_inputs: Vec<_> = tx_body.inputs.iter().cloned().collect();
            sorted_inputs.sort_by(|a, b| {
                a.transaction_id
                    .cmp(&b.transaction_id)
                    .then(a.index.cmp(&b.index))
            });
            if let Some(input) = sorted_inputs.get(key.index as usize) {
                let oref = encode_output_reference(&OutputReference {
                    tx_hash: input.transaction_id,
                    index: input.index,
                });
                constr(1, vec![oref]) // Spending
            } else {
                constr(1, vec![encode_output_reference(&OutputReference {
                    tx_hash: [0u8; 32].into(),
                    index: 0,
                })])
            }
        }
        RedeemerTag::Reward => {
            // Look up the withdrawal at this index
            if let Some(withdrawals) = &tx_body.withdrawals {
                let sorted_accounts: Vec<_> = withdrawals.iter().map(|(a, _)| a.clone()).collect();
                if let Some(account) = sorted_accounts.get(key.index as usize) {
                    let cred = encode_reward_account_credential(account);
                    constr(2, vec![cred]) // Rewarding
                } else {
                    constr(2, vec![constr(0, vec![PlutusData::BoundedBytes(vec![].into())])])
                }
            } else {
                constr(2, vec![constr(0, vec![PlutusData::BoundedBytes(vec![].into())])])
            }
        }
        RedeemerTag::Mint => {
            constr(0, vec![PlutusData::BoundedBytes(vec![].into())]) // Minting
        }
        _ => {
            constr(0, vec![PlutusData::BoundedBytes(vec![].into())])
        }
    }
}

/// Parse a reward account (29 bytes: 1 header + 28 hash) into a Credential.
fn encode_reward_account_credential(account: &[u8]) -> PlutusData {
    if account.len() != 29 {
        return constr(0, vec![PlutusData::BoundedBytes(account.to_vec().into())]);
    }
    let header = account[0];
    let hash = &account[1..29];
    // Bit 4 of header: 0 = key, 1 = script
    if header & 0x10 != 0 {
        // Script credential
        constr(1, vec![PlutusData::BoundedBytes(hash.to_vec().into())])
    } else {
        // Key credential
        constr(0, vec![PlutusData::BoundedBytes(hash.to_vec().into())])
    }
}

/// Convert our BigInt to PlutusData integer.
fn bigint_to_plutus(value: &crate::bigint::BigInt) -> PlutusData {
    use num_traits::ToPrimitive;
    if let Some(n) = value.clone().unwrap().to_i64() {
        pd_int(n)
    } else {
        let bytes = value.clone().unwrap().to_signed_bytes_be();
        PlutusData::BigInt(pallas_primitives::BigInt::BigNInt(bytes.into()))
    }
}
