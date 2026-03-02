use anyhow::Context as _;
use acropolis_common::Point;
use pallas_addresses::ScriptHash;
use pallas_primitives::PlutusData;
use plutus_parser::AsPlutus;
use serde::ser::SerializeStruct;
use serde::Serializer;

use crate::bigint::BigInt;
use crate::cardano_types::{AssetClass, TransactionInput, Value};
use crate::multisig::Multisig;
use crate::sundaev3::{Ident, PlutusAddress};

/// Serde helpers for encoding `Vec<u8>` fields as hex strings in JSON.
mod hex_ser {
    use serde::Serializer;

    pub fn bytes<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(v))
    }

    pub fn vec_bytes<S: Serializer>(v: &Vec<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(Some(v.len()))?;
        for b in v { seq.serialize_element(&hex::encode(b))?; }
        seq.end()
    }

    pub fn vec_bytes_pair_as_map<S: Serializer>(v: &Vec<(Vec<u8>, Vec<u8>)>, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = s.serialize_map(Some(v.len()))?;
        for (k, v) in v { map.serialize_entry(&hex::encode(k), &hex::encode(v))?; }
        map.end()
    }

    pub fn opt_vec_bytes<S: Serializer>(v: &Option<Vec<Vec<u8>>>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(list) => vec_bytes(list, s),
            None => s.serialize_none(),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Pool / Vault types
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PoolDatum {
    pub assets: Vec<(AssetClass, BigInt)>,
    pub total_lp: BigInt,
    pub circulating_lp: BigInt,
    pub preminted_lp: BigInt,
    pub identifier: Ident,
    pub actions: Vec<ActionEntry>,
    #[serde(serialize_with = "hex_ser::vec_bytes_pair_as_map")]
    pub module_state: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ActionEntry {
    pub tag: BigInt,
    pub enabled: bool,
    #[serde(serialize_with = "hex_ser::vec_bytes")]
    pub modules: Vec<Vec<u8>>,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct VaultState {
    pub assets: Vec<(AssetClass, BigInt)>,
    pub total_lp: BigInt,
    pub circulating_lp: BigInt,
    pub preminted_lp: BigInt,
}

impl VaultState {
    pub fn from_pool(datum: &PoolDatum) -> Self {
        VaultState {
            assets: datum.assets.clone(),
            total_lp: datum.total_lp.clone(),
            circulating_lp: datum.circulating_lp.clone(),
            preminted_lp: datum.preminted_lp.clone(),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Transcript types
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TranscriptEntry {
    pub state_after: VaultState,
    pub fee_budget: BigInt,
    pub operation_tag: BigInt,
    pub operation_data: PlutusData,
}

#[derive(AsPlutus, Debug, PartialEq, Eq)]
pub enum VaultRedeemer {
    EscapeHatch {
        redeemed_lp: BigInt,
    },
    Upgrade,
    EmergencyDisable {
        target_tag: BigInt,
        set_enabled: bool,
    },
    Action {
        tag: BigInt,
        transcript: Vec<TranscriptEntry>,
        pool_input_index: BigInt,
        pool_output_index: BigInt,
    },
}

// ──────────────────────────────────────────────────────────────────────────────
// Order types
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Clone, AsPlutus, Debug, PartialEq, Eq)]
pub enum Destination {
    Fixed(PlutusAddress, Option<PlutusData>),
    SelfDestination,
}

impl serde::Serialize for Destination {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Destination::SelfDestination => serializer.serialize_str("self"),
            Destination::Fixed(addr, _datum) => {
                let mut s = serializer.serialize_struct("Destination", 1)?;
                s.serialize_field("address", addr)?;
                s.end()
            }
        }
    }
}

#[derive(Clone, AsPlutus, Debug, PartialEq, Eq, serde::Serialize)]
pub struct OrderDatum {
    pub owner: Multisig,
    pub destination: Destination,
    pub constraints: OrderConstraints,
    pub extension: PlutusData,
}

#[derive(Clone, AsPlutus, Debug, PartialEq, Eq)]
pub enum OrderConstraints {
    Simple {
        min_received: Vec<(AssetClass, BigInt)>,
    },
    Structured {
        steps: Vec<IntentStep>,
    },
}

impl serde::Serialize for OrderConstraints {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            OrderConstraints::Simple { min_received } => {
                let mut s = serializer.serialize_struct("OrderConstraints", 1)?;
                s.serialize_field("min_received", min_received)?;
                s.end()
            }
            OrderConstraints::Structured { steps } => {
                let mut s = serializer.serialize_struct("OrderConstraints", 1)?;
                s.serialize_field("steps", steps)?;
                s.end()
            }
        }
    }
}

#[derive(Clone, AsPlutus, Debug, PartialEq, Eq, serde::Serialize)]
pub struct IntentStep {
    pub operation_tag: BigInt,
    pub pool_ident: Ident,
}

/// An order can be spent either to Scoop (execute) it, or to cancel it
#[derive(AsPlutus, Debug, PartialEq, Eq)]
pub enum OrderRedeemer {
    Cancel,
    Scoop { own_input_index: u64 },
}

// ──────────────────────────────────────────────────────────────────────────────
// Settings types
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SettingsDatum {
    pub settings_admin: Multisig,
    pub treasury_admin: Multisig,
    #[serde(serialize_with = "hex_ser::bytes")]
    pub treasury_address: Vec<u8>,
    #[serde(serialize_with = "hex_ser::opt_vec_bytes")]
    pub authorized_scoopers: Option<Vec<Vec<u8>>>,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SettingsNode {
    pub key: Vec<u8>,
    pub config: PoolConfig,
    pub next: Option<Vec<u8>>,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PoolConfig {
    pub actions: Vec<ActionEntry>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Shared Plutus types (must be structs, not tuples, to match Aiken's Constr encoding)
// ──────────────────────────────────────────────────────────────────────────────

/// Aiken `Rational { num, den }` — encoded as Constr(0, [num, den]).
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Rational {
    pub num: BigInt,
    pub den: BigInt,
}

/// Aiken `OutputReference { transaction_id, output_index }`.
/// In PlutusV3, TxId is de-newtyped so this is Constr(0, [bytes, idx]).
#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct OutputRef {
    pub transaction_id: Vec<u8>,
    pub output_index: u64,
}

// ──────────────────────────────────────────────────────────────────────────────
// Module config types
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ConstantProductConfig {
    pub fee: Rational,
}

// ──────────────────────────────────────────────────────────────────────────────
// Withdrawal redeemer types
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct OrderValidatorRedeemer {
    pub entries: Vec<OrderValidatorEntry>,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct OrderValidatorEntry {
    pub input_index: u64,
    pub output_index: u64,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum ConstantProductRedeemer {
    Create,
    Operate { entries: Vec<CPOperateEntry> },
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct CPOperateEntry {
    pub vault_oref: OutputRef,
    pub config: ConstantProductConfig,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct FeeSplitConfig {
    pub protocol_share: Rational,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum FeeSplitRedeemer {
    Create { config: FeeSplitConfig },
    Operate { entries: Vec<FSOperateEntry> },
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct FSOperateEntry {
    pub vault_oref: OutputRef,
    pub config: FeeSplitConfig,
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub enum FairnessRedeemer {
    Create,
    Operate { entries: Vec<FairnessOperateEntry> },
}

#[derive(Debug, AsPlutus, Clone, PartialEq, Eq)]
pub struct FairnessOperateEntry {
    pub pool_ident: Ident,
    pub scooper: Vec<u8>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Execution configuration
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ScooperExecution {
    #[serde(default)]
    pub scooper_secret_key: String,
    #[serde(default)]
    pub scooper_secret_key_file: Option<String>,
    pub submit_url: String,
    pub fee: (u64, u64),
    pub protocol_share: (u64, u64),
    pub module_scripts: ModuleScripts,
    pub plutus_v3_cost_model: Vec<i64>,
}

impl ScooperExecution {
    /// If `scooper_secret_key_file` is set, read the file and populate
    /// `scooper_secret_key`. Call this once at startup.
    pub fn resolve_secret_key(&mut self) -> anyhow::Result<()> {
        if let Some(path) = &self.scooper_secret_key_file {
            let contents = std::fs::read_to_string(path)
                .with_context(|| format!("reading secret key file: {path}"))?;
            let trimmed = contents.trim();
            // Handle Cardano CLI skey JSON format: { "cborHex": "5820<hex>" }
            if trimmed.starts_with('{') {
                let json: serde_json::Value = serde_json::from_str(trimmed)
                    .with_context(|| format!("parsing skey JSON file: {path}"))?;
                let cbor_hex = json["cborHex"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("skey file missing cborHex field: {path}"))?;
                // Strip CBOR wrapping (5820 = 32-byte bytestring prefix)
                self.scooper_secret_key = cbor_hex
                    .strip_prefix("5820")
                    .unwrap_or(cbor_hex)
                    .to_string();
            } else {
                self.scooper_secret_key = trimmed.to_string();
            }
        }
        anyhow::ensure!(
            !self.scooper_secret_key.is_empty(),
            "scooper-secret-key or scooper-secret-key-file must be set"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ModuleScripts {
    pub constant_product: ScriptRefInfo,
    pub fee_split: ScriptRefInfo,
    pub fairness: ScriptRefInfo,
    pub vault: ScriptRefInfo,
    pub order: ScriptRefInfo,
    pub pool_mint: ScriptRefInfo,
    pub settings: ScriptRefInfo,
}

#[serde_with::serde_as]
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ScriptRefInfo {
    pub hash: ScriptHash,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub ref_utxo: crate::cardano_types::TransactionInput,
    /// Hex-encoded CBOR-wrapped script (double-wrapped: CBOR bytestring containing FLAT-encoded UPLC)
    #[serde(default)]
    pub script_cbor: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Indexed state wrapper types
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize)]
pub struct SundaeV4Pool {
    pub input: TransactionInput,
    pub value: Value,
    pub pool_datum: PoolDatum,
    pub slot: u64,
}

impl PartialOrd for SundaeV4Pool {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.slot.cmp(&other.slot))
    }
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct SundaeV4Order {
    pub input: TransactionInput,
    pub value: Value,
    pub datum: OrderDatum,
    pub slot: u64,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct SundaeV4Settings {
    pub input: TransactionInput,
    pub value: crate::cardano_types::Value,
    pub datum: SettingsDatum,
    pub slot: u64,
}

// ──────────────────────────────────────────────────────────────────────────────
// Protocol configuration
// ──────────────────────────────────────────────────────────────────────────────

#[serde_with::serde_as]
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SundaeV4Protocol {
    pub vault_script_hash: ScriptHash,
    pub order_script_hashes: Vec<ScriptHash>,
    pub settings_script_hash: ScriptHash,
    pub settings_nft: AssetClass,
    pub pool_nft_policy: ScriptHash,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub starting_point: Point,
    pub execution: Option<ScooperExecution>,
    #[serde(default)]
    pub blueprint: Option<crate::blueprint::Blueprint>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_v4_pool_datum() {
        // PoolDatum with 2 assets (ADA + token), total_lp=1000000, circ_lp=500, preminted=999500
        // identifier=0xdeadbeef, 1 action (tag=100, enabled=true, modules=[0xaa]),
        // module_state=[(0xaa, 0xbb)]
        let bytes = hex::decode(concat!(
            "d8799f",                           // Constr 0 (PoolDatum)
            "9f",                               // List: assets
            "9f9f4040ff00ff",                   // (("",""), 0) - ADA with 0 reserves
            "9f9f44010203044405060708ff01ff",    // ((0x01020304,0x05060708),1) - token
            "ff",
            "1a000f4240",                       // total_lp = 1_000_000
            "1901f4",                           // circulating_lp = 500
            "1a000f3e4c",                       // preminted_lp = 999_500
            "44deadbeef",                       // identifier
            "9f",                               // List: actions
            "d8799f1864d87a80",                 // ActionEntry { tag: 100, enabled: true,
            "9f41aaffff",                       // modules: [0xaa] }
            "ff",
            "9f",                               // List: module_state
            "9f41aa41bbff",                     // (0xaa, 0xbb)
            "ff",
            "ff"
        ))
        .unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let pool: PoolDatum = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(pool.identifier, Ident::new(&[0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(pool.total_lp, BigInt::from(1_000_000));
        assert_eq!(pool.circulating_lp, BigInt::from(500));
        assert_eq!(pool.preminted_lp, BigInt::from(998_988));
        assert_eq!(pool.assets.len(), 2);
        assert_eq!(pool.actions.len(), 1);
        assert_eq!(pool.actions[0].tag, BigInt::from(100));
        assert!(pool.actions[0].enabled);
        assert_eq!(pool.module_state.len(), 1);
    }

    #[test]
    fn test_decode_v4_vault_state() {
        // VaultState with 2 assets, total_lp=1000, circ_lp=500, preminted=500
        let bytes = hex::decode(concat!(
            "d8799f",
            "9f9f9f4040ff1a00989680ff9f9f44010203044405060708ff1a004c4b40ffff",
            "1903e8",       // total_lp = 1000
            "1901f4",       // circulating_lp = 500
            "1901f4",       // preminted_lp = 500
            "ff"
        ))
        .unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let vs: VaultState = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(vs.total_lp, BigInt::from(1000));
        assert_eq!(vs.circulating_lp, BigInt::from(500));
        assert_eq!(vs.preminted_lp, BigInt::from(500));
        assert_eq!(vs.assets.len(), 2);
    }

    #[test]
    fn test_decode_v4_order_datum_simple() {
        // OrderDatum with Simple constraints
        // owner: Signature(28 bytes = 56 hex chars)
        let bytes = hex::decode(concat!(
            "d8799f",                                           // Constr 0 (OrderDatum)
            "d8799f581c",                                       // owner: Signature(28 bytes)
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaff",
            "d87a80",                                           // destination: Self
            "d8799f",                                           // constraints: Simple
            "9f9f9f4040ff1a000f4240ffff",                       // min_received: [(ADA, 1_000_000)]
            "ff",
            "d87980",                                           // extension: unit
            "ff"
        ))
        .unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let order: OrderDatum = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(
            order.owner,
            Multisig::Signature(vec![0xaa; 28])
        );
        assert_eq!(order.destination, Destination::SelfDestination);
        match &order.constraints {
            OrderConstraints::Simple { min_received } => {
                assert_eq!(min_received.len(), 1);
                assert_eq!(min_received[0].1, BigInt::from(1_000_000));
            }
            _ => panic!("expected Simple constraints"),
        }
    }

    #[test]
    fn test_decode_v4_order_datum_structured() {
        // OrderDatum with Structured constraints
        let bytes = hex::decode(concat!(
            "d8799f",                                           // Constr 0 (OrderDatum)
            "d8799f581c",                                       // owner: Signature
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbff",
            "d87a80",                                           // destination: Self
            "d87a9f",                                           // constraints: Structured
            "9f",                                               // steps list
            "d8799f1864",                                       // IntentStep { tag: 100,
            "44deadbeef",                                       // pool_ident: 0xdeadbeef }
            "ffff",
            "ff",
            "d87980",                                           // extension: unit
            "ff"
        ))
        .unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let order: OrderDatum = AsPlutus::from_plutus(pd).unwrap();
        match &order.constraints {
            OrderConstraints::Structured { steps } => {
                assert_eq!(steps.len(), 1);
                assert_eq!(steps[0].operation_tag, BigInt::from(100));
                assert_eq!(steps[0].pool_ident, Ident::new(&[0xde, 0xad, 0xbe, 0xef]));
            }
            _ => panic!("expected Structured constraints"),
        }
    }

    #[test]
    fn test_decode_v4_order_redeemer_cancel() {
        // Cancel = Constr 0
        let bytes = hex::decode("d87980").unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let redeemer: OrderRedeemer = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(redeemer, OrderRedeemer::Cancel);
    }

    #[test]
    fn test_decode_v4_order_redeemer_scoop() {
        // Scoop { own_input_index: 3 } = Constr 1 [3]
        let bytes = hex::decode("d87a9f03ff").unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let redeemer: OrderRedeemer = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(redeemer, OrderRedeemer::Scoop { own_input_index: 3 });
    }

    #[test]
    fn test_decode_v4_vault_redeemer_action() {
        // VaultRedeemer::Action { tag: 100, transcript: [], pool_input_index: 0, pool_output_index: 0 }
        // = Constr 3 [100, [], 0, 0]
        let bytes = hex::decode("d87c9f18649fff0000ff").unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let redeemer: VaultRedeemer = AsPlutus::from_plutus(pd).unwrap();
        match redeemer {
            VaultRedeemer::Action {
                tag,
                transcript,
                pool_input_index,
                pool_output_index,
            } => {
                assert_eq!(tag, BigInt::from(100));
                assert!(transcript.is_empty());
                assert_eq!(pool_input_index, BigInt::from(0));
                assert_eq!(pool_output_index, BigInt::from(0));
            }
            _ => panic!("expected Action"),
        }
    }

    #[test]
    fn test_decode_v4_settings_datum() {
        // SettingsDatum { admin: Sig(0xaa..28), treasury_admin: Sig(0xbb..28),
        //   treasury_address: 0xcccc, authorized_scoopers: Some([0xdd..28]) }
        let bytes = hex::decode(concat!(
            "d8799f",
            "d8799f581c",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaff",
            "d8799f581c",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbff",
            "42cccc",
            "d8799f9f581c",
            "ddddddddddddddddddddddddddddddddddddddddddddddddddddddddffff",
            "ff"
        ))
        .unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let settings: SettingsDatum = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(settings.treasury_address, vec![0xcc, 0xcc]);
        assert!(settings.authorized_scoopers.is_some());
        assert_eq!(settings.authorized_scoopers.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_decode_v4_constant_product_config() {
        // ConstantProductConfig { fee: Rational { num: 3, den: 1000 } }
        // Constr(0, [Constr(0, [3, 1000])]) — both struct and Rational are Constr-encoded
        let bytes = hex::decode("d8799fd8799f031903e8ffff").unwrap();
        let pd: PlutusData = minicbor::decode(&bytes).unwrap();
        let config: ConstantProductConfig = AsPlutus::from_plutus(pd).unwrap();
        assert_eq!(config.fee.num, BigInt::from(3));
        assert_eq!(config.fee.den, BigInt::from(1000));
    }

    #[test]
    fn test_vault_state_from_pool() {
        let pool = PoolDatum {
            assets: vec![
                (AssetClass { policy: vec![], token: vec![] }, BigInt::from(100)),
                (AssetClass { policy: vec![1], token: vec![2] }, BigInt::from(200)),
            ],
            total_lp: BigInt::from(1000),
            circulating_lp: BigInt::from(500),
            preminted_lp: BigInt::from(500),
            identifier: Ident::new(&[0xab]),
            actions: vec![],
            module_state: vec![],
        };
        let state = VaultState::from_pool(&pool);
        assert_eq!(state.assets, pool.assets);
        assert_eq!(state.total_lp, pool.total_lp);
        assert_eq!(state.circulating_lp, pool.circulating_lp);
        assert_eq!(state.preminted_lp, pool.preminted_lp);
    }

    #[test]
    fn test_vault_redeemer_encoding() {
        use crate::cardano_types::AssetClass;

        // Build a minimal VaultRedeemer::Action and check its CBOR hex
        let state = VaultState {
            assets: vec![
                (AssetClass { policy: vec![0xaa], token: vec![0xbb] }, BigInt::from(100)),
            ],
            total_lp: BigInt::from(1000),
            circulating_lp: BigInt::from(500),
            preminted_lp: BigInt::from(500),
        };
        let entry = TranscriptEntry {
            state_after: state.clone(),
            fee_budget: BigInt::from(1),
            operation_tag: BigInt::from(100),
            operation_data: VaultState {
                assets: vec![],
                total_lp: BigInt::from(0),
                circulating_lp: BigInt::from(0),
                preminted_lp: BigInt::from(0),
            }.to_plutus(),
        };
        let redeemer = VaultRedeemer::Action {
            tag: BigInt::from(100),
            transcript: vec![entry],
            pool_input_index: BigInt::from(0u64),
            pool_output_index: BigInt::from(0u64),
        };
        let pd = redeemer.to_plutus();
        let cbor = minicbor::to_vec(&pd).unwrap();
        let hex = hex::encode(&cbor);
        eprintln!("VaultRedeemer CBOR hex: {hex}");

        // Verify structure: should be Constr(3, [tag, transcript, pool_input_idx, pool_output_idx])
        if let PlutusData::Constr(c) = &pd {
            assert_eq!(c.tag, 124, "Action should be variant 3 → tag 124");
            let fields = c.fields.clone().to_vec();
            assert_eq!(fields.len(), 4, "Action should have 4 fields");
        } else {
            panic!("expected Constr");
        }
    }
}
