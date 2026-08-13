//! Test harness for end-to-end scoop transaction evaluation.
//!
//! Loads a Blueprint fixture, builds a ScriptStore and ScooperExecution,
//! and provides `build_and_eval` to run the full pipeline (tx build → eval).

#[cfg(test)]
pub(crate) mod test_harness {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use pallas_codec::utils::MaybeIndefArray;
    use pallas_crypto::hash::Hasher;
    use pallas_crypto::key::ed25519::SecretKey;
    use pallas_primitives::{Hash, PlutusData, TransactionInput};
    use plutus_parser::AsPlutus;

    use crate::bigint::BigInt;
    use crate::blueprint::Blueprint;
    use crate::cardano_types::{AssetClass, Value};
    use crate::multisig::Multisig;
    use crate::sundaev3::Ident;
    use crate::sundaev4::batch::{Batch, ScoopPlan};
    use crate::sundaev4::evaluator::{EvalResult, ScriptStore, evaluate_scoop_tx};
    use crate::sundaev4::submit::encode_language_views;
    use crate::sundaev4::tx_builder::{MultiPoolBuildResult, build_multi_pool_scoop_tx};
    use crate::sundaev4::types::*;

    /// The cost model from our devnet config.
    pub const PLUTUS_V3_COST_MODEL: &[i64] = &[
        100788, 420, 1, 1, 1000, 173, 0, 1, 1000, 59957, 4, 1, 11183, 32,
        201305, 8356, 4, 16000, 100, 16000, 100, 16000, 100, 16000, 100, 16000, 100, 16000,
        100, 100, 100, 16000, 100, 94375, 32, 132994, 32, 61462, 4, 72010, 178, 0,
        1, 22151, 32, 91189, 769, 4, 2, 85848, 123203, 7305, -900, 1716, 549, 57,
        85848, 0, 1, 1, 1000, 42921, 4, 2, 24548, 29498, 38, 1, 898148, 27279,
        1, 51775, 558, 1, 39184, 1000, 60594, 1, 141895, 32, 83150, 32, 15299, 32,
        76049, 1, 13169, 4, 22100, 10, 28999, 74, 1, 28999, 74, 1, 43285, 552,
        1, 44749, 541, 1, 33852, 32, 68246, 32, 72362, 32, 7243, 32, 7391, 32,
        11546, 32, 85848, 123203, 7305, -900, 1716, 549, 57, 85848, 0, 1, 90434, 519,
        0, 1, 74433, 32, 85848, 123203, 7305, -900, 1716, 549, 57, 85848, 0, 1,
        1, 85848, 123203, 7305, -900, 1716, 549, 57, 85848, 0, 1, 955506, 213312, 0,
        2, 270652, 22588, 4, 1457325, 64566, 4, 20467, 1, 4, 0, 141992, 32, 100788,
        420, 1, 1, 81663, 32, 59498, 32, 20142, 32, 24588, 32, 20744, 32, 25933,
        32, 43053543, 10, 53384111, 14333, 10, 43574283, 26308, 10, 16000, 100, 16000,
        100, 962335, 18, 2780678, 6, 442008, 1, 52538055, 3756, 18, 267929, 18, 76433006, 8868,
        18, 52948122, 18, 1995836, 36, 3227919, 12, 901022, 1, 166917843, 4307, 36, 284546, 36,
        158221314, 26549, 36, 74698472, 36, 333849714, 1, 254006273, 72, 2174038, 72, 2261318, 64571, 4,
        207616, 8310, 4, 1293828, 28716, 63, 0, 1, 1006041, 43623, 251, 0, 1, 100181,
        726, 719, 0, 1, 100181, 726, 719, 0, 1, 100181, 726, 719, 0, 1,
        107878, 680, 0, 1, 95336, 1, 281145, 18848, 0, 1, 180194, 159, 1, 1,
        158519, 8942, 0, 1, 159378, 8813, 0, 1, 107490, 3298, 1, 106057, 655, 1,
        1964219, 24520, 3,
    ];

    /// Deterministic test secret key (same as devnet config).
    const SCOOPER_SECRET_KEY: &str =
        "0101010101010101010101010101010101010101010101010101010101010101";

    /// Constraint-module hashes from the loaded blueprint, published so the
    /// free-standing order constructors (`make_order` & co) can build real
    /// PR#11 constraint lists without threading `TestEnv` through 30 call
    /// sites. Set once per process by `TestEnv::from_blueprint_file`; all
    /// tests load the same fixture.
    pub struct TestConstraintCtx {
        pub swap_order: Vec<u8>,
        pub basic_order: Vec<u8>,
        pub route_order: Vec<u8>,
        pub fairness_order: Vec<u8>,
        pub strategy_order: Vec<u8>,
        pub settings_mint: Vec<u8>,
    }
    pub static TEST_CTX: std::sync::OnceLock<Option<TestConstraintCtx>> =
        std::sync::OnceLock::new();

    /// OrderConfig token names used by the harness (mirroring the CLI roles).
    pub const CFG_SWAP: &[u8] = b"cfg-swap";
    pub const CFG_BASIC: &[u8] = b"cfg-basic";
    pub const CFG_STRATEGY: &[u8] = b"cfg-strategy";

    /// Complete test environment for building and evaluating scoop transactions.
    pub struct TestEnv {
        pub exec: ScooperExecution,
        pub scripts: ScriptStore,
        /// Loaded Butane runtime for conversion-leg tests (repo config).
        pub butane: Option<crate::sundaev4::butane::ButaneRuntime>,
        pub ref_utxo_outputs: BTreeMap<crate::cardano_types::TransactionInput, crate::cardano_types::TransactionOutput>,
        pub language_views: Vec<u8>,
        pub collateral_utxo: TransactionInput,
        pub collateral_value: Value,
        pub funding_utxo: TransactionInput,
        pub funding_value: Value,
        pub order_configs: BTreeMap<Vec<u8>, Arc<crate::sundaev4::SundaeV4OrderConfig>>,
    }

    impl TestEnv {
        /// Load from a blueprint fixture file.
        pub fn from_blueprint_file(path: &str) -> Self {
            // Surface evaluator traces in test output (RUST_LOG to widen).
            let _ = tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("error")),
                )
                .with_test_writer()
                .try_init();
            let data = std::fs::read_to_string(path)
                .unwrap_or_else(|e| panic!("failed to read blueprint fixture {path}: {e}"));
            let blueprint: Blueprint = serde_json::from_str(&data)
                .unwrap_or_else(|e| panic!("failed to parse blueprint fixture: {e}"));

            let scripts = ScriptStore::from_blueprint(&blueprint)
                .expect("ScriptStore::from_blueprint failed");

            let module_scripts = blueprint
                .to_v4_module_scripts()
                .expect("blueprint.to_v4_module_scripts failed");

            let language_views = encode_language_views(PLUTUS_V3_COST_MODEL);

            // Build ref_utxo_outputs: for each validator with compiled_code,
            // create a synthetic TransactionOutput at the reference UTxO location.
            let mut ref_utxo_outputs = BTreeMap::new();
            for validator in &blueprint.validators {
                if let Some(code_hex) = &validator.compiled_code {
                    // Find the reference for this validator
                    if let Some(reference) = blueprint.references.iter().find(|r| {
                        r.key == validator.title
                            || validator.title.contains(&r.key)
                            || r.key.contains(&validator.title)
                    }) {
                        let tx_hash_bytes = hex::decode(&reference.tx_in.hash).unwrap();
                        let tx_hash: Hash<32> = tx_hash_bytes.as_slice().try_into().unwrap();
                        let input = crate::cardano_types::TransactionInput::new(
                            tx_hash,
                            reference.tx_in.index,
                        );

                        let script_cbor = hex::decode(code_hex).unwrap();
                        let script = pallas_primitives::PlutusScript::<3>(
                            pallas_primitives::Bytes::from(script_cbor),
                        );

                        ref_utxo_outputs.insert(
                            input,
                            crate::cardano_types::TransactionOutput {
                                address: pallas_addresses::Address::Shelley(
                                    pallas_addresses::ShelleyAddress::new(
                                        pallas_addresses::Network::Testnet,
                                        pallas_addresses::ShelleyPaymentPart::Key([0u8; 28].into()),
                                        pallas_addresses::ShelleyDelegationPart::Null,
                                    ),
                                ),
                                value: Value::default(),
                                datum: crate::cardano_types::RawDatum::None,
                                script_ref: Some(crate::cardano_types::ScriptRef::PlutusV3(script)),
                            },
                        );
                    }
                }
            }

            // PR#11 constraint context + synthetic OrderConfig settings
            // entries. Only present when the blueprint carries the modular
            // constraint validators (post-PR#11 fixtures).
            let settings_mint = blueprint
                .validators
                .iter()
                .find(|v| v.title == "settingsMint" || v.title.contains("settings_mint"))
                .map(|v| hex::decode(&v.hash).expect("settingsMint hash hex"));
            let ctx = match (
                &module_scripts.swap_order,
                &module_scripts.basic_order,
                &module_scripts.route_order,
                &module_scripts.fairness_order,
                &module_scripts.strategy_order,
                settings_mint,
            ) {
                (Some(sw), Some(ba), Some(ro), Some(fo), Some(st), Some(sm)) => {
                    Some(TestConstraintCtx {
                        swap_order: sw.hash.as_ref().to_vec(),
                        basic_order: ba.hash.as_ref().to_vec(),
                        route_order: ro.hash.as_ref().to_vec(),
                        fairness_order: fo.hash.as_ref().to_vec(),
                        strategy_order: st.hash.as_ref().to_vec(),
                        settings_mint: sm,
                    })
                }
                _ => None,
            };
            let mut order_configs: BTreeMap<
                Vec<u8>,
                Arc<crate::sundaev4::SundaeV4OrderConfig>,
            > = BTreeMap::new();
            if let Some(ctx) = &ctx {
                // Mirrors the CLI's mint-order-config presets.
                let entries: [(&[u8], Vec<Vec<u8>>); 3] = [
                    (CFG_SWAP, vec![
                        ctx.swap_order.clone(),
                        ctx.route_order.clone(),
                        ctx.fairness_order.clone(),
                    ]),
                    (CFG_BASIC, vec![
                        ctx.basic_order.clone(),
                        ctx.fairness_order.clone(),
                    ]),
                    (CFG_STRATEGY, vec![
                        ctx.strategy_order.clone(),
                        ctx.route_order.clone(),
                        ctx.fairness_order.clone(),
                    ]),
                ];
                for (i, (token, required)) in entries.into_iter().enumerate() {
                    let token = token.to_vec();
                    let mut value = Value::default();
                    value.insert(&ada(), BigInt::from(2_000_000i64));
                    value.insert(
                        &AssetClass { policy: ctx.settings_mint.clone(), token: token.clone() },
                        BigInt::from(1i64),
                    );
                    order_configs.insert(
                        token.clone(),
                        Arc::new(crate::sundaev4::SundaeV4OrderConfig {
                            input: crate::cardano_types::TransactionInput::new(
                                [0xE1; 32].into(),
                                i as u64,
                            ),
                            value,
                            token_name: token.clone(),
                            config: crate::sundaev4::types::OrderConfig {
                                label: token,
                                required_constraints: required,
                            },
                            slot: 1,
                        }),
                    );
                }
            }
            let _ = TEST_CTX.set(ctx);

            let exec = ScooperExecution {
                scooper_secret_key: SCOOPER_SECRET_KEY.to_string(),
                scooper_secret_key_file: None,
                scooper_stake_keyhash: None,
                submit_url: String::new(),
                fee: (3, 1000),
                protocol_share: (1, 2),
                module_scripts,
                plutus_v3_cost_model: PLUTUS_V3_COST_MODEL.to_vec(),
                slot_config: crate::sundaev4::types::SlotConfig {
                    zero_slot: 0,
                    zero_time: 0,
                    slot_length: 1000,
                },
                pool_configs: std::collections::BTreeMap::new(),
                max_tx_ex_mem: 14_000_000,
                max_tx_ex_steps: 10_000_000_000,
                max_tx_size: 16_384,
                budget_padding: (6, 5),
                blacklisted_pools: std::collections::BTreeSet::new(),
                cost_per_pool_lovelace: 0,
                cost_per_step_lovelace: 0,
                strategy_peers: Vec::new(),
                conversions: Vec::new(),
                butane: None,
                plutus_v2_cost_model: None,
                partial_fill_margin: None,
                partial_fill_fee_estimate: 2_500_000,
            };

            // Collateral: deterministic UTxO with enough ADA
            let collateral_utxo = TransactionInput {
                transaction_id: [0xCC; 32].into(),
                index: 0,
            };
            let mut collateral_value = Value::default();
            collateral_value.insert(&ada(), BigInt::from(100_000_000i64));

            // Funding UTxO: covers pool-output min-ada bumps and recycles the
            // remainder as scooper change.
            let funding_utxo = TransactionInput {
                transaction_id: [0xFD; 32].into(),
                index: 0,
            };
            let mut funding_value = Value::default();
            funding_value.insert(&ada(), BigInt::from(50_000_000i64));

            TestEnv {
                exec,
                scripts,
                butane: None,
                ref_utxo_outputs,
                language_views,
                collateral_utxo,
                collateral_value,
                funding_utxo,
                funding_value,
                order_configs,
            }
        }

        /// Build a multi-pool scoop tx and evaluate it.
        ///
        /// Returns the build result and eval result on success.
        pub fn build_and_eval(
            &self,
            batches: &[Batch],
            settings: &SundaeV4Settings,
            slot: u64,
        ) -> anyhow::Result<(MultiPoolBuildResult, EvalResult)> {
            // Non-routed convenience: wrap a slice of batches in an empty
            // ScoopPlan. Routed tests should use `build_and_eval_plan`.
            let plan = ScoopPlan {
                batches: batches.to_vec(),
                routes: Vec::new(),
                global_seq: Vec::new(),
                conversions: Vec::new(),
            };
            self.build_and_eval_plan(&plan, settings, slot)
        }

        /// Build + eval from a full ScoopPlan (with routes + global_seq).
        /// Use this when testing routed orders via the accumulator.
        /// Load the Butane runtime from the committed preview config and
        /// admit its scripts to the eval store. Returns false (test should
        /// skip) when the artifact isn't available.
        pub fn enable_butane(&mut self) -> bool {
            let Ok(raw) = std::fs::read_to_string("config/preview-v4.json") else {
                return false;
            };
            let cfg: serde_json::Value = serde_json::from_str(&raw).unwrap();
            fn find_butane(v: &serde_json::Value) -> Option<&serde_json::Value> {
                match v {
                    serde_json::Value::Object(m) => {
                        m.get("butane").or_else(|| m.values().find_map(find_butane))
                    }
                    _ => None,
                }
            }
            let Some(section) = find_butane(&cfg) else { return false };
            let Ok(mut parsed) =
                serde_json::from_value::<crate::sundaev4::butane::ButaneConfig>(section.clone())
            else {
                return false;
            };
            parsed.deployment_file = "config/butane-v2.deployment.preview.json".into();
            let Ok(rt) = crate::sundaev4::butane::ButaneRuntime::load(&parsed) else {
                return false;
            };
            for ds in rt.scripts.values() {
                self.scripts
                    .insert_with_version(&ds.script_bytes, ds.plutus_version)
                    .expect("butane script inserts");
            }
            // Preview's live V2 cost model, from the same config.
            fn find_v2(v: &serde_json::Value) -> Option<&serde_json::Value> {
                match v {
                    serde_json::Value::Object(m) => m
                        .get("plutus-v2-cost-model")
                        .or_else(|| m.values().find_map(find_v2)),
                    _ => None,
                }
            }
            let v2: Vec<i64> = find_v2(&cfg)
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .expect("v2 cost model in preview config");
            self.exec.plutus_v2_cost_model = Some(v2);
            self.butane = Some(rt);
            true
        }

        pub fn build_and_eval_plan(
            &self,
            plan: &ScoopPlan,
            settings: &SundaeV4Settings,
            slot: u64,
        ) -> anyhow::Result<(MultiPoolBuildResult, EvalResult)> {

            let build = build_multi_pool_scoop_tx(
                plan,
                settings,
                &self.exec,
                // Tests have no wall clock worth consulting: tip and "now"
                // are the same slot, which is the tip-anchored behaviour.
                crate::sundaev4::tx_builder::ValidityWindow::new(slot, slot),
                &self.language_views,
                &self.collateral_utxo,
                &self.collateral_value,
                None, // no ex_units → default budgets
                &self.ref_utxo_outputs,
                None, // fee_override
                &self.order_configs,
                &std::collections::BTreeMap::new(), // strategy_executions
                Some((self.funding_utxo.clone(), &self.funding_value)),
                self.butane.as_ref(),
            )?;

            let eval = evaluate_scoop_tx(
                &build.tx_body,
                &build.redeemers,
                &build.resolved_inputs,
                &build.resolved_ref_inputs,
                &self.scripts,
                PLUTUS_V3_COST_MODEL,
                self.exec.plutus_v2_cost_model.as_deref(),
                build.tx_hash,
                &self.exec.slot_config,
                None,
            )?;

            // Lovelace conservation: inputs == outputs + fee. Phase-2 eval
            // can't see this — the ledger rejects violations at submit
            // (ValueNotConservedUTxO), so assert it here where the suite can
            // catch fee-accounting regressions.
            {
                use num_traits::ToPrimitive;
                let ada_lo = ada();
                let in_ada: u64 = build
                    .tx_body
                    .inputs
                    .iter()
                    .map(|i| {
                        let key = crate::cardano_types::TransactionInput::new(
                            i.transaction_id,
                            i.index,
                        );
                        build
                            .resolved_inputs
                            .get(&key)
                            .map(|r| r.value.get(&ada_lo).unwrap().to_u64().unwrap_or(0))
                            .unwrap_or(0)
                    })
                    .sum();
                let out_ada: u64 = build
                    .tx_body
                    .outputs
                    .iter()
                    .map(|o| match o {
                        pallas_primitives::conway::PseudoTransactionOutput::PostAlonzo(b) => {
                            match &b.value {
                                pallas_primitives::conway::Value::Coin(c) => *c,
                                pallas_primitives::conway::Value::Multiasset(c, _) => *c,
                            }
                        }
                        pallas_primitives::conway::PseudoTransactionOutput::Legacy(_) => 0,
                    })
                    .sum();
                anyhow::ensure!(
                    in_ada == out_ada + build.tx_body.fee,
                    "lovelace not conserved: inputs {} != outputs {} + fee {} (diff {})",
                    in_ada,
                    out_ada,
                    build.tx_body.fee,
                    in_ada as i128 - out_ada as i128 - build.tx_body.fee as i128,
                );
            }

            Ok((build, eval))
        }

        /// Get the scooper's key hash (28 bytes).
        pub fn scooper_keyhash(&self) -> Vec<u8> {
            let bytes = hex::decode(&self.exec.scooper_secret_key).unwrap();
            let arr: [u8; 32] = bytes.try_into().unwrap();
            let sk = SecretKey::from(arr);
            let pk = sk.public_key();
            let pk_bytes: [u8; 32] = pk.as_ref().try_into().unwrap();
            let hash: Hash<28> = Hasher::<224>::hash(&pk_bytes);
            hash.to_vec()
        }

        /// Compute `module_state` entries for the pool datum.
        ///
        /// Each entry is `(module_script_hash, blake2b_256(config_cbor))`.
        /// The constant_product config hash uses `ConstantProductConfig { fee }`,
        /// the fee_split config hash uses `FeeSplitConfig { protocol_share }`,
        /// and fairness has an empty-array config `0x80`.
        pub fn module_state(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
            let cp_config = ConstantProductConfig {
                fee: Rational {
                    num: BigInt::from(self.exec.fee.0),
                    den: BigInt::from(self.exec.fee.1),
                },
            };
            let cp_cbor = minicbor::to_vec(&cp_config.to_plutus()).unwrap();
            let cp_hash = Hasher::<256>::hash(&cp_cbor).to_vec();

            let fs_config = FeeSplitConfig {
                protocol_share: Rational {
                    num: BigInt::from(self.exec.protocol_share.0),
                    den: BigInt::from(self.exec.protocol_share.1),
                },
            };
            let fs_cbor = minicbor::to_vec(&fs_config.to_plutus()).unwrap();
            let fs_hash = Hasher::<256>::hash(&fs_cbor).to_vec();

            // Fairness module has no config — empty CBOR array
            let fairness_hash = Hasher::<256>::hash(&[0x80]).to_vec();

            vec![
                (self.exec.module_scripts.constant_product.hash.to_vec(), cp_hash),
                (self.exec.module_scripts.fee_split.hash.to_vec(), fs_hash),
                (self.exec.module_scripts.fairness.hash.to_vec(), fairness_hash),
            ]
        }

        /// The action entry modules list: [cp_hash, fs_hash, fairness_hash].
        pub fn action_modules(&self) -> Vec<Vec<u8>> {
            vec![
                self.exec.module_scripts.constant_product.hash.to_vec(),
                self.exec.module_scripts.fee_split.hash.to_vec(),
                self.exec.module_scripts.fairness.hash.to_vec(),
            ]
        }

        /// Compute `module_state` entries for a CS pool.
        ///
        /// Same structure as CP module_state but uses CS module hash and config.
        pub fn cs_module_state(&self, prices: &[BigInt], fee: &Rational) -> Vec<(Vec<u8>, Vec<u8>)> {
            let cs_script = self.exec.module_scripts.constant_sum.as_ref()
                .expect("blueprint must include constantSum validator for CS tests");

            let cs_config = ConstantSumConfig {
                prices: prices.to_vec(),
                fee: fee.clone(),
                bounty_k: Rational { num: BigInt::from(0), den: BigInt::from(1) },
                balance_fee: Rational { num: BigInt::from(0), den: BigInt::from(1) },
            };
            let cs_cbor = minicbor::to_vec(&cs_config.to_plutus()).unwrap();
            let cs_hash = Hasher::<256>::hash(&cs_cbor).to_vec();

            let fs_config = FeeSplitConfig {
                protocol_share: Rational {
                    num: BigInt::from(self.exec.protocol_share.0),
                    den: BigInt::from(self.exec.protocol_share.1),
                },
            };
            let fs_cbor = minicbor::to_vec(&fs_config.to_plutus()).unwrap();
            let fs_hash = Hasher::<256>::hash(&fs_cbor).to_vec();

            let fairness_hash = Hasher::<256>::hash(&[0x80]).to_vec();

            vec![
                (cs_script.hash.to_vec(), cs_hash),
                (self.exec.module_scripts.fee_split.hash.to_vec(), fs_hash),
                (self.exec.module_scripts.fairness.hash.to_vec(), fairness_hash),
            ]
        }

        /// The action entry modules list for CS pools: [cs_hash, fs_hash, fairness_hash].
        pub fn cs_action_modules(&self) -> Vec<Vec<u8>> {
            let cs_script = self.exec.module_scripts.constant_sum.as_ref()
                .expect("blueprint must include constantSum validator for CS tests");
            vec![
                cs_script.hash.to_vec(),
                self.exec.module_scripts.fee_split.hash.to_vec(),
                self.exec.module_scripts.fairness.hash.to_vec(),
            ]
        }
    }

    // ─── Shared test helpers ──────────────────────────────────────────────────

    pub fn ada() -> AssetClass {
        AssetClass { policy: vec![], token: vec![] }
    }

    /// Create a token with a 28-byte policy hash (required for pallas Value building).
    pub fn token(policy_byte: u8, name_byte: u8) -> AssetClass {
        AssetClass {
            policy: vec![policy_byte; 28],
            token: vec![name_byte],
        }
    }

    pub fn token_a() -> AssetClass {
        token(0x01, 0x02)
    }

    pub fn token_b() -> AssetClass {
        token(0x03, 0x04)
    }

    pub fn token_e() -> AssetClass {
        token(0x09, 0x0A)
    }

    pub fn token_f() -> AssetClass {
        token(0x0B, 0x0C)
    }

    pub fn unit_pd() -> pallas_primitives::PlutusData {
        pallas_primitives::PlutusData::Constr(pallas_primitives::Constr {
            tag: 121,
            any_constructor: None,
            fields: MaybeIndefArray::Def(vec![]),
        })
    }

    /// Create a token-to-token pool with the proper module pipeline (CP + FS + fairness).
    ///
    /// The pool's `actions` and `module_state` reference the real module script
    /// hashes from `env`, so the on-chain validators accept the transaction.
    /// The pool value includes the pool NFT and preminted LP tokens, plus ADA
    /// for the min UTxO requirement.
    ///
    /// Note: V4 pools are token-to-token — ADA is in the value for min UTxO
    /// but is NOT one of the tracked trading pair assets.
    pub fn make_pool(
        env: &TestEnv,
        ident_byte: u8,
        tok_a: AssetClass,
        tok_a_reserve: i64,
        tok_b: AssetClass,
        tok_b_reserve: i64,
    ) -> Arc<SundaeV4Pool> {
        // 28-byte ident (padded with ident_byte)
        let ident_bytes = vec![ident_byte; 28];

        // CIP-68 token names
        let mut nft_name = vec![0x00, 0x0d, 0xe1, 0x40]; // 000de140
        nft_name.extend_from_slice(&ident_bytes);
        let mut lp_name = vec![0x00, 0x14, 0xdf, 0x10]; // 0014df10
        lp_name.extend_from_slice(&ident_bytes);

        let pool_mint_policy = env.exec.module_scripts.pool_mint.hash.to_vec();

        let nft_asset = AssetClass {
            policy: pool_mint_policy.clone(),
            token: nft_name,
        };
        let lp_asset = AssetClass {
            policy: pool_mint_policy,
            token: lp_name,
        };

        let total_lp = 1_000_000_000i64;
        let circulating_lp = 0i64;
        let preminted_lp = total_lp;

        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(50_000_000i64)); // min UTxO
        value.insert(&tok_a, BigInt::from(tok_a_reserve));
        value.insert(&tok_b, BigInt::from(tok_b_reserve));
        value.insert(&nft_asset, BigInt::from(1i64)); // pool NFT
        value.insert(&lp_asset, BigInt::from(preminted_lp)); // preminted LP tokens

        // Use ident_byte in the tx hash to make distinct inputs
        let mut tx_hash = [0u8; 32];
        tx_hash[0] = ident_byte;

        Arc::new(SundaeV4Pool {
            input: crate::cardano_types::TransactionInput::new(tx_hash.into(), 0),
            value,
            pool_datum: PoolDatum {
                assets: vec![
                    (tok_a, BigInt::from(tok_a_reserve)),
                    (tok_b, BigInt::from(tok_b_reserve)),
                ],
                total_lp: BigInt::from(total_lp),
                circulating_lp: BigInt::from(circulating_lp),
                preminted_lp: BigInt::from(preminted_lp),
                identifier: Ident::new(&ident_bytes),
                actions: vec![
                    ActionEntry {
                        tag: BigInt::from(100),
                        enabled: true,
                        modules: env.action_modules(),
                    },
                ],
                module_state: env.module_state(),
            },
            pool_type: PoolType::ConstantProduct {
                fee: Rational {
                    num: BigInt::from(env.exec.fee.0),
                    den: BigInt::from(env.exec.fee.1),
                },
            },
            slot: 100,
            fee_split_config: None,
        })
    }

    /// A concentrated-liquidity pool. Only the router/accumulator fields matter
    /// (pool_type, assets, total_lp) — module_state is left CP-shaped since the
    /// property tests exercise routing + accumulation, not on-chain eval.
    #[allow(clippy::too_many_arguments)]
    pub fn make_cl_pool(
        env: &TestEnv,
        ident_byte: u8,
        tok_a: AssetClass,
        tok_a_reserve: i64,
        tok_b: AssetClass,
        tok_b_reserve: i64,
        total_lp: i64,
        spa_num: i64,
        spa_den: i64,
        spb_num: i64,
        spb_den: i64,
        fee_num: i64,
        fee_den: i64,
    ) -> Arc<SundaeV4Pool> {
        let ident_bytes = vec![ident_byte; 28];
        let mut nft_name = vec![0x00, 0x0d, 0xe1, 0x40];
        nft_name.extend_from_slice(&ident_bytes);
        let mut lp_name = vec![0x00, 0x14, 0xdf, 0x10];
        lp_name.extend_from_slice(&ident_bytes);
        let pool_mint_policy = env.exec.module_scripts.pool_mint.hash.to_vec();
        let nft_asset = AssetClass { policy: pool_mint_policy.clone(), token: nft_name };
        let lp_asset = AssetClass { policy: pool_mint_policy, token: lp_name };

        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(50_000_000i64));
        if tok_a_reserve > 0 {
            value.insert(&tok_a, BigInt::from(tok_a_reserve));
        }
        if tok_b_reserve > 0 {
            value.insert(&tok_b, BigInt::from(tok_b_reserve));
        }
        value.insert(&nft_asset, BigInt::from(1i64));
        value.insert(&lp_asset, BigInt::from(total_lp));

        let mut tx_hash = [0u8; 32];
        tx_hash[0] = ident_byte;

        Arc::new(SundaeV4Pool {
            input: crate::cardano_types::TransactionInput::new(tx_hash.into(), 0),
            value,
            pool_datum: PoolDatum {
                assets: vec![
                    (tok_a, BigInt::from(tok_a_reserve)),
                    (tok_b, BigInt::from(tok_b_reserve)),
                ],
                total_lp: BigInt::from(total_lp),
                circulating_lp: BigInt::from(total_lp),
                preminted_lp: BigInt::from(0i64),
                identifier: Ident::new(&ident_bytes),
                actions: vec![ActionEntry {
                    tag: BigInt::from(100),
                    enabled: true,
                    modules: env.action_modules(),
                }],
                module_state: env.module_state(),
            },
            pool_type: PoolType::ConcentratedLiquidity {
                sqrt_price_a: Rational { num: BigInt::from(spa_num), den: BigInt::from(spa_den) },
                sqrt_price_b: Rational { num: BigInt::from(spb_num), den: BigInt::from(spb_den) },
                fee: Rational { num: BigInt::from(fee_num), den: BigInt::from(fee_den) },
            },
            slot: 100,
            fee_split_config: None,
        })
    }

    /// Create a constant-sum pool with the proper module pipeline (CS + FS + fairness).
    ///
    /// Similar to `make_pool` but uses CS module scripts and PoolType::ConstantSum.
    pub fn make_cs_pool(
        env: &TestEnv,
        ident_byte: u8,
        assets: Vec<(AssetClass, i64)>,
        prices: Vec<BigInt>,
        fee: Rational,
    ) -> Arc<SundaeV4Pool> {
        let ident_bytes = vec![ident_byte; 28];

        let mut nft_name = vec![0x00, 0x0d, 0xe1, 0x40];
        nft_name.extend_from_slice(&ident_bytes);
        let mut lp_name = vec![0x00, 0x14, 0xdf, 0x10];
        lp_name.extend_from_slice(&ident_bytes);

        let pool_mint_policy = env.exec.module_scripts.pool_mint.hash.to_vec();

        let nft_asset = AssetClass {
            policy: pool_mint_policy.clone(),
            token: nft_name,
        };
        let lp_asset = AssetClass {
            policy: pool_mint_policy,
            token: lp_name,
        };

        let total_lp = 1_000_000_000i64;
        let circulating_lp = 0i64;
        let preminted_lp = total_lp;

        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(50_000_000i64));
        for (asset, reserve) in &assets {
            value.insert(asset, BigInt::from(*reserve));
        }
        value.insert(&nft_asset, BigInt::from(1i64));
        value.insert(&lp_asset, BigInt::from(preminted_lp));

        let mut tx_hash = [0u8; 32];
        tx_hash[0] = ident_byte;

        let datum_assets: Vec<(AssetClass, BigInt)> = assets
            .iter()
            .map(|(a, r)| (a.clone(), BigInt::from(*r)))
            .collect();

        Arc::new(SundaeV4Pool {
            input: crate::cardano_types::TransactionInput::new(tx_hash.into(), 0),
            value,
            pool_datum: PoolDatum {
                assets: datum_assets,
                total_lp: BigInt::from(total_lp),
                circulating_lp: BigInt::from(circulating_lp),
                preminted_lp: BigInt::from(preminted_lp),
                identifier: Ident::new(&ident_bytes),
                actions: vec![
                    ActionEntry {
                        tag: BigInt::from(100),
                        enabled: true,
                        modules: env.cs_action_modules(),
                    },
                ],
                module_state: env.cs_module_state(&prices, &fee),
            },
            pool_type: PoolType::ConstantSum {
                prices,
                fee,
                bounty_k: Rational { num: BigInt::from(0), den: BigInt::from(1) },
                balance_fee: Rational { num: BigInt::from(0), den: BigInt::from(1) },
            },
            slot: 100,
            fee_split_config: None,
        })
    }

    /// Create an order that offers `offer_tok` and wants `want_tok` in return.
    ///
    /// For a token-to-token pool with assets (A, B):
    /// - To sell A for B: `make_order(token_a(), 10_000_000, token_b(), 1, slot)`
    /// - To sell B for A: `make_order(token_b(), 10_000_000, token_a(), 1, slot)`
    pub fn make_order(
        offer_tok: AssetClass,
        offer_amount: i64,
        want_tok: AssetClass,
        min_want: i64,
        slot: u64,
    ) -> Arc<SundaeV4Order> {
        make_order_with_budget(offer_tok, offer_amount, want_tok, min_want, slot, 1_500_000)
    }

    /// make_order with an explicit fee budget — partial-fill tests need
    /// budgets large enough that the pro-rata fee cap covers a fee share.
    pub fn make_order_with_budget(
        offer_tok: AssetClass,
        offer_amount: i64,
        want_tok: AssetClass,
        min_want: i64,
        slot: u64,
        budget: i64,
    ) -> Arc<SundaeV4Order> {
        let mut value = Value::default();
        // Production orders carry 5 ADA (CLI default: 2 min-UTxO + 3 budget);
        // the no-subsidy guard rejects orders that can't retain min-UTxO
        // after their fee share.
        value.insert(&ada(), BigInt::from(5_000_000i64));
        value.insert(&offer_tok, BigInt::from(offer_amount));

        // Use slot in tx hash for uniqueness
        let mut tx_hash = [0u8; 32];
        tx_hash[0] = 0xD0;
        tx_hash[1..9].copy_from_slice(&slot.to_be_bytes());

        let order = SundaeV4Order::test_swap_order(
            crate::cardano_types::TransactionInput::new(tx_hash.into(), 0),
            value,
            Multisig::Signature(vec![0xAA; 28]),
            // Full-consume fills must pay a non-self destination: PR#11's
            // swap module treats an output at the order address as a
            // continuation (partial fill), which requires remaining_offered
            // to stay positive — a full fill to Self is unsatisfiable.
            Destination::Fixed(
                crate::sundaev3::PlutusAddress {
                    payment_credential: crate::sundaev3::Credential::VerificationKey(
                        [0xAA; 28].into(),
                    ),
                    stake_credential: None,
                },
                None,
            ),
            (offer_tok, BigInt::from(offer_amount)),
            (want_tok, BigInt::from(min_want)),
            BigInt::from(budget),
            slot,
        );
        Arc::new(with_real_constraints(order, CFG_SWAP))
    }

    /// A swap expressed through the BASIC constraint module (tag 2 under the
    /// basic hash, fields `(offered list, min_received list)`). The basic
    /// validator only checks aggregate consumption/floors, so these orders
    /// may be routed, split, and blended freely — there's no route module in
    /// the CFG_BASIC preset and no swap-module full-fill semantics.
    pub fn make_basic_swap_order(
        offer_tok: AssetClass,
        offer_amount: i64,
        want_tok: AssetClass,
        min_want: i64,
        slot: u64,
    ) -> Arc<SundaeV4Order> {
        use plutus_parser::AsPlutus;

        let mut value = Value::default();
        // ADA offers ride in the same asset as the budget/min-utxo buffer:
        // sum them rather than letting the second insert clobber the first.
        if offer_tok == ada() {
            value.insert(&ada(), BigInt::from(5_000_000i64 + offer_amount));
        } else {
            value.insert(&ada(), BigInt::from(5_000_000i64));
            value.insert(&offer_tok, BigInt::from(offer_amount));
        }
        let mut tx_hash = [0u8; 32];
        tx_hash[0] = 0xD1; // distinct namespace from make_order's 0xD0
        tx_hash[1..9].copy_from_slice(&slot.to_be_bytes());

        let order = SundaeV4Order::test_swap_order(
            crate::cardano_types::TransactionInput::new(tx_hash.into(), 0),
            value,
            Multisig::Signature(vec![0xAA; 28]),
            Destination::Fixed(
                crate::sundaev3::PlutusAddress {
                    payment_credential: crate::sundaev3::Credential::VerificationKey(
                        [0xAA; 28].into(),
                    ),
                    stake_credential: None,
                },
                None,
            ),
            (offer_tok.clone(), BigInt::from(offer_amount)),
            (want_tok.clone(), BigInt::from(min_want)),
            BigInt::from(1_500_000i64),
            slot,
        );
        // Re-encode the constraint payload in the basic module's layout:
        // ctor 2 with (offered list, min_received list).
        let offered: Vec<(AssetClass, BigInt)> =
            vec![(offer_tok, BigInt::from(offer_amount))];
        let min_received: Vec<(AssetClass, BigInt)> =
            vec![(want_tok, BigInt::from(min_want))];
        let payload = PlutusData::Constr(pallas_primitives::Constr {
            tag: 123, // ctor 2 (121 + 2): scooper-side "swap" dispatch tag
            any_constructor: None,
            fields: pallas_primitives::MaybeIndefArray::Def(vec![
                offered.to_plutus(),
                min_received.to_plutus(),
            ]),
        });
        let order = with_real_constraints_payload(order, CFG_BASIC, Some(payload));
        Arc::new(order)
    }

    /// Rewrite a test order's datum to the PR#11 modular shape using the
    /// real constraint hashes from the loaded blueprint (when available):
    /// the module-specific payload keeps its slot, route gets an empty pool
    /// whitelist, fairness gets void — mirroring the CLI's constraint list
    /// builders. No-op on pre-PR#11 fixtures.
    pub fn with_real_constraints(mut order: SundaeV4Order, cfg_token: &[u8]) -> SundaeV4Order {
        let Some(Some(ctx)) = TEST_CTX.get() else {
            return order;
        };
        let payload = order
            .datum
            .constraints
            .first()
            .map(|(_, d)| d.clone())
            .expect("test order carries a constraint payload");
        let empty_list = PlutusData::Array(MaybeIndefArray::Def(vec![]));
        let void = PlutusData::Constr(pallas_primitives::Constr {
            tag: 121,
            any_constructor: None,
            fields: MaybeIndefArray::Def(vec![]),
        });
        let required: &[Vec<u8>] = match cfg_token {
            t if t == CFG_SWAP => &[
                ctx.swap_order.clone(),
                ctx.route_order.clone(),
                ctx.fairness_order.clone(),
            ],
            t if t == CFG_BASIC => &[ctx.basic_order.clone(), ctx.fairness_order.clone()],
            t if t == CFG_STRATEGY => &[
                ctx.strategy_order.clone(),
                ctx.route_order.clone(),
                ctx.fairness_order.clone(),
            ],
            _ => panic!("unknown cfg token"),
        };
        order.datum.config_token = cfg_token.to_vec();
        order.datum.constraints = required
            .iter()
            .map(|h| {
                let data = if *h == ctx.route_order {
                    empty_list.clone()
                } else if *h == ctx.fairness_order {
                    void.clone()
                } else {
                    payload.clone()
                };
                (h.clone(), data)
            })
            .collect();
        order
    }

    /// Override the route constraint's pool whitelist on a test order.
    /// `with_real_constraints` writes an empty list (unrestricted); whitelist
    /// tests swap in explicit pool idents. No-op on pre-PR#11 fixtures or
    /// orders without the route module.
    pub fn with_route_whitelist(
        order: Arc<SundaeV4Order>,
        whitelist: &[Ident],
    ) -> Arc<SundaeV4Order> {
        let Some(Some(ctx)) = TEST_CTX.get() else {
            return order;
        };
        let data = PlutusData::Array(MaybeIndefArray::Def(
            whitelist
                .iter()
                .map(|i| PlutusData::BoundedBytes(i.to_bytes().to_vec().into()))
                .collect(),
        ));
        let mut order = match Arc::try_unwrap(order) {
            Ok(o) => o,
            Err(_) => panic!("with_route_whitelist requires sole ownership of the order"),
        };
        for (h, d) in order.datum.constraints.iter_mut() {
            if *h == ctx.route_order {
                *d = data.clone();
            }
        }
        Arc::new(order)
    }

    /// Build a settings UTxO from the test env's scooper keyhash.
    ///
    /// The settings value includes the settings NFT (settingsMint policy, empty token name)
    /// so that the fairness validator can find it in reference inputs.
    /// `with_real_constraints`, but overriding the constraint payload used
    /// for the class module (basic-swap tests re-encode the swap fields in
    /// the basic module's layout).
    /// A proportional deposit expressed through the BASIC constraint module
    /// (ctor 0: `(offered list, min_received list)` where min_received names
    /// the pool's LP token). Value carries every offered asset plus the ADA
    /// buffer.
    pub fn make_basic_deposit_order(
        offered: Vec<(AssetClass, i64)>,
        lp_asset: AssetClass,
        min_lp: i64,
        slot: u64,
    ) -> Arc<SundaeV4Order> {
        use plutus_parser::AsPlutus;

        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(5_000_000i64));
        for (a, q) in &offered {
            if *a == ada() {
                let cur = value.get(&ada());
                value.insert(&ada(), &cur + BigInt::from(*q));
            } else {
                value.insert(a, BigInt::from(*q));
            }
        }

        let mut tx_hash = [0u8; 32];
        tx_hash[0] = 0xD2; // distinct namespace
        tx_hash[1..9].copy_from_slice(&slot.to_be_bytes());

        // Seed with a swap-shaped order, then rewrite payload + parsed
        // constraint into the Deposit shape.
        let (seed_offer, seed_amount) = offered[0].clone();
        let order = SundaeV4Order::test_swap_order(
            crate::cardano_types::TransactionInput::new(tx_hash.into(), 0),
            value,
            Multisig::Signature(vec![0xAA; 28]),
            Destination::Fixed(
                crate::sundaev3::PlutusAddress {
                    payment_credential: crate::sundaev3::Credential::VerificationKey(
                        [0xAA; 28].into(),
                    ),
                    stake_credential: None,
                },
                None,
            ),
            (seed_offer, BigInt::from(seed_amount)),
            (lp_asset.clone(), BigInt::from(min_lp)),
            BigInt::from(1_500_000i64),
            slot,
        );

        let offered_bi: Vec<(AssetClass, BigInt)> = offered
            .into_iter()
            .map(|(a, q)| (a, BigInt::from(q)))
            .collect();
        let min_received: Vec<(AssetClass, BigInt)> =
            vec![(lp_asset, BigInt::from(min_lp))];
        let payload = PlutusData::Constr(pallas_primitives::Constr {
            tag: 121, // ctor 0: Deposit
            any_constructor: None,
            fields: pallas_primitives::MaybeIndefArray::Def(vec![
                offered_bi.clone().to_plutus(),
                min_received.clone().to_plutus(),
            ]),
        });
        let mut order = with_real_constraints_payload(order, CFG_BASIC, Some(payload));
        order.constraint = crate::sundaev4::types::Constraint::Deposit {
            offered: offered_bi,
            min_received,
        };
        Arc::new(order)
    }

    /// A proportional withdraw through the BASIC constraint module (ctor 1:
    /// `(offered list, min_received list)` where offered is the pool's LP
    /// token).
    pub fn make_basic_withdraw_order(
        lp_asset: AssetClass,
        lp_amount: i64,
        min_received: Vec<(AssetClass, i64)>,
        slot: u64,
    ) -> Arc<SundaeV4Order> {
        use plutus_parser::AsPlutus;

        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(5_000_000i64));
        value.insert(&lp_asset, BigInt::from(lp_amount));

        let mut tx_hash = [0u8; 32];
        tx_hash[0] = 0xD3;
        tx_hash[1..9].copy_from_slice(&slot.to_be_bytes());

        let (seed_want, seed_min) = min_received[0].clone();
        let order = SundaeV4Order::test_swap_order(
            crate::cardano_types::TransactionInput::new(tx_hash.into(), 0),
            value,
            Multisig::Signature(vec![0xAA; 28]),
            Destination::Fixed(
                crate::sundaev3::PlutusAddress {
                    payment_credential: crate::sundaev3::Credential::VerificationKey(
                        [0xAA; 28].into(),
                    ),
                    stake_credential: None,
                },
                None,
            ),
            (lp_asset.clone(), BigInt::from(lp_amount)),
            (seed_want, BigInt::from(seed_min)),
            BigInt::from(1_500_000i64),
            slot,
        );

        let offered_bi: Vec<(AssetClass, BigInt)> =
            vec![(lp_asset, BigInt::from(lp_amount))];
        let min_bi: Vec<(AssetClass, BigInt)> = min_received
            .into_iter()
            .map(|(a, q)| (a, BigInt::from(q)))
            .collect();
        let payload = PlutusData::Constr(pallas_primitives::Constr {
            tag: 122, // ctor 1: Withdraw
            any_constructor: None,
            fields: pallas_primitives::MaybeIndefArray::Def(vec![
                offered_bi.clone().to_plutus(),
                min_bi.clone().to_plutus(),
            ]),
        });
        let mut order = with_real_constraints_payload(order, CFG_BASIC, Some(payload));
        order.constraint = crate::sundaev4::types::Constraint::Withdraw {
            offered: offered_bi,
            min_received: min_bi,
        };
        Arc::new(order)
    }

    pub fn with_real_constraints_payload(
        order: SundaeV4Order,
        cfg_token: &[u8],
        payload_override: Option<PlutusData>,
    ) -> SundaeV4Order {
        let mut order = with_real_constraints(order, cfg_token);
        if let Some(payload) = payload_override {
            let Some(Some(ctx)) = TEST_CTX.get() else { return order };
            for (h, data) in order.datum.constraints.iter_mut() {
                if *h == ctx.basic_order || *h == ctx.swap_order {
                    *data = payload.clone();
                }
            }
        }
        order
    }

    pub fn make_settings(_env: &TestEnv, scooper_keyhash: &[u8]) -> SundaeV4Settings {
        // The settings NFT is minted by the settingsMint script.
        // We extract its policy from the settings ScriptRefInfo hash in module_scripts.
        // Actually, the settings NFT policy is separate from the settings validator hash.
        // On devnet: settingsMint = 35a98a94fb936993259612746054dd4b80c6fe33f3163780db1a9e1e
        // This is baked into the validator, so we need it from the blueprint.
        // For the test harness, we'll hardcode the devnet settingsMint policy.
        // The fairness validator looks for inputs containing this NFT policy with
        // any token name.
        let settings_nft_policy = TEST_CTX
            .get()
            .and_then(|c| c.as_ref())
            .map(|c| c.settings_mint.clone())
            .unwrap_or_else(|| {
                hex::decode("35a98a94fb936993259612746054dd4b80c6fe33f3163780db1a9e1e").unwrap()
            });
        let settings_nft = AssetClass {
            policy: settings_nft_policy,
            token: vec![], // empty token name
        };

        SundaeV4Settings {
            input: crate::cardano_types::TransactionInput::new([0xEE; 32].into(), 0),
            value: {
                let mut v = Value::default();
                v.insert(&ada(), BigInt::from(10_000_000i64));
                v.insert(&settings_nft, BigInt::from(1i64));
                v
            },
            datum: SettingsDatum {
                settings_admin: Multisig::Signature(vec![0xFF; 28]),
                treasury_admin: Multisig::Signature(vec![0xFF; 28]),
                authorized_scoopers: Some(vec![scooper_keyhash.to_vec()]),
                extension: PlutusData::Constr(pallas_primitives::Constr {
                    tag: 121,
                    any_constructor: None,
                    fields: pallas_codec::utils::MaybeIndefArray::Def(vec![]),
                }),
            },
            slot: 1,
        }
    }
}
