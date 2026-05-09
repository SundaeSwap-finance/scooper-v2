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
    use crate::sundaev4::batch::Batch;
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

    /// Complete test environment for building and evaluating scoop transactions.
    pub struct TestEnv {
        pub exec: ScooperExecution,
        pub scripts: ScriptStore,
        pub ref_utxo_outputs: BTreeMap<crate::cardano_types::TransactionInput, crate::cardano_types::TransactionOutput>,
        pub language_views: Vec<u8>,
        pub collateral_utxo: TransactionInput,
        pub collateral_value: Value,
    }

    impl TestEnv {
        /// Load from a blueprint fixture file.
        pub fn from_blueprint_file(path: &str) -> Self {
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

            let exec = ScooperExecution {
                scooper_secret_key: SCOOPER_SECRET_KEY.to_string(),
                scooper_secret_key_file: None,
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
            };

            // Collateral: deterministic UTxO with enough ADA
            let collateral_utxo = TransactionInput {
                transaction_id: [0xCC; 32].into(),
                index: 0,
            };
            let mut collateral_value = Value::default();
            collateral_value.insert(&ada(), BigInt::from(100_000_000i64));

            TestEnv {
                exec,
                scripts,
                ref_utxo_outputs,
                language_views,
                collateral_utxo,
                collateral_value,
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
            let build = build_multi_pool_scoop_tx(
                batches,
                settings,
                &self.exec,
                slot,
                &self.language_views,
                &self.collateral_utxo,
                &self.collateral_value,
                None, // no ex_units → default budgets
                &self.ref_utxo_outputs,
            )?;

            let eval = evaluate_scoop_tx(
                &build.tx_body,
                &build.redeemers,
                &build.resolved_inputs,
                &build.resolved_ref_inputs,
                &self.scripts,
                PLUTUS_V3_COST_MODEL,
                build.tx_hash,
                &self.exec.slot_config,
            )?;

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
            },
            slot: 100,
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
        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(2_000_000i64)); // min UTxO for order
        value.insert(&offer_tok, BigInt::from(offer_amount));

        // Use slot in tx hash for uniqueness
        let mut tx_hash = [0u8; 32];
        tx_hash[0] = 0xD0;
        tx_hash[1..9].copy_from_slice(&slot.to_be_bytes());

        Arc::new(SundaeV4Order::test_swap_order(
            crate::cardano_types::TransactionInput::new(tx_hash.into(), 0),
            value,
            Multisig::Signature(vec![0xAA; 28]),
            Destination::SelfDestination,
            (offer_tok, BigInt::from(offer_amount)),
            (want_tok, BigInt::from(min_want)),
            BigInt::from(1_500_000i64),
            slot,
        ))
    }

    /// Build a settings UTxO from the test env's scooper keyhash.
    ///
    /// The settings value includes the settings NFT (settingsMint policy, empty token name)
    /// so that the fairness validator can find it in reference inputs.
    pub fn make_settings(_env: &TestEnv, scooper_keyhash: &[u8]) -> SundaeV4Settings {
        // The settings NFT is minted by the settingsMint script.
        // We extract its policy from the settings ScriptRefInfo hash in module_scripts.
        // Actually, the settings NFT policy is separate from the settings validator hash.
        // On devnet: settingsMint = 35a98a94fb936993259612746054dd4b80c6fe33f3163780db1a9e1e
        // This is baked into the validator, so we need it from the blueprint.
        // For the test harness, we'll hardcode the devnet settingsMint policy.
        // The fairness validator looks for inputs containing this NFT policy with
        // any token name.
        let settings_nft_policy = hex::decode("35a98a94fb936993259612746054dd4b80c6fe33f3163780db1a9e1e").unwrap();
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
                treasury_address: vec![0x60, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                    0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
                authorized_scoopers: Some(vec![scooper_keyhash.to_vec()]),
                order_modules: vec![],
                min_share_batcher: BigInt::from(0),
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
