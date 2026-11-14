//! The v4 script configuration: the four boot-fixed validators, and a
//! registry of every other script keyed by script hash.
//!
//! A pool names the modules it uses by hash in its action map, and an order
//! names its constraint modules by hash in its datum. More than one hash of
//! one kind can be live on a network at once (for example a `fee_split` for
//! the constant-sum pools and a newer `fee_split` for the
//! concentrated-liquidity pools). The scooper therefore looks up a script by
//! the hash that the pool or order names, and chooses behaviour from the
//! [`ModuleKind`] of that hash.
//!
//! Config form, under `protocol.v4.execution.module-scripts`: each key is a
//! role. A role key maps to one `{hash, ref-utxo}` object, or to a JSON array
//! of them. The four boot-fixed roles (`pool`, `order`, `pool-mint`,
//! `settings`) take one object only.

use std::collections::BTreeMap;

use pallas_addresses::ScriptHash;

use crate::bigint::BigInt;
use crate::sundaev4::types::PoolDatum;

/// The behaviour class of a module script. The scooper selects the entry
/// shape, the math and the redeemer form from the kind. A future change in
/// behaviour between two versions of one module is a new variant here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub enum ModuleKind {
    ConstantProduct,
    ConstantSum,
    Stableswap,
    ConcentratedLiquidity,
    FeeSplit,
    Fairness,
    Oracle,
    SwapOrder,
    BasicOrder,
    RouteOrder,
    FairnessOrder,
    StrategyOrder,
    FeeConstraint,
}

impl ModuleKind {
    pub const ALL: [ModuleKind; 13] = [
        ModuleKind::ConstantProduct,
        ModuleKind::ConstantSum,
        ModuleKind::Stableswap,
        ModuleKind::ConcentratedLiquidity,
        ModuleKind::FeeSplit,
        ModuleKind::Fairness,
        ModuleKind::Oracle,
        ModuleKind::SwapOrder,
        ModuleKind::BasicOrder,
        ModuleKind::RouteOrder,
        ModuleKind::FairnessOrder,
        ModuleKind::StrategyOrder,
        ModuleKind::FeeConstraint,
    ];

    /// The canonical config key for this kind.
    pub fn config_key(self) -> &'static str {
        match self {
            ModuleKind::ConstantProduct => "constant-product",
            ModuleKind::ConstantSum => "constant-sum",
            ModuleKind::Stableswap => "stableswap",
            ModuleKind::ConcentratedLiquidity => "banded-concentrated-liquidity",
            ModuleKind::FeeSplit => "fee-split",
            ModuleKind::Fairness => "fairness",
            ModuleKind::Oracle => "oracle",
            ModuleKind::SwapOrder => "swap-order",
            ModuleKind::BasicOrder => "basic-order",
            ModuleKind::RouteOrder => "route-order",
            ModuleKind::FairnessOrder => "fairness-order",
            ModuleKind::StrategyOrder => "strategy-order",
            ModuleKind::FeeConstraint => "fee-constraint",
        }
    }

    /// The kind a config key names. `concentrated-liquidity` is an alias of
    /// `banded-concentrated-liquidity`.
    pub fn from_config_key(key: &str) -> Option<ModuleKind> {
        if key == "concentrated-liquidity" {
            return Some(ModuleKind::ConcentratedLiquidity);
        }
        ModuleKind::ALL.into_iter().find(|k| k.config_key() == key)
    }

    /// True for the pool invariant modules: the module that a pool action
    /// lists first, and that sets the pool's math.
    pub fn is_invariant(self) -> bool {
        matches!(
            self,
            ModuleKind::ConstantProduct
                | ModuleKind::ConstantSum
                | ModuleKind::Stableswap
                | ModuleKind::ConcentratedLiquidity
        )
    }

    /// True for the modules a pool action can list.
    pub fn is_pool_module(self) -> bool {
        self.is_invariant()
            || matches!(
                self,
                ModuleKind::FeeSplit | ModuleKind::Fairness | ModuleKind::Oracle
            )
    }

    /// True for the order-constraint modules that an OrderConfig can list.
    pub fn is_order_constraint(self) -> bool {
        !self.is_pool_module()
    }
}

impl std::fmt::Display for ModuleKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.config_key())
    }
}

#[serde_with::serde_as]
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ScriptRefInfo {
    pub hash: ScriptHash,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub ref_utxo: crate::cardano_types::TransactionInput,
    /// Hex-encoded CBOR-wrapped script (double-wrapped: CBOR bytestring containing FLAT-encoded UPLC)
    /// Hex-encoded script CBOR, populated from blueprint. Retained for serde round-trip.
    #[serde(default)]
    #[allow(dead_code)]
    pub script_cbor: Option<String>,
}

/// One registered module script.
#[derive(Clone, Debug)]
pub struct ModuleEntry {
    pub kind: ModuleKind,
    pub script: ScriptRefInfo,
}

/// Every non-boot-fixed script, keyed by script hash. One hash has exactly
/// one kind. One kind can have many hashes.
#[derive(Clone, Debug, Default)]
pub struct ModuleRegistry {
    entries: BTreeMap<ScriptHash, ModuleEntry>,
}

fn hash_of_slice(hash: &[u8]) -> Option<ScriptHash> {
    let arr: [u8; 28] = hash.try_into().ok()?;
    Some(ScriptHash::new(arr))
}

impl ModuleRegistry {
    /// Add a script. Fails when the hash is already registered, under the
    /// same kind or a different kind.
    pub fn insert(&mut self, kind: ModuleKind, script: ScriptRefInfo) -> Result<(), String> {
        if let Some(prev) = self.entries.get(&script.hash) {
            return Err(if prev.kind == kind {
                format!(
                    "module script {} is listed twice under '{kind}'",
                    script.hash
                )
            } else {
                format!(
                    "module script {} is listed under both '{}' and '{kind}'",
                    script.hash, prev.kind
                )
            });
        }
        self.entries.insert(script.hash, ModuleEntry { kind, script });
        Ok(())
    }

    /// The registered entry for a hash.
    pub fn get(&self, hash: &[u8]) -> Option<&ModuleEntry> {
        self.entries.get(&hash_of_slice(hash)?)
    }

    /// The kind of a registered hash.
    pub fn kind_of(&self, hash: &[u8]) -> Option<ModuleKind> {
        self.get(hash).map(|e| e.kind)
    }

    /// Every script of one kind, in hash order.
    pub fn of_kind(&self, kind: ModuleKind) -> impl Iterator<Item = &ScriptRefInfo> + '_ {
        self.entries.values().filter(move |e| e.kind == kind).map(|e| &e.script)
    }

    /// True when at least one script of `kind` is registered.
    pub fn has_kind(&self, kind: ModuleKind) -> bool {
        self.of_kind(kind).next().is_some()
    }

    /// Every hash of one kind, as raw bytes.
    pub fn hashes_of_kind(&self, kind: ModuleKind) -> Vec<Vec<u8>> {
        self.of_kind(kind).map(|s| s.hash.as_ref().to_vec()).collect()
    }

    /// Every registered entry, in hash order.
    pub fn iter(&self) -> impl Iterator<Item = &ModuleEntry> + '_ {
        self.entries.values()
    }
}

#[derive(Clone, Debug)]
pub struct ModuleScripts {
    pub pool: ScriptRefInfo,
    pub order: ScriptRefInfo,
    pub pool_mint: ScriptRefInfo,
    pub settings: ScriptRefInfo,
    /// Every other script, keyed by hash.
    pub modules: ModuleRegistry,
}

/// The config value of one role key: one script, or a list of them.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum OneOrMany {
    One(ScriptRefInfo),
    Many(Vec<ScriptRefInfo>),
}

impl<'de> serde::Deserialize<'de> for ModuleScripts {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let raw: BTreeMap<String, OneOrMany> = BTreeMap::deserialize(d)?;
        ModuleScripts::from_role_map(raw.into_iter().map(|(k, v)| {
            let scripts = match v {
                OneOrMany::One(s) => vec![s],
                OneOrMany::Many(v) => v,
            };
            (k, scripts)
        }))
        .map_err(D::Error::custom)
    }
}

impl ModuleScripts {
    /// Build from `(role key, scripts)` pairs. The four boot-fixed roles
    /// take exactly one script. Every other key must name a [`ModuleKind`].
    /// `fee-split` and `fairness` must have at least one script.
    pub fn from_role_map(
        roles: impl IntoIterator<Item = (String, Vec<ScriptRefInfo>)>,
    ) -> Result<Self, String> {
        let mut pool = None;
        let mut order = None;
        let mut pool_mint = None;
        let mut settings = None;
        let mut modules = ModuleRegistry::default();
        let mut fixed_hashes: BTreeMap<ScriptHash, &'static str> = BTreeMap::new();
        for (key, scripts) in roles {
            let fixed = match key.as_str() {
                "pool" => Some(("pool", &mut pool)),
                "order" => Some(("order", &mut order)),
                "pool-mint" => Some(("pool-mint", &mut pool_mint)),
                "settings" => Some(("settings", &mut settings)),
                _ => None,
            };
            if let Some((name, slot)) = fixed {
                let mut scripts = scripts.into_iter();
                let (Some(s), None) = (scripts.next(), scripts.next()) else {
                    return Err(format!("module-scripts.{name} must be exactly one script"));
                };
                if let Some(prev) = fixed_hashes.insert(s.hash, name) {
                    return Err(format!(
                        "script {} is listed under both '{prev}' and '{name}'",
                        s.hash
                    ));
                }
                *slot = Some(s);
                continue;
            }
            let kind = ModuleKind::from_config_key(&key)
                .ok_or_else(|| format!("module-scripts: unknown role '{key}'"))?;
            for s in scripts {
                modules.insert(kind, s)?;
            }
        }
        for e in modules.iter() {
            if let Some(name) = fixed_hashes.get(&e.script.hash) {
                return Err(format!(
                    "script {} is listed under both '{name}' and '{}'",
                    e.script.hash, e.kind
                ));
            }
        }
        for kind in [ModuleKind::FeeSplit, ModuleKind::Fairness] {
            if !modules.has_kind(kind) {
                return Err(format!(
                    "module-scripts.{kind} must list at least one script"
                ));
            }
        }
        let missing = |name: &str| format!("module-scripts.{name} is required");
        Ok(ModuleScripts {
            pool: pool.ok_or_else(|| missing("pool"))?,
            order: order.ok_or_else(|| missing("order"))?,
            pool_mint: pool_mint.ok_or_else(|| missing("pool-mint"))?,
            settings: settings.ok_or_else(|| missing("settings"))?,
            modules,
        })
    }

    /// The kind of a registered module hash.
    pub fn kind_of(&self, hash: &[u8]) -> Option<ModuleKind> {
        self.modules.kind_of(hash)
    }

    /// The registered module script for a hash.
    pub fn get(&self, hash: &[u8]) -> Option<&ScriptRefInfo> {
        self.modules.get(hash).map(|e| &e.script)
    }

    /// Every registered module script of one kind.
    #[cfg(test)]
    pub fn of_kind(&self, kind: ModuleKind) -> impl Iterator<Item = &ScriptRefInfo> + '_ {
        self.modules.of_kind(kind)
    }

    /// The first script of `kind` in hash order. Test fixtures configure one
    /// script per kind; production code must look up by the hash a pool or
    /// order names instead.
    #[cfg(test)]
    pub fn first_of(&self, kind: ModuleKind) -> Option<&ScriptRefInfo> {
        self.of_kind(kind).next()
    }

    /// Every script: the four boot-fixed ones, then the registry.
    pub fn all_scripts(&self) -> impl Iterator<Item = &ScriptRefInfo> + '_ {
        [&self.pool, &self.order, &self.pool_mint, &self.settings]
            .into_iter()
            .chain(self.modules.iter().map(|e| &e.script))
    }

    /// Resolve the modules a pool's scoop action uses. See [`PoolModules`].
    pub fn pool_modules(&self, pool_datum: &PoolDatum) -> Result<PoolModules, String> {
        PoolModules::resolve(pool_datum, self)
    }

    /// The pool's scoop action: the first enabled action whose first module
    /// is a registered invariant module.
    pub fn scoop_action<'a>(
        &self,
        pool_datum: &'a PoolDatum,
    ) -> Option<&'a crate::sundaev4::types::ActionEntry> {
        pool_datum.actions.iter().filter(|a| a.enabled).find(|a| {
            a.modules.first().and_then(|h| self.kind_of(h)).is_some_and(ModuleKind::is_invariant)
        })
    }

    /// The pool's invariant module: its kind and hash. This is the
    /// classification step of [`ModuleScripts::pool_modules`] only. The
    /// other modules of the action are not checked.
    pub fn pool_invariant(&self, pool_datum: &PoolDatum) -> Option<(ModuleKind, ScriptHash)> {
        let first = self.scoop_action(pool_datum)?.modules.first()?;
        Some((self.kind_of(first)?, hash_of_slice(first)?))
    }

    /// The hash of the first module of `kind` that the pool's scoop action
    /// lists. The other modules of the action are not checked.
    pub fn pool_module_of_kind(
        &self,
        pool_datum: &PoolDatum,
        kind: ModuleKind,
    ) -> Option<ScriptHash> {
        self.scoop_action(pool_datum)?
            .modules
            .iter()
            .find(|h| self.kind_of(h) == Some(kind))
            .and_then(|h| hash_of_slice(h))
    }
}

/// The modules one pool's scoop action lists, resolved against the config.
///
/// The scoop action is the first enabled action whose first module is a
/// registered invariant module (CP, CS, stableswap or CL). Each module that
/// action lists must be a registered pool module, and no kind can appear
/// twice. A pool that fails these checks is not scoopable by this scooper.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolModules {
    /// The tag of the scoop action. The pool spend redeemer carries it.
    pub action_tag: BigInt,
    /// The modules the scoop action lists, in list order. The first is the
    /// invariant module.
    pub modules: Vec<(ModuleKind, ScriptHash)>,
}

impl PoolModules {
    fn resolve(pool_datum: &PoolDatum, scripts: &ModuleScripts) -> Result<PoolModules, String> {
        let action = scripts.scoop_action(pool_datum).ok_or_else(|| {
            let firsts: Vec<String> = pool_datum
                .actions
                .iter()
                .filter(|a| a.enabled)
                .filter_map(|a| a.modules.first().map(hex::encode))
                .collect();
            format!(
                "no enabled action lists a configured invariant module first \
                     (first modules: [{}])",
                firsts.join(", ")
            )
        })?;
        let mut modules: Vec<(ModuleKind, ScriptHash)> = Vec::with_capacity(action.modules.len());
        for (i, h) in action.modules.iter().enumerate() {
            let kind = scripts.kind_of(h).ok_or_else(|| {
                format!(
                    "action {} lists module {} that the config does not know",
                    action.tag,
                    hex::encode(h)
                )
            })?;
            if !kind.is_pool_module() {
                return Err(format!(
                    "action {} lists module {} of kind '{kind}', which is not a pool module",
                    action.tag,
                    hex::encode(h)
                ));
            }
            if i > 0 && kind.is_invariant() {
                return Err(format!(
                    "action {} lists a second invariant module {}",
                    action.tag,
                    hex::encode(h)
                ));
            }
            if modules.iter().any(|(k, _)| *k == kind) {
                return Err(format!("action {} lists two '{kind}' modules", action.tag));
            }
            // `kind_of` succeeded, so the slice is 28 bytes.
            modules.push((kind, hash_of_slice(h).expect("registered hash is 28 bytes")));
        }
        Ok(PoolModules {
            action_tag: action.tag.clone(),
            modules,
        })
    }

    /// The invariant module: its kind and hash.
    pub fn invariant(&self) -> (ModuleKind, &ScriptHash) {
        let (k, h) = &self.modules[0];
        (*k, h)
    }

    /// The hash of the listed module of `kind`, if the action lists one.
    pub fn hash_of(&self, kind: ModuleKind) -> Option<&ScriptHash> {
        self.modules.iter().find(|(k, _)| *k == kind).map(|(_, h)| h)
    }

    pub fn fee_split(&self) -> Option<&ScriptHash> {
        self.hash_of(ModuleKind::FeeSplit)
    }

    pub fn fairness(&self) -> Option<&ScriptHash> {
        self.hash_of(ModuleKind::Fairness)
    }

    pub fn oracle(&self) -> Option<&ScriptHash> {
        self.hash_of(ModuleKind::Oracle)
    }
}

