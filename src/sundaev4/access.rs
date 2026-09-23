//! Operator-configured per-pool trading allowlists.
//!
//! A pool named in the config is closed: the scooper serves an order against
//! it only when every credential that can act on that order is listed. This is
//! scooper policy, not a chain rule — a denied order stays valid on chain, so
//! the guarantee holds only while every authorized scooper runs the same list.

use std::collections::{BTreeMap, BTreeSet};

use num_traits::cast::ToPrimitive;

use crate::multisig::Multisig;
use crate::sundaev3::{Credential, Ident};
use crate::sundaev4::types::{Destination, StrategyConstraints, SundaeV4Order};

/// Credentials permitted to trade on one pool: 28-byte key or script hashes.
/// A script hash here satisfies the destination check for everything that
/// script pays, so the script's own rules decide who receives the assets.
#[derive(Clone, Debug, Default)]
pub struct PoolAllowlist {
    pub credentials: BTreeSet<Vec<u8>>,
}

/// Allowlists by pool ident. A pool absent from the map is unrestricted.
///
/// Configured as hex strings but held as bytes, so a key's hex casing can't
/// decide whether it matches.
#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(try_from = "BTreeMap<String, RawPoolAllowlist>")]
pub struct PoolAllowlists(pub BTreeMap<Ident, PoolAllowlist>);

#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct RawPoolAllowlist {
    credentials: BTreeSet<String>,
}

impl TryFrom<BTreeMap<String, RawPoolAllowlist>> for PoolAllowlists {
    type Error = String;

    fn try_from(raw: BTreeMap<String, RawPoolAllowlist>) -> Result<Self, String> {
        let mut out = BTreeMap::new();
        for (ident, list) in raw {
            let credentials =
                list.credentials.iter().map(|c| hash28(c)).collect::<Result<_, _>>()?;
            if out.insert(Ident::new(&hash28(&ident)?), PoolAllowlist { credentials }).is_some() {
                return Err(format!("pool-allowlists names pool {ident} twice"));
            }
        }
        Ok(Self(out))
    }
}

fn hash28(s: &str) -> Result<Vec<u8>, String> {
    match hex::decode(s) {
        Ok(b) if b.len() == 28 => Ok(b),
        _ => Err(format!("pool-allowlists: {s:?} is not a 28-byte hex hash")),
    }
}

impl PoolAllowlists {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn is_restricted(&self, pool: &Ident) -> bool {
        self.0.contains_key(pool)
    }

    /// Whether the scooper may serve `order` against `pool`.
    ///
    /// `strategy` is the order's decoded strategy constraints, when it has
    /// them; dispatch drops the `Constraint::Strategy` variant when it
    /// synthesizes a swap, so the caller carries them separately.
    pub fn permits(
        &self,
        pool: &Ident,
        order: &SundaeV4Order,
        strategy: Option<&StrategyConstraints>,
    ) -> bool {
        let Some(list) = self.0.get(pool) else {
            return true;
        };
        let listed = &list.credentials;

        if satisfiable_without_listed(&order.datum.owner, listed) {
            return false;
        }
        if !destination_listed(&order.datum.destination, listed) {
            return false;
        }
        match strategy {
            None => true,
            Some(s) => {
                !satisfiable_without_listed(&s.auth, listed)
                    && s.final_destinations.iter().all(|d| destination_listed(d, listed))
            }
        }
    }

    /// Drop the pools `order` may not trade against.
    ///
    /// Must be applied to the map the router receives, never to an upstream
    /// one: `Accumulator::current_pool_view` re-inserts every pool the batch
    /// has already touched, so a pool filtered out before that overlay comes
    /// back the moment one permitted order has used it.
    pub fn retain_visible<P>(
        &self,
        mut pools: BTreeMap<Ident, P>,
        order: &SundaeV4Order,
        strategy: Option<&StrategyConstraints>,
    ) -> BTreeMap<Ident, P> {
        if self.is_empty() {
            return pools;
        }
        pools.retain(|ident, _| self.permits(ident, order, strategy));
        pools
    }
}

/// Whether `m` can be satisfied by someone holding none of `listed`.
///
/// Testing leaves for membership instead would pass any node with no
/// credential leaves — `AllOf([])`, `AtLeast(0, [])`, a bare `Before` — each of
/// which anyone satisfies.
fn satisfiable_without_listed(m: &Multisig, listed: &BTreeSet<Vec<u8>>) -> bool {
    match m {
        Multisig::Signature(k) | Multisig::Script(k) => !listed.contains(k),
        // A time bound restricts when, never who.
        Multisig::Before(_) | Multisig::After(_) => true,
        Multisig::AllOf(xs) => xs.iter().all(|x| satisfiable_without_listed(x, listed)),
        Multisig::AnyOf(xs) => xs.iter().any(|x| satisfiable_without_listed(x, listed)),
        Multisig::AtLeast(n, xs) => match n.clone().unwrap().to_u64() {
            // A threshold that doesn't fit u64 is either negative (anyone
            // clears it) or past any reachable count; deny on both.
            None => true,
            Some(n) => {
                xs.iter().filter(|x| satisfiable_without_listed(x, listed)).count() as u64 >= n
            }
        },
    }
}

/// Whether a destination pays a listed credential.
///
/// `SelfDestination` returns the assets to the order script under the order's
/// own datum, so the owner check is what governs it.
fn destination_listed(dest: &Destination, listed: &BTreeSet<Vec<u8>>) -> bool {
    match dest {
        Destination::SelfDestination => true,
        Destination::Fixed(addr, _) => {
            let hash = match &addr.payment_credential {
                Credential::VerificationKey(h) => h.as_slice(),
                Credential::Script(h) => h.as_slice(),
            };
            listed.contains(hash)
        }
    }
}

/// Log each configured pool's status, and warn where chain settings let other
/// parties fill what this scooper declines.
pub fn log_config(
    allowlists: &PoolAllowlists,
    authorized_scoopers: Option<&Vec<Multisig>>,
    indexed_pools: &BTreeMap<Ident, impl Sized>,
) {
    if allowlists.is_empty() {
        return;
    }
    if authorized_scoopers.is_none() {
        tracing::warn!(
            "pool-allowlists are configured but the settings datum authorizes any \
             scooper: anyone can fill orders this scooper declines",
        );
    }
    for (ident, list) in &allowlists.0 {
        if indexed_pools.contains_key(ident) {
            tracing::info!(
                pool = %ident,
                credentials = list.credentials.len(),
                "pool is allowlist-restricted",
            );
        } else {
            tracing::warn!(
                pool = %ident,
                "pool-allowlists names a pool that is not indexed; if this ident is \
                 mistyped the pool is not restricted at all",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bigint::BigInt;
    use crate::cardano_types::{AssetClass, TransactionInput, Value};
    use crate::sundaev3::PlutusAddress;

    const ALICE: [u8; 28] = [0xAA; 28];
    const BOB: [u8; 28] = [0xBB; 28];
    const STRANGER: [u8; 28] = [0xCC; 28];
    const POOL: [u8; 28] = [0x11; 28];

    fn listed(hashes: &[[u8; 28]]) -> BTreeSet<Vec<u8>> {
        hashes.iter().map(|h| h.to_vec()).collect()
    }

    fn sig(h: [u8; 28]) -> Multisig {
        Multisig::Signature(h.to_vec())
    }

    fn pool() -> Ident {
        Ident::new(&POOL)
    }

    fn ada() -> AssetClass {
        AssetClass {
            policy: vec![],
            token: vec![],
        }
    }

    fn fixed_to(h: [u8; 28]) -> Destination {
        Destination::Fixed(
            PlutusAddress {
                payment_credential: Credential::VerificationKey(h.into()),
                stake_credential: None,
            },
            None,
        )
    }

    fn order(owner: Multisig, destination: Destination) -> SundaeV4Order {
        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(10_000_000i64));
        SundaeV4Order::test_swap_order(
            TransactionInput::new([0x01; 32].into(), 0),
            value,
            owner,
            destination,
            (ada(), BigInt::from(5_000_000i64)),
            (
                AssetClass {
                    policy: vec![0x01; 28],
                    token: b"TOK".to_vec(),
                },
                BigInt::from(1i64),
            ),
            BigInt::from(2_000_000i64),
            1,
        )
    }

    /// Alice and Bob are listed on POOL; STRANGER never is.
    fn allowlists() -> PoolAllowlists {
        PoolAllowlists(BTreeMap::from([(
            pool(),
            PoolAllowlist {
                credentials: listed(&[ALICE, BOB]),
            },
        )]))
    }

    // ── Config parsing ───────────────────────────────────────────────────

    fn parse(json: serde_json::Value) -> Result<PoolAllowlists, serde_json::Error> {
        serde_json::from_value(json)
    }

    #[test]
    fn uppercase_hex_still_restricts() {
        // POOL's hex (0x11…) has no letters to change case.
        let lettered = Ident::new(&[0xAB; 28]);
        let lists = parse(serde_json::json!({
            hex::encode_upper(lettered.to_bytes()): { "credentials": [hex::encode_upper(ALICE)] }
        }))
        .unwrap();
        assert!(lists.is_restricted(&lettered));
        assert!(lists.permits(&lettered, &order(sig(ALICE), fixed_to(ALICE)), None));
        assert!(!lists.permits(&lettered, &order(sig(STRANGER), fixed_to(STRANGER)), None));
    }

    #[test]
    fn keys_differing_only_in_case_are_one_pool() {
        // POOL's hex (0x11…) has no letters to change case.
        let err = parse(serde_json::json!({
            hex::encode(ALICE): { "credentials": [] },
            hex::encode_upper(ALICE): { "credentials": [] },
        }))
        .unwrap_err();
        assert!(err.to_string().contains("twice"), "{err}");
    }

    #[test]
    fn non_hash_entries_are_rejected() {
        for bad in [
            serde_json::json!({ "11": { "credentials": [] } }),
            serde_json::json!({ "not hex": { "credentials": [] } }),
            serde_json::json!({ hex::encode(POOL): { "credentials": ["aabb"] } }),
            serde_json::json!({ hex::encode(POOL): { "credentails": [] } }),
        ] {
            assert!(parse(bad.clone()).is_err(), "accepted {bad}");
        }
    }

    // ── Multisig satisfiability ──────────────────────────────────────────

    #[test]
    fn listed_signature_is_not_satisfiable_without_it() {
        let l = listed(&[ALICE]);
        assert!(!satisfiable_without_listed(&sig(ALICE), &l));
        assert!(satisfiable_without_listed(&sig(STRANGER), &l));
    }

    #[test]
    fn any_of_needs_every_branch_listed() {
        let l = listed(&[ALICE, BOB]);
        assert!(!satisfiable_without_listed(
            &Multisig::AnyOf(vec![sig(ALICE), sig(BOB)]),
            &l
        ));
        assert!(satisfiable_without_listed(
            &Multisig::AnyOf(vec![sig(ALICE), sig(STRANGER)]),
            &l
        ));
    }

    #[test]
    fn all_of_needs_only_one_listed_branch() {
        // A stranger cannot act alone: alice's signature is still required.
        let l = listed(&[ALICE]);
        assert!(!satisfiable_without_listed(
            &Multisig::AllOf(vec![sig(ALICE), sig(STRANGER)]),
            &l
        ));
    }

    #[test]
    fn at_least_one_of_listed_and_stranger_is_open() {
        let l = listed(&[ALICE]);
        assert!(satisfiable_without_listed(
            &Multisig::AtLeast(BigInt::from(1i64), vec![sig(ALICE), sig(STRANGER)]),
            &l
        ));
        assert!(!satisfiable_without_listed(
            &Multisig::AtLeast(BigInt::from(2i64), vec![sig(ALICE), sig(STRANGER)]),
            &l
        ));
    }

    #[test]
    fn vacuous_nodes_are_open_to_anyone() {
        let l = listed(&[ALICE]);
        // `all` over an empty list holds, so nobody has to sign.
        assert!(satisfiable_without_listed(&Multisig::AllOf(vec![]), &l));
        // A zero threshold is met by zero satisfied branches.
        assert!(satisfiable_without_listed(
            &Multisig::AtLeast(BigInt::from(0i64), vec![]),
            &l
        ));
        assert!(satisfiable_without_listed(
            &Multisig::AtLeast(BigInt::from(0i64), vec![sig(ALICE)]),
            &l
        ));
        // A negative threshold cannot be read as a count.
        assert!(satisfiable_without_listed(
            &Multisig::AtLeast(BigInt::from(-1i64), vec![sig(ALICE)]),
            &l
        ));
    }

    #[test]
    fn time_bounds_restrict_when_not_who() {
        let l = listed(&[ALICE]);
        assert!(satisfiable_without_listed(
            &Multisig::Before(BigInt::from(i64::MAX)),
            &l
        ));
        assert!(satisfiable_without_listed(
            &Multisig::After(BigInt::from(0i64)),
            &l
        ));
        // A time bound beside a listed signature under AllOf still requires it.
        assert!(!satisfiable_without_listed(
            &Multisig::AllOf(vec![sig(ALICE), Multisig::Before(BigInt::from(i64::MAX))]),
            &l
        ));
        // Under AnyOf it widens the owner to everyone.
        assert!(satisfiable_without_listed(
            &Multisig::AnyOf(vec![sig(ALICE), Multisig::Before(BigInt::from(i64::MAX))]),
            &l
        ));
    }

    #[test]
    fn nested_trees_recurse() {
        let l = listed(&[ALICE, BOB]);
        assert!(!satisfiable_without_listed(
            &Multisig::AllOf(vec![Multisig::AnyOf(vec![sig(ALICE), sig(BOB)])]),
            &l
        ));
        assert!(satisfiable_without_listed(
            &Multisig::AllOf(vec![Multisig::AnyOf(vec![sig(ALICE), sig(STRANGER)])]),
            &l
        ));
    }

    // ── Order-level verdicts ─────────────────────────────────────────────

    #[test]
    fn unrestricted_pool_permits_anyone() {
        let other = Ident::new(&[0x99; 28]);
        let o = order(sig(STRANGER), fixed_to(STRANGER));
        assert!(allowlists().permits(&other, &o, None));
    }

    #[test]
    fn empty_config_permits_anyone() {
        let o = order(sig(STRANGER), fixed_to(STRANGER));
        assert!(PoolAllowlists::default().permits(&pool(), &o, None));
    }

    #[test]
    fn listed_owner_and_destination_pass() {
        let o = order(sig(ALICE), fixed_to(BOB));
        assert!(allowlists().permits(&pool(), &o, None));
    }

    #[test]
    fn unlisted_destination_denies_a_listed_owner() {
        let o = order(sig(ALICE), fixed_to(STRANGER));
        assert!(!allowlists().permits(&pool(), &o, None));
    }

    #[test]
    fn vacuous_owner_with_self_destination_is_denied() {
        // The fill returns to the order script under this datum, so an owner
        // anyone can satisfy is an owner anyone can cancel — and walk off with.
        let o = order(Multisig::AllOf(vec![]), Destination::SelfDestination);
        assert!(!allowlists().permits(&pool(), &o, None));
    }

    #[test]
    fn self_destination_passes_under_a_listed_owner() {
        let o = order(sig(ALICE), Destination::SelfDestination);
        assert!(allowlists().permits(&pool(), &o, None));
    }

    #[test]
    fn script_credential_matches_when_listed() {
        let lists = PoolAllowlists(BTreeMap::from([(
            pool(),
            PoolAllowlist {
                credentials: listed(&[ALICE, STRANGER]),
            },
        )]));
        let o = order(Multisig::Script(STRANGER.to_vec()), fixed_to(ALICE));
        assert!(lists.permits(&pool(), &o, None));
    }

    // ── Strategy orders ──────────────────────────────────────────────────

    fn strategy(auth: Multisig, finals: Vec<Destination>) -> StrategyConstraints {
        StrategyConstraints {
            auth,
            final_destinations: finals,
        }
    }

    #[test]
    fn unlisted_auth_denies() {
        let o = order(sig(ALICE), fixed_to(BOB));
        let s = strategy(sig(STRANGER), vec![]);
        assert!(!allowlists().permits(&pool(), &o, Some(&s)));
        let s = strategy(sig(BOB), vec![]);
        assert!(allowlists().permits(&pool(), &o, Some(&s)));
    }

    #[test]
    fn one_unlisted_final_destination_denies() {
        let o = order(sig(ALICE), fixed_to(BOB));
        let s = strategy(sig(ALICE), vec![fixed_to(BOB), fixed_to(STRANGER)]);
        assert!(!allowlists().permits(&pool(), &o, Some(&s)));
        let s = strategy(sig(ALICE), vec![fixed_to(BOB), fixed_to(ALICE)]);
        assert!(allowlists().permits(&pool(), &o, Some(&s)));
    }

    // ── retain_visible ───────────────────────────────────────────────────

    fn pool_map(idents: &[Ident]) -> BTreeMap<Ident, ()> {
        idents.iter().map(|i| (i.clone(), ())).collect()
    }

    #[test]
    fn retain_visible_drops_a_restricted_pool_from_an_overlay_map() {
        let other = Ident::new(&[0x99; 28]);
        let map = pool_map(&[pool(), other.clone()]);
        let denied = order(sig(STRANGER), fixed_to(STRANGER));
        let kept = allowlists().retain_visible(map.clone(), &denied, None);
        assert_eq!(kept.keys().collect::<Vec<_>>(), vec![&other]);

        let permitted = order(sig(ALICE), fixed_to(BOB));
        let kept = allowlists().retain_visible(map.clone(), &permitted, None);
        assert_eq!(kept.len(), 2);
    }

    #[test]
    fn retain_visible_is_identity_without_config() {
        let map = pool_map(&[pool()]);
        let denied = order(sig(STRANGER), fixed_to(STRANGER));
        assert_eq!(
            PoolAllowlists::default().retain_visible(map, &denied, None).len(),
            1
        );
    }
}
