//! Tracks in-flight transaction chains per pool.
//!
//! When we submit a scoop tx, we predict the resulting pool UTxO and can
//! immediately build a chained tx that spends it. This module tracks those
//! chains and handles settlement confirmation, failure discard, and TTL expiry.
//!
//! Multi-pool transactions are supported: a single InFlightTx can reference
//! multiple pools, and is inserted into each pool's chain. When any one pool's
//! chain is discarded (e.g. competitor scoops), all related pools' chains are
//! cascade-discarded.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use pallas_primitives::Hash;
use tracing::{info, warn};

use crate::cardano_types::TransactionInput;
use crate::sundaev3::Ident;
use crate::sundaev4::types::SundaeV4Order;
use crate::sundaev4::SundaeV4Pool;

/// A predicted pool UTxO resulting from a submitted tx.
#[derive(Clone, Debug)]
pub struct PredictedPoolUtxo {
    /// The predicted input reference: (tx_hash, output_index)
    pub input: TransactionInput,
    /// The predicted pool state after the tx settles
    pub pool: Arc<SundaeV4Pool>,
}

/// A submitted transaction that hasn't settled yet.
///
/// May reference one or more pools (multi-pool transactions).
#[derive(Clone, Debug)]
pub struct InFlightTx {
    pub tx_hash: Hash<32>,
    /// Pool identifiers touched by this transaction.
    pub pool_idents: Vec<Ident>,
    pub consumed_orders: Vec<Arc<SundaeV4Order>>,
    /// Predicted pool UTxOs, one per pool in this tx.
    pub predicted_pools: Vec<(Ident, PredictedPoolUtxo)>,
    /// Transaction validity upper bound (slot)
    pub ttl: u64,
}

impl InFlightTx {
    /// Get the predicted pool for a specific ident within this multi-pool tx.
    pub fn predicted_pool_for(&self, ident: &Ident) -> Option<&PredictedPoolUtxo> {
        self.predicted_pools
            .iter()
            .find(|(id, _)| id == ident)
            .map(|(_, p)| p)
    }
}

/// Tracks in-flight transaction chains per pool.
///
/// Each pool can have at most one chain of transactions. Each tx in the chain
/// consumes the predicted output of the previous tx. Multi-pool txs are
/// inserted into each pool's chain.
pub struct ChainTracker {
    chains: BTreeMap<Ident, Vec<InFlightTx>>,
}

impl ChainTracker {
    pub fn new() -> Self {
        Self {
            chains: BTreeMap::new(),
        }
    }

    /// Record a submitted transaction (possibly multi-pool).
    /// Inserts into each pool's chain.
    pub fn record_submission(&mut self, tx: InFlightTx) {
        for ident in &tx.pool_idents {
            self.chains.entry(ident.clone()).or_default().push(tx.clone());
        }
    }

    /// Get the latest predicted pool state for a given pool, if we have
    /// an in-flight chain for it.
    pub fn latest_predicted_pool(&self, pool_ident: &Ident) -> Option<&PredictedPoolUtxo> {
        self.chains
            .get(pool_ident)
            .and_then(|chain| chain.last())
            .and_then(|tx| tx.predicted_pool_for(pool_ident))
    }

    /// Next chain index for a pool (0 if no chain exists). Used in tests.
    #[cfg(test)]
    pub fn next_chain_index(&self, pool_ident: &Ident) -> usize {
        self.chains
            .get(pool_ident)
            .map(|chain| chain.len())
            .unwrap_or(0)
    }

    /// Collect all order inputs consumed by in-flight transactions.
    /// These should be excluded from candidate selection.
    pub fn in_flight_order_inputs(&self) -> BTreeSet<TransactionInput> {
        // Deduplicate across chains (multi-pool txs appear in multiple chains)
        let mut seen_tx_hashes = BTreeSet::new();
        let mut inputs = BTreeSet::new();
        for chain in self.chains.values() {
            for tx in chain {
                if seen_tx_hashes.insert(tx.tx_hash) {
                    for order in &tx.consumed_orders {
                        inputs.insert(order.input.clone());
                    }
                }
            }
        }
        inputs
    }

    /// Confirm that a transaction has settled on-chain. Removes the settled
    /// tx and all its predecessors in the chain (they must have settled too).
    pub fn confirm_settlement(&mut self, pool_ident: &Ident, tx_hash: &Hash<32>) {
        let Some(chain) = self.chains.get_mut(pool_ident) else {
            return;
        };

        // Find the position of the settled tx
        let pos = chain.iter().position(|tx| &tx.tx_hash == tx_hash);
        if let Some(idx) = pos {
            // Remove the settled tx and all predecessors
            let removed: Vec<_> = chain.drain(..=idx).collect();
            info!(
                pool = %pool_ident,
                tx_hash = %hex::encode(tx_hash),
                removed_count = removed.len(),
                remaining = chain.len(),
                "chain settlement confirmed"
            );
        }

        // Clean up empty chains
        if chain.is_empty() {
            self.chains.remove(pool_ident);
        }
    }

    /// Discard the entire chain for a pool (e.g. because a competitor
    /// scooped the pool, or our tx failed).
    pub fn discard_chain(&mut self, pool_ident: &Ident) {
        if let Some(chain) = self.chains.remove(pool_ident) {
            warn!(
                pool = %pool_ident,
                chain_len = chain.len(),
                "discarding in-flight chain"
            );
        }
    }

    /// Discard the chain for a pool and cascade to all related pools.
    ///
    /// When a multi-pool tx becomes invalid (e.g. a competitor scoops one of
    /// the pools), all pools in that tx must be invalidated. This finds all
    /// tx_hashes in the pool's chain, collects all pool_idents from those txs,
    /// and discards all their chains.
    pub fn discard_chain_and_related(&mut self, pool_ident: &Ident) {
        let Some(chain) = self.chains.get(pool_ident) else {
            return;
        };

        // Collect all pool idents referenced by txs in this chain
        let mut related_idents: BTreeSet<Ident> = BTreeSet::new();
        for tx in chain {
            for ident in &tx.pool_idents {
                related_idents.insert(ident.clone());
            }
        }

        if related_idents.len() > 1 {
            warn!(
                pool = %pool_ident,
                related_pools = related_idents.len(),
                "cascade-discarding related multi-pool chains"
            );
        }

        for ident in &related_idents {
            self.discard_chain(ident);
        }
    }

    /// Discard all chains (e.g. on rollback).
    pub fn discard_all(&mut self) {
        let count: usize = self.chains.values().map(|c| c.len()).sum();
        if count > 0 {
            warn!(total_txs = count, "discarding all in-flight chains");
            self.chains.clear();
        }
    }

    /// Discard chains whose first (oldest) tx TTL has passed.
    /// If the first tx expired, the entire chain is invalid.
    /// Uses cascade discard for multi-pool awareness.
    pub fn expire_stale(&mut self, current_slot: u64) {
        let stale_pools: Vec<Ident> = self
            .chains
            .iter()
            .filter_map(|(ident, chain)| {
                chain.first().and_then(|first_tx| {
                    if current_slot > first_tx.ttl {
                        Some(ident.clone())
                    } else {
                        None
                    }
                })
            })
            .collect();

        for ident in stale_pools {
            warn!(pool = %ident, current_slot, "expiring stale in-flight chain");
            self.discard_chain_and_related(&ident);
        }
    }

    /// Find an in-flight tx whose predicted pool output matches the given
    /// on-chain pool input. Returns the tx_hash for use with `confirm_settlement`.
    pub fn find_settled_tx(&self, pool_ident: &Ident, pool_input: &TransactionInput) -> Option<Hash<32>> {
        self.chains
            .get(pool_ident)?
            .iter()
            .find(|tx| {
                tx.predicted_pool_for(pool_ident)
                    .map(|p| p.input == *pool_input)
                    .unwrap_or(false)
            })
            .map(|tx| tx.tx_hash)
    }

    /// Check if we have any in-flight chains. Used in tests.
    #[cfg(test)]
    pub fn has_in_flight(&self) -> bool {
        !self.chains.is_empty()
    }

    /// Get the pool identifiers that have in-flight chains. Used in tests.
    #[cfg(test)]
    pub fn in_flight_pools(&self) -> Vec<Ident> {
        self.chains.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bigint::BigInt;
    use crate::cardano_types::Value;
    use crate::sundaev4::types::PoolDatum;

    fn make_pool(ident_byte: u8) -> Arc<SundaeV4Pool> {
        use crate::sundaev4::types::{PoolType, Rational};
        Arc::new(SundaeV4Pool {
            input: TransactionInput::new([0xaa; 32].into(), 0),
            value: Value::default(),
            pool_datum: PoolDatum {
                assets: vec![],
                total_lp: BigInt::from(0),
                circulating_lp: BigInt::from(0),
                preminted_lp: BigInt::from(0),
                identifier: Ident::new(&[ident_byte]),
                actions: vec![],
                module_state: vec![],
            },
            pool_type: PoolType::ConstantProduct {
                fee: Rational { num: BigInt::from(3), den: BigInt::from(1000) },
            },
            slot: 100,
        })
    }

    fn make_in_flight(ident_byte: u8, hash_byte: u8, ttl: u64, _chain_index: usize) -> InFlightTx {
        let tx_hash: Hash<32> = [hash_byte; 32].into();
        let pool = make_pool(ident_byte);
        let ident = Ident::new(&[ident_byte]);
        InFlightTx {
            tx_hash,
            pool_idents: vec![ident.clone()],
            consumed_orders: vec![],
            predicted_pools: vec![(ident, PredictedPoolUtxo {
                input: TransactionInput::new(tx_hash, 0),
                pool,
            })],
            ttl,
        }
    }

    fn make_multi_pool_in_flight(
        ident_bytes: &[u8],
        hash_byte: u8,
        ttl: u64,
        _chain_index: usize,
    ) -> InFlightTx {
        let tx_hash: Hash<32> = [hash_byte; 32].into();
        let pool_idents: Vec<Ident> = ident_bytes.iter().map(|&b| Ident::new(&[b])).collect();
        let predicted_pools: Vec<_> = ident_bytes.iter().enumerate().map(|(i, &b)| {
            let pool = make_pool(b);
            (Ident::new(&[b]), PredictedPoolUtxo {
                input: TransactionInput::new(tx_hash, i as u64),
                pool,
            })
        }).collect();
        InFlightTx {
            tx_hash,
            pool_idents,
            consumed_orders: vec![],
            predicted_pools,
            ttl,
        }
    }

    #[test]
    fn test_record_and_latest() {
        let mut tracker = ChainTracker::new();
        let ident = Ident::new(&[0x01]);

        let tx1 = make_in_flight(0x01, 0xaa, 200, 0);
        tracker.record_submission(tx1);

        assert!(tracker.latest_predicted_pool(&ident).is_some());
        assert_eq!(tracker.next_chain_index(&ident), 1);

        let tx2 = make_in_flight(0x01, 0xbb, 260, 1);
        tracker.record_submission(tx2);

        assert_eq!(tracker.next_chain_index(&ident), 2);
        let latest = tracker.latest_predicted_pool(&ident).unwrap();
        assert_eq!(latest.input.0.transaction_id, Hash::<32>::from([0xbb; 32]));
    }

    #[test]
    fn test_confirm_settlement() {
        let mut tracker = ChainTracker::new();
        let ident = Ident::new(&[0x01]);

        let tx1 = make_in_flight(0x01, 0xaa, 200, 0);
        let tx2 = make_in_flight(0x01, 0xbb, 260, 1);
        let tx3 = make_in_flight(0x01, 0xcc, 320, 2);
        tracker.record_submission(tx1);
        tracker.record_submission(tx2);
        tracker.record_submission(tx3);

        // Confirm tx2 settles — should remove tx1 and tx2, leave tx3
        let hash2: Hash<32> = [0xbb; 32].into();
        tracker.confirm_settlement(&ident, &hash2);

        assert_eq!(tracker.next_chain_index(&ident), 1);
        let latest = tracker.latest_predicted_pool(&ident).unwrap();
        assert_eq!(latest.input.0.transaction_id, Hash::<32>::from([0xcc; 32]));
    }

    #[test]
    fn test_discard_chain() {
        let mut tracker = ChainTracker::new();
        let ident = Ident::new(&[0x01]);

        tracker.record_submission(make_in_flight(0x01, 0xaa, 200, 0));
        tracker.record_submission(make_in_flight(0x01, 0xbb, 260, 1));

        tracker.discard_chain(&ident);
        assert!(!tracker.has_in_flight());
    }

    #[test]
    fn test_discard_all() {
        let mut tracker = ChainTracker::new();

        tracker.record_submission(make_in_flight(0x01, 0xaa, 200, 0));
        tracker.record_submission(make_in_flight(0x02, 0xbb, 200, 0));

        assert!(tracker.has_in_flight());
        tracker.discard_all();
        assert!(!tracker.has_in_flight());
    }

    #[test]
    fn test_expire_stale() {
        let mut tracker = ChainTracker::new();
        let ident1 = Ident::new(&[0x01]);
        let ident2 = Ident::new(&[0x02]);

        // Pool 1: TTL 200
        tracker.record_submission(make_in_flight(0x01, 0xaa, 200, 0));
        // Pool 2: TTL 300
        tracker.record_submission(make_in_flight(0x02, 0xbb, 300, 0));

        // At slot 250: pool 1 should expire, pool 2 should remain
        tracker.expire_stale(250);

        assert!(tracker.latest_predicted_pool(&ident1).is_none());
        assert!(tracker.latest_predicted_pool(&ident2).is_some());
    }

    #[test]
    fn test_in_flight_order_inputs() {
        let mut tracker = ChainTracker::new();

        let order_input = TransactionInput::new([0xff; 32].into(), 7);
        let order = Arc::new(crate::sundaev4::SundaeV4Order {
            input: order_input.clone(),
            value: Value::default(),
            datum: crate::sundaev4::SimpleOrderDatum {
                owner: crate::multisig::Multisig::Signature(vec![0xaa; 28]),
                destination: crate::sundaev4::Destination::SelfDestination,
                offer: (crate::cardano_types::AssetClass { policy: vec![], token: vec![] }, crate::bigint::BigInt::from(0i64)),
                min_received: (crate::cardano_types::AssetClass { policy: vec![], token: vec![] }, crate::bigint::BigInt::from(0i64)),
                max_protocol_fee: crate::bigint::BigInt::from(0i64),
                extension: pallas_primitives::PlutusData::Constr(
                    pallas_primitives::Constr { tag: 121, any_constructor: None, fields: pallas_codec::utils::MaybeIndefArray::Def(vec![]) }
                ),
            },
            slot: 1,
        });

        let mut tx = make_in_flight(0x01, 0xaa, 200, 0);
        tx.consumed_orders = vec![order];
        tracker.record_submission(tx);

        let in_flight = tracker.in_flight_order_inputs();
        assert!(in_flight.contains(&order_input));
    }

    #[test]
    fn test_multi_pool_record_and_latest() {
        let mut tracker = ChainTracker::new();
        let ident_a = Ident::new(&[0x01]);
        let ident_b = Ident::new(&[0x02]);

        let tx = make_multi_pool_in_flight(&[0x01, 0x02], 0xaa, 200, 0);
        tracker.record_submission(tx);

        // Both pools should have a chain
        assert!(tracker.latest_predicted_pool(&ident_a).is_some());
        assert!(tracker.latest_predicted_pool(&ident_b).is_some());
        assert_eq!(tracker.next_chain_index(&ident_a), 1);
        assert_eq!(tracker.next_chain_index(&ident_b), 1);
    }

    #[test]
    fn test_multi_pool_cascade_discard() {
        let mut tracker = ChainTracker::new();
        let ident_a = Ident::new(&[0x01]);
        let ident_b = Ident::new(&[0x02]);

        // Multi-pool tx touches both pools
        let tx = make_multi_pool_in_flight(&[0x01, 0x02], 0xaa, 200, 0);
        tracker.record_submission(tx);

        // Discard pool A's chain with cascade — pool B should also be discarded
        tracker.discard_chain_and_related(&ident_a);

        assert!(tracker.latest_predicted_pool(&ident_a).is_none());
        assert!(tracker.latest_predicted_pool(&ident_b).is_none());
        assert!(!tracker.has_in_flight());
    }

    #[test]
    fn test_multi_pool_no_duplicate_orders() {
        // Multi-pool tx appears in both chains but orders should not be
        // double-counted
        let mut tracker = ChainTracker::new();

        let order_input = TransactionInput::new([0xff; 32].into(), 7);
        let order = Arc::new(crate::sundaev4::SundaeV4Order {
            input: order_input.clone(),
            value: Value::default(),
            datum: crate::sundaev4::SimpleOrderDatum {
                owner: crate::multisig::Multisig::Signature(vec![0xaa; 28]),
                destination: crate::sundaev4::Destination::SelfDestination,
                offer: (crate::cardano_types::AssetClass { policy: vec![], token: vec![] }, crate::bigint::BigInt::from(0i64)),
                min_received: (crate::cardano_types::AssetClass { policy: vec![], token: vec![] }, crate::bigint::BigInt::from(0i64)),
                max_protocol_fee: crate::bigint::BigInt::from(0i64),
                extension: pallas_primitives::PlutusData::Constr(
                    pallas_primitives::Constr { tag: 121, any_constructor: None, fields: pallas_codec::utils::MaybeIndefArray::Def(vec![]) }
                ),
            },
            slot: 1,
        });

        let mut tx = make_multi_pool_in_flight(&[0x01, 0x02], 0xaa, 200, 0);
        tx.consumed_orders = vec![order];
        tracker.record_submission(tx);

        let in_flight = tracker.in_flight_order_inputs();
        // Should contain the order exactly once (set semantics)
        assert_eq!(in_flight.len(), 1);
        assert!(in_flight.contains(&order_input));
    }
}
