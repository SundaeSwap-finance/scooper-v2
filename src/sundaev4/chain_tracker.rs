//! Tracks in-flight transaction chains per pool.
//!
//! When we submit a scoop tx, we predict the resulting pool UTxO and can
//! immediately build a chained tx that spends it. This module tracks those
//! chains and handles settlement confirmation, failure discard, and TTL expiry.

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
    /// The predicted input reference: (tx_hash, output_index=0)
    pub input: TransactionInput,
    /// The predicted pool state after the tx settles
    pub pool: Arc<SundaeV4Pool>,
}

/// A submitted transaction that hasn't settled yet.
#[derive(Clone, Debug)]
pub struct InFlightTx {
    pub tx_hash: Hash<32>,
    pub tx_hash_hex: String,
    pub pool_ident: Ident,
    pub consumed_orders: Vec<Arc<SundaeV4Order>>,
    pub predicted_pool: PredictedPoolUtxo,
    /// Transaction validity upper bound (slot)
    pub ttl: u64,
    /// Position in chain (0 = first)
    pub chain_index: usize,
}

/// Tracks in-flight transaction chains per pool.
///
/// Each pool can have at most one chain of transactions. Each tx in the chain
/// consumes the predicted output of the previous tx.
pub struct ChainTracker {
    chains: BTreeMap<Ident, Vec<InFlightTx>>,
}

impl ChainTracker {
    pub fn new() -> Self {
        Self {
            chains: BTreeMap::new(),
        }
    }

    /// Record a submitted transaction. Returns the predicted pool UTxO for
    /// building the next chained tx.
    pub fn record_submission(&mut self, tx: InFlightTx) -> PredictedPoolUtxo {
        let predicted = tx.predicted_pool.clone();
        let ident = tx.pool_ident.clone();
        self.chains.entry(ident).or_default().push(tx);
        predicted
    }

    /// Get the latest predicted pool state for a given pool, if we have
    /// an in-flight chain for it.
    pub fn latest_predicted_pool(&self, pool_ident: &Ident) -> Option<&PredictedPoolUtxo> {
        self.chains
            .get(pool_ident)
            .and_then(|chain| chain.last())
            .map(|tx| &tx.predicted_pool)
    }

    /// Next chain index for a pool (0 if no chain exists).
    pub fn next_chain_index(&self, pool_ident: &Ident) -> usize {
        self.chains
            .get(pool_ident)
            .map(|chain| chain.len())
            .unwrap_or(0)
    }

    /// Collect all order inputs consumed by in-flight transactions.
    /// These should be excluded from candidate selection.
    pub fn in_flight_order_inputs(&self) -> BTreeSet<TransactionInput> {
        let mut inputs = BTreeSet::new();
        for chain in self.chains.values() {
            for tx in chain {
                for order in &tx.consumed_orders {
                    inputs.insert(order.input.clone());
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
            self.chains.remove(&ident);
        }
    }

    /// Find an in-flight tx whose predicted pool output matches the given
    /// on-chain pool input. Returns the tx_hash for use with `confirm_settlement`.
    pub fn find_settled_tx(&self, pool_ident: &Ident, pool_input: &TransactionInput) -> Option<Hash<32>> {
        self.chains
            .get(pool_ident)?
            .iter()
            .find(|tx| tx.predicted_pool.input == *pool_input)
            .map(|tx| tx.tx_hash)
    }

    /// Check if we have any in-flight chains.
    pub fn has_in_flight(&self) -> bool {
        !self.chains.is_empty()
    }

    /// Get the pool identifiers that have in-flight chains.
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
            slot: 100,
        })
    }

    fn make_in_flight(ident_byte: u8, hash_byte: u8, ttl: u64, chain_index: usize) -> InFlightTx {
        let tx_hash: Hash<32> = [hash_byte; 32].into();
        let pool = make_pool(ident_byte);
        InFlightTx {
            tx_hash,
            tx_hash_hex: hex::encode(tx_hash),
            pool_ident: Ident::new(&[ident_byte]),
            consumed_orders: vec![],
            predicted_pool: PredictedPoolUtxo {
                input: TransactionInput::new(tx_hash, 0),
                pool,
            },
            ttl,
            chain_index,
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
            datum: crate::sundaev4::OrderDatum {
                owner: crate::multisig::Multisig::Signature(vec![0xaa; 28]),
                destination: crate::sundaev4::Destination::SelfDestination,
                constraints: crate::sundaev4::OrderConstraints::Simple { min_received: vec![] },
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
}
