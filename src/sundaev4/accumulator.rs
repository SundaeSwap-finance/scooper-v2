//! Incremental multi-pool transaction accumulator.
//!
//! Instead of grouping orders by pool and building one tx per pool, this module
//! lets the scooper add orders one at a time. Each order is executed against its
//! pool's running state. After all orders are accumulated, `into_batches()`
//! produces `Vec<Batch>` ready for the multi-pool tx builder.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::bigint::BigInt;
use crate::cardano_types::AssetClass;
use crate::sundaev3::Ident;
use crate::sundaev4::batch::{self, Batch, ContinuationSwap, FulfillmentOverride, ResolvedSwap};
use crate::sundaev4::router::RoutingPlan;
use crate::sundaev4::swap_math;
use crate::sundaev4::types::SundaeV4Pool;

/// Per-pool running state within a multi-pool tx being built incrementally.
#[derive(Clone)]
pub struct PoolAccum {
    pub pool: Arc<SundaeV4Pool>,
    pub ident: Ident,
    pub running_assets: Vec<(AssetClass, BigInt)>,
    pub initial_total_lp: BigInt,
    pub swaps: Vec<ResolvedSwap>,
    pub continuations: Vec<ContinuationSwap>,
}

/// Incrementally-built multi-pool transaction state.
#[derive(Clone)]
pub struct Accumulator {
    pub pools: BTreeMap<Ident, PoolAccum>,
    fee: (u64, u64),
    protocol_share: (u64, u64),
}

impl Accumulator {
    pub fn new(fee: (u64, u64), protocol_share: (u64, u64)) -> Self {
        Self {
            pools: BTreeMap::new(),
            fee,
            protocol_share,
        }
    }

    /// Try to add an order targeting `pool_ident`. If the pool hasn't been
    /// touched yet, `effective_pool` initializes its running state.
    ///
    /// Returns `Ok(())` if the order was successfully executed against the
    /// pool's running reserves, or `Err(reason)` if it couldn't execute.
    pub fn try_add_order(
        &mut self,
        order: &Arc<crate::sundaev4::types::SundaeV4Order>,
        pool_ident: &Ident,
        effective_pool: &Arc<SundaeV4Pool>,
    ) -> Result<(), String> {
        let accum = self.pools.entry(pool_ident.clone()).or_insert_with(|| {
            PoolAccum {
                pool: effective_pool.clone(),
                ident: pool_ident.clone(),
                running_assets: effective_pool.pool_datum.assets.clone(),
                initial_total_lp: effective_pool.pool_datum.total_lp.clone(),
                swaps: Vec::new(),
                continuations: Vec::new(),
            }
        });

        let swap = batch::try_execute_order(
            order,
            &accum.running_assets,
            &accum.initial_total_lp,
            self.fee,
            self.protocol_share,
        )
        .ok_or_else(|| "order cannot execute against running pool state".to_string())?;

        // Update running reserves
        accum.running_assets[swap.input_idx].1 =
            &accum.running_assets[swap.input_idx].1 + &swap.dx;
        accum.running_assets[swap.output_idx].1 =
            &accum.running_assets[swap.output_idx].1 - &swap.dy;

        accum.swaps.push(swap);
        Ok(())
    }

    /// Try to add a routed order (multi-hop and/or split) to the accumulator.
    ///
    /// For the entry hop's primary pool (largest allocation), creates a
    /// `ResolvedSwap` with a `fulfillment_override` pointing to the final
    /// hop's output. For all other pools in the route (including split pools
    /// on the entry hop), creates `ContinuationSwap` entries.
    ///
    /// Returns `Ok(())` on success or `Err(reason)` if the route can't execute
    /// against the running pool states.
    pub fn try_add_routed_order(
        &mut self,
        order: &Arc<crate::sundaev4::types::SundaeV4Order>,
        route: &RoutingPlan,
        pools: &BTreeMap<Ident, Arc<SundaeV4Pool>>,
    ) -> Result<(), String> {
        use num_traits::Signed;

        // Clone pool accums for trial execution
        let mut trial_pools = self.pools.clone();

        // We'll collect the swaps/continuations to commit if successful
        struct PendingSwap {
            pool_ident: Ident,
            swap: ResolvedSwap,
        }
        struct PendingContinuation {
            pool_ident: Ident,
            cont: ContinuationSwap,
        }
        let mut pending_swaps: Vec<PendingSwap> = Vec::new();
        let mut pending_continuations: Vec<PendingContinuation> = Vec::new();

        let mut final_output_asset: Option<AssetClass> = None;
        let mut final_output_amount = BigInt::from(0);

        // Track actual output from previous hop so subsequent hops use the
        // real dy (not the router's estimate).  This ensures ADA flows cancel
        // exactly across pools for routed orders.
        let mut prev_hop_output = BigInt::from(0);

        for (hop_idx, hop) in route.hops.iter().enumerate() {
            let is_entry_hop = hop_idx == 0;
            let mut this_hop_output = BigInt::from(0);

            for (split_idx, split) in hop.splits.iter().enumerate() {
                let pool_ident = &split.pool.ident;

                // Get effective pool — from trial state if already there, else from chain
                let effective_pool = match trial_pools.get(pool_ident) {
                    Some(accum) => accum.pool.clone(),
                    None => match pools.get(pool_ident) {
                        Some(p) => p.clone(),
                        None => return Err(format!("pool {} not found", pool_ident)),
                    },
                };

                // Initialize pool accum if not already present
                let accum = trial_pools.entry(pool_ident.clone()).or_insert_with(|| {
                    PoolAccum {
                        pool: effective_pool.clone(),
                        ident: pool_ident.clone(),
                        running_assets: effective_pool.pool_datum.assets.clone(),
                        initial_total_lp: effective_pool.pool_datum.total_lp.clone(),
                        swaps: Vec::new(),
                        continuations: Vec::new(),
                    }
                });

                // Determine input/output direction for this pool
                let (input_idx, output_idx) = self.find_direction_for_tokens(
                    &accum.running_assets,
                    &hop.input_token,
                    &hop.output_token,
                ).ok_or_else(|| format!("can't determine direction for pool {}", pool_ident))?;

                // For hop 0, use the router's split amount. For subsequent hops,
                // use the actual output from the previous hop (single-split) or
                // distribute proportionally (multi-split).
                let dx = if is_entry_hop {
                    split.input_amount.clone()
                } else if hop.splits.len() == 1 {
                    prev_hop_output.clone()
                } else {
                    // Multi-split on a non-entry hop: scale by the router's
                    // proportion of this split vs. the hop total
                    let hop_total: BigInt = hop.splits.iter()
                        .map(|s| s.input_amount.clone())
                        .fold(BigInt::from(0), |a, b| &a + &b);
                    if hop_total.is_positive() {
                        &prev_hop_output * &split.input_amount / &hop_total
                    } else {
                        split.input_amount.clone()
                    }
                };

                let dy = swap_math::cp_swap_result(
                    &accum.running_assets[input_idx].1,
                    &accum.running_assets[output_idx].1,
                    &dx,
                    self.fee.0,
                    self.fee.1,
                );
                if !dy.is_positive() {
                    return Err(format!("zero output from pool {}", pool_ident));
                }

                // Update running reserves
                accum.running_assets[input_idx].1 =
                    &accum.running_assets[input_idx].1 + &dx;
                accum.running_assets[output_idx].1 =
                    &accum.running_assets[output_idx].1 - &dy;

                this_hop_output = &this_hop_output + &dy;

                // Track final output from last hop
                if hop_idx == route.hops.len() - 1 {
                    final_output_asset = Some(hop.output_token.clone());
                    final_output_amount = &final_output_amount + &dy;
                }

                if is_entry_hop && split_idx == 0 {
                    // Primary entry swap — creates a ResolvedSwap
                    pending_swaps.push(PendingSwap {
                        pool_ident: pool_ident.clone(),
                        swap: ResolvedSwap {
                            order: order.clone(),
                            input_idx,
                            output_idx,
                            dx: dx.clone(),
                            dy: dy.clone(),
                            fulfillment_override: None, // set below after all hops
                        },
                    });
                } else {
                    // All other splits/hops — continuation swaps
                    pending_continuations.push(PendingContinuation {
                        pool_ident: pool_ident.clone(),
                        cont: ContinuationSwap {
                            originating_order: order.clone(),
                            input_idx,
                            output_idx,
                            dx: dx.clone(),
                            dy: dy.clone(),
                        },
                    });
                }

                // Push to trial accum
                if is_entry_hop && split_idx == 0 {
                    accum.swaps.push(ResolvedSwap {
                        order: order.clone(),
                        input_idx,
                        output_idx,
                        dx: dx.clone(),
                        dy: dy.clone(),
                        fulfillment_override: None,
                    });
                } else {
                    accum.continuations.push(ContinuationSwap {
                        originating_order: order.clone(),
                        input_idx,
                        output_idx,
                        dx: dx.clone(),
                        dy: dy.clone(),
                    });
                }
            }

            prev_hop_output = this_hop_output;
        }

        // Check min_received against the final routed output
        if let crate::sundaev4::OrderConstraints::Simple { min_received } = &order.datum.constraints {
            for (asset, min_qty) in min_received {
                if final_output_asset.as_ref() == Some(asset) && &final_output_amount < min_qty {
                    return Err(format!(
                        "routed output {} below min_received {}",
                        final_output_amount, min_qty
                    ));
                }
            }
        }

        // Set fulfillment override on the primary swap if multi-hop or multi-split.
        // Must update the swap in trial_pools directly since that's what gets committed.
        if route.hops.len() > 1 || route.hops.iter().any(|h| h.splits.len() > 1) {
            if let (Some(ps), Some(out_asset)) = (pending_swaps.first(), &final_output_asset) {
                if let Some(accum) = trial_pools.get_mut(&ps.pool_ident) {
                    // The primary swap was the last one pushed to this pool's accum
                    if let Some(swap) = accum.swaps.last_mut() {
                        swap.fulfillment_override = Some(FulfillmentOverride {
                            output_asset: out_asset.clone(),
                            amount: final_output_amount.clone(),
                        });
                    }
                }
            }
        }

        // Commit: replace pool states
        self.pools = trial_pools;

        Ok(())
    }

    /// Find which pool asset indices correspond to the given input/output tokens.
    fn find_direction_for_tokens(
        &self,
        running_assets: &[(AssetClass, BigInt)],
        input_token: &AssetClass,
        output_token: &AssetClass,
    ) -> Option<(usize, usize)> {
        let mut input_idx = None;
        let mut output_idx = None;
        for (i, (asset, _)) in running_assets.iter().enumerate() {
            if asset == input_token {
                input_idx = Some(i);
            }
            if asset == output_token {
                output_idx = Some(i);
            }
        }
        match (input_idx, output_idx) {
            (Some(i), Some(o)) if i != o => Some((i, o)),
            _ => None,
        }
    }

    /// Total number of orders across all pools.
    pub fn order_count(&self) -> usize {
        self.pools.values().map(|a| a.swaps.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.pools.is_empty() || self.order_count() == 0
    }

    /// Total number of orders across all pools (including continuations).
    pub fn total_transcript_entries(&self) -> usize {
        self.pools.values().map(|a| a.swaps.len() + a.continuations.len()).sum()
    }

    /// Convert accumulated state into `Vec<Batch>` for the tx builder.
    ///
    /// For each pool, replays the fee_budget computation (matching
    /// `assemble_batch`) to produce `final_total_lp`. Includes both
    /// swaps and continuations in the replay.
    pub fn into_batches(self) -> Vec<Batch> {
        let mut batches = Vec::new();

        for (_ident, accum) in self.pools {
            if accum.swaps.is_empty() && accum.continuations.is_empty() {
                continue;
            }

            // Replay fee budget computation to get final_total_lp.
            // Process swaps first, then continuations (matching transcript order).
            let mut total_fee_budget = BigInt::from(0);
            let mut replay_assets = accum.pool.pool_datum.assets.clone();

            for swap in &accum.swaps {
                let prev_a = replay_assets[0].1.clone();
                let prev_b = replay_assets[1].1.clone();
                replay_assets[swap.input_idx].1 =
                    &replay_assets[swap.input_idx].1 + &swap.dx;
                replay_assets[swap.output_idx].1 =
                    &replay_assets[swap.output_idx].1 - &swap.dy;
                let fb = swap_math::cp_fee_budget(
                    &prev_a,
                    &prev_b,
                    &replay_assets[0].1,
                    &replay_assets[1].1,
                    &accum.initial_total_lp,
                );
                total_fee_budget = &total_fee_budget + &fb;
            }

            for cont in &accum.continuations {
                let prev_a = replay_assets[0].1.clone();
                let prev_b = replay_assets[1].1.clone();
                replay_assets[cont.input_idx].1 =
                    &replay_assets[cont.input_idx].1 + &cont.dx;
                replay_assets[cont.output_idx].1 =
                    &replay_assets[cont.output_idx].1 - &cont.dy;
                let fb = swap_math::cp_fee_budget(
                    &prev_a,
                    &prev_b,
                    &replay_assets[0].1,
                    &replay_assets[1].1,
                    &accum.initial_total_lp,
                );
                total_fee_budget = &total_fee_budget + &fb;
            }

            let total_protocol_lp = swap_math::compute_protocol_lp(
                &total_fee_budget,
                self.protocol_share.0,
                self.protocol_share.1,
            );
            let final_total_lp = &accum.initial_total_lp + &total_protocol_lp;

            batches.push(Batch {
                pool: accum.pool,
                pool_ident: accum.ident,
                swaps: accum.swaps,
                continuations: accum.continuations,
                final_assets: accum.running_assets,
                final_total_lp,
            });
        }

        batches
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cardano_types::{TransactionInput, Value};
    use crate::multisig::Multisig;
    use crate::sundaev4::types::{Destination, OrderConstraints, OrderDatum, PoolDatum, SundaeV4Order};
    use pallas_codec::utils::MaybeIndefArray;

    fn ada() -> AssetClass {
        AssetClass { policy: vec![], token: vec![] }
    }

    fn token_a() -> AssetClass {
        AssetClass { policy: vec![0x01], token: vec![0x02] }
    }

    fn token_b() -> AssetClass {
        AssetClass { policy: vec![0x03], token: vec![0x04] }
    }

    fn unit_pd() -> pallas_primitives::PlutusData {
        pallas_primitives::PlutusData::Constr(pallas_primitives::Constr {
            tag: 121,
            any_constructor: None,
            fields: MaybeIndefArray::Def(vec![]),
        })
    }

    fn make_pool(ident_byte: u8, ada_reserve: i64, token: AssetClass, token_reserve: i64) -> Arc<SundaeV4Pool> {
        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(ada_reserve));
        value.insert(&token, BigInt::from(token_reserve));

        Arc::new(SundaeV4Pool {
            input: TransactionInput::new([ident_byte; 32].into(), 0),
            value,
            pool_datum: PoolDatum {
                assets: vec![
                    (ada(), BigInt::from(ada_reserve)),
                    (token, BigInt::from(token_reserve)),
                ],
                total_lp: BigInt::from(1_000_000),
                circulating_lp: BigInt::from(500_000),
                preminted_lp: BigInt::from(500_000),
                identifier: Ident::new(&[ident_byte]),
                actions: vec![],
                module_state: vec![],
            },
            slot: 100,
        })
    }

    fn make_buy_order(ada_amount: i64, min_token: AssetClass, min_qty: i64, slot: u64) -> Arc<SundaeV4Order> {
        let mut value = Value::default();
        value.insert(&ada(), BigInt::from(ada_amount));

        Arc::new(SundaeV4Order {
            input: TransactionInput::new([slot as u8; 32].into(), 0),
            value,
            datum: OrderDatum {
                owner: Multisig::Signature(vec![0xaa; 28]),
                destination: Destination::SelfDestination,
                constraints: OrderConstraints::Simple {
                    min_received: vec![(min_token, BigInt::from(min_qty))],
                },
                extension: unit_pd(),
            },
            slot,
        })
    }

    #[test]
    fn test_single_order_accumulator() {
        let pool = make_pool(0xAA, 1_000_000_000, token_a(), 1_000_000_000);
        let ident = pool.pool_datum.identifier.clone();
        let order = make_buy_order(10_000_000, token_a(), 1, 1);

        let mut accum = Accumulator::new((3, 1000), (1, 2));
        assert!(accum.is_empty());

        accum.try_add_order(&order, &ident, &pool).unwrap();
        assert_eq!(accum.order_count(), 1);
        assert!(!accum.is_empty());

        let batches = accum.into_batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].swaps.len(), 1);
        assert_eq!(batches[0].pool_ident, ident);
    }

    #[test]
    fn test_multi_order_same_pool() {
        let pool = make_pool(0xAA, 1_000_000_000, token_a(), 1_000_000_000);
        let ident = pool.pool_datum.identifier.clone();

        let mut accum = Accumulator::new((3, 1000), (1, 2));
        for slot in 1..=3u64 {
            let order = make_buy_order(10_000_000, token_a(), 1, slot);
            accum.try_add_order(&order, &ident, &pool).unwrap();
        }

        assert_eq!(accum.order_count(), 3);
        let batches = accum.into_batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].swaps.len(), 3);
    }

    #[test]
    fn test_multi_pool_accumulator() {
        let pool_a = make_pool(0xAA, 1_000_000_000, token_a(), 1_000_000_000);
        let pool_b = make_pool(0xBB, 1_000_000_000, token_b(), 1_000_000_000);
        let ident_a = pool_a.pool_datum.identifier.clone();
        let ident_b = pool_b.pool_datum.identifier.clone();

        let mut accum = Accumulator::new((3, 1000), (1, 2));

        let order1 = make_buy_order(10_000_000, token_a(), 1, 1);
        accum.try_add_order(&order1, &ident_a, &pool_a).unwrap();

        let order2 = make_buy_order(10_000_000, token_b(), 1, 2);
        accum.try_add_order(&order2, &ident_b, &pool_b).unwrap();

        assert_eq!(accum.order_count(), 2);
        let batches = accum.into_batches();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].swaps.len(), 1);
        assert_eq!(batches[1].swaps.len(), 1);
    }

    #[test]
    fn test_failed_order_doesnt_pollute() {
        let pool = make_pool(0xAA, 1_000_000_000, token_a(), 1_000_000_000);
        let ident = pool.pool_datum.identifier.clone();

        // Order that wants more tokens than the swap would produce
        let bad_order = make_buy_order(10_000_000, token_a(), 999_999_999, 1);

        let mut accum = Accumulator::new((3, 1000), (1, 2));
        assert!(accum.try_add_order(&bad_order, &ident, &pool).is_err());
        assert!(accum.is_empty());
    }

    #[test]
    fn test_accumulator_matches_assemble_batch() {
        // Verify that the accumulator produces the same batch as assemble_batch
        // for a single pool with multiple orders
        use crate::sundaev4::batch::{assemble_batch, BatchLimits};

        let pool = make_pool(0xAA, 1_000_000_000, token_a(), 1_000_000_000);
        let ident = pool.pool_datum.identifier.clone();
        let fee = (3u64, 1000u64);
        let protocol_share = (1u64, 2u64);

        let orders: Vec<_> = (1..=3u64)
            .map(|slot| make_buy_order(10_000_000, token_a(), 1, slot))
            .collect();

        // Build via assemble_batch
        let batch_classic = assemble_batch(
            &pool, &orders, fee, protocol_share, &BatchLimits { max_orders: 30 },
        ).unwrap();

        // Build via accumulator (same order)
        let mut accum = Accumulator::new(fee, protocol_share);
        for order in &orders {
            accum.try_add_order(order, &ident, &pool).unwrap();
        }
        let accum_batches = accum.into_batches();
        assert_eq!(accum_batches.len(), 1);
        let batch_accum = &accum_batches[0];

        // Same number of swaps
        assert_eq!(batch_classic.swaps.len(), batch_accum.swaps.len());
        // Same final assets
        assert_eq!(batch_classic.final_assets, batch_accum.final_assets);
        // Same final_total_lp
        assert_eq!(batch_classic.final_total_lp, batch_accum.final_total_lp);
    }
}
