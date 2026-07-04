-- Posted strategy intents (SignedStrategyExecutions + execution hints).
-- Persisted so a scooper restart doesn't drop live intents; pruned
-- aggressively when the target order is spent or the window expires.
CREATE TABLE sundae_v4_strategy_intents (
    intent_id BLOB NOT NULL PRIMARY KEY,
    order_tx_id BLOB NOT NULL,
    order_index BIGINT NOT NULL,
    sse_cbor BLOB NOT NULL,
    hint TEXT,
    expiry_ms BIGINT NOT NULL,
    received_at_ms BIGINT NOT NULL
);
CREATE INDEX sundae_v4_strategy_intents_order_idx
    ON sundae_v4_strategy_intents (order_tx_id, order_index);
CREATE INDEX sundae_v4_strategy_intents_expiry_idx
    ON sundae_v4_strategy_intents (expiry_ms);
