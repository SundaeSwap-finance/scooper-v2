-- Terminal intent status tombstones: when an intent leaves the live set
-- (executed / expired / order gone), keep a small row so the submitter can
-- still query the outcome by intent id. sse_cbor is blanked on
-- terminalisation; tombstones are deep-cleaned well after expiry.
ALTER TABLE sundae_v4_strategy_intents ADD COLUMN status TEXT;
ALTER TABLE sundae_v4_strategy_intents ADD COLUMN status_tx BLOB;
