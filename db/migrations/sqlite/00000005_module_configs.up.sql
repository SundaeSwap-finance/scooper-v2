-- Replace the CS-specific `pool_configs` table with a generic per-module
-- table. Any module that stores a config hash in pool.module_state (CS,
-- fee_split, CL, etc.) can have its on-chain config persisted here.
--
-- The old table only stored CS configs and is rebuildable from chain, so
-- we drop and recreate rather than migrate.
DROP TABLE IF EXISTS sundae_v3_pool_configs;
DROP TABLE IF EXISTS sundae_v4_pool_configs;

CREATE TABLE sundae_v3_module_configs (
    pool_id BLOB NOT NULL,
    module_hash BLOB NOT NULL,
    config_cbor BLOB NOT NULL,
    created_slot BIGINT NOT NULL,
    PRIMARY KEY (pool_id, module_hash)
);
CREATE INDEX sundae_v3_module_configs_created_slot_idx ON sundae_v3_module_configs (created_slot);

CREATE TABLE sundae_v4_module_configs (
    pool_id BLOB NOT NULL,
    module_hash BLOB NOT NULL,
    config_cbor BLOB NOT NULL,
    created_slot BIGINT NOT NULL,
    PRIMARY KEY (pool_id, module_hash)
);
CREATE INDEX sundae_v4_module_configs_created_slot_idx ON sundae_v4_module_configs (created_slot);
