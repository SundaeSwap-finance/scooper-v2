DROP TABLE IF EXISTS sundae_v3_module_configs;
DROP TABLE IF EXISTS sundae_v4_module_configs;

-- Recreate the old pool_configs tables (down-migration restores the prior
-- schema; data is not recoverable here — operators rebootstrap from chain).
CREATE TABLE sundae_v3_pool_configs (
    pool_id BLOB NOT NULL PRIMARY KEY,
    config_cbor BLOB NOT NULL,
    created_slot BIGINT NOT NULL
);
CREATE INDEX sundae_v3_pool_configs_created_slot_idx ON sundae_v3_pool_configs (created_slot);

CREATE TABLE sundae_v4_pool_configs (
    pool_id BLOB NOT NULL PRIMARY KEY,
    config_cbor BLOB NOT NULL,
    created_slot BIGINT NOT NULL
);
CREATE INDEX sundae_v4_pool_configs_created_slot_idx ON sundae_v4_pool_configs (created_slot);
