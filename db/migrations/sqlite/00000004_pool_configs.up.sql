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
