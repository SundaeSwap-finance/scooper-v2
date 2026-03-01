CREATE TABLE sundae_v3_txos (
    tx_id BLOB NOT NULL,
    txo_index INT NOT NULL,
    txo_type TEXT NOT NULL,
    created_slot BIGINT NOT NULL,
    spent_slot BIGINT,
    spent_height BIGINT,
    spent_tx_id BLOB NULL,
    era INT NOT NULL,
    txo BLOB NOT NULL,
    PRIMARY KEY (tx_id, txo_index)
);
CREATE INDEX sundae_v3_txos_created_slot_idx ON sundae_v3_txos (created_slot);
CREATE INDEX sundae_v3_txos_spent_slot_idx ON sundae_v3_txos (spent_slot);
CREATE INDEX sundae_v3_txos_spent_height_idx ON sundae_v3_txos (spent_height);

CREATE TABLE sundae_v3_scoop_records (
    tx_id BLOB NOT NULL PRIMARY KEY,
    slot BIGINT NOT NULL,
    pool_id BLOB NOT NULL,
    n_orders INT NOT NULL,
    scooper BLOB NOT NULL
);
CREATE INDEX sundae_v3_scoop_records_slot_idx ON sundae_v3_scoop_records (slot);
CREATE INDEX sundae_v3_scoop_records_scooper_idx ON sundae_v3_scoop_records (scooper);

CREATE TABLE acropolis_cursors (
    id TEXT PRIMARY KEY NOT NULL,
    bytes BLOB NOT NULL
);
