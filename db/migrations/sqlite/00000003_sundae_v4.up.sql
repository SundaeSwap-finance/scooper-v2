CREATE TABLE sundae_v4_txos (
    tx_id BLOB NOT NULL,
    txo_index INT NOT NULL,
    txo_type TEXT NOT NULL,
    created_slot BIGINT NOT NULL,
    spent_slot BIGINT,
    spent_height BIGINT,
    era INT NOT NULL,
    txo BLOB NOT NULL,
    address BLOB NOT NULL,
    datum BLOB NULL,
    PRIMARY KEY (tx_id, txo_index)
);
CREATE INDEX sundae_v4_txos_created_slot_idx ON sundae_v4_txos (created_slot);
CREATE INDEX sundae_v4_txos_spent_slot_idx ON sundae_v4_txos (spent_slot);
CREATE INDEX sundae_v4_txos_spent_height_idx ON sundae_v4_txos (spent_height);

CREATE TABLE sundae_v4_datums(
    hash BLOB PRIMARY KEY NOT NULL,
    datum BLOB NOT NULL,
    created_slot BIGINT NOT NULL
);
CREATE INDEX sundae_v4_datums_created_slot_idx ON sundae_v4_datums (created_slot);
