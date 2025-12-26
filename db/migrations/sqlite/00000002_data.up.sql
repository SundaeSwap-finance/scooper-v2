ALTER TABLE sundae_v3_txos
ADD COLUMN datum BLOB NULL;

CREATE TABLE sundae_datums(
    hash BLOB PRIMARY KEY NOT NULL,
    datum BLOB NOT NULL,
    created_slot BIGINT NOT NULL
);
CREATE INDEX sundae_datums_created_slot_idx ON sundae_datums (created_slot);