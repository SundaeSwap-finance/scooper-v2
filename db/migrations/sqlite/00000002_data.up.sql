ALTER TABLE sundae_v3_txos
ADD COLUMN datum BLOB NULL;

ALTER TABLE sundae_v3_txos
ADD COLUMN address BLOB NOT NULL;

CREATE TABLE sundae_v3_datums(
    hash BLOB PRIMARY KEY NOT NULL,
    datum BLOB NOT NULL,
    created_slot BIGINT NOT NULL
);
CREATE INDEX sundae_v3_datums_created_slot_idx ON sundae_v3_datums (created_slot);
