DROP INDEX sundae_v3_datums_created_slot_idx;
DROP TABLE sundae_v3_datums;
ALTER TABLE sundae_v3_txos DROP COLUMN address;
ALTER TABLE sundae_v3_txos DROP COLUMN datum;
