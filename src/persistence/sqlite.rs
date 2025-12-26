use std::{collections::HashMap, path::PathBuf};

use acropolis_module_custom_indexer::cursor_store::{CursorEntry, CursorSaveError};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use sqlx::{
    FromRow, Pool, Row, Sqlite,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow},
};
use tracing::warn;

use crate::{
    cardano_types::TransactionInput,
    persistence::{
        CursorDaoImpl, PersistedDatum, PersistedTxo, Persistence, SundaeV3Dao, SundaeV3TxChanges,
    },
};

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct SqliteConfig {
    filename: Option<PathBuf>,
}
impl SqliteConfig {
    fn to_options(&self) -> (SqlitePoolOptions, SqliteConnectOptions) {
        let mut pool_opts = SqlitePoolOptions::new();
        let mut conn_opts = SqliteConnectOptions::new();
        if let Some(filename) = &self.filename {
            conn_opts = conn_opts.filename(filename).create_if_missing(true);
        } else {
            warn!(
                "No sqlite filename specified, storing in memory by default. Set persistence.sqlite.filename in configuration to fix this."
            );
            pool_opts = pool_opts
                .max_connections(1)
                .idle_timeout(None)
                .max_lifetime(None);
            conn_opts = conn_opts.in_memory(true);
        }
        (pool_opts, conn_opts)
    }
}

pub struct SqlitePersistence {
    pool: Pool<Sqlite>,
}

impl SqlitePersistence {
    pub async fn new(config: &SqliteConfig) -> Result<Self> {
        let (pool_opts, conn_opts) = config.to_options();
        let pool = pool_opts.connect_with(conn_opts).await?;
        sqlx::migrate!("db/migrations/sqlite").run(&pool).await?;
        Ok(Self { pool })
    }
}

impl Persistence for SqlitePersistence {
    fn sundae_v3_dao(&self) -> Box<dyn super::SundaeV3Dao> {
        Box::new(SqliteSundaeV3Dao {
            pool: self.pool.clone(),
        })
    }

    fn cursor_store(&self) -> super::CursorDao {
        super::CursorDao(Box::new(SqliteCursorDaoImpl {
            pool: self.pool.clone(),
        }))
    }
}

pub struct SqliteSundaeV3Dao {
    pool: Pool<Sqlite>,
}

#[async_trait]
impl SundaeV3Dao for SqliteSundaeV3Dao {
    async fn apply_tx_changes(&self, changes: SundaeV3TxChanges) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        if !changes.created_txos.is_empty() {
            let insert_created_txo_query = {
                let column_names = "tx_id, txo_index, txo_type, created_slot, spent_slot, spent_height, era, txo, datum";
                let values_clauses =
                    vec!["(?,?,?,?,NULL,NULL,?,?,?)".to_string(); changes.created_txos.len()]
                        .join(",");
                format!("INSERT INTO sundae_v3_txos ({column_names}) VALUES {values_clauses};")
            };
            let mut query = sqlx::query(&insert_created_txo_query);

            for created_txo in changes.created_txos {
                query = query
                    .bind(created_txo.txo_id.0.transaction_id.to_vec())
                    .bind(created_txo.txo_id.0.index as i64)
                    .bind(created_txo.txo_type)
                    .bind(created_txo.created_slot as i64)
                    .bind(created_txo.era)
                    .bind(created_txo.txo)
                    .bind(created_txo.datum);
            }

            query.execute(&mut *tx).await?;
        }

        for spent_txo in changes.spent_txos {
            sqlx::query(
                "UPDATE sundae_v3_txos SET spent_slot = ?, spent_height = ? WHERE tx_id = ? AND txo_index = ?;",
            )
            .bind(changes.slot as i64)
            .bind(changes.height as i64)
            .bind(spent_txo.0.transaction_id.to_vec())
            .bind(spent_txo.0.index as i64)
            .execute(&mut *tx)
            .await?;
        }

        if !changes.metadata_datums.is_empty() {
            let insert_datum_query = {
                let column_names = "hash, datum, created_slot";
                let values_clauses =
                    vec!["(?,?,?)".to_string(); changes.metadata_datums.len()].join(",");
                format!(
                    "INSERT INTO sundae_datums ({column_names}) VALUES {values_clauses} ON CONFLICT DO UPDATE SET created_slot = excluded.created_slot;"
                )
            };
            let mut query = sqlx::query(&insert_datum_query);

            for datum in changes.metadata_datums {
                query = query
                    .bind(datum.hash)
                    .bind(datum.datum)
                    .bind(datum.created_slot as i64)
            }

            query.execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn rollback(&self, slot: u64) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("DELETE FROM sundae_v3_txos WHERE created_slot > ?;")
            .bind(slot as i64)
            .execute(&mut *tx)
            .await?;

        sqlx::query(
            "UPDATE sundae_v3_txos SET spent_slot = NULL, spent_height = NULL WHERE spent_slot > ?",
        )
        .bind(slot as i64)
        .execute(&mut *tx)
        .await?;

        sqlx::query("DELETE FROM sundae_datums WHERE created_slot > ?;")
            .bind(slot as i64)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn load_datums(&self) -> Result<Vec<PersistedDatum>> {
        let query = "
            SELECT hash, datum, created_slot
            FROM sundae_datums
            ORDER BY created_slot, hash
        ";
        Ok(sqlx::query_as(query).fetch_all(&self.pool).await?)
    }

    async fn load_txos(&self) -> Result<Vec<PersistedTxo>> {
        let query = "
            SELECT tx_id, txo_index, txo_type, created_slot, era, txo, datum
            FROM sundae_v3_txos
            WHERE spent_slot IS NULL
            ORDER BY created_slot, tx_id, txo_index;
        ";
        Ok(sqlx::query_as(query).fetch_all(&self.pool).await?)
    }

    async fn prune_txos(&self, min_height: u64) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM sundae_v3_txos WHERE spent_height < ?")
            .bind(min_height as i64)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

impl FromRow<'_, SqliteRow> for PersistedTxo {
    fn from_row(row: &'_ SqliteRow) -> Result<Self, sqlx::Error> {
        let tx_id: Vec<u8> = row.try_get("tx_id")?;
        let txo_index: i64 = row.try_get("txo_index")?;
        let txo_type: String = row.try_get("txo_type")?;
        let created_slot: i64 = row.try_get("created_slot")?;
        let era: u16 = row.try_get("era")?;
        let txo: Vec<u8> = row.try_get("txo")?;
        let datum: Option<Vec<u8>> = row.try_get("datum")?;

        Ok(Self {
            txo_id: TransactionInput::new(tx_id.as_slice().into(), txo_index as u64),
            txo_type,
            created_slot: created_slot as u64,
            era,
            txo,
            datum,
        })
    }
}

impl FromRow<'_, SqliteRow> for PersistedDatum {
    fn from_row(row: &'_ SqliteRow) -> Result<Self, sqlx::Error> {
        let hash: Vec<u8> = row.try_get("hash")?;
        let datum: Vec<u8> = row.try_get("datum")?;
        let created_slot: i64 = row.try_get("created_slot")?;
        Ok(Self {
            hash,
            datum,
            created_slot: created_slot as u64,
        })
    }
}

struct SqliteCursorDaoImpl {
    pool: Pool<Sqlite>,
}

#[async_trait]
impl CursorDaoImpl for SqliteCursorDaoImpl {
    async fn load(&self) -> Result<HashMap<String, CursorEntry>> {
        let query = "
            SELECT id, bytes
            FROM acropolis_cursors;
        ";
        let entries = sqlx::query(query)
            .try_map(parse_cursor_entry)
            .fetch_all(&self.pool)
            .await?;
        let mut result = HashMap::new();
        for (id, bytes) in entries {
            let cursor = serde_json::from_slice(&bytes)?;
            result.insert(id, cursor);
        }
        Ok(result)
    }

    async fn save(&self, entries: &HashMap<String, CursorEntry>) -> Result<(), CursorSaveError> {
        let mut tx = self.pool.begin().await.map_err(|err| {
            warn!("could not open transaction: {err:#}");
            let failed = entries.keys().cloned().collect();
            CursorSaveError { failed }
        })?;
        sqlx::query("DELETE FROM acropolis_cursors;")
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                warn!("could not clear cursors: {err:#}");
                let failed = entries.keys().cloned().collect();
                CursorSaveError { failed }
            })?;
        let mut failed = vec![];
        for (id, cursor) in entries {
            if let Err(err) = save_entry(&mut tx, id, cursor).await {
                warn!("could not save cursor for {id}: {err:#}");
                failed.push(id.clone());
            }
        }
        tx.commit().await.map_err(|err| {
            warn!("could not commit transaction: {err:#}");
            let failed = entries.keys().cloned().collect();
            CursorSaveError { failed }
        })?;
        if failed.is_empty() {
            Ok(())
        } else {
            Err(CursorSaveError { failed })
        }
    }
}

fn parse_cursor_entry(row: SqliteRow) -> Result<(String, Vec<u8>), sqlx::error::Error> {
    let id: String = row.try_get("id")?;
    let bytes: Vec<u8> = row.try_get("bytes")?;
    Ok((id, bytes))
}

async fn save_entry(
    tx: &mut sqlx::SqliteTransaction<'_>,
    id: &str,
    cursor: &CursorEntry,
) -> Result<()> {
    let bytes = serde_json::to_vec(cursor)?;
    sqlx::query("INSERT INTO acropolis_cursors(id, bytes) VALUES(?,?);")
        .bind(id)
        .bind(bytes)
        .execute(tx.as_mut())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use acropolis_common::{Point, hash::Hash};
    use acropolis_module_custom_indexer::cursor_store::CursorStore;

    use super::*;

    async fn new_db() -> Result<SqlitePersistence> {
        SqlitePersistence::new(&SqliteConfig { filename: None }).await
    }

    fn preview_pool() -> PersistedTxo {
        let tx_id = "f9fad594fb6cda70fc7a05cf286a77c7c1218a0ecee4bb0d0946c767f3a745d1";
        let txo = "
            a30058393044a1eb2d9f58add4eb1932
            bd0048e6a1947e85e3fe4f32956a1104
            14cc27980a8557fe9db2c9ac0a2677f4
            d1306dbf10689983758f0b8dbe01821a
            01312d00a2581c44a1eb2d9f58add4eb
            1932bd0048e6a1947e85e3fe4f32956a
            110414a15820000de1402e74e6af9739
            616dd021f547bca1f68c937b566bb6ca
            2e4782e7600101581cfa3eff2047fdf9
            293c5feef4dc85ce58097ea1c6da4845
            a351535183a14574494e44591a01312d
            00028201d818585ad8799f581c2e74e6
            af9739616dd021f547bca1f68c937b56
            6bb6ca2e4782e760019f9f4040ff9f58
            1cfa3eff2047fdf9293c5feef4dc85ce
            58097ea1c6da4845a351535183457449
            4e4459ffff1a01312d000505d87a8000
            00ff
        "
        .split_whitespace()
        .collect::<String>();
        PersistedTxo {
            txo_id: TransactionInput::new(tx_id.parse().unwrap(), 0),
            txo_type: "pool".to_string(),
            created_slot: 48463593,
            era: 7,
            txo: hex::decode(txo).unwrap(),
            datum: None,
        }
    }

    fn preview_order() -> PersistedTxo {
        let tx_id = "9f7459d311f3b79bd3dccfe37231189d3bb7df2dd108c435af28687861e0acc3";
        let txo = "
            a300583910cfad1914b599d18bffd14d
            2bbd696019c2899cbdd6a03325cdf680
            bc121fd22e0b57ac206fefc763f8bfa0
            771919f5218b40691eea4514d0011a00
            c65d40028201d81858e1d8799fd8799f
            581c2baab4c73a1cd60176f903a29a9c
            92ed4237c88622da51e9179121a3ffd8
            799f581c121fd22e0b57ac206fefc763
            f8bfa0771919f5218b40691eea4514d0
            ff1a000f4240d8799fd8799fd8799f58
            1cc279a3fb3b4e62bbc78e288783b580
            45d4ae82a18867d8352d02775affd879
            9fd8799fd8799f581c121fd22e0b57ac
            206fefc763f8bfa0771919f5218b4069
            1eea4514d0ffffffffd87980ffd87a9f
            9f40401a00989680ff9f581c99b071ce
            8580d6a3a11b4902145adb8bfd0d2a03
            935af8cf66403e15465342455252591a
            00f65febffff43d87980ff
        "
        .split_whitespace()
        .collect::<String>();
        PersistedTxo {
            txo_id: TransactionInput::new(tx_id.parse().unwrap(), 0),
            txo_type: "order".to_string(),
            created_slot: 48465289,
            era: 7,
            txo: hex::decode(txo).unwrap(),
            datum: None,
        }
    }

    fn preview_order_2() -> PersistedTxo {
        let tx_id = "fa215edb442c87566e0c6eeefe50ec6ba189d556c14cab9c614f3d4cf64485d0";
        let txo = "
            a300583910cfad1914b599d18bffd14d
            2bbd696019c2899cbdd6a03325cdf680
            bc121fd22e0b57ac206fefc763f8bfa0
            771919f5218b40691eea4514d001821a
            002dc6c0a1581c44a1eb2d9f58add4eb
            1932bd0048e6a1947e85e3fe4f32956a
            110414a158200014df1070a5be631ece
            9fbb484c806a201aec847a362fa1e5d2
            783cd0df32b91a000f4240028201d818
            58f3d8799fd8799f581c70a5be631ece
            9fbb484c806a201aec847a362fa1e5d2
            783cd0df32b9ffd8799f581c121fd22e
            0b57ac206fefc763f8bfa0771919f521
            8b40691eea4514d0ff1a000f4240d879
            9fd8799fd8799f581cc279a3fb3b4e62
            bbc78e288783b58045d4ae82a18867d8
            352d02775affd8799fd8799fd8799f58
            1c121fd22e0b57ac206fefc763f8bfa0
            771919f5218b40691eea4514d0ffffff
            ffd87980ffd87c9f9f581c44a1eb2d9f
            58add4eb1932bd0048e6a1947e85e3fe
            4f32956a11041458200014df1070a5be
            631ece9fbb484c806a201aec847a362f
            a1e5d2783cd0df32b91a000f4240ffff
            43d87980ff
        "
        .split_whitespace()
        .collect::<String>();
        PersistedTxo {
            txo_id: TransactionInput::new(tx_id.parse().unwrap(), 0),
            txo_type: "order".to_string(),
            created_slot: 48467939,
            era: 7,
            txo: hex::decode(txo).unwrap(),
            datum: None,
        }
    }

    fn preview_datum() -> PersistedDatum {
        PersistedDatum {
            hash: hex::decode("8ecfafddfa732227ba5b494183fd3150a4c8614656e6182f92c25ee2d1480019").unwrap(),
            datum: hex::decode("d8799fd8799f581c711aba6b66849e39f31b99e21bce68addf0dc80426d01e84cfd8d30dffd8799f581c9095c31cb90ada28af85689f241acd13c52bc1be231a5bd0301bb7b6ff1a0007a120d8799fd8799fd87a9f581ccfad1914b599d18bffd14d2bbd696019c2899cbdd6a03325cdf680bcffd87a80ffd87a9f582018e7433189c114a1f0474624cfb69ba21bf751b6be574d4af2276f6f7976c717ffffd87a9f9f40401a0001cf62ff9f581c63f9a5fc96d4f87026e97af4569975016b50eef092a46859b61898e54f0014df106f7263666178746f6b656e1a00989680ffff43d87980ff").unwrap(),
            created_slot: 69882150,
        }
    }

    #[tokio::test]
    async fn should_load_txos() -> Result<()> {
        let db = new_db().await?;
        let dao = db.sundae_v3_dao();

        let pool = preview_pool();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: pool.created_slot,
            height: 1,
            created_txos: vec![pool.clone()],
            spent_txos: vec![],
            metadata_datums: vec![],
        })
        .await?;
        let order = preview_order();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: order.created_slot,
            height: 2,
            created_txos: vec![order.clone()],
            spent_txos: vec![],
            metadata_datums: vec![],
        })
        .await?;

        let txos = dao.load_txos().await?;
        assert_eq!(txos, vec![pool, order]);

        Ok(())
    }

    #[tokio::test]
    async fn should_load_datums() -> Result<()> {
        let db = new_db().await?;
        let dao = db.sundae_v3_dao();

        let datum = preview_datum();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: datum.created_slot,
            height: 0,
            created_txos: vec![],
            spent_txos: vec![],
            metadata_datums: vec![datum.clone()],
        })
        .await?;

        let datums = dao.load_datums().await?;
        assert_eq!(datums, vec![datum]);
        Ok(())
    }

    #[tokio::test]
    async fn should_update_datums_created_slot() -> Result<()> {
        let db = new_db().await?;
        let dao = db.sundae_v3_dao();

        let datum = preview_datum();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: datum.created_slot,
            height: 0,
            created_txos: vec![],
            spent_txos: vec![],
            metadata_datums: vec![datum.clone()],
        })
        .await?;

        let mut datum2 = datum.clone();
        datum2.created_slot = datum.created_slot + 20;
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: datum.created_slot,
            height: 0,
            created_txos: vec![],
            spent_txos: vec![],
            metadata_datums: vec![datum2.clone()],
        })
        .await?;

        let datums = dao.load_datums().await?;
        assert_eq!(datums, vec![datum2]);
        Ok(())
    }

    #[tokio::test]
    async fn should_not_load_spent_txos() -> Result<()> {
        let db = new_db().await?;
        let dao = db.sundae_v3_dao();

        let pool = preview_pool();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: pool.created_slot,
            height: 1,
            created_txos: vec![pool.clone()],
            spent_txos: vec![],
            metadata_datums: vec![],
        })
        .await?;
        let order = preview_order();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: order.created_slot,
            height: 2,
            created_txos: vec![order.clone()],
            spent_txos: vec![],
            metadata_datums: vec![],
        })
        .await?;

        // The order TXO was spent
        let order = preview_order();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: order.created_slot + 10,
            height: 3,
            created_txos: vec![],
            spent_txos: vec![order.txo_id.clone()],
            metadata_datums: vec![],
        })
        .await?;

        let txos = dao.load_txos().await?;
        assert_eq!(txos, vec![pool]);

        Ok(())
    }

    #[tokio::test]
    async fn should_remove_rolled_back_txos() -> Result<()> {
        let db = new_db().await?;
        let dao = db.sundae_v3_dao();

        let pool = preview_pool();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: pool.created_slot,
            height: 1,
            created_txos: vec![pool.clone()],
            spent_txos: vec![],
            metadata_datums: vec![],
        })
        .await?;
        let order = preview_order();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: order.created_slot,
            height: 2,
            created_txos: vec![order.clone()],
            spent_txos: vec![],
            metadata_datums: vec![],
        })
        .await?;

        // Roll back to the pool creation, which was before the order creation
        dao.rollback(pool.created_slot).await?;

        let txos = dao.load_txos().await?;
        assert_eq!(txos, vec![pool]);

        Ok(())
    }

    #[tokio::test]
    async fn should_load_rolled_back_spends() -> Result<()> {
        let db = new_db().await?;
        let dao = db.sundae_v3_dao();

        let pool = preview_pool();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: pool.created_slot,
            height: 1,
            created_txos: vec![pool.clone()],
            spent_txos: vec![],
            metadata_datums: vec![],
        })
        .await?;
        let order = preview_order();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: order.created_slot,
            height: 2,
            created_txos: vec![order.clone()],
            spent_txos: vec![],
            metadata_datums: vec![],
        })
        .await?;

        // the order was spent
        let order = preview_order();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: order.created_slot + 10,
            height: 3,
            created_txos: vec![],
            spent_txos: vec![order.txo_id.clone()],
            metadata_datums: vec![],
        })
        .await?;

        // Roll back to the order creation
        dao.rollback(order.created_slot).await?;

        let txos = dao.load_txos().await?;
        assert_eq!(txos, vec![pool, order]);

        Ok(())
    }

    #[tokio::test]
    async fn should_prune_history() -> Result<()> {
        let db = new_db().await?;
        let dao = db.sundae_v3_dao();

        // Height 1: pool created
        let pool = preview_pool();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: pool.created_slot,
            height: 1,
            created_txos: vec![pool.clone()],
            spent_txos: vec![],
            metadata_datums: vec![],
        })
        .await?;

        // Height 2: order created
        let order = preview_order();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: order.created_slot,
            height: 2,
            created_txos: vec![order.clone()],
            spent_txos: vec![],
            metadata_datums: vec![],
        })
        .await?;

        // Height 3: order spent
        let order = preview_order();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: order.created_slot + 10,
            height: 3,
            created_txos: vec![],
            spent_txos: vec![order.txo_id.clone()],
            metadata_datums: vec![],
        })
        .await?;

        // Height 6: new order placed
        let order_2 = preview_order_2();
        dao.apply_tx_changes(SundaeV3TxChanges {
            slot: order_2.created_slot,
            height: 6,
            created_txos: vec![order_2],
            spent_txos: vec![],
            metadata_datums: vec![],
        })
        .await?;

        // now prune history to after that order spend
        dao.prune_txos(4).await?;

        // Roll back to the order creation
        dao.rollback(order.created_slot).await?;

        // We are no longer tracking the order, but we didn't forget the pool
        let txos = dao.load_txos().await?;
        assert_eq!(txos, vec![pool]);

        Ok(())
    }

    #[tokio::test]
    async fn cursor_store_should_load_no_cursors() -> Result<()> {
        let db = new_db().await?;
        let dao = db.cursor_store();

        assert!(dao.load().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn cursor_store_should_load_cursor() -> Result<()> {
        let db = new_db().await?;
        let dao = db.cursor_store();

        let tip = Point::Specific {
            hash: Hash::default(),
            slot: 1337,
        };
        let cursor = CursorEntry { tip, halted: false };
        let mut entries = HashMap::new();
        entries.insert("abc".to_string(), cursor.clone());

        dao.save(&entries).await?;

        let new_entries = dao.load().await?;
        assert_eq!(new_entries.len(), entries.len());
        let new_cursor = new_entries.get("abc").unwrap();
        assert_eq!(new_cursor.tip, cursor.tip);
        assert_eq!(new_cursor.halted, cursor.halted);

        Ok(())
    }

    #[tokio::test]
    async fn cursor_store_should_overwrite_cursor() -> Result<()> {
        let db = new_db().await?;
        let dao = db.cursor_store();

        let tip = Point::Specific {
            hash: Hash::default(),
            slot: 1337,
        };
        let mut cursor = CursorEntry { tip, halted: false };
        let mut entries = HashMap::new();
        entries.insert("abc".to_string(), cursor.clone());
        dao.save(&entries).await?;

        cursor.tip = Point::Specific {
            hash: Hash::default(),
            slot: 1338,
        };
        cursor.halted = true;
        entries.insert("abc".to_string(), cursor.clone());
        dao.save(&entries).await?;

        let new_entries = dao.load().await?;
        assert_eq!(new_entries.len(), entries.len());
        let new_cursor = new_entries.get("abc").unwrap();
        assert_eq!(new_cursor.tip, cursor.tip);
        assert_eq!(new_cursor.halted, cursor.halted);

        Ok(())
    }

    #[tokio::test]
    async fn cursor_store_should_remove_cursor() -> Result<()> {
        let db = new_db().await?;
        let dao = db.cursor_store();

        let tip = Point::Specific {
            hash: Hash::default(),
            slot: 1337,
        };
        let cursor = CursorEntry { tip, halted: false };
        let mut entries = HashMap::new();
        entries.insert("abc".to_string(), cursor.clone());
        dao.save(&entries).await?;

        entries.clear();
        dao.save(&entries).await?;

        assert!(dao.load().await?.is_empty());
        Ok(())
    }
}
