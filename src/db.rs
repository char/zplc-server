use std::path::Path;

use anyhow::{Context, Result, bail};
use deadpool_sqlite::{Config, Runtime};
use rusqlite::{Connection, OptionalExtension};

pub use deadpool_sqlite::Pool;

pub struct Entry {
    pub seq: u64,
    pub did: String,
    pub created_at: String,
}

fn initialize(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 5000;

         CREATE TABLE IF NOT EXISTS plc_idents (
           id INTEGER PRIMARY KEY NOT NULL,
           did TEXT NOT NULL UNIQUE ON CONFLICT IGNORE
         ) STRICT;

         CREATE TABLE IF NOT EXISTS meta (
           key TEXT PRIMARY KEY NOT NULL,
           value TEXT NOT NULL
         ) STRICT;",
    )?;
    Ok(())
}

pub fn open(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(path)?;
    initialize(&conn)?;
    Ok(conn)
}

pub async fn open_pool(path: &Path, max_size: usize) -> Result<Pool> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let pool = Config::new(path)
        .builder(Runtime::Tokio1)?
        .max_size(max_size)
        .build()?;

    let mut connections = Vec::with_capacity(max_size);
    for _ in 0..max_size {
        let conn = pool.get().await?;
        conn.interact(|conn| initialize(conn))
            .await
            .map_err(|error| anyhow::anyhow!("database interaction failed: {error}"))??;
        connections.push(conn);
    }
    Ok(pool)
}

pub fn latest_seq(conn: &Connection) -> Result<u64> {
    let value: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'latest_seq'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    value.map_or(Ok(0), |value| {
        value.parse().context("invalid latest_seq in database")
    })
}

pub fn ingest(conn: &mut Connection, entries: &[Entry]) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    let last = entries.last().unwrap();
    let tx = conn.transaction()?;
    let latest = latest_seq(&tx)?;
    for (offset, entry) in entries.iter().enumerate() {
        let expected = latest + offset as u64 + 1;
        if entry.seq != expected {
            bail!(
                "non-contiguous PLC sequence: expected {expected}, got {}",
                entry.seq
            );
        }
    }

    {
        let mut insert = tx.prepare_cached("INSERT INTO plc_idents (did) VALUES (?)")?;
        for entry in entries {
            insert.execute([&entry.did])?;
        }
    }
    tx.execute(
        "INSERT INTO meta (key, value) VALUES ('latest_seq', ?1)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        [last.seq.to_string()],
    )?;
    tx.execute(
        "INSERT INTO meta (key, value) VALUES ('latest_created_at', ?1)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        [&last.created_at],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn zplc_to_did(conn: &Connection, id: i64) -> Result<Option<String>> {
    conn.query_row("SELECT did FROM plc_idents WHERE id = ?", [id], |row| {
        row.get(0)
    })
    .optional()
    .map_err(Into::into)
}

pub fn did_to_zplc(conn: &Connection, did: &str) -> Result<Option<i64>> {
    conn.query_row("SELECT id FROM plc_idents WHERE did = ?", [did], |row| {
        row.get(0)
    })
    .optional()
    .map_err(Into::into)
}

pub fn latest_created_at(conn: &Connection) -> Result<Option<String>> {
    conn.query_row(
        "SELECT value FROM meta WHERE key = 'latest_created_at'",
        [],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_follow_first_appearance() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "CREATE TABLE plc_idents (
               id INTEGER PRIMARY KEY NOT NULL,
               did TEXT NOT NULL UNIQUE ON CONFLICT IGNORE
             ) STRICT;
             CREATE TABLE meta (
               key TEXT PRIMARY KEY NOT NULL,
               value TEXT NOT NULL
             ) STRICT;",
        )?;

        ingest(
            &mut conn,
            &[
                Entry {
                    seq: 1,
                    did: "did:plc:a".into(),
                    created_at: "one".into(),
                },
                Entry {
                    seq: 2,
                    did: "did:plc:b".into(),
                    created_at: "two".into(),
                },
                Entry {
                    seq: 3,
                    did: "did:plc:a".into(),
                    created_at: "three".into(),
                },
                Entry {
                    seq: 4,
                    did: "did:plc:c".into(),
                    created_at: "four".into(),
                },
            ],
        )?;

        assert_eq!(did_to_zplc(&conn, "did:plc:a")?, Some(1));
        assert_eq!(did_to_zplc(&conn, "did:plc:b")?, Some(2));
        assert_eq!(did_to_zplc(&conn, "did:plc:c")?, Some(3));
        assert_eq!(latest_seq(&conn)?, 4);
        assert_eq!(latest_created_at(&conn)?.as_deref(), Some("four"));
        Ok(())
    }

    #[test]
    fn rejects_gaps_atomically() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "CREATE TABLE plc_idents (
               id INTEGER PRIMARY KEY NOT NULL,
               did TEXT NOT NULL UNIQUE ON CONFLICT IGNORE
             ) STRICT;
             CREATE TABLE meta (
               key TEXT PRIMARY KEY NOT NULL,
               value TEXT NOT NULL
             ) STRICT;",
        )?;

        assert!(
            ingest(
                &mut conn,
                &[Entry {
                    seq: 2,
                    did: "did:plc:a".into(),
                    created_at: "two".into()
                }],
            )
            .is_err()
        );
        assert_eq!(did_to_zplc(&conn, "did:plc:a")?, None);
        Ok(())
    }
}
