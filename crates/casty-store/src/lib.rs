//! The store of the durable types over a SQL database: PostgreSQL and the databases that speak its protocol, MySQL and
//! MariaDB and those that speak theirs, and SQLite, told apart by the scheme of a URL.
//!
//! A record is one row of `casty_records`, found by `id`: a digest of the actor and the key, of a fixed size, which
//! every database compares as bytes whatever its collation, its limit on the length of a key or the words it keeps for
//! itself. The actor and the key are kept beside it for whoever reads the table. The version orders the writes of a
//! key as bytes: a save keeps the greatest, and a drop forgets a record that is not later than it.
//!
//! What a store asks of a database is what they all do alike: a select, an update, an insert and a delete of one row
//! by `id`, and no upsert, whose syntax is each database's own. A save updates the row whose version is earlier than
//! its own. When nothing was updated it inserts the row, and when another save inserted it first, it reads the version
//! that save kept, and goes again when its own is later. What differs between the databases is the table, whose binary
//! types each names its own way, and the placeholders of PostgreSQL.

use std::fmt::Write as _;

use blake2::digest::consts::U32;
use blake2::{Blake2b, Digest};
use sqlx::any::{AnyPoolOptions, install_default_drivers};
use sqlx::error::ErrorKind;
use sqlx::{AnyPool, Database, Executor, MySql, Postgres, Row, Sqlite};

pub use sqlx::Error;

/// A record as a store gives it back: its version, and its state, or nothing for a deletion.
pub type Record = (Vec<u8>, Option<Vec<u8>>);

const LOAD: &str = "SELECT version, state FROM casty_records WHERE id = ?";
const UPDATE: &str = "UPDATE casty_records SET version = ?, state = ? WHERE id = ? AND version < ?";
const INSERT: &str =
    "INSERT INTO casty_records (id, actor, actor_key, version, state) VALUES (?, ?, ?, ?, ?)";
const KEPT: &str = "SELECT version FROM casty_records WHERE id = ?";
const DROP: &str = "DELETE FROM casty_records WHERE id = ? AND version <= ?";
const PROBE: &str = "SELECT id FROM casty_records WHERE 1 = 0";

// The driver of MySQL gives a `VARBINARY` back as text, so what is read is a `BLOB`. The key is never read.
const POSTGRES_TABLE: &str = "CREATE TABLE IF NOT EXISTS casty_records (
    id BYTEA PRIMARY KEY, actor TEXT NOT NULL, actor_key TEXT NOT NULL, version BYTEA NOT NULL, state BYTEA
)";
const MYSQL_TABLE: &str = "CREATE TABLE IF NOT EXISTS casty_records (
    id BINARY(32) PRIMARY KEY, actor LONGTEXT NOT NULL, actor_key LONGTEXT NOT NULL, version BLOB NOT NULL,
    state LONGBLOB
) CHARACTER SET utf8mb4";
const SQLITE_TABLE: &str = "CREATE TABLE IF NOT EXISTS casty_records (
    id BLOB PRIMARY KEY, actor TEXT NOT NULL, actor_key TEXT NOT NULL, version BLOB NOT NULL, state BLOB
) WITHOUT ROWID";

/// The records of one database, reached through a pool of connections to it.
#[derive(Debug)]
pub struct Sql {
    pool: AnyPool,
    statements: Statements,
}

/// The statements of a store, with the placeholders of its database.
#[derive(Debug)]
struct Statements {
    load: String,
    update: String,
    insert: String,
    kept: String,
    drop: String,
}

impl Statements {
    fn placed(place: fn(&str) -> String) -> Self {
        Self {
            load: place(LOAD),
            update: place(UPDATE),
            insert: place(INSERT),
            kept: place(KEPT),
            drop: place(DROP),
        }
    }
}

impl Sql {
    /// Connect to the database `url` names, and make its table when it has none. A URL whose scheme names none of
    /// the databases this knows is a `Configuration` error.
    pub async fn open(url: &str) -> Result<Self, Error> {
        install_default_drivers();
        let pool = AnyPoolOptions::new().connect(url).await?;
        match Self::over(&pool).await {
            Ok(statements) => Ok(Self { pool, statements }),
            Err(failed) => {
                pool.close().await;
                Err(failed)
            }
        }
    }

    /// Make the table of the database of `pool`, and the statements it takes.
    async fn over(pool: &AnyPool) -> Result<Statements, Error> {
        let backend = pool.acquire().await?.backend_name().to_owned();
        let (table, place): (_, fn(&str) -> String) = match backend.as_str() {
            Postgres::NAME => (POSTGRES_TABLE, numbered),
            MySql::NAME => (MYSQL_TABLE, str::to_owned),
            Sqlite::NAME => (SQLITE_TABLE, str::to_owned),
            other => {
                return Err(Error::Configuration(
                    format!("{other} is not a database casty keeps records in").into(),
                ));
            }
        };
        if let Err(failed) = pool.execute(table).await {
            // Stores opening a database at once race to make the table, and PostgreSQL fails the ones that lose, with a
            // violation of the uniqueness of its catalog, instead of waiting for the winner. A table that is there is
            // all a store needs, whatever kept this one from making it.
            if pool.execute(PROBE).await.is_err() {
                return Err(failed);
            }
        }
        if backend == Sqlite::NAME {
            // A write-ahead log lets the processes sharing the file read while one of them writes. Processes opening a
            // new file at once race to switch it, and SQLite refuses the switches that lose without waiting: the file
            // keeps the log the winner set, which every connection to it then uses.
            let _ = pool.execute("PRAGMA journal_mode = WAL").await;
        }
        Ok(Statements::placed(place))
    }

    /// The record of `(actor, key)`, or nothing when there is none.
    pub async fn load(&self, actor: &str, key: &str) -> Result<Option<Record>, Error> {
        let id = id(actor, key);
        let row = sqlx::query(&self.statements.load)
            .bind(&id[..])
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| Ok((row.try_get(0)?, row.try_get(1)?)))
            .transpose()
    }

    /// Keep `(version, state)` as the record of `(actor, key)`, unless its record has a version that is not earlier.
    pub async fn save(
        &self,
        actor: &str,
        key: &str,
        version: &[u8],
        state: Option<&[u8]>,
    ) -> Result<(), Error> {
        let id = id(actor, key);
        loop {
            let updated = sqlx::query(&self.statements.update)
                .bind(version)
                .bind(state)
                .bind(&id[..])
                .bind(version)
                .execute(&self.pool)
                .await?;
            if updated.rows_affected() > 0 {
                return Ok(());
            }
            let inserted = sqlx::query(&self.statements.insert)
                .bind(&id[..])
                .bind(actor)
                .bind(key)
                .bind(version)
                .bind(state)
                .execute(&self.pool)
                .await;
            match inserted {
                Ok(_) => return Ok(()),
                Err(Error::Database(taken)) if taken.kind() == ErrorKind::UniqueViolation => {}
                Err(failed) => return Err(failed),
            }
            let kept: Option<Vec<u8>> = sqlx::query_scalar(&self.statements.kept)
                .bind(&id[..])
                .fetch_optional(&self.pool)
                .await?;
            // A row that is gone again was dropped in between, and one that is earlier was written in between.
            if kept.is_some_and(|kept| kept.as_slice() >= version) {
                return Ok(());
            }
        }
    }

    /// Forget the record of `(actor, key)` if its version is not later than `version`.
    pub async fn forget(&self, actor: &str, key: &str, version: &[u8]) -> Result<(), Error> {
        let id = id(actor, key);
        sqlx::query(&self.statements.drop)
            .bind(&id[..])
            .bind(version)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Close every connection, once the statements under way are done.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

/// The row of `(actor, key)`: the length of the actor goes first, so that no other pair spells the same bytes.
fn id(actor: &str, key: &str) -> [u8; 32] {
    Blake2b::<U32>::new()
        .chain_update((actor.len() as u64).to_be_bytes())
        .chain_update(actor)
        .chain_update(key)
        .finalize()
        .into()
}

/// `statement` with the placeholders of PostgreSQL, `$1`, `$2`, ..., in place of each `?`.
fn numbered(statement: &str) -> String {
    let mut parts = statement.split('?');
    let mut numbered = parts.next().unwrap_or_default().to_owned();
    for (index, part) in parts.enumerate() {
        let _ = write!(numbered, "${}{part}", index + 1);
    }
    numbered
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::task::JoinSet;

    use super::*;

    /// A new SQLite file named after `test`, removed with its log when this is dropped.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(test: &str) -> Self {
            let file =
                std::env::temp_dir().join(format!("casty-store-{}-{test}.db", std::process::id()));
            let scratch = Self(file);
            scratch.remove();
            scratch
        }

        /// Not `sqlite::memory:`: each connection of the pool would open a database of its own.
        fn url(&self) -> String {
            format!("sqlite://{}?mode=rwc", self.0.display())
        }

        fn remove(&self) {
            for ending in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{ending}", self.0.display()));
            }
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            self.remove();
        }
    }

    /// A store on a new file named after `test`, and the file.
    async fn opened(test: &str) -> (Scratch, Sql) {
        let file = Scratch::new(test);
        let store = Sql::open(&file.url()).await.expect("a new file opens");
        (file, store)
    }

    fn version(order: u8) -> Vec<u8> {
        let mut version = vec![0; 32];
        version[31] = order;
        version
    }

    #[tokio::test]
    async fn a_save_is_kept_only_over_an_earlier_version() {
        let (_file, store) = opened("save").await;
        assert_eq!(store.load("a", "k").await.unwrap(), None);
        store
            .save("a", "k", &version(2), Some(b"two"))
            .await
            .unwrap();
        store
            .save("a", "k", &version(1), Some(b"one"))
            .await
            .unwrap();
        store
            .save("a", "k", &version(2), Some(b"again"))
            .await
            .unwrap();
        assert_eq!(
            store.load("a", "k").await.unwrap(),
            Some((version(2), Some(b"two".to_vec())))
        );
        store.save("a", "k", &version(3), None).await.unwrap();
        assert_eq!(
            store.load("a", "k").await.unwrap(),
            Some((version(3), None))
        );
    }

    #[tokio::test]
    async fn a_drop_forgets_only_a_record_that_is_not_later() {
        let (_file, store) = opened("drop").await;
        store.save("a", "k", &version(2), None).await.unwrap();
        store.forget("a", "k", &version(1)).await.unwrap();
        assert_eq!(
            store.load("a", "k").await.unwrap(),
            Some((version(2), None))
        );
        store.forget("a", "k", &version(2)).await.unwrap();
        assert_eq!(store.load("a", "k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn of_saves_racing_for_a_key_the_greatest_is_kept() {
        let (_file, store) = opened("race").await;
        let store = Arc::new(store);
        let mut saving = JoinSet::new();
        for order in [5, 1, 9, 3, 7, 2, 8, 4, 6] {
            let store = Arc::clone(&store);
            saving
                .spawn(async move { store.save("a", "k", &version(order), Some(&[order])).await });
        }
        while let Some(saved) = saving.join_next().await {
            saved.unwrap().unwrap();
        }
        assert_eq!(
            store.load("a", "k").await.unwrap(),
            Some((version(9), Some(vec![9])))
        );
    }

    /// Stores opening a database at once race to make its table: each on a thread of its own, or the handshakes of
    /// their connections, one after the other on one thread, space them out. On the databases of `CASTY_STORES`,
    /// which `make test-stores` starts for the tests alone, the table goes first, so that they race again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn stores_opening_a_database_without_the_table_at_once_all_open() {
        let file = Scratch::new("at-once");
        let mut urls = vec![file.url()];
        for url in std::env::var("CASTY_STORES")
            .unwrap_or_default()
            .split_whitespace()
        {
            install_default_drivers();
            let pool = AnyPool::connect(url).await.expect("the database is up");
            pool.execute("DROP TABLE IF EXISTS casty_records")
                .await
                .unwrap();
            pool.close().await;
            urls.push(url.to_owned());
        }
        for url in urls {
            let mut opening = JoinSet::new();
            for _ in 0..16 {
                let url = url.clone();
                opening.spawn(async move { Sql::open(&url).await });
            }
            while let Some(opened) = opening.join_next().await {
                let store = opened
                    .unwrap()
                    .unwrap_or_else(|failed| panic!("{url}: {failed}"));
                store.close().await;
            }
        }
    }

    #[tokio::test]
    async fn a_url_of_another_database_is_refused() {
        let refused = Sql::open("redis://localhost").await.unwrap_err();
        assert!(matches!(refused, Error::Configuration(_)), "{refused}");
    }

    #[test]
    fn keys_a_separator_would_confuse_are_rows_apart() {
        assert_ne!(id("a/b", "c"), id("a", "b/c"));
        assert_ne!(id("ab", "c"), id("a", "bc"));
    }

    #[test]
    fn postgresql_numbers_its_placeholders() {
        assert_eq!(
            numbered(DROP),
            "DELETE FROM casty_records WHERE id = $1 AND version <= $2"
        );
    }
}
