//! Minimal single-node SQLite catalog adapter.
//!
//! This module is intentionally independent of `catalog.rs`, `dag.rs`, and
//! `reference_store.rs`: it is an adapter seam, not a replacement for those models.
//! The catalog transaction is durable; DAG media is an explicitly separate
//! seam and is not claimed to be atomically committed with these rows.

#[cfg(any())]
mod legacy {
    use rusqlite::{params, Connection, OptionalExtension, Transaction};
    use std::path::Path;

    pub const DAG_DURABILITY_SEAM: &str =
    "DAG media must be flushed by its FileDevice/in-memory adapter before the catalog result is published; the cross-store handoff is not atomically committed";

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum CrashPoint {
        None,
        AfterCollections,
        AfterFiles,
        AfterVersions,
        AfterIntents,
        AfterResults,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct CollectionRecord {
        pub id: u64,
        pub name: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FileRecord {
        pub id: u64,
        pub collection_id: u64,
        pub name: String,
        pub head_version_id: Option<u64>,
        pub head_generation: u64,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct VersionRecord {
        pub id: u64,
        pub file_id: u64,
        pub generation: u64,
        pub commit_id: [u8; 32],
        pub parent_version_id: Option<u64>,
        pub size: u64,
        pub digest: [u8; 32],
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct IntentRecord {
        pub operation_id: u64,
        pub actor_id: u64,
        pub file_id: u64,
        pub state: String,
        pub expected_head_version_id: Option<u64>,
        pub expected_head_generation: u64,
        pub version_id: u64,
        pub abort_reason: Option<String>,
        pub pinned: bool,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct OperationRecord {
        pub operation_id: u64,
        pub actor_id: u64,
        pub kind: String,
        pub request_fingerprint: [u8; 32],
        pub result: Option<String>,
        pub error: Option<String>,
    }

    #[derive(Clone, Debug, Default, Eq, PartialEq)]
    pub struct CatalogBatch {
        pub collections: Vec<CollectionRecord>,
        pub files: Vec<FileRecord>,
        pub versions: Vec<VersionRecord>,
        pub intents: Vec<IntentRecord>,
        pub operations: Vec<OperationRecord>,
    }

    #[derive(Debug)]
    pub struct SqliteCatalogStore {
        connection: Connection,
    }

    impl SqliteCatalogStore {
        pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
            let connection = Connection::open(path)?;
            Self::from_connection(connection)
        }

        pub fn in_memory() -> rusqlite::Result<Self> {
            Self::from_connection(Connection::open_in_memory()?)
        }

        fn from_connection(connection: Connection) -> rusqlite::Result<Self> {
            connection.pragma_update(None, "journal_mode", "WAL")?;
            connection.pragma_update(None, "synchronous", "FULL")?;
            connection.pragma_update(None, "foreign_keys", "ON")?;
            connection.busy_timeout(std::time::Duration::from_secs(5))?;
            connection.execute_batch(SCHEMA)?;
            Ok(Self { connection })
        }

        pub fn persist(&mut self, batch: &CatalogBatch) -> rusqlite::Result<()> {
            self.persist_with_cut(batch, CrashPoint::None)
        }

        pub fn persist_with_cut(
            &mut self,
            batch: &CatalogBatch,
            cut: CrashPoint,
        ) -> rusqlite::Result<()> {
            let transaction = self.connection.transaction()?;
            insert_batch(&transaction, batch, cut)?;
            transaction.commit()
        }

        pub fn counts(&self) -> rusqlite::Result<CatalogCounts> {
            Ok(CatalogCounts {
                collections: count(&self.connection, "collections")?,
                files: count(&self.connection, "files")?,
                versions: count(&self.connection, "versions")?,
                intents: count(&self.connection, "intents")?,
                operations: count(&self.connection, "operations")?,
            })
        }

        pub fn durability_pragmas(&self) -> rusqlite::Result<(String, String)> {
            let journal_mode = self
                .connection
                .query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
            let synchronous: i64 = self
                .connection
                .query_row("PRAGMA synchronous", [], |row| row.get(0))?;
            Ok((journal_mode, synchronous.to_string()))
        }

        pub fn operation(&self, operation_id: u64) -> rusqlite::Result<Option<OperationRecord>> {
            self.connection
                .query_row(
                    "SELECT actor_id, kind, request_fingerprint, result, error
                 FROM operations WHERE operation_id = ?1",
                    [operation_id],
                    |row| {
                        let fingerprint: Vec<u8> = row.get(2)?;
                        Ok(OperationRecord {
                            operation_id,
                            actor_id: row.get(0)?,
                            kind: row.get(1)?,
                            request_fingerprint: bytes32(&fingerprint)?,
                            result: row.get(3)?,
                            error: row.get(4)?,
                        })
                    },
                )
                .optional()
        }
    }

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub struct CatalogCounts {
        pub collections: u64,
        pub files: u64,
        pub versions: u64,
        pub intents: u64,
        pub operations: u64,
    }

    fn insert_batch(
        transaction: &Transaction<'_>,
        batch: &CatalogBatch,
        cut: CrashPoint,
    ) -> rusqlite::Result<()> {
        for collection in &batch.collections {
            transaction.execute(
                "INSERT INTO collections(id, name) VALUES (?1, ?2)",
                params![collection.id, collection.name],
            )?;
        }
        fail_at(cut, CrashPoint::AfterCollections)?;

        for file in &batch.files {
            transaction.execute(
                "INSERT INTO files(id, collection_id, name, head_version_id, head_generation)
             VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    file.id,
                    file.collection_id,
                    file.name,
                    file.head_version_id,
                    file.head_generation
                ],
            )?;
        }
        fail_at(cut, CrashPoint::AfterFiles)?;

        for version in &batch.versions {
            transaction.execute(
            "INSERT INTO versions(id, file_id, generation, commit_id, parent_version_id, size, digest)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                version.id,
                version.file_id,
                version.generation,
                version.commit_id.as_slice(),
                version.parent_version_id,
                version.size,
                version.digest.as_slice()
            ],
        )?;
        }
        fail_at(cut, CrashPoint::AfterVersions)?;

        for intent in &batch.intents {
            transaction.execute(
                "INSERT INTO intents(
                operation_id, actor_id, file_id, state, expected_head_version_id,
                expected_head_generation, version_id, abort_reason, pinned
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    intent.operation_id,
                    intent.actor_id,
                    intent.file_id,
                    intent.state,
                    intent.expected_head_version_id,
                    intent.expected_head_generation,
                    intent.version_id,
                    intent.abort_reason,
                    intent.pinned
                ],
            )?;
        }
        fail_at(cut, CrashPoint::AfterIntents)?;

        for operation in &batch.operations {
            transaction.execute(
                "INSERT INTO operations(
                operation_id, actor_id, kind, request_fingerprint, result, error
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    operation.operation_id,
                    operation.actor_id,
                    operation.kind,
                    operation.request_fingerprint.as_slice(),
                    operation.result,
                    operation.error
                ],
            )?;
        }
        fail_at(cut, CrashPoint::AfterResults)
    }

    fn fail_at(actual: CrashPoint, expected: CrashPoint) -> rusqlite::Result<()> {
        if actual == expected {
            return Err(rusqlite::Error::InvalidParameterName(format!(
                "injected crash at {expected:?}"
            )));
        }
        Ok(())
    }

    fn count(connection: &Connection, table: &str) -> rusqlite::Result<u64> {
        connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
    }

    fn bytes32(bytes: &[u8]) -> rusqlite::Result<[u8; 32]> {
        bytes.try_into().map_err(|_| rusqlite::Error::InvalidQuery)
    }

    const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS collections (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    collection_id INTEGER NOT NULL REFERENCES collections(id),
    name TEXT NOT NULL,
    head_version_id INTEGER,
    head_generation INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS versions (
    id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL REFERENCES files(id),
    generation INTEGER NOT NULL,
    commit_id BLOB NOT NULL CHECK(length(commit_id) = 32),
    parent_version_id INTEGER,
    size INTEGER NOT NULL,
    digest BLOB NOT NULL CHECK(length(digest) = 32)
);
CREATE TABLE IF NOT EXISTS intents (
    operation_id INTEGER PRIMARY KEY,
    actor_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL REFERENCES files(id),
    state TEXT NOT NULL,
    expected_head_version_id INTEGER,
    expected_head_generation INTEGER NOT NULL,
    version_id INTEGER NOT NULL REFERENCES versions(id),
    abort_reason TEXT,
    pinned INTEGER NOT NULL CHECK(pinned IN (0, 1))
);
CREATE TABLE IF NOT EXISTS operations (
    operation_id INTEGER PRIMARY KEY,
    actor_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    request_fingerprint BLOB NOT NULL CHECK(length(request_fingerprint) = 32),
    result TEXT,
    error TEXT
);
"#;
}

// Sol schema v1 adapter.  The legacy module above is kept private only to
// make this uncommitted adapter migration easy to review; all public names
// below are the current API.
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use std::path::Path;

pub const DAG_DURABILITY_SEAM: &str =
    "SQLite catalog and DAG media are separate durable stores; their handoff is not atomic";
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CrashPoint {
    None,
    AfterCollections,
    AfterFiles,
    AfterVersions,
    AfterIntents,
    AfterResults,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrincipalRecord {
    pub id: u64,
    pub kind: String,
    pub state: String,
    pub authz_epoch: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipRecord {
    pub organization_id: u64,
    pub member_id: u64,
    pub capability: String,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionRecord {
    pub id: u64,
    pub owner_id: u64,
    pub name: String,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileRecord {
    pub id: u64,
    pub collection_id: u64,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionInfo {
    pub id: u64,
    pub owner_id: u64,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileInfo {
    pub id: u64,
    pub collection_id: u64,
    pub name: String,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionRecord {
    pub id: u64,
    pub file_id: u64,
    pub generation: u64,
    pub commit_id: [u8; 32],
    pub parent_version_id: Option<u64>,
    pub size: u64,
    pub digest: [u8; 32],
}

/// A catalog-owned version descriptor. It intentionally carries no DAG node
/// or operation record, so the coordinator can validate media through its
/// separate adapter without exposing storage internals to callers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogVersion {
    pub id: u64,
    pub file_id: u64,
    pub generation: u64,
    pub commit_id: [u8; 32],
    pub parent_version_id: Option<u64>,
    pub size: u64,
    pub digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogOperationState {
    InProgress,
    Succeeded,
    Failed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadAuthorization {
    Missing,
    Unauthorized,
    NotPublished,
    Authorized,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadRecord {
    pub file_id: u64,
    pub version_id: Option<u64>,
    pub generation: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntentRecord {
    pub operation_id: u64,
    pub actor_id: u64,
    pub file_id: u64,
    pub owner_epoch: u64,
    pub owner_nonce: u64,
    pub expected_head_version_id: Option<u64>,
    pub expected_head_generation: u64,
    pub candidate_version_id: Option<u64>,
    pub state: String,
    pub abort_reason: Option<String>,
    pub pinned: bool,
    pub request_fingerprint: [u8; 32],
    pub authz_epoch: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationRecord {
    pub operation_id: u64,
    pub actor_id: u64,
    pub kind: String,
    pub request_fingerprint: [u8; 32],
    pub state: String,
    pub result: Option<String>,
    pub error: Option<String>,
}

/// A fencing epoch owned by the single-node coordinator.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CoordinatorEpoch(u64);

impl CoordinatorEpoch {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryIntent {
    Nonterminal(IntentRecord),
    PublishedTombstone(IntentRecord),
    AbortedTombstone(IntentRecord),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryWork {
    Resume(IntentRecord),
    TombstoneDagBinding {
        intent: IntentRecord,
        terminal: TombstoneTerminal,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TombstoneTerminal {
    Published,
    Aborted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EpochClaim {
    Claimed(CoordinatorEpoch),
    Stale { current: CoordinatorEpoch },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClaimIntentOutcome {
    Claimed(RecoveryIntent),
    Missing,
    Fenced {
        current: CoordinatorEpoch,
    },
    AlreadyClaimed {
        owner_epoch: CoordinatorEpoch,
        owner_nonce: u64,
    },
    FutureOwner {
        owner_epoch: CoordinatorEpoch,
        current: CoordinatorEpoch,
    },
    Terminal(RecoveryIntent),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum T2Outcome {
    Applied,
    MissingIntent,
    Fenced,
    NotPreparing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum T3Outcome {
    Published,
    AlreadyPublished,
    MissingIntent,
    MissingOperation,
    Fenced,
    NotCommitDurable,
    AuthorizationDenied,
    MissingCandidate,
    VersionConflict,
    HeadConflict,
}
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogBatch {
    pub principals: Vec<PrincipalRecord>,
    pub memberships: Vec<MembershipRecord>,
    pub collections: Vec<CollectionRecord>,
    pub files: Vec<FileRecord>,
    pub heads: Vec<HeadRecord>,
    pub versions: Vec<VersionRecord>,
    pub intents: Vec<IntentRecord>,
    pub operations: Vec<OperationRecord>,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CatalogCounts {
    pub principals: u64,
    pub memberships: u64,
    pub collections: u64,
    pub files: u64,
    pub heads: u64,
    pub versions: u64,
    pub intents: u64,
    pub operations: u64,
}
#[derive(Debug)]
pub struct SqliteCatalogStore {
    connection: Connection,
    committed_transactions: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CatalogDurabilityMetrics {
    pub committed_transactions: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalCheckpointMode {
    Passive,
    Full,
    Restart,
    Truncate,
}

impl WalCheckpointMode {
    const fn as_sql(self) -> &'static str {
        match self {
            Self::Passive => "PASSIVE",
            Self::Full => "FULL",
            Self::Restart => "RESTART",
            Self::Truncate => "TRUNCATE",
        }
    }
}

fn validate_schema_v3(connection: &Connection) -> rusqlite::Result<()> {
    for statement in SCHEMA_V3.split(';') {
        let statement = statement.trim();
        let Some(rest) = statement.strip_prefix("CREATE TABLE IF NOT EXISTS ") else {
            continue;
        };
        let Some((table, _)) = rest.split_once('(') else {
            return Err(rusqlite::Error::InvalidQuery);
        };
        let table = table.trim();
        let actual: String = connection.query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
            [table],
            |row| row.get(0),
        )?;
        if normalize_ddl(&actual) != normalize_ddl(statement) {
            return Err(rusqlite::Error::InvalidQuery);
        }
    }
    Ok(())
}

fn migrate_v2_to_v3(connection: &mut Connection) -> rusqlite::Result<()> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS catalog_allocators(name TEXT PRIMARY KEY,next_id INTEGER NOT NULL CHECK(next_id > 0));
         INSERT OR IGNORE INTO catalog_allocators(name,next_id)
             SELECT 'version',COALESCE(MAX(id),0)+1 FROM file_versions
             ;
         UPDATE catalog_allocators
            SET next_id=MAX(next_id,(SELECT COALESCE(MAX(id),0)+1 FROM file_versions))
          WHERE name='version';
         CREATE UNIQUE INDEX IF NOT EXISTS file_versions_file_commit_unique
             ON file_versions(file_id,commit_id);
         ALTER TABLE catalog_meta RENAME TO catalog_meta_v2;
         CREATE TABLE catalog_meta(id INTEGER PRIMARY KEY CHECK(id=1),schema_version INTEGER NOT NULL CHECK(schema_version=3),coordinator_epoch INTEGER NOT NULL,allocators TEXT NOT NULL);
         INSERT INTO catalog_meta(id,schema_version,coordinator_epoch,allocators)
             SELECT id,3,coordinator_epoch,allocators FROM catalog_meta_v2;
         DROP TABLE catalog_meta_v2;",
    )?;
    tx.commit()
}

fn normalize_ddl(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
        .replace("create table if not exists ", "create table ")
}

impl SqliteCatalogStore {
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        Self::from_connection(Connection::open(path)?, true)
    }
    pub fn in_memory() -> rusqlite::Result<Self> {
        Self::from_connection(Connection::open_in_memory()?, false)
    }
    fn from_connection(mut connection: Connection, require_wal: bool) -> rusqlite::Result<Self> {
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        let journal_mode: String =
            connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        let synchronous: i64 = connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
        let foreign_keys: i64 =
            connection.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
        let journal_ok = journal_mode.eq_ignore_ascii_case("wal")
            || (!require_wal && journal_mode.eq_ignore_ascii_case("memory"));
        if !journal_ok || synchronous != 2 || foreign_keys != 1 {
            return Err(rusqlite::Error::InvalidQuery);
        }
        let has_catalog_meta: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='catalog_meta')",
            [],
            |row| row.get(0),
        )?;
        let has_any_table: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table')",
            [],
            |row| row.get(0),
        )?;
        if !has_catalog_meta && has_any_table {
            return Err(rusqlite::Error::InvalidQuery);
        }
        if !has_catalog_meta {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_batch(SCHEMA_V3)?;
            tx.commit()?;
        }
        let mut schema_version: u64 = connection.query_row(
            "SELECT schema_version FROM catalog_meta WHERE id = 1",
            [],
            |row| row.get(0),
        )?;
        if schema_version == 2 {
            migrate_v2_to_v3(&mut connection)?;
            schema_version = 3;
        }
        if schema_version != 3 {
            return Err(rusqlite::Error::InvalidQuery);
        }
        validate_schema_v3(&connection)?;
        Ok(Self {
            connection,
            committed_transactions: 0,
        })
    }
    fn with_immediate_transaction<T>(
        &mut self,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let value = f(&tx)?;
        tx.commit()?;
        self.committed_transactions = self.committed_transactions.saturating_add(1);
        Ok(value)
    }

    /// Installs the initial principal, collection, file, and empty head as one
    /// durable metadata transaction. Repeating an identical bootstrap is safe.
    pub fn bootstrap(
        &mut self,
        principal_id: u64,
        collection_id: u64,
        file_id: u64,
        collection_name: &str,
        file_name: &str,
    ) -> rusqlite::Result<()> {
        self.with_immediate_transaction(|tx| {
            tx.execute(
                "INSERT OR IGNORE INTO principal(id,kind,state,authz_epoch) VALUES (?1,'user','active',0)",
                [principal_id],
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO collections(id,owner_id,name) VALUES (?1,?2,?3)",
                params![collection_id, principal_id, collection_name],
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO files(id,collection_id,name) VALUES (?1,?2,?3)",
                params![file_id, collection_id, file_name],
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO file_head(file_id,version_id,generation) VALUES (?1,NULL,0)",
                [file_id],
            )?;
            let principal: (String, String, u64) = tx.query_row(
                "SELECT kind,state,authz_epoch FROM principal WHERE id=?1",
                [principal_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            let collection: (u64, String) = tx.query_row(
                "SELECT owner_id,name FROM collections WHERE id=?1",
                [collection_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let file: (u64, String) = tx.query_row(
                "SELECT collection_id,name FROM files WHERE id=?1",
                [file_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let head: (Option<u64>, u64) = tx.query_row(
                "SELECT version_id,generation FROM file_head WHERE file_id=?1",
                [file_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let valid_head = match head.0 {
                None => head.1 == 0,
                Some(_) => head.1 > 0,
            };
            if principal != ("user".into(), "active".into(), 0)
                || collection != (principal_id, collection_name.to_owned())
                || file != (collection_id, file_name.to_owned())
                || !valid_head
            {
                return Err(rusqlite::Error::InvalidQuery);
            }
            for (name, next_id) in [
                ("principal", principal_id.saturating_add(1)),
                ("collection", collection_id.saturating_add(1)),
                ("file", file_id.saturating_add(1)),
            ] {
                tx.execute(
                    "INSERT INTO catalog_allocators(name,next_id) VALUES (?1,?2)
                     ON CONFLICT(name) DO UPDATE SET next_id=MAX(next_id,excluded.next_id)",
                    params![name, next_id],
                )?;
            }
            Ok(())
        })
    }

    pub fn create_collection(
        &mut self,
        actor_id: u64,
        name: &str,
    ) -> rusqlite::Result<CollectionInfo> {
        self.with_immediate_transaction(|tx| {
            let authorized: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM principal WHERE id=?1 AND state='active')",
                [actor_id],
                |r| r.get(0),
            )?;
            if !authorized {
                return Err(rusqlite::Error::InvalidQuery);
            }
            if let Some(existing) = tx
                .query_row(
                    "SELECT id,owner_id,name FROM collections WHERE owner_id=?1 AND name=?2",
                    params![actor_id, name],
                    |r| {
                        Ok(CollectionInfo {
                            id: r.get(0)?,
                            owner_id: r.get(1)?,
                            name: r.get(2)?,
                        })
                    },
                )
                .optional()?
            {
                return Ok(existing);
            }
            let id = allocate_id(tx, "collection")?;
            tx.execute(
                "INSERT INTO collections(id,owner_id,name) VALUES (?1,?2,?3)",
                params![id, actor_id, name],
            )?;
            Ok(CollectionInfo {
                id,
                owner_id: actor_id,
                name: name.to_owned(),
            })
        })
    }

    pub fn create_file(
        &mut self,
        actor_id: u64,
        collection_id: u64,
        name: &str,
    ) -> rusqlite::Result<FileInfo> {
        self.with_immediate_transaction(|tx| {
            let owner: Option<u64> = tx
                .query_row(
                    "SELECT owner_id FROM collections WHERE id=?1",
                    [collection_id],
                    |r| r.get(0),
                )
                .optional()?;
            let actor_active: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM principal WHERE id=?1 AND state='active')",
                [actor_id],
                |r| r.get(0),
            )?;
            if owner != Some(actor_id) || !actor_active {
                return Err(rusqlite::Error::InvalidQuery);
            }
            if let Some(existing) = tx
                .query_row(
                    "SELECT id,collection_id,name FROM files WHERE collection_id=?1 AND name=?2",
                    params![collection_id, name],
                    |r| {
                        Ok(FileInfo {
                            id: r.get(0)?,
                            collection_id: r.get(1)?,
                            name: r.get(2)?,
                        })
                    },
                )
                .optional()?
            {
                return Ok(existing);
            }
            let id = allocate_id(tx, "file")?;
            tx.execute(
                "INSERT INTO files(id,collection_id,name) VALUES (?1,?2,?3)",
                params![id, collection_id, name],
            )?;
            tx.execute(
                "INSERT INTO file_head(file_id,version_id,generation) VALUES (?1,NULL,0)",
                [id],
            )?;
            Ok(FileInfo {
                id,
                collection_id,
                name: name.to_owned(),
            })
        })
    }

    pub fn terminal_operation_ids(&self) -> rusqlite::Result<Vec<u64>> {
        let mut statement = self.connection.prepare(
            "SELECT operation_id FROM publish_intents
             WHERE state IN ('published','aborted') ORDER BY operation_id",
        )?;
        let ids = statement
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<u64>, _>>();
        ids
    }
    pub fn coordinator_epoch(&self) -> rusqlite::Result<u64> {
        self.connection.query_row(
            "SELECT coordinator_epoch FROM catalog_meta WHERE id=1",
            [],
            |r| r.get(0),
        )
    }
    pub fn coordinator_epoch_typed(&self) -> rusqlite::Result<CoordinatorEpoch> {
        Ok(CoordinatorEpoch::new(self.coordinator_epoch()?))
    }
    pub fn principal_authz_epoch(&self, actor_id: u64) -> rusqlite::Result<Option<u64>> {
        self.connection
            .query_row(
                "SELECT authz_epoch FROM principal WHERE id=?1 AND state='active'",
                [actor_id],
                |r| r.get(0),
            )
            .optional()
    }
    pub fn read_head(&self, file_id: u64) -> rusqlite::Result<Option<HeadRecord>> {
        self.connection
            .query_row(
                "SELECT version_id,generation FROM file_head WHERE file_id=?1",
                [file_id],
                |r| {
                    Ok(HeadRecord {
                        file_id,
                        version_id: r.get(0)?,
                        generation: r.get(1)?,
                    })
                },
            )
            .optional()
    }
    pub fn read_version(
        &self,
        file_id: u64,
        version_id: u64,
    ) -> rusqlite::Result<Option<CatalogVersion>> {
        self.read_version_record(file_id, version_id, true)
    }

    /// Reads a candidate for the coordinator recovery path. Callers must not
    /// expose this result to users until the publish intent is terminal.
    pub fn read_candidate_version(
        &self,
        file_id: u64,
        version_id: u64,
    ) -> rusqlite::Result<Option<CatalogVersion>> {
        self.read_version_record(file_id, version_id, false)
    }

    fn read_version_record(
        &self,
        file_id: u64,
        version_id: u64,
        published_only: bool,
    ) -> rusqlite::Result<Option<CatalogVersion>> {
        self.connection
            .query_row(
                "SELECT id,generation,commit_id,parent_version_id,size,digest
                   FROM file_versions
                  WHERE file_id=?1 AND id=?2
                    AND (?3=0 OR EXISTS(
                        SELECT 1 FROM publish_intents
                         WHERE candidate_version_id=file_versions.id
                           AND state='published'
                    ))",
                params![file_id, version_id, published_only],
                |r| {
                    let commit_id: Vec<u8> = r.get(2)?;
                    let digest: Vec<u8> = r.get(5)?;
                    Ok(CatalogVersion {
                        id: r.get(0)?,
                        file_id,
                        generation: r.get(1)?,
                        commit_id: commit_id
                            .try_into()
                            .map_err(|_| rusqlite::Error::InvalidQuery)?,
                        parent_version_id: r.get(3)?,
                        size: r.get(4)?,
                        digest: digest
                            .try_into()
                            .map_err(|_| rusqlite::Error::InvalidQuery)?,
                    })
                },
            )
            .optional()
    }
    pub fn authorize_read_version(&self, actor_id: u64, version_id: u64) -> rusqlite::Result<bool> {
        self.connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM file_versions AS v
                JOIN files AS f ON f.id=v.file_id
                JOIN collections AS c ON c.id=f.collection_id
                JOIN principal AS p ON p.id=?1 AND p.state='active'
                WHERE v.id=?2 AND (c.owner_id=?1 OR EXISTS(
                    SELECT 1 FROM membership AS m
                    WHERE m.organization_id=c.owner_id AND m.member_id=?1
                      AND m.capability IN ('read','write','manage_members')
                ))
                AND EXISTS(
                    SELECT 1 FROM publish_intents AS i
                    WHERE i.candidate_version_id=v.id AND i.state='published'
                )
            )",
            params![actor_id, version_id],
            |r| r.get(0),
        )
    }
    pub fn read_authorization(
        &self,
        actor_id: u64,
        file_id: u64,
        version_id: u64,
    ) -> rusqlite::Result<ReadAuthorization> {
        self.connection.query_row(
            "SELECT CASE
                WHEN NOT EXISTS(SELECT 1 FROM file_versions WHERE id=?2 AND file_id=?3) THEN 'missing'
                WHEN NOT EXISTS(
                    SELECT 1 FROM file_versions AS v
                    JOIN files AS f ON f.id=v.file_id
                    JOIN collections AS c ON c.id=f.collection_id
                    JOIN principal AS p ON p.id=?1 AND p.state='active'
                    WHERE v.id=?2 AND v.file_id=?3 AND (c.owner_id=?1 OR EXISTS(
                        SELECT 1 FROM membership AS m
                        WHERE m.organization_id=c.owner_id AND m.member_id=?1
                          AND m.capability IN ('read','write','manage_members')
                    ))
                ) THEN 'unauthorized'
                WHEN NOT EXISTS(SELECT 1 FROM publish_intents WHERE candidate_version_id=?2 AND state='published') THEN 'not_published'
                ELSE 'authorized' END",
            params![actor_id, version_id, file_id],
            |r| match r.get::<_, String>(0)?.as_str() {
                "missing" => Ok(ReadAuthorization::Missing),
                "unauthorized" => Ok(ReadAuthorization::Unauthorized),
                "not_published" => Ok(ReadAuthorization::NotPublished),
                "authorized" => Ok(ReadAuthorization::Authorized),
                _ => Err(rusqlite::Error::InvalidQuery),
            },
        )
    }
    pub fn version_is_published(&self, version_id: u64) -> rusqlite::Result<bool> {
        self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM publish_intents WHERE candidate_version_id=?1 AND state='published')",
            [version_id],
            |r| r.get(0),
        )
    }
    pub fn operation_state(
        &self,
        actor_id: u64,
        operation_id: u64,
    ) -> rusqlite::Result<Option<CatalogOperationState>> {
        self.connection
            .query_row(
                "SELECT state FROM operation_results WHERE operation_id=?1 AND actor_id=?2",
                params![operation_id, actor_id],
                |r| {
                    Ok(match r.get::<_, String>(0)?.as_str() {
                        "succeeded" => CatalogOperationState::Succeeded,
                        "failed" => CatalogOperationState::Failed,
                        "in_progress" => CatalogOperationState::InProgress,
                        _ => return Err(rusqlite::Error::InvalidQuery),
                    })
                },
            )
            .optional()
    }
    pub fn acquire_reader_lease(
        &mut self,
        actor_id: u64,
        file_id: u64,
        version_id: u64,
        coordinator_epoch: u64,
    ) -> rusqlite::Result<u64> {
        self.with_immediate_transaction(|tx| {
            let authorized: bool = tx.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM file_versions AS v
                    JOIN files AS f ON f.id=v.file_id
                    JOIN collections AS c ON c.id=f.collection_id
                    JOIN principal AS p ON p.id=?1 AND p.state='active'
                    WHERE v.id=?3 AND v.file_id=?2 AND (c.owner_id=?1 OR EXISTS(
                        SELECT 1 FROM membership AS m
                        WHERE m.organization_id=c.owner_id AND m.member_id=?1
                          AND m.capability IN ('read','write','manage_members')
                    ))
                )",
                params![actor_id, file_id, version_id],
                |r| r.get(0),
            )?;
            if !authorized || !self_authorized_version(tx, version_id)? {
                return Err(rusqlite::Error::InvalidQuery);
            }
            let current_epoch: u64 = tx.query_row(
                "SELECT coordinator_epoch FROM catalog_meta WHERE id=1",
                [],
                |r| r.get(0),
            )?;
            if current_epoch != coordinator_epoch {
                return Err(rusqlite::Error::InvalidQuery);
            }
            tx.execute(
                "INSERT INTO reader_leases(file_id,version_id,actor_id,coordinator_epoch) VALUES (?1,?2,?3,?4)",
                params![file_id, version_id, actor_id, coordinator_epoch],
            )?;
            Ok(tx.last_insert_rowid() as u64)
        })
    }
    pub fn release_reader_lease(&mut self, lease_id: u64) -> rusqlite::Result<()> {
        self.with_immediate_transaction(|tx| {
            tx.execute("DELETE FROM reader_leases WHERE lease_id=?1", [lease_id])?;
            Ok(())
        })
    }
    pub fn reader_lease_active(
        &self,
        lease_id: u64,
        actor_id: u64,
        file_id: u64,
        version_id: u64,
        coordinator_epoch: u64,
    ) -> rusqlite::Result<bool> {
        self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM reader_leases WHERE lease_id=?1 AND actor_id=?2 AND file_id=?3 AND version_id=?4 AND coordinator_epoch=?5 AND coordinator_epoch=(SELECT coordinator_epoch FROM catalog_meta WHERE id=1))",
            params![lease_id, actor_id, file_id, version_id, coordinator_epoch],
            |r| r.get(0),
        )
    }
    pub fn claim_coordinator_epoch(
        &mut self,
        expected: CoordinatorEpoch,
        next: CoordinatorEpoch,
    ) -> rusqlite::Result<EpochClaim> {
        self.with_immediate_transaction(|tx| {
            let changed = tx.execute(
                "UPDATE catalog_meta
                    SET coordinator_epoch = ?1
                  WHERE id = 1 AND coordinator_epoch = ?2 AND ?1 > ?2",
                params![next.get(), expected.get()],
            )?;
            if changed == 1 {
                Ok(EpochClaim::Claimed(next))
            } else {
                let current: u64 = tx.query_row(
                    "SELECT coordinator_epoch FROM catalog_meta WHERE id = 1",
                    [],
                    |r| r.get(0),
                )?;
                Ok(EpochClaim::Stale {
                    current: CoordinatorEpoch::new(current),
                })
            }
        })
    }
    pub fn cas_owner_epoch(&mut self, expected: u64, next: u64) -> rusqlite::Result<bool> {
        self.with_immediate_transaction(|tx| {
            Ok(tx.execute(
                "UPDATE catalog_meta SET coordinator_epoch=?1 WHERE id=1 AND coordinator_epoch=?2 AND ?1 > ?2",
                params![next, expected],
            )? == 1)
        })
    }
    pub fn recovery_work(&self) -> rusqlite::Result<Vec<RecoveryWork>> {
        let mut statement = self
            .connection
            .prepare("SELECT operation_id FROM publish_intents ORDER BY operation_id")?;
        let ids = statement.query_map([], |row| row.get::<_, u64>(0))?;
        let mut work = Vec::new();
        for id in ids {
            let intent =
                load_intent(&self.connection, id?)?.ok_or(rusqlite::Error::InvalidQuery)?;
            let classified = classify_intent(intent);
            work.push(match &classified {
                RecoveryIntent::Nonterminal(intent) => RecoveryWork::Resume(intent.clone()),
                RecoveryIntent::PublishedTombstone(intent) => RecoveryWork::TombstoneDagBinding {
                    intent: intent.clone(),
                    terminal: TombstoneTerminal::Published,
                },
                RecoveryIntent::AbortedTombstone(intent) => RecoveryWork::TombstoneDagBinding {
                    intent: intent.clone(),
                    terminal: TombstoneTerminal::Aborted,
                },
            });
        }
        Ok(work)
    }
    pub fn claim_intent(
        &mut self,
        operation_id: u64,
        epoch: CoordinatorEpoch,
        nonce: u64,
    ) -> rusqlite::Result<ClaimIntentOutcome> {
        self.with_immediate_transaction(|tx| {
            let Some(intent) = load_intent(tx, operation_id)? else {
                return Ok(ClaimIntentOutcome::Missing);
            };
            let current: u64 = tx.query_row(
                "SELECT coordinator_epoch FROM catalog_meta WHERE id = 1",
                [],
                |r| r.get(0),
            )?;
            let classified = classify_intent(intent.clone());
            if matches!(
                classified,
                RecoveryIntent::PublishedTombstone(_) | RecoveryIntent::AbortedTombstone(_)
            ) {
                return Ok(ClaimIntentOutcome::Terminal(classified));
            }
            if current != epoch.get() {
                return Ok(ClaimIntentOutcome::Fenced {
                    current: CoordinatorEpoch::new(current),
                });
            }
            if intent.owner_epoch > CoordinatorEpoch::new(current).get() {
                return Ok(ClaimIntentOutcome::FutureOwner {
                    owner_epoch: CoordinatorEpoch::new(intent.owner_epoch),
                    current: CoordinatorEpoch::new(current),
                });
            }
            if intent.owner_epoch == epoch.get() && intent.owner_nonce == nonce {
                return Ok(ClaimIntentOutcome::Claimed(classified));
            }
            if intent.owner_epoch == epoch.get() {
                return Ok(ClaimIntentOutcome::AlreadyClaimed {
                    owner_epoch: epoch,
                    owner_nonce: intent.owner_nonce,
                });
            }
            let changed = tx.execute(
                "UPDATE publish_intents
                    SET owner_epoch = ?1, owner_nonce = ?2, pinned = 1
                  WHERE operation_id = ?3
                    AND state IN ('preparing','commit_durable')
                    AND owner_epoch < ?1
                    AND (SELECT coordinator_epoch FROM catalog_meta WHERE id = 1) = ?1",
                params![epoch.get(), nonce, operation_id],
            )?;
            if changed != 1 {
                return Err(rusqlite::Error::InvalidQuery);
            }
            Ok(ClaimIntentOutcome::Claimed(RecoveryIntent::Nonterminal(
                load_intent(tx, operation_id)?.ok_or(rusqlite::Error::InvalidQuery)?,
            )))
        })
    }
    pub fn persist(&mut self, b: &CatalogBatch) -> rusqlite::Result<()> {
        self.persist_with_cut(b, CrashPoint::None)
    }
    pub fn persist_with_cut(&mut self, b: &CatalogBatch, cut: CrashPoint) -> rusqlite::Result<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_v1(&tx, b, cut)?;
        tx.commit()?;
        self.committed_transactions = self.committed_transactions.saturating_add(1);
        Ok(())
    }
    pub fn t1_prepare(&mut self, i: &IntentRecord) -> rusqlite::Result<()> {
        self.with_immediate_transaction(|tx| {
            let matches: bool = tx.query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM operation_results
                  WHERE operation_id=?1 AND actor_id=?2 AND kind='publish'
                    AND fingerprint=?3 AND state NOT IN ('succeeded','failed')
                ) AND EXISTS(
                   SELECT 1 FROM principal
                  WHERE id=?2 AND state='active' AND authz_epoch=?4
                )",
                params![i.operation_id, i.actor_id, i.request_fingerprint.as_slice(), i.authz_epoch],
                |r| r.get(0),
            )?;
            if !matches {
                return Err(rusqlite::Error::InvalidQuery);
            }
            tx.execute("INSERT INTO publish_intents(operation_id,actor_id,file_id,owner_epoch,owner_nonce,expected_head_version_id,expected_head_generation,state,pinned,request_fingerprint,authz_epoch) VALUES (?1,?2,?3,?4,?5,?6,?7,'preparing',?8,?9,?10)",params![i.operation_id,i.actor_id,i.file_id,i.owner_epoch,i.owner_nonce,i.expected_head_version_id,i.expected_head_generation,i.pinned,i.request_fingerprint.as_slice(),i.authz_epoch])?;
            Ok(())
        })
    }
    pub fn prepare_publish(
        &mut self,
        o: &OperationRecord,
        i: &IntentRecord,
    ) -> rusqlite::Result<bool> {
        if o.operation_id != i.operation_id
            || o.actor_id != i.actor_id
            || o.request_fingerprint != i.request_fingerprint
        {
            return Err(rusqlite::Error::InvalidQuery);
        }
        self.with_immediate_transaction(|tx| {
            let existing: bool = tx.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM operation_results
                    WHERE operation_id=?1
                       OR (actor_id=?2 AND kind=?3 AND fingerprint=?4)
                )",
                params![o.operation_id, o.actor_id, o.kind, o.request_fingerprint.as_slice()],
                |r| r.get(0),
            )?;
            if existing {
                return Ok(false);
            }
            let authorized: bool = tx.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM catalog_meta AS cm
                    JOIN principal AS p ON p.id=?2 AND p.state='active' AND p.authz_epoch=?3
                    JOIN files AS f ON f.id=?4
                    JOIN collections AS c ON c.id=f.collection_id
                    WHERE cm.id=1 AND cm.coordinator_epoch=?1
                      AND (c.owner_id=p.id OR EXISTS(
                          SELECT 1 FROM membership AS m
                          WHERE m.organization_id=c.owner_id AND m.member_id=p.id
                            AND m.capability IN ('write','manage_members')
                      ))
                      AND ((?5 IS NULL AND ?6=0) OR EXISTS(
                          SELECT 1 FROM file_head AS h
                          WHERE h.file_id=?4 AND h.version_id IS ?5 AND h.generation=?6
                      ))
                )",
                params![i.owner_epoch, i.actor_id, i.authz_epoch, i.file_id, i.expected_head_version_id, i.expected_head_generation],
                |r| r.get(0),
            )?;
            if !authorized {
                return Err(rusqlite::Error::InvalidQuery);
            }
            tx.execute(
                "INSERT INTO operation_results(operation_id,actor_id,kind,fingerprint,state,result,error) VALUES (?1,?2,?3,?4,'in_progress',NULL,NULL)",
                params![o.operation_id, o.actor_id, o.kind, o.request_fingerprint.as_slice()],
            )?;
            tx.execute(
                "INSERT INTO publish_intents(operation_id,actor_id,file_id,owner_epoch,owner_nonce,expected_head_version_id,expected_head_generation,state,pinned,request_fingerprint,authz_epoch) VALUES (?1,?2,?3,?4,?5,?6,?7,'preparing',?8,?9,?10)",
                params![i.operation_id,i.actor_id,i.file_id,i.owner_epoch,i.owner_nonce,i.expected_head_version_id,i.expected_head_generation,i.pinned,i.request_fingerprint.as_slice(),i.authz_epoch],
            )?;
            Ok(true)
        })
    }
    pub fn t2_record_candidate(
        &mut self,
        op: u64,
        epoch: u64,
        nonce: u64,
        version: u64,
    ) -> rusqlite::Result<bool> {
        Ok(matches!(
            self.t2_record_candidate_typed(op, CoordinatorEpoch::new(epoch), nonce, version)?,
            T2Outcome::Applied
        ))
    }
    pub fn t2_record_candidate_typed(
        &mut self,
        op: u64,
        epoch: CoordinatorEpoch,
        nonce: u64,
        version: u64,
    ) -> rusqlite::Result<T2Outcome> {
        self.with_immediate_transaction(|tx| {
            let current: u64 = tx.query_row(
                "SELECT coordinator_epoch FROM catalog_meta WHERE id=1",
                [],
                |r| r.get(0),
            )?;
            let Some(intent) = load_intent(tx, op)? else {
                return Ok(T2Outcome::MissingIntent);
            };
            if current != epoch.get()
                || intent.owner_epoch != epoch.get()
                || intent.owner_nonce != nonce
            {
                return Ok(T2Outcome::Fenced);
            }
            if intent.state != "preparing" {
                return Ok(T2Outcome::NotPreparing);
            }
            tx.execute(
                "UPDATE publish_intents SET candidate_version_id=?1,state='commit_durable'
                  WHERE operation_id=?2 AND owner_epoch=?3 AND owner_nonce=?4
                    AND state='preparing'",
                params![version, op, epoch.get(), nonce],
            )?;
            Ok(T2Outcome::Applied)
        })
    }
    pub fn t2_record_version(
        &mut self,
        op: u64,
        epoch: CoordinatorEpoch,
        nonce: u64,
        version: &CatalogVersion,
    ) -> rusqlite::Result<T2Outcome> {
        self.with_immediate_transaction(|tx| {
            let current: u64 = tx.query_row(
                "SELECT coordinator_epoch FROM catalog_meta WHERE id=1",
                [],
                |r| r.get(0),
            )?;
            let Some(intent) = load_intent(tx, op)? else {
                return Ok(T2Outcome::MissingIntent);
            };
            if current != epoch.get()
                || intent.owner_epoch != epoch.get()
                || intent.owner_nonce != nonce
            {
                return Ok(T2Outcome::Fenced);
            }
            if intent.file_id != version.file_id {
                return Ok(T2Outcome::NotPreparing);
            }
            let version_id = if intent.state == "commit_durable" {
                let candidate = intent
                    .candidate_version_id
                    .ok_or(rusqlite::Error::InvalidQuery)?;
                if version.id != 0 && version.id != candidate {
                    return Ok(T2Outcome::NotPreparing);
                }
                candidate
            } else if intent.state == "preparing" && version.id == 0 {
                let next_id: u64 = tx.query_row(
                    "SELECT next_id FROM catalog_allocators WHERE name='version'",
                    [],
                    |r| r.get(0),
                )?;
                tx.execute(
                    "UPDATE catalog_allocators SET next_id=?1 WHERE name='version'",
                    [next_id.checked_add(1).ok_or(rusqlite::Error::InvalidQuery)?],
                )?;
                next_id
            } else if intent.state == "preparing" {
                version.id
            } else {
                return Ok(T2Outcome::NotPreparing);
            };
            if intent.state == "commit_durable" {
                let matches: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM file_versions WHERE id=?1 AND file_id=?2 AND generation=?3 AND commit_id=?4 AND parent_version_id IS ?5 AND size=?6 AND digest=?7)",
                    params![version_id,version.file_id,version.generation,version.commit_id.as_slice(),version.parent_version_id,version.size,version.digest.as_slice()],
                    |r| r.get(0),
                )?;
                return Ok(if matches { T2Outcome::Applied } else { T2Outcome::NotPreparing });
            }
            tx.execute(
                "INSERT OR IGNORE INTO file_versions(id,file_id,generation,commit_id,parent_version_id,size,digest) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![version_id,version.file_id,version.generation,version.commit_id.as_slice(),version.parent_version_id,version.size,version.digest.as_slice()],
            )?;
            let matches: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM file_versions WHERE id=?1 AND file_id=?2 AND generation=?3 AND commit_id=?4 AND parent_version_id IS ?5 AND size=?6 AND digest=?7)",
                params![version_id,version.file_id,version.generation,version.commit_id.as_slice(),version.parent_version_id,version.size,version.digest.as_slice()],
                |r| r.get(0),
            )?;
            if !matches {
                return Err(rusqlite::Error::InvalidQuery);
            }
            advance_version_allocator(tx, version_id)?;
            tx.execute(
                "UPDATE publish_intents SET candidate_version_id=?1,state='commit_durable' WHERE operation_id=?2 AND owner_epoch=?3 AND owner_nonce=?4 AND state='preparing'",
                params![version_id,op,epoch.get(),nonce],
            )?;
            Ok(T2Outcome::Applied)
        })
    }
    pub fn candidate_version_id(&self, operation_id: u64) -> rusqlite::Result<Option<u64>> {
        self.connection
            .query_row(
                "SELECT candidate_version_id FROM publish_intents WHERE operation_id=?1",
                [operation_id],
                |r| r.get(0),
            )
            .optional()
    }
    pub fn t3_publish(&mut self, op: u64, epoch: u64, nonce: u64) -> rusqlite::Result<bool> {
        Ok(matches!(
            self.t3_publish_typed(op, CoordinatorEpoch::new(epoch), nonce)?,
            T3Outcome::Published
        ))
    }
    pub fn t3_publish_typed(
        &mut self,
        op: u64,
        epoch: CoordinatorEpoch,
        nonce: u64,
    ) -> rusqlite::Result<T3Outcome> {
        self.with_immediate_transaction(|tx| t3_publish_in_transaction(tx, epoch, op, nonce))
    }
    pub fn abort(
        &mut self,
        op: u64,
        epoch: u64,
        nonce: u64,
        reason: &str,
    ) -> rusqlite::Result<bool> {
        self.with_immediate_transaction(|tx|{let n=tx.execute("UPDATE publish_intents SET state='aborted',abort_reason=?1 WHERE operation_id=?2 AND owner_epoch=?3 AND owner_nonce=?4 AND state NOT IN ('published','aborted') AND (SELECT coordinator_epoch FROM catalog_meta WHERE id=1)=?3",params![reason,op,epoch,nonce])?;if n==1{tx.execute("UPDATE operation_results SET state='failed',error=?1 WHERE operation_id=?2",params![reason,op])?;}Ok(n==1)})
    }
    pub fn record_operation(&mut self, o: &OperationRecord) -> rusqlite::Result<bool> {
        self.with_immediate_transaction(|tx| {
            let existing: Option<(u64, String, String, Vec<u8>)> = tx
                .query_row(
                    "SELECT actor_id, kind, state, fingerprint
                       FROM operation_results
                      WHERE operation_id = ?1
                         OR (actor_id = ?2 AND kind = ?3 AND fingerprint = ?4)",
                    params![o.operation_id, o.actor_id, o.kind, o.request_fingerprint.as_slice()],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .optional()?;
            if existing.is_some() {
                return Ok(false);
            }
            Ok(tx.execute(
                "INSERT INTO operation_results(operation_id,actor_id,kind,fingerprint,state,result,error)
                 VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![
                    o.operation_id,
                    o.actor_id,
                    o.kind,
                    o.request_fingerprint.as_slice(),
                    o.state,
                    o.result,
                    o.error
                ],
            )? == 1)
        })
    }
    pub fn counts(&self) -> rusqlite::Result<CatalogCounts> {
        Ok(CatalogCounts {
            principals: count_v1(&self.connection, "principal")?,
            memberships: count_v1(&self.connection, "membership")?,
            collections: count_v1(&self.connection, "collections")?,
            files: count_v1(&self.connection, "files")?,
            heads: count_v1(&self.connection, "file_head")?,
            versions: count_v1(&self.connection, "file_versions")?,
            intents: count_v1(&self.connection, "publish_intents")?,
            operations: count_v1(&self.connection, "operation_results")?,
        })
    }
    pub fn durability_pragmas(&self) -> rusqlite::Result<(String, String)> {
        Ok((
            self.connection
                .query_row("PRAGMA journal_mode", [], |r| r.get(0))?,
            self.connection
                .query_row::<i64, _, _>("PRAGMA synchronous", [], |r| r.get(0))?
                .to_string(),
        ))
    }
    pub fn durability_metrics(&self) -> CatalogDurabilityMetrics {
        CatalogDurabilityMetrics {
            committed_transactions: self.committed_transactions,
        }
    }
    pub fn checkpoint_wal(&self, mode: WalCheckpointMode) -> rusqlite::Result<(i64, i64, i64)> {
        self.connection.query_row(
            &format!("PRAGMA wal_checkpoint({})", mode.as_sql()),
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
    }
    pub fn operation(&self, op: u64) -> rusqlite::Result<Option<OperationRecord>> {
        self.connection.query_row("SELECT actor_id,kind,fingerprint,state,result,error FROM operation_results WHERE operation_id=?1",[op],|r|{let f:Vec<u8>=r.get(2)?;Ok(OperationRecord{operation_id:op,actor_id:r.get(0)?,kind:r.get(1)?,request_fingerprint:f.try_into().map_err(|_|rusqlite::Error::InvalidQuery)?,state:r.get(3)?,result:r.get(4)?,error:r.get(5)?})}).optional()
    }
}

fn t3_publish_in_transaction(
    tx: &rusqlite::Transaction<'_>,
    epoch: CoordinatorEpoch,
    op: u64,
    nonce: u64,
) -> rusqlite::Result<T3Outcome> {
    let Some(intent) = load_intent(tx, op)? else {
        return Ok(T3Outcome::MissingIntent);
    };
    let current: u64 = tx.query_row(
        "SELECT coordinator_epoch FROM catalog_meta WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    if current != epoch.get() || intent.owner_epoch != epoch.get() || intent.owner_nonce != nonce {
        return Ok(T3Outcome::Fenced);
    }
    if intent.state == "published" {
        return Ok(T3Outcome::AlreadyPublished);
    }
    if intent.state != "commit_durable" {
        return Ok(T3Outcome::NotCommitDurable);
    }
    let operation_ready: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM operation_results
              WHERE operation_id=?1 AND actor_id=?2 AND kind='publish'
                AND fingerprint=?3 AND state NOT IN ('succeeded','failed')
        )",
        params![op, intent.actor_id, intent.request_fingerprint.as_slice()],
        |r| r.get(0),
    )?;
    if !operation_ready {
        return Ok(T3Outcome::MissingOperation);
    }
    let principal_active: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM principal
              WHERE id=?1 AND state='active' AND authz_epoch=?2
        )",
        params![intent.actor_id, intent.authz_epoch],
        |r| r.get(0),
    )?;
    if !principal_active {
        return Ok(T3Outcome::AuthorizationDenied);
    }
    let authorized: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM collections AS c
             JOIN files AS f ON f.collection_id=c.id
            WHERE f.id=?1 AND c.owner_id=?2
           UNION ALL
           SELECT 1 FROM collections AS c
             JOIN files AS f ON f.collection_id=c.id
             JOIN membership AS m ON m.organization_id=c.owner_id
                                  AND m.member_id=?2
            WHERE f.id=?1 AND m.capability IN ('write','manage_members')
        )",
        params![intent.file_id, intent.actor_id],
        |r| r.get(0),
    )?;
    if !authorized {
        return Ok(T3Outcome::AuthorizationDenied);
    }
    let Some(candidate) = intent.candidate_version_id else {
        return Ok(T3Outcome::MissingCandidate);
    };
    let next_generation = intent
        .expected_head_generation
        .checked_add(1)
        .ok_or(rusqlite::Error::InvalidQuery)?;
    let valid_version: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM file_versions
              WHERE id=?1 AND file_id=?2 AND generation=?3
                AND parent_version_id IS ?4
        )",
        params![
            candidate,
            intent.file_id,
            next_generation,
            intent.expected_head_version_id
        ],
        |r| r.get(0),
    )?;
    if !valid_version {
        return Ok(T3Outcome::VersionConflict);
    }
    let head_matches: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM file_head
              WHERE file_id=?1 AND generation=?2 AND version_id IS ?3
        )",
        params![
            intent.file_id,
            intent.expected_head_generation,
            intent.expected_head_version_id
        ],
        |r| r.get(0),
    )?;
    if !head_matches {
        return Ok(T3Outcome::HeadConflict);
    }
    tx.execute(
        "UPDATE file_head SET version_id=?1,generation=?2
          WHERE file_id=?3 AND generation=?4 AND version_id IS ?5",
        params![
            candidate,
            next_generation,
            intent.file_id,
            intent.expected_head_generation,
            intent.expected_head_version_id
        ],
    )?;
    tx.execute(
        "UPDATE publish_intents SET state='published',pinned=0
          WHERE operation_id=?1 AND state='commit_durable'",
        [op],
    )?;
    tx.execute(
        "UPDATE operation_results SET state='succeeded',result=?1,error=NULL
          WHERE operation_id=?2 AND actor_id=?3 AND kind='publish'
            AND fingerprint=?4 AND state NOT IN ('succeeded','failed')",
        params![
            format!("version:{candidate}"),
            op,
            intent.actor_id,
            intent.request_fingerprint.as_slice()
        ],
    )?;
    Ok(T3Outcome::Published)
}

fn count_v1(c: &Connection, t: &str) -> rusqlite::Result<u64> {
    c.query_row(&format!("SELECT COUNT(*) FROM {t}"), [], |r| r.get(0))
}

fn advance_version_allocator(
    tx: &rusqlite::Transaction<'_>,
    version_id: u64,
) -> rusqlite::Result<()> {
    let next_id = version_id
        .checked_add(1)
        .ok_or(rusqlite::Error::InvalidQuery)?;
    tx.execute(
        "UPDATE catalog_allocators
            SET next_id=CASE WHEN next_id < ?1 THEN ?1 ELSE next_id END
          WHERE name='version'",
        [next_id],
    )?;
    Ok(())
}

fn self_authorized_version(tx: &Transaction<'_>, version_id: u64) -> rusqlite::Result<bool> {
    tx.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM publish_intents WHERE candidate_version_id=?1 AND state='published'
        )",
        [version_id],
        |r| r.get(0),
    )
}

fn load_intent(c: &Connection, operation_id: u64) -> rusqlite::Result<Option<IntentRecord>> {
    c.query_row(
        "SELECT actor_id,file_id,owner_epoch,owner_nonce,expected_head_version_id,
                expected_head_generation,candidate_version_id,state,abort_reason,pinned,
                request_fingerprint,authz_epoch
           FROM publish_intents WHERE operation_id=?1",
        [operation_id],
        |r| {
            let fingerprint: Vec<u8> = r.get(10)?;
            Ok(IntentRecord {
                operation_id,
                actor_id: r.get(0)?,
                file_id: r.get(1)?,
                owner_epoch: r.get(2)?,
                owner_nonce: r.get(3)?,
                expected_head_version_id: r.get(4)?,
                expected_head_generation: r.get(5)?,
                candidate_version_id: r.get(6)?,
                state: r.get(7)?,
                abort_reason: r.get(8)?,
                pinned: r.get(9)?,
                request_fingerprint: fingerprint
                    .try_into()
                    .map_err(|_| rusqlite::Error::InvalidQuery)?,
                authz_epoch: r.get(11)?,
            })
        },
    )
    .optional()
}

fn classify_intent(intent: IntentRecord) -> RecoveryIntent {
    match intent.state.as_str() {
        "published" => RecoveryIntent::PublishedTombstone(intent),
        "aborted" => RecoveryIntent::AbortedTombstone(intent),
        _ => RecoveryIntent::Nonterminal(intent),
    }
}

fn cut_v1(a: CrashPoint, e: CrashPoint) -> rusqlite::Result<()> {
    if a == e {
        Err(rusqlite::Error::InvalidParameterName(format!("cut {e:?}")))
    } else {
        Ok(())
    }
}
fn insert_v1(
    tx: &rusqlite::Transaction<'_>,
    b: &CatalogBatch,
    cut: CrashPoint,
) -> rusqlite::Result<()> {
    for p in &b.principals {
        tx.execute(
            "INSERT INTO principal VALUES (?1,?2,?3,?4)",
            params![p.id, p.kind, p.state, p.authz_epoch],
        )?;
    }
    for m in &b.memberships {
        tx.execute(
            "INSERT INTO membership VALUES (?1,?2,?3)",
            params![m.organization_id, m.member_id, m.capability],
        )?;
    }
    for c in &b.collections {
        tx.execute(
            "INSERT INTO collections VALUES (?1,?2,?3)",
            params![c.id, c.owner_id, c.name],
        )?;
    }
    cut_v1(cut, CrashPoint::AfterCollections)?;
    for f in &b.files {
        tx.execute(
            "INSERT INTO files VALUES (?1,?2,?3)",
            params![f.id, f.collection_id, f.name],
        )?;
    }
    cut_v1(cut, CrashPoint::AfterFiles)?;
    let mut pending = b.versions.clone();
    while !pending.is_empty() {
        let before = pending.len();
        let mut deferred = Vec::new();
        for v in pending {
            if v.parent_version_id
                .is_some_and(|parent| !b.versions.iter().any(|candidate| candidate.id == parent))
            {
                return Err(rusqlite::Error::InvalidQuery);
            }
            let parent_ready = v.parent_version_id.is_none()
                || tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM file_versions WHERE id = ?1)",
                    [v.parent_version_id.unwrap_or_default()],
                    |r| r.get(0),
                )?;
            if parent_ready {
                tx.execute(
                    "INSERT INTO file_versions VALUES (?1,?2,?3,?4,?5,?6,?7)",
                    params![
                        v.id,
                        v.file_id,
                        v.generation,
                        v.commit_id.as_slice(),
                        v.parent_version_id,
                        v.size,
                        v.digest.as_slice()
                    ],
                )?;
                advance_version_allocator(tx, v.id)?;
            } else {
                deferred.push(v);
            }
        }
        if deferred.len() == before {
            return Err(rusqlite::Error::InvalidQuery);
        }
        pending = deferred;
    }
    cut_v1(cut, CrashPoint::AfterVersions)?;
    for h in &b.heads {
        tx.execute(
            "INSERT INTO file_head VALUES (?1,?2,?3)",
            params![h.file_id, h.version_id, h.generation],
        )?;
    }
    for i in &b.intents {
        tx.execute(
            "INSERT INTO publish_intents VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![
                i.operation_id,
                i.actor_id,
                i.file_id,
                i.owner_epoch,
                i.owner_nonce,
                i.expected_head_version_id,
                i.expected_head_generation,
                i.candidate_version_id,
                i.state,
                i.abort_reason,
                i.pinned,
                i.request_fingerprint.as_slice(),
                i.authz_epoch
            ],
        )?;
    }
    cut_v1(cut, CrashPoint::AfterIntents)?;
    for o in &b.operations {
        tx.execute(
            "INSERT INTO operation_results VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![
                o.operation_id,
                o.actor_id,
                o.kind,
                o.request_fingerprint.as_slice(),
                o.state,
                o.result,
                o.error
            ],
        )?;
    }
    cut_v1(cut, CrashPoint::AfterResults)
}

fn allocate_id(tx: &rusqlite::Transaction<'_>, name: &str) -> rusqlite::Result<u64> {
    let table = match name {
        "principal" => "principal",
        "collection" => "collections",
        "file" => "files",
        "version" => "file_versions",
        _ => return Err(rusqlite::Error::InvalidQuery),
    };
    tx.execute(
        &format!(
            "INSERT OR IGNORE INTO catalog_allocators(name,next_id)
             SELECT ?1,COALESCE(MAX(id),0)+1 FROM {table}"
        ),
        [name],
    )?;
    let id: u64 = tx.query_row(
        "SELECT next_id FROM catalog_allocators WHERE name=?1",
        [name],
        |r| r.get(0),
    )?;
    let next = id.checked_add(1).ok_or(rusqlite::Error::InvalidQuery)?;
    tx.execute(
        "UPDATE catalog_allocators SET next_id=?1 WHERE name=?2",
        params![next, name],
    )?;
    Ok(id)
}

const SCHEMA_V3: &str = r#"
CREATE TABLE IF NOT EXISTS catalog_meta(id INTEGER PRIMARY KEY CHECK(id=1),schema_version INTEGER NOT NULL CHECK(schema_version=3),coordinator_epoch INTEGER NOT NULL,allocators TEXT NOT NULL); INSERT OR IGNORE INTO catalog_meta VALUES(1,3,0,'{}');
CREATE TABLE IF NOT EXISTS catalog_allocators(name TEXT PRIMARY KEY,next_id INTEGER NOT NULL CHECK(next_id > 0));
INSERT OR IGNORE INTO catalog_allocators(name,next_id) VALUES('version',1);
CREATE TABLE IF NOT EXISTS principal(id INTEGER PRIMARY KEY,kind TEXT NOT NULL,state TEXT NOT NULL,authz_epoch INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS membership(organization_id INTEGER NOT NULL REFERENCES principal(id),member_id INTEGER NOT NULL REFERENCES principal(id),capability TEXT NOT NULL,PRIMARY KEY(organization_id,member_id,capability));
CREATE TABLE IF NOT EXISTS collections(id INTEGER PRIMARY KEY,owner_id INTEGER NOT NULL REFERENCES principal(id),name TEXT NOT NULL,UNIQUE(owner_id,name));
CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY,collection_id INTEGER NOT NULL REFERENCES collections(id),name TEXT NOT NULL,UNIQUE(collection_id,name));
CREATE TABLE IF NOT EXISTS file_versions(id INTEGER PRIMARY KEY,file_id INTEGER NOT NULL REFERENCES files(id),generation INTEGER NOT NULL,commit_id BLOB NOT NULL CHECK(typeof(commit_id)='blob' AND length(commit_id)=32),parent_version_id INTEGER,size INTEGER NOT NULL,digest BLOB NOT NULL CHECK(typeof(digest)='blob' AND length(digest)=32),UNIQUE(file_id,id),FOREIGN KEY(file_id,parent_version_id) REFERENCES file_versions(file_id,id));
CREATE UNIQUE INDEX IF NOT EXISTS file_versions_file_commit_unique ON file_versions(file_id,commit_id);
CREATE TABLE IF NOT EXISTS file_head(file_id INTEGER PRIMARY KEY REFERENCES files(id),version_id INTEGER,generation INTEGER NOT NULL,FOREIGN KEY(file_id,version_id) REFERENCES file_versions(file_id,id));
CREATE TABLE IF NOT EXISTS publish_intents(operation_id INTEGER PRIMARY KEY,actor_id INTEGER NOT NULL REFERENCES principal(id),file_id INTEGER NOT NULL REFERENCES files(id),owner_epoch INTEGER NOT NULL,owner_nonce INTEGER NOT NULL,expected_head_version_id INTEGER,expected_head_generation INTEGER NOT NULL,candidate_version_id INTEGER,state TEXT NOT NULL CHECK(state IN ('preparing','commit_durable','published','aborted')),abort_reason TEXT,pinned INTEGER NOT NULL DEFAULT 0 CHECK(pinned IN(0,1)),request_fingerprint BLOB NOT NULL CHECK(typeof(request_fingerprint)='blob' AND length(request_fingerprint)=32),authz_epoch INTEGER NOT NULL,FOREIGN KEY(file_id,expected_head_version_id) REFERENCES file_versions(file_id,id),FOREIGN KEY(file_id,candidate_version_id) REFERENCES file_versions(file_id,id));
CREATE TABLE IF NOT EXISTS operation_results(operation_id INTEGER PRIMARY KEY,actor_id INTEGER NOT NULL REFERENCES principal(id),kind TEXT NOT NULL,fingerprint BLOB NOT NULL CHECK(typeof(fingerprint)='blob' AND length(fingerprint)=32),state TEXT NOT NULL,result TEXT,error TEXT,UNIQUE(actor_id,kind,fingerprint));
CREATE TABLE IF NOT EXISTS reader_leases(lease_id INTEGER PRIMARY KEY,file_id INTEGER NOT NULL REFERENCES files(id),version_id INTEGER NOT NULL,actor_id INTEGER NOT NULL REFERENCES principal(id),coordinator_epoch INTEGER NOT NULL,FOREIGN KEY(file_id,version_id) REFERENCES file_versions(file_id,id));
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_migration_rebuilds_meta_and_seeds_the_version_allocator() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE catalog_meta(id INTEGER PRIMARY KEY CHECK(id=1),schema_version INTEGER NOT NULL CHECK(schema_version=2),coordinator_epoch INTEGER NOT NULL,allocators TEXT NOT NULL);
                 INSERT INTO catalog_meta VALUES(1,2,9,'{}');
                 CREATE TABLE file_versions(id INTEGER PRIMARY KEY,file_id INTEGER NOT NULL,generation INTEGER NOT NULL,commit_id BLOB NOT NULL,parent_version_id INTEGER,size INTEGER NOT NULL,digest BLOB NOT NULL);
                 CREATE TABLE catalog_allocators(name TEXT PRIMARY KEY,next_id INTEGER NOT NULL);
                 INSERT INTO catalog_allocators VALUES('version',1);
                 INSERT INTO file_versions VALUES(41,7,1,zeroblob(32),NULL,0,zeroblob(32));",
            )
            .unwrap();
        migrate_v2_to_v3(&mut connection).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT schema_version FROM catalog_meta WHERE id=1",
                    [],
                    |row| row.get::<_, u64>(0),
                )
                .unwrap(),
            3
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT next_id FROM catalog_allocators WHERE name='version'",
                    [],
                    |row| row.get::<_, u64>(0),
                )
                .unwrap(),
            42
        );
    }
}
