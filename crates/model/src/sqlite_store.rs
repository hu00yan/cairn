//! Minimal single-node SQLite catalog adapter.
//!
//! This module is intentionally independent of `catalog.rs`, `dag.rs`, and
//! `native.rs`: it is an adapter seam, not a replacement for those models.
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
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
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
}

type T3Row = (u64, u64, Option<u64>, u64, Option<u64>, String, String);

fn validate_schema_v1(connection: &Connection) -> rusqlite::Result<()> {
    for statement in SCHEMA_V1.split(';') {
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
    fn from_connection(connection: Connection, require_wal: bool) -> rusqlite::Result<Self> {
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
        connection.execute_batch(SCHEMA_V1)?;
        let schema_version: u64 = connection.query_row(
            "SELECT schema_version FROM catalog_meta WHERE id = 1",
            [],
            |row| row.get(0),
        )?;
        if schema_version != 1 {
            return Err(rusqlite::Error::InvalidQuery);
        }
        validate_schema_v1(&connection)?;
        Ok(Self { connection })
    }
    pub fn with_immediate_transaction<T>(
        &mut self,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let value = f(&tx)?;
        tx.commit()?;
        Ok(value)
    }
    pub fn coordinator_epoch(&self) -> rusqlite::Result<u64> {
        self.connection.query_row(
            "SELECT coordinator_epoch FROM catalog_meta WHERE id=1",
            [],
            |r| r.get(0),
        )
    }
    pub fn cas_owner_epoch(&mut self, expected: u64, next: u64) -> rusqlite::Result<bool> {
        self.with_immediate_transaction(|tx| {
            Ok(tx.execute(
                "UPDATE catalog_meta SET coordinator_epoch=?1 WHERE id=1 AND coordinator_epoch=?2 AND ?1 > ?2",
                params![next, expected],
            )? == 1)
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
        tx.commit()
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
    pub fn t2_record_candidate(
        &mut self,
        op: u64,
        epoch: u64,
        nonce: u64,
        version: u64,
    ) -> rusqlite::Result<bool> {
        self.with_immediate_transaction(|tx|Ok(tx.execute("UPDATE publish_intents SET candidate_version_id=?1,state='commit_durable' WHERE operation_id=?2 AND owner_epoch=?3 AND owner_nonce=?4 AND state='preparing' AND (SELECT coordinator_epoch FROM catalog_meta WHERE id=1)=?3",params![version,op,epoch,nonce])?==1))
    }
    pub fn t3_publish(&mut self, op: u64, epoch: u64, nonce: u64) -> rusqlite::Result<bool> {
        self.with_immediate_transaction(|tx| {
            let row: Option<T3Row> = tx
                .query_row(
                    "SELECT i.file_id, i.actor_id, i.candidate_version_id,
                            i.expected_head_generation, i.expected_head_version_id,
                            p.state, o.state
                       FROM publish_intents AS i
                       JOIN principal AS p ON p.id = i.actor_id
                       JOIN operation_results AS o
                         ON o.operation_id = i.operation_id
                        AND o.actor_id = i.actor_id
                        AND o.kind = 'publish'
                        AND o.fingerprint = i.request_fingerprint
                        AND p.authz_epoch = i.authz_epoch
                      WHERE i.operation_id = ?1
                        AND i.owner_epoch = ?2
                        AND i.owner_nonce = ?3
                        AND i.state = 'commit_durable'
                        AND (SELECT coordinator_epoch FROM catalog_meta WHERE id = 1) = ?2",
                    params![op, epoch, nonce],
                    |r| {
                        Ok((
                            r.get(0)?,
                            r.get(1)?,
                            r.get(2)?,
                            r.get(3)?,
                            r.get(4)?,
                            r.get(5)?,
                            r.get(6)?,
                        ))
                    },
                )
                .optional()?;
            let Some((
                file,
                actor,
                candidate,
                generation,
                expected,
                principal_state,
                operation_state,
            )) = row
            else {
                return Ok(false);
            };
            if principal_state != "active"
                || matches!(operation_state.as_str(), "succeeded" | "failed")
            {
                return Ok(false);
            }

            let authorized: bool = tx.query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM collections AS c
                   JOIN files AS f ON f.collection_id = c.id
                  WHERE f.id = ?1 AND c.owner_id = ?2
                 UNION ALL
                 SELECT 1 FROM files AS f
                 JOIN collections AS c ON c.id = f.collection_id
                 JOIN membership AS m ON m.organization_id = c.owner_id
                                      AND m.member_id = ?2
                WHERE f.id = ?1 AND m.capability IN ('write', 'manage_members')
                )",
                params![file, actor],
                |r| r.get(0),
            )?;
            if !authorized {
                return Ok(false);
            }

            let Some(candidate) = candidate else {
                return Ok(false);
            };
            let next_generation = generation
                .checked_add(1)
                .ok_or(rusqlite::Error::InvalidQuery)?;
            let valid_version: bool = tx.query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM file_versions
                  WHERE id = ?1 AND file_id = ?2 AND generation = ?3
                    AND parent_version_id IS ?4
                )",
                params![candidate, file, next_generation, expected],
                |r| r.get(0),
            )?;
            if !valid_version {
                return Ok(false);
            }
            let head_matches: bool = tx.query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM file_head
                  WHERE file_id = ?1 AND generation = ?2 AND version_id IS ?3
                )",
                params![file, generation, expected],
                |r| r.get(0),
            )?;
            if !head_matches {
                return Ok(false);
            }

            let changed = tx.execute(
                "UPDATE file_head
                    SET version_id = ?1, generation = ?2
                  WHERE file_id = ?3 AND generation = ?4 AND version_id IS ?5",
                params![candidate, next_generation, file, generation, expected],
            )?;
            if changed != 1 {
                return Err(rusqlite::Error::InvalidQuery);
            }
            if tx.execute(
                "UPDATE publish_intents SET state = 'published', pinned = 0
                  WHERE operation_id = ?1 AND state = 'commit_durable'",
                [op],
            )? != 1
            {
                return Err(rusqlite::Error::InvalidQuery);
            }
            if tx.execute(
                "UPDATE operation_results
                    SET state = 'succeeded', result = ?1, error = NULL
                  WHERE operation_id = ?2 AND actor_id = ?3
                    AND kind = 'publish' AND state NOT IN ('succeeded', 'failed')",
                params![format!("version:{candidate}"), op, actor],
            )? != 1
            {
                return Err(rusqlite::Error::InvalidQuery);
            }
            Ok(true)
        })
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
    pub fn operation(&self, op: u64) -> rusqlite::Result<Option<OperationRecord>> {
        self.connection.query_row("SELECT actor_id,kind,fingerprint,state,result,error FROM operation_results WHERE operation_id=?1",[op],|r|{let f:Vec<u8>=r.get(2)?;Ok(OperationRecord{operation_id:op,actor_id:r.get(0)?,kind:r.get(1)?,request_fingerprint:f.try_into().map_err(|_|rusqlite::Error::InvalidQuery)?,state:r.get(3)?,result:r.get(4)?,error:r.get(5)?})}).optional()
    }
}
fn count_v1(c: &Connection, t: &str) -> rusqlite::Result<u64> {
    c.query_row(&format!("SELECT COUNT(*) FROM {t}"), [], |r| r.get(0))
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
const SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS catalog_meta(id INTEGER PRIMARY KEY CHECK(id=1),schema_version INTEGER NOT NULL CHECK(schema_version=1),coordinator_epoch INTEGER NOT NULL,allocators TEXT NOT NULL); INSERT OR IGNORE INTO catalog_meta VALUES(1,1,0,'{}');
CREATE TABLE IF NOT EXISTS principal(id INTEGER PRIMARY KEY,kind TEXT NOT NULL,state TEXT NOT NULL,authz_epoch INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS membership(organization_id INTEGER NOT NULL REFERENCES principal(id),member_id INTEGER NOT NULL REFERENCES principal(id),capability TEXT NOT NULL,PRIMARY KEY(organization_id,member_id,capability));
CREATE TABLE IF NOT EXISTS collections(id INTEGER PRIMARY KEY,owner_id INTEGER NOT NULL REFERENCES principal(id),name TEXT NOT NULL,UNIQUE(owner_id,name));
CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY,collection_id INTEGER NOT NULL REFERENCES collections(id),name TEXT NOT NULL,UNIQUE(collection_id,name));
CREATE TABLE IF NOT EXISTS file_versions(id INTEGER PRIMARY KEY,file_id INTEGER NOT NULL REFERENCES files(id),generation INTEGER NOT NULL,commit_id BLOB NOT NULL CHECK(typeof(commit_id)='blob' AND length(commit_id)=32),parent_version_id INTEGER,size INTEGER NOT NULL,digest BLOB NOT NULL CHECK(typeof(digest)='blob' AND length(digest)=32),UNIQUE(file_id,id),FOREIGN KEY(file_id,parent_version_id) REFERENCES file_versions(file_id,id));
CREATE TABLE IF NOT EXISTS file_head(file_id INTEGER PRIMARY KEY REFERENCES files(id),version_id INTEGER,generation INTEGER NOT NULL,FOREIGN KEY(file_id,version_id) REFERENCES file_versions(file_id,id));
CREATE TABLE IF NOT EXISTS publish_intents(operation_id INTEGER PRIMARY KEY,actor_id INTEGER NOT NULL REFERENCES principal(id),file_id INTEGER NOT NULL REFERENCES files(id),owner_epoch INTEGER NOT NULL,owner_nonce INTEGER NOT NULL,expected_head_version_id INTEGER,expected_head_generation INTEGER NOT NULL,candidate_version_id INTEGER,state TEXT NOT NULL CHECK(state IN ('preparing','commit_durable','published','aborted')),abort_reason TEXT,pinned INTEGER NOT NULL DEFAULT 0 CHECK(pinned IN(0,1)),request_fingerprint BLOB NOT NULL CHECK(typeof(request_fingerprint)='blob' AND length(request_fingerprint)=32),authz_epoch INTEGER NOT NULL,FOREIGN KEY(file_id,expected_head_version_id) REFERENCES file_versions(file_id,id),FOREIGN KEY(file_id,candidate_version_id) REFERENCES file_versions(file_id,id));
CREATE TABLE IF NOT EXISTS operation_results(operation_id INTEGER PRIMARY KEY,actor_id INTEGER NOT NULL REFERENCES principal(id),kind TEXT NOT NULL,fingerprint BLOB NOT NULL CHECK(typeof(fingerprint)='blob' AND length(fingerprint)=32),state TEXT NOT NULL,result TEXT,error TEXT,UNIQUE(actor_id,kind,fingerprint));
"#;
