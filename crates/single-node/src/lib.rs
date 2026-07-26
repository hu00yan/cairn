//! The external seam for a single-node Cairn store.
//!
//! This crate intentionally contains only the coordinator-facing shape. A
//! production implementation will keep the catalog and DAG as two durable
//! stores behind the coordinator; neither store's implementation type is part
//! of this interface.

#[cfg(any())]
mod legacy {

    use cairn_catalog::{FileId, Head, OperationId, OperationRecord, VersionId};
    use std::{error::Error, fmt, path::PathBuf};

    /// Locations owned by the future single-node coordinator.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SingleNodeConfig {
        /// Location of the durable catalog store.
        pub catalog_path: PathBuf,
        /// Location of the durable content/version store.
        pub data_path: PathBuf,
    }

    impl SingleNodeConfig {
        /// Creates a configuration for the two durable stores.
        pub fn new(catalog_path: impl Into<PathBuf>, data_path: impl Into<PathBuf>) -> Self {
            Self {
                catalog_path: catalog_path.into(),
                data_path: data_path.into(),
            }
        }
    }

    /// Errors returned while the coordinator implementation is being filled in.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum SingleNodeError {
        /// The requested coordinator operation has not been implemented yet.
        NotImplemented,
        /// The coordinator or one of its durable stores is not available.
        Unavailable,
    }

    impl fmt::Display for SingleNodeError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::NotImplemented => {
                    formatter.write_str("single-node operation is not implemented")
                }
                Self::Unavailable => formatter.write_str("single-node coordinator is unavailable"),
            }
        }
    }

    impl Error for SingleNodeError {}

    /// An opaque immutable snapshot handle.
    #[derive(Debug)]
    pub struct SnapshotHandle {
        _private: (),
    }

    /// An opaque write handle owned by the coordinator.
    #[derive(Debug)]
    pub struct WriteHandle {
        _private: (),
    }

    /// Public interface for a single-node coordinator.
    pub trait SingleNodeStoreApi {
        /// Opens the coordinator and completes startup recovery before use.
        fn open(config: SingleNodeConfig) -> Result<SingleNodeStore, SingleNodeError>;

        /// Opens an immutable snapshot for a file and optional version.
        fn open_snapshot(
            &self,
            file: FileId,
            version: Option<VersionId>,
        ) -> Result<SnapshotHandle, SingleNodeError>;

        /// Begins a write against the caller's expected file head.
        fn begin_write(
            &self,
            file: FileId,
            expected_head: Head,
            operation_id: OperationId,
        ) -> Result<WriteHandle, SingleNodeError>;

        /// Looks up the durable result for an operation, if one exists.
        fn query_operation(
            &self,
            operation_id: OperationId,
        ) -> Result<Option<OperationRecord>, SingleNodeError>;
    }

    /// Coordinator seam for the single-node implementation.
    #[derive(Debug)]
    pub struct SingleNodeStore {
        _private: (),
    }

    impl SingleNodeStore {
        /// Opens a single-node coordinator.
        ///
        /// The production implementation will encapsulate two durable stores
        /// behind this coordinator. Store recovery and commit ordering therefore
        /// remain internal to this crate.
        pub fn open(_config: SingleNodeConfig) -> Result<Self, SingleNodeError> {
            Err(SingleNodeError::NotImplemented)
        }

        /// Opens an immutable snapshot for a file and optional version.
        pub fn open_snapshot(
            &self,
            _file: FileId,
            _version: Option<VersionId>,
        ) -> Result<SnapshotHandle, SingleNodeError> {
            Err(SingleNodeError::NotImplemented)
        }

        /// Begins a write against the caller's expected file head.
        pub fn begin_write(
            &self,
            _file: FileId,
            _expected_head: Head,
            _operation_id: OperationId,
        ) -> Result<WriteHandle, SingleNodeError> {
            Err(SingleNodeError::NotImplemented)
        }

        /// Looks up the durable result for an operation, if one exists.
        pub fn query_operation(
            &self,
            _operation_id: OperationId,
        ) -> Result<Option<OperationRecord>, SingleNodeError> {
            Err(SingleNodeError::NotImplemented)
        }
    }

    impl SingleNodeStoreApi for SingleNodeStore {
        fn open(config: SingleNodeConfig) -> Result<SingleNodeStore, SingleNodeError> {
            Self::open(config)
        }

        fn open_snapshot(
            &self,
            file: FileId,
            version: Option<VersionId>,
        ) -> Result<SnapshotHandle, SingleNodeError> {
            Self::open_snapshot(self, file, version)
        }

        fn begin_write(
            &self,
            file: FileId,
            expected_head: Head,
            operation_id: OperationId,
        ) -> Result<WriteHandle, SingleNodeError> {
            Self::begin_write(self, file, expected_head, operation_id)
        }

        fn query_operation(
            &self,
            operation_id: OperationId,
        ) -> Result<Option<OperationRecord>, SingleNodeError> {
            Self::query_operation(self, operation_id)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn public_seam_compiles_without_exposing_store_implementations() {
            fn open<T: SingleNodeStoreApi>(config: SingleNodeConfig) {
                let _ = T::open(config);
            }

            open::<SingleNodeStore>(SingleNodeConfig::new("catalog", "data"));
            assert!(matches!(
                SingleNodeStore::open(SingleNodeConfig::new("catalog", "data")),
                Err(SingleNodeError::NotImplemented)
            ));
        }
    }
}

use cairn_catalog::sqlite_catalog::{
    CatalogOperationState, CatalogVersion, ClaimIntentOutcome, CoordinatorEpoch, EpochClaim,
    FileInfo, IntentRecord, OperationRecord, ReadAuthorization, RecoveryIntent, RecoveryWork,
    SqliteCatalogStore, T2Outcome, T3Outcome,
};
use cairn_catalog::{FileId, Head, OperationId, VersionId};
use cairn_device::{
    dag_store::{FileDagStore, FileDagStoreError},
    io::FileDevice,
};
use std::{
    error::Error,
    fmt,
    fs::{File, OpenOptions},
    ops::Range,
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SingleNodeConfig {
    pub catalog_path: PathBuf,
    pub data_path: PathBuf,
    pub actor_id: u64,
}

impl SingleNodeConfig {
    pub fn new(
        catalog_path: impl Into<PathBuf>,
        data_path: impl Into<PathBuf>,
        actor_id: u64,
    ) -> Self {
        Self {
            catalog_path: catalog_path.into(),
            data_path: data_path.into(),
            actor_id,
        }
    }
}

#[derive(Debug)]
pub enum SingleNodeError {
    CatalogUnavailable,
    DeviceUnavailable,
    DagUnavailable,
    Corrupt,
    MetadataMismatch,
    NoVersion,
    UnsupportedWrite,
    Unauthorized,
    NotPublished,
    OutOfBounds,
    Unavailable,
    Poisoned,
}

impl fmt::Display for SingleNodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "single-node error: {self:?}")
    }
}
impl Error for SingleNodeError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotInfo {
    pub len: u64,
    pub digest: [u8; 32],
}

const MAX_WRITE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteResult {
    pub version: VersionId,
    pub info: SnapshotInfo,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BootstrapIds {
    pub principal: u64,
    pub collection: u64,
    pub file: FileId,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReclaimReport {
    /// Terminal operation bindings removed from the live DAG index.
    pub tombstoned_handoffs: u64,
    /// Bytes physically freed. The current DAG is append-only, so this is 0.
    pub bytes_reclaimed: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitFaultPoint {
    None,
    AfterDagAppend,
    AfterDagBind,
    AfterCatalogCandidate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationStatus {
    Unknown,
    InProgress,
    Succeeded,
    Failed,
}

#[derive(Clone)]
pub struct SingleNodeStore {
    catalog: Arc<Mutex<SqliteCatalogStore>>,
    dag: Arc<Mutex<FileDagStore<FileDevice>>>,
    _dag_lock: Arc<DagWriterLock>,
    actor_id: u64,
    coordinator_epoch: u64,
}

#[derive(Debug)]
struct DagWriterLock {
    _file: File,
}

impl DagWriterLock {
    fn acquire(data_path: &Path) -> Result<Self, SingleNodeError> {
        let lock_path = data_path.with_extension("dag.lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(|_| SingleNodeError::DeviceUnavailable)?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            return Err(SingleNodeError::Unavailable);
        }
        Ok(Self { _file: file })
    }
}

pub struct SnapshotHandle {
    catalog: Arc<Mutex<SqliteCatalogStore>>,
    dag: Arc<Mutex<FileDagStore<FileDevice>>>,
    commit_id: [u8; 32],
    file_id: u64,
    version_id: u64,
    lease_id: u64,
    actor_id: u64,
    coordinator_epoch: u64,
    info: SnapshotInfo,
}

pub struct WriteHandle {
    catalog: Arc<Mutex<SqliteCatalogStore>>,
    dag: Arc<Mutex<FileDagStore<FileDevice>>>,
    coordinator_epoch: u64,
    file_id: FileId,
    operation_id: OperationId,
    expected_head: Head,
    parent_commit: Option<[u8; 32]>,
    bytes: Vec<u8>,
    committed: Option<WriteResult>,
    _dag_lock: Arc<DagWriterLock>,
}

impl SingleNodeStore {
    /// Creates the initial metadata rows in one SQLite transaction.
    ///
    /// The DAG file must already exist and be preallocated by the device layer;
    /// this method only bootstraps catalog authority.
    pub fn bootstrap(
        config: &SingleNodeConfig,
        collection_name: &str,
        file_name: &str,
    ) -> Result<BootstrapIds, SingleNodeError> {
        let mut catalog = SqliteCatalogStore::open(&config.catalog_path)
            .map_err(|_| SingleNodeError::CatalogUnavailable)?;
        catalog
            .bootstrap(config.actor_id, 1, 1, collection_name, file_name)
            .map_err(|_| SingleNodeError::CatalogUnavailable)?;
        Ok(BootstrapIds {
            principal: config.actor_id,
            collection: 1,
            file: FileId::from_raw(1),
        })
    }

    pub fn open(config: SingleNodeConfig) -> Result<Self, SingleNodeError> {
        let dag_lock = Arc::new(DagWriterLock::acquire(&config.data_path)?);
        let device =
            FileDevice::open(config.data_path).map_err(|_| SingleNodeError::DeviceUnavailable)?;
        let mut dag = FileDagStore::open(device).map_err(map_dag)?;
        let mut catalog = SqliteCatalogStore::open(config.catalog_path)
            .map_err(|_| SingleNodeError::CatalogUnavailable)?;
        let current_epoch = catalog
            .coordinator_epoch()
            .map_err(|_| SingleNodeError::CatalogUnavailable)?;
        let next_epoch = current_epoch
            .checked_add(1)
            .ok_or(SingleNodeError::CatalogUnavailable)?;
        let coordinator_epoch = match catalog
            .claim_coordinator_epoch(
                cairn_catalog::sqlite_catalog::CoordinatorEpoch::new(current_epoch),
                cairn_catalog::sqlite_catalog::CoordinatorEpoch::new(next_epoch),
            )
            .map_err(|_| SingleNodeError::CatalogUnavailable)?
        {
            EpochClaim::Claimed(epoch) => epoch.get(),
            EpochClaim::Stale { .. } => return Err(SingleNodeError::CatalogUnavailable),
        };
        for work in catalog
            .recovery_work()
            .map_err(|_| SingleNodeError::CatalogUnavailable)?
        {
            let intent = match work {
                RecoveryWork::TombstoneDagBinding { intent, .. } => {
                    let operation_id = OperationId::from_raw(intent.operation_id);
                    if let Some(commit_id) = dag.operation_binding(operation_id) {
                        dag.tombstone_operation(operation_id, commit_id)
                            .map_err(map_dag)?;
                    }
                    continue;
                }
                RecoveryWork::Resume(intent) => intent,
            };
            let claimed = catalog
                .claim_intent(
                    intent.operation_id,
                    CoordinatorEpoch::new(coordinator_epoch),
                    intent.operation_id,
                )
                .map_err(|_| SingleNodeError::CatalogUnavailable)?;
            let RecoveryIntent::Nonterminal(intent) = (match claimed {
                ClaimIntentOutcome::Claimed(recovery) | ClaimIntentOutcome::Terminal(recovery) => {
                    recovery
                }
                ClaimIntentOutcome::AlreadyClaimed { .. }
                | ClaimIntentOutcome::Missing
                | ClaimIntentOutcome::Fenced { .. }
                | ClaimIntentOutcome::FutureOwner { .. } => {
                    return Err(SingleNodeError::CatalogUnavailable)
                }
            }) else {
                continue;
            };
            if intent.state == "commit_durable" && intent.candidate_version_id.is_some() {
                let candidate = intent.candidate_version_id.unwrap();
                let version = catalog
                    .read_candidate_version(intent.file_id, candidate)
                    .map_err(|_| SingleNodeError::CatalogUnavailable)?
                    .ok_or(SingleNodeError::CatalogUnavailable)?;
                let operation_id = OperationId::from_raw(intent.operation_id);
                let commit_id = dag
                    .operation_binding(operation_id)
                    .ok_or(SingleNodeError::DagUnavailable)?;
                if commit_id != version.commit_id {
                    abort_recovery(
                        &mut catalog,
                        &mut dag,
                        &intent,
                        coordinator_epoch,
                        Some(commit_id),
                        "recovery_candidate_commit_mismatch",
                    )?;
                    continue;
                }
                let verified = match dag.verified_snapshot(commit_id) {
                    Ok(verified) => verified,
                    Err(_) => {
                        abort_recovery(
                            &mut catalog,
                            &mut dag,
                            &intent,
                            coordinator_epoch,
                            Some(commit_id),
                            "recovery_candidate_dag_invalid",
                        )?;
                        continue;
                    }
                };
                if verified.logical_size != version.size
                    || verified.content_digest != version.digest
                {
                    abort_recovery(
                        &mut catalog,
                        &mut dag,
                        &intent,
                        coordinator_epoch,
                        Some(commit_id),
                        "recovery_candidate_metadata_mismatch",
                    )?;
                    continue;
                }
                let outcome = catalog
                    .t3_publish_typed(
                        intent.operation_id,
                        CoordinatorEpoch::new(coordinator_epoch),
                        intent.owner_nonce,
                    )
                    .map_err(|_| SingleNodeError::CatalogUnavailable)?;
                match outcome {
                    T3Outcome::Published | T3Outcome::AlreadyPublished => {}
                    T3Outcome::Fenced => return Err(SingleNodeError::CatalogUnavailable),
                    _ => abort_recovery(
                        &mut catalog,
                        &mut dag,
                        &intent,
                        coordinator_epoch,
                        Some(commit_id),
                        "recovery_publish_failed",
                    )?,
                }
            } else if intent.state == "preparing"
                && intent.candidate_version_id.is_none()
                && dag
                    .operation_binding(OperationId::from_raw(intent.operation_id))
                    .is_none()
            {
                let aborted = catalog
                    .abort(
                        intent.operation_id,
                        coordinator_epoch,
                        intent.owner_nonce,
                        "recovery_without_dag_binding",
                    )
                    .map_err(|_| SingleNodeError::CatalogUnavailable)?;
                if !aborted {
                    return Err(SingleNodeError::CatalogUnavailable);
                }
            } else if intent.state == "preparing" && intent.candidate_version_id.is_none() {
                let operation_id = OperationId::from_raw(intent.operation_id);
                if let Some(commit_id) = dag.operation_binding(operation_id) {
                    let verified = match dag.verified_snapshot(commit_id) {
                        Ok(verified) => verified,
                        Err(_) => {
                            abort_recovery(
                                &mut catalog,
                                &mut dag,
                                &intent,
                                coordinator_epoch,
                                Some(commit_id),
                                "recovery_dag_binding_invalid",
                            )?;
                            continue;
                        }
                    };
                    let generation = intent
                        .expected_head_generation
                        .checked_add(1)
                        .ok_or(SingleNodeError::CatalogUnavailable)?;
                    let candidate = CatalogVersion {
                        id: 0,
                        file_id: intent.file_id,
                        generation,
                        commit_id,
                        parent_version_id: intent.expected_head_version_id,
                        size: verified.logical_size,
                        digest: verified.content_digest,
                    };
                    let t2 = catalog
                        .t2_record_version(
                            intent.operation_id,
                            CoordinatorEpoch::new(coordinator_epoch),
                            intent.owner_nonce,
                            &candidate,
                        )
                        .map_err(|_| SingleNodeError::CatalogUnavailable)?;
                    if !matches!(t2, T2Outcome::Applied) {
                        abort_recovery(
                            &mut catalog,
                            &mut dag,
                            &intent,
                            coordinator_epoch,
                            Some(commit_id),
                            "recovery_candidate_record_failed",
                        )?;
                        continue;
                    }
                    let outcome = catalog
                        .t3_publish_typed(
                            intent.operation_id,
                            CoordinatorEpoch::new(coordinator_epoch),
                            intent.owner_nonce,
                        )
                        .map_err(|_| SingleNodeError::CatalogUnavailable)?;
                    match outcome {
                        T3Outcome::Published | T3Outcome::AlreadyPublished => {}
                        T3Outcome::Fenced => return Err(SingleNodeError::CatalogUnavailable),
                        _ => abort_recovery(
                            &mut catalog,
                            &mut dag,
                            &intent,
                            coordinator_epoch,
                            Some(commit_id),
                            "recovery_publish_failed",
                        )?,
                    }
                }
            }
        }
        Ok(Self {
            catalog: Arc::new(Mutex::new(catalog)),
            dag: Arc::new(Mutex::new(dag)),
            _dag_lock: dag_lock,
            actor_id: config.actor_id,
            coordinator_epoch,
        })
    }

    pub fn open_snapshot(
        &self,
        file: FileId,
        version: Option<VersionId>,
    ) -> Result<SnapshotHandle, SingleNodeError> {
        let mut catalog = self.catalog.lock().map_err(|_| SingleNodeError::Poisoned)?;
        let version_id = match version {
            Some(id) => id.get(),
            None => catalog
                .read_head(file.get())
                .map_err(|_| SingleNodeError::CatalogUnavailable)?
                .and_then(|h| h.version_id)
                .ok_or(SingleNodeError::NoVersion)?,
        };
        match catalog
            .read_authorization(self.actor_id, file.get(), version_id)
            .map_err(|_| SingleNodeError::CatalogUnavailable)?
        {
            ReadAuthorization::Authorized => {}
            ReadAuthorization::Missing => return Err(SingleNodeError::NoVersion),
            ReadAuthorization::Unauthorized => return Err(SingleNodeError::Unauthorized),
            ReadAuthorization::NotPublished => return Err(SingleNodeError::NotPublished),
        }
        let version = catalog
            .read_version(file.get(), version_id)
            .map_err(|_| SingleNodeError::CatalogUnavailable)?
            .ok_or(SingleNodeError::NoVersion)?;
        let lease_id = catalog
            .acquire_reader_lease(
                self.actor_id,
                file.get(),
                version_id,
                self.coordinator_epoch,
            )
            .map_err(|_| SingleNodeError::CatalogUnavailable)?;
        drop(catalog);
        let verified_result = {
            let mut dag = self.dag.lock().map_err(|_| SingleNodeError::Poisoned)?;
            dag.verified_snapshot(version.commit_id)
        };
        let verified = match verified_result {
            Ok(verified) => verified,
            Err(error) => {
                let mut catalog = self.catalog.lock().map_err(|_| SingleNodeError::Poisoned)?;
                let _ = catalog.release_reader_lease(lease_id);
                return Err(map_dag(error));
            }
        };
        if verified.logical_size != version.size || verified.content_digest != version.digest {
            let mut catalog = self.catalog.lock().map_err(|_| SingleNodeError::Poisoned)?;
            let _ = catalog.release_reader_lease(lease_id);
            return Err(SingleNodeError::MetadataMismatch);
        }
        Ok(SnapshotHandle {
            catalog: Arc::clone(&self.catalog),
            dag: Arc::clone(&self.dag),
            commit_id: verified.commit_id,
            file_id: file.get(),
            version_id: version.id,
            actor_id: self.actor_id,
            coordinator_epoch: self.coordinator_epoch,
            lease_id,
            info: SnapshotInfo {
                len: verified.logical_size,
                digest: verified.content_digest,
            },
        })
    }

    /// Creates a collection owned by the configured actor. Repeating the same
    /// name returns the durable existing row.
    pub fn create_collection(&self, name: &str) -> Result<u64, SingleNodeError> {
        let mut catalog = self.catalog.lock().map_err(|_| SingleNodeError::Poisoned)?;
        catalog
            .create_collection(self.actor_id, name)
            .map(|collection| collection.id)
            .map_err(|_| SingleNodeError::Unauthorized)
    }

    /// Returns the current catalog head used as the optimistic write token.
    pub fn current_head(&self, file: FileId) -> Result<Head, SingleNodeError> {
        let catalog = self.catalog.lock().map_err(|_| SingleNodeError::Poisoned)?;
        let head = catalog
            .read_head(file.get())
            .map_err(|_| SingleNodeError::CatalogUnavailable)?
            .ok_or(SingleNodeError::NoVersion)?;
        Ok(Head {
            version_id: head.version_id.map(VersionId::from_raw),
            generation: head.generation,
        })
    }

    /// Creates a file in a collection. Repeating the same name is idempotent.
    pub fn create_file(&self, collection: u64, name: &str) -> Result<FileInfo, SingleNodeError> {
        let mut catalog = self.catalog.lock().map_err(|_| SingleNodeError::Poisoned)?;
        catalog
            .create_file(self.actor_id, collection, name)
            .map_err(|_| SingleNodeError::Unauthorized)
    }

    /// Completes terminal catalog-to-DAG handoffs that are safe to retire.
    ///
    /// This is logical reclamation only. Physical DAG compaction requires a
    /// separate rewrite protocol and is intentionally not implied here.
    pub fn reclaim(&self) -> Result<ReclaimReport, SingleNodeError> {
        let ids = {
            let catalog = self.catalog.lock().map_err(|_| SingleNodeError::Poisoned)?;
            catalog
                .terminal_operation_ids()
                .map_err(|_| SingleNodeError::CatalogUnavailable)?
        };
        let mut report = ReclaimReport::default();
        let mut dag = self.dag.lock().map_err(|_| SingleNodeError::Poisoned)?;
        for operation_id in ids {
            let operation_id = OperationId::from_raw(operation_id);
            if let Some(commit_id) = dag.operation_binding(operation_id) {
                dag.tombstone_operation(operation_id, commit_id)
                    .map_err(map_dag)?;
                report.tombstoned_handoffs += 1;
            }
        }
        Ok(report)
    }

    pub fn begin_write(
        &self,
        file: FileId,
        expected_head: Head,
        operation_id: OperationId,
    ) -> Result<WriteHandle, SingleNodeError> {
        let (bytes, parent_commit) = match expected_head.version_id {
            Some(version) => {
                let snapshot = self.open_snapshot(file, Some(version))?;
                if snapshot.len() as usize > MAX_WRITE_BYTES {
                    return Err(SingleNodeError::UnsupportedWrite);
                }
                (
                    snapshot.read_range(0..snapshot.len())?,
                    Some(snapshot.commit_id),
                )
            }
            None => (Vec::new(), None),
        };
        let mut catalog = self.catalog.lock().map_err(|_| SingleNodeError::Poisoned)?;
        let authz_epoch = catalog
            .principal_authz_epoch(self.actor_id)
            .map_err(|_| SingleNodeError::CatalogUnavailable)?
            .ok_or(SingleNodeError::Unauthorized)?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"cairn/single-node/write/v1");
        hasher.update(&self.actor_id.to_le_bytes());
        hasher.update(&file.get().to_le_bytes());
        hasher.update(&operation_id.get().to_le_bytes());
        hasher.update(&expected_head.generation.to_le_bytes());
        let fingerprint = *hasher.finalize().as_bytes();
        let intent = IntentRecord {
            operation_id: operation_id.get(),
            actor_id: self.actor_id,
            file_id: file.get(),
            owner_epoch: self.coordinator_epoch,
            owner_nonce: operation_id.get(),
            expected_head_version_id: expected_head.version_id.map(VersionId::get),
            expected_head_generation: expected_head.generation,
            candidate_version_id: None,
            state: "preparing".into(),
            abort_reason: None,
            pinned: true,
            request_fingerprint: fingerprint,
            authz_epoch,
        };
        let operation = OperationRecord {
            operation_id: operation_id.get(),
            actor_id: self.actor_id,
            kind: "publish".into(),
            request_fingerprint: fingerprint,
            state: "in_progress".into(),
            result: None,
            error: None,
        };
        if !catalog
            .prepare_publish(&operation, &intent)
            .map_err(|_| SingleNodeError::CatalogUnavailable)?
        {
            return Err(SingleNodeError::Unavailable);
        }
        drop(catalog);
        Ok(WriteHandle {
            catalog: Arc::clone(&self.catalog),
            dag: Arc::clone(&self.dag),
            coordinator_epoch: self.coordinator_epoch,
            file_id: file,
            operation_id,
            expected_head,
            parent_commit,
            bytes,
            committed: None,
            _dag_lock: Arc::clone(&self._dag_lock),
        })
    }

    pub fn query_operation(
        &self,
        _operation_id: OperationId,
    ) -> Result<OperationStatus, SingleNodeError> {
        let catalog = self.catalog.lock().map_err(|_| SingleNodeError::Poisoned)?;
        Ok(
            match catalog
                .operation_state(self.actor_id, _operation_id.get())
                .map_err(|_| SingleNodeError::CatalogUnavailable)?
            {
                None => OperationStatus::Unknown,
                Some(CatalogOperationState::InProgress) => OperationStatus::InProgress,
                Some(CatalogOperationState::Succeeded) => OperationStatus::Succeeded,
                Some(CatalogOperationState::Failed) => OperationStatus::Failed,
            },
        )
    }
}

impl WriteHandle {
    pub fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<(), SingleNodeError> {
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(SingleNodeError::OutOfBounds)?;
        if end as usize > MAX_WRITE_BYTES {
            return Err(SingleNodeError::UnsupportedWrite);
        }
        let offset = offset as usize;
        let end = end as usize;
        if self.bytes.len() < end {
            self.bytes.resize(end, 0);
        }
        self.bytes[offset..end].copy_from_slice(bytes);
        Ok(())
    }

    pub fn truncate(&mut self, len: u64) -> Result<(), SingleNodeError> {
        let len = usize::try_from(len).map_err(|_| SingleNodeError::OutOfBounds)?;
        if len > self.bytes.len() || len > MAX_WRITE_BYTES {
            return Err(SingleNodeError::OutOfBounds);
        }
        self.bytes.truncate(len);
        Ok(())
    }

    pub fn commit(&mut self) -> Result<WriteResult, SingleNodeError> {
        self.commit_with_fault(CommitFaultPoint::None)
    }

    pub fn commit_with_fault(
        &mut self,
        fault: CommitFaultPoint,
    ) -> Result<WriteResult, SingleNodeError> {
        if let Some(result) = self.committed {
            return Ok(result);
        }
        let verified = {
            let mut dag = self.dag.lock().map_err(|_| SingleNodeError::Poisoned)?;
            let verified = dag
                .append_snapshot(&self.bytes, self.parent_commit)
                .map_err(map_dag)?;
            if fault == CommitFaultPoint::AfterDagAppend {
                return Err(SingleNodeError::Unavailable);
            }
            dag.bind_operation(self.operation_id, verified.commit_id)
                .map_err(map_dag)?;
            if fault == CommitFaultPoint::AfterDagBind {
                return Err(SingleNodeError::Unavailable);
            }
            verified
        };
        let version = CatalogVersion {
            id: 0,
            file_id: self.file_id.get(),
            generation: self
                .expected_head
                .generation
                .checked_add(1)
                .ok_or(SingleNodeError::Corrupt)?,
            commit_id: verified.commit_id,
            parent_version_id: self.expected_head.version_id.map(VersionId::get),
            size: verified.logical_size,
            digest: verified.content_digest,
        };
        let mut catalog = self.catalog.lock().map_err(|_| SingleNodeError::Poisoned)?;
        match catalog
            .t2_record_version(
                self.operation_id.get(),
                CoordinatorEpoch::new(self.coordinator_epoch),
                self.operation_id.get(),
                &version,
            )
            .map_err(|_| SingleNodeError::CatalogUnavailable)?
        {
            T2Outcome::Applied => {}
            T2Outcome::MissingIntent | T2Outcome::Fenced | T2Outcome::NotPreparing => {
                return Err(SingleNodeError::Unavailable)
            }
        }
        let version_id = catalog
            .candidate_version_id(self.operation_id.get())
            .map_err(|_| SingleNodeError::CatalogUnavailable)?
            .ok_or(SingleNodeError::Unavailable)?;
        if fault == CommitFaultPoint::AfterCatalogCandidate {
            return Err(SingleNodeError::Unavailable);
        }
        match catalog
            .t3_publish_typed(
                self.operation_id.get(),
                CoordinatorEpoch::new(self.coordinator_epoch),
                self.operation_id.get(),
            )
            .map_err(|_| SingleNodeError::CatalogUnavailable)?
        {
            T3Outcome::Published | T3Outcome::AlreadyPublished => {}
            T3Outcome::Fenced => return Err(SingleNodeError::Unavailable),
            outcome => {
                let aborted = catalog
                    .abort(
                        self.operation_id.get(),
                        self.coordinator_epoch,
                        self.operation_id.get(),
                        &format!("publish_{outcome:?}"),
                    )
                    .map_err(|_| SingleNodeError::CatalogUnavailable)?;
                if !aborted {
                    return Err(SingleNodeError::Unavailable);
                }
                drop(catalog);
                let mut dag = self.dag.lock().map_err(|_| SingleNodeError::Poisoned)?;
                dag.tombstone_operation(self.operation_id, verified.commit_id)
                    .map_err(map_dag)?;
                return Err(SingleNodeError::Unavailable);
            }
        }
        let result = WriteResult {
            version: VersionId::from_raw(version_id),
            info: SnapshotInfo {
                len: verified.logical_size,
                digest: verified.content_digest,
            },
        };
        self.committed = Some(result);
        Ok(result)
    }

    pub fn abort(&mut self) -> Result<(), SingleNodeError> {
        let mut catalog = self.catalog.lock().map_err(|_| SingleNodeError::Poisoned)?;
        catalog
            .abort(
                self.operation_id.get(),
                self.coordinator_epoch,
                self.operation_id.get(),
                "client_abort",
            )
            .map(|_| ())
            .map_err(|_| SingleNodeError::CatalogUnavailable)
    }
}

impl SnapshotHandle {
    pub fn info(&self) -> SnapshotInfo {
        self.info
    }
    pub fn len(&self) -> u64 {
        self.info.len
    }
    pub fn is_empty(&self) -> bool {
        self.info.len == 0
    }
    pub fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SingleNodeError> {
        let catalog = self.catalog.lock().map_err(|_| SingleNodeError::Poisoned)?;
        match catalog
            .read_authorization(self.actor_id, self.file_id, self.version_id)
            .map_err(|_| SingleNodeError::CatalogUnavailable)?
        {
            ReadAuthorization::Authorized => {}
            ReadAuthorization::Missing => return Err(SingleNodeError::NoVersion),
            ReadAuthorization::Unauthorized => return Err(SingleNodeError::Unauthorized),
            ReadAuthorization::NotPublished => return Err(SingleNodeError::NotPublished),
        }
        if !catalog
            .reader_lease_active(
                self.lease_id,
                self.actor_id,
                self.file_id,
                self.version_id,
                self.coordinator_epoch,
            )
            .map_err(|_| SingleNodeError::CatalogUnavailable)?
        {
            return Err(SingleNodeError::Unavailable);
        }
        if range.start > range.end || range.end > self.info.len {
            return Err(SingleNodeError::OutOfBounds);
        }
        let mut dag = self.dag.lock().map_err(|_| SingleNodeError::Poisoned)?;
        dag.read_snapshot_range(self.commit_id, range)
            .map_err(map_dag)
    }
}

impl Drop for SnapshotHandle {
    fn drop(&mut self) {
        if let Ok(mut catalog) = self.catalog.lock() {
            let _ = catalog.release_reader_lease(self.lease_id);
        }
    }
}

fn map_dag(error: FileDagStoreError) -> SingleNodeError {
    match error {
        FileDagStoreError::Device(_) => SingleNodeError::DagUnavailable,
        FileDagStoreError::RangeOutOfBounds { .. } => SingleNodeError::OutOfBounds,
        _ => SingleNodeError::Corrupt,
    }
}

fn abort_recovery(
    catalog: &mut SqliteCatalogStore,
    dag: &mut FileDagStore<FileDevice>,
    intent: &IntentRecord,
    coordinator_epoch: u64,
    commit_id: Option<[u8; 32]>,
    reason: &str,
) -> Result<(), SingleNodeError> {
    let aborted = catalog
        .abort(
            intent.operation_id,
            coordinator_epoch,
            intent.owner_nonce,
            reason,
        )
        .map_err(|_| SingleNodeError::CatalogUnavailable)?;
    if !aborted {
        return Err(SingleNodeError::CatalogUnavailable);
    }
    if let Some(commit_id) = commit_id {
        dag.tombstone_operation(OperationId::from_raw(intent.operation_id), commit_id)
            .map_err(map_dag)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_catalog::sqlite_catalog::{
        CatalogBatch, CollectionRecord, FileRecord, HeadRecord, PrincipalRecord,
    };
    use cairn_device::io::FileDevice;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_paths() -> (PathBuf, PathBuf) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let suffix = format!(
            "{nonce}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        (
            std::env::temp_dir().join(format!("cairn-single-node-{suffix}.db")),
            std::env::temp_dir().join(format!("cairn-single-node-{suffix}.dag")),
        )
    }

    fn initialize_catalog(catalog_path: &PathBuf) {
        let mut catalog = SqliteCatalogStore::open(catalog_path).unwrap();
        catalog
            .persist(&CatalogBatch {
                principals: vec![PrincipalRecord {
                    id: 1,
                    kind: "user".into(),
                    state: "active".into(),
                    authz_epoch: 0,
                }],
                collections: vec![CollectionRecord {
                    id: 10,
                    owner_id: 1,
                    name: "docs".into(),
                }],
                files: vec![FileRecord {
                    id: 20,
                    collection_id: 10,
                    name: "readme".into(),
                }],
                heads: vec![HeadRecord {
                    file_id: 20,
                    version_id: None,
                    generation: 0,
                }],
                ..CatalogBatch::default()
            })
            .unwrap();
    }

    fn cleanup(catalog_path: &PathBuf, data_path: &PathBuf) {
        let _ = std::fs::remove_file(catalog_path);
        let _ = std::fs::remove_file(catalog_path.with_extension("db-wal"));
        let _ = std::fs::remove_file(catalog_path.with_extension("db-shm"));
        let _ = std::fs::remove_file(data_path);
    }

    #[test]
    fn open_is_a_real_two_store_open_and_fails_closed() {
        let result = SingleNodeStore::open(SingleNodeConfig::new(
            "/definitely/missing/catalog.db",
            "/definitely/missing/data.dag",
            0,
        ));
        assert!(matches!(
            result,
            Err(SingleNodeError::CatalogUnavailable) | Err(SingleNodeError::DeviceUnavailable)
        ));
    }

    #[test]
    fn dag_writer_lock_rejects_a_second_coordinator() {
        let (catalog_path, data_path) = test_paths();
        initialize_catalog(&catalog_path);
        FileDevice::create_preallocated(&data_path, 4 * 1024 * 1024).unwrap();
        let config = SingleNodeConfig::new(&catalog_path, &data_path, 1);
        let first = SingleNodeStore::open(config.clone()).unwrap();
        assert!(matches!(
            SingleNodeStore::open(config),
            Err(SingleNodeError::Unavailable)
        ));
        drop(first);
        cleanup(&catalog_path, &data_path);
    }

    #[test]
    fn committed_write_survives_sqlite_and_dag_reopen() {
        let (catalog_path, data_path) = test_paths();
        initialize_catalog(&catalog_path);
        FileDevice::create_preallocated(&data_path, 4 * 1024 * 1024).unwrap();
        let config = SingleNodeConfig::new(&catalog_path, &data_path, 1);

        let result = {
            let store = SingleNodeStore::open(config.clone()).unwrap();
            let mut write = store
                .begin_write(
                    FileId::from_raw(20),
                    Head {
                        version_id: None,
                        generation: 0,
                    },
                    OperationId::from_raw(40),
                )
                .unwrap();
            write.write_at(0, b"hello, cairn").unwrap();
            let committed = write.commit().unwrap();
            assert_eq!(
                store.query_operation(OperationId::from_raw(40)).unwrap(),
                OperationStatus::Succeeded
            );
            assert_eq!(
                store
                    .open_snapshot(FileId::from_raw(20), None)
                    .unwrap()
                    .read_range(0..12)
                    .unwrap(),
                b"hello, cairn"
            );
            committed
        };

        let reopened = SingleNodeStore::open(config.clone()).unwrap();
        let snapshot = reopened
            .open_snapshot(FileId::from_raw(20), Some(result.version))
            .unwrap();
        assert_eq!(snapshot.info(), result.info);
        assert_eq!(
            snapshot.read_range(0..snapshot.len()).unwrap(),
            b"hello, cairn"
        );
        assert_eq!(
            reopened.query_operation(OperationId::from_raw(40)).unwrap(),
            OperationStatus::Succeeded
        );
        cleanup(&catalog_path, &data_path);
    }

    #[test]
    fn normal_t3_head_conflict_aborts_and_tombstones_the_candidate() {
        let (catalog_path, data_path) = test_paths();
        initialize_catalog(&catalog_path);
        FileDevice::create_preallocated(&data_path, 4 * 1024 * 1024).unwrap();
        let config = SingleNodeConfig::new(&catalog_path, &data_path, 1);
        let store = SingleNodeStore::open(config.clone()).unwrap();
        let expected = Head {
            version_id: None,
            generation: 0,
        };
        let mut stale = store
            .begin_write(FileId::from_raw(20), expected, OperationId::from_raw(50))
            .unwrap();
        let mut winner = store
            .begin_write(FileId::from_raw(20), expected, OperationId::from_raw(51))
            .unwrap();
        winner.write_at(0, b"winner").unwrap();
        winner.commit().unwrap();
        stale.write_at(0, b"stale").unwrap();
        assert!(matches!(stale.commit(), Err(SingleNodeError::Unavailable)));
        assert_eq!(
            store.query_operation(OperationId::from_raw(50)).unwrap(),
            OperationStatus::Failed
        );
        drop(stale);
        drop(winner);
        drop(store);

        let reopened = SingleNodeStore::open(config).unwrap();
        assert_eq!(
            reopened.query_operation(OperationId::from_raw(50)).unwrap(),
            OperationStatus::Failed
        );
        assert!(reopened
            .dag
            .lock()
            .unwrap()
            .operation_tombstone(OperationId::from_raw(50))
            .is_some());
        cleanup(&catalog_path, &data_path);
    }

    #[test]
    fn reopen_publishes_a_durable_candidate_after_interrupted_t3() {
        let (catalog_path, data_path) = test_paths();
        initialize_catalog(&catalog_path);
        FileDevice::create_preallocated(&data_path, 4 * 1024 * 1024).unwrap();
        let config = SingleNodeConfig::new(&catalog_path, &data_path, 1);

        {
            let store = SingleNodeStore::open(config.clone()).unwrap();
            let mut write = store
                .begin_write(
                    FileId::from_raw(20),
                    Head {
                        version_id: None,
                        generation: 0,
                    },
                    OperationId::from_raw(41),
                )
                .unwrap();
            write.write_at(0, b"recovery").unwrap();
            let verified = {
                let mut dag = write.dag.lock().unwrap();
                let verified = dag
                    .append_snapshot(&write.bytes, write.parent_commit)
                    .unwrap();
                dag.bind_operation(write.operation_id, verified.commit_id)
                    .unwrap();
                verified
            };
            let version = CatalogVersion {
                id: 0,
                file_id: 20,
                generation: 1,
                commit_id: verified.commit_id,
                parent_version_id: None,
                size: verified.logical_size,
                digest: verified.content_digest,
            };
            let mut catalog = write.catalog.lock().unwrap();
            assert_eq!(
                catalog
                    .t2_record_version(41, CoordinatorEpoch::new(1), 41, &version)
                    .unwrap(),
                T2Outcome::Applied
            );
        }

        let reopened = SingleNodeStore::open(config.clone()).unwrap();
        let snapshot = reopened.open_snapshot(FileId::from_raw(20), None).unwrap();
        assert_eq!(snapshot.read_range(0..8).unwrap(), b"recovery");
        assert_eq!(
            reopened.query_operation(OperationId::from_raw(41)).unwrap(),
            OperationStatus::Succeeded
        );
        let commit_id = reopened
            .dag
            .lock()
            .unwrap()
            .operation_binding(OperationId::from_raw(41))
            .unwrap();
        drop(snapshot);
        drop(reopened);

        let reopened = SingleNodeStore::open(config.clone()).unwrap();
        let dag = reopened.dag.lock().unwrap();
        assert_eq!(
            dag.operation_binding(OperationId::from_raw(41)),
            None,
            "startup tombstones the published operation binding"
        );
        assert_eq!(
            dag.operation_tombstone(OperationId::from_raw(41)),
            Some(commit_id)
        );
        drop(dag);
        let snapshot = reopened.open_snapshot(FileId::from_raw(20), None).unwrap();
        assert_eq!(snapshot.read_range(0..8).unwrap(), b"recovery");
        cleanup(&catalog_path, &data_path);
    }

    #[test]
    fn reopen_recovers_a_preparing_operation_binding() {
        let (catalog_path, data_path) = test_paths();
        initialize_catalog(&catalog_path);
        FileDevice::create_preallocated(&data_path, 4 * 1024 * 1024).unwrap();
        let config = SingleNodeConfig::new(&catalog_path, &data_path, 1);

        let commit_id = {
            let store = SingleNodeStore::open(config.clone()).unwrap();
            let mut write = store
                .begin_write(
                    FileId::from_raw(20),
                    Head {
                        version_id: None,
                        generation: 0,
                    },
                    OperationId::from_raw(42),
                )
                .unwrap();
            write.write_at(0, b"aborted").unwrap();
            let mut dag = write.dag.lock().unwrap();
            let verified = dag
                .append_snapshot(&write.bytes, write.parent_commit)
                .unwrap();
            dag.bind_operation(write.operation_id, verified.commit_id)
                .unwrap();
            drop(dag);
            verified.commit_id
        };

        let reopened = SingleNodeStore::open(config.clone()).unwrap();
        assert_eq!(
            reopened.query_operation(OperationId::from_raw(42)).unwrap(),
            OperationStatus::Succeeded
        );
        assert_eq!(
            reopened
                .dag
                .lock()
                .unwrap()
                .operation_binding(OperationId::from_raw(42)),
            Some(commit_id)
        );
        drop(reopened);

        let reopened = SingleNodeStore::open(config).unwrap();
        let dag = reopened.dag.lock().unwrap();
        assert_eq!(dag.operation_binding(OperationId::from_raw(42)), None);
        assert_eq!(
            dag.operation_tombstone(OperationId::from_raw(42)),
            Some(commit_id)
        );
        drop(dag);
        cleanup(&catalog_path, &data_path);
    }

    #[test]
    fn startup_rejects_a_catalog_candidate_without_a_dag_binding() {
        let (catalog_path, data_path) = test_paths();
        initialize_catalog(&catalog_path);
        FileDevice::create_preallocated(&data_path, 4 * 1024 * 1024).unwrap();
        let config = SingleNodeConfig::new(&catalog_path, &data_path, 1);

        {
            let store = SingleNodeStore::open(config.clone()).unwrap();
            let write = store
                .begin_write(
                    FileId::from_raw(20),
                    Head {
                        version_id: None,
                        generation: 0,
                    },
                    OperationId::from_raw(43),
                )
                .unwrap();
            let mut catalog = write.catalog.lock().unwrap();
            assert_eq!(
                catalog
                    .t2_record_version(
                        43,
                        CoordinatorEpoch::new(1),
                        43,
                        &CatalogVersion {
                            id: 0,
                            file_id: 20,
                            generation: 1,
                            commit_id: [3; 32],
                            parent_version_id: None,
                            size: 1,
                            digest: [4; 32],
                        },
                    )
                    .unwrap(),
                T2Outcome::Applied
            );
        }

        assert!(matches!(
            SingleNodeStore::open(config),
            Err(SingleNodeError::DagUnavailable)
        ));
        cleanup(&catalog_path, &data_path);
    }

    #[test]
    fn bootstrap_management_and_reclaim_are_reopen_safe() {
        let (catalog_path, data_path) = test_paths();
        FileDevice::create_preallocated(&data_path, 4 * 1024 * 1024).unwrap();
        let config = SingleNodeConfig::new(&catalog_path, &data_path, 7);
        let ids = SingleNodeStore::bootstrap(&config, "docs", "readme").unwrap();
        assert_eq!(ids.file, FileId::from_raw(1));
        let store = SingleNodeStore::open(config.clone()).unwrap();
        assert_eq!(store.create_collection("archive").unwrap(), 2);
        assert_eq!(store.create_collection("archive").unwrap(), 2);
        let file = store.create_file(2, "old").unwrap();
        assert_eq!(store.create_file(2, "old").unwrap(), file);
        let report = store.reclaim().unwrap();
        assert_eq!(report.bytes_reclaimed, 0);
        drop(store);
        let reopened = SingleNodeStore::open(config).unwrap();
        assert_eq!(reopened.create_file(2, "old").unwrap(), file);
        cleanup(&catalog_path, &data_path);
    }

    #[test]
    fn commit_fault_points_are_recovered_on_reopen() {
        for (fault, operation_id, expected) in [
            (
                CommitFaultPoint::AfterDagAppend,
                51,
                OperationStatus::Failed,
            ),
            (
                CommitFaultPoint::AfterDagBind,
                52,
                OperationStatus::Succeeded,
            ),
            (
                CommitFaultPoint::AfterCatalogCandidate,
                53,
                OperationStatus::Succeeded,
            ),
        ] {
            let (catalog_path, data_path) = test_paths();
            initialize_catalog(&catalog_path);
            FileDevice::create_preallocated(&data_path, 4 * 1024 * 1024).unwrap();
            let config = SingleNodeConfig::new(&catalog_path, &data_path, 1);
            let store = SingleNodeStore::open(config.clone()).unwrap();
            let mut write = store
                .begin_write(
                    FileId::from_raw(20),
                    Head {
                        version_id: None,
                        generation: 0,
                    },
                    OperationId::from_raw(operation_id),
                )
                .unwrap();
            write.write_at(0, b"fault-window").unwrap();
            assert!(matches!(
                write.commit_with_fault(fault),
                Err(SingleNodeError::Unavailable)
            ));
            drop(write);
            drop(store);
            let reopened = SingleNodeStore::open(config).unwrap();
            assert_eq!(
                reopened
                    .query_operation(OperationId::from_raw(operation_id))
                    .unwrap(),
                expected
            );
            cleanup(&catalog_path, &data_path);
        }
    }

    #[test]
    fn cloned_store_serializes_concurrent_metadata_creation() {
        let (catalog_path, data_path) = test_paths();
        FileDevice::create_preallocated(&data_path, 4 * 1024 * 1024).unwrap();
        let config = SingleNodeConfig::new(&catalog_path, &data_path, 9);
        SingleNodeStore::bootstrap(&config, "docs", "readme").unwrap();
        let store = SingleNodeStore::open(config.clone()).unwrap();
        let left = store.clone();
        let right = store.clone();
        let a = std::thread::spawn(move || left.create_collection("parallel").unwrap());
        let b = std::thread::spawn(move || right.create_collection("parallel").unwrap());
        assert_eq!(a.join().unwrap(), b.join().unwrap());
        cleanup(&catalog_path, &data_path);
    }
}
