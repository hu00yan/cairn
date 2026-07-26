//! The external seam for a single-node Cairn store.
//!
//! This crate intentionally contains only the coordinator-facing shape. A
//! production implementation will keep the catalog and DAG as two durable
//! stores behind the coordinator; neither store's implementation type is part
//! of this interface.

#[cfg(any())]
mod legacy {

    use cairn_model::{FileId, Head, OperationId, OperationRecord, VersionId};
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

use cairn_device::{FileDagStore, FileDagStoreError, FileDevice};
use cairn_model::sqlite_store::{
    CatalogOperationState, EpochClaim, ReadAuthorization, SqliteCatalogStore,
};
use cairn_model::{FileId, Head, OperationId, VersionId};
use std::{
    error::Error,
    fmt,
    ops::Range,
    path::PathBuf,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationStatus {
    Unknown,
    InProgress,
    Succeeded,
    Failed,
}

pub struct SingleNodeStore {
    catalog: Arc<Mutex<SqliteCatalogStore>>,
    dag: Arc<Mutex<FileDagStore<FileDevice>>>,
    actor_id: u64,
    coordinator_epoch: u64,
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

#[derive(Debug)]
pub struct WriteHandle;

impl SingleNodeStore {
    pub fn open(config: SingleNodeConfig) -> Result<Self, SingleNodeError> {
        let device =
            FileDevice::open(config.data_path).map_err(|_| SingleNodeError::DeviceUnavailable)?;
        let dag = FileDagStore::open(device).map_err(map_dag)?;
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
                cairn_model::sqlite_store::CoordinatorEpoch::new(current_epoch),
                cairn_model::sqlite_store::CoordinatorEpoch::new(next_epoch),
            )
            .map_err(|_| SingleNodeError::CatalogUnavailable)?
        {
            EpochClaim::Claimed(epoch) => epoch.get(),
            EpochClaim::Stale { .. } => return Err(SingleNodeError::CatalogUnavailable),
        };
        Ok(Self {
            catalog: Arc::new(Mutex::new(catalog)),
            dag: Arc::new(Mutex::new(dag)),
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

    pub fn begin_write(
        &self,
        _file: FileId,
        _expected_head: Head,
        _operation_id: OperationId,
    ) -> Result<WriteHandle, SingleNodeError> {
        Err(SingleNodeError::UnsupportedWrite)
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
