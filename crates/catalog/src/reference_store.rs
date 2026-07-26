//! In-memory reference implementation of the catalog and write contract.

use crate::catalog::{
    CatalogError, CollectionId, FileId, FileVersion, Head, ModelCatalog, OperationId,
    OperationKind, OperationRecord, OperationResult, PrincipalId, VersionId,
};
use crate::dag::{
    CommitNode, Dag, DagError, Node, NodeId, SnapshotNode, MAX_CONTENT_NODE_PAYLOAD,
    MAX_LOGICAL_FILE_SIZE,
};
use std::{
    ops::Range,
    sync::{Arc, RwLock},
};

pub const MAX_RANGE_WRITE_BYTES: usize = MAX_CONTENT_NODE_PAYLOAD;
const MAX_PATCH_BYTES_IN_FLIGHT: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct StoreConfig {
    catalog: ModelCatalog,
    dag: Dag,
    actor: PrincipalId,
}

impl StoreConfig {
    pub fn new(catalog: ModelCatalog, dag: Dag, actor: PrincipalId) -> Self {
        Self {
            catalog,
            dag,
            actor,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeError {
    Catalog(CatalogError),
    DagFailure(&'static str),
    InvalidRange,
    OutputTooSmall,
    WriteTooLarge,
    CannotExtend,
    TransactionClosed,
    SnapshotNotFound,
    OverlappingWrite,
    TooManyWrites,
    TooManyPatchBytes,
}

impl From<CatalogError> for NativeError {
    fn from(value: CatalogError) -> Self {
        Self::Catalog(value)
    }
}

impl From<DagError> for NativeError {
    fn from(value: DagError) -> Self {
        let message = match value {
            DagError::InvalidKind(_) => "invalid DAG node kind",
            DagError::InvalidPayload(_) => "invalid DAG payload",
            DagError::InvalidReference(_) => "invalid DAG reference",
            DagError::ReferenceKind { .. } => "invalid DAG reference kind",
            DagError::ArithmeticOverflow => "DAG arithmetic overflow",
            DagError::MissingNode(_) => "missing DAG node",
            DagError::HashMismatch(_) => "DAG hash mismatch",
            DagError::InvalidSnapshot(_) => "invalid DAG snapshot",
            DagError::ResourceLimit(_) => "DAG resource limit",
            DagError::OperationConflict(_) => "DAG operation conflict",
        };
        Self::DagFailure(message)
    }
}

#[derive(Debug)]
pub struct Store {
    catalog: Arc<RwLock<ModelCatalog>>,
    dag: Arc<RwLock<Dag>>,
    actor: PrincipalId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Version {
    pub id: VersionId,
    pub file: FileId,
    pub generation: u64,
    pub parent_version_id: Option<VersionId>,
    pub size: u64,
}

impl From<FileVersion> for Version {
    fn from(value: FileVersion) -> Self {
        Self {
            id: value.id,
            file: value.file,
            generation: value.generation,
            parent_version_id: value.parent_version_id,
            size: value.size,
        }
    }
}

impl Store {
    pub(crate) fn open(config: StoreConfig) -> Result<Self, NativeError> {
        if config.catalog.principal(config.actor).is_none() {
            return Err(NativeError::Catalog(CatalogError::NotFound));
        }
        let mut catalog = config.catalog;
        let mut dag = config.dag;
        catalog.reconcile_startup(&mut dag)?;
        Ok(Self {
            catalog: Arc::new(RwLock::new(catalog)),
            dag: Arc::new(RwLock::new(dag)),
            actor: config.actor,
        })
    }

    pub fn open_default() -> Result<Self, NativeError> {
        let mut catalog = ModelCatalog::new();
        let actor = catalog.create_principal(crate::catalog::PrincipalKind::User)?;
        Self::open(StoreConfig::new(catalog, Dag::new(), actor))
    }

    pub fn create_collection(
        &mut self,
        name: impl Into<String>,
        operation_id: OperationId,
    ) -> Result<CollectionId, NativeError> {
        self.catalog
            .write()
            .map_err(|_| NativeError::DagFailure("catalog lock poisoned"))?
            .create_collection(self.actor, self.actor, name, operation_id)
            .map_err(Into::into)
    }

    pub fn create_file(
        &mut self,
        collection: CollectionId,
        name: impl Into<String>,
        operation_id: OperationId,
    ) -> Result<FileId, NativeError> {
        self.catalog
            .write()
            .map_err(|_| NativeError::DagFailure("catalog lock poisoned"))?
            .create_file(self.actor, collection, name, operation_id)
            .map_err(Into::into)
    }

    pub fn allocate_operation_id(&mut self) -> Result<OperationId, NativeError> {
        self.catalog
            .write()
            .map_err(|_| NativeError::DagFailure("catalog lock poisoned"))?
            .allocate_operation_id()
            .map_err(Into::into)
    }

    pub fn head(&self, file: FileId) -> Result<Head, NativeError> {
        self.catalog
            .read()
            .map_err(|_| NativeError::DagFailure("catalog lock poisoned"))?
            .head(self.actor, file)
            .map_err(Into::into)
    }

    pub fn open_snapshot(
        &self,
        file: FileId,
        version: Option<VersionId>,
    ) -> Result<SnapshotHandle, NativeError> {
        let mut catalog = self
            .catalog
            .write()
            .map_err(|_| NativeError::DagFailure("catalog lock poisoned"))?;
        let versions = catalog.list_versions(self.actor, file)?;
        let selected = match version {
            Some(id) => versions
                .iter()
                .find(|candidate| candidate.id == id)
                .cloned()
                .ok_or(NativeError::SnapshotNotFound)?,
            None => {
                let head = catalog.head(self.actor, file)?;
                match head.version_id {
                    Some(id) => versions
                        .iter()
                        .find(|candidate| candidate.id == id)
                        .cloned()
                        .ok_or(NativeError::SnapshotNotFound)?,
                    None => {
                        return Ok(SnapshotHandle {
                            dag: Arc::clone(&self.dag),
                            lease: Some(Arc::new(ReaderLease {
                                catalog: Arc::clone(&self.catalog),
                                commit: None,
                            })),
                            actor: self.actor,
                            file: Some(file),
                            version: None,
                            commit: None,
                            snapshot: None,
                            len: 0,
                        })
                    }
                }
            }
        };
        catalog.pin_reader(self.actor, file, selected.id, selected.commit_id)?;
        drop(catalog);
        let (snapshot, len) = match self.snapshot_ref(selected.commit_id) {
            Ok(value) => value,
            Err(error) => {
                if let Ok(mut catalog) = self.catalog.write() {
                    catalog.unpin_reader(selected.commit_id);
                }
                return Err(error);
            }
        };
        Ok(SnapshotHandle {
            dag: Arc::clone(&self.dag),
            lease: Some(Arc::new(ReaderLease {
                catalog: Arc::clone(&self.catalog),
                commit: Some(selected.commit_id),
            })),
            actor: self.actor,
            file: Some(file),
            version: Some(selected.id),
            commit: Some(selected.commit_id),
            snapshot: Some(snapshot),
            len,
        })
    }

    pub fn begin_write(
        &mut self,
        file: FileId,
        expected_head: Head,
        operation_id: OperationId,
    ) -> Result<WriteTxn, NativeError> {
        let base_snapshot = self.open_snapshot(file, expected_head.version_id)?;
        let intent = self
            .catalog
            .write()
            .map_err(|_| NativeError::DagFailure("catalog lock poisoned"))?
            .begin_publish(self.actor, file, operation_id, expected_head, None)?;
        let fence = self
            .catalog
            .read()
            .map_err(|_| NativeError::DagFailure("catalog lock poisoned"))?
            .claim_token(self.actor, intent.operation_id)?;
        Ok(WriteTxn {
            file,
            expected_head,
            operation_id,
            base_snapshot: base_snapshot.snapshot,
            base_len: base_snapshot.len,
            logical_size: base_snapshot.len,
            patches: Vec::new(),
            fence,
            store: self,
            state: TxnState::Open,
        })
    }

    pub fn query_operation(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<OperationView>, NativeError> {
        Ok(self
            .catalog
            .read()
            .map_err(|_| NativeError::DagFailure("catalog lock poisoned"))?
            .query_operation(self.actor, operation_id)?
            .map(OperationView::from))
    }

    /// Opens an immutable view without exposing the backing DAG snapshot.
    pub fn open_file_view(
        &self,
        file: FileId,
        version: Option<VersionId>,
    ) -> Result<SnapshotHandle, NativeError> {
        self.open_snapshot(file, version)
    }

    /// Returns only a terminal publish outcome. A pending operation is `None`.
    pub fn operation_terminal(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<OperationTerminal>, NativeError> {
        let Some(operation) = self.query_operation(operation_id)? else {
            return Ok(None);
        };
        if let Some(error) = operation.error {
            return Ok(Some(OperationTerminal::Aborted { error }));
        }
        match operation.result {
            Some(OperationViewResult::Version {
                id,
                generation,
                size,
                parent_version_id,
            }) => Ok(Some(OperationTerminal::Committed(Version {
                id,
                file: operation.file.ok_or(NativeError::SnapshotNotFound)?,
                generation,
                parent_version_id,
                size,
            }))),
            Some(OperationViewResult::Aborted) => Ok(Some(OperationTerminal::Aborted {
                error: CatalogError::Aborted,
            })),
            _ => Ok(None),
        }
    }

    fn snapshot_ref(&self, commit_id: NodeId) -> Result<(NodeId, u64), NativeError> {
        let dag = self
            .dag
            .read()
            .map_err(|_| NativeError::DagFailure("DAG lock poisoned"))?;
        let Node::Commit(commit) = dag
            .get(&commit_id)
            .ok_or(DagError::MissingNode(commit_id))?
        else {
            return Err(DagError::InvalidReference(commit_id).into());
        };
        let Node::Snapshot(snapshot) = dag
            .get(&commit.snapshot)
            .ok_or(DagError::MissingNode(commit.snapshot))?
        else {
            return Err(DagError::InvalidReference(commit.snapshot).into());
        };
        Ok((commit.snapshot, snapshot.logical_size))
    }
}

#[derive(Clone, Debug)]
pub struct SnapshotHandle {
    dag: Arc<RwLock<Dag>>,
    lease: Option<Arc<ReaderLease>>,
    actor: PrincipalId,
    file: Option<FileId>,
    version: Option<VersionId>,
    commit: Option<NodeId>,
    snapshot: Option<NodeId>,
    len: u64,
}

#[derive(Debug)]
struct ReaderLease {
    catalog: Arc<RwLock<ModelCatalog>>,
    commit: Option<NodeId>,
}

impl Drop for ReaderLease {
    fn drop(&mut self) {
        if let Ok(mut catalog) = self.catalog.write() {
            if let Some(commit) = self.commit {
                catalog.unpin_reader(commit);
            }
        }
    }
}

impl SnapshotHandle {
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn read_range(&self, range: Range<u64>, output: &mut [u8]) -> Result<usize, NativeError> {
        if let Some(lease) = &self.lease {
            let catalog = lease
                .catalog
                .read()
                .map_err(|_| NativeError::DagFailure("catalog lock poisoned"))?;
            catalog.validate_reader(
                self.actor,
                self.file.ok_or(NativeError::SnapshotNotFound)?,
                self.version,
                lease.commit,
            )?;
        }
        if range.start > range.end || range.end > self.len {
            return Err(NativeError::InvalidRange);
        }
        let length =
            usize::try_from(range.end - range.start).map_err(|_| NativeError::InvalidRange)?;
        if output.len() < length {
            return Err(NativeError::OutputTooSmall);
        }
        match self.snapshot {
            Some(snapshot) => {
                let dag = self
                    .dag
                    .read()
                    .map_err(|_| NativeError::DagFailure("DAG lock poisoned"))?;
                let Node::Commit(commit) = dag
                    .get(&self.commit.ok_or(NativeError::SnapshotNotFound)?)
                    .ok_or(NativeError::SnapshotNotFound)?
                else {
                    return Err(NativeError::SnapshotNotFound);
                };
                if commit.snapshot != snapshot {
                    return Err(NativeError::SnapshotNotFound);
                }
                dag.reconstruct_range(snapshot, range, output)
                    .map_err(Into::into)
            }
            None => Ok(0),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationView {
    pub operation_id: OperationId,
    pub kind: OperationKind,
    file: Option<FileId>,
    parent_version_id: Option<VersionId>,
    pub result: Option<OperationViewResult>,
    pub error: Option<CatalogError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationViewResult {
    File(FileId),
    Version {
        id: VersionId,
        generation: u64,
        size: u64,
        parent_version_id: Option<VersionId>,
    },
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationTerminal {
    Committed(Version),
    Aborted { error: CatalogError },
}

impl From<&OperationRecord> for OperationView {
    fn from(record: &OperationRecord) -> Self {
        Self {
            operation_id: record.operation_id,
            kind: record.kind,
            file: match &record.result {
                Some(OperationResult::Version(version)) => Some(version.file),
                _ => None,
            },
            parent_version_id: match &record.result {
                Some(OperationResult::Version(version)) => version.parent_version_id,
                _ => None,
            },
            result: record.result.as_ref().and_then(|result| match result {
                OperationResult::File(id) => Some(OperationViewResult::File(*id)),
                OperationResult::Version(version) => Some(OperationViewResult::Version {
                    id: version.id,
                    generation: version.generation,
                    size: version.size,
                    parent_version_id: version.parent_version_id,
                }),
                OperationResult::Aborted => Some(OperationViewResult::Aborted),
                OperationResult::Collection(_) => None,
            }),
            error: record.error.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TxnState {
    Open,
    Committed,
    Aborted,
}

pub struct WriteTxn<'a> {
    file: FileId,
    expected_head: Head,
    operation_id: OperationId,
    base_snapshot: Option<NodeId>,
    base_len: u64,
    logical_size: u64,
    patches: Vec<RangePatch>,
    fence: crate::catalog::FenceToken,
    store: &'a mut Store,
    state: TxnState,
}

const MAX_PENDING_RANGES: usize = 1024;

struct RangePatch {
    range: Range<u64>,
    bytes: Vec<u8>,
}

impl WriteTxn<'_> {
    pub fn write_range(&mut self, range: Range<u64>, bytes: &[u8]) -> Result<(), NativeError> {
        self.ensure_open()?;
        if range.start > range.end {
            return Err(NativeError::InvalidRange);
        }
        let length =
            usize::try_from(range.end - range.start).map_err(|_| NativeError::InvalidRange)?;
        if length != bytes.len() {
            return Err(NativeError::InvalidRange);
        }
        if length == 0 {
            return Ok(());
        }
        if length > MAX_RANGE_WRITE_BYTES {
            return Err(NativeError::WriteTooLarge);
        }
        let patch_bytes = self
            .patches
            .iter()
            .try_fold(0usize, |total, patch| total.checked_add(patch.bytes.len()))
            .ok_or(NativeError::TooManyPatchBytes)?;
        if patch_bytes
            .checked_add(length)
            .is_none_or(|total| total > MAX_PATCH_BYTES_IN_FLIGHT)
        {
            return Err(NativeError::TooManyPatchBytes);
        }
        if self.patches.len() == MAX_PENDING_RANGES {
            return Err(NativeError::TooManyWrites);
        }
        let new_size = range.end.max(self.logical_size);
        if new_size > MAX_LOGICAL_FILE_SIZE {
            return Err(NativeError::WriteTooLarge);
        }
        if self
            .patches
            .iter()
            .any(|patch| patch.range.start < range.end && range.start < patch.range.end)
        {
            return Err(NativeError::OverlappingWrite);
        }
        let position = self
            .patches
            .binary_search_by_key(&range.start, |patch| patch.range.start)
            .unwrap_or_else(|position| position);
        self.patches.insert(
            position,
            RangePatch {
                range,
                bytes: bytes.to_vec(),
            },
        );
        self.logical_size = new_size;
        Ok(())
    }

    pub fn truncate(&mut self, new_size: u64) -> Result<(), NativeError> {
        self.ensure_open()?;
        if new_size > self.logical_size {
            return Err(NativeError::CannotExtend);
        }
        self.logical_size = new_size;
        for patch in &mut self.patches {
            if patch.range.start >= new_size {
                patch.range = new_size..new_size;
                patch.bytes.clear();
            } else if patch.range.end > new_size {
                patch.range.end = new_size;
                patch
                    .bytes
                    .truncate((new_size - patch.range.start) as usize);
            }
        }
        self.patches.retain(|patch| !patch.range.is_empty());
        Ok(())
    }

    pub fn commit(mut self) -> Result<Version, NativeError> {
        self.ensure_open()?;
        let parent = self.expected_head.version_id.and_then(|id| {
            self.store
                .catalog
                .read()
                .ok()
                .and_then(|catalog| catalog.list_versions(self.store.actor, self.file).ok())
                .and_then(|versions| versions.into_iter().find(|version| version.id == id))
                .map(|version| version.commit_id)
        });
        let mut dag = self
            .store
            .dag
            .write()
            .map_err(|_| NativeError::DagFailure("DAG lock poisoned"))?;
        let patch_refs = self
            .patches
            .iter()
            .map(|patch| (patch.range.clone(), patch.bytes.as_slice()))
            .collect::<Vec<_>>();
        let commit = build_commit(
            &mut dag,
            self.base_snapshot,
            self.base_len,
            self.logical_size,
            &patch_refs,
            parent,
        )?;
        let handoff = dag.commit_operation(self.operation_id, commit)?;
        drop(dag);
        {
            let dag = self
                .store
                .dag
                .read()
                .map_err(|_| NativeError::DagFailure("DAG lock poisoned"))?;
            if !dag.receipt_is_active(&handoff.receipt) {
                return Err(NativeError::Catalog(CatalogError::ReceiptNotDurable));
            }
        }
        {
            let mut catalog = self
                .store
                .catalog
                .write()
                .map_err(|_| NativeError::DagFailure("catalog lock poisoned"))?;
            catalog.register_verified_receipt(
                self.store.actor,
                self.operation_id,
                self.fence,
                handoff.clone(),
            )?;
            catalog.bind_candidate(self.store.actor, self.operation_id, self.fence)?;
        }
        let outcome = self
            .store
            .catalog
            .write()
            .map_err(|_| NativeError::DagFailure("catalog lock poisoned"))?
            .publish(self.store.actor, self.operation_id, Some(self.fence));
        // The catalog terminal result is authoritative. An active DAG
        // binding is retryable cleanup state and must not turn a successful
        // publish into an error.
        let permit = self
            .store
            .catalog
            .read()
            .ok()
            .and_then(|catalog| catalog.terminal_permit(self.operation_id).ok());
        if let Some(permit) = permit {
            let mut dag = self
                .store
                .dag
                .write()
                .map_err(|_| NativeError::DagFailure("DAG lock poisoned"))?;
            let _ = dag.tombstone_operation(self.operation_id, handoff, permit);
        }
        match outcome {
            Ok(version) => {
                self.state = TxnState::Committed;
                Ok(version.into())
            }
            Err(error) => {
                self.state = TxnState::Aborted;
                Err(error.into())
            }
        }
    }

    pub fn abort(mut self) -> Result<(), NativeError> {
        self.ensure_open()?;
        self.store
            .catalog
            .write()
            .map_err(|_| NativeError::DagFailure("catalog lock poisoned"))?
            .abort(self.operation_id, self.store.actor, Some(self.fence))?;
        self.state = TxnState::Aborted;
        Ok(())
    }

    fn ensure_open(&self) -> Result<(), NativeError> {
        (self.state == TxnState::Open)
            .then_some(())
            .ok_or(NativeError::TransactionClosed)
    }
}

fn build_commit(
    dag: &mut Dag,
    base_snapshot: Option<NodeId>,
    base_len: u64,
    logical_size: u64,
    patches: &[(Range<u64>, &[u8])],
    parent: Option<NodeId>,
) -> Result<NodeId, NativeError> {
    let root = dag.patch_snapshot(base_snapshot, base_len, logical_size, patches)?;
    let snapshot = dag.insert(Node::Snapshot(SnapshotNode {
        logical_size,
        range_map_root: root,
        content_digest: dag.digest_range_map(root, logical_size)?,
    }))?;
    dag.insert(Node::Commit(CommitNode { snapshot, parent }))
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reopen_preserves_a_normal_abort_without_a_dag_handoff() {
        let mut store = Store::open_default().unwrap();
        let collection_op = store.allocate_operation_id().unwrap();
        let collection = store.create_collection("docs", collection_op).unwrap();
        let file_op = store.allocate_operation_id().unwrap();
        let file = store.create_file(collection, "abort", file_op).unwrap();
        let operation_id = store.allocate_operation_id().unwrap();
        let txn = store
            .begin_write(file, store.head(file).unwrap(), operation_id)
            .unwrap();
        txn.abort().unwrap();

        let reopened = Store::open(StoreConfig::new(
            store.catalog.read().unwrap().clone(),
            store.dag.read().unwrap().clone(),
            store.actor,
        ))
        .unwrap();
        assert_eq!(
            reopened.operation_terminal(operation_id),
            Ok(Some(OperationTerminal::Aborted {
                error: CatalogError::Aborted,
            }))
        );
    }
}
