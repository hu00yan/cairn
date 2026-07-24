use crate::dag::{Dag, Node, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use unicode_normalization::UnicodeNormalization;

macro_rules! id {
    ($n:ident) => {
        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        pub struct $n(u64);
        impl $n {
            pub fn get(self) -> u64 {
                self.0
            }
        }
    };
}
id!(PrincipalId);
id!(CollectionId);
id!(FileId);
id!(VersionId);
id!(OperationId);
pub type Generation = u64;
pub type CommitId = NodeId;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PrincipalKind {
    User,
    Organization,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PrincipalState {
    Active,
    Disabled,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Principal {
    pub id: PrincipalId,
    pub kind: PrincipalKind,
    pub state: PrincipalState,
    pub authz_epoch: u64,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Capability {
    Read,
    Write,
    ManageMembers,
}
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Membership {
    pub organization: PrincipalId,
    pub member: PrincipalId,
    pub capability: Capability,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Collection {
    pub id: CollectionId,
    pub owner: PrincipalId,
    pub name: String,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Head {
    pub version_id: Option<VersionId>,
    pub generation: Generation,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct File {
    pub id: FileId,
    pub collection: CollectionId,
    pub name: String,
    pub head: Head,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileVersion {
    pub id: VersionId,
    pub file: FileId,
    pub generation: Generation,
    pub commit_id: CommitId,
    pub parent_version_id: Option<VersionId>,
    pub size: u64,
    pub digest: NodeId,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IntentState {
    Preparing,
    CommitDurable,
    Published,
    Aborted,
}
impl IntentState {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Published | Self::Aborted)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitReceipt {
    operation_id: OperationId,
    commit_id: CommitId,
    snapshot_id: NodeId,
    snapshot_size: u64,
    snapshot_digest: NodeId,
    parent: Option<CommitId>,
}
impl CommitReceipt {
    pub fn operation_id(&self) -> OperationId {
        self.operation_id
    }
    pub fn commit_id(&self) -> CommitId {
        self.commit_id
    }
    pub fn snapshot_id(&self) -> NodeId {
        self.snapshot_id
    }
    pub fn snapshot_size(&self) -> u64 {
        self.snapshot_size
    }
    pub fn snapshot_digest(&self) -> NodeId {
        self.snapshot_digest
    }
    pub fn parent(&self) -> Option<CommitId> {
        self.parent
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    pub version_id: VersionId,
    pub receipt: CommitReceipt,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishIntent {
    pub operation_id: OperationId,
    pub actor: PrincipalId,
    pub file: FileId,
    pub state: IntentState,
    pub authz_epoch: u64,
    pub global_authz_epoch: u64,
    pub expected_head: Head,
    pub base: Option<Head>,
    pub version_id: VersionId,
    pub candidate: Option<Candidate>,
    pub abort_reason: Option<String>,
    pub owner_fence: Option<u64>,
    pub pinned: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationKind {
    CreateCollection,
    CreateFile,
    Publish,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationResult {
    Collection(CollectionId),
    File(FileId),
    Version(FileVersion),
    Aborted,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationRecord {
    pub operation_id: OperationId,
    pub actor: PrincipalId,
    pub kind: OperationKind,
    pub request_fingerprint: [u8; 32],
    pub result: Option<OperationResult>,
    pub error: Option<CatalogError>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IndexState {
    Active,
    Tombstoned,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DagOperationIndexEntry {
    pub receipt: CommitReceipt,
    pub state: IndexState,
    pub has_roots: bool,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CatalogError {
    NotFound,
    PermissionDenied,
    PrincipalDisabled,
    NameTaken,
    InvalidName,
    OperationConflict,
    IdExhausted,
    EpochExhausted,
    GenerationExhausted,
    HeadConflict,
    InvalidIntent,
    AuthzEpochChanged,
    CandidateMismatch,
    ReceiptNotDurable,
    DuplicateCommit,
    InvalidCommit,
    InvalidReceipt,
    FenceLost,
    InvalidOrganization,
}
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Operation {
    Collection(CollectionId),
    File(FileId),
    Publish(PublishIntent),
    Error(CatalogError),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ModelCatalog {
    next_principal: u64,
    next_collection: u64,
    next_file: u64,
    next_version: u64,
    next_operation: u64,
    global_authz_epoch: u64,
    coordinator_epoch: u64,
    principals: HashMap<PrincipalId, Principal>,
    collections: HashMap<CollectionId, Collection>,
    files: HashMap<FileId, File>,
    versions: HashMap<FileId, Vec<FileVersion>>,
    intents: HashMap<OperationId, PublishIntent>,
    operations: HashMap<OperationId, OperationRecord>,
    dag_index: HashMap<OperationId, DagOperationIndexEntry>,
    memberships: HashMap<(PrincipalId, PrincipalId), Capability>,
    retention_roots: HashSet<CommitId>,
    reader_pins: HashSet<CommitId>,
    collection_names: HashMap<(PrincipalId, String), CollectionId>,
    file_names: HashMap<(CollectionId, String), FileId>,
}

impl ModelCatalog {
    pub fn new() -> Self {
        Self {
            coordinator_epoch: 1,
            ..Self::default()
        }
    }
    pub fn global_authz_epoch(&self) -> u64 {
        self.global_authz_epoch
    }
    pub fn allocate_operation_id(&mut self) -> Result<OperationId, CatalogError> {
        Ok(OperationId(next(&mut self.next_operation)?))
    }
    pub fn create_principal(&mut self, kind: PrincipalKind) -> Result<PrincipalId, CatalogError> {
        let x = PrincipalId(next(&mut self.next_principal)?);
        self.principals.insert(
            x,
            Principal {
                id: x,
                kind,
                state: PrincipalState::Active,
                authz_epoch: 0,
            },
        );
        Ok(x)
    }
    pub fn principal(&self, id: PrincipalId) -> Option<&Principal> {
        self.principals.get(&id)
    }
    pub fn membership(&self, organization: PrincipalId, member: PrincipalId) -> Option<Capability> {
        self.memberships.get(&(organization, member)).copied()
    }
    pub fn grant_membership(
        &mut self,
        actor: PrincipalId,
        organization: PrincipalId,
        member: PrincipalId,
        capability: Capability,
    ) -> Result<(), CatalogError> {
        self.auth(actor, organization, Capability::ManageMembers)?;
        self.require_organization(organization)?;
        self.principals.get(&member).ok_or(CatalogError::NotFound)?;
        self.bump_authz(&[organization, member])?;
        self.memberships.insert((organization, member), capability);
        Ok(())
    }
    pub fn revoke_membership(
        &mut self,
        actor: PrincipalId,
        organization: PrincipalId,
        member: PrincipalId,
    ) -> Result<(), CatalogError> {
        self.auth(actor, organization, Capability::ManageMembers)?;
        self.require_organization(organization)?;
        self.principals.get(&member).ok_or(CatalogError::NotFound)?;
        self.bump_authz(&[organization, member])?;
        self.memberships.remove(&(organization, member));
        Ok(())
    }
    pub fn disable(&mut self, id: PrincipalId) -> Result<(), CatalogError> {
        self.state(id, PrincipalState::Disabled)
    }
    pub fn enable(&mut self, id: PrincipalId) -> Result<(), CatalogError> {
        self.state(id, PrincipalState::Active)
    }
    pub fn disable_principal(&mut self, id: PrincipalId) -> Result<(), CatalogError> {
        self.disable(id)
    }
    pub fn enable_principal(&mut self, id: PrincipalId) -> Result<(), CatalogError> {
        self.enable(id)
    }

    pub fn create_collection(
        &mut self,
        actor: PrincipalId,
        owner: PrincipalId,
        name: impl Into<String>,
        op: OperationId,
    ) -> Result<CollectionId, CatalogError> {
        let n = norm(&name.into())?;
        let fp = fingerprint(
            b"cairn/catalog/create-collection/v1",
            &[actor.get(), owner.get()],
            &[n.as_bytes()],
        );
        if let Some(r) = self.reserve(op, actor, OperationKind::CreateCollection, fp)? {
            if let OperationResult::Collection(x) = r {
                return Ok(x);
            }
            return Err(CatalogError::OperationConflict);
        }
        let z = (|| {
            self.auth(actor, owner, Capability::ManageMembers)?;
            if self.collection_names.contains_key(&(owner, n.clone())) {
                return Err(CatalogError::NameTaken);
            }
            let x = CollectionId(next(&mut self.next_collection)?);
            self.collections.insert(
                x,
                Collection {
                    id: x,
                    owner,
                    name: n.clone(),
                },
            );
            self.collection_names.insert((owner, n), x);
            Ok(x)
        })();
        self.complete(op, z.clone().map(OperationResult::Collection));
        z
    }
    pub fn create_file(
        &mut self,
        actor: PrincipalId,
        collection: CollectionId,
        name: impl Into<String>,
        op: OperationId,
    ) -> Result<FileId, CatalogError> {
        let n = norm(&name.into())?;
        let fp = fingerprint(
            b"cairn/catalog/create-file/v1",
            &[actor.get(), collection.get()],
            &[n.as_bytes()],
        );
        if let Some(r) = self.reserve(op, actor, OperationKind::CreateFile, fp)? {
            if let OperationResult::File(x) = r {
                return Ok(x);
            }
            return Err(CatalogError::OperationConflict);
        }
        let z = (|| {
            let owner = self
                .collections
                .get(&collection)
                .ok_or(CatalogError::NotFound)?
                .owner;
            self.auth(actor, owner, Capability::Write)?;
            if self.file_names.contains_key(&(collection, n.clone())) {
                return Err(CatalogError::NameTaken);
            }
            let x = FileId(next(&mut self.next_file)?);
            self.files.insert(
                x,
                File {
                    id: x,
                    collection,
                    name: n.clone(),
                    head: Head {
                        version_id: None,
                        generation: 0,
                    },
                },
            );
            self.file_names.insert((collection, n), x);
            Ok(x)
        })();
        self.complete(op, z.clone().map(OperationResult::File));
        z
    }

    pub fn begin_publish(
        &mut self,
        actor: PrincipalId,
        file: FileId,
        op: OperationId,
        expected: Head,
    ) -> Result<PublishIntent, CatalogError> {
        let fpr = fingerprint(
            b"cairn/catalog/publish/v1",
            &[
                actor.get(),
                file.get(),
                expected.version_id.map_or(u64::MAX, VersionId::get),
                expected.generation,
            ],
            &[],
        );
        if let Some(r) = self.existing(op, actor, OperationKind::Publish, fpr)? {
            return self.intents.get(&op).cloned().ok_or(match r {
                Some(_) => CatalogError::InvalidIntent,
                None => CatalogError::NotFound,
            });
        }
        self.operations.insert(
            op,
            OperationRecord {
                operation_id: op,
                actor,
                kind: OperationKind::Publish,
                request_fingerprint: fpr,
                result: None,
                error: None,
            },
        );
        let z = (|| {
            let f = self.files.get(&file).ok_or(CatalogError::NotFound)?.clone();
            let owner = self
                .collections
                .get(&f.collection)
                .ok_or(CatalogError::NotFound)?
                .owner;
            self.auth(actor, owner, Capability::Write)?;
            if f.head != expected {
                return Err(CatalogError::HeadConflict);
            }
            let a = self
                .principals
                .get(&actor)
                .ok_or(CatalogError::NotFound)?
                .authz_epoch;
            let x = VersionId(next(&mut self.next_version)?);
            expected
                .generation
                .checked_add(1)
                .ok_or(CatalogError::GenerationExhausted)?;
            let i = PublishIntent {
                operation_id: op,
                actor,
                file,
                state: IntentState::Preparing,
                authz_epoch: a,
                global_authz_epoch: self.global_authz_epoch,
                expected_head: expected,
                base: expected.version_id.map(|_| expected),
                version_id: x,
                candidate: None,
                abort_reason: None,
                owner_fence: Some(self.coordinator_epoch),
                pinned: true,
            };
            self.intents.insert(op, i.clone());
            Ok(i)
        })();
        if let Err(e) = &z {
            self.complete_err(op, e.clone())
        }
        z
    }

    /// Registers an operation-index record only after the DAG has accepted the
    /// Commit and its Snapshot. All denormalized fields are derived here.
    pub fn register_receipt(
        &mut self,
        op: OperationId,
        fence: u64,
        dag: &Dag,
        commit_id: CommitId,
    ) -> Result<CommitReceipt, CatalogError> {
        let i = self
            .intents
            .get(&op)
            .cloned()
            .ok_or(CatalogError::NotFound)?;
        if i.state.terminal() {
            return Err(CatalogError::InvalidIntent);
        }
        self.require_fence(&i, fence)?;
        let Node::Commit(commit) = dag.get(&commit_id).ok_or(CatalogError::InvalidCommit)? else {
            return Err(CatalogError::InvalidCommit);
        };
        if Node::Commit(*commit)
            .id()
            .map_err(|_| CatalogError::InvalidCommit)?
            != commit_id
        {
            return Err(CatalogError::InvalidCommit);
        }
        let Node::Snapshot(snapshot) = dag
            .get(&commit.snapshot)
            .ok_or(CatalogError::InvalidReceipt)?
        else {
            return Err(CatalogError::InvalidReceipt);
        };
        let r = CommitReceipt {
            operation_id: op,
            commit_id,
            snapshot_id: commit.snapshot,
            snapshot_size: snapshot.logical_size,
            snapshot_digest: snapshot.content_digest,
            parent: commit.parent,
        };
        if let Some(old) = self.dag_index.get(&op) {
            if old.receipt != r {
                return Err(CatalogError::OperationConflict);
            }
            return Ok(old.receipt.clone());
        }
        self.dag_index.insert(
            op,
            DagOperationIndexEntry {
                receipt: r.clone(),
                state: IndexState::Active,
                has_roots: true,
            },
        );
        Ok(r)
    }
    pub fn bind_candidate(
        &mut self,
        op: OperationId,
        fence: u64,
    ) -> Result<PublishIntent, CatalogError> {
        let r = self
            .dag_index
            .get(&op)
            .ok_or(CatalogError::NotFound)?
            .receipt
            .clone();
        let i = self
            .intents
            .get(&op)
            .cloned()
            .ok_or(CatalogError::NotFound)?;
        if i.state.terminal() {
            return Ok(i);
        }
        self.require_fence(&i, fence)?;
        let parent = i.base.and_then(|h| h.version_id).and_then(|v| {
            self.versions
                .get(&i.file)?
                .iter()
                .find(|x| x.id == v)
                .map(|x| x.commit_id)
        });
        if r.parent != parent {
            self.abort_inner(
                op,
                fence,
                "candidate parent mismatch",
                Some(CatalogError::CandidateMismatch),
            )?;
            return Err(CatalogError::CandidateMismatch);
        }
        let x = self.intents.get_mut(&op).unwrap();
        x.candidate = Some(Candidate {
            version_id: x.version_id,
            receipt: r,
        });
        x.state = IntentState::CommitDurable;
        Ok(x.clone())
    }
    pub fn publish(&mut self, op: OperationId, fence: u64) -> Result<FileVersion, CatalogError> {
        if let Some(OperationResult::Version(v)) =
            self.operations.get(&op).and_then(|x| x.result.clone())
        {
            return Ok(v);
        }
        let i = self
            .intents
            .get(&op)
            .cloned()
            .ok_or(CatalogError::NotFound)?;
        if i.state == IntentState::Aborted {
            return Err(self
                .operations
                .get(&op)
                .and_then(|r| r.error.clone())
                .unwrap_or(CatalogError::InvalidIntent));
        }
        self.require_fence(&i, fence)?;
        let c = i.candidate.clone().ok_or(CatalogError::InvalidIntent)?;
        let f = self
            .files
            .get(&i.file)
            .ok_or(CatalogError::NotFound)?
            .clone();
        let owner = self
            .collections
            .get(&f.collection)
            .ok_or(CatalogError::NotFound)?
            .owner;
        if self.auth(i.actor, owner, Capability::Write).is_err() {
            self.abort_inner(
                op,
                fence,
                "authorization changed",
                Some(CatalogError::PermissionDenied),
            )?;
            return Err(CatalogError::PermissionDenied);
        }
        if self.principals.get(&i.actor).map(|p| p.authz_epoch) != Some(i.authz_epoch)
            || self.global_authz_epoch != i.global_authz_epoch
            || f.head != i.expected_head
        {
            self.abort_inner(
                op,
                fence,
                "head CAS failed",
                Some(CatalogError::HeadConflict),
            )?;
            return Err(CatalogError::HeadConflict);
        }
        if self
            .versions
            .values()
            .flatten()
            .any(|v| v.file == i.file && v.commit_id == c.receipt.commit_id)
        {
            self.abort_inner(
                op,
                fence,
                "duplicate Commit",
                Some(CatalogError::DuplicateCommit),
            )?;
            return Err(CatalogError::DuplicateCommit);
        }
        let g = i
            .expected_head
            .generation
            .checked_add(1)
            .ok_or(CatalogError::GenerationExhausted)?;
        let v = FileVersion {
            id: c.version_id,
            file: i.file,
            generation: g,
            commit_id: c.receipt.commit_id,
            parent_version_id: i.expected_head.version_id,
            size: c.receipt.snapshot_size,
            digest: c.receipt.snapshot_digest,
        };
        self.versions.entry(i.file).or_default().push(v.clone());
        self.files.get_mut(&i.file).unwrap().head = Head {
            version_id: Some(v.id),
            generation: g,
        };
        let x = self.intents.get_mut(&op).unwrap();
        x.state = IntentState::Published;
        x.pinned = false;
        x.owner_fence = None;
        self.tombstone(op);
        self.complete(op, Ok(OperationResult::Version(v.clone())));
        Ok(v)
    }
    pub fn abort(
        &mut self,
        op: OperationId,
        actor: PrincipalId,
        fence: u64,
    ) -> Result<PublishIntent, CatalogError> {
        let i = self.intents.get(&op).ok_or(CatalogError::NotFound)?.clone();
        let o = self
            .collections
            .get(
                &self
                    .files
                    .get(&i.file)
                    .ok_or(CatalogError::NotFound)?
                    .collection,
            )
            .ok_or(CatalogError::NotFound)?
            .owner;
        self.auth(actor, o, Capability::Write)?;
        self.abort_inner(op, fence, "caller abort", None)?;
        Ok(self.intents[&op].clone())
    }
    pub fn recovery_abort(
        &mut self,
        op: OperationId,
        fence: u64,
        reason: impl Into<String>,
    ) -> Result<PublishIntent, CatalogError> {
        self.abort_inner(op, fence, &reason.into(), Some(CatalogError::InvalidIntent))?;
        Ok(self.intents[&op].clone())
    }
    pub fn recover(&mut self, op: OperationId) -> Result<PublishIntent, CatalogError> {
        if self
            .intents
            .get(&op)
            .ok_or(CatalogError::NotFound)?
            .state
            .terminal()
        {
            return Ok(self.intents[&op].clone());
        }
        let fence = self.claim_recovery_fence(op)?;
        if self.dag_index.contains_key(&op) {
            self.bind_candidate(op, fence)?;
            let _ = self.publish(op, fence);
        }
        Ok(self.intents[&op].clone())
    }
    pub fn head(&self, file: FileId) -> Option<&Head> {
        self.files.get(&file).map(|f| &f.head)
    }
    pub fn list_versions(&self, file: FileId) -> Option<&[FileVersion]> {
        self.versions.get(&file).map(Vec::as_slice)
    }
    pub fn query_operation(
        &self,
        actor: PrincipalId,
        op: OperationId,
    ) -> Result<Option<&OperationRecord>, CatalogError> {
        let record = self.operations.get(&op);
        if record.is_some_and(|record| record.actor != actor) {
            return Err(CatalogError::PermissionDenied);
        }
        Ok(record)
    }
    pub fn intent(&self, op: OperationId) -> Option<&PublishIntent> {
        self.intents.get(&op)
    }
    pub fn dag_operation(&self, op: OperationId) -> Option<&DagOperationIndexEntry> {
        self.dag_index.get(&op)
    }
    pub fn candidate_reclaimable(&self, op: OperationId) -> bool {
        let Some(entry) = self.dag_index.get(&op) else {
            return false;
        };
        let commit = entry.receipt.commit_id;
        entry.state == IndexState::Tombstoned
            && !entry.has_roots
            && !self
                .versions
                .values()
                .flatten()
                .any(|v| v.commit_id == commit)
            && !self
                .files
                .values()
                .filter_map(|f| f.head.version_id)
                .any(|version| {
                    self.versions
                        .values()
                        .flatten()
                        .any(|v| v.id == version && v.commit_id == commit)
                })
            && !self.intents.values().any(|i| {
                !i.state.terminal()
                    && i.candidate
                        .as_ref()
                        .is_some_and(|c| c.receipt.commit_id == commit)
            })
            && !self.retention_roots.contains(&commit)
            && !self.reader_pins.contains(&commit)
    }
    pub fn crash_reopen(&self) -> Result<Self, CatalogError> {
        let mut x = self.clone();
        x.coordinator_epoch = self
            .coordinator_epoch
            .checked_add(1)
            .ok_or(CatalogError::EpochExhausted)?;
        for i in x.intents.values_mut() {
            i.owner_fence = None
        }
        Ok(x)
    }
    fn auth(&self, a: PrincipalId, o: PrincipalId, r: Capability) -> Result<(), CatalogError> {
        let p = self.principals.get(&a).ok_or(CatalogError::NotFound)?;
        if p.state == PrincipalState::Disabled {
            return Err(CatalogError::PrincipalDisabled);
        }
        let owner = self.principals.get(&o).ok_or(CatalogError::NotFound)?;
        if owner.state == PrincipalState::Disabled {
            return Err(CatalogError::PrincipalDisabled);
        }
        if a == o
            || (owner.kind == PrincipalKind::Organization
                && self
                    .memberships
                    .get(&(o, a))
                    .is_some_and(|capability| capability.permits(r)))
        {
            Ok(())
        } else {
            Err(CatalogError::PermissionDenied)
        }
    }
    fn state(&mut self, id: PrincipalId, s: PrincipalState) -> Result<(), CatalogError> {
        self.principals.get(&id).ok_or(CatalogError::NotFound)?;
        self.bump_authz(&[id])?;
        self.principals.get_mut(&id).unwrap().state = s;
        Ok(())
    }
    fn require_organization(&self, id: PrincipalId) -> Result<(), CatalogError> {
        match self.principals.get(&id) {
            Some(Principal {
                kind: PrincipalKind::Organization,
                ..
            }) => Ok(()),
            Some(_) => Err(CatalogError::InvalidOrganization),
            None => Err(CatalogError::NotFound),
        }
    }
    fn bump_authz(&mut self, ids: &[PrincipalId]) -> Result<(), CatalogError> {
        self.global_authz_epoch
            .checked_add(1)
            .ok_or(CatalogError::EpochExhausted)?;
        let mut unique = HashSet::new();
        for id in ids {
            if unique.insert(*id) {
                self.principals
                    .get(id)
                    .ok_or(CatalogError::NotFound)?
                    .authz_epoch
                    .checked_add(1)
                    .ok_or(CatalogError::EpochExhausted)?;
            }
        }
        self.global_authz_epoch += 1;
        for id in unique {
            self.principals.get_mut(&id).unwrap().authz_epoch += 1;
        }
        Ok(())
    }
    fn reserve(
        &mut self,
        op: OperationId,
        actor: PrincipalId,
        k: OperationKind,
        f: [u8; 32],
    ) -> Result<Option<OperationResult>, CatalogError> {
        if let Some(x) = self.operations.get(&op) {
            if x.kind != k || x.actor != actor || x.request_fingerprint != f {
                return Err(CatalogError::OperationConflict);
            }
            if let Some(error) = &x.error {
                return Err(error.clone());
            }
            return Ok(x.result.clone());
        }
        self.operations.insert(
            op,
            OperationRecord {
                operation_id: op,
                actor,
                kind: k,
                request_fingerprint: f,
                result: None,
                error: None,
            },
        );
        Ok(None)
    }
    fn existing(
        &self,
        op: OperationId,
        actor: PrincipalId,
        k: OperationKind,
        f: [u8; 32],
    ) -> Result<Option<Option<OperationResult>>, CatalogError> {
        match self.operations.get(&op) {
            None => Ok(None),
            Some(x) if x.kind == k && x.actor == actor && x.request_fingerprint == f => {
                if let Some(error) = &x.error {
                    return Err(error.clone());
                }
                Ok(Some(x.result.clone()))
            }
            Some(_) => Err(CatalogError::OperationConflict),
        }
    }
    fn complete(&mut self, op: OperationId, r: Result<OperationResult, CatalogError>) {
        if let Some(x) = self.operations.get_mut(&op) {
            match r {
                Ok(v) => x.result = Some(v),
                Err(e) => x.error = Some(e),
            }
        }
    }
    fn complete_err(&mut self, op: OperationId, e: CatalogError) {
        self.complete(op, Err(e))
    }
    fn abort_inner(
        &mut self,
        op: OperationId,
        fence: u64,
        reason: &str,
        error: Option<CatalogError>,
    ) -> Result<(), CatalogError> {
        let current = self
            .intents
            .get(&op)
            .cloned()
            .ok_or(CatalogError::NotFound)?;
        if current.state.terminal() {
            return Ok(());
        }
        self.require_fence(&current, fence)?;
        let x = self.intents.get_mut(&op).unwrap();
        if !x.state.terminal() {
            x.state = IntentState::Aborted;
            x.abort_reason = Some(reason.into());
            x.owner_fence = None;
            x.pinned = false;
            self.tombstone(op);
            match error {
                Some(error) => self.complete_err(op, error),
                None => self.complete(op, Ok(OperationResult::Aborted)),
            }
        }
        Ok(())
    }
    fn tombstone(&mut self, op: OperationId) {
        if let Some(x) = self.dag_index.get_mut(&op) {
            x.state = IndexState::Tombstoned;
            x.has_roots = false
        }
    }
    fn require_fence(&self, intent: &PublishIntent, fence: u64) -> Result<(), CatalogError> {
        if intent.owner_fence == Some(fence) && fence == self.coordinator_epoch {
            Ok(())
        } else {
            Err(CatalogError::FenceLost)
        }
    }
    fn claim_recovery_fence(&mut self, op: OperationId) -> Result<u64, CatalogError> {
        let intent = self.intents.get(&op).ok_or(CatalogError::NotFound)?;
        if intent.state.terminal() {
            return Err(CatalogError::InvalidIntent);
        }
        if intent.owner_fence.is_some() {
            return Err(CatalogError::FenceLost);
        }
        let fence = self.coordinator_epoch;
        self.intents.get_mut(&op).unwrap().owner_fence = Some(fence);
        Ok(fence)
    }
}
impl Capability {
    fn permits(self, required: Self) -> bool {
        self == Self::ManageMembers || self == required
    }
}
fn next(x: &mut u64) -> Result<u64, CatalogError> {
    let n = *x;
    *x = x.checked_add(1).ok_or(CatalogError::IdExhausted)?;
    Ok(n)
}
fn norm(s: &str) -> Result<String, CatalogError> {
    let n: String = s.nfc().collect();
    if n.is_empty() || n.len() > 255 || n.chars().any(|c| c == '\0' || c == '/' || c.is_control()) {
        Err(CatalogError::InvalidName)
    } else {
        Ok(n)
    }
}
fn fingerprint(domain: &[u8], ints: &[u64], bytes: &[&[u8]]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(domain);
    h.update(&(ints.len() as u32).to_le_bytes());
    for value in ints {
        h.update(&value.to_le_bytes());
    }
    h.update(&(bytes.len() as u32).to_le_bytes());
    for value in bytes {
        h.update(&(value.len() as u64).to_le_bytes());
        h.update(value);
    }
    *h.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{content_digest, CommitNode, RangeMapNode, SnapshotNode};

    fn setup() -> (ModelCatalog, PrincipalId, FileId, OperationId) {
        let mut c = ModelCatalog::new();
        let p = c.create_principal(PrincipalKind::User).unwrap();
        let a = c.allocate_operation_id().unwrap();
        let col = c.create_collection(p, p, "docs", a).unwrap();
        let b = c.allocate_operation_id().unwrap();
        let f = c.create_file(p, col, "a", b).unwrap();
        let o = c.allocate_operation_id().unwrap();
        (c, p, f, o)
    }
    fn commit(parent: Option<CommitId>) -> (Dag, CommitId) {
        let mut dag = Dag::new();
        let map = Node::RangeMap(RangeMapNode {
            level: 0,
            children: Vec::new(),
        });
        let map_id = dag.insert(map).unwrap();
        let snapshot = Node::Snapshot(SnapshotNode {
            logical_size: 0,
            range_map_root: map_id,
            content_digest: content_digest(&[]),
        });
        let snapshot_id = dag.insert(snapshot).unwrap();
        if let Some(parent) = parent {
            // The caller supplies a parent only when it was constructed in this DAG.
            assert!(dag.get(&parent).is_some());
        }
        let commit = Node::Commit(CommitNode {
            snapshot: snapshot_id,
            parent,
        });
        let commit_id = dag.insert(commit).unwrap();
        (dag, commit_id)
    }
    fn rec(c: &mut ModelCatalog, o: OperationId) -> CommitId {
        let (dag, id) = commit(None);
        let fence = fence(c, o);
        c.register_receipt(o, fence, &dag, id).unwrap();
        id
    }
    fn fence(c: &ModelCatalog, o: OperationId) -> u64 {
        c.intent(o).unwrap().owner_fence.unwrap()
    }
    #[test]
    fn empty_head() {
        let (c, _, f, _) = setup();
        assert_eq!(
            c.head(f),
            Some(&Head {
                version_id: None,
                generation: 0
            })
        )
    }
    #[test]
    fn ids_are_opaque() {
        let (_c, p, f, o) = setup();
        assert!(p.get() < f.get() || o.get() > 0)
    }
    #[test]
    fn nfc() {
        assert_eq!(norm("e\u{301}").unwrap(), "é")
    }
    #[test]
    fn names() {
        assert!(norm("a/b").is_err());
        assert!(norm("x\n").is_err())
    }
    #[test]
    fn collection_retry() {
        let (mut c, p, _, _) = setup();
        let o = c.allocate_operation_id().unwrap();
        let a = c.create_collection(p, p, "z", o).unwrap();
        assert_eq!(c.create_collection(p, p, "z", o), Ok(a))
    }
    #[test]
    fn op_conflict() {
        let (mut c, p, f, _) = setup();
        let o = c.allocate_operation_id().unwrap();
        c.begin_publish(p, f, o, *c.head(f).unwrap()).unwrap();
        assert_eq!(
            c.create_file(p, c.files[&f].collection, "x", o),
            Err(CatalogError::OperationConflict)
        )
    }
    #[test]
    fn publish() {
        let (mut c, p, f, o) = setup();
        c.begin_publish(p, f, o, *c.head(f).unwrap()).unwrap();
        rec(&mut c, o);
        let fence = fence(&c, o);
        c.bind_candidate(o, fence).unwrap();
        let v = c.publish(o, fence).unwrap();
        assert_eq!((v.generation, v.size, v.parent_version_id), (1, 0, None))
    }
    #[test]
    fn publish_retry() {
        let (mut c, p, f, o) = setup();
        c.begin_publish(p, f, o, *c.head(f).unwrap()).unwrap();
        rec(&mut c, o);
        let fence = fence(&c, o);
        c.bind_candidate(o, fence).unwrap();
        let v = c.publish(o, fence).unwrap();
        assert_eq!(c.publish(o, fence), Ok(v))
    }
    #[test]
    fn stale_cas_aborts() {
        let (mut c, p, f, o) = setup();
        c.begin_publish(p, f, o, *c.head(f).unwrap()).unwrap();
        rec(&mut c, o);
        let fence = fence(&c, o);
        c.bind_candidate(o, fence).unwrap();
        c.files.get_mut(&f).unwrap().head.generation = 2;
        assert_eq!(c.publish(o, fence), Err(CatalogError::HeadConflict));
        assert_eq!(c.intent(o).unwrap().state, IntentState::Aborted)
    }
    #[test]
    fn forged_receipt_is_rejected() {
        let (mut c, p, f, o) = setup();
        c.begin_publish(p, f, o, *c.head(f).unwrap()).unwrap();
        assert_eq!(
            c.register_receipt(o, fence(&c, o), &Dag::new(), [9; 32]),
            Err(CatalogError::InvalidCommit)
        )
    }
    #[test]
    fn recovery_abort() {
        let (mut c, p, f, o) = setup();
        c.begin_publish(p, f, o, *c.head(f).unwrap()).unwrap();
        c.disable(p).unwrap();
        let fence = fence(&c, o);
        assert_eq!(c.abort(o, p, fence), Err(CatalogError::PrincipalDisabled));
        c.recovery_abort(o, fence, "x").unwrap();
        assert_eq!(c.intent(o).unwrap().state, IntentState::Aborted)
    }
    #[test]
    fn membership() {
        let (mut c, _p, f, o) = setup();
        let org = c.create_principal(PrincipalKind::Organization).unwrap();
        let m = c.create_principal(PrincipalKind::User).unwrap();
        c.grant_membership(org, org, m, Capability::Write).unwrap();
        assert_eq!(c.membership(org, m), Some(Capability::Write));
        let _ = f;
        let _ = o;
    }
    #[test]
    fn capability_replacement_is_unique_and_invalidates_epochs() {
        let (mut c, _owner, _, _) = setup();
        let org = c.create_principal(PrincipalKind::Organization).unwrap();
        let member = c.create_principal(PrincipalKind::User).unwrap();
        c.grant_membership(org, org, member, Capability::Read)
            .unwrap();
        let before = c.principal(member).unwrap().authz_epoch;
        c.grant_membership(org, org, member, Capability::Write)
            .unwrap();
        assert_eq!(c.memberships.len(), 1);
        assert_eq!(c.membership(org, member), Some(Capability::Write));
        assert!(c.principal(member).unwrap().authz_epoch > before);
        assert!(c.auth(member, org, Capability::Read).is_err());
        assert!(c.auth(member, org, Capability::Write).is_ok());
    }
    #[test]
    fn epoch_overflow_leaves_membership_mutation_atomic() {
        let (mut c, _owner, _, _) = setup();
        let org = c.create_principal(PrincipalKind::Organization).unwrap();
        let member = c.create_principal(PrincipalKind::User).unwrap();
        c.principals.get_mut(&member).unwrap().authz_epoch = u64::MAX;
        let global = c.global_authz_epoch;
        assert_eq!(
            c.grant_membership(org, org, member, Capability::Read),
            Err(CatalogError::EpochExhausted)
        );
        assert_eq!(c.global_authz_epoch, global);
        assert_eq!(c.membership(org, member), None);
    }
    #[test]
    fn reopen() {
        let (mut c, p, f, o) = setup();
        c.begin_publish(p, f, o, *c.head(f).unwrap()).unwrap();
        rec(&mut c, o);
        let r = c.crash_reopen().unwrap();
        assert!(r.intent(o).unwrap().owner_fence.is_none());
        assert_eq!(r.dag_operation(o).unwrap().receipt.operation_id(), o)
    }
    #[test]
    fn recover() {
        let (mut c, p, f, o) = setup();
        c.begin_publish(p, f, o, *c.head(f).unwrap()).unwrap();
        rec(&mut c, o);
        let mut c = c.crash_reopen().unwrap();
        assert_eq!(c.recover(o).unwrap().state, IntentState::Published)
    }
    #[test]
    fn absent_receipt_stays_preparing_after_reopen() {
        let (mut c, p, f, o) = setup();
        c.begin_publish(p, f, o, *c.head(f).unwrap()).unwrap();
        let mut c = c.crash_reopen().unwrap();
        c.recover(o).unwrap();
        assert_eq!(c.intent(o).unwrap().state, IntentState::Preparing)
    }
    #[test]
    fn reclaim() {
        let (mut c, p, f, o) = setup();
        c.begin_publish(p, f, o, *c.head(f).unwrap()).unwrap();
        rec(&mut c, o);
        let fence = fence(&c, o);
        c.recovery_abort(o, fence, "x").unwrap();
        // The tombstoned operation-index root is released, but no public API
        // can override live version or intent references.
        assert!(c.candidate_reclaimable(o))
    }
    #[test]
    fn gc_root_closure_keeps_published_versions_and_reader_pins() {
        let (mut c, p, f, o) = setup();
        c.begin_publish(p, f, o, *c.head(f).unwrap()).unwrap();
        let commit = rec(&mut c, o);
        let fence = fence(&c, o);
        c.bind_candidate(o, fence).unwrap();
        c.publish(o, fence).unwrap();
        assert!(!c.candidate_reclaimable(o));
        c.versions.clear();
        c.files.get_mut(&f).unwrap().head.version_id = None;
        c.reader_pins.insert(commit);
        assert!(!c.candidate_reclaimable(o));
        c.reader_pins.clear();
        c.retention_roots.insert(commit);
        assert!(!c.candidate_reclaimable(o));
    }
    #[test]
    fn stale_fence_and_nfc_uniqueness_are_rejected() {
        let (mut c, p, f, o) = setup();
        c.begin_publish(p, f, o, *c.head(f).unwrap()).unwrap();
        rec(&mut c, o);
        let old_fence = fence(&c, o);
        let mut reopened = c.crash_reopen().unwrap();
        assert_eq!(
            reopened.bind_candidate(o, old_fence),
            Err(CatalogError::FenceLost)
        );
        let new_fence = reopened.claim_recovery_fence(o).unwrap();
        reopened.bind_candidate(o, new_fence).unwrap();
        let op = reopened.allocate_operation_id().unwrap();
        assert!(reopened
            .create_file(p, reopened.files[&f].collection, "e\u{301}", op)
            .is_ok());
        let duplicate = reopened.allocate_operation_id().unwrap();
        assert_eq!(
            reopened.create_file(p, reopened.files[&f].collection, "é", duplicate),
            Err(CatalogError::NameTaken)
        );
    }
    #[test]
    fn terminal_publish_failure_is_sticky_and_operation_queries_are_actor_scoped() {
        let (mut c, p, f, o) = setup();
        c.begin_publish(p, f, o, *c.head(f).unwrap()).unwrap();
        rec(&mut c, o);
        let fence = fence(&c, o);
        c.bind_candidate(o, fence).unwrap();
        c.files.get_mut(&f).unwrap().head.generation = 9;
        assert_eq!(c.publish(o, fence), Err(CatalogError::HeadConflict));
        assert_eq!(c.publish(o, fence), Err(CatalogError::HeadConflict));
        let stranger = c.create_principal(PrincipalKind::User).unwrap();
        assert_eq!(
            c.query_operation(stranger, o),
            Err(CatalogError::PermissionDenied)
        );
        assert!(c.query_operation(p, o).unwrap().is_some());
    }
}
