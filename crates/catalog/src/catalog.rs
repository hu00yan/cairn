use crate::dag::{Dag, NodeId};
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
            pub const fn from_raw(value: u64) -> Self {
                Self(value)
            }

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

/// An opaque, single-intent coordinator claim.  The nonce makes claims made in
/// one coordinator epoch distinct; callers can only use the token returned for
/// their intent.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FenceToken {
    coordinator_epoch: u64,
    nonce: u64,
}

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitReceipt {
    pub(crate) operation_id: OperationId,
    pub(crate) commit_id: CommitId,
    pub(crate) snapshot_id: NodeId,
    pub(crate) snapshot_size: u64,
    pub(crate) snapshot_digest: NodeId,
    pub(crate) parent: Option<CommitId>,
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Candidate {
    pub version_id: VersionId,
    pub receipt: CommitReceipt,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishIntent {
    pub operation_id: OperationId,
    pub actor: PrincipalId,
    pub file: FileId,
    pub state: IntentState,
    pub authz_epoch: u64,
    pub expected_head: Head,
    pub base: Option<Head>,
    pub version_id: VersionId,
    pub candidate: Option<Candidate>,
    pub abort_reason: Option<String>,
    owner_fence: Option<FenceToken>,
    pub pinned: bool,
}

/// The actor-visible part of a publish intent. Coordinator claims deliberately
/// do not cross this API boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicPublishIntent {
    pub operation_id: OperationId,
    pub actor: PrincipalId,
    pub file: FileId,
    pub state: IntentState,
    pub expected_head: Head,
    pub base: Option<Head>,
    pub version_id: VersionId,
    pub abort_reason: Option<String>,
    pub pinned: bool,
}
impl From<&PublishIntent> for PublicPublishIntent {
    fn from(intent: &PublishIntent) -> Self {
        Self {
            operation_id: intent.operation_id,
            actor: intent.actor,
            file: intent.file,
            state: intent.state,
            expected_head: intent.expected_head,
            base: intent.base,
            version_id: intent.version_id,
            abort_reason: intent.abort_reason.clone(),
            pinned: intent.pinned,
        }
    }
}

/// A verified input from the durable-DAG side of the two-phase handoff.
///
/// This is intentionally not evidence of a cross-store atomic transaction.
/// It only lets the catalog consume a receipt that the DAG seam has already
/// verified as durable. A missing handoff therefore fails closed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableDagReceipt {
    pub(crate) receipt: CommitReceipt,
}

/// Opaque catalog-to-DAG handoff authorizing a terminal binding tombstone.
///
/// It is issued only after the catalog has durably recorded a Published or
/// Aborted intent and tombstoned its matching index entry.  The private
/// receipt prevents another crate module from manufacturing a permit.
#[allow(dead_code)] // Consumed by the catalog↔DAG recovery coordinator seam.
pub(crate) struct CatalogTerminalPermit {
    receipt: CommitReceipt,
}

impl CatalogTerminalPermit {
    #[allow(dead_code)] // Called by Dag when the coordinator performs GC.
    pub(crate) fn matches(&self, receipt: &CommitReceipt) -> bool {
        self.receipt == *receipt
    }
}

/// Capability held by the recovery coordinator after a simulated reopen. Its
/// constructor is private so ordinary principals cannot invoke recovery.
#[derive(Clone, Debug)]
pub struct RecoveryAuthority(());

/// Opaque, process-local authority for system principal administration.
///
/// This is deliberately neither serializable nor part of `ModelCatalog`:
/// system management is not an identity that can appear in the principal
/// schema or survive a reopen.
#[derive(Clone, Debug)]
pub(crate) struct SystemAuthority(());

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
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DagBindingState {
    /// The DAG atomically committed the operation receipt, but the catalog has
    /// not yet durably attached it to the publish intent.
    ReceiptCommitted,
    /// The catalog transaction bound the receipt to this intent.
    Bound,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogHandoff {
    pub receipt: CommitReceipt,
    pub state: IndexState,
    pub has_roots: bool,
    pub binding: DagBindingState,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CatalogError {
    NotFound,
    Aborted,
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
    RecoveryAuthorityRequired,
}
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Operation {
    Collection(CollectionId),
    File(FileId),
    Publish(PublishIntent),
    Error(CatalogError),
}

#[derive(Clone, Debug, Default)]
pub struct ModelCatalog {
    next_principal: u64,
    next_collection: u64,
    next_file: u64,
    next_version: u64,
    next_operation: u64,
    coordinator_epoch: u64,
    next_fence_nonce: u64,
    principals: HashMap<PrincipalId, Principal>,
    collections: HashMap<CollectionId, Collection>,
    files: HashMap<FileId, File>,
    versions: HashMap<FileId, Vec<FileVersion>>,
    intents: HashMap<OperationId, PublishIntent>,
    operations: HashMap<OperationId, OperationRecord>,
    handoffs: HashMap<OperationId, CatalogHandoff>,
    memberships: HashMap<(PrincipalId, PrincipalId), Capability>,
    retention_roots: HashSet<CommitId>,
    reader_pins: HashMap<CommitId, u64>,
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
    /// Returns the secret coordinator claim only for a nonterminal intent.
    /// Terminal retries are authenticated by their durable operation record,
    /// not by an in-memory coordinator claim.
    pub fn claim_token(
        &self,
        actor: PrincipalId,
        op: OperationId,
    ) -> Result<FenceToken, CatalogError> {
        let intent = self.intents.get(&op).ok_or(CatalogError::NotFound)?;
        self.authorize_publish_actor(actor, intent)?;
        intent.owner_fence.ok_or(CatalogError::FenceLost)
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
        if actor == member {
            return Err(CatalogError::PermissionDenied);
        }
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
        if actor == member {
            return Err(CatalogError::PermissionDenied);
        }
        self.principals.get(&member).ok_or(CatalogError::NotFound)?;
        self.bump_authz(&[organization, member])?;
        self.memberships.remove(&(organization, member));
        Ok(())
    }
    #[allow(dead_code)]
    pub(crate) fn disable(
        &mut self,
        _authority: &SystemAuthority,
        id: PrincipalId,
    ) -> Result<(), CatalogError> {
        self.state(id, PrincipalState::Disabled)
    }
    #[allow(dead_code)]
    pub(crate) fn enable(
        &mut self,
        _authority: &SystemAuthority,
        id: PrincipalId,
    ) -> Result<(), CatalogError> {
        self.state(id, PrincipalState::Active)
    }
    pub fn create_collection(
        &mut self,
        actor: PrincipalId,
        owner: PrincipalId,
        name: impl Into<String>,
        op: OperationId,
    ) -> Result<CollectionId, CatalogError> {
        let raw_name = name.into();
        let fp = fingerprint(
            b"cairn/catalog/create-collection/v1",
            &[actor.get(), owner.get()],
            &[raw_name.as_bytes()],
        );
        if let Some(r) = self.reserve(op, actor, OperationKind::CreateCollection, fp)? {
            if let OperationResult::Collection(x) = r {
                return Ok(x);
            }
            return Err(CatalogError::OperationConflict);
        }
        let z = (|| {
            let n = norm(&raw_name)?;
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
        let raw_name = name.into();
        let fp = fingerprint(
            b"cairn/catalog/create-file/v1",
            &[actor.get(), collection.get()],
            &[raw_name.as_bytes()],
        );
        if let Some(r) = self.reserve(op, actor, OperationKind::CreateFile, fp)? {
            if let OperationResult::File(x) = r {
                return Ok(x);
            }
            return Err(CatalogError::OperationConflict);
        }
        let z = (|| {
            let n = norm(&raw_name)?;
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
        proof: Option<FenceToken>,
    ) -> Result<PublicPublishIntent, CatalogError> {
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
        if self.operations.contains_key(&op) {
            if !self.intents.contains_key(&op) {
                self.existing(op, actor, OperationKind::Publish, fpr)?;
                return Err(CatalogError::InvalidIntent);
            }
            let intent = &self.intents[&op];
            self.authorize_publish_actor(actor, intent)?;
            self.require_operation_identity(op, actor, OperationKind::Publish, fpr)?;
            if intent.state.terminal() {
                self.require_terminal_operation(op)?;
                return Ok(PublicPublishIntent::from(intent));
            }
            let proof = proof.ok_or(CatalogError::FenceLost)?;
            self.require_fence(intent, proof)?;
            return Ok(PublicPublishIntent::from(intent));
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
                expected_head: expected,
                base: expected.version_id.map(|_| expected),
                version_id: x,
                candidate: None,
                abort_reason: None,
                owner_fence: Some(self.allocate_fence()?),
                pinned: true,
            };
            self.intents.insert(op, i.clone());
            Ok(PublicPublishIntent::from(&i))
        })();
        if let Err(e) = &z {
            self.complete_err(op, e.clone())
        }
        z
    }

    /// Consumes a verified durable-DAG handoff into the catalog half of the
    /// protocol. The DAG remains the sole source of the operation binding.
    pub fn register_receipt(
        &mut self,
        actor: PrincipalId,
        op: OperationId,
        fence: FenceToken,
        dag: &Dag,
        handoff: DurableDagReceipt,
    ) -> Result<CommitReceipt, CatalogError> {
        let i = self
            .intents
            .get(&op)
            .cloned()
            .ok_or(CatalogError::NotFound)?;
        self.authorize_publish_actor(actor, &i)?;
        self.require_fence(&i, fence)?;
        if i.state.terminal() {
            return Err(CatalogError::InvalidIntent);
        }
        let r = handoff.receipt;
        if r.operation_id != op {
            return Err(CatalogError::OperationConflict);
        }
        if !dag.receipt_is_active(&r) {
            return Err(CatalogError::ReceiptNotDurable);
        }
        if let Some(old) = self.handoffs.get(&op) {
            if old.receipt != r {
                return Err(CatalogError::OperationConflict);
            }
            return Ok(old.receipt.clone());
        }
        self.handoffs.insert(
            op,
            CatalogHandoff {
                receipt: r.clone(),
                state: IndexState::Active,
                has_roots: true,
                binding: DagBindingState::ReceiptCommitted,
            },
        );
        Ok(r)
    }

    pub(crate) fn register_verified_receipt(
        &mut self,
        actor: PrincipalId,
        op: OperationId,
        fence: FenceToken,
        handoff: DurableDagReceipt,
    ) -> Result<CommitReceipt, CatalogError> {
        let i = self
            .intents
            .get(&op)
            .cloned()
            .ok_or(CatalogError::NotFound)?;
        self.authorize_publish_actor(actor, &i)?;
        self.require_fence(&i, fence)?;
        if i.state.terminal() {
            return Err(CatalogError::InvalidIntent);
        }
        let r = handoff.receipt;
        if r.operation_id != op {
            return Err(CatalogError::OperationConflict);
        }
        if let Some(old) = self.handoffs.get(&op) {
            if old.receipt != r {
                return Err(CatalogError::OperationConflict);
            }
            return Ok(old.receipt.clone());
        }
        self.handoffs.insert(
            op,
            CatalogHandoff {
                receipt: r.clone(),
                state: IndexState::Active,
                has_roots: true,
                binding: DagBindingState::ReceiptCommitted,
            },
        );
        Ok(r)
    }
    pub fn bind_candidate(
        &mut self,
        actor: PrincipalId,
        op: OperationId,
        fence: FenceToken,
    ) -> Result<PublicPublishIntent, CatalogError> {
        let i = self
            .intents
            .get(&op)
            .cloned()
            .ok_or(CatalogError::NotFound)?;
        self.authorize_publish_actor(actor, &i)?;
        self.require_fence(&i, fence)?;
        if i.state.terminal() {
            return Ok(PublicPublishIntent::from(&i));
        }
        let r = self
            .handoffs
            .get(&op)
            .ok_or(CatalogError::NotFound)?
            .receipt
            .clone();
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
        self.handoffs.get_mut(&op).unwrap().binding = DagBindingState::Bound;
        Ok(PublicPublishIntent::from(&*x))
    }
    pub fn publish(
        &mut self,
        actor: PrincipalId,
        op: OperationId,
        fence: Option<FenceToken>,
    ) -> Result<FileVersion, CatalogError> {
        let i = self
            .intents
            .get(&op)
            .cloned()
            .ok_or(CatalogError::NotFound)?;
        self.authorize_publish_actor(actor, &i)?;
        self.require_operation_identity(
            op,
            i.actor,
            OperationKind::Publish,
            publish_request_fingerprint(&i),
        )?;
        if i.state.terminal() {
            return self.terminal_publish_result(op);
        }
        let fence = fence.ok_or(CatalogError::FenceLost)?;
        self.require_fence(&i, fence)?;
        if i.state == IntentState::Aborted {
            return Err(self
                .operations
                .get(&op)
                .and_then(|r| r.error.clone())
                .unwrap_or(CatalogError::InvalidIntent));
        }
        let c = i.candidate.clone().ok_or(CatalogError::InvalidIntent)?;
        if self.handoffs.get(&op).is_none_or(|entry| {
            entry.binding != DagBindingState::Bound || entry.receipt != c.receipt
        }) {
            return Err(CatalogError::ReceiptNotDurable);
        }
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
        if self.principals.get(&i.actor).map(|p| p.authz_epoch) != Some(i.authz_epoch) {
            self.abort_inner(
                op,
                fence,
                "authorization changed",
                Some(CatalogError::AuthzEpochChanged),
            )?;
            return Err(CatalogError::AuthzEpochChanged);
        }
        if self.auth(i.actor, owner, Capability::Write).is_err() {
            self.abort_inner(
                op,
                fence,
                "authorization denied",
                Some(CatalogError::PermissionDenied),
            )?;
            return Err(CatalogError::PermissionDenied);
        }
        if f.head != i.expected_head {
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
        fence: Option<FenceToken>,
    ) -> Result<PublicPublishIntent, CatalogError> {
        let i = self.intents.get(&op).ok_or(CatalogError::NotFound)?.clone();
        self.authorize_publish_actor(actor, &i)?;
        self.require_operation_identity(
            op,
            i.actor,
            OperationKind::Publish,
            publish_request_fingerprint(&i),
        )?;
        if i.state.terminal() {
            self.require_terminal_operation(op)?;
            return Ok(PublicPublishIntent::from(&i));
        }
        let fence = fence.ok_or(CatalogError::FenceLost)?;
        self.require_fence(&i, fence)?;
        self.abort_inner(op, fence, "caller abort", None)?;
        Ok(PublicPublishIntent::from(&self.intents[&op]))
    }
    pub fn recovery_abort(
        &mut self,
        _recovery: &RecoveryAuthority,
        op: OperationId,
        fence: FenceToken,
        reason: impl Into<String>,
    ) -> Result<PublicPublishIntent, CatalogError> {
        self.abort_inner(op, fence, &reason.into(), Some(CatalogError::InvalidIntent))?;
        Ok(PublicPublishIntent::from(&self.intents[&op]))
    }
    pub fn recover(
        &mut self,
        _recovery: &RecoveryAuthority,
        dag: &Dag,
        op: OperationId,
    ) -> Result<PublicPublishIntent, CatalogError> {
        if self
            .intents
            .get(&op)
            .ok_or(CatalogError::NotFound)?
            .state
            .terminal()
        {
            return Ok(PublicPublishIntent::from(&self.intents[&op]));
        }
        // Inspect the durable authority before taking a claim or authenticating
        // the original actor. A missing binding is an incomplete handoff, not
        // evidence that recovery may abort the intent.
        let Some(handoff) = dag.operation_binding(op) else {
            let fence = self.claim_recovery_fence(op)?;
            self.abort_inner(
                op,
                fence,
                "DAG operation binding missing during recovery",
                Some(CatalogError::InvalidIntent),
            )?;
            // A missing binding is an incomplete handoff. It is safe to
            // terminate the catalog intent, but never safe to leave it
            // permanently PREPARING.
            return Ok(PublicPublishIntent::from(&self.intents[&op]));
        };
        let actor = self.intents[&op].actor;
        let fence = self.claim_recovery_fence(op)?;
        let result = (|| {
            let intent = self.intents.get(&op).ok_or(CatalogError::NotFound)?;
            self.authorize_publish_actor(actor, intent)?;
            // DAG is the durable operation-binding authority. Import the
            // binding that survived the crash window before catalog registration.
            let receipt = handoff.receipt;
            match self.handoffs.get(&op) {
                Some(existing) if existing.receipt != receipt => {
                    return Err(CatalogError::OperationConflict)
                }
                Some(_) => {}
                None => {
                    self.handoffs.insert(
                        op,
                        CatalogHandoff {
                            receipt,
                            state: IndexState::Active,
                            has_roots: true,
                            binding: DagBindingState::ReceiptCommitted,
                        },
                    );
                }
            }
            self.bind_candidate(actor, op, fence)?;
            self.publish(actor, op, Some(fence)).map(|_| ())
        })();
        if let Err(error) = result {
            let reason = match error {
                CatalogError::AuthzEpochChanged => "authorization changed during recovery",
                CatalogError::PermissionDenied | CatalogError::PrincipalDisabled => {
                    "authorization denied during recovery"
                }
                _ => "recovery could not safely publish",
            };
            self.abort_inner(op, fence, reason, Some(error.clone()))?;
            return Err(error);
        }
        Ok(PublicPublishIntent::from(&self.intents[&op]))
    }

    /// Reconciles the two in-memory durability seams after a simulated
    /// reopen. Catalog terminal state is authoritative; an active DAG binding
    /// is merely retryable cleanup. Nonterminal intents are recovered from a
    /// surviving DAG binding or safely aborted when the handoff is missing.
    pub(crate) fn reconcile_startup(&mut self, dag: &mut Dag) -> Result<(), CatalogError> {
        let recovery = Self::recovery_authority();
        let operations: Vec<OperationId> = self.intents.keys().copied().collect();
        for op in operations {
            if self
                .intents
                .get(&op)
                .is_some_and(|intent| !intent.state.terminal())
            {
                if let Err(error) = self.recover(&recovery, dag, op) {
                    if !self.intents[&op].state.terminal() {
                        return Err(error);
                    }
                }
            }
        }

        let terminal: Vec<OperationId> = self
            .intents
            .iter()
            .filter_map(|(op, intent)| intent.state.terminal().then_some(*op))
            .collect();
        for op in terminal {
            let state = self.intents[&op].state;
            let Some(handoff) = self.handoffs.get(&op).cloned() else {
                // A caller can abort before the DAG commit/handoff exists.
                // That is a complete pre-DAG terminal state, not evidence of
                // an incomplete publication.
                if state == IntentState::Aborted {
                    continue;
                }
                return Err(CatalogError::InvalidIntent);
            };
            if let Some(binding) = dag.operation_binding(op) {
                let permit = self.terminal_permit(op)?;
                dag.tombstone_operation(op, binding, permit)
                    .map_err(|_| CatalogError::InvalidIntent)?;
            } else if !dag.receipt_is_tombstoned(&handoff.receipt) {
                return Err(CatalogError::InvalidIntent);
            }
        }
        Ok(())
    }
    pub fn head(&self, actor: PrincipalId, file: FileId) -> Result<Head, CatalogError> {
        let f = self.files.get(&file).ok_or(CatalogError::NotFound)?;
        self.auth(actor, self.owner_for_file(f)?, Capability::Read)?;
        Ok(f.head)
    }
    pub fn list_versions(
        &self,
        actor: PrincipalId,
        file: FileId,
    ) -> Result<Vec<FileVersion>, CatalogError> {
        let f = self.files.get(&file).ok_or(CatalogError::NotFound)?;
        self.auth(actor, self.owner_for_file(f)?, Capability::Read)?;
        Ok(self.versions.get(&file).cloned().unwrap_or_default())
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
        let principal = self.principals.get(&actor).ok_or(CatalogError::NotFound)?;
        if principal.state == PrincipalState::Disabled {
            return Err(CatalogError::PrincipalDisabled);
        }
        Ok(record)
    }
    pub fn intent(
        &self,
        actor: PrincipalId,
        op: OperationId,
    ) -> Result<PublicPublishIntent, CatalogError> {
        self.authorize_intent_read(actor, op)?;
        let intent = &self.intents[&op];
        Ok(PublicPublishIntent::from(intent))
    }

    pub(crate) fn pin_reader(
        &mut self,
        actor: PrincipalId,
        file: FileId,
        version: VersionId,
        commit: CommitId,
    ) -> Result<(), CatalogError> {
        let versions = self.list_versions(actor, file)?;
        if !versions
            .iter()
            .any(|candidate| candidate.id == version && candidate.commit_id == commit)
        {
            return Err(CatalogError::NotFound);
        }
        *self.reader_pins.entry(commit).or_insert(0) += 1;
        Ok(())
    }

    pub(crate) fn validate_reader(
        &self,
        actor: PrincipalId,
        file: FileId,
        version: Option<VersionId>,
        commit: Option<CommitId>,
    ) -> Result<(), CatalogError> {
        let versions = self.list_versions(actor, file)?;
        match (version, commit) {
            (Some(version), Some(commit)) => versions
                .iter()
                .any(|candidate| candidate.id == version && candidate.commit_id == commit)
                .then_some(())
                .ok_or(CatalogError::NotFound),
            (None, None) => Ok(()),
            _ => Err(CatalogError::NotFound),
        }
    }

    pub(crate) fn unpin_reader(&mut self, commit: CommitId) {
        if let Some(count) = self.reader_pins.get_mut(&commit) {
            *count -= 1;
            if *count == 0 {
                self.reader_pins.remove(&commit);
            }
        }
    }
    /// A candidate is reclaimable only after both durable indexes tombstone
    /// the same operation+receipt, catalog has no terminal/live references,
    /// and no other root reaches it through the complete parent closure.
    pub fn candidate_reclaimable(&self, dag: &Dag, op: OperationId) -> bool {
        let Some(entry) = self.handoffs.get(&op) else {
            return false;
        };
        let commit = entry.receipt.commit_id;
        entry.state == IndexState::Tombstoned
            && !entry.has_roots
            && entry.binding == DagBindingState::Bound
            && self
                .intents
                .get(&op)
                .is_none_or(|intent| intent.state.terminal())
            && dag.receipt_is_tombstoned(&entry.receipt)
            && !self.dag_root_closure_contains(dag, op, commit)
    }
    /// Hands the DAG the catalog half of the terminal-tombstone proof.
    ///
    /// This is crate-private deliberately: the permit is a handoff between
    /// the catalog and DAG durability seams, not caller-controlled evidence.
    #[allow(dead_code)] // Called by the catalog↔DAG recovery coordinator seam.
    pub(crate) fn terminal_permit(
        &self,
        op: OperationId,
    ) -> Result<CatalogTerminalPermit, CatalogError> {
        let intent = self.intents.get(&op).ok_or(CatalogError::NotFound)?;
        let handoff = self.handoffs.get(&op).ok_or(CatalogError::NotFound)?;
        if !intent.state.terminal()
            || handoff.state != IndexState::Tombstoned
            || handoff.binding != DagBindingState::Bound
        {
            return Err(CatalogError::InvalidIntent);
        }
        Ok(CatalogTerminalPermit {
            receipt: handoff.receipt.clone(),
        })
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
    fn owner_for_file(&self, file: &File) -> Result<PrincipalId, CatalogError> {
        self.collections
            .get(&file.collection)
            .map(|collection| collection.owner)
            .ok_or(CatalogError::NotFound)
    }
    fn authorize_intent_read(
        &self,
        actor: PrincipalId,
        op: OperationId,
    ) -> Result<(), CatalogError> {
        let intent = self.intents.get(&op).ok_or(CatalogError::NotFound)?;
        let principal = self.principals.get(&actor).ok_or(CatalogError::NotFound)?;
        if principal.state == PrincipalState::Disabled {
            return Err(CatalogError::PrincipalDisabled);
        }
        if actor == intent.actor {
            Ok(())
        } else {
            Err(CatalogError::PermissionDenied)
        }
    }
    fn authorize_publish_actor(
        &self,
        actor: PrincipalId,
        intent: &PublishIntent,
    ) -> Result<(), CatalogError> {
        if actor != intent.actor {
            return Err(CatalogError::PermissionDenied);
        }
        let file = self.files.get(&intent.file).ok_or(CatalogError::NotFound)?;
        let owner = self.owner_for_file(file)?;
        self.auth(actor, owner, Capability::Write)
    }
    fn allocate_fence(&mut self) -> Result<FenceToken, CatalogError> {
        let nonce = next(&mut self.next_fence_nonce)?;
        Ok(FenceToken {
            coordinator_epoch: self.coordinator_epoch,
            nonce,
        })
    }
    fn dag_root_closure_contains(
        &self,
        dag: &Dag,
        candidate_operation: OperationId,
        target: CommitId,
    ) -> bool {
        let roots: Vec<CommitId> = self
            .versions
            .values()
            .flatten()
            .map(|version| version.commit_id)
            .chain(self.retention_roots.iter().copied())
            .chain(self.reader_pins.keys().copied())
            .chain(self.intents.values().filter_map(|intent| {
                (!intent.state.terminal())
                    .then(|| {
                        intent
                            .candidate
                            .as_ref()
                            .map(|candidate| candidate.receipt.commit_id)
                    })
                    .flatten()
            }))
            .chain(self.intents.values().filter_map(|intent| {
                (!intent.state.terminal())
                    .then_some(intent.expected_head.version_id)
                    .flatten()
                    .and_then(|version_id| {
                        self.versions
                            .get(&intent.file)?
                            .iter()
                            .find(|version| version.id == version_id)
                            .map(|version| version.commit_id)
                    })
            }))
            .chain(self.handoffs.iter().filter_map(|(operation, entry)| {
                (*operation != candidate_operation
                    && entry.state == IndexState::Active
                    && entry.has_roots)
                    .then_some(entry.receipt.commit_id)
            }))
            .chain(dag.bound_commit_roots().filter_map(|(operation, commit)| {
                // Every active DAG binding is independently durable and must
                // retain its closure, even when the catalog has already
                // tombstoned its index.  Only the candidate may stop being a
                // root, and only after its catalog terminal+tombstone state.
                (operation != candidate_operation
                    || !self.operation_terminal_and_tombstoned(operation))
                .then_some(commit)
            }))
            .collect();
        // An unavailable root cannot safely prove that its ancestor is dead.
        dag.root_closure_contains(roots, target).unwrap_or(true)
    }
    fn operation_terminal_and_tombstoned(&self, operation: OperationId) -> bool {
        self.intents
            .get(&operation)
            .is_some_and(|intent| intent.state.terminal())
            && self.handoffs.get(&operation).is_some_and(|handoff| {
                handoff.state == IndexState::Tombstoned && handoff.binding == DagBindingState::Bound
            })
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
    #[allow(dead_code)]
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
    /// Validates the durable idempotency identity without interpreting its
    /// terminal result. `begin_publish` uses this so a terminal abort can still
    /// return its safe public intent view while `publish` replays the stored
    /// terminal error.
    fn require_operation_identity(
        &self,
        op: OperationId,
        actor: PrincipalId,
        kind: OperationKind,
        fingerprint: [u8; 32],
    ) -> Result<(), CatalogError> {
        match self.operations.get(&op) {
            Some(record)
                if record.operation_id == op
                    && record.kind == kind
                    && record.actor == actor
                    && record.request_fingerprint == fingerprint =>
            {
                Ok(())
            }
            Some(_) => Err(CatalogError::OperationConflict),
            None => Err(CatalogError::NotFound),
        }
    }
    /// A terminal intent is valid only when its durable operation record has a
    /// corresponding result or sticky error. This is the cross-restart retry
    /// authority; coordinator fences never participate here.
    fn require_terminal_operation(&self, op: OperationId) -> Result<(), CatalogError> {
        let record = self.operations.get(&op).ok_or(CatalogError::NotFound)?;
        if record.result.is_some() || record.error.is_some() {
            Ok(())
        } else {
            Err(CatalogError::InvalidIntent)
        }
    }
    fn terminal_publish_result(&self, op: OperationId) -> Result<FileVersion, CatalogError> {
        let record = self.operations.get(&op).ok_or(CatalogError::NotFound)?;
        match (&record.result, &record.error) {
            (Some(OperationResult::Version(version)), None) => Ok(version.clone()),
            (_, Some(error)) => Err(error.clone()),
            _ => Err(CatalogError::InvalidIntent),
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
        fence: FenceToken,
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
        if let Some(x) = self.handoffs.get_mut(&op) {
            x.state = IndexState::Tombstoned;
            x.has_roots = false
        }
    }
    fn require_fence(&self, intent: &PublishIntent, fence: FenceToken) -> Result<(), CatalogError> {
        if intent.owner_fence == Some(fence) && fence.coordinator_epoch == self.coordinator_epoch {
            Ok(())
        } else {
            Err(CatalogError::FenceLost)
        }
    }
    fn claim_recovery_fence(&mut self, op: OperationId) -> Result<FenceToken, CatalogError> {
        let intent = self.intents.get(&op).ok_or(CatalogError::NotFound)?;
        if intent.state.terminal() {
            return Err(CatalogError::InvalidIntent);
        }
        if intent.owner_fence.is_some() {
            return Err(CatalogError::FenceLost);
        }
        let fence = self.allocate_fence()?;
        self.intents.get_mut(&op).unwrap().owner_fence = Some(fence);
        Ok(fence)
    }

    /// Crate-private, production recovery bootstrap seam. Public callers
    /// cannot mint this capability.
    #[allow(dead_code)]
    pub(crate) fn recovery_authority() -> RecoveryAuthority {
        RecoveryAuthority(())
    }
    /// Crate-private, process-local administration authority. It is never
    /// represented as a `Principal` or serialized with catalog state.
    #[allow(dead_code)]
    pub(crate) fn system_authority() -> SystemAuthority {
        SystemAuthority(())
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

fn publish_request_fingerprint(intent: &PublishIntent) -> [u8; 32] {
    fingerprint(
        b"cairn/catalog/publish/v1",
        &[
            intent.actor.get(),
            intent.file.get(),
            intent
                .expected_head
                .version_id
                .map_or(u64::MAX, VersionId::get),
            intent.expected_head.generation,
        ],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{
        CommitNode, ContentNode, Node, NodeKind, RangeMapEntry, RangeMapNode, SnapshotNode,
    };

    fn setup() -> (ModelCatalog, PrincipalId, FileId, OperationId) {
        let mut catalog = ModelCatalog::new();
        let actor = catalog.create_principal(PrincipalKind::User).unwrap();
        let collection_op = catalog.allocate_operation_id().unwrap();
        let collection = catalog
            .create_collection(actor, actor, "docs", collection_op)
            .unwrap();
        let file_op = catalog.allocate_operation_id().unwrap();
        let file = catalog
            .create_file(actor, collection, "a", file_op)
            .unwrap();
        let operation = catalog.allocate_operation_id().unwrap();
        (catalog, actor, file, operation)
    }

    fn commit() -> (Dag, CommitId) {
        let mut dag = Dag::new();
        let map = dag
            .insert(Node::RangeMap(RangeMapNode {
                level: 0,
                children: vec![],
            }))
            .unwrap();
        let snapshot = dag
            .insert(Node::Snapshot(SnapshotNode {
                logical_size: 0,
                range_map_root: map,
                content_digest: crate::dag::content_digest(&[]),
            }))
            .unwrap();
        let commit = dag
            .insert(Node::Commit(CommitNode {
                snapshot,
                parent: None,
            }))
            .unwrap();
        (dag, commit)
    }

    fn register(
        catalog: &mut ModelCatalog,
        actor: PrincipalId,
        op: OperationId,
        dag: &mut Dag,
        commit: CommitId,
    ) -> FenceToken {
        let fence = catalog.claim_token(actor, op).unwrap();
        let handoff = dag.commit_operation(op, commit).unwrap();
        catalog
            .register_receipt(actor, op, fence, dag, handoff)
            .unwrap();
        fence
    }

    #[test]
    fn dag_is_the_only_operation_binding_authority() {
        let (mut catalog, actor, file, op) = setup();
        catalog
            .begin_publish(actor, file, op, catalog.head(actor, file).unwrap(), None)
            .unwrap();
        let (mut dag, commit) = commit();
        let fence = catalog.claim_token(actor, op).unwrap();
        let handoff = dag.commit_operation(op, commit).unwrap();
        catalog
            .register_receipt(actor, op, fence, &dag, handoff)
            .unwrap();
        catalog.bind_candidate(actor, op, fence).unwrap();
        assert!(catalog.publish(actor, op, Some(fence)).is_ok());
    }

    #[test]
    fn revoked_recovery_is_terminal_and_releases_claim() {
        let (mut catalog, actor, file, op) = setup();
        catalog
            .begin_publish(actor, file, op, catalog.head(actor, file).unwrap(), None)
            .unwrap();
        let (mut dag, commit) = commit();
        register(&mut catalog, actor, op, &mut dag, commit);
        let authority = ModelCatalog::system_authority();
        catalog.disable(&authority, actor).unwrap();
        let mut reopened = catalog.crash_reopen().unwrap();
        let authority = ModelCatalog::recovery_authority();
        assert_eq!(
            reopened.recover(&authority, &dag, op),
            Err(CatalogError::PrincipalDisabled)
        );
        let intent = &reopened.intents[&op];
        assert_eq!(intent.state, IntentState::Aborted);
        assert!(!intent.pinned);
        assert!(intent.owner_fence.is_none());
        assert_eq!(
            reopened.operations[&op].error,
            Some(CatalogError::PrincipalDisabled)
        );
    }

    #[test]
    fn crash_between_dag_binding_and_catalog_import_recovers_from_dag() {
        let (mut catalog, actor, file, op) = setup();
        catalog
            .begin_publish(actor, file, op, catalog.head(actor, file).unwrap(), None)
            .unwrap();
        let (mut dag, commit) = commit();
        dag.commit_operation(op, commit).unwrap();
        let dag = dag.crash_reopen();
        let mut reopened = catalog.crash_reopen().unwrap();
        assert!(!reopened.handoffs.contains_key(&op));
        let authority = ModelCatalog::recovery_authority();
        let recovered = reopened.recover(&authority, &dag, op).unwrap();
        assert_eq!(recovered.state, IntentState::Published);
    }

    #[test]
    fn terminal_retries_require_the_original_actor_but_no_fence_proof() {
        let (mut catalog, actor, file, op) = setup();
        catalog
            .begin_publish(actor, file, op, catalog.head(actor, file).unwrap(), None)
            .unwrap();
        let (mut dag, commit) = commit();
        let fence = register(&mut catalog, actor, op, &mut dag, commit);
        catalog.bind_candidate(actor, op, fence).unwrap();
        let version = catalog.publish(actor, op, Some(fence)).unwrap();
        let stranger = catalog.create_principal(PrincipalKind::User).unwrap();
        assert_eq!(
            catalog.publish(stranger, op, None),
            Err(CatalogError::PermissionDenied)
        );
        assert_eq!(catalog.publish(actor, op, None), Ok(version));
        assert_eq!(
            catalog.abort(op, stranger, None),
            Err(CatalogError::PermissionDenied)
        );
        assert_eq!(
            catalog.intent(stranger, op),
            Err(CatalogError::PermissionDenied)
        );
        assert_eq!(
            catalog.intent(actor, op).unwrap().state,
            IntentState::Published
        );
    }

    #[test]
    fn terminal_publish_replay_rejects_tampered_operation_records() {
        let (mut catalog, actor, file, op) = setup();
        catalog
            .begin_publish(actor, file, op, catalog.head(actor, file).unwrap(), None)
            .unwrap();
        let (mut dag, commit) = commit();
        let fence = register(&mut catalog, actor, op, &mut dag, commit);
        catalog.bind_candidate(actor, op, fence).unwrap();
        catalog.publish(actor, op, Some(fence)).unwrap();
        let other_actor = catalog.create_principal(PrincipalKind::User).unwrap();

        let mut actor_mismatch = catalog.clone();
        actor_mismatch.operations.get_mut(&op).unwrap().actor = other_actor;
        assert_eq!(
            actor_mismatch.publish(actor, op, None),
            Err(CatalogError::OperationConflict)
        );

        let mut kind_mismatch = catalog.clone();
        kind_mismatch.operations.get_mut(&op).unwrap().kind = OperationKind::CreateFile;
        assert_eq!(
            kind_mismatch.publish(actor, op, None),
            Err(CatalogError::OperationConflict)
        );

        let mut fingerprint_mismatch = catalog.clone();
        fingerprint_mismatch
            .operations
            .get_mut(&op)
            .unwrap()
            .request_fingerprint = [0; 32];
        assert_eq!(
            fingerprint_mismatch.publish(actor, op, None),
            Err(CatalogError::OperationConflict)
        );
    }

    #[test]
    fn terminal_abort_replay_rejects_tampered_operation_records() {
        let (mut catalog, actor, file, op) = setup();
        catalog
            .begin_publish(actor, file, op, catalog.head(actor, file).unwrap(), None)
            .unwrap();
        let fence = catalog.claim_token(actor, op).unwrap();
        catalog.abort(op, actor, Some(fence)).unwrap();
        let other_actor = catalog.create_principal(PrincipalKind::User).unwrap();

        let mut actor_mismatch = catalog.clone();
        actor_mismatch.operations.get_mut(&op).unwrap().actor = other_actor;
        assert_eq!(
            actor_mismatch.abort(op, actor, None),
            Err(CatalogError::OperationConflict)
        );

        let mut kind_mismatch = catalog.clone();
        kind_mismatch.operations.get_mut(&op).unwrap().kind = OperationKind::CreateFile;
        assert_eq!(
            kind_mismatch.abort(op, actor, None),
            Err(CatalogError::OperationConflict)
        );

        let mut fingerprint_mismatch = catalog.clone();
        fingerprint_mismatch
            .operations
            .get_mut(&op)
            .unwrap()
            .request_fingerprint = [0; 32];
        assert_eq!(
            fingerprint_mismatch.abort(op, actor, None),
            Err(CatalogError::OperationConflict)
        );
    }

    #[test]
    fn begin_publish_nonterminal_retry_requires_fence_but_terminal_retry_does_not() {
        let (mut catalog, actor, file, op) = setup();
        let expected = catalog.head(actor, file).unwrap();
        catalog
            .begin_publish(actor, file, op, expected, None)
            .unwrap();
        let fence = catalog.claim_token(actor, op).unwrap();
        assert_eq!(
            catalog.begin_publish(actor, file, op, expected, None),
            Err(CatalogError::FenceLost)
        );
        assert_eq!(
            catalog
                .begin_publish(actor, file, op, expected, Some(fence))
                .unwrap()
                .state,
            IntentState::Preparing
        );

        let (mut dag, commit) = commit();
        register(&mut catalog, actor, op, &mut dag, commit);
        catalog.bind_candidate(actor, op, fence).unwrap();
        catalog.publish(actor, op, Some(fence)).unwrap();
        assert_eq!(
            catalog
                .begin_publish(actor, file, op, expected, None)
                .unwrap()
                .state,
            IntentState::Published
        );
        assert_eq!(
            catalog
                .begin_publish(actor, file, op, expected, Some(fence))
                .unwrap()
                .state,
            IntentState::Published
        );
        let stranger = catalog.create_principal(PrincipalKind::User).unwrap();
        assert_eq!(
            catalog.begin_publish(stranger, file, op, expected, Some(fence)),
            Err(CatalogError::PermissionDenied)
        );
    }

    #[test]
    fn recovery_terminal_results_survive_reopen_without_fence_proofs() {
        let (mut catalog, actor, file, publish_op) = setup();
        let expected = catalog.head(actor, file).unwrap();
        catalog
            .begin_publish(actor, file, publish_op, expected, None)
            .unwrap();
        let (mut dag, first_commit) = commit();
        dag.commit_operation(publish_op, first_commit).unwrap();
        let mut reopened = catalog.crash_reopen().unwrap();
        let authority = ModelCatalog::recovery_authority();
        assert_eq!(
            reopened
                .recover(&authority, &dag, publish_op)
                .unwrap()
                .state,
            IntentState::Published
        );
        let published = reopened.publish(actor, publish_op, None).unwrap();
        let mut reopened = reopened.crash_reopen().unwrap();
        assert_eq!(reopened.publish(actor, publish_op, None), Ok(published));

        let abort_op = reopened.allocate_operation_id().unwrap();
        let expected = reopened.head(actor, file).unwrap();
        reopened
            .begin_publish(actor, file, abort_op, expected, None)
            .unwrap();
        // A parentless Commit cannot be the candidate for this non-initial head.
        let content = dag
            .insert(Node::Content(ContentNode {
                bytes: b"x".to_vec(),
            }))
            .unwrap();
        let map = dag
            .insert(Node::RangeMap(RangeMapNode {
                level: 0,
                children: vec![RangeMapEntry {
                    logical_start: 0,
                    logical_len: 1,
                    content_offset: 0,
                    child_kind: NodeKind::Content,
                    child: content,
                }],
            }))
            .unwrap();
        let snapshot = dag
            .insert(Node::Snapshot(SnapshotNode {
                logical_size: 1,
                range_map_root: map,
                content_digest: crate::dag::content_digest(b"x"),
            }))
            .unwrap();
        let parentless_commit = dag
            .insert(Node::Commit(CommitNode {
                snapshot,
                parent: None,
            }))
            .unwrap();
        dag.commit_operation(abort_op, parentless_commit).unwrap();
        let mut reopened = reopened.crash_reopen().unwrap();
        assert_eq!(
            reopened.recover(&authority, &dag, abort_op),
            Err(CatalogError::CandidateMismatch)
        );
        assert_eq!(
            reopened.abort(abort_op, actor, None).unwrap().state,
            IntentState::Aborted
        );
        let mut reopened = reopened.crash_reopen().unwrap();
        assert_eq!(
            reopened.abort(abort_op, actor, None).unwrap().state,
            IntentState::Aborted
        );
        let stranger = reopened.create_principal(PrincipalKind::User).unwrap();
        assert_eq!(
            reopened.publish(stranger, publish_op, None),
            Err(CatalogError::PermissionDenied)
        );
        assert_eq!(
            reopened.abort(abort_op, stranger, None),
            Err(CatalogError::PermissionDenied)
        );
    }

    #[test]
    fn operation_idempotency_nfc_and_actor_epoch_overflow_are_atomic() {
        let mut catalog = ModelCatalog::new();
        let owner = catalog.create_principal(PrincipalKind::User).unwrap();
        let op = catalog.allocate_operation_id().unwrap();
        let _collection = catalog
            .create_collection(owner, owner, "e\u{301}", op)
            .unwrap();
        assert_eq!(
            catalog.create_collection(owner, owner, "é", op),
            Err(CatalogError::OperationConflict)
        );
        let other_op = catalog.allocate_operation_id().unwrap();
        assert_eq!(
            catalog.create_collection(owner, owner, "é", other_op),
            Err(CatalogError::NameTaken)
        );
        assert_eq!(
            catalog.create_collection(owner, owner, "other", op),
            Err(CatalogError::OperationConflict)
        );

        let org = catalog
            .create_principal(PrincipalKind::Organization)
            .unwrap();
        let member = catalog.create_principal(PrincipalKind::User).unwrap();
        catalog.principals.get_mut(&member).unwrap().authz_epoch = u64::MAX;
        assert_eq!(
            catalog.grant_membership(org, org, member, Capability::Read),
            Err(CatalogError::EpochExhausted)
        );
        assert_eq!(catalog.membership(org, member), None);
    }

    #[test]
    fn stale_fence_and_missing_binding_do_not_destroy_recoverable_intents() {
        let (mut catalog, actor, file, op) = setup();
        catalog
            .begin_publish(actor, file, op, catalog.head(actor, file).unwrap(), None)
            .unwrap();
        let (mut dag, first_commit) = commit();
        let old_fence = register(&mut catalog, actor, op, &mut dag, first_commit);
        let mut reopened = catalog.crash_reopen().unwrap();
        assert_eq!(
            reopened.bind_candidate(actor, op, old_fence),
            Err(CatalogError::FenceLost)
        );

        let missing_op = reopened.allocate_operation_id().unwrap();
        reopened
            .begin_publish(
                actor,
                file,
                missing_op,
                reopened.head(actor, file).unwrap(),
                None,
            )
            .unwrap();
        let authority = ModelCatalog::system_authority();
        reopened.disable(&authority, actor).unwrap();
        let mut reopened = reopened.crash_reopen().unwrap();
        let authority = ModelCatalog::recovery_authority();
        assert_eq!(
            reopened
                .recover(&authority, &dag, missing_op)
                .unwrap()
                .state,
            IntentState::Aborted
        );
        assert!(!reopened.intents[&missing_op].pinned);
        assert_eq!(
            reopened
                .recover(&authority, &dag, missing_op)
                .unwrap()
                .state,
            IntentState::Aborted
        );
    }

    #[test]
    fn actor_epoch_overflow_is_atomic() {
        let mut catalog = ModelCatalog::new();
        let org = catalog
            .create_principal(PrincipalKind::Organization)
            .unwrap();
        let member = catalog.create_principal(PrincipalKind::User).unwrap();
        catalog.principals.get_mut(&member).unwrap().authz_epoch = u64::MAX;
        assert_eq!(
            catalog.grant_membership(org, org, member, Capability::Read),
            Err(CatalogError::EpochExhausted)
        );
        assert_eq!(catalog.principal(org).unwrap().authz_epoch, 0);
        assert_eq!(catalog.membership(org, member), None);
    }

    #[test]
    fn capability_replacement_overwrites_the_prior_grant() {
        let mut catalog = ModelCatalog::new();
        let org = catalog
            .create_principal(PrincipalKind::Organization)
            .unwrap();
        let member = catalog.create_principal(PrincipalKind::User).unwrap();
        catalog
            .grant_membership(org, org, member, Capability::Read)
            .unwrap();
        catalog
            .grant_membership(org, org, member, Capability::Write)
            .unwrap();
        assert_eq!(catalog.membership(org, member), Some(Capability::Write));
    }

    #[test]
    fn published_version_roots_and_reader_pins_are_not_reclaimable() {
        let (dag, commit) = commit();
        let mut catalog = ModelCatalog::new();
        let op = catalog.allocate_operation_id().unwrap();
        let receipt = CommitReceipt {
            operation_id: op,
            commit_id: commit,
            snapshot_id: match dag.get(&commit).unwrap() {
                Node::Commit(node) => node.snapshot,
                _ => unreachable!(),
            },
            snapshot_size: 0,
            snapshot_digest: crate::dag::content_digest(&[]),
            parent: None,
        };
        catalog.handoffs.insert(
            op,
            CatalogHandoff {
                receipt: receipt.clone(),
                state: IndexState::Tombstoned,
                has_roots: false,
                binding: DagBindingState::Bound,
            },
        );
        let file = FileId(1);
        catalog.versions.insert(
            file,
            vec![FileVersion {
                id: VersionId(1),
                file,
                generation: 1,
                commit_id: commit,
                parent_version_id: None,
                size: 0,
                digest: receipt.snapshot_digest,
            }],
        );
        assert!(!catalog.candidate_reclaimable(&dag, op));
        catalog.versions.clear();
        catalog.reader_pins.insert(commit, 1);
        assert!(!catalog.candidate_reclaimable(&dag, op));
    }

    #[test]
    fn reader_pin_counts_survive_one_of_two_handles_closing() {
        let (dag, commit) = commit();
        let (mut catalog, actor, file, _) = setup();
        let version = VersionId(1);
        catalog.versions.insert(
            file,
            vec![FileVersion {
                id: version,
                file,
                generation: 1,
                commit_id: commit,
                parent_version_id: None,
                size: 0,
                digest: crate::dag::content_digest(&[]),
            }],
        );
        catalog.pin_reader(actor, file, version, commit).unwrap();
        catalog.pin_reader(actor, file, version, commit).unwrap();
        assert_eq!(catalog.reader_pins.get(&commit), Some(&2));
        catalog.unpin_reader(commit);
        assert_eq!(catalog.reader_pins.get(&commit), Some(&1));
        catalog.unpin_reader(commit);
        assert!(!catalog.reader_pins.contains_key(&commit));
        assert!(dag.get(&commit).is_some());
    }

    #[test]
    fn startup_accepts_pre_dag_abort_without_handoff() {
        let (mut catalog, actor, file, op) = setup();
        let head = catalog.head(actor, file).unwrap();
        catalog.begin_publish(actor, file, op, head, None).unwrap();
        let fence = catalog.claim_token(actor, op).unwrap();
        catalog.abort(op, actor, Some(fence)).unwrap();
        catalog.handoffs.clear();
        let mut dag = Dag::new();
        catalog.reconcile_startup(&mut dag).unwrap();
        assert_eq!(catalog.intents[&op].state, IntentState::Aborted);
    }

    #[test]
    fn startup_rejects_terminal_handoff_without_dag_proof() {
        let (mut catalog, actor, file, op) = setup();
        let head = catalog.head(actor, file).unwrap();
        catalog.begin_publish(actor, file, op, head, None).unwrap();
        let fence = catalog.claim_token(actor, op).unwrap();
        catalog.abort(op, actor, Some(fence)).unwrap();

        catalog.handoffs.insert(
            op,
            CatalogHandoff {
                receipt: CommitReceipt {
                    operation_id: op,
                    commit_id: [0; 32],
                    snapshot_id: [0; 32],
                    snapshot_size: 0,
                    snapshot_digest: crate::dag::content_digest(&[]),
                    parent: None,
                },
                state: IndexState::Tombstoned,
                has_roots: false,
                binding: DagBindingState::Bound,
            },
        );
        let mut dag_without_binding = Dag::new();
        assert_eq!(
            catalog.reconcile_startup(&mut dag_without_binding),
            Err(CatalogError::InvalidIntent)
        );
    }

    #[test]
    fn terminal_catalog_permit_allows_a_tombstoned_candidate_to_be_reclaimed() {
        let (mut dag, commit) = commit();
        let (mut catalog, actor, file, op) = setup();
        catalog
            .begin_publish(actor, file, op, catalog.head(actor, file).unwrap(), None)
            .unwrap();
        let handoff = dag.commit_operation(op, commit).unwrap();
        let fence = catalog.claim_token(actor, op).unwrap();
        catalog
            .register_receipt(actor, op, fence, &dag, handoff.clone())
            .unwrap();
        catalog.bind_candidate(actor, op, fence).unwrap();
        catalog.publish(actor, op, Some(fence)).unwrap();
        catalog.versions.clear();
        assert!(!catalog.candidate_reclaimable(&dag, op));
        let permit = catalog.terminal_permit(op).unwrap();
        dag.tombstone_operation(op, handoff, permit).unwrap();
        assert!(catalog.candidate_reclaimable(&dag, op));
    }

    #[test]
    fn active_dag_binding_retains_parent_despite_a_tombstoned_catalog_index() {
        let (mut dag, first) = commit();
        let snapshot = match dag.get(&first).unwrap() {
            Node::Commit(commit) => commit.snapshot,
            _ => unreachable!(),
        };
        let second = dag
            .insert(Node::Commit(CommitNode {
                snapshot,
                parent: Some(first),
            }))
            .unwrap();
        let (mut catalog, actor, file, first_op) = setup();
        catalog
            .begin_publish(
                actor,
                file,
                first_op,
                catalog.head(actor, file).unwrap(),
                None,
            )
            .unwrap();
        let first_handoff = dag.commit_operation(first_op, first).unwrap();
        let first_fence = catalog.claim_token(actor, first_op).unwrap();
        catalog
            .register_receipt(actor, first_op, first_fence, &dag, first_handoff.clone())
            .unwrap();
        catalog
            .bind_candidate(actor, first_op, first_fence)
            .unwrap();
        catalog.publish(actor, first_op, Some(first_fence)).unwrap();

        let second_op = catalog.allocate_operation_id().unwrap();
        catalog
            .begin_publish(
                actor,
                file,
                second_op,
                catalog.head(actor, file).unwrap(),
                None,
            )
            .unwrap();
        let second_handoff = dag.commit_operation(second_op, second).unwrap();
        let second_fence = catalog.claim_token(actor, second_op).unwrap();
        catalog
            .register_receipt(actor, second_op, second_fence, &dag, second_handoff)
            .unwrap();
        catalog
            .bind_candidate(actor, second_op, second_fence)
            .unwrap();
        catalog
            .publish(actor, second_op, Some(second_fence))
            .unwrap();

        // Both catalog entries are tombstoned after publish, but B's durable
        // DAG binding remains Active and must retain its parent A.
        catalog.versions.clear();
        let first_permit = catalog.terminal_permit(first_op).unwrap();
        dag.tombstone_operation(first_op, first_handoff, first_permit)
            .unwrap();
        assert!(!catalog.candidate_reclaimable(&dag, first_op));
    }

    #[test]
    fn terminal_permit_rejects_incomplete_catalog_and_mismatched_receipt() {
        let (mut dag, first) = commit();
        let snapshot = match dag.get(&first).unwrap() {
            Node::Commit(commit) => commit.snapshot,
            _ => unreachable!(),
        };
        let second = dag
            .insert(Node::Commit(CommitNode {
                snapshot,
                parent: Some(first),
            }))
            .unwrap();
        let (mut catalog, actor, file, first_op) = setup();
        catalog
            .begin_publish(
                actor,
                file,
                first_op,
                catalog.head(actor, file).unwrap(),
                None,
            )
            .unwrap();
        let first_handoff = dag.commit_operation(first_op, first).unwrap();
        let first_fence = catalog.claim_token(actor, first_op).unwrap();
        catalog
            .register_receipt(actor, first_op, first_fence, &dag, first_handoff.clone())
            .unwrap();
        catalog
            .bind_candidate(actor, first_op, first_fence)
            .unwrap();
        assert!(matches!(
            catalog.terminal_permit(first_op),
            Err(CatalogError::InvalidIntent)
        ));
        assert!(dag.operation_binding(first_op).is_some());
        assert!(dag.crash_reopen().operation_binding(first_op).is_some());

        catalog.publish(actor, first_op, Some(first_fence)).unwrap();
        let permit = catalog.terminal_permit(first_op).unwrap();
        let second_op = catalog.allocate_operation_id().unwrap();
        let second_handoff = dag.commit_operation(second_op, second).unwrap();
        assert_eq!(
            dag.tombstone_operation(second_op, second_handoff, permit),
            Err(crate::dag::DagError::OperationConflict(second_op))
        );
        assert!(dag.operation_binding(second_op).is_some());
        assert!(dag.crash_reopen().operation_binding(second_op).is_some());
    }

    #[test]
    fn catalog_tombstone_before_dag_tombstone_is_not_reclaimable_after_reopen() {
        let (mut dag, commit) = commit();
        let mut catalog = ModelCatalog::new();
        let op = catalog.allocate_operation_id().unwrap();
        let receipt = dag.commit_operation(op, commit).unwrap().receipt;
        catalog.handoffs.insert(
            op,
            CatalogHandoff {
                receipt,
                state: IndexState::Tombstoned,
                has_roots: false,
                binding: DagBindingState::Bound,
            },
        );
        let catalog = catalog.crash_reopen().unwrap();
        let dag = dag.crash_reopen();
        assert!(!catalog.candidate_reclaimable(&dag, op));
    }

    #[test]
    fn invalid_create_names_reserve_sticky_errors_before_normalization() {
        let mut catalog = ModelCatalog::new();
        let actor = catalog.create_principal(PrincipalKind::User).unwrap();
        let collection_op = catalog.allocate_operation_id().unwrap();
        assert_eq!(
            catalog.create_collection(actor, actor, "bad/name", collection_op),
            Err(CatalogError::InvalidName)
        );
        assert_eq!(catalog.collections.len(), 0);
        assert_eq!(
            catalog.create_collection(actor, actor, "good", collection_op),
            Err(CatalogError::OperationConflict)
        );
        assert_eq!(
            catalog.operations[&collection_op].error,
            Some(CatalogError::InvalidName)
        );

        let valid_collection_op = catalog.allocate_operation_id().unwrap();
        let collection = catalog
            .create_collection(actor, actor, "docs", valid_collection_op)
            .unwrap();
        let file_op = catalog.allocate_operation_id().unwrap();
        assert_eq!(
            catalog.create_file(actor, collection, "bad/name", file_op),
            Err(CatalogError::InvalidName)
        );
        assert_eq!(catalog.files.len(), 0);
        assert_eq!(
            catalog.create_file(actor, collection, "good", file_op),
            Err(CatalogError::OperationConflict)
        );
        assert_eq!(
            catalog.operations[&file_op].error,
            Some(CatalogError::InvalidName)
        );
    }

    #[test]
    fn dag_only_binding_survives_the_catalog_import_crash_window() {
        let (mut dag, target) = commit();
        let snapshot = match dag.get(&target).unwrap() {
            Node::Commit(node) => node.snapshot,
            _ => unreachable!(),
        };
        let dag_only_root = dag
            .insert(Node::Commit(CommitNode {
                snapshot,
                parent: Some(target),
            }))
            .unwrap();
        let mut catalog = ModelCatalog::new();
        let target_op = catalog.allocate_operation_id().unwrap();
        let dag_only_op = catalog.allocate_operation_id().unwrap();
        dag.commit_operation(dag_only_op, dag_only_root).unwrap();
        let dag = dag.crash_reopen();
        let receipt = CommitReceipt {
            operation_id: target_op,
            commit_id: target,
            snapshot_id: snapshot,
            snapshot_size: 0,
            snapshot_digest: crate::dag::content_digest(&[]),
            parent: None,
        };
        catalog.handoffs.insert(
            target_op,
            CatalogHandoff {
                receipt,
                state: IndexState::Tombstoned,
                has_roots: false,
                binding: DagBindingState::Bound,
            },
        );
        assert!(!catalog.candidate_reclaimable(&dag, target_op));
    }

    #[test]
    fn operation_idempotency_and_sticky_failures_are_preserved() {
        let (mut catalog, actor, file, op) = setup();
        let stale = Head {
            version_id: None,
            generation: 1,
        };
        assert_eq!(
            catalog.begin_publish(actor, file, op, stale, None),
            Err(CatalogError::HeadConflict)
        );
        assert_eq!(
            catalog.begin_publish(actor, file, op, stale, None),
            Err(CatalogError::HeadConflict)
        );
        let collection_op = catalog.allocate_operation_id().unwrap();
        let collection = catalog
            .create_collection(actor, actor, "once", collection_op)
            .unwrap();
        assert_eq!(
            catalog.create_collection(actor, actor, "once", collection_op),
            Ok(collection)
        );
    }

    #[test]
    fn stale_head_failure_is_sticky_and_operation_views_are_actor_scoped() {
        let (mut catalog, actor, file, op) = setup();
        let expected = catalog.head(actor, file).unwrap();
        catalog
            .begin_publish(actor, file, op, expected, None)
            .unwrap();
        let (mut dag, commit) = commit();
        let fence = register(&mut catalog, actor, op, &mut dag, commit);
        catalog.bind_candidate(actor, op, fence).unwrap();
        catalog.files.get_mut(&file).unwrap().head.generation += 1;
        assert_eq!(
            catalog.publish(actor, op, Some(fence)),
            Err(CatalogError::HeadConflict)
        );
        assert_eq!(
            catalog.publish(actor, op, None),
            Err(CatalogError::HeadConflict)
        );
        let stranger = catalog.create_principal(PrincipalKind::User).unwrap();
        assert_eq!(
            catalog.query_operation(stranger, op),
            Err(CatalogError::PermissionDenied)
        );
    }

    #[test]
    fn unrelated_organization_epoch_change_does_not_abort_publish() {
        let mut catalog = ModelCatalog::new();
        let org_a = catalog
            .create_principal(PrincipalKind::Organization)
            .unwrap();
        let org_b = catalog
            .create_principal(PrincipalKind::Organization)
            .unwrap();
        let writer_b = catalog.create_principal(PrincipalKind::User).unwrap();
        catalog
            .grant_membership(org_b, org_b, writer_b, Capability::ManageMembers)
            .unwrap();
        let collection_op = catalog.allocate_operation_id().unwrap();
        let collection = catalog
            .create_collection(writer_b, org_b, "b", collection_op)
            .unwrap();
        let file_op = catalog.allocate_operation_id().unwrap();
        let file = catalog
            .create_file(writer_b, collection, "b", file_op)
            .unwrap();
        let op = catalog.allocate_operation_id().unwrap();
        catalog
            .begin_publish(
                writer_b,
                file,
                op,
                catalog.head(writer_b, file).unwrap(),
                None,
            )
            .unwrap();
        let (mut dag, commit) = commit();
        let fence = register(&mut catalog, writer_b, op, &mut dag, commit);
        catalog.bind_candidate(writer_b, op, fence).unwrap();
        let unrelated = catalog.create_principal(PrincipalKind::User).unwrap();
        catalog
            .grant_membership(org_a, org_a, unrelated, Capability::Read)
            .unwrap();
        assert!(catalog.publish(writer_b, op, Some(fence)).is_ok());
    }

    #[test]
    fn gc_keeps_shared_multi_level_parent_roots() {
        let (mut dag, first) = commit();
        let snapshot = match dag.get(&first).unwrap() {
            Node::Commit(commit) => commit.snapshot,
            _ => unreachable!(),
        };
        let second = dag
            .insert(Node::Commit(CommitNode {
                snapshot,
                parent: Some(first),
            }))
            .unwrap();
        let third = dag
            .insert(Node::Commit(CommitNode {
                snapshot,
                parent: Some(first),
            }))
            .unwrap();
        let mut catalog = ModelCatalog::new();
        let op = catalog.allocate_operation_id().unwrap();
        let receipt = dag.commit_operation(op, first).unwrap().receipt;
        catalog.handoffs.insert(
            op,
            CatalogHandoff {
                receipt,
                state: IndexState::Tombstoned,
                has_roots: false,
                binding: DagBindingState::Bound,
            },
        );
        catalog.retention_roots.insert(second);
        catalog.reader_pins.insert(third, 1);
        assert!(!catalog.candidate_reclaimable(&dag, op));
    }

    #[test]
    fn system_authority_is_not_a_principal() {
        let mut catalog = ModelCatalog::new();
        let org_a = catalog
            .create_principal(PrincipalKind::Organization)
            .unwrap();
        let manager_a = catalog.create_principal(PrincipalKind::User).unwrap();
        let org_b = catalog
            .create_principal(PrincipalKind::Organization)
            .unwrap();
        let user_b = catalog.create_principal(PrincipalKind::User).unwrap();
        catalog
            .grant_membership(org_a, org_a, manager_a, Capability::ManageMembers)
            .unwrap();
        catalog
            .grant_membership(org_b, org_b, user_b, Capability::Read)
            .unwrap();
        let principal_count = catalog.principals.len();
        let authority = ModelCatalog::system_authority();
        catalog.disable(&authority, user_b).unwrap();
        assert_eq!(catalog.principals.len(), principal_count);
        assert!(catalog.principals.values().all(|principal| {
            matches!(
                principal.kind,
                PrincipalKind::User | PrincipalKind::Organization
            )
        }));
        assert_eq!(
            catalog.principal(user_b).unwrap().state,
            PrincipalState::Disabled
        );
    }
}
