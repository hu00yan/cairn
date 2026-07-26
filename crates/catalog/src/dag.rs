use crate::catalog::{
    CatalogTerminalPermit, CommitId, CommitReceipt, DurableDagReceipt, OperationId,
};
use std::{collections::HashMap, fmt, ops::Range};

pub type NodeId = [u8; 32];

const NODE_DOMAIN: &[u8] = b"cairn/node/v3";
const LOGICAL_BYTES_DOMAIN: &[u8] = b"cairn/logical-bytes/v1";
const MAX_CHILDREN: usize = 256;
const MAX_LEVEL: u8 = 8;
pub const MAX_LOGICAL_FILE_SIZE: u64 = 4 * 1024 * 1024 * 1024;
pub const MAX_CONTENT_NODE_PAYLOAD: usize = 8 * 1024 * 1024;
pub const MAX_SNAPSHOT_EXTENTS: usize = 1_048_576;
const MAX_REFERENCE_RECONSTRUCT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum NodeKind {
    Content = 1,
    ZeroRun = 2,
    RangeMap = 3,
    Snapshot = 4,
    Commit = 5,
}

impl TryFrom<u16> for NodeKind {
    type Error = DagError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Content),
            2 => Ok(Self::ZeroRun),
            3 => Ok(Self::RangeMap),
            4 => Ok(Self::Snapshot),
            5 => Ok(Self::Commit),
            _ => Err(DagError::InvalidKind(value)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContentNode {
    pub bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ZeroRunNode {
    pub len: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeMapEntry {
    pub logical_start: u64,
    pub logical_len: u64,
    pub content_offset: u64,
    pub child_kind: NodeKind,
    pub child: NodeId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeMapNode {
    pub level: u8,
    pub children: Vec<RangeMapEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotNode {
    pub logical_size: u64,
    pub range_map_root: NodeId,
    pub content_digest: NodeId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitNode {
    pub snapshot: NodeId,
    pub parent: Option<NodeId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Node {
    Content(ContentNode),
    ZeroRun(ZeroRunNode),
    RangeMap(RangeMapNode),
    Snapshot(SnapshotNode),
    Commit(CommitNode),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DagError {
    InvalidKind(u16),
    InvalidPayload(&'static str),
    InvalidReference(NodeId),
    ReferenceKind {
        expected: NodeKind,
        actual: NodeKind,
    },
    ArithmeticOverflow,
    MissingNode(NodeId),
    HashMismatch(NodeId),
    InvalidSnapshot(&'static str),
    ResourceLimit(&'static str),
    OperationConflict(OperationId),
}

/// The durable lifecycle of an operation-to-commit binding. A tombstone keeps
/// the operation identity and receipt available for validation while removing
/// the binding as a retention root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DagOperationBindingState {
    Active,
    Tombstoned,
}

#[derive(Clone, Copy, Debug)]
struct OperationBinding {
    commit_id: CommitId,
    state: DagOperationBindingState,
}

impl fmt::Display for DagError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DAG error: {self:?}")
    }
}

impl std::error::Error for DagError {}

impl Node {
    pub fn kind(&self) -> NodeKind {
        match self {
            Self::Content(_) => NodeKind::Content,
            Self::ZeroRun(_) => NodeKind::ZeroRun,
            Self::RangeMap(_) => NodeKind::RangeMap,
            Self::Snapshot(_) => NodeKind::Snapshot,
            Self::Commit(_) => NodeKind::Commit,
        }
    }

    pub fn id(&self) -> Result<NodeId, DagError> {
        let payload = encode_node_payload(self)?;
        Ok(node_id(self.kind(), &payload))
    }
}

pub fn node_id(kind: NodeKind, canonical_payload: &[u8]) -> NodeId {
    let mut h = blake3::Hasher::new();
    h.update(NODE_DOMAIN);
    h.update(&(kind as u16).to_le_bytes());
    h.update(&(canonical_payload.len() as u64).to_le_bytes());
    h.update(canonical_payload);
    *h.finalize().as_bytes()
}

pub fn content_digest(bytes: &[u8]) -> NodeId {
    let mut h = blake3::Hasher::new();
    h.update(LOGICAL_BYTES_DOMAIN);
    h.update(bytes);
    *h.finalize().as_bytes()
}

pub fn encode_node_payload(node: &Node) -> Result<Vec<u8>, DagError> {
    let mut out = Vec::new();
    match node {
        Node::Content(content) => {
            if content.bytes.len() > MAX_CONTENT_NODE_PAYLOAD {
                return Err(DagError::ResourceLimit("Content node payload"));
            }
            out.extend_from_slice(&(content.bytes.len() as u64).to_le_bytes());
            out.extend_from_slice(&content.bytes);
        }
        Node::ZeroRun(run) => {
            if run.len > MAX_LOGICAL_FILE_SIZE {
                return Err(DagError::ResourceLimit("ZeroRun length"));
            }
            out.extend_from_slice(&run.len.to_le_bytes());
        }
        Node::RangeMap(map) => encode_range_map(map, &mut out)?,
        Node::Snapshot(snapshot) => {
            if snapshot.logical_size > MAX_LOGICAL_FILE_SIZE {
                return Err(DagError::ResourceLimit("Snapshot logical size"));
            }
            out.extend_from_slice(&snapshot.logical_size.to_le_bytes());
            out.extend_from_slice(&snapshot.range_map_root);
            out.extend_from_slice(&snapshot.content_digest);
        }
        Node::Commit(commit) => {
            out.extend_from_slice(&commit.snapshot);
            match commit.parent {
                None => out.push(0),
                Some(parent) => {
                    out.push(1);
                    out.extend_from_slice(&parent);
                }
            }
        }
    }
    Ok(out)
}

pub fn decode_node(kind: NodeKind, payload: &[u8]) -> Result<Node, DagError> {
    match kind {
        NodeKind::Content => {
            let len = read_u64(payload, 0)?;
            let len = usize::try_from(len).map_err(|_| DagError::ArithmeticOverflow)?;
            if len > MAX_CONTENT_NODE_PAYLOAD {
                return Err(DagError::ResourceLimit("Content node payload"));
            }
            let end = 8usize
                .checked_add(len)
                .ok_or(DagError::ArithmeticOverflow)?;
            if end != payload.len() {
                return Err(DagError::InvalidPayload("Content length or trailing bytes"));
            }
            Ok(Node::Content(ContentNode {
                bytes: payload[8..].to_vec(),
            }))
        }
        NodeKind::ZeroRun => {
            if payload.len() != 8 {
                return Err(DagError::InvalidPayload("ZeroRun payload length"));
            }
            let len = read_u64(payload, 0)?;
            if len > MAX_LOGICAL_FILE_SIZE {
                return Err(DagError::ResourceLimit("ZeroRun length"));
            }
            Ok(Node::ZeroRun(ZeroRunNode { len }))
        }
        NodeKind::RangeMap => decode_range_map(payload).map(Node::RangeMap),
        NodeKind::Snapshot => {
            if payload.len() != 72 {
                return Err(DagError::InvalidPayload("Snapshot payload length"));
            }
            let logical_size = read_u64(payload, 0)?;
            if logical_size > MAX_LOGICAL_FILE_SIZE {
                return Err(DagError::ResourceLimit("Snapshot logical size"));
            }
            let mut root = [0; 32];
            root.copy_from_slice(&payload[8..40]);
            let mut digest = [0; 32];
            digest.copy_from_slice(&payload[40..72]);
            Ok(Node::Snapshot(SnapshotNode {
                logical_size,
                range_map_root: root,
                content_digest: digest,
            }))
        }
        NodeKind::Commit => {
            if payload.len() != 33 && payload.len() != 65 {
                return Err(DagError::InvalidPayload("Commit payload length"));
            }
            let mut snapshot = [0; 32];
            snapshot.copy_from_slice(&payload[..32]);
            let parent = match payload[32] {
                0 if payload.len() == 33 => None,
                1 if payload.len() == 65 => {
                    let mut id = [0; 32];
                    id.copy_from_slice(&payload[33..]);
                    Some(id)
                }
                _ => {
                    return Err(DagError::InvalidPayload(
                        "Commit parent_present must be 0 or 1",
                    ))
                }
            };
            Ok(Node::Commit(CommitNode { snapshot, parent }))
        }
    }
}

fn encode_range_map(map: &RangeMapNode, out: &mut Vec<u8>) -> Result<(), DagError> {
    validate_map_shape(map)?;
    out.extend_from_slice(&[map.level, u8::from(map.level == 0)]);
    out.extend_from_slice(&(map.children.len() as u32).to_le_bytes());
    for entry in &map.children {
        out.extend_from_slice(&entry.logical_start.to_le_bytes());
        out.extend_from_slice(&entry.logical_len.to_le_bytes());
        out.extend_from_slice(&entry.content_offset.to_le_bytes());
        out.extend_from_slice(&(entry.child_kind as u16).to_le_bytes());
        out.extend_from_slice(&entry.child);
    }
    Ok(())
}

fn decode_range_map(payload: &[u8]) -> Result<RangeMapNode, DagError> {
    if payload.len() < 6 {
        return Err(DagError::InvalidPayload("RangeMap header"));
    }
    let level = payload[0];
    if level > MAX_LEVEL || payload[1] != u8::from(level == 0) {
        return Err(DagError::InvalidPayload("RangeMap level/leaf flag"));
    }
    let count = u32::from_le_bytes(payload[2..6].try_into().unwrap()) as usize;
    if count > MAX_CHILDREN || payload.len() != 6 + count * 58 {
        return Err(DagError::InvalidPayload("RangeMap child count or length"));
    }
    let mut children = Vec::with_capacity(count);
    for index in 0..count {
        let p = 6 + index * 58;
        let kind = NodeKind::try_from(u16::from_le_bytes(
            payload[p + 24..p + 26].try_into().unwrap(),
        ))?;
        let mut child = [0; 32];
        child.copy_from_slice(&payload[p + 26..p + 58]);
        children.push(RangeMapEntry {
            logical_start: read_u64(payload, p)?,
            logical_len: read_u64(payload, p + 8)?,
            content_offset: read_u64(payload, p + 16)?,
            child_kind: kind,
            child,
        });
    }
    let map = RangeMapNode { level, children };
    validate_map_shape(&map)?;
    Ok(map)
}

fn validate_map_shape(map: &RangeMapNode) -> Result<(), DagError> {
    if map.level > MAX_LEVEL || map.children.len() > MAX_CHILDREN {
        return Err(DagError::InvalidPayload("RangeMap bounds"));
    }
    if map.children.is_empty() && map.level != 0 {
        return Err(DagError::InvalidPayload("only level-zero map may be empty"));
    }
    let mut previous_end = None;
    for entry in &map.children {
        if entry.logical_len == 0 || previous_end.is_some_and(|end| entry.logical_start != end) {
            return Err(DagError::InvalidPayload(
                "RangeMap entries must be contiguous, sorted, and non-zero",
            ));
        }
        let end = entry
            .logical_start
            .checked_add(entry.logical_len)
            .ok_or(DagError::ArithmeticOverflow)?;
        previous_end = Some(end);
        let expected = if map.level == 0 {
            matches!(entry.child_kind, NodeKind::Content | NodeKind::ZeroRun)
        } else {
            entry.child_kind == NodeKind::RangeMap
        };
        if !expected
            || (map.level > 0 && entry.content_offset != 0)
            || (entry.child_kind == NodeKind::ZeroRun && entry.content_offset != 0)
        {
            return Err(DagError::InvalidPayload("RangeMap child kind or offset"));
        }
    }
    Ok(())
}

fn map_span(map: &RangeMapNode) -> Option<(u64, u64)> {
    let first = map.children.first()?.logical_start;
    let last = map.children.last()?;
    let end = last.logical_start.checked_add(last.logical_len)?;
    Some((first, end - first))
}

fn read_u64(bytes: &[u8], at: usize) -> Result<u64, DagError> {
    let end = at.checked_add(8).ok_or(DagError::ArithmeticOverflow)?;
    bytes
        .get(at..end)
        .ok_or(DagError::InvalidPayload("truncated integer"))
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
}

#[derive(Clone, Debug, Default)]
pub struct Dag {
    nodes: HashMap<NodeId, Node>,
    /// Durable with the commit in the reference model. This is the sole
    /// operation -> commit authority; catalog only records its handoff.
    operation_commits: HashMap<OperationId, OperationBinding>,
}

impl Dag {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, node: Node) -> Result<NodeId, DagError> {
        let id = node.id()?;
        if let Some(existing) = self.nodes.get(&id) {
            if existing != &node {
                return Err(DagError::HashMismatch(id));
            }
            return Ok(id);
        }
        self.validate_references(&node)?;
        if let Node::Snapshot(snapshot) = &node {
            self.validate_snapshot(snapshot)?;
        }
        self.nodes.insert(id, node);
        Ok(id)
    }

    pub fn get(&self, id: &NodeId) -> Option<&Node> {
        self.nodes.get(id)
    }

    /// Atomically binds an operation to an already validated durable Commit.
    /// The opaque receipt can only be created at this seam.
    pub fn commit_operation(
        &mut self,
        operation_id: OperationId,
        commit_id: CommitId,
    ) -> Result<DurableDagReceipt, DagError> {
        let Node::Commit(commit) = self
            .nodes
            .get(&commit_id)
            .ok_or(DagError::MissingNode(commit_id))?
        else {
            return Err(DagError::InvalidReference(commit_id));
        };
        if Node::Commit(*commit).id()? != commit_id {
            return Err(DagError::HashMismatch(commit_id));
        }
        let Node::Snapshot(snapshot) = self
            .nodes
            .get(&commit.snapshot)
            .ok_or(DagError::MissingNode(commit.snapshot))?
        else {
            return Err(DagError::InvalidReference(commit.snapshot));
        };
        if let Some(existing) = self.operation_commits.get(&operation_id) {
            if existing.commit_id != commit_id {
                return Err(DagError::OperationConflict(operation_id));
            }
        } else {
            self.operation_commits.insert(
                operation_id,
                OperationBinding {
                    commit_id,
                    state: DagOperationBindingState::Active,
                },
            );
        }
        Ok(DurableDagReceipt {
            receipt: CommitReceipt {
                operation_id,
                commit_id,
                snapshot_id: commit.snapshot,
                snapshot_size: snapshot.logical_size,
                snapshot_digest: snapshot.content_digest,
                parent: commit.parent,
            },
        })
    }

    pub(crate) fn receipt_is_active(&self, receipt: &CommitReceipt) -> bool {
        self.operation_binding_receipt(receipt.operation_id)
            .is_some_and(|(expected, state)| {
                state == DagOperationBindingState::Active && expected == *receipt
            })
    }

    /// Look up the durable operation binding after a catalog restart. The
    /// receipt remains opaque because only this DAG seam constructs it.
    pub fn operation_binding(&self, operation_id: OperationId) -> Option<DurableDagReceipt> {
        let binding = self.operation_commits.get(&operation_id)?;
        (binding.state == DagOperationBindingState::Active).then_some(())?;
        let commit_id = binding.commit_id;
        let Node::Commit(commit) = self.nodes.get(&commit_id)? else {
            return None;
        };
        let Node::Snapshot(snapshot) = self.nodes.get(&commit.snapshot)? else {
            return None;
        };
        Some(DurableDagReceipt {
            receipt: CommitReceipt {
                operation_id,
                commit_id,
                snapshot_id: commit.snapshot,
                snapshot_size: snapshot.logical_size,
                snapshot_digest: snapshot.content_digest,
                parent: commit.parent,
            },
        })
    }

    /// Durably tombstones a binding only when the catalog supplies its terminal
    /// handoff as well as the exact receipt that this DAG committed. Tombstoned
    /// bindings stay queryable for recovery/GC validation but no longer retain
    /// their commit.
    #[allow(dead_code)] // Called by the catalog↔DAG recovery coordinator seam.
    pub(crate) fn tombstone_operation(
        &mut self,
        operation_id: OperationId,
        handoff: DurableDagReceipt,
        permit: CatalogTerminalPermit,
    ) -> Result<(), DagError> {
        let receipt = handoff.receipt;
        if receipt.operation_id != operation_id {
            return Err(DagError::OperationConflict(operation_id));
        }
        if !permit.matches(&receipt) {
            return Err(DagError::OperationConflict(operation_id));
        }
        let Some((expected, state)) = self.operation_binding_receipt(operation_id) else {
            return Err(DagError::OperationConflict(operation_id));
        };
        if expected != receipt {
            return Err(DagError::OperationConflict(operation_id));
        }
        if state == DagOperationBindingState::Active {
            self.operation_commits.get_mut(&operation_id).unwrap().state =
                DagOperationBindingState::Tombstoned;
        }
        Ok(())
    }

    pub(crate) fn receipt_is_tombstoned(&self, receipt: &CommitReceipt) -> bool {
        self.operation_binding_receipt(receipt.operation_id)
            .is_some_and(|(expected, state)| {
                state == DagOperationBindingState::Tombstoned && expected == *receipt
            })
    }

    fn operation_binding_receipt(
        &self,
        operation_id: OperationId,
    ) -> Option<(CommitReceipt, DagOperationBindingState)> {
        let binding = self.operation_commits.get(&operation_id)?;
        let Node::Commit(commit) = self.nodes.get(&binding.commit_id)? else {
            return None;
        };
        let Node::Snapshot(snapshot) = self.nodes.get(&commit.snapshot)? else {
            return None;
        };
        Some((
            CommitReceipt {
                operation_id,
                commit_id: binding.commit_id,
                snapshot_id: commit.snapshot,
                snapshot_size: snapshot.logical_size,
                snapshot_digest: snapshot.content_digest,
                parent: commit.parent,
            },
            binding.state,
        ))
    }

    /// Enumerates durable operation bindings with their operation IDs. The
    /// catalog classifies bindings absent from its handoffs as DAG-only and
    /// excludes the candidate's own tombstoned binding at the unbind/reclaim
    /// seam, so it cannot retain itself forever.
    pub fn bound_commit_roots(&self) -> impl Iterator<Item = (OperationId, CommitId)> + '_ {
        self.operation_commits
            .iter()
            .filter_map(|(operation, binding)| {
                (binding.state == DagOperationBindingState::Active)
                    .then_some((*operation, binding.commit_id))
            })
    }

    /// Nodes and operation bindings are both durable at `commit_operation`.
    pub fn crash_reopen(&self) -> Self {
        self.clone()
    }

    /// Tests reachability through the complete durable object closure. `None`
    /// means a root was malformed or unavailable, so callers must retain
    /// conservatively rather than reclaim based on an incomplete traversal.
    pub fn root_closure_contains(
        &self,
        roots: impl IntoIterator<Item = NodeId>,
        target: NodeId,
    ) -> Option<bool> {
        let mut pending: Vec<NodeId> = roots.into_iter().collect();
        let mut visited = std::collections::HashSet::new();
        while let Some(id) = pending.pop() {
            if !visited.insert(id) {
                continue;
            }
            if id == target {
                return Some(true);
            }
            let node = self.nodes.get(&id)?;
            match node {
                Node::Content(_) | Node::ZeroRun(_) => {}
                Node::RangeMap(map) => pending.extend(map.children.iter().map(|entry| entry.child)),
                Node::Snapshot(snapshot) => pending.push(snapshot.range_map_root),
                Node::Commit(commit) => {
                    pending.push(commit.snapshot);
                    if let Some(parent) = commit.parent {
                        pending.push(parent);
                    }
                }
            }
        }
        Some(false)
    }

    pub fn reconstruct(&self, snapshot: &SnapshotNode) -> Result<Vec<u8>, DagError> {
        self.validate_snapshot(snapshot)?;
        let size =
            usize::try_from(snapshot.logical_size).map_err(|_| DagError::ArithmeticOverflow)?;
        if size > MAX_REFERENCE_RECONSTRUCT_BYTES {
            return Err(DagError::ResourceLimit("reference reconstruction output"));
        }
        let mut output = Vec::new();
        let root = self
            .nodes
            .get(&snapshot.range_map_root)
            .ok_or(DagError::MissingNode(snapshot.range_map_root))?;
        if root.id()? != snapshot.range_map_root {
            return Err(DagError::HashMismatch(snapshot.range_map_root));
        }
        self.reconstruct_map(root, 0, snapshot.logical_size, &mut output)?;
        if output.len() as u64 != snapshot.logical_size
            || content_digest(&output) != snapshot.content_digest
        {
            return Err(DagError::InvalidSnapshot(
                "logical size or content digest mismatch",
            ));
        }
        Ok(output)
    }

    /// Builds a new snapshot map from an existing snapshot and disjoint range
    /// patches. Existing leaf nodes are retained by ID; only affected map
    /// entries and newly written Content nodes are created.
    pub fn patch_snapshot(
        &mut self,
        base_snapshot: Option<NodeId>,
        base_len: u64,
        logical_size: u64,
        patches: &[(Range<u64>, &[u8])],
    ) -> Result<NodeId, DagError> {
        if logical_size > MAX_LOGICAL_FILE_SIZE {
            return Err(DagError::InvalidSnapshot("invalid patched Snapshot size"));
        }
        let mut leaves = Vec::new();
        if let Some(snapshot_id) = base_snapshot {
            let Node::Snapshot(snapshot) = self
                .nodes
                .get(&snapshot_id)
                .ok_or(DagError::MissingNode(snapshot_id))?
            else {
                return Err(DagError::InvalidReference(snapshot_id));
            };
            if snapshot.logical_size != base_len {
                return Err(DagError::InvalidSnapshot("base Snapshot size mismatch"));
            }
            self.collect_leaf_entries(snapshot.range_map_root, &mut leaves)?;
        }
        for pair in patches.windows(2) {
            if pair[0].0.end > pair[1].0.start {
                return Err(DagError::InvalidSnapshot("overlapping Snapshot patches"));
            }
        }
        for (range, bytes) in patches {
            if range.start >= range.end
                || range.end > logical_size
                || range.end - range.start != bytes.len() as u64
            {
                return Err(DagError::InvalidSnapshot("invalid Snapshot patch"));
            }
        }
        let patch_nodes = patches
            .iter()
            .map(|(_, bytes)| {
                self.insert(Node::Content(ContentNode {
                    bytes: bytes.to_vec(),
                }))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut boundaries = vec![0, logical_size];
        for leaf in &leaves {
            if leaf.logical_start < logical_size {
                boundaries.push(leaf.logical_start);
                boundaries.push((leaf.logical_start + leaf.logical_len).min(logical_size));
            }
        }
        for (range, _) in patches {
            boundaries.push(range.start);
            boundaries.push(range.end);
        }
        boundaries.sort_unstable();
        boundaries.dedup();
        let mut entries = Vec::new();
        for window in boundaries.windows(2) {
            let start = window[0];
            let end = window[1];
            if start == end {
                continue;
            }
            if let Some((index, (range, _))) = patches
                .iter()
                .enumerate()
                .find(|(_, (range, _))| range.start <= start && start < range.end)
            {
                entries.push(RangeMapEntry {
                    logical_start: start,
                    logical_len: end - start,
                    content_offset: start - range.start,
                    child_kind: NodeKind::Content,
                    child: patch_nodes[index],
                });
            } else if let Some(leaf) = leaves.iter().find(|leaf| {
                leaf.logical_start <= start
                    && start < leaf.logical_start + leaf.logical_len
                    && end <= leaf.logical_start + leaf.logical_len
            }) {
                let (child_kind, child) = if leaf.child_kind == NodeKind::ZeroRun {
                    (
                        NodeKind::ZeroRun,
                        self.insert(Node::ZeroRun(ZeroRunNode { len: end - start }))?,
                    )
                } else {
                    (leaf.child_kind, leaf.child)
                };
                entries.push(RangeMapEntry {
                    logical_start: start,
                    logical_len: end - start,
                    content_offset: if child_kind == NodeKind::Content {
                        leaf.content_offset + start - leaf.logical_start
                    } else {
                        0
                    },
                    child_kind,
                    child,
                });
            } else {
                let child = self.insert(Node::ZeroRun(ZeroRunNode { len: end - start }))?;
                entries.push(RangeMapEntry {
                    logical_start: start,
                    logical_len: end - start,
                    content_offset: 0,
                    child_kind: NodeKind::ZeroRun,
                    child,
                });
            }
        }
        self.build_map_from_entries(entries)
    }

    pub fn digest_range_map(&self, root: NodeId, logical_size: u64) -> Result<NodeId, DagError> {
        let node = self.nodes.get(&root).ok_or(DagError::MissingNode(root))?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(LOGICAL_BYTES_DOMAIN);
        let mut extents = 0;
        self.hash_map(node, 0, logical_size, &mut hasher, &mut extents)?;
        Ok(*hasher.finalize().as_bytes())
    }

    fn collect_leaf_entries(
        &self,
        node_id: NodeId,
        leaves: &mut Vec<RangeMapEntry>,
    ) -> Result<(), DagError> {
        let Node::RangeMap(map) = self
            .nodes
            .get(&node_id)
            .ok_or(DagError::MissingNode(node_id))?
        else {
            return Err(DagError::InvalidReference(node_id));
        };
        for entry in &map.children {
            match entry.child_kind {
                NodeKind::Content | NodeKind::ZeroRun => leaves.push(entry.clone()),
                NodeKind::RangeMap => self.collect_leaf_entries(entry.child, leaves)?,
                _ => return Err(DagError::InvalidSnapshot("invalid leaf map child")),
            }
        }
        Ok(())
    }

    fn build_map_from_entries(
        &mut self,
        mut nodes: Vec<RangeMapEntry>,
    ) -> Result<NodeId, DagError> {
        if nodes.is_empty() {
            return self.insert(Node::RangeMap(RangeMapNode {
                level: 0,
                children: nodes,
            }));
        }
        let mut level = 0;
        while nodes.len() > MAX_CHILDREN {
            let mut parents = Vec::new();
            for group in nodes.chunks(MAX_CHILDREN) {
                let child = self.insert(Node::RangeMap(RangeMapNode {
                    level,
                    children: group.to_vec(),
                }))?;
                let start = group[0].logical_start;
                let end = group.last().unwrap().logical_start + group.last().unwrap().logical_len;
                parents.push(RangeMapEntry {
                    logical_start: start,
                    logical_len: end - start,
                    content_offset: 0,
                    child_kind: NodeKind::RangeMap,
                    child,
                });
            }
            nodes = parents;
            level += 1;
        }
        self.insert(Node::RangeMap(RangeMapNode {
            level,
            children: nodes,
        }))
    }

    /// Reconstructs one bounded logical range directly into caller storage.
    /// The snapshot is validated before reading, but no allocation proportional
    /// to the logical file size is performed.
    pub fn reconstruct_range(
        &self,
        snapshot_id: NodeId,
        range: Range<u64>,
        output: &mut [u8],
    ) -> Result<usize, DagError> {
        let Node::Snapshot(snapshot) = self
            .nodes
            .get(&snapshot_id)
            .ok_or(DagError::MissingNode(snapshot_id))?
        else {
            return Err(DagError::InvalidReference(snapshot_id));
        };
        self.validate_snapshot_shape(snapshot)?;
        if Node::Snapshot(*snapshot).id()? != snapshot_id {
            return Err(DagError::HashMismatch(snapshot_id));
        }
        if range.start > range.end || range.end > snapshot.logical_size {
            return Err(DagError::InvalidSnapshot("range outside Snapshot"));
        }
        let length =
            usize::try_from(range.end - range.start).map_err(|_| DagError::ArithmeticOverflow)?;
        if output.len() < length {
            return Err(DagError::ResourceLimit("range output buffer"));
        }
        if length == 0 {
            return Ok(0);
        }
        let root = self
            .nodes
            .get(&snapshot.range_map_root)
            .ok_or(DagError::MissingNode(snapshot.range_map_root))?;
        let mut written = 0usize;
        self.reconstruct_map_range(root, 0, snapshot.logical_size, &range, output, &mut written)?;
        if written != length {
            return Err(DagError::InvalidSnapshot("range coverage mismatch"));
        }
        Ok(written)
    }

    fn validate_snapshot(&self, snapshot: &SnapshotNode) -> Result<(), DagError> {
        self.validate_snapshot_shape(snapshot)?;
        let root = self
            .nodes
            .get(&snapshot.range_map_root)
            .ok_or(DagError::MissingNode(snapshot.range_map_root))?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(LOGICAL_BYTES_DOMAIN);
        let mut extents = 0;
        self.hash_map(root, 0, snapshot.logical_size, &mut hasher, &mut extents)?;
        if *hasher.finalize().as_bytes() != snapshot.content_digest {
            return Err(DagError::InvalidSnapshot("content digest mismatch"));
        }
        Ok(())
    }

    fn validate_snapshot_shape(&self, snapshot: &SnapshotNode) -> Result<(), DagError> {
        if snapshot.logical_size > MAX_LOGICAL_FILE_SIZE {
            return Err(DagError::ResourceLimit("Snapshot logical size"));
        }
        let root = self
            .nodes
            .get(&snapshot.range_map_root)
            .ok_or(DagError::MissingNode(snapshot.range_map_root))?;
        let Node::RangeMap(root_map) = root else {
            return Err(DagError::ReferenceKind {
                expected: NodeKind::RangeMap,
                actual: root.kind(),
            });
        };
        validate_map_shape(root_map)?;
        if snapshot.logical_size == 0 {
            if root_map.level != 0 || !root_map.children.is_empty() {
                return Err(DagError::InvalidSnapshot("non-canonical empty root"));
            }
        } else if map_span(root_map) != Some((0, snapshot.logical_size)) {
            return Err(DagError::InvalidSnapshot("root does not cover Snapshot"));
        }
        Ok(())
    }

    fn hash_map(
        &self,
        node: &Node,
        expected_start: u64,
        logical_size: u64,
        hasher: &mut blake3::Hasher,
        extents: &mut usize,
    ) -> Result<(), DagError> {
        let Node::RangeMap(map) = node else {
            return Err(DagError::ReferenceKind {
                expected: NodeKind::RangeMap,
                actual: node.kind(),
            });
        };
        let span = map_span(map).unwrap_or((expected_start, 0));
        if span.0 != expected_start
            || span
                .0
                .checked_add(span.1)
                .ok_or(DagError::ArithmeticOverflow)?
                > logical_size
        {
            return Err(DagError::InvalidSnapshot("RangeMap span is invalid"));
        }
        for entry in &map.children {
            let child = self
                .nodes
                .get(&entry.child)
                .ok_or(DagError::MissingNode(entry.child))?;
            match child {
                Node::Content(content) => {
                    *extents = extents
                        .checked_add(1)
                        .ok_or(DagError::ResourceLimit("Snapshot extent count"))?;
                    if *extents > MAX_SNAPSHOT_EXTENTS {
                        return Err(DagError::ResourceLimit("Snapshot extent count"));
                    }
                    let start = usize::try_from(entry.content_offset)
                        .map_err(|_| DagError::ArithmeticOverflow)?;
                    let len = usize::try_from(entry.logical_len)
                        .map_err(|_| DagError::ArithmeticOverflow)?;
                    let end = start.checked_add(len).ok_or(DagError::ArithmeticOverflow)?;
                    let bytes = content
                        .bytes
                        .get(start..end)
                        .ok_or(DagError::InvalidSnapshot("Content range out of bounds"))?;
                    hasher.update(bytes);
                }
                Node::ZeroRun(run) => {
                    *extents = extents
                        .checked_add(1)
                        .ok_or(DagError::ResourceLimit("Snapshot extent count"))?;
                    if *extents > MAX_SNAPSHOT_EXTENTS {
                        return Err(DagError::ResourceLimit("Snapshot extent count"));
                    }
                    if run.len != entry.logical_len {
                        return Err(DagError::InvalidSnapshot("ZeroRun length mismatch"));
                    }
                    let mut remaining = run.len;
                    let zeros = [0u8; 8192];
                    while remaining != 0 {
                        let count = remaining.min(zeros.len() as u64) as usize;
                        hasher.update(&zeros[..count]);
                        remaining -= count as u64;
                    }
                }
                Node::RangeMap(_) => {
                    self.hash_map(child, entry.logical_start, logical_size, hasher, extents)?;
                }
                _ => return Err(DagError::InvalidSnapshot("invalid RangeMap child")),
            }
        }
        Ok(())
    }

    fn validate_references(&self, node: &Node) -> Result<(), DagError> {
        let refs = match node {
            Node::RangeMap(map) => map
                .children
                .iter()
                .map(|e| (e.child, e.child_kind))
                .collect::<Vec<_>>(),
            Node::Snapshot(s) => vec![(s.range_map_root, NodeKind::RangeMap)],
            Node::Commit(c) => {
                let mut v = vec![(c.snapshot, NodeKind::Snapshot)];
                if let Some(p) = c.parent {
                    v.push((p, NodeKind::Commit));
                }
                v
            }
            _ => Vec::new(),
        };
        for (id, expected) in refs {
            let child = self.nodes.get(&id).ok_or(DagError::MissingNode(id))?;
            if child.kind() != expected {
                return Err(DagError::ReferenceKind {
                    expected,
                    actual: child.kind(),
                });
            }
        }
        if let Node::RangeMap(map) = node {
            for entry in &map.children {
                let child = self
                    .nodes
                    .get(&entry.child)
                    .ok_or(DagError::MissingNode(entry.child))?;
                match (map.level, child) {
                    (0, Node::Content(content)) => {
                        let end = entry
                            .content_offset
                            .checked_add(entry.logical_len)
                            .ok_or(DagError::ArithmeticOverflow)?;
                        if end > content.bytes.len() as u64 {
                            return Err(DagError::InvalidSnapshot(
                                "Content range exceeds node length",
                            ));
                        }
                    }
                    (0, Node::ZeroRun(run)) if run.len == entry.logical_len => {}
                    (0, Node::ZeroRun(_)) => {
                        return Err(DagError::InvalidSnapshot("ZeroRun length mismatch"))
                    }
                    (level, Node::RangeMap(child_map))
                        if level > 0 && child_map.level + 1 == level =>
                    {
                        if map_span(child_map) != Some((entry.logical_start, entry.logical_len)) {
                            return Err(DagError::InvalidSnapshot("child RangeMap span mismatch"));
                        }
                    }
                    _ => {
                        return Err(DagError::InvalidSnapshot(
                            "RangeMap child level or kind mismatch",
                        ))
                    }
                }
            }
        }
        Ok(())
    }

    fn reconstruct_map(
        &self,
        node: &Node,
        expected_start: u64,
        logical_size: u64,
        out: &mut Vec<u8>,
    ) -> Result<(), DagError> {
        let Node::RangeMap(map) = node else {
            return Err(DagError::ReferenceKind {
                expected: NodeKind::RangeMap,
                actual: node.kind(),
            });
        };
        validate_map_shape(map)?;
        let mut next = expected_start;
        for entry in &map.children {
            if entry.logical_start != next
                || entry
                    .logical_start
                    .checked_add(entry.logical_len)
                    .ok_or(DagError::ArithmeticOverflow)?
                    > logical_size
            {
                return Err(DagError::InvalidSnapshot(
                    "RangeMap does not cover Snapshot",
                ));
            }
            next = entry
                .logical_start
                .checked_add(entry.logical_len)
                .ok_or(DagError::ArithmeticOverflow)?;
            match self
                .nodes
                .get(&entry.child)
                .ok_or(DagError::MissingNode(entry.child))?
            {
                Node::ZeroRun(run) => {
                    if entry.logical_len != run.len {
                        return Err(DagError::InvalidSnapshot("ZeroRun length mismatch"));
                    }
                    out.resize(out.len() + run.len as usize, 0);
                }
                Node::Content(content) => {
                    let end = entry
                        .content_offset
                        .checked_add(entry.logical_len)
                        .ok_or(DagError::ArithmeticOverflow)?;
                    if end > content.bytes.len() as u64 {
                        return Err(DagError::InvalidSnapshot("Content offset out of bounds"));
                    }
                    out.extend_from_slice(
                        &content.bytes[entry.content_offset as usize..end as usize],
                    );
                }
                child => self.reconstruct_map(child, entry.logical_start, logical_size, out)?,
            }
        }
        Ok(())
    }

    fn reconstruct_map_range(
        &self,
        node: &Node,
        expected_start: u64,
        expected_end: u64,
        range: &Range<u64>,
        output: &mut [u8],
        written: &mut usize,
    ) -> Result<(), DagError> {
        let Node::RangeMap(map) = node else {
            return Err(DagError::ReferenceKind {
                expected: NodeKind::RangeMap,
                actual: node.kind(),
            });
        };
        validate_map_shape(map)?;
        if map_span(map) != Some((expected_start, expected_end - expected_start)) {
            return Err(DagError::InvalidSnapshot("RangeMap span is invalid"));
        }
        let mut next = expected_start;
        for entry in &map.children {
            let entry_end = entry
                .logical_start
                .checked_add(entry.logical_len)
                .ok_or(DagError::ArithmeticOverflow)?;
            if entry.logical_start != next || entry_end > expected_end {
                return Err(DagError::InvalidSnapshot(
                    "RangeMap does not cover requested Snapshot span",
                ));
            }
            next = entry_end;
            let overlap_start = range.start.max(entry.logical_start);
            let overlap_end = range.end.min(entry_end);
            if overlap_start >= overlap_end {
                continue;
            }
            let destination = usize::try_from(overlap_start - range.start)
                .map_err(|_| DagError::ArithmeticOverflow)?;
            let count = usize::try_from(overlap_end - overlap_start)
                .map_err(|_| DagError::ArithmeticOverflow)?;
            let destination_end = destination
                .checked_add(count)
                .ok_or(DagError::ArithmeticOverflow)?;
            let child = self
                .nodes
                .get(&entry.child)
                .ok_or(DagError::MissingNode(entry.child))?;
            if child.id()? != entry.child {
                return Err(DagError::HashMismatch(entry.child));
            }
            match child {
                Node::ZeroRun(run) => {
                    if run.len != entry.logical_len {
                        return Err(DagError::InvalidSnapshot("ZeroRun length mismatch"));
                    }
                    output[destination..destination_end].fill(0);
                    *written = written
                        .checked_add(count)
                        .ok_or(DagError::ArithmeticOverflow)?;
                }
                Node::Content(content) => {
                    let content_start = entry
                        .content_offset
                        .checked_add(overlap_start - entry.logical_start)
                        .ok_or(DagError::ArithmeticOverflow)?;
                    let content_end = content_start
                        .checked_add(count as u64)
                        .ok_or(DagError::ArithmeticOverflow)?;
                    if content_end > content.bytes.len() as u64 {
                        return Err(DagError::InvalidSnapshot("Content offset out of bounds"));
                    }
                    output[destination..destination_end].copy_from_slice(
                        &content.bytes[content_start as usize..content_end as usize],
                    );
                    *written = written
                        .checked_add(count)
                        .ok_or(DagError::ArithmeticOverflow)?;
                }
                child => self.reconstruct_map_range(
                    child,
                    entry.logical_start,
                    entry_end,
                    range,
                    output,
                    written,
                )?,
            }
        }
        if next != expected_end {
            return Err(DagError::InvalidSnapshot(
                "RangeMap does not cover requested Snapshot span",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_commit(dag: &mut Dag) -> NodeId {
        let map = dag
            .insert(Node::RangeMap(RangeMapNode {
                level: 0,
                children: Vec::new(),
            }))
            .unwrap();
        let snapshot = dag
            .insert(Node::Snapshot(SnapshotNode {
                logical_size: 0,
                range_map_root: map,
                content_digest: content_digest(&[]),
            }))
            .unwrap();
        dag.insert(Node::Commit(CommitNode {
            snapshot,
            parent: None,
        }))
        .unwrap()
    }

    #[test]
    fn operation_bindings_allow_content_addressed_commit_reuse() {
        let mut dag = Dag::new();
        let mut catalog = crate::catalog::ModelCatalog::new();
        let first = empty_commit(&mut dag);
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
        let operation = catalog.allocate_operation_id().unwrap();
        assert!(dag.commit_operation(operation, first).is_ok());
        assert!(dag.commit_operation(operation, first).is_ok());
        assert!(matches!(
            dag.commit_operation(operation, second),
            Err(DagError::OperationConflict(_))
        ));
        let other_operation = catalog.allocate_operation_id().unwrap();
        assert!(dag.commit_operation(other_operation, first).is_ok());
    }

    #[test]
    fn operation_binding_rejects_non_commit_and_missing_snapshot() {
        let mut dag = Dag::new();
        let mut catalog = crate::catalog::ModelCatalog::new();
        let map = dag
            .insert(Node::RangeMap(RangeMapNode {
                level: 0,
                children: Vec::new(),
            }))
            .unwrap();
        assert!(matches!(
            dag.commit_operation(catalog.allocate_operation_id().unwrap(), map),
            Err(DagError::InvalidReference(_))
        ));
        let missing_snapshot = [9; 32];
        let commit = Node::Commit(CommitNode {
            snapshot: missing_snapshot,
            parent: None,
        });
        let commit_id = commit.id().unwrap();
        dag.nodes.insert(commit_id, commit);
        assert!(matches!(
            dag.commit_operation(catalog.allocate_operation_id().unwrap(), commit_id),
            Err(DagError::MissingNode(id)) if id == missing_snapshot
        ));
    }

    #[test]
    fn operation_binding_survives_clone_reopen() {
        let mut dag = Dag::new();
        let mut catalog = crate::catalog::ModelCatalog::new();
        let commit = empty_commit(&mut dag);
        let operation = catalog.allocate_operation_id().unwrap();
        let receipt = dag.commit_operation(operation, commit).unwrap();
        let reopened = dag.crash_reopen();
        assert_eq!(reopened.operation_binding(operation), Some(receipt));
    }

    #[test]
    fn content_payload_vectors_are_canonical() {
        let empty = encode_node_payload(&Node::Content(ContentNode { bytes: Vec::new() })).unwrap();
        assert_eq!(empty, 0u64.to_le_bytes());
        let abc = encode_node_payload(&Node::Content(ContentNode {
            bytes: b"abc".to_vec(),
        }))
        .unwrap();
        assert_eq!(abc, b"\x03\0\0\0\0\0\0\0abc");
    }

    #[test]
    fn empty_range_map_is_six_bytes() {
        let payload = encode_node_payload(&Node::RangeMap(RangeMapNode {
            level: 0,
            children: Vec::new(),
        }))
        .unwrap();
        assert_eq!(payload, b"\0\x01\0\0\0\0");
        assert_eq!(
            decode_node(NodeKind::RangeMap, &payload).unwrap(),
            Node::RangeMap(RangeMapNode {
                level: 0,
                children: Vec::new()
            })
        );
    }

    #[test]
    fn invalid_encodings_are_rejected() {
        assert!(decode_node(NodeKind::Content, &[1, 0, 0, 0, 0, 0, 0, 0]).is_err());
        assert!(decode_node(NodeKind::RangeMap, b"\0\0\0\0\0\0").is_err());
        assert!(decode_node(NodeKind::RangeMap, b"\x01\0\0\0\0\0").is_err());
        assert!(decode_node(NodeKind::ZeroRun, &u64::MAX.to_le_bytes()).is_err());
        let mut commit = vec![0; 33];
        commit[32] = 2;
        assert!(decode_node(NodeKind::Commit, &commit).is_err());
        let bad = RangeMapNode {
            level: 0,
            children: vec![
                RangeMapEntry {
                    logical_start: 0,
                    logical_len: 1,
                    content_offset: 0,
                    child_kind: NodeKind::Content,
                    child: [0; 32],
                },
                RangeMapEntry {
                    logical_start: 2,
                    logical_len: 1,
                    content_offset: 0,
                    child_kind: NodeKind::Content,
                    child: [0; 32],
                },
            ],
        };
        assert!(encode_node_payload(&Node::RangeMap(bad)).is_err());
    }

    #[test]
    fn snapshot_limits_are_checked_before_reconstruction() {
        let empty = Node::RangeMap(RangeMapNode {
            level: 0,
            children: Vec::new(),
        });
        let empty_id = empty.id().unwrap();
        let mut dag = Dag::new();
        dag.insert(empty).unwrap();
        let snapshot = SnapshotNode {
            logical_size: MAX_LOGICAL_FILE_SIZE + 1,
            range_map_root: empty_id,
            content_digest: content_digest(&[]),
        };
        assert!(matches!(
            dag.reconstruct(&snapshot),
            Err(DagError::ResourceLimit("Snapshot logical size"))
        ));
        let mut payload = [0u8; 72];
        payload[..8].copy_from_slice(&(MAX_LOGICAL_FILE_SIZE + 1).to_le_bytes());
        assert!(matches!(
            decode_node(NodeKind::Snapshot, &payload),
            Err(DagError::ResourceLimit("Snapshot logical size"))
        ));
    }

    #[test]
    fn range_map_reconstructs_and_hashes_logical_bytes() {
        let content = Node::Content(ContentNode {
            bytes: b"hello".to_vec(),
        });
        let content_id = content.id().unwrap();
        let zeros = Node::ZeroRun(ZeroRunNode { len: 2 });
        let zeros_id = zeros.id().unwrap();
        let map = Node::RangeMap(RangeMapNode {
            level: 0,
            children: vec![
                RangeMapEntry {
                    logical_start: 0,
                    logical_len: 2,
                    content_offset: 1,
                    child_kind: NodeKind::Content,
                    child: content_id,
                },
                RangeMapEntry {
                    logical_start: 2,
                    logical_len: 2,
                    content_offset: 0,
                    child_kind: NodeKind::ZeroRun,
                    child: zeros_id,
                },
                RangeMapEntry {
                    logical_start: 4,
                    logical_len: 4,
                    content_offset: 0,
                    child_kind: NodeKind::Content,
                    child: content_id,
                },
            ],
        });
        let map_id = map.id().unwrap();
        let bytes = b"el\0\0hell";
        let snapshot = SnapshotNode {
            logical_size: bytes.len() as u64,
            range_map_root: map_id,
            content_digest: content_digest(bytes),
        };
        let mut dag = Dag::new();
        dag.insert(content).unwrap();
        dag.insert(zeros).unwrap();
        dag.insert(map).unwrap();
        assert_eq!(dag.reconstruct(&snapshot).unwrap(), bytes);
    }

    #[test]
    fn bounded_reconstruct_range_matches_full_reconstruction() {
        let content = Node::Content(ContentNode {
            bytes: b"0123456789abcdef".to_vec(),
        });
        let content_id = content.id().unwrap();
        let map = Node::RangeMap(RangeMapNode {
            level: 0,
            children: vec![RangeMapEntry {
                logical_start: 0,
                logical_len: 16,
                content_offset: 0,
                child_kind: NodeKind::Content,
                child: content_id,
            }],
        });
        let map_id = map.id().unwrap();
        let snapshot = Node::Snapshot(SnapshotNode {
            logical_size: 16,
            range_map_root: map_id,
            content_digest: content_digest(b"0123456789abcdef"),
        });
        let mut dag = Dag::new();
        dag.insert(content).unwrap();
        dag.insert(map).unwrap();
        let snapshot_id = dag.insert(snapshot).unwrap();
        let full = dag
            .reconstruct(match dag.get(&snapshot_id).unwrap() {
                Node::Snapshot(snapshot) => snapshot,
                _ => unreachable!(),
            })
            .unwrap();

        for start in 0..=16 {
            for end in start..=16 {
                let mut output = vec![0xa5; (end - start) as usize];
                assert_eq!(
                    dag.reconstruct_range(snapshot_id, start..end, &mut output)
                        .unwrap(),
                    (end - start) as usize
                );
                assert_eq!(output, full[start as usize..end as usize]);
            }
        }
    }

    #[test]
    fn nested_range_map_preserves_snapshot_global_coordinates() {
        let content = Node::Content(ContentNode {
            bytes: b"abcdef".to_vec(),
        });
        let content_id = content.id().unwrap();
        let child = Node::RangeMap(RangeMapNode {
            level: 0,
            children: vec![RangeMapEntry {
                logical_start: 4,
                logical_len: 2,
                content_offset: 1,
                child_kind: NodeKind::Content,
                child: content_id,
            }],
        });
        let child_id = child.id().unwrap();
        let zeros = Node::ZeroRun(ZeroRunNode { len: 4 });
        let zeros_id = zeros.id().unwrap();
        let mut dag = Dag::new();
        dag.insert(content).unwrap();
        dag.insert(child).unwrap();
        // A level-one root may only contain RangeMaps; represent the leading
        // zeros with a level-zero RangeMap leaf.
        let zero_leaf = Node::RangeMap(RangeMapNode {
            level: 0,
            children: vec![RangeMapEntry {
                logical_start: 0,
                logical_len: 4,
                content_offset: 0,
                child_kind: NodeKind::ZeroRun,
                child: zeros_id,
            }],
        });
        let zero_leaf_id = zero_leaf.id().unwrap();
        let root = Node::RangeMap(RangeMapNode {
            level: 1,
            children: vec![
                RangeMapEntry {
                    logical_start: 0,
                    logical_len: 4,
                    content_offset: 0,
                    child_kind: NodeKind::RangeMap,
                    child: zero_leaf_id,
                },
                RangeMapEntry {
                    logical_start: 4,
                    logical_len: 2,
                    content_offset: 0,
                    child_kind: NodeKind::RangeMap,
                    child: child_id,
                },
            ],
        });
        let root_id = root.id().unwrap();
        dag.insert(zeros).unwrap();
        dag.insert(zero_leaf).unwrap();
        dag.insert(root).unwrap();
        let bytes = vec![0, 0, 0, 0, b'b', b'c'];
        let snapshot = SnapshotNode {
            logical_size: 6,
            range_map_root: root_id,
            content_digest: content_digest(&bytes),
        };
        assert_eq!(dag.reconstruct(&snapshot).unwrap(), bytes);
    }

    #[test]
    fn commit_parent_flag_is_canonical() {
        let snapshot = [7; 32];
        let none = encode_node_payload(&Node::Commit(CommitNode {
            snapshot,
            parent: None,
        }))
        .unwrap();
        assert_eq!(none.len(), 33);
        assert_eq!(none[32], 0);
        let parent = encode_node_payload(&Node::Commit(CommitNode {
            snapshot,
            parent: Some([8; 32]),
        }))
        .unwrap();
        assert_eq!(parent.len(), 65);
        assert_eq!(parent[32], 1);
        assert!(decode_node(NodeKind::Commit, &[&snapshot[..], &[0], &[9]].concat()).is_err());
    }
}
