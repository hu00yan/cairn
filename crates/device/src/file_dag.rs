use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};

use blake3::Hasher;
use cairn_catalog::dag::{content_digest, MAX_CONTENT_NODE_PAYLOAD, MAX_SNAPSHOT_EXTENTS};
use cairn_catalog::{
    decode_node, encode_node_payload, CommitId, CommitNode, ContentNode, DagError, Node, NodeId,
    NodeKind, OperationId, RangeMapEntry, RangeMapNode, SnapshotNode,
};

use crate::io::{BlockDevice, DeviceError};

/// The fixed on-device header: kind (u16), payload length (u32), checksum (32).
pub const RECORD_HEADER_LEN: usize = 2 + 4 + 32;
const RECORD_FOOTER_MAGIC: [u8; 8] = *b"CAIRNEND";
const RECORD_FOOTER_LEN: usize = 8 + 8 + 32;
const BINDING_KIND: u16 = 0x100;
const TOMBSTONE_KIND: u16 = 0x101;
const MAX_RECORD_PAYLOAD: usize = 16 * 1024 * 1024;
const MAX_SNAPSHOT_READ_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordKind {
    Node(NodeKind),
    OperationBinding,
    OperationTombstone,
}

impl RecordKind {
    fn code(self) -> u16 {
        match self {
            Self::Node(kind) => kind as u16,
            Self::OperationBinding => BINDING_KIND,
            Self::OperationTombstone => TOMBSTONE_KIND,
        }
    }

    fn decode(code: u16) -> Result<Self, FileDagStoreError> {
        if code == BINDING_KIND {
            return Ok(Self::OperationBinding);
        }
        if code == TOMBSTONE_KIND {
            return Ok(Self::OperationTombstone);
        }
        NodeKind::try_from(code)
            .map(Self::Node)
            .map_err(FileDagStoreError::Dag)
    }
}

#[derive(Debug)]
pub enum FileDagStoreError {
    Device(DeviceError),
    Dag(DagError),
    MissingNode(NodeId),
    InvalidReference(NodeId),
    InvalidSnapshot(&'static str),
    RangeOutOfBounds { start: u64, end: u64, size: u64 },
    ResourceLimit(&'static str),
    CorruptRecord { offset: u64, reason: &'static str },
    RecordTooLarge(usize),
    CapacityExhausted { needed: u64, remaining: u64 },
    OperationConflict(u64),
}

#[derive(Clone, Copy, Debug)]
struct OperationBinding {
    commit_id: CommitId,
}

/// A verified immutable snapshot descriptor safe for a coordinator boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerifiedSnapshot {
    pub commit_id: CommitId,
    pub logical_size: u64,
    pub content_digest: NodeId,
}

impl std::fmt::Display for FileDagStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "file DAG store error: {self:?}")
    }
}

impl std::error::Error for FileDagStoreError {}

impl From<DeviceError> for FileDagStoreError {
    fn from(error: DeviceError) -> Self {
        Self::Device(error)
    }
}

impl From<DagError> for FileDagStoreError {
    fn from(error: DagError) -> Self {
        Self::Dag(error)
    }
}

/// An append-only record log over a fixed-capacity [`BlockDevice`].
///
/// A scan stops at an all-zero header or an incomplete final record. A complete
/// record with an invalid kind, length, checksum, or DAG hash is rejected.
pub struct FileDagStore<D> {
    device: D,
    next_offset: u64,
    nodes: HashMap<NodeId, u64>,
    bindings: HashMap<u64, OperationBinding>,
    tombstones: HashMap<u64, CommitId>,
    validated_snapshots: HashSet<NodeId>,
}

impl<D: BlockDevice> FileDagStore<D> {
    pub fn open(device: D) -> Result<Self, FileDagStoreError> {
        let mut store = Self {
            next_offset: 0,
            device,
            nodes: HashMap::new(),
            bindings: HashMap::new(),
            tombstones: HashMap::new(),
            validated_snapshots: HashSet::new(),
        };
        store.scan()?;
        Ok(store)
    }

    pub fn append_node(&mut self, node: &Node) -> Result<NodeId, FileDagStoreError> {
        let payload = encode_node_payload(node)?;
        let id = node.id()?;
        if let Some(&offset) = self.nodes.get(&id) {
            let existing = self.read_node_at(offset)?;
            if existing != *node {
                return Err(FileDagStoreError::Dag(DagError::HashMismatch(id)));
            }
            return Ok(id);
        }
        self.append_record(RecordKind::Node(node.kind()), &payload)?;
        let offset = self.next_offset - record_size(payload.len()) as u64;
        self.nodes.insert(id, offset);
        Ok(id)
    }

    /// Appends a complete immutable snapshot and its commit in dependency order.
    /// This is intentionally a narrow builder for the single-node coordinator;
    /// patching can later preserve unaffected leaves instead of materializing bytes.
    pub fn append_snapshot(
        &mut self,
        bytes: &[u8],
        parent: Option<CommitId>,
    ) -> Result<VerifiedSnapshot, FileDagStoreError> {
        let mut children = Vec::new();
        for (index, chunk) in bytes.chunks(MAX_CONTENT_NODE_PAYLOAD).enumerate() {
            let child = self.append_node(&Node::Content(ContentNode {
                bytes: chunk.to_vec(),
            }))?;
            children.push(RangeMapEntry {
                logical_start: (index * MAX_CONTENT_NODE_PAYLOAD) as u64,
                logical_len: chunk.len() as u64,
                content_offset: 0,
                child_kind: NodeKind::Content,
                child,
            });
        }
        let range_map = self.append_node(&Node::RangeMap(RangeMapNode { level: 0, children }))?;
        let digest = content_digest(bytes);
        let snapshot = self.append_node(&Node::Snapshot(SnapshotNode {
            logical_size: bytes.len() as u64,
            range_map_root: range_map,
            content_digest: digest,
        }))?;
        let commit = self.append_node(&Node::Commit(CommitNode { snapshot, parent }))?;
        Ok(VerifiedSnapshot {
            commit_id: commit,
            logical_size: bytes.len() as u64,
            content_digest: digest,
        })
    }

    pub fn bind_operation(
        &mut self,
        operation_id: OperationId,
        commit_id: CommitId,
    ) -> Result<(), FileDagStoreError> {
        self.validate_commit(commit_id)?;
        if self.tombstones.contains_key(&operation_id.get()) {
            return Err(FileDagStoreError::OperationConflict(operation_id.get()));
        }
        if let Some(existing) = self.bindings.get(&operation_id.get()) {
            if existing.commit_id != commit_id {
                return Err(FileDagStoreError::OperationConflict(operation_id.get()));
            }
            return Ok(());
        }
        let mut payload = Vec::with_capacity(40);
        payload.extend_from_slice(&operation_id.get().to_le_bytes());
        payload.extend_from_slice(&commit_id);
        self.append_record(RecordKind::OperationBinding, &payload)?;
        self.bindings
            .insert(operation_id.get(), OperationBinding { commit_id });
        Ok(())
    }

    /// Appends a durable terminal marker for an active operation binding.
    /// The marker is sticky: it remains queryable after reopen, but the
    /// operation no longer contributes an active commit root.
    pub fn tombstone_operation(
        &mut self,
        operation_id: OperationId,
        commit_id: CommitId,
    ) -> Result<(), FileDagStoreError> {
        self.validate_commit(commit_id)?;
        if let Some(&existing) = self.tombstones.get(&operation_id.get()) {
            return (existing == commit_id)
                .then_some(())
                .ok_or(FileDagStoreError::OperationConflict(operation_id.get()));
        }
        let Some(existing) = self.bindings.get(&operation_id.get()) else {
            return Err(FileDagStoreError::OperationConflict(operation_id.get()));
        };
        if existing.commit_id != commit_id {
            return Err(FileDagStoreError::OperationConflict(operation_id.get()));
        }
        let mut payload = Vec::with_capacity(40);
        payload.extend_from_slice(&operation_id.get().to_le_bytes());
        payload.extend_from_slice(&commit_id);
        self.append_record(RecordKind::OperationTombstone, &payload)?;
        self.bindings.remove(&operation_id.get());
        self.tombstones.insert(operation_id.get(), commit_id);
        Ok(())
    }

    pub fn node(&mut self, id: &NodeId) -> Result<Option<Node>, FileDagStoreError> {
        self.nodes
            .get(id)
            .copied()
            .map(|offset| self.read_node_at(offset))
            .transpose()
    }

    /// Reads a bounded logical range from a durable commit.
    ///
    /// Only the requested range is materialized.  The commit, snapshot, map
    /// path, and every leaf intersecting the range are re-read and validated
    /// from the device on each call; this deliberately does not trust the
    /// in-memory index as content validation.
    pub fn read_snapshot_range(
        &mut self,
        commit_id: CommitId,
        range: Range<u64>,
    ) -> Result<Vec<u8>, FileDagStoreError> {
        if range.start > range.end {
            return Err(FileDagStoreError::RangeOutOfBounds {
                start: range.start,
                end: range.end,
                size: 0,
            });
        }
        let commit = self.read_required_node(commit_id)?;
        let Node::Commit(commit) = commit else {
            return Err(FileDagStoreError::InvalidReference(commit_id));
        };
        let snapshot_id = commit.snapshot;
        let snapshot = self.read_required_node(snapshot_id)?;
        let Node::Snapshot(snapshot) = snapshot else {
            return Err(FileDagStoreError::InvalidReference(snapshot_id));
        };
        if range.end > snapshot.logical_size {
            return Err(FileDagStoreError::RangeOutOfBounds {
                start: range.start,
                end: range.end,
                size: snapshot.logical_size,
            });
        }
        let length = usize::try_from(range.end - range.start)
            .map_err(|_| FileDagStoreError::ResourceLimit("snapshot range length"))?;
        if length > MAX_SNAPSHOT_READ_BYTES {
            return Err(FileDagStoreError::ResourceLimit("snapshot range length"));
        }
        self.validate_snapshot_digest(snapshot_id, &snapshot)?;
        let root = self.read_required_node(snapshot.range_map_root)?;
        let Node::RangeMap(root) = root else {
            return Err(FileDagStoreError::InvalidReference(snapshot.range_map_root));
        };
        validate_map_root(&root, snapshot.logical_size)?;

        let mut output = vec![0; length];
        let mut budget = ReadBudget::default();
        if length != 0 {
            self.read_map_range(
                root,
                snapshot.logical_size,
                0..snapshot.logical_size,
                range,
                &mut output,
                &mut budget,
            )?;
        }
        Ok(output)
    }

    pub fn verified_snapshot(
        &mut self,
        commit_id: CommitId,
    ) -> Result<VerifiedSnapshot, FileDagStoreError> {
        let commit = self.read_required_node(commit_id)?;
        let Node::Commit(commit_node) = commit else {
            return Err(FileDagStoreError::InvalidReference(commit_id));
        };
        let snapshot_id = commit_node.snapshot;
        let snapshot = self.read_required_node(snapshot_id)?;
        let Node::Snapshot(snapshot_node) = snapshot else {
            return Err(FileDagStoreError::InvalidReference(snapshot_id));
        };
        self.validate_snapshot_digest(snapshot_id, &snapshot_node)?;
        Ok(VerifiedSnapshot {
            commit_id,
            logical_size: snapshot_node.logical_size,
            content_digest: snapshot_node.content_digest,
        })
    }

    pub fn operation_binding(&self, operation_id: OperationId) -> Option<CommitId> {
        self.bindings
            .get(&operation_id.get())
            .map(|binding| binding.commit_id)
    }

    pub fn operation_tombstone(&self, operation_id: OperationId) -> Option<CommitId> {
        self.tombstones.get(&operation_id.get()).copied()
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn into_inner(self) -> D {
        self.device
    }

    fn append_record(&mut self, kind: RecordKind, payload: &[u8]) -> Result<(), FileDagStoreError> {
        if payload.len() > MAX_RECORD_PAYLOAD {
            return Err(FileDagStoreError::RecordTooLarge(payload.len()));
        }
        let size = record_size(payload.len()) as u64;
        let remaining = self.device.len().saturating_sub(self.next_offset);
        if size > remaining {
            return Err(FileDagStoreError::CapacityExhausted {
                needed: size,
                remaining,
            });
        }
        let header = encode_header(kind, payload);
        self.device.write_at(self.next_offset, &header)?;
        self.device
            .write_at(self.next_offset + RECORD_HEADER_LEN as u64, payload)?;
        self.device.flush_data()?;
        let footer = encode_footer(kind, payload);
        self.device.write_at(
            self.next_offset + RECORD_HEADER_LEN as u64 + payload.len() as u64,
            &footer,
        )?;
        self.device.flush_data()?;
        self.device.write_at(
            self.next_offset + RECORD_HEADER_LEN as u64 + payload.len() as u64,
            &RECORD_FOOTER_MAGIC,
        )?;
        self.device.flush_data()?;
        self.next_offset += size;
        Ok(())
    }

    fn scan(&mut self) -> Result<(), FileDagStoreError> {
        while self.next_offset + RECORD_HEADER_LEN as u64 <= self.device.len() {
            let offset = self.next_offset;
            let mut header = [0; RECORD_HEADER_LEN];
            self.device.read_at(offset, &mut header)?;
            if header.iter().all(|byte| *byte == 0) {
                break;
            }
            let kind_code = u16::from_le_bytes([header[0], header[1]]);
            let len = u32::from_le_bytes(header[2..6].try_into().unwrap()) as usize;
            if len == 0 {
                return Err(FileDagStoreError::CorruptRecord {
                    offset,
                    reason: "zero-length record",
                });
            }
            if len > MAX_RECORD_PAYLOAD {
                return Err(FileDagStoreError::CorruptRecord {
                    offset,
                    reason: "payload too large",
                });
            }
            let total = record_size(len) as u64;
            if total > self.device.len() - offset {
                break;
            }
            let mut payload = vec![0; len];
            self.device
                .read_at(offset + RECORD_HEADER_LEN as u64, &mut payload)?;
            let footer_offset = offset + RECORD_HEADER_LEN as u64 + len as u64;
            let mut footer = [0; RECORD_FOOTER_LEN];
            self.device.read_at(footer_offset, &mut footer)?;
            if !validate_footer(offset, kind_code, &payload, &footer)? {
                break;
            }
            let kind = RecordKind::decode(kind_code)?;
            if checksum(kind_code, &payload).as_slice() != &header[6..] {
                return Err(FileDagStoreError::CorruptRecord {
                    offset,
                    reason: "checksum mismatch",
                });
            }
            self.index_record(offset, kind, &payload)?;
            self.next_offset += total;
        }
        Ok(())
    }

    fn index_record(
        &mut self,
        offset: u64,
        kind: RecordKind,
        payload: &[u8],
    ) -> Result<(), FileDagStoreError> {
        match kind {
            RecordKind::Node(node_kind) => {
                let node = decode_node(node_kind, payload)?;
                let id = node.id()?;
                self.nodes.insert(id, offset);
            }
            RecordKind::OperationBinding => {
                if payload.len() != 40 {
                    return Err(FileDagStoreError::CorruptRecord {
                        offset,
                        reason: "binding payload length",
                    });
                }
                let operation_id = u64::from_le_bytes(payload[..8].try_into().unwrap());
                let mut commit_id = [0; 32];
                commit_id.copy_from_slice(&payload[8..]);
                self.validate_commit(commit_id)?;
                if self.tombstones.contains_key(&operation_id) {
                    return Err(FileDagStoreError::OperationConflict(operation_id));
                }
                if let Some(existing) = self.bindings.get(&operation_id) {
                    if existing.commit_id != commit_id {
                        return Err(FileDagStoreError::OperationConflict(operation_id));
                    }
                } else {
                    self.bindings
                        .insert(operation_id, OperationBinding { commit_id });
                }
            }
            RecordKind::OperationTombstone => {
                if payload.len() != 40 {
                    return Err(FileDagStoreError::CorruptRecord {
                        offset,
                        reason: "tombstone payload length",
                    });
                }
                let operation_id = u64::from_le_bytes(payload[..8].try_into().unwrap());
                let mut commit_id = [0; 32];
                commit_id.copy_from_slice(&payload[8..]);
                self.validate_commit(commit_id)?;
                if let Some(existing) = self.tombstones.get(&operation_id) {
                    if *existing != commit_id {
                        return Err(FileDagStoreError::OperationConflict(operation_id));
                    }
                    return Ok(());
                }
                let Some(existing) = self.bindings.get(&operation_id) else {
                    return Err(FileDagStoreError::OperationConflict(operation_id));
                };
                if existing.commit_id != commit_id {
                    return Err(FileDagStoreError::OperationConflict(operation_id));
                }
                self.bindings.remove(&operation_id);
                self.tombstones.insert(operation_id, commit_id);
            }
        }
        Ok(())
    }

    fn read_node_at(&mut self, offset: u64) -> Result<Node, FileDagStoreError> {
        let mut header = [0; RECORD_HEADER_LEN];
        self.device.read_at(offset, &mut header)?;
        let kind_code = u16::from_le_bytes([header[0], header[1]]);
        let kind = RecordKind::decode(kind_code)?;
        let len = u32::from_le_bytes(header[2..6].try_into().unwrap()) as usize;
        if len > MAX_RECORD_PAYLOAD {
            return Err(FileDagStoreError::CorruptRecord {
                offset,
                reason: "payload too large",
            });
        }
        let total = record_size(len) as u64;
        if total > self.device.len().saturating_sub(offset) {
            return Err(FileDagStoreError::CorruptRecord {
                offset,
                reason: "record truncated",
            });
        }
        let mut payload = vec![0; len];
        self.device
            .read_at(offset + RECORD_HEADER_LEN as u64, &mut payload)?;
        let footer_offset = offset + RECORD_HEADER_LEN as u64 + len as u64;
        let mut footer = [0; RECORD_FOOTER_LEN];
        self.device.read_at(footer_offset, &mut footer)?;
        if !validate_footer(offset, kind_code, &payload, &footer)? {
            return Err(FileDagStoreError::CorruptRecord {
                offset,
                reason: "record footer missing",
            });
        }
        let RecordKind::Node(node_kind) = kind else {
            return Err(FileDagStoreError::CorruptRecord {
                offset,
                reason: "index is not a node",
            });
        };
        if checksum(kind_code, &payload).as_slice() != &header[6..] {
            return Err(FileDagStoreError::CorruptRecord {
                offset,
                reason: "checksum mismatch",
            });
        }
        let node = decode_node(node_kind, &payload)?;
        let id = node.id()?;
        if self.nodes.get(&id).copied() != Some(offset) {
            return Err(FileDagStoreError::CorruptRecord {
                offset,
                reason: "node id mismatch",
            });
        }
        Ok(node)
    }

    fn validate_commit(&mut self, commit_id: CommitId) -> Result<(), FileDagStoreError> {
        let offset = self
            .nodes
            .get(&commit_id)
            .copied()
            .ok_or(DagError::MissingNode(commit_id))?;
        let node = self.read_node_at(offset)?;
        let Node::Commit(_) = node else {
            return Err(DagError::InvalidReference(commit_id).into());
        };
        if node.id()? != commit_id {
            return Err(DagError::HashMismatch(commit_id).into());
        }
        Ok(())
    }

    fn read_required_node(&mut self, id: NodeId) -> Result<Node, FileDagStoreError> {
        self.node(&id)?.ok_or(FileDagStoreError::MissingNode(id))
    }

    fn read_map_range(
        &mut self,
        map: cairn_catalog::RangeMapNode,
        logical_size: u64,
        expected: Range<u64>,
        requested: Range<u64>,
        output: &mut [u8],
        budget: &mut ReadBudget,
    ) -> Result<(), FileDagStoreError> {
        budget.map_nodes = budget
            .map_nodes
            .checked_add(1)
            .ok_or(FileDagStoreError::ResourceLimit("range-map nodes"))?;
        if budget.map_nodes > MAX_SNAPSHOT_EXTENTS {
            return Err(FileDagStoreError::ResourceLimit("range-map nodes"));
        }
        let span = map_span_for_read(&map)?;
        let map_end = span
            .0
            .checked_add(span.1)
            .ok_or(FileDagStoreError::InvalidSnapshot("map span overflow"))?;
        if span != (expected.start, expected.end - expected.start) || map_end > logical_size {
            return Err(FileDagStoreError::InvalidSnapshot("map span is invalid"));
        }

        let mut covered = 0usize;
        for entry in map.children {
            let entry_end = entry
                .logical_start
                .checked_add(entry.logical_len)
                .ok_or(FileDagStoreError::InvalidSnapshot("entry span overflow"))?;
            if entry_end <= requested.start || entry.logical_start >= requested.end {
                continue;
            }
            let start = entry.logical_start.max(requested.start);
            let end = entry_end.min(requested.end);
            let output_start = usize::try_from(start - requested.start)
                .map_err(|_| FileDagStoreError::ResourceLimit("output offset"))?;
            let output_end = usize::try_from(end - requested.start)
                .map_err(|_| FileDagStoreError::ResourceLimit("output offset"))?;
            let target = output
                .get_mut(output_start..output_end)
                .ok_or(FileDagStoreError::InvalidSnapshot("output coverage"))?;
            let child_offset = start - entry.logical_start;
            match entry.child_kind {
                cairn_catalog::NodeKind::RangeMap => {
                    if map.level == 0 {
                        return Err(FileDagStoreError::InvalidReference(entry.child));
                    }
                    let child = self.read_required_node(entry.child)?;
                    let Node::RangeMap(child_map) = child else {
                        return Err(FileDagStoreError::InvalidReference(entry.child));
                    };
                    if child_map.level + 1 != map.level {
                        return Err(FileDagStoreError::InvalidReference(entry.child));
                    }
                    self.read_map_range(
                        child_map,
                        logical_size,
                        entry.logical_start..entry_end,
                        start..end,
                        target,
                        budget,
                    )?;
                }
                cairn_catalog::NodeKind::ZeroRun => {
                    budget.leaves = budget
                        .leaves
                        .checked_add(1)
                        .ok_or(FileDagStoreError::ResourceLimit("snapshot extents"))?;
                    if budget.leaves > MAX_SNAPSHOT_EXTENTS {
                        return Err(FileDagStoreError::ResourceLimit("snapshot extents"));
                    }
                    if map.level != 0 || entry.content_offset != 0 {
                        return Err(FileDagStoreError::InvalidReference(entry.child));
                    }
                    let child = self.read_required_node(entry.child)?;
                    let Node::ZeroRun(run) = child else {
                        return Err(FileDagStoreError::InvalidReference(entry.child));
                    };
                    if run.len != entry.logical_len {
                        return Err(FileDagStoreError::InvalidSnapshot(
                            "ZeroRun length mismatch",
                        ));
                    }
                    target.fill(0);
                }
                cairn_catalog::NodeKind::Content => {
                    budget.leaves = budget
                        .leaves
                        .checked_add(1)
                        .ok_or(FileDagStoreError::ResourceLimit("snapshot extents"))?;
                    if budget.leaves > MAX_SNAPSHOT_EXTENTS {
                        return Err(FileDagStoreError::ResourceLimit("snapshot extents"));
                    }
                    if map.level != 0 {
                        return Err(FileDagStoreError::InvalidReference(entry.child));
                    }
                    self.read_content_range(
                        entry.child,
                        entry.content_offset.checked_add(child_offset).ok_or(
                            FileDagStoreError::InvalidSnapshot("content offset overflow"),
                        )?,
                        target,
                    )?;
                }
                _ => return Err(FileDagStoreError::InvalidReference(entry.child)),
            }
            covered = covered
                .checked_add(target.len())
                .ok_or(FileDagStoreError::ResourceLimit("range coverage"))?;
        }
        if covered != output.len() {
            return Err(FileDagStoreError::InvalidSnapshot(
                "range coverage mismatch",
            ));
        }
        Ok(())
    }

    fn read_content_range(
        &mut self,
        id: NodeId,
        content_offset: u64,
        output: &mut [u8],
    ) -> Result<(), FileDagStoreError> {
        let node = self.read_required_node(id)?;
        let Node::Content(content) = node else {
            return Err(FileDagStoreError::InvalidReference(id));
        };
        let start = usize::try_from(content_offset)
            .map_err(|_| FileDagStoreError::ResourceLimit("content offset"))?;
        let end = start
            .checked_add(output.len())
            .ok_or(FileDagStoreError::ResourceLimit("content range"))?;
        let bytes = content
            .bytes
            .get(start..end)
            .ok_or(FileDagStoreError::InvalidSnapshot(
                "Content range out of bounds",
            ))?;
        output.copy_from_slice(bytes);
        Ok(())
    }

    fn validate_snapshot_digest(
        &mut self,
        snapshot_id: NodeId,
        snapshot: &cairn_catalog::SnapshotNode,
    ) -> Result<(), FileDagStoreError> {
        if self.validated_snapshots.contains(&snapshot_id) {
            return Ok(());
        }
        let root = self.read_required_node(snapshot.range_map_root)?;
        let Node::RangeMap(root) = root else {
            return Err(FileDagStoreError::InvalidReference(snapshot.range_map_root));
        };
        validate_map_root(&root, snapshot.logical_size)?;
        let mut hasher = Hasher::new();
        hasher.update(b"cairn/logical-bytes/v1");
        let mut budget = ReadBudget::default();
        let mut offset = 0u64;
        while offset < snapshot.logical_size {
            let end = offset
                .saturating_add(MAX_SNAPSHOT_READ_BYTES as u64)
                .min(snapshot.logical_size);
            let mut chunk = vec![0; (end - offset) as usize];
            self.read_map_range(
                root.clone(),
                snapshot.logical_size,
                0..snapshot.logical_size,
                offset..end,
                &mut chunk,
                &mut budget,
            )?;
            hasher.update(&chunk);
            offset = end;
        }
        if *hasher.finalize().as_bytes() != snapshot.content_digest {
            return Err(FileDagStoreError::InvalidSnapshot(
                "content digest mismatch",
            ));
        }
        self.validated_snapshots.insert(snapshot_id);
        Ok(())
    }
}

#[derive(Default)]
struct ReadBudget {
    map_nodes: usize,
    leaves: usize,
}

fn map_span_for_read(map: &cairn_catalog::RangeMapNode) -> Result<(u64, u64), FileDagStoreError> {
    let Some(first) = map.children.first() else {
        return Ok((0, 0));
    };
    let last = map.children.last().unwrap();
    let end = last
        .logical_start
        .checked_add(last.logical_len)
        .ok_or(FileDagStoreError::InvalidSnapshot("map span overflow"))?;
    Ok((first.logical_start, end - first.logical_start))
}

fn validate_map_root(
    map: &cairn_catalog::RangeMapNode,
    logical_size: u64,
) -> Result<(), FileDagStoreError> {
    if logical_size == 0 {
        if map.level != 0 || !map.children.is_empty() {
            return Err(FileDagStoreError::InvalidSnapshot(
                "non-canonical empty root",
            ));
        }
        return Ok(());
    }
    let span = map_span_for_read(map)?;
    if span != (0, logical_size) {
        return Err(FileDagStoreError::InvalidSnapshot(
            "root does not cover snapshot",
        ));
    }
    Ok(())
}

fn record_size(payload_len: usize) -> usize {
    RECORD_HEADER_LEN + payload_len + RECORD_FOOTER_LEN
}

fn encode_header(kind: RecordKind, payload: &[u8]) -> [u8; RECORD_HEADER_LEN] {
    let mut header = [0; RECORD_HEADER_LEN];
    header[..2].copy_from_slice(&kind.code().to_le_bytes());
    header[2..6].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    header[6..].copy_from_slice(&checksum(kind.code(), payload));
    header
}

fn checksum(kind: u16, payload: &[u8]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(&kind.to_le_bytes());
    hasher.update(&(payload.len() as u32).to_le_bytes());
    hasher.update(payload);
    *hasher.finalize().as_bytes()
}

fn encode_footer(kind: RecordKind, payload: &[u8]) -> [u8; RECORD_FOOTER_LEN] {
    let mut footer = [0; RECORD_FOOTER_LEN];
    footer[8..16].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    footer[16..].copy_from_slice(&checksum(kind.code(), payload));
    footer
}

fn validate_footer(
    offset: u64,
    kind: u16,
    payload: &[u8],
    footer: &[u8; RECORD_FOOTER_LEN],
) -> Result<bool, FileDagStoreError> {
    if footer[..8] != RECORD_FOOTER_MAGIC {
        return Ok(false);
    }
    let footer_len = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    if footer_len != payload.len() as u64 || footer[16..] != checksum(kind, payload) {
        return Err(FileDagStoreError::CorruptRecord {
            offset,
            reason: "record footer mismatch",
        });
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DeviceEffect, DeviceEventKind, DeviceRule, DeviceScript, EventOccurrence, EventSelector,
        SimDisk,
    };
    use cairn_catalog::{
        content_digest, CommitNode, ContentNode, ModelCatalog, NodeKind, RangeMapEntry,
        RangeMapNode, SnapshotNode, ZeroRunNode,
    };

    fn operation_id() -> OperationId {
        ModelCatalog::new().allocate_operation_id().unwrap()
    }

    #[test]
    fn nodes_and_bindings_survive_scan_reopen() {
        let disk = SimDisk::new(4096);
        let mut store = FileDagStore::open(disk).unwrap();
        let node = Node::ZeroRun(ZeroRunNode { len: 123 });
        let node_id = store.append_node(&node).unwrap();
        let operation = operation_id();
        let map = store
            .append_node(&Node::RangeMap(RangeMapNode {
                level: 0,
                children: Vec::new(),
            }))
            .unwrap();
        let snapshot = store
            .append_node(&Node::Snapshot(SnapshotNode {
                logical_size: 0,
                range_map_root: map,
                content_digest: content_digest(&[]),
            }))
            .unwrap();
        let commit = Node::Commit(CommitNode {
            snapshot,
            parent: None,
        });
        let commit = store.append_node(&commit).unwrap();
        store.bind_operation(operation, commit).unwrap();

        let disk = store.into_inner();
        let mut reopened = FileDagStore::open(disk).unwrap();
        assert_eq!(reopened.node(&node_id).unwrap(), Some(node));
        assert_eq!(reopened.operation_binding(operation), Some(commit));
    }

    #[test]
    fn operation_tombstone_is_sticky_and_survives_scan_reopen() {
        let disk = SimDisk::new(4096);
        let mut store = FileDagStore::open(disk).unwrap();
        let map = store
            .append_node(&Node::RangeMap(RangeMapNode {
                level: 0,
                children: Vec::new(),
            }))
            .unwrap();
        let snapshot = store
            .append_node(&Node::Snapshot(SnapshotNode {
                logical_size: 0,
                range_map_root: map,
                content_digest: content_digest(&[]),
            }))
            .unwrap();
        let commit = store
            .append_node(&Node::Commit(CommitNode {
                snapshot,
                parent: None,
            }))
            .unwrap();
        let operation = operation_id();
        store.bind_operation(operation, commit).unwrap();
        assert_eq!(store.operation_binding(operation), Some(commit));
        let after_bind = store.next_offset();
        store.bind_operation(operation, commit).unwrap();
        assert_eq!(store.next_offset(), after_bind);

        let other_commit = store
            .append_node(&Node::Commit(CommitNode {
                snapshot,
                parent: Some(commit),
            }))
            .unwrap();
        assert!(matches!(
            store.tombstone_operation(operation, other_commit),
            Err(FileDagStoreError::OperationConflict(id)) if id == operation.get()
        ));

        store.tombstone_operation(operation, commit).unwrap();
        let after_tombstone = store.next_offset();
        assert_eq!(store.operation_binding(operation), None);
        assert_eq!(store.operation_tombstone(operation), Some(commit));
        store.tombstone_operation(operation, commit).unwrap();
        assert_eq!(store.next_offset(), after_tombstone);
        assert!(matches!(
            store.bind_operation(operation, commit),
            Err(FileDagStoreError::OperationConflict(id)) if id == operation.get()
        ));

        let disk = store.into_inner();
        let reopened = FileDagStore::open(disk).unwrap();
        assert_eq!(reopened.operation_binding(operation), None);
        assert_eq!(reopened.operation_tombstone(operation), Some(commit));
    }

    #[test]
    fn checksum_corruption_is_rejected() {
        let disk = SimDisk::new(4096);
        let mut store = FileDagStore::open(disk).unwrap();
        store
            .append_node(&Node::ZeroRun(ZeroRunNode { len: 1 }))
            .unwrap();
        let offset = store.next_offset - record_size(8) as u64;
        let mut disk = store.into_inner();
        let mut byte = [0; 1];
        disk.read_at(offset + 6, &mut byte).unwrap();
        byte[0] ^= 1;
        disk.write_at(offset + 6, &byte).unwrap();
        disk.flush_all().unwrap();

        assert!(matches!(
            FileDagStore::open(disk),
            Err(FileDagStoreError::CorruptRecord {
                reason: "checksum mismatch",
                ..
            })
        ));
    }

    #[test]
    fn zeroed_header_checksum_with_a_complete_footer_is_rejected() {
        let disk = SimDisk::new(4096);
        let mut store = FileDagStore::open(disk).unwrap();
        store
            .append_node(&Node::ZeroRun(ZeroRunNode { len: 1 }))
            .unwrap();
        let offset = store.next_offset - record_size(8) as u64;
        let mut disk = store.into_inner();
        disk.write_at(offset + 6, &[0; 32]).unwrap();
        disk.flush_all().unwrap();

        assert!(matches!(
            FileDagStore::open(disk),
            Err(FileDagStoreError::CorruptRecord {
                reason: "checksum mismatch",
                ..
            })
        ));
    }

    #[test]
    fn binding_requires_an_existing_commit_node() {
        let disk = SimDisk::new(4096);
        let mut store = FileDagStore::open(disk).unwrap();
        let operation = operation_id();
        assert!(matches!(
            store.bind_operation(operation, [7; 32]),
            Err(FileDagStoreError::Dag(DagError::MissingNode(_)))
        ));
    }

    #[test]
    fn binding_requires_a_commit_node() {
        let disk = SimDisk::new(4096);
        let mut store = FileDagStore::open(disk).unwrap();
        let node_id = store
            .append_node(&Node::ZeroRun(ZeroRunNode { len: 1 }))
            .unwrap();
        assert!(matches!(
            store.bind_operation(operation_id(), node_id),
            Err(FileDagStoreError::Dag(DagError::InvalidReference(_)))
        ));
    }

    #[test]
    fn crash_cut_after_flushed_header_is_truncated_payload_tail() {
        let script = DeviceScript {
            rules: vec![DeviceRule {
                selector: EventSelector {
                    kind: DeviceEventKind::Write,
                    occurrence: EventOccurrence::Exact(2),
                    range: None,
                },
                effect: DeviceEffect::TearAndCrashAfter { durable_prefix: 3 },
            }],
            ..DeviceScript::default()
        };
        let mut disk = SimDisk::from_script(4096, script).unwrap();
        let node = Node::Commit(CommitNode {
            snapshot: [8; 32],
            parent: None,
        });
        let payload = encode_node_payload(&node).unwrap();
        let header = encode_header(RecordKind::Node(node.kind()), &payload);
        disk.write_at(0, &header).unwrap();
        disk.flush_data().unwrap();
        assert!(disk.write_at(RECORD_HEADER_LEN as u64, &payload).is_err());
        assert_ne!(
            &disk.durable_bytes()[RECORD_HEADER_LEN..RECORD_HEADER_LEN + payload.len()],
            payload.as_slice()
        );

        assert_eq!(FileDagStore::open(disk).unwrap().next_offset(), 0);
    }

    #[test]
    fn crash_cut_in_header_is_treated_as_truncated_tail() {
        let script = DeviceScript {
            rules: vec![DeviceRule {
                selector: EventSelector {
                    kind: DeviceEventKind::Write,
                    occurrence: EventOccurrence::Exact(1),
                    range: None,
                },
                effect: DeviceEffect::TearAndCrashAfter { durable_prefix: 5 },
            }],
            ..DeviceScript::default()
        };
        let disk = SimDisk::from_script(4096, script).unwrap();
        let mut store = FileDagStore::open(disk).unwrap();
        let error = store
            .append_node(&Node::ZeroRun(ZeroRunNode { len: 9 }))
            .unwrap_err();
        assert!(matches!(error, FileDagStoreError::Device(_)));
        let disk = store.into_inner();
        let reopened = FileDagStore::open(disk).unwrap();
        assert_eq!(reopened.next_offset(), 0);
    }

    fn persisted_fixture() -> (SimDisk, CommitId, Vec<u8>) {
        let disk = SimDisk::new(32 * 1024);
        let mut store = FileDagStore::open(disk).unwrap();
        let first = b"abcdefgh".to_vec();
        let last = b"WXYZ".to_vec();
        let first_id = store
            .append_node(&Node::Content(ContentNode {
                bytes: first.clone(),
            }))
            .unwrap();
        let zero_id = store
            .append_node(&Node::ZeroRun(ZeroRunNode { len: 3 }))
            .unwrap();
        let last_id = store
            .append_node(&Node::Content(ContentNode {
                bytes: last.clone(),
            }))
            .unwrap();
        let map_id = store
            .append_node(&Node::RangeMap(RangeMapNode {
                level: 0,
                children: vec![
                    RangeMapEntry {
                        logical_start: 0,
                        logical_len: 8,
                        content_offset: 0,
                        child_kind: NodeKind::Content,
                        child: first_id,
                    },
                    RangeMapEntry {
                        logical_start: 8,
                        logical_len: 3,
                        content_offset: 0,
                        child_kind: NodeKind::ZeroRun,
                        child: zero_id,
                    },
                    RangeMapEntry {
                        logical_start: 11,
                        logical_len: 4,
                        content_offset: 0,
                        child_kind: NodeKind::Content,
                        child: last_id,
                    },
                ],
            }))
            .unwrap();
        let expected = [first, vec![0; 3], last].concat();
        let snapshot_id = store
            .append_node(&Node::Snapshot(SnapshotNode {
                logical_size: expected.len() as u64,
                range_map_root: map_id,
                content_digest: content_digest(&expected),
            }))
            .unwrap();
        let commit_id = store
            .append_node(&Node::Commit(CommitNode {
                snapshot: snapshot_id,
                parent: None,
            }))
            .unwrap();
        (store.into_inner(), commit_id, expected)
    }

    #[test]
    fn append_snapshot_builds_a_reopenable_commit() {
        let disk = SimDisk::new(32 * 1024);
        let mut store = FileDagStore::open(disk).unwrap();
        let bytes = b"durable write".to_vec();
        let verified = store.append_snapshot(&bytes, None).unwrap();
        assert_eq!(
            store
                .read_snapshot_range(verified.commit_id, 0..bytes.len() as u64)
                .unwrap(),
            bytes
        );
        let disk = store.into_inner();
        let mut reopened = FileDagStore::open(disk).unwrap();
        assert_eq!(
            reopened.verified_snapshot(verified.commit_id).unwrap(),
            verified
        );
    }

    #[test]
    fn snapshot_range_reads_only_requested_logical_bytes_and_survives_reopen() {
        let (disk, commit, expected) = persisted_fixture();
        let mut store = FileDagStore::open(disk).unwrap();
        for start in 0..=expected.len() as u64 {
            for end in start..=expected.len() as u64 {
                assert_eq!(
                    store.read_snapshot_range(commit, start..end).unwrap(),
                    expected[start as usize..end as usize]
                );
            }
        }
        let disk = store.into_inner();
        let mut reopened = FileDagStore::open(disk).unwrap();
        assert_eq!(
            reopened.read_snapshot_range(commit, 6..13).unwrap(),
            b"gh\0\0\0WX"
        );
        assert_eq!(
            reopened.verified_snapshot(commit).unwrap(),
            VerifiedSnapshot {
                commit_id: commit,
                logical_size: expected.len() as u64,
                content_digest: content_digest(&expected),
            }
        );
    }

    #[test]
    fn snapshot_range_errors_are_classified() {
        let (disk, commit, expected) = persisted_fixture();
        let mut store = FileDagStore::open(disk).unwrap();
        assert!(matches!(
            store.read_snapshot_range(commit, 0..expected.len() as u64 + 1),
            Err(FileDagStoreError::RangeOutOfBounds { .. })
        ));
        let reversed_start = 2;
        let reversed_end = 1;
        assert!(matches!(
            store.read_snapshot_range(commit, reversed_start..reversed_end),
            Err(FileDagStoreError::RangeOutOfBounds { .. })
        ));
        assert!(matches!(
            store.read_snapshot_range([9; 32], 0..1),
            Err(FileDagStoreError::MissingNode(_))
        ));
    }

    #[test]
    fn snapshot_range_rejects_commit_to_non_snapshot_reference() {
        let disk = SimDisk::new(4096);
        let mut store = FileDagStore::open(disk).unwrap();
        let zero = store
            .append_node(&Node::ZeroRun(ZeroRunNode { len: 1 }))
            .unwrap();
        let commit = store
            .append_node(&Node::Commit(CommitNode {
                snapshot: zero,
                parent: None,
            }))
            .unwrap();
        assert!(matches!(
            store.read_snapshot_range(commit, 0..1),
            Err(FileDagStoreError::InvalidReference(_))
        ));
    }

    #[test]
    fn snapshot_range_rejects_a_validly_hashed_snapshot_with_wrong_content_digest() {
        let (disk, commit, _) = persisted_fixture();
        let mut store = FileDagStore::open(disk).unwrap();
        let snapshot_id = match store.node(&commit).unwrap().unwrap() {
            Node::Commit(commit) => commit.snapshot,
            _ => unreachable!(),
        };
        let original = match store.node(&snapshot_id).unwrap() {
            Some(Node::Snapshot(snapshot)) => snapshot,
            _ => unreachable!(),
        };
        let snapshot = store
            .append_node(&Node::Snapshot(SnapshotNode {
                content_digest: [9; 32],
                ..original
            }))
            .unwrap();
        let bad_commit = store
            .append_node(&Node::Commit(CommitNode {
                snapshot,
                parent: None,
            }))
            .unwrap();
        assert!(matches!(
            store.read_snapshot_range(bad_commit, 0..1),
            Err(FileDagStoreError::InvalidSnapshot(
                "content digest mismatch"
            ))
        ));
    }
}
