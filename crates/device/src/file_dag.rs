use std::collections::HashMap;

use blake3::Hasher;
use cairn_model::{
    decode_node, encode_node_payload, CommitId, DagError, Node, NodeId, NodeKind, OperationId,
};

use crate::{BlockDevice, DeviceError};

/// The fixed on-device header: kind (u16), payload length (u32), checksum (32).
pub const RECORD_HEADER_LEN: usize = 2 + 4 + 32;
const RECORD_FOOTER_MAGIC: [u8; 8] = *b"CAIRNEND";
const RECORD_FOOTER_LEN: usize = 8 + 8 + 32;
const BINDING_KIND: u16 = 0x100;
const MAX_RECORD_PAYLOAD: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordKind {
    Node(NodeKind),
    OperationBinding,
}

impl RecordKind {
    fn code(self) -> u16 {
        match self {
            Self::Node(kind) => kind as u16,
            Self::OperationBinding => BINDING_KIND,
        }
    }

    fn decode(code: u16) -> Result<Self, FileDagStoreError> {
        if code == BINDING_KIND {
            return Ok(Self::OperationBinding);
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
    CorruptRecord { offset: u64, reason: &'static str },
    RecordTooLarge(usize),
    CapacityExhausted { needed: u64, remaining: u64 },
    OperationConflict(u64),
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
    bindings: HashMap<u64, (CommitId, u64)>,
}

impl<D: BlockDevice> FileDagStore<D> {
    pub fn open(device: D) -> Result<Self, FileDagStoreError> {
        let mut store = Self {
            next_offset: 0,
            device,
            nodes: HashMap::new(),
            bindings: HashMap::new(),
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

    pub fn bind_operation(
        &mut self,
        operation_id: OperationId,
        commit_id: CommitId,
    ) -> Result<(), FileDagStoreError> {
        self.validate_commit(commit_id)?;
        if let Some(&(existing, _)) = self.bindings.get(&operation_id.get()) {
            if existing != commit_id {
                return Err(FileDagStoreError::OperationConflict(operation_id.get()));
            }
            return Ok(());
        }
        let mut payload = Vec::with_capacity(40);
        payload.extend_from_slice(&operation_id.get().to_le_bytes());
        payload.extend_from_slice(&commit_id);
        let offset = self.next_offset;
        self.append_record(RecordKind::OperationBinding, &payload)?;
        self.bindings
            .insert(operation_id.get(), (commit_id, offset));
        Ok(())
    }

    pub fn node(&mut self, id: &NodeId) -> Result<Option<Node>, FileDagStoreError> {
        self.nodes
            .get(id)
            .copied()
            .map(|offset| self.read_node_at(offset))
            .transpose()
    }

    pub fn operation_binding(&self, operation_id: OperationId) -> Option<CommitId> {
        self.bindings
            .get(&operation_id.get())
            .map(|(commit, _)| *commit)
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
                if let Some((existing, _)) = self.bindings.get(&operation_id) {
                    if *existing != commit_id {
                        return Err(FileDagStoreError::OperationConflict(operation_id));
                    }
                } else {
                    self.bindings.insert(operation_id, (commit_id, offset));
                }
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
    use cairn_model::{
        content_digest, CommitNode, ModelCatalog, RangeMapNode, SnapshotNode, ZeroRunNode,
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
}
