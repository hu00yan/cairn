//! Legacy chunk/manifest record engine retained for compatibility tests.
//!
//! New single-node protocol work belongs to `cairn-catalog`, `cairn-device`,
//! and `cairn-single-node`; this crate is not the new storage kernel.

#![deny(unsafe_code)]
use cairn_device::io::{BlockDevice, DeviceError};
use std::collections::HashMap;

pub type ObjectId = [u8; 32];
pub type Generation = u64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkRef {
    pub id: ObjectId,
    pub len: u32,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Object {
    pub id: ObjectId,
    pub len: u64,
    pub chunks: Vec<ChunkRef>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Root {
    pub generation: Generation,
    pub manifest: ObjectId,
}

pub trait Hasher {
    fn hash(&self, data: &[u8]) -> ObjectId;
}
#[derive(Clone, Copy, Debug, Default)]
pub struct Blake3Hasher;
impl Hasher for Blake3Hasher {
    fn hash(&self, data: &[u8]) -> ObjectId {
        *blake3::hash(data).as_bytes()
    }
}

#[derive(Debug)]
pub enum Error {
    Device(DeviceError),
    Corruption(&'static str),
    NotFound(ObjectId),
    InvalidInput(&'static str),
}
impl From<DeviceError> for Error {
    fn from(e: DeviceError) -> Self {
        Self::Device(e)
    }
}

pub type Index = HashMap<ObjectId, u64>;

pub const HEADER_SIZE: usize = 32;
const MAGIC: u32 = 0x4341_4952;
const VERSION: u16 = 1;
const MAX_RECORD_PAYLOAD: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordKind {
    Chunk = 1,
    Manifest = 2,
    RootCommit = 4,
}
impl TryFrom<u16> for RecordKind {
    type Error = Error;
    fn try_from(v: u16) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(Self::Chunk),
            2 => Ok(Self::Manifest),
            4 => Ok(Self::RootCommit),
            _ => Err(Error::Corruption("unknown record kind")),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RecordHeader {
    pub kind: RecordKind,
    pub payload_len: u32,
    pub generation: u64,
    pub checksum: u64,
}
impl RecordHeader {
    fn encode_without_checksum(&self) -> [u8; HEADER_SIZE] {
        let mut b = [0; HEADER_SIZE];
        b[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        b[4..6].copy_from_slice(&VERSION.to_le_bytes());
        b[6..8].copy_from_slice(&(self.kind as u16).to_le_bytes());
        b[8..12].copy_from_slice(&self.payload_len.to_le_bytes());
        b[12..20].copy_from_slice(&self.generation.to_le_bytes());
        b
    }
    fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut b = self.encode_without_checksum();
        b[20..28].copy_from_slice(&self.checksum.to_le_bytes());
        b
    }
    fn decode(b: &[u8; HEADER_SIZE]) -> Result<Self, Error> {
        if u32::from_le_bytes(b[0..4].try_into().unwrap()) != MAGIC {
            return Err(Error::Corruption("bad record magic"));
        }
        if u16::from_le_bytes(b[4..6].try_into().unwrap()) != VERSION {
            return Err(Error::Corruption("unsupported record version"));
        }
        let len = u32::from_le_bytes(b[8..12].try_into().unwrap());
        if u64::from(len) > MAX_RECORD_PAYLOAD {
            return Err(Error::Corruption("record too large"));
        }
        Ok(Self {
            kind: RecordKind::try_from(u16::from_le_bytes(b[6..8].try_into().unwrap()))?,
            payload_len: len,
            generation: u64::from_le_bytes(b[12..20].try_into().unwrap()),
            checksum: u64::from_le_bytes(b[20..28].try_into().unwrap()),
        })
    }
}

fn checksum(header: &RecordHeader, payload: &[u8]) -> u64 {
    let mut h = blake3::Hasher::new();
    h.update(&header.encode_without_checksum());
    h.update(payload);
    u64::from_le_bytes(h.finalize().as_bytes()[..8].try_into().unwrap())
}
fn aligned(n: u64) -> Result<u64, Error> {
    n.checked_add(7)
        .map(|x| x & !7)
        .ok_or(Error::Corruption("offset overflow"))
}

pub struct Store<D: BlockDevice> {
    pub device: D,
    pub index: Index,
    pub root: Option<Root>,
    pub append_offset: u64,
}
impl<D: BlockDevice> Store<D> {
    pub fn format(device: D) -> Result<Self, Error> {
        if device.len() < 4096 {
            return Err(Error::InvalidInput("device too small"));
        }
        Ok(Self {
            device,
            index: HashMap::new(),
            root: None,
            append_offset: 0,
        })
    }
    pub fn open(device: D) -> Result<Self, Error> {
        let mut store = Self::format(device)?;
        let end = store.device.len();
        let mut offset: u64 = 0;
        while offset
            .checked_add(HEADER_SIZE as u64)
            .is_some_and(|x| x <= end)
        {
            let mut raw = [0; HEADER_SIZE];
            store.device.read_at(offset, &mut raw)?;
            if raw.iter().all(|x| *x == 0) {
                break;
            }
            let header = RecordHeader::decode(&raw)?;
            let next = offset
                .checked_add(HEADER_SIZE as u64)
                .and_then(|x| x.checked_add(header.payload_len as u64))
                .ok_or(Error::Corruption("record offset overflow"))?;
            if next > end {
                return Err(Error::Corruption("truncated record"));
            }
            let mut payload = vec![0; header.payload_len as usize];
            store
                .device
                .read_at(offset + HEADER_SIZE as u64, &mut payload)?;
            if checksum(&header, &payload) != header.checksum {
                return Err(Error::Corruption("record checksum mismatch"));
            }
            match header.kind {
                RecordKind::Chunk | RecordKind::Manifest => {
                    if payload.len() < 32 {
                        return Err(Error::Corruption("object payload too short"));
                    }
                    let id: ObjectId = payload[..32].try_into().unwrap();
                    store.index.insert(id, offset);
                }
                RecordKind::RootCommit => {
                    if payload.len() != 40 {
                        return Err(Error::Corruption("invalid root commit"));
                    }
                    let manifest = payload[..32].try_into().unwrap();
                    let generation = u64::from_le_bytes(payload[32..40].try_into().unwrap());
                    if store
                        .root
                        .as_ref()
                        .is_none_or(|r| generation > r.generation)
                    {
                        store.root = Some(Root {
                            generation,
                            manifest,
                        });
                    }
                }
            }
            offset = aligned(next)?;
        }
        store.append_offset = offset;
        Ok(store)
    }
    pub fn current_root(&self) -> Option<Root> {
        self.root.clone()
    }
    pub fn put_bytes(&mut self, data: &[u8]) -> Result<ObjectId, Error> {
        let id = Blake3Hasher.hash(data);
        if self.index.contains_key(&id) {
            return Ok(id);
        }
        let mut payload = Vec::with_capacity(32 + data.len());
        payload.extend_from_slice(&id);
        payload.extend_from_slice(data);
        self.append(RecordKind::Chunk, &payload, 0)?;
        Ok(id)
    }
    pub fn get_bytes(&mut self, id: &ObjectId) -> Result<Vec<u8>, Error> {
        let offset = *self.index.get(id).ok_or(Error::NotFound(*id))?;
        let mut raw = [0; HEADER_SIZE];
        self.device.read_at(offset, &mut raw)?;
        let h = RecordHeader::decode(&raw)?;
        let mut p = vec![0; h.payload_len as usize];
        self.device.read_at(offset + HEADER_SIZE as u64, &mut p)?;
        if checksum(&h, &p) != h.checksum || p[..32] != id[..] {
            return Err(Error::Corruption("object verification failed"));
        }
        Ok(p[32..].to_vec())
    }
    pub fn commit_root(
        &mut self,
        manifest: ObjectId,
        generation: Generation,
    ) -> Result<Root, Error> {
        if !self.index.contains_key(&manifest) {
            return Err(Error::NotFound(manifest));
        }
        if self
            .root
            .as_ref()
            .is_some_and(|r| generation <= r.generation)
        {
            return Err(Error::InvalidInput("root generation must increase"));
        }
        let mut payload = Vec::with_capacity(40);
        payload.extend_from_slice(&manifest);
        payload.extend_from_slice(&generation.to_le_bytes());
        self.append(RecordKind::RootCommit, &payload, generation)?;
        self.device.flush_all()?;
        let root = Root {
            generation,
            manifest,
        };
        self.root = Some(root.clone());
        Ok(root)
    }
    fn append(&mut self, kind: RecordKind, payload: &[u8], generation: u64) -> Result<u64, Error> {
        if payload.len() as u64 > MAX_RECORD_PAYLOAD || payload.len() > u32::MAX as usize {
            return Err(Error::InvalidInput("payload too large"));
        }
        let mut h = RecordHeader {
            kind,
            payload_len: payload.len() as u32,
            generation,
            checksum: 0,
        };
        h.checksum = checksum(&h, payload);
        let end = self
            .append_offset
            .checked_add(HEADER_SIZE as u64)
            .and_then(|x| x.checked_add(payload.len() as u64))
            .ok_or(Error::InvalidInput("device offset overflow"))?;
        if end > self.device.len() {
            return Err(Error::InvalidInput("device full"));
        }
        self.device.write_at(self.append_offset, &h.encode())?;
        self.device
            .write_at(self.append_offset + HEADER_SIZE as u64, payload)?;
        self.device.flush_data()?;
        let at = self.append_offset;
        self.append_offset = aligned(end)?;
        if matches!(kind, RecordKind::Chunk | RecordKind::Manifest) {
            let id: ObjectId = payload
                .get(..32)
                .ok_or(Error::InvalidInput("object payload too short"))?
                .try_into()
                .map_err(|_| Error::InvalidInput("object id missing"))?;
            self.index.insert(id, at);
        }
        Ok(at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_device::io::SimDisk;

    #[test]
    fn put_get_commit_and_reopen() {
        let disk = SimDisk::new(128 * 1024);
        let mut store = Store::format(disk).unwrap();
        let id = store.put_bytes(b"hello").unwrap();
        assert_eq!(store.get_bytes(&id).unwrap(), b"hello");
        store.commit_root(id, 1).unwrap();
        let mut reopened = Store::open(store.device).unwrap();
        assert_eq!(
            reopened.current_root(),
            Some(Root {
                generation: 1,
                manifest: id
            })
        );
        assert_eq!(reopened.get_bytes(&id).unwrap(), b"hello");
    }

    #[test]
    fn corrupted_record_is_rejected() {
        let disk = SimDisk::new(128 * 1024);
        let mut store = Store::format(disk).unwrap();
        let id = store.put_bytes(b"hello").unwrap();
        store.device.durable_bytes();
        let mut disk = store.device;
        disk.write_at(40, b"X").unwrap();
        disk.flush_data().unwrap();
        assert!(matches!(Store::open(disk), Err(Error::Corruption(_))));
        let _ = id;
    }
}
