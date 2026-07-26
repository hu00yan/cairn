#![deny(unsafe_code)]
use cairn_device::io::{BlockDevice, DeviceError};
use std::collections::{HashMap, HashSet};

pub type ObjectId = [u8; 32];
pub type Generation = u64;
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;
pub const SLOT_SIZE: u64 = 4096;
pub const RECORDS_START: u64 = SLOT_SIZE * 2;
const H: usize = 32;
const SB: usize = 128;
const MAGIC: u32 = 0x4341_4952;
const SMAGIC: u32 = 0x4341_5350;
const VERSION: u16 = 2;
const MAX: u64 = 64 * 1024 * 1024;
const MANIFEST_HEADER_SIZE: usize = 16;
const MANIFEST_CHUNK_REF_SIZE: usize = 36;
const MAX_MANIFEST_CHUNKS: usize =
    ((MAX - 32 - MANIFEST_HEADER_SIZE as u64) / MANIFEST_CHUNK_REF_SIZE as u64) as usize;

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
        chunk_id(data)
    }
}

#[derive(Debug)]
pub enum Error {
    Device(DeviceError),
    Corruption(&'static str),
    NotFound(ObjectId),
    WrongObjectKind(&'static str),
    InvalidInput(&'static str),
    Capacity,
    ResourceExhausted(&'static str),
    Unformatted,
    UnsupportedFormat,
    RequiresRecovery,
}
impl From<DeviceError> for Error {
    fn from(e: DeviceError) -> Self {
        Self::Device(e)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Chunk = 1,
    Manifest = 2,
    Root = 4,
}
impl TryFrom<u16> for Kind {
    type Error = Error;
    fn try_from(v: u16) -> Result<Self, Error> {
        match v {
            1 => Ok(Self::Chunk),
            2 => Ok(Self::Manifest),
            4 => Ok(Self::Root),
            _ => Err(Error::Corruption("unknown record kind")),
        }
    }
}
#[derive(Clone, Copy, Debug)]
struct Header {
    kind: Kind,
    len: u32,
    generation: u64,
    checksum: u64,
}
impl Header {
    fn base(&self) -> [u8; H] {
        let mut b = [0; H];
        b[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        b[4..6].copy_from_slice(&VERSION.to_le_bytes());
        b[6..8].copy_from_slice(&(self.kind as u16).to_le_bytes());
        b[8..12].copy_from_slice(&self.len.to_le_bytes());
        b[12..20].copy_from_slice(&self.generation.to_le_bytes());
        b
    }
    fn encode(&self) -> [u8; H] {
        let mut b = self.base();
        b[20..28].copy_from_slice(&self.checksum.to_le_bytes());
        b
    }
    fn decode(b: &[u8; H]) -> Result<Self, Error> {
        if u32::from_le_bytes(b[0..4].try_into().unwrap()) != MAGIC {
            return Err(Error::Corruption("bad record magic"));
        };
        if u16::from_le_bytes(b[4..6].try_into().unwrap()) != VERSION {
            return Err(Error::UnsupportedFormat);
        };
        if b[28..32] != [0; 4] {
            return Err(Error::Corruption("header reserved"));
        };
        let len = u32::from_le_bytes(b[8..12].try_into().unwrap());
        if u64::from(len) > MAX {
            return Err(Error::Corruption("record too large"));
        };
        Ok(Self {
            kind: Kind::try_from(u16::from_le_bytes(b[6..8].try_into().unwrap()))?,
            len,
            generation: u64::from_le_bytes(b[12..20].try_into().unwrap()),
            checksum: u64::from_le_bytes(b[20..28].try_into().unwrap()),
        })
    }
}
fn digest(h: &Header, p: &[u8]) -> ObjectId {
    let mut x = blake3::Hasher::new();
    x.update(&h.base());
    x.update(p);
    *x.finalize().as_bytes()
}
fn chunk_id(data: &[u8]) -> ObjectId {
    let mut h = blake3::Hasher::new();
    h.update(b"cairn/chunk/v1\0");
    h.update(data);
    *h.finalize().as_bytes()
}
fn manifest_id(body: &[u8]) -> ObjectId {
    let mut h = blake3::Hasher::new();
    h.update(b"cairn/manifest/v1\0");
    h.update(body);
    *h.finalize().as_bytes()
}
fn manifest_id_for_chunks(chunks: &[ChunkRef], total: u64) -> ObjectId {
    let mut h = blake3::Hasher::new();
    h.update(b"cairn/manifest/v1\0");
    h.update(&1u16.to_le_bytes());
    h.update(&0u16.to_le_bytes());
    h.update(&(chunks.len() as u32).to_le_bytes());
    h.update(&total.to_le_bytes());
    for chunk in chunks {
        h.update(&chunk.id);
        h.update(&chunk.len.to_le_bytes());
    }
    *h.finalize().as_bytes()
}
fn align(n: u64) -> Result<u64, Error> {
    n.checked_add(7)
        .map(|v| v & !7)
        .ok_or(Error::Corruption("offset overflow"))
}

#[derive(Clone, Copy)]
struct Super {
    generation: u64,
    root_offset: u64,
    root_len: u32,
    manifest: ObjectId,
    root_digest: ObjectId,
}
impl Super {
    fn empty() -> Self {
        Self {
            generation: 0,
            root_offset: 0,
            root_len: 0,
            manifest: [0; 32],
            root_digest: [0; 32],
        }
    }
    fn encode(&self) -> [u8; SB] {
        let mut b = [0; SB];
        b[0..4].copy_from_slice(&SMAGIC.to_le_bytes());
        b[4..6].copy_from_slice(&VERSION.to_le_bytes());
        b[8..16].copy_from_slice(&self.generation.to_le_bytes());
        b[16..24].copy_from_slice(&self.root_offset.to_le_bytes());
        b[24..28].copy_from_slice(&self.root_len.to_le_bytes());
        b[28..60].copy_from_slice(&self.manifest);
        b[60..92].copy_from_slice(&self.root_digest);
        let checksum = *blake3::hash(&b[..96]).as_bytes();
        b[96..128].copy_from_slice(&checksum);
        b
    }
    fn decode(b: &[u8; SB]) -> Result<Self, Error> {
        if b.iter().all(|v| *v == 0) {
            return Err(Error::Unformatted);
        };
        if u32::from_le_bytes(b[0..4].try_into().unwrap()) != SMAGIC {
            return Err(Error::Corruption("super magic"));
        };
        if b[6..8] != [0; 2]
            || b[92..96] != [0; 4]
            || blake3::hash(&b[..96]).as_bytes() != &b[96..128]
        {
            return Err(Error::Corruption("super checksum"));
        };
        if u16::from_le_bytes(b[4..6].try_into().unwrap()) != VERSION {
            return Err(Error::UnsupportedFormat);
        };
        Ok(Self {
            generation: u64::from_le_bytes(b[8..16].try_into().unwrap()),
            root_offset: u64::from_le_bytes(b[16..24].try_into().unwrap()),
            root_len: u32::from_le_bytes(b[24..28].try_into().unwrap()),
            manifest: b[28..60].try_into().unwrap(),
            root_digest: b[60..92].try_into().unwrap(),
        })
    }
}
#[derive(Clone, Copy)]
struct Entry {
    offset: u64,
    kind: Kind,
    len: u32,
}

pub struct Store<D: BlockDevice> {
    device: D,
    index: HashMap<ObjectId, Entry>,
    root: Option<Root>,
    append_offset: u64,
    active: u8,
    requires_recovery: bool,
}
impl<D: BlockDevice> Store<D> {
    pub fn format(mut device: D) -> Result<Self, Error> {
        if device.len() < RECORDS_START + 4096 {
            return Err(Error::InvalidInput("device too small"));
        };
        let b = Super::empty().encode();
        device.write_at(0, &b)?;
        device.write_at(SLOT_SIZE, &b)?;
        device.flush_all()?;
        Ok(Self {
            device,
            index: HashMap::new(),
            root: None,
            append_offset: RECORDS_START,
            active: 0,
            requires_recovery: false,
        })
    }
    fn root_candidate_valid(device: &D, s: &Super) -> Result<bool, Error> {
        if s.root_offset == 0 {
            return Ok(s.generation == 0 && s.root_len == 0);
        }
        if s.root_offset % 8 != 0
            || s.root_offset < RECORDS_START
            || s.root_len != 72
            || s.root_offset
                .checked_add(s.root_len as u64)
                .is_none_or(|x| x > device.len())
        {
            return Ok(false);
        }
        let mut raw = [0; H];
        device
            .read_at(s.root_offset, &mut raw)
            .map_err(Error::Device)?;
        let h = match Header::decode(&raw) {
            Ok(h) => h,
            Err(Error::UnsupportedFormat) => return Err(Error::UnsupportedFormat),
            Err(Error::Corruption(_)) => return Ok(false),
            Err(e) => return Err(e),
        };
        if h.kind != Kind::Root || h.generation != s.generation || h.len != 40 {
            return Ok(false);
        }
        let mut p = [0; 40];
        device
            .read_at(s.root_offset + H as u64, &mut p)
            .map_err(Error::Device)?;
        Ok(p[..32] == s.manifest && digest(&h, &p) == s.root_digest)
    }
    pub fn open(device: D) -> Result<Self, Error> {
        let mut a = [0; SB];
        let mut b = [0; SB];
        let read_a = device.read_at(0, &mut a);
        let read_b = device.read_at(SLOT_SIZE, &mut b);
        if let Err(e) = read_a {
            if read_b.is_err() {
                return Err(Error::Device(e));
            }
        } else if let Err(e) = read_b {
            if a.iter().all(|v| *v == 0) {
                return Err(Error::Device(e));
            }
        }
        let da = Super::decode(&a);
        let db = Super::decode(&b);
        if matches!(&da, Err(Error::UnsupportedFormat))
            || matches!(&db, Err(Error::UnsupportedFormat))
        {
            return Err(Error::UnsupportedFormat);
        }
        let mut candidates = Vec::new();
        if let Ok(s) = da {
            if Self::root_candidate_valid(&device, &s)? {
                candidates.push((s, 0));
            }
        }
        if let Ok(s) = db {
            if Self::root_candidate_valid(&device, &s)? {
                candidates.push((s, 1));
            }
        }
        if candidates.is_empty() {
            return if matches!(da, Err(Error::Unformatted)) && matches!(db, Err(Error::Unformatted))
            {
                Err(Error::Unformatted)
            } else {
                Err(Error::Corruption("no valid superblock"))
            };
        }
        candidates.sort_by_key(|(s, _)| std::cmp::Reverse(s.generation));
        if candidates.len() == 2
            && candidates[0].0.generation == candidates[1].0.generation
            && (candidates[0].0.root_offset != candidates[1].0.root_offset
                || candidates[0].0.root_len != candidates[1].0.root_len
                || candidates[0].0.manifest != candidates[1].0.manifest
                || candidates[0].0.root_digest != candidates[1].0.root_digest)
        {
            return Err(Error::Corruption("conflicting superblocks"));
        }
        let mut st = Self {
            device,
            index: HashMap::new(),
            root: None,
            append_offset: RECORDS_START,
            active: candidates[0].1,
            requires_recovery: false,
        };
        let mut selected_end = None;
        for (s, active) in candidates {
            let end = if s.root_offset == 0 {
                RECORDS_START
            } else {
                s.root_offset
                    .checked_add(s.root_len as u64)
                    .ok_or(Error::Corruption("root offset overflow"))?
            };
            st.index.clear();
            st.root = None;
            st.active = active;
            if let Err(e) = st.scan(RECORDS_START, end) {
                if !matches!(e, Error::Corruption(_) | Error::NotFound(_)) {
                    return Err(e);
                }
                continue;
            }
            if s.root_offset != 0 {
                let (h, p) = match st.read(s.root_offset) {
                    Ok(value) => value,
                    Err(e) => {
                        if !matches!(e, Error::Corruption(_) | Error::NotFound(_)) {
                            return Err(e);
                        }
                        continue;
                    }
                };
                if h.kind != Kind::Root
                    || h.generation != s.generation
                    || p.len() != 40
                    || p[..32] != s.manifest
                    || u64::from_le_bytes(p[32..40].try_into().unwrap()) != s.generation
                    || digest(&h, &p) != s.root_digest
                {
                    continue;
                }
                st.root = Some(Root {
                    generation: s.generation,
                    manifest: s.manifest,
                });
                if let Err(e) = st.validate_manifest(&s.manifest) {
                    if !matches!(
                        e,
                        Error::Corruption(_) | Error::NotFound(_) | Error::WrongObjectKind(_)
                    ) {
                        return Err(e);
                    }
                    continue;
                }
            }
            selected_end = Some(end);
            break;
        }
        let end = selected_end.ok_or(Error::Corruption("no complete generation"))?;
        st.append_offset = st.tail(end)?;
        Ok(st)
    }
    pub fn current_root(&self) -> Option<Root> {
        self.root.clone()
    }
    pub fn into_device(self) -> D {
        self.device
    }
    pub fn put_bytes(&mut self, data: &[u8]) -> Result<ObjectId, Error> {
        if self.requires_recovery {
            return Err(Error::RequiresRecovery);
        }
        if data.len() as u64 > MAX - 32 {
            return Err(Error::InvalidInput("object too large"));
        };
        let id = chunk_id(data);
        if self.index.contains_key(&id) {
            let entry = *self.index.get(&id).unwrap();
            if entry.kind != Kind::Chunk {
                return Err(Error::Corruption("object id collision"));
            }
            self.validate_chunk_ref(&id, data.len() as u32)?;
            return Ok(id);
        };
        let mut p = Vec::new();
        p.try_reserve_exact(32 + data.len())
            .map_err(|_| Error::ResourceExhausted("chunk allocation failed"))?;
        p.extend_from_slice(&id);
        p.extend_from_slice(data);
        match self.append(Kind::Chunk, &p, 0) {
            Ok(_) => Ok(id),
            Err(Error::Capacity) => Err(Error::Capacity),
            Err(e) => {
                self.requires_recovery = true;
                Err(e)
            }
        }
    }

    pub fn put_object(&mut self, data: &[u8]) -> Result<ObjectId, Error> {
        self.put_object_with_chunk_size(data, DEFAULT_CHUNK_SIZE)
    }

    pub fn put_object_with_chunk_size(
        &mut self,
        data: &[u8],
        chunk_size: usize,
    ) -> Result<ObjectId, Error> {
        if self.requires_recovery {
            return Err(Error::RequiresRecovery);
        }
        if chunk_size == 0 {
            return Err(Error::InvalidInput("chunk size must be non-zero"));
        }
        if chunk_size > u32::MAX as usize {
            return Err(Error::InvalidInput("chunk size exceeds manifest limit"));
        }
        let chunk_count = data.len().div_ceil(chunk_size);
        if chunk_count > MAX_MANIFEST_CHUNKS {
            return Err(Error::InvalidInput("manifest too large"));
        }
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(chunk_count)
            .map_err(|_| Error::ResourceExhausted("chunk index allocation failed"))?;
        let mut new_chunk_ids = HashSet::new();
        new_chunk_ids
            .try_reserve(chunk_count)
            .map_err(|_| Error::ResourceExhausted("chunk deduplication allocation failed"))?;
        let mut required_end = self.append_offset;
        for bytes in data.chunks(chunk_size) {
            let id = chunk_id(bytes);
            chunks.push(ChunkRef {
                id,
                len: bytes.len() as u32,
            });
            if !self.index.contains_key(&id) && new_chunk_ids.insert(id) {
                required_end = required_end
                    .checked_add(record_span(32 + bytes.len())?)
                    .ok_or(Error::Capacity)?;
            }
        }
        let total = chunks
            .iter()
            .try_fold(0u64, |sum, chunk| sum.checked_add(u64::from(chunk.len)))
            .ok_or(Error::InvalidInput("manifest overflow"))?;
        let manifest_id = manifest_id_for_chunks(&chunks, total);
        let manifest_exists = match self.index.get(&manifest_id) {
            None => false,
            Some(entry) if entry.kind == Kind::Manifest => true,
            Some(_) => return Err(Error::Corruption("object id collision")),
        };
        if manifest_exists {
            self.validate_manifest_record(&manifest_id)?;
        }
        for chunk in &chunks {
            if self.index.contains_key(&chunk.id) {
                self.validate_chunk_ref(&chunk.id, chunk.len)?;
            }
        }
        let manifest_payload_len = 32usize
            .checked_add(MANIFEST_HEADER_SIZE)
            .and_then(|value| {
                chunk_count
                    .checked_mul(MANIFEST_CHUNK_REF_SIZE)
                    .and_then(|refs| value.checked_add(refs))
            })
            .ok_or(Error::InvalidInput("manifest too large"))?;
        if !manifest_exists {
            required_end = required_end
                .checked_add(record_span(manifest_payload_len)?)
                .ok_or(Error::Capacity)?;
        }
        if required_end > self.device.len() {
            return Err(Error::Capacity);
        }
        self.index
            .try_reserve(
                new_chunk_ids
                    .len()
                    .saturating_add(usize::from(!manifest_exists)),
            )
            .map_err(|_| Error::ResourceExhausted("object index allocation failed"))?;
        for bytes in data.chunks(chunk_size) {
            self.put_bytes(bytes)?;
        }
        self.put_manifest(&chunks)
    }
    pub fn put_manifest(&mut self, chunks: &[ChunkRef]) -> Result<ObjectId, Error> {
        if self.requires_recovery {
            return Err(Error::RequiresRecovery);
        }
        if chunks.len() > MAX_MANIFEST_CHUNKS {
            return Err(Error::InvalidInput("manifest too large"));
        }
        let count =
            u32::try_from(chunks.len()).map_err(|_| Error::InvalidInput("too many chunks"))?;
        let total = chunks
            .iter()
            .try_fold(0u64, |a, c| a.checked_add(c.len as u64))
            .ok_or(Error::InvalidInput("manifest overflow"))?;
        let n = 16usize
            .checked_add(
                chunks
                    .len()
                    .checked_mul(36)
                    .ok_or(Error::InvalidInput("manifest too large"))?,
            )
            .ok_or(Error::InvalidInput("manifest too large"))?;
        if n > (MAX as usize).saturating_sub(32) {
            return Err(Error::InvalidInput("manifest too large"));
        }
        let mut body = Vec::new();
        body.try_reserve_exact(n)
            .map_err(|_| Error::ResourceExhausted("manifest allocation failed"))?;
        body.extend_from_slice(&1u16.to_le_bytes());
        body.extend_from_slice(&0u16.to_le_bytes());
        body.extend_from_slice(&count.to_le_bytes());
        body.extend_from_slice(&total.to_le_bytes());
        for c in chunks {
            body.extend_from_slice(&c.id);
            body.extend_from_slice(&c.len.to_le_bytes())
        }
        let id = manifest_id(&body);
        if let Some(entry) = self.index.get(&id) {
            if entry.kind == Kind::Manifest {
                self.validate_manifest_record(&id)?;
                return Ok(id);
            }
            return Err(Error::Corruption("object id collision"));
        }
        let mut p = Vec::new();
        p.try_reserve_exact(32 + n)
            .map_err(|_| Error::ResourceExhausted("manifest allocation failed"))?;
        p.extend_from_slice(&id);
        p.extend_from_slice(&body);
        match self.append(Kind::Manifest, &p, 0) {
            Ok(_) => Ok(id),
            Err(Error::Capacity) => Err(Error::Capacity),
            Err(e) => {
                self.requires_recovery = true;
                Err(e)
            }
        }
    }
    pub fn get_bytes(&mut self, id: &ObjectId) -> Result<Vec<u8>, Error> {
        let e = *self.index.get(id).ok_or(Error::NotFound(*id))?;
        if e.kind != Kind::Chunk || e.len < 32 {
            return Err(Error::Corruption("invalid chunk entry"));
        };
        let (_, p) = self.read(e.offset)?;
        if p.len() < 32 || p[..32] != id[..] || chunk_id(&p[32..]) != *id {
            return Err(Error::Corruption("chunk hash"));
        };
        let payload_len = p.len() - 32;
        let mut data = Vec::new();
        data.try_reserve_exact(payload_len)
            .map_err(|_| Error::ResourceExhausted("chunk allocation failed"))?;
        data.extend_from_slice(&p[32..]);
        Ok(data)
    }

    pub fn get_manifest(&mut self, id: &ObjectId) -> Result<Object, Error> {
        self.read_manifest(id)
    }

    pub fn read_root(&mut self) -> Result<Option<Vec<u8>>, Error> {
        let Some(root) = self.root.clone() else {
            return Ok(None);
        };
        self.read_object(&root.manifest).map(Some)
    }

    pub fn read_object(&mut self, id: &ObjectId) -> Result<Vec<u8>, Error> {
        let object = self.read_manifest(id)?;
        let capacity = usize::try_from(object.len)
            .map_err(|_| Error::ResourceExhausted("object length does not fit usize"))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| Error::ResourceExhausted("object allocation exceeds limit"))?;
        for chunk in object.chunks {
            let data = self.get_bytes(&chunk.id)?;
            if data.len() != chunk.len as usize {
                return Err(Error::Corruption("chunk length mismatch"));
            }
            bytes.extend_from_slice(&data);
        }
        if bytes.len() as u64 != object.len {
            return Err(Error::Corruption("object length mismatch"));
        }
        Ok(bytes)
    }
    pub fn commit_root(
        &mut self,
        manifest: ObjectId,
        generation: Generation,
    ) -> Result<Root, Error> {
        if self.requires_recovery {
            return Err(Error::RequiresRecovery);
        };
        if generation == 0 {
            return Err(Error::InvalidInput("root generation must be non-zero"));
        }
        if !self
            .index
            .get(&manifest)
            .is_some_and(|e| e.kind == Kind::Manifest)
        {
            return Err(Error::NotFound(manifest));
        };
        if self
            .root
            .as_ref()
            .is_some_and(|r| generation <= r.generation)
        {
            return Err(Error::InvalidInput("root generation must increase"));
        };
        self.validate_manifest(&manifest)?;
        let mut p = Vec::with_capacity(40);
        p.extend_from_slice(&manifest);
        p.extend_from_slice(&generation.to_le_bytes());
        let off = match self.append(Kind::Root, &p, generation) {
            Ok(x) => x,
            Err(Error::Capacity) => return Err(Error::Capacity),
            Err(e) => {
                self.requires_recovery = true;
                return Err(e);
            }
        };
        let (h, _) = self.read(off)?;
        let sb = Super {
            generation,
            root_offset: off,
            root_len: 72,
            manifest,
            root_digest: digest(&h, &p),
        }
        .encode();
        let inactive = 1 - self.active;
        if let Err(e) = self
            .device
            .write_at(u64::from(inactive) * SLOT_SIZE, &sb)
            .and_then(|_| self.device.flush_all())
        {
            self.requires_recovery = true;
            return Err(e.into());
        };
        self.active = inactive;
        let r = Root {
            generation,
            manifest,
        };
        self.root = Some(r.clone());
        Ok(r)
    }
    pub fn validate_manifest(&mut self, id: &ObjectId) -> Result<(), Error> {
        let p = self.read_manifest_payload(id)?;
        let body = &p[32..];
        let (count, total) = Self::manifest_layout(body)?;
        self.validate_manifest_chunks(body, count, total)
    }

    fn validate_manifest_record(&mut self, id: &ObjectId) -> Result<(), Error> {
        let p = self.read_manifest_payload(id)?;
        Self::manifest_layout(&p[32..]).map(|_| ())
    }

    fn read_manifest(&mut self, id: &ObjectId) -> Result<Object, Error> {
        let id = *id;
        let p = self.read_manifest_payload(&id)?;
        let body = &p[32..];
        let (count, total) = Self::manifest_layout(body)?;
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(count)
            .map_err(|_| Error::ResourceExhausted("manifest index allocation failed"))?;
        for i in 0..count {
            let a = MANIFEST_HEADER_SIZE + i * MANIFEST_CHUNK_REF_SIZE;
            let cid: ObjectId = body[a..a + 32].try_into().unwrap();
            let len = u32::from_le_bytes(body[a + 32..a + 36].try_into().unwrap());
            self.validate_chunk_ref(&cid, len)?;
            chunks.push(ChunkRef { id: cid, len });
        }
        Ok(Object {
            id,
            len: total,
            chunks,
        })
    }
    fn read_manifest_payload(&mut self, id: &ObjectId) -> Result<Vec<u8>, Error> {
        let id = *id;
        let e = *self.index.get(&id).ok_or(Error::NotFound(id))?;
        if e.kind != Kind::Manifest {
            return Err(Error::WrongObjectKind("manifest"));
        }
        let (_, p) = self.read(e.offset)?;
        if p.len() < 32 + MANIFEST_HEADER_SIZE || manifest_id(&p[32..]) != id {
            return Err(Error::Corruption("manifest hash"));
        }
        Ok(p)
    }
    fn manifest_layout(body: &[u8]) -> Result<(usize, u64), Error> {
        if body.len() < MANIFEST_HEADER_SIZE {
            return Err(Error::Corruption("manifest format"));
        }
        let count = u32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
        let need = MANIFEST_HEADER_SIZE
            .checked_add(
                count
                    .checked_mul(MANIFEST_CHUNK_REF_SIZE)
                    .ok_or(Error::Corruption("manifest count"))?,
            )
            .ok_or(Error::Corruption("manifest length"))?;
        if body.len() != need || body[0..2] != 1u16.to_le_bytes() || body[2..4] != [0; 2] {
            return Err(Error::Corruption("manifest format"));
        }
        let total = u64::from_le_bytes(body[8..16].try_into().unwrap());
        Ok((count, total))
    }
    fn validate_manifest_chunks(
        &mut self,
        body: &[u8],
        count: usize,
        total: u64,
    ) -> Result<(), Error> {
        let mut sum = 0u64;
        for i in 0..count {
            let a = MANIFEST_HEADER_SIZE + i * MANIFEST_CHUNK_REF_SIZE;
            let cid: ObjectId = body[a..a + 32].try_into().unwrap();
            let len = u32::from_le_bytes(body[a + 32..a + 36].try_into().unwrap());
            sum = sum
                .checked_add(u64::from(len))
                .ok_or(Error::Corruption("manifest total"))?;
            self.validate_chunk_ref(&cid, len)?;
        }
        if sum != total {
            return Err(Error::Corruption("manifest total mismatch"));
        }
        Ok(())
    }
    fn validate_chunk_ref(&mut self, id: &ObjectId, len: u32) -> Result<(), Error> {
        let e = *self.index.get(id).ok_or(Error::NotFound(*id))?;
        if e.kind != Kind::Chunk {
            return Err(Error::Corruption("manifest kind"));
        }
        let (_, p) = self.read(e.offset)?;
        if p.len() < 32
            || p.len() - 32 != len as usize
            || p[..32] != id[..]
            || chunk_id(&p[32..]) != *id
        {
            return Err(Error::Corruption("manifest chunk"));
        }
        Ok(())
    }
    fn append(&mut self, kind: Kind, p: &[u8], generation: u64) -> Result<u64, Error> {
        if p.len() > MAX as usize {
            return Err(Error::InvalidInput("payload too large"));
        };
        let mut h = Header {
            kind,
            len: u32::try_from(p.len()).map_err(|_| Error::InvalidInput("payload too large"))?,
            generation,
            checksum: 0,
        };
        h.checksum = u64::from_le_bytes(digest(&h, p)[..8].try_into().unwrap());
        let at = self.append_offset;
        let end = at
            .checked_add((H + p.len()) as u64)
            .ok_or(Error::InvalidInput("offset overflow"))?;
        let next = align(end)?;
        if next > self.device.len() {
            return Err(Error::Capacity);
        };
        let object_id = if matches!(kind, Kind::Chunk | Kind::Manifest) {
            Some(
                p.get(..32)
                    .ok_or(Error::InvalidInput("object id"))?
                    .try_into()
                    .map_err(|_| Error::InvalidInput("object id"))?,
            )
        } else {
            None
        };
        if let Some(id) = object_id {
            if !self.index.contains_key(&id) {
                self.index
                    .try_reserve(1)
                    .map_err(|_| Error::ResourceExhausted("object index allocation failed"))?;
            }
        }
        self.device.write_at(at, &h.encode())?;
        self.device.write_at(at + H as u64, p)?;
        self.device.flush_data()?;
        self.append_offset = next;
        if let Some(id) = object_id {
            self.index.entry(id).or_insert(Entry {
                offset: at,
                kind,
                len: h.len,
            });
        }
        Ok(at)
    }
    fn read(&mut self, off: u64) -> Result<(Header, Vec<u8>), Error> {
        if off % 8 != 0
            || off < RECORDS_START
            || off
                .checked_add(H as u64)
                .is_none_or(|x| x > self.device.len())
        {
            return Err(Error::Corruption("record offset"));
        };
        let mut b = [0; H];
        self.device.read_at(off, &mut b)?;
        let h = Header::decode(&b)?;
        let end = off
            .checked_add(H as u64)
            .and_then(|x| x.checked_add(h.len as u64))
            .ok_or(Error::Corruption("record length"))?;
        if end > self.device.len() {
            return Err(Error::Corruption("truncated record"));
        };
        let payload_len = h.len as usize;
        let mut p = Vec::new();
        p.try_reserve_exact(payload_len)
            .map_err(|_| Error::ResourceExhausted("record payload allocation failed"))?;
        p.resize(payload_len, 0);
        self.device.read_at(off + H as u64, &mut p)?;
        if u64::from_le_bytes(digest(&h, &p)[..8].try_into().unwrap()) != h.checksum {
            return Err(Error::Corruption("record checksum"));
        };
        Ok((h, p))
    }
    fn scan(&mut self, start: u64, end: u64) -> Result<(), Error> {
        let mut o = start;
        while o < end {
            let (h, p) = self.read(o)?;
            let next = align(o + (H + p.len()) as u64)?;
            if next > end {
                return Err(Error::Corruption("committed boundary"));
            };
            if matches!(h.kind, Kind::Chunk | Kind::Manifest) {
                if p.len() < 32 {
                    return Err(Error::Corruption("object payload"));
                };
                let id: ObjectId = p[..32].try_into().unwrap();
                let valid = match h.kind {
                    Kind::Chunk => chunk_id(&p[32..]) == id,
                    Kind::Manifest => manifest_id(&p[32..]) == id,
                    Kind::Root => false,
                };
                if !valid {
                    return Err(Error::Corruption("object hash"));
                }
                self.index_record(
                    id,
                    Entry {
                        offset: o,
                        kind: h.kind,
                        len: h.len,
                    },
                )?;
            }
            o = next
        }
        if o != end {
            Err(Error::Corruption("committed prefix"))
        } else {
            Ok(())
        }
    }
    fn tail(&mut self, start: u64) -> Result<u64, Error> {
        let mut o = start;
        while o + H as u64 <= self.device.len() {
            let mut b = [0; H];
            self.device.read_at(o, &mut b)?;
            if b.iter().all(|v| *v == 0) {
                break;
            }
            let (_, p) = match self.read(o) {
                Ok(value) => value,
                Err(e @ Error::Corruption(_)) => {
                    let _ = e;
                    break;
                }
                Err(e) => return Err(e),
            };
            o = align(o + (H + p.len()) as u64)?;
        }
        Ok(o)
    }
    fn index_record(&mut self, id: ObjectId, entry: Entry) -> Result<(), Error> {
        if self.index.contains_key(&id) {
            return Ok(());
        }
        self.index
            .try_reserve(1)
            .map_err(|_| Error::ResourceExhausted("object index allocation failed"))?;
        self.index.insert(id, entry);
        Ok(())
    }
}

fn record_span(payload_len: usize) -> Result<u64, Error> {
    let bytes = H
        .checked_add(payload_len)
        .ok_or(Error::InvalidInput("record size overflow"))?;
    align(u64::try_from(bytes).map_err(|_| Error::InvalidInput("record size overflow"))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_device::{
        DeviceEffect, DeviceEventKind, DeviceRule, DeviceScript, EventOccurrence, EventSelector,
        SimDisk,
    };
    fn disk() -> SimDisk {
        SimDisk::new(256 * 1024)
    }
    #[test]
    fn commit_reopen() {
        let mut s = Store::format(disk()).unwrap();
        let c = s.put_bytes(b"hello").unwrap();
        let m = s.put_manifest(&[ChunkRef { id: c, len: 5 }]).unwrap();
        s.commit_root(m, 1).unwrap();
        let mut r = Store::open(s.into_device()).unwrap();
        assert_eq!(
            r.current_root(),
            Some(Root {
                generation: 1,
                manifest: m
            })
        );
        assert_eq!(r.get_bytes(&c).unwrap(), b"hello")
    }

    #[test]
    fn committed_root_reads_manifest_and_reassembles_object() {
        let mut store = Store::format(disk()).unwrap();
        let first = store.put_bytes(b"hello").unwrap();
        let second = store.put_bytes(b" world").unwrap();
        let manifest = store
            .put_manifest(&[
                ChunkRef { id: first, len: 5 },
                ChunkRef { id: second, len: 6 },
            ])
            .unwrap();
        store.commit_root(manifest, 1).unwrap();

        let object = store.get_manifest(&manifest).unwrap();
        assert_eq!(object.len, 11);
        assert_eq!(object.chunks.len(), 2);
        assert_eq!(store.read_root().unwrap(), Some(b"hello world".to_vec()));

        let mut reopened = Store::open(store.into_device()).unwrap();
        assert_eq!(reopened.read_root().unwrap(), Some(b"hello world".to_vec()));
    }

    #[test]
    fn chunked_put_and_object_read_round_trip() {
        let data: Vec<u8> = (0..2500).map(|index| (index % 251) as u8).collect();
        let mut store = Store::format(disk()).unwrap();
        let manifest = store.put_object_with_chunk_size(&data, 1000).unwrap();
        let object = store.get_manifest(&manifest).unwrap();
        assert_eq!(object.chunks.len(), 3);
        assert_eq!(store.read_object(&manifest).unwrap(), data);
        store.commit_root(manifest, 1).unwrap();
        let mut reopened = Store::open(store.into_device()).unwrap();
        assert_eq!(reopened.read_root().unwrap(), Some(data));
    }

    #[test]
    fn chunked_put_rejects_zero_chunk_size_before_writing() {
        let mut store = Store::format(disk()).unwrap();
        assert!(matches!(
            store.put_object_with_chunk_size(b"data", 0),
            Err(Error::InvalidInput("chunk size must be non-zero"))
        ));
        assert_eq!(store.put_object(b"data").unwrap().len(), 32);
    }

    #[test]
    fn chunked_put_rejects_unencodable_manifest_before_writing() {
        let mut store = Store::format(disk()).unwrap();
        let before = store.append_offset;
        let data = vec![0u8; MAX_MANIFEST_CHUNKS + 1];
        assert!(matches!(
            store.put_object_with_chunk_size(&data, 1),
            Err(Error::InvalidInput("manifest too large"))
        ));
        assert_eq!(store.append_offset, before);
    }

    #[test]
    fn chunked_put_rejects_capacity_before_writing_and_remains_usable() {
        let mut store = Store::format(SimDisk::new(16 * 1024)).unwrap();
        let before = store.append_offset;
        let data: Vec<u8> = (0..12 * 1024).map(|index| (index / 4096) as u8).collect();
        assert!(matches!(
            store.put_object_with_chunk_size(&data, 4 * 1024),
            Err(Error::Capacity)
        ));
        assert_eq!(store.append_offset, before);
        assert!(!store.requires_recovery);
        let manifest = store.put_object(b"ok").unwrap();
        assert_eq!(store.read_object(&manifest).unwrap(), b"ok");
    }

    #[test]
    fn read_object_rejects_a_chunk_id_as_the_wrong_object_kind() {
        let mut store = Store::format(disk()).unwrap();
        let chunk = store.put_bytes(b"chunk").unwrap();
        assert!(matches!(
            store.read_object(&chunk),
            Err(Error::WrongObjectKind("manifest"))
        ));
    }

    #[test]
    fn repeated_object_put_deduplicates_the_manifest_record() {
        let mut store = Store::format(SimDisk::new(16 * 1024)).unwrap();
        let data = vec![0x5a; 4096];
        let first = store.put_object(&data).unwrap();
        store.put_bytes(&vec![0xa5; 3800]).unwrap();
        let end = store.append_offset;
        assert_eq!(store.put_object(&data).unwrap(), first);
        assert_eq!(store.append_offset, end);
    }

    #[test]
    fn chunked_put_preserves_requires_recovery_before_validating_inputs() {
        let disk = SimDisk::from_script(
            256 * 1024,
            DeviceScript {
                rules: vec![DeviceRule {
                    selector: EventSelector {
                        kind: DeviceEventKind::Write,
                        occurrence: EventOccurrence::Exact(3),
                        range: None,
                    },
                    effect: DeviceEffect::CrashBefore,
                }],
                ..Default::default()
            },
        )
        .unwrap();
        let mut store = Store::format(disk).unwrap();
        assert!(store.put_bytes(b"poison").is_err());
        assert!(matches!(
            store.put_object_with_chunk_size(b"data", 0),
            Err(Error::RequiresRecovery)
        ));
    }

    #[test]
    fn deduplication_does_not_trust_a_corrupted_index_entry() {
        let mut store = Store::format(disk()).unwrap();
        let chunk = store.put_bytes(b"chunk").unwrap();
        let manifest = store
            .put_manifest(&[ChunkRef { id: chunk, len: 5 }])
            .unwrap();
        store.commit_root(manifest, 1).unwrap();
        let other = store.put_manifest(&[]).unwrap();
        store.commit_root(other, 2).unwrap();
        let other_entry = *store.index.get(&other).unwrap();
        store.index.insert(manifest, other_entry);
        assert!(matches!(
            store.put_manifest(&[ChunkRef { id: chunk, len: 5 }]),
            Err(Error::Corruption("manifest hash"))
        ));
    }

    #[test]
    fn direct_capacity_rejections_do_not_poison_the_store() {
        let mut store = Store::format(SimDisk::new(16 * 1024)).unwrap();
        let full_chunk = vec![0x7f; 8050];
        store.put_bytes(&full_chunk).unwrap();
        assert!(matches!(store.put_manifest(&[]), Err(Error::Capacity)));
        assert!(!store.requires_recovery);
        store.put_bytes(b"x").unwrap();

        let mut store = Store::format(SimDisk::new(16 * 1024)).unwrap();
        let chunk = store.put_bytes(b"root").unwrap();
        let manifest = store
            .put_manifest(&[ChunkRef { id: chunk, len: 4 }])
            .unwrap();
        store.put_bytes(&vec![0x23; 7900]).unwrap();
        assert!(matches!(
            store.commit_root(manifest, 1),
            Err(Error::Capacity)
        ));
        assert!(!store.requires_recovery);
    }

    #[test]
    fn chunked_put_boundary_matrix_round_trips() {
        for chunk_size in [1usize, 2, 3, 7, 16, 1024] {
            for len in 0..=33usize {
                let data: Vec<u8> = (0..len)
                    .map(|index| (index as u8).wrapping_mul(17))
                    .collect();
                let mut store = Store::format(disk()).unwrap();
                let manifest = store.put_object_with_chunk_size(&data, chunk_size).unwrap();
                assert_eq!(store.read_object(&manifest).unwrap(), data);
            }
        }
    }

    #[test]
    fn empty_committed_root_reads_as_empty_object() {
        let mut store = Store::format(disk()).unwrap();
        let manifest = store.put_manifest(&[]).unwrap();
        store.commit_root(manifest, 1).unwrap();
        assert_eq!(store.get_manifest(&manifest).unwrap().len, 0);
        assert_eq!(store.read_root().unwrap(), Some(Vec::new()));
    }

    #[test]
    fn multi_chunk_object_larger_than_record_limit_survives_reopen() {
        const CHUNK_SIZE: usize = 1024 * 1024;
        const CHUNK_COUNT: usize = 65;
        let mut store = Store::format(SimDisk::new(96 * 1024 * 1024)).unwrap();
        let mut chunks = Vec::with_capacity(CHUNK_COUNT);
        for index in 0..CHUNK_COUNT {
            let data = vec![u8::try_from(index).unwrap(); CHUNK_SIZE];
            let id = store.put_bytes(&data).unwrap();
            chunks.push(ChunkRef {
                id,
                len: CHUNK_SIZE as u32,
            });
        }
        let manifest = store.put_manifest(&chunks).unwrap();
        store.commit_root(manifest, 1).unwrap();

        let mut reopened = Store::open(store.into_device()).unwrap();
        let object = reopened.read_root().unwrap().unwrap();
        assert_eq!(object.len(), CHUNK_SIZE * CHUNK_COUNT);
        assert_eq!(object[0], 0);
        assert_eq!(object[CHUNK_SIZE], 1);
        assert_eq!(object.last(), Some(&64));
    }
    #[test]
    fn chunk_is_not_manifest() {
        let mut s = Store::format(disk()).unwrap();
        let c = s.put_bytes(b"x").unwrap();
        assert!(matches!(s.commit_root(c, 1), Err(Error::NotFound(_))))
    }

    #[test]
    fn generation_zero_is_reserved() {
        let mut s = Store::format(disk()).unwrap();
        let c = s.put_bytes(b"x").unwrap();
        let m = s.put_manifest(&[ChunkRef { id: c, len: 1 }]).unwrap();
        assert!(matches!(
            s.commit_root(m, 0),
            Err(Error::InvalidInput("root generation must be non-zero"))
        ));
    }

    #[test]
    fn uncommitted_objects_are_not_visible_after_reopen() {
        let mut s = Store::format(disk()).unwrap();
        let c = s.put_bytes(b"uncommitted").unwrap();
        let mut disk = s.into_device();
        disk.power_loss();
        let mut reopened = Store::open(disk).unwrap();
        assert!(matches!(reopened.get_bytes(&c), Err(Error::NotFound(_))));
    }

    #[test]
    fn contract_round_trip_matrix() {
        // Bounded property-style coverage without sockets, clocks, or external RNG.
        for case in 0..32u8 {
            let mut data = Vec::with_capacity(1 + usize::from(case) * 17);
            for i in 0..(1 + usize::from(case) * 17) {
                data.push(case.wrapping_mul(31).wrapping_add(i as u8));
            }
            let mut store = Store::format(disk()).unwrap();
            let id = store.put_bytes(&data).unwrap();
            assert_eq!(id, chunk_id(&data));
            let manifest = store
                .put_manifest(&[ChunkRef {
                    id,
                    len: u32::try_from(data.len()).unwrap(),
                }])
                .unwrap();
            let generation = u64::from(case) + 1;
            store.commit_root(manifest, generation).unwrap();
            let mut reopened = Store::open(store.into_device()).unwrap();
            assert_eq!(reopened.current_root().unwrap().generation, generation);
            assert_eq!(reopened.get_bytes(&id).unwrap(), data);
        }
    }

    #[test]
    fn contract_append_failure_requires_recovery() {
        let mut store = Store::format(
            SimDisk::from_script(
                256 * 1024,
                DeviceScript {
                    rules: vec![DeviceRule {
                        selector: EventSelector {
                            kind: DeviceEventKind::Write,
                            occurrence: EventOccurrence::Exact(3),
                            range: None,
                        },
                        effect: DeviceEffect::Fail,
                    }],
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            store.put_bytes(b"first"),
            Err(Error::RequiresRecovery) | Err(Error::Device(_))
        ));
        assert!(matches!(
            store.put_bytes(b"second"),
            Err(Error::RequiresRecovery)
        ));
    }
}
