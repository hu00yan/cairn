#![deny(unsafe_code)]
use cairn_device::{BlockDevice, DeviceError};
use std::collections::HashMap;

pub type ObjectId = [u8; 32];
pub type Generation = u64;
pub const SLOT_SIZE: u64 = 4096;
pub const RECORDS_START: u64 = SLOT_SIZE * 2;
const H: usize = 32;
const SB: usize = 128;
const MAGIC: u32 = 0x4341_4952;
const SMAGIC: u32 = 0x4341_5350;
const VERSION: u16 = 2;
const MAX: u64 = 64 * 1024 * 1024;

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
    InvalidInput(&'static str),
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
                    if !matches!(e, Error::Corruption(_) | Error::NotFound(_)) {
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
            return Ok(id);
        };
        let mut p = Vec::with_capacity(32 + data.len());
        p.extend_from_slice(&id);
        p.extend_from_slice(data);
        if let Err(e) = self.append(Kind::Chunk, &p, 0) {
            self.requires_recovery = true;
            return Err(e);
        }
        Ok(id)
    }
    pub fn put_manifest(&mut self, chunks: &[ChunkRef]) -> Result<ObjectId, Error> {
        if self.requires_recovery {
            return Err(Error::RequiresRecovery);
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
            .map_err(|_| Error::InvalidInput("manifest too large"))?;
        body.extend_from_slice(&1u16.to_le_bytes());
        body.extend_from_slice(&0u16.to_le_bytes());
        body.extend_from_slice(&count.to_le_bytes());
        body.extend_from_slice(&total.to_le_bytes());
        for c in chunks {
            body.extend_from_slice(&c.id);
            body.extend_from_slice(&c.len.to_le_bytes())
        }
        let id = manifest_id(&body);
        let mut p = Vec::new();
        p.try_reserve_exact(32 + n)
            .map_err(|_| Error::InvalidInput("manifest too large"))?;
        p.extend_from_slice(&id);
        p.extend_from_slice(&body);
        if let Err(e) = self.append(Kind::Manifest, &p, 0) {
            self.requires_recovery = true;
            return Err(e);
        }
        Ok(id)
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
        Ok(p[32..].to_vec())
    }

    pub fn get_manifest(&mut self, id: &ObjectId) -> Result<Object, Error> {
        self.read_manifest(id)
    }

    pub fn read_root(&mut self) -> Result<Option<Vec<u8>>, Error> {
        let Some(root) = self.root.clone() else {
            return Ok(None);
        };
        let object = self.read_manifest(&root.manifest)?;
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
        Ok(Some(bytes))
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
        self.read_manifest(id).map(|_| ())
    }

    fn read_manifest(&mut self, id: &ObjectId) -> Result<Object, Error> {
        let id = *id;
        let e = *self.index.get(&id).ok_or(Error::NotFound(id))?;
        if e.kind != Kind::Manifest {
            return Err(Error::Corruption("root not manifest"));
        }
        let (_, p) = self.read(e.offset)?;
        if p.len() < 48 || manifest_id(&p[32..]) != id {
            return Err(Error::Corruption("manifest hash"));
        }
        let body = &p[32..];
        let count = u32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
        let need = 16usize
            .checked_add(
                count
                    .checked_mul(36)
                    .ok_or(Error::Corruption("manifest count"))?,
            )
            .ok_or(Error::Corruption("manifest length"))?;
        if body.len() != need || body[0..2] != 1u16.to_le_bytes() || body[2..4] != [0; 2] {
            return Err(Error::Corruption("manifest format"));
        }
        let total = u64::from_le_bytes(body[8..16].try_into().unwrap());
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(count)
            .map_err(|_| Error::ResourceExhausted("manifest index allocation failed"))?;
        let mut sum = 0u64;
        for i in 0..count {
            let a = 16 + i * 36;
            let cid: ObjectId = body[a..a + 32].try_into().unwrap();
            let len = u32::from_le_bytes(body[a + 32..a + 36].try_into().unwrap());
            sum = sum
                .checked_add(u64::from(len))
                .ok_or(Error::Corruption("manifest total"))?;
            let ce = *self.index.get(&cid).ok_or(Error::NotFound(cid))?;
            if ce.kind != Kind::Chunk {
                return Err(Error::Corruption("manifest kind"));
            }
            let (_, cp) = self.read(ce.offset)?;
            if cp.len() < 32
                || cp.len() - 32 != len as usize
                || cp[..32] != cid
                || chunk_id(&cp[32..]) != cid
            {
                return Err(Error::Corruption("manifest chunk"));
            }
            chunks.push(ChunkRef { id: cid, len });
        }
        if sum != total {
            return Err(Error::Corruption("manifest total mismatch"));
        }
        Ok(Object {
            id,
            len: total,
            chunks,
        })
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
        if end > self.device.len() {
            return Err(Error::InvalidInput("device full"));
        };
        self.device.write_at(at, &h.encode())?;
        self.device.write_at(at + H as u64, p)?;
        self.device.flush_data()?;
        self.append_offset = align(end)?;
        if matches!(kind, Kind::Chunk | Kind::Manifest) {
            let id: ObjectId = p
                .get(..32)
                .ok_or(Error::InvalidInput("object id"))?
                .try_into()
                .map_err(|_| Error::InvalidInput("object id"))?;
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
        let mut p = vec![0; h.len as usize];
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
                self.index.entry(id).or_insert(Entry {
                    offset: o,
                    kind: h.kind,
                    len: h.len,
                });
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
