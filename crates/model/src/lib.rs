pub mod catalog;
pub mod dag;
pub mod native;

pub use native::{
    NativeError, OperationTerminal, OperationView, OperationViewResult, SnapshotHandle, Store,
    StoreConfig, Version, WriteTxn, MAX_RANGE_WRITE_BYTES,
};

pub use catalog::{
    Candidate, Capability, CatalogError, CatalogHandoff, Collection, CollectionId, CommitId,
    CommitReceipt, DagBindingState, DurableDagReceipt, FenceToken, File, FileId, FileVersion,
    Generation as CatalogGeneration, Head, IndexState, IntentState, Membership, ModelCatalog,
    Operation, OperationId, OperationKind, OperationRecord, OperationResult, Principal,
    PrincipalId, PrincipalKind, PrincipalState, PublicPublishIntent, PublishIntent, VersionId,
};

pub use dag::{
    content_digest, decode_node, encode_node_payload, node_id, CommitNode, ContentNode, Dag,
    DagError, DagOperationBindingState, Node, NodeId, NodeKind, RangeMapEntry, RangeMapNode,
    SnapshotNode, ZeroRunNode,
};

use std::collections::HashMap;

pub type ObjectId = [u8; 32];
pub type Generation = u64;

const CHUNK_DOMAIN: &[u8] = b"cairn/chunk/v1\0";
const MANIFEST_DOMAIN: &[u8] = b"cairn/manifest/v1\0";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Root {
    pub generation: Generation,
    pub manifest: ObjectId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkRef {
    pub id: ObjectId,
    pub len: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Manifest {
    pub chunks: Vec<ChunkRef>,
    pub total_len: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidObjectId(ObjectId),
    ConflictingObject(ObjectId),
    NotFound(ObjectId),
    InvalidManifest(ObjectId),
    InvalidGeneration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Object {
    Chunk(Vec<u8>),
    Manifest(Manifest),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Model {
    committed: HashMap<ObjectId, Object>,
    pending: HashMap<ObjectId, Object>,
    root: Option<Root>,
}

pub fn chunk_id(bytes: &[u8]) -> ObjectId {
    digest(CHUNK_DOMAIN, bytes)
}

pub fn manifest_id(body: &[u8]) -> ObjectId {
    digest(MANIFEST_DOMAIN, body)
}

fn digest(domain: &[u8], bytes: &[u8]) -> ObjectId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

impl Model {
    pub fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), Error> {
        if chunk_id(&bytes) != id {
            return Err(Error::InvalidObjectId(id));
        }
        self.stage(id, Object::Chunk(bytes))
    }

    pub fn put_bytes(&mut self, bytes: Vec<u8>) -> Result<ObjectId, Error> {
        let id = chunk_id(&bytes);
        self.put(id, bytes)?;
        Ok(id)
    }

    pub fn put_manifest(&mut self, chunks: &[ChunkRef]) -> Result<ObjectId, Error> {
        let total_len = chunks
            .iter()
            .try_fold(0u64, |total, chunk| total.checked_add(u64::from(chunk.len)))
            .ok_or(Error::InvalidManifest([0; 32]))?;
        let body = encode_manifest(chunks, total_len);
        let id = manifest_id(&body);
        self.stage(
            id,
            Object::Manifest(Manifest {
                chunks: chunks.to_vec(),
                total_len,
            }),
        )?;
        Ok(id)
    }

    pub fn get(&self, id: &ObjectId) -> Option<&[u8]> {
        match self.committed.get(id) {
            Some(Object::Chunk(bytes)) => Some(bytes.as_slice()),
            _ => None,
        }
    }

    pub fn pending(&self, id: &ObjectId) -> Option<&[u8]> {
        match self.pending.get(id) {
            Some(Object::Chunk(bytes)) => Some(bytes.as_slice()),
            _ => None,
        }
    }

    pub fn current_root(&self) -> Option<Root> {
        self.root.clone()
    }

    pub fn committed_manifest(&self, id: &ObjectId) -> Option<Manifest> {
        match self.committed.get(id) {
            Some(Object::Manifest(manifest)) => Some(manifest.clone()),
            _ => None,
        }
    }

    pub fn commit_root(
        &mut self,
        manifest: ObjectId,
        generation: Generation,
    ) -> Result<Root, Error> {
        if generation == 0
            || self
                .root
                .as_ref()
                .is_some_and(|root| generation <= root.generation)
        {
            return Err(Error::InvalidGeneration);
        }
        let candidate = self
            .pending
            .get(&manifest)
            .or_else(|| self.committed.get(&manifest));
        let Some(Object::Manifest(manifest_object)) = candidate else {
            return Err(
                if self.committed.contains_key(&manifest) || self.pending.contains_key(&manifest) {
                    Error::InvalidManifest(manifest)
                } else {
                    Error::NotFound(manifest)
                },
            );
        };
        for chunk in &manifest_object.chunks {
            let object = self
                .pending
                .get(&chunk.id)
                .or_else(|| self.committed.get(&chunk.id));
            match object {
                Some(Object::Chunk(bytes)) if bytes.len() == chunk.len as usize => {}
                _ => return Err(Error::NotFound(chunk.id)),
            }
        }
        self.committed.extend(self.pending.drain());
        let root = Root {
            generation,
            manifest,
        };
        self.root = Some(root.clone());
        Ok(root)
    }

    pub fn crash_reopen(&self) -> Self {
        Self {
            committed: self.committed.clone(),
            pending: HashMap::new(),
            root: self.root.clone(),
        }
    }

    pub fn reopen(&self) -> Self {
        self.crash_reopen()
    }

    fn stage(&mut self, id: ObjectId, object: Object) -> Result<(), Error> {
        if self.committed.contains_key(&id) {
            return match self.committed.get(&id) {
                Some(existing) if existing == &object => Ok(()),
                _ => Err(Error::ConflictingObject(id)),
            };
        }
        match self.pending.get(&id) {
            Some(existing) if existing == &object => Ok(()),
            Some(_) => Err(Error::ConflictingObject(id)),
            None => {
                self.pending.insert(id, object);
                Ok(())
            }
        }
    }
}

fn encode_manifest(chunks: &[ChunkRef], total_len: u64) -> Vec<u8> {
    let mut body = Vec::with_capacity(16 + chunks.len() * 36);
    body.extend_from_slice(&1u16.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
    body.extend_from_slice(&total_len.to_le_bytes());
    for chunk in chunks {
        body.extend_from_slice(&chunk.id);
        body.extend_from_slice(&chunk.len.to_le_bytes());
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    fn forged_id(byte: u8) -> ObjectId {
        [byte; 32]
    }

    #[test]
    fn bounded_operation_matrix_preserves_invariants() {
        let first_bytes = b"hello".to_vec();
        let second_bytes = b"world!".to_vec();
        let mut model = Model::default();
        let first = chunk_id(&first_bytes);
        let second = chunk_id(&second_bytes);

        assert_eq!(
            model.put(forged_id(1), first_bytes.clone()),
            Err(Error::InvalidObjectId(forged_id(1)))
        );
        assert_eq!(model.put(first, first_bytes.clone()), Ok(()));
        assert_eq!(model.put(second, second_bytes.clone()), Ok(()));
        assert_eq!(
            model.put(first, b"different".to_vec()),
            Err(Error::InvalidObjectId(first))
        );

        let missing = forged_id(2);
        let existing_then_missing = model
            .put_manifest(&[
                ChunkRef {
                    id: first,
                    len: first_bytes.len() as u32,
                },
                ChunkRef {
                    id: missing,
                    len: 3,
                },
            ])
            .unwrap();
        assert_eq!(
            model.commit_root(existing_then_missing, 1),
            Err(Error::NotFound(missing))
        );

        let missing_then_existing = model
            .put_manifest(&[
                ChunkRef {
                    id: missing,
                    len: 3,
                },
                ChunkRef {
                    id: second,
                    len: second_bytes.len() as u32,
                },
            ])
            .unwrap();
        assert_eq!(
            model.commit_root(missing_then_existing, 1),
            Err(Error::NotFound(missing))
        );

        let manifest = model
            .put_manifest(&[
                ChunkRef {
                    id: first,
                    len: first_bytes.len() as u32,
                },
                ChunkRef {
                    id: second,
                    len: second_bytes.len() as u32,
                },
            ])
            .unwrap();
        assert_eq!(model.commit_root(manifest, 9).unwrap().generation, 9);
        assert_eq!(
            model.commit_root(manifest, 0),
            Err(Error::InvalidGeneration)
        );
        assert_eq!(
            model.commit_root(manifest, 9),
            Err(Error::InvalidGeneration)
        );
        assert_eq!(
            model.commit_root(manifest, 8),
            Err(Error::InvalidGeneration)
        );

        let next_manifest = model
            .put_manifest(&[ChunkRef {
                id: second,
                len: second_bytes.len() as u32,
            }])
            .unwrap();
        assert_eq!(model.commit_root(next_manifest, 42).unwrap().generation, 42);

        let pending_id = model.put_bytes(b"not committed".to_vec()).unwrap();
        let mut reopened = model.crash_reopen();
        assert_eq!(reopened.current_root(), model.current_root());
        assert_eq!(reopened.get(&first), Some(first_bytes.as_slice()));
        assert_eq!(reopened.get(&pending_id), None);
        assert_eq!(
            reopened.commit_root(pending_id, 43),
            Err(Error::NotFound(pending_id))
        );
    }

    #[test]
    fn manifest_encoding_matches_core() {
        let first_bytes = b"core-first";
        let second_bytes = b"core-second";
        let first = chunk_id(first_bytes);
        let second = chunk_id(second_bytes);
        let chunks = [
            ChunkRef {
                id: first,
                len: first_bytes.len() as u32,
            },
            ChunkRef {
                id: second,
                len: second_bytes.len() as u32,
            },
        ];

        let disk = cairn_device::SimDisk::new(16 * 1024);
        let mut core = cairn_core::Store::format(disk).unwrap();
        assert_eq!(core.put_bytes(first_bytes).unwrap(), first);
        assert_eq!(core.put_bytes(second_bytes).unwrap(), second);
        let core_manifest = core
            .put_manifest(
                &chunks
                    .iter()
                    .map(|chunk| cairn_core::ChunkRef {
                        id: chunk.id,
                        len: chunk.len,
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();

        let mut model = Model::default();
        model.put(first, first_bytes.to_vec()).unwrap();
        model.put(second, second_bytes.to_vec()).unwrap();
        let model_manifest = model.put_manifest(&chunks).unwrap();

        assert_eq!(model_manifest, core_manifest);
    }
}
