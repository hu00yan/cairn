use cairn_catalog::{ChunkRef, Error as ModelError, Model, ObjectId, Root as ModelRoot};
use cairn_core::{ChunkRef as CoreChunkRef, Error as CoreError, Root as CoreRoot, Store};
use cairn_device::io::SimDisk;

const DISK_SIZE: usize = 64 * 1024;

fn disk() -> SimDisk {
    SimDisk::new(DISK_SIZE)
}

fn core_chunks(chunks: &[ChunkRef]) -> Vec<CoreChunkRef> {
    chunks
        .iter()
        .map(|chunk| CoreChunkRef {
            id: chunk.id,
            len: chunk.len,
        })
        .collect()
}

fn assert_roots_equal(core: Option<CoreRoot>, model: Option<ModelRoot>) {
    assert_eq!(
        core.map(|root| (root.generation, root.manifest)),
        model.map(|root| (root.generation, root.manifest))
    );
}

#[test]
fn core_and_model_use_the_same_chunk_and_manifest_ids() {
    let bytes = [b"first chunk".as_slice(), b"second chunk".as_slice()];
    let mut core = Store::format(disk()).unwrap();
    let mut model = Model::default();
    let mut chunks = Vec::new();

    for data in bytes {
        let core_id = core.put_bytes(data).unwrap();
        let model_id = model.put_bytes(data.to_vec()).unwrap();
        assert_eq!(core_id, model_id);
        chunks.push(ChunkRef {
            id: model_id,
            len: data.len() as u32,
        });
    }

    let core_manifest = core.put_manifest(&core_chunks(&chunks)).unwrap();
    let model_manifest = model.put_manifest(&chunks).unwrap();
    assert_eq!(core_manifest, model_manifest);
}

#[test]
fn core_and_model_commit_the_same_roots_at_generations_9_and_42() {
    let first = b"generation nine";
    let second = b"generation forty two";
    let mut core = Store::format(disk()).unwrap();
    let mut model = Model::default();

    let first_id = core.put_bytes(first).unwrap();
    assert_eq!(model.put_bytes(first.to_vec()).unwrap(), first_id);
    let first_chunks = [CoreChunkRef {
        id: first_id,
        len: first.len() as u32,
    }];
    let first_manifest = core.put_manifest(&first_chunks).unwrap();
    assert_eq!(
        model
            .put_manifest(&[ChunkRef {
                id: first_id,
                len: first.len() as u32,
            }])
            .unwrap(),
        first_manifest
    );
    let core_root_9 = core.commit_root(first_manifest, 9).unwrap();
    let model_root_9 = model.commit_root(first_manifest, 9).unwrap();
    assert_roots_equal(Some(core_root_9), Some(model_root_9));

    let second_id = core.put_bytes(second).unwrap();
    assert_eq!(model.put_bytes(second.to_vec()).unwrap(), second_id);
    let core_manifest_42 = core
        .put_manifest(&[CoreChunkRef {
            id: second_id,
            len: second.len() as u32,
        }])
        .unwrap();
    let model_manifest_42 = model
        .put_manifest(&[ChunkRef {
            id: second_id,
            len: second.len() as u32,
        }])
        .unwrap();
    assert_eq!(core_manifest_42, model_manifest_42);
    let core_root_42 = core.commit_root(core_manifest_42, 42).unwrap();
    let model_root_42 = model.commit_root(model_manifest_42, 42).unwrap();
    assert_roots_equal(Some(core_root_42), Some(model_root_42));
}

fn mirrored_manifest(
    core: &mut Store<SimDisk>,
    model: &mut Model,
    chunks: &[ChunkRef],
) -> ObjectId {
    let core_manifest = core.put_manifest(&core_chunks(chunks)).unwrap();
    let model_manifest = model.put_manifest(chunks).unwrap();
    assert_eq!(core_manifest, model_manifest);
    core_manifest
}

fn valid_chunk(core: &mut Store<SimDisk>, model: &mut Model, bytes: &[u8]) -> ChunkRef {
    let id = core.put_bytes(bytes).unwrap();
    assert_eq!(model.put_bytes(bytes.to_vec()).unwrap(), id);
    ChunkRef {
        id,
        len: bytes.len() as u32,
    }
}

fn assert_chunk_as_root(
    core_result: Result<CoreRoot, CoreError>,
    model_result: Result<ModelRoot, ModelError>,
    object_id: ObjectId,
) {
    assert!(matches!(
        core_result,
        Err(CoreError::NotFound(id)) if id == object_id
    ));
    assert!(matches!(
        model_result,
        Err(ModelError::InvalidManifest(id)) if id == object_id
    ));
}

fn assert_missing_chunk(
    core_result: Result<CoreRoot, CoreError>,
    model_result: Result<ModelRoot, ModelError>,
    object_id: ObjectId,
) {
    assert!(matches!(
        core_result,
        Err(CoreError::NotFound(id)) if id == object_id
    ));
    assert!(matches!(
        model_result,
        Err(ModelError::NotFound(id)) if id == object_id
    ));
}

fn assert_invalid_generation(
    core_result: Result<CoreRoot, CoreError>,
    model_result: Result<ModelRoot, ModelError>,
    expected_core_message: &'static str,
) {
    assert!(matches!(
        core_result,
        Err(CoreError::InvalidInput(message)) if message == expected_core_message
    ));
    assert_eq!(model_result, Err(ModelError::InvalidGeneration));
}

#[test]
fn core_and_model_report_commit_semantics_and_commit_all_chunks() {
    let mut core = Store::format(disk()).unwrap();
    let mut model = Model::default();

    let old_chunks = [
        valid_chunk(&mut core, &mut model, b"old first"),
        valid_chunk(&mut core, &mut model, b"old second"),
    ];

    let chunk_as_root = old_chunks[0].id;
    assert_chunk_as_root(
        core.commit_root(chunk_as_root, 1),
        model.commit_root(chunk_as_root, 1),
        chunk_as_root,
    );
    assert_roots_equal(core.current_root(), model.current_root());

    let missing = [7; 32];
    let existing_then_missing = mirrored_manifest(
        &mut core,
        &mut model,
        &[
            old_chunks[0].clone(),
            ChunkRef {
                id: missing,
                len: 4,
            },
        ],
    );
    assert_missing_chunk(
        core.commit_root(existing_then_missing, 1),
        model.commit_root(existing_then_missing, 1),
        missing,
    );

    let missing_then_existing = mirrored_manifest(
        &mut core,
        &mut model,
        &[
            ChunkRef {
                id: missing,
                len: 4,
            },
            old_chunks[1].clone(),
        ],
    );
    assert_missing_chunk(
        core.commit_root(missing_then_existing, 1),
        model.commit_root(missing_then_existing, 1),
        missing,
    );
    assert_roots_equal(core.current_root(), model.current_root());

    let manifest_9 = mirrored_manifest(&mut core, &mut model, &old_chunks);
    let core_root_9 = core.commit_root(manifest_9, 9).unwrap();
    let model_root_9 = model.commit_root(manifest_9, 9).unwrap();
    assert_eq!(
        (core_root_9.generation, core_root_9.manifest),
        (model_root_9.generation, model_root_9.manifest)
    );
    assert_roots_equal(core.current_root(), model.current_root());

    assert_invalid_generation(
        core.commit_root(manifest_9, 0),
        model.commit_root(manifest_9, 0),
        "root generation must be non-zero",
    );
    assert_invalid_generation(
        core.commit_root(manifest_9, 9),
        model.commit_root(manifest_9, 9),
        "root generation must increase",
    );
    assert_invalid_generation(
        core.commit_root(manifest_9, 8),
        model.commit_root(manifest_9, 8),
        "root generation must increase",
    );

    let new_chunks = [
        valid_chunk(&mut core, &mut model, b"new first"),
        valid_chunk(&mut core, &mut model, b"new second"),
    ];
    let all_chunks = [
        old_chunks[0].clone(),
        old_chunks[1].clone(),
        new_chunks[0].clone(),
        new_chunks[1].clone(),
    ];
    let manifest_42 = mirrored_manifest(&mut core, &mut model, &all_chunks);
    let core_root_42 = core.commit_root(manifest_42, 42).unwrap();
    let model_root_42 = model.commit_root(manifest_42, 42).unwrap();
    assert_eq!(
        (core_root_42.generation, core_root_42.manifest),
        (model_root_42.generation, model_root_42.manifest)
    );
    assert_roots_equal(core.current_root(), model.current_root());
    for chunk in all_chunks {
        assert_eq!(
            core.get_bytes(&chunk.id).unwrap(),
            model.get(&chunk.id).unwrap()
        );
    }
}

#[test]
fn reopen_after_crash_only_exposes_the_last_committed_root() {
    let mut core = Store::format(disk()).unwrap();
    let mut model = Model::default();
    let committed_chunk = valid_chunk(&mut core, &mut model, b"committed bytes");
    let committed = mirrored_manifest(&mut core, &mut model, &[committed_chunk]);
    let core_root = core.commit_root(committed, 9).unwrap();
    let model_root = model.commit_root(committed, 9).unwrap();

    let pending = b"pending bytes";
    let pending_id = core.put_bytes(pending).unwrap();
    assert_eq!(model.put_bytes(pending.to_vec()).unwrap(), pending_id);
    let mut disk = core.into_device();
    disk.power_loss();
    let mut reopened_core = Store::open(disk).unwrap();
    let reopened_model = model.reopen();
    assert_eq!(reopened_core.current_root(), Some(core_root));
    assert_eq!(reopened_model.current_root(), Some(model_root));
    assert!(matches!(
        reopened_core.get_bytes(&pending_id),
        Err(CoreError::NotFound(id)) if id == pending_id
    ));
    assert_eq!(reopened_model.get(&pending_id), None);
}
