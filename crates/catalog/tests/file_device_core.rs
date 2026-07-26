#![cfg(any(unix, windows))]

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cairn_core::{ChunkRef, Error as CoreError, Store};
use cairn_device::io::{BlockDevice, FileDevice};

const DISK_SIZE: u64 = 256 * 1024;

fn test_path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "cairn-file-store-{}-{nonce}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

struct TestFile(Option<PathBuf>);

impl TestFile {
    fn new(path: PathBuf) -> Self {
        Self(Some(path))
    }

    fn cleanup(&mut self) {
        let path = self.0.take().expect("test file already cleaned up");
        fs::remove_file(path).expect("remove temporary file");
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = fs::remove_file(path);
        }
    }
}

#[test]
#[ignore = "real filesystem recovery gate; run explicitly with --ignored"]
fn format_empty_file_closes_and_reopens() {
    let path = test_path();
    let mut cleanup = TestFile::new(path.clone());

    let store = Store::format(FileDevice::create_preallocated(&path, DISK_SIZE).unwrap()).unwrap();
    drop(store.into_device());

    let reopened = Store::open(FileDevice::open(&path).unwrap()).unwrap();
    assert_eq!(reopened.current_root(), None);
    drop(reopened.into_device());
    cleanup.cleanup();
}

#[test]
#[ignore = "real filesystem recovery gate; run explicitly with --ignored"]
fn store_commit_reopen_and_latest_root_corruption_falls_back() {
    let path = test_path();
    let mut cleanup = TestFile::new(path.clone());

    let mut store =
        Store::format(FileDevice::create_preallocated(&path, DISK_SIZE).unwrap()).unwrap();
    let committed = b"committed bytes";
    let committed_id = store.put_bytes(committed).unwrap();
    let committed_manifest = store
        .put_manifest(&[ChunkRef {
            id: committed_id,
            len: committed.len() as u32,
        }])
        .unwrap();
    let root_9 = store.commit_root(committed_manifest, 9).unwrap();

    let pending = b"pending but uncommitted bytes";
    let pending_id = store.put_bytes(pending).unwrap();
    let pending_manifest = store
        .put_manifest(&[ChunkRef {
            id: pending_id,
            len: pending.len() as u32,
        }])
        .unwrap();
    drop(store.into_device());

    let mut reopened = Store::open(FileDevice::open(&path).unwrap()).unwrap();
    assert_eq!(reopened.current_root(), Some(root_9.clone()));
    assert_eq!(reopened.get_bytes(&committed_id).unwrap(), committed);
    assert!(matches!(
        reopened.get_bytes(&pending_id),
        Err(CoreError::NotFound(id)) if id == pending_id
    ));
    assert!(matches!(
        reopened.commit_root(pending_manifest, 10),
        Err(CoreError::NotFound(id)) if id == pending_manifest
    ));

    let next = b"next committed bytes";
    let next_id = reopened.put_bytes(next).unwrap();
    let next_manifest = reopened
        .put_manifest(&[ChunkRef {
            id: next_id,
            len: next.len() as u32,
        }])
        .unwrap();
    let root_42 = reopened.commit_root(next_manifest, 42).unwrap();
    drop(reopened.into_device());

    let mut final_store = Store::open(FileDevice::open(&path).unwrap()).unwrap();
    assert_eq!(final_store.current_root(), Some(root_42));
    assert_eq!(final_store.get_bytes(&next_id).unwrap(), next);
    assert_eq!(final_store.get_bytes(&committed_id).unwrap(), committed);
    drop(final_store.into_device());

    let mut damaged = FileDevice::open(&path).unwrap();
    damaged.write_at(0, &[0xa5]).unwrap();
    damaged.flush_all().unwrap();
    let mut fallback = Store::open(damaged).unwrap();
    assert_eq!(fallback.current_root(), Some(root_9));
    assert_eq!(fallback.get_bytes(&committed_id).unwrap(), committed);
    assert!(matches!(
        fallback.get_bytes(&next_id),
        Err(CoreError::NotFound(id)) if id == next_id
    ));
    assert!(matches!(
        fallback.get_bytes(&pending_id),
        Err(CoreError::NotFound(id)) if id == pending_id
    ));
    drop(fallback.into_device());
    cleanup.cleanup();
}
