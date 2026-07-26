#![cfg(any(unix, windows))]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cairn_catalog::{Node, ZeroRunNode};
use cairn_device::{dag_store::FileDagStore, io::FileDevice};

fn test_path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "cairn-file-dag-{}-{nonce}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

#[test]
fn file_device_record_log_survives_reopen() {
    let path = test_path();
    let result = {
        let device = FileDevice::create_preallocated(&path, 4096).unwrap();
        let mut store = FileDagStore::open(device).unwrap();
        let node = Node::ZeroRun(ZeroRunNode { len: 8192 });
        let id = store.append_node(&node).unwrap();
        drop(store.into_inner());

        let device = FileDevice::open(&path).unwrap();
        let mut reopened = FileDagStore::open(device).unwrap();
        assert_eq!(reopened.node(&id).unwrap(), Some(node));
        drop(reopened.into_inner());
        Ok::<(), Box<dyn std::error::Error>>(())
    };
    let _ = std::fs::remove_file(&path);
    result.unwrap();
}
