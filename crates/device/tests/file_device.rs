#![cfg(any(unix, windows))]

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cairn_device::{BlockDevice, DeviceError, FileDevice, IoOperation};

fn test_path(name: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "cairn-file-device-{name}-{}-{nonce}-{}",
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
#[ignore = "real filesystem smoke test; run explicitly with --ignored"]
fn writes_flushes_and_reopens() {
    let path = test_path("reopen");
    let mut cleanup = TestFile::new(path.clone());
    let mut device = FileDevice::create_preallocated(&path, 4096).unwrap();

    device.write_at(128, b"cairn").unwrap();
    device.flush_data().unwrap();
    drop(device);

    let device = FileDevice::open(&path).unwrap();
    let mut bytes = [0; 5];
    device.read_at(128, &mut bytes).unwrap();
    assert_eq!(&bytes, b"cairn");
    cleanup.cleanup();
}

#[test]
#[ignore = "real filesystem smoke test; run explicitly with --ignored"]
fn bounds_are_reported_without_touching_the_file() {
    let path = test_path("bounds");
    let mut cleanup = TestFile::new(path.clone());
    let mut device = FileDevice::create_preallocated(&path, 8).unwrap();
    let before = [0; 8];

    let error = device.write_at(7, b"xx").unwrap_err();
    assert_eq!(
        error,
        DeviceError::OutOfBounds {
            offset: 7,
            len: 2,
            capacity: 8,
        }
    );
    let mut after = [0; 8];
    device.read_at(0, &mut after).unwrap();
    assert_eq!(after, before);

    let error = FileDevice::open(&path).unwrap().flush_all();
    assert!(error.is_ok());
    cleanup.cleanup();
}

#[test]
#[ignore = "real filesystem smoke test; run explicitly with --ignored"]
fn open_reports_filesystem_errors_through_device_error() {
    let path = test_path("missing");
    let error = FileDevice::open(&path).unwrap_err();
    assert!(matches!(
        error,
        DeviceError::Io {
            operation: IoOperation::Open,
            ..
        }
    ));
}
