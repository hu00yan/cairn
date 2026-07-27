use std::{
    fs,
    hint::black_box,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use cairn_catalog::sqlite_catalog::SqliteCatalogStore;
use cairn_device::{dag_store::FileDagStore, io::FileDevice};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};

const DEVICE_CAPACITY: u64 = 64 * 1024 * 1024;
const SNAPSHOT_BYTES: usize = 1024 * 1024;
const READ_BYTES: u64 = 4096;

struct RealFileEnv {
    root: PathBuf,
}

impl RealFileEnv {
    fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cairn-bench-{label}-{nonce}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create benchmark directory");
        Self { root }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }
}

impl Drop for RealFileEnv {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn create_dag(path: &Path) -> FileDagStore<FileDevice> {
    let device =
        FileDevice::create_preallocated(path, DEVICE_CAPACITY).expect("create real FileDevice");
    FileDagStore::open(device).expect("open empty FileDagStore")
}

fn snapshot_payload() -> Vec<u8> {
    (0..SNAPSHOT_BYTES).map(|index| index as u8).collect()
}

fn bench_dag_append_flush_commit(c: &mut Criterion) {
    let payload = snapshot_payload();
    let mut group = c.benchmark_group("real_file_dag");
    group.throughput(Throughput::Bytes(payload.len() as u64));
    group.bench_function("append_flush_commit_1_mib", |b| {
        b.iter_batched(
            || {
                let env = RealFileEnv::new("append");
                let store = create_dag(&env.path("data.dag"));
                (env, store)
            },
            |(_env, mut store)| {
                black_box(
                    store
                        .append_snapshot(black_box(&payload), None)
                        .expect("append and flush snapshot"),
                )
            },
            BatchSize::PerIteration,
        )
    });
    group.finish();
}

fn bench_range_read(c: &mut Criterion) {
    let env = RealFileEnv::new("read");
    let mut store = create_dag(&env.path("data.dag"));
    let commit = store
        .append_snapshot(&snapshot_payload(), None)
        .expect("seed durable snapshot")
        .commit_id;
    let start = (SNAPSHOT_BYTES as u64 / 2) - (READ_BYTES / 2);

    let mut group = c.benchmark_group("real_file_read");
    group.throughput(Throughput::Bytes(READ_BYTES));
    group.bench_function("snapshot_range_4_kib", |b| {
        b.iter(|| {
            black_box(
                store
                    .read_snapshot_range(commit, start..start + READ_BYTES)
                    .expect("read durable range"),
            )
        })
    });
    group.finish();
}

fn bench_dag_group_commit(c: &mut Criterion) {
    let payloads: Vec<Vec<u8>> = (0..4)
        .map(|seed| {
            (0..64 * 1024)
                .map(|index| (index as u8).wrapping_add(seed))
                .collect()
        })
        .collect();
    let mut group = c.benchmark_group("real_file_dag");
    group.throughput(Throughput::Bytes(
        payloads.iter().map(Vec::len).sum::<usize>() as u64,
    ));
    group.bench_function("append_group_commit_4x64_kib", |b| {
        b.iter_batched(
            || {
                let env = RealFileEnv::new("group");
                let store = create_dag(&env.path("data.dag"));
                (env, store)
            },
            |(_env, mut store)| {
                let snapshots: Vec<(&[u8], Option<[u8; 32]>)> = payloads
                    .iter()
                    .map(|payload| (payload.as_slice(), None))
                    .collect();
                black_box(
                    store
                        .append_snapshot_batch(&snapshots)
                        .expect("append grouped snapshots"),
                )
            },
            BatchSize::PerIteration,
        )
    });
    group.finish();
}

fn bench_reopen_scan(c: &mut Criterion) {
    let env = RealFileEnv::new("reopen");
    let path = env.path("data.dag");
    let mut store = create_dag(&path);
    store
        .append_snapshot(&snapshot_payload(), None)
        .expect("seed durable snapshot");
    drop(store);

    c.bench_function("real_file_dag/reopen_scan_1_mib", |b| {
        b.iter(|| {
            let device = FileDevice::open(black_box(&path)).expect("reopen FileDevice");
            black_box(FileDagStore::open(device).expect("scan durable DAG"))
        })
    });
}

fn bench_sqlite_catalog_transaction(c: &mut Criterion) {
    c.bench_function("sqlite_catalog/bootstrap_transaction", |b| {
        b.iter_batched(
            || {
                let env = RealFileEnv::new("sqlite");
                let store =
                    SqliteCatalogStore::open(env.path("catalog.db")).expect("open SQLite catalog");
                (env, store)
            },
            |(_env, mut store)| {
                store
                    .bootstrap(1, 1, 1, "bench", "data")
                    .expect("commit catalog transaction");
                black_box(store)
            },
            BatchSize::PerIteration,
        )
    });
}

criterion_group!(
    storage,
    bench_dag_append_flush_commit,
    bench_dag_group_commit,
    bench_range_read,
    bench_reopen_scan,
    bench_sqlite_catalog_transaction
);
criterion_main!(storage);
