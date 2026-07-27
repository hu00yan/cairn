use cairn_catalog::sqlite_catalog::{SqliteCatalogStore, WalCheckpointMode};
use cairn_device::{dag_store::FileDagStore, io::FileDevice};
use cairn_single_node::{SingleNodeConfig, SingleNodeStore};
use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const DEVICE_CAPACITY: u64 = 64 * 1024 * 1024;
const SNAPSHOT_BYTES: usize = 64 * 1024;
const DEFAULT_ITERATIONS: usize = 20;

struct TempEnv {
    root: PathBuf,
}

impl TempEnv {
    fn new(label: &str, ordinal: usize) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cairn-bench-{label}-{}-{nonce}-{ordinal}",
            std::process::id()
        ));
        fs::create_dir(&root).expect("create benchmark directory");
        Self { root }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }
}

impl Drop for TempEnv {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn payload(seed: u8) -> Vec<u8> {
    (0..SNAPSHOT_BYTES)
        .map(|index| (index as u8).wrapping_add(seed))
        .collect()
}

fn summarize(label: &str, samples: &mut [Duration]) {
    samples.sort_unstable();
    let micros: Vec<u128> = samples.iter().map(Duration::as_micros).collect();
    let p50 = micros[(micros.len() - 1) / 2];
    let p99 = micros[((micros.len() * 99).saturating_sub(1) / 100).min(micros.len() - 1)];
    println!(
        "{label}: samples={} p50_us={p50} p99_us={p99}",
        micros.len()
    );
}

fn summarize_write_amplification(label: &str, samples: &mut [f64]) {
    samples.sort_by(f64::total_cmp);
    let p50 = samples[(samples.len() - 1) / 2];
    let p99 = samples[((samples.len() * 99).saturating_sub(1) / 100).min(samples.len() - 1)];
    println!(
        "{label}: samples={} p50_ratio={p50:.3} p99_ratio={p99:.3}",
        samples.len()
    );
}

fn sample_dag_single(ordinal: usize) -> (Duration, f64) {
    let env = TempEnv::new("dag-single", ordinal);
    let path = env.path("data.dag");
    let device = FileDevice::create_preallocated(&path, DEVICE_CAPACITY).unwrap();
    let mut store = FileDagStore::open(device).unwrap();
    let data = payload(ordinal as u8);
    let before = store.next_offset().unwrap();
    let start = Instant::now();
    store.append_snapshot(&data, None).unwrap();
    let durable_bytes = store.next_offset().unwrap() - before;
    (start.elapsed(), durable_bytes as f64 / data.len() as f64)
}

fn sample_dag_group(ordinal: usize) -> (Duration, f64) {
    let env = TempEnv::new("dag-group", ordinal);
    let path = env.path("data.dag");
    let device = FileDevice::create_preallocated(&path, DEVICE_CAPACITY).unwrap();
    let mut store = FileDagStore::open(device).unwrap();
    let first = payload(ordinal as u8);
    let second = payload(ordinal.wrapping_add(1) as u8);
    let snapshots = [(first.as_slice(), None), (second.as_slice(), None)];
    let before = store.next_offset().unwrap();
    let start = Instant::now();
    store.append_snapshot_batch(&snapshots).unwrap();
    let durable_bytes = store.next_offset().unwrap() - before;
    let logical_bytes = (first.len() + second.len()) as f64;
    (start.elapsed(), durable_bytes as f64 / logical_bytes)
}

fn sample_reopen(ordinal: usize) -> Duration {
    let env = TempEnv::new("reopen", ordinal);
    let path = env.path("data.dag");
    let device = FileDevice::create_preallocated(&path, DEVICE_CAPACITY).unwrap();
    let mut store = FileDagStore::open(device).unwrap();
    store
        .append_snapshot(&payload(ordinal as u8), None)
        .unwrap();
    drop(store);
    let start = Instant::now();
    let device = FileDevice::open(&path).unwrap();
    let store = FileDagStore::open(device).unwrap();
    assert!(store.next_offset().unwrap() > 0);
    start.elapsed()
}

fn seeded_sqlite(ordinal: usize) -> (TempEnv, SqliteCatalogStore) {
    let env = TempEnv::new("sqlite", ordinal);
    let path = env.path("catalog.db");
    let mut store = SqliteCatalogStore::open(&path).unwrap();
    store.bootstrap(1, 1, 1, "bench", "file").unwrap();
    for index in 0..64 {
        store
            .create_collection(1, &format!("backlog-{index}"))
            .unwrap();
    }
    (env, store)
}

fn sample_sqlite_commit(ordinal: usize, checkpoint_before: bool) -> Duration {
    let (_env, mut store) = seeded_sqlite(ordinal);
    if checkpoint_before {
        store.checkpoint_wal(WalCheckpointMode::Truncate).unwrap();
    }
    let start = Instant::now();
    store.create_collection(1, "measured-commit").unwrap();
    let metrics = store.durability_metrics();
    assert!(metrics.committed_transactions >= 66);
    start.elapsed()
}

fn sample_sqlite_checkpoint(ordinal: usize) -> Duration {
    let (_env, store) = seeded_sqlite(ordinal);
    let start = Instant::now();
    store.checkpoint_wal(WalCheckpointMode::Truncate).unwrap();
    start.elapsed()
}

fn sample_lock_contention(ordinal: usize, threads: usize) -> Duration {
    let env = TempEnv::new("lock", ordinal);
    let config = SingleNodeConfig::new(env.path("catalog.db"), env.path("data.dag"), 1);
    FileDevice::create_preallocated(&config.data_path, DEVICE_CAPACITY).unwrap();
    SingleNodeStore::bootstrap(&config, "bench", "file").unwrap();
    let store = SingleNodeStore::open(config).unwrap();
    let barrier = Arc::new(Barrier::new(threads + 1));
    let start = Instant::now();
    thread::scope(|scope| {
        for thread_id in 0..threads {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                barrier.wait();
                for index in 0..8 {
                    store
                        .create_collection(&format!("c-{thread_id}-{index}"))
                        .unwrap();
                }
            });
        }
        barrier.wait();
    });
    start.elapsed()
}

fn main() {
    let iterations = std::env::args()
        .skip_while(|arg| arg != "--iterations")
        .nth(1)
        .map(|value| value.parse().expect("--iterations must be a number"))
        .unwrap_or(DEFAULT_ITERATIONS);
    assert!(iterations > 0);

    let dag_single = (0..iterations).map(sample_dag_single).collect::<Vec<_>>();
    let (mut samples, mut amplification): (Vec<_>, Vec<_>) = dag_single.into_iter().unzip();
    summarize("dag_single_commit", &mut samples);
    summarize_write_amplification("dag_single_write_amplification", &mut amplification);
    let dag_group = (0..iterations).map(sample_dag_group).collect::<Vec<_>>();
    let (mut samples, mut amplification): (Vec<_>, Vec<_>) = dag_group.into_iter().unzip();
    summarize("dag_group_commit_2", &mut samples);
    summarize_write_amplification("dag_group_write_amplification", &mut amplification);
    let mut samples = (0..iterations).map(sample_reopen).collect::<Vec<_>>();
    summarize("dag_reopen_scan", &mut samples);
    let mut samples = (0..iterations)
        .map(|ordinal| sample_sqlite_commit(ordinal, false))
        .collect::<Vec<_>>();
    summarize("sqlite_commit_after_wal_backlog", &mut samples);
    let mut samples = (0..iterations)
        .map(|ordinal| sample_sqlite_commit(ordinal, true))
        .collect::<Vec<_>>();
    summarize("sqlite_commit_after_truncate_checkpoint", &mut samples);
    let mut samples = (0..iterations)
        .map(sample_sqlite_checkpoint)
        .collect::<Vec<_>>();
    summarize("sqlite_truncate_checkpoint", &mut samples);
    let mut samples = (0..iterations)
        .map(|ordinal| sample_lock_contention(ordinal, 4))
        .collect::<Vec<_>>();
    summarize("catalog_4_thread_contention", &mut samples);

    println!("contract: dag_single=1 data barrier, dag_group_commit_2=1 shared data barrier");
    println!("machine={}", std::env::consts::ARCH);
}
