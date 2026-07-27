# Storage benchmark harness

The Criterion harness exercises only explicit real-file paths:

- `FileDevice` plus `FileDagStore::append_snapshot`, including durable record
  flushes;
- a 4 KiB `FileDagStore::read_snapshot_range` from a durable 1 MiB snapshot;
- `FileDevice::open` plus `FileDagStore::open` scanning a durable record log;
- a `SqliteCatalogStore::bootstrap` transaction with WAL and
  `synchronous=FULL`.

Run `cargo bench -p cairn-single-node --bench storage`.

For explicit p50/p99 output across real temporary files, run:

```text
cargo run -p cairn-single-node --bin cairn-bench -- --iterations 20
```

This reports single-snapshot versus two-snapshot group commit, DAG reopen
scan, SQLite commit latency after a WAL backlog with and without an explicit
truncate checkpoint, checkpoint latency itself, and four-thread catalog lock
contention.

Group-commit latency must be normalized by the number of snapshots in the
batch. A two-snapshot batch taking less than twice the single-snapshot time is
the relevant comparison; the raw batch latency is not itself a faster single
write.

The harness creates isolated directories under the OS temporary directory. It
does not use `SimDisk`, in-memory SQLite, or fake devices. Record the filesystem,
mount options, storage device, OS, CPU, power policy, Criterion configuration,
and commit with every run.

Every report must include p50 and p99 latency, DAG reopen time, and write
amplification (`durable DAG record bytes / logical bytes committed`). The
custom harness reports this logical DAG-record ratio from `next_offset`; it is
not a claim about physical SSD or filesystem block write amplification.
Criterion's summary alone is insufficient: retain raw samples, and collect
filesystem or block-I/O byte counters separately when physical amplification
matters. Do not publish a physical number when its counter was not captured.
No baseline performance result is asserted here.
