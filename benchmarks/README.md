# Benchmark plan

There are no benchmark numbers yet. Do not add numbers without recording the
machine, filesystem, capacity, log size, and durability settings.

The first benchmark harness should use real `FileDevice` and SQLite files,
with cleanup isolated from the default test loop. The minimum cases are:

1. append a snapshot and complete the SQLite-backed commit, including flushes;
2. read a 4 KiB range from a published snapshot;
3. reopen and scan DAG logs at several log sizes;
4. measure SQLite prepare, candidate record, and publish transactions;
5. measure a future GC rewrite by throughput, pause time, and write
   amplification;
6. measure concurrent readers and serialized writers separately.

Every case should report at least p50 and p99 latency. Write benchmarks must
also report bytes appended per logical byte committed. Reopen benchmarks must
verify the recovered head and content digest before recording a sample.

Criterion is appropriate for the in-process cases, but durability and reopen
measurements must remain explicit real-file scenarios rather than fake-device
microbenchmarks.

## Durability accounting

On Unix, `FileDevice::flush_data` maps to `fdatasync` (`File::sync_data`) and
`flush_all` maps to `fsync` (`File::sync_all`). A preallocated DAG only needs a
data durability barrier for appended records; file creation, resize, rename,
and directory-entry changes require the stronger metadata barrier.

The append path must write a complete record, including its footer magic, then
issue one data barrier for the complete snapshot. It must not synchronize
separately for the header, payload, footer, or the individual DAG records that
make up a snapshot. The current contract test counts one data barrier for a
four-record snapshot, and one shared barrier for a snapshot batch. SQLite
remains responsible for its own WAL `synchronous=FULL` metadata transaction
barriers.

The current harness reports DAG data barriers and latency/write amplification;
the full SQLite-backed publish transaction is still a separate benchmark case
to add before making end-to-end throughput claims.
