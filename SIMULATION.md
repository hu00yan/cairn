# Cairn deterministic simulation policy

Cairn's distributed experiments run inside one process with in-memory devices,
an explicit virtual clock, and a deterministic event queue. This follows the
useful part of FoundationDB's simulation approach: model the whole cluster in
one process, inject failures deterministically, and replay a failing seed or
trace. See the [FoundationDB simulation testing overview](https://apple.github.io/foundationdb/testing.html)
and [client testing documentation](https://apple.github.io/foundationdb/client-testing.html).

## Default safety rule

No default unit, property, chaos, or distributed test may create a real file,
temporary file, raw device, or filesystem-backed `FileDevice`. The default test
path uses `SimDisk` and `SimNetwork` only. This protects SSD lifetime and keeps
tests reproducible.

Real file durability checks are explicit opt-in smoke tests, bounded in count
and size, and must clean up their files. Raw-device experiments are not part of
the first implementation.

## Virtual time

Network and device latency are logical ticks, not wall-clock sleeps. A test can
set a link to `latency_ticks = 50`, advance the virtual clock, and assert the
delivery boundary immediately. This makes timeout, retry, partition, and repair
experiments fast and deterministic.

## Current simulator surface

The first implementation now provides these deterministic primitives:

- `SimDisk`: volatile versus durable bytes, pending writes, explicit flush,
  crash recovery, atomic-write-unit torn-write behavior, configurable flush
  subset/order, durable corruption, read failure, dropped write, short write,
  operation failure/crash, one-shot fault schedules, and virtual I/O latency;
- `SimNetwork`: numeric per-link latency, drop, duplicate, deterministic
  reordering frozen at send time, directional connection interruption,
  node pause/resume, node crash/restart with epoch, and bidirectional
  partitions/healing. A partition or disconnect drops packets at the send or
  delivery boundary; it does not give the sender an omniscient remote-state
  error;
- `cairn-ec`: checksum-bound `ShardBuffer` inputs exclude corrupted shards and
  `reconstruct_all` returns a repaired complete stripe;
- both components expose operation/time state and the network trace records
  the inputs that affect delivery (payload, epochs, policy, delivery time,
  fault boundaries, and virtual-time advances), sufficient to reproduce the
  current bounded tests without wall-clock sleeps or real devices;
- the trace is an input record, not yet a standalone generic replay runner.

`SimDisk` fault operation numbers intentionally cover state-changing writes and
flushes only; reads use persistent range faults because `BlockDevice::read_at`
is observational and takes `&self`. A future concurrent-reader seam may add a
separate read event stream without changing durability operation numbering.

The next simulator-only additions are broader property/state-machine
generators and a standalone replay runner. They must stay on these in-memory
seams before the storage core starts depending on them.

## SR-01: bounded store replay

The first replay slice uses a versioned JSON case and runs the same bounded
store operations against `cairn-core + SimDisk` and `cairn-model`. It supports
`put_chunk`, `put_manifest`, `commit_root`, and a final `crash_reopen`. A case
may inject one deterministic crash immediately before the final reopen. The
crash phases cover record header, record payload, record flush, superblock
write, and superblock flush, each before or after the selected operation.

The runner is intentionally single-node: it does not use `FileDevice`,
`SimNetwork`, sockets, wall-clock sleeps, torn writes, or multi-fault schedules.
Cases are bounded to 64 operations, 32 slots, 4 KiB per chunk, 64 KiB total
payload, and a 16 KiB–1 MiB simulated disk. The JSON runner also rejects
inputs larger than 1 MiB before deserialization, so the CLI's input bound is
enforced before constructing an untrusted case.

Run the replay tests and fixtures with:

```text
cargo test -p cairn-sim --all-targets --offline
cargo run -p cairn-sim --bin cairn-replay -- crates/sim/tests/fixtures/v1-basic.json
cargo run -p cairn-sim --bin cairn-replay -- crates/sim/tests/fixtures/v1-crash-after-superblock-flush.json
```

## SR-02: replay trust boundary

SR-02 adds a private pure-computation oracle to the replay runner. It does not
use `cairn-model`, the physical fault planner, or device operation IDs. The
oracle independently computes object IDs, manifest validity, pending versus
visible objects, and recovery roots.

The fixed two-generation matrix starts with generation 10, stages generation
20, and checks all 22 crash cuts. It requires the old root to survive every
cut except `CrashAfter(SuperblockFlush)`, where the call still returns an
injected crash but recovery must expose generation 20. Recovery probes chunks
and manifests separately, including records that are physically visible but
logically invalid. If the recovered generation is `u64::MAX`, root and chunk
visibility checks still run; manifest terminal probes are skipped because the
core API requires a strictly larger probe generation.

This remains a bounded single-node trust layer. It does not claim coverage for
torn/reordered writes, multiple faults, format crash, compaction, EC,
replication, networking, or filesystem durability.

## Fault dimensions

The minimum simulation surface is:

- device: volatile/durable/pending state, flush, crash, torn writes, reorder,
  corruption, capacity exhaustion, read/write/flush failure;
- network: per-link latency, drop, duplicate, reorder, disconnect, pause, and
  bidirectional partition;
- process: node pause, crash, restart, and deterministic recovery;
- observability: seed/trace, virtual time, operation sequence, durable digest,
  pending counts, and fault cursor.

The simulator must not attempt to reproduce SSD firmware, physical seek,
NVMe/SATA timing, or every operating-system cache behavior. Those are separate
performance experiments, not correctness prerequisites.
