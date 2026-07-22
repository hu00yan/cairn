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

Real file visibility and recovery checks are explicit opt-in contract tests,
bounded in count and size, and must clean up their files. They are not part of
the default workspace gate and do not claim to simulate power loss. Raw-device
experiments are not part of the first implementation.

## Virtual time

Network and device latency are logical ticks, not wall-clock sleeps. A test can
set a link to `latency_ticks = 50`, advance the virtual clock, and assert the
delivery boundary immediately. This makes timeout, retry, partition, and repair
experiments fast and deterministic.

## Current simulator surface

The first implementation now provides these deterministic primitives:

- `SimDisk`: volatile versus durable bytes, pending writes, explicit flush,
  power-loss recovery, atomic-unit torn writes, bounded partial flushes,
  deterministic device-local reorder, durable corruption, bad media ranges,
  read/write/flush failure, dropped and short I/O, crash-before/after and
  tear-and-crash effects, and virtual I/O latency;
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

`DeviceScript` is the only simulator configuration surface. Selectors match
device event kind, physical byte range, and global device-event occurrence
(`any`, `exact`, or `every`); they never name a Cairn operation. The script is
serializable and replayable. `FileDevice` remains the production adapter and
is not expected to reproduce simulated power-loss or torn-media behavior.

## SR-01: bounded store replay

The replay slice uses a versioned JSON case and runs the same bounded store
operations against `cairn-core + SimDisk` and `cairn-model`. It supports
`put_chunk`, `put_manifest`, `commit_root`, a final `crash_reopen`, and a
serializable `DeviceScript`. Device crashes are selected by device event and
physical range, not by a store mutation phase. Fault-free cases are checked
against the independent logical oracle; faulted crashes are checked at the
device trace and reopen boundary.

The runner is intentionally single-node: it does not use `FileDevice`,
`SimNetwork`, sockets, or wall-clock sleeps. Its `DeviceScript` may inject
tears and multiple non-overlapping device rules, but the storage oracle does
not assign upper-layer meaning to arbitrary fault combinations. Cases are
bounded to 64 operations, 32 slots, 4 KiB per chunk, 64 KiB total
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
use `cairn-model` or device operation IDs. The
oracle independently computes object IDs, manifest validity, pending versus
visible objects, and recovery roots.

The device-level tests cover volatile/durable visibility, partial flushes,
atomic-unit tears, reorder, bad ranges, every device effect, power loss, trace
order, and deterministic virtual time. The storage replay runner does not
pretend that a logical model can predict every arbitrary device script; it
gates fault-free logical equivalence and separately gates device semantics.

This remains a bounded single-node logical trust layer. Device-effect coverage
lives in the independent HAL corpus; the logical oracle does not claim to
predict multiple arbitrary device faults, compaction, EC, replication,
networking, or filesystem durability.

## SR-03: deterministic state-machine corpus

SR-03 adds an in-tree SplitMix64 generator and deterministic shrinker. It
generates bounded multi-generation traces with duplicate writes, empty and
mis-sized manifest references, rejected generations, latency variation, and a
randomly selected final chunk, manifest, or valid root operation. A separate
10,000-seed device-script corpus exercises fault effects independently of the
store oracle.

The logical replay corpus runs 1,000 seeds, while the independent device
script corpus runs 10,000 seeds, in `crates/sim/tests/state_machine.rs`.
Each logical seed is stored in the `ReplayCase.seed` field, and a logical
failure is reduced in a fixed order (remove operations, then payloads and
generations) before the test prints JSON accepted by `cairn-replay`. The
generator adds no third-party dependency and does not replace the independent
replay oracle.

The corpus has observed deterministic coverage for duplicate payloads,
mis-sized references, accepted/rejected generations, device effect classes,
all device event kinds, bad-range failures, and no-wall-clock virtual time.
The device corpus independently computes the expected durable and pending
state for its fixed schedules; unit tests cover the remaining operation-level
outcomes. It stores cross-platform golden hashes for representative logical
seeds and keeps a failure's structured class during shrinking. A core failure
class includes its recovery stage and, when applicable, the underlying device
failure kind.

This is a coverage amplifier, not a claim that the single-node simulator is
complete. The device-only HAL and its independent corpus are the correctness
seam; the storage replay oracle deliberately remains a logical oracle for
fault-free equivalence and does not invent upper-layer meanings for device
effects.

## Fault dimensions

The minimum simulation surface is:

- device: volatile/durable/pending state, flush, power loss, torn writes,
  partial persistence, reorder, corruption, capacity exhaustion,
  read/write/flush failure;
- network: per-link latency, drop, duplicate, reorder, disconnect, pause, and
  bidirectional partition;
- process: node pause, crash, restart, and deterministic recovery;
- observability: seed/trace, virtual time, device event sequence, durable
  digest, and pending counts.

The simulator must not attempt to reproduce SSD firmware, physical seek,
NVMe/SATA timing, or every operating-system cache behavior. Those become
composable latency/state policies over the same device events, not new core
APIs.

## SR-04: block-device HAL direction

The simulator's correctness seam is the `BlockDevice` contract, not a disk
brand or a filesystem implementation. `SimDisk` models one deterministic
device with four deliberately small dimensions:

1. **Physical range** — every read/write has a half-open byte range. Bad media
   is a persistent set of ranges; an overlapping read or write returns a stable
   device error.
2. **Persistence** — a successful write updates the volatile device view and a
   pending write set. A dropped write is acknowledged but updates neither.
   `flush_data`/`flush_all` advance device-local pending writes to the durable
   medium, optionally with a bounded flush budget. `power_loss` discards
   pending state and rebuilds the volatile view from durable bytes.
3. **Device events** — each device call internally records a monotonic event
   with kind, range, effect, and sequence number. Fault rules match device
   event kind, physical range, and a device event occurrence. They never match
   a Cairn core operation ID. There is no legacy operation-fault path.
   The event clock is protected by one mutex: concurrent observational reads
   are linearized there, and virtual latency accumulates in that same order.
4. **Virtual time** — each event gets `base_latency + seeded_jitter`. The
   jitter is a pure function of the device seed and event sequence, so the same
   initial bytes, call sequence, and script produce the same trace.

The fault vocabulary stays at the device boundary: error, timeout, dropped
write, short write, torn write at atomic units, crash before/after an event,
and bounded flush reordering. HDD-like seek/rotation delay, SSD queueing,
garbage-collection stalls, thermal slowdown, and periodic media degradation are
latency or fault policies over these same events; they do not become separate
core APIs. The model intentionally does not simulate firmware algorithms or
filesystem directories.

This follows the useful Linux boundary: blk-mq may queue, merge, and reorder
requests and does not guarantee completion order; volatile write-back caches
need explicit flushes for data-integrity points; block error injection is
expressed as operation plus sector range; and device-mapper exposes delay and
periodic flakiness as composable device behaviors. See the [Linux blk-mq
documentation](https://www.kernel.org/doc/html/latest/block/blk-mq.html),
[volatile write-back cache control](https://docs.kernel.org/block/writeback_cache_control.html),
[block error injection](https://www.kernel.org/doc/html/next/block/error-injection.html),
and [`dm-delay`/`dm-flakey`](https://docs.kernel.org/admin-guide/device-mapper/delay.html).

SR-04 implementation order is event trace, seeded latency, persistent bad
ranges, event fault rules, then torn/reordered flush behavior. Each layer gets
device-only properties first; the storage property runner consumes only the
device script and the resulting trace.
