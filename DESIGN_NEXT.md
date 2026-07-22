# Cairn next design: DAG storage and virtual file view

Status: design draft. No implementation may start until this document passes
the adversarial design review and all unresolved terms below are resolved.

## 1. Goal and phase-one limits

Phase one is a single-node, crash-consistent, versioned file store for logical
files up to 4 GiB (`size <= 4 * 1024^3`). Files of a few dozen MiB are a normal
use case; the implementation must stream them and must not require a whole
file-sized allocation. Empty files are valid unless a higher-level policy
rejects them.

The user-facing namespace has exactly two managed ownership levels:

```text
Principal -> Collection -> File -> FileVersion
```

`File` is a named entry inside a Collection, not a third namespace level, and
`FileVersion` is history rather than namespace. There is no arbitrary directory
tree in phase one. A FileVersion points to one
immutable DAG Commit. Updating a File creates another version; it never mutates
the bytes of an existing version.

The phase-one implementation must support:

- sequential and random range reads;
- local range writes against a chosen base version;
- copy-on-write reuse of unchanged ranges;
- version listing and a current head;
- restricted one-time downloads and public publications;
- expiration, revocation, and delayed reclamation;
- one registered DAG media adapter per phase-one deployment (the registry may
  contain other adapter types for future phases);
- deterministic in-memory simulation and a real single-node adapter.

It does not promise replication, erasure coding at the virtual-view layer,
multi-writer merging, arbitrary S3 compatibility, or a network protocol.

## 2. Layering and ownership

```text
Download / publication policy
              |
Virtual file view + SQLite catalog
              |
DAG snapshot and range-map store
              |
Media pool / FileDevice / SimDisk adapters
```

The layers have strict ownership:

| Concern | Owner | Must not be owned by |
| --- | --- | --- |
| Bytes, immutable nodes, parent references | DAG store | SQLite catalog |
| Logical file names and collection membership | SQLite catalog | DAG node codec |
| File version/head CAS | SQLite catalog | media adapter |
| Range-to-content mapping | DAG store | HTTP/download layer |
| Token redemption and session state | SQLite catalog | DAG store |
| Media selection and physical offsets | media/placement layer | virtual file view |
| Byte correctness and Snapshot identity | DAG store | access policy |

SQLite is the phase-one catalog and control-plane database. It is not the
authoritative copy of file bytes. Every cross-store write first creates a
durable `publish_intent`; the intent pins its operation and candidate Commit
until publication or explicit abort. An unreferenced Commit is reclaimable only
after no non-terminal intent can still publish it.

## 3. DAG data model

### 3.1 Node identity

Every node has a domain-separated content identifier. `payload_length` is the
number of bytes in `canonical_payload` after encoding, not the logical content
length:

```text
NodeId = BLAKE3("cairn/node/v3" || u16_le(node_kind) ||
                u64_le(payload_length) || canonical_payload)
```

The fixed node-kind values are `Content=1`, `ZeroRun=2`, `RangeMap=3`,
`Snapshot=4`, and `Commit=5`. Integers are little-endian fixed-width values;
arrays are prefixed by checked `u32` counts; byte strings are prefixed by
checked `u64` lengths. A node ID cannot be reused for two kinds or payloads.
Canonical payload field order is fixed; there is no alternate encoding:

- `Content`: `u64 length || raw bytes`;
- `ZeroRun`: `u64 length`;
- `RangeMap`: `u8 level || u8 leaf_flag || u32 child_count`, then each child as
  `u64 logical_start || u64 logical_len || u64 content_offset || u16 child_kind ||
  NodeId child`. `level=0` is a leaf whose children are only `Content` or
  `ZeroRun`; `level>0` is internal and its children are only RangeMaps with
  `level-1`. All offsets are Snapshot-global logical offsets. For ZeroRun,
  `content_offset=0`; for Content, `content_offset + logical_len` must not
  overflow the Content length. Entries are sorted, non-overlapping, non-zero,
  and each node covers exactly one contiguous interval. Internal children must
  cover exactly one contiguous interval too. The root covers exactly
  `[0, logical_size)`, except an empty Snapshot's canonical empty level-0 map.
  `leaf_flag` is exactly 1 iff `level == 0`; internal entries require
  `content_offset=0`; a ZeroRun entry requires `logical_len` to equal the
  referenced ZeroRun length. The canonical empty map is exactly
  `00 01 00 00 00 00` (level, leaf flag, zero child count), and is legal
  only as an empty Snapshot root. Decoding, commit, and proof verification
  reject any violation;
- `Snapshot`: `u64 logical_size || NodeId range_map_root || [u8; 32] content_digest`;
- `Commit`: `NodeId snapshot || u8 parent_present || [NodeId parent if present]`.
  `parent_present` is exactly `0` when the parent is absent and exactly `1`
  when it is present; every other value is rejected.

`ZeroRun` uses this same formula with `node_kind = ZeroRun`; it does not have a
second hash domain. Operation IDs and receipts are not part of any NodeId.

The normative empty Content payload is `00 00 00 00 00 00 00 00`; the payload
for `abc` is `03 00 00 00 00 00 00 00 61 62 63`, so its `payload_length` is 11.
`content_digest` is `BLAKE3("cairn/logical-bytes/v1" || logical_bytes)` where
`logical_bytes` is the complete stream reconstructed from offset zero. The
empty digest hashes the domain bytes alone; chunking, plan boundaries, and
physical placement cannot change it.

At minimum phase one has these node kinds:

- `Content`: immutable bytes;
- `ZeroRun`: canonical logical zero bytes with a checked run length;
- `RangeMap`: sorted logical ranges and child NodeIds;
- `Snapshot`: logical size, RangeMap root, and logical content digest;
- `Commit`: SnapshotId plus optional parent CommitId. A separate durable DAG
  operation index maps operation_id to CommitId; operation_id is not part of
  CommitId, so identical history is not duplicated merely by retrying.

The catalog may store a `CommitId`; it must not synthesize a different identity
for the same DAG state.

### 3.2 Range map

A RangeMap is a persistent balanced mapping from logical offsets to content
references. Its invariants are:

- ranges are sorted by logical start;
- ranges do not overlap;
- adjacent equivalent ranges may be coalesced;
- every range lies within the Snapshot logical size;
- a read plan covers the requested range exactly once;
- a patch creates new nodes only along affected paths and for changed content;
- ranges use half-open `[start, end)` intervals and the Snapshot covers every
  byte in `[0, logical_size)` either with content or a `ZeroRun` node. A
  `ZeroRun` NodeId uses the general NodeId formula with payload
  `u64_le(length)`; it has no physical payload and always reads as zero bytes.
  Its range-map entry
  has content offset zero.

The logical modification unit is independent of the physical allocation unit.
For phase one, content extents may be sized in the MiB range while physical
media blocks may be much larger. The system must not force a 256 MiB rewrite
for a small patch merely because a media adapter allocates in 256 MiB units.

The initial RangeMap may use a persistent B-tree or another bounded-depth
structure. The interface must not expose its shape. Overlay chains are not an
acceptable unbounded implementation: reads must have a bounded plan depth, and
compaction may create a structurally different equivalent SnapshotId; phase one
does not rewrite external catalog references in place. Online compaction is not
part of phase one.

### 3.3 Snapshot and commit

A Snapshot is the complete logical byte state of one FileVersion. Its identity
is structural and immutable; `content_digest` is the identity of the logical
bytes independent of tree shape. A Commit is a durable DAG record that names a
Snapshot and optionally one parent Commit. Parent history belongs to Commit,
not Snapshot. A Commit is an independent durable root; the DAG store has no
single global current logical root.

The DAG store guarantees:

1. A committed Snapshot is readable after reopen.
2. An uncommitted Snapshot is never returned through a committed root.
3. A failed commit either leaves the prior Commit durable and unpublished or
   returns a recovery-required state; it never reports a partially published
   root as complete.
4. The content digest is computed from logical bytes, not physical placement.
5. Reusing content in a new Snapshot does not mutate the old Snapshot.

## 4. DAG interface seam

The internal DAG seam is intentionally small. `verify_plan` and
`open_reader` are callable only by FileView; callers receive a lease-bound
reader page, never a plan that can be detached from authorization.

```text
DAG.open(media_pool) -> DagStore
DAG.begin() -> DagTxn
DAG.read_node(NodeId) -> Node
DAG.first_plan_page(SnapshotId, logical_range) -> PlanPage
DAG.next_plan_page(SnapshotId, logical_range, dag_cursor) -> PlanPage
DAG.verify_plan(PlanPage) -> VerifiedPlanPage
DAG.open_reader(VerifiedPlanPage) -> VerifiedReader
DAG.retain(root_set) -> RetentionPlan
DAG.reclaim(plan) -> ReclamationResult
```

```text
DagTxn.put_content(bytes) -> NodeId
DagTxn.patch_range(base_snapshot, non_overlapping_writes) -> SnapshotId
DagTxn.truncate(snapshot, new_size) -> SnapshotId
DagTxn.commit(snapshot, parent_commit, operation_id) -> CommitReceipt
DagTxn.abort() -> ()
```

The notation `bytes` above means a bounded streaming input, not an in-memory
slice. The phase-one resource contract is:

- `MAX_LOGICAL_FILE_SIZE = 4 GiB`;
- `MAX_CONTENT_NODE_PAYLOAD = 8 MiB`;
- `MAX_PATCH_WRITES = 4096` and `MAX_PATCH_BYTES_IN_FLIGHT = 64 MiB`;
- one RangeMap node has at most 256 children and depth at most 8;
- one Snapshot has at most 1,048,576 logical extents; a patch that would
  exceed this limit fails with `FragmentationLimit` and must be compacted
  offline before further writes;
- one PlanPage has at most 4096 segments and 1 MiB encoded proof;
- one request reads at most 256 MiB; a larger file is served as multiple
  requests/plans;
- full upload is a sequence of bounded content-stream steps in one logical
  publish operation. No step allocates proportional to the final file size;
- every decoder validates lengths before allocation and rejects values above
  these bounds.

The final Rust interface will use a reader/iterator seam for content and writes
so a 4 GiB file cannot be represented by one required `Vec<u8>`.

The interface includes these non-type rules:

- `put_content` is idempotent for identical content;
- `patch_range` takes sorted, non-overlapping half-open writes. Overlap is an
  error, not an implicit last-write-wins rule. A write may extend EOF and the
  gap is canonical zero-fill; `truncate` is the only operation that reduces
  size;
- every offset and length uses checked `u64` arithmetic. The resulting size is
  at most 4 GiB. Empty ranges are no-ops, and a write of zero bytes never
  extends the file;
- `commit` makes all referenced nodes durable before publishing its root;
- `first_plan_page` accepts a half-open range fully contained in `[0, size]`; a
  non-empty out-of-bounds range is an error, and an empty range at EOF is valid;
- `first_plan_page` and `next_plan_page` are bound to one Snapshot and cannot
  silently cross versions;
- `first_plan_page` creates the first page; `next_plan_page` accepts only the
  opaque DAG cursor returned by the previous page. A `PlanPage` contains the
  SnapshotId, original range, half-open covered range, page sequence, previous
  page digest, optional next cursor, and its authenticated map proofs. The
  cursor authenticates SnapshotId, original range, next offset, page sequence,
  and previous page digest; DAG verification rejects skipped or reordered
  pages. Replaying an already valid page is idempotent and may return duplicate
  bytes; it is not a security violation. DAG cursors have no access-policy
  meaning;
- each plan carries an authenticated RangeMap path proof and Content NodeId;
  `verify_plan` checks the Snapshot descriptor, map proof, non-overlap, exact
  coverage, and content offsets. It does not read content bytes and therefore
  does not claim media integrity;
- one PlanPage is bounded to at most 4096 segments and 1 MiB of encoded proof
  data. The RangeMap depth is at most 8 in the phase-one format;
- a caller may trust only `VerifiedPlanPage`, never an unverified segment list;
- `verify_plan` is an internal validation step, not a caller-authorized seam.
  FileView holds one coordinator read permit while rechecking authorization,
  verifying the page proof, and acquiring its ReadLease. It returns a
  lease-bound `VerifiedReaderPage`, not a free-standing plan that can be pinned
  after GC. Successful lease acquisition is the read linearization point;
  revocation after it may not interrupt that page, but blocks the next page.
  `open_reader` is private to FileView and rejects pages without a live lease;
- `VerifiedReader` verifies every
  Content NodeId/hash as bytes are streamed. A media error or hash mismatch
  aborts the reader before returning the affected bytes; no silently corrupted
  extent is delivered.

Every valid Snapshot is readable through a finite sequence of PlanPages;
fragmentation is never reported as an unrecoverable “too many segments” error.
The page count is bounded by the Snapshot's extent-count limit, and the DAG
cursor prevents the caller from requesting an unbounded page allocation.

The Virtual File View owns authorization pagination. It returns a
`DownloadCursor` containing the authorization kind, exactly one session or
publication ID, fixed version/Snapshot/range, page sequence, and the opaque DAG
cursor, authenticated by the view. Each `next_read_page(auth, cursor)` call
rechecks the matching session or publication state and expiry before invoking
the DAG seam; revoke or expiration therefore blocks the next page without
making the DAG layer depend on SQLite or tokens.

### 4.1 GC barrier

`reclaim` uses the single-node coordinator's exclusive GC barrier. Publish
operations and GC do not run concurrently in phase one:

The coordinator is a read/write permit. A FileView read holds a shared read
permit through authorization check, proof verification, and lease registration.
Every authorization-changing operation (`revoke`, `expire`, session close,
publication/grant tombstoning, and destroy) takes the exclusive write permit
for its SQLite state change. Thus lease registration and revocation have one
total order: a lease registered first may finish its current page; a revoke
committed first prevents that lease registration. GC takes the exclusive permit
as well and therefore cannot mark a Snapshot between verification and lease
registration.

1. The coordinator stops new publish operations and waits for every existing
   publish operation to finish or be fenced/aborted. A paused operation keeps
   its intent and prevents GC; it is never bypassed by a wall-clock timeout.
2. The coordinator begins a new GC epoch and blocks new reader leases,
   publish-intent creation, DAG commits, operation-index publication, and
   catalog head publication.
3. Revalidate catalog roots, non-terminal publish intents, operation-index
   entries, and existing reader leases while holding the barrier.
4. If any root or pin changed, abandon the plan and restart; no deletion occurs.
5. Mark the deletion set for this epoch. Existing reader leases keep their
   nodes alive; the sweeper waits for them or aborts without deleting.
6. Delete only nodes still in the deletion set and only under the barrier.
7. End the epoch; new readers, publish intents, and head publications may
   proceed. All three operations use the same coordinator barrier, so the DAG
   scan and SQLite root set have one linearization point.

An intent is created before a DAG transaction may publish a candidate. A reader
must acquire its lease before the barrier can mark its Snapshot. This closes
the verify-to-pin and scan-to-delete races. A process crash drops process-local
reader leases; durable publish intents remain until recovery resolves them. The
epoch state and deletion set are durable in the media operation journal. On
startup, an incomplete epoch is cancelled without executing its old deletion
set; reads, publishes, and GC remain blocked until the catalog and operation
index are reopened and a fresh epoch is started. Every sweep recomputes roots
and pins under that new barrier.

## 5. SQLite catalog and virtual file view

SQLite stores the virtual view, not the data DAG. The minimum logical tables
are:

```text
principal(id, kind, status, authz_epoch)
authz_state(singleton_id, epoch)
membership(subject_principal_id, organization_id, capability, status)
collection(id, principal_id, name, status, unique(principal_id, name))
file(id, collection_id, name, status, unique(collection_id, name))
file_version(id, file_id, commit_id, parent_version_id, size, digest,
             created_at, expires_at, status)
file_head(file_id, version_id, generation,
          FOREIGN KEY(file_id, version_id) REFERENCES file_version(file_id, id))
publish_intent(operation_id, version_id, file_id, base_version_id,
               expected_head_version_id, expected_head_generation,
               candidate_commit_id, actor_id, authz_epoch, state, abort_reason,
               created_at, owner_fence)
publication(id, version_id, mode, public_token_hash, expires_at, state, revoked_at,
            UNIQUE(id, mode), UNIQUE(id, version_id))
download_grant(id, token_hash, version_id, publication_id, publication_mode,
               expires_at, state, redemption_id,
               redeemed_at, revoked_at)
download_session(id, grant_id, session_secret_hash, expires_at,
                 closed_at, UNIQUE(grant_id))
destroy_intent(operation_id, file_id, actor_id, authz_epoch, state, created_at, owner_fence)
operation_result(operation_id, kind, state, result_id, created_at)
```

The schema contract is part of the interface, not an application convention:

- every primary key, state, hash, and required foreign key is `NOT NULL`;
  nullable columns are listed explicitly below;
- all primary keys are stable opaque IDs; `operation_id`, `token_hash`, and
  `session_secret_hash` are unique;
- `authz_state` has exactly one row. Every membership/capability mutation is
  committed in SQLite while incrementing its monotonic `epoch`; the same
  transaction updates any affected Principal `authz_epoch`. The epoch is the
  authoritative authorization fence, not a value supplied by the caller.
- `membership` records organization membership and capability state. Resource
  ownership is derived from the collection's Principal. A final publish or
  recovery transaction checks the stored actor, ownership, capability, active
  Principal, and `authz_state.epoch = publish_intent.authz_epoch` in SQLite.
- `file_version` has `UNIQUE(file_id, id)`; `file_head` references that
  composite key, and `parent_version_id` references the same File through
  `FOREIGN KEY(file_id, parent_version_id)`;
- `publish_intent.base_version_id` and `expected_head_version_id` use the same
  `(file_id, version_id)` composite foreign key, so no intent can name a base
  or expected head from another File;
- `publish_intent.version_id` is a preallocated opaque reservation ID, not a
  foreign key; the `file_version` row with that primary key is created only in
  the final publication transaction. `base_version_id` and
  `expected_head_version_id` are foreign keys when non-NULL;
- foreign keys are enabled on every SQLite connection;
- `publish_intent.state` is exactly `PREPARING`, `COMMIT_DURABLE`, `PUBLISHED`,
  or `ABORTED`; terminal states cannot transition;
- `abort_reason` is NULL until `ABORTED` and is required in `ABORTED`;
- `file_version.parent_version_id` is NULL exactly for the first version of a
  File and is otherwise a foreign key to the expected base version;
- `file_version.expires_at` is NULL for no expiration;
- `publish_intent.base_version_id` and `expected_head_version_id` are NULL
  exactly when the captured head is the empty `(NULL, 0)` head; they are both
  non-NULL for every later publish. `candidate_commit_id` is NULL in
  `PREPARING`, and may remain NULL in `ABORTED` only when
  `abort_reason = NO_DAG_COMMIT`; it is non-NULL in `COMMIT_DURABLE`,
  `PUBLISHED`, and every other `ABORTED` state;
  a first-version Commit has `parent_commit_id = NULL`, while a later Commit
  must name the base Commit;
- `download_grant.state` is exactly `ISSUED`, `REDEEMED`, or `REVOKED`;
- `download_grant.redemption_id` and `redeemed_at` are NULL in `ISSUED`, and
  may remain NULL when an unredeemed grant transitions directly to `REVOKED`;
  both are non-NULL in `REDEEMED`. `revoked_at` is NULL unless state is
  `REVOKED`;
- `UNIQUE(download_session.grant_id)` makes one grant produce at most one
  session; `session_secret_hash` is required and immutable;
- `download_grant.publication_id` is NULL for an independent version grant and
  non-NULL when the grant is issued from a restricted Publication;
  `publication_mode` is NULL exactly when `publication_id` is NULL, and the
  session joins this immutable grant row rather than copying the relationship;
- `download_session.closed_at` is NULL only while the session is open;
- `principal.kind` is exactly `USER` or `ORGANIZATION`, and
  `principal.status` is exactly `ACTIVE` or `DISABLED`;
- `collection.status` is exactly `ACTIVE` or `DESTROYED`;
- `publication.mode` is exactly `RESTRICTED` or `PUBLIC`;
- `publication.expires_at` is NULL for no expiration and `revoked_at` is NULL
  unless state is `REVOKED`; `publication.state` is exactly `ACTIVE`,
  `EXPIRED`, or `REVOKED`;
- `publication.public_token_hash` is non-NULL and unique in `PUBLIC` mode, and
  is NULL in `RESTRICTED` mode. A public URL carries the corresponding random
  bearer; only its hash is stored.
- A grant with `publication_id IS NOT NULL` must have
  `publication_mode = RESTRICTED` and must satisfy the composite foreign keys
`(publication_id, publication_mode) -> publication(id, mode)` and
  `(publication_id, version_id) -> publication(id, version_id)`. A direct grant
  has both publication columns NULL and is bound only to its non-NULL version;
  SQLite CHECK/FK constraints reject mixed forms.
- A session stores no copied version/publication fields. Its grant FK is the
  sole source of those values, so a session cannot drift to another version.
- `publication.state = EXPIRED` and `file_version.status = EXPIRED` are set by
  the coordinator when `now >= expires_at`; they are never set when the
  corresponding expiration is NULL. Expiration checks are repeated on every
  open/read/grant operation, so a background expiry job is only an index
  maintenance optimization;
- `file.status` is exactly `ACTIVE`, `DESTROYING`, or `DESTROYED`; only
  `ACTIVE` files accept new intents or publications;
- `file_version.status` is exactly `ACTIVE`, `EXPIRED`, or `TOMBSTONED`;
- `file_head.version_id` is NULL only for an empty or destroyed File. Its
  generation is zero exactly for an `ACTIVE` empty File. A `DESTROYED` File
  retains the final generation value plus one; generation is never reset or
  reused;
- `destroy_intent.state` is exactly `PREPARING` or `DESTROYED`; it has no
  abort transition. If its first transaction is not durable, no intent exists;
  if it exists, the first transaction has already linearized and irrevocably
  authorized destruction; recovery resumes the second transaction until it is
  DESTROYED, regardless of later authz-epoch changes;
- `file_version` references a verified Commit and its `(file_id, commit_id)`
  pair is unique. `parent_version_id` records catalog provenance; it is not
  inferred from Commit history.
- `operation_result` is the durable idempotency result for every catalog
  mutation that accepts an operation_id; terminal results are queryable until
  the configured audit-retention period ends.

The catalog has a monotonically increasing schema version. Open fails closed
if migration is incomplete, foreign keys are off, WAL is unavailable, or the
durability profile cannot be verified.

Every catalog connection applies and verifies this correctness profile before
use: `PRAGMA foreign_keys=ON`, `PRAGMA journal_mode=WAL` with result `wal`,
`PRAGMA synchronous=FULL`, and the bounded busy/transaction policy. The intent
insert, candidate binding, and publication transaction all use this profile.
Connection creation fails if any setting cannot be established. Startup runs a
WAL recovery/checkpoint validation and schema-version validation; runtime mode
drift is a fatal catalog error, not an automatic downgrade.

Names are normalized as Unicode NFC, preserve case, reject empty strings,
NUL/control characters and `/`, and are limited to 255 UTF-8 bytes after NFC.
The normalized byte sequence is stored and used for uniqueness. A File has one current head;
history remains addressable by version ID while retention permits it. A head
continues to name the newest committed version even after access expiration;
normal reads return `Expired`, while an authorized writer may explicitly patch
that version. Expiration never silently moves a head backward. An expired head
remains a retention root, so its bytes cannot be reclaimed until the File is
explicitly destroyed or its head is replaced by an authorized administrative
operation.

Version status and access status are separate. A version may be immutable and
retained while its public publication is revoked. Expiration removes access;
reclamation removes data only after no retained version or recovery policy
references the Commit.

The virtual view exposes these operations:

```text
create_collection(actor, principal, name, operation_id)
create_file(actor, collection, name, operation_id)
begin_publish(actor, file, operation_id, expected_head_version, expected_head_generation)
    -> PublishTxn
PublishTxn.write_range(range, streaming_input)
PublishTxn.truncate(new_size)
PublishTxn.commit() -> FileVersion
query_operation(actor, operation_id)
list_versions(actor, file)
open_version(actor, file, selector)
create_publication(actor, version, mode, ttl, operation_id) -> Publication
issue_grant(actor, version, restricted_publication?, ttl, operation_id) -> Grant
redeem_grant(grant_token, operation_id, redemption_id, session_secret) -> Session
revoke(actor, publication_or_grant, operation_id)
expire(actor, version_or_publication, operation_id)
destroy_file(actor, file, operation_id)
```

`actor` is an authenticated, non-forgeable PrincipalId injected by the
authentication boundary, never accepted from an untrusted request body. Every
Every `publish_intent` stores actor_id and the authorization-policy epoch.
Creation, final publication, and recovery of a publish recheck that the actor
is active, owns the resource (including organization membership), and still has
the required capability, and the unchanged SQLite authorization epoch.
Disabling a Principal or revoking its capability therefore cannot race a
publish recovery: the
SQLite transaction that commits the revocation either precedes and invalidates
the publish, or follows a publish that linearized first. Grant redemption is
bearer-authorized and is the explicit exception to owner checks. The reference
model includes these revocation races.

`begin_publish` reads and reserves the expected `(file_head.version_id,
file_head.generation)` under the per-File coordinator lock; the actual compare
and swap occurs again in `PublishTxn.commit`. A patch based on an old head fails
with a conflict;
phase one does not silently overwrite a newer version and does not merge
divergent writes. Phase one does not publish a branch based on a non-head
version. `file_head.generation` is per File, starts at zero, and
increments exactly once for each successful head update. It is unrelated to
the DAG's physical scan generation.

`destroy_file` takes the same per-File coordinator lock as publication and the
exclusive write permit. Its first SQLite transaction checks actor ownership,
active capability, and the current authorization epoch, then creates the
durable `destroy_intent` with actor_id/authz_epoch and changes the
File from `ACTIVE` to `DESTROYING`; if that transaction is not durable, no
destroy has begun. This durable transaction is the destruction authorization
linearization point: later membership or capability changes cannot undo it.
The coordinator then advances its epoch and quiesces all
publishers. A publish DAG transaction and its operation-index publication both
require the current coordinator epoch and per-intent owner fence, so an old
owner cannot finish after `DESTROYING` is durable.

With that fence held, the coordinator reconciles every non-terminal publish
intent for the File. If its DAG operation index has a candidate, it binds that
candidate and then aborts with `abort_reason = FILE_DESTROYED`; if no candidate
exists, it aborts with `abort_reason = NO_DAG_COMMIT`. These SQLite transitions
are retryable while `destroy_intent` remains `PREPARING`; a crash cannot return
the File to ACTIVE or lose the destroy intent. It does not touch the DAG
operation index because SQLite and DAG are separate durable stores. After
recovery or the original owner holds the destroy fence, a second SQLite
transaction atomically clears the head, increments the generation once, marks
all versions `TOMBSTONED`, marks all publications `REVOKED`, changes the File
to `DESTROYED`, and changes the destroy intent to `DESTROYED`. The head row
remains with `version_id = NULL` and the post-destroy generation; it is not
reset to the empty-file generation. There is no partial catalog destroy: the
second transaction either commits all these mutations or none of them. A
publish transaction must recheck `file.status == ACTIVE` and its intent fence
in its final SQLite transaction, so earlier authorization cannot resurrect a
destroyed File. Recovery always resumes a `DESTROYING` intent; it does not
choose an implementation-defined abort. After the second SQLite transaction
is durable, the coordinator independently writes idempotent operation-index
tombstones for the affected publish operations. If tombstoning fails, the
SQLite terminal state remains authoritative and a retryable tombstone job keeps
the DAG mapping from being reclaimed prematurely. Destroy is idempotent by
operation_id.

## 6. Cross-store commit protocol

SQLite and the DAG store cannot participate in one native transaction. The
protocol therefore makes the DAG commit durable first and the SQLite catalog
publish second:

1. Allocate a caller-stable `operation_id` and `version_id`, and capture the
   expected head version plus generation.
2. The single-node coordinator owns the GC barrier and catalog writer. In
   SQLite, durably insert a complete `publish_intent` in `PREPARING` state.
   It contains every CAS input and is a GC pin; it is never removed merely
   because a process paused.
3. Read and authorize the expected base FileVersion; persist the actor and
   authorization-policy epoch in the intent, and require the same authorization
   check again during recovery and final publication.
4. Build and durably commit a new DAG Snapshot/Commit. The CommitReceipt is
   not a DAG node: it is a durable operation-index record atomically written
   with the Commit, mapping operation_id to CommitId and proving the Commit is
   durable.
5. In SQLite, claim the intent with an atomic owner-fence CAS and set it to
   `COMMIT_DURABLE` with candidate_commit_id. A recovery worker may claim an
   abandoned non-terminal intent only after a new coordinator epoch is
   established, and only by advancing owner_fence. An old coordinator cannot
   transition the intent after it loses its fence; wall-clock timeout alone is
   never a reclaim authorization.
6. In one SQLite write transaction, recheck the actor's Principal and
   capability, File ownership, File `ACTIVE` status, and the
   expected head. For the first version, expected head and base version are
   NULL and candidate parent must be NULL. Otherwise candidate parent must
   equal the base Commit. Recompute CommitId from SnapshotId and parent, and
   derive size/digest from the verified Snapshot descriptor. Then insert
   `file_version`, update `file_head`, set the intent to `PUBLISHED`, and commit
   with WAL plus `synchronous=FULL`.
7. If the head CAS fails, the fence owner sets the intent to `ABORTED`; the
   candidate remains an orphan eligible for reclamation after the next barrier.
8. Return the result by operation_id. A retry first queries that operation_id;
   it never creates a second version while the result is uncertain.

Crash outcomes are explicit:

- Before DAG commit: no new visible version; the non-terminal intent remains
  for recovery or explicit abort.
- After DAG commit but before the SQLite candidate binding: the durable DAG
  operation index maps operation_id to the candidate. The active intent pins
  that operation even though candidate_commit_id is still NULL. Recovery finds
  the candidate by operation_id, claims the intent by fence CAS, and either
  publishes it if the expected head still matches or aborts it.
- After SQLite commit: the catalog references a durable DAG Commit and the
  operation query returns the version even if the original response was lost.
- SQLite commit result uncertain: reopen and query operation_id. If another
  writer advanced the head, the operation still has a definitive PUBLISHED or
  ABORTED record; blind retry is forbidden.

The ordering rule is mandatory: SQLite may never publish a reference to a DAG
Commit that has not completed its durable commit. GC may reclaim a candidate
only after the intent is terminal and a GC barrier confirms that no catalog row,
retention root, operation-index entry, or active read pin references it. SQLite
catalog rows are authoritative for namespace visibility; DAG roots are
authoritative for byte correctness.

The DAG operation index has its own lifecycle. It is `ACTIVE` while a
non-terminal intent may need recovery. Once SQLite durably records `PUBLISHED`
or `ABORTED`, the coordinator writes an `OPERATION_TOMBSTONE` and the index
entry no longer contributes a retention root. A published result remains
queryable in SQLite by operation_id; an aborted candidate becomes reclaimable
only after the GC barrier. This prevents the recovery index from becoming a
permanent data root.

## 7. Download capabilities

A grant authorizes one redemption, not one TCP connection. The grant bearer
token is random; only its hash is stored. Redemption includes a caller-supplied
stable `redemption_id` and a caller-generated random `session_secret`. The
server stores only `session_secret_hash` and atomically transitions
`ISSUED -> REDEEMED`. Retrying the same `redemption_id` with the same
`session_secret` returns the same session handle; the client already possesses
the bearer needed to use it even if the first response was lost. A different
redemption ID or different session secret fails. The server never needs to
recover a secret from a hash.

Each range request rechecks the session, grant, and version status, plus the
bound Publication when `publication_id` is non-NULL, and the trusted clock.
Time uses half-open validity: `now < expires_at` is valid;
`now >= expires_at` is expired. Revocation immediately prevents new plans and
invalidates active sessions. A plan already handed to a media reader may finish
the bytes already authorized; no new range may start after revocation or
expiration.

Redeem is one SQLite transaction: it locks the grant row, verifies its token
hash and expiry, then either inserts the one session and marks the grant
redeemed, or (for the same redemption_id and session-secret hash) returns the
existing session. The transaction cannot create two sessions for one grant.

`create_publication`, `issue_grant`, `redeem_grant`, and `revoke` are catalog
transactions with stable operation IDs and idempotent result lookup. A
`RESTRICTED` publication may issue grants bound to itself; an independent grant
may target a version without a publication. A `PUBLIC` publication is accessed
directly with its public bearer and does not create a grant/session. For
`redeem_grant`, `operation_id` identifies the catalog operation and
`redemption_id` additionally enforces one redemption per grant; retries require
both IDs and the same session secret. Grant TTL is required and at most 30
days; a session expires no later than one hour and
no later than its grant. A public publication may be unbounded or have a TTL of
at most one year. All TTLs use `now < expires_at` validity.

Public access is a revocable Publication pointing to a fixed version. It is not
a mutable “latest” URL in phase one. Tokens, sessions, and public URLs never
grant access to a raw NodeId without passing through the view's authorization
check. A version expiration also invalidates publications and sessions for that
version; it does not erase the DAG bytes.

The virtual view exposes one read seam for all access modes:

```text
ReadAuthorization::Session { session_id, session_secret }
ReadAuthorization::Public { publication_id, public_secret }
FileView.first_read_page(auth, range) -> (VerifiedReaderPage, DownloadCursor)
FileView.next_read_page(auth, DownloadCursor) -> (VerifiedReaderPage, DownloadCursor)
```

`DownloadCursor` is authenticated by the Virtual File View and contains
`authorization_kind`, exactly one of `session_id` or `publication_id`, the
fixed `version_id`, fixed `SnapshotId`, original range, page sequence, and the
opaque DAG cursor. The next-page operation rejects any mismatch between the
presented authorization and these bound fields before consulting the DAG.
The cursor is therefore not transferable between a public publication, a
restricted session, or two different versions.

For a session authorization, the view checks the session, grant, fixed version,
and, when present, the bound restricted Publication. For public authorization,
it checks the publication ID, hashes the public secret, and checks
publication/version status. Both paths check expiry and revocation on every
page. The DAG receives only the authenticated structural cursor after the view
has authorized the request; it never sees SQLite IDs or bearer secrets.

## 8. Placement and media

The placement seam classifies nodes, not user files:

```text
NodeClass::RangeMap
NodeClass::SnapshotMetadata
NodeClass::Content
```

A placement policy chooses Media adapters by latency, endurance, capacity, and
failure policy. SQLite catalog pages are outside this DAG placement seam. The
first implementation uses exactly one required Media and therefore has no
redundancy promise. A future multi-media policy succeeds only when every
required replica has durably acknowledged every referenced node and its commit
marker; partial success is unpublished and recoverable as an intent failure.
Placement indexes are rebuildable hints and never change NodeId. Duplicate
copies must compare equal by NodeId; conflicting bytes are corruption.

The simulator must model both the DAG media and the SQLite/catalog durability
boundary. Simulating only the data device is insufficient once SQLite controls
visibility.

## 9. Required reference model before production implementation

Before FileView code is implemented, create deterministic in-memory adapters:

```text
ModelMediaPool
ModelDagStore
ModelCatalog
ModelFileView
ModelDownloadAccess
```

The model must generate and shrink operations for:

- create and rename-free collection/file creation;
- full upload and multiple patch operations that may target overlapping ranges
  across different versions;
- concurrent-head conflicts;
- old/new version reads;
- DAG commit crashes at every cross-store boundary;
- SQLite commit/reopen failures;
- grant redemption races and session expiry;
- public publication revoke/expire;
- orphan DAG reclamation;
- media placement and one media failure.
- destroy operations with both SQLite transaction boundaries, crashes at each
  boundary, old owners acting after fence, recovery re-entry, failed
  operation-index tombstones, and GC concurrent with all of these;
- GC/publish/read interleavings with active intents and reader pins;
- long process pauses beyond any wall-clock grace period;
- SQLite commit success with a lost response, WAL/checkpoint boundaries, and
  catalog reopen;
- grant redeem retry, revoke/expire races, and clock-boundary cases;
- placement metadata loss, duplicate conflict, and partial multi-media commit.

The oracle checks catalog visibility and reconstructed bytes, plus intent/index
state, monotonic fences, publishability, retention roots, and reclaim
eligibility after every crash/recovery step. It does not compare physical
offsets or record layout. A real SQLite/FileDevice integration gate
then checks the same protocol with bounded files.

## 10. Normative phase-one decisions

The following are no longer open implementation choices:

1. File ranges are half-open, gaps are canonical zero-fill, overlapping writes
   are rejected, and `truncate` is separate.
2. `SnapshotId` is structural, `content_digest` is logical-byte identity, and
   `CommitId` carries parent history. Compaction may create a new equivalent
   Snapshot; it does not mutate or silently replace an old one.
3. `operation_id` is stable across retries. The catalog is queried by operation
   ID when SQLite commit outcome is uncertain.
4. A durable `publish_intent` pins candidate commits. GC has no time-only rule:
   it requires a terminal intent and a fresh reachability check. Active reader
   pins protect a Snapshot while a verified plan is being consumed.
5. A File head CAS compares both version ID and per-file generation. There is
   no global current DAG root and no automatic head rollback on expiration.
6. Phase-one SQLite uses WAL and `synchronous=FULL` as the correctness profile.
   Any weaker mode is an explicitly named non-durable mode and cannot claim
   crash-consistent publication.
7. Phase one has one required DAG media adapter. Multi-media quorum, repair,
   and redundancy are future interfaces, not implicit behavior.
8. Grant redemption is idempotent by redemption ID; grant revoke/version
   expiration stops all future plans and active sessions, while already-started
   byte delivery may finish.
9. Principals are opaque IDs supplied by an external authentication layer.
10. The catalog derives `size`, `digest`, and parent linkage from a verified
    Commit/Snapshot; callers cannot provide contradictory denormalized values.

Only after Sol review finds no BLOCKING or IMPORTANT ambiguity may production
implementation start.
