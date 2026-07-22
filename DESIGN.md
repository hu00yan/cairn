# Cairn storage design

Cairn is a local crash-consistent, content-addressed DAG store. Its first implementation has one append writer, concurrent-safe device addressing, immutable records, and atomic publication of a generation-numbered root. An object API may project byte streams onto this DAG, but it is not the storage kernel's data model. The kernel deliberately has no dynamic plugins, async runtime, distributed protocol, ACL, directory, SQL, or S3 API.

The only stable seams are `BlockDevice`, optional `Chunker`, and the upper-level object/index API. BLAKE3 and the record/recovery rules remain kernel decisions until their invariants are proven. The simulation profile currently uses 6 data shards plus 4 parity shards: any 6 verified shards reconstruct the stripe, so up to 4 shard losses are tolerated.

The test device is the block-device HAL: it separates volatile bytes, durable
bytes, and pending writes, and can inject deterministic range-based errors and
latency without exposing HDD/SSD details to the store. Most fault tests use it;
the real file backend also has deterministic I/O and reopen/recovery contract
tests in an explicit filesystem gate; the default workspace gate remains
fully in-memory.

The current v2 layout is a two-level DAG projection: a committed root points to
one flat manifest, and the manifest points to immutable chunks. The byte-stream
helpers currently live in `cairn-core`; they are not yet a general node/edge
adapter seam. General DAG node kinds, edge semantics, reachability and
compaction must be frozen before calling this a general DAG kernel.

The target kernel has two explicit planes: durable metadata that describes DAG
nodes, edges, roots and placement, and durable content that stores node payloads.
Both use the block-device seam, while placement may choose different media for
metadata and content according to latency, endurance and capacity. This is a
storage layout decision, not a bucket or `get_object` API.

Implementation order: model and simulator, record codec/scanner, transaction commit, file device, DAG/chunking, reachability-based compaction, then optional object and service adapters.
