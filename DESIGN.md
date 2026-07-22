# Cairn storage design

Cairn is a local crash-consistent, content-addressed store. Its first implementation has one append writer, concurrent-safe device addressing, immutable records, and atomic publication of a generation-numbered root. It deliberately has no dynamic plugins, async runtime, distributed protocol, ACL, directory, SQL, or S3 API.

The only stable seams are `BlockDevice`, optional `Chunker`, and the upper-level object/index API. BLAKE3 and the record/recovery rules remain kernel decisions until their invariants are proven. The simulation profile currently uses 6 data shards plus 4 parity shards: any 6 verified shards reconstruct the stripe, so up to 4 shard losses are tolerated.

The test device is the block-device HAL: it separates volatile bytes, durable
bytes, and pending writes, and can inject deterministic range-based errors and
latency without exposing HDD/SSD details to the store. Most tests use it; real
file durability tests are opt-in and intentionally scarce.

Implementation order: model and simulator, record codec/scanner, transaction commit, file device, DAG/chunking, offline compaction.
