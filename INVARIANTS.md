# Invariants

1. A committed root references only existing, validated objects.
2. Written objects are immutable.
3. One content hash identifies one content value.
4. Recovery exposes only a complete committed generation.
5. Uncommitted objects may leak space but never become visible.
6. Corruption is reported, never returned as silent bad bytes.
7. Invalid checkpoints fall back to a full scan.
8. Compaction preserves visible logical state.
9. Untrusted disk bytes cannot cause panic or unbounded allocation.
10. Unsupported format versions are rejected.
11. Erasure repair accepts only position-bound, checksum-verified shards; it never returns silently corrupted bytes.
12. A simulation trace is deterministic for the same operation sequence, fault schedule, and virtual-time inputs.
