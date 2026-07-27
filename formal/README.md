# Formal protocol models

`publish_recovery.tla` is a standalone TLA+ model of catalog visibility,
durable DAG candidates, T1/T2/T3 publication, crash/reopen recovery, fencing,
idempotent retry, and reader roots.

The model documents the protocol state machine. It does not prove that the Rust
implementation refines the model, that filesystem flushes satisfy the assumed
durability contract, or that SQLite and DAG media form one atomic transaction.
Those obligations remain covered by Rust contract tests, crash/reopen tests,
and real-file benchmarks.

## TLC check

The checked configuration is intentionally bounded: one operation, three
candidate versions, one reader, and three fencing epochs. The model also
assumes that crashes eventually stop (`<>[]processOpen`); without that
assumption, recovery cannot be live because a process may crash forever.

With TLA+ Toolbox 1.7.4 installed, run:

```text
java -XX:+UseParallelGC -cp \
  "/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar" \
  tlc2.TLC -config formal/publish_recovery.cfg formal/publish_recovery.tla
```

The current bounded check passes with 96 distinct states and 199 generated
states. This is a bounded protocol sanity check, not a proof of the Rust
implementation or of unbounded deployments.
