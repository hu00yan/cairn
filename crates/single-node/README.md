# cairn-node

`cairn-node` is the first long-running external seam for the single-node
storage kernel. It owns one SQLite catalog and one local immutable DAG file.
The HTTP layer does not implement publish or recovery semantics; it delegates
those operations to `SingleNodeStore`.

Start a fresh node:

```text
cargo run -p cairn-single-node --bin cairn-node -- \
  --catalog /tmp/cairn/catalog.db \
  --data /tmp/cairn/data.dag \
  --listen 127.0.0.1:8080
```

The first start creates a 64 MiB preallocated DAG and a default collection and
file. Later starts reopen both stores and run startup recovery before serving
requests.

The initial HTTP surface is intentionally small:

- `GET /healthz`
- `POST /v1/collections` with `{"name":"docs"}`
- `POST /v1/collections/{collection}/files` with `{"name":"readme"}`
- `POST /v1/files/{file}/writes` with operation and expected-head JSON
- `PUT /v1/writes/{operation}/data?offset=0` with raw bytes
- `POST /v1/writes/{operation}/commit`
- `DELETE /v1/writes/{operation}`
- `GET /v1/files/{file}/data`

This is a local development and integration interface. Authentication,
TLS, streaming uploads, request cancellation, and multi-node placement are
not part of this first daemon.
