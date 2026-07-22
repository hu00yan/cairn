# Cairn domain glossary

This glossary describes the domain, not its Rust representation or storage
format.

## Storage terms

- **DAG node**: An immutable content-addressed value that may reference other
  DAG nodes.
- **Content node**: A DAG node whose payload is file data.
- **Range map**: A persistent DAG structure mapping logical file ranges to
  content nodes.
- **Snapshot**: An immutable root of a complete logical file state.
- **Commit**: A durable DAG history record naming a Snapshot and optionally a
  parent Commit. It is not a user-visible access publication.
- **Placement**: The decision about which physical media stores a DAG node.
- **Media**: A physical or simulated durable storage adapter with capacity and
  failure characteristics.

## Virtual view terms

- **Principal**: A user or organization recognized by the control plane.
- **Collection**: The second-level namespace owned by a Principal. It is a
  bounded grouping, not an arbitrary directory tree.
- **File**: A stable logical name inside a Collection.
- **File version**: A catalog record pointing to exactly one immutable Commit.
- **Head**: The catalog pointer naming the current version of a File.
- **Publication**: A separately controlled decision to expose a File version
  through a public or restricted download path.

## Access terms

- **Grant bearer token**: A capability authorizing one redemption for one
  File version under a policy.
- **Download session**: The short-lived capability created by redeeming a
  grant. A session bearer is distinct from the grant bearer and may perform
  multiple range requests.
- **Expiration**: A time-based access transition. It does not immediately
  erase immutable DAG data.
- **Reclamation**: Physical removal of DAG nodes that are no longer reachable
  from retained roots.

The DAG layer does not use bucket, object, or `get_object` as domain concepts.
