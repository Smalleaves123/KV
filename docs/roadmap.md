# Roadmap

This roadmap lists practical next steps for turning KVEngine from a learning
engine into a more complete storage-system project.

## Near Term

### Documentation

- Keep README and docs aligned with code changes.
- Add diagrams for write path, read path, compaction, and Raft replication.
- Add examples for embedding the C++ DB API directly.

### Build And Tooling

- Extend CI coverage across more operating systems and compilers.
- Add optional TSan CI once runtime is stable.
- Keep the repository-wide clang-format and clang-tidy configuration aligned
  with the supported toolchain.
- Package a developer preset with `CMakePresets.json`.

### Test Quality

- Add deterministic crash-recovery tests.
- Add WAL corruption and truncated-record tests at DB-open level.
- Add more compaction tests around snapshots and tombstone retention.
- Add stress tests for concurrent readers and writers.
- Add network protocol fuzz-style parser tests.

## Storage Engine

### Data TTL

- `DB::Expire`, `DB::TTL`, and `DB::Persist` are implemented for local and
  Raft-backed DB access.
- TTL is persisted through WAL replay and compatible SST value envelopes.
- Reads, snapshots, iterators, scans, and compaction preserve TTL semantics.
- A future background expiry reaper can add physical tombstone cleanup without
  changing the logical read behavior.

### WAL

- WAL segment rotation.
- WAL truncation or archival after successful flush/compaction.
- Group commit for higher write throughput.
- Versioned WAL format.

### MemTable

- Immutable memtables.
- Background flush.
- Configurable skiplist parameters.
- More precise memory accounting.

### SSTable

- Per-block checksums.
- Range scan API.
- Configurable block size and Bloom filter bits per key.
- Multi-level compaction.
- Compaction picking based on size and overlap.
- Table-cache capacity option in `DBOptions`.

### Recovery

- Stronger manifest recovery behavior.
- Manifest rewrite/checkpoint.
- Repair tool for partial metadata corruption.

## Transactions

- Expose explicit isolation mode options if more modes are added.
- Add deadlock-free pessimistic transaction mode using the lock manager.
- Add transaction metrics.
- Add transaction timeout and automatic cleanup for long-running sessions.

## Network Layer

- Full RESP request parser.
- Values with spaces or binary payloads.
- Pipelining tests and documentation.
- Authentication.
- TLS.
- Better client redirection in Raft mode.

## Raft And Cluster

- Persistent snapshots and InstallSnapshot RPC.
- Membership changes.
- Log compaction.
- Better leader transfer and failover tests.
- Client-side routing helper.
- Operational docs for running a local three-node cluster.

## Data Types

Existing helper data types include counter, hash, and list. Useful additions:

- set;
- sorted set;
- bitmap;
- stream-like append-only list;
- common TTL behavior across all data types.

## Observability

- Structured logging.
- Metrics endpoint.
- Per-operation latency histograms.
- WAL fsync latency.
- Flush and compaction timing.
- Raft state and replication lag metrics.

## Benchmarking

- Wire real benchmark binaries.
- Add latency histograms and persisted benchmark reports.
- Track read/write/compaction/cache metrics per workload.

## Release Readiness

- Installable library target.
- Versioned public API.
- Stable on-disk format versioning.
- Upgrade/downgrade story.
- Clear support matrix for operating systems and compilers.
