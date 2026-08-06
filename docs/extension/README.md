# KVEngine Extension Guides

This directory breaks the future extension work into focused engineering
guides. Each guide is meant to help a future contributor move from an idea to
an implementation plan without having to rediscover the relevant code paths.

Each module answers five questions:

1. Why the work matters.
2. Which existing files should be read first.
3. What design direction is recommended.
4. Which tests should be added.
5. What "done" should mean for that area.

Recommended order:

1. [WAL Lifecycle](01_wal_lifecycle.md)
2. [Immutable MemTable And Background Flush](02_immutable_memtable_background_flush.md)
3. [Crash Recovery Test Harness](03_crash_recovery.md)
4. [SSTable And Compaction](04_sstable_compaction.md)
5. [Benchmarks And Observability](05_benchmarks_observability.md)
6. [Network Protocol Hardening](06_network_protocol.md)
7. [Raft And Cluster Completeness](07_raft_cluster.md)
8. [Redis-like Data Types](08_data_types.md)

The intent of this sequence is to strengthen single-node storage correctness
first, then improve performance and visibility, and only then expand the
service protocol, distributed behavior, and higher-level data structures.

## General Engineering Rules

### Preserve Existing Semantics

Any extension should preserve these existing engine rules:

- Writes are appended to the WAL before they are applied to the MemTable.
- Sequence numbers define version visibility.
- A snapshot can only observe versions visible at the time it was created.
- Tombstones represent deletes and must not be dropped while an active snapshot
  may still need them.
- TTL metadata belongs to a value version. Visibility is resolved by sequence
  number first, then expiration is checked against wall-clock time.
- The manifest is the durable metadata source for the live SSTable set.

### Use Crash-Safe Ordering

When a change involves data files and metadata, prefer this ordering:

1. Write the new data file.
2. Make the new data file durable.
3. Append the metadata record.
4. Make the metadata durable.
5. Delete or archive obsolete files.
6. Record obsolete-file removal if the format requires it.

Never let durable metadata point to a file that may not exist or may be
incomplete after a crash.

### Test At The Right Boundary

For each subsystem, prefer a layered test set:

- Low-level unit tests for codecs, file formats, parsers, and algorithms.
- DB-boundary tests through `kv::DB` for persistence and visibility behavior.
- Integration tests for network, Raft, server/client behavior, or real sockets.

### Keep Documentation In Sync

If a change affects behavior, configuration, file formats, network protocol, or
operations, update the relevant docs:

- `README.md`
- `docs/configuration.md`
- the subsystem document such as `docs/wal.md`, `docs/sstable.md`, or
  `docs/raft.md`
- `config/server.yaml` when configuration changes

## Useful Commands

Build apps and tests:

```bash
./scripts/build.sh
```

Run the full test suite:

```bash
ctest --test-dir build --output-on-failure
```

Run focused tests:

```bash
./scripts/run_tests.sh -R DBTest
./scripts/run_tests.sh -R Raft
```

Start the server:

```bash
./build/apps/kv_server --config=config/server.yaml
```

Open the direct local CLI:

```bash
./build/apps/kv_cli data/db
```
