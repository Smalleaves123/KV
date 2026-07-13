# Architecture

KVEngine is organized as a small log-structured key-value database. The public
entry point is `kv::DB`, implemented by `DBImpl`. The engine writes updates to a
WAL, stores recent versions in a memtable, flushes immutable data into SSTable
files, and uses a manifest to recover the set of live SST files after restart.

## Main Components

```text
Client / app
    |
    v
kv::DB API
    |
    +-- WAL append
    +-- MemTable update
    +-- SSTable flush
    +-- Manifest update
    +-- Compaction
    +-- Cache / table cache
```

## Platform Boundary

Platform-specific socket and durable-file operations are isolated under
`include/kv/common/`. The network layer uses the socket compatibility API for
Winsock, Linux, and macOS, while WAL synchronization uses the corresponding
`fsync` or `_commit` implementation. Higher-level storage and protocol code
does not include Unix-only headers.

Subsystems:

- `src/engine`: DB orchestration, write path, read path, snapshots,
  transactions, recovery, compaction triggers.
- `src/wal`: durable log record encoding, writer, reader, and replay.
- `src/memtable`: versioned in-memory table.
- `src/sstable`: persistent sorted table format, block encoding, Bloom filter,
  table reader, and table cache.
- `src/version`: manifest metadata.
- `src/cache`: optional value cache implementations.
- `src/net`: TCP server and line-oriented command execution.
- `src/raft`: experimental replication layer.

## Write Path

For `Put`, `Delete`, and `WriteBatch`:

1. Validate the DB is open and the key is non-empty.
2. Allocate the next sequence number.
3. Append a record to the WAL.
4. Optionally sync the WAL if `WriteOptions::sync` or
   `DBOptions::sync_on_write` is enabled.
5. Apply the versioned record to the memtable.
6. Update the latest-key sequence index used by optimistic transactions.
7. Flush the memtable when `memtable_write_buffer_size` is reached.
8. Optionally run compaction after a flush.

Sequence numbers are monotonically increasing and define the visibility order
for snapshots and transaction validation.

## Read Path

For `Get`:

1. Validate the key and snapshot handle.
2. Resolve the read sequence:
   - latest read: `uint64_t::max()`;
   - snapshot read: snapshot sequence.
3. Search the memtable for the newest visible version.
4. If not found, search SST files from newest to oldest.
5. Each SST lookup checks the table cache, Bloom filter, index block, and data
   block.
6. If value caching is enabled and the read is a latest read, SST hits are
   cached by key.

Deletes are represented as tombstones. A visible tombstone returns `NotFound`.

## Persistence

Durability uses three persistent structures:

- WAL: replayed on open to restore unflushed records.
- SST files: immutable sorted files created by memtable flush and compaction.
- Manifest: append-only metadata that records live SST files and their maximum
  sequence numbers.

On open, the DB:

1. Opens or creates the manifest.
2. Loads SST metadata from the manifest, or scans the SST directory if the
   manifest is absent.
3. Replays the WAL if it exists.
4. Sets `next_seq` to one greater than the largest sequence found.
5. Rebuilds the latest-key sequence index.

## Compaction

Compaction merges SST files into one newer SST file. It keeps the newest version
per key and preserves deletion tombstones needed to represent deletes. Manual
compaction is exposed through `DB::Compact()` and `kv_admin compact`.

Automatic compaction can run after memtable flush when:

- `auto_compaction_enabled` is true;
- the number of SST files reaches `compaction_min_input_files`;
- no snapshot is active.

Active snapshots block compaction because old versions may still be visible.

## Snapshots

A snapshot captures the latest sequence number at creation time. Reads using
that snapshot only see versions with `seq <= snapshot.sequence()`.

Snapshots must be released with `ReleaseSnapshot`. Compaction is rejected or
skipped while any snapshot is active.

## Transactions

Transactions use optimistic concurrency control:

- transaction start sequence is captured at `BeginTransaction`;
- reads are served from the start sequence view plus local writes;
- commit validates that every read key and written key has not changed since
  the start sequence;
- committed writes are applied as normal WAL + memtable updates.

See [Transactions And Snapshots](transaction.md).

## Observability

The DB exposes:

- `CacheStats`: value-cache hits, misses, evictions, expirations.
- `ReadPathStats`: table-cache hits/misses/evictions/entries and Bloom filter
  query/negative counts.
- `CompactionStats`: attempts, skipped attempts, successes, failures.
- server health logs: connection and transaction counters.

`INFO`, `STATS`, and `kv_admin stats` surface much of this information.

## Threading Model

`DBImpl` protects mutable engine state with a mutex. The network server accepts
TCP clients and dispatches each connection to a thread pool. Raft has separate
threads for ticking, RPC listening, and applying committed entries.

The project favors clarity over maximum concurrency. There is room to reduce
lock granularity in the future.
