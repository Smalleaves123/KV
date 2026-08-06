# Immutable MemTable And Background Flush Guide

KVEngine already has a MemTable, flush to SSTable, manifest records, and
compaction triggers. The next step is to decouple flush work from the foreground
write path. When the active MemTable reaches its threshold, it should rotate
into an immutable MemTable and a background worker should flush it while new
writes continue in a fresh active MemTable.

This makes the engine look much more like a real LSM implementation and creates
a foundation for write stalls, flush metrics, and multi-level compaction.

## Goals

Short-term goals:

- Add an active mutable MemTable.
- Add an immutable MemTable queue.
- Flush immutable MemTables in a background thread.
- Read from active, immutable, and SSTable layers.
- Make `Close()` wait for background flush safely.

Medium-term goals:

- Support multiple immutable MemTables.
- Add write stall/backpressure.
- Improve MemTable memory accounting.
- Expose flush queue length, flush duration, and write stall metrics.

## Existing Code To Read First

- `include/kv/memtable/memtable.h`
- `src/memtable/memtable.cpp`
- `include/kv/memtable/skiplist.h`
- `include/kv/memtable/skiplist.tpp`
- `src/engine/db_impl.cpp`
- `include/kv/engine/db_impl.h`
- `include/kv/engine/db.h`
- `src/sstable/table_builder.cpp`
- `src/version/manifest.cpp`
- `src/version/version_set.cpp`
- `src/version/compaction.cpp`
- `include/kv/concurrency/thread_pool.h`
- `src/concurrency/thread_pool.cpp`
- `tests/memtable/`
- `tests/engine/db_test.cpp`
- `tests/engine/compaction_test.cpp`
- `tests/engine/iterator_test.cpp`

Related docs:

- `docs/memtable.md`
- `docs/sstable.md`
- `docs/architecture.md`

## Existing Write Path To Preserve

Before changing the code, verify the current `DBImpl` write flow:

1. Validate that the DB is open and the key is valid.
2. Allocate a sequence number.
3. Append the mutation to the WAL.
4. Sync the WAL when `WriteOptions::sync` or `DBOptions::sync_on_write` asks
   for it.
5. Apply the mutation to the MemTable.
6. Update the latest-key sequence index used by optimistic transactions.
7. Flush if `memtable_write_buffer_size` is exceeded.
8. Trigger auto-compaction if configured.

Background flush must preserve WAL-before-MemTable ordering.

## Recommended Internal State

Add state similar to this inside `DBImpl`:

```cpp
std::unique_ptr<MemTable> mutable_mem_;
std::deque<ImmutableMemTable> immutable_mems_;
std::thread flush_thread_;
std::mutex mutex_;
std::condition_variable flush_cv_;
std::condition_variable flush_done_cv_;
bool shutting_down_ = false;
Status background_error_;
```

If `DBImpl` already has a main mutex, prefer reusing the existing lock model
instead of introducing several locks that are hard to reason about.

Each immutable MemTable should carry metadata:

```cpp
struct ImmutableMemTable {
  std::unique_ptr<MemTable> table;
  uint64_t min_sequence = 0;
  uint64_t max_sequence = 0;
  size_t approximate_bytes = 0;
};
```

`max_sequence` matters for future WAL cleanup: after this immutable MemTable is
flushed and recorded in the manifest, WAL segments up to that sequence may
become obsolete.

## Rotation Flow

Foreground write flow:

1. Append the WAL record.
2. Apply the record to the active mutable MemTable.
3. If the active MemTable is below threshold, return.
4. If it exceeds threshold:
   - move the active MemTable into the immutable queue;
   - create a fresh active MemTable;
   - notify the flush worker.
5. Return the write result.

Do not perform SSTable file building while holding the DB mutex. Otherwise the
flush is still effectively foreground work.

## Background Flush Worker

The worker loop should:

1. Wait until `immutable_mems_` is non-empty or shutdown begins.
2. Remove the oldest immutable MemTable from the queue.
3. Release the DB mutex.
4. Build an SSTable from the MemTable contents.
5. Append and sync the manifest add-file record.
6. Update the version set.
7. Reacquire the mutex.
8. Record success or background error.
9. Notify waiting writers or `Close()`.

Flush immutable MemTables FIFO in the first version. That keeps sequence ranges
and WAL cleanup simple.

## Read Path Changes

Reads must check layers from newest to oldest:

1. Active mutable MemTable.
2. Immutable MemTables from newest to oldest.
3. SSTables from newest to oldest, or by level rules once multi-level
   compaction exists.

Every layer must honor the requested read sequence:

- latest reads use the latest visible version;
- snapshot reads use `seq <= snapshot.sequence()`;
- tombstones still hide older values;
- TTL is checked after sequence visibility is resolved.

## Iterator And Scan Stability

Iterator creation should take a stable view. If the current iterator already
materializes visible entries while holding the DB lock, keep that pattern. If it
streams from live structures, background flush can make iteration unstable.

The simplest first implementation:

1. Lock the DB.
2. Gather visible entries from active, immutable, and SSTable sources.
3. Resolve latest visible versions.
4. Return an iterator over the materialized result.

This is not the fastest approach, but it is clear and safe.

## Snapshot Semantics

Snapshot reads must remain unchanged after this feature.

Test scenario:

1. Write `key=v1`.
2. Create a snapshot.
3. Write `key=v2`.
4. Trigger MemTable rotation and background flush.
5. Snapshot reads `v1`.
6. Latest read returns `v2`.

Run this both before and after the background flush completes.

## Write Stall And Backpressure

If background flush cannot keep up, immutable MemTables will accumulate. Add:

```cpp
size_t max_immutable_memtables = 2;
```

When the queue reaches the limit, foreground writes should wait:

```text
while immutable_mems_.size() >= max_immutable_memtables:
  wait flush_done_cv
```

First version behavior can be simple blocking. Later versions can add:

- soft stalls;
- hard stalls;
- stall duration metrics;
- timeout or `Busy` status.

## Background Error Handling

Background flush failure must not be ignored.

Recommended behavior:

- Store the first `background_error_`.
- New writes return that error or a clear read-only error.
- `Close()` returns the background error.
- Reads may continue from already-visible state, but this must be documented.

Errors to test:

- SSTable build failure.
- manifest append failure.
- manifest sync failure.
- version set update failure.

## Close Behavior

`DB::Close()` should:

1. Block new writes.
2. Decide whether to rotate and flush a non-empty active MemTable.
3. Notify the flush worker.
4. Wait until the immutable queue is empty.
5. Join the flush thread.
6. Close WAL, manifest, and other resources.

Normal close should preferably flush the active MemTable. Crash recovery should
still work from WAL, but clean shutdown should leave less replay work.

## Auto Compaction

After background flush lands, auto-compaction should be triggered after a flush
success, not inside the foreground threshold-crossing write.

First version:

- The flush worker may call the existing compaction trigger after a successful
  flush.

Later version:

- Add a separate compaction worker so flush and compaction do not block each
  other.

## Memory Accounting

Improve approximate memory tracking over time. Include estimates for:

- key bytes;
- value bytes;
- internal key and sequence metadata;
- skiplist node overhead;
- tombstone metadata;
- TTL metadata.

The first implementation does not need perfect accounting. It needs stable,
monotonic accounting good enough to trigger flush.

## Test Plan

Rotation tests:

- use a tiny `memtable_write_buffer_size`;
- write enough records to rotate;
- verify reads from active and immutable layers.

Background flush tests:

- trigger rotation;
- wait for flush;
- verify SSTable creation;
- reopen and verify data.

Multiple immutable tests:

- block or slow the flush worker with a test hook;
- create multiple immutable MemTables;
- verify newest immutable values win;
- verify FIFO flush order.

Snapshot tests:

- snapshot sees old values before and after rotation;
- snapshot sees old values before and after flush;
- latest reads see new values.

Shutdown tests:

- trigger background flush;
- call `Close()` immediately;
- verify close waits;
- reopen and verify data.

Error tests:

- inject SSTable build failure;
- inject manifest failure;
- verify new writes observe the background error.

## Acceptance Criteria

Basic acceptance:

- The full existing test suite passes.
- Threshold-crossing writes no longer perform the main SSTable build work.
- Background flush updates SSTable and manifest state correctly.
- `Close()` does not lose data.

Behavioral acceptance:

- Active, immutable, and SSTable read ordering is correct.
- Snapshot semantics are unchanged.
- Iterators remain stable.
- Background errors are visible to the caller.

## Suggested Pull Request Split

1. Add clearer MemTable memory and sequence-range metadata.
2. Introduce active and immutable MemTable layers while keeping flush
   synchronous.
3. Add the background flush worker.
4. Add write stall/backpressure.
5. Move auto-compaction triggering to flush completion.
6. Add flush metrics.
