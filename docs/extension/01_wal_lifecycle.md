# WAL Lifecycle Extension Guide

The WAL is the durability foundation of the storage engine. KVEngine already
has WAL append, reading, record encoding, replay, and DB-open recovery. The
next step is lifecycle management: segment rotation, cleanup after flush,
optional archival, format versioning, and eventually group commit.

This should be one of the first major storage extensions because later work on
background flush, crash recovery, and compaction will all depend on clearer WAL
boundaries.

## Goals

Short-term goals:

- Support multiple WAL segments.
- Rotate the active segment when it reaches a configured size.
- Replay all live segments in order during `DB::Open`.
- Safely delete or archive obsolete WAL segments after flush and manifest sync.

Medium-term goals:

- Add a WAL file or record format version.
- Add group commit to reduce sync overhead under concurrent writes.
- Expose WAL information in admin/status output and metrics.

## Existing Code To Read First

- `include/kv/wal/log_record.h`
- `src/wal/log_record.cpp`
- `include/kv/wal/wal_writer.h`
- `src/wal/wal_writer.cpp`
- `include/kv/wal/wal_reader.h`
- `src/wal/wal_reader.cpp`
- `include/kv/wal/wal_manager.h`
- `src/wal/wal_manager.cpp`
- `src/wal/log_recovery.cpp`
- `src/engine/recovery.cpp`
- `src/engine/db_impl.cpp`
- `include/kv/engine/db.h`
- `tests/wal/`
- `tests/engine/recovery_test.cpp`

Related docs:

- `docs/wal.md`
- `docs/architecture.md`

## Current Behavior To Preserve

The current WAL appears to be primarily single-file based:

- `DBOptions::wal_path` can explicitly specify the WAL path.
- If `wal_path` is empty, the DB derives a path from `db_path`.
- Each mutation is appended to the WAL before being applied to the MemTable.
- `DB::Open` replays the WAL to restore unflushed records.

Segmented WAL support should not break this behavior. Keep an explicit
compatibility path for `wal_path` so existing tests and local data continue to
work.

## Recommended Segment Layout

Use a directory under the DB path:

```text
data/db/
  wal/
    000001.wal
    000002.wal
    000003.wal
```

Rules:

- Segment ids are monotonically increasing.
- File names are fixed-width numeric strings so lexical order matches id order.
- The largest id is the active segment.
- Recovery replays segments from smallest id to largest id.

For the first implementation, scanning file names is enough. A separate WAL
metadata file can be added later if startup cost or cleanup state becomes more
complex.

## Suggested Options

Add fields to `DBOptions`:

```cpp
size_t wal_segment_size_bytes = 64 * 1024 * 1024;
bool wal_segmented = true;
bool wal_archive_enabled = false;
std::string wal_dir;
```

Compatibility rules:

- If `wal_path` is explicitly set, use legacy single-file WAL mode.
- Otherwise, use `wal_dir` if set.
- Otherwise, use `db_path + "/wal"`.

Keep defaults conservative and make sure existing tests do not need broad
rewrites.

## Segment Metadata

Internally track metadata like:

```cpp
struct WALSegmentMeta {
  uint64_t segment_id = 0;
  std::string path;
  uint64_t min_sequence = 0;
  uint64_t max_sequence = 0;
  uint64_t size_bytes = 0;
  bool active = false;
};
```

The first version can compute `min_sequence` and `max_sequence` by scanning the
segment during recovery. Later versions can persist these values in a segment
header or metadata file.

## Segment Header

A later version should add a small header:

```text
magic: "KVWAL\0\1"
format_version: uint32
segment_id: uint64
created_at_ms: uint64
```

Because this affects backward compatibility, do it after basic segmented WAL
works. The reader should eventually handle:

- legacy WAL files with no header;
- new WAL files with a valid header;
- invalid magic as `Corruption`;
- unknown future versions as `NotSupported` or `Corruption`.

## Rotation Flow

Prefer checking for rotation after an append:

1. Append the current record to the active segment.
2. Update the active segment byte size.
3. If the segment exceeds `wal_segment_size_bytes`:
   - sync the current segment;
   - close the current writer;
   - create the next segment;
   - open a writer for the new segment.

Important details:

- Rotation does not affect sequence numbers.
- If rotation fails, the DB should return an error or enter a non-writable
  state. It must not keep accepting writes into an unknown WAL state.
- Recovery must read records from both old and new segments.

## Cleanup After Flush

The cleanup condition is not "an SSTable was written". It is "the SSTable and
the manifest metadata for it are durable".

Recommended ordering:

1. Flush the MemTable to a complete SSTable.
2. Sync the SSTable.
3. Append the manifest add-file record.
4. Sync the manifest.
5. Update the flushed sequence watermark.
6. Find WAL segments whose `max_sequence <= flushed_sequence`.
7. Delete or archive those obsolete segments.

Do not truncate the active segment in the first version. Only delete whole
obsolete segments. This keeps crash recovery much easier to reason about.

## Archive Mode

If `wal_archive_enabled=false`, delete obsolete segments.

If `wal_archive_enabled=true`, move obsolete files to:

```text
data/db/wal_archive/
  000001.wal
  000002.wal
```

Rules:

- Move or rename must succeed before the segment is removed from the live set.
- `DB::Open` should only replay the live WAL directory, not the archive.
- Admin tools may optionally report archived segment counts and bytes.

## Group Commit

Group commit should come after rotation and cleanup.

Basic approach:

1. The first writer entering the write path becomes the group leader.
2. Later writers enqueue their records and wait.
3. The leader collects a small batch of pending records.
4. The leader appends all records in sequence order.
5. If any request requires sync, the leader performs one sync.
6. The leader applies records to the MemTable in sequence order.
7. All writers are notified with their individual status.

Possible internal structure:

```cpp
struct PendingWrite {
  uint64_t sequence = 0;
  LogRecord record;
  WriteOptions options;
  Status status;
  bool done = false;
  std::condition_variable cv;
};
```

Be careful with object lifetimes. A queued pending write should not be
destroyed while the group leader still references it.

## Recovery Behavior

During `DB::Open`:

1. Detect legacy single-file WAL versus segmented WAL.
2. If segmented, scan `wal_dir`.
3. Sort segments by id.
4. Replay each segment in order.
5. Ignore an incomplete trailing record only in the latest segment.
6. Treat truncation or checksum errors in middle segments more strictly,
   preferably as `Corruption`.

The latest segment may have a torn tail from a crash. A middle segment should
have been closed and synced, so corruption there is more suspicious.

## Test Plan

Low-level WAL tests:

- automatic rotation with a very small segment size;
- fixed-width file-name ordering;
- active segment id increments;
- append continues after close/open of a new segment;
- reader can replay multiple segments;
- legacy single-file WAL still works.

DB recovery tests:

- write enough data to rotate, reopen, and read all values;
- recover deletes and tombstones across segment boundaries;
- recover TTL records across segment boundaries;
- delete obsolete segments after flush;
- reopen after obsolete segments were deleted;
- preserve unflushed data in the active segment.

Failure-injection tests:

- failure while creating a new segment;
- sync failure;
- crash before manifest sync, with no WAL cleanup;
- crash after manifest sync, where cleanup can happen on the next open;
- truncated tail in the latest segment;
- checksum corruption in a middle segment.

## Acceptance Criteria

Basic acceptance:

- The full existing test suite passes.
- New rotation and multi-segment recovery tests pass.
- Explicit `wal_path` single-file mode remains usable.

Behavioral acceptance:

- Long write workloads no longer require replaying every historical write.
- Old WAL segments are removed only after their data is durable in SSTables and
  the manifest.
- Replay order preserves sequence-number semantics.
- Errors return explicit `Status` values and do not silently drop data.

## Suggested Pull Request Split

1. Refactor `WALManager` to introduce a segment abstraction while preserving
   single-file behavior.
2. Add segmented WAL writing and recovery.
3. Add cleanup of obsolete segments after flush.
4. Add WAL format/header versioning.
5. Add group commit.
