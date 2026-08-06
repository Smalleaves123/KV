# Crash Recovery Test Harness Guide

Crash recovery is the core correctness story for a storage engine. KVEngine
already has WAL replay, manifest recovery, SSTable reads, and several reopen
tests. The next step is a deterministic test harness that can inject failures
at important persistence points, reopen the DB, and compare results with a
simple reference model.

This work can start before major implementation changes. The earlier it lands,
the more protection it gives to WAL, flush, manifest, and compaction refactors.

## Goals

Short-term goals:

- Test WAL truncation, checksum errors, and manifest add/remove behavior at the
  `DB::Open` boundary.
- Add controlled failure points for important persistence phases.
- Compare reopened DB state against a small reference model.

Medium-term goals:

- Support deterministic random operation sequences.
- Add a fake clock for TTL tests.
- Cover compaction interruption and recovery.
- Cover multi-segment WAL recovery once segmented WAL exists.

## Existing Code To Read First

- `src/engine/recovery.cpp`
- `src/engine/db_impl.cpp`
- `src/wal/log_recovery.cpp`
- `src/wal/wal_reader.cpp`
- `src/wal/log_record.cpp`
- `src/version/manifest.cpp`
- `src/version/version_set.cpp`
- `src/sstable/table_builder.cpp`
- `src/sstable/table_reader.cpp`
- `src/version/compaction.cpp`
- `tests/engine/recovery_test.cpp`
- `tests/wal/`
- `tests/version/manifest_test.cpp`
- `tests/engine/compaction_test.cpp`

Related docs:

- `docs/wal.md`
- `docs/sstable.md`
- `docs/architecture.md`

## Testing Philosophy

Do not only test that low-level readers reject bad files. The most valuable
tests use the public DB boundary:

1. Create a DB.
2. Execute a small sequence of writes.
3. Simulate a crash, write failure, or file corruption at a specific point.
4. Reopen with `DB::Open`.
5. Verify state through `Get`, `TTL`, iterator, and stats.

Low-level tests are still useful, but DB-open tests catch realistic bugs in
ordering and metadata durability.

## Reference Model

Start with a small in-test model:

```cpp
struct ModelValue {
  bool exists = false;
  std::string value;
  std::optional<int64_t> expire_at_ms;
};

std::map<std::string, ModelValue> model;
```

Supported model operations:

- put;
- delete;
- expire;
- persist;
- advance logical time;
- flush;
- compact;
- reopen.

After each reopen, compare:

- `Get` results;
- `TTL` results;
- iterator ordering;
- tombstone behavior.

## Fake Clock

TTL tests should not depend heavily on real sleeps. A fake clock makes tests
faster and deterministic.

Possible abstraction:

```cpp
class Clock {
 public:
  virtual ~Clock() = default;
  virtual int64_t NowMillis() const = 0;
};
```

Production uses a system clock. Tests use a controllable fake clock.

This may touch:

- DB TTL checks;
- `TTLManager`;
- compaction behavior around expired values.

If this is too invasive for the first step, keep the first recovery tests
non-TTL or use only a few short sleeps. The long-term direction should still be
a fake clock.

## Failure Points

Add a test-only failure injection mechanism. Keep it out of the public API.

Possible failure points:

```cpp
enum class FailurePoint {
  kAfterWALAppend,
  kAfterMemTableApply,
  kAfterSSTableWriteBeforeManifest,
  kAfterManifestAppendBeforeSync,
  kAfterManifestSyncBeforeWALCleanup,
  kDuringCompactionOutput,
  kAfterCompactionAddBeforeRemove,
};
```

Tests can configure:

```cpp
InjectFailure(FailurePoint::kAfterManifestAppendBeforeSync,
              Status::IOError("injected failure"));
```

Rules:

- Default behavior is no injection.
- Hooks must be thread-safe once background flush exists.
- Each test must reset hooks.
- Release builds should not carry unnecessary test machinery.

## Important Crash Points

### After WAL Append

Scenario:

1. WAL append succeeds.
2. Crash occurs before MemTable apply.
3. DB reopens.

Expected behavior:

- If the WAL record is complete and durable, the write is recovered.
- If future tests can simulate non-durable OS buffers, unsynced writes may be
  lost, but this is difficult to model portably.

### After MemTable Apply Before Flush

Scenario:

1. `Put` succeeds.
2. Data exists only in WAL and MemTable.
3. Crash or close occurs.
4. DB reopens.

Expected behavior:

- WAL replay restores the value.

### After SSTable Write Before Manifest

Scenario:

1. Flush writes an SSTable.
2. Manifest add-file record is not durable.
3. Crash occurs.
4. DB reopens.

Expected behavior depends on the chosen metadata policy:

- If the manifest is authoritative, the orphan SSTable is ignored.
- If SST directory scanning is supported as fallback, the rules must be
  explicit and tested.

The key requirement is that an orphan file must not introduce wrong values.

### After Manifest Sync Before WAL Cleanup

Scenario:

1. SSTable is durable.
2. Manifest add-file record is durable.
3. Obsolete WAL has not been deleted.
4. DB reopens.

Expected behavior:

- Data is present.
- WAL replay does not create incorrect duplicates.
- The next sequence number remains monotonic.

### After WAL Cleanup

Scenario:

1. SSTable and manifest are durable.
2. Obsolete WAL segment is deleted.
3. DB reopens.

Expected behavior:

- Data is recovered from SSTables.
- Recovery no longer depends on the deleted WAL.

### During Compaction

Important phases:

- output SSTable is partially written;
- output SSTable is complete but not in the manifest;
- manifest records the output but not old-file removal;
- old-file removal is partially recorded.

Expected behavior:

- No wrong value is returned.
- Latest committed values are not lost.
- Extra old files are acceptable if version metadata remains deterministic.

## WAL Corruption Tests

Cases to cover:

- truncated tail in the latest WAL;
- checksum error in the latest WAL;
- truncated middle WAL segment;
- checksum error in a middle WAL segment;
- unknown record type;
- record length beyond file size.

Suggested policy:

- incomplete trailing record in the latest segment may be ignored;
- checksum mismatch should return `Corruption`;
- middle segment corruption should return `Corruption`.

Document the exact policy in `docs/wal.md`.

## Manifest Corruption Tests

Cases to cover:

- empty manifest;
- partial trailing manifest record;
- add-file record pointing to a missing SSTable;
- remove-file record for a missing SSTable;
- duplicate add/remove records;
- checksum failure if manifest records gain checksums.

Suggested policy:

- partial trailing record can be ignored if it is clearly at the end;
- middle corruption should return `Corruption`;
- missing SSTable references should probably return `Corruption` in the first
  strict version.

## SSTable Corruption Tests

Once SSTable checksums exist, cover:

- bad footer magic;
- block handle beyond file size;
- data block checksum mismatch;
- index block checksum mismatch;
- filter block checksum mismatch.

DB-open validation can be staged:

- first version: lazy validation when a bad block is read;
- later version: admin verify command that scans all SSTables.

## Deterministic Random Testing

Use a fixed seed:

```cpp
std::mt19937 rng(12345);
```

Randomly choose operations:

- `Put`;
- `Delete`;
- `Expire`;
- `Persist`;
- `Flush`;
- `Compact`;
- `Reopen`.

After every N operations, reopen and compare against the model.

Rules:

- Print the operation sequence on failure.
- Keep operation counts small enough for CI.
- Use fixed seeds for reproducibility.

## Suggested Test Files

Possible additions:

```text
tests/engine/crash_recovery_test.cpp
tests/engine/recovery_model.h
tests/engine/failure_injection.h
```

If helpers are only for tests, keep them under `tests/` and avoid adding them
to public headers.

## Acceptance Criteria

Basic acceptance:

- DB-open recovery tests cover WAL, manifest, and SSTable edge cases.
- Failure injection does not affect normal builds.
- Tests are deterministic and do not rely on long sleeps.

Behavioral acceptance:

- Durable writes survive reopen.
- Files that never reached durable metadata do not become incorrectly visible.
- Replayed records do not break sequence ordering.
- Corruption returns explicit errors rather than silently returning bad values.

## Suggested Pull Request Split

1. Add DB-open tests for WAL tail truncation and corruption.
2. Add manifest recovery edge-case tests.
3. Add failure injection hooks.
4. Add model-based deterministic recovery tests.
5. Add segmented WAL and compaction interruption cases.
