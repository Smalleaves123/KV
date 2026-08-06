# SSTable And Compaction Extension Guide

SSTables and compaction define read performance, space amplification, write
amplification, and long-term stability in an LSM-style engine. KVEngine already
has a table builder, table reader, blocks, Bloom filters, table cache, manifest
metadata, and manual/automatic compaction. The next steps are file integrity,
configurable table layout, range scans, multi-level compaction, and better
compaction picking.

## Goals

Short-term goals:

- Add checksums to SSTable blocks.
- Add a table format version or a clear compatibility strategy.
- Expose block size, Bloom bits per key, and table-cache capacity options.
- Add corruption tests.

Medium-term goals:

- Add a range scan API.
- Introduce L0/L1+ multi-level compaction.
- Pick compaction inputs based on size and key-range overlap.
- Strengthen snapshot, tombstone, and TTL semantics during compaction.

## Existing Code To Read First

- `include/kv/sstable/block.h`
- `src/sstable/block.cpp`
- `include/kv/sstable/block_builder.h`
- `src/sstable/block_builder.cpp`
- `include/kv/sstable/block_iterator.h`
- `src/sstable/block_iterator.cpp`
- `include/kv/sstable/table_builder.h`
- `src/sstable/table_builder.cpp`
- `include/kv/sstable/table_reader.h`
- `src/sstable/table_reader.cpp`
- `include/kv/sstable/footer.h`
- `src/sstable/footer.cpp`
- `include/kv/sstable/filter_block.h`
- `src/sstable/filter_block.cpp`
- `src/sstable/table_cache.cpp`
- `include/kv/version/file_meta.h`
- `src/version/file_meta.cpp`
- `src/version/version.cpp`
- `src/version/version_set.cpp`
- `src/version/manifest.cpp`
- `src/version/compaction.cpp`
- `src/engine/db_impl.cpp`
- `tests/sstable/`
- `tests/version/`
- `tests/engine/compaction_test.cpp`
- `tests/engine/iterator_test.cpp`

Related docs:

- `docs/sstable.md`
- `docs/architecture.md`

## SSTable Checksums

### Recommended Scope

Start with data block checksums, then extend to:

- index blocks;
- filter blocks;
- the footer;
- value envelopes if needed.

A simple block trailer format is enough:

```text
block payload
uint32 checksum
```

If the current block handle stores `offset` and `size`, define whether `size`
includes the trailer. Prefer this rule:

- block handle size means payload size;
- reader reads an additional 4-byte trailer;
- block decoder only receives the payload.

This keeps checksum handling in the table reader instead of leaking it into
every block consumer.

### Checksum Algorithm

Use a stable on-disk checksum:

- CRC32C is a common choice, but may require a dependency.
- CRC32 is acceptable for a first version if implemented locally.

Do not use `std::hash`. It is not a stable file-format primitive.

### Compatibility Strategy

There are two main options:

1. Add a table format version and let the reader recognize legacy files without
   checksums.
2. Support only the new format and regenerate test data.

Prefer the first option. Keeping backward compatibility makes the project more
credible and avoids surprising local data loss.

## Table Format Version

Add format metadata in the footer or a table header:

```text
magic
format_version
checksum_type
```

If the current footer already has a magic value, extend the footer carefully.
Check whether the footer has fixed-size assumptions.

Suggested behavior:

- version 0: legacy format, no block checksums;
- version 1: block checksums enabled;
- unknown newer version: return `Corruption` or `NotSupported`.

## Configurable Table Options

Add fields to `DBOptions`:

```cpp
size_t sstable_block_size_bytes = 4 * 1024;
int bloom_bits_per_key = 10;
size_t table_cache_capacity = 128;
```

Implementation path:

1. Add the fields to `DBOptions`.
2. Pass them from `DBImpl` into table builder and table reader/cache setup.
3. Make `TableBuilder` use block size when deciding to finish a data block.
4. Make `FilterBlockBuilder` use Bloom bits per key.
5. Make `TableCache` use the configured capacity.
6. Update server config parsing.
7. Update `config/server.yaml` and documentation.

Defaults should match current behavior as closely as possible.

## Range Scan API

The existing iterator can support scans, but an explicit API is useful for
applications and network commands:

```cpp
Status Scan(const ReadOptions& options,
            const Slice& start_key,
            const Slice& end_key,
            size_t limit,
            std::vector<std::pair<std::string, std::string>>* out);
```

Recommended semantics:

- `start_key` is inclusive.
- `end_key` is exclusive.
- empty `end_key` means no upper bound.
- `limit == 0` means no limit.

The first version can implement this using `NewIterator`. Later versions can
optimize by seeking directly into SSTable indexes.

## Multi-Level Compaction

### File Metadata

Add a level field to `FileMeta`:

```cpp
int level = 0;
```

The manifest must persist this level. Recovery must rebuild the same level
layout.

### Level Semantics

Recommended model:

- L0 contains newly flushed SSTables and may have overlapping key ranges.
- L1+ contain non-overlapping key ranges within the same level.
- L0 reads must check files newest to oldest.
- L1+ reads can find the candidate file by key range.

### First Compaction Policy

Start simple:

- trigger L0 compaction when L0 file count exceeds a threshold;
- choose all L0 files or the oldest group of L0 files;
- find all L1 files whose ranges overlap the input range;
- merge and output new L1 files.

Later:

- trigger L1+ compaction by total level size;
- choose the largest or oldest file;
- include overlapping files in the next level.

Correctness is more important than a clever picker in the first version.

## Tombstone Rules

Compaction must handle tombstones conservatively.

Basic rules:

- If a snapshot is active, do not drop versions it may still observe.
- If a tombstone is the newest version for a key and lower levels may contain
  older values, keep the tombstone.
- If the implementation cannot prove a tombstone is safe to drop, keep it.

The current project already skips or rejects compaction when snapshots are
active. Preserve that conservative behavior until more precise version-retention
rules exist.

## TTL Rules

TTL metadata belongs to value versions. During compaction:

- preserve TTL metadata for live values;
- do not accidentally revive expired values;
- do not physically drop expired values until the snapshot story is tested;
- keep snapshot behavior around `Expire` and `Persist` unchanged.

For the first version, avoid aggressive TTL cleanup during compaction. Logical
read behavior is more important than reclaiming a small amount of space.

## Read Path With Levels

After levels are introduced, point lookup should use:

1. active MemTable;
2. immutable MemTables, if implemented;
3. L0 files newest to oldest;
4. L1+ files by key range.

Important details:

- L0 can overlap, so newest-to-oldest order matters.
- L1+ should not overlap within a level.
- Snapshot reads still need sequence filtering.
- Bloom filters and table cache remain useful at every level.

## Test Plan

Checksum tests:

- write and read a valid SSTable;
- mutate one byte in a data block and expect `Corruption`;
- mutate an index block and expect seek/get failure;
- mutate footer magic and expect reader open failure;
- verify legacy SSTable behavior according to the compatibility policy.

Options tests:

- small block size creates multiple data blocks;
- Bloom bits per key affects filter size or behavior;
- table cache capacity limits entries and causes evictions.

Range scan tests:

- empty DB;
- single key;
- inclusive start and exclusive end;
- limit handling;
- scan across MemTable and SSTables;
- tombstones and expired keys are skipped;
- snapshot scan sees the correct historical view.

Multi-level tests:

- newest L0 value wins when files overlap;
- L0 to L1 compaction preserves reads;
- L1 non-overlap invariant is maintained;
- L1 to L2 compaction preserves reads;
- tombstones still hide older values after compaction;
- active snapshots block or constrain compaction.

## Acceptance Criteria

Basic acceptance:

- Corrupted SSTable bytes do not silently return wrong values.
- New options have safe defaults.
- Range scan behavior is documented and tested.

Behavioral acceptance:

- Point lookup, iterator, and scan remain correct after compaction.
- Snapshot, TTL, and tombstone semantics do not regress.
- Manifest recovery restores level information.

## Suggested Pull Request Split

1. Add block checksums and reader validation.
2. Add table format versioning and legacy compatibility.
3. Add configurable table options.
4. Add range scan API.
5. Add level metadata to `FileMeta` and the manifest.
6. Implement L0 to L1 compaction.
7. Implement size-based L1+ compaction.
