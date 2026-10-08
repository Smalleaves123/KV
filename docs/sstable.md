# SSTable

SSTables are immutable sorted files created from memtable flushes and
compaction. They are the main persistent read structure after data leaves the
WAL/memtable path.

## Files

- Public headers: `include/kv/sstable/`
- Implementation: `src/sstable/`
- Manifest metadata: `include/kv/version/`, `src/version/`
- Tests: `tests/sstable/`, `tests/engine/compaction_test.cpp`

## File Layout

An SST file contains:

```text
data block(s)
filter block
index block
footer
```

The footer stores block handles for the index and filter blocks, the maximum
sequence number in the file, and a magic value.

## Data Blocks

Data blocks store sorted entries. `BlockBuilder` uses restart points to support
prefix-compressed key encoding. Each logical entry includes:

- user key;
- sequence number;
- value type: value or tombstone;
- value bytes.

`BlockIterator` decodes and seeks within a block.

## Index Block

The index block maps each data block's last key to a block handle:

```text
last_key -> block_offset + block_size
```

`TableReader::Get` binary-searches the index to choose the candidate data block.

## Filter Block

Each SST has one Bloom filter over its keys. On a point lookup:

1. `TableReader` queries the Bloom filter.
2. A negative answer returns `NotFound` without reading the index/data block.
3. A positive answer continues to the index and block lookup.

Bloom filter query and negative counts are exposed through `ReadPathStats`.
Range-pruned lookups are counted separately, so a key rejected by file bounds
does not inflate Bloom-filter statistics.

## Table Cache

`TableCache` is an LRU cache of open `TableReader` instances keyed by file path.
It avoids repeatedly opening the same SST and decoding its footer/index/filter
metadata.

Shared readers synchronize stream seeks and reads, so compaction iterators and
foreground lookups can safely read different blocks of the same SST.

Stats exposed through `ReadPathStats`:

- `table_cache_hits`
- `table_cache_misses`
- `table_cache_evictions`
- `table_cache_entries`
- `range_prunes`

## Read Order

Point reads select the visible entry with the highest sequence number across
candidate SST files, including tombstones and expired values. Publication order
does not determine priority: a compaction may publish older entries after a
concurrent flush. Files whose maximum sequence cannot beat an already-found
version can be skipped.

## Compaction

Compaction uses a streaming k-way merge over sorted SST iterators. It keeps the
newest version per key and preserves tombstones that represent deletes without
materializing every key in a map. The heap holds one entry per input file, so
merge memory is proportional to the number of inputs plus one decoded block per
iterator. The output SST is synced before its manifest add-file record is
synced. Only then are old manifest records and files removed. A crash after the
new record is durable but before old-file removal leaves both generations live;
recovery resolves duplicate entries by sequence number.

Flushes create L0 files. When the L0 trigger is reached, compaction selects all
L0 files and L1 files whose recorded ranges overlap the L0 input range. The
merged output is recorded at L1; non-overlapping L1 files remain intact. Files
with unknown ranges, including legacy Manifest records, are included
conservatively. Point reads use the ranges to skip files that cannot contain the
requested key.

If L1-to-L2 compaction filters out every entry, the empty output is committed
by removing the input manifest records and files without adding an output SST.

Manual compaction:

```bash
./build/apps/kv_admin compact data/db
```

Programmatic compaction:

```cpp
db->Compact();
```

## Range Scans

`DB::Scan` returns a stable, key-ordered view in `[start_key, end_key)`. An
empty `end_key` has no upper bound and `limit == 0` returns every visible key.
The scan uses the same sequence, tombstone, and TTL resolution rules as a DB
iterator.

## Manifest

The manifest is an append-only metadata file that tracks live SST files. It is
used during DB open to recover the SST set without scanning arbitrary files.

If the manifest is missing, the DB can fall back to scanning the SST directory.

### File Metadata

`FileMeta` (also named `ManifestFileMeta`) stores:

- `file_number`, `file_path`, and `max_seq`;
- `file_size`: the complete physical SST size, including filter, index and footer;
- `level`: the intended compaction level, defaulting to L0;
- `smallest_key` and `largest_key`: inclusive bounds over all user keys in the
  file, including tombstones and expired versions.

Flush produces L0 metadata and L0-to-L1 compaction records its output at L1.
`VersionSet` recovery and checkpoint creation/installation preserve all of
these fields. Point reads compare visible entry sequences, with range pruning
for files that have known bounds.

Both key bounds empty means the range is unknown. A range with only one empty
bound or with `smallest_key > largest_key` is invalid. Keys are stored as raw
bytes, so embedded NUL and non-ASCII bytes are preserved.

### Manifest Record Compatibility

All integer fields use little-endian fixed-width encoding. Existing tags keep
their original layout:

```text
1 | file_number:u64 | max_seq:u64 | path_length:u32 | path
2 | file_number:u64
```

Metadata records written by the current implementation use tag 4 and append a
CRC32 over the record body. The previous metadata tag 3 remains readable for
backward compatibility:

```text
3 | file_number:u64 | max_seq:u64 | path_length:u32 | path
  | level:u32 | file_size:u64 | smallest_length:u32 | largest_length:u32
  | smallest_key | largest_key

4 | same body as tag 3 | checksum:u32
```

Remove records written by the current implementation use tag 5:

```text
5 | file_number:u64 | checksum:u32
```

New readers accept mixed old/new records. A legacy add-file record recovers as
L0 with unknown size (`0`) and unknown key bounds; opening a legacy DB does not
rewrite its metadata. A subsequent flush writes tag 3, and full compaction
replaces legacy inputs with output that has complete metadata. Checkpoints
preserve unknown metadata when copying legacy files.

Old binaries cannot read tag 3. This is backward read compatibility, not support
for downgrading a database after the new writer has appended records. SST file
bytes are unchanged by this manifest extension.

Recovery ignores an incomplete trailing record and never publishes a partial
file description. It rejects unknown tags, zero file numbers, invalid key
ranges, paths over 4096 bytes, and individual key bounds over 64 MiB. This
extends the existing manifest recovery rules; it does not add record checksums
or atomic multi-file version edits. A complete tag 4 or tag 5 record with a
checksum mismatch is rejected as corruption; an incomplete trailing checksum
is treated as an undurable tail and ignored.

## Current Limitations

- Only the initial L0-to-L1 policy is implemented; deeper levels and size-based
  picking are still pending.
- Index, filter, and footer checksums are not implemented.
- Compaction runs in the flush worker and is protected by the DB mutex.
