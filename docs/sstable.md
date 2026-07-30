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

## Table Cache

`TableCache` is an LRU cache of open `TableReader` instances keyed by file path.
It avoids repeatedly opening the same SST and decoding its footer/index/filter
metadata.

Stats exposed through `ReadPathStats`:

- `table_cache_hits`
- `table_cache_misses`
- `table_cache_evictions`
- `table_cache_entries`

## Read Order

SST files are searched from newest to oldest. The first visible version for the
target key wins. This preserves overwrite semantics across flushes and
compactions.

## Compaction

Compaction merges live SST files into a new SST file. The current implementation
keeps the newest version per key and preserves tombstones that represent
deletes. The output SST is synced before its manifest add-file record is
synced. Only then are old manifest records and files removed. A crash after the
new record is durable but before old-file removal leaves both generations live;
recovery reads the newer compaction output first, preserving the latest value.

Manual compaction:

```bash
./build/apps/kv_admin compact data/db
```

Programmatic compaction:

```cpp
db->Compact();
```

## Manifest

The manifest is an append-only metadata file that tracks live SST files. It is
used during DB open to recover the SST set without scanning arbitrary files.

If the manifest is missing, the DB can fall back to scanning the SST directory.

## Current Limitations

- No multi-level compaction strategy yet.
- No range-scan API.
- No per-block checksum in SST files.
- WAL truncation after flush/compaction is not implemented.
- Compaction is synchronous and protected by the DB mutex.
