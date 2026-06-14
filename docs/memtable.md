# MemTable

The memtable stores recent writes in memory before they are flushed to SSTable
files. It is versioned: multiple entries for the same user key can coexist with
different sequence numbers.

## Files

- Public headers: `include/kv/memtable/`
- Implementation: `src/memtable/`
- Tests: `tests/memtable/`

## Entry Model

Each memtable entry contains:

- user key;
- sequence number;
- record type: value or deletion tombstone;
- value bytes.

Entries are ordered so lookup can find the newest version for a key first.
Snapshot reads scan forward until they find a version visible at the requested
sequence.

## Write Flow

After a WAL record is appended, `DBImpl` inserts the same update into the
memtable:

- put: stores key, sequence, and value;
- delete: stores key and sequence with a tombstone type.

The memtable tracks approximate memory usage. When it reaches
`DBOptions::memtable_write_buffer_size`, `DBImpl` flushes it to an SST file.

## Read Flow

For point reads:

1. Seek to the target key.
2. Walk versions for that key.
3. Return the first entry with `entry.seq <= read_seq`.
4. Return `NotFound` if the visible entry is a tombstone or no visible version
   exists.

The memtable is checked before SST files. This makes recent writes visible
immediately and ensures newer memtable versions override older SST versions.

## Flush Flow

When flushing:

1. `DBImpl` iterates all memtable entries in sorted order.
2. `TableBuilder` writes them into a new SST file.
3. A manifest add-file record is appended.
4. The SST file path is added to the in-memory SST list.
5. The memtable is cleared.
6. Automatic compaction may run.

## Snapshots And Tombstones

Snapshots depend on old versions. A key may have several memtable and SST
versions so a snapshot can still read an older value after a newer put or
delete. Delete tombstones are visible records, not immediate physical removal.

## Tuning

`DBOptions::memtable_write_buffer_size` controls flush frequency. Smaller values
create SST files more often and make tests deterministic. Larger values reduce
flush overhead but keep more data in memory and WAL replay.

## Current Limitations

- There is one mutable memtable. Immutable memtables and asynchronous flush are
  not implemented.
- Flush happens synchronously inside the write path.
- Memory accounting is approximate.
