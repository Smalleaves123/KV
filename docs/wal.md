# Write-Ahead Log

The write-ahead log is the first durability layer in KVEngine. Every committed
write is appended to the WAL before it is applied to the memtable. On restart,
the WAL is replayed to reconstruct records that have not yet been represented
only by SST files.

## Files

- Public headers: `include/kv/wal/`
- Implementation: `src/wal/`
- Tests: `tests/wal/`, `tests/engine/recovery_test.cpp`

When `DBOptions::wal_path` is explicitly set, KVEngine keeps the legacy
single-file WAL behavior. Otherwise, segmented WAL is enabled by default and
uses `DBOptions::wal_dir` or `db_path + "/wal"`. Segment names are fixed-width,
monotonically increasing numeric files ending in `.wal`.

## Record Format

`LogRecordCodec` encodes records as:

```text
checksum:4 | type:1 | seq:8 | key_size:4 | value_size:4 | key | value
```

Fields:

- `checksum`: checksum of the payload after the checksum field.
- `type`: `0` for put, `1` for delete.
- `seq`: monotonically increasing DB sequence number.
- `key_size`: byte length of the key.
- `value_size`: byte length of the value. Delete records require an empty
  value.

The decoder rejects:

- truncated buffers;
- checksum mismatch;
- unknown record types;
- zero sequence numbers;
- delete records with a non-empty value.

## Write Flow

For each put/delete:

1. `DBImpl` chooses the next sequence number.
2. `WALWriter::AppendPut` or `WALWriter::AppendDelete` encodes and appends the
   record.
3. If syncing is required, `WALWriter::Sync` is called.
4. The same sequence is applied to the memtable.

`DBOptions::sync_on_write` enables syncing by default. Individual calls can
also set `WriteOptions::sync`.

## Replay Flow

On open:

1. A legacy WAL is replayed as one file; a segmented WAL replays all segment
   files in numeric order.
2. Put records are applied to the memtable.
3. Delete records are applied as tombstones.
4. The maximum replayed sequence number is returned.
5. `DBImpl` sets `next_seq` to the max sequence from WAL and SST metadata plus
   one.

Replay is used together with SST manifest recovery. SST files supply the max
sequence for flushed data; WAL supplies unflushed data. During a flush, the
SST file is synced before its manifest add-file record is synced. The current
single-file WAL remains available after a successful flush. In segmented mode,
closed segments whose highest sequence is covered by the durable SST and
manifest record are deleted; the active segment is never truncated or deleted.

## Error Handling

Opening a DB with `create_if_missing = false` fails when there is no WAL and no
SST metadata to recover. Recovery ignores an incomplete trailing WAL record,
but checksum mismatches and malformed complete records return `Corruption`.

## Current Limitations

- Group commit is not implemented.
- The WAL format is intentionally simple and not versioned yet.

## Useful Tests

```bash
ctest --test-dir build -R WAL
ctest --test-dir build -R RecoveryTest
```
