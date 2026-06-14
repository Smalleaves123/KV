# Transactions And Snapshots

KVEngine supports snapshots and optimistic transactions. Both are based on DB
sequence numbers.

## Snapshots

A snapshot captures the latest committed sequence at the time it is created:

```cpp
const Snapshot* snap = db->GetSnapshot();

ReadOptions ro;
ro.snapshot = snap;

std::string value;
Status s = db->Get(ro, "key", &value);

db->ReleaseSnapshot(snap);
```

Snapshot reads see only versions with `seq <= snapshot.sequence()`.

## Snapshot Rules

- A snapshot must be released with `ReleaseSnapshot`.
- Releasing `nullptr` or an unknown snapshot returns `InvalidArgument`.
- Compaction is rejected or skipped while snapshots are active.
- Snapshot reads do not use the value cache.

## Optimistic Transactions

Transactions use optimistic concurrency control. They do not lock keys during
the whole transaction. Instead, they validate at commit time.

```cpp
std::unique_ptr<Transaction> txn;
Status s = db->BeginTransaction(TxnOptions{}, &txn);

txn->Put("name", "alice");
std::string value;
txn->Get("name", &value);  // read-own-write

s = txn->Commit();
```

## Transaction Flow

1. `BeginTransaction` records `start_seq`.
2. Reads use the `start_seq` view.
3. Local writes are stored in the transaction object and shadow DB reads.
4. Commit validates:
   - every key in the read set;
   - every key in the write set.
5. If any validated key changed after `start_seq`, commit returns a conflict.
6. If validation succeeds, writes are appended to the WAL and memtable.

## Transaction Options

```cpp
TxnOptions options;
options.sync_on_commit = true;
```

`sync_on_commit` syncs the WAL after transaction writes are appended.

## CLI Transaction Commands

The CLI maps transaction commands to the same session behavior used by the
server:

```text
kv> txn begin
OK
kv> set a 1
OK
kv> txn exec
OK
```

Server commands:

```text
BEGIN
SET a 1
GET a
EXEC
```

Abort:

```text
ABORT
```

## Conflict Example

Transaction A starts at sequence 10 and reads `k`. Transaction B commits a write
to `k` at sequence 11. When A commits, validation sees that `k` changed after
sequence 10 and returns a conflict.

## Raft Limitation

When Raft is enabled in `kv_server`, write batches and transactions are not
supported through the Raft wrapper. Single-key `SET`, `GET`, and `DEL` are the
intended command path.

## Current Limitations

- No rollback log is needed because uncommitted writes are kept in the
  transaction object.
- No long-running lock manager is used for optimistic transactions.
- No explicit isolation-level setting beyond the current serializable-style
  validation.
- Active snapshots block compaction, so leaked snapshots can cause SST growth.
