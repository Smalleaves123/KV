# Idempotency and request deduplication

Use `SET NX` to reserve a request ID exactly once while its key remains live.
Concurrent requests share a single accepted payload. Retrying does not overwrite
that payload or extend its lifetime. This supports duplicate webhook detection,
request acceptance receipts, and retry suppression within a bounded window.

## TCP example

Start `kv_server` and connect using the instructions in
[Network Protocol](network_protocol.md). With authentication enabled, send
`AUTH password` first.

```text
SET idem:tenant-a:create-order:request-42 {"request_hash":"abc","receipt_id":"receipt-42"} NX EX 86400
SET idem:tenant-a:create-order:request-42 {"request_hash":"abc","receipt_id":"different"} NX EX 86400
GET idem:tenant-a:create-order:request-42
PTTL idem:tenant-a:create-order:request-42
```

The first request returns `OK`; the duplicate returns nil. `GET` returns the
first receipt and `PTTL` reports the remaining deduplication window. Options
are case-insensitive; `SET key value EX 86400 NX` is equivalent. RESP bulk-string
requests support binary values and JSON containing spaces.

Scope request IDs by tenant and operation. Store a fingerprint of the request
alongside the receipt, and reject reuse of an ID with different parameters in
the application. For a duplicate, read the existing receipt and return the same
acceptance response. If that read is nil because the window expired in between,
retry the conditional write rather than inventing a previous result.

## Embedded API

```cpp
kv::WriteOptions write;
write.sync = true;
write.ttl_ms = 24LL * 60 * 60 * 1000;
bool inserted = false;
kv::Status status = db->PutIfAbsent(write, request_key, receipt, &inserted);
if (!status.ok()) {
  // Outcome may be uncertain after an I/O error; retry with the same request ID.
} else if (inserted) {
  // This request created the receipt.
} else {
  // Read and replay the existing receipt; validate the stored request fingerprint.
}
```

The `inserted` output is meaningful when the status is OK. `ttl_ms = 0` keeps a
persistent claim; negative TTLs are invalid. A null output pointer is invalid.
Missing, deleted, and expired keys accept a new claim. A rejected claim does
not append a DB write, advance its sequence, change its value, or refresh TTL.
`SET` options are unavailable inside `BEGIN`/`EXEC` transactions.

Build and run the concurrency and restart demonstration:

```bash
cmake -S . -B build -DKV_BUILD_EXAMPLES=ON
cmake --build build --target kv_example_idempotency_demo
./build/examples/kv_example_idempotency_demo data/idempotency_demo
```

## Durability and Raft

Claims use the existing value-plus-absolute-expiry WAL and SST formats, so a
restart preserves both the receipt and the original deadline. Embedded callers
can use `sync = true` for WAL synchronization before success. TCP writes follow
the configured [durability policy](configuration.md); set
`storage.sync_on_write: true` when acceptance must wait for WAL synchronization.
Expired claim files are reclaimed through the
[session expiry cleanup](session_storage.md#physical-cleanup) worker.

In Raft mode send requests to the leader. The leader first commits a read
barrier, then verifies all preceding log entries have been applied. It evaluates
absence and appends the decided write under the same Raft mutex. Concurrent
ordinary writes and requests through different adapter instances share this
ordering. Followers apply the accepted value and absolute expiry directly,
including when a delayed follower has already reclaimed an older claim.
Repeated requests may append Raft barriers but do not append a duplicate value.

## Application boundary

This primitive deduplicates stored claims; it does not atomically commit an
external payment, message delivery, or business database update. If work fails
after reserving a request ID, a retry sees the existing claim. The application
must track pending/completed work durably and use the same request ID in a
downstream idempotent workflow. Do not treat an acceptance receipt as proof that
the external operation completed.

A TTL defines when an ID may be accepted again. Choose a window that covers
the longest retry and processing period. Plain `SET`, `DEL`, and TTL renewal
can change a claim; restrict those operations in the application. This example
does not provide ownership-checked completion, lock release, or fencing tokens.
