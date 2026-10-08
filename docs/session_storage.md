# Session storage

KVEngine supports a session lifecycle over the existing TCP service or the
embedded C++ API: create with an expiry, look up the payload, renew on activity,
and revoke on logout. The application supplies the session token and payload;
the KV service stores them under a `session:` key prefix.

## TCP example

Start the server:

```bash
./build/apps/kv_server --config=config/server.yaml
```

Connect with `nc 127.0.0.1 9527` and send:

```text
SET session:demo-token {"user_id":"alice"} EX 1800
GET session:demo-token
PTTL session:demo-token
PEXPIRE session:demo-token 1800000
DEL session:demo-token
GET session:demo-token
```

Creation returns `OK`. Lookup returns the JSON payload. `PTTL` reports remaining
milliseconds, and renewal returns `1` while preserving the payload. After logout,
lookup returns nil. If authentication is configured, send `AUTH password` first.
RESP array requests preserve binary payloads and work with the same commands.

## Behavior

- `SET key value EX seconds` and `PX milliseconds` store the value and expiry
  atomically in a single WAL record. They also work through the Raft adapter.
- Durations for `SET` must be positive integers. Invalid durations fail before
  changing the existing value. Plain `SET` replaces the value and clears TTL.
- `EXPIRE` and `PEXPIRE` renew a live key. Zero or negative durations revoke it.
  Missing or expired keys return `0` and are not recreated.
- `TTL` reports whole seconds and `PTTL` reports milliseconds. Both return `-1`
  for persistent keys and `-2` for missing or expired keys.
- `DEL` revokes the session. `PERSIST` removes its TTL when permanence is desired.
- Expiry uses an absolute Unix timestamp persisted through WAL, SST files, and
  checkpoints. Restarting does not reset the lifetime. Snapshot reads also obey
  wall-clock expiry.
- Expiry options on `SET` are rejected inside `BEGIN`/`EXEC`; ordinary
  transaction writes remain persistent.

## Embedded example

```cpp
kv::WriteOptions write;
write.sync = true;
write.ttl_ms = 30 * 60 * 1000;
db->Put(write, "session:token", payload);
db->ExpireMillis(kv::WriteOptions{}, "session:token", 30 * 60 * 1000);
int64_t remaining_ms = -2;
db->PTTL(kv::ReadOptions{}, "session:token", &remaining_ms);
```

Build and run the standalone demonstration:

```bash
cmake -S . -B build -DKV_BUILD_EXAMPLES=ON
cmake --build build --target kv_example_session_demo
./build/examples/kv_example_session_demo data/session_demo
```

## Physical cleanup

```yaml
storage:
  expiry_cleanup_interval_ms: 1000
```

Expired sessions immediately disappear from reads. At the configured interval,
the background worker flushes pending mutable data and compacts all current SST
inputs into L2, removing expired values and shadowed history. It can reclaim an
idle database even below ordinary compaction thresholds. Cleanup replaces old
deadlines on renewal and preserves writes published while compaction runs.
Active snapshots and transactions defer the pass until they are released.

The current reclamation pass can rewrite live SST data too; increase the interval
for larger datasets or set it to `0` to disable physical cleanup. Logical expiry
continues to work. WAL storage is reclaimed by the existing segment
rotation/flush policy, and Raft retains its own logs until log compaction.

`KV_EXPIRY_CLEANUP_INTERVAL_MS` overrides the YAML value. Cleanup successes,
failures, bytes, and duration appear in the existing compaction statistics.

In Raft mode the leader chooses the expiry timestamp once, and renewal records
include the payload. This allows a follower that reclaimed its old session while
disconnected to recover the renewed session when replication resumes.
