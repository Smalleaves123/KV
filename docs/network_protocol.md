# Network Protocol

`kv_server` exposes a TCP command protocol. It accepts simple line-oriented
commands and RESP array requests with bulk-string arguments. Responses are
encoded with RESP-like frames.

## Starting The Server

```bash
./build/apps/kv_server --config=config/server.yaml
```

or:

```bash
./build/apps/kv_server 9527 data/db
```

## Request Format

The simplest request format is one command per line:

```text
COMMAND arg1 arg2 ...
```

The server accepts `\n` line endings. Command names are case-insensitive.

Line commands are whitespace-tokenized, so keys and values cannot contain
spaces in this mode.

## RESP Array Request Format

RESP array requests are also accepted:

```text
*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$11\r\nhello world\r\n
```

This is parsed as:

```text
SET "k" "hello world"
```

Only arrays of bulk strings are supported. This is enough for binary-safe keys
and values in the current command set, but it is not a full Redis protocol
implementation.

## Response Format

Responses use RESP-like encodings:

```text
+OK\r\n                 simple string
-ERRmessage\r\n         error
$3\r\nfoo\r\n           bulk string
$-1\r\n                 nil
*2\r\n$1\r\na\r\n...    array
```

## Commands

### PING

```text
PING
```

Response:

```text
+PONG\r\n
```

### SET

```text
SET key value
```

Writes a key.

Response:

```text
+OK\r\n
```

### GET

```text
GET key
```

Returns a bulk string or nil:

```text
$5\r\nvalue\r\n
$-1\r\n
```

### DEL

```text
DEL key
```

Stores a tombstone for the key. Deleting a missing key still returns `OK`.

### MGET

```text
MGET key1 key2 key3
```

Returns a RESP array with one item per key. Missing keys are nil.

### BEGIN

Starts a transaction inside the current connection session.

### EXEC

Commits the active transaction.

### ABORT

Rolls back the active transaction.

### INFO / STATS

Returns a bulk string containing newline-separated metrics:

```text
cache.hit=0
cache.miss=0
cache.evict=0
cache.expire=0
read.table_cache_hits=0
read.table_cache_misses=0
read.table_cache_evictions=0
read.table_cache_entries=0
read.bloom_queries=0
read.bloom_negatives=0
compaction.trigger_attempts=0
compaction.skipped_due_snapshot=0
compaction.skipped_due_threshold=0
compaction.succeeded=0
compaction.failed=0
```

### CLUSTER

Cluster commands are available when the server is started with a configured
cluster manager.

```text
CLUSTER ROUTE key [replica_count]
CLUSTER STATUS
CLUSTER STATUS node_id
CLUSTER BATCH SET key value [SET key value ...] [DEL key ...]
```

`CLUSTER ROUTE` returns the node selected for a key. If `replica_count` is
provided, the command returns up to that many replica nodes in routing order.

`CLUSTER STATUS` returns cluster-wide node counts and a per-node snapshot.
`CLUSTER STATUS node_id` returns a single node entry.

`CLUSTER BATCH` applies a batch of `SET` and `DEL` operations as one write
batch after routing validation. Current behavior is intentionally conservative:

- all keys in the batch must route to the same node;
- that node must match the configured local node id when one is set;
- cross-node batch execution is rejected with an error;
- batch writes are still applied locally through `DB::Write`.

This keeps the command deterministic while the cluster control plane remains
lightweight.

## Transaction Sessions

Transactions are connection-local. A `BEGIN` on one TCP connection does not
affect another connection.

Inside a transaction:

- `SET` and `DEL` are staged.
- `GET` sees staged writes first.
- `EXEC` validates and commits.
- `ABORT` discards staged writes.

If a transaction conflict occurs, the session resets its transaction state.

## Server Stats

The server tracks:

- total connections;
- active connections;
- total requests;
- transaction begin/commit/abort/conflict counts.

These are printed periodically by `kv_server` health logs.

## Raft Mode

When Raft is enabled:

- writes are proposed through Raft and must be sent to the leader;
- reads require the node to be leader and pass a linearizable read barrier;
- write batches and transactions are not supported by the Raft wrapper.

Non-leader nodes return an error containing the known leader id.

## Current Limitations

- RESP request support is limited to arrays of bulk strings.
- No authentication or TLS.
- Values with whitespace require RESP array requests.
- No pipelining contract is documented beyond sequential line handling.
- Error frames currently use `-ERR` followed directly by the message.
- Cluster commands rely on an in-process cluster manager and do not yet
  perform distributed replication or cross-node batch fan-out.
