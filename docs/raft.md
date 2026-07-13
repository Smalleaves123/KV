# Raft

KVEngine includes an experimental Raft replication path. It is useful for
learning and integration testing, but it is not production-ready.

## Components

- `RaftNode`: core Raft state machine.
- `RaftLog`: in-memory/logical log management.
- `RaftStorageImpl`: persistent hard-state and log storage support.
- `RaftServer`: RPC listener, tick loop, peer RPC sending, commit apply loop.
- `RaftDBAdapter`: server-side DB adapter that routes writes through Raft.
- `WriteApplier`: small interface used by Raft to apply committed writes
  without coupling Raft to the full `DB` API.

## Write Flow

When Raft is enabled in `kv_server`:

1. A client sends `SET` or `DEL` to the TCP server.
2. `RaftDBAdapter` checks that the local node is leader.
3. The wrapper encodes the write command.
4. `RaftServer::Propose` appends the command through `RaftNode`.
5. The leader replicates entries with AppendEntries RPC.
6. Once committed, `RaftServer` applies the command to the local DB.
7. Followers apply committed entries to their local DBs.

## Read Flow

`GET` in Raft mode currently requires the node to be leader. The wrapper calls
`LinearizableReadBarrier()` before reading from the local DB.

Follower reads return an error containing the known leader id.

## Command Support In Raft Mode

Supported through the server wrapper:

- `SET`
- `GET`
- `DEL`
- `INFO`
- `STATS`

Not supported in Raft mode:

- write batches;
- transactions.

## Configuration

Example single config shape:

```yaml
raft:
  enabled: true
  node_id: 1
  raft_port: 9528
  data_dir: data/raft/node1
  peers:
    - id: 1
      host: 127.0.0.1
      raft_port: 9528
    - id: 2
      host: 127.0.0.1
      raft_port: 9628
    - id: 3
      host: 127.0.0.1
      raft_port: 9728
```

Environment format:

```bash
KV_RAFT=1
KV_RAFT_NODE_ID=1
KV_RAFT_PORT=9528
KV_RAFT_DATA_DIR=data/raft/node1
KV_RAFT_PEERS=1:127.0.0.1:9528,2:127.0.0.1:9628,3:127.0.0.1:9728
```

## Local Three-Node Example

Terminal 1:

```bash
KV_RAFT=1 \
KV_RAFT_NODE_ID=1 \
KV_RAFT_PORT=9528 \
KV_RAFT_DATA_DIR=data/raft/node1 \
KV_RAFT_PEERS=1:127.0.0.1:9528,2:127.0.0.1:9628,3:127.0.0.1:9728 \
./build/apps/kv_server 9527 data/db-node1
```

Terminal 2:

```bash
KV_RAFT=1 \
KV_RAFT_NODE_ID=2 \
KV_RAFT_PORT=9628 \
KV_RAFT_DATA_DIR=data/raft/node2 \
KV_RAFT_PEERS=1:127.0.0.1:9528,2:127.0.0.1:9628,3:127.0.0.1:9728 \
./build/apps/kv_server 9627 data/db-node2
```

Terminal 3:

```bash
KV_RAFT=1 \
KV_RAFT_NODE_ID=3 \
KV_RAFT_PORT=9728 \
KV_RAFT_DATA_DIR=data/raft/node3 \
KV_RAFT_PEERS=1:127.0.0.1:9528,2:127.0.0.1:9628,3:127.0.0.1:9728 \
./build/apps/kv_server 9727 data/db-node3
```

Watch health logs to find the leader:

```text
[health] ... raft_role=leader raft_leader=1
```

Send writes to the leader's client port:

```bash
printf "SET alpha beta\nGET alpha\n" | nc 127.0.0.1 9527
```

If you send a write to a follower, the server returns an error with the known
leader id.

## Tests

```bash
ctest --test-dir build -R Raft
ctest --test-dir build -R RaftServerIntegrationTest
```

In restricted environments, listener socket tests may skip.

## Current Limitations

- No dynamic membership changes.
- No InstallSnapshot RPC.
- No production operational tooling.
- No client-side redirect protocol beyond an error message.
- Raft write batches and transactions are not supported by `RaftDBAdapter`.
- The implementation is designed for learning and testing, not production
  deployment.
