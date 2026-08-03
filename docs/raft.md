# Raft

KVEngine includes an experimental Raft replication path. It is useful for
learning and integration testing, but it is not production-ready.

## Components

- `RaftNode`: core Raft state machine.
- `RaftLog`: in-memory/logical log management.
- `RaftStorageImpl`: persistent hard-state and log storage support.
- `RaftServer`: RPC listener, tick loop, peer RPC sending, commit apply loop.
- `InstallSnapshot`: checkpoint transfer and follower state-machine restore.
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

Follower reads return an error containing the known leader id. Writes received
by a follower return `-MOVED host:port` when the current leader's client
address is configured, so a RESP client can retry against that node.

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
      client_port: 9527
    - id: 2
      host: 127.0.0.1
      raft_port: 9628
      client_port: 9627
    - id: 3
      host: 127.0.0.1
      raft_port: 9728
      client_port: 9727
  # Optional bootstrap membership. If omitted, all peer ids are members.
  members: [1, 2, 3]
```

Environment format:

```bash
KV_RAFT=1
KV_RAFT_NODE_ID=1
KV_RAFT_PORT=9528
KV_RAFT_DATA_DIR=data/raft/node1
KV_RAFT_PEERS=1:127.0.0.1:9528:9527,2:127.0.0.1:9628:9627,3:127.0.0.1:9728:9727
KV_RAFT_MEMBERS=1,2,3
```

## Local Three-Node Example

Terminal 1:

```bash
KV_RAFT=1 \
KV_RAFT_NODE_ID=1 \
KV_RAFT_PORT=9528 \
KV_RAFT_DATA_DIR=data/raft/node1 \
KV_RAFT_PEERS=1:127.0.0.1:9528:9527,2:127.0.0.1:9628:9627,3:127.0.0.1:9728:9727 \
./build/apps/kv_server 9527 data/db-node1
```

Terminal 2:

```bash
KV_RAFT=1 \
KV_RAFT_NODE_ID=2 \
KV_RAFT_PORT=9628 \
KV_RAFT_DATA_DIR=data/raft/node2 \
KV_RAFT_PEERS=1:127.0.0.1:9528:9527,2:127.0.0.1:9628:9627,3:127.0.0.1:9728:9727 \
./build/apps/kv_server 9627 data/db-node2
```

Terminal 3:

```bash
KV_RAFT=1 \
KV_RAFT_NODE_ID=3 \
KV_RAFT_PORT=9728 \
KV_RAFT_DATA_DIR=data/raft/node3 \
KV_RAFT_PEERS=1:127.0.0.1:9528:9527,2:127.0.0.1:9628:9627,3:127.0.0.1:9728:9727 \
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

If you send a write to a follower, it returns a redirect such as:

```text
-MOVED 127.0.0.1:9527
```

Retry the same request against that client port. A stopped leader is replaced
by a new leader once the remaining two nodes form a majority; restart the old
node with the same `data_dir` and it will receive committed log entries again.

## Metrics

With `server.metrics_port` enabled, `/metrics` exports `kv_raft_role`,
`kv_raft_term`, `kv_raft_voted_for`, `kv_raft_leader_id`,
`kv_raft_commit_index`, `kv_raft_applied_index`, and
`kv_raft_last_log_index`, `kv_raft_snapshot_last_included_index`,
`kv_raft_members`, `kv_raft_member`, and
per-peer replication progress metrics for a server started in Raft mode.

Snapshot transfer uses a bounded, single-message archive intended for small
local clusters. The archive is limited by the Raft RPC frame size; larger
state-machine checkpoints should be split into chunks before production use.

On startup, a node first validates the persisted snapshot metadata, installs
the matching DB checkpoint, and then replays retained committed entries after
the snapshot index. This makes the checkpoint, Raft log boundary, and DB state
recover as one chain. If metadata exists but its checkpoint is missing or
invalid, startup fails instead of silently serving an incomplete database.

Membership changes are submitted by the leader through
`RaftServer::ChangeMembership({node_ids...})`. The configuration command is
committed with the old quorum, applied on every node, and persisted separately
from the log. Add the new node's address to `RaftConfig::peers` on all nodes
before committing the change. Changes are intentionally single-step; issue
one add/remove at a time and keep a majority available during the operation.

## Tests

```bash
ctest --test-dir build -R Raft
ctest --test-dir build -R RaftServerIntegrationTest
```

In restricted environments, listener socket tests may skip.

## Current Limitations

- `RaftServer::CreateSnapshot()` creates a local, reopenable DB checkpoint at
  the latest applied Raft index and compacts the local Raft log. A lagging
  follower can receive and install that checkpoint through InstallSnapshot;
  restart recovery installs the same checkpoint before replaying the log tail.
- Membership changes use single-step reconfiguration rather than joint
  consensus; do not remove multiple members in one command or lose the old
  majority while changing configuration.
- Snapshot transfer currently uses one bounded message rather than a resumable
  chunk stream.
- No production operational tooling.
- Redirects apply to follower writes; follower reads still return the adapter's
  leader error.
- Raft write batches and transactions are not supported by `RaftDBAdapter`.
- The implementation is designed for learning and testing, not production
  deployment.
