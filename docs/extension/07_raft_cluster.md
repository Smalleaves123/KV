# Raft And Cluster Completeness Guide

KVEngine already has an experimental Raft replication path: Raft node, log,
storage, server, RPC codec, DB adapter, and cluster routing. To move this from
an educational experiment toward a more complete replicated mode, the project
needs stronger persistence tests, leader redirection, persistent snapshots,
InstallSnapshot RPC, log compaction, failover tests, and operational docs.

This is high-value work, but it is also high-complexity. It is best tackled
after the single-node WAL, recovery, and compaction story is stronger.

## Goals

Short-term goals:

- Strengthen Raft restart and persistence tests.
- Return leader information when followers receive write requests.
- Document how to run a local three-node cluster.
- Expose Raft role, term, commit index, and applied index metrics.

Medium-term goals:

- Implement persistent snapshots.
- Implement InstallSnapshot RPC.
- Implement Raft log compaction.
- Add leader failover and follower catch-up tests.

Long-term goals:

- Membership changes.
- Leader transfer.
- A client-side routing helper.
- Deterministic network partition tests.

## Existing Code To Read First

- `include/kv/raft/raft_node.h`
- `src/raft/raft_node.cpp`
- `include/kv/raft/raft_log.h`
- `src/raft/raft_log.cpp`
- `include/kv/raft/raft_storage.h`
- `include/kv/raft/raft_storage_impl.h`
- `src/raft/raft_storage_impl.cpp`
- `include/kv/raft/raft_rpc.h`
- `include/kv/raft/raft_rpc_codec.h`
- `src/raft/raft_rpc_codec.cpp`
- `include/kv/raft/raft_server.h`
- `src/raft/raft_server.cpp`
- `include/kv/raft/raft_db_adapter.h`
- `src/raft/raft_db_adapter.cpp`
- `include/kv/cluster/`
- `src/cluster/`
- `tests/raft/`
- `tests/cluster/`
- `config/server.yaml`
- `docs/raft.md`

## Keep The Experimental Status Honest

The README describes Raft as experimental. Keep that honesty in the docs until
the implementation supports enough operational behavior.

Document these points clearly:

- whether node restart is supported;
- whether follower catch-up is supported;
- whether snapshots are supported;
- whether dynamic membership changes are supported;
- whether writes must be sent to the leader.

## Persistent Snapshots

### Why Snapshots Matter

Without snapshots, the Raft log grows without bound. New or lagging followers
must replay a large amount of log history to catch up.

### Snapshot Metadata

At minimum:

```cpp
struct RaftSnapshotMeta {
  uint64_t last_included_index = 0;
  uint64_t last_included_term = 0;
};
```

The snapshot also needs state-machine data. For KVEngine, that means a DB
checkpoint at a specific applied index.

### DB Checkpoint Strategy

For the first version, prefer a stop-the-world checkpoint:

1. Pause Raft apply.
2. Flush the DB's current MemTable.
3. Ensure manifest and SSTables are durable.
4. Copy or hard-link the DB data directory into a snapshot directory.
5. Write snapshot metadata.
6. Resume apply.

This is not the fastest approach, but it is clear and testable.

Possible layout:

```text
data/raft/
  snapshot/
    meta
    db/
      MANIFEST
      sst/
```

## InstallSnapshot RPC

Add a request shape like:

```cpp
struct InstallSnapshotRequest {
  uint64_t term = 0;
  uint64_t leader_id = 0;
  uint64_t last_included_index = 0;
  uint64_t last_included_term = 0;
  uint64_t offset = 0;
  std::string data;
  bool done = false;
};
```

The simplest first version may send the entire snapshot in one request. If so,
document that it is intended for small local test clusters.

A stronger version should support chunks:

- `offset`;
- `data`;
- `done`;
- temporary snapshot file;
- atomic rename when complete.

## Snapshot Recovery

On node startup:

1. Read snapshot metadata.
2. Restore the DB checkpoint.
3. Read Raft log entries after the snapshot.
4. Replay those entries through the DB adapter.

Guarantees:

- entries included in the snapshot are not applied twice;
- entries after the snapshot are applied in order;
- applied index and commit index are restored consistently.

## Raft Log Compaction

Once snapshots exist, compact the Raft log:

- remove entries with `index <= last_included_index`;
- preserve `last_included_term` for consistency checks;
- define how term lookup behaves for compacted indexes.

Recommended behavior:

- `Term(last_included_index)` returns `last_included_term`;
- `Term(index < last_included_index)` returns 0 or `NotFound`, and Raft logic
  handles that explicitly.

## Follower Catch-Up

When a leader sees that a follower's `next_index` is at or before the snapshot
index:

1. Send InstallSnapshot.
2. Follower installs the snapshot atomically.
3. Leader updates that follower's match/next index.
4. Normal AppendEntries resumes after the snapshot.

Tests should cover a follower that falls behind after the leader has already
compacted old log entries.

## Leader Redirection

When the network command layer receives a write on a follower, it should return
enough information for the client to retry.

The Raft server should expose:

```cpp
bool IsLeader() const;
std::optional<NodeAddress> GetLeaderAddress() const;
```

The session layer can then either execute the write or return a redirect/error
containing the leader address.

## Membership Changes

This is difficult and should not be the first Raft extension.

Options:

1. Full joint consensus, matching the Raft paper.
2. Static configuration only, requiring a stopped cluster to change members.
3. A restricted controlled add/remove flow, clearly documented as limited.

If the goal is educational completeness, full joint consensus is the best
eventual target. If the goal is a quick demo, static configuration plus honest
documentation is acceptable.

## Operational Documentation

Add a guide for running a local three-node cluster, with commands like:

```bash
KV_RAFT=1 KV_RAFT_NODE_ID=1 KV_RAFT_PORT=9528 \
KV_RAFT_PEERS=1:127.0.0.1:9528,2:127.0.0.1:9628,3:127.0.0.1:9728 \
./build/apps/kv_server 9527 data/node1
```

Then provide equivalent commands for node 2 and node 3.

The guide should explain:

- how to find the leader;
- how to write data;
- how to read data;
- what happens when the leader is killed;
- what follower write requests return.

## Metrics

Expose:

- role;
- term;
- voted_for;
- commit_index;
- applied_index;
- last_log_index;
- snapshot_last_included_index;
- per-peer match_index;
- per-peer next_index;
- per-peer replication lag.

Keep labels bounded by configured peer ids.

## Test Plan

Node-level tests:

- election timeout;
- vote grant/reject;
- stale term step-down;
- AppendEntries conflict truncation;
- commit index advancement;
- compacted-log term lookup.

Storage tests:

- log persists across reopen;
- snapshot metadata persists across reopen;
- compacted old entries are no longer present;
- entries after the snapshot remain present.

Integration tests:

- leader replicates a committed write;
- follower restarts and catches up;
- leader restarts and a new leader is elected;
- lagging follower receives InstallSnapshot;
- leader compacts and follower still catches up;
- follower write returns redirect information.

## Acceptance Criteria

Basic acceptance:

- A local three-node cluster can be started from docs.
- Writes still work after leader change.
- Followers can catch up.
- Committed data survives restart.

Behavioral acceptance:

- Log compaction does not break committed state.
- Snapshot install is atomic; failed install does not leave half-installed
  state visible.
- Follower redirect information is sufficient for a client retry.

## Suggested Pull Request Split

1. Add Raft metrics and leader redirect.
2. Add three-node operational docs and restart tests.
3. Add snapshot metadata persistence.
4. Implement local DB checkpoint snapshots.
5. Implement InstallSnapshot RPC.
6. Implement Raft log compaction.
7. Add follower catch-up integration tests.
8. Evaluate membership changes.
