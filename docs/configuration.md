# Configuration

`kv_server` can be configured through a YAML-like config file, positional
arguments, and environment variables. The parser is intentionally small and
supports the fields documented here.

## Config File Selection

Default path:

```bash
config/server.yaml
```

Explicit path:

```bash
./build/apps/kv_server --config=config/server.yaml
```

Environment override:

```bash
KV_CONFIG=config/server.yaml ./build/apps/kv_server
```

If both `--config=` and `KV_CONFIG` are provided, `--config=` wins.

## Positional Overrides

After optional `--config=...`, positional arguments can override the client port
and DB path:

```bash
./build/apps/kv_server 9527 data/db
./build/apps/kv_server --config=config/server.yaml 9527 data/db
```

## Server Section

```yaml
server:
  port: 9527
  metrics_port: 9090
  requirepass: ""
  db_path: data/db
```

Fields:

- `port`: TCP client port for line-oriented commands.
- `metrics_port`: optional HTTP port for `/health`, `/ready`, and `/metrics`;
  `0` disables the monitoring server.
- `requirepass`: optional password required by every TCP client before it can
  run commands. An empty value disables authentication.
- `db_path`: directory used to derive default WAL, SST, and manifest paths.

Derived default storage paths:

```text
wal_path      = db_path + "/wal.log"
sst_dir       = db_path + "/sst"
manifest_path = db_path + "/MANIFEST"
```

Environment override:

```bash
KV_METRICS_PORT=9090
KV_REQUIREPASS=secret
```

## Storage Section

```yaml
storage:
  sync_on_write: false
  memtable_write_buffer_size: 4194304
  sstable_block_size_bytes: 4096
  bloom_bits_per_key: 10
  table_cache_capacity: 64
  compaction_min_input_files: 2
  auto_compaction_enabled: true
```

Fields:

- `sync_on_write`: sync WAL writes by default.
- `memtable_write_buffer_size`: flush threshold in bytes.
- `sstable_block_size_bytes`: target SSTable data-block size in bytes.
- `bloom_bits_per_key`: Bloom filter bits allocated per SSTable key.
- `table_cache_capacity`: maximum number of open SSTable readers cached.
- `compaction_min_input_files`: minimum SST file count before compaction.
- `auto_compaction_enabled`: try compaction after flush when the threshold is
  met.

Environment overrides:

```bash
KV_SYNC_ON_WRITE=0
KV_MEMTABLE_WRITE_BUFFER_SIZE=4194304
KV_SSTABLE_BLOCK_SIZE_BYTES=4096
KV_BLOOM_BITS_PER_KEY=10
KV_TABLE_CACHE_CAPACITY=64
KV_COMPACTION_MIN_INPUT_FILES=2
KV_AUTO_COMPACTION=1
```

## Cache Section

```yaml
cache:
  enabled: false
  policy: lru
  capacity: 1024
  ttl_ms: 0
```

Fields:

- `enabled`: enables the latest-read value cache.
- `policy`: `lru`, `lfu`, `shard_lru`, or `slru`.
- `capacity`: maximum number of value-cache entries.
- `ttl_ms`: default cache TTL in milliseconds. `0` means no expiry.

Environment overrides:

```bash
KV_CACHE=1
KV_CACHE_POLICY=lru
KV_CACHE_CAPACITY=4096
KV_CACHE_TTL_MS=0
```

`KV_CACHE_POLICY` accepts:

- `lru`
- `lfu`
- `shard_lru`
- `slru`

## Cluster Section

```yaml
cluster:
  local_node_id: local
  nodes:
    - id: local
      host: 127.0.0.1
      port: 9527
      weight: 1
      alive: true
```

Fields:

- `local_node_id`: node id treated as the local process for routing
  validation. If omitted, `kv_server` defaults it to `local`.
- `nodes`: cluster member list used by the in-process cluster manager for
  routing and status queries.
- `id`: stable node identifier.
- `host`: node host name or IP.
- `port`: client port used by the node.
- `weight`: relative routing weight.
- `alive`: whether the node participates in routing.

Environment override:

```bash
KV_CLUSTER_LOCAL_NODE_ID=local
KV_CLUSTER_NODES=id:local,host:127.0.0.1,port:9527,weight:1,alive:true;id:peer2,host:127.0.0.1,port:9528,weight:1,alive:true
```

`KV_CLUSTER_LOCAL_NODE_ID` sets the local node id used by routing validation.

`KV_CLUSTER_NODES` format:

```text
id:<id>,host:<host>,port:<port>,weight:<weight>,alive:<true|false>;...
```

Notes:

- If no cluster nodes are configured, `kv_server` adds a local node for
  routing and status reporting.
- `cluster_local_node_id` should match the node that is actually serving the
  client connection; `CLUSTER BATCH` rejects requests when this id is missing.
- `CLUSTER PLAN` is a client-side routing helper that groups batch operations
  by node without executing them and returns a fixed RESP schema instead of
  free-form text.
- `CLUSTER BATCH` uses the same partitioning logic internally before deciding
  whether the local node can execute the request.
- The current cluster command surface is node-routing oriented; it is not a
  distributed control plane.

## Raft Section

```yaml
raft:
  enabled: false
  node_id: 1
  raft_port: 9528
  data_dir: data/raft
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

Fields:

- `enabled`: starts the Raft RPC server and wraps DB writes through Raft.
- `node_id`: numeric id of this node.
- `raft_port`: TCP port for Raft RPCs.
- `data_dir`: directory for Raft persistent state.
- `peers`: cluster peer list.

Environment overrides:

```bash
KV_RAFT=1
KV_RAFT_NODE_ID=1
KV_RAFT_PORT=9528
KV_RAFT_DATA_DIR=data/raft/node1
KV_RAFT_PEERS=1:127.0.0.1:9528,2:127.0.0.1:9628,3:127.0.0.1:9728
```

`KV_RAFT_PEERS` format:

```text
node_id:host:raft_port,node_id:host:raft_port
```

## DBOptions Reference

The C++ API uses `DBOptions`:

```cpp
DBOptions options;
options.db_path = "data/db";
options.create_if_missing = true;
options.sync_on_write = false;
options.memtable_write_buffer_size = 4 * 1024 * 1024;
options.sstable_block_size_bytes = 4 * 1024;
options.bloom_bits_per_key = 10;
options.table_cache_capacity = 64;
options.cache_enabled = false;
options.compaction_min_input_files = 2;
options.auto_compaction_enabled = true;
```

Important fields:

- `db_path`: base directory used for default paths.
- `wal_path`: explicit WAL path. If empty, defaults from `db_path`.
- `sst_dir`: explicit SST directory. If empty, defaults from `db_path`.
- `manifest_path`: explicit manifest path. If empty, defaults from `db_path`.
- `create_if_missing`: allow opening a new DB if files are missing.
- `sync_on_write`: fsync-like behavior after each write by default.
- `memtable_write_buffer_size`: flush threshold.
- `sstable_block_size_bytes`, `bloom_bits_per_key`, `table_cache_capacity`:
  SSTable layout, Bloom filter, and open-table cache settings.
- `cache_enabled`, `cache_policy`, `cache_capacity`,
  `cache_default_ttl_ms`: value cache settings.
- `compaction_min_input_files`: minimum SST count before compaction.
- `auto_compaction_enabled`: run compaction automatically after flush when the
  threshold is met.

## Precedence Summary

For `kv_server`:

1. hard-coded defaults;
2. config file;
3. environment variables for cache/storage/Raft/config path;
4. positional port and DB path arguments.

## Limitations

- The config parser is not a full YAML parser.
- Unknown fields are ignored.
- Invalid fields may fall back to defaults unless the parser detects a hard
  error in peer entries.
- Custom WAL, SST, and manifest paths are not exposed directly in
  `server.yaml` yet.
