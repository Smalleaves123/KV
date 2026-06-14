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
  db_path: data/db
```

Fields:

- `port`: TCP client port for line-oriented commands.
- `db_path`: directory used to derive default WAL, SST, and manifest paths.

Derived default storage paths:

```text
wal_path      = db_path + "/wal.log"
sst_dir       = db_path + "/sst"
manifest_path = db_path + "/MANIFEST"
```

## Cache Section

```yaml
cache:
  enabled: false
  capacity: 1024
  ttl_ms: 0
```

Fields:

- `enabled`: enables the latest-read value cache.
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
- `cache_enabled`, `cache_policy`, `cache_capacity`,
  `cache_default_ttl_ms`: value cache settings.
- `compaction_min_input_files`: minimum SST count before compaction.
- `auto_compaction_enabled`: run compaction automatically after flush when the
  threshold is met.

## Precedence Summary

For `kv_server`:

1. hard-coded defaults;
2. config file;
3. environment variables for cache/Raft/config path;
4. positional port and DB path arguments.

## Limitations

- The config parser is not a full YAML parser.
- Unknown fields are ignored.
- Invalid fields may fall back to defaults unless the parser detects a hard
  error in peer entries.
- Some `DBOptions` fields are not exposed directly in `server.yaml` yet.
