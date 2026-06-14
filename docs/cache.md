# Cache

KVEngine has two cache layers:

1. an optional value cache for latest reads from SST files;
2. an always-present table-reader cache for SST metadata and file handles.

## Value Cache

The value cache is controlled by `DBOptions`:

```cpp
DBOptions options;
options.cache_enabled = true;
options.cache_policy = CachePolicy::kLRU;
options.cache_capacity = 1024;
options.cache_default_ttl_ms = 0;
```

Supported policies:

- `CachePolicy::kLRU`
- `CachePolicy::kLFU`
- `CachePolicy::kShardLRU`

The cache stores latest-version values read from SST files. Snapshot reads do
not use the value cache because a cached latest value may not match an older
snapshot sequence.

## TTL

`cache_default_ttl_ms` controls default expiration:

- `0`: no expiration;
- positive value: entries expire after that many milliseconds;
- non-positive values passed to individual cache operations mean no expiration.

## Value Cache Stats

`GetCacheStats` returns:

- `hit`
- `miss`
- `evict`
- `expire`

The TCP `INFO`/`STATS` commands and `kv_admin stats` expose these values.

## Table Cache

`TableCache` caches `TableReader` objects. This avoids repeatedly reopening SST
files and rereading their footer/index/filter metadata.

Read path stats include:

- `table_cache_hits`
- `table_cache_misses`
- `table_cache_evictions`
- `table_cache_entries`

## Bloom Filter Stats

SSTable Bloom filter counters are also exposed through `ReadPathStats`:

- `bloom_queries`: number of SST Bloom filter checks;
- `bloom_negatives`: number of checks that rejected a key.

A high negative ratio usually means Bloom filters are avoiding unnecessary data
block reads.

## Server Configuration

`config/server.yaml`:

```yaml
cache:
  enabled: true
  capacity: 4096
  ttl_ms: 0
```

Environment overrides:

```bash
KV_CACHE=1
KV_CACHE_POLICY=lru
KV_CACHE_CAPACITY=4096
KV_CACHE_TTL_MS=0
```

`KV_CACHE_POLICY` accepts `lru`, `lfu`, `shard_lru`, or `slru`.

## Current Limitations

- The value cache is keyed by user key only and is used only for latest reads.
- Writes do not proactively invalidate cached SST values in every path.
- Cache capacity is entry-count based, not byte based.
- Table cache capacity is currently fixed inside `DBImpl`.
