# Benchmarks And Observability Extension Guide

Benchmarks and observability make storage changes measurable. KVEngine already
has a benchmark directory, cache/read-path stats, compaction stats, and an
optional monitoring endpoint. The next step is to turn those pieces into a
repeatable performance workflow: stable workloads, JSON reports, latency
percentiles, and low-cardinality service metrics.

## Goals

Short-term goals:

- Create reproducible benchmark workloads.
- Emit machine-readable JSON reports.
- Measure per-operation latency.
- Expand the monitoring endpoint with core engine metrics.

Medium-term goals:

- Report p50, p95, and p99 latency.
- Track WAL, flush, compaction, cache, and Bloom filter metrics.
- Add a benchmark comparison script.
- Establish a baseline for performance regression checks.

## Existing Code To Read First

- `bench/`
- `bench/CMakeLists.txt`
- `scripts/run_bench.sh`
- `include/kv/engine/db.h`
- `src/engine/db_impl.cpp`
- `include/kv/cache/cache.h`
- `src/net/monitoring_server.cpp`
- `include/kv/net/monitoring_server.h`
- `src/net/server.cpp`
- `src/net/session.cpp`
- `apps/kv_bench_main.cpp`
- `apps/kv_server.cpp`
- `apps/server_config.cpp`
- `config/server.yaml`
- `docs/benchmark.md`
- `docs/configuration.md`

## Benchmark CLI Shape

Use consistent arguments across benchmark binaries:

```text
--db-path
--workload
--num-ops
--value-size
--key-count
--threads
--sync
--cache
--seed
--report-json
```

Use fixed seeds by default so benchmark behavior is reproducible.

## Recommended Workloads

### Sequential Write

Write ordered keys:

```text
key000000001 -> value
key000000002 -> value
```

This measures:

- WAL append throughput;
- MemTable insertion cost;
- flush cost;
- SSTable build cost.

### Random Write

Write random keys within a fixed keyspace:

```text
key = random(0, key_count)
```

This measures:

- overwrite behavior;
- MemTable pressure;
- compaction effectiveness for duplicate keys.

### Random Read

Preload the DB, then issue random reads.

Run variants:

- cache disabled;
- cache cold;
- cache warm;
- missing-key reads to exercise Bloom filters.

### Mixed Read/Write

Suggested mixes:

- 80% reads, 20% writes;
- 50% reads, 50% writes;
- 95% reads, 5% writes.

This approximates normal service workloads.

### Scan

Measure iterator or range scan behavior:

- small ranges;
- large ranges;
- scans with limit;
- scans across multiple SSTables.

### Compaction Heavy

Use a small MemTable threshold to generate many SSTables quickly.

This measures:

- compaction count;
- write amplification;
- tail latency during compaction;
- read behavior with many files.

## JSON Report Format

Suggested report:

```json
{
  "workload": "random_read",
  "num_ops": 100000,
  "threads": 4,
  "value_size": 128,
  "key_count": 1000000,
  "seed": 12345,
  "duration_ms": 1234,
  "throughput_ops_per_sec": 81037.2,
  "latency_us": {
    "avg": 12.3,
    "p50": 9,
    "p95": 31,
    "p99": 78,
    "max": 1002
  },
  "db_stats": {
    "cache_hits": 0,
    "cache_misses": 0,
    "bloom_queries": 0,
    "bloom_negatives": 0,
    "compactions": 0
  }
}
```

Keep field names stable so future comparison tools can depend on them.

## Latency Percentiles

The first version can store every operation latency in a vector and sort at the
end. This is simple and accurate for moderate workloads.

Later versions can use:

- fixed-bucket histograms;
- HDR histogram;
- reservoir sampling.

Start simple and make the report useful before optimizing memory overhead.

## Metrics To Add

### DB Metrics

- `kv_db_open`
- `kv_db_sequence_current`
- `kv_db_memtable_bytes`
- `kv_db_immutable_memtables`
- `kv_db_sst_files`
- `kv_db_snapshots_active`

### WAL Metrics

- `kv_wal_append_total`
- `kv_wal_append_errors_total`
- `kv_wal_append_latency_seconds`
- `kv_wal_sync_total`
- `kv_wal_sync_latency_seconds`
- `kv_wal_active_segment_id`
- `kv_wal_live_segments`
- `kv_wal_obsolete_segments_deleted_total`

### Flush Metrics

- `kv_flush_total`
- `kv_flush_errors_total`
- `kv_flush_duration_seconds`
- `kv_flush_output_bytes`
- `kv_flush_queue_length`

### Compaction Metrics

- `kv_compaction_total`
- `kv_compaction_errors_total`
- `kv_compaction_duration_seconds`
- `kv_compaction_input_files`
- `kv_compaction_output_files`
- `kv_compaction_input_bytes`
- `kv_compaction_output_bytes`
- `kv_compaction_skipped_snapshot_total`

### Request Metrics

- `kv_request_total{command="GET"}`
- `kv_request_errors_total{command="GET"}`
- `kv_request_duration_seconds{command="GET"}`
- `kv_response_bytes_total{command="GET"}`

Labels must be low-cardinality. Command name is fine. Key name, value, client
address, or arbitrary error strings are not.

### Raft Metrics

- `kv_raft_enabled`
- `kv_raft_role`
- `kv_raft_term`
- `kv_raft_commit_index`
- `kv_raft_applied_index`
- `kv_raft_last_log_index`
- `kv_raft_replication_lag`

## Structured Logging

Add stable structured logs for lifecycle events:

```text
event=db_open db_path=data/db status=ok recovered_wal_records=123 sst_files=5
event=flush_start memtable_bytes=4194304 max_sequence=1000
event=flush_finish file=000123.sst output_bytes=1048576 duration_ms=37
event=compaction_start level=0 input_files=4
event=compaction_finish output_files=1 duration_ms=120
event=wal_rotate old_segment=7 new_segment=8
event=raft_role_change old=follower new=leader term=12
```

The first version can write to stdout/stderr. Later versions can introduce a
logger abstraction.

## Connecting Benchmarks To Metrics

Benchmarks should capture stats before and after a workload:

1. Read DB stats before the workload.
2. Run the workload.
3. Read DB stats after the workload.
4. Store deltas in the JSON report.

This makes it possible to connect latency changes with flushes, compactions,
cache hit rate, and Bloom filter behavior.

## Test Plan

Benchmark tests:

- a tiny workload runs quickly in CI;
- JSON report is parseable;
- required fields are present;
- fixed seed produces stable key sequences.

Metrics tests:

- monitoring endpoint includes expected metric names;
- `GET` and `SET` increment request counters;
- invalid commands increment error counters;
- cache hit/miss stats are visible;
- compaction stats are visible after a forced compaction.

Logging tests:

- avoid brittle full-text log assertions;
- test that major lifecycle events call the logger with required fields if a
  logger abstraction exists.

## Acceptance Criteria

Basic acceptance:

- Benchmarks can run through a script.
- JSON reports are stable and parseable.
- Monitoring exposes core DB, WAL, flush, compaction, and request metrics.

Behavioral acceptance:

- Developers can compare throughput and percentiles before and after a change.
- Metrics labels remain low-cardinality.
- Metrics collection does not significantly slow down normal requests.

## Suggested Pull Request Split

1. Unify benchmark CLI arguments and workload framework.
2. Add latency percentiles and JSON report output.
3. Expand DB internal stats.
4. Expand the monitoring endpoint.
5. Add a benchmark comparison tool.
6. Add detailed WAL, flush, and compaction metrics.
