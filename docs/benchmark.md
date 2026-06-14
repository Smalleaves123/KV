# Benchmarking

Benchmark support is currently scaffolded but not fully wired. The `bench/`
directory contains placeholder files, and `scripts/run_bench.sh` is currently
empty.

This document describes the intended benchmark plan and gives manual commands
that are useful today.

## Current State

Available:

- unit and integration tests;
- `kv_admin stats` for cache/read/compaction counters;
- server health logs every five seconds;
- CMake build profiles;
- sanitizer builds.

Not yet available:

- a built benchmark target;
- standardized benchmark output;
- reproducible workload generator script.

## Manual Smoke Benchmark

For now, use the CLI or TCP server for small manual checks:

```bash
./scripts/build.sh
./build/apps/kv_server --config=config/server.yaml
```

In another terminal:

```bash
printf "SET a 1\nGET a\nPING\n" | nc 127.0.0.1 9527
```

Inspect DB stats:

```bash
./build/apps/kv_admin stats data/db
```

## Recommended Future Benchmark Targets

Point write workloads:

- sequential keys;
- random keys;
- overwrite-heavy workload;
- delete-heavy workload.

Point read workloads:

- memtable-only reads;
- SST-only reads;
- mixed memtable/SST reads;
- Bloom-filter negative reads;
- cache warm and cold reads.

Mixed workloads:

- 50 percent reads / 50 percent writes;
- 90 percent reads / 10 percent writes;
- transaction conflict workload;
- compaction-under-load workload.

Replication workloads:

- single-node Raft smoke;
- three-node replicated write latency;
- leader failover once supported.

## Metrics To Report

Each benchmark should print:

- operation count;
- elapsed time;
- throughput;
- average latency;
- p50/p95/p99 latency;
- WAL bytes written;
- SST count and bytes;
- compaction count and time;
- cache hit/miss/eviction/expiration counts;
- table cache and Bloom filter read-path stats.

## Suggested Output Format

Prefer machine-readable output:

```json
{
  "name": "read_random_sst_warm_cache",
  "operations": 100000,
  "seconds": 1.23,
  "ops_per_sec": 81300,
  "latency_us": {
    "avg": 12.3,
    "p50": 9.0,
    "p95": 31.0,
    "p99": 80.0
  }
}
```

## Build Profiles

Use Release for throughput numbers:

```bash
BUILD_DIR=build-release BUILD_TYPE=Release ./scripts/build.sh
```

Use sanitizer builds for correctness, not performance:

```bash
BUILD_DIR=build-asan ENABLE_SANITIZERS=ON ./scripts/build.sh
ctest --test-dir build-asan --output-on-failure
```

## Roadmap

The benchmark system should eventually add:

1. real CMake benchmark targets;
2. `scripts/run_bench.sh`;
3. reproducible temporary DB setup and cleanup;
4. JSON output;
5. comparison support against previous runs.
