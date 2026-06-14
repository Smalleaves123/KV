# Benchmarking

Benchmark support currently includes a small DB benchmark binary and a helper
script. It is intentionally simple and has no third-party benchmark framework
dependency.

## Current State

Available:

- `kv_db_bench` benchmark target;
- `scripts/run_bench.sh`;
- JSON or text output;
- workloads for write, read, mixed, and negative-read scenarios;
- cache/read-path/compaction counters in benchmark output.

## Manual Smoke Benchmark

Run the default mixed workload:

```bash
./scripts/run_bench.sh
```

Run specific workloads:

```bash
WORKLOAD=write OPS=50000 ./scripts/run_bench.sh
WORKLOAD=read OPS=50000 CACHE_FLAG=--cache ./scripts/run_bench.sh
WORKLOAD=negative-read OPS=50000 ./scripts/run_bench.sh
```

Direct binary usage:

```bash
cmake --preset bench
cmake --build --preset bench
./build-bench/bench/kv_db_bench --workload mixed --ops 10000 --cache
```

## Workloads

- `write`: repeated point writes.
- `read`: seeds the DB, then performs point reads.
- `mixed`: seeds the DB, then mixes reads and writes.
- `negative-read`: seeds the DB, then reads missing keys to exercise Bloom
  filter negatives.

Script environment variables:

```bash
BUILD_DIR=build-bench
WORKLOAD=mixed
OPS=10000
VALUE_SIZE=100
READ_PERCENT=80
DB_PATH=test_tmp/bench/db
CACHE_FLAG=--cache
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

## Remaining Benchmark Work

The benchmark system should eventually add:

1. latency histograms;
2. WAL and SST byte counters;
3. compaction timing;
4. comparison support against previous runs;
5. network/server benchmarks.
