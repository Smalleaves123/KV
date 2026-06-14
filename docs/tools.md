# Tools And Operations

This document covers the command-line programs in `apps/` and common local
operations.

## Build Apps

```bash
./scripts/build.sh
```

The apps are placed under `build/apps/`:

```text
build/apps/kv_server
build/apps/kv_cli
build/apps/kv_admin
```

CMake presets are also available:

```bash
cmake --preset debug
cmake --build --preset debug
ctest --preset debug
```

## kv_server

`kv_server` opens a DB and listens for TCP commands.

Default config:

```bash
./build/apps/kv_server --config=config/server.yaml
```

Port and DB path override:

```bash
./build/apps/kv_server 9527 data/db
```

With cache:

```bash
KV_CACHE=1 KV_CACHE_CAPACITY=4096 ./build/apps/kv_server 9527 data/db
```

The process handles `SIGINT` and `SIGTERM`. Stop it with `Ctrl-C`.

Health logs are printed every five seconds:

```text
[health] running=yes port=9527 active_connections=0 total_connections=1 ...
```

## kv_cli

`kv_cli` opens a DB directly. It does not connect to `kv_server`.

```bash
./build/apps/kv_cli data/db
```

Supported CLI commands:

```text
get <key>
set <key> <value>
del <key>
mget <k1> <k2> ...
ping
info
stats
snap create <name>
snap get <name> <key>
snap release <name>
cache
txn begin
txn exec
txn abort
help
quit
```

Example:

```text
kv> set color blue
OK
kv> get color
blue
kv> stats
cache_hit=0 cache_miss=0 cache_evict=0 cache_expire=0
```

Snapshot example:

```text
kv> set name alice
OK
kv> snap create s1
snapshot s1 created
kv> set name bob
OK
kv> snap get s1 name
alice
kv> snap release s1
released s1
```

Transaction example:

```text
kv> txn begin
OK
kv> set k v
OK
kv> txn exec
OK
```

## kv_admin

`kv_admin` inspects and maintains an on-disk DB directory.

Usage:

```bash
./build/apps/kv_admin status <db_path>
./build/apps/kv_admin stats <db_path>
./build/apps/kv_admin compact <db_path>
./build/apps/kv_admin list-sst <db_path>
./build/apps/kv_admin manifest-dump <db_path>
```

### status

Prints derived WAL, manifest, and SST paths plus basic file counts:

```bash
./build/apps/kv_admin status data/db
```

### stats

Opens the DB and prints cache, read-path, and compaction stats:

```bash
./build/apps/kv_admin stats data/db
```

Example fields:

```text
cache.hit
cache.miss
read.table_cache_hits
read.bloom_queries
compact.success
```

### compact

Runs manual compaction:

```bash
./build/apps/kv_admin compact data/db
```

Compaction may fail with `NotFound` if there are not enough SST files, or with
`AlreadyExists` if active snapshots prevent compaction.

### list-sst

Lists `.sst` files under the DB SST directory:

```bash
./build/apps/kv_admin list-sst data/db
```

### manifest-dump

Recovers and prints manifest file metadata:

```bash
./build/apps/kv_admin manifest-dump data/db
```

## Simple TCP Check

With the server running:

```bash
printf "PING\nSET k v\nGET k\n" | nc 127.0.0.1 9527
```

Expected RESP-like output:

```text
+PONG
+OK
$1
v
```

The exact display depends on how your terminal shows CRLF.

RESP array requests support values with spaces:

```bash
printf '*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$11\r\nhello world\r\n' | nc 127.0.0.1 9527
```

## Benchmark Tool

Run the default benchmark:

```bash
./scripts/run_bench.sh
```

Examples:

```bash
WORKLOAD=write OPS=50000 ./scripts/run_bench.sh
WORKLOAD=read OPS=50000 CACHE_FLAG=--cache ./scripts/run_bench.sh
```

## Cleaning

Clean build outputs:

```bash
./scripts/clean.sh
```

Remove runtime test data manually when needed:

```bash
rm -rf test_tmp data/db
```

Use destructive cleanup carefully if a DB contains data you want to keep.
