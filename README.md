# KVEngine

KVEngine is a C++17 educational key-value storage engine. It implements the
core pieces of a log-structured database: write-ahead logging, a versioned
memtable, SSTable files, manifest-based recovery, snapshots, optimistic
transactions, cache layers, a Redis-like TCP command server, and an
experimental Raft replication path.

The project is intentionally compact. It is useful for learning how storage
engines are assembled and for experimenting with durability, compaction,
transactions, caching, and replication without carrying the weight of a
production database.

## What Is Implemented

- Durable point writes through WAL append and replay.
- Versioned in-memory writes through `MemTable`.
- SSTable flush, point lookup, Bloom filters, table-reader cache, and manifest
  recovery.
- Manual and automatic compaction of SST files.
- Snapshot reads by sequence number.
- Optimistic transactions with read/write conflict detection.
- Write batches.
- LRU, LFU, and sharded LRU cache implementations with TTL support.
- TCP command server with Redis-like RESP responses.
- CLI and admin utilities.
- Experimental Raft node/server integration for replicated writes.
- GoogleTest coverage for the storage engine, WAL, SSTable, network layer,
  cache, transactions, data types, cluster routing, and Raft.

## Repository Layout

```text
include/kv/        Public headers
src/engine/        DB implementation, recovery, write batches, snapshots
src/wal/           WAL record format, reader, writer, and replay
src/memtable/      Skiplist-backed versioned memtable
src/sstable/       Block, table builder, table reader, filter block, table cache
src/version/       Manifest and version metadata
src/cache/         LRU, LFU, sharded LRU, TTL manager
src/net/           TCP server, command parser, session, RESP protocol
src/txn/           Transaction manager and lock manager components
src/raft/          Experimental Raft node/server/storage/RPC code
src/data_types/    Counter, hash, and list helpers
apps/              kv_server, kv_cli, kv_admin
tests/             GoogleTest suites
docs/              Detailed subsystem documentation
config/            Example server configuration
```

## Requirements

- CMake 3.16 or newer.
- A C++17 compiler. The project is regularly built with AppleClang.
- Network access on the first test build if GoogleTest is not already present
  in the build directory.

## Build

The default script builds tests and apps into `build/`:

```bash
./scripts/build.sh
```

Useful environment variables:

```bash
BUILD_DIR=build-release BUILD_TYPE=Release ./scripts/build.sh
BUILD_APPS=OFF BUILD_TESTS=ON ./scripts/build.sh
```

Manual CMake usage:

```bash
cmake -S . -B build -DKV_BUILD_TESTS=ON -DKV_BUILD_APPS=ON
cmake --build build --parallel
```

Preset usage:

```bash
cmake --preset debug
cmake --build --preset debug
ctest --preset debug

cmake --preset release
cmake --build --preset release
```

`compile_commands.json` is generated in the build directory for clangd and
static-analysis tools.

## Sanitizer Build

AddressSanitizer and UndefinedBehaviorSanitizer can be enabled with:

```bash
BUILD_DIR=build-asan ENABLE_SANITIZERS=ON SANITIZERS=address,undefined ./scripts/build.sh
ctest --test-dir build-asan --output-on-failure
```

ThreadSanitizer can be tried with a separate build directory:

```bash
BUILD_DIR=build-tsan ENABLE_SANITIZERS=ON SANITIZERS=thread ./scripts/build.sh
```

## Test

Run the full test suite:

```bash
./scripts/run_tests.sh
```

Forward extra CTest arguments:

```bash
./scripts/run_tests.sh -R DBTest
ctest --test-dir build --output-on-failure
```

Some integration tests open loopback sockets. In heavily sandboxed
environments they may skip with an explicit message.

## Run The Server

Build apps first:

```bash
./scripts/build.sh
```

Start the server with the default config:

```bash
./build/apps/kv_server --config=config/server.yaml
```

Equivalent positional form:

```bash
./build/apps/kv_server 9527 data/db
```

The server periodically prints health lines with connection counters,
transaction counters, cache stats, and Raft role information when Raft is
enabled.

## Use The CLI

The CLI opens a local DB directly. It does not connect to `kv_server`.

```bash
./build/apps/kv_cli data/db
```

Example session:

```text
kv> set name alice
OK
kv> get name
alice
kv> snap create s1
snapshot s1 created
kv> set name bob
OK
kv> snap get s1 name
alice
kv> snap release s1
released s1
kv> quit
```

## Admin Tool

`kv_admin` inspects and maintains an on-disk DB path:

```bash
./build/apps/kv_admin status data/db
./build/apps/kv_admin stats data/db
./build/apps/kv_admin compact data/db
./build/apps/kv_admin list-sst data/db
./build/apps/kv_admin manifest-dump data/db
```

## TCP Commands

The TCP server accepts line-oriented commands and responds with RESP-like
frames:

```text
PING
SET key value
GET key
DEL key
MGET key1 key2 key3
BEGIN
EXEC
ABORT
INFO
STATS
```

It also accepts RESP array requests with bulk-string arguments, which allows
values containing spaces:

```text
*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$11\r\nhello world\r\n
```

See [Network Protocol](docs/network_protocol.md) for command details and
response formats.

## Configuration

The default config file is [config/server.yaml](config/server.yaml).

Important sections:

- `server.port`: TCP client port.
- `server.db_path`: DB directory.
- `storage.sync_on_write`, `storage.memtable_write_buffer_size`,
  `storage.compaction_min_input_files`,
  `storage.auto_compaction_enabled`.
- `cache.enabled`, `cache.capacity`, `cache.ttl_ms`.
- `raft.enabled`, `raft.node_id`, `raft.raft_port`, `raft.data_dir`,
  `raft.peers`.

Environment variables override selected settings:

```bash
KV_CONFIG=config/server.yaml
KV_CACHE=1
KV_CACHE_POLICY=lru
KV_CACHE_CAPACITY=4096
KV_CACHE_TTL_MS=0
KV_SYNC_ON_WRITE=0
KV_MEMTABLE_WRITE_BUFFER_SIZE=4194304
KV_COMPACTION_MIN_INPUT_FILES=2
KV_AUTO_COMPACTION=1
KV_RAFT=1
KV_RAFT_NODE_ID=1
KV_RAFT_PORT=9528
KV_RAFT_DATA_DIR=data/raft/node1
KV_RAFT_PEERS=1:127.0.0.1:9528,2:127.0.0.1:9628,3:127.0.0.1:9728
```

## Documentation

- [Architecture](docs/architecture.md)
- [WAL](docs/wal.md)
- [MemTable](docs/memtable.md)
- [SSTable](docs/sstable.md)
- [Cache](docs/cache.md)
- [Transactions And Snapshots](docs/transaction.md)
- [Network Protocol](docs/network_protocol.md)
- [Configuration](docs/configuration.md)
- [Tools And Operations](docs/tools.md)
- [Raft](docs/raft.md)
- [Benchmarking](docs/benchmark.md)
- [Roadmap](docs/roadmap.md)

## Current Limitations

- Range scans are not exposed through the public DB API.
- The command parser is whitespace-token based; values with spaces are not
  supported by the TCP command layer.
- Raft support is experimental. Replicated writes are implemented, but
  production-grade membership changes, snapshots, and operational tooling are
  still future work.
- A basic DB benchmark binary is available, but broader benchmark coverage is
  still planned.
- The project is not yet packaged as an installable library.
