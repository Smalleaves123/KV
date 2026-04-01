#!/usr/bin/env bash
set -euo pipefail

# ==============================
# KV Storage Engine Project Init
# ==============================

echo "==> Creating KV engine directory structure..."

dirs=(
  "cmake"
  "config"
  "docs"
  "scripts"
  "third_party"
  "assets"
  "data/db"
  "data/wal"
  "data/sst"
  "data/manifest"

  "include/kv/common"
  "include/kv/util"
  "include/kv/concurrency"
  "include/kv/cache"
  "include/kv/memtable"
  "include/kv/wal"
  "include/kv/sstable"
  "include/kv/version"
  "include/kv/engine"
  "include/kv/txn"
  "include/kv/net"
  "include/kv/cluster"
  "include/kv/raft"

  "src/common"
  "src/util"
  "src/concurrency"
  "src/cache"
  "src/memtable"
  "src/wal"
  "src/sstable"
  "src/version"
  "src/engine"
  "src/txn"
  "src/net"
  "src/cluster"
  "src/raft"

  "apps"
  "examples"

  "tests/common"
  "tests/cache"
  "tests/memtable"
  "tests/wal"
  "tests/sstable"
  "tests/engine"
  "tests/txn"
  "tests/net"

  "bench"
  "tools"
)

for d in "${dirs[@]}"; do
  mkdir -p "$d"
done

echo "==> Creating empty source & config files..."

files=(
  "CMakeLists.txt"
  "README.md"
  ".gitignore"
  ".clang-format"
  ".clang-tidy"
  "LICENSE"

  "cmake/ProjectOptions.cmake"
  "cmake/Warnings.cmake"
  "cmake/Sanitizers.cmake"
  "cmake/Dependencies.cmake"

  "config/kv.yaml"
  "config/server.yaml"

  "docs/architecture.md"
  "docs/roadmap.md"
  "docs/wal.md"
  "docs/memtable.md"
  "docs/sstable.md"
  "docs/cache.md"
  "docs/transaction.md"
  "docs/network_protocol.md"
  "docs/benchmark.md"

  "scripts/build.sh"
  "scripts/run_tests.sh"
  "scripts/run_bench.sh"
  "scripts/format.sh"
  "scripts/clean.sh"

  "third_party/.gitkeep"
  "assets/.gitkeep"
  "data/db/.gitkeep"
  "data/wal/.gitkeep"
  "data/sst/.gitkeep"
  "data/manifest/.gitkeep"

  "include/kv/common/status.h"
  "include/kv/common/error.h"
  "include/kv/common/types.h"
  "include/kv/common/options.h"
  "include/kv/common/config.h"
  "include/kv/common/slice.h"
  "include/kv/common/noncopyable.h"
  "include/kv/common/logger.h"
  "include/kv/common/timer.h"
  "include/kv/common/comparator.h"
  "include/kv/common/constants.h"

  "include/kv/util/file_util.h"
  "include/kv/util/string_util.h"
  "include/kv/util/crc32.h"
  "include/kv/util/codec.h"
  "include/kv/util/arena.h"
  "include/kv/util/bloom_filter.h"
  "include/kv/util/snowflake.h"

  "include/kv/concurrency/mutex.h"
  "include/kv/concurrency/rw_lock.h"
  "include/kv/concurrency/spin_lock.h"
  "include/kv/concurrency/thread_pool.h"
  "include/kv/concurrency/blocking_queue.h"
  "include/kv/concurrency/latch.h"

  "include/kv/cache/cache.h"
  "include/kv/cache/lru_cache.h"
  "include/kv/cache/lfu_cache.h"
  "include/kv/cache/shard_lru_cache.h"
  "include/kv/cache/ttl_manager.h"

  "include/kv/memtable/memtable.h"
  "include/kv/memtable/skiplist.h"
  "include/kv/memtable/skiplist_node.h"
  "include/kv/memtable/memtable_iterator.h"

  "include/kv/wal/log_record.h"
  "include/kv/wal/wal_writer.h"
  "include/kv/wal/wal_reader.h"
  "include/kv/wal/wal_manager.h"
  "include/kv/wal/log_recovery.h"

  "include/kv/sstable/block.h"
  "include/kv/sstable/block_builder.h"
  "include/kv/sstable/block_iterator.h"
  "include/kv/sstable/filter_block.h"
  "include/kv/sstable/table_builder.h"
  "include/kv/sstable/table_reader.h"
  "include/kv/sstable/table_cache.h"
  "include/kv/sstable/footer.h"
  "include/kv/sstable/meta_index.h"

  "include/kv/version/file_meta.h"
  "include/kv/version/version.h"
  "include/kv/version/version_set.h"
  "include/kv/version/manifest.h"
  "include/kv/version/compaction.h"

  "include/kv/engine/db.h"
  "include/kv/engine/db_impl.h"
  "include/kv/engine/iterator.h"
  "include/kv/engine/snapshot.h"
  "include/kv/engine/write_batch.h"
  "include/kv/engine/merge_operator.h"
  "include/kv/engine/recovery.h"

  "include/kv/txn/txn.h"
  "include/kv/txn/txn_manager.h"
  "include/kv/txn/lock_manager.h"
  "include/kv/txn/write_batch_with_index.h"

  "include/kv/net/protocol.h"
  "include/kv/net/codec.h"
  "include/kv/net/connection.h"
  "include/kv/net/session.h"
  "include/kv/net/command.h"
  "include/kv/net/command_parser.h"
  "include/kv/net/server.h"

  "include/kv/cluster/node.h"
  "include/kv/cluster/hash_ring.h"
  "include/kv/cluster/router.h"
  "include/kv/cluster/cluster_manager.h"

  "include/kv/raft/raft_node.h"
  "include/kv/raft/raft_log.h"
  "include/kv/raft/raft_rpc.h"
  "include/kv/raft/raft_state.h"
  "include/kv/raft/raft_storage.h"

  "src/CMakeLists.txt"

  "src/common/status.cpp"
  "src/common/config.cpp"
  "src/common/logger.cpp"
  "src/common/timer.cpp"
  "src/common/comparator.cpp"

  "src/util/file_util.cpp"
  "src/util/string_util.cpp"
  "src/util/crc32.cpp"
  "src/util/codec.cpp"
  "src/util/arena.cpp"
  "src/util/bloom_filter.cpp"
  "src/util/snowflake.cpp"

  "src/concurrency/rw_lock.cpp"
  "src/concurrency/thread_pool.cpp"
  "src/concurrency/blocking_queue.cpp"
  "src/concurrency/latch.cpp"

  "src/cache/lru_cache.cpp"
  "src/cache/lfu_cache.cpp"
  "src/cache/shard_lru_cache.cpp"
  "src/cache/ttl_manager.cpp"

  "src/memtable/memtable.cpp"
  "src/memtable/skiplist.cpp"
  "src/memtable/memtable_iterator.cpp"

  "src/wal/log_record.cpp"
  "src/wal/wal_writer.cpp"
  "src/wal/wal_reader.cpp"
  "src/wal/wal_manager.cpp"
  "src/wal/log_recovery.cpp"

  "src/sstable/block.cpp"
  "src/sstable/block_builder.cpp"
  "src/sstable/block_iterator.cpp"
  "src/sstable/filter_block.cpp"
  "src/sstable/table_builder.cpp"
  "src/sstable/table_reader.cpp"
  "src/sstable/table_cache.cpp"
  "src/sstable/footer.cpp"
  "src/sstable/meta_index.cpp"

  "src/version/file_meta.cpp"
  "src/version/version.cpp"
  "src/version/version_set.cpp"
  "src/version/manifest.cpp"
  "src/version/compaction.cpp"

  "src/engine/db.cpp"
  "src/engine/db_impl.cpp"
  "src/engine/snapshot.cpp"
  "src/engine/write_batch.cpp"
  "src/engine/recovery.cpp"

  "src/txn/txn.cpp"
  "src/txn/txn_manager.cpp"
  "src/txn/lock_manager.cpp"
  "src/txn/write_batch_with_index.cpp"

  "src/net/protocol.cpp"
  "src/net/codec.cpp"
  "src/net/connection.cpp"
  "src/net/session.cpp"
  "src/net/command.cpp"
  "src/net/command_parser.cpp"
  "src/net/server.cpp"

  "src/cluster/node.cpp"
  "src/cluster/hash_ring.cpp"
  "src/cluster/router.cpp"
  "src/cluster/cluster_manager.cpp"

  "src/raft/raft_node.cpp"
  "src/raft/raft_log.cpp"
  "src/raft/raft_rpc.cpp"
  "src/raft/raft_state.cpp"
  "src/raft/raft_storage.cpp"

  "apps/CMakeLists.txt"
  "apps/kv_cli.cpp"
  "apps/kv_server.cpp"
  "apps/kv_admin.cpp"
  "apps/kv_bench_main.cpp"

  "examples/basic_put_get.cpp"
  "examples/wal_recovery_demo.cpp"
  "examples/cache_demo.cpp"
  "examples/skiplist_demo.cpp"

  "tests/CMakeLists.txt"
  "tests/common/status_test.cpp"
  "tests/common/codec_test.cpp"
  "tests/common/bloom_filter_test.cpp"
  "tests/cache/lru_cache_test.cpp"
  "tests/cache/lfu_cache_test.cpp"
  "tests/cache/ttl_manager_test.cpp"
  "tests/memtable/skiplist_test.cpp"
  "tests/memtable/memtable_test.cpp"
  "tests/wal/wal_writer_test.cpp"
  "tests/wal/wal_reader_test.cpp"
  "tests/wal/recovery_test.cpp"
  "tests/sstable/block_test.cpp"
  "tests/sstable/table_builder_test.cpp"
  "tests/sstable/table_reader_test.cpp"
  "tests/engine/db_test.cpp"
  "tests/engine/write_batch_test.cpp"
  "tests/engine/snapshot_test.cpp"
  "tests/txn/txn_test.cpp"
  "tests/txn/lock_manager_test.cpp"
  "tests/net/protocol_test.cpp"
  "tests/net/parser_test.cpp"

  "bench/CMakeLists.txt"
  "bench/cache_bench.cpp"
  "bench/wal_bench.cpp"
  "bench/memtable_bench.cpp"
  "bench/sstable_bench.cpp"
  "bench/db_bench.cpp"

  "tools/sst_dump.cpp"
  "tools/wal_dump.cpp"
  "tools/manifest_dump.cpp"
  "tools/gen_test_data.cpp"
)

for f in "${files[@]}"; do
  touch "$f"
done

echo "==> Setting executable permissions for scripts..."
chmod +x \
  scripts/build.sh \
  scripts/run_tests.sh \
  scripts/run_bench.sh \
  scripts/format.sh \
  scripts/clean.sh

echo -e "\n✅ KV project skeleton created successfully!"