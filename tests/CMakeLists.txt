include(FetchContent)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(kv_memtable_test
  memtable/skiplist_test.cpp
  memtable/memtable_test.cpp
)

target_link_libraries(kv_memtable_test
  PRIVATE
    kv
    GTest::gtest_main
)

target_include_directories(kv_memtable_test
  PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

add_executable(kv_wal_test
  wal/log_record_test.cpp
  wal/wal_writer_test.cpp
  wal/wal_reader_test.cpp
)

target_link_libraries(kv_wal_test
  PRIVATE
    kv
    GTest::gtest_main
)

target_include_directories(kv_wal_test
  PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

add_executable(kv_engine_test
  engine/db_test.cpp
  engine/cache_integration_test.cpp
  engine/compaction_test.cpp
  engine/write_batch_test.cpp
  engine/recovery_test.cpp
  engine/transaction_test.cpp
)

target_link_libraries(kv_engine_test
  PRIVATE
    kv
    GTest::gtest_main
)

target_include_directories(kv_engine_test
  PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

add_executable(kv_version_test
  version/manifest_test.cpp
  version/compaction_test.cpp
  version/version_test.cpp
)

target_link_libraries(kv_version_test
  PRIVATE
    kv
    GTest::gtest_main
)

target_include_directories(kv_version_test
  PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

add_executable(kv_net_test
  net/parser_test.cpp
  net/protocol_test.cpp
  net/command_test.cpp
  net/codec_test.cpp
  net/connection_test.cpp
  net/server_integration_test.cpp
)

target_link_libraries(kv_net_test
  PRIVATE
    kv
    GTest::gtest_main
)

target_include_directories(kv_net_test
  PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

add_executable(kv_cache_test
  cache/bloom_filter_test.cpp
  cache/lru_cache_test.cpp
  cache/lfu_cache_test.cpp
  cache/shard_lru_cache_test.cpp
  cache/ttl_manager_test.cpp
)

target_link_libraries(kv_cache_test
  PRIVATE
    kv
    GTest::gtest_main
)

target_include_directories(kv_cache_test
  PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

add_executable(kv_cluster_test
  cluster/cluster_manager_test.cpp
)

target_link_libraries(kv_cluster_test
  PRIVATE
    kv
    GTest::gtest_main
)

target_include_directories(kv_cluster_test
  PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

add_executable(kv_txn_test
  txn/lock_manager_test.cpp
  txn/txn_test.cpp
)

target_link_libraries(kv_txn_test
  PRIVATE
    kv
    GTest::gtest_main
)

target_include_directories(kv_txn_test
  PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

add_executable(kv_raft_test
  raft/raft_log_test.cpp
  raft/raft_node_test.cpp
  raft/raft_server_integration_test.cpp
)

target_link_libraries(kv_raft_test
  PRIVATE
    kv
    GTest::gtest_main
)

target_include_directories(kv_raft_test
  PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

add_executable(kv_data_types_test
  data_types/counter_test.cpp
  data_types/hash_test.cpp
  data_types/list_test.cpp
)

target_link_libraries(kv_data_types_test
  PRIVATE
    kv
    GTest::gtest_main
)

target_include_directories(kv_data_types_test
  PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

include(GoogleTest)
gtest_discover_tests(kv_memtable_test)
gtest_discover_tests(kv_wal_test)
gtest_discover_tests(kv_engine_test)
gtest_discover_tests(kv_version_test)
gtest_discover_tests(kv_net_test)
gtest_discover_tests(kv_cache_test)
gtest_discover_tests(kv_cluster_test)
gtest_discover_tests(kv_txn_test)
gtest_discover_tests(kv_raft_test)
gtest_discover_tests(kv_data_types_test)
