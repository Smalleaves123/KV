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
  engine/recovery_test.cpp
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

include(GoogleTest)
gtest_discover_tests(kv_memtable_test)
gtest_discover_tests(kv_wal_test)
gtest_discover_tests(kv_engine_test)
