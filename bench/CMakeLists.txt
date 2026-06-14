add_executable(kv_db_bench
  db_bench.cpp
)

target_link_libraries(kv_db_bench
  PRIVATE
    kv
)

target_include_directories(kv_db_bench
  PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)
