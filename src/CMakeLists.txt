add_library(kv
  common/status.cpp
  common/slice.cpp
  memtable/memtable.cpp
  wal/log_record.cpp
  wal/wal_writer.cpp
  wal/wal_reader.cpp
  version/manifest.cpp
  engine/recovery.cpp
  engine/write_batch.cpp
  engine/db.cpp
  engine/db_impl.cpp
)

target_include_directories(kv
  PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_compile_features(kv PUBLIC cxx_std_17)
