set(KV_EXAMPLE_SOURCES
  basic_put_get.cpp
  cache_demo.cpp
  skiplist_demo.cpp
  wal_recovery_demo.cpp
)

foreach(source IN LISTS KV_EXAMPLE_SOURCES)
  get_filename_component(name "${source}" NAME_WE)
  kv_add_executable_if_nonempty("kv_example_${name}"
                                "${CMAKE_CURRENT_SOURCE_DIR}/${source}")
endforeach()
