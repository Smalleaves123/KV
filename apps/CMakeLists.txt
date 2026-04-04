add_executable(kv_cli
  kv_cli.cpp
)

target_link_libraries(kv_cli
  PRIVATE
    kv
)

add_executable(kv_admin
  kv_admin.cpp
)

target_link_libraries(kv_admin
  PRIVATE
    kv
)

add_executable(kv_server
  kv_server.cpp
)

target_link_libraries(kv_server
  PRIVATE
    kv
)
