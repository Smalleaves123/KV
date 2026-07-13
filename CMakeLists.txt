cmake_minimum_required(VERSION 3.16)

project(KVEngine VERSION 0.1.0 LANGUAGES CXX)

if(WIN32)
  add_compile_definitions(_WIN32_WINNT=0x0601 WIN32_LEAN_AND_MEAN)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/ProjectOptions.cmake)
kv_setup_global_options()

option(KV_BUILD_TESTS "Build tests" ON)
option(KV_BUILD_EXAMPLES "Build examples" OFF)
option(KV_BUILD_APPS "Build apps" OFF)
option(KV_BUILD_BENCHMARKS "Build benchmark binaries" OFF)
option(KV_ENABLE_SANITIZERS "Enable sanitizer instrumentation" OFF)
set(KV_SANITIZERS "address,undefined" CACHE STRING
    "Comma-separated sanitizer list used when KV_ENABLE_SANITIZERS=ON")

add_subdirectory(src)
kv_enable_sanitizers(kv)

if(WIN32)
  target_link_libraries(kv PUBLIC ws2_32)
endif()

if(KV_BUILD_APPS)
  add_subdirectory(apps)
endif()

if(KV_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(KV_BUILD_BENCHMARKS)
  add_subdirectory(bench)
endif()

if(KV_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
