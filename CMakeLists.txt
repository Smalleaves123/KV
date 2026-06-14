cmake_minimum_required(VERSION 3.16)

project(KVEngine VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(KV_BUILD_TESTS "Build tests" ON)
option(KV_BUILD_EXAMPLES "Build examples" OFF)
option(KV_BUILD_APPS "Build apps" OFF)
option(KV_ENABLE_SANITIZERS "Enable sanitizer instrumentation" OFF)
set(KV_SANITIZERS "address,undefined" CACHE STRING
    "Comma-separated sanitizer list used when KV_ENABLE_SANITIZERS=ON")

if(MSVC)
  add_compile_options(/W4)
else()
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

if(KV_ENABLE_SANITIZERS)
  if(MSVC)
    message(FATAL_ERROR "KV_ENABLE_SANITIZERS is not supported with MSVC")
  endif()
  add_compile_options(-fsanitize=${KV_SANITIZERS} -fno-omit-frame-pointer)
  add_link_options(-fsanitize=${KV_SANITIZERS})
endif()

add_subdirectory(src)

if(KV_BUILD_APPS)
  add_subdirectory(apps)
endif()

if(KV_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
