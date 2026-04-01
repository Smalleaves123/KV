cmake_minimum_required(VERSION 3.16)

project(KVEngine VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(KV_BUILD_TESTS "Build tests" ON)
option(KV_BUILD_EXAMPLES "Build examples" OFF)
option(KV_BUILD_APPS "Build apps" OFF)

if(MSVC)
  add_compile_options(/W4)
else()
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

add_subdirectory(src)

if(KV_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()