#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-build}"
BUILD_CONFIG="${BUILD_CONFIG:-Debug}"

cmake -S . -B "${BUILD_DIR}" -DKV_BUILD_TESTS=ON
cmake --build "${BUILD_DIR}" --parallel --config "${BUILD_CONFIG}"
ctest --test-dir "${BUILD_DIR}" -C "${BUILD_CONFIG}" --output-on-failure "$@"
