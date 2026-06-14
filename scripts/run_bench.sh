#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-build-bench}"
WORKLOAD="${WORKLOAD:-mixed}"
OPS="${OPS:-10000}"
VALUE_SIZE="${VALUE_SIZE:-100}"
READ_PERCENT="${READ_PERCENT:-80}"
DB_PATH="${DB_PATH:-test_tmp/bench/db}"
CACHE_FLAG="${CACHE_FLAG:-}"

cmake -S . -B "${BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DKV_BUILD_TESTS=OFF \
  -DKV_BUILD_APPS=OFF \
  -DKV_BUILD_BENCHMARKS=ON

cmake --build "${BUILD_DIR}" --parallel

"${BUILD_DIR}/bench/kv_db_bench" \
  --db "${DB_PATH}" \
  --workload "${WORKLOAD}" \
  --ops "${OPS}" \
  --value-size "${VALUE_SIZE}" \
  --read-percent "${READ_PERCENT}" \
  ${CACHE_FLAG}
