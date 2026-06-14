#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-build}"

cmake --build "${BUILD_DIR}" --target clean
