#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-build}"
BUILD_CONFIG="${BUILD_CONFIG:-Debug}"

cmake --build "${BUILD_DIR}" --target clean --config "${BUILD_CONFIG}"
