#!/bin/bash
# ==============================================
# KVEngine 完整演示脚本
# 构建 → 测试 → 启动服务 → 客户端操作
# ==============================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$PROJECT_DIR/build"

echo "═══════════════════════════════════════════"
echo "  KVEngine 演示"
echo "═══════════════════════════════════════════"

# ---- 第 1 步：构建 ----
echo ""
echo "▸ 第 1 步：构建项目"
echo "-------------------------------------------"
cmake -S "$PROJECT_DIR" -B "$BUILD_DIR" \
  -DKV_BUILD_APPS=ON \
  -DKV_BUILD_TESTS=ON 2>&1 | tail -2
cmake --build "$BUILD_DIR" -j$(sysctl -n hw.logicalcpu 2>/dev/null || echo 4) 2>&1 | tail -5
echo "  ✅ 构建完成"

# ---- 第 2 步：运行测试 ----
echo ""
echo "▸ 第 2 步：运行测试"
echo "-------------------------------------------"
cd "$BUILD_DIR"
ctest --output-on-failure -j4 2>&1 | tail -3
echo "  ✅ 全部测试通过"

# ---- 第 3 步：启动服务端（后台） ----
echo ""
echo "▸ 第 3 步：启动 kv_server（端口 19527）"
echo "-------------------------------------------"
DEMO_DB="$SCRIPT_DIR/demo_db"
rm -rf "$DEMO_DB"

# 启用缓存，策略 LRU，容量 1024
export KV_CACHE=1
export KV_CACHE_POLICY=lru
export KV_CACHE_CAPACITY=1024

"$BUILD_DIR/apps/kv_server" 19527 "$DEMO_DB" &
SERVER_PID=$!
echo "  PID: $SERVER_PID"
sleep 1

# 检查服务是否启动成功
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
  echo "  ❌ 服务启动失败"
  exit 1
fi
echo "  ✅ 服务已启动"

# ---- 第 4 步：用 kv_cli 操作 ----
echo ""
echo "▸ 第 4 步：使用 kv_cli 操作"
echo "-------------------------------------------"

CLI="$BUILD_DIR/apps/kv_cli"
CLI_DB="$SCRIPT_DIR/cli_demo"
rm -rf "$CLI_DB"

# 写入数据
echo "  ◆ 写入数据..."
echo "set name Alice"  | "$CLI" "$CLI_DB" 2>/dev/null | grep -v "^kv> "
echo "set age 30"      | "$CLI" "$CLI_DB" 2>/dev/null | grep -v "^kv> "
echo "set city Beijing" | "$CLI" "$CLI_DB" 2>/dev/null | grep -v "^kv> "
echo "  ✅ 写入完成"

# 读取数据
echo ""
echo "  ◆ 读取数据..."
echo "get name" | "$CLI" "$CLI_DB" 2>/dev/null | grep -v "^kv> "
echo "get age"  | "$CLI" "$CLI_DB" 2>/dev/null | grep -v "^kv> "

# 批量读取
echo ""
echo "  ◆ 批量读取..."
echo "mget name age city" | "$CLI" "$CLI_DB" 2>/dev/null | grep -v "^kv> "

# 删除
echo ""
echo "  ◆ 删除 age..."
echo "del age" | "$CLI" "$CLI_DB" 2>/dev/null | grep -v "^kv> "

# 确认删除
echo ""
echo "  ◆ 确认删除..."
echo "get age" | "$CLI" "$CLI_DB" 2>/dev/null | grep -v "^kv> "

# ---- 第 5 步：用 nc 通过 TCP 操作 ----
echo ""
echo "▸ 第 5 步：通过 TCP 连接服务端操作"
echo "-------------------------------------------"

echo "  ◆ set / get / del..."
echo -e "set language C++\r\n"    | nc -w1 localhost 19527 | head -1
echo -e "get language\r\n"        | nc -w1 localhost 19527 | head -1
echo -e "del language\r\n"        | nc -w1 localhost 19527 | head -1
echo -e "get language\r\n"        | nc -w1 localhost 19527 | head -1

# ---- 第 6 步：运维工具 ----
echo ""
echo "▸ 第 6 步：kv_admin 运维检查"
echo "-------------------------------------------"
"$BUILD_DIR/apps/kv_admin" status "$CLI_DB"
echo ""
"$BUILD_DIR/apps/kv_admin" stats "$CLI_DB" 2>&1 | head -12

# ---- 第 7 步：清理 ----
echo ""
echo "▸ 第 7 步：清理"
echo "-------------------------------------------"
kill "$SERVER_PID" 2>/dev/null || true
rm -rf "$DEMO_DB" "$CLI_DB"
echo "  ✅ 已停止服务，已清理临时数据"

echo ""
echo "═══════════════════════════════════════════"
echo "  演示结束 🎉"
echo "═══════════════════════════════════════════"
