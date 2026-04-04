#include "kv/net/command.h"

#include <filesystem>
#include <memory>
#include <sstream>
#include <string>

#include "gtest/gtest.h"
#include "kv/engine/db.h"
#include "kv/net/session.h"

namespace kv::net {
namespace {

DBOptions MakeDBOptions(const std::string& name) {
  static int counter = 0;
  ++counter;

  DBOptions options;
  std::ostringstream oss;
  oss << "test_tmp/net/" << name << "_" << counter;
  const std::string base = oss.str();

  options.wal_path = base + ".wal";
  options.sst_dir = base + "_sst";
  options.manifest_path = base + ".manifest";
  options.memtable_write_buffer_size = 1ULL << 20;
  return options;
}

void RemovePathIfExists(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove(path, ec);
}

void RemoveDirIfExists(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
}

std::unique_ptr<DB> OpenDBForTest(const std::string& name) {
  DBOptions options = MakeDBOptions(name);
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  Status s = DB::Open(options, &db);
  EXPECT_TRUE(s.ok()) << s.ToString();
  return db;
}

TEST(CommandExecutorTest, Ping) {
  auto db = OpenDBForTest("ping");
  CommandExecutor exec(db.get());

  Command cmd;
  cmd.type = CommandType::kPing;
  EXPECT_EQ(exec.Execute(cmd), "+PONG\r\n");
}

TEST(CommandExecutorTest, SetGetDelFlow) {
  auto db = OpenDBForTest("set_get_del");
  CommandExecutor exec(db.get());

  Command set_cmd{CommandType::kSet, {"name", "alice"}, "SET name alice"};
  EXPECT_EQ(exec.Execute(set_cmd), "+OK\r\n");

  Command get_cmd{CommandType::kGet, {"name"}, "GET name"};
  EXPECT_EQ(exec.Execute(get_cmd), "$5\r\nalice\r\n");

  Command del_cmd{CommandType::kDel, {"name"}, "DEL name"};
  EXPECT_EQ(exec.Execute(del_cmd), "+OK\r\n");

  EXPECT_EQ(exec.Execute(get_cmd), "$-1\r\n");
}

TEST(CommandExecutorTest, MGet) {
  auto db = OpenDBForTest("mget");
  CommandExecutor exec(db.get());

  EXPECT_EQ(exec.Execute(Command{CommandType::kSet, {"k1", "v1"}, ""}), "+OK\r\n");
  EXPECT_EQ(exec.Execute(Command{CommandType::kSet, {"k3", "v3"}, ""}), "+OK\r\n");

  const std::string resp =
      exec.Execute(Command{CommandType::kMGet, {"k1", "k2", "k3"}, ""});
  EXPECT_EQ(resp, "*3\r\n$2\r\nv1\r\n$-1\r\n$2\r\nv3\r\n");
}

TEST(CommandExecutorTest, InvalidCommand) {
  auto db = OpenDBForTest("invalid_cmd");
  CommandExecutor exec(db.get());

  Command cmd;
  cmd.type = CommandType::kInvalid;
  const std::string resp = exec.Execute(cmd);
  EXPECT_EQ(resp, "-ERRunknown command\r\n");
}

TEST(SessionTest, HandleLineParsesAndExecutes) {
  auto db = OpenDBForTest("session");
  Session session(db.get());

  EXPECT_EQ(session.HandleLine("SET a 1"), "+OK\r\n");
  EXPECT_EQ(session.HandleLine("GET a"), "$1\r\n1\r\n");
  EXPECT_EQ(session.HandleLine("GET missing"), "$-1\r\n");
  EXPECT_EQ(session.HandleLine("PING"), "+PONG\r\n");
}

}  // namespace
}  // namespace kv::net
