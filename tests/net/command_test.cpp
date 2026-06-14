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

TEST(CommandExecutorTest, InfoAndStats) {
  auto db = OpenDBForTest("info_stats");
  CommandExecutor exec(db.get());

  const std::string info = exec.Execute(Command{CommandType::kInfo, {}, "INFO"});
  EXPECT_FALSE(info.empty());
  EXPECT_NE(info.find("cache.hit="), std::string::npos);
  EXPECT_NE(info.find("read.table_cache_hits="), std::string::npos);
  EXPECT_NE(info.find("read.bloom_queries="), std::string::npos);
  EXPECT_NE(info.find("compaction.trigger_attempts="), std::string::npos);

  const std::string stats = exec.Execute(Command{CommandType::kStats, {}, "STATS"});
  EXPECT_FALSE(stats.empty());
  EXPECT_NE(stats.find("cache.miss="), std::string::npos);
  EXPECT_NE(stats.find("read.table_cache_misses="), std::string::npos);
  EXPECT_NE(stats.find("compaction.succeeded="), std::string::npos);
}

TEST(SessionTest, HandleLineParsesAndExecutes) {
  auto db = OpenDBForTest("session");
  Session session(db.get());

  EXPECT_EQ(session.HandleLine("SET a 1"), "+OK\r\n");
  EXPECT_EQ(session.HandleLine("GET a"), "$1\r\n1\r\n");
  EXPECT_EQ(session.HandleLine("GET missing"), "$-1\r\n");
  EXPECT_EQ(session.HandleLine("PING"), "+PONG\r\n");
}

TEST(SessionTest, InfoAndStatsInAndOutTransaction) {
  auto db = OpenDBForTest("session_info_stats");
  Session session(db.get());

  std::string resp = session.HandleLine("INFO");
  EXPECT_NE(resp.find("cache.hit="), std::string::npos);

  EXPECT_EQ(session.HandleLine("BEGIN"), "+OK\r\n");
  resp = session.HandleLine("STATS");
  EXPECT_NE(resp.find("read.bloom_queries="), std::string::npos);
  EXPECT_EQ(session.HandleLine("ABORT"), "+OK\r\n");
}

TEST(SessionTest, TransactionBeginExecFlow) {
  auto db = OpenDBForTest("txn_flow");
  Session session(db.get());

  EXPECT_EQ(session.HandleLine("BEGIN"), "+OK\r\n");
  EXPECT_EQ(session.HandleLine("SET a 1"), "+OK\r\n");
  EXPECT_EQ(session.HandleLine("GET a"), "$1\r\n1\r\n");
  EXPECT_EQ(session.HandleLine("EXEC"), "+OK\r\n");
  EXPECT_EQ(session.HandleLine("GET a"), "$1\r\n1\r\n");
}

TEST(SessionTest, TransactionAbortDropsWrites) {
  auto db = OpenDBForTest("txn_abort");
  Session session(db.get());

  EXPECT_EQ(session.HandleLine("BEGIN"), "+OK\r\n");
  EXPECT_EQ(session.HandleLine("SET k v"), "+OK\r\n");
  EXPECT_EQ(session.HandleLine("ABORT"), "+OK\r\n");
  EXPECT_EQ(session.HandleLine("GET k"), "$-1\r\n");
}

TEST(SessionTest, TransactionProtocolErrors) {
  auto db = OpenDBForTest("txn_errors");
  Session session(db.get());

  EXPECT_EQ(session.HandleLine("EXEC"), "-ERRno active transaction\r\n");
  EXPECT_EQ(session.HandleLine("ABORT"), "-ERRno active transaction\r\n");
  EXPECT_EQ(session.HandleLine("BEGIN"), "+OK\r\n");
  EXPECT_EQ(session.HandleLine("BEGIN"), "-ERRtransaction already active\r\n");
  EXPECT_EQ(session.HandleLine("EXEC"), "+OK\r\n");
  EXPECT_EQ(session.HandleLine("EXEC"), "-ERRno active transaction\r\n");
}

TEST(SessionTest, TransactionConflictResetsSessionState) {
  auto db = OpenDBForTest("txn_conflict");
  Session s1(db.get());
  Session s2(db.get());

  EXPECT_EQ(s1.HandleLine("BEGIN"), "+OK\r\n");
  EXPECT_EQ(s2.HandleLine("BEGIN"), "+OK\r\n");
  EXPECT_EQ(s1.HandleLine("SET k v1"), "+OK\r\n");
  EXPECT_EQ(s2.HandleLine("SET k v2"), "+OK\r\n");
  EXPECT_EQ(s2.HandleLine("EXEC"), "+OK\r\n");

  EXPECT_EQ(s1.HandleLine("EXEC"), "-ERRtransaction conflict\r\n");
  EXPECT_EQ(s1.HandleLine("EXEC"), "-ERRno active transaction\r\n");
}

TEST(SessionTest, SessionDestructorRollsBackActiveTransaction) {
  auto db = OpenDBForTest("txn_destructor_rollback");
  {
    Session session(db.get());
    EXPECT_EQ(session.HandleLine("BEGIN"), "+OK\r\n");
    EXPECT_EQ(session.HandleLine("SET ghost 1"), "+OK\r\n");
  }

  Session reader(db.get());
  EXPECT_EQ(reader.HandleLine("GET ghost"), "$-1\r\n");
}

}  // namespace
}  // namespace kv::net
