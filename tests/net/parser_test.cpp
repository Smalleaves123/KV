#include "kv/net/command_parser.h"

#include "gtest/gtest.h"

namespace kv::net {
namespace {

TEST(CommandParserTest, EmptyLineIsInvalid) {
  Command cmd = CommandParser::ParseLine("");
  EXPECT_EQ(cmd.type, CommandType::kInvalid);
  EXPECT_TRUE(cmd.args.empty());
}

TEST(CommandParserTest, ParsesSetCommand) {
  Command cmd = CommandParser::ParseLine("SET name alice");
  ASSERT_EQ(cmd.type, CommandType::kSet);
  ASSERT_EQ(cmd.args.size(), 2U);
  EXPECT_EQ(cmd.args[0], "name");
  EXPECT_EQ(cmd.args[1], "alice");
}

TEST(CommandParserTest, ParsesCaseInsensitiveCommand) {
  Command cmd = CommandParser::ParseLine("mGeT k1 k2 k3");
  ASSERT_EQ(cmd.type, CommandType::kMGet);
  ASSERT_EQ(cmd.args.size(), 3U);
  EXPECT_EQ(cmd.args[0], "k1");
  EXPECT_EQ(cmd.args[1], "k2");
  EXPECT_EQ(cmd.args[2], "k3");
}

TEST(CommandParserTest, ParsesTTLCommands) {
  EXPECT_EQ(CommandParser::ParseLine("EXPIRE key 10").type,
            CommandType::kExpire);
  EXPECT_EQ(CommandParser::ParseLine("ttl key").type, CommandType::kTTL);
  EXPECT_EQ(CommandParser::ParseLine("PERSIST key").type,
            CommandType::kPersist);
}

TEST(CommandParserTest, ParsesDataTypeCommands) {
  EXPECT_EQ(CommandParser::ParseLine("INCR hits").type,
            CommandType::kIncr);
  EXPECT_EQ(CommandParser::ParseLine("HSET user name Alice").type,
            CommandType::kHSet);
  EXPECT_EQ(CommandParser::ParseLine("HGETALL user").type,
            CommandType::kHGetAll);
  EXPECT_EQ(CommandParser::ParseLine("LPUSH queue item").type,
            CommandType::kLPush);
  EXPECT_EQ(CommandParser::ParseLine("LRANGE queue 0 -1").type,
            CommandType::kLRange);
}

TEST(CommandParserTest, UnknownCommandIsInvalid) {
  Command cmd = CommandParser::ParseLine("NOOP a b");
  EXPECT_EQ(cmd.type, CommandType::kInvalid);
  ASSERT_EQ(cmd.args.size(), 2U);
  EXPECT_EQ(cmd.args[0], "a");
  EXPECT_EQ(cmd.args[1], "b");
}

TEST(CommandParserTest, ParsesTransactionCommands) {
  Command begin = CommandParser::ParseLine("BEGIN");
  EXPECT_EQ(begin.type, CommandType::kBegin);
  EXPECT_TRUE(begin.args.empty());

  Command exec = CommandParser::ParseLine("EXEC");
  EXPECT_EQ(exec.type, CommandType::kExec);
  EXPECT_TRUE(exec.args.empty());

  Command abort = CommandParser::ParseLine("ABORT");
  EXPECT_EQ(abort.type, CommandType::kAbort);
  EXPECT_TRUE(abort.args.empty());
}

TEST(CommandParserTest, ParsesInfoAndStatsCommands) {
  Command info = CommandParser::ParseLine("INFO");
  EXPECT_EQ(info.type, CommandType::kInfo);
  EXPECT_TRUE(info.args.empty());

  Command stats = CommandParser::ParseLine("stats");
  EXPECT_EQ(stats.type, CommandType::kStats);
  EXPECT_TRUE(stats.args.empty());
}

TEST(CommandParserTest, ParsesClusterCommand) {
  Command cmd = CommandParser::ParseLine("CLUSTER route user:1 2");
  ASSERT_EQ(cmd.type, CommandType::kCluster);
  ASSERT_EQ(cmd.args.size(), 3U);
  EXPECT_EQ(cmd.args[0], "route");
  EXPECT_EQ(cmd.args[1], "user:1");
  EXPECT_EQ(cmd.args[2], "2");
}

}  // namespace
}  // namespace kv::net
