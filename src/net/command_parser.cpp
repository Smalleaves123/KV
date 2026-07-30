// src/net/command_parser.cpp
#include "kv/net/command_parser.h"

#include <algorithm>
#include <cctype>
#include <sstream>

namespace kv::net {

std::string CommandParser::ToUpper(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
    return static_cast<char>(std::toupper(c));
  });
  return s;
}

Command CommandParser::ParseLine(const std::string& line) {
  std::istringstream iss(line);
  std::vector<std::string> tokens;
  std::string token;
  while (iss >> token) {
    tokens.push_back(token);
  }
  Command cmd = ParseTokens(tokens);
  cmd.raw = line;
  return cmd;
}

Command CommandParser::ParseTokens(const std::vector<std::string>& tokens) {
  Command cmd;
  if (tokens.empty()) {
    cmd.type = CommandType::kInvalid;
    return cmd;
  }

  std::string head = ToUpper(tokens[0]);
  cmd.raw = tokens[0];
  for (size_t i = 1; i < tokens.size(); ++i) {
    cmd.args.push_back(tokens[i]);
  }

  if (head == "PING") cmd.type = CommandType::kPing;
  else if (head == "GET") cmd.type = CommandType::kGet;
  else if (head == "SET") cmd.type = CommandType::kSet;
  else if (head == "DEL") cmd.type = CommandType::kDel;
  else if (head == "EXPIRE") cmd.type = CommandType::kExpire;
  else if (head == "TTL") cmd.type = CommandType::kTTL;
  else if (head == "PERSIST") cmd.type = CommandType::kPersist;
  else if (head == "MGET") cmd.type = CommandType::kMGet;
  else if (head == "INFO") cmd.type = CommandType::kInfo;
  else if (head == "STATS") cmd.type = CommandType::kStats;
  else if (head == "CLUSTER") cmd.type = CommandType::kCluster;
  else if (head == "BEGIN") cmd.type = CommandType::kBegin;
  else if (head == "EXEC") cmd.type = CommandType::kExec;
  else if (head == "ABORT") cmd.type = CommandType::kAbort;
  else if (head == "SCAN") cmd.type = CommandType::kScan;
  else if (head == "INCR") cmd.type = CommandType::kIncr;
  else if (head == "INCRBY") cmd.type = CommandType::kIncrBy;
  else if (head == "DECR") cmd.type = CommandType::kDecr;
  else if (head == "DECRBY") cmd.type = CommandType::kDecrBy;
  else if (head == "HSET") cmd.type = CommandType::kHSet;
  else if (head == "HGET") cmd.type = CommandType::kHGet;
  else if (head == "HDEL") cmd.type = CommandType::kHDel;
  else if (head == "HEXISTS") cmd.type = CommandType::kHExists;
  else if (head == "HLEN") cmd.type = CommandType::kHLen;
  else if (head == "HGETALL") cmd.type = CommandType::kHGetAll;
  else if (head == "HKEYS") cmd.type = CommandType::kHKeys;
  else if (head == "HVALS") cmd.type = CommandType::kHVals;
  else if (head == "LPUSH") cmd.type = CommandType::kLPush;
  else if (head == "RPUSH") cmd.type = CommandType::kRPush;
  else if (head == "LPOP") cmd.type = CommandType::kLPop;
  else if (head == "RPOP") cmd.type = CommandType::kRPop;
  else if (head == "LLEN") cmd.type = CommandType::kLLen;
  else if (head == "LINDEX") cmd.type = CommandType::kLIndex;
  else if (head == "LRANGE") cmd.type = CommandType::kLRange;
  else if (head == "AUTH") cmd.type = CommandType::kAuth;
  else cmd.type = CommandType::kInvalid;

  return cmd;
}

}  
