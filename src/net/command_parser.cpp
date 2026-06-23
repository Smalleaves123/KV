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
  else if (head == "MGET") cmd.type = CommandType::kMGet;
  else if (head == "INFO") cmd.type = CommandType::kInfo;
  else if (head == "STATS") cmd.type = CommandType::kStats;
  else if (head == "CLUSTER") cmd.type = CommandType::kCluster;
  else if (head == "BEGIN") cmd.type = CommandType::kBegin;
  else if (head == "EXEC") cmd.type = CommandType::kExec;
  else if (head == "ABORT") cmd.type = CommandType::kAbort;
  else cmd.type = CommandType::kInvalid;

  return cmd;
}

}  
