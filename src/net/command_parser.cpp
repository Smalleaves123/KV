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
  Command cmd;
  cmd.raw = line;

  std::istringstream iss(line);
  std::string head;
  if (!(iss >> head)) {
    cmd.type = CommandType::kInvalid;
    return cmd;
  }

  head = ToUpper(head);

  std::string token;
  while (iss >> token) {
    cmd.args.push_back(token);
  }

  if (head == "PING") cmd.type = CommandType::kPing;
  else if (head == "GET") cmd.type = CommandType::kGet;
  else if (head == "SET") cmd.type = CommandType::kSet;
  else if (head == "DEL") cmd.type = CommandType::kDel;
  else if (head == "MGET") cmd.type = CommandType::kMGet;
  else if (head == "INFO") cmd.type = CommandType::kInfo;
  else if (head == "STATS") cmd.type = CommandType::kStats;
  else if (head == "BEGIN") cmd.type = CommandType::kBegin;
  else if (head == "EXEC") cmd.type = CommandType::kExec;
  else if (head == "ABORT") cmd.type = CommandType::kAbort;
  else cmd.type = CommandType::kInvalid;

  return cmd;
}

}  
