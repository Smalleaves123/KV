// src/net/session.cpp
#include "kv/net/session.h"

namespace kv::net {

Session::Session(DB* db) : executor_(db) {}

std::string Session::HandleLine(const std::string& line) const {
  const Command cmd = CommandParser::ParseLine(line);
  return executor_.Execute(cmd);
}

}  // namespace kv::net
