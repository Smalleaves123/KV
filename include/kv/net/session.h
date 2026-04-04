#pragma once

#include <string>

#include "kv/engine/db.h"
#include "kv/net/command.h"
#include "kv/net/command_parser.h"

namespace kv::net {

class Session {
 public:
  explicit Session(DB* db);

  // 输入一行命令，返回编码后的响应
  std::string HandleLine(const std::string& line) const;

 private:
  CommandExecutor executor_;
};

}  // namespace kv::net
