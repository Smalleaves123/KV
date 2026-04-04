#pragma once

#include <string>

#include "kv/net/command.h"

namespace kv::net {

class CommandParser {
 public:
  // 单行命令解析（例如：SET a b）
  static Command ParseLine(const std::string& line);

 private:
  static std::string ToUpper(std::string s);
};

}  // namespace kv::net
