#pragma once

#include <string>
#include <vector>

#include "kv/engine/db.h"

namespace kv::net {

enum class CommandType {
  kInvalid = 0,
  kPing,
  kGet,
  kSet,
  kDel,
  kMGet,
};

struct Command {
  CommandType type = CommandType::kInvalid;
  std::vector<std::string> args;  // 不含命令名本身
  std::string raw;
};

class CommandExecutor {
 public:
  explicit CommandExecutor(DB* db);

  // 返回协议层响应字符串（已编码）
  std::string Execute(const Command& cmd) const;

 private:
  DB* db_;
};

}  // namespace kv::net
