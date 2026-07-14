#pragma once

#include <string>
#include <vector>

#include "kv/engine/db.h"

namespace kv {
class ClusterManager;
}

namespace kv::net {

enum class CommandType {
  kInvalid = 0,
  kPing,
  kGet,
  kSet,
  kDel,
  kExpire,
  kTTL,
  kPersist,
  kMGet,
  kInfo,
  kStats,
  kCluster,
  kBegin,
  kExec,
  kAbort,
  kScan,
};

struct Command {
  CommandType type = CommandType::kInvalid;
  std::vector<std::string> args;  // 不含命令名本身
  std::string raw;
};

class CommandExecutor {
 public:
  explicit CommandExecutor(DB* db, const ClusterManager* cluster_manager = nullptr);

  // 返回协议层响应字符串（已编码）
  std::string Execute(const Command& cmd) const;

 private:
  DB* db_;
  const ClusterManager* cluster_manager_;
};

}  // namespace kv::net
