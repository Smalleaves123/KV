#pragma once

#include <string>
#include <vector>

#include "kv/common/socket_compat.h"
#include "kv/common/status.h"

namespace kv::net {

class Connection {
 public:
  explicit Connection(platform::SocketHandle fd = platform::kInvalidSocket);
  ~Connection();

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  Connection(Connection&& other) noexcept;
  Connection& operator=(Connection&& other) noexcept;

  bool IsOpen() const noexcept;
  platform::SocketHandle fd() const noexcept;

  Status ReadLine(std::string* line);
  Status ReadRequest(std::vector<std::string>* tokens);
  Status WriteAll(const std::string& data);
  Status Close();

 private:
  platform::SocketHandle fd_;
  std::string read_buffer_;
};

}  // namespace kv::net
