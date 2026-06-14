#pragma once

#include <string>
#include <vector>

#include "kv/common/status.h"

namespace kv::net {

class Connection {
 public:
  explicit Connection(int fd = -1);
  ~Connection();

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  Connection(Connection&& other) noexcept;
  Connection& operator=(Connection&& other) noexcept;

  bool IsOpen() const noexcept;
  int fd() const noexcept;

  Status ReadLine(std::string* line);
  Status ReadRequest(std::vector<std::string>* tokens);
  Status WriteAll(const std::string& data);
  Status Close();

 private:
  int fd_;
  std::string read_buffer_;
};

}  // namespace kv::net
