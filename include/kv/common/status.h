#pragma once

#include <string>

namespace kv {

enum class StatusCode {
  kOk = 0,
  kNotFound,
  kInvalidArgument,
  kIOError,
  kCorruption,
  kAlreadyExists,
};

class Status {
 public:
  Status() noexcept;
  Status(StatusCode code, std::string message);

  static Status OK();
  static Status NotFound(std::string message = {});
  static Status InvalidArgument(std::string message = {});
  static Status IOError(std::string message = {});
  static Status Corruption(std::string message = {});
  static Status AlreadyExists(std::string message = {});

  bool ok() const noexcept;
  bool IsNotFound() const noexcept;
  bool IsInvalidArgument() const noexcept;
  bool IsIOError() const noexcept;
  bool IsCorruption() const noexcept;
  bool IsAlreadyExists() const noexcept;

  StatusCode code() const noexcept;
  const std::string& message() const noexcept;
  std::string ToString() const;

 private:
  static const char* CodeAsString(StatusCode code) noexcept;

  StatusCode code_;
  std::string message_;
};

}  // namespace kv