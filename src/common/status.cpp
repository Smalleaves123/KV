#include "kv/common/status.h"

#include <utility>

namespace kv {

Status::Status() noexcept : code_(StatusCode::kOk) {}

Status::Status(StatusCode code, std::string message)
    : code_(code), message_(std::move(message)) {}

Status Status::OK() {
  return Status();
}

Status Status::NotFound(std::string message) {
  return Status(StatusCode::kNotFound, std::move(message));
}

Status Status::InvalidArgument(std::string message) {
  return Status(StatusCode::kInvalidArgument, std::move(message));
}

Status Status::IOError(std::string message) {
  return Status(StatusCode::kIOError, std::move(message));
}

Status Status::Corruption(std::string message) {
  return Status(StatusCode::kCorruption, std::move(message));
}

Status Status::AlreadyExists(std::string message) {
  return Status(StatusCode::kAlreadyExists, std::move(message));
}

bool Status::ok() const noexcept {
  return code_ == StatusCode::kOk;
}

bool Status::IsNotFound() const noexcept {
  return code_ == StatusCode::kNotFound;
}

bool Status::IsInvalidArgument() const noexcept {
  return code_ == StatusCode::kInvalidArgument;
}

bool Status::IsIOError() const noexcept {
  return code_ == StatusCode::kIOError;
}

bool Status::IsCorruption() const noexcept {
  return code_ == StatusCode::kCorruption;
}

bool Status::IsAlreadyExists() const noexcept {
  return code_ == StatusCode::kAlreadyExists;
}

StatusCode Status::code() const noexcept {
  return code_;
}

const std::string& Status::message() const noexcept {
  return message_;
}

std::string Status::ToString() const {
  if (ok()) {
    return "OK";
  }
  if (message_.empty()) {
    return CodeAsString(code_);
  }
  return std::string(CodeAsString(code_)) + ": " + message_;
}

const char* Status::CodeAsString(StatusCode code) noexcept {
  switch (code) {
    case StatusCode::kOk:
      return "OK";
    case StatusCode::kNotFound:
      return "NotFound";
    case StatusCode::kInvalidArgument:
      return "InvalidArgument";
    case StatusCode::kIOError:
      return "IOError";
    case StatusCode::kCorruption:
      return "Corruption";
    case StatusCode::kAlreadyExists:
      return "AlreadyExists";
    default:
      return "Unknown";
  }
}

}  // namespace kv