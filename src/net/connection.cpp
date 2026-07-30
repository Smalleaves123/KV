#include "kv/net/connection.h"

#include <utility>

#include "kv/net/codec.h"

namespace kv::net {

Connection::Connection(platform::SocketHandle fd)
    : fd_(fd), read_buffer_() {}

Connection::~Connection() {
  (void)Close();
}

Connection::Connection(Connection&& other) noexcept
    : fd_(other.fd_), read_buffer_(std::move(other.read_buffer_)) {
  other.fd_ = platform::kInvalidSocket;
}

Connection& Connection::operator=(Connection&& other) noexcept {
  if (this != &other) {
    (void)Close();
    fd_ = other.fd_;
    read_buffer_ = std::move(other.read_buffer_);
    other.fd_ = platform::kInvalidSocket;
  }
  return *this;
}

bool Connection::IsOpen() const noexcept {
  return platform::IsValidSocket(fd_);
}

platform::SocketHandle Connection::fd() const noexcept {
  return fd_;
}

Status Connection::ReadLine(std::string* line) {
  if (line == nullptr) {
    return Status::InvalidArgument("line output is null");
  }
  if (!IsOpen()) {
    return Status::IOError("connection is closed");
  }

  if (LineCodec::TryDecodeLine(&read_buffer_, line)) {
    return Status::OK();
  }

  char buf[1024];
  while (true) {
    const platform::SocketIoResult n =
        platform::ReceiveSocket(fd_, buf, sizeof(buf), 0);
    if (n == 0) {
      return Status::NotFound("peer closed");
    }
    if (n < 0) {
      const int error = platform::LastSocketError();
      if (platform::IsRetryableSocketError(error)) {
        continue;
      }
      return Status::IOError("recv failed: " +
                             platform::SocketErrorString(error));
    }

    read_buffer_.append(buf, static_cast<size_t>(n));
    if (LineCodec::TryDecodeLine(&read_buffer_, line)) {
      return Status::OK();
    }
  }
}

Status Connection::ReadRequest(std::vector<std::string>* tokens) {
  if (tokens == nullptr) {
    return Status::InvalidArgument("request output is null");
  }
  if (!IsOpen()) {
    return Status::IOError("connection is closed");
  }

  std::string error;
  RequestDecodeResult result =
      RequestCodec::TryDecode(&read_buffer_, tokens, &error);
  if (result == RequestDecodeResult::kOk) {
    return Status::OK();
  }
  if (result == RequestDecodeResult::kError) {
    return Status::InvalidArgument(error);
  }

  char buf[1024];
  while (true) {
    const platform::SocketIoResult n =
        platform::ReceiveSocket(fd_, buf, sizeof(buf), 0);
    if (n == 0) {
      return Status::NotFound("peer closed");
    }
    if (n < 0) {
      const int error = platform::LastSocketError();
      if (platform::IsRetryableSocketError(error)) {
        continue;
      }
      return Status::IOError("recv failed: " +
                             platform::SocketErrorString(error));
    }

    read_buffer_.append(buf, static_cast<size_t>(n));
    result = RequestCodec::TryDecode(&read_buffer_, tokens, &error);
    if (result == RequestDecodeResult::kOk) {
      return Status::OK();
    }
    if (result == RequestDecodeResult::kError) {
      return Status::InvalidArgument(error);
    }
  }
}

Status Connection::WriteAll(const std::string& data) {
  if (!IsOpen()) {
    return Status::IOError("connection is closed");
  }

  size_t sent = 0;
  while (sent < data.size()) {
    const platform::SocketIoResult n = platform::SendSocket(
        fd_, data.data() + sent, data.size() - sent, 0);
    if (n < 0) {
      if (platform::IsInterruptedSocketError(platform::LastSocketError())) {
        continue;
      }
      return Status::IOError("send failed: " + platform::SocketErrorString(
                                 platform::LastSocketError()));
    }
    if (n == 0) {
      return Status::IOError("send returned 0");
    }
    sent += static_cast<size_t>(n);
  }

  return Status::OK();
}

Status Connection::Close() {
  if (!platform::IsValidSocket(fd_)) {
    return Status::OK();
  }

  if (platform::CloseSocket(fd_) != 0) {
    const int error = platform::LastSocketError();
    fd_ = platform::kInvalidSocket;
    return Status::IOError("close failed: " +
                           platform::SocketErrorString(error));
  }

  fd_ = platform::kInvalidSocket;
  return Status::OK();
}

}  // namespace kv::net
