#include "kv/net/connection.h"

#include <cerrno>
#include <cstring>
#include <utility>

#include <sys/socket.h>
#include <unistd.h>

#include "kv/net/codec.h"

namespace kv::net {

Connection::Connection(int fd) : fd_(fd), read_buffer_() {}

Connection::~Connection() {
  (void)Close();
}

Connection::Connection(Connection&& other) noexcept
    : fd_(other.fd_), read_buffer_(std::move(other.read_buffer_)) {
  other.fd_ = -1;
}

Connection& Connection::operator=(Connection&& other) noexcept {
  if (this != &other) {
    (void)Close();
    fd_ = other.fd_;
    read_buffer_ = std::move(other.read_buffer_);
    other.fd_ = -1;
  }
  return *this;
}

bool Connection::IsOpen() const noexcept {
  return fd_ >= 0;
}

int Connection::fd() const noexcept {
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
    const ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
    if (n == 0) {
      return Status::NotFound("peer closed");
    }
    if (n < 0) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        continue;
      }
      return Status::IOError("recv failed: " + std::string(std::strerror(errno)));
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
  if (RequestCodec::TryDecode(&read_buffer_, tokens, &error)) {
    if (!error.empty()) {
      return Status::InvalidArgument(error);
    }
    return Status::OK();
  }

  char buf[1024];
  while (true) {
    const ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
    if (n == 0) {
      return Status::NotFound("peer closed");
    }
    if (n < 0) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        continue;
      }
      return Status::IOError("recv failed: " + std::string(std::strerror(errno)));
    }

    read_buffer_.append(buf, static_cast<size_t>(n));
    if (RequestCodec::TryDecode(&read_buffer_, tokens, &error)) {
      if (!error.empty()) {
        return Status::InvalidArgument(error);
      }
      return Status::OK();
    }
  }
}

Status Connection::WriteAll(const std::string& data) {
  if (!IsOpen()) {
    return Status::IOError("connection is closed");
  }

  size_t sent = 0;
  while (sent < data.size()) {
    const ssize_t n = ::send(fd_, data.data() + sent, data.size() - sent, 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return Status::IOError("send failed: " + std::string(std::strerror(errno)));
    }
    if (n == 0) {
      return Status::IOError("send returned 0");
    }
    sent += static_cast<size_t>(n);
  }

  return Status::OK();
}

Status Connection::Close() {
  if (fd_ < 0) {
    return Status::OK();
  }

  if (::close(fd_) != 0) {
    const int e = errno;
    fd_ = -1;
    return Status::IOError("close failed: " + std::string(std::strerror(e)));
  }

  fd_ = -1;
  return Status::OK();
}

}  // namespace kv::net
