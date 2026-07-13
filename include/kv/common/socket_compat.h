#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#endif

namespace kv::platform {

#ifdef _WIN32
using SocketHandle = SOCKET;
using SocketLength = int;
using SocketIoResult = int;
constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
using SocketHandle = int;
using SocketLength = socklen_t;
using SocketIoResult = ssize_t;
constexpr SocketHandle kInvalidSocket = -1;
#endif

bool IsValidSocket(SocketHandle socket) noexcept;
int LastSocketError() noexcept;
bool IsInterruptedSocketError(int error) noexcept;
bool IsRetryableSocketError(int error) noexcept;
std::string SocketErrorString(int error);

int CloseSocket(SocketHandle socket) noexcept;
int ShutdownSocket(SocketHandle socket) noexcept;
SocketIoResult ReceiveSocket(SocketHandle socket, void* data, size_t size,
                             int flags) noexcept;
SocketIoResult SendSocket(SocketHandle socket, const void* data, size_t size,
                          int flags) noexcept;
int SetSocketOptionInt(SocketHandle socket, int level, int option,
                       int value) noexcept;
int SetReceiveTimeout(SocketHandle socket, int timeout_ms) noexcept;
int SetSendTimeout(SocketHandle socket, int timeout_ms) noexcept;

// On Windows Winsock must be initialized before the first socket call. On
// POSIX this class is a no-op, which keeps call sites platform-independent.
class SocketRuntime {
 public:
  SocketRuntime() = default;
  ~SocketRuntime();

  SocketRuntime(const SocketRuntime&) = delete;
  SocketRuntime& operator=(const SocketRuntime&) = delete;

  bool Start() noexcept;
  void Stop() noexcept;

 private:
  bool started_ = false;
};

}  // namespace kv::platform
