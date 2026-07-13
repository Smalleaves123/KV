#include "kv/common/socket_compat.h"

#include <cstring>
#include <mutex>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/time.h>
#include <unistd.h>
#endif

namespace kv::platform {
namespace {

std::mutex g_runtime_mu;
size_t g_runtime_refs = 0;

}  // namespace

bool IsValidSocket(SocketHandle socket) noexcept {
  return socket != kInvalidSocket;
}

int LastSocketError() noexcept {
#ifdef _WIN32
  return WSAGetLastError();
#else
  return errno;
#endif
}

bool IsInterruptedSocketError(int error) noexcept {
#ifdef _WIN32
  return error == WSAEINTR;
#else
  return error == EINTR;
#endif
}

bool IsRetryableSocketError(int error) noexcept {
#ifdef _WIN32
  return error == WSAEINTR || error == WSAEWOULDBLOCK ||
         error == WSAEINPROGRESS || error == WSAETIMEDOUT;
#else
  return error == EINTR || error == EAGAIN || error == EWOULDBLOCK;
#endif
}

std::string SocketErrorString(int error) {
#ifdef _WIN32
  return "winsock error " + std::to_string(error);
#else
  return std::strerror(error);
#endif
}

int CloseSocket(SocketHandle socket) noexcept {
  if (!IsValidSocket(socket)) {
    return 0;
  }
#ifdef _WIN32
  return ::closesocket(socket);
#else
  return ::close(socket);
#endif
}

int ShutdownSocket(SocketHandle socket) noexcept {
  if (!IsValidSocket(socket)) {
    return 0;
  }
#ifdef _WIN32
  return ::shutdown(socket, SD_BOTH);
#else
  return ::shutdown(socket, SHUT_RDWR);
#endif
}

SocketIoResult ReceiveSocket(SocketHandle socket, void* data, size_t size,
                             int flags) noexcept {
#ifdef _WIN32
  return ::recv(socket, static_cast<char*>(data), static_cast<int>(size),
                flags);
#else
  return ::recv(socket, data, size, flags);
#endif
}

SocketIoResult SendSocket(SocketHandle socket, const void* data, size_t size,
                          int flags) noexcept {
#ifdef _WIN32
  return ::send(socket, static_cast<const char*>(data), static_cast<int>(size),
                flags);
#else
  return ::send(socket, data, size, flags);
#endif
}

int SetSocketOptionInt(SocketHandle socket, int level, int option,
                       int value) noexcept {
#ifdef _WIN32
  return ::setsockopt(socket, level, option,
                      reinterpret_cast<const char*>(&value), sizeof(value));
#else
  return ::setsockopt(socket, level, option, &value, sizeof(value));
#endif
}

int SetReceiveTimeout(SocketHandle socket, int timeout_ms) noexcept {
#ifdef _WIN32
  const DWORD value = static_cast<DWORD>(timeout_ms);
  return ::setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO,
                      reinterpret_cast<const char*>(&value), sizeof(value));
#else
  timeval value{};
  value.tv_sec = timeout_ms / 1000;
  value.tv_usec = (timeout_ms % 1000) * 1000;
  return ::setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &value, sizeof(value));
#endif
}

int SetSendTimeout(SocketHandle socket, int timeout_ms) noexcept {
#ifdef _WIN32
  const DWORD value = static_cast<DWORD>(timeout_ms);
  return ::setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO,
                      reinterpret_cast<const char*>(&value), sizeof(value));
#else
  timeval value{};
  value.tv_sec = timeout_ms / 1000;
  value.tv_usec = (timeout_ms % 1000) * 1000;
  return ::setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &value, sizeof(value));
#endif
}

SocketRuntime::~SocketRuntime() { Stop(); }

bool SocketRuntime::Start() noexcept {
  if (started_) {
    return true;
  }

  std::lock_guard<std::mutex> lock(g_runtime_mu);
#ifdef _WIN32
  if (g_runtime_refs == 0) {
    WSADATA data{};
    if (::WSAStartup(MAKEWORD(2, 2), &data) != 0) {
      return false;
    }
  }
#endif
  ++g_runtime_refs;
  started_ = true;
  return true;
}

void SocketRuntime::Stop() noexcept {
  if (!started_) {
    return;
  }

  std::lock_guard<std::mutex> lock(g_runtime_mu);
  if (g_runtime_refs > 0) {
    --g_runtime_refs;
#ifdef _WIN32
    if (g_runtime_refs == 0) {
      (void)::WSACleanup();
    }
#endif
  }
  started_ = false;
}

}  // namespace kv::platform
