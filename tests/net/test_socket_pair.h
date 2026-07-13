#pragma once

#include "kv/common/socket_compat.h"

namespace kv::net::test {

struct SocketPair {
  platform::SocketHandle first = platform::kInvalidSocket;
  platform::SocketHandle second = platform::kInvalidSocket;
};

inline bool CreateSocketPair(SocketPair* pair) {
  if (pair == nullptr) {
    return false;
  }

#ifdef _WIN32
  static platform::SocketRuntime runtime;
  static const bool runtime_started = runtime.Start();
  if (!runtime_started) {
    return false;
  }

  const platform::SocketHandle listener = ::socket(AF_INET, SOCK_STREAM, 0);
  if (!platform::IsValidSocket(listener)) {
    return false;
  }
  (void)platform::SetSocketOptionInt(listener, SOL_SOCKET, SO_REUSEADDR, 1);

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(0);
  if (::bind(listener, reinterpret_cast<sockaddr*>(&address),
             sizeof(address)) != 0 ||
      ::listen(listener, 1) != 0) {
    (void)platform::CloseSocket(listener);
    return false;
  }

  platform::SocketLength length = sizeof(address);
  if (::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length) !=
      0) {
    (void)platform::CloseSocket(listener);
    return false;
  }

  const platform::SocketHandle client = ::socket(AF_INET, SOCK_STREAM, 0);
  if (!platform::IsValidSocket(client) ||
      ::connect(client, reinterpret_cast<sockaddr*>(&address),
                sizeof(address)) != 0) {
    (void)platform::CloseSocket(client);
    (void)platform::CloseSocket(listener);
    return false;
  }

  platform::SocketLength peer_length = sizeof(address);
  const platform::SocketHandle server =
      ::accept(listener, reinterpret_cast<sockaddr*>(&address), &peer_length);
  (void)platform::CloseSocket(listener);
  if (!platform::IsValidSocket(server)) {
    (void)platform::CloseSocket(client);
    return false;
  }

  pair->first = server;
  pair->second = client;
  return true;
#else
  platform::SocketHandle fds[2] = {platform::kInvalidSocket,
                                   platform::kInvalidSocket};
  if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
    return false;
  }
  pair->first = fds[0];
  pair->second = fds[1];
  return true;
#endif
}

inline void CloseSocketPair(SocketPair* pair) {
  if (pair == nullptr) {
    return;
  }
  (void)platform::CloseSocket(pair->first);
  (void)platform::CloseSocket(pair->second);
  pair->first = platform::kInvalidSocket;
  pair->second = platform::kInvalidSocket;
}

}  // namespace kv::net::test
