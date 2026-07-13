#pragma once

#include <cerrno>
#include <cstring>
#include <string>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <sys/stat.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace kv::platform {

inline int OpenSyncFile(const std::string& path, bool append) noexcept {
#ifdef _WIN32
  int flags = _O_WRONLY | _O_CREAT | _O_BINARY;
  flags |= append ? _O_APPEND : _O_TRUNC;
  return ::_open(path.c_str(), flags, _S_IREAD | _S_IWRITE);
#else
  int flags = O_WRONLY | O_CREAT;
  flags |= append ? O_APPEND : O_TRUNC;
  return ::open(path.c_str(), flags, 0644);
#endif
}

inline int CloseFile(int fd) noexcept {
  if (fd < 0) {
    return 0;
  }
#ifdef _WIN32
  return ::_close(fd);
#else
  return ::close(fd);
#endif
}

inline int SyncFile(int fd) noexcept {
  if (fd < 0) {
    return 0;
  }
#ifdef _WIN32
  return ::_commit(fd);
#else
  return ::fsync(fd);
#endif
}

inline std::string FileErrorString() {
  return std::strerror(errno);
}

}  // namespace kv::platform
