#pragma once

#include <cerrno>
#include <cstring>
#include <string>

#ifdef _WIN32
#include <windows.h>
#include <fcntl.h>
#include <io.h>
#include <sys/stat.h>
#else
#include <cstdio>
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

inline int ReplaceFile(const std::string& source, const std::string& destination) noexcept {
#ifdef _WIN32
  return ::MoveFileExA(source.c_str(), destination.c_str(),
                       MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)
             ? 0
             : -1;
#else
  return ::rename(source.c_str(), destination.c_str());
#endif
}

inline int SyncDirectory(const std::string& path) noexcept {
#ifdef _WIN32
  (void)path;
  return 0;
#else
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return -1;
  }
  const int result = ::fsync(fd);
  const int close_result = ::close(fd);
  return result == 0 ? close_result : result;
#endif
}

inline std::string FileErrorString() {
  return std::strerror(errno);
}

}  // namespace kv::platform
