#pragma once

#include <cstdint>
#include <fstream>
#include <string>

#include "kv/wal/log_record.h"
#include "kv/common/status.h"
#include "kv/common/slice.h"

namespace kv {

class WALWriter{
public:
    WALWriter();
    ~WALWriter();

    WALWriter(const WALWriter& ) = delete;
    WALWriter& operator=(const WALWriter& ) = delete;

    Status Open(const std::string & file_path, bool append = true);
    Status Close();

    Status Append(const LogRecord & record);
    Status AppendPut(uint64_t seq, const Slice & key, const Slice & value);
    Status AppendPutWithTTL(uint64_t seq, const Slice& key, const Slice& value,
                            uint64_t expires_at_ms);
    Status AppendDelete(uint64_t seq, const Slice & key);

    Status Sync();

    bool IsOpen() const noexcept;
    uint64_t file_size() const noexcept;
    const std::string & file_path() const noexcept;

private:
    std::ofstream stream_;
    std::string file_path_;
    uint64_t file_size_;
    int sync_fd_;  // raw file descriptor for fsync
};

}
