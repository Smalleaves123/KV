#pragma once 

#include "kv/common/status.h"

#include <cstdint>
#include <string>

namespace kv {

class MemTable;
class Recovery{
public:
    //回放单个WAL文件到memtable
    static Status ReplayWAL(const std::string & wal_path,MemTable * memtable);
    //同时返回回放到的最大seq
    static Status ReplayWAL(const std::string & wal_path,MemTable * memtable,uint64_t * max_seq);
};

}
