#pragma once 

#include <cstdint>

namespace kv{
class Snapshot{
public:
    virtual ~Snapshot() = default;
    virtual uint64_t sequence() const noexcept = 0;
    
};

}