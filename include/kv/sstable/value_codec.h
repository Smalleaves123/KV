#pragma once

#include <cstdint>
#include <string>

namespace kv {

// Encodes TTL metadata without changing the legacy SST block entry layout.
// An expiry of zero returns the original value unchanged.
std::string EncodeSSTValue(const std::string& value, uint64_t expires_at_ms);

// Decodes values written by EncodeSSTValue. Legacy values are returned as-is.
void DecodeSSTValue(const std::string& encoded, std::string* value,
                    uint64_t* expires_at_ms);

}  // namespace kv
