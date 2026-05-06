#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace kv {
class TTLManager{
public:
    explicit TTLManager(int64_t default_ttl_ms = 0);
    void SetDefaultTTL(int64_t default_ttl_ms);

    // ttl_ms < 0: 使用 default_ttl_ms
    // ttl_ms <= 0: 表示不过期（移除该 key 的 TTL）
    void SetTTL(const std::string& key, int64_t ttl_ms = -1);

    // key 不存在 TTL 或 TTL 未到期 -> false
    bool IsExpired(const std::string& key) const;
    bool HasTTL(const std::string& key) const;

    void Erase(const std::string& key);
    void Clear();

  size_t Size() const;

  // 清理过期项，返回清理数量；可选返回被清理的 key
  size_t PurgeExpired(std::vector<std::string>* expired_keys = nullptr);

private:
    using TimePoint = std::chrono::steady_clock::time_point;
    static TimePoint Now();
    mutable std::mutex mu_;
    int64_t default_ttl_ms_;
    std::unordered_map<std::string ,TimePoint> expires_;
};
}