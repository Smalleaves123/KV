#pragma once

#include <string>
#include <vector>

namespace kv::net::protocol {

// 简单文本协议（兼容 redis 风格的最小子集）
std::string SimpleString(const std::string& s); // +xxx\r\n
std::string Error(const std::string& msg);      // -ERR xxx\r\n
std::string BulkString(const std::string& s);   // $len\r\n...\r\n
std::string Nil();                              // $-1\r\n
std::string Array(const std::vector<std::string>& encoded_items); // *n + items

}  // namespace kv::net::protocol
