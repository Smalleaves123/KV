#include "kv/net/protocol.h"

namespace kv::net::protocol {

std::string SimpleString(const std::string &s) {
    return "+" + s + "\r\n";
}

std::string Integer(int64_t value) {
    return ":" + std::to_string(value) + "\r\n";
}

std::string Error (const std::string & msg){
    return "-ERR" + msg + "\r\n";
}

std::string BulkString (const std::string &s){
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}

std::string Nil(){
    return "$-1\r\n";
}

std::string Array(const std::vector<std::string> & encoded_items){
    std::string out = "*" + std::to_string(encoded_items.size()) + "\r\n";
    for (const auto & it :encoded_items){
        out += it;
    }
    return out;
}
}
