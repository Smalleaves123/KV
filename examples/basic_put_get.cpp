#include "kv/engine/db.h"

#include <iostream>
#include <memory>
#include <string>

int main() {
  kv::DBOptions options;
  options.db_path = "test_tmp/example_basic";

  std::unique_ptr<kv::DB> db;
  kv::Status s = kv::DB::Open(options, &db);
  if (!s.ok()) {
    std::cerr << "open failed: " << s.ToString() << "\n";
    return 1;
  }

  s = db->Put(kv::WriteOptions{true}, "hello", "world");
  if (!s.ok()) {
    std::cerr << "put failed: " << s.ToString() << "\n";
    return 1;
  }

  std::string value;
  s = db->Get(kv::ReadOptions{}, "hello", &value);
  if (!s.ok()) {
    std::cerr << "get failed: " << s.ToString() << "\n";
    return 1;
  }
  std::cout << "hello = " << value << "\n";
  return db->Close().ok() ? 0 : 1;
}
