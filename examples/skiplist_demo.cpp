#include "kv/memtable/skiplist.h"

#include <iostream>

int main() {
  kv::SkipList<int> list;
  list.Insert(30);
  list.Insert(10);
  list.Insert(20);

  for (auto it = list.Begin(); it.Valid(); it.Next()) {
    std::cout << it.key() << "\n";
  }
  return 0;
}
