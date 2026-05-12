#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace kv {

class Block;

class BlockIterator {
 public:
  BlockIterator(const Block& block);

  void SeekToFirst();
  void SeekToLast();
  void Seek(std::string_view target);
  void Next();
  void Prev();

  bool Valid() const noexcept;
  std::string_view key() const noexcept;
  uint64_t seq() const noexcept;
  uint8_t type() const noexcept;
  std::string_view value() const noexcept;

 private:
  struct Entry {
    std::string_view key;
    uint64_t seq;
    uint8_t type;
    std::string_view value;
    uint32_t offset;
  };

  Entry ParseEntryAt(uint32_t offset) const;
  uint32_t RestartOffset(int index) const;
  void SeekToRestart(int index);

  const Block& block_;
  uint32_t current_;
  Entry current_entry_;
  bool valid_;
};

}  // namespace kv
