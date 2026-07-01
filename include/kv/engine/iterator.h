#pragma once

#include "kv/common/slice.h"

namespace kv {

// Abstract iterator over key-value pairs, ordered by key (lexicographic).
//
// Seek(target) positions the iterator at the first key >= target.
// Valid() returns true if the iterator is positioned at a live entry.
// key() / value() return the current entry (only valid when Valid()).
// Next() advances to the next distinct key.
class Iterator {
 public:
  virtual ~Iterator() = default;

  // Position at the first entry with key >= target.
  virtual void Seek(const Slice& target) = 0;

  // Position at the very first entry.
  virtual void SeekToFirst() = 0;

  // Advance to the next entry (different key).
  virtual void Next() = 0;

  // Returns true iff the iterator is positioned at a valid entry.
  virtual bool Valid() const = 0;

  // Return the key of the current entry. Only valid when Valid().
  virtual Slice key() const = 0;

  // Return the value of the current entry. Only valid when Valid().
  virtual Slice value() const = 0;
};

}  // namespace kv
