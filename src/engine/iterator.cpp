#include "kv/engine/iterator.h"

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "kv/memtable/memtable.h"
#include "kv/sstable/table_reader.h"

namespace kv {

namespace {

// Uniform wrapper over MemTable::Iterator and TableIterator.
struct Source {
  std::unique_ptr<MemTable::Iterator> mem_iter;
  std::unique_ptr<TableIterator> sst_iter;
  bool is_memtable = false;

  bool Valid() const {
    return is_memtable ? mem_iter->Valid() : sst_iter->Valid();
  }

  std::string_view Key() const {
    if (is_memtable) return mem_iter->entry().key;
    return sst_iter->key();
  }

  uint64_t Seq() const {
    if (is_memtable) return mem_iter->entry().seq;
    return sst_iter->seq();
  }

  uint8_t Type() const {
    if (is_memtable) return static_cast<uint8_t>(mem_iter->entry().type);
    return sst_iter->type();
  }

  std::string_view Value() const {
    if (is_memtable) return mem_iter->entry().value;
    return sst_iter->value();
  }

  void Next() {
    if (is_memtable) {
      mem_iter->Next();
    } else {
      sst_iter->Next();
    }
  }

  void Seek(const Slice& target) {
    if (is_memtable) {
      mem_iter->Seek(target);
    } else {
      sst_iter->Seek(std::string(target.data(), target.size()));
    }
  }

  void SeekToFirst() {
    if (is_memtable) {
      mem_iter->SeekToFirst();
    } else {
      sst_iter->SeekToFirst();
    }
  }
};

class MergingIterator final : public Iterator {
 public:
  MergingIterator(std::unique_ptr<MemTable::Iterator> mem_iter,
                   std::vector<std::unique_ptr<TableIterator>> sst_iters,
                   uint64_t read_seq)
      : read_seq_(read_seq), valid_(false) {
    if (mem_iter != nullptr) {
      Source src;
      src.mem_iter = std::move(mem_iter);
      src.is_memtable = true;
      sources_.push_back(std::move(src));
    }
    for (auto& sst_iter : sst_iters) {
      if (sst_iter == nullptr) continue;
      Source src;
      src.sst_iter = std::move(sst_iter);
      src.is_memtable = false;
      sources_.push_back(std::move(src));
    }
  }

  void SeekToFirst() override {
    for (auto& src : sources_) {
      src.SeekToFirst();
    }
    SkipToNextKey();
  }

  void Seek(const Slice& target) override {
    for (auto& src : sources_) {
      src.Seek(target);
    }
    SkipToNextKey();
  }

  void Next() override {
    if (!valid_) return;
    // Advance all sources past the current key, then find the next visible key.
    for (auto& src : sources_) {
      if (!src.Valid()) continue;
      if (src.Key() == current_key_) {
        // Skip all entries for this key from this source.
        while (src.Valid() && src.Key() == current_key_) {
          src.Next();
        }
      }
    }
    SkipToNextKey();
  }

  bool Valid() const override { return valid_; }

  Slice key() const override { return current_key_; }

  Slice value() const override { return current_value_; }

 private:
  // Find the index of the source with the smallest key among all valid sources.
  // Returns -1 if no valid sources remain.
  int FindSmallestSource() const {
    int best = -1;
    for (size_t i = 0; i < sources_.size(); ++i) {
      if (!sources_[i].Valid()) continue;
      if (best < 0 || sources_[i].Key() < sources_[best].Key()) {
        best = static_cast<int>(i);
      }
    }
    return best;
  }

  // Advance to the next visible (non-deleted, within read_seq) key.
  // Within each source, scans past invisible/tombstone versions per key.
  void SkipToNextKey() {
    while (true) {
      const int best = FindSmallestSource();
      if (best < 0) {
        valid_ = false;
        return;
      }

      const std::string candidate_key(sources_[best].Key());

      // Scan all versions across all sources for this key.  For each source
      // we walk through every entry whose key == candidate_key, record the
      // newest visible version (seq <= read_seq), then advance past them so
      // the source is ready for the next key.
      uint64_t best_seq = 0;
      int best_version_source = -1;
      bool is_tombstone = false;
      std::string best_value;

      for (size_t si = 0; si < sources_.size(); ++si) {
        auto& src = sources_[si];
        if (!src.Valid()) continue;
        if (src.Key() != candidate_key) continue;

        // Walk all entries for this key inside this source.
        while (src.Valid() && src.Key() == candidate_key) {
          if (src.Seq() <= read_seq_ && src.Seq() > best_seq) {
            best_seq = src.Seq();
            best_version_source = static_cast<int>(si);
            is_tombstone = (src.Type() == 1);
            best_value = std::string(src.Value());  // capture now, before advance
          }
          src.Next();
        }
      }

      if (best_version_source >= 0 && !is_tombstone) {
        current_key_ = std::string(candidate_key);
        current_value_ = std::move(best_value);
        valid_ = true;
        return;
      }

      // No visible entry for this key — sources already advanced, try next.
    }
  }

  std::vector<Source> sources_;
  uint64_t read_seq_;
  bool valid_;
  std::string current_key_;
  std::string current_value_;
};

}  // namespace

std::unique_ptr<Iterator> NewMergingIterator(
    std::unique_ptr<MemTable::Iterator> mem_iter,
    std::vector<std::unique_ptr<TableIterator>> sst_iters,
    uint64_t read_seq) {
  return std::make_unique<MergingIterator>(
      std::move(mem_iter), std::move(sst_iters), read_seq);
}

}  // namespace kv
