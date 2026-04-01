#pragma once

#include <cstddef>
#include <functional>
#include <random>
#include <vector>

namespace kv {

template <typename Key, typename Comparator = std::less<Key>>
class SkipList {
 private:
  struct Node {
    Node();
    explicit Node(const Key& k, int height);
    explicit Node(Key&& k, int height);

    Key key;
    std::vector<Node*> next;
  };

 public:
  class Iterator {
   public:
    Iterator();

    bool Valid() const noexcept;
    const Key& key() const;
    void Next();
    void SeekToFirst();
    void Seek(const Key& target);

   private:
    friend class SkipList;

    Iterator(const SkipList* list, Node* node);

    const SkipList* list_;
    Node* node_;
  };

  explicit SkipList(Comparator compare = Comparator(), int max_height = 12);
  ~SkipList();

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  bool Empty() const noexcept;
  size_t Size() const noexcept;

  void Clear();

  void Insert(const Key& key);
  void Insert(Key&& key);

  Iterator Begin() const;
  Iterator FindGreaterOrEqual(const Key& target) const;

 private:
  template <typename K>
  void InsertImpl(K&& key);

  Node* FindGreaterOrEqualNode(const Key& target) const;
  int RandomHeight();

  Comparator compare_;
  int max_height_;
  int current_height_;
  Node head_;
  size_t size_;
  std::mt19937 rng_;
  std::bernoulli_distribution promote_;
};

}  // namespace kv

#include "kv/memtable/skiplist.tpp"