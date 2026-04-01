#pragma once

#include <algorithm>
#include <cassert>
#include <utility>

namespace kv {

// ====================
// SkipList::Node
// ====================

template <typename Key, typename Comparator>
SkipList<Key, Comparator>::Node::Node() : key{}, next() {}

template <typename Key, typename Comparator>
SkipList<Key, Comparator>::Node::Node(const Key& k, int height)
    : key(k), next(static_cast<size_t>(height), nullptr) {}

template <typename Key, typename Comparator>
SkipList<Key, Comparator>::Node::Node(Key&& k, int height)
    : key(std::move(k)), next(static_cast<size_t>(height), nullptr) {}

// ====================
// SkipList::Iterator
// ====================

template <typename Key, typename Comparator>
SkipList<Key, Comparator>::Iterator::Iterator()
    : list_(nullptr), node_(nullptr) {}

template <typename Key, typename Comparator>
SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list, Node* node)
    : list_(list), node_(node) {}

template <typename Key, typename Comparator>
bool SkipList<Key, Comparator>::Iterator::Valid() const noexcept {
  return node_ != nullptr;
}

template <typename Key, typename Comparator>
const Key& SkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template <typename Key, typename Comparator>
void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->next[0];
}

template <typename Key, typename Comparator>
void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  if (list_ == nullptr) {
    node_ = nullptr;
    return;
  }
  node_ = list_->head_.next[0];
}

template <typename Key, typename Comparator>
void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  if (list_ == nullptr) {
    node_ = nullptr;
    return;
  }
  node_ = list_->FindGreaterOrEqualNode(target);
}

// ====================
// SkipList
// ====================

template <typename Key, typename Comparator>
SkipList<Key, Comparator>::SkipList(Comparator compare, int max_height)
    : compare_(std::move(compare)),
      max_height_(std::max(1, max_height)),
      current_height_(1),
      head_(Key{}, std::max(1, max_height)),
      size_(0),
      rng_(std::random_device{}()),
      promote_(0.25) {}

template <typename Key, typename Comparator>
SkipList<Key, Comparator>::~SkipList() {
  Clear();
}

template <typename Key, typename Comparator>
bool SkipList<Key, Comparator>::Empty() const noexcept {
  return size_ == 0;
}

template <typename Key, typename Comparator>
size_t SkipList<Key, Comparator>::Size() const noexcept {
  return size_;
}

template <typename Key, typename Comparator>
void SkipList<Key, Comparator>::Clear() {
  Node* x = head_.next[0];
  while (x != nullptr) {
    Node* next = x->next[0];
    delete x;
    x = next;
  }

  std::fill(head_.next.begin(), head_.next.end(), nullptr);
  current_height_ = 1;
  size_ = 0;
}

template <typename Key, typename Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
  InsertImpl(key);
}

template <typename Key, typename Comparator>
void SkipList<Key, Comparator>::Insert(Key&& key) {
  InsertImpl(std::move(key));
}

template <typename Key, typename Comparator>
typename SkipList<Key, Comparator>::Iterator
SkipList<Key, Comparator>::Begin() const {
  return Iterator(this, head_.next[0]);
}

template <typename Key, typename Comparator>
typename SkipList<Key, Comparator>::Iterator
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& target) const {
  return Iterator(this, FindGreaterOrEqualNode(target));
}

template <typename Key, typename Comparator>
template <typename K>
void SkipList<Key, Comparator>::InsertImpl(K&& key) {
  const Key& key_ref = key;
  std::vector<Node*> prev(static_cast<size_t>(max_height_), nullptr);

  Node* x = &head_;
  for (int level = current_height_ - 1; level >= 0; --level) {
    while (x->next[level] != nullptr &&
           compare_(x->next[level]->key, key_ref)) {
      x = x->next[level];
    }
    prev[static_cast<size_t>(level)] = x;
  }

  const int height = RandomHeight();
  if (height > current_height_) {
    for (int i = current_height_; i < height; ++i) {
      prev[static_cast<size_t>(i)] = &head_;
    }
    current_height_ = height;
  }

  Node* node = new Node(std::forward<K>(key), height);
  for (int i = 0; i < height; ++i) {
    node->next[static_cast<size_t>(i)] =
        prev[static_cast<size_t>(i)]->next[static_cast<size_t>(i)];
    prev[static_cast<size_t>(i)]->next[static_cast<size_t>(i)] = node;
  }

  ++size_;
}

template <typename Key, typename Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqualNode(const Key& target) const {
  Node* x = const_cast<Node*>(&head_);

  for (int level = current_height_ - 1; level >= 0; --level) {
    while (x->next[level] != nullptr &&
           compare_(x->next[level]->key, target)) {
      x = x->next[level];
    }
  }

  return x->next[0];
}

template <typename Key, typename Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
  int height = 1;
  while (height < max_height_ && promote_(rng_)) {
    ++height;
  }
  return height;
}

}  // namespace kv