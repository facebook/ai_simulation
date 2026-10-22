/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace packsim {

/**
 * A growable circular FIFO with O(1) push_back, pop_front, and random access.
 * Backed by a single right-sized heap block (capacity is a power of two), so a
 * buffer holding a few small elements costs tens of bytes; std::deque's >=512
 * byte per-node granularity is what dominates when many short per-path queues
 * coexist, which is what this replaces. Elements are managed with explicit
 * construct/destroy (placement new), so T need not be default-constructible --
 * only copy- or move-constructible. front()/operator[] on an empty buffer are
 * undefined, matching std::deque.
 *
 * Example:
 *   RingBuffer<int> rb;
 *   rb.push_back(1);
 *   rb.push_back(2);
 *   int oldest = rb.front();  // 1
 *   rb.pop_front();
 *
 * Prior art: folly::DynamicRingQueue is the same growable power-of-two ring,
 * but it has no random-access operator[], requires a default-constructible T
 * (it backs storage with make_unique<T[]>), and is move-only -- none of which
 * fit the per-path send-window use here, so this reimplements the pattern with
 * indexing, placement-new, and deep copy.
 */
template <typename T>
class RingBuffer {
  static_assert(
      alignof(T) <= alignof(std::max_align_t),
      "RingBuffer backs storage with new[], which only guarantees "
      "max_align_t alignment; over-aligned T is unsupported.");

 public:
  RingBuffer() = default;

  // Copyable (deep copy) and movable, matching std::deque so this is a drop-in
  // for a PathState member that may itself be copied.
  RingBuffer(const RingBuffer& other) {
    copyFrom(other);
  }

  RingBuffer& operator=(const RingBuffer& other) {
    if (this != &other) {
      RingBuffer copy(other);
      swap(copy);
    }
    return *this;
  }

  RingBuffer(RingBuffer&& other) noexcept {
    swap(other);
  }

  RingBuffer& operator=(RingBuffer&& other) noexcept {
    if (this != &other) {
      destroyAll();
      storage_.reset();
      capacity_ = mask_ = head_ = size_ = 0;
      swap(other);
    }
    return *this;
  }

  ~RingBuffer() {
    destroyAll();
  }

  void swap(RingBuffer& other) noexcept {
    std::swap(storage_, other.storage_);
    std::swap(capacity_, other.capacity_);
    std::swap(mask_, other.mask_);
    std::swap(head_, other.head_);
    std::swap(size_, other.size_);
  }

  [[nodiscard]] size_t size() const noexcept {
    return size_;
  }

  [[nodiscard]] bool empty() const noexcept {
    return size_ == 0;
  }

  T& operator[](size_t i) noexcept {
    return slot((head_ + i) & mask_);
  }

  const T& operator[](size_t i) const noexcept {
    return slot((head_ + i) & mask_);
  }

  T& at(size_t i) {
    checkBounds(i);
    return (*this)[i];
  }

  const T& at(size_t i) const {
    checkBounds(i);
    return (*this)[i];
  }

  T& front() noexcept {
    return (*this)[0];
  }

  const T& front() const noexcept {
    return (*this)[0];
  }

  void push_back(const T& v) {
    if (size_ + 1 > capacity_) {
      // v may alias an element of this buffer; copy it before ensureCapacity
      // reallocates and invalidates the reference (matches std::deque, whose
      // stable nodes make self-insertion safe).
      T tmp(v);
      ensureCapacity(size_ + 1);
      new (rawSlot((head_ + size_) & mask_)) T(std::move(tmp));
    } else {
      new (rawSlot((head_ + size_) & mask_)) T(v);
    }
    ++size_;
  }

  void push_back(T&& v) {
    if (size_ + 1 > capacity_) {
      // v may alias an element of this buffer; move it out before
      // ensureCapacity reallocates and invalidates the reference (matches
      // push_back(const T&)).
      T tmp(std::move(v));
      ensureCapacity(size_ + 1);
      new (rawSlot((head_ + size_) & mask_)) T(std::move(tmp));
    } else {
      new (rawSlot((head_ + size_) & mask_)) T(std::move(v));
    }
    ++size_;
  }

  // Unlike push_back, this does not copy args before the (possible)
  // reallocation, so args must not reference an element of this buffer (e.g.
  // emplace_back(front())) -- use push_back for a self-referential insert.
  template <typename... Args>
  void emplace_back(Args&&... args) {
    ensureCapacity(size_ + 1);
    // Parenthesized init to match std::deque::emplace_back semantics (brace
    // init would prefer an initializer_list constructor when one exists).
    new (rawSlot((head_ + size_) & mask_)) T(std::forward<Args>(args)...);
    ++size_;
  }

  void pop_front() {
    slot(head_).~T();
    head_ = (head_ + 1) & mask_;
    --size_;
  }

  void clear() {
    destroyAll();
    head_ = 0;
    size_ = 0;
  }

  // Forward iterator over elements in logical (front-to-back) order (satisfies
  // LegacyForwardIterator, so it works with range-for and STL algorithms).
  template <typename Ref, typename Owner>
  class Iter {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = std::remove_reference_t<Ref>*;
    using reference = Ref;

    Iter() = default;
    Iter(Owner owner, size_t i) : owner_(owner), i_(i) {}

    Ref operator*() const {
      return (*owner_)[i_];
    }

    pointer operator->() const {
      return &(*owner_)[i_];
    }

    Iter& operator++() {
      ++i_;
      return *this;
    }

    Iter operator++(int) {
      Iter tmp = *this;
      ++i_;
      return tmp;
    }

    // Memberwise (owner_ then i_); != is synthesized from this in C++20.
    bool operator==(const Iter&) const = default;

   private:
    Owner owner_{};
    size_t i_{0};
  };

  Iter<T&, RingBuffer*> begin() {
    return {this, 0};
  }

  Iter<T&, RingBuffer*> end() {
    return {this, size_};
  }

  Iter<const T&, const RingBuffer*> begin() const {
    return {this, 0};
  }

  Iter<const T&, const RingBuffer*> end() const {
    return {this, size_};
  }

 private:
  std::byte* rawSlot(size_t idx) {
    return storage_.get() + idx * sizeof(T);
  }

  T& slot(size_t idx) {
    return *std::launder(reinterpret_cast<T*>(rawSlot(idx)));
  }

  const T& slot(size_t idx) const {
    return *std::launder(
        reinterpret_cast<const T*>(storage_.get() + idx * sizeof(T)));
  }

  void checkBounds(size_t i) const {
    if (i >= size_) {
      throw std::out_of_range("RingBuffer::at index out of range");
    }
  }

  void destroyAll() {
    for (size_t i = 0; i < size_; ++i) {
      slot((head_ + i) & mask_).~T();
    }
  }

  // Deep-copy `other`, releasing any existing elements first so it is safe on a
  // non-empty target as well (today only the copy constructor calls it, on a
  // fresh buffer; assignment goes through copy-and-swap).
  void copyFrom(const RingBuffer& other) {
    destroyAll();
    size_ = 0;
    head_ = 0;
    ensureCapacity(other.size_);
    // A throwing copy ctor never runs ~RingBuffer, so release the prefix built
    // so far here instead of leaking it (mirrors ensureCapacity).
    try {
      for (size_t i = 0; i < other.size_; ++i) {
        new (rawSlot(i)) T(other[i]);
        ++size_;
      }
    } catch (...) {
      destroyAll();
      size_ = 0;
      throw;
    }
  }

  // Reallocate to the next power of two >= needed, moving live elements so the
  // front sits at index 0.
  void ensureCapacity(size_t needed) {
    if (needed <= capacity_) {
      return;
    }
    // Largest power-of-two size_t; past it the doubling below overflows to 0
    // and spins. Real callers never approach this (per-path windows are tiny),
    // but bound it like std::vector::reserve rather than loop forever.
    constexpr size_t kMaxCapacity = ~(SIZE_MAX >> 1);
    if (needed > kMaxCapacity) {
      throw std::length_error("RingBuffer capacity exceeds size_t");
    }
    size_t newCapacity = capacity_ == 0 ? 1 : capacity_;
    while (newCapacity < needed) {
      newCapacity <<= 1;
    }
    auto next = std::make_unique<std::byte[]>(newCapacity * sizeof(T));
    // Move (or copy, for a throwing-move T) every element into `next` before
    // destroying any source element, so a throw only unwinds the partial `next`
    // and leaves the source intact (strong exception safety).
    size_t built = 0;
    try {
      for (; built < size_; ++built) {
        new (next.get() + built * sizeof(T))
            T(std::move_if_noexcept(slot((head_ + built) & mask_)));
      }
    } catch (...) {
      for (size_t j = 0; j < built; ++j) {
        std::launder(reinterpret_cast<T*>(next.get() + j * sizeof(T)))->~T();
      }
      throw;
    }
    for (size_t i = 0; i < size_; ++i) {
      slot((head_ + i) & mask_).~T();
    }
    storage_ = std::move(next);
    capacity_ = newCapacity;
    mask_ = newCapacity - 1;
    head_ = 0;
  }

  std::unique_ptr<std::byte[]> storage_;
  size_t capacity_{0};
  size_t mask_{0};
  size_t head_{0};
  size_t size_{0};
};

} // namespace packsim
