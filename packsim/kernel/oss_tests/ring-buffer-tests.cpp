/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/ring-buffer.h"

namespace packsim {

namespace {
// Live-instance counter whose copy ctor can throw on demand, to check the copy
// constructor leaks nothing when an element copy throws.
struct ThrowOnCopy {
  static inline int liveCount = 0;
  // -1 = never throw
  static inline int copiesUntilThrow = -1;

  ThrowOnCopy() {
    ++liveCount;
  }

  ThrowOnCopy(const ThrowOnCopy&) {
    if (copiesUntilThrow == 0) {
      throw std::runtime_error("copy budget exhausted");
    }
    if (copiesUntilThrow > 0) {
      --copiesUntilThrow;
    }
    ++liveCount;
  }

  ThrowOnCopy(ThrowOnCopy&&) noexcept {
    ++liveCount;
  }

  ~ThrowOnCopy() {
    --liveCount;
  }
  // Only constructed and destroyed in the tests; RingBuffer never assigns
  // elements, so assignment has no throw/count analog to define.
  ThrowOnCopy& operator=(const ThrowOnCopy&) = delete;
  ThrowOnCopy& operator=(ThrowOnCopy&&) = delete;
};
} // namespace

TEST(RingBufferTest, PushSizeIndex) {
  RingBuffer<size_t> rb;
  EXPECT_TRUE(rb.empty());
  for (size_t i = 0; i < 5; ++i) {
    rb.push_back(i * 10);
  }
  ASSERT_EQ(rb.size(), 5u);
  EXPECT_FALSE(rb.empty());
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(rb[i], i * 10) << "index returns the i-th pushed value in order";
  }
  EXPECT_EQ(rb.front(), 0U) << "front is the oldest element";
}

TEST(RingBufferTest, PopFrontIsFifo) {
  RingBuffer<int> rb;
  for (int i = 0; i < 4; ++i) {
    rb.push_back(i);
  }
  rb.pop_front();
  rb.pop_front();
  ASSERT_EQ(rb.size(), 2u);
  EXPECT_EQ(rb.front(), 2) << "front advances past popped elements";
  EXPECT_EQ(rb[0], 2);
  EXPECT_EQ(rb[1], 3);
}

TEST(RingBufferTest, MutableFrontAndIndex) {
  RingBuffer<int> rb;
  rb.push_back(1);
  rb.push_back(2);
  rb.front() = 100;
  rb[1] = 200;
  EXPECT_EQ(rb[0], 100) << "front() returns a mutable reference";
  EXPECT_EQ(rb[1], 200) << "operator[] returns a mutable reference";
}

TEST(RingBufferTest, GrowsPreservingOrderAcrossWrap) {
  // Advance the head so the live range straddles the capacity boundary, then
  // grow, to catch a relinearization bug that only shows with head != 0.
  RingBuffer<size_t> rb;
  for (size_t i = 0; i < 4; ++i) {
    rb.push_back(i);
  }
  rb.pop_front();
  rb.pop_front(); // live = [2, 3], head advanced
  for (size_t i = 4; i < 12; ++i) {
    rb.push_back(i); // wraps into freed slots, then grows
  }
  ASSERT_EQ(rb.size(), 10u);
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(rb[i], i + 2)
        << "growth across a wrapped head preserves FIFO order at index " << i;
  }
}

TEST(RingBufferTest, IterationIsFrontToBack) {
  RingBuffer<int> rb;
  for (int i = 0; i < 3; ++i) {
    rb.push_back(i + 1);
  }
  rb.pop_front();
  rb.push_back(4); // live = [2, 3, 4], wrapped
  std::vector<int> seen;
  for (auto& v : rb) {
    seen.emplace_back(v);
  }
  EXPECT_EQ(seen, (std::vector<int>{2, 3, 4}))
      << "range-for yields elements in logical front-to-back order";
}

TEST(RingBufferTest, AtThrowsOutOfRange) {
  RingBuffer<int> rb;
  rb.push_back(5);
  EXPECT_EQ(rb.at(0), 5);
  EXPECT_THAT(
      [&] { rb.at(1); },
      testing::ThrowsMessage<std::out_of_range>(
          testing::HasSubstr("index out of range")))
      << "at() bounds-checks like std::deque::at with a descriptive message";
}

TEST(RingBufferTest, BoolElementsGiveRealReferences) {
  // std::vector<bool> bit-packs and cannot yield a bool&; the receive bitmap
  // does front()=true and operator[]=true, so RingBuffer must return real refs.
  RingBuffer<bool> rb;
  for (int i = 0; i < 3; ++i) {
    rb.push_back(false);
  }
  rb[1] = true;
  rb.front() = true;
  EXPECT_TRUE(rb[0]);
  EXPECT_TRUE(rb[1]);
  EXPECT_FALSE(rb[2]);
}

TEST(RingBufferTest, EmplaceBackConstructsElement) {
  RingBuffer<std::pair<int, int>> rb;
  rb.emplace_back(1, 2);
  EXPECT_EQ(rb[0].first, 1);
  EXPECT_EQ(rb[0].second, 2);
}

TEST(RingBufferTest, CopyIsDeepAndIndependent) {
  RingBuffer<int> a;
  for (int i = 0; i < 3; ++i) {
    a.push_back(i);
  }
  RingBuffer<int> b = a;
  b[0] = 99;
  b.push_back(7);
  EXPECT_EQ(a[0], 0) << "mutating the copy must not touch the original";
  ASSERT_EQ(a.size(), 3u);
  EXPECT_EQ(b[0], 99);
  ASSERT_EQ(b.size(), 4u);
}

TEST(RingBufferTest, ClearMakesBufferReusable) {
  RingBuffer<int> rb;
  for (int i = 0; i < 3; ++i) {
    rb.push_back(i);
  }
  rb.clear();
  EXPECT_TRUE(rb.empty());
  rb.push_back(9);
  EXPECT_EQ(rb.front(), 9) << "buffer is reusable after clear";
}

TEST(RingBufferTest, SatisfiesForwardIterator) {
  RingBuffer<int> rb;
  for (int i = 0; i < 4; ++i) {
    rb.push_back(i);
  }
  rb.pop_front();
  rb.push_back(4); // live = [1, 2, 3, 4], wrapped

  // Post-increment returns the pre-increment position.
  auto it = rb.begin();
  auto prev = it++;
  EXPECT_EQ(*prev, 1) << "post-increment yields the element before advancing";
  EXPECT_EQ(*it, 2);

  // Multipass: a forward iterator can traverse the same range twice with the
  // same result (an input iterator could not).
  std::vector<int> pass1(rb.begin(), rb.end());
  std::vector<int> pass2(rb.begin(), rb.end());
  EXPECT_EQ(pass1, pass2);
  EXPECT_EQ(pass1, (std::vector<int>{1, 2, 3, 4}));

  // Usable with STL algorithms that take iterators.
  EXPECT_EQ(std::count(rb.begin(), rb.end(), 3), 1);
  EXPECT_NE(std::find(rb.begin(), rb.end(), 4), rb.end());

  // Default-constructed iterators are comparable (a singular value).
  decltype(rb.begin()) a;
  decltype(rb.begin()) b;
  EXPECT_TRUE(a == b);
}

TEST(RingBufferTest, IteratorEqualityConsidersOwner) {
  RingBuffer<int> a;
  a.push_back(1);
  RingBuffer<int> b;
  b.push_back(1);
  // Same index into different buffers must not compare equal.
  EXPECT_FALSE(a.begin() == b.begin())
      << "iterators into different buffers are not equal";
  // A singular (default-constructed) iterator is not any buffer's begin().
  decltype(a.begin()) singular;
  EXPECT_FALSE(singular == a.begin())
      << "a singular iterator does not equal a real position";
}

TEST(RingBufferTest, SelfReferentialMovePushBackIsSafe) {
  // push_back(std::move(front())) aliases an element across the reallocation it
  // triggers; the moved-out value must survive (would be a use-after-free
  // without the guard, caught under ASAN).
  RingBuffer<std::string> rb;
  rb.push_back("zero");
  rb.push_back("one"); // capacity is now 2; the next push_back reallocates
  rb.push_back(std::move(rb.front()));
  ASSERT_EQ(rb.size(), 3u);
  EXPECT_EQ(rb[2], "zero") << "the self-referentially moved element survived";
}

TEST(RingBufferTest, CopyConstructorCleansUpWhenElementCopyThrows) {
  ThrowOnCopy::liveCount = 0;
  ThrowOnCopy::copiesUntilThrow = -1;
  {
    RingBuffer<ThrowOnCopy> src;
    for (int i = 0; i < 4; ++i) {
      src.emplace_back();
    }
    ASSERT_EQ(ThrowOnCopy::liveCount, 4);
    ThrowOnCopy::copiesUntilThrow = 2; // the third element copy throws
    EXPECT_THAT(
        [&] { RingBuffer<ThrowOnCopy>{src}; },
        testing::ThrowsMessage<std::runtime_error>(
            testing::HasSubstr("copy budget exhausted")))
        << "a throwing element copy propagates out of the copy constructor";
    EXPECT_EQ(ThrowOnCopy::liveCount, 4)
        << "the failed copy destroyed the elements it had built; only the "
           "source's four remain (no leak)";
  }
  EXPECT_EQ(ThrowOnCopy::liveCount, 0)
      << "destroying the source leaves no live instances";
}

} // namespace packsim
