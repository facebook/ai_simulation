/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstddef>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/util/memory-usage.h"

namespace packsim {

TEST(MemoryUsage, ReportsANonZeroPeak) {
  // A running process has always resident at least its own text and stack.
  EXPECT_GT(getPeakRssBytes(), 0u);
}

TEST(MemoryUsage, PeakNeverDecreases) {
  const size_t before = getPeakRssBytes();

  // Touch the pages so the allocation is resident rather than just reserved.
  constexpr size_t kAllocBytes = 64u << 20;
  auto block = std::make_unique<std::vector<char>>(kAllocBytes, 1);
  EXPECT_EQ((*block)[kAllocBytes - 1], 1);
  const size_t during = getPeakRssBytes();
  block.reset();

  // ru_maxrss is a high-water mark, so freeing must not walk it back.
  EXPECT_GE(during, before);
  EXPECT_GE(getPeakRssBytes(), during);
}

} // namespace packsim
