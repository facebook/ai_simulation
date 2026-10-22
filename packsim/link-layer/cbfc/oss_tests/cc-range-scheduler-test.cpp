/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/cc-range-scheduler.h"

#include <cstdint>
#include <optional>
#include <stdexcept>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace packsim::cbfc {

TEST(CcRangeSchedulerTest, OneRangeQueuesAndPopsOnce) {
  // Arrange
  CcRangeScheduler ranges(/*numRanges=*/1);

  // Act
  ranges.queueAll();
  ranges.queueAll(); // re-queueing a queued range adds nothing

  // Assert
  EXPECT_TRUE(ranges.hasQueued());
  EXPECT_EQ(ranges.pop(), std::optional<uint8_t>{0});
  EXPECT_FALSE(ranges.hasQueued());
  EXPECT_EQ(ranges.pop(), std::nullopt);
}

TEST(CcRangeSchedulerTest, TwoRangesPopInOrder) {
  // Arrange
  CcRangeScheduler ranges(/*numRanges=*/2);
  ranges.queueAll();

  // Act + Assert
  EXPECT_EQ(ranges.pop(), std::optional<uint8_t>{0});
  EXPECT_EQ(ranges.pop(), std::optional<uint8_t>{1});
  EXPECT_EQ(ranges.pop(), std::nullopt);
}

// A timer that re-queues every range after each pop must still reach the
// upper range; a lowest-range-first scan would pop range 0 forever.
TEST(CcRangeSchedulerTest, RoundRobinDoesNotStarveTheUpperRange) {
  // Arrange
  CcRangeScheduler ranges(/*numRanges=*/2);

  // Act + Assert
  ranges.queueAll();
  EXPECT_EQ(ranges.pop(), std::optional<uint8_t>{0});
  ranges.queueAll();
  EXPECT_EQ(ranges.pop(), std::optional<uint8_t>{1});
  EXPECT_EQ(ranges.pop(), std::optional<uint8_t>{0});
}

TEST(CcRangeSchedulerTest, ClearDropsQueuedRangesAndRestartsAtZero) {
  // Arrange
  CcRangeScheduler ranges(/*numRanges=*/2);
  ranges.queueAll();
  ASSERT_EQ(ranges.pop(), std::optional<uint8_t>{0});

  // Act
  ranges.clear();

  // Assert
  EXPECT_FALSE(ranges.hasQueued());
  ranges.queueAll();
  EXPECT_EQ(ranges.pop(), std::optional<uint8_t>{0});
}

TEST(CcRangeSchedulerTest, RejectsARangeCountOutsideOneToTwo) {
  EXPECT_THAT(
      [] { CcRangeScheduler ranges(/*numRanges=*/0); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("0 CC_Update ranges")));
  EXPECT_THAT(
      [] { CcRangeScheduler ranges(/*numRanges=*/3); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("3 CC_Update ranges")));
}

} // namespace packsim::cbfc
