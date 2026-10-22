/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>

#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/switches_types.h"
#include "packsim/kernel/time.h"
#include "packsim/switch/buffer-tracker.h"

namespace packsim {

// `SwitchStats::addPacket` / `removePacket` take the current time as an
// argument, so the bubble accounting can be driven directly with no scheduler
// and no wall clock.
class SwitchStatsBubbleTest : public ::testing::Test {
 protected:
  // Only the cell counts drive the bubble accounting; `cellSize` and the byte
  // count are never read by it and exist solely to keep the fixture realistic.
  static constexpr uint32_t kCellSizeBytes = 100;
  static constexpr uint32_t kPktBytes = 200;
  static constexpr uint32_t kPktCells = 2;

  // Admit one packet, charged entirely to the shared region.
  void enqueueAtNs(const uint64_t ns) {
    stats_.addPacket(
        kPktBytes, kPktCells, 0, 0, kPktCells, 0, Time::fromNs(ns));
  }

  // Release one packet from the shared region.
  void dequeueAtNs(const uint64_t ns) {
    stats_.removePacket(kPktCells, 0, 0, kPktCells, 0, Time::fromNs(ns));
  }

  SwitchStats stats_{.cellSize = kCellSizeBytes};
};

TEST_F(
    SwitchStatsBubbleTest,
    FirstEnqueueStartsTheWindowWithoutCountingABubble) {
  // Arrange / Act
  enqueueAtNs(100);

  // Assert: the empty period before the very first packet is not a bubble.
  EXPECT_EQ(stats_.firstDataTimeNs, 100u);
  EXPECT_EQ(stats_.totalBubbleTimeNs, 0u);
  EXPECT_EQ(stats_.thisBubbleStartTimeNs, SwitchStats::kNoTimeNs);
}

TEST_F(SwitchStatsBubbleTest, DrainToEmptyOpensABubbleButAddsNoTimeYet) {
  // Arrange
  enqueueAtNs(100);

  // Act
  dequeueAtNs(300);

  // Assert: the bubble is only closed (and charged) by the next enqueue.
  EXPECT_EQ(stats_.usedCells, 0u);
  EXPECT_EQ(stats_.thisBubbleStartTimeNs, 300u);
  EXPECT_EQ(stats_.totalBubbleTimeNs, 0u);
}

TEST_F(SwitchStatsBubbleTest, PartialDrainLeavingCellsDoesNotOpenABubble) {
  // Arrange: two packets in, so one dequeue leaves the queue non-empty.
  enqueueAtNs(100);
  enqueueAtNs(150);

  // Act
  dequeueAtNs(300);

  // Assert
  EXPECT_EQ(stats_.usedCells, kPktCells);
  EXPECT_EQ(stats_.thisBubbleStartTimeNs, SwitchStats::kNoTimeNs);
}

TEST_F(SwitchStatsBubbleTest, RefillAfterDrainChargesTheIdleGap) {
  // Arrange
  enqueueAtNs(100);
  dequeueAtNs(300);

  // Act
  enqueueAtNs(500);

  // Assert: 300ns..500ns was idle.
  EXPECT_EQ(stats_.totalBubbleTimeNs, 200u);
  EXPECT_EQ(stats_.firstDataTimeNs, 100u);
}

TEST_F(SwitchStatsBubbleTest, SuccessiveBubblesAccumulate) {
  // Arrange
  enqueueAtNs(100);
  dequeueAtNs(200);

  // Act / Assert: first bubble 200..260.
  enqueueAtNs(260);
  EXPECT_EQ(stats_.totalBubbleTimeNs, 60u);

  // Act / Assert: second bubble 300..400 adds to the running total.
  dequeueAtNs(300);
  enqueueAtNs(400);
  EXPECT_EQ(stats_.totalBubbleTimeNs, 160u);
}

TEST_F(SwitchStatsBubbleTest, RefillInTheSameNanosecondChargesNoBubble) {
  // Arrange: the drain and the refill land on the same nanosecond, so the
  // `ns > thisBubbleStartTimeNs` guard rejects the zero-length bubble.
  enqueueAtNs(100);
  dequeueAtNs(300);

  // Act
  enqueueAtNs(300);

  // Assert
  EXPECT_EQ(stats_.totalBubbleTimeNs, 0u);
}

TEST_F(SwitchStatsBubbleTest, SerializeAfterFinalDrainExcludesTrailingIdle) {
  // Arrange: busy 100..200, idle 200..260, busy 260..300, then empty.
  enqueueAtNs(100);
  dequeueAtNs(200);
  enqueueAtNs(260);
  dequeueAtNs(300);

  // Act: serialize well after the final drain.
  const auto ser = stats_.serializeState(Time::fromNs(900));

  // Assert: the window stops at the final drain rather than at 900, so the
  // 600ns the queue spent idle with no more work to do is not charged as a
  // bubble. 100..300, of which 60ns was idle.
  EXPECT_EQ(*ser.firstDataTimeNs(), 100U);
  EXPECT_EQ(*ser.lastDataTimeNs(), 300U);
  EXPECT_EQ(*ser.totalBubbleTimeNs(), 60U);
  EXPECT_DOUBLE_EQ(*ser.bubbleFrac(), 60.0 / 200.0);
}

TEST_F(SwitchStatsBubbleTest, SerializeMidBurstEndsTheWindowAtSerialization) {
  // Arrange: idle 200..500, then still occupied when the state is serialized.
  enqueueAtNs(100);
  dequeueAtNs(200);
  enqueueAtNs(500);

  // Act
  const auto ser = stats_.serializeState(Time::fromNs(500));

  // Assert: the window ends where the queue refilled, so the 300ns bubble the
  // refill closed is divided by a window that contains it. Ending the window
  // at the previous drain instead would put 300ns over 100ns and report a
  // queue as idle three times longer than it existed.
  EXPECT_EQ(*ser.lastDataTimeNs(), 500U);
  EXPECT_EQ(*ser.totalBubbleTimeNs(), 300U);
  EXPECT_DOUBLE_EQ(*ser.bubbleFrac(), 300.0 / 400.0);
}

TEST_F(SwitchStatsBubbleTest, SerializeMidBurstGrowsTheWindowWhileOccupied) {
  // Arrange: the same history, serialized later and still occupied.
  enqueueAtNs(100);
  dequeueAtNs(200);
  enqueueAtNs(500);

  // Act
  const auto ser = stats_.serializeState(Time::fromNs(900));

  // Assert: the extra 400ns is busy time, so it widens the denominator and
  // leaves the bubble total alone.
  EXPECT_EQ(*ser.lastDataTimeNs(), 900U);
  EXPECT_EQ(*ser.totalBubbleTimeNs(), 300U);
  EXPECT_DOUBLE_EQ(*ser.bubbleFrac(), 300.0 / 800.0);
}

TEST_F(SwitchStatsBubbleTest, SerializeBeforeAnyDrainReportsSerializationTime) {
  // Arrange: data arrived and the queue never emptied, so
  // `thisBubbleStartTimeNs` is still the sentinel.
  enqueueAtNs(100);

  // Act
  const auto ser = stats_.serializeState(Time::fromNs(500));

  // Assert: the sentinel never reaches the wire. It is UINT64_MAX and these
  // fields are signed on the wire, so passing it through would report -1 and
  // divide by a window of nearly the whole 64-bit range.
  EXPECT_EQ(*ser.firstDataTimeNs(), 100U);
  EXPECT_EQ(*ser.lastDataTimeNs(), 500U);
  EXPECT_EQ(*ser.totalBubbleTimeNs(), 0U);
  EXPECT_DOUBLE_EQ(*ser.bubbleFrac(), 0.0);
}

TEST_F(SwitchStatsBubbleTest, SerializeWithNeverUsedStatsReportsZeroWindow) {
  // Arrange: no traffic at all, so both timestamps are still the sentinel.

  // Act
  const auto ser = stats_.serializeState(Time::fromNs(500));

  // Assert: a queue that never held a packet reports an empty window rather
  // than the sentinel, which would otherwise reach the wire as -1.
  EXPECT_EQ(*ser.firstDataTimeNs(), 0U);
  EXPECT_EQ(*ser.lastDataTimeNs(), 0U);
  EXPECT_EQ(*ser.totalBubbleTimeNs(), 0U);
  EXPECT_DOUBLE_EQ(*ser.bubbleFrac(), 0.0);
}

} // namespace packsim
