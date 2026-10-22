/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>

#include <gtest/gtest.h>

#include "packsim/endpoint/runtime/message-tracker.h"
#include "packsim/transport/transport.h"

namespace packsim {

// Helper to create a Message with the given fields
static Message createTestMessage(
    uint32_t workloadId,
    uint32_t srcRank,
    uint32_t dstRank,
    uint32_t connectionMessageId,
    uint32_t sizeBytes) {
  Message msg;
  msg.workloadId = workloadId;
  msg.srcRank = srcRank;
  msg.dstRank = dstRank;
  msg.connectionMessageId = connectionMessageId;
  msg.sizeBytes = sizeBytes;
  return msg;
}

// Test that recordSend stores all fields and the timestamp correctly
TEST(MessageTrackerTest, RecordSendStoresCorrectFields) {
  // Arrange
  MessageTracker tracker;
  auto msg = createTestMessage(
      /*workloadId=*/1,
      /*srcRank=*/10,
      /*dstRank=*/20,
      /*connectionMessageId=*/42,
      /*sizeBytes=*/1024);
  constexpr uint64_t sendTimeNs = 5000;

  // Act
  tracker.recordSend(&msg, sendTimeNs);

  // Assert
  const auto& records = tracker.getSendRecords();
  ASSERT_EQ(records.size(), 1);
  const auto& record = records[0];
  EXPECT_EQ(*record.workloadId(), 1);
  EXPECT_EQ(*record.srcRank(), 10);
  EXPECT_EQ(*record.dstRank(), 20);
  EXPECT_EQ(*record.messageIndex(), 42);
  EXPECT_EQ(*record.sizeBytes(), 1024);
  EXPECT_EQ(*record.sendTimeNs(), 5000);
}

// Test that recordRecv stores the correct key and timestamp
TEST(MessageTrackerTest, RecordRecvStoresCorrectKeyAndTimestamp) {
  // Arrange
  MessageTracker tracker;
  auto msg = createTestMessage(
      /*workloadId=*/1,
      /*srcRank=*/10,
      /*dstRank=*/20,
      /*connectionMessageId=*/42,
      /*sizeBytes=*/1024);
  constexpr uint64_t recvTimeNs = 9000;

  // Act
  tracker.recordRecv(&msg, recvTimeNs);

  // Assert
  const auto& recvTimes = tracker.getRecvTimes();
  ASSERT_EQ(recvTimes.size(), 1);
  constexpr MessageKey expectedKey{1, 10, 20, 42, 1};
  auto it = recvTimes.find(expectedKey);
  ASSERT_NE(it, recvTimes.end());
  EXPECT_EQ(it->second, 9000);
}

// Test that multiple sends from different src/dst pairs are stored
// independently
TEST(MessageTrackerTest, MultipleSendsStoredIndependently) {
  // Arrange
  MessageTracker tracker;
  auto msg1 = createTestMessage(1, 10, 20, 100, 512);
  auto msg2 = createTestMessage(2, 30, 40, 200, 2048);

  // Act
  tracker.recordSend(&msg1, 1000);
  tracker.recordSend(&msg2, 2000);

  // Assert
  const auto& records = tracker.getSendRecords();
  ASSERT_EQ(records.size(), 2);

  EXPECT_EQ(*records[0].srcRank(), 10);
  EXPECT_EQ(*records[0].dstRank(), 20);
  EXPECT_EQ(*records[0].sendTimeNs(), 1000);

  EXPECT_EQ(*records[1].srcRank(), 30);
  EXPECT_EQ(*records[1].dstRank(), 40);
  EXPECT_EQ(*records[1].sendTimeNs(), 2000);
}

// Test that multiple recvs with different keys are stored independently
TEST(MessageTrackerTest, MultipleRecvsStoredIndependently) {
  // Arrange
  MessageTracker tracker;
  auto msg1 = createTestMessage(1, 10, 20, 100, 512);
  auto msg2 = createTestMessage(2, 30, 40, 200, 2048);

  // Act
  tracker.recordRecv(&msg1, 3000);
  tracker.recordRecv(&msg2, 4000);

  // Assert
  const auto& recvTimes = tracker.getRecvTimes();
  ASSERT_EQ(recvTimes.size(), 2);

  constexpr MessageKey key1{1, 10, 20, 100, 1};
  constexpr MessageKey key2{2, 30, 40, 200, 1};
  EXPECT_EQ(recvTimes.at(key1), 3000);
  EXPECT_EQ(recvTimes.at(key2), 4000);
}

// Test that getSendRecords returns empty vector when nothing recorded
TEST(MessageTrackerTest, GetSendRecordsReturnsEmptyWhenNothingRecorded) {
  // Arrange
  MessageTracker tracker;

  // Act & Assert
  const auto& records = tracker.getSendRecords();
  EXPECT_TRUE(records.empty());
}

// Test that getRecvTimes returns empty map when nothing recorded
TEST(MessageTrackerTest, GetRecvTimesReturnsEmptyWhenNothingRecorded) {
  // Arrange
  MessageTracker tracker;

  // Act & Assert
  const auto& recvTimes = tracker.getRecvTimes();
  EXPECT_TRUE(recvTimes.empty());
}

// Test that messages with same (workloadId, srcRank, dstRank,
// connectionMessageId) but different iterations are stored independently (no
// key collision).
TEST(MessageTrackerTest, DifferentIterationsAreDistinguished) {
  // Arrange
  MessageTracker tracker;
  auto msg1 = createTestMessage(1, 10, 20, 42, 1024);
  msg1.iteration = 1;
  auto msg2 = createTestMessage(1, 10, 20, 42, 1024);
  msg2.iteration = 2;

  // Act
  tracker.recordRecv(&msg1, 5000);
  tracker.recordRecv(&msg2, 6000);

  // Assert — both should be stored with distinct keys
  const auto& recvTimes = tracker.getRecvTimes();
  ASSERT_EQ(recvTimes.size(), 2);
  constexpr MessageKey key1{1, 10, 20, 42, 1};
  constexpr MessageKey key2{1, 10, 20, 42, 2};
  EXPECT_EQ(recvTimes.at(key1), 5000);
  EXPECT_EQ(recvTimes.at(key2), 6000);
}

} // namespace packsim
