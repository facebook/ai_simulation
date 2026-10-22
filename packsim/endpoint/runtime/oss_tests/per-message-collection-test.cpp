/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "packsim/endpoint/runtime/message-tracker.h"
#include "packsim/transport/transport.h"

namespace packsim {

// Helper to create a Message with the given fields
static Message makeMsg(
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

// -----------------------------------------------------------------
// Test: send on node A, recv on node B → joined record has both timestamps
// -----------------------------------------------------------------
TEST(PerMessageCollectionTest, SendAndRecvOnDifferentNodesAreJoined) {
  // Arrange
  MessageTracker senderTracker;
  MessageTracker receiverTracker;

  auto msg = makeMsg(
      /*workloadId=*/1,
      /*srcRank=*/0,
      /*dstRank=*/1,
      /*connectionMessageId=*/100,
      /*sizeBytes=*/4096);

  senderTracker.recordSend(&msg, /*timeNs=*/1000);
  receiverTracker.recordRecv(&msg, /*timeNs=*/5000);

  std::vector<const MessageTracker*> trackers{&senderTracker, &receiverTracker};

  // Act
  auto grouped = collectPerMessageResults(trackers);

  // Assert
  ASSERT_EQ(grouped.size(), 1);
  ASSERT_TRUE(grouped.contains(1));
  const auto& records = grouped.at(1);
  ASSERT_EQ(records.size(), 1);

  const auto& r = records[0];
  EXPECT_EQ(*r.workloadId(), 1);
  EXPECT_EQ(*r.srcRank(), 0);
  EXPECT_EQ(*r.dstRank(), 1);
  EXPECT_EQ(*r.messageIndex(), 100);
  EXPECT_EQ(*r.sizeBytes(), 4096);
  EXPECT_EQ(*r.sendTimeNs(), 1000);
  EXPECT_EQ(*r.recvTimeNs(), 5000);
}

// -----------------------------------------------------------------
// Test: send record with no matching recv → excluded from results
// (one-sided RDMA and other send-only patterns not yet supported)
// -----------------------------------------------------------------
TEST(PerMessageCollectionTest, SendWithNoRecvIsExcluded) {
  // Arrange
  MessageTracker senderTracker;

  auto msg = makeMsg(
      /*workloadId=*/2,
      /*srcRank=*/0,
      /*dstRank=*/3,
      /*connectionMessageId=*/50,
      /*sizeBytes=*/512);

  senderTracker.recordSend(&msg, /*timeNs=*/2000);

  std::vector<const MessageTracker*> trackers{&senderTracker};

  // Act
  auto grouped = collectPerMessageResults(trackers);

  // Assert — unmatched sends are filtered out
  EXPECT_TRUE(grouped.empty());
}

// -----------------------------------------------------------------
// Test: multiple messages across multiple workloads → correctly grouped
// -----------------------------------------------------------------
TEST(PerMessageCollectionTest, MultipleMessagesGroupedByWorkloadId) {
  // Arrange
  // Node A sends for workload 10, Node B sends for workload 20
  // Node C receives for both workloads
  MessageTracker nodeA;
  MessageTracker nodeB;
  MessageTracker nodeC;

  auto msgW10_1 = makeMsg(10, 0, 1, 1, 1024);
  auto msgW10_2 = makeMsg(10, 0, 2, 2, 2048);
  auto msgW20_1 = makeMsg(20, 3, 4, 1, 512);

  nodeA.recordSend(&msgW10_1, 100);
  nodeA.recordSend(&msgW10_2, 200);
  nodeB.recordSend(&msgW20_1, 300);

  nodeC.recordRecv(&msgW10_1, 500);
  nodeC.recordRecv(&msgW10_2, 600);
  nodeC.recordRecv(&msgW20_1, 700);

  std::vector<const MessageTracker*> trackers{&nodeA, &nodeB, &nodeC};

  // Act
  auto grouped = collectPerMessageResults(trackers);

  // Assert
  ASSERT_EQ(grouped.size(), 2);

  // Workload 10: 2 messages
  ASSERT_TRUE(grouped.contains(10));
  EXPECT_EQ(grouped.at(10).size(), 2);

  // Workload 20: 1 message
  ASSERT_TRUE(grouped.contains(20));
  ASSERT_EQ(grouped.at(20).size(), 1);
  EXPECT_EQ(*grouped.at(20)[0].sendTimeNs(), 300);
  EXPECT_EQ(*grouped.at(20)[0].recvTimeNs(), 700);
}

// -----------------------------------------------------------------
// Test: empty trackers → no perMessageResults
// -----------------------------------------------------------------
TEST(PerMessageCollectionTest, EmptyTrackersProduceNoResults) {
  // Arrange
  MessageTracker emptyA;
  MessageTracker emptyB;

  std::vector<const MessageTracker*> trackers{&emptyA, &emptyB};

  // Act
  auto grouped = collectPerMessageResults(trackers);

  // Assert
  EXPECT_TRUE(grouped.empty());
}

// -----------------------------------------------------------------
// Test: recv without matching send is silently ignored
// -----------------------------------------------------------------
TEST(PerMessageCollectionTest, RecvWithoutSendIsIgnored) {
  // Arrange
  MessageTracker receiverOnly;

  auto msg = makeMsg(5, 0, 1, 99, 256);
  receiverOnly.recordRecv(&msg, 8000);

  std::vector<const MessageTracker*> trackers{&receiverOnly};

  // Act
  auto grouped = collectPerMessageResults(trackers);

  // Assert — no send record means nothing to join onto
  EXPECT_TRUE(grouped.empty());
}

// -----------------------------------------------------------------
// Test: send and recv on the SAME node (loopback) are joined
// -----------------------------------------------------------------
TEST(PerMessageCollectionTest, SendAndRecvOnSameNodeAreJoined) {
  // Arrange
  MessageTracker tracker;

  auto msg = makeMsg(3, 0, 0, 7, 128);
  tracker.recordSend(&msg, 1000);
  tracker.recordRecv(&msg, 1500);

  std::vector<const MessageTracker*> trackers{&tracker};

  // Act
  auto grouped = collectPerMessageResults(trackers);

  // Assert
  ASSERT_EQ(grouped.size(), 1);
  const auto& records = grouped.at(3);
  ASSERT_EQ(records.size(), 1);
  EXPECT_EQ(*records[0].sendTimeNs(), 1000);
  EXPECT_EQ(*records[0].recvTimeNs(), 1500);
}

// -----------------------------------------------------------------
// Test: makeMessageKey produces correct key from thrift record
// -----------------------------------------------------------------
TEST(PerMessageCollectionTest, MakeMessageKeyMatchesTrackerKey) {
  // Arrange
  MessageTracker tracker;
  auto msg = makeMsg(1, 10, 20, 42, 1024);
  tracker.recordSend(&msg, 5000);

  // Act
  const auto& record = tracker.getSendRecords()[0];
  auto key = makeMessageKey(record);

  // Assert — key should match what recordRecv would produce
  MessageKey expectedKey{1, 10, 20, 42, 1};
  EXPECT_EQ(key, expectedKey);
}

} // namespace packsim
