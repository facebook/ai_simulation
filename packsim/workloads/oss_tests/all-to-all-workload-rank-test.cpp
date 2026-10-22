/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/util/sim-fix.h"
#include "packsim/workloads/all-to-all.h"
#include "packsim/workloads/oss_tests/all-to-all-nic-aware-chunked-test-fixture.h"
#include "packsim/workloads/oss_tests/rank-harness.h"

namespace packsim {

class AllToAllWorkloadRankTest : public packsim::WorkloadRankTestHarness,
                                 public ::testing::TestWithParam<std::tuple<
                                     uint32_t /* endpoints */,
                                     uint32_t /* size */,
                                     uint32_t /* chunks */>> {
 public:
  void SetUp() override {
    workCfg_.name() = "A2A";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());

    auto a2a = packsim::workload::thrift::AllToAll();
    a2a.sizeBytes() = std::get<1>(GetParam());
    a2a.numChunks() = std::get<2>(GetParam());
    a2a.numGpusPerNvDomain() = 1;
    workCfg_.config()->allToAll() = a2a;

    run();
  }
};

TEST_P(AllToAllWorkloadRankTest, Validate) {
  validateStats();
}

INSTANTIATE_TEST_SUITE_P(
    AllToAllWorkloadRankTestSweep,
    AllToAllWorkloadRankTest,
    ::testing::Combine(
        ::testing::Values(64, 128, 256),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(1, 2, 4)));

class AllToAllMessageSizeTest : public packsim::WorkloadRankTestHarness,
                                public ::testing::Test {
 protected:
  // Two ranks: each rank sends sizeBytes / 2 bytes to its peer, in numChunks
  // messages.
  void configure(
      const uint64_t sizeBytes,
      const uint32_t numChunks = 1,
      const bool usePxn = false,
      const uint32_t numGpusPerNvDomain = 1) {
    workCfg_.name() = "A2A_SIZE";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = 2;

    auto a2a = packsim::workload::thrift::AllToAll();
    a2a.sizeBytes() = sizeBytes;
    a2a.numChunks() = numChunks;
    a2a.usePxn() = usePxn;
    a2a.numGpusPerNvDomain() = numGpusPerNvDomain;
    workCfg_.config()->allToAll() = a2a;
  }
};

// Each rank sends its peer two 3 GiB chunks. The 6 GiB it receives does not
// fit in 32 bits, and `validateStats()` checks it against `getExpectedBytes()`.
TEST_F(AllToAllMessageSizeTest, ExpectedBytesCountsMoreThan32BitsPerPeer) {
  constexpr uint64_t kChunkBytes = uint64_t{3} << 30;
  configure(/*sizeBytes=*/2 * 2 * kChunkBytes, /*numChunks=*/2);
  run();
  validateStats();
}

// Two GPUs per NV domain turn a 3 GiB chunk into a 6 GiB PXN message, which a
// `uint32_t` message size cannot hold.
TEST_F(AllToAllMessageSizeTest, ThrowsWhenPxnMessageDoesNotFitUint32) {
  constexpr uint64_t kChunkBytes = uint64_t{3} << 30;
  configure(
      /*sizeBytes=*/2 * kChunkBytes,
      /*numChunks=*/1,
      /*usePxn=*/true,
      /*numGpusPerNvDomain=*/2);
  EXPECT_THAT(
      [&]() { run(); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("does not fit in 32 bits")));
}

TEST_F(AllToAllMessageSizeTest, ThrowsWhenMaxChunksInFlightIsZero) {
  configure(/*sizeBytes=*/1024);
  workCfg_.config()->allToAll()->maxChunksInFlight() = 0;
  EXPECT_THAT(
      [&]() { run(); },
      ::testing::ThrowsMessage<std::invalid_argument>(::testing::HasSubstr(
          "Workload A2A_SIZE: AllToAll maxChunksInFlight must be at least 1, got 0")));
}

TEST_F(AllToAllMessageSizeTest, ThrowsWhenNumChunksIsZero) {
  configure(/*sizeBytes=*/1024, /*numChunks=*/0);
  EXPECT_THAT(
      [&]() { run(); },
      ::testing::ThrowsMessage<std::invalid_argument>(::testing::HasSubstr(
          "Workload A2A_SIZE: AllToAll numChunks must be at least 1, got 0")));
}

// One byte over two ranks truncates the message size to 0, so neither rank
// has traffic.
TEST_F(AllToAllMessageSizeTest, RanksWithZeroByteMessagesComplete) {
  configure(/*sizeBytes=*/1);
  run();
  validateStats();
}

// Both ranks share one NV domain, so neither has a peer to exchange messages
// with: each completes at once and expects no bytes.
TEST_F(AllToAllMessageSizeTest, RanksWhosePeersShareTheirNvDomainComplete) {
  configure(/*sizeBytes=*/1024);
  auto& rankToNvDomain = workCfg_.rankToNvDomain().ensure();
  rankToNvDomain.emplace(0, 0);
  rankToNvDomain.emplace(1, 0);

  run();

  for (const auto& rank : ranks_) {
    EXPECT_EQ(rank->getExpectedBytes(), 0);
  }
  validateStats();
}

// A rank with no traffic starts and ends when it runs, not at time 0.
TEST_F(AllToAllMessageSizeTest, RanksWithZeroByteMessagesStartWhenTheyRun) {
  constexpr uint64_t kStartTimeNs = 1000;
  configure(/*sizeBytes=*/1);
  workCfg_.startTimeNs() = kStartTimeNs;

  run();

  for (const auto& rank : ranks_) {
    const auto result = rank->getThriftResult();
    EXPECT_EQ(*result.startTimeNs(), kStartTimeNs);
    EXPECT_EQ(*result.endTimeNs(), kStartTimeNs);
  }
}

TEST_F(AllToAllMessageSizeTest, AcceptsTheLargestUint32MessageSize) {
  configure(2 * uint64_t{std::numeric_limits<uint32_t>::max()});
  EXPECT_NO_THROW(run());
}

TEST_F(AllToAllMessageSizeTest, ThrowsWhenMessageSizeDoesNotFitUint32) {
  configure(uint64_t{1} << 33);
  EXPECT_THAT(
      [&]() { run(); },
      ::testing::ThrowsMessage<std::range_error>(
          ::testing::HasSubstr("Overflow during arithmetic conversion")));
}

// Test that messages can be received/acked out of order
// This is important for multi-NIC/multi-QP configurations where
// messages may arrive in a different order than they were sent
class AllToAllOutOfOrderTest : public packsim::WorkloadRankTestHarness,
                               public ::testing::Test {
 public:
  void SetUp() override {
    workCfg_.name() = "A2A_OOO";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = 4;

    auto a2a = packsim::workload::thrift::AllToAll();
    a2a.sizeBytes() = 4096;
    a2a.numChunks() = 4; // Multiple chunks to test out-of-order handling
    a2a.numGpusPerNvDomain() = 1;
    workCfg_.config()->allToAll() = a2a;

    run();
  }
};

TEST_F(AllToAllOutOfOrderTest, MessagesCanBeReceivedOutOfOrder) {
  // This test verifies that the workload completes successfully
  // even when messages are processed in arbitrary order.
  // The test harness naturally handles message delivery, and
  // the unordered_map based storage in AllToAllRank allows
  // messages to be acked/received in any order.
  //
  // With 4 chunks per peer, messages can arrive out of order (e.g., chunk 3
  // before chunk 1), and the workload should still complete successfully
  // because we use unordered_map keyed by chunk ID, not a FIFO deque.
  validateStats();

  // Verify all ranks completed
  EXPECT_EQ(completedRanks_.size(), 4);
}

// Test that directly validates out-of-order message handling
// by simulating reverse-order message delivery
TEST(AllToAllOutOfOrderDirectTest, RequestReceivedHandlesReverseOrder) {
  // Create a minimal AllToAllRank and verify that requestReceived
  // can handle messages arriving in reverse order (chunk 3, 2, 1, 0)

  packsim::Scheduler scheduler;
  packsim::SimContext simCtx(&scheduler, 0);

  packsim::workload::thrift::Workload workCfg;
  workCfg.name() = "OOO_DIRECT";
  workCfg.workloadId() = 0;
  workCfg.numEndpoints() = 2;
  workCfg.rankToId() = {{0, {0}}, {1, {1}}};
  workCfg.rankToIdList() = {{0, {{0}}}, {1, {{1}}}};

  auto a2a = packsim::workload::thrift::AllToAll();
  a2a.sizeBytes() = 1024;
  a2a.numChunks() = 4;
  a2a.numGpusPerNvDomain() = 1;
  workCfg.config()->allToAll() = a2a;

  // Build QPN maps
  std::vector<packsim::workload::thrift::Workload> workCfgs({workCfg});
  packsim::buildQPNMap(workCfgs);
  uint32_t nextQpn = 0;
  packsim::buildNicQPNMap(workCfgs, nextQpn);
  workCfg = std::move(workCfgs.at(0));

  // Track messages and completion
  std::vector<std::unique_ptr<packsim::Message>> sentMsgs;
  std::vector<std::unique_ptr<packsim::Message>> recvMsgs;
  bool rankCompleted = false;

  auto smcb = [&](packsim::Message* msg) {
    auto copy = std::make_unique<packsim::Message>(*msg);
    sentMsgs.emplace_back(std::move(copy));
  };
  auto rmcb = [&](packsim::Message* msg) {
    auto copy = std::make_unique<packsim::Message>(*msg);
    recvMsgs.emplace_back(std::move(copy));
  };
  auto rccb = [&](uint32_t, uint32_t) { rankCompleted = true; };

  packsim::WorkloadRankParams params{
      .simCtx = &simCtx,
      .workloadId = 0,
      .workCfg = workCfg,
      .rank = 0,
      .smcb = smcb,
      .rmcb = rmcb,
      .rccb = rccb,
      .parent = nullptr};

  packsim::AllToAllRank rank(params);

  // Run to post receives and sends
  rank.run();
  scheduler.run();

  // Filter to only data messages (not notify messages which have sizeBytes ==
  // kNotifyMsgSize)
  std::vector<packsim::Message*> dataRecvMsgs;
  for (auto& msg : recvMsgs) {
    if (msg->sizeBytes > packsim::kNotifyMsgSize) { // Data messages are larger
      dataRecvMsgs.emplace_back(msg.get());
    }
  }

  // Verify we have 4 data receives (4 chunks from peer 1)
  ASSERT_EQ(dataRecvMsgs.size(), 4);

  // Sort by connectionMessageId descending to simulate REVERSE order (3, 2, 1,
  // 0)
  std::sort(dataRecvMsgs.begin(), dataRecvMsgs.end(), [](auto* a, auto* b) {
    return a->connectionMessageId > b->connectionMessageId;
  });

  // Deliver messages in reverse order
  for (auto* msgToDeliver : dataRecvMsgs) {
    // This should NOT throw - the unordered_map allows out-of-order delivery
    EXPECT_NO_THROW(rank.requestReceived(msgToDeliver))
        << "requestReceived should handle chunk "
        << msgToDeliver->connectionMessageId << " out of order";
  }
}

TEST_P(AllToAllNicAwareChunkedTest, SendRecvQpAssignmentMatches) {
  validateStats();
}

INSTANTIATE_TEST_SUITE_P(
    AllToAllNicAwareChunkedSweep,
    AllToAllNicAwareChunkedTest,
    ::testing::Values(
        // 18 chunks, 16 maxInFlight, 4 NIC pairs: triggers multi-call
        // progress() where QP assignment must persist across calls
        std::make_tuple(6u, 18u, 16u, 4u),
        // 9 chunks, 4 maxInFlight, 4 NIC pairs: multiple progress() calls
        std::make_tuple(4u, 9u, 4u, 4u),
        // 9 chunks, 16 maxInFlight, 4 NIC pairs: single progress() call
        std::make_tuple(4u, 9u, 16u, 4u),
        // 20 chunks, 8 maxInFlight, 3 NIC pairs: stress test
        std::make_tuple(8u, 20u, 8u, 3u)));

} // namespace packsim
