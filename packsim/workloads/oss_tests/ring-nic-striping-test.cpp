/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/workloads/oss_tests/rank-harness.h"

using packsim::workload::thrift::AllReduceAlgorithm;
using packsim::workload::thrift::RingNicSelectionMode;
using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim {

class RingNicStripingTest : public WorkloadRankTestHarness,
                            public ::testing::Test {
 protected:
  static constexpr uint32_t kNumRanks = 4;
  static constexpr uint32_t kNumNics = 4;
  static constexpr uint32_t kNumChunks = 4;
  // Each NIC pair has one data QP followed by one notification QP.
  static constexpr uint32_t kQpsPerNic = 2;

  void configureTopology(
      const bool isAsymmetric,
      const bool requiresRematching = false) {
    workCfg_.name() = "RING_NIC_STRIPING_TEST";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = kNumRanks;

    for (uint32_t rank = 0; rank < kNumRanks; ++rank) {
      workCfg_.rankToId()->emplace(rank, std::vector<uint32_t>{rank, 0});
      std::vector<std::vector<uint32_t>> endpointIds;
      endpointIds.reserve(kNumNics);
      for (uint32_t nic = 0; nic < kNumNics; ++nic) {
        endpointIds.emplace_back(std::vector<uint32_t>{rank, nic});
      }
      workCfg_.rankToIdList()->emplace(rank, std::move(endpointIds));
    }

    for (uint32_t srcRank = 0; srcRank < kNumRanks; ++srcRank) {
      const uint32_t dstRank = (srcRank + 1) % kNumRanks;
      for (uint32_t srcNic = 0; srcNic < kNumNics; ++srcNic) {
        std::vector<uint32_t> dstNics;
        if (requiresRematching && srcNic == 0) {
          dstNics = {0, 1};
        } else if (requiresRematching && srcNic == 1) {
          dstNics = {0};
        } else {
          dstNics = {isAsymmetric ? (srcNic + 1) % kNumNics : srcNic};
        }
        for (const uint32_t dstNic : dstNics) {
          for (uint32_t qp = 0; qp < kQpsPerNic; ++qp) {
            packsim::thrift::QueuePairTemplate qpTemplate;
            qpTemplate.ranks()->srcRank() = srcRank;
            qpTemplate.ranks()->dstRank() = dstRank;
            qpTemplate.nics()->srcNicIdx() = srcNic;
            qpTemplate.nics()->dstNicIdx() = dstNic;
            qpTemplate.ports()->srcPorts() = {0};
            qpTemplate.ports()->dstPorts() = {0};
            workCfg_.qpTemplates()->emplace_back(std::move(qpTemplate));
          }
        }
      }
    }
  }

  void configureAllReduce(const RingNicSelectionMode mode) {
    packsim::workload::thrift::AllReduce allReduce;
    allReduce.sizeBytes() = kNumRanks * kNumChunks * 1024;
    allReduce.algorithm() = AllReduceAlgorithm::RING;
    allReduce.numChunks() = kNumChunks;
    allReduce.maxChunksInFlight() = kNumChunks;
    allReduce.numRails() = 1;
    allReduce.nicSelectionMode() = mode;
    workCfg_.config()->allReduce() = std::move(allReduce);
  }

  void configureAllGather(const RingNicSelectionMode mode) {
    packsim::workload::thrift::AllGather allGather;
    allGather.sizeBytes() = kNumRanks * kNumChunks * 1024;
    allGather.algorithm() = AllReduceAlgorithm::RING;
    allGather.numChunks() = kNumChunks;
    allGather.maxChunksInFlight() = kNumChunks;
    allGather.numRails() = 1;
    allGather.nicSelectionMode() = mode;
    workCfg_.config()->allGather() = std::move(allGather);
  }

  void wrapAllReduceInBatch() {
    packsim::workload::thrift::Workload child;
    child.name() = "RING_CHILD";
    child.workloadId() = 0;
    child.numEndpoints() = kNumRanks;
    child.config() = *workCfg_.config();

    packsim::workload::thrift::Batch batch;
    batch.children()->emplace_back(std::move(child));
    workCfg_.config()->batch() = std::move(batch);
  }

  void expectEveryNicUsedEqually() const {
    for (const auto& stats : rankStats_) {
      ASSERT_EQ(stats.sentMsgsByEndpoint.size(), kNumNics);
      ASSERT_EQ(stats.rcvdMsgsByEndpoint.size(), kNumNics);
      const size_t sentPerNic = stats.sentMsgs / kNumNics;
      const size_t receivedPerNic = stats.rcvdMsgs / kNumNics;
      for (uint32_t nic = 0; nic < kNumNics; ++nic) {
        EXPECT_EQ(stats.sentMsgsByEndpoint.at(nic), sentPerNic);
        EXPECT_EQ(stats.rcvdMsgsByEndpoint.at(nic), receivedPerNic);
      }
    }
  }

  void expectEveryNicUsedWithinOneMessage() const {
    for (const auto& stats : rankStats_) {
      ASSERT_EQ(stats.sentMsgsByEndpoint.size(), kNumNics);
      ASSERT_EQ(stats.rcvdMsgsByEndpoint.size(), kNumNics);
      std::vector<size_t> sentCounts;
      std::vector<size_t> receivedCounts;
      for (uint32_t nic = 0; nic < kNumNics; ++nic) {
        sentCounts.emplace_back(stats.sentMsgsByEndpoint.at(nic));
        receivedCounts.emplace_back(stats.rcvdMsgsByEndpoint.at(nic));
      }
      const auto [minSent, maxSent] =
          std::minmax_element(sentCounts.begin(), sentCounts.end());
      const auto [minReceived, maxReceived] =
          std::minmax_element(receivedCounts.begin(), receivedCounts.end());
      EXPECT_LE(*maxSent - *minSent, 1);
      EXPECT_LE(*maxReceived - *minReceived, 1);
    }
  }
};

TEST_F(RingNicStripingTest, AllReduceUsesEveryNicWithAsymmetricMappings) {
  configureTopology(/*isAsymmetric=*/true);
  configureAllReduce(RingNicSelectionMode::ALL_AVAILABLE_NICS);

  run();

  validateStats();
  expectEveryNicUsedEqually();
}

TEST_F(RingNicStripingTest, AllGatherUsesEveryNic) {
  configureTopology(/*isAsymmetric=*/false);
  configureAllGather(RingNicSelectionMode::ALL_AVAILABLE_NICS);

  run();

  validateStats();
  expectEveryNicUsedEqually();
}

TEST_F(RingNicStripingTest, FindsFullNicMatchingWhenGreedyChoiceWouldFail) {
  configureTopology(/*isAsymmetric=*/false, /*requiresRematching=*/true);
  configureAllReduce(RingNicSelectionMode::ALL_AVAILABLE_NICS);

  run();

  validateStats();
  expectEveryNicUsedEqually();
}

TEST_F(RingNicStripingTest, BatchChildUsesParentNicPaths) {
  configureTopology(/*isAsymmetric=*/true);
  configureAllReduce(RingNicSelectionMode::ALL_AVAILABLE_NICS);
  wrapAllReduceInBatch();

  run();

  validateStats();
  expectEveryNicUsedEqually();
}

TEST_F(RingNicStripingTest, NonMultipleChunkCountStaysBalanced) {
  configureTopology(/*isAsymmetric=*/false);
  configureAllGather(RingNicSelectionMode::ALL_AVAILABLE_NICS);
  workCfg_.config()->allGather()->numChunks() = kNumChunks + 1;
  workCfg_.config()->allGather()->maxChunksInFlight() = kNumChunks + 1;

  run();

  validateStats();
  expectEveryNicUsedWithinOneMessage();
}

TEST_F(RingNicStripingTest, SingleNicModePreservesExistingRouting) {
  configureTopology(/*isAsymmetric=*/false);
  configureAllReduce(RingNicSelectionMode::SINGLE_NIC);

  run();

  validateStats();
  for (const auto& stats : rankStats_) {
    EXPECT_EQ(stats.sentMsgsByEndpoint.size(), 1);
    EXPECT_EQ(stats.sentMsgsByEndpoint.at(0), stats.sentMsgs);
    EXPECT_EQ(stats.rcvdMsgsByEndpoint.size(), 1);
    EXPECT_EQ(stats.rcvdMsgsByEndpoint.at(0), stats.rcvdMsgs);
  }
}

TEST_F(RingNicStripingTest, RejectsTooFewChunksToUseEveryNic) {
  configureTopology(/*isAsymmetric=*/false);
  configureAllReduce(RingNicSelectionMode::ALL_AVAILABLE_NICS);
  workCfg_.config()->allReduce()->numChunks() = kNumNics - 1;

  EXPECT_THAT(
      [&]() { run(); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("requires at least 4 chunks")));
}

TEST_F(RingNicStripingTest, RejectsTooSmallInFlightWindow) {
  configureTopology(/*isAsymmetric=*/false);
  configureAllReduce(RingNicSelectionMode::ALL_AVAILABLE_NICS);
  workCfg_.config()->allReduce()->maxChunksInFlight() = kNumNics - 1;

  EXPECT_THAT(
      [&]() { run(); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("requires at least 4 chunks in flight")));
}

TEST_F(RingNicStripingTest, RejectsChannelSettings) {
  configureTopology(/*isAsymmetric=*/false);
  configureAllReduce(RingNicSelectionMode::ALL_AVAILABLE_NICS);
  workCfg_.channelSettings() = packsim::workload::thrift::ChannelSettings();

  EXPECT_THAT(
      [&]() { run(); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("cannot be combined with channel settings")));
}

TEST_F(RingNicStripingTest, RejectsChannelSettingsOwnedByBatchParent) {
  configureTopology(/*isAsymmetric=*/false);
  configureAllReduce(RingNicSelectionMode::ALL_AVAILABLE_NICS);
  wrapAllReduceInBatch();
  workCfg_.channelSettings() = packsim::workload::thrift::ChannelSettings();

  EXPECT_THAT(
      [&]() { run(); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("cannot be combined with channel settings")));
}

} // namespace packsim
