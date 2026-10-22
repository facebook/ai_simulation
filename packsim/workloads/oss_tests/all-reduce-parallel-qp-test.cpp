/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstddef>
#include <cstdint>
#include <tuple>

#include <folly/Math.h>
#include <gtest/gtest.h>

#include "packsim/kernel/ceil-divide.h"
#include "packsim/workloads/oss_tests/rank-harness.h"

namespace packsim {

using packsim::workload::thrift::AllReduceAlgorithm;

namespace {

// Messages a rank sends for one `totalMessageSize` transfer split over
// `numChannels` channels of `messageSize`-byte messages; the last channel
// absorbs the remainder. Example: 302 bytes, 3 channels, 100-byte messages
// -> chunks 100, 100, 102 -> 1 + 1 + 2 = 4 messages.
uint32_t getParallelQPNumMessages(
    const uint32_t totalMessageSize,
    const uint32_t numChannels,
    const uint32_t messageSize) {
  uint32_t totalMessages = 0;
  for (uint32_t i = 0; i < numChannels; ++i) {
    uint32_t chunkSize = totalMessageSize / numChannels;
    if (i == numChannels - 1) {
      chunkSize = totalMessageSize - chunkSize * (numChannels - 1);
    }
    totalMessages += ceilDivide32(chunkSize, messageSize);
  }
  return totalMessages;
}

} // namespace

/** Ring **/
class AllReduceRingParallelQPTest
    : public packsim::WorkloadRankTestHarness,
      public ::testing::TestWithParam<
          //(numRanks, sizeBytes, numChunks, numChannels, numQPsPerChannel)
          std::tuple<uint32_t, uint32_t, uint32_t, uint32_t, uint32_t>> {
 public:
  void SetUp() override {
    workCfg_.name() = "AllReduce";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());

    auto channelSettings = packsim::workload::thrift::ChannelSettings();
    channelSettings.numChannels() = std::get<3>(GetParam());
    channelSettings.numQPsPerChannel() = std::get<4>(GetParam());
    workCfg_.channelSettings() = channelSettings;

    auto ar = packsim::workload::thrift::AllReduce();
    ar.sizeBytes() = std::get<1>(GetParam());
    ar.numChunks() = std::get<2>(GetParam());
    ar.algorithm() = AllReduceAlgorithm::RING;
    workCfg_.config()->allReduce() = ar;
  }
};

TEST_P(AllReduceRingParallelQPTest, Validate) {
  run();
  validateStats();
  EXPECT_EQ(completedRanks_.size(), ranks_.size());

  uint32_t numRanks = *workCfg_.numEndpoints();
  uint32_t numChunks = std::get<2>(GetParam());
  uint32_t collSize = std::get<1>(GetParam());

  // numSteps ctrl msgs plus # of data msgs:
  size_t expMsgs = (2 * numRanks - 2) * numChunks;

  const uint32_t rankMessageSize =
      folly::divCeil(collSize, numChunks * numRanks);
  expMsgs *= getParallelQPNumMessages(
      rankMessageSize,
      workCfg_.channelSettings()->numChannels().value(),
      workCfg_.channelSettings()->channelMessageSizeBytes().value());

  for (const auto& rs : rankStats_) {
    EXPECT_EQ(rs.sentMsgs, expMsgs);
    EXPECT_EQ(rs.rcvdMsgs, expMsgs);
  }
}

INSTANTIATE_TEST_SUITE_P(
    AllReduceRingParallelQPTestSweep,
    AllReduceRingParallelQPTest,
    ::testing::Combine(
        ::testing::Values(8, 64, 256),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(1, 2, 4, 8),
        ::testing::Values(1, 10),
        ::testing::Values(1, 5)));

/** Double Binary Tree **/
class AllReduceDBTParallelQPTest
    : public packsim::WorkloadRankTestHarness,
      public ::testing::TestWithParam<
          //(numRanks, sizeBytes, numChunks, numChannels, numQPsPerChannel)
          std::tuple<uint32_t, uint32_t, uint32_t, uint32_t, uint32_t>> {
 public:
  void SetUp() override {
    workCfg_.name() = "AllReduce";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());
    auto channelSettings = packsim::workload::thrift::ChannelSettings();
    channelSettings.numChannels() = std::get<3>(GetParam());
    channelSettings.numQPsPerChannel() = std::get<4>(GetParam());
    workCfg_.channelSettings() = channelSettings;

    auto ar = packsim::workload::thrift::AllReduce();
    ar.sizeBytes() = std::get<1>(GetParam());
    ar.numChunks() = std::get<2>(GetParam());
    ar.algorithm() = AllReduceAlgorithm::DOUBLE_BINARY_TREE;
    workCfg_.config()->allReduce() = ar;
  }
};

TEST_P(AllReduceDBTParallelQPTest, Validate) {
  run();
  validateStats();
}

INSTANTIATE_TEST_SUITE_P(
    AllReduceDBTParallelQPTestSweep,
    AllReduceDBTParallelQPTest,
    ::testing::Combine(
        ::testing::Values(8, 64, 256),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(1, 2, 4, 8),
        ::testing::Values(1, 10),
        ::testing::Values(1, 5)));

/** Recursive halving and doubling **/
class AllReduceRHDParallelQPTest : public packsim::WorkloadRankTestHarness,
                                   public ::testing::TestWithParam<std::tuple<
                                       uint32_t,
                                       uint32_t,
                                       uint32_t,
                                       uint32_t,
                                       AllReduceAlgorithm>> {
 public:
  void SetUp() override {
    workCfg_.name() = "AllReduce";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());
    auto channelSettings = packsim::workload::thrift::ChannelSettings();
    channelSettings.numChannels() = std::get<2>(GetParam());
    channelSettings.numQPsPerChannel() = std::get<3>(GetParam());
    workCfg_.channelSettings() = channelSettings;

    auto ar = packsim::workload::thrift::AllReduce();
    ar.sizeBytes() = std::get<1>(GetParam());
    ar.algorithm() = std::get<4>(GetParam());
    workCfg_.config()->allReduce() = ar;
  }
};

TEST_P(AllReduceRHDParallelQPTest, Validate) {
  run();
  validateStats();
  EXPECT_EQ(completedRanks_.size(), ranks_.size());
}

INSTANTIATE_TEST_SUITE_P(
    AllReduceRHDParallelQPTestSweep,
    AllReduceRHDParallelQPTest,
    ::testing::Combine(
        ::testing::Values(8, 64, 256),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(1, 10),
        ::testing::Values(1, 5),
        ::testing::Values(
            AllReduceAlgorithm::RECURSIVE_MSG_OPT,
            AllReduceAlgorithm::RECURSIVE_DIST_OPT)));

} // namespace packsim
