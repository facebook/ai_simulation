/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <tuple>

#include <gtest/gtest.h>

#include "packsim/workloads/oss_tests/rank-harness.h"

namespace packsim {

using packsim::workload::thrift::AllReduceAlgorithm;

/** Recursive **/

class AllGatherRDTest : public packsim::WorkloadRankTestHarness,
                        public ::testing::TestWithParam<std::tuple<
                            uint32_t /* endpoints */,
                            uint32_t /* channels */,
                            uint32_t /* qps per channel */,
                            uint32_t /* size */,
                            AllReduceAlgorithm>> {
 public:
  void SetUp() override {
    workCfg_.name() = "AllGather";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());

    auto channelSettings = packsim::workload::thrift::ChannelSettings();
    channelSettings.numChannels() = std::get<2>(GetParam());
    channelSettings.numQPsPerChannel() = std::get<3>(GetParam());
    workCfg_.channelSettings() = channelSettings;

    auto ag = packsim::workload::thrift::AllGather();
    ag.sizeBytes() = std::get<1>(GetParam());
    ag.algorithm() = std::get<4>(GetParam());
    workCfg_.config()->allGather() = ag;
  }
};

TEST_P(AllGatherRDTest, Validate) {
  run();
  validateStats();
  EXPECT_EQ(completedRanks_.size(), ranks_.size());
}

INSTANTIATE_TEST_SUITE_P(
    AllGatherRDTestSweep,
    AllGatherRDTest,
    ::testing::Combine(
        ::testing::Values(8, 64, 256),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(1, 10),
        ::testing::Values(1, 5),
        ::testing::Values(
            AllReduceAlgorithm::RECURSIVE_MSG_OPT,
            AllReduceAlgorithm::RECURSIVE_DIST_OPT)));

/** Ring **/
class AllGatherRingTest
    : public packsim::WorkloadRankTestHarness,
      public ::testing::TestWithParam<
          std::tuple<uint32_t, uint32_t, uint32_t, uint32_t, uint32_t>> {
 public:
  void SetUp() override {
    workCfg_.name() = "AllGather";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());
    auto channelSettings = packsim::workload::thrift::ChannelSettings();
    channelSettings.numChannels() = std::get<3>(GetParam());
    channelSettings.numQPsPerChannel() = std::get<4>(GetParam());
    workCfg_.channelSettings() = channelSettings;

    auto ag = packsim::workload::thrift::AllGather();
    ag.sizeBytes() = std::get<1>(GetParam());
    ag.numChunks() = std::get<2>(GetParam());
    ag.algorithm() = AllReduceAlgorithm::RING;
    workCfg_.config()->allGather() = ag;
  }
};

TEST_P(AllGatherRingTest, Validate) {
  run();
  validateStats();
  EXPECT_EQ(completedRanks_.size(), ranks_.size());
}

INSTANTIATE_TEST_SUITE_P(
    AllGatherRingTestSweep,
    AllGatherRingTest,
    ::testing::Combine(
        ::testing::Values(8, 64, 256), // numEndpoints
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024), // sizeBytes
        ::testing::Values(1, 2, 4, 8), // numChunks
        ::testing::Values(1, 10),
        ::testing::Values(1, 5)));

} // namespace packsim
