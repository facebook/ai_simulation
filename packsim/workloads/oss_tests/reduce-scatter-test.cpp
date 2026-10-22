/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <tuple>

#include <gtest/gtest.h>

#include "packsim/workloads/oss_tests/rank-harness.h"

using packsim::workload::thrift::AllReduceAlgorithm;

namespace packsim {

/** Recursive **/

class ReduceScatterRecursiveTest
    : public packsim::WorkloadRankTestHarness,
      public ::testing::TestWithParam<
          std::tuple<uint32_t, uint32_t, AllReduceAlgorithm>> {
 public:
  void SetUp() override {
    workCfg_.name() = "ReduceScatter";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());

    auto rs = packsim::workload::thrift::ReduceScatter();
    rs.sizeBytes() = std::get<1>(GetParam());
    rs.algorithm() = std::get<2>(GetParam());
    workCfg_.config()->reduceScatter() = rs;
  }
};

TEST_P(ReduceScatterRecursiveTest, Validate) {
  run();
  validateStats();
  EXPECT_EQ(completedRanks_.size(), ranks_.size());

  auto numRanks = *workCfg_.numEndpoints();
  uint32_t numSteps = std::log2(numRanks);

  // numSteps ctrl msgs plus # of data msgs:
  size_t expMsgs =
      std::get<2>(GetParam()) == AllReduceAlgorithm::RECURSIVE_MSG_OPT
      ? numSteps + numSteps
      : numSteps + numRanks - 1;

  for (const auto& rs : rankStats_) {
    EXPECT_EQ(rs.sentMsgs, expMsgs);
    EXPECT_EQ(rs.rcvdMsgs, expMsgs);
  }
}

INSTANTIATE_TEST_SUITE_P(
    ReduceScatterRecursiveTestSweep,
    ReduceScatterRecursiveTest,
    ::testing::Combine(
        ::testing::Values(8, 64, 256),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(
            AllReduceAlgorithm::RECURSIVE_MSG_OPT,
            AllReduceAlgorithm::RECURSIVE_DIST_OPT)));

/** Ring **/

class ReduceScatterRingTest : public packsim::WorkloadRankTestHarness,
                              public ::testing::TestWithParam<
                                  std::tuple<uint32_t, uint32_t, uint32_t>> {
 public:
  void SetUp() override {
    workCfg_.name() = "ReduceScatter";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());

    auto rs = packsim::workload::thrift::ReduceScatter();
    rs.sizeBytes() = std::get<1>(GetParam());
    rs.algorithm() = AllReduceAlgorithm::RING;
    rs.numChunks() = std::get<2>(GetParam());
    workCfg_.config()->reduceScatter() = rs;
  }
};

TEST_P(ReduceScatterRingTest, Validate) {
  run();
  validateStats();
  EXPECT_EQ(completedRanks_.size(), ranks_.size());

  auto numRanks = *workCfg_.numEndpoints();
  uint32_t numChunks = std::get<2>(GetParam());

  // numSteps ctrl msgs plus # of data msgs:
  size_t expMsgs = (numRanks - 1) * numChunks;

  for (const auto& rs : rankStats_) {
    EXPECT_EQ(rs.sentMsgs, expMsgs);
    EXPECT_EQ(rs.rcvdMsgs, expMsgs);
  }
}

INSTANTIATE_TEST_SUITE_P(
    ReduceScatterRingTestSweep,
    ReduceScatterRingTest,
    ::testing::Combine(
        ::testing::Values(8, 64, 256),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(1, 2, 4, 8)));

} // namespace packsim
