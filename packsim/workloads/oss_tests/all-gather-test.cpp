/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <tuple>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/workloads/oss_tests/rank-harness.h"

namespace packsim {

using packsim::workload::thrift::AllReduceAlgorithm;
using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

/** Recursive **/

class AllGatherRDTest
    : public packsim::WorkloadRankTestHarness,
      public ::testing::TestWithParam<
          std::tuple<uint32_t, uint32_t, AllReduceAlgorithm>> {
 public:
  void SetUp() override {
    workCfg_.name() = "AllGather";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());

    auto ag = packsim::workload::thrift::AllGather();
    ag.sizeBytes() = std::get<1>(GetParam());
    ag.algorithm() = std::get<2>(GetParam());
    workCfg_.config()->allGather() = ag;
  }
};

TEST_P(AllGatherRDTest, Validate) {
  run();
  validateStats();
  EXPECT_EQ(completedRanks_.size(), ranks_.size());

  auto numRanks = *workCfg_.numEndpoints();
  uint32_t numSteps = log2(numRanks);

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
    AllGatherRDTestSweep,
    AllGatherRDTest,
    ::testing::Combine(
        ::testing::Values(8, 64, 256),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(
            AllReduceAlgorithm::RECURSIVE_MSG_OPT,
            AllReduceAlgorithm::RECURSIVE_DIST_OPT)));

/** The largest recursive step message must fit in 32 bits **/
class AllGatherStepMessageSizeTest : public packsim::WorkloadRankTestHarness,
                                     public ::testing::Test {
 protected:
  // Four ranks in message optimized mode: the message doubles every step, so
  // the last step sends sizeBytes / 2.
  void configure(const uint64_t sizeBytes) {
    workCfg_.name() = "AllGather";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = 4;
    auto ag = packsim::workload::thrift::AllGather();
    ag.sizeBytes() = sizeBytes;
    ag.algorithm() = AllReduceAlgorithm::RECURSIVE_MSG_OPT;
    workCfg_.config()->allGather() = ag;
  }
};

TEST_F(AllGatherStepMessageSizeTest, AcceptsLastStepThatFitsUint32) {
  configure(4 * ((uint64_t{1} << 31) - 1));
  run();
  validateStats();
}

TEST_F(AllGatherStepMessageSizeTest, ThrowsWhenLastStepDoesNotFitUint32) {
  // The first step sends 2^31 bytes, which fits; the last step sends 2^32.
  configure(uint64_t{1} << 33);
  EXPECT_THAT(
      [&]() { buildRanks(); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Step message of 4294967296 bytes does not fit")));
}

/** A recursive all-gather needs at least two ranks **/
class AllGatherConfigValidationTest : public packsim::WorkloadRankTestHarness,
                                      public ::testing::Test {};

TEST_F(AllGatherConfigValidationTest, RecursiveRejectsASingleEndpoint) {
  workCfg_.name() = "AG_CFG";
  workCfg_.workloadId() = 0;
  workCfg_.numEndpoints() = 1;
  auto ag = packsim::workload::thrift::AllGather();
  ag.sizeBytes() = 1024;
  ag.algorithm() = AllReduceAlgorithm::RECURSIVE_MSG_OPT;
  workCfg_.config()->allGather() = ag;
  EXPECT_THAT(
      [&]() { buildRanks(); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Workload AG_CFG: Recursive numEndpoints must be at least 2, got 1")));
}

/** Notify ACKs that reach an already complete rank are ignored **/
class AllGatherLateNotifyAckTest
    : public packsim::WorkloadRankTestHarness,
      public ::testing::TestWithParam<AllReduceAlgorithm> {};

TEST_P(AllGatherLateNotifyAckTest, IgnoresNotifyAcksAfterCompletion) {
  constexpr uint64_t kNotifyAckDelayNs = 1'000'000;
  workCfg_.name() = "AllGather";
  workCfg_.workloadId() = 0;
  workCfg_.numEndpoints() = 8;
  auto ag = packsim::workload::thrift::AllGather();
  ag.sizeBytes() = 1024;
  ag.algorithm() = GetParam();
  workCfg_.config()->allGather() = ag;
  notifyAckDelay_ = Time::fromNs(kNotifyAckDelayNs);

  run();
  validateStats();
  for (const auto& rank : ranks_) {
    EXPECT_LT(*rank->getThriftResult().endTimeNs(), kNotifyAckDelayNs);
  }
}

INSTANTIATE_TEST_SUITE_P(
    AllGatherLateNotifyAckTestSweep,
    AllGatherLateNotifyAckTest,
    ::testing::Values(
        AllReduceAlgorithm::RECURSIVE_MSG_OPT,
        AllReduceAlgorithm::RECURSIVE_DIST_OPT));

/** Ring **/
class AllGatherRingTest : public packsim::WorkloadRankTestHarness,
                          public ::testing::TestWithParam<
                              std::tuple<uint32_t, uint32_t, uint32_t>> {
 public:
  void SetUp() override {
    workCfg_.name() = "AllGather";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());

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
    AllGatherRingTestSweep,
    AllGatherRingTest,
    ::testing::Combine(
        ::testing::Values(8, 64, 256), // numEndpoints
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024), // sizeBytes
        ::testing::Values(1, 2, 4, 8))); // numChunks

} // namespace packsim
