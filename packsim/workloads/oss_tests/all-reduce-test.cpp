/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <tuple>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/workloads/oss_tests/rank-harness.h"

using packsim::workload::thrift::AllReduceAlgorithm;
using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim {

/** Ring **/
class AllReduceRingTest : public packsim::WorkloadRankTestHarness,
                          public ::testing::TestWithParam<
                              std::tuple<uint32_t, uint32_t, uint32_t>> {
 public:
  void SetUp() override {
    workCfg_.name() = "AllReduce";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());

    auto ar = packsim::workload::thrift::AllReduce();
    ar.sizeBytes() = std::get<1>(GetParam());
    ar.numChunks() = std::get<2>(GetParam());
    ar.algorithm() = AllReduceAlgorithm::RING;
    workCfg_.config()->allReduce() = ar;
  }
};

TEST_P(AllReduceRingTest, Validate) {
  run();
  validateStats();
  EXPECT_EQ(completedRanks_.size(), ranks_.size());

  auto numRanks = *workCfg_.numEndpoints();
  uint32_t numChunks = std::get<2>(GetParam());

  // numSteps ctrl msgs plus # of data msgs:
  size_t expMsgs = (2 * numRanks - 2) * numChunks;

  for (const auto& rs : rankStats_) {
    EXPECT_EQ(rs.sentMsgs, expMsgs);
    EXPECT_EQ(rs.rcvdMsgs, expMsgs);
  }
}

INSTANTIATE_TEST_SUITE_P(
    AllReduceRingTestSweep,
    AllReduceRingTest,
    ::testing::Combine(
        ::testing::Values(8, 64, 256),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(1, 2, 4, 8)));

/** Double Binary Tree **/
class AllReduceDBTTest : public packsim::WorkloadRankTestHarness,
                         public ::testing::TestWithParam<
                             std::tuple<uint32_t, uint32_t, uint32_t>> {
 public:
  void SetUp() override {
    workCfg_.name() = "AllReduce";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());

    auto ar = packsim::workload::thrift::AllReduce();
    ar.sizeBytes() = std::get<1>(GetParam());
    ar.numChunks() = std::get<2>(GetParam());
    ar.algorithm() = AllReduceAlgorithm::DOUBLE_BINARY_TREE;
    workCfg_.config()->allReduce() = ar;
  }
};

TEST_P(AllReduceDBTTest, Validate) {
  run();
  validateStats();
}

INSTANTIATE_TEST_SUITE_P(
    AllReduceDBTTestSweep,
    AllReduceDBTTest,
    ::testing::Combine(
        ::testing::Values(8, 64, 256),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(1, 2, 4, 8)));

/** Test that non-power-of-2 ranks throw for DBT **/
class AllReduceDBTValidationTest : public packsim::WorkloadRankTestHarness,
                                   public ::testing::Test {};

TEST_F(AllReduceDBTValidationTest, ThrowsOnNonPowerOf2Ranks) {
  workCfg_.name() = "AllReduce";
  workCfg_.workloadId() = 0;
  workCfg_.numEndpoints() = 6; // Non-power-of-2
  auto ar = packsim::workload::thrift::AllReduce();
  ar.sizeBytes() = 1024;
  ar.numChunks() = 1;
  ar.algorithm() = AllReduceAlgorithm::DOUBLE_BINARY_TREE;
  workCfg_.config()->allReduce() = ar;
  EXPECT_THAT(
      [&]() { buildRanks(); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("numRanks_ (6) must be a power of 2")));
}

/** Configs that divide by zero, hang or build a degenerate tree **/
class AllReduceConfigValidationTest : public packsim::WorkloadRankTestHarness,
                                      public ::testing::Test {
 protected:
  void configure(
      const AllReduceAlgorithm algorithm,
      const uint32_t numEndpoints,
      const uint32_t numChunks,
      const uint32_t maxChunksInFlight = 1) {
    workCfg_.name() = "AR_CFG";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = numEndpoints;
    auto ar = packsim::workload::thrift::AllReduce();
    ar.sizeBytes() = 1024;
    ar.numChunks() = numChunks;
    ar.maxChunksInFlight() = maxChunksInFlight;
    ar.algorithm() = algorithm;
    workCfg_.config()->allReduce() = ar;
  }
};

TEST_F(AllReduceConfigValidationTest, DbtRejectsZeroNumChunks) {
  configure(
      AllReduceAlgorithm::DOUBLE_BINARY_TREE,
      /*numEndpoints=*/4,
      /*numChunks=*/0);
  EXPECT_THAT(
      [&]() { buildRanks(); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Workload AR_CFG: AllReduceDbt numChunks must be at least 1, got 0")));
}

TEST_F(AllReduceConfigValidationTest, DbtRejectsASingleEndpoint) {
  configure(
      AllReduceAlgorithm::DOUBLE_BINARY_TREE,
      /*numEndpoints=*/1,
      /*numChunks=*/1);
  EXPECT_THAT(
      [&]() { buildRanks(); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Workload AR_CFG: AllReduceDbt numEndpoints must be at least 2, got 1")));
}

TEST_F(AllReduceConfigValidationTest, RingRejectsASingleEndpoint) {
  configure(AllReduceAlgorithm::RING, /*numEndpoints=*/1, /*numChunks=*/1);
  EXPECT_THAT(
      [&]() { buildRanks(); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Workload AR_CFG: Ring numEndpoints must be at least 2, got 1")));
}

TEST_F(AllReduceConfigValidationTest, RecursiveRejectsASingleEndpoint) {
  configure(
      AllReduceAlgorithm::RECURSIVE_MSG_OPT,
      /*numEndpoints=*/1,
      /*numChunks=*/1);
  EXPECT_THAT(
      [&]() { buildRanks(); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Workload AR_CFG: Recursive numEndpoints must be at least 2, got 1")));
}

TEST_F(AllReduceConfigValidationTest, RingRejectsZeroMaxChunksInFlight) {
  configure(
      AllReduceAlgorithm::RING,
      /*numEndpoints=*/4,
      /*numChunks=*/2,
      /*maxChunksInFlight=*/0);
  EXPECT_THAT(
      [&]() { buildRanks(); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Workload AR_CFG: Ring maxChunksInFlight must be at least 1, got 0")));
}

/** Message sizes and chunk counts must fit in 32 bits **/
class AllReduceMessageSizeTest : public packsim::WorkloadRankTestHarness,
                                 public ::testing::Test {
 protected:
  // Two ranks: with one chunk, a ring chunk and a tree half are both
  // sizeBytes / 2 bytes, and the ring has two steps.
  void configure(
      const AllReduceAlgorithm algorithm,
      const uint64_t sizeBytes,
      const uint32_t numChunks = 1) {
    workCfg_.name() = "AllReduce";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = 2;
    auto ar = packsim::workload::thrift::AllReduce();
    ar.sizeBytes() = sizeBytes;
    ar.numChunks() = numChunks;
    ar.algorithm() = algorithm;
    workCfg_.config()->allReduce() = ar;
  }

  static constexpr uint64_t kMaxUint32 = std::numeric_limits<uint32_t>::max();
};

TEST_F(AllReduceMessageSizeTest, RingAcceptsTheLargestUint32ChunkSize) {
  configure(AllReduceAlgorithm::RING, 2 * kMaxUint32);
  run();
  validateStats();
  EXPECT_EQ(ranks_.at(0)->getExpectedBytes(), 2 * kMaxUint32);
}

TEST_F(AllReduceMessageSizeTest, RingThrowsWhenChunkSizeDoesNotFitUint32) {
  configure(AllReduceAlgorithm::RING, uint64_t{1} << 33);
  EXPECT_THAT(
      [&]() { buildRanks(); },
      ThrowsMessage<std::runtime_error>(HasSubstr("ceilDivide32 overflowed")));
}

TEST_F(AllReduceMessageSizeTest, RingThrowsWhenChunkCountDoesNotFitUint32) {
  configure(AllReduceAlgorithm::RING, 0, std::numeric_limits<uint32_t>::max());
  EXPECT_THAT(
      [&]() { buildRanks(); },
      ThrowsMessage<std::range_error>(
          HasSubstr("Overflow during arithmetic conversion")));
}

TEST_F(AllReduceMessageSizeTest, DbtAcceptsTheLargestUint32TreeSize) {
  configure(AllReduceAlgorithm::DOUBLE_BINARY_TREE, 2 * kMaxUint32);
  run();
  validateStats();
}

TEST_F(AllReduceMessageSizeTest, DbtThrowsWhenTreeSizeDoesNotFitUint32) {
  configure(AllReduceAlgorithm::DOUBLE_BINARY_TREE, uint64_t{1} << 33);
  EXPECT_THAT(
      [&]() { buildRanks(); },
      ThrowsMessage<std::range_error>(
          HasSubstr("Overflow during arithmetic conversion")));
}

/** Recursive halving and doubling **/
class AllReduceRHDTest
    : public packsim::WorkloadRankTestHarness,
      public ::testing::TestWithParam<
          std::tuple<uint32_t, uint32_t, AllReduceAlgorithm>> {
 public:
  void SetUp() override {
    workCfg_.name() = "AllReduce";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());

    auto ar = packsim::workload::thrift::AllReduce();
    ar.sizeBytes() = std::get<1>(GetParam());
    ar.algorithm() = std::get<2>(GetParam());
    workCfg_.config()->allReduce() = ar;
  }
};

TEST_P(AllReduceRHDTest, Validate) {
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
  expMsgs *= 2;

  for (const auto& rs : rankStats_) {
    EXPECT_EQ(rs.sentMsgs, expMsgs);
    EXPECT_EQ(rs.rcvdMsgs, expMsgs);
  }
}

INSTANTIATE_TEST_SUITE_P(
    AllReduceRHDTestSweep,
    AllReduceRHDTest,
    ::testing::Combine(
        ::testing::Values(8, 64, 256),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(
            AllReduceAlgorithm::RECURSIVE_MSG_OPT,
            AllReduceAlgorithm::RECURSIVE_DIST_OPT)));

/** Notify ACKs that reach an already complete rank are ignored **/
class AllReduceLateNotifyAckTest
    : public packsim::WorkloadRankTestHarness,
      public ::testing::TestWithParam<AllReduceAlgorithm> {};

TEST_P(AllReduceLateNotifyAckTest, IgnoresNotifyAcksAfterCompletion) {
  constexpr uint64_t kNotifyAckDelayNs = 1'000'000;
  workCfg_.name() = "AllReduce";
  workCfg_.workloadId() = 0;
  workCfg_.numEndpoints() = 8;
  auto ar = packsim::workload::thrift::AllReduce();
  ar.sizeBytes() = 1024;
  ar.algorithm() = GetParam();
  workCfg_.config()->allReduce() = ar;
  notifyAckDelay_ = Time::fromNs(kNotifyAckDelayNs);

  run();
  validateStats();
  for (const auto& rank : ranks_) {
    EXPECT_LT(*rank->getThriftResult().endTimeNs(), kNotifyAckDelayNs);
  }
}

INSTANTIATE_TEST_SUITE_P(
    AllReduceLateNotifyAckTestSweep,
    AllReduceLateNotifyAckTest,
    ::testing::Values(
        AllReduceAlgorithm::RECURSIVE_MSG_OPT,
        AllReduceAlgorithm::RECURSIVE_DIST_OPT));

} // namespace packsim
