/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/util/builders.h"
#include "packsim/workloads/oss_tests/rank-harness.h"

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim {

/** Batch of a DBT AllReduce, an RHD AllReduce and a nested batch **/
class BatchTest : public packsim::WorkloadRankTestHarness,
                  public ::testing::TestWithParam<
                      std::tuple<uint32_t, uint32_t, uint32_t>> {
 public:
  void SetUp() override {
    uint32_t numRanks = std::get<0>(GetParam());
    uint32_t collSize = std::get<1>(GetParam());
    uint32_t numChunks = std::get<2>(GetParam());
    workCfg_.name() = "Batch";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = numRanks;
    workCfg_.config()->batch() = packsim::workload::thrift::Batch();

    auto children = workCfg_.config()->batch()->children();
    children->emplace_back(
        packsim::getArDbtCfg(simCtx_.random, numRanks, collSize, numChunks));
    children->emplace_back(
        packsim::getArRhdCfg(simCtx_.random, numRanks, collSize, numChunks));

    children->emplace_back(workCfg_); // Nest a batch workload
  }
};

TEST_P(BatchTest, Validate) {
  run();
  validateStats();
}

INSTANTIATE_TEST_SUITE_P(
    BatchTestSweep,
    BatchTest,
    ::testing::Combine(
        ::testing::Values(8, 64),
        ::testing::Values(1024, 1024 * 1024),
        ::testing::Values(1, 2, 4)));

/** Notifies and notify ACKs reaching a complete batch are ignored **/
class BatchLateNotifyTest : public packsim::WorkloadRankTestHarness,
                            public ::testing::Test {
 protected:
  static constexpr uint32_t kNumRanks = 8;
  static constexpr uint64_t kLateNs = 1'000'000;

  // One recursive AllGather child, which completes long before kLateNs.
  void SetUp() override {
    workCfg_.name() = "Batch";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = kNumRanks;
    workCfg_.config()->batch() = packsim::workload::thrift::Batch();

    packsim::workload::thrift::Workload child;
    child.name() = "AllGather";
    child.workloadId() = 0;
    child.numEndpoints() = kNumRanks;
    auto ag = packsim::workload::thrift::AllGather();
    ag.sizeBytes() = 1024;
    ag.algorithm() =
        packsim::workload::thrift::AllReduceAlgorithm::RECURSIVE_MSG_OPT;
    child.config()->allGather() = ag;
    workCfg_.config()->batch()->children()->emplace_back(std::move(child));
  }

  void expectCompletedBeforeLateEvents() {
    EXPECT_EQ(completedRanks_.size(), ranks_.size());
    for (const auto& rank : ranks_) {
      EXPECT_LT(*rank->getThriftResult().endTimeNs(), kLateNs);
    }
  }
};

TEST_F(BatchLateNotifyTest, IgnoresNotifyAcksAfterCompletion) {
  notifyAckDelay_ = Time::fromNs(kLateNs);

  run();
  validateStats();
  expectCompletedBeforeLateEvents();
}

/** A child rank with no traffic ends its trace event before the next child **/
class BatchNoTrafficChildTest : public packsim::WorkloadRankTestHarness,
                                public ::testing::Test {
 protected:
  static constexpr uint32_t kNumRanks = 4;

  // Two vectorized AllToAll children in which only rank 0 sends, to rank 1:
  // ranks 2 and 3 have no traffic in either child.
  void SetUp() override {
    workCfg_.name() = "Batch";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = kNumRanks;
    workCfg_.config()->batch() = packsim::workload::thrift::Batch();

    packsim::workload::thrift::Workload child;
    child.name() = "A2AV";
    child.workloadId() = 0;
    child.numEndpoints() = kNumRanks;
    auto a2a = packsim::workload::thrift::AllToAll();
    a2a.sizeBytes() = 0;
    a2a.numChunks() = 1;
    a2a.vectorizedSizesBytes()->resize(
        kNumRanks, std::vector<uint32_t>(kNumRanks, 0));
    a2a.vectorizedSizesBytes()->at(0).at(1) = 1024;
    child.config()->allToAll() = a2a;
    workCfg_.config()->batch()->children()->emplace_back(child);
    workCfg_.config()->batch()->children()->emplace_back(std::move(child));
  }
};

TEST_F(BatchNoTrafficChildTest, RanksWithoutTrafficCompleteEveryChild) {
  run();
  validateStats();
}

/** A zero-size Permutation child ends its trace event before the next child **/
class BatchZeroSizePermutationTest : public packsim::WorkloadRankTestHarness,
                                     public ::testing::Test {
 protected:
  static constexpr uint32_t kNumRanks = 2;

  void SetUp() override {
    workCfg_.name() = "Batch";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = kNumRanks;
    workCfg_.config()->batch() = packsim::workload::thrift::Batch();

    packsim::workload::thrift::Workload child;
    child.name() = "PERM_ZERO";
    child.workloadId() = 0;
    child.numEndpoints() = kNumRanks;
    auto perm = packsim::workload::thrift::Permutation();
    perm.sizeBytes() = 0;
    perm.numChunks() = 1;
    perm.dstRanks() = {1, 0};
    child.config()->permutation() = perm;
    workCfg_.config()->batch()->children()->emplace_back(child);
    workCfg_.config()->batch()->children()->emplace_back(std::move(child));
  }
};

TEST_F(BatchZeroSizePermutationTest, RanksCompleteEveryChild) {
  run();
  validateStats();
}

TEST_F(BatchLateNotifyTest, IgnoresNotifiesAfterCompletion) {
  scheduler_.scheduleAfter(
      &simCtx_, Time::fromNs(kLateNs), [this]() { ranks_.at(1)->notify(0); });

  run();
  expectCompletedBeforeLateEvents();
  for (const auto& rs : rankStats_) {
    EXPECT_EQ(rs.sentMsgs, rs.ackdMsgs);
  }
  EXPECT_EQ(rankStats_.at(0).rcvdMsgs, rankStats_.at(2).rcvdMsgs + 1);
}

/** A batch needs at least one child **/
class BatchConfigValidationTest : public packsim::WorkloadRankTestHarness,
                                  public ::testing::Test {};

TEST_F(BatchConfigValidationTest, RejectsABatchWithNoChildren) {
  workCfg_.name() = "BATCH_CFG";
  workCfg_.workloadId() = 0;
  workCfg_.numEndpoints() = 2;
  workCfg_.config()->batch() = packsim::workload::thrift::Batch();
  EXPECT_THAT(
      [&]() { buildRanks(); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Workload BATCH_CFG: Batch children must not be empty")));
}

} // namespace packsim
