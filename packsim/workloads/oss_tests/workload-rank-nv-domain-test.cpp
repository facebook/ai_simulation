/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/workloads/oss_tests/rank-harness.h"

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim {

// Minimal concrete WorkloadRank subclass to test base class methods directly
class TestableWorkloadRank : public WorkloadRank {
 public:
  using WorkloadRank::WorkloadRank;
  void run() override {}

  uint64_t getExpectedBytes() override {
    return 0;
  }

  bool complete() override {
    return true;
  }

  using WorkloadRank::isInSameNvDomain;
};

class WorkloadRankNvDomainTest : public WorkloadRankTestHarness,
                                 public ::testing::Test {
 protected:
  void SetUp() override {
    workCfg_.name() = "NV_DOMAIN_UNIT_TEST";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = 8;

    populateRankToId(workCfg_);
  }

  std::unique_ptr<TestableWorkloadRank> createRank(uint32_t rank) {
    auto sendCb = [this](Message* msg) { sendMessage(msg); };
    auto recvCb = [this](Message* msg) { recvMessage(msg); };
    auto completeCb = [this](uint32_t workloadId, uint32_t rank) {
      rankComplete(workloadId, rank);
    };

    WorkloadRankParams params{
        .simCtx = &simCtx_,
        .workloadId = 0,
        .workCfg = workCfg_,
        .rank = rank,
        .smcb = sendCb,
        .rmcb = recvCb,
        .rccb = completeCb,
        .parent = nullptr};
    return std::make_unique<TestableWorkloadRank>(params);
  }
};

TEST_F(WorkloadRankNvDomainTest, ReturnsFalseWhenNvDomainMapNotSet) {
  // Without rankToNvDomain, isInSameNvDomain should always return false
  auto rank0 = createRank(0);
  auto rank1 = createRank(1);

  EXPECT_FALSE(rank0->isInSameNvDomain(1));
  EXPECT_FALSE(rank1->isInSameNvDomain(0));
  EXPECT_FALSE(rank0->isInSameNvDomain(4));
}

TEST_F(WorkloadRankNvDomainTest, ExplicitNvDomainMapping) {
  folly::F14FastMap<uint32_t, uint32_t> rankToNvDomain;

  // ranks 0,2,4 in domain 0; ranks 1,3,5 in domain 1; ranks 6,7 in domain 2
  rankToNvDomain[0] = 0;
  rankToNvDomain[1] = 1;
  rankToNvDomain[2] = 0;
  rankToNvDomain[3] = 1;
  rankToNvDomain[4] = 0;
  rankToNvDomain[5] = 1;
  rankToNvDomain[6] = 2;
  rankToNvDomain[7] = 2;

  workCfg_.rankToNvDomain() = std::move(rankToNvDomain);

  auto rank0 = createRank(0);
  auto rank1 = createRank(1);
  auto rank6 = createRank(6);

  // Same NV domain
  EXPECT_TRUE(rank0->isInSameNvDomain(2));
  EXPECT_TRUE(rank0->isInSameNvDomain(4));
  EXPECT_TRUE(rank1->isInSameNvDomain(3));
  EXPECT_TRUE(rank1->isInSameNvDomain(5));
  EXPECT_TRUE(rank6->isInSameNvDomain(7));

  // Different NV domain
  EXPECT_FALSE(rank0->isInSameNvDomain(1));
  EXPECT_FALSE(rank0->isInSameNvDomain(3));
  EXPECT_FALSE(rank0->isInSameNvDomain(6));
  EXPECT_FALSE(rank1->isInSameNvDomain(2));
  EXPECT_FALSE(rank6->isInSameNvDomain(0));

  // Self test
  EXPECT_TRUE(rank0->isInSameNvDomain(0));
  EXPECT_TRUE(rank1->isInSameNvDomain(1));
  EXPECT_TRUE(rank6->isInSameNvDomain(6));
}

TEST_F(WorkloadRankNvDomainTest, ThrowsForMissingRankInExplicitMapping) {
  folly::F14FastMap<uint32_t, uint32_t> rankToNvDomain;
  rankToNvDomain[0] = 0;
  rankToNvDomain[1] = 1;
  rankToNvDomain[2] = 0;

  workCfg_.rankToNvDomain() = std::move(rankToNvDomain);

  auto rank0 = createRank(0);
  auto rank7 = createRank(7);

  // Known ranks work fine
  EXPECT_TRUE(rank0->isInSameNvDomain(2));
  EXPECT_FALSE(rank0->isInSameNvDomain(1));

  // Missing peer rank throws
  EXPECT_THAT(
      [&]() { rank0->isInSameNvDomain(7); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Rank 7 not found in rankToNvDomain")));

  // Missing own rank throws
  EXPECT_THAT(
      [&]() { rank7->isInSameNvDomain(0); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Rank 7 not found in rankToNvDomain")));

  // Self-check for unmapped rank still returns true (short-circuit)
  EXPECT_TRUE(rank7->isInSameNvDomain(7));
}

} // namespace packsim
