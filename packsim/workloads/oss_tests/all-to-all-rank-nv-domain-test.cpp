/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>

#include <folly/container/F14Map.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/workloads/all-to-all.h"
#include "packsim/workloads/oss_tests/rank-harness.h"

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim {

// Unit test for AllToAllRank::isInSameNvDomain function
class AllToAllRankNvDomainTest : public packsim::WorkloadRankTestHarness,
                                 public ::testing::Test {
 protected:
  void SetUp() override {
    workCfg_.name() = "A2A_UNIT_TEST";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = 8;

    auto a2a = packsim::workload::thrift::AllToAll();
    a2a.sizeBytes() = 1024;
    a2a.numChunks() = 1;
    a2a.numGpusPerNvDomain() = 4;
    workCfg_.config()->allToAll() = a2a;
  }

  // Helper to create a TestableAllToAllRank instance for testing
  std::unique_ptr<packsim::AllToAllRank> createAllToAllRank(uint32_t rank) {
    // Create callback functions
    auto sendCb = [this](packsim::Message* msg) { sendMessage(msg); };
    auto recvCb = [this](packsim::Message* msg) { recvMessage(msg); };
    auto completeCb = [this](uint32_t workloadId, uint32_t rank) {
      rankComplete(workloadId, rank);
    };

    // Initialize params with reference members
    packsim::WorkloadRankParams params{
        .simCtx = &simCtx_,
        .workloadId = 0,
        .workCfg = workCfg_,
        .rank = rank,
        .smcb = sendCb,
        .rmcb = recvCb,
        .rccb = completeCb,
        .parent = nullptr};
    return std::make_unique<packsim::AllToAllRank>(params);
  }
};

TEST_F(AllToAllRankNvDomainTest, TestUnsetMappingIgnoresNumGpusPerNvDomain) {
  // Without rankToNvDomain no domains are derived from numGpusPerNvDomain (4
  // in SetUp), so no two ranks share one.

  auto rank0 = createAllToAllRank(0);
  auto rank1 = createAllToAllRank(1);
  auto rank4 = createAllToAllRank(4);
  auto rank7 = createAllToAllRank(7);

  EXPECT_FALSE(rank0->isInSameNvDomain(1));
  EXPECT_FALSE(rank1->isInSameNvDomain(2));
  EXPECT_FALSE(rank4->isInSameNvDomain(3));
  EXPECT_FALSE(rank7->isInSameNvDomain(5));
}

TEST_F(AllToAllRankNvDomainTest, TestExplicitNvDomainMapping) {
  // Test explicit NV domain mapping using rankToNvDomain
  folly::F14FastMap<uint32_t, uint32_t> rankToNvDomain;

  // Custom mapping: ranks 0,2,4 in domain 0; ranks 1,3,5 in domain 1;
  // ranks 6,7 in domain 2
  rankToNvDomain[0] = 0;
  rankToNvDomain[1] = 1;
  rankToNvDomain[2] = 0;
  rankToNvDomain[3] = 1;
  rankToNvDomain[4] = 0;
  rankToNvDomain[5] = 1;
  rankToNvDomain[6] = 2;
  rankToNvDomain[7] = 2;

  workCfg_.rankToNvDomain() = std::move(rankToNvDomain);

  auto rank0 = createAllToAllRank(0);
  auto rank1 = createAllToAllRank(1);
  auto rank6 = createAllToAllRank(6);

  // Same NV domain tests (using explicit mapping)
  EXPECT_TRUE(rank0->isInSameNvDomain(2)); // Both in domain 0
  EXPECT_TRUE(rank0->isInSameNvDomain(4)); // Both in domain 0
  EXPECT_TRUE(rank1->isInSameNvDomain(3)); // Both in domain 1
  EXPECT_TRUE(rank1->isInSameNvDomain(5)); // Both in domain 1
  EXPECT_TRUE(rank6->isInSameNvDomain(7)); // Both in domain 2

  // Different NV domain tests (using explicit mapping)
  EXPECT_FALSE(rank0->isInSameNvDomain(1)); // domain 0 vs domain 1
  EXPECT_FALSE(rank0->isInSameNvDomain(3)); // domain 0 vs domain 1
  EXPECT_FALSE(rank0->isInSameNvDomain(6)); // domain 0 vs domain 2
  EXPECT_FALSE(rank1->isInSameNvDomain(2)); // domain 1 vs domain 0
  EXPECT_FALSE(rank6->isInSameNvDomain(0)); // domain 2 vs domain 0

  // Self test (same rank)
  EXPECT_TRUE(rank0->isInSameNvDomain(0));
  EXPECT_TRUE(rank1->isInSameNvDomain(1));
  EXPECT_TRUE(rank6->isInSameNvDomain(6));
}

TEST_F(AllToAllRankNvDomainTest, TestExplicitMappingWithMissingRank) {
  // Test explicit mapping where a rank is missing from the map
  folly::F14FastMap<uint32_t, uint32_t> rankToNvDomain;

  // Only map some ranks, leave rank 7 unmapped
  rankToNvDomain[0] = 0;
  rankToNvDomain[1] = 1;
  rankToNvDomain[2] = 0;

  workCfg_.rankToNvDomain() = std::move(rankToNvDomain);

  auto rank0 = createAllToAllRank(0);
  auto rank7 = createAllToAllRank(7);

  // Test with mapped ranks
  EXPECT_TRUE(rank0->isInSameNvDomain(2)); // Both in domain 0
  EXPECT_FALSE(rank0->isInSameNvDomain(1)); // domain 0 vs domain 1

  EXPECT_THAT(
      [&]() { rank0->isInSameNvDomain(7); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Rank 7 not found in rankToNvDomain")));

  EXPECT_THAT(
      [&]() { rank7->isInSameNvDomain(0); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Rank 7 not found in rankToNvDomain")));

  EXPECT_TRUE(rank7->isInSameNvDomain(7));
}

} // namespace packsim
