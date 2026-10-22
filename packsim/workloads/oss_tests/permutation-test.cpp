/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <numeric>
#include <stdexcept>
#include <tuple>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/random.h"
#include "packsim/workloads/oss_tests/rank-harness.h"

namespace packsim {

class PermutationWorkloadRankTest
    : public packsim::WorkloadRankTestHarness,
      public ::testing::TestWithParam<
          std::tuple<uint32_t, uint32_t, uint32_t>> {
 public:
  void SetUp() override {
    workCfg_.name() = "Permutation";
    workCfg_.workloadId() = 0;
    auto numRanks = std::get<0>(GetParam());
    workCfg_.numEndpoints() = numRanks;

    auto perm = packsim::workload::thrift::Permutation();
    perm.sizeBytes() = std::get<1>(GetParam());
    perm.numChunks() = std::get<2>(GetParam());

    std::vector<uint32_t> ranks(numRanks);
    std::iota(ranks.begin(), ranks.end(), 0U);
    simCtx_.random.shuffle(ranks.begin(), ranks.end());
    perm.dstRanks()->resize(numRanks);
    for (uint32_t i = 0; i < numRanks; i++) {
      perm.dstRanks()[ranks[i]] = ranks[(i + 1) % numRanks];
    }
    workCfg_.config()->permutation() = perm;

    run();
  }
};

TEST_P(PermutationWorkloadRankTest, Validate) {
  validateStats();
}

INSTANTIATE_TEST_SUITE_P(
    PermutationWorkloadRankTestSweep,
    PermutationWorkloadRankTest,
    ::testing::Combine(
        ::testing::Values(8, 128, 256),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(1, 3, 7, 15)));

class PermutationZeroSizeTest : public packsim::WorkloadRankTestHarness,
                                public ::testing::Test {};

TEST_F(PermutationZeroSizeTest, RanksWithZeroSizeBytesComplete) {
  workCfg_.name() = "PERM_ZERO";
  workCfg_.workloadId() = 0;
  workCfg_.numEndpoints() = 2;
  auto perm = packsim::workload::thrift::Permutation();
  perm.sizeBytes() = 0;
  perm.numChunks() = 1;
  perm.dstRanks() = {1, 0};
  workCfg_.config()->permutation() = perm;

  run();

  validateStats();
}

class PermutationConfigValidationTest : public packsim::WorkloadRankTestHarness,
                                        public ::testing::Test {};

TEST_F(PermutationConfigValidationTest, RejectsZeroNumChunks) {
  workCfg_.name() = "PERM_CFG";
  workCfg_.workloadId() = 0;
  workCfg_.numEndpoints() = 2;
  auto perm = packsim::workload::thrift::Permutation();
  perm.sizeBytes() = 1024;
  perm.numChunks() = 0;
  perm.dstRanks() = {1, 0};
  workCfg_.config()->permutation() = perm;
  EXPECT_THAT(
      [&]() { buildRanks(); },
      ::testing::ThrowsMessage<std::invalid_argument>(::testing::HasSubstr(
          "Workload PERM_CFG: Permutation numChunks must be at least 1, got 0")));
}

} // namespace packsim
