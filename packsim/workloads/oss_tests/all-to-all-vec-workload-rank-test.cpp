/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <tuple>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/workloads/oss_tests/rank-harness.h"

namespace packsim {

class AllToAllVecWorkloadRankTest : public packsim::WorkloadRankTestHarness,
                                    public ::testing::TestWithParam<std::tuple<
                                        uint32_t /* ranks */,
                                        uint32_t /* size */,
                                        uint32_t /* chunks */>> {
 public:
  void SetUp() override {
    auto numRanks = std::get<0>(GetParam());
    auto sizeBytes = std::get<1>(GetParam());
    workCfg_.name() = "A2A";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = numRanks;

    auto a2a = packsim::workload::thrift::AllToAll();
    a2a.sizeBytes() = 0;
    a2a.numChunks() = std::get<2>(GetParam());
    a2a.vectorizedSizesBytes()->resize(numRanks);
    for (auto& v : *a2a.vectorizedSizesBytes()) {
      v.resize(numRanks);
    }

    const uint32_t x = numRanks / 2;
    const uint32_t y = numRanks / 3;
    a2a.vectorizedSizesBytes()->at(x).at(y) = sizeBytes;

    workCfg_.config()->allToAll() = a2a;

    run();
  }
};

TEST_P(AllToAllVecWorkloadRankTest, Validate) {
  validateStats();
}

// Ranks 0 and 1 share an NV domain, as do ranks 2 and 3. Rank 0 sends to rank
// 1, which the network never carries, and to rank 2, which it does; rank 3
// sends to rank 1, so rank 1 has network traffic of its own.
class AllToAllVecNvDomainTest : public packsim::WorkloadRankTestHarness,
                                public ::testing::Test {
 protected:
  static constexpr uint32_t kNumRanks = 4;

  void SetUp() override {
    workCfg_.name() = "A2AV_NV";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = kNumRanks;
    workCfg_.rankToNvDomain() = {{0, 0}, {1, 0}, {2, 1}, {3, 1}};

    auto a2a = packsim::workload::thrift::AllToAll();
    a2a.sizeBytes() = 0;
    a2a.numChunks() = 1;
    a2a.vectorizedSizesBytes()->resize(
        kNumRanks, std::vector<uint32_t>(kNumRanks, 0));
    a2a.vectorizedSizesBytes()->at(0).at(1) = 1024;
    a2a.vectorizedSizesBytes()->at(0).at(2) = 2048;
    a2a.vectorizedSizesBytes()->at(3).at(1) = 512;
    workCfg_.config()->allToAll() = a2a;
  }
};

TEST_F(AllToAllVecNvDomainTest, ExpectsBytesOnlyFromPeersOutsideTheNvDomain) {
  run();

  validateStats();
}

// With PXN, ranks 0 and 2 share rail 0 and ranks 1 and 3 rail 1; ranks 0 and
// 1, and ranks 2 and 3, share an NV domain. A rank receives each same-rail
// peer's data once for every GPU in the peer's NV domain, and nothing across
// rails or inside its own NV domain.
class AllToAllVecPxnTest : public packsim::WorkloadRankTestHarness,
                           public ::testing::Test {
 protected:
  static constexpr uint32_t kNumRanks = 4;

  void SetUp() override {
    workCfg_.name() = "A2AV_PXN";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = kNumRanks;
    workCfg_.rankToNvDomain() = {{0, 0}, {1, 0}, {2, 1}, {3, 1}};

    auto a2a = packsim::workload::thrift::AllToAll();
    a2a.sizeBytes() = 0;
    a2a.numChunks() = 3;
    a2a.usePxn() = true;
    a2a.numGpusPerNvDomain() = 2;
    auto& sizes = *a2a.vectorizedSizesBytes();
    sizes.resize(kNumRanks, std::vector<uint32_t>(kNumRanks, 0));
    sizes.at(0).at(2) = 1000;
    sizes.at(2).at(0) = 500;
    sizes.at(1).at(3) = 300;
    // Across rails and inside an NV domain: never carried.
    sizes.at(0).at(3) = 700;
    sizes.at(0).at(1) = 64;
    workCfg_.config()->allToAll() = a2a;
  }
};

TEST_F(AllToAllVecPxnTest, ExpectsEachSameRailPeersDataOncePerNvDomainGpu) {
  run();

  validateStats();
}

INSTANTIATE_TEST_SUITE_P(
    AllToAllVecWorkloadRankTestSweep,
    AllToAllVecWorkloadRankTest,
    ::testing::Combine(
        ::testing::Values(64, 128, 256),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(1, 2, 4)));

} // namespace packsim
