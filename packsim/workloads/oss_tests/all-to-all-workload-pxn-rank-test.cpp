/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/workloads/oss_tests/rank-harness.h"

namespace packsim {

class AllToAllWorkloadPxnRankTest : public packsim::WorkloadRankTestHarness,
                                    public ::testing::TestWithParam<std::tuple<
                                        uint32_t /* endpoints */,
                                        uint32_t /* size */,
                                        uint32_t /* chunks */>> {
 public:
  void SetUp() override {
    workCfg_.name() = "A2A";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());

    auto a2a = packsim::workload::thrift::AllToAll();
    a2a.usePxn() = true;
    a2a.numGpusPerNvDomain() = 8;
    a2a.sizeBytes() = std::get<1>(GetParam());
    a2a.numChunks() = std::get<2>(GetParam());
    workCfg_.config()->allToAll() = a2a;
    run();
  }
};

TEST_P(AllToAllWorkloadPxnRankTest, Validate) {
  validateStatsPxn();
}

INSTANTIATE_TEST_SUITE_P(
    AllToAllVecWorkloadPxnRankTestSweep,
    AllToAllWorkloadPxnRankTest,
    ::testing::Combine(
        ::testing::Values(64, 128),
        ::testing::Values(1024),
        ::testing::Values(1)));

} // namespace packsim
