/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <tuple>

#include <folly/Portability.h> // For IsSanitizeThread
#include <folly/logging/xlog.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/workloads/oss_tests/rank-harness.h"

namespace packsim {

class AllToAllWorkloadRankParallelQPTest
    : public packsim::WorkloadRankTestHarness,
      public ::testing::TestWithParam<std::tuple<
          uint32_t /* endpoints */,
          uint32_t /* size */,
          uint32_t /* chunks */,
          uint32_t /* channels */,
          uint32_t /* qps per channel */>> {
 public:
  void SetUp() override {
    workCfg_.name() = "A2A";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());

    auto a2a = packsim::workload::thrift::AllToAll();
    a2a.sizeBytes() = std::get<1>(GetParam());
    a2a.numChunks() = std::get<2>(GetParam());
    auto channelSettings = packsim::workload::thrift::ChannelSettings();
    channelSettings.numChannels() = std::get<3>(GetParam());
    channelSettings.numQPsPerChannel() = std::get<4>(GetParam());
    workCfg_.channelSettings() = channelSettings;

    a2a.usePxn() = true;
    a2a.numGpusPerNvDomain() = 8;
    workCfg_.config()->allToAll() = a2a;

    skipTest_ = (folly::kIsSanitizeThread || folly::kIsSanitizeAddress) &&
        std::get<0>(GetParam()) > 64;
  }

  bool skipTest_ = false;
};

TEST_P(AllToAllWorkloadRankParallelQPTest, Validate) {
  if (skipTest_) {
    // NOTE: Not using GTEST_SKIP to avoid generating warnings on diffs.
    // GTEST_SKIP() << "Skipping test due to a sanitizer and large number of
    // endpoints.";
    auto const numEndpoints = std::get<0>(GetParam());
    XLOGF(
        WARN, "Skipping sanitizer test for large #endpoints: {}", numEndpoints);
    return;
  }
  run();
  validateStats();
}

} // namespace packsim
