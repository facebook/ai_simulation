/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <folly/lang/Hint.h>
#include <folly/logging/LogStreamProcessor.h>
#include <folly/logging/xlog.h>
#include <gtest/gtest.h>

#include "packsim/endpoint/runtime/workload-node.h"
#include "packsim/kernel/device-id.h"
#include "packsim/topology/oss_tests/clos-coordinates.h"
#include "packsim/topology/oss_tests/routing-test-qos.h"
#include "packsim/util/config-builders.h"

namespace packsim {
namespace {

TEST_P(DprRoutingQosTest, CheckDstRtswUplinkCounters) {
  auto srcDevId = src_->getFlexId();
  auto dstDevId = dst_->getFlexId();
  auto srcInfo = packsim::ClosCoordinates::fromFlexId(srcDevId);
  auto dstInfo = packsim::ClosCoordinates::fromFlexId(dstDevId);

  bool sameRack =
      (srcInfo.zone == dstInfo.zone) && (srcInfo.rack == dstInfo.rack);

  auto numUplinks = cliCfg_.numCtsws;
  auto accelPerHost = cliCfg_.numAccelPerHost;
  auto dprRail = (dstInfo.host * accelPerHost + dstInfo.accel) % numUplinks;

  std::vector<std::vector<uint64_t>> expUpRxCounts(
      numUplinks, std::vector<uint64_t>(kNumQosQueues, 0));
  for (uint8_t q = 0; q < kNumQosQueues; ++q) {
    if (sameRack) {
      expUpRxCounts[dprRail][q] = 0;
    } else {
      expUpRxCounts[dprRail][q] = (q == dataCos_) ? 1 : 0;
    }
  }

  auto upCounts = GetUplinkCounts(dstRtsw_);
  auto upRxCounts = upCounts.second;
  if (srcInfo.zone == dstInfo.zone) {
    XLOGF(INFO, "Src zone: {}, Dst zone: {}", srcInfo.zone, dstInfo.zone);
    EXPECT_EQ(upRxCounts, expUpRxCounts);
  }

  uint64_t totalRxPkts = 0;
  for (const auto& e : upRxCounts) {
    for (uint8_t q = 0; q < kNumQosQueues; ++q) {
      totalRxPkts += e[q];
    }
  }
  EXPECT_EQ(totalRxPkts, sameRack ? 0U : 1U);
}

// Small datacenter (32 accelerators per zone): one packet to each accelerator
// of two racks, which covers every RTSW rail
INSTANTIATE_TEST_SUITE_P(
    DprVariations128EachRailIntraZone,
    DprRoutingQosTest,
    ::testing::Combine(
        ::testing::Values(128u),
        ::testing::Values(0u),
        ::testing::Range(16u, 32u),
        ::testing::Range<uint8_t>(1, 3),
        ::testing::Range<uint8_t>(4, 7)));

INSTANTIATE_TEST_SUITE_P(
    DprVariations128EachRailInterZone,
    DprRoutingQosTest,
    ::testing::Combine(
        ::testing::Values(128u),
        ::testing::Values(0u),
        ::testing::Range(112u, 128u),
        ::testing::Range<uint8_t>(5, 7),
        ::testing::Range<uint8_t>(0, 4)));

} // namespace
} // namespace packsim
