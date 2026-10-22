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

#include <gtest/gtest.h>

#include "packsim/endpoint/runtime/workload-node.h"
#include "packsim/kernel/device-id.h"
#include "packsim/topology/oss_tests/clos-coordinates.h"
#include "packsim/topology/oss_tests/routing-test-qos.h"
#include "packsim/util/config-builders.h"

namespace packsim {
namespace {

TEST_P(DprRoutingQosTest, CheckSrcRtswUplinkCounters) {
  auto numUplinks = cliCfg_.numCtsws;

  auto srcDevId = src_->getFlexId();
  auto dstDevId = dst_->getFlexId();
  auto srcInfo = packsim::ClosCoordinates::fromFlexId(srcDevId);
  auto dstInfo = packsim::ClosCoordinates::fromFlexId(dstDevId);

  auto accelPerHost = cliCfg_.numAccelPerHost;
  auto dprRail = (dstInfo.host * accelPerHost + dstInfo.accel) % numUplinks;

  bool sameRack =
      (srcInfo.zone == dstInfo.zone) && (srcInfo.rack == dstInfo.rack);

  // We expect to have one packet transmitted on the correct uplink and no other
  // packets transmitted.
  std::vector<std::vector<uint64_t>> expUpTxCounts(
      numUplinks, std::vector<uint64_t>(kNumQosQueues, 0));
  for (uint8_t q = 0; q < kNumQosQueues; ++q) {
    if (sameRack) {
      expUpTxCounts[dprRail][q] = 0;
    } else {
      expUpTxCounts[dprRail][q] = (q == dataCos_) ? 1 : 0;
    }
  }

  auto upCounts = GetUplinkCounts(srcRtsw_);
  auto upTxCounts = upCounts.first;
  EXPECT_EQ(upTxCounts, expUpTxCounts);

  uint64_t totalTxPkts = 0;
  for (const auto& e : upTxCounts) {
    for (uint8_t q = 0; q < kNumQosQueues; ++q) {
      totalTxPkts += e[q];
    }
  }
  EXPECT_EQ(totalTxPkts, sameRack ? 0U : 1U);
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
