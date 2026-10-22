/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <stddef.h>
#include <cstdint>
#include <iostream>
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

TEST_P(DprRoutingQosTest, CheckDstAtswUplinkCounters) {
  auto srcDevId = src_->getFlexId();
  auto dstDevId = dst_->getFlexId();
  auto srcInfo = packsim::ClosCoordinates::fromFlexId(srcDevId);
  auto dstInfo = packsim::ClosCoordinates::fromFlexId(dstDevId);

  if (srcInfo.zone == dstInfo.zone) {
    return;
  }

  auto numZones = numAccel_ / cliCfg_.numAccelPerZone;
  auto numDnlinks = cliCfg_.numCtsws / kFourZoneClosPlanes *
      cliCfg_.numCtswAtswLinks * numZones;

  auto atswId = GetAtswId(dst_->getFlexId());

  auto dprRail = GetAtswDprRail(dst_->getFlexId());
  XLOGF(INFO, "DPR Rail: {}, Num Links: {}", dprRail, numDnlinks);

  std::vector<std::vector<uint64_t>> expDnTxCounts(
      numDnlinks, std::vector<uint64_t>(kNumQosQueues, 0));
  for (uint8_t q = 0; q < kNumQosQueues; ++q) {
    expDnTxCounts[dprRail][q] = (q == dataCos_) ? 1 : 0;
  }

  // for ack
  auto ackAtswId = GetAtswId(src_->getFlexId());
  if (ackAtswId == atswId) {
    dprRail = GetAtswDprRail(src_->getFlexId());
    expDnTxCounts[dprRail][ackCos_] = 1;
  }

  ASSERT_EQ(atsws_.size(), cliCfg_.numAtsws);

  auto dnCounts = GetDownlinkCounts(atsws_.at(atswId));
  auto upDnCounts = dnCounts.first;
  for (size_t i = 0; i < upDnCounts.size(); ++i) {
    for (uint8_t q = 0; q < kNumQosQueues; ++q) {
      if (upDnCounts[i][q] != 0) {
        std::cerr << "Found non zero at " << i << " size " << upDnCounts.size()
                  << std::endl;
      }
    }
  }
  EXPECT_EQ(expDnTxCounts, upDnCounts);
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
