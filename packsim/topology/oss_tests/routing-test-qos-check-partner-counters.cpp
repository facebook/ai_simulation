/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>
#include <vector>

#include <folly/lang/Hint.h>
#include <gtest/gtest.h>

#include "packsim/link-layer/eth-port.h"
#include "packsim/link-layer/port-stats.h"
#include "packsim/switch/fb-switch.h"
#include "packsim/topology/oss_tests/routing-test-qos.h"

namespace packsim {
namespace {

TEST_P(DprRoutingQosTest, CheckPartnerCounters) {
  auto srcSum = srcRtsw_->getSummary();
  auto dstSum = dstRtsw_->getSummary();

  auto srcIntfStats = srcRtswPort_->getAllPortStats();
  auto dstIntfStats = dstRtswPort_->getAllPortStats();
  for (uint8_t i = 0; i < kNumQosQueues; ++i) {
    if (i == dataCos_) {
      EXPECT_EQ(srcIntfStats.at(i).rxPkts, 1);
      EXPECT_EQ(dstIntfStats.at(i).txPkts, 1);
    } else {
      EXPECT_EQ(srcIntfStats.at(i).rxPkts, 0);
      EXPECT_EQ(dstIntfStats.at(i).txPkts, 0);
    }
    if (i == ackCos_) {
      EXPECT_EQ(srcIntfStats.at(i).txPkts, 1);
      EXPECT_EQ(dstIntfStats.at(i).rxPkts, 1);
    } else {
      EXPECT_EQ(srcIntfStats.at(i).txPkts, 0);
      EXPECT_EQ(dstIntfStats.at(i).rxPkts, 0);
    }
  }
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
