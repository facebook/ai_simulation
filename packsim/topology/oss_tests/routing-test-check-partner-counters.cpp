/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>

#include <folly/lang/Hint.h>
#include <gtest/gtest.h>

#include "packsim/link-layer/eth-port.h"
#include "packsim/topology/oss_tests/routing-test.h"

namespace packsim {
namespace {

TEST_P(DprRoutingTest, CheckPartnerCounters) {
  expectPartnerCounters();
}

// Small datacenter (32 accelerators per zone): one packet to each accelerator
// of two racks, which covers every RTSW rail
INSTANTIATE_TEST_SUITE_P(
    DprVariations128EachRailIntraZone,
    DprRoutingTest,
    ::testing::Combine(
        ::testing::Values(128u),
        ::testing::Values(0u),
        ::testing::Range(16u, 32u)));

INSTANTIATE_TEST_SUITE_P(
    DprVariations128EachRailInterZone,
    DprRoutingTest,
    ::testing::Combine(
        ::testing::Values(128u),
        ::testing::Values(0u),
        ::testing::Range(112u, 128u)));

INSTANTIATE_TEST_SUITE_P(
    DprVariations256,
    DprRoutingTest,
    ::testing::Combine(
        ::testing::Values(256u),
        ::testing::Values(0u, 127u, 129u),
        ::testing::Values(53u, 12u, 255u)));

} // namespace
} // namespace packsim
