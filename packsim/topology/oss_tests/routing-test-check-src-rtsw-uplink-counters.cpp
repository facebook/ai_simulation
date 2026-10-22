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
#include "packsim/topology/oss_tests/routing-test.h"
#include "packsim/util/config-builders.h"

namespace packsim {
namespace {

TEST_P(DprRoutingTest, CheckSrcRtswUplinkCounters) {
  expectSrcRtswUplinkCounters();
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
