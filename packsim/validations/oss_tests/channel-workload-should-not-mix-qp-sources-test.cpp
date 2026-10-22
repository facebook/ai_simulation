/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/validations/channel-workload-should-not-mix-qp-sources.h"
#include "packsim/validations/oss_tests/channel-workload-should-not-mix-qp-sources-test-fixture.h"

namespace packsim::details {

using ::testing::HasSubstr;
using Fixture = ChannelWorkloadShouldNotMixQpSourcesTestFixture;

TEST(ChannelWorkloadShouldNotMixQpSourcesTest, AcceptsChannelsAlone) {
  const auto simCfg = Fixture::makeChannelSimulation();

  EXPECT_FALSE(
      ChannelWorkloadShouldNotMixQpSources()
          .validate(simCfg, /*ignoreWarnings=*/false)
          .has_value());
}

TEST(ChannelWorkloadShouldNotMixQpSourcesTest, FlagsQpTemplates) {
  auto simCfg = Fixture::makeChannelSimulation();
  Fixture::getOnlyWorkload(simCfg).qpTemplates()->emplace_back();

  const auto result = ChannelWorkloadShouldNotMixQpSources().validate(
      simCfg, /*ignoreWarnings=*/false);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->errors.size(), 1);
  EXPECT_THAT(
      *result->errors.begin(),
      HasSubstr("Workload a2a sets channelSettings together with qpTemplates"));
}

TEST(
    ChannelWorkloadShouldNotMixQpSourcesTest,
    FlagsANicQpnPairMapWithoutItsQpnPairMap) {
  auto simCfg = Fixture::makeChannelSimulation();
  (*Fixture::getOnlyWorkload(simCfg).nicQpnPairMap())[thrift::RankPair{}];

  const auto result = ChannelWorkloadShouldNotMixQpSources().validate(
      simCfg, /*ignoreWarnings=*/false);

  ASSERT_TRUE(result.has_value());
  EXPECT_THAT(
      *result->errors.begin(),
      HasSubstr("a nicQpnPairMap without its qpnPairMap"));
}

// fixupSimCfg derives the two maps together, so a config it already fixed up
// carries both.
TEST(
    ChannelWorkloadShouldNotMixQpSourcesTest,
    AcceptsANicQpnPairMapWithItsQpnPairMap) {
  auto simCfg = Fixture::makeChannelSimulation();
  (*Fixture::getOnlyWorkload(simCfg).nicQpnPairMap())[thrift::RankPair{}];
  (*Fixture::getOnlyWorkload(simCfg).qpnPairMap())[0][1].emplace_back();

  EXPECT_FALSE(
      ChannelWorkloadShouldNotMixQpSources()
          .validate(simCfg, /*ignoreWarnings=*/false)
          .has_value());
}

TEST(
    ChannelWorkloadShouldNotMixQpSourcesTest,
    IgnoresWorkloadsWithoutChannels) {
  auto simCfg = Fixture::makeChannelSimulation();
  Fixture::getOnlyWorkload(simCfg).channelSettings().reset();
  Fixture::getOnlyWorkload(simCfg).qpTemplates()->emplace_back();

  EXPECT_FALSE(
      ChannelWorkloadShouldNotMixQpSources()
          .validate(simCfg, /*ignoreWarnings=*/false)
          .has_value());
}

} // namespace packsim::details
