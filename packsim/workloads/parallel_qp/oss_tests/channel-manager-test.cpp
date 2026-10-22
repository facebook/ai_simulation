/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <memory>
#include <tuple>

#include <gtest/gtest.h>

#include "packsim/workloads/parallel_qp/oss_tests/channel-manager-harness.h"

namespace packsim {

class ChannelManagerTester
    : public ::testing::TestWithParam<
          std::tuple<uint32_t, uint32_t, uint32_t, uint32_t, uint32_t>> {
 public:
  void SetUp() override {}
};

TEST_P(ChannelManagerTester, Validate) {
  auto numChannels = std::get<0>(GetParam());
  auto numQPsPerChannel = std::get<1>(GetParam());
  auto channelMsgSizeBytes = std::get<2>(GetParam());
  auto numMessages = std::get<3>(GetParam());
  auto msgSizeBytes = std::get<4>(GetParam());

  auto channelManagerTest = std::make_unique<packsim::ChannelManagerHarness>(
      numChannels, numQPsPerChannel, channelMsgSizeBytes, numMessages);
  channelManagerTest->run(msgSizeBytes);
  channelManagerTest->validateStats();
}

TEST(ChannelManagerIterationTest, SendHandleCarriesWorkloadAndIteration) {
  ChannelManagerHarness harness(
      /* numChannels= */ 1,
      /* numQPsPerChannel= */ 1,
      /* channelMsgSizeBytes= */ 1024 * 1024,
      /* numMessages= */ 1);
  harness.channelManagerRank0->setIteration(3);

  const auto handle = harness.channelManagerRank0->send(
      /* fromRank= */ 0,
      /* toRank= */ 1,
      /* sizeBytes= */ 1024,
      /* chunkId= */ 0,
      /* endpointId= */ 0,
      /* sendDelayNs= */ 0,
      /* writeImm= */ false,
      /* write= */ false,
      /* writeImmData= */ 0);

  EXPECT_EQ(handle->workloadId, 0u);
  EXPECT_EQ(handle->iteration, 3u);
}

INSTANTIATE_TEST_SUITE_P(
    ChannelManagerTestSweep,
    ChannelManagerTester,
    ::testing::Combine(
        ::testing::Values(1, 10),
        ::testing::Values(1, 5),
        ::testing::Values(1024 * 1024 * 1024, 1024 * 1024),
        ::testing::Values(5),
        ::testing::Values(10 * 1024 * 1024, 100 * 1024 * 1024)));

} // namespace packsim
