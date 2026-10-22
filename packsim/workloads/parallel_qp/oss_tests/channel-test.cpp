/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <functional>
#include <map>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/workloads/parallel_qp/channel.h"

namespace packsim {

class ChannelDeathTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Setup rankToIdList with only rank 0
    rankToIdList_[0] = {{0}};

    // Setup QPNPair
    packsim::workload::thrift::QPNPair qpnPair;
    qpnPair.local() = 1;
    qpnPair.remote() = 2;
    qpList_.emplace_back(qpnPair);

    // Setup callbacks (no-op for death tests)
    smcb_ = [](packsim::Message*) {};
    rmcb_ = [](packsim::Message*) {};
  }

  std::map<uint32_t, std::vector<std::vector<uint32_t>>> rankToIdList_;
  std::vector<packsim::workload::thrift::QPNPair> qpList_;
  std::function<void(packsim::Message*)> smcb_;
  std::function<void(packsim::Message*)> rmcb_;
};

TEST_F(ChannelDeathTest, SendFailsWhenFromRankNotFound) {
  packsim::ChannelParams params{
      .smcb = smcb_,
      .rmcb = rmcb_,
      .numChannels = 1,
      .numQPsPerChannel = 1,
      .messageSizeBytes = 1024,
      .workloadId = 0,
      .workload = nullptr,
      .rankToIdList = rankToIdList_,
      .qpPairMap = {},
  };

  packsim::Channel channel(params, qpList_);

  // fromRank=99 is not in rankToIdList_ (only rank 0 exists)
  EXPECT_DEATH(
      channel.send(
          /*fromRank=*/99,
          /*toRank=*/0,
          /*sizeBytes=*/1024,
          /*chunkId=*/0,
          /*channelId=*/0,
          /*endpointId=*/0,
          /*sendDelayNs=*/0,
          /*writeImm=*/false,
          /*write=*/false,
          /*writeImmData=*/0),
      "Rank 99 not found in rankToIdList");
}

TEST_F(ChannelDeathTest, SendFailsWhenToRankNotFound) {
  packsim::ChannelParams params{
      .smcb = smcb_,
      .rmcb = rmcb_,
      .numChannels = 1,
      .numQPsPerChannel = 1,
      .messageSizeBytes = 1024,
      .workloadId = 0,
      .workload = nullptr,
      .rankToIdList = rankToIdList_,
      .qpPairMap = {},
  };

  packsim::Channel channel(params, qpList_);

  // fromRank=0 exists, but toRank=99 is not in rankToIdList_
  EXPECT_DEATH(
      channel.send(
          /*fromRank=*/0,
          /*toRank=*/99,
          /*sizeBytes=*/1024,
          /*chunkId=*/0,
          /*channelId=*/0,
          /*endpointId=*/0,
          /*sendDelayNs=*/0,
          /*writeImm=*/false,
          /*write=*/false,
          /*writeImmData=*/0),
      "Rank 99 not found in rankToIdList");
}

} // namespace packsim
