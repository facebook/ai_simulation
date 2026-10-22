/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <map>
#include <stdexcept>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/transport/transport.h"
#include "packsim/workloads/parallel_qp/channel_manager.h"
#include "packsim/workloads/parallel_qp/params.h"

namespace packsim {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

// ChannelManager cuts each peer's queue-pair list into numChannels channels of
// numQPsPerChannel queue pairs when it is constructed, so these cases build it
// directly against a hand-made qpnPairMap; nothing is sent.
class ChannelManagerConstructionTest : public ::testing::Test {
 protected:
  static constexpr uint32_t kNumChannels = 2;
  static constexpr uint32_t kNumQPsPerChannel = 2;
  static constexpr uint32_t kWorkloadId = 7;

  ChannelParams makeParams(uint32_t rank) const {
    return ChannelParams{
        .smcb = noopSubmitMessage_,
        .rmcb = noopSubmitMessage_,
        .numChannels = kNumChannels,
        .numQPsPerChannel = kNumQPsPerChannel,
        .messageSizeBytes = 1024,
        .workloadId = kWorkloadId,
        .workload = nullptr,
        .rankToIdList = rankToIdList_,
        .qpPairMap = qpPairMap_,
        .rank = rank,
    };
  }

  // Give `fromRank` `count` queue pairs to `toRank`, with distinct QPNs.
  void addQueuePairs(uint32_t fromRank, uint32_t toRank, uint32_t count) {
    auto& qpList = qpPairMap_[fromRank][toRank];
    for (uint32_t i = 0; i < count; ++i) {
      auto& qpnPair = qpList.emplace_back();
      qpnPair.local() = ++nextQpn_;
      qpnPair.remote() = ++nextQpn_;
    }
  }

  ChannelParams::SubmitMessageFn noopSubmitMessage_ = [](Message*) {};
  std::map<uint32_t, std::vector<std::vector<uint32_t>>> rankToIdList_{
      {0, {{0}}},
      {1, {{1}}},
      {2, {{2}}}};
  packsim::workload::thrift::QPNPairMapMap qpPairMap_;
  uint32_t nextQpn_{0};
};

// An incast rank above numSources, or a compute rank, has no peers, so the
// workload builds no queue pairs for it.
TEST_F(ChannelManagerConstructionTest, RankWithoutQueuePairsIsAccepted) {
  addQueuePairs(/*fromRank=*/0, /*toRank=*/1, kNumChannels * kNumQPsPerChannel);
  addQueuePairs(/*fromRank=*/1, /*toRank=*/0, kNumChannels * kNumQPsPerChannel);

  EXPECT_NO_THROW(ChannelManager{makeParams(/*rank=*/2)});
}

TEST_F(
    ChannelManagerConstructionTest,
    QueuePairListThatFillsEveryChannelIsAccepted) {
  addQueuePairs(/*fromRank=*/0, /*toRank=*/1, kNumChannels * kNumQPsPerChannel);

  EXPECT_NO_THROW(ChannelManager{makeParams(/*rank=*/0)});
}

// The last channel's slice would run past the end of a shorter list.
TEST_F(
    ChannelManagerConstructionTest,
    QueuePairListShorterThanChannelsIsRejected) {
  addQueuePairs(
      /*fromRank=*/0, /*toRank=*/1, kNumChannels * kNumQPsPerChannel - 1);

  EXPECT_THAT(
      [this]() { ChannelManager{makeParams(/*rank=*/0)}; },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Workload 7: rank 0 has 3 queue pairs to rank 1 in qpnPairMap, but "
          "channelSettings needs 4 (2 channels x 2 queue pairs per channel)")));
}

} // namespace packsim
