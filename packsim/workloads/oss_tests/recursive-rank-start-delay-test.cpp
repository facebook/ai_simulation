/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/transport/transport.h"
#include "packsim/util/sim-fix.h"
#include "packsim/workloads/builder.h"
#include "packsim/workloads/rank/workload-rank.h"

using packsim::workload::thrift::AllReduce;
using packsim::workload::thrift::AllReduceAlgorithm;
using packsim::workload::thrift::ReduceScatter;
using packsim::workload::thrift::WorkloadSpecificConfig;
using ::testing::ElementsAre;
using ::testing::Pair;

namespace packsim {

namespace {
// Start delay of rank 0 in every test (ns).
constexpr uint64_t kStartDelayNs = 500;
// Bytes each workload reduces or gathers, split evenly over the two ranks.
constexpr uint64_t kSizeBytes = 1024;
// Size of each message in the one step two ranks take (bytes).
constexpr uint32_t kStepBytes = kSizeBytes / 2;
} // namespace

/**
 * Rank 0 of a two-rank recursive workload whose rank 0 starts kStartDelayNs
 * late. The rank's messages are captured, never delivered: a test hands the
 * rank its peer's messages itself.
 */
class RecursiveRankStartDelayTest : public ::testing::Test {
 protected:
  void SetUp() override {
    smcb_ = [this](Message* msg) {
      sentMsgs_.emplace_back(*msg);
      sentSizesAndTimesNs_.emplace_back(msg->sizeBytes, simCtx_.nowNs());
    };
    rmcb_ = [this](Message* msg) { postedRecvs_.emplace_back(*msg); };
    rccb_ = [](uint32_t /*workloadId*/, uint32_t /*rank*/) {};
  }

  std::unique_ptr<WorkloadRank> buildRank(
      const WorkloadSpecificConfig& config) {
    workCfg_.name() = "Recursive";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = 2;
    workCfg_.explicitStaggeredStartTimeNs() = {kStartDelayNs, 0};
    workCfg_.config() = config;
    std::map<uint32_t, std::vector<uint32_t>> rankToId;
    std::map<uint32_t, std::vector<std::vector<uint32_t>>> rankToIdList;
    for (uint32_t i = 0; i < 2; ++i) {
      rankToId[i] = {i};
      rankToIdList[i] = {{i}};
    }
    workCfg_.rankToId() = rankToId;
    workCfg_.rankToIdList() = rankToIdList;
    std::vector<workload::thrift::Workload> workCfgs({workCfg_});
    buildQPNMap(workCfgs);
    uint32_t nextQpn = 0;
    buildNicQPNMap(workCfgs, nextQpn);
    workCfg_ = std::move(workCfgs.at(0));
    return buildWorkloadRank(
        &simCtx_, /*workloadId=*/0, workCfg_, /*rank=*/0, smcb_, rmcb_, rccb_);
  }

  // A copy of the first receive the rank posted with `sizeBytes`.
  Message findPostedRecv(const uint32_t sizeBytes) const {
    for (const auto& msg : postedRecvs_) {
      if (msg.sizeBytes == sizeBytes) {
        return msg;
      }
    }
    ADD_FAILURE() << "no posted receive of " << sizeBytes << " bytes";
    return Message{};
  }

  // A copy of the first message the rank sent with `sizeBytes`.
  Message findSentMsg(const uint32_t sizeBytes) const {
    for (const auto& msg : sentMsgs_) {
      if (msg.sizeBytes == sizeBytes) {
        return msg;
      }
    }
    ADD_FAILURE() << "no sent message of " << sizeBytes << " bytes";
    return Message{};
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  workload::thrift::Workload workCfg_;
  std::function<void(Message*)> smcb_;
  std::function<void(Message*)> rmcb_;
  std::function<void(uint32_t, uint32_t)> rccb_;
  // Copies of the messages the rank sent, in order.
  std::vector<Message> sentMsgs_;
  // Size of each message the rank sent and when it sent it (ns), in order.
  std::vector<std::pair<uint32_t, uint64_t>> sentSizesAndTimesNs_;
  // Copies of the receives the rank posted, in order.
  std::vector<Message> postedRecvs_;
};

TEST_F(RecursiveRankStartDelayTest, ReduceScatterNotifiesItsPeerAfterTheDelay) {
  ReduceScatter rs;
  rs.sizeBytes() = kSizeBytes;
  rs.algorithm() = AllReduceAlgorithm::RECURSIVE_MSG_OPT;
  WorkloadSpecificConfig config;
  config.reduceScatter() = rs;
  const auto rank = buildRank(config);

  rank->run();
  scheduler_.run();

  EXPECT_THAT(
      sentSizesAndTimesNs_, ElementsAre(Pair(kNotifyMsgSize, kStartDelayNs)));
}

TEST_F(RecursiveRankStartDelayTest, AllReduceNotifiesItsPeerAfterTheDelay) {
  AllReduce ar;
  ar.sizeBytes() = kSizeBytes;
  ar.algorithm() = AllReduceAlgorithm::RECURSIVE_MSG_OPT;
  WorkloadSpecificConfig config;
  config.allReduce() = ar;
  const auto rank = buildRank(config);

  rank->run();
  scheduler_.run();

  EXPECT_THAT(
      sentSizesAndTimesNs_, ElementsAre(Pair(kNotifyMsgSize, kStartDelayNs)));
}

TEST_F(
    RecursiveRankStartDelayTest,
    AllReduceStartsAllGatherWithoutAnotherDelay) {
  AllReduce ar;
  ar.sizeBytes() = kSizeBytes;
  ar.algorithm() = AllReduceAlgorithm::RECURSIVE_MSG_OPT;
  WorkloadSpecificConfig config;
  config.allReduce() = ar;
  const auto rank = buildRank(config);
  rank->run();
  scheduler_.run();

  // Finish the reduce-scatter phase at the start time: the peer's notify
  // arrives, so the rank sends its data; the peer's data arrives; the rank's
  // data is acknowledged.
  Message peerNotify = findPostedRecv(kNotifyMsgSize);
  rank->requestReceived(&peerNotify);
  Message peerData = findPostedRecv(kStepBytes);
  rank->requestReceived(&peerData);
  Message ownData = findSentMsg(kStepBytes);
  rank->requestAcked(&ownData);

  // The all-gather phase notifies its peer at once.
  EXPECT_THAT(
      sentSizesAndTimesNs_,
      ElementsAre(
          Pair(kNotifyMsgSize, kStartDelayNs),
          Pair(kStepBytes, kStartDelayNs),
          Pair(kNotifyMsgSize, kStartDelayNs)));
}

} // namespace packsim
