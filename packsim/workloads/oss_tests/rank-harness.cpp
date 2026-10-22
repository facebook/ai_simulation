/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/oss_tests/rank-harness.h"

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "packsim/kernel/sim-context.h"
#include "packsim/util/sim-fix.h"
#include "packsim/workloads/all-to-all.h"
#include "packsim/workloads/builder.h"

namespace packsim {

namespace {
// Minimum spacing enforced between successive messages on a connection.
constexpr int kMessageGapNs = 100;
// Floor applied to a message's size when computing its send timing.
constexpr uint32_t kMinMessageSizeForTiming = 100U;
// Delay before an ACK is delivered back to the sender.
constexpr int kAckDelayNs = 100;
// Lookahead large enough that all events share one sim context (100us in fs).
constexpr int64_t kLookaheadTimeFs = 100'000'000'000;
// Endpoints are grouped this many ranks per host.
constexpr size_t kRanksPerHost = 8;
} // namespace

void WorkloadRankTestHarness::sendMessage(Message* msg) {
  auto nextMsgTime =
      nextMsgTime_[{msg->srcRank, msg->dstRank, msg->localQPN, msg->remoteQPN}];
  auto now = simCtx_.now();
  if (now > nextMsgTime) {
    nextMsgTime = now + Time::fromNs(kMessageGapNs);
  }
  nextMsgTime +=
      Time::fromNs(std::max(msg->sizeBytes, kMinMessageSizeForTiming));
  nextMsgTime_[{msg->srcRank, msg->dstRank, msg->localQPN, msg->remoteQPN}] =
      nextMsgTime;

  rankStats_.at(msg->srcRank).sentBytes += msg->sizeBytes;
  rankStats_.at(msg->srcRank).sentMsgs++;
  rankStats_.at(msg->srcRank).sentMsgsByEndpoint[msg->endpointId]++;
  scheduler_.scheduleAfter(
      &simCtx_,
      nextMsgTime - now,
      &WorkloadRankTestHarness::deliverMessage,
      this,
      msg);
}

void WorkloadRankTestHarness::recvMessage(Message* msg) {
  // Key is {srcRank, dstRank, srcQPN, dstQPN} so need to flip local and remote
  // here
  recvMsgs_[{msg->srcRank, msg->dstRank, msg->remoteQPN, msg->localQPN}]
      .emplace_back(msg);
}

void WorkloadRankTestHarness::rankComplete(
    uint32_t /* workloadId */,
    uint32_t rank) {
  EXPECT_LT(rank, ranks_.size());
  EXPECT_FALSE(completedRanks_.contains(rank));
  completedRanks_.emplace(rank);
}

void WorkloadRankTestHarness::buildRanks() {
  uint64_t startTimeNs = *workCfg_.startTimeNs();
  ranks_.clear();
  rankStats_.resize(*workCfg_.numEndpoints());
  auto smcb = [this](Message* msg) { sendMessage(msg); };
  auto rmcb = [this](Message* msg) { recvMessage(msg); };
  auto rccb = [this](uint32_t workloadId, uint32_t rank) {
    rankComplete(workloadId, rank);
  };
  for (uint32_t i = 0; i < *workCfg_.numEndpoints(); i++) {
    auto w = buildWorkloadRank(&simCtx_, 0, workCfg_, i, smcb, rmcb, rccb);
    scheduler_.scheduleAfter(
        &simCtx_, Time::fromNs(startTimeNs), &WorkloadRank::run, w.get());
    ranks_.emplace_back(std::move(w));
  }
}

void WorkloadRankTestHarness::deliverMessage(Message* msg) {
  auto& q =
      recvMsgs_[{msg->srcRank, msg->dstRank, msg->localQPN, msg->remoteQPN}];
  if (q.empty()) {
    throw std::runtime_error(
        fmt::format(
            "Queue should not be empty src {}, dst {}, local qpn {} remote {}",
            msg->srcRank,
            msg->dstRank,
            msg->localQPN,
            msg->remoteQPN));
  }
  if (msg->connectionMessageId != q.front()->connectionMessageId) {
    throw std::runtime_error(
        fmt::format(
            "Received message out of order src {}, dst {}, local qpn {} remote {} connection id {} expected connection id {}",
            msg->srcRank,
            msg->dstRank,
            msg->localQPN,
            msg->remoteQPN,
            msg->connectionMessageId,
            q.front()->connectionMessageId));
  }

  if (msg->sizeBytes != q.front()->sizeBytes) {
    throw std::runtime_error(
        fmt::format(
            "Received message with wrong size src {}, dst {}, local qpn {} remote {} size {} expected size {}",
            msg->srcRank,
            msg->dstRank,
            msg->localQPN,
            msg->remoteQPN,
            msg->sizeBytes,
            q.front()->sizeBytes));
  }

  rankStats_.at(msg->dstRank).rcvdBytes += msg->sizeBytes;
  rankStats_.at(msg->dstRank).rcvdMsgs++;
  rankStats_.at(msg->dstRank).rcvdMsgsByEndpoint[q.front()->dstEndpointId]++;
  const Time ackDelay =
      notifyAckDelay_.has_value() && ranks_.at(msg->srcRank)->isNotifyMsg(msg)
      ? *notifyAckDelay_
      : Time::fromNs(kAckDelayNs);
  ranks_.at(msg->dstRank)->requestReceived(q.front());
  q.pop_front();
  scheduler_.scheduleAfter(
      &simCtx_, ackDelay, &WorkloadRankTestHarness::ackMessage, this, *msg);
}

void WorkloadRankTestHarness::ackMessage(Message msg) {
  rankStats_.at(msg.srcRank).ackdBytes += msg.sizeBytes;
  rankStats_.at(msg.srcRank).ackdMsgs++;
  ranks_.at(msg.srcRank)->requestAcked(&msg);
}

void WorkloadRankTestHarness::run() {
  populateRankToId(workCfg_);

  std::vector<packsim::workload::thrift::Workload> workCfgs({workCfg_});
  packsim::buildQPNMap(workCfgs);
  uint32_t nextQpn = 0;
  packsim::buildNicQPNMap(workCfgs, nextQpn);
  workCfg_ = std::move(workCfgs.at(0));

  buildRanks();
  // All events should be on kDefaultSimCtx so we can set the lookahead to a
  // very large value (100us)
  scheduler_.setLookaheadTimeFs(kLookaheadTimeFs);
  scheduler_.run();
}

void WorkloadRankTestHarness::validateStatsPxn() {
  EXPECT_GT(ranks_.size(), 0) << "No ranks were created";
  for (const auto& rs : rankStats_) {
    EXPECT_EQ(rs.sentMsgs, rs.ackdMsgs)
        << "All received messages must be ACKed";
  }
  const size_t numPeerHosts = ranks_.size() / kRanksPerHost - 1;
  for (size_t i = 0; i < ranks_.size(); i++) {
    AllToAllRank* curr = dynamic_cast<AllToAllRank*>(ranks_[i].get());
    EXPECT_EQ(rankStats_[i].sentMsgs, numPeerHosts * 2);
    EXPECT_EQ(rankStats_[i].rcvdMsgs, numPeerHosts * 2);
    EXPECT_EQ(rankStats_[i].ackdMsgs, numPeerHosts * 2);
    for (size_t j = 0; j < ranks_.size(); j++) {
      if (i == j) {
        continue;
      }
      EXPECT_EQ(
          curr->notifyRcvd_[j],
          (i % kRanksPerHost == j % kRanksPerHost) ? 1 : 0);
    }
    EXPECT_EQ(curr->getExpectedBytes(), rankStats_[i].rcvdBytes);
  }
  EXPECT_EQ(completedRanks_.size(), ranks_.size());

  auto endTimeNs = simCtx_.nowNs();
  for (const auto& rank : ranks_) {
    auto res = rank->getThriftResult();
    EXPECT_LE(*res.startTimeNs(), endTimeNs);
    EXPECT_LE(*res.endTimeNs(), endTimeNs);
  }
}

void WorkloadRankTestHarness::validateStats() {
  EXPECT_GT(ranks_.size(), 0) << "No ranks were created";
  for (const auto& rs : rankStats_) {
    EXPECT_EQ(rs.sentMsgs, rs.ackdMsgs)
        << "All received messages must be ACKed";
  }
  for (size_t i = 0; i < ranks_.size(); i++) {
    EXPECT_EQ(ranks_[i]->getExpectedBytes(), rankStats_[i].rcvdBytes);
  }
  EXPECT_EQ(completedRanks_.size(), ranks_.size());

  auto endTimeNs = simCtx_.nowNs();
  for (const auto& rank : ranks_) {
    auto res = rank->getThriftResult();
    EXPECT_LE(*res.startTimeNs(), endTimeNs);
    EXPECT_LE(*res.endTimeNs(), endTimeNs);
  }
}

void WorkloadRankTestHarness::populateRankToId(
    packsim::workload::thrift::Workload& workCfg) {
  auto& rankToId = *workCfg.rankToId();
  auto& rankToIdList = *workCfg.rankToIdList();
  rankToId.clear();
  for (uint32_t i = 0; i < *workCfg.numEndpoints(); i++) {
    rankToId.insert({i, {i}});
    rankToIdList.insert({i, {{i}}});
  }
}

} // namespace packsim
