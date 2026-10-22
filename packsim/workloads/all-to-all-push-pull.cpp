/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/all-to-all-push-pull.h"

#include <numeric>
#include <stdexcept>

#include <fmt/format.h>
#include <folly/logging/xlog.h>

#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

AllToAllPushPullRank::AllToAllPushPullRank(const WorkloadRankParams& params)
    : AllToAllRank(params) {
  const auto& allToAllCfg = *workCfg_->config()->allToAll();
  workloadMode_ = *allToAllCfg.workloadMode();

  if (workloadMode_ != workload::thrift::WorkloadMode::PUSH &&
      workloadMode_ != workload::thrift::WorkloadMode::PULL) {
    throw std::invalid_argument(
        "AllToAllPushPullRank requires WorkloadMode PUSH or PULL");
  }

  // WRITE/READ operations skip the SEND/RECV and notify paths entirely.
  // Zero out base class counters and mark notifies sent to avoid spurious
  // error logs in the base class destructor.
  remRecvs_ = 0;
  remSends_ = 0;
  notifiesSent_ = true;

  if (workloadMode_ == workload::thrift::WorkloadMode::PULL &&
      hasChannelManagerInHierarchy()) {
    throw std::invalid_argument(
        fmt::format(
            "Workload {}: PULL mode cannot be combined with channelSettings "
            "(on this workload or on a batch that contains it): channels "
            "post plain receives rather than READs",
            workloadName_));
  }

  readMsgs_.resize(
      workloadMode_ == workload::thrift::WorkloadMode::PULL ? numRanks_ : 0);
  writeMsgs_.resize(
      workloadMode_ == workload::thrift::WorkloadMode::PUSH ? numRanks_ : 0);
}

void AllToAllPushPullRank::run() {
  traceEvent_ = simCtx_->tracer.startEvent(
      "AllToAllPushPullRank", "Workloads", workloadId_, rank_);

  if (rank_ == 0) {
    XLOGF(
        INFO,
        "AllToAllPushPullRank workloadMode={}",
        workloadMode_ == workload::thrift::WorkloadMode::PUSH ? "PUSH"
                                                              : "PULL");
  }

  runAfterStartDelay([this] {
    startTimeNs_ = getTimeNs();
    if (workloadMode_ == workload::thrift::WorkloadMode::PULL) {
      startReads();
    } else {
      startWrites();
    }
  });
}

void AllToAllPushPullRank::startWrites() {
  std::vector<uint32_t> offsets(numRanks_ - 1);
  std::iota(offsets.begin(), offsets.end(), 1U);

  for (const uint32_t rankOff : offsets) {
    const uint32_t peerRank = (rank_ + rankOff) % numRanks_;
    if (isPeerSkipped(peerRank)) {
      continue;
    }

    const auto numQps = getTotalDataQPCount(peerRank);
    // @oss-disable
    for (uint32_t chunk = 0, qpNumToUse = 0; chunk < numChunks_; chunk++) {
      const auto msgSize =
          getSendSize(peerRank, chunk) * (usePxn_ ? numGpusPerNvDomain_ : 1U);

      XLOGF(
          DBG,
          "AllToAllPushPullRank::write rank_: {}, peerRank: {}, chunkId: {}, msgSize: {} bytes",
          rank_,
          peerRank,
          chunk,
          msgSize);

      if (msgSize > 0) {
        std::unique_ptr<Message> msg;
        if (isNicAwareModeActive()) {
          // @oss-disable
          msg = sendWithQPIndex(
              peerRank,
              msgSize,
              chunk,
              qpNumToUse,
              /*sendDelayNs=*/0,
              /*writeImm=*/false,
              /*write=*/true);
          qpNumToUse = (qpNumToUse + 1) % numQps;
        } else {
          msg = send(
              peerRank,
              msgSize,
              chunk,
              /*endpointId=*/0,
              /*sendDelayNs=*/0,
              /*writeImm=*/false,
              /*write=*/true);
        }
        writeMsgs_[peerRank].try_emplace(chunk, std::move(msg));
        ++remWrites_;
      }
    }
  }

  if (remWrites_ == 0) {
    completeWorkload();
  }
}

void AllToAllPushPullRank::startReads() {
  std::vector<uint32_t> offsets(numRanks_ - 1);
  std::iota(offsets.begin(), offsets.end(), 1U);

  for (const uint32_t rankOff : offsets) {
    const uint32_t peerRank = (rank_ + rankOff) % numRanks_;
    if (isPeerSkipped(peerRank)) {
      continue;
    }

    const auto numQps = getTotalDataQPCount(peerRank);
    // @oss-disable
    for (uint32_t chunk = 0, qpNumToUse = 0; chunk < numChunks_; chunk++) {
      const auto msgSize =
          getRecvSize(peerRank, chunk) * (usePxn_ ? numGpusPerNvDomain_ : 1U);

      XLOGF(
          DBG,
          "AllToAllPushPullRank::read rank_: {}, peerRank: {}, chunkId: {}, msgSize: {} bytes",
          rank_,
          peerRank,
          chunk,
          msgSize);

      if (msgSize > 0) {
        std::unique_ptr<Message> msg;
        if (isNicAwareModeActive()) {
          // @oss-disable
          msg = recvWithQPIndex(
              peerRank,
              msgSize,
              chunk,
              qpNumToUse,
              /*sendDelayNs=*/0,
              /*isRead=*/true);
          qpNumToUse = (qpNumToUse + 1) % numQps;
        } else {
          msg = recv(
              peerRank,
              msgSize,
              chunk,
              /*endpointId=*/0,
              /*sendDelayNs=*/0,
              /*isRead=*/true);
        }
        readMsgs_[peerRank].try_emplace(chunk, std::move(msg));
        ++remReads_;
      }
    }
  }

  if (remReads_ == 0) {
    completeWorkload();
  }
}

void AllToAllPushPullRank::requestAcked(Message* msg) {
  if (workloadMode_ == workload::thrift::WorkloadMode::PULL) {
    auto& msgMap = readMsgs_.at(msg->srcRank);
    const auto it = msgMap.find(msg->connectionMessageId);
    if (it == msgMap.end()) {
      throw std::runtime_error(
          fmt::format(
              "Received unexpected READ CQE for chunk {} from {}",
              msg->connectionMessageId,
              msg->srcRank));
    }
    msgMap.erase(it);
    remReads_--;
  } else {
    auto& msgMap = writeMsgs_.at(msg->dstRank);
    const auto it = msgMap.find(msg->connectionMessageId);
    if (it == msgMap.end()) {
      throw std::runtime_error(
          fmt::format(
              "Received unexpected WRITE CQE for chunk {} to {}",
              msg->connectionMessageId,
              msg->dstRank));
    }
    msgMap.erase(it);
    remWrites_--;
  }
  checkCompletionAndContinue();
}

void AllToAllPushPullRank::recordEndTime() {
  endTimeNs_ = getTimeNs();
  traceEvent_->end(simCtx_->now());
}

void AllToAllPushPullRank::completeWorkload() {
  recordEndTime();
  onRankComplete(workloadId_, rank_);
}

void AllToAllPushPullRank::checkCompletionAndContinue() {
  if (complete()) {
    if (workloadMode_ == workload::thrift::WorkloadMode::PULL) {
      // On a transport that models RDMA READ, a READ completes at the
      // initiator when its read-response data arrives, but the responder's
      // read-response messages close only once the initiator's ACKs reach
      // them; RoCEv2 rejects READs. The rank's end time is recorded
      // here; rank-complete is raised kPullDrainDelayNs later so those
      // messages can close, which keeps the simulation running past the
      // last READ.
      recordEndTime();
      simCtx_->schedule(Time::fromNs(kPullDrainDelayNs), [this]() {
        onRankComplete(workloadId_, rank_);
      });
    } else {
      completeWorkload();
    }
  }
}

bool AllToAllPushPullRank::complete() {
  if (workloadMode_ == workload::thrift::WorkloadMode::PULL) {
    return remReads_ == 0;
  }
  return remWrites_ == 0;
}

uint64_t AllToAllPushPullRank::getExpectedBytes() {
  uint64_t numPeers = 0;
  const uint64_t pxnMultiplier = usePxn_ ? numGpusPerNvDomain_ : 1U;

  if (usePxn_) {
    numPeers = numRanks_ / numGpusPerNvDomain_ - 1;
  } else {
    for (uint32_t peer = 0; peer < numRanks_; ++peer) {
      if (peer != rank_ && !isInSameNvDomain(peer)) {
        ++numPeers;
      }
    }
  }

  // Data bytes only — no notify bytes for WRITE/READ operations
  const uint64_t dataBytes =
      uint64_t{messageSize_} * numChunks_ * pxnMultiplier;
  return numPeers * dataBytes;
}

} // namespace packsim
