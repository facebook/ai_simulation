/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/permutation-push-pull.h"

#include <stdexcept>

#include <fmt/format.h>
#include <folly/logging/xlog.h>

#include "packsim/kernel/ceil-divide.h"
#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

PermutationPushPullRank::PermutationPushPullRank(
    const WorkloadRankParams& params)
    : PermutationRank(params) {
  const auto& permCfg = *workCfg_->config()->permutation();
  workloadMode_ = *permCfg.workloadMode();

  if (workloadMode_ != workload::thrift::WorkloadMode::PUSH &&
      workloadMode_ != workload::thrift::WorkloadMode::PULL) {
    throw std::invalid_argument(
        "PermutationPushPullRank requires WorkloadMode PUSH or PULL");
  }

  // WRITE/READ operations skip the SEND/RECV and notify paths entirely.
  // Mark notify received to avoid spurious state in the base class.
  notifyRcvd_ = true;

  if (workloadMode_ == workload::thrift::WorkloadMode::PULL &&
      hasChannelManagerInHierarchy()) {
    throw std::invalid_argument(
        fmt::format(
            "Workload {}: PULL mode cannot be combined with channelSettings "
            "(on this workload or on a batch that contains it): channels "
            "post plain receives rather than READs",
            workloadName_));
  }
}

void PermutationPushPullRank::run() {
  traceEvent_ = simCtx_->tracer.startEvent(
      "PermutationPushPullRank", "Workloads", workloadId_, rank_);

  if (rank_ == 0) {
    XLOGF(
        INFO,
        "PermutationPushPullRank workloadMode={}",
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

void PermutationPushPullRank::startWrites() {
  const uint32_t maxMsgSize = ceilDivide32(sendSize_, numChunks_);
  uint64_t rem = sendSize_;
  uint32_t chunkId = 0;
  uint32_t qpNumToUse = chunkId;
  const auto numQps = getTotalDataQPCount(dstRank_);
  // @oss-disable

  while (rem > 0) {
    const uint32_t msgSize =
        rem < maxMsgSize ? static_cast<uint32_t>(rem) : maxMsgSize;
    rem -= msgSize;

    XLOGF(
        DBG,
        "PermutationPushPullRank::write rank_: {}, dstRank: {}, chunkId: {}, msgSize: {} bytes",
        rank_,
        dstRank_,
        chunkId,
        msgSize);

    std::unique_ptr<Message> msg;
    if (isNicAwareModeActive()) {
      // @oss-disable
      msg = sendWithQPIndex(
          dstRank_,
          msgSize,
          chunkId,
          qpNumToUse,
          /*sendDelayNs=*/0,
          /*writeImm=*/false,
          /*write=*/true);
      qpNumToUse = (qpNumToUse + 1) % numQps;
    } else {
      msg = send(
          dstRank_,
          msgSize,
          chunkId,
          /*endpointId=*/0,
          /*sendDelayNs=*/0,
          /*writeImm=*/false,
          /*write=*/true);
    }
    writeMsgs_.emplace(chunkId, std::move(msg));
    ++remWrites_;
    chunkId++;
  }

  if (remWrites_ == 0) {
    completeWorkload();
  }
}

void PermutationPushPullRank::startReads() {
  const uint32_t maxMsgSize = ceilDivide32(sendSize_, numChunks_);
  uint64_t rem = sendSize_;
  uint32_t chunkId = 0;
  uint32_t qpNumToUse = chunkId;
  const auto numQps = getTotalDataQPCount(srcRank_);
  // @oss-disable

  while (rem > 0) {
    const uint32_t msgSize =
        rem < maxMsgSize ? static_cast<uint32_t>(rem) : maxMsgSize;
    rem -= msgSize;

    XLOGF(
        DBG,
        "PermutationPushPullRank::read rank_: {}, srcRank: {}, chunkId: {}, msgSize: {} bytes",
        rank_,
        srcRank_,
        chunkId,
        msgSize);

    std::unique_ptr<Message> msg;
    if (isNicAwareModeActive()) {
      // @oss-disable
      msg = recvWithQPIndex(
          srcRank_,
          msgSize,
          chunkId,
          qpNumToUse,
          /*sendDelayNs=*/0,
          /*isRead=*/true);
      qpNumToUse = (qpNumToUse + 1) % numQps;
    } else {
      msg = recv(
          srcRank_,
          msgSize,
          chunkId,
          /*endpointId=*/0,
          /*sendDelayNs=*/0,
          /*isRead=*/true);
    }
    readMsgs_.emplace(chunkId, std::move(msg));
    ++remReads_;
    chunkId++;
  }

  if (remReads_ == 0) {
    completeWorkload();
  }
}

void PermutationPushPullRank::requestAcked(Message* msg) {
  if (workloadMode_ == workload::thrift::WorkloadMode::PULL) {
    const auto it = readMsgs_.find(msg->connectionMessageId);
    if (it == readMsgs_.end()) {
      throw std::runtime_error(
          fmt::format(
              "Received unexpected READ CQE for chunk {} from {}",
              msg->connectionMessageId,
              msg->srcRank));
    }
    readMsgs_.erase(it);
    remReads_--;
  } else {
    const auto it = writeMsgs_.find(msg->connectionMessageId);
    if (it == writeMsgs_.end()) {
      throw std::runtime_error(
          fmt::format(
              "Received unexpected WRITE CQE for chunk {} to {}",
              msg->connectionMessageId,
              msg->dstRank));
    }
    writeMsgs_.erase(it);
    remWrites_--;
  }
  checkCompletionAndContinue();
}

void PermutationPushPullRank::recordEndTime() {
  endTimeNs_ = getTimeNs();
  traceEvent_->end(simCtx_->now());
}

void PermutationPushPullRank::completeWorkload() {
  recordEndTime();
  onRankComplete(workloadId_, rank_);
}

void PermutationPushPullRank::checkCompletionAndContinue() {
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

bool PermutationPushPullRank::complete() {
  if (workloadMode_ == workload::thrift::WorkloadMode::PULL) {
    return remReads_ == 0;
  }
  return remWrites_ == 0;
}

uint64_t PermutationPushPullRank::getExpectedBytes() {
  // Data bytes only — no notify bytes for WRITE/READ operations
  return sendSize_;
}

} // namespace packsim
