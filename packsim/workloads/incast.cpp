/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/incast.h"

#include <algorithm>
#include <functional>
#include <stdexcept>

#include <fmt/format.h>
#include <folly/Conv.h>
#include <folly/Math.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

namespace packsim {

constexpr uint32_t kIncastDestinationRank = 0;
// The destination plus at least one source.
constexpr uint32_t kMinIncastEndpoints = 2;

IncastRank::IncastRank(const WorkloadRankParams& params)
    : WorkloadRank(params) {
  this->setOnRequestAckedCallback([this](Message* msg) { requestAcked(msg); });
  this->setOnRequestReceivedCallback(
      [this](Message* msg) { requestReceived(msg); });

  const auto& incastCfg = *workCfg_->config()->incast();
  sendSize_ = folly::to<uint32_t>(*incastCfg.sizeBytes());
  numChunks_ = *incastCfg.numChunks();
  requirePositiveField("Incast", "numChunks", numChunks_);
  numSrcs_ = resolveNumSources(*workCfg_);
  // @oss-disable

  workloadMode_ = *incastCfg.workloadMode();
  if (workloadMode_ != workload::thrift::WorkloadMode::SENDRCV &&
      workloadMode_ != workload::thrift::WorkloadMode::PUSH) {
    throw std::invalid_argument(
        fmt::format(
            "IncastRank only supports SENDRCV and PUSH modes, got {}",
            apache::thrift::util::enumNameSafe(workloadMode_)));
  }
}

uint32_t IncastRank::resolveNumSources(const workload::thrift::Workload& w) {
  const uint32_t numEndpoints = *w.numEndpoints();
  if (numEndpoints < kMinIncastEndpoints) {
    throw std::invalid_argument(
        fmt::format(
            "Incast workload '{}' needs at least {} endpoints, got {}",
            *w.name(),
            kMinIncastEndpoints,
            numEndpoints));
  }
  const uint32_t numSources = *w.config()->incast()->numSources();
  if (numSources == 0) {
    return numEndpoints - 1;
  }
  if (numSources >= numEndpoints) {
    throw std::invalid_argument(
        fmt::format(
            "Incast workload '{}' has numSources {}, but {} endpoints allow at most {}",
            *w.name(),
            numSources,
            numEndpoints,
            numEndpoints - 1));
  }
  return numSources;
}

void IncastRank::run() {
  traceEvent_ =
      simCtx_->tracer.startEvent("IncastRank", "Workloads", workloadId_, rank_);

  if (isPushMode()) {
    // PUSH mode: only ranks in [1, numSrcs_] write to rank 0, matching
    // SENDRCV semantics (where only those ranks are notified by rank 0).
    // No recv posting or notify exchange needed.
    if (isPushSender()) {
      runAfterStartDelay([this] {
        startTimeNs_ = getTimeNs();
        sendMsgs();
      });
    } else {
      // Destination and ranks beyond numSrcs_ have no work in PUSH mode —
      // complete immediately.
      completeWithoutWork();
    }
    return;
  }

  if (rank_ > numSrcs_) {
    // Rank 0 notifies only ranks 1..numSrcs_, so the rest have no work.
    completeWithoutWork();
    return;
  }

  if (rank_ != kIncastDestinationRank) {
    minSendTimeNs_ = getTimeNs() + startTimeDelayNs_;
    startTimeNs_ = minSendTimeNs_;
    return;
  }

  // Post recvs at rank 0
  const uint32_t maxMsgSize = folly::divCeil(sendSize_, numChunks_);
  recvMsgs_.resize(numRanks_);
  for (uint32_t srcRank = 1; srcRank < numSrcs_ + 1; srcRank++) {
    const auto numQps = getTotalDataQPCount(srcRank);
    uint32_t rem = sendSize_;
    uint32_t qpNumToUse = 0;
    // @oss-disable
    for (uint32_t i = 0; i < numChunks_; i++) {
      const auto msgSize = std::min(rem, maxMsgSize);
      rem -= msgSize;
      if (isNicAwareModeActive() && numQps > 0) {
        // @oss-disable
        recvMsgs_.at(srcRank).emplace_back(
            recvWithQPIndex(srcRank, msgSize, i, qpNumToUse));
        qpNumToUse = (qpNumToUse + 1) % numQps;
      } else {
        recvMsgs_.at(srcRank).emplace_back(recv(srcRank, msgSize, i));
      }
      ++outstandingRecvs_;
    }
    if (rem != 0) {
      throw std::runtime_error("Logic bug: recvs not fully posted");
    }
    notify(srcRank);
  }
}

void IncastRank::completeWithoutWork() {
  startTimeNs_ = getTimeNs();
  endTimeNs_ = startTimeNs_;
  traceEvent_->end(simCtx_->now());
  onRankComplete(workloadId_, rank_);
}

void IncastRank::progress() {
  if (notifyRcvd_) {
    return;
  }
  notifyRcvd_ = checkNotifyReceived(kIncastDestinationRank);
  if (notifyRcvd_) {
    const auto nowNs = getTimeNs();
    if (nowNs < minSendTimeNs_) {
      simCtx_->schedule(
          Time::fromNs(minSendTimeNs_ - nowNs), &IncastRank::sendMsgs, this);
    } else {
      sendMsgs();
    }
  }
}

void IncastRank::sendMsgs() {
  const auto numQps = getTotalDataQPCount(kIncastDestinationRank);
  const bool useOneSided = isPushMode();
  uint32_t rem = sendSize_;
  const uint32_t maxMsgSize = folly::divCeil(sendSize_, numChunks_);
  uint32_t qpNumToUse = 0;
  // @oss-disable
  for (uint32_t chunkId = 0; chunkId < numChunks_; chunkId++) {
    const auto msgSize = std::min(rem, maxMsgSize);
    rem -= msgSize;
    if (isNicAwareModeActive() && numQps > 0) {
      // @oss-disable
      sendMsgs_.emplace_back(sendWithQPIndex(
          kIncastDestinationRank,
          msgSize,
          chunkId,
          qpNumToUse,
          /*sendDelayNs=*/0,
          /*writeImm=*/false,
          /*write=*/useOneSided));
      qpNumToUse = (qpNumToUse + 1) % numQps;
    } else {
      sendMsgs_.emplace_back(send(
          kIncastDestinationRank,
          msgSize,
          chunkId,
          /*endpointId=*/0,
          /*sendDelayNs=*/0,
          /*writeImm=*/false,
          /*write=*/useOneSided));
    }
    if (useOneSided) {
      ++outstandingWrites_;
    }
  }
}

void IncastRank::requestReceived(Message* rcvdMsg) {
  if (isNotifyMsg(rcvdMsg)) {
    return notifyReceived(rcvdMsg->srcRank);
  }
  if (rank_ != kIncastDestinationRank) {
    throw std::runtime_error("Logic bug: only rank 0 should receive messages");
  }
  const auto srcRank = rcvdMsg->srcRank;
  auto& q = recvMsgs_.at(srcRank);
  if (q.empty()) {
    throw std::runtime_error("Logic bug: recvMsg Q is empty");
  }
  const auto it = std::find_if(q.begin(), q.end(), [&](const auto& msg) {
    return msg->connectionMessageId == rcvdMsg->connectionMessageId;
  });
  if (it == q.end()) {
    throw std::runtime_error(
        fmt::format(
            "Received unexpected message ID {} from rank {}",
            rcvdMsg->connectionMessageId,
            srcRank));
  }
  q.erase(it);
  --outstandingRecvs_;
  if (complete()) {
    endTimeNs_ = getTimeNs();
    traceEvent_->end(simCtx_->now());
    onRankComplete(workloadId_, rank_);
  }
}

void IncastRank::requestAcked(Message* sentMsg) {
  if (isNotifyMsg(sentMsg)) {
    return notifyAcked(sentMsg->dstRank);
  }
  if (rank_ == kIncastDestinationRank && !isPushMode()) {
    throw std::runtime_error("Logic bug: rank 0 should not send messages");
  }
  if (sendMsgs_.empty()) {
    throw std::runtime_error("Logic bug: sendMsg Q is empty");
  }
  const auto it =
      std::find_if(sendMsgs_.begin(), sendMsgs_.end(), [&](const auto& msg) {
        return msg->connectionMessageId == sentMsg->connectionMessageId;
      });
  if (it == sendMsgs_.end()) {
    throw std::runtime_error(
        fmt::format(
            "Got unexpected send CQE with message ID {}",
            sentMsg->connectionMessageId));
  }
  sendMsgs_.erase(it);
  if (isPushMode()) {
    --outstandingWrites_;
  }
  if (complete()) {
    endTimeNs_ = getTimeNs();
    traceEvent_->end(simCtx_->now());
    onRankComplete(workloadId_, rank_);
  }
}

uint64_t IncastRank::getExpectedBytes() {
  if (isPushMode()) {
    // One-sided WRITEs: each sender writes sendSize_ bytes, no notify overhead.
    // Only ranks in [1, numSrcs_] send; destination and out-of-range ranks
    // do nothing.
    return isPushSender() ? sendSize_ : 0;
  }
  if (rank_ == kIncastDestinationRank) {
    return uint64_t{sendSize_} * numSrcs_;
  }
  // A SENDRCV sender receives only rank 0's notify.
  return isSender() ? kNotifyMsgSize : 0;
}

bool IncastRank::complete() {
  if (isPushMode()) {
    // Only ranks in [1, numSrcs_] perform writes in PUSH mode; destination
    // and ranks beyond numSrcs_ have no work.
    return isPushSender() ? outstandingWrites_ == 0 : true;
  }
  if (rank_ == kIncastDestinationRank) {
    return outstandingRecvs_ == 0;
  }
  return !isSender() || (sendMsgs_.empty() && notifyRcvd_);
}

} // namespace packsim
