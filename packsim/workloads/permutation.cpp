/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/permutation.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <vector>

#include <fmt/format.h>
#include <folly/container/F14Set.h>
#include <folly/logging/xlog.h>

#include "packsim/kernel/ceil-divide.h"

namespace packsim {

PermutationRank::PermutationRank(const WorkloadRankParams& params)
    : WorkloadRank(params) {
  const auto& permCfg = *workCfg_->config()->permutation();
  validateDstRanks(*permCfg.dstRanks());
  for (uint32_t i = 0; i < permCfg.dstRanks()->size(); i++) {
    if (permCfg.dstRanks()->at(i) == rank_) {
      srcRank_ = i;
    }
  }
  dstRank_ = permCfg.dstRanks()->at(rank_);
  sendSize_ = *permCfg.sizeBytes();
  numChunks_ = *permCfg.numChunks();
  requirePositiveField("Permutation", "numChunks", numChunks_);
  // @oss-disable
  this->setOnRequestAckedCallback([this](Message* msg) { requestAcked(msg); });
  this->setOnRequestReceivedCallback(
      [this](Message* msg) { requestReceived(msg); });
}

void PermutationRank::run() {
  traceEvent_ = simCtx_->tracer.startEvent(
      "PermutationRank", "Workloads", workloadId_, rank_);
  if (sendSize_ == 0) {
    // Nothing to move: complete without exchanging the notify, as an AllToAll
    // rank with no traffic does.
    startTimeNs_ = getTimeNs();
    endTimeNs_ = startTimeNs_;
    traceEvent_->end(simCtx_->now());
    onRankComplete(workloadId_, rank_);
    return;
  }
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
    // Use round-robin QP distribution when NIC-aware mode is active,
    // otherwise fall back to legacy single-QP mode
    std::unique_ptr<Message> recvMsg;
    if (isNicAwareModeActive()) {
      // @oss-disable
      XLOGF(
          DBG, "recvWithQPIndex using QP {} for chunk {}", qpNumToUse, chunkId);
      recvMsg = recvWithQPIndex(srcRank_, msgSize, chunkId, qpNumToUse);
      qpNumToUse = (qpNumToUse + 1) % numQps;
    } else {
      recvMsg = recv(srcRank_, msgSize, chunkId);
    }
    recvMsgs_.emplace(chunkId, std::move(recvMsg));
    chunkId++;
  }
  simCtx_->schedule(
      Time::fromNs(startTimeDelayNs_), &PermutationRank::sendNotify, this);
}

void PermutationRank::sendNotify() {
  startTimeNs_ = getTimeNs();
  notify(srcRank_);
}

void PermutationRank::progress() {
  if (notifyRcvd_) {
    return;
  }
  if (!notifyRcvd_) {
    notifyRcvd_ = checkNotifyReceived(dstRank_);
  }
  if (notifyRcvd_) {
    sendMsgs();
  }
}

void PermutationRank::sendMsgs() {
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
    // Use round-robin QP distribution when NIC-aware mode is active,
    // otherwise fall back to legacy single-QP mode
    std::unique_ptr<Message> sendMsg;
    if (isNicAwareModeActive()) {
      // @oss-disable
      XLOGF(
          DBG, "sendWithQPIndex using QP {} for chunk {}", qpNumToUse, chunkId);
      sendMsg = sendWithQPIndex(dstRank_, msgSize, chunkId, qpNumToUse);
      qpNumToUse = (qpNumToUse + 1) % numQps;
    } else {
      sendMsg = send(dstRank_, msgSize, chunkId);
    }
    sendMsgs_.emplace(chunkId, std::move(sendMsg));
    chunkId++;
  }
}

void PermutationRank::requestReceived(Message* rcvdMsg) {
  if (isNotifyMsg(rcvdMsg)) {
    return notifyReceived(rcvdMsg->srcRank);
  }
  // Messages may arrive out of order when using multiple QPs/NICs.
  // Search for the matching message instead of expecting FIFO order.
  const auto it = recvMsgs_.find(rcvdMsg->connectionMessageId);
  if (it == recvMsgs_.end()) {
    throw std::runtime_error(
        fmt::format(
            "Rank {} received unexpected chunk {} from rank {}",
            rank_,
            rcvdMsg->connectionMessageId,
            rcvdMsg->srcRank));
  }
  recvMsgs_.erase(it);
  if (complete()) {
    endTimeNs_ = getTimeNs();
    traceEvent_->end(simCtx_->now());
    onRankComplete(workloadId_, rank_);
  }
}

void PermutationRank::requestAcked(Message* sentMsg) {
  if (isNotifyMsg(sentMsg)) {
    return notifyAcked(sentMsg->dstRank);
  }
  // Acks may arrive out of order when using multiple QPs/NICs.
  // Search for the matching message in the map instead of expecting FIFO order.
  const auto it = sendMsgs_.find(sentMsg->connectionMessageId);
  if (it == sendMsgs_.end()) {
    throw std::runtime_error(
        fmt::format(
            "Received unexpected CQE for chunk {} to {}",
            sentMsg->connectionMessageId,
            sentMsg->dstRank));
  }
  sendMsgs_.erase(it);
  if (complete()) {
    endTimeNs_ = getTimeNs();
    traceEvent_->end(simCtx_->now());
    onRankComplete(workloadId_, rank_);
  }
}

uint64_t PermutationRank::getExpectedBytes() {
  if (sendSize_ == 0) {
    return 0;
  }
  return sendSize_ + kNotifyMsgSize;
}

bool PermutationRank::complete() {
  return recvMsgs_.empty() && sendMsgs_.empty() && notifyRcvd_;
}

// validate the given matrix is correct
void PermutationRank::validateDstRanks(const std::vector<uint32_t>& dstRanks) {
  // first, the size must be numRanks
  if (dstRanks.size() != numRanks_) {
    throw std::runtime_error(
        fmt::format("Incorrect dst ranks size {}!", dstRanks.size()));
  }
  for (uint32_t i = 0; i < numRanks_; i++) {
    if (dstRanks.at(i) == i) {
      throw std::runtime_error(
          fmt::format("Cannot have rank {} sending to itself", i));
    }
  }
  // the elements must be exactly 0 ~ numRanks-1
  folly::F14FastSet<uint32_t> expRanks;
  expRanks.reserve(dstRanks.size());
  for (const auto& r : dstRanks) {
    expRanks.emplace(r);
  }
  for (uint32_t i = 0; i < numRanks_; i++) {
    if (!expRanks.contains(i)) {
      throw std::runtime_error(fmt::format("Rank {} not in dstRanks!", i));
    }
  }
}

} // namespace packsim
