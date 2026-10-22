/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/all-to-all.h"

#include <limits>
#include <numeric>
#include <string>
#include <utility>

#include <fmt/format.h>
#include <folly/Conv.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>

namespace packsim {

AllToAllRank::AllToAllRank(const WorkloadRankParams& params)
    : WorkloadRank(params) {
  const auto& allToAllCfg = *workCfg_->config()->allToAll();
  numChunks_ = *allToAllCfg.numChunks();
  requirePositiveField("AllToAll", "numChunks", numChunks_);
  // @oss-disable
  messageSize_ = folly::to<uint32_t>(
      *allToAllCfg.sizeBytes() / (uint64_t{numRanks_} * numChunks_));
  maxChunksInFlight_ = *allToAllCfg.maxChunksInFlight();
  requirePositiveField("AllToAll", "maxChunksInFlight", maxChunksInFlight_);

  nextChunk_.resize(numRanks_);
  sendQpNumToUse_.resize(numRanks_, 0);
  notifyRcvd_.resize(numRanks_, false);
  sendMsgs_.resize(numRanks_);
  recvMsgs_.resize(numRanks_);

  this->setOnRequestAckedCallback([this](Message* msg) { requestAcked(msg); });
  this->setOnRequestReceivedCallback(
      [this](Message* msg) { requestReceived(msg); });

  usePxn_ = *allToAllCfg.usePxn();
  numGpusPerNvDomain_ = *allToAllCfg.numGpusPerNvDomain();
  if (numGpusPerNvDomain_ == 0) {
    throw std::invalid_argument(
        fmt::format(
            "numGpusPerNvDomain is {}, must be > 0!", numGpusPerNvDomain_));
  }
  // With PXN one message carries the chunks of every GPU in the NV domain, and
  // a message size is a `uint32_t`.
  const uint64_t pxnMessageBytes = uint64_t{messageSize_} * numGpusPerNvDomain_;
  if (usePxn_ && pxnMessageBytes > std::numeric_limits<uint32_t>::max()) {
    throw std::invalid_argument(
        fmt::format(
            "PXN message of {} bytes ({} bytes per rank times {} GPUs per NV "
            "domain) does not fit in 32 bits",
            pxnMessageBytes,
            messageSize_,
            numGpusPerNvDomain_));
  }
}

AllToAllRank::~AllToAllRank() {
  try {
    if (!complete()) {
      // Count remaining messages in queues for debugging
      size_t remainingSendMsgs = 0;
      size_t remainingRecvMsgs = 0;
      std::vector<std::pair<uint32_t, size_t>> sendQueueSizes;
      std::vector<std::pair<uint32_t, size_t>> recvQueueSizes;

      for (uint32_t peer = 0; peer < sendMsgs_.size(); ++peer) {
        if (!sendMsgs_[peer].empty()) {
          remainingSendMsgs += sendMsgs_[peer].size();
          sendQueueSizes.emplace_back(peer, sendMsgs_[peer].size());
        }
      }
      for (uint32_t peer = 0; peer < recvMsgs_.size(); ++peer) {
        if (!recvMsgs_[peer].empty()) {
          remainingRecvMsgs += recvMsgs_[peer].size();
          recvQueueSizes.emplace_back(peer, recvMsgs_[peer].size());
        }
      }

      XLOGF(
          ERR,
          "AllToAllRank destroyed without completing: rank={} workloadId={} "
          "remRecvs={} remSends={} notifiesSent={} "
          "remainingSendMsgs={} remainingRecvMsgs={}",
          rank_,
          workloadId_,
          remRecvs_,
          remSends_,
          notifiesSent_,
          remainingSendMsgs,
          remainingRecvMsgs);

      // Log which peers have remaining messages and their message IDs
      for (const auto& [peer, count] : sendQueueSizes) {
        std::vector<uint32_t> msgIds;
        msgIds.reserve(sendMsgs_[peer].size());
        for (const auto& [chunkId, msg] : sendMsgs_[peer]) {
          msgIds.emplace_back(msg->connectionMessageId);
        }
        XLOGF(
            ERR,
            "  rank={} has {} unsent messages to peer={}: [{}]",
            rank_,
            count,
            peer,
            folly::join(", ", msgIds));
      }
      for (const auto& [peer, count] : recvQueueSizes) {
        std::vector<uint32_t> msgIds;
        msgIds.reserve(recvMsgs_[peer].size());
        for (const auto& [chunkId, msg] : recvMsgs_[peer]) {
          msgIds.emplace_back(msg->connectionMessageId);
        }
        XLOGF(
            ERR,
            "  rank={} has {} unreceived messages from peer={}: [{}]",
            rank_,
            count,
            peer,
            folly::join(", ", msgIds));
      }
    }
  } catch (...) {
    // Swallow exceptions to prevent std::terminate during stack unwinding
  }
}

void AllToAllRank::run() {
  traceEvent_ = simCtx_->tracer.startEvent(
      "AllToAllRank", "Workloads", workloadId_, rank_);
  if (!hasTraffic()) {
    // Nothing to send or receive: done, without exchanging notifies.
    startTimeNs_ = getTimeNs();
    endTimeNs_ = startTimeNs_;
    traceEvent_->end(simCtx_->now());
    onRankComplete(workloadId_, rank_);
    return;
  }

  // Post all recvs
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
      // scale msgSize by number of rails for PXN
      const auto msgSize =
          getRecvSize(peerRank, chunk) * (usePxn_ ? numGpusPerNvDomain_ : 1U);

      XLOGF(
          DBG,
          "AllToAllRank::recv rank_: {}, rankOff: {}, peerRank: {}, chunkId: {}, msgSize: {} bytes",
          rank_,
          rankOff,
          peerRank,
          chunk,
          msgSize);
      if (msgSize) {
        // Use round-robin QP distribution when NIC-aware mode is active,
        // otherwise fall back to legacy single-QP mode
        std::unique_ptr<Message> recvMsg;
        if (isNicAwareModeActive()) {
          // @oss-disable
          XLOGF(
              DBG,
              "recvWithQPIndex using QP {} for chunk {}",
              qpNumToUse,
              chunk);
          recvMsg = recvWithQPIndex(peerRank, msgSize, chunk, qpNumToUse);
          qpNumToUse = (qpNumToUse + 1) % numQps;
        } else {
          recvMsg = recv(peerRank, msgSize, chunk);
        }
        recvMsgs_[peerRank].emplace(chunk, std::move(recvMsg));
        ++remRecvs_;
      }
      if (getSendSize(peerRank, chunk)) {
        // Check if we'll be sending this chunk either
        ++remSends_;
      }
    }
  }

  simCtx_->schedule(
      Time::fromNs(startTimeDelayNs_), &AllToAllRank::sendNotifies, this);
}

void AllToAllRank::sendNotifies() {
  startTimeNs_ = getTimeNs();
  for (uint32_t peer = 0; peer < numRanks_; peer++) {
    if (!isPeerSkipped(peer)) {
      notify(peer);
    }
  }
  notifiesSent_ = true;
  progress();
}

void AllToAllRank::progress() {
  // Check for outstanding notifications
  constexpr uint64_t wqePostDelayNs{0};

  // Randomly iterate over offsets to randomize QP memory request ordering
  std::vector<uint32_t> offsets(numRanks_ - 1);
  std::iota(offsets.begin(), offsets.end(), 1U);
  simCtx_->random.shuffle(offsets.begin(), offsets.end());

  for (const auto& rankOff : offsets) {
    const uint32_t peerRank = (rank_ + rankOff) % numRanks_;
    if (isPeerSkipped(peerRank)) {
      continue;
    }
    if (!notifyRcvd_[peerRank]) {
      notifyRcvd_[peerRank] = checkNotifyReceived(peerRank);
    }
    if (!notifyRcvd_[peerRank]) {
      continue;
    }

    const auto numQps = getTotalDataQPCount(peerRank);
    // Post messages if we have space
    while (sendMsgs_[peerRank].size() < maxChunksInFlight_ &&
           nextChunk_[peerRank] < numChunks_) {
      const uint32_t chunk = nextChunk_[peerRank]++;
      const auto msgSize =
          getSendSize(peerRank, chunk) * (usePxn_ ? numGpusPerNvDomain_ : 1U);
      XLOGF(
          DBG,
          "AllToAllRank::send rank_: {}, rankOff: {}, peerRank: {}, chunkId: {}, msgSize: {} bytes",
          rank_,
          rankOff,
          peerRank,
          chunk,
          msgSize);

      if (msgSize > 0) {
        const auto msgSendDelay =
            getSendDelay(peerRank, chunk) + wqePostDelayNs;
        // Use round-robin QP distribution when NIC-aware mode is active,
        // otherwise fall back to legacy single-QP mode
        std::unique_ptr<Message> sendMsg;
        if (isNicAwareModeActive()) {
          // @oss-disable
              // @oss-disable
          XLOGF(
              DBG,
              "sendWithQPIndex using QP {} for chunk {}",
              sendQpNumToUse_[peerRank],
              chunk);
          sendMsg = sendWithQPIndex(
              peerRank,
              msgSize,
              chunk,
              sendQpNumToUse_[peerRank],
              msgSendDelay);
          sendQpNumToUse_[peerRank] = (sendQpNumToUse_[peerRank] + 1) % numQps;
        } else {
          sendMsg = send(peerRank, msgSize, chunk, 0, msgSendDelay);
        }
        sendMsgs_[peerRank].emplace(chunk, std::move(sendMsg));
      }
    }
  }
}

void AllToAllRank::requestReceived(Message* msg) {
  if (isNotifyMsg(msg)) {
    return notifyReceived(msg->srcRank);
  }
  // Messages may arrive out of order when using multiple QPs/NICs.
  // Search for the matching message instead of expecting FIFO order.
  auto& recvMap = recvMsgs_.at(msg->srcRank);
  const auto it = recvMap.find(msg->connectionMessageId);
  if (it == recvMap.end()) {
    throw std::runtime_error(
        fmt::format(
            "Rank {} received unexpected chunk {} from rank {}",
            rank_,
            msg->connectionMessageId,
            msg->srcRank));
  }
  recvMap.erase(it);
  --remRecvs_;
  checkCompletionAndContinue();
}

void AllToAllRank::requestAcked(Message* msg) {
  if (isNotifyMsg(msg)) {
    return notifyAcked(msg->dstRank);
  }
  // Acks may arrive out of order when using multiple QPs/NICs.
  // Search for the matching message in the queue instead of expecting FIFO
  // order.
  auto& sendMap = sendMsgs_.at(msg->dstRank);
  const auto it = sendMap.find(msg->connectionMessageId);
  if (it == sendMap.end()) {
    throw std::runtime_error(
        fmt::format(
            "Received unexpected CQE for chunk {} to {}",
            msg->connectionMessageId,
            msg->dstRank));
  }
  sendMap.erase(it);
  --remSends_;
  checkCompletionAndContinue();
}

void AllToAllRank::checkCompletionAndContinue() {
  if (complete()) {
    endTimeNs_ = getTimeNs();
    traceEvent_->end(simCtx_->now());
    onRankComplete(workloadId_, rank_);
  } else {
    progress();
  }
}

bool AllToAllRank::complete() {
  return remRecvs_ == 0 && remSends_ == 0 && notifiesSent_;
}

uint64_t AllToAllRank::getExpectedBytes() {
  // A rank without traffic completes in `run()` without exchanging notifies.
  if (!hasTraffic()) {
    return 0;
  }
  if (!usePxn_) {
    uint64_t numPeers = 0;

    for (uint32_t peer = 0; peer < numRanks_; ++peer) {
      if (!isPeerSkipped(peer)) {
        ++numPeers;
      }
    }

    return numPeers * (uint64_t{messageSize_} * numChunks_ + kNotifyMsgSize);
  }

  const uint64_t numPeers = numRanks_ / numGpusPerNvDomain_ - 1;

  const uint64_t numBytesPerMessageWithNotify =
      uint64_t{messageSize_} * numChunks_ * numGpusPerNvDomain_ +
      kNotifyMsgSize;
  // total data size calc:
  return numPeers * numBytesPerMessageWithNotify;
}

bool AllToAllRank::hasTraffic() const {
  for (uint32_t peer = 0; peer < numRanks_; ++peer) {
    if (isPeerSkipped(peer)) {
      continue;
    }
    for (uint32_t chunk = 0; chunk < numChunks_; ++chunk) {
      if (getSendSize(peer, chunk) > 0 || getRecvSize(peer, chunk) > 0) {
        return true;
      }
    }
  }
  return false;
}

bool AllToAllRank::isInSameRail(const uint32_t peerRank) const {
  const uint32_t rail = rank_ % numGpusPerNvDomain_;
  const uint32_t peerRail = peerRank % numGpusPerNvDomain_;
  return rail == peerRail;
}

bool AllToAllRank::isPeerSkipped(const uint32_t peerRank) const {
  // With PXN a rank only exchanges messages with the GPUs on its rail.
  return peerRank == rank_ || isInSameNvDomain(peerRank) ||
      (usePxn_ && !isInSameRail(peerRank));
}

} // namespace packsim
