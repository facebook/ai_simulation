/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/all-to-all-vec.h"

#include <cstdint>
#include <stdexcept>

#include <fmt/format.h>
#include <folly/logging/xlog.h>

namespace packsim {

namespace {

/**
 * Report a msgDelaysNs matrix whose shape does not match vectorizedSizesBytes.
 *
 * @throw std::runtime_error  Always; this helper exists only to throw.
 */
[[noreturn]] void throwInconsistentDelayShape() {
  throw std::runtime_error(
      "Inconsistent msgDelaysNs vector size. Please ensure the shape of msgDelaysNs matches that of vectorizedSizesBytes");
}

} // namespace

VectorizedAllToAllRank::VectorizedAllToAllRank(const WorkloadRankParams& params)
    : AllToAllRank(params) {
  const auto& allToAllCfg = *workCfg_->config()->allToAll();
  const auto& sizes = *allToAllCfg.vectorizedSizesBytes();
  const auto& delays = *allToAllCfg.msgDelaysNs();

  // vectorizedSizesBytes must be a numRanks x numRanks matrix: this rank reads
  // its send row sizes[rank_][*] and recv column sizes[*][rank_], and the fill
  // loop below writes sendSizes_/recvSizes_ (both sized to numRanks_) for every
  // row. A non-square shape would index out of bounds, so validate up front.
  if (sizes.size() != numRanks_) {
    throw std::runtime_error(
        fmt::format(
            "vectorizedSizesBytes has {} rows but the workload has {} ranks; expected a numRanks x numRanks matrix",
            sizes.size(),
            numRanks_));
  }
  for (uint32_t i = 0; i < sizes.size(); i++) {
    if (sizes[i].size() != numRanks_) {
      throw std::runtime_error(
          fmt::format(
              "vectorizedSizesBytes row {} has {} columns but the workload has {} ranks; expected a numRanks x numRanks matrix",
              i,
              sizes[i].size(),
              numRanks_));
    }
  }

  // A rank sends nothing to itself, so the diagonal must be zero.
  for (uint32_t i = 0; i < numRanks_; i++) {
    if (sizes[i][i] != 0) {
      throw std::invalid_argument(
          fmt::format(
              "Workload {}: vectorizedSizesBytes[{}][{}] is {}, but a rank "
              "sends nothing to itself, so the diagonal must be 0",
              workloadName_,
              i,
              i,
              sizes[i][i]));
    }
  }

  // If msgDelaysNs is populated, ensure that the shape of msgDelaysNs matches
  // the shape of vectorizedSizesBytes
  if (delays.size() != 0) {
    if (delays.size() != sizes.size()) {
      throwInconsistentDelayShape();
    }

    for (uint32_t i = 0; i < sizes.size(); i++) {
      if (delays[i].size() != sizes[i].size()) {
        throwInconsistentDelayShape();
      }
    }
  }

  sendSizes_.resize(numRanks_);
  recvSizes_.resize(numRanks_);
  sendDelays_.resize(numRanks_);
  for (uint32_t i = 0; i < sizes.size(); i++) {
    sendSizes_[i] = sizes[rank_][i];
    recvSizes_[i] = sizes[i][rank_];

    if (delays.size() == 0) {
      // If msgDelaysNs is not populated, use 0 as default delay
      sendDelays_[i] = 0;
    } else {
      sendDelays_[i] = delays[rank_][i];
    }
    XLOGF(
        DBG,
        "a2av rank {} -> {}: sendSize = {}, sendDelay = {}",
        rank_,
        i,
        sendSizes_[i],
        sendDelays_[i]);
  }
}

void VectorizedAllToAllRank::sendNotifies() {
  startTimeNs_ = getTimeNs();
  for (uint32_t peer = 0; peer < numRanks_; peer++) {
    if (!isPeerSkipped(peer) && recvSizes_[peer] > 0) {
      notify(peer);
    }
  }
  notifiesSent_ = true;
  progress();
}

uint64_t VectorizedAllToAllRank::getExpectedBytes() {
  // A rank without traffic completes in `run()` without exchanging notifies.
  if (!hasTraffic()) {
    return 0;
  }
  // Only from the peers `run()` posts receives for; with PXN each receive
  // carries the chunks of every GPU in the sender's NV domain. A peer this
  // rank sends to notifies it first.
  const uint64_t pxnScale = usePxn_ ? numGpusPerNvDomain_ : 1;
  uint64_t expectedBytes = 0;
  for (uint32_t peer = 0; peer < numRanks_; peer++) {
    if (isPeerSkipped(peer)) {
      continue;
    }
    expectedBytes += recvSizes_[peer] * pxnScale;
    if (sendSizes_[peer] > 0) {
      expectedBytes += kNotifyMsgSize;
    }
  }
  return expectedBytes;
}

uint32_t VectorizedAllToAllRank::getSendSize(
    const uint32_t dstRank,
    const uint32_t chunk) const {
  const auto totalSize = sendSizes_.at(dstRank);
  if (chunk < (numChunks_ - 1)) {
    return totalSize / numChunks_;
  }
  return totalSize - (numChunks_ - 1) * (totalSize / numChunks_);
}

uint64_t VectorizedAllToAllRank::getSendDelay(
    const uint32_t dstRank,
    const uint32_t chunkId) const {
  // Only the first chunk is delayed; the later chunks follow it right away.
  // That holds while `maxChunksInFlight` is 1, its default: each chunk is
  // posted only once the one before it has been acknowledged. With more in
  // flight, the chunks posted together with the first get no delay and go out
  // ahead of it. The tests exercise delays with single-chunk workloads only.
  const auto retVal = chunkId == 0 ? sendDelays_.at(dstRank) : 0;
  XLOGF(
      DBG,
      "VectorizedAllToAllRank::getSendDelay {} -> {}, chunkId: {}, delay {}ns",
      rank_,
      dstRank,
      chunkId,
      retVal);
  return retVal;
}

uint32_t VectorizedAllToAllRank::getRecvSize(
    const uint32_t srcRank,
    const uint32_t chunk) const {
  const auto totalSize = recvSizes_.at(srcRank);
  if (chunk < (numChunks_ - 1)) {
    return totalSize / numChunks_;
  }
  return totalSize - (numChunks_ - 1) * (totalSize / numChunks_);
}

} // namespace packsim
