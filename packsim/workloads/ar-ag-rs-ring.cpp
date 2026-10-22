/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/ar-ag-rs-ring.h"

#include <algorithm>
#include <functional>
#include <optional>
#include <stdexcept>
#include <vector>

#include <fmt/format.h>
#include <folly/Conv.h>
#include <folly/Utility.h>
#include <folly/logging/xlog.h>

#include "packsim/kernel/ceil-divide.h"

using packsim::workload::thrift::WorkloadSpecificConfig;

namespace packsim {
RingRank::RingRank(const WorkloadRankParams& params) : WorkloadRank(params) {
  // A one-rank ring makes the rank its own neighbor, so it would send to
  // itself, except with NV domains: its one neighbor then shares its domain,
  // and the rank sends nothing and completes at once.
  if (!hasNvDomainSkipping()) {
    requireAtLeastTwoRanks("Ring");
  }
  this->setOnRequestAckedCallback([this](Message* msg) { requestAcked(msg); });
  this->setOnRequestReceivedCallback(
      [this](Message* msg) { requestReceived(msg); });

  // Compute original ring neighbors
  left_ = (rank_ + numRanks_ - 1) % numRanks_;
  right_ = (rank_ + 1) % numRanks_;

  // Determine whether to skip send/recv based on NV domain membership
  skipSend_ = isInSameNvDomain(right_);
  skipRecv_ = isInSameNvDomain(left_);

  initFromCollectiveConfig();
  applyNicSelectionMode();

  XLOGF(
      DBG,
      "RingRank wkld={} rank={} numRanks={} left={} right={} skipSend={} skipRecv={}"
      " totalChunks={} chunkSizeBytes={} maxChunksInFlight={} localChunks={}",
      *workCfg_->workloadId(),
      rank_,
      numRanks_,
      left_,
      right_,
      skipSend_,
      skipRecv_,
      totalChunks_,
      chunkSizeBytes_,
      maxChunksInFlight_,
      localChunks_);
}

template <typename TCollectiveCfg>
void RingRank::initChunks(const TCollectiveCfg& cfg, const uint64_t numSteps) {
  localChunks_ = *cfg.numChunks();
  requirePositiveField("Ring", "numChunks", localChunks_);
  chunkSizeBytes_ =
      ceilDivide32(*cfg.sizeBytes(), uint64_t{localChunks_} * numRanks_);
  totalChunks_ = folly::to<uint32_t>(numSteps * uint64_t{localChunks_});
  maxChunksInFlight_ = *cfg.maxChunksInFlight();
  requirePositiveField("Ring", "maxChunksInFlight", maxChunksInFlight_);
}

void RingRank::initFromCollectiveConfig() {
  switch (workCfg_->config()->getType()) {
    case WorkloadSpecificConfig::Type::allReduce: {
      XLOGF(DBG, "Workload {} is AllReduce", *workCfg_->workloadId());
      const auto& arCfg = *workCfg_->config()->allReduce();
      initChunks(arCfg, /*numSteps=*/2 * numRanks_ - 2);
      nicSelectionMode_ = *arCfg.nicSelectionMode();
      return;
    }
    case WorkloadSpecificConfig::Type::allGather: {
      XLOGF(DBG, "Workload {} is AllGather", *workCfg_->workloadId());
      const auto& agCfg = *workCfg_->config()->allGather();
      initChunks(agCfg, /*numSteps=*/numRanks_ - 1);
      nicSelectionMode_ = *agCfg.nicSelectionMode();
      return;
    }
    case WorkloadSpecificConfig::Type::reduceScatter:
      XLOGF(DBG, "Workload {} is ReduceScatter", *workCfg_->workloadId());
      initChunks(
          *workCfg_->config()->reduceScatter(), /*numSteps=*/numRanks_ - 1);
      return;
    case WorkloadSpecificConfig::Type::__EMPTY__:
    case WorkloadSpecificConfig::Type::allToAll:
    case WorkloadSpecificConfig::Type::incast:
    case WorkloadSpecificConfig::Type::permutation:
    // @oss-disable
    case WorkloadSpecificConfig::Type::batch:
    // @oss-disable
    // @oss-disable
    // @oss-disable
    case WorkloadSpecificConfig::Type::compute:
      break;
  }
  // Also reached by an arm added to WorkloadSpecificConfig without a case here.
  throw std::invalid_argument("Unsupported workload type");
}

void RingRank::applyNicSelectionMode() {
  switch (nicSelectionMode_) {
    case workload::thrift::RingNicSelectionMode::SINGLE_NIC:
      return;
    case workload::thrift::RingNicSelectionMode::ALL_AVAILABLE_NICS:
      initializeNicPaths();
      return;
  }
  // A value outside the enum.
  throw std::invalid_argument(
      fmt::format(
          "Unsupported ring NIC selection mode {}",
          folly::to_underlying(nicSelectionMode_)));
}

void RingRank::run() {
  // Interior ranks: both neighbors in same NV domain, no network activity.
  // NVLink data transfer within the domain is modeled as instantaneous,
  // so this rank completes immediately with zero duration.
  if (skipSend_ && skipRecv_) {
    startTimeNs_ = getTimeNs();
    endTimeNs_ = getTimeNs();
    onRankComplete(workloadId_, rank_);
    return;
  }

  // Post recvs from left neighbor (entry and full ranks only)
  if (!skipRecv_) {
    // When NV domain skipping is active, EXIT ranks queue all totalChunks_
    // sends upfront (not paced by incoming recvs). In the ROCE transport, the
    // ACK is sent in onPacketRcvd() before the NicRxBuffer schedules the
    // requestReceived callback. This means the sender can receive an ACK and
    // send the next chunk before the receiver posts the corresponding recv,
    // causing an RNR. Pre-posting all recvs upfront eliminates this race.
    const auto initialChunks = hasNvDomainSkipping()
        ? totalChunks_
        : std::min(totalChunks_, maxChunksInFlight_);
    for (uint32_t chunkId = 0; chunkId < initialChunks; ++chunkId) {
      recvMsgs_.try_emplace(chunkId, recvChunk(chunkId));
    }
    postedRecvs_ = initialChunks;
  }

  if (!skipSend_) {
    // Exit ranks (skipRecv_=true): queue all chunks upfront since NVLink
    // data from same-domain predecessors is assumed instant.
    // Full ranks: queue localChunks_ and rely on recv-triggers-send pipeline.
    queuedChunks_ = skipRecv_ ? totalChunks_ : localChunks_;
    simCtx_->schedule(
        Time::fromNs(startTimeDelayNs_), &RingRank::trySendChunks, this, true);
    XLOGF(DBG, "Queuing {} chunks at start", queuedChunks_);
  } else {
    // Entry ranks (recv only): mark as started for timing
    startTimeNs_ = getTimeNs();
    started_ = true;
  }
}

void RingRank::trySendChunks(const bool begin) {
  started_ |= begin;
  if (!started_) {
    return;
  }
  if (begin) {
    startTimeNs_ = getTimeNs();
  }
  const uint32_t inFlightWindow = maxChunksInFlight_ - chunksInFlight_;
  const uint32_t numChunksToSend = std::min(inFlightWindow, queuedChunks_);
  for (uint32_t i = 0; i < numChunksToSend; ++i) {
    const uint32_t chunkId = sentChunks_++;
    sendMsgs_.try_emplace(chunkId, sendChunk(chunkId));
    --queuedChunks_;
    ++chunksInFlight_;
    XLOGF(
        DBG,
        "{}ns @ {}: Sending chunk {} ({} in flight)",
        getTimeNs(),
        rank_,
        chunkId,
        chunksInFlight_);
  }
}

void RingRank::requestReceived(Message* msg) {
  if (msg == nullptr) {
    throw std::runtime_error("Ring receive completion has no message");
  }
  const uint32_t chunkId = msg->connectionMessageId;
  if (recvMsgs_.erase(chunkId) != 1) {
    throw std::runtime_error(
        fmt::format(
            "Ring receive completion for unknown chunk {} on rank {}",
            chunkId,
            rank_));
  }

  // Forward received data to right neighbor (full ranks only)
  if (!skipSend_ && (queuedChunks_ + sentChunks_) < totalChunks_) {
    if (started_) {
      const auto nowNs = simCtx_->nowNs();
      XLOGF(
          DBG,
          "{}ns @ {}: Req Recv; {} chunks in flight",
          nowNs,
          rank_,
          chunksInFlight_);
    }
    ++queuedChunks_;
    trySendChunks();
  }
  if (postedRecvs_ < totalChunks_) {
    recvMsgs_.try_emplace(postedRecvs_, recvChunk(postedRecvs_));
    ++postedRecvs_;
  }
  if (complete()) {
    endTimeNs_ = getTimeNs();
    onRankComplete(workloadId_, rank_);
  }
}

void RingRank::requestAcked(Message* msg) {
  if (msg == nullptr) {
    throw std::runtime_error("Ring send completion has no message");
  }
  const uint32_t chunkId = msg->connectionMessageId;
  if (sendMsgs_.erase(chunkId) != 1) {
    throw std::runtime_error(
        fmt::format(
            "Ring send completion for unknown chunk {} on rank {}",
            chunkId,
            rank_));
  }

  const auto nowNs = simCtx_->nowNs();
  XLOGF(
      DBG,
      "{}ns @ {}: Req Ackd; {} chunks in flight",
      nowNs,
      rank_,
      chunksInFlight_);
  --chunksInFlight_;
  if (complete()) {
    endTimeNs_ = getTimeNs();
    onRankComplete(workloadId_, rank_);
  } else {
    trySendChunks();
  }
}

uint64_t RingRank::getExpectedBytes() {
  if (skipRecv_) {
    return 0;
  }
  return uint64_t{totalChunks_} * chunkSizeBytes_;
}

bool RingRank::complete() {
  const bool sendsDone = skipSend_ ||
      (sentChunks_ == totalChunks_ && chunksInFlight_ == 0 &&
       sendMsgs_.empty());
  const bool recvsDone = skipRecv_ || recvMsgs_.empty();
  return sendsDone && recvsDone;
}

bool RingRank::hasNvDomainSkipping() const {
  return workCfg_->rankToNvDomain().has_value() &&
      !workCfg_->rankToNvDomain()->empty();
}

size_t RingRank::getNicCountForEdge(
    const uint32_t srcRank,
    const uint32_t dstRank) const {
  const auto& routingConfig = getRoutingWorkloadConfig();
  const auto& rankToIdList = *routingConfig.rankToIdList();
  const auto srcRankIt = rankToIdList.find(srcRank);
  const auto dstRankIt = rankToIdList.find(dstRank);
  if (srcRankIt == rankToIdList.end() || dstRankIt == rankToIdList.end()) {
    throw std::invalid_argument(
        fmt::format(
            "Missing endpoint mapping for ring edge {} -> {}",
            srcRank,
            dstRank));
  }

  const size_t srcNicCount = srcRankIt->second.size();
  const size_t dstNicCount = dstRankIt->second.size();
  if (srcNicCount == 0) {
    throw std::invalid_argument(
        fmt::format(
            "Ring edge {} -> {} has no endpoint mappings", srcRank, dstRank));
  }
  if (srcNicCount != dstNicCount) {
    throw std::invalid_argument(
        fmt::format(
            "Ring edge {} -> {} has {} source NICs but {} destination NICs",
            srcRank,
            dstRank,
            srcNicCount,
            dstNicCount));
  }
  return srcNicCount;
}

RingRank::RingNicPathCandidates RingRank::buildNicPathCandidates(
    const uint32_t srcRank,
    const uint32_t dstRank,
    const size_t nicCount) const {
  packsim::thrift::RankPair rankPair;
  rankPair.srcRank() = srcRank;
  rankPair.dstRank() = dstRank;

  const auto& routingConfig = getRoutingWorkloadConfig();
  const auto rankPairIt = routingConfig.nicQpnPairMap()->find(rankPair);
  if (rankPairIt == routingConfig.nicQpnPairMap()->end()) {
    throw std::invalid_argument(
        fmt::format(
            "No NIC paths are configured for ring edge {} -> {}",
            srcRank,
            dstRank));
  }

  RingNicPathCandidates candidatesBySrcNic(nicCount);
  for (const auto& [nicPair, qpnPairs] : rankPairIt->second) {
    const uint32_t srcNicIdx = *nicPair.srcNicIdx();
    const uint32_t dstNicIdx = *nicPair.dstNicIdx();
    if (srcNicIdx >= nicCount || dstNicIdx >= nicCount) {
      throw std::invalid_argument(
          fmt::format(
              "Ring edge {} -> {} has out-of-range NIC pair {} -> {}",
              srcRank,
              dstRank,
              srcNicIdx,
              dstNicIdx));
    }
    if (qpnPairs.empty()) {
      throw std::invalid_argument(
          fmt::format(
              "Ring edge {} -> {} NIC pair {} -> {} has no QPs",
              srcRank,
              dstRank,
              srcNicIdx,
              dstNicIdx));
    }

    const auto& qpnPair = qpnPairs.front();
    candidatesBySrcNic.at(srcNicIdx).emplace_back(
        RingNicPath{
            .localQpn = *qpnPair.local(),
            .remoteQpn = *qpnPair.remote(),
            .endpointId = srcNicIdx,
            .dstEndpointId = dstNicIdx});
  }
  return candidatesBySrcNic;
}

std::vector<RingRank::RingNicPath> RingRank::selectNicPathMatching(
    const RingNicPathCandidates& candidatesBySrcNic,
    const uint32_t srcRank,
    const uint32_t dstRank) {
  const size_t nicCount = candidatesBySrcNic.size();
  std::vector<std::optional<size_t>> srcNicByDstNic(nicCount);
  std::vector<RingNicPath> paths(nicCount);
  const std::function<bool(size_t, std::vector<uint8_t>&)> assignPath =
      [&](const size_t srcNic, std::vector<uint8_t>& visitedDstNics) {
        for (const auto& candidate : candidatesBySrcNic.at(srcNic)) {
          const size_t dstNic = candidate.dstEndpointId;
          if (visitedDstNics.at(dstNic)) {
            continue;
          }
          visitedDstNics.at(dstNic) = 1;
          const std::optional<size_t> previousSrcNic =
              srcNicByDstNic.at(dstNic);
          if (previousSrcNic.has_value() &&
              !assignPath(*previousSrcNic, visitedDstNics)) {
            continue;
          }
          srcNicByDstNic.at(dstNic) = srcNic;
          paths.at(srcNic) = candidate;
          return true;
        }
        return false;
      };

  for (size_t srcNic = 0; srcNic < nicCount; ++srcNic) {
    std::vector<uint8_t> visitedDstNics(nicCount, 0);
    if (!assignPath(srcNic, visitedDstNics)) {
      throw std::invalid_argument(
          fmt::format(
              "Ring edge {} -> {} cannot use all {} NICs with one compatible path per NIC",
              srcRank,
              dstRank,
              nicCount));
    }
  }
  return paths;
}

std::vector<RingRank::RingNicPath> RingRank::buildNicPaths(
    const uint32_t srcRank,
    const uint32_t dstRank) const {
  const size_t nicCount = getNicCountForEdge(srcRank, dstRank);
  return selectNicPathMatching(
      buildNicPathCandidates(srcRank, dstRank, nicCount), srcRank, dstRank);
}

void RingRank::initializeNicPaths() {
  if (hasChannelManagerInHierarchy()) {
    throw std::invalid_argument(
        "Ring all-NIC selection cannot be combined with channel settings");
  }
  if (!skipSend_) {
    sendNicPaths_ = buildNicPaths(rank_, right_);
  }
  if (!skipRecv_) {
    recvNicPaths_ = buildNicPaths(left_, rank_);
  }

  const size_t numNicPaths =
      std::max(sendNicPaths_.size(), recvNicPaths_.size());
  if (numNicPaths == 0) {
    return;
  }
  if (localChunks_ < numNicPaths) {
    throw std::invalid_argument(
        fmt::format(
            "Ring all-NIC selection requires at least {} chunks, but numChunks is {}",
            numNicPaths,
            localChunks_));
  }
  if (maxChunksInFlight_ < numNicPaths) {
    throw std::invalid_argument(
        fmt::format(
            "Ring all-NIC selection requires at least {} chunks in flight, but maxChunksInFlight is {}",
            numNicPaths,
            maxChunksInFlight_));
  }
}

std::unique_ptr<Message> RingRank::sendChunk(const uint32_t chunkId) {
  if (nicSelectionMode_ == workload::thrift::RingNicSelectionMode::SINGLE_NIC) {
    return send(right_, chunkSizeBytes_, chunkId);
  }
  const auto& path = sendNicPaths_.at(chunkId % sendNicPaths_.size());
  return sendWithQPN(
      right_,
      chunkSizeBytes_,
      chunkId,
      path.localQpn,
      path.remoteQpn,
      path.endpointId,
      path.dstEndpointId);
}

std::unique_ptr<Message> RingRank::recvChunk(const uint32_t chunkId) {
  if (nicSelectionMode_ == workload::thrift::RingNicSelectionMode::SINGLE_NIC) {
    return recv(left_, chunkSizeBytes_, chunkId);
  }
  const auto& path = recvNicPaths_.at(chunkId % recvNicPaths_.size());
  return recvWithQPN(
      left_,
      chunkSizeBytes_,
      chunkId,
      path.remoteQpn,
      path.localQpn,
      path.dstEndpointId,
      path.endpointId);
}

} // namespace packsim
