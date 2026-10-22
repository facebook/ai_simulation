/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/rank/workload-rank.h"

#include <memory>
#include <stdexcept>
#include <string>

#include <fmt/format.h>
#include <folly/Conv.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>

namespace packsim {

using packsim::workload::thrift::NicPairToQPNList;
using packsim::workload::thrift::QPNPair;

WorkloadRank::WorkloadRank(const WorkloadRankParams& params)
    : onRankComplete_(params.rccb),
      sendMessageFn_(params.smcb),
      recvMessageFn_(params.rmcb),
      simCtx_(params.simCtx),
      workCfg_(&params.workCfg),
      parent_(params.parent),
      workloadName_(params.workCfg.name().value()),
      workloadId_(params.workloadId),
      numRanks_(*params.workCfg.numEndpoints()),
      rank_(params.rank),
      startTimeRandomSplayNs_(*params.workCfg.maxStaggeredStartTimeNs()) {
  if (params.workCfg.explicitStaggeredStartTimeNs()->size() != 0) {
    if (params.workCfg.explicitStaggeredStartTimeNs()->size() != numRanks_) {
      throw std::runtime_error(
          fmt::format(
              "Workload {} has {} explicit staggered start times, but {} ranks",
              workloadName_,
              params.workCfg.explicitStaggeredStartTimeNs()->size(),
              numRanks_));
    }
    startTimeExplicitSplayNs_ =
        params.workCfg.explicitStaggeredStartTimeNs()->at(params.rank);
    startTimeDelayNs_ = startTimeExplicitSplayNs_;
  } else {
    startTimeDelayNs_ =
        simCtx_->random.sampleUniform<uint64_t>(0, startTimeRandomSplayNs_);
  }

  if (params.workCfg.channelSettings().has_value()) {
    const auto& channelSettings = *params.workCfg.channelSettings();
    const ChannelParams channelParams{
        .smcb = sendMessageFn_,
        .rmcb = recvMessageFn_,
        .numChannels = *channelSettings.numChannels(),
        .numQPsPerChannel = *channelSettings.numQPsPerChannel(),
        .messageSizeBytes = *channelSettings.channelMessageSizeBytes(),
        .workloadId = params.workloadId,
        .workload = &params.workCfg,
        .rankToIdList = params.workCfg.rankToIdList().value(),
        .qpPairMap = params.workCfg.qpnPairMap().value(),
        .rank = params.rank};

    channelManager_ = std::make_unique<ChannelManager>(channelParams);
  }

  buildQpSelection();
  for (const auto& peer : getPeers()) {
    postRecvNotify(peer);
  }
}

void WorkloadRank::buildQpSelection() {
  if (parent_ != nullptr || workCfg_->nicQpnPairMap()->empty()) {
    return;
  }
  // nicQpnPairMap is ordered by RankPair (srcRank before dstRank), so this
  // rank's pairs are one contiguous range -- populate the cache in one pass.
  const auto& nicQpnPairMap = *workCfg_->nicQpnPairMap();
  packsim::thrift::RankPair rangeStart;
  rangeStart.srcRank() = rank_;
  rangeStart.dstRank() = 0;
  for (auto it = nicQpnPairMap.lower_bound(rangeStart);
       it != nicQpnPairMap.end() && *it->first.srcRank() == rank_;
       ++it) {
    uint32_t totalDataQpCount = 0;
    for (const auto& [nicPair, qpnPairs] : it->second) {
      totalDataQpCount += getDataQPCountFromTotal(qpnPairs.size());
    }
    qpSelectionByPeer_[*it->first.dstRank()] = {&it->second, totalDataQpCount};
  }
}

const NicPairToQPNList* WorkloadRank::qpnsByNicPairFor(
    const uint32_t peer) const {
  const auto it = qpSelectionByPeer_.find(peer);
  return it == qpSelectionByPeer_.end() ? nullptr : it->second.qpnsByNicPair;
}

const packsim::workload::thrift::Workload&
WorkloadRank::getRoutingWorkloadConfig() const {
  return parent_ == nullptr ? *workCfg_ : parent_->getRoutingWorkloadConfig();
}

bool WorkloadRank::hasChannelManagerInHierarchy() const {
  return channelManager_ != nullptr ||
      (parent_ != nullptr && parent_->hasChannelManagerInHierarchy());
}

std::vector<uint32_t> WorkloadRank::getPeers() const {
  if (parent_ != nullptr) {
    return parent_->getPeers();
  }
  // Prefer nicQpnPairMap when available (e.g., when using qpTemplates)
  if (!workCfg_->nicQpnPairMap()->empty()) {
    // nicQpnPairMap is ordered by RankPair (srcRank before dstRank), so this
    // rank's pairs are one contiguous range; each key is distinct, so peers are
    // already unique.
    const auto& nicQpnPairMap = *workCfg_->nicQpnPairMap();
    packsim::thrift::RankPair rangeStart;
    rangeStart.srcRank() = rank_;
    rangeStart.dstRank() = 0;
    std::vector<uint32_t> peers;
    peers.reserve(numRanks_ - 1);
    for (auto it = nicQpnPairMap.lower_bound(rangeStart);
         it != nicQpnPairMap.end() && *it->first.srcRank() == rank_;
         ++it) {
      peers.emplace_back(*it->first.dstRank());
    }
    return peers;
  }
  // Fall back to legacy qpnPairMap
  if (workCfg_->qpnPairMap()->empty()) {
    return {};
  }
  const auto it = workCfg_->qpnPairMap()->find(rank_);
  if (it == workCfg_->qpnPairMap()->end()) {
    std::vector<uint32_t> qpnKeys;
    // Print out all the keys in the map for debugging
    for (auto qpnIt = workCfg_->qpnPairMap()->begin();
         qpnIt != workCfg_->qpnPairMap()->end();
         ++qpnIt) {
      qpnKeys.emplace_back(qpnIt->first);
    }
    throw std::runtime_error(
        fmt::format(
            "Failed to find rank {} in qpnPairMap. Available keys in qpnPairMap: [{}]",
            rank_,
            folly::join(", ", qpnKeys)));
  }
  std::vector<uint32_t> peers;
  const auto& peerToQpnsMap = it->second;
  peers.reserve(peerToQpnsMap.size());
  for (const auto& [peer, _] : peerToQpnsMap) {
    peers.emplace_back(peer);
  }
  return peers;
}

std::unique_ptr<Message> WorkloadRank::send(
    const uint32_t toRank,
    const uint32_t size,
    const uint32_t chunkId,
    const uint32_t endpointId,
    const uint64_t sendDelayNs,
    const bool writeImm,
    const bool write,
    const uint32_t writeImmData,
    const uint64_t dataPtr) {
  if (parent_ != nullptr) {
    return parent_->send(
        toRank,
        size,
        chunkId,
        endpointId,
        sendDelayNs,
        writeImm,
        write,
        writeImmData,
        dataPtr);
  }
  if (channelManager_) {
    return channelManager_->send(
        rank_,
        toRank,
        size,
        chunkId,
        endpointId,
        sendDelayNs,
        writeImm,
        write,
        writeImmData);
  }

  const auto& qpnPair = getDataQPNs(toRank);
  return sendWithQPN(
      toRank,
      size,
      chunkId,
      *qpnPair.local(),
      *qpnPair.remote(),
      endpointId,
      endpointId,
      sendDelayNs,
      writeImm,
      write,
      writeImmData,
      0,
      dataPtr);
}

std::unique_ptr<Message> WorkloadRank::recv(
    const uint32_t fromRank,
    const uint32_t size,
    const uint32_t chunkId,
    const uint32_t endpointId,
    const uint64_t sendDelayNs,
    const bool isRead,
    const uint64_t dataPtr) {
  if (parent_ != nullptr) {
    return parent_->recv(
        fromRank, size, chunkId, endpointId, sendDelayNs, isRead, dataPtr);
  }

  if (channelManager_) {
    return channelManager_->recv(fromRank, rank_, size, chunkId, endpointId);
  }

  const auto& qpnPair = getDataQPNs(fromRank);
  return recvWithQPN(
      fromRank,
      size,
      chunkId,
      *qpnPair.local(),
      *qpnPair.remote(),
      endpointId,
      endpointId,
      sendDelayNs,
      isRead,
      dataPtr);
}

std::unique_ptr<Message> WorkloadRank::sendWithQPN(
    const uint32_t toRank,
    const uint32_t size,
    const uint32_t chunkId,
    const uint32_t localQPN,
    const uint32_t remoteQPN,
    const uint32_t endpointId,
    const uint32_t dstEndpointId,
    const uint64_t sendDelayNs,
    const bool writeImm,
    const bool write,
    const uint32_t writeImmData,
    const uint32_t sendOrWriteData,
    const uint64_t dataPtr) {
  if (parent_) {
    return parent_->sendWithQPN(
        toRank,
        size,
        chunkId,
        localQPN,
        remoteQPN,
        endpointId,
        dstEndpointId,
        sendDelayNs,
        writeImm,
        write,
        writeImmData,
        sendOrWriteData,
        dataPtr);
  }
  auto msg = std::make_unique<Message>();
  msg->sizeBytes = size;
  msg->connectionMessageId = chunkId;
  msg->srcRank = rank_;
  msg->dstRank = toRank;
  msg->srcIdx =
      FlexDeviceId(workCfg_->rankToIdList()->at(rank_).at(endpointId));
  msg->dstIdx =
      FlexDeviceId(workCfg_->rankToIdList()->at(toRank).at(dstEndpointId));
  msg->send = true;
  msg->recv = false;
  msg->localQPN = localQPN;
  msg->remoteQPN = remoteQPN;
  msg->workloadId = workloadId_;
  msg->iteration = iteration_;
  msg->workload = workCfg_;
  msg->endpointId = endpointId;
  msg->dstEndpointId = dstEndpointId;
  msg->sendDelayNs = sendDelayNs;
  msg->isWriteImm = writeImm;
  msg->isWrite = write;
  msg->writeImmData = writeImmData;
  msg->sendOrWriteData = sendOrWriteData;
  msg->dataPtr = dataPtr;
  sendMessageFn_(msg.get());
  return msg;
}

std::unique_ptr<Message> WorkloadRank::recvWithQPN(
    const uint32_t fromRank,
    const uint32_t size,
    const uint32_t chunkId,
    const uint32_t localQPN,
    const uint32_t remoteQPN,
    const uint32_t endpointId,
    const uint32_t dstEndpointId,
    const uint64_t sendDelayNs,
    const bool isRead,
    const uint64_t dataPtr) {
  if (parent_) {
    return parent_->recvWithQPN(
        fromRank,
        size,
        chunkId,
        localQPN,
        remoteQPN,
        endpointId,
        dstEndpointId,
        sendDelayNs,
        isRead,
        dataPtr);
  }
  auto msg = std::make_unique<Message>();
  msg->sizeBytes = size;
  msg->connectionMessageId = chunkId;
  msg->srcRank = fromRank;
  msg->dstRank = rank_;
  msg->srcIdx =
      FlexDeviceId(workCfg_->rankToIdList()->at(fromRank).at(dstEndpointId));
  msg->dstIdx =
      FlexDeviceId(workCfg_->rankToIdList()->at(rank_).at(endpointId));
  msg->send = false;
  msg->recv = true;
  msg->isRead = isRead;
  msg->dataPtr = dataPtr;
  msg->localQPN = localQPN;
  msg->remoteQPN = remoteQPN;
  msg->workloadId = workloadId_;
  msg->iteration = iteration_;
  msg->workload = workCfg_;
  msg->endpointId = endpointId;
  msg->dstEndpointId = dstEndpointId;
  msg->sendDelayNs = sendDelayNs;
  recvMessageFn_(msg.get()); // WorkloadNode::makeReceiveRequest
  return msg;
}

const QPNPair& WorkloadRank::getDataQPNs(const uint32_t peer) const {
  if (parent_ != nullptr) {
    return parent_->getDataQPNs(peer);
  }
  // When using nicQpnPairMap, get the first QP from the first NIC pair
  if (!workCfg_->nicQpnPairMap()->empty()) {
    const auto* qpnsByNicPair = qpnsByNicPairFor(peer);
    if (qpnsByNicPair != nullptr && !qpnsByNicPair->empty()) {
      // Get the first NIC pair's first data QPN
      return qpnsByNicPair->begin()->second.at(0);
    }
    // nicQpnPairMap is populated but doesn't have this rank pair - don't fall
    // through to qpnPairMap which may be empty when using nicQpnPairMap
    throw std::out_of_range(
        fmt::format(
            "No QPN entry found for rank pair ({}, {}) in workload '{}'. "
            "nicQpnPairMap has {} entries but does not contain this rank pair. "
            "Ensure QP templates cover all rank combinations.",
            rank_,
            peer,
            workloadName_,
            workCfg_->nicQpnPairMap()->size()));
  }
  return workCfg_->qpnPairMap()->at(rank_).at(peer).at(0);
}

const QPNPair& WorkloadRank::getNotifyQPNs(const uint32_t peer) const {
  if (parent_ != nullptr) {
    return parent_->getNotifyQPNs(peer);
  }
  // When using nicQpnPairMap, get the last QP from the first NIC pair
  // (notify QP is conventionally the last one)
  if (!workCfg_->nicQpnPairMap()->empty()) {
    const auto* qpnsByNicPair = qpnsByNicPairFor(peer);
    if (qpnsByNicPair != nullptr && !qpnsByNicPair->empty()) {
      // Get the first NIC pair's last QPN (notify QP)
      return qpnsByNicPair->begin()->second.back();
    }
    // nicQpnPairMap is populated but doesn't have this rank pair - don't fall
    // through to qpnPairMap which may be empty when using nicQpnPairMap
    throw std::out_of_range(
        fmt::format(
            "No QPN entry found for rank pair ({}, {}) in workload '{}'. "
            "nicQpnPairMap has {} entries but does not contain this rank pair. "
            "Ensure QP templates cover all rank combinations.",
            rank_,
            peer,
            workloadName_,
            workCfg_->nicQpnPairMap()->size()));
  }
  return workCfg_->qpnPairMap()->at(rank_).at(peer).back();
}

packsim::thrift::NicPair WorkloadRank::getNotifyNicPair(
    const uint32_t peer) const {
  if (parent_ != nullptr) {
    return parent_->getNotifyNicPair(peer);
  }
  if (const auto* qpnsByNicPair = qpnsByNicPairFor(peer);
      qpnsByNicPair != nullptr && !qpnsByNicPair->empty()) {
    return qpnsByNicPair->begin()->first;
  }
  packsim::thrift::NicPair legacyNicPair;
  legacyNicPair.srcNicIdx() = 0;
  legacyNicPair.dstNicIdx() = 0;
  return legacyNicPair;
}

const QPNPair& WorkloadRank::getDataQPNs(
    const uint32_t peer,
    const uint32_t srcNicIdx,
    const uint32_t dstNicIdx) const {
  if (parent_ != nullptr) {
    return parent_->getDataQPNs(peer, srcNicIdx, dstNicIdx);
  }

  // Check if nicQpnPairMap is available for multi-NIC configurations
  if (!workCfg_->nicQpnPairMap()->empty()) {
    // New structure: {srcRank, dstRank} -> {srcNicIdx, dstNicIdx} ->
    // list<QPNPair>
    const auto* qpnsByNicPair = qpnsByNicPairFor(peer);
    if (qpnsByNicPair == nullptr) {
      throw std::out_of_range(
          fmt::format(
              "No QPN entry found for rank pair ({}, {}) in workload '{}'. "
              "nicQpnPairMap has {} entries but does not contain this rank pair.",
              rank_,
              peer,
              workloadName_,
              workCfg_->nicQpnPairMap()->size()));
    }

    packsim::thrift::NicPair nicKey;
    nicKey.srcNicIdx() = srcNicIdx;
    nicKey.dstNicIdx() = dstNicIdx;

    const auto nicIt = qpnsByNicPair->find(nicKey);
    if (nicIt == qpnsByNicPair->end()) {
      throw std::out_of_range(
          fmt::format(
              "No QPN entry found for NIC pair ({}, {}) at rank pair ({}, {}) in "
              "workload '{}'. The rank pair has {} NIC pairs but does not contain "
              "this NIC pair.",
              srcNicIdx,
              dstNicIdx,
              rank_,
              peer,
              workloadName_,
              qpnsByNicPair->size()));
    }

    if (nicIt->second.empty()) {
      throw std::out_of_range(
          fmt::format(
              "QPN list is empty for NIC pair ({}, {}) at rank pair ({}, {}) in "
              "workload '{}'.",
              srcNicIdx,
              dstNicIdx,
              rank_,
              peer,
              workloadName_));
    }

    return nicIt->second.at(0);
  }

  // Fall back to legacy behavior
  return getDataQPNs(peer);
}

const QPNPair& WorkloadRank::getNotifyQPNs(
    const uint32_t peer,
    const uint32_t srcNicIdx,
    const uint32_t dstNicIdx) const {
  if (parent_ != nullptr) {
    return parent_->getNotifyQPNs(peer, srcNicIdx, dstNicIdx);
  }

  // Check if nicQpnPairMap is available for multi-NIC configurations
  if (!workCfg_->nicQpnPairMap()->empty()) {
    // New structure: {srcRank, dstRank} -> {srcNicIdx, dstNicIdx} ->
    // list<QPNPair> For notify, we use the last QPNPair in the list (same as
    // legacy behavior)
    const auto* qpnsByNicPair = qpnsByNicPairFor(peer);
    if (qpnsByNicPair == nullptr) {
      throw std::out_of_range(
          fmt::format(
              "No QPN entry found for rank pair ({}, {}) in workload '{}'. "
              "nicQpnPairMap has {} entries but does not contain this rank pair.",
              rank_,
              peer,
              workloadName_,
              workCfg_->nicQpnPairMap()->size()));
    }

    packsim::thrift::NicPair nicKey;
    nicKey.srcNicIdx() = srcNicIdx;
    nicKey.dstNicIdx() = dstNicIdx;

    const auto nicIt = qpnsByNicPair->find(nicKey);
    if (nicIt == qpnsByNicPair->end()) {
      throw std::out_of_range(
          fmt::format(
              "No QPN entry found for NIC pair ({}, {}) at rank pair ({}, {}) in "
              "workload '{}'. The rank pair has {} NIC pairs but does not contain "
              "this NIC pair.",
              srcNicIdx,
              dstNicIdx,
              rank_,
              peer,
              workloadName_,
              qpnsByNicPair->size()));
    }

    if (nicIt->second.empty()) {
      throw std::out_of_range(
          fmt::format(
              "QPN list is empty for NIC pair ({}, {}) at rank pair ({}, {}) in "
              "workload '{}'.",
              srcNicIdx,
              dstNicIdx,
              rank_,
              peer,
              workloadName_));
    }

    return nicIt->second.back();
  }

  // Fall back to legacy behavior
  return getNotifyQPNs(peer);
}

// Helper to calculate data QP count from total QPs.
// All QPs except the last one are for data (the last is for notify). A NIC pair
// with one QP uses it for data: notifies use only a rank pair's first NIC pair,
// which fixupSimCfg requires to have at least two.
uint32_t WorkloadRank::getDataQPCountFromTotal(const size_t totalQPs) const {
  return totalQPs > 1 ? folly::to<uint32_t>(totalQPs - 1) : 1;
}

void WorkloadRank::requirePositiveField(
    const std::string& workloadType,
    const std::string& field,
    const uint32_t value) const {
  if (value == 0) {
    throw std::invalid_argument(
        fmt::format(
            "Workload {}: {} {} must be at least 1, got {}",
            workloadName_,
            workloadType,
            field,
            value));
  }
}

void WorkloadRank::requireAtLeastTwoRanks(
    const std::string& workloadType) const {
  if (numRanks_ < 2) {
    throw std::invalid_argument(
        fmt::format(
            "Workload {}: {} numEndpoints must be at least 2, got {}",
            workloadName_,
            workloadType,
            numRanks_));
  }
}

bool WorkloadRank::isNicAwareModeActive() const {
  if (parent_ != nullptr) {
    return parent_->isNicAwareModeActive();
  }
  // When ChannelManager is active, use the legacy path (not NIC-aware mode)
  // because ChannelManager has its own QP management
  if (channelManager_) {
    return false;
  }
  return !workCfg_->nicQpnPairMap()->empty();
}

uint32_t WorkloadRank::getTotalDataQPCount(const uint32_t peer) const {
  if (parent_ != nullptr) {
    return parent_->getTotalDataQPCount(peer);
  }

  // Check if nicQpnPairMap is available for multi-NIC configurations
  if (!workCfg_->nicQpnPairMap()->empty()) {
    const auto it = qpSelectionByPeer_.find(peer);
    return it == qpSelectionByPeer_.end() ? 0 : it->second.totalDataQpCount;
  }

  // Fall back to legacy qpnPairMap
  const auto rankIt = workCfg_->qpnPairMap()->find(rank_);
  if (rankIt == workCfg_->qpnPairMap()->end()) {
    return 0;
  }
  const auto peerIt = rankIt->second.find(peer);
  if (peerIt == rankIt->second.end()) {
    return 0;
  }
  // In legacy mode, last QP is for notify, rest are for data
  return getDataQPCountFromTotal(peerIt->second.size());
}

const QPNPair& WorkloadRank::getDataQPNByIndexWithNic(
    const uint32_t peer,
    const uint32_t qpIndex,
    uint32_t& srcNicIdx,
    uint32_t& dstNicIdx) const {
  if (parent_ != nullptr) {
    return parent_->getDataQPNByIndexWithNic(
        peer, qpIndex, srcNicIdx, dstNicIdx);
  }

  // Check if nicQpnPairMap is available for multi-NIC configurations
  if (workCfg_->nicQpnPairMap()->empty()) {
    throw std::runtime_error("workload->nicQpnPairMap must be populated");
  }

  const auto* qpnsByNicPair = qpnsByNicPairFor(peer);
  if (qpnsByNicPair == nullptr) {
    throw std::runtime_error(
        fmt::format(
            "No QPN mapping found for rank {} -> peer {}", rank_, peer));
  }

  // Iterate through NIC pairs and find the QP at the given flat index
  uint32_t currentIndex = 0;
  for (const auto& [nicPair, qpnPairs] : *qpnsByNicPair) {
    const uint32_t dataQPCount = getDataQPCountFromTotal(qpnPairs.size());
    if (qpIndex < currentIndex + dataQPCount) {
      srcNicIdx = *nicPair.srcNicIdx();
      dstNicIdx = *nicPair.dstNicIdx();
      return qpnPairs.at(qpIndex - currentIndex);
    }
    currentIndex += dataQPCount;
  }

  throw std::runtime_error(
      fmt::format(
          "QP index {} out of range for rank {} -> peer {} (total QPs: {})",
          qpIndex,
          rank_,
          peer,
          currentIndex));
}

std::unique_ptr<Message> WorkloadRank::sendWithQPIndex(
    const uint32_t toRank,
    const uint32_t size,
    const uint32_t chunkId,
    const uint32_t qpIndex,
    const uint64_t sendDelayNs,
    const bool writeImm,
    const bool write,
    const uint32_t writeImmData,
    const uint64_t dataPtr) {
  if (parent_ != nullptr) {
    return parent_->sendWithQPIndex(
        toRank,
        size,
        chunkId,
        qpIndex,
        sendDelayNs,
        writeImm,
        write,
        writeImmData,
        dataPtr);
  }

  // sendWithQPIndex requires NIC-aware mode (nicQpnPairMap must be populated)
  if (!isNicAwareModeActive()) {
    throw std::runtime_error(
        "sendWithQPIndex requires NIC-aware mode (nicQpnPairMap must be "
        "populated). Use send() for legacy qpnPairMap mode.");
  }

  // Get total QP count and select QP using round-robin
  const uint32_t totalQPs = getTotalDataQPCount(toRank);
  if (totalQPs == 0) {
    throw std::runtime_error(
        fmt::format("No data QPs available for rank {} -> {}", rank_, toRank));
  }

  const uint32_t selectedQPIndex = qpIndex % totalQPs;
  uint32_t srcNicIdx = 0;
  uint32_t dstNicIdx = 0;
  const auto& qpnPair =
      getDataQPNByIndexWithNic(toRank, selectedQPIndex, srcNicIdx, dstNicIdx);

  XLOGF(
      DBG,
      "[sendWithQPIndex] matchKey={{{}->{},chunk={},qpIdx={}}} selectedQPIndex={} totalQPs={} srcNicIdx={} dstNicIdx={} localQPN={} remoteQPN={} lookupKey={{{},{}}}",
      rank_,
      toRank,
      chunkId,
      qpIndex,
      selectedQPIndex,
      totalQPs,
      srcNicIdx,
      dstNicIdx,
      *qpnPair.local(),
      *qpnPair.remote(),
      rank_,
      toRank);

  // srcNicIdx/dstNicIdx from nicQpnPairMap are 0-indexed.
  return sendWithQPN(
      toRank,
      size,
      chunkId,
      *qpnPair.local(),
      *qpnPair.remote(),
      srcNicIdx,
      dstNicIdx,
      sendDelayNs,
      writeImm,
      write,
      writeImmData,
      0,
      dataPtr);
}

std::unique_ptr<Message> WorkloadRank::recvWithQPIndex(
    const uint32_t fromRank,
    const uint32_t size,
    const uint32_t chunkId,
    const uint32_t qpIndex,
    const uint64_t sendDelayNs,
    const bool isRead,
    const uint64_t dataPtr) {
  if (parent_ != nullptr) {
    return parent_->recvWithQPIndex(
        fromRank, size, chunkId, qpIndex, sendDelayNs, isRead, dataPtr);
  }

  // recvWithQPIndex requires NIC-aware mode (nicQpnPairMap must be populated)
  if (!isNicAwareModeActive()) {
    throw std::runtime_error(
        "recvWithQPIndex requires NIC-aware mode (nicQpnPairMap must be "
        "populated). Use recv() for legacy qpnPairMap mode.");
  }

  // Get total QP count and select QP using round-robin
  const uint32_t totalQPs = getTotalDataQPCount(fromRank);
  if (totalQPs == 0) {
    throw std::runtime_error(
        fmt::format(
            "No data QPs available for rank {} <- {}", rank_, fromRank));
  }

  const uint32_t selectedQPIndex = qpIndex % totalQPs;
  uint32_t srcNicIdx = 0;
  uint32_t dstNicIdx = 0;
  const auto& qpnPair =
      getDataQPNByIndexWithNic(fromRank, selectedQPIndex, srcNicIdx, dstNicIdx);

  XLOGF(
      DBG,
      "[recvWithQPIndex] matchKey={{{}->{},chunk={},qpIdx={}}} selectedQPIndex={} totalQPs={} srcNicIdx={} dstNicIdx={} localQPN={} remoteQPN={} lookupKey={{{},{}}}",
      fromRank,
      rank_,
      chunkId,
      qpIndex,
      selectedQPIndex,
      totalQPs,
      srcNicIdx,
      dstNicIdx,
      *qpnPair.local(),
      *qpnPair.remote(),
      rank_,
      fromRank);

  // For recv: srcNicIdx is our local NIC (we are the destination),
  // dstNicIdx is the sender's NIC (the source)
  // srcNicIdx/dstNicIdx from nicQpnPairMap are 0-indexed.
  return recvWithQPN(
      fromRank,
      size,
      chunkId,
      *qpnPair.local(),
      *qpnPair.remote(),
      srcNicIdx,
      dstNicIdx,
      sendDelayNs,
      isRead,
      dataPtr);
}

std::unique_ptr<Message> WorkloadRank::sendWithNic(
    const uint32_t toRank,
    const uint32_t size,
    const uint32_t chunkId,
    const uint32_t srcNicIdx,
    const uint32_t dstNicIdx,
    const uint32_t endpointId,
    const uint64_t sendDelayNs,
    const bool writeImm,
    const bool write,
    const uint32_t writeImmData,
    const uint64_t dataPtr) {
  if (parent_ != nullptr) {
    return parent_->sendWithNic(
        toRank,
        size,
        chunkId,
        srcNicIdx,
        dstNicIdx,
        endpointId,
        sendDelayNs,
        writeImm,
        write,
        writeImmData,
        dataPtr);
  }

  const auto& qpnPair = getDataQPNs(toRank, srcNicIdx, dstNicIdx);
  return sendWithQPN(
      toRank,
      size,
      chunkId,
      *qpnPair.local(),
      *qpnPair.remote(),
      endpointId,
      endpointId,
      sendDelayNs,
      writeImm,
      write,
      writeImmData,
      0,
      dataPtr);
}

void WorkloadRank::notify(const uint32_t toRank) {
  if (parent_ != nullptr) {
    return parent_->notify(toRank);
  }
  const auto& qpnPair = getNotifyQPNs(toRank);
  // Travel on the NICs that own the notify QP: NIC 0 need not reach the peer,
  // e.g. when it only cables to a rack-local switch.
  const auto nicPair = getNotifyNicPair(toRank);
  auto msg = sendWithQPN(
      toRank,
      kNotifyMsgSize,
      kNotifyConnectionMessageId,
      *qpnPair.local(),
      *qpnPair.remote(),
      /*endpointId=*/*nicPair.srcNicIdx(),
      /*dstEndpointId=*/*nicPair.dstNicIdx());
  sendNotifyMessages_[toRank].emplace_back(std::move(msg));
}

void WorkloadRank::postRecvNotify(const uint32_t fromRank) {
  if (parent_ != nullptr) {
    return parent_->postRecvNotify(fromRank);
  }
  if (!recvNotifyMessages_[fromRank].empty()) {
    return;
  }
  // Use second queue pair for notifies
  const auto& qpnPair = getNotifyQPNs(fromRank);
  const auto nicPair = getNotifyNicPair(fromRank);
  auto msg = recvWithQPN(
      fromRank,
      kNotifyMsgSize,
      kNotifyConnectionMessageId,
      *qpnPair.local(),
      *qpnPair.remote(),
      /*endpointId=*/*nicPair.srcNicIdx(),
      /*dstEndpointId=*/*nicPair.dstNicIdx());
  recvNotifyMessages_[fromRank].emplace_back(std::move(msg));
}

bool WorkloadRank::isNotifyMsg(Message* msg) const {
  if (parent_ != nullptr) {
    return parent_->isNotifyMsg(msg);
  }
  const uint32_t peer = msg->srcRank == rank_ ? msg->dstRank : msg->srcRank;
  const auto& qpnPair = getNotifyQPNs(peer);
  return *qpnPair.local() == msg->localQPN;
}

void WorkloadRank::notifyReceived(const uint32_t fromRank) {
  recvNotifyMessages_.at(fromRank).pop_front();
  notifyRecvCount_[fromRank]++;
  postRecvNotify(fromRank);
  progress();
}

void WorkloadRank::notifyAcked(const uint32_t toRank) {
  sendNotifyMessages_.at(toRank).pop_front();
  notifyAckedCount_[toRank]++;
  progress();
}

void WorkloadRank::onRankComplete(uint32_t workloadId, uint32_t rank) {
  XCHECK(onRankComplete_) << "no rank-complete subscriber registered on rank "
                          << rank;
  onRankComplete_(workloadId, rank);
}

void WorkloadRank::onRequestAcked(Message* msg) {
  XCHECK(onRequestAcked_) << "no request-acked subscriber registered on rank "
                          << rank_;
  onRequestAcked_(msg);
}

void WorkloadRank::onRequestReceived(Message* msg) {
  XCHECK(onRequestReceived_)
      << "no request-received subscriber registered on rank " << rank_;
  onRequestReceived_(msg);
}

void WorkloadRank::requestAcked(Message* msg) {
  if (channelManager_ && !isNotifyMsg(msg)) {
    return channelManager_->requestAcked(msg);
  }
  onRequestAcked(msg);
}

void WorkloadRank::setOnRequestAckedCallback(
    const WorkloadRankParams::OnRequestAckedCallback& cb) {
  onRequestAcked_ = cb;
  if (channelManager_) {
    channelManager_->setOnRequestAckedCallback(cb);
  }
}

void WorkloadRank::requestReceived(Message* msg) {
  if (channelManager_ && !isNotifyMsg(msg)) {
    return channelManager_->requestReceived(msg);
  }
  onRequestReceived(msg);
}

void WorkloadRank::setOnRequestReceivedCallback(
    const WorkloadRankParams::OnRequestReceivedCallback& cb) {
  onRequestReceived_ = cb;
  if (channelManager_) {
    channelManager_->setOnRequestReceivedCallback(cb);
  }
}

bool WorkloadRank::checkNotifyReceived(const uint32_t fromRank) {
  if (parent_ != nullptr) {
    return parent_->checkNotifyReceived(fromRank);
  }
  if (notifyRecvCount_[fromRank] == 0) {
    return false;
  }
  notifyRecvCount_[fromRank]--;
  return true;
}

bool WorkloadRank::checkNotifyAcked(const uint32_t fromRank) {
  if (parent_ != nullptr) {
    return parent_->checkNotifyAcked(fromRank);
  }
  if (notifyAckedCount_[fromRank] == 0) {
    return false;
  }
  notifyAckedCount_[fromRank]--;
  return true;
}

uint64_t WorkloadRank::getTimeNs() {
  return simCtx_->nowNs();
}

bool WorkloadRank::isInSameNvDomain(const uint32_t peerRank) const {
  if (workCfg_->rankToNvDomain().has_value() &&
      !workCfg_->rankToNvDomain()->empty()) {
    if (rank_ == peerRank) {
      return true;
    }
    const auto endIt = workCfg_->rankToNvDomain()->end();
    const auto rankIt = workCfg_->rankToNvDomain()->find(rank_);
    const auto peerIt = workCfg_->rankToNvDomain()->find(peerRank);
    if (rankIt == endIt) {
      throw std::runtime_error(
          fmt::format("Rank {} not found in rankToNvDomain", rank_));
    } else if (peerIt == endIt) {
      throw std::runtime_error(
          fmt::format("Rank {} not found in rankToNvDomain", peerRank));
    }

    return rankIt->second == peerIt->second;
  }

  return false;
}

} // namespace packsim
