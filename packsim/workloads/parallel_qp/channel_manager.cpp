/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/parallel_qp/channel_manager.h"

#include <cstdint>
#include <memory>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/logging/xlog.h>

namespace packsim {

ChannelManager::ChannelManager(ChannelParams const& params)
    : numChannels_(params.numChannels),
      numQPsPerChannel_(params.numQPsPerChannel),
      rank_(params.rank),
      workloadId_(params.workloadId) {
  XLOGF_IF(FATAL, params.numChannels == 0, "Number of channels cannot be 0");
  XLOGF_IF(
      FATAL,
      params.numQPsPerChannel == 0,
      "Number of QPs per channel cannot be 0");
  XLOGF_IF(
      FATAL, params.messageSizeBytes == 0, "Message size bytes cannot be 0");
  createChannels(params);
}

void ChannelManager::setIteration(uint32_t const iter) noexcept {
  iteration_ = iter;
  for (auto& [_, channels] : rankChannelMap_) {
    for (auto& channel : channels) {
      channel->setIteration(iter);
    }
  }
}

void ChannelManager::createChannels(ChannelParams const& params) {
  // A rank with no peers in this workload, such as an incast rank above
  // numSources or a compute rank, has no queue pairs and needs no channels.
  const auto peersIt = params.qpPairMap.find(rank_);
  if (peersIt == params.qpPairMap.end()) {
    return;
  }
  const uint64_t numQpsNeeded = uint64_t{numChannels_} * numQPsPerChannel_;
  for (const auto& [peerRank, qpList] : peersIt->second) {
    if (qpList.size() < numQpsNeeded) {
      throw std::invalid_argument(
          fmt::format(
              "Workload {}: rank {} has {} queue pairs to rank {} in "
              "qpnPairMap, but channelSettings needs {} ({} channels x {} "
              "queue pairs per channel)",
              workloadId_,
              rank_,
              qpList.size(),
              peerRank,
              numQpsNeeded,
              numChannels_,
              numQPsPerChannel_));
    }
    const std::span<const packsim::workload::thrift::QPNPair> allQps(qpList);
    for (size_t i = 0; i < numChannels_; i++) {
      const auto channelQps =
          allQps.subspan(i * numQPsPerChannel_, numQPsPerChannel_);
      auto qps = std::vector<packsim::workload::thrift::QPNPair>(
          channelQps.begin(), channelQps.end());
      auto ch = std::make_unique<Channel>(params, qps);
      rankChannelMap_[peerRank].emplace_back(std::move(ch));
    }
  }
}

Message ChannelManager::enqueue(
    const uint32_t fromRank,
    const uint32_t toRank,
    const uint32_t sizeBytes,
    const uint32_t chunkId,
    const uint32_t endpointId,
    const uint64_t sendDelayNs,
    const bool writeImm,
    const bool write,
    const uint32_t writeImmData,
    const bool isSend) {
  auto msg = Message{};
  msg.srcRank = fromRank;
  msg.dstRank = toRank;
  msg.sizeBytes = sizeBytes;
  msg.connectionMessageId = chunkId;
  msg.endpointId = endpointId;
  msg.sendDelayNs = sendDelayNs;
  msg.isWriteImm = writeImm;
  msg.isWrite = write;
  msg.writeImmData = writeImmData;
  msg.workloadId = workloadId_;
  msg.iteration = iteration_;
  if (isSend) {
    sendMessageQueue_[std::make_pair(fromRank, toRank)].push(msg);
  } else {
    recvMessageQueue_[std::make_pair(fromRank, toRank)].push(msg);
  }
  return msg;
}

std::unique_ptr<Message> ChannelManager::send(
    const uint32_t fromRank,
    const uint32_t toRank,
    const uint32_t sizeBytes,
    const uint32_t chunkId,
    const uint32_t endpointId,
    const uint64_t sendDelayNs,
    const bool writeImm,
    const bool write,
    const uint32_t writeImmData) {
  auto msg = enqueue(
      fromRank,
      toRank,
      sizeBytes,
      chunkId,
      endpointId,
      sendDelayNs,
      writeImm,
      write,
      writeImmData,
      /*isSend=*/true);

  if (areSendChannelsFree(toRank)) {
    const auto front =
        sendMessageQueue_[std::make_pair(fromRank, toRank)].front();
    sendMessageQueue_[std::make_pair(fromRank, toRank)].pop();
    sendToChannels(
        front.srcRank,
        front.dstRank,
        front.sizeBytes,
        front.connectionMessageId,
        front.endpointId,
        front.sendDelayNs,
        front.isWriteImm,
        front.isWrite,
        front.writeImmData);
  }
  return std::make_unique<Message>(msg);
}

void ChannelManager::sendToChannels(
    const uint32_t fromRank,
    const uint32_t toRank,
    const uint32_t sizeBytes,
    const uint32_t chunkId,
    const uint32_t endpointId,
    const uint64_t sendDelayNs,
    const bool writeImm,
    const bool write,
    const uint32_t writeImmData) {
  for (uint32_t i = 0; i < numChannels_; i++) {
    auto chunkSize = sizeBytes / numChannels_;
    if (i == numChannels_ - 1) {
      chunkSize = sizeBytes - chunkSize * (numChannels_ - 1);
    }
    if (chunkSize == 0) {
      continue;
    }

    rankChannelMap_.at(toRank).at(i)->send(
        fromRank,
        toRank,
        chunkSize,
        chunkId,
        i,
        endpointId,
        sendDelayNs,
        writeImm,
        write,
        writeImmData);
  }
  rankSendMessageSizeMap_[std::make_pair(fromRank, toRank)] = sizeBytes;
}

std::unique_ptr<Message> ChannelManager::recv(
    const uint32_t fromRank,
    const uint32_t toRank,
    const uint32_t sizeBytes,
    const uint32_t chunkId,
    const uint32_t endpointId,
    const uint64_t sendDelayNs) {
  auto msg = enqueue(
      fromRank,
      toRank,
      sizeBytes,
      chunkId,
      endpointId,
      sendDelayNs,
      /*writeImm=*/false,
      /*write=*/false,
      0,
      /*isSend=*/false);

  if (areRecvChannelsFree(fromRank)) {
    const auto front =
        recvMessageQueue_[std::make_pair(fromRank, toRank)].front();
    recvMessageQueue_[std::make_pair(fromRank, toRank)].pop();
    recvOnChannels(
        front.srcRank,
        front.dstRank,
        front.sizeBytes,
        front.connectionMessageId,
        front.endpointId,
        front.sendDelayNs);
  }

  return std::make_unique<Message>(msg);
}

void ChannelManager::recvOnChannels(
    const uint32_t fromRank,
    const uint32_t toRank,
    const uint32_t sizeBytes,
    const uint32_t chunkId,
    const uint32_t endpointId,
    const uint64_t sendDelayNs) {
  for (uint32_t i = 0; i < numChannels_; i++) {
    auto chunkSize = sizeBytes / numChannels_;
    if (i == numChannels_ - 1) {
      chunkSize = sizeBytes - chunkSize * (numChannels_ - 1);
    }

    if (chunkSize == 0) {
      continue;
    }

    rankChannelMap_.at(fromRank).at(i)->recv(
        fromRank, toRank, chunkSize, chunkId, i, endpointId, sendDelayNs);
  }

  rankRecvMessageSizeMap_[std::make_pair(fromRank, toRank)] = sizeBytes;
}

void ChannelManager::requestAcked(Message* msg) {
  rankChannelMap_.at(msg->dstRank).at(msg->channelID)->requestAcked(msg);

  const bool allChannelsDone = areSendChannelsFree(msg->dstRank);
  if (!allChannelsDone) {
    rankChannelMap_.at(msg->dstRank)
        .at(msg->channelID)
        ->requestAckedCleanup(msg);
    return;
  }

  msg->sizeBytes =
      rankSendMessageSizeMap_[std::make_pair(msg->srcRank, msg->dstRank)];

  auto& q = sendMessageQueue_[std::make_pair(msg->srcRank, msg->dstRank)];
  if (!q.empty()) {
    const auto front = q.front();
    q.pop();
    sendToChannels(
        front.srcRank,
        front.dstRank,
        front.sizeBytes,
        front.connectionMessageId,
        front.endpointId,
        front.sendDelayNs,
        front.isWriteImm,
        front.isWrite,
        front.writeImmData);
  }
  onRequestAcked(msg);
  rankChannelMap_.at(msg->dstRank).at(msg->channelID)->requestAckedCleanup(msg);
}

void ChannelManager::requestReceived(Message* msg) {
  rankChannelMap_.at(msg->srcRank).at(msg->channelID)->requestReceived(msg);

  if (!areRecvChannelsFree(msg->srcRank)) {
    rankChannelMap_.at(msg->srcRank)
        .at(msg->channelID)
        ->requestReceivedCleanup(msg);
    return;
  }

  msg->sizeBytes =
      rankRecvMessageSizeMap_[std::make_pair(msg->srcRank, msg->dstRank)];

  auto& q = recvMessageQueue_[std::make_pair(msg->srcRank, msg->dstRank)];
  if (!q.empty()) {
    const auto front = q.front();
    q.pop();
    recvOnChannels(
        front.srcRank,
        front.dstRank,
        front.sizeBytes,
        front.connectionMessageId,
        front.endpointId,
        front.sendDelayNs);
  }

  onRequestReceived(msg);
  rankChannelMap_.at(msg->srcRank)
      .at(msg->channelID)
      ->requestReceivedCleanup(msg);
}

bool ChannelManager::areSendChannelsFree(const uint32_t rank) const {
  const auto& channels = rankChannelMap_.at(rank);
  bool allChannelsDone = true;
  for (uint32_t i = 0; i < numChannels_; i++) {
    allChannelsDone = channels.at(i)->hasSendCompleted() && allChannelsDone;
  }
  return allChannelsDone;
}

bool ChannelManager::areRecvChannelsFree(const uint32_t rank) const {
  const auto& channels = rankChannelMap_.at(rank);
  bool allChannelsDone = true;
  for (uint32_t i = 0; i < numChannels_; i++) {
    allChannelsDone = channels.at(i)->hasRecvCompleted() && allChannelsDone;
  }
  return allChannelsDone;
}

void ChannelManager::setOnRequestAckedCallback(
    const ChannelParams::OnRequestAckedCallback& cb) {
  onRequestAcked_ = cb;
}

void ChannelManager::setOnRequestReceivedCallback(
    const ChannelParams::OnRequestReceivedCallback& cb) {
  onRequestReceived_ = cb;
}

void ChannelManager::onRequestAcked(Message* msg) {
  XCHECK(onRequestAcked_) << "no request-acked subscriber on channel manager";
  onRequestAcked_(msg);
}

void ChannelManager::onRequestReceived(Message* msg) {
  XCHECK(onRequestReceived_)
      << "no request-received subscriber on channel manager";
  onRequestReceived_(msg);
}

} // namespace packsim
