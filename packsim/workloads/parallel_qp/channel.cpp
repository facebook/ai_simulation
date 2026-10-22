/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/parallel_qp/channel.h"

#include <memory>
#include <stdexcept>
#include <utility>

#include <fmt/format.h>
#include <folly/logging/xlog.h>

namespace packsim {
Channel::Channel(
    ChannelParams const& params,
    std::vector<packsim::workload::thrift::QPNPair> qpList)
    : messageSizeBytes_(params.messageSizeBytes),
      numQPsPerChannel_(params.numQPsPerChannel),
      qpList_(std::move(qpList)),
      rankToIdList_(params.rankToIdList),
      workloadId_(params.workloadId),
      workload_(params.workload),
      smcb_(params.smcb),
      rmcb_(params.rmcb) {}

void Channel::send(
    const uint32_t fromRank,
    const uint32_t toRank,
    const uint32_t sizeBytes,
    const uint32_t chunkId,
    const uint32_t channelId,
    const uint32_t endpointId,
    const uint64_t sendDelayNs,
    const bool writeImm,
    const bool write,
    const uint32_t writeImmData) {
  setSendCompleted(false);

  numSendsExpected_ = sizeBytes / messageSizeBytes_;
  if (sizeBytes % messageSizeBytes_ != 0) {
    numSendsExpected_++;
  }
  numChunksSent_ = 0;

  // Nothing is dispatched, so no ack will ever arrive to raise the flag.
  if (numSendsExpected_ == 0) {
    setSendCompleted(true);
    return;
  }

  for (uint32_t i = 0; i < numSendsExpected_; i++) {
    const auto qpNum = i % numQPsPerChannel_;
    const auto qpnPair = qpList_.at(qpNum);
    auto messageSize = messageSizeBytes_;
    if (numSendsExpected_ - 1 ==
        i) { // last chunk might be smaller than messageSizeBytes
      messageSize = sizeBytes - ((numSendsExpected_ - 1) * messageSizeBytes_);
    }

    qpSendMessageQueue_[std::make_pair(*qpnPair.local(), *qpnPair.remote())]
        .emplace_back(messageSize);
  }

  for (auto& i : qpSendMessageQueue_) {
    if (!i.second.empty()) {
      auto msg = buildMessage(
          fromRank,
          toRank,
          chunkId,
          channelId,
          endpointId,
          endpointId,
          sendDelayNs,
          writeImm,
          write,
          writeImmData,
          /*isSend=*/true,
          i.second.front(),
          i.first.first,
          i.first.second);
      i.second.pop_front();

      sendWithQPN(msg.get());

      const auto qp = std::make_pair(msg->localQPN, msg->remoteQPN);
      sendMsgs_[qp].emplace_back(std::move(msg));
    }
  }
}

void Channel::requestAcked(Message* msg) {
  numChunksSent_++;
  if (numChunksSent_ == numSendsExpected_) {
    setSendCompleted(true);
    return;
  }

  auto qp = std::make_pair(msg->localQPN, msg->remoteQPN);
  if (qpSendMessageQueue_[qp].empty()) {
    return;
  }
  auto newMsg = buildMessage(
      msg->srcRank,
      msg->dstRank,
      msg->connectionMessageId,
      msg->channelID,
      msg->endpointId,
      msg->dstEndpointId,
      msg->sendDelayNs,
      msg->isWriteImm,
      msg->isWrite,
      msg->writeImmData,
      /*isSend=*/true,
      qpSendMessageQueue_[qp].front(),
      msg->localQPN,
      msg->remoteQPN);

  qpSendMessageQueue_[qp].pop_front();
  sendWithQPN(newMsg.get());
  sendMsgs_[qp].emplace_back(std::move(newMsg));
}

void Channel::requestAckedCleanup(Message* msg) {
  const auto chunkId = msg->connectionMessageId;
  const auto dstRank = msg->dstRank;
  auto& msgQueue = sendMsgs_.at(std::make_pair(msg->localQPN, msg->remoteQPN));
  if (msgQueue.empty()) {
    throw std::runtime_error(
        fmt::format(
            "Channel received CQE for chunk {} to {}, but no chunk is outstanding",
            chunkId,
            dstRank));
  }
  const auto& expMsg = msgQueue.front();
  if (expMsg->connectionMessageId != chunkId) {
    throw std::runtime_error(
        fmt::format(
            "Channel received CQE for chunk {} to {}, expected {}",
            chunkId,
            dstRank,
            expMsg->connectionMessageId));
  }
  msgQueue.pop_front();
}

void Channel::requestReceived(Message* /* msg */) {
  numChunksReceived_++;

  if (numChunksReceived_ == numReceivesExpected_) {
    numChunksReceived_ = 0;
    setRecvCompleted(true);
  }
}

void Channel::requestReceivedCleanup(Message* msg) {
  const auto chunkId = msg->connectionMessageId;
  const auto srcRank = msg->srcRank;
  auto& msgQueue = recvMsgs_.at(std::make_pair(msg->localQPN, msg->remoteQPN));
  if (msgQueue.empty()) {
    throw std::runtime_error(
        fmt::format(
            "Channel received chunk {} from {}, but no receive is outstanding",
            chunkId,
            srcRank));
  }
  const auto& expMsg = msgQueue.front();
  if (expMsg->connectionMessageId != chunkId) {
    throw std::runtime_error(
        fmt::format(
            "Channel received chunk {} from {}, expected {}",
            chunkId,
            srcRank,
            expMsg->connectionMessageId));
  }
  msgQueue.pop_front();
}

std::unique_ptr<Message> Channel::buildMessage(
    const uint32_t fromRank,
    const uint32_t toRank,
    const uint32_t chunkId,
    const uint32_t channelId,
    const uint32_t endpointId,
    const uint32_t dstEndpointId,
    const uint64_t sendDelayNs,
    const bool writeImm,
    const bool write,
    const uint32_t writeImmData,
    const bool isSend,
    const uint32_t sizeBytes,
    const uint32_t localQPN,
    const uint32_t remoteQPN) const {
  if (!rankToIdList_.contains(fromRank)) {
    XLOGF(FATAL, "Rank {} not found in rankToIdList", fromRank);
  } else if (!rankToIdList_.contains(toRank)) {
    XLOGF(FATAL, "Rank {} not found in rankToIdList", toRank);
  }
  auto msg = std::make_unique<Message>();
  msg->connectionMessageId = chunkId;
  msg->srcRank = fromRank;
  msg->dstRank = toRank;
  msg->srcIdx = FlexDeviceId(rankToIdList_.at(fromRank).at(endpointId));
  msg->dstIdx = FlexDeviceId(rankToIdList_.at(toRank).at(dstEndpointId));
  msg->send = isSend;
  msg->recv = !isSend;
  msg->workloadId = workloadId_;
  msg->iteration = iteration_;
  msg->workload = workload_;
  msg->endpointId = endpointId;
  msg->dstEndpointId = dstEndpointId;
  msg->sendDelayNs = sendDelayNs;
  msg->isWriteImm = writeImm;
  msg->isWrite = write;
  msg->writeImmData = writeImmData;
  msg->channelID = channelId;
  msg->sizeBytes = sizeBytes;
  msg->localQPN = localQPN;
  msg->remoteQPN = remoteQPN;
  return msg;
}

void Channel::sendWithQPN(Message* msg) {
  smcb_(msg);
}

void Channel::recv(
    const uint32_t fromRank,
    const uint32_t toRank,
    const uint32_t sizeBytes,
    const uint32_t chunkId,
    const uint32_t channelId,
    const uint32_t endpointId,
    const uint64_t sendDelayNs) {
  setRecvCompleted(false);

  numChunksReceived_ = 0;
  numReceivesExpected_ = sizeBytes / messageSizeBytes_;
  if (sizeBytes % messageSizeBytes_ != 0) {
    numReceivesExpected_++;
  }

  // Nothing is posted, so no completion will ever arrive to raise the flag.
  if (numReceivesExpected_ == 0) {
    setRecvCompleted(true);
    return;
  }

  for (uint32_t i = 0; i < numReceivesExpected_; i++) {
    const auto qpNum = i % numQPsPerChannel_;
    const auto qpnPair = qpList_.at(qpNum);
    auto messageSize = messageSizeBytes_;
    if (numReceivesExpected_ - 1 ==
        i) { // last chunk might be smaller than messageSizeBytes
      messageSize =
          sizeBytes - ((numReceivesExpected_ - 1) * messageSizeBytes_);
    }
    auto msg = buildMessage(
        fromRank,
        toRank,
        chunkId,
        channelId,
        endpointId,
        endpointId,
        sendDelayNs,
        /*writeImm=*/false,
        /*write=*/false,
        0,
        /*isSend=*/false,
        messageSize,
        *qpnPair.local(),
        *qpnPair.remote());

    recvWithQPN(msg.get());

    const auto qp = std::make_pair(*qpnPair.local(), *qpnPair.remote());
    recvMsgs_[qp].emplace_back(std::move(msg));
  }
}

void Channel::recvWithQPN(Message* msg) {
  rmcb_(msg);
}

void Channel::setSendCompleted(const bool isCompleted) {
  isSendCompleted_ = isCompleted;
}

bool Channel::hasSendCompleted() const noexcept {
  return isSendCompleted_;
}

void Channel::setRecvCompleted(const bool isCompleted) {
  isRecvCompleted_ = isCompleted;
}

bool Channel::hasRecvCompleted() const noexcept {
  return isRecvCompleted_;
}

} // namespace packsim
