/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/parallel_qp/oss_tests/channel-manager-harness.h"

#include <cstdint>
#include <memory>
#include <stdexcept>

#include <fmt/format.h>
#include <folly/container/F14Map.h>

namespace packsim {

ChannelManagerHarness::ChannelManagerHarness(
    uint32_t numChannels,
    uint32_t numQPsPerChannel,
    uint32_t channelMsgSizeBytes,
    uint32_t numMessages)
    : numMessages_(numMessages) {
  populateRankToId();
  buildQPNMap(numChannels * numQPsPerChannel);
  smcb = [this](Message* msg) { scheduleSendMessage(msg); };
  rmcb = [this](Message* msg) { recvMessage(msg); };
  sendCompletedCb = [this](Message* msg) { sendCompleted(msg); };
  recvCompletedCb = [this](Message* msg) { receiveCompleted(msg); };

  channelManagerRank0 = std::make_unique<ChannelManager>(ChannelParams{
      .smcb = smcb,
      .rmcb = rmcb,
      .numChannels = numChannels,
      .numQPsPerChannel = numQPsPerChannel,
      .messageSizeBytes = channelMsgSizeBytes,
      .workloadId = 0,
      .workload = nullptr,
      .rankToIdList = rankToIdList_,
      .qpPairMap = qpPairMap_,
      .rank = 0,
  });
  channelManagerRank0->setOnRequestAckedCallback(sendCompletedCb);
  channelManagerRank0->setOnRequestReceivedCallback(recvCompletedCb);

  channelManagerRank1 = std::make_unique<ChannelManager>(ChannelParams{
      .smcb = smcb,
      .rmcb = rmcb,
      .numChannels = numChannels,
      .numQPsPerChannel = numQPsPerChannel,
      .messageSizeBytes = channelMsgSizeBytes,
      .workloadId = 0,
      .workload = nullptr,
      .rankToIdList = rankToIdList_,
      .qpPairMap = qpPairMap_,
      .rank = 1,
  });

  channelManagerRank1->setOnRequestAckedCallback(sendCompletedCb);
  channelManagerRank1->setOnRequestReceivedCallback(recvCompletedCb);
}

void ChannelManagerHarness::scheduleSendMessage(Message* msg) {
  scheduler_.scheduleAfter(
      &simCtx_,
      Time::fromNs(5000),
      &ChannelManagerHarness::sendMessage,
      this,
      msg);
}

void ChannelManagerHarness::sendMessage(Message* msg) {
  // pass as callback to channel manager
  auto& q =
      recvMsgs_[{msg->srcRank, msg->dstRank, msg->localQPN, msg->remoteQPN}];
  if (q.size() == 0) {
    throw std::runtime_error(
        fmt::format(
            "Queue should not be empty src {}, dst {}, local qpn {} remote {}",
            msg->srcRank,
            msg->dstRank,
            msg->localQPN,
            msg->remoteQPN));
  }

  if (msg->connectionMessageId != q.front()->connectionMessageId) {
    throw std::runtime_error(
        fmt::format(
            "Received message out of order src {}, dst {}, local qpn {} remote {} connection id {} expected connection id {}",
            msg->srcRank,
            msg->dstRank,
            msg->localQPN,
            msg->remoteQPN,
            msg->connectionMessageId,
            q.front()->connectionMessageId));
  }

  if (msg->sizeBytes != q.front()->sizeBytes) {
    throw std::runtime_error(
        fmt::format(
            "Received message with wrong size src {}, dst {}, local qpn {} remote {} size {} expected size {}",
            msg->srcRank,
            msg->dstRank,
            msg->localQPN,
            msg->remoteQPN,
            msg->sizeBytes,
            q.front()->sizeBytes));
  }
  bytesSent += msg->sizeBytes;
  channelMsgSent++;
  requestReceived(msg);
  q.pop_front();
  requestAcked(msg);
}

void ChannelManagerHarness::recvMessage(Message* msg) {
  // Key is {srcRank, dstRank, srcQPN, dstQPN} so need to flip local and
  // remote
  bytesReceived += msg->sizeBytes;
  channelMsgReceived++;
  recvMsgs_[{msg->srcRank, msg->dstRank, msg->remoteQPN, msg->localQPN}]
      .emplace_back(msg);
}

void ChannelManagerHarness::requestReceived(Message* msg) {
  channelManagerRank1->requestReceived(msg);
}

void ChannelManagerHarness::requestAcked(Message* msg) {
  channelManagerRank0->requestAcked(msg);
}

void ChannelManagerHarness::sendCompleted(Message* msg) {
  msgSent++;
  if (sendQ.size() == 0) {
    throw std::runtime_error(
        fmt::format(
            "Send queue should not be empty src {}, dst {}, local qpn {} remote {} connection id {}",
            msg->srcRank,
            msg->dstRank,
            msg->localQPN,
            msg->remoteQPN,
            msg->connectionMessageId));
  }

  auto& front = sendQ.front();
  if (front->connectionMessageId != msg->connectionMessageId) {
    throw std::runtime_error(
        fmt::format(
            "SendCompleted callback out of order src {}, dst {}, local qpn {} remote {} connection id {} expected connection id {}",
            msg->srcRank,
            msg->dstRank,
            msg->localQPN,
            msg->remoteQPN,
            msg->connectionMessageId,
            front->connectionMessageId));
  }
  sendQ.pop_front();

  if (msgSent < numMessages_) {
    sendQ.emplace_back(channelManagerRank0->send(
        msg->srcRank,
        msg->dstRank,
        msg->sizeBytes,
        msgSent,
        0,
        0,
        /*writeImm=*/false,
        /*write=*/false,
        false));
  }
}

void ChannelManagerHarness::receiveCompleted(Message* msg) {
  msgReceived++;
  auto& front = recvQ.front();
  if (front->connectionMessageId != msg->connectionMessageId) {
    throw std::runtime_error(
        fmt::format(
            "ReceiveCompleted callback out of order src {}, dst {}, local qpn {} remote {} connection id {} expected connection id {}",
            msg->srcRank,
            msg->dstRank,
            msg->localQPN,
            msg->remoteQPN,
            msg->connectionMessageId,
            front->connectionMessageId));
  }
  recvQ.pop_front();

  if (msgReceived < numMessages_) {
    recvQ.emplace_back(channelManagerRank1->recv(
        msg->srcRank, msg->dstRank, msg->sizeBytes, msgReceived, 0));
  }
}

void ChannelManagerHarness::populateRankToId() {
  rankToIdList_.clear();
  for (uint32_t i = 0; i < kNumEndpoints; i++) {
    rankToIdList_.insert({i, {{i}}});
  }
};

void ChannelManagerHarness::validateStats() {
  if (msgSent != numMessages_ || msgReceived != numMessages_) {
    throw std::runtime_error(
        fmt::format(
            "Not all messages sent or received {} {} {}",
            msgSent,
            msgReceived,
            numMessages_));
  }

  if (channelMsgSent != channelMsgReceived || channelMsgSent == 0 ||
      channelMsgReceived == 0) {
    throw std::runtime_error(
        fmt::format(
            "Not all channel messages sent or received {} {}",
            channelMsgSent,
            channelMsgReceived));
  }

  if (bytesSent != bytesReceived || bytesSent == 0 || bytesReceived == 0 ||
      bytesReceived != expectedBytes) {
    throw std::runtime_error(
        fmt::format(
            "Not all bytes sent or received {} {} {}",
            channelMsgSent,
            channelMsgReceived,
            expectedBytes));
  }
}

void ChannelManagerHarness::run(uint32_t msgSizeBytes) {
  expectedBytes = msgSizeBytes * numMessages_;
  simCtx_.scheduleNow([this, msgSizeBytes] {
    recvQ.emplace_back(channelManagerRank1->recv(0, 1, msgSizeBytes, 0, 0));
    sendQ.emplace_back(channelManagerRank0->send(
        0,
        1,
        msgSizeBytes,
        0,
        0,
        0,
        /*writeImm=*/false,
        /*write=*/false,
        false));
  });
  scheduler_.run();
}

void ChannelManagerHarness::buildQPNMap(uint32_t numQPs) {
  folly::F14FastMap<uint32_t, uint32_t> nextQpId;
  for (uint32_t rank1 = 0; rank1 < kNumEndpoints; rank1++) {
    for (uint32_t rank2 = rank1 + 1; rank2 < kNumEndpoints; rank2++) {
      for (uint32_t i = 0; i < numQPs; i++) {
        uint32_t qp1 = ++nextQpId[rank1];
        uint32_t qp2 = ++nextQpId[rank2];
        packsim::workload::thrift::QPNPair pair1, pair2;
        pair1.local() = qp1;
        pair1.remote() = qp2;
        pair2.local() = qp2;
        pair2.remote() = qp1;
        qpPairMap_[rank1][rank2].emplace_back(pair1);
        qpPairMap_[rank2][rank1].emplace_back(pair2);
      }
    }
  }
}

}; // namespace packsim
