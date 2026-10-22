/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <deque>
#include <list>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include <folly/container/F14Map.h>

#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/transport/transport.h"
#include "packsim/workloads/parallel_qp/params.h"

namespace packsim {

class Channel {
 private:
  const uint32_t messageSizeBytes_{1024 * 1024};
  const uint32_t numQPsPerChannel_{0};
  bool isSendCompleted_{true};
  bool isRecvCompleted_{true};

  // maintain a list of messages to be sent for each QP
  folly::F14FastMap<
      std::pair<uint32_t /* local QP */, uint32_t /* remote QP */>,
      std::list<uint32_t /* message size bytes */>>
      qpSendMessageQueue_;

  uint32_t numSendsExpected_{0};
  uint32_t numChunksSent_{0};
  uint32_t numChunksReceived_{0};
  uint32_t numReceivesExpected_{0};

  const std::vector<packsim::workload::thrift::QPNPair> qpList_;
  // The workload's rank-to-device list, which must outlive this channel as
  // `workload_` must.
  const std::map<uint32_t, std::vector<std::vector<uint32_t>>>& rankToIdList_;
  const uint32_t workloadId_;
  uint32_t iteration_{1};
  const packsim::workload::thrift::Workload* workload_;
  const ChannelParams::SubmitMessageFn& smcb_;
  const ChannelParams::SubmitMessageFn& rmcb_;

  void setSendCompleted(bool isCompleted);
  void setRecvCompleted(bool isCompleted);
  std::unique_ptr<Message> buildMessage(
      uint32_t fromRank,
      uint32_t toRank,
      uint32_t chunkId,
      uint32_t channelId,
      uint32_t endpointId,
      uint32_t dstEndpointId,
      uint64_t sendDelayNs,
      bool writeImm,
      bool write,
      uint32_t writeImmData,
      bool isSend,
      uint32_t sizeBytes,
      uint32_t localQPN,
      uint32_t remoteQPN) const;

  // Messages in flight to (send) and from (recv) this channel's one peer rank,
  // keyed by QP pair. `ChannelManager` routes every message exchanged with a
  // peer to that peer's channels.
  std::map<
      std::pair<uint32_t /* local QP */, uint32_t /* remote QP */>,
      std::deque<std::unique_ptr<Message>>>
      sendMsgs_;
  std::map<
      std::pair<uint32_t /* local QP */, uint32_t /* remote QP */>,
      std::deque<std::unique_ptr<Message>>>
      recvMsgs_;

 public:
  Channel(
      ChannelParams const& params,
      std::vector<packsim::workload::thrift::QPNPair> qpList);

  // Stamp `iter` on the messages this channel creates from now on.
  void setIteration(uint32_t iter) noexcept {
    iteration_ = iter;
  }

  void send(
      uint32_t fromRank,
      uint32_t toRank,
      uint32_t sizeBytes,
      uint32_t chunkId,
      uint32_t channelId,
      uint32_t endpointId,
      uint64_t sendDelayNs,
      bool writeImm,
      bool write,
      uint32_t writeImmData);

  void recv(
      uint32_t fromRank,
      uint32_t toRank,
      uint32_t sizeBytes,
      uint32_t chunkId,
      uint32_t channelId,
      uint32_t endpointId = 0,
      uint64_t sendDelayNs = 0);

  void sendWithQPN(Message* msg);
  void recvWithQPN(Message* msg);
  void requestAcked(Message* msg);
  void requestAckedCleanup(Message* msg);
  void requestReceived(Message* msg);
  void requestReceivedCleanup(Message* msg);
  bool hasSendCompleted() const noexcept;
  bool hasRecvCompleted() const noexcept;
};

} // namespace packsim
