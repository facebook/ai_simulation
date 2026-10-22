/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include <folly/container/F14Map.h>

#include "packsim/transport/transport.h"
#include "packsim/workloads/parallel_qp/channel.h"
#include "packsim/workloads/parallel_qp/params.h"

namespace packsim {
// ChannelManager class is responsible for providing functionality to use a set
// of channels between a pair of ranks for sending and receiving messages
class ChannelManager {
 private:
  const uint32_t numChannels_;
  const uint32_t numQPsPerChannel_;
  const uint32_t rank_;
  const uint32_t workloadId_;
  uint32_t iteration_{1};
  folly::F14FastMap<
      uint32_t, /* peer rank */
      std::vector<std::unique_ptr<Channel>>>
      rankChannelMap_;

  ChannelParams::OnRequestAckedCallback onRequestAcked_;
  ChannelParams::OnRequestReceivedCallback onRequestReceived_;

  // keep track of send/recv message sizes for each (fromRank, toRank) pair
  folly::F14FastMap<
      std::pair<uint32_t /* src rank */, uint32_t /* dst rank */>,
      uint32_t /* message size bytes */>
      rankSendMessageSizeMap_;
  folly::F14FastMap<
      std::pair<uint32_t /* src rank */, uint32_t /* dst rank */>,
      uint32_t /* message size bytes */>
      rankRecvMessageSizeMap_;

  // keep track of send/recv messages to prevent out of order receives
  folly::F14FastMap<
      std::pair<uint32_t /* src rank */, uint32_t /* dst rank */>,
      std::queue<Message>>
      sendMessageQueue_;
  folly::F14FastMap<
      std::pair<uint32_t, /* src rank */ uint32_t /* dst rank */>,
      std::queue<Message>>
      recvMessageQueue_;

  void createChannels(ChannelParams const& params);
  bool areSendChannelsFree(uint32_t rank) const;
  bool areRecvChannelsFree(uint32_t rank) const;
  Message enqueue(
      uint32_t fromRank,
      uint32_t toRank,
      uint32_t sizeBytes,
      uint32_t chunkId,
      uint32_t endpointId,
      uint64_t sendDelayNs,
      bool writeImm,
      bool write,
      uint32_t writeImmData,
      bool isSend);

  void sendToChannels(
      uint32_t fromRank,
      uint32_t toRank,
      uint32_t sizeBytes,
      uint32_t chunkId,
      uint32_t endpointId,
      uint64_t sendDelayNs,
      bool writeImm,
      bool write,
      uint32_t writeImmData);

  void recvOnChannels(
      uint32_t fromRank,
      uint32_t toRank,
      uint32_t sizeBytes,
      uint32_t chunkId,
      uint32_t endpointId,
      uint64_t sendDelayNs);

 public:
  /**
   * Build numChannels channels to each peer that `params.rank` has queue
   * pairs with in `params.qpPairMap`, each taking the next numQPsPerChannel
   * queue pairs of that peer's list. A rank with no entry gets no channels.
   *
   * @param params  Channel settings, queue pairs and callbacks.
   *
   * @throw std::invalid_argument  If a peer's list holds fewer than
   *                               numChannels x numQPsPerChannel queue pairs.
   */
  explicit ChannelManager(ChannelParams const& params);

  // Stamp `iter` on the messages this manager and every channel create from
  // now on.
  void setIteration(uint32_t iter) noexcept;
  std::unique_ptr<Message> send(
      uint32_t fromRank,
      uint32_t toRank,
      uint32_t sizeBytes,
      uint32_t chunkId,
      uint32_t endpointId,
      uint64_t sendDelayNs,
      bool writeImm,
      bool write,
      uint32_t writeImmData);

  std::unique_ptr<Message> recv(
      uint32_t fromRank,
      uint32_t toRank,
      uint32_t sizeBytes,
      uint32_t chunkId,
      uint32_t endpointId = 0,
      uint64_t sendDelayNs = 0);

  void requestAcked(Message* msg);
  void requestReceived(Message* msg);
  void setOnRequestAckedCallback(
      const ChannelParams::OnRequestAckedCallback& cb);
  void setOnRequestReceivedCallback(
      const ChannelParams::OnRequestReceivedCallback& cb);

  // Raise the request-acked / request-received events. The members are read
  // nowhere else.
  void onRequestAcked(Message* msg);
  void onRequestReceived(Message* msg);
};

} // namespace packsim
