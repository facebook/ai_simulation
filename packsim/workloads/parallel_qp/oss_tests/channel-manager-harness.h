/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "packsim/transport/transport.h"
#include "packsim/workloads/parallel_qp/channel_manager.h"
#include "packsim/workloads/parallel_qp/params.h"

namespace packsim {

class ChannelManagerHarness {
 public:
  std::unique_ptr<ChannelManager> channelManagerRank0, channelManagerRank1;
  explicit ChannelManagerHarness(
      uint32_t numChannels,
      uint32_t numQPsPerChannel,
      uint32_t channelMsgSizeBytes,
      uint32_t numMessages);
  void sendMessage(Message* msg);
  void recvMessage(Message* msg);
  void requestReceived(Message* msg);
  void requestAcked(Message* msg);
  void sendCompleted(Message* msg);
  void receiveCompleted(Message* msg);
  void validateStats();
  void scheduleSendMessage(Message* msg);
  void run(uint32_t msgSizeBytes);

 private:
  std::function<void(Message*)> smcb, rmcb, sendCompletedCb, recvCompletedCb;
  std::unordered_map<
      std::tuple<
          uint32_t /* src rank */,
          uint32_t /* dst rank */,
          uint32_t /* local QPN */,
          uint32_t /* remote QPN */>,
      std::deque<Message*>>
      recvMsgs_;
  static constexpr uint32_t kNumEndpoints = 2;
  const uint32_t numMessages_;
  uint32_t msgSent{0}, msgReceived{0};
  std::map<uint32_t, std::vector<std::vector<uint32_t>>> rankToIdList_;
  packsim::workload::thrift::QPNPairMapMap qpPairMap_;
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  void populateRankToId();
  void buildQPNMap(uint32_t numQPs);

  std::deque<std::unique_ptr<Message>> sendQ;
  std::deque<std::unique_ptr<Message>> recvQ;

  uint32_t bytesSent{0}, bytesReceived{0};
  uint32_t channelMsgSent{0}, channelMsgReceived{0};
  uint32_t expectedBytes{0};
};

} // namespace packsim
