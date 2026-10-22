/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <folly/container/F14Map.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/transport/transport.h"
#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

// Parameters are rank and size, respectively
class WorkloadRankTestHarness {
 public:
  virtual ~WorkloadRankTestHarness() = default;

 protected:
  // Callbacks for WorkloadRank; a test overrides them to inspect each message.
  virtual void sendMessage(Message* msg);
  virtual void recvMessage(Message* msg);
  void rankComplete(uint32_t workloadId, uint32_t rank);

  // Utility Functions
  void buildRanks();
  void deliverMessage(Message* msg);
  void ackMessage(Message msg);

  void run();
  void validateStatsPxn();
  void validateStats();
  static void populateRankToId(packsim::workload::thrift::Workload& workCfg);

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  packsim::workload::thrift::Workload workCfg_;
  std::vector<std::unique_ptr<WorkloadRank>> ranks_;
  std::unordered_map<
      std::tuple<
          uint32_t /* src rank */,
          uint32_t /* dst rank */,
          uint32_t /* local QPN */,
          uint32_t /* remote QPN */>,
      std::deque<Message*>>
      recvMsgs_;
  std::unordered_map<std::tuple<uint32_t, uint32_t, uint32_t, uint32_t>, Time>
      nextMsgTime_;
  // Delay before the ACK of a notify message is delivered back to the sender.
  // Unset means notifies are ACKed after the same delay as data messages.
  std::optional<Time> notifyAckDelay_;
  std::unordered_set<uint32_t> completedRanks_;

  struct RankStats {
    RankStats() = default;
    size_t sentBytes{0};
    size_t rcvdBytes{0};
    size_t ackdBytes{0};
    size_t sentMsgs{0};
    size_t rcvdMsgs{0};
    size_t ackdMsgs{0};
    folly::F14FastMap<uint32_t, size_t> sentMsgsByEndpoint;
    folly::F14FastMap<uint32_t, size_t> rcvdMsgsByEndpoint;
  };

  std::vector<RankStats> rankStats_;
};

} // namespace packsim
