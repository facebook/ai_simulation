/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <vector>

#include <folly/container/F14Map.h>
#include <folly/container/vector_bool.h>

#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

class AgRsRecursiveRank : public WorkloadRank {
 public:
  explicit AgRsRecursiveRank(const WorkloadRankParams& params);
  void run() override;
  void requestReceived(Message* msg);
  void requestAcked(Message* msg);
  uint64_t getExpectedBytes() override;
  bool complete() override;
  void progress() override;
  const std::vector<uint32_t>& getPeers() const noexcept {
    return peers_;
  }

 private:
  uint64_t sizeBytes_;
  uint32_t nSteps_;
  uint32_t sendsPostedThisStep_{0};
  uint32_t completedSendsThisStep_{0};
  uint32_t completedRecvsThisStep_{0};
  uint32_t stepMsgs_{1};
  uint32_t stepMsgSize_;
  uint32_t step_{0};
  bool complete_{false};
  packsim::workload::thrift::WorkloadSpecificConfig::Type wkldType_;
  packsim::workload::thrift::AllReduceAlgorithm algorithm_;
  std::vector<uint32_t> peers_;
  folly::F14FastMap<uint32_t, uint32_t> peerToStep_;
  std::vector<uint32_t> dists_;
  folly::vector_bool<> ctrlRcvd_;
  std::vector<std::deque<std::unique_ptr<Message>>> sends_;
  std::vector<std::deque<std::unique_ptr<Message>>> recvs_;
};

} // namespace packsim
