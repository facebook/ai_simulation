/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include <folly/container/F14Map.h>

// @oss-disable
#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

class PermutationRank : public WorkloadRank {
 public:
  explicit PermutationRank(const WorkloadRankParams& params);
  void run() override;
  virtual void requestReceived(Message* msg);
  virtual void requestAcked(Message* msg);
  uint64_t getExpectedBytes() override;
  bool complete() override;
  void progress() override;

 protected:
  uint64_t sendSize_;
  uint32_t numChunks_;
  uint32_t srcRank_;
  uint32_t dstRank_;
  // @oss-disable
  // Use maps keyed by chunkId to handle out-of-order message delivery
  // when using multiple QPs/NICs for round-robin distribution
  folly::F14FastMap<uint32_t, std::unique_ptr<Message>> sendMsgs_;
  folly::F14FastMap<uint32_t, std::unique_ptr<Message>> recvMsgs_;
  bool notifyRcvd_{false};

 private:
  void sendMsgs();
  void sendNotify();
  void validateDstRanks(const std::vector<uint32_t>& dstRanks);
};

} // namespace packsim
