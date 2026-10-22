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

#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/workloads/permutation.h"

namespace packsim {

/**
 * PermutationPushPullRank implements WRITE and READ operations for Permutation
 * workloads. It derives from PermutationRank and replaces the SEND/RECV path
 * with PUSH (WRITE) or PULL (READ) semantics.
 *
 * In PUSH mode, each rank writes data to its destination rank.
 * In PULL mode, each rank reads data from its source rank.
 *
 * There is no notify exchange — WRITE/READ operations do not require
 * the remote side to post matching RECV buffers.
 */
class PermutationPushPullRank : public PermutationRank {
 public:
  explicit PermutationPushPullRank(const WorkloadRankParams& params);

  void run() override;
  void requestAcked(Message* msg) override;
  bool complete() override;
  uint64_t getExpectedBytes() override;

 private:
  void startReads();
  void startWrites();
  void checkCompletionAndContinue();
  void recordEndTime();
  void completeWorkload();

  folly::F14FastMap<uint32_t, std::unique_ptr<Message>> readMsgs_;
  folly::F14FastMap<uint32_t, std::unique_ptr<Message>> writeMsgs_;
  uint32_t remReads_{0};
  uint32_t remWrites_{0};

  workload::thrift::WorkloadMode workloadMode_;
};

} // namespace packsim
