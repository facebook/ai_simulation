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
#include "packsim/workloads/all-to-all.h"

namespace packsim {

/**
 * AllToAllPushPullRank implements WRITE and READ operations for AllToAll
 * workloads. It derives from AllToAllRank and replaces the SEND/RECV path
 * with PUSH (WRITE) or PULL (READ) semantics.
 *
 * In PUSH mode, the requester writes data to the responder.
 * In PULL mode, the requester reads data from the responder.
 *
 * There is no notify exchange — WRITE/READ operations do not require
 * the remote side to post matching RECV buffers.
 */
class AllToAllPushPullRank : public AllToAllRank {
 public:
  explicit AllToAllPushPullRank(const WorkloadRankParams& params);

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

  std::vector<folly::F14FastMap<uint32_t, std::unique_ptr<Message>>> readMsgs_;
  std::vector<folly::F14FastMap<uint32_t, std::unique_ptr<Message>>> writeMsgs_;
  uint32_t remReads_{0};
  uint32_t remWrites_{0};

  workload::thrift::WorkloadMode workloadMode_;
};

} // namespace packsim
