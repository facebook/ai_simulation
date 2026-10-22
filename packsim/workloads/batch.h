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

#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

class BatchRank : public WorkloadRank {
 public:
  explicit BatchRank(const WorkloadRankParams& params);
  void run() override;
  void requestReceived(Message* msg);
  void requestAcked(Message* msg);
  uint64_t getExpectedBytes() override;
  bool complete() override;
  void sendMsg(Message* msg);
  void recvMsg(Message* msg);
  void childComplete(uint32_t workloadId, uint32_t rank);
  void progress() override;

 private:
  std::vector<std::unique_ptr<WorkloadRank>> children_;
  std::vector<std::unique_ptr<packsim::workload::thrift::Workload>> childCfgs_;
  uint32_t activeChildIdx_{0};
  bool complete_{false};
};

} // namespace packsim
