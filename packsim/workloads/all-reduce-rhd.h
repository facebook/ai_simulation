/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <memory>

#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

class ArRecursiveRank : public WorkloadRank {
 public:
  explicit ArRecursiveRank(const WorkloadRankParams& params);
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
  packsim::workload::thrift::Workload rsWkldCfg_;
  packsim::workload::thrift::Workload agWkldCfg_;
  std::unique_ptr<WorkloadRank> rsChild_;
  std::unique_ptr<WorkloadRank> agChild_;
  WorkloadRank* activeChild_;
};

} // namespace packsim
