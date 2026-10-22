/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

class ComputeRank : public WorkloadRank {
  friend class WorkloadRankTestHarness;

 public:
  explicit ComputeRank(const WorkloadRankParams& params);
  void run() override;
  [[noreturn]] virtual void requestReceived(Message* msg);
  [[noreturn]] virtual void requestAcked(Message* msg);
  virtual uint64_t getExpectedBytes() override;
  bool complete() override;

 private:
  uint64_t computeTimeNs_;
  bool complete_{false};
};

} // namespace packsim
