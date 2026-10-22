/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/compute.h"

#include <algorithm>
#include <functional>
#include <stdexcept>

namespace packsim {

ComputeRank::ComputeRank(const WorkloadRankParams& params)
    : WorkloadRank(params) {
  this->setOnRequestAckedCallback([this](Message* msg) { requestAcked(msg); });
  this->setOnRequestReceivedCallback(
      [this](Message* msg) { requestReceived(msg); });

  if (workCfg_->config()->getType() !=
      packsim::workload::thrift::WorkloadSpecificConfig::Type::compute) {
    throw std::runtime_error(
        "Invalid config passed down to ComputeRank workload.");
  }
  if (*workCfg_->config()->compute()->sizeBytes() != 0) {
    throw std::runtime_error(
        "Workload size for compute workload must be zero.");
  }
  const uint64_t computeTimeNs =
      *workCfg_->config()->compute()->computeTimeNs();
  uint64_t maxComputeTimeNs = computeTimeNs;
  uint64_t minComputeTimeNs = computeTimeNs;
  if (workCfg_->config()->compute()->computeTimeJitterNs()) {
    const uint64_t computeTimeJitterNs =
        *workCfg_->config()->compute()->computeTimeJitterNs();
    maxComputeTimeNs += computeTimeJitterNs;
    minComputeTimeNs =
        computeTimeNs - std::min(computeTimeNs, computeTimeJitterNs);
  }
  computeTimeNs_ = simCtx_->random.sampleUniform<uint64_t>(
      minComputeTimeNs, maxComputeTimeNs);
}

void ComputeRank::run() {
  traceEvent_ = simCtx_->tracer.startEvent(
      "ComputeRank", "Workloads", workloadId_, rank_);
  simCtx_->schedule(Time::fromNs(startTimeDelayNs_), [this]() {
    startTimeNs_ = getTimeNs();
    simCtx_->schedule(Time::fromNs(computeTimeNs_), [this]() {
      complete_ = true;
      endTimeNs_ = getTimeNs();
      traceEvent_->end(simCtx_->now());
      onRankComplete(workloadId_, rank_);
    });
  });
}

void ComputeRank::requestReceived(Message* /* msg */) {
  throw std::runtime_error(
      "For ComputeRank, this method should never be called.");
}

void ComputeRank::requestAcked(Message* /* msg */) {
  throw std::runtime_error(
      "For ComputeRank, this method should never be called.");
}

bool ComputeRank::complete() {
  return complete_;
}

uint64_t ComputeRank::getExpectedBytes() {
  return 0;
}

} // namespace packsim
