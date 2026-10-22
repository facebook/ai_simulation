/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/all-reduce-rhd.h"

#include <cstdint>
#include <functional>
#include <string>

#include <fmt/format.h>

namespace packsim {

ArRecursiveRank::ArRecursiveRank(const WorkloadRankParams& params)
    : WorkloadRank(params) {
  // Rejected here so the error names this workload, not its `_rs` phase.
  requireAtLeastTwoRanks("Recursive");
  rsWkldCfg_ = *workCfg_;
  agWkldCfg_ = *workCfg_;

  this->setOnRequestAckedCallback([this](Message* msg) { requestAcked(msg); });
  this->setOnRequestReceivedCallback(
      [this](Message* msg) { requestReceived(msg); });

  rsWkldCfg_.config() = packsim::workload::thrift::WorkloadSpecificConfig();
  agWkldCfg_.config() = packsim::workload::thrift::WorkloadSpecificConfig();
  rsWkldCfg_.config()->reduceScatter() =
      packsim::workload::thrift::ReduceScatter();
  agWkldCfg_.config()->allGather() = packsim::workload::thrift::AllGather();

  const auto& arCfg = *workCfg_->config()->allReduce();
  auto& rsCfg = *rsWkldCfg_.config()->reduceScatter();
  auto& agCfg = *agWkldCfg_.config()->allGather();

  rsCfg.sizeBytes() = *arCfg.sizeBytes();
  agCfg.sizeBytes() = *arCfg.sizeBytes();
  rsCfg.algorithm() = *arCfg.algorithm();
  agCfg.algorithm() = *arCfg.algorithm();

  rsWkldCfg_.name() = fmt::format("{}_rs", *workCfg_->name());
  agWkldCfg_.name() = fmt::format("{}_ag", *workCfg_->name());

  const auto rankCompleteCb = [this](uint32_t workloadId, uint32_t rank) {
    childComplete(workloadId, rank);
  };

  rsChild_ = params.factory(
      simCtx_,
      workloadId_,
      rsWkldCfg_,
      rank_,
      sendMessageFn_,
      recvMessageFn_,
      rankCompleteCb,
      this);
  agChild_ = params.factory(
      simCtx_,
      workloadId_,
      agWkldCfg_,
      params.rank,
      params.smcb,
      params.rmcb,
      rankCompleteCb,
      this);
  activeChild_ = rsChild_.get();
  // The children are this rank's two phases: this rank's start delay holds
  // back the first one's receives and notify (see run()), and neither phase
  // adds a delay of its own. As in the other workloads, the first step's sends
  // go out as soon as that step's peer notifies, even during the delay.
  rsChild_->startTimeDelayNs_ = 0;
  agChild_->startTimeDelayNs_ = 0;
}

void ArRecursiveRank::sendMsg(Message* msg) {
  sendMessageFn_(msg);
}

void ArRecursiveRank::recvMsg(Message* msg) {
  recvMessageFn_(msg);
}

void ArRecursiveRank::childComplete(
    const uint32_t workloadId,
    const uint32_t rank) {
  if (activeChild_ == rsChild_.get()) {
    activeChild_ = agChild_.get();
    agChild_->run();
  } else {
    endTimeNs_ = getTimeNs();
    onRankComplete(workloadId, rank);
  }
}

void ArRecursiveRank::run() {
  runAfterStartDelay([this] {
    rsChild_->run();
    startTimeNs_ = getTimeNs();
  });
}

void ArRecursiveRank::requestReceived(Message* msg) {
  if (isNotifyMsg(msg)) {
    notifyReceived(msg->srcRank);
    activeChild_->progress();
    return;
  }
  activeChild_->onRequestReceived(msg);
}

void ArRecursiveRank::requestAcked(Message* msg) {
  if (isNotifyMsg(msg)) {
    notifyAcked(msg->dstRank);
    activeChild_->progress();
    return;
  }

  activeChild_->onRequestAcked(msg);
}

uint64_t ArRecursiveRank::getExpectedBytes() {
  return rsChild_->getExpectedBytes() + agChild_->getExpectedBytes();
}

bool ArRecursiveRank::complete() {
  return rsChild_->complete() && agChild_->complete();
}

void ArRecursiveRank::progress() {
  activeChild_->progress();
}

} // namespace packsim
