/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/batch.h"

#include <functional>
#include <stdexcept>
#include <utility>

#include <fmt/format.h>

namespace packsim {
BatchRank::BatchRank(const WorkloadRankParams& params) : WorkloadRank(params) {
  this->setOnRequestAckedCallback([this](Message* msg) { requestAcked(msg); });
  this->setOnRequestReceivedCallback(
      [this](Message* msg) { requestReceived(msg); });
  const auto completeCb = [this](uint32_t workloadId, uint32_t rank) {
    childComplete(workloadId, rank);
  };
  children_.reserve(workCfg_->config()->batch()->children()->size());
  for (const auto& cw : *workCfg_->config()->batch()->children()) {
    auto child = params.factory(
        simCtx_,
        workloadId_,
        cw,
        rank_,
        params.smcb,
        params.rmcb,
        completeCb,
        this);
    children_.emplace_back(std::move(child));
  }
  // `run()` and `complete()` start from the first child.
  if (children_.empty()) {
    throw std::invalid_argument(
        fmt::format(
            "Workload {}: Batch children must not be empty", workloadName_));
  }
}

void BatchRank::run() {
  children_.at(0)->run();
}

void BatchRank::requestReceived(Message* msg) {
  if (isNotifyMsg(msg)) {
    notifyReceived(msg->srcRank);
    progress();
    return;
  }
  children_.at(activeChildIdx_)->requestReceived(msg);
}

void BatchRank::requestAcked(Message* msg) {
  if (isNotifyMsg(msg)) {
    notifyAcked(msg->dstRank);
    progress();
    return;
  }
  children_.at(activeChildIdx_)->requestAcked(msg);
}

uint64_t BatchRank::getExpectedBytes() {
  uint64_t totalBytes{0};
  for (const auto& child : children_) {
    totalBytes += child->getExpectedBytes();
  }
  return totalBytes;
}

bool BatchRank::complete() {
  if (activeChildIdx_ != children_.size()) {
    return false;
  }
  // First time workloads are complete
  if (!complete_) {
    startTimeNs_ = children_.front()->startTimeNs_;
    endTimeNs_ = children_.back()->endTimeNs_;
  }
  complete_ = true;
  return complete_;
}

void BatchRank::sendMsg(Message* msg) {
  sendMessageFn_(msg);
}

void BatchRank::recvMsg(Message* msg) {
  recvMessageFn_(msg);
}

void BatchRank::childComplete(const uint32_t workloadId, const uint32_t rank) {
  activeChildIdx_++;
  if (complete()) {
    onRankComplete(workloadId, rank);
    return;
  }
  children_.at(activeChildIdx_)->run();
}

void BatchRank::progress() {
  if (complete()) {
    return;
  }
  children_.at(activeChildIdx_)->progress();
}

} // namespace packsim
