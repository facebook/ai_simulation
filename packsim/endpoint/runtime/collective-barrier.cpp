/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/endpoint/runtime/collective-barrier.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <tuple>
#include <utility>

#include <fmt/format.h>

namespace packsim {

CollectiveBarrier::CollectiveBarrier(
    SimContext* const leaderContext,
    SimContext* const context,
    uint32_t const rank,
    const packsim::workload::thrift::Workload* workCfg,
    CollectiveBarrier* const leaderBarrier)
    : leaderContext_(leaderContext),
      context_(context),
      rank_(rank),
      workCfg_(workCfg),
      leaderBarrier_(leaderBarrier) {}

void CollectiveBarrier::arrive(std::function<void()> callback) {
  leaderContext_->scheduleRemote(
      &CollectiveBarrier::postRankArrived,
      (leaderBarrier_ == nullptr) ? this : leaderBarrier_,
      rank_,
      context_,
      std::move(callback));
}

void CollectiveBarrier::postRankArrived(
    uint32_t const rank,
    SimContext* const context,
    std::function<void()> callback) {
  if (std::find_if(
          rankState_.begin(), rankState_.end(), [rank](const auto& elem) {
            return std::get<0>(elem) == rank;
          }) != rankState_.end()) {
    throw std::runtime_error(
        fmt::format(
            "Rank {} of workload ID {} arrived at the barrier more than once.",
            rank,
            *workCfg_->workloadId()));
  }
  rankState_.emplace_back(rank, context, std::move(callback));
  if (rankState_.size() == workCfg_->rankToId()->size()) {
    for (auto& [remoteRank, remoteContext, remoteCallback] : rankState_) {
      if (!workCfg_->rankToId()->contains(remoteRank)) {
        throw std::runtime_error(
            fmt::format(
                "Rank {} completed for workload ID {} was not part of the original config.",
                remoteRank,
                *workCfg_->workloadId()));
      }
      remoteContext->scheduleRemote(std::move(remoteCallback));
    }
    rankState_.clear();
  }
}

} // namespace packsim
