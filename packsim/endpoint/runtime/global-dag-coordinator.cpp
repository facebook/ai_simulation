/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/endpoint/runtime/global-dag-coordinator.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/logging/xlog.h>

namespace packsim {

namespace {
constexpr auto kGlobalDagSimContextName = "global_dag_coordinator_context";

// A DAG without nodes never completes an iteration, so it neither keeps the
// other DAGs repeating nor keeps the run from finishing.
bool anyDagNeedsIteration(
    const std::vector<DagIterationInfos>& dagInfos,
    uint32_t const minIters) noexcept {
  return std::any_of(
      dagInfos.begin(),
      dagInfos.end(),
      [minIters](const DagIterationInfos& dag) {
        return !dag.allNodes.empty() && dag.numIters < minIters;
      });
}

void checkDagId(uint32_t const dagId, size_t const numDags) {
  if (dagId >= numDags) {
    throw std::runtime_error(
        fmt::format(
            "Invalid DAG id {}: DAG ids must be below the number of DAGs, {}.",
            dagId,
            numDags));
  }
}

bool allDagsFinished(const std::vector<DagIterationInfos>& dagInfos) noexcept {
  return std::all_of(
      dagInfos.begin(), dagInfos.end(), [](const DagIterationInfos& dag) {
        return dag.finished || dag.allNodes.empty();
      });
}
} // namespace

GlobalDagCoordinator::GlobalDagCoordinator(
    Scheduler* scheduler,
    size_t const numDags,
    uint32_t const minIters)
    : dagBarrierContext_(
          std::make_unique<SimContext>(scheduler, kGlobalDagSimContextName)),
      dagInfos_(numDags),
      minIters_(minIters) {}

void GlobalDagCoordinator::addDagNode(
    uint32_t const dagId,
    SimContext* const context) {
  checkDagId(dagId, dagInfos_.size());
  dagInfos_[dagId].allNodes.emplace(context);
}

bool GlobalDagCoordinator::hasDagNodes() const noexcept {
  return std::any_of(
      dagInfos_.begin(), dagInfos_.end(), [](const DagIterationInfos& dag) {
        return !dag.allNodes.empty();
      });
}

void GlobalDagCoordinator::arrive(
    uint32_t const dagId,
    SimContext* const context,
    std::function<void(uint32_t /* iter # */)> callback) {
  checkDagId(dagId, dagInfos_.size());
  dagBarrierContext_->scheduleRemote([this,
                                      dagId = dagId,
                                      context = context,
                                      callback =
                                          std::move(callback)]() mutable {
    auto& dagInfo = dagInfos_[dagId];
    if (!dagInfo.allNodes.contains(context)) {
      throw std::runtime_error(
          fmt::format(
              "Invalid context {} provided as completed.",
              static_cast<void*>(context)));
    }
    if (dagInfo.finished) {
      throw std::runtime_error(
          fmt::format(
              "Context {} arrived at DAG {}, which has already finished.",
              static_cast<void*>(context),
              dagId));
    }
    if (!dagInfo.completedNodes.emplace(context, std::move(callback)).second) {
      throw std::runtime_error(
          fmt::format(
              "Context {} arrived twice in one iteration of DAG {}.",
              static_cast<void*>(context),
              dagId));
    }
    // Check if all nodes in the DAG have completed.
    if (dagInfo.completedNodes.size() == dagInfo.allNodes.size()) {
      auto& numIters = dagInfo.numIters;
      ++numIters;
      XLOGF(INFO, "DAG ID {} completed {} iterations.", dagId, numIters);
      if (anyDagNeedsIteration(dagInfos_, minIters_)) {
        for (auto& [nextIterCtx, nextIterCallback] : dagInfo.completedNodes) {
          nextIterCtx->scheduleRemote(std::move(nextIterCallback), numIters);
        }
        dagInfo.completedNodes.clear();
        XLOGF(INFO, "DAG ID {} starting iteration {}...", dagId, numIters + 1);
      } else {
        dagInfo.finished = true;
        dagInfo.completedNodes.clear();
        XLOGF(INFO, "DAG ID {} finished after {} iterations.", dagId, numIters);
        if (allDagsFinished(dagInfos_)) {
          onAllDagsFinished();
        }
      }
    }
  });
}

void GlobalDagCoordinator::onAllDagsFinished() {
  XCHECK(onAllDagsFinished_)
      << "no all-DAGs-finished subscriber registered on the DAG coordinator";
  onAllDagsFinished_();
}

} // namespace packsim
