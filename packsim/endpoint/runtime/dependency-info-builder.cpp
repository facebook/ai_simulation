/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/endpoint/runtime/dependency-info-builder.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/String.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"

namespace packsim {

namespace {

[[noreturn]] void throwMissingWorkload(const std::string& workloadName) {
  throw std::invalid_argument(
      fmt::format(
          "Workload name {} used in DAG is missing from input workloads list.",
          workloadName));
}

[[noreturn]] void throwDuplicateWorkloadId(uint32_t workloadId) {
  throw std::invalid_argument(
      fmt::format(
          "Workload ID {} used in DAG is already used by another DAG.",
          workloadId));
}

/**
 * Walks the workload DAGs in a WorkloadNewSchedule and assembles
 * DependencyInfos. Stateful so the nested DAG walk shares the running maps
 * without threading them through every helper.
 *
 * NOTE: observedWorkloadIds_ is populated only in finalizeDag(), i.e. after a
 * whole DAG's edges are processed. The duplicate-ID check therefore fires only
 * across DAGs, enforcing "each workload belongs to exactly one DAG" while
 * allowing a workload to appear on both sides of an edge within its own DAG.
 */
class DagWalker {
 public:
  explicit DagWalker(const packsim::thrift::WorkloadNewSchedule& schedule)
      : schedule_(schedule) {}

  std::unique_ptr<DependencyInfos> run() {
    // Capture the optional once so the presence check and the dereferences
    // below act on the same handle. build() delegates the workloadDags guard
    // here, keeping the check and its uses co-located (also required by the
    // unchecked-optional-access analyzer).
    auto workloadDags = schedule_.workloadDags();
    if (!workloadDags.has_value()) {
      return nullptr;
    }
    depInfos_.minIterationsPerDag =
        schedule_.minIterationsPerDag().to_optional();
    buildOriginalToInternalNames();
    depInfos_.dagList.reserve(workloadDags->size());
    uint32_t dagId = 0;
    for (const auto& workloadDag : *workloadDags) {
      processDag(workloadDag, dagId++);
    }
    return std::make_unique<DependencyInfos>(std::move(depInfos_));
  }

 private:
  // Map each original (user-facing) workload name to the internal workload
  // names it expands to (e.g. per-rail sub-workloads).
  void buildOriginalToInternalNames() {
    for (const auto& [internalName, originalInfo] :
         *schedule_.workloadNameInfos()) {
      originalToInternalNames_[*originalInfo.originalWorkloadName()].emplace(
          internalName);
    }
  }

  // Internal workload names an original workload name expands to; throws if the
  // name is not a known input workload.
  const folly::F14FastSet<std::string>& internalNamesFor(
      const std::string& originalName) const {
    const auto it = originalToInternalNames_.find(originalName);
    if (it == originalToInternalNames_.end()) {
      throwMissingWorkload(originalName);
    }
    return it->second;
  }

  uint32_t internalWorkloadId(const std::string& internalName) const {
    const auto& nameInfos = *schedule_.workloadNameInfos();
    return *nameInfos.at(internalName).internalWorkloadId();
  }

  // Record a dependency edge (dependency -> dependent) for every internal
  // workload the dependency name expands to, adding each to the DAG.
  void addDependencyEdges(
      uint32_t dependentWorkloadId,
      const std::string& dependencyName,
      DependencyDagInfos& dagInfos) {
    for (const auto& internalDependencyName :
         internalNamesFor(dependencyName)) {
      const auto dependencyWorkloadId =
          internalWorkloadId(internalDependencyName);
      if (observedWorkloadIds_.contains(dependencyWorkloadId)) {
        throwDuplicateWorkloadId(dependencyWorkloadId);
      }
      depInfos_.workloadIdDependents[dependencyWorkloadId].emplace(
          dependentWorkloadId);
      depInfos_.workloadIdDependencies[dependentWorkloadId].emplace(
          dependencyWorkloadId);
      dagInfos.allWorkloadIds.emplace(dependencyWorkloadId);
    }
  }

  // Record every dependency declared for one original workload name.
  template <typename DependencyNames>
  void processWorkload(
      const std::string& workloadName,
      const DependencyNames& dependencyNames,
      DependencyDagInfos& dagInfos) {
    for (const auto& internalName : internalNamesFor(workloadName)) {
      const auto workloadId = internalWorkloadId(internalName);
      if (observedWorkloadIds_.contains(workloadId)) {
        throwDuplicateWorkloadId(workloadId);
      }
      dagInfos.allWorkloadIds.emplace(workloadId);
      for (const auto& dependencyName : dependencyNames) {
        addDependencyEdges(workloadId, dependencyName, dagInfos);
      }
    }
  }

  // Mark every workload in a fully-processed DAG as observed and classify it as
  // a starting (no dependencies) and/or finishing (no dependents) node.
  void finalizeDag(DependencyDagInfos& dagInfos, uint32_t dagId) {
    for (const auto workloadId : dagInfos.allWorkloadIds) {
      observedWorkloadIds_.emplace(workloadId);
      depInfos_.workloadToDagId.emplace(workloadId, dagId);
      if (!depInfos_.workloadIdDependents.contains(workloadId)) {
        dagInfos.finishingWorkloadIds.emplace(workloadId);
      }
      if (!depInfos_.workloadIdDependencies.contains(workloadId)) {
        dagInfos.startingWorkloadIds.emplace(workloadId);
      }
    }
  }

  /**
   * Checks that every workload of a DAG can eventually start, with Kahn's
   * algorithm: a workload can start once all its dependencies have finished.
   * A workload that never reaches that point is on a dependency cycle or waits
   * behind one, so the DAG could never finish.
   *
   * @param dagInfos  The DAG, with its edges already recorded in depInfos_.
   * @param dagId     The DAG's index in dagList, for the error message.
   *
   * @throw           std::invalid_argument naming the workloads that can never
   *                  start, by the names the DAG was written with.
   */
  void checkAcyclic(const DependencyDagInfos& dagInfos, uint32_t dagId) const {
    folly::F14FastMap<uint32_t, size_t> numUnfinishedDependencies;
    numUnfinishedDependencies.reserve(dagInfos.allWorkloadIds.size());
    std::vector<uint32_t> startableWorkloadIds;
    for (const auto workloadId : dagInfos.allWorkloadIds) {
      const auto it = depInfos_.workloadIdDependencies.find(workloadId);
      const size_t numDependencies =
          it == depInfos_.workloadIdDependencies.end() ? 0 : it->second.size();
      numUnfinishedDependencies.emplace(workloadId, numDependencies);
      if (numDependencies == 0) {
        startableWorkloadIds.emplace_back(workloadId);
      }
    }

    size_t numFinished = 0;
    while (!startableWorkloadIds.empty()) {
      const uint32_t workloadId = startableWorkloadIds.back();
      startableWorkloadIds.pop_back();
      ++numFinished;
      const auto dependents = depInfos_.workloadIdDependents.find(workloadId);
      if (dependents == depInfos_.workloadIdDependents.end()) {
        continue;
      }
      for (const auto dependentId : dependents->second) {
        const auto pending = numUnfinishedDependencies.find(dependentId);
        if (pending != numUnfinishedDependencies.end() &&
            --pending->second == 0) {
          startableWorkloadIds.emplace_back(dependentId);
        }
      }
    }
    if (numFinished == dagInfos.allWorkloadIds.size()) {
      return;
    }

    // Report the names the DAG was written with, sorted so the message is
    // stable.
    std::set<std::string> blockedNames;
    for (const auto& [internalName, nameInfo] :
         *schedule_.workloadNameInfos()) {
      const auto pending =
          numUnfinishedDependencies.find(*nameInfo.internalWorkloadId());
      if (pending != numUnfinishedDependencies.end() && pending->second > 0) {
        blockedNames.emplace(*nameInfo.originalWorkloadName());
      }
    }
    throw std::invalid_argument(
        fmt::format(
            "Workload DAG {} has a dependency cycle, so these workloads can "
            "never start: {}",
            dagId,
            folly::join(", ", blockedNames)));
  }

  // `dagId` must equal the index this DAG gets in `dagList`.
  void processDag(
      const packsim::workload::thrift::WorkloadDag& workloadDag,
      uint32_t dagId) {
    auto& dagInfos = depInfos_.dagList.emplace_back();
    for (const auto& [workloadName, dependencyNames] :
         *workloadDag.workloadDependencies()) {
      processWorkload(workloadName, dependencyNames, dagInfos);
    }
    checkAcyclic(dagInfos, dagId);
    finalizeDag(dagInfos, dagId);
  }

  const packsim::thrift::WorkloadNewSchedule& schedule_;
  folly::F14FastMap<std::string, folly::F14FastSet<std::string>>
      originalToInternalNames_;
  folly::F14FastSet<uint32_t> observedWorkloadIds_;
  DependencyInfos depInfos_;
};

} // namespace

std::unique_ptr<DependencyInfos> DependencyInfoBuilder::build(
    const packsim::thrift::Simulation& simCfg) {
  // DagWalker::run() returns nullptr when no workload DAGs are configured.
  return DagWalker(*simCfg.newWorkloadSchedule()).run();
}

} // namespace packsim
