/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include <folly/container/F14Map.h>

#include "packsim/endpoint/runtime/global-dag-coordinator.h"
#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

/**
 * Information about a single dependency DAG within the workload graph.
 */
struct DependencyDagInfos {
  // All workload IDs in this DAG.
  folly::F14FastSet<uint32_t /* workload ID */> allWorkloadIds;
  // Workload IDs in this DAG with no dependencies.
  folly::F14FastSet<uint32_t /* workload ID */> startingWorkloadIds;
  // Workload IDs in this DAG with no dependents.
  folly::F14FastSet<uint32_t /* workload ID */> finishingWorkloadIds;
};

/**
 * Complete dependency information for all workloads in a simulation.
 */
struct DependencyInfos {
  // For each workload, the set of workloads that depend on it (its successors).
  folly::F14FastMap<
      uint32_t /* workload ID */,
      folly::F14FastSet<uint32_t /* succeeding workload ID */>>
      workloadIdDependents{};
  // For each workload, the set of workloads it depends on (its predecessors).
  folly::F14FastMap<
      uint32_t /* workload ID */,
      folly::F14FastSet<uint32_t /* preceding workload ID */>>
      workloadIdDependencies{};
  // One entry per independent dependency DAG in the workload graph.
  std::vector<DependencyDagInfos> dagList{};
  // Maps each workload ID to the index of its DAG in dagList.
  folly::F14FastMap<
      uint32_t /* workload ID */,
      uint32_t /* DAG ID (index in dagList) */>
      workloadToDagId{};
  // Minimum iteration count required across all DAGs, if constrained.
  std::optional<uint32_t> minIterationsPerDag{};
};

/**
 * Tracks workload dependencies and coordinates execution of dependent
 * workloads.
 */
class DependencyTracker {
 public:
  // The node asks the tracker to watch its DAG; these are how the tracker asks
  // back. Both are commands the tracker issues, not notifications it sends, so
  // they are `Fn` rather than callbacks.

  // Starts a workload whose dependencies are all satisfied.
  using TriggerWorkloadFn = std::function<void(uint32_t /* workload ID */)>;

  // Rebuilds a workload rank for the next iteration of its DAG.
  using BuildWorkloadFn = std::function<void(
      const packsim::workload::thrift::Workload& /* workload config */,
      const uint32_t /* rank */,
      const uint32_t /* iteration */)>;

  using WorkloadDependencyMap = folly::F14FastMap<
      uint32_t /* workload ID */,
      folly::F14FastSet<uint32_t /* preceding workload ID */>>;
  using DagWorkloadMap = folly::F14FastMap<
      uint32_t /* DAG ID */,
      folly::F14FastSet<uint32_t /* workload ID */>>;

  /**
   * Constructs a DependencyTracker for managing workload dependencies.
   *
   * @param workloads                Map of workload ID to WorkloadRank
   * instances
   * @param infos                    Dependency information for all workloads
   * @param triggerWorkloadFn        Starts a workload whose dependencies
   * are satisfied
   * @param buildWorkloadFn          Rebuilds a workload rank for the next
   * iteration
   * @param globalDagCoordinator     Global DAG coordinator for cross-node
   * synchronization
   * @param simCtx                   Simulation context
   */
  DependencyTracker(
      folly::F14FastMap<uint32_t, std::unique_ptr<packsim::WorkloadRank>> const&
          workloads,
      DependencyInfos const& infos,
      TriggerWorkloadFn const& triggerWorkloadFn,
      BuildWorkloadFn const& buildWorkloadFn,
      GlobalDagCoordinator* FOLLY_NONNULL globalDagCoordinator,
      SimContext* FOLLY_NONNULL simCtx);

  /**
   * Notifies the tracker that a workload has completed.
   *
   * @param workloadId The ID of the completed workload
   */
  void postWorkloadCompletion(uint32_t const workloadId);

  /**
   * Gets the dependency information.
   *
   * @return Pointer to the DependencyInfos structure
   */
  const DependencyInfos* getDependencyInfos() const;

  /**
   * Filters a dependency map to include only dependencies for node-local
   * workloads.
   *
   * @param originalDepMap Original dependency map
   * @param nodeWorkloads  Workloads local to this node
   * @return               Filtered dependency map
   */
  static WorkloadDependencyMap filterDependencyMapByNodeWorkloads(
      WorkloadDependencyMap const& originalDepMap,
      folly::F14FastMap<
          uint32_t /* workload ID */,
          std::unique_ptr<packsim::WorkloadRank>> const& nodeWorkloads);

  /**
   * Filters a DAG-to-workload map to include only node-local workloads.
   *
   * @param infos         Dependency information
   * @param nodeWorkloads Workloads local to this node
   * @return              Filtered DAG-to-workload map
   */
  static DagWorkloadMap filterDagWorkloadMapByNodeWorkloads(
      DependencyInfos const& infos,
      folly::F14FastMap<
          uint32_t /* workload ID */,
          std::unique_ptr<packsim::WorkloadRank>> const& nodeWorkloads);

  /**
   * Checks if a DAG iteration is complete and triggers the next iteration if
   * needed.
   *
   * @param simCtx                    Simulation context
   * @param globalDagCoordinator      Global DAG coordinator
   * @param workloadIdDependencies    Map of workload dependencies
   * @param dagToWorkloadIds          Map of DAG IDs to workload IDs
   * @param workloadId                The workload ID that just completed
   * @param infos                     Dependency information
   * @param workloads                 Map of workload ID to WorkloadRank
   * instances
   * @param triggerWorkloadFn         Starts a workload whose dependencies
   * are satisfied
   * @param buildWorkloadFn           Rebuilds a workload rank for the next
   * iteration
   */
  static void checkAndTriggerNextIteration(
      SimContext* FOLLY_NONNULL simCtx,
      GlobalDagCoordinator* FOLLY_NONNULL globalDagCoordinator,
      WorkloadDependencyMap& workloadIdDependencies,
      DagWorkloadMap& dagToWorkloadIds,
      uint32_t const workloadId,
      DependencyInfos const& infos,
      folly::F14FastMap<
          uint32_t /* workload ID */,
          std::unique_ptr<packsim::WorkloadRank>> const& workloads,
      TriggerWorkloadFn const& triggerWorkloadFn,
      BuildWorkloadFn const& buildWorkloadFn);

 private:
  // Reference to all workloads in the simulation
  folly::F14FastMap<
      uint32_t /* workload ID */,
      std::unique_ptr<packsim::WorkloadRank>> const& workloads_;
  // Reference to dependency information
  DependencyInfos const& infos_;
  // Starts a workload whose dependencies are satisfied.
  TriggerWorkloadFn const triggerWorkloadFn_;
  // Rebuilds a workload rank for the next iteration.
  BuildWorkloadFn const buildWorkloadFn_;

  // Filtered dependency map for node-local workloads
  WorkloadDependencyMap workloadIdDependencies_;
  // Filtered DAG-to-workload map for node-local workloads
  DagWorkloadMap dagToWorkloadIds_;
  // Global DAG coordinator for cross-node synchronization
  GlobalDagCoordinator* globalDagCoordinator_;
  // Simulation context
  SimContext* simCtx_;
};

} // namespace packsim
