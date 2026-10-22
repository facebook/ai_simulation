/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include "packsim/kernel/sim-context.h"

namespace packsim {

struct DagIterationInfos {
  // Number of iterations completed by all nodes in this DAG so far.
  uint32_t numIters = 0;
  // Set when this DAG completes an iteration that no DAG needs repeated; it is
  // never restarted after that.
  bool finished = false;
  // Nodes that finished the current iteration, each with its completion
  // callback.
  folly::F14FastMap<SimContext*, std::function<void(uint32_t /* iter # */)>>
      completedNodes;
  // All nodes participating in this DAG.
  folly::F14FastSet<SimContext*> allNodes;
};

class GlobalDagCoordinator {
 public:
  /**
   * Create a global DAG coordinator.
   *
   * @param  scheduler  Pointer to global PDES scheduler.
   * @param  numDags    Number of DAGs in total.
   * @param  minIters   Minimum iterations for all DAGs before the simulation
   *                    can terminate. Default of zero means no lower bound.
   */
  GlobalDagCoordinator(
      Scheduler* scheduler,
      size_t const numDags,
      uint32_t const minIters = 0);

  virtual ~GlobalDagCoordinator() = default;

  /**
   * Add a context (i.e., node) to the selected DAG id.
   *
   * @param  dagId    DAG id for the DAG to which node should be added.
   * @param  context  Context that represents the node.
   */
  virtual void addDagNode(uint32_t const dagId, SimContext* const context);

  // Whether any DAG has a node, and so can raise the all-DAGs-finished event.
  bool hasDagNodes() const noexcept;

  /**
   * Tell global DAG coordinator that this node has completed the current
   * iteration for the given DAG id.
   *
   * @param  dagId     DAG id of the DAG whose iteration the node completed.
   * @param  context   Context that represents the node.
   * @param  callback  Starts this node's next iteration of the DAG. Called
   *                   with the number of iterations completed once every node
   *                   of the DAG has arrived, unless no DAG needs another
   *                   iteration.
   *
   * @throw std::runtime_error If `dagId` is not a DAG of this coordinator.
   *        When the arrival is processed, on the coordinator's context, if
   *        `context` is not a node of the DAG, already arrived in this
   *        iteration, or the DAG has finished.
   */
  virtual void arrive(
      uint32_t const dagId,
      SimContext* const context,
      std::function<void(uint32_t /* iter # */)> callback);

  // Raised on the coordinator's own context once every DAG that has nodes has
  // finished: each completed an iteration when all of them had completed at
  // least `minIters`, so none is restarted again.
  using OnAllDagsFinishedCallback = std::function<void()>;

  // Set the callback raised when every DAG has finished its last iteration.
  void setOnAllDagsFinishedCallback(const OnAllDagsFinishedCallback& cb) {
    onAllDagsFinished_ = cb;
  }

 private:
  // Raise the all-DAGs-finished event.
  void onAllDagsFinished();

  std::unique_ptr<SimContext> dagBarrierContext_;
  std::vector<DagIterationInfos> dagInfos_;
  uint32_t const minIters_;
  // Subscriber to the all-DAGs-finished event.
  OnAllDagsFinishedCallback onAllDagsFinished_;
};

} // namespace packsim
