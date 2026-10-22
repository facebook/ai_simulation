/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include "packsim/endpoint/runtime/dependency-tracker.h"
#include "packsim/if/gen-cpp2/config_types.h"

namespace packsim {

/**
 * Builds DependencyInfos from the workload DAGs declared in a simulation
 * config. Extracted from PackSimSimulation so the DAG parsing and validation is
 * a single-responsibility unit, testable without constructing a full
 * simulation.
 */
class DependencyInfoBuilder {
 public:
  /**
   * Build dependency information from the workload DAGs in a simulation config.
   *
   * @param simCfg  Simulation config whose
   *                newWorkloadSchedule().workloadDags() are parsed.
   *
   * @return        DependencyInfos describing the DAGs, or nullptr when no
   *                workload DAGs are configured (DAG-based scheduling
   * disabled).
   *
   * @throw         std::invalid_argument if a DAG references a workload name
   * absent from the input workloads, a workload ID appears in more than one
   * DAG, or a DAG has a dependency cycle.
   */
  static std::unique_ptr<DependencyInfos> build(
      const packsim::thrift::Simulation& simCfg);
};

} // namespace packsim
