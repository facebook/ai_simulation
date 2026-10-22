/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <utility>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"

namespace packsim::details {

/**
 * Simulations for the tests of ChannelWorkloadShouldNotMixQpSources.
 */
class ChannelWorkloadShouldNotMixQpSourcesTestFixture {
 public:
  /**
   * A simulation with one all-to-all workload named "a2a" that uses channels
   * and sets nothing else that numbers QPs.
   *
   * @return  The simulation config.
   */
  static thrift::Simulation makeChannelSimulation() {
    thrift::Simulation simCfg;
    workload::thrift::Workload workload;
    workload.name() = "a2a";
    workload.config()->allToAll() = workload::thrift::AllToAll{};
    workload.channelSettings() = workload::thrift::ChannelSettings{};
    simCfg.newWorkloadSchedule()->workloads()->emplace_back(
        std::move(workload));
    return simCfg;
  }

  /**
   * The one workload of a simulation from makeChannelSimulation().
   *
   * @param simCfg  The simulation config.
   *
   * @return        Its first workload.
   */
  static workload::thrift::Workload& getOnlyWorkload(
      thrift::Simulation& simCfg) {
    return simCfg.newWorkloadSchedule()->workloads()->at(0);
  }
};

} // namespace packsim::details
