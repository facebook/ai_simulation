/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <gtest/gtest.h>

#include "packsim/endpoint/runtime/workload-node.h"
#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/node/flex-node.h"
#include "packsim/transport/oss_tests/roce-v2-test-config.h"

namespace packsim {

/*
 * Message-tracking workload-node fixture shared by the workload-node test
 * suites.
 */

class WorkloadNodeMessageTrackerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    scheduler_.buildNewEngine();

    nicConfig_ = packsim::thrift::NicConfig();
    RoceV2TestConfig::configureRoceV2Nic(nicConfig_);

    nicMap_["default"] = nicConfig_;
  }

  packsim::thrift::Node createNodeConfig() {
    packsim::thrift::Node nodeCfg;
    nodeCfg.id() = {1, 0, 1, 1, 0, 0, 0, 0};
    nodeCfg.type() = packsim::thrift::NodeType::ACCEL;

    packsim::thrift::EndpointConfig epCfg;
    epCfg.nicIds()->push_back({1, 0, 1, 1, 1, 0, 0, 0});
    epCfg.portIdx() = 0;
    epCfg.lpmSpec() = packsim::thrift::LpmTableSpec{};
    nodeCfg.endpointCfgs()->emplace_back(epCfg);

    return nodeCfg;
  }

  std::unique_ptr<WorkloadNode> createWorkloadNode(
      const packsim::thrift::NetworkLevelFeatureConfig* featureConfig =
          nullptr) {
    auto nodeCfg = createNodeConfig();
    FlexNodeParams params{
        .simCtx = &simCtx_,
        .name = "workload_node_tracker_test",
        .flexId = FlexDeviceId("1:0:1:1:0:0:0:0"),
        .lpmTable = nullptr,
        .topo = nullptr,
    };
    return std::make_unique<WorkloadNode>(
        params, nodeCfg, nicMap_, featureConfig);
  }

 protected:
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  packsim::thrift::NicConfig nicConfig_;
  std::map<std::string, packsim::thrift::NicConfig> nicMap_;
};

} // namespace packsim
