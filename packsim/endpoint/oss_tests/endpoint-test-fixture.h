/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "packsim/endpoint/endpoint.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/node/flex-node.h"
#include "packsim/transport/oss_tests/roce-v2-test-config.h"

namespace packsim {

/*
 * Endpoint fixtures shared by the endpoint test suites. The
 * transport-construction cases each select their own transport arm on top of
 * the RoCEv2 base config.
 */

// A minimal RoCEv2 NIC config and simulation context shared by the endpoint
// fixtures below.
class EndpointTestBase : public ::testing::Test {
 protected:
  // Port number is stored at this index in FlexDeviceId.values array
  static constexpr uint8_t kPortIndexInFlexDeviceId = 5;
  static constexpr int kRackIndexInFlexDeviceId = 2;
  static constexpr int kNumPorts = 4;
  static constexpr int kPortSpeedGbps = 100;
  static constexpr int kPrefixLen = 5;

  void SetUp() override {
    scheduler_.buildNewEngine();

    nicConfig_ = std::make_unique<packsim::thrift::NicConfig>();
    nicConfig_->speed() = fmt::format("{}Gbps", kPortSpeedGbps);
    nicConfig_->readGbps() = kPortSpeedGbps * kNumPorts;
    // Sets the transport arm and the single-CoS buffer sizes RoCEv2 needs.
    RoceV2TestConfig::configureRoceV2Nic(*nicConfig_);
    nicConfig_->queueScheduleCfg() =
        packsim::thrift::SchedulingAlgorithmType::RR;
    nicConfig_->queueFilterCfg() = packsim::thrift::QueueFilterConfig{};
    nicConfig_->queueCfg() = std::vector{packsim::thrift::PortQueueConfig{}};
    nicConfig_->txLatencyNs() = 0;
    nicConfig_->rxLatencyNs() = 0;
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  std::unique_ptr<packsim::thrift::NicConfig> nicConfig_;
};

class EndpointTransportConstructionTest : public EndpointTestBase {
 protected:
  std::unique_ptr<Endpoint> makeEndpoint() {
    FlexNodeParams params{
        .simCtx = &simCtx_,
        .name = "xpu0",
        .flexId = FlexDeviceId("1:0:0:0:0:0:0:0"),
        .lpmTable = nullptr,
        .topo = nullptr,
    };
    return std::make_unique<Endpoint>(params, nicConfig_.get());
  }
};

class EndpointAddressingTest : public EndpointTestBase {};

} // namespace packsim
