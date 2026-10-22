/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <memory>
#include <unordered_set>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "packsim/endpoint/endpoint.h"
#include "packsim/if/gen-cpp2/qp_pair_config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/node/flex-node.h"
#include "packsim/transport/oss_tests/roce-v2-test-config.h"

namespace packsim {

/**
 * Test fixture for Endpoint QPN port config functionality.
 *
 * Creates a simple topology with an endpoint that has 4 ports
 * connecting to 2 switches for testing port restrictions.
 */
class EndpointQpnPortConfigTest : public ::testing::Test {
 protected:
  static constexpr uint8_t kPortIndexInFlexDeviceId = 5;
  static constexpr int kNumPorts = 4;
  static constexpr int kPortSpeedGbps = 100;

  void SetUp() override {
    scheduler_.buildNewEngine();

    nicConfig_ = std::make_unique<packsim::thrift::NicConfig>();
    nicConfig_->speed() = fmt::format("{}Gbps", kPortSpeedGbps);
    nicConfig_->readGbps() = kPortSpeedGbps * kNumPorts;
    RoceV2TestConfig::configureRoceV2Nic(*nicConfig_);
    nicConfig_->queueScheduleCfg() =
        packsim::thrift::SchedulingAlgorithmType::RR;
    // queueFilterCfg and queueCfg are deliberately left at their thrift
    // defaults: this suite exercises QPN-to-port mapping, not queueing.
    // What makes the RoCEv2 NicRxBuffer valid here is perCosXoff, set by
    // configureRoceV2Nic above -- the buffer takes its queue count from that
    // vector and builds its own PortQueueConfig list internally, never reading
    // queueCfg. queueFilterCfg only selects the filter type and thresholds.
    nicConfig_->allowReachableRoutesOnly() = false;

    FlexNodeParams params{
        .simCtx = &simCtx_,
        .name = "xpu0",
        .flexId = FlexDeviceId("1:0:1:1:0:0:0:0"),
        .lpmTable = nullptr,
        .topo = nullptr,
    };
    endpoint_ = std::make_unique<Endpoint>(
        params, nicConfig_.get(), kPortIndexInFlexDeviceId);

    switch1Id_ = FlexDeviceId("2:0:1:0:0:0:0:0");
    switch2Id_ = FlexDeviceId("2:0:2:0:0:0:0:0");

    // Partition the ports across two switches: the first half to switch1, the
    // second half to switch2 (for kNumPorts=4: ports 1,2 -> switch1, ports 3,4
    // -> switch2).
    for (uint16_t portNum = 1; portNum <= kNumPorts; ++portNum) {
      createPortWithSwitch(
          portNum, portNum <= kNumPorts / 2 ? switch1Id_ : switch2Id_);
    }
  }

  void createPortWithSwitch(uint16_t portNum, const FlexDeviceId& switchId) {
    FlexDeviceId portId = endpoint_->getFlexId();
    portId.values[kPortIndexInFlexDeviceId] = portNum;

    const std::string portSpeed = fmt::format("{}Gbps", kPortSpeedGbps);
    auto port = std::make_unique<EthernetPort>(&simCtx_, portSpeed, portId);
    auto switchPort =
        std::make_unique<EthernetPort>(&simCtx_, portSpeed, switchId);
    auto channel = std::make_shared<EthernetChannel>(100);

    port->attach(channel.get());
    switchPort->attach(channel.get());

    EthernetPort* portPtr = port.get();
    endpoint_->addPort(portPtr, switchId, 0, portNum);

    ports_.emplace_back(std::move(port));
    switchPorts_.emplace_back(std::move(switchPort));
    channels_.emplace_back(channel);
  }

 protected:
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  std::unique_ptr<packsim::thrift::NicConfig> nicConfig_;
  std::unique_ptr<Endpoint> endpoint_;

  FlexDeviceId switch1Id_;
  FlexDeviceId switch2Id_;

  std::vector<std::unique_ptr<EthernetPort>> ports_;
  std::vector<std::unique_ptr<EthernetPort>> switchPorts_;
  std::vector<std::shared_ptr<EthernetChannel>> channels_;
};

// =============================================================================
// Basic API Tests: setQpnPortConfig / getQpnPortConfig
// =============================================================================

TEST_F(EndpointQpnPortConfigTest, SetAndGetQpnPortConfig) {
  // Setup: Define port restrictions for QPN 100
  constexpr uint32_t qpn = 100;
  const std::vector<uint16_t> srcPorts = {0, 1};
  const std::vector<uint16_t> dstPorts = {2, 3};

  packsim::thrift::QpnPortConfig portConfig;
  portConfig.srcPorts() = srcPorts;
  portConfig.dstPorts() = dstPorts;

  // Execute: Set port config
  endpoint_->setQpnPortConfig(qpn, portConfig);

  // Assert: Verify config is retrievable
  const auto* config = endpoint_->getQpnPortConfig(qpn);
  ASSERT_NE(config, nullptr);
  EXPECT_EQ(*config->srcPorts(), srcPorts);
  EXPECT_EQ(*config->dstPorts(), dstPorts);
}

TEST_F(EndpointQpnPortConfigTest, MultipleQpnsHaveSeparateConfigs) {
  // Setup: Set different configs for QPN 100 and QPN 200
  constexpr uint32_t qpn100 = 100;
  constexpr uint32_t qpn200 = 200;
  const std::vector<uint16_t> srcPorts100 = {0, 1};
  const std::vector<uint16_t> dstPorts100 = {0, 1};
  const std::vector<uint16_t> srcPorts200 = {2, 3};
  const std::vector<uint16_t> dstPorts200 = {2, 3};

  packsim::thrift::QpnPortConfig portConfig100;
  portConfig100.srcPorts() = srcPorts100;
  portConfig100.dstPorts() = dstPorts100;

  packsim::thrift::QpnPortConfig portConfig200;
  portConfig200.srcPorts() = srcPorts200;
  portConfig200.dstPorts() = dstPorts200;

  // Execute: Set both configs
  endpoint_->setQpnPortConfig(qpn100, portConfig100);
  endpoint_->setQpnPortConfig(qpn200, portConfig200);

  // Assert: Each QPN has its own config
  const auto* config100 = endpoint_->getQpnPortConfig(qpn100);
  const auto* config200 = endpoint_->getQpnPortConfig(qpn200);

  ASSERT_NE(config100, nullptr);
  ASSERT_NE(config200, nullptr);

  EXPECT_EQ(*config100->srcPorts(), srcPorts100);
  EXPECT_EQ(*config100->dstPorts(), dstPorts100);

  EXPECT_EQ(*config200->srcPorts(), srcPorts200);
  EXPECT_EQ(*config200->dstPorts(), dstPorts200);
}

// =============================================================================
// Routing Table Key Tests
// =============================================================================

TEST_F(
    EndpointQpnPortConfigTest,
    SameDestinationDifferentQpnsHaveDifferentConnections) {
  // Setup: Two QPNs going to the same destination with different port configs
  FlexDeviceId destination("1:0:0:0:0:0:0:1");
  constexpr uint32_t srcQPN1 = 100;
  constexpr uint32_t srcQPN2 = 200;
  constexpr uint32_t dstQPN = 300;

  // QPN1 can only use ports 0,1 -> ports 1,2 in FlexDeviceId
  packsim::thrift::QpnPortConfig portConfig1;
  portConfig1.srcPorts() = std::vector<uint16_t>{0, 1};
  portConfig1.dstPorts() = std::vector<uint16_t>{0, 1};
  endpoint_->setQpnPortConfig(srcQPN1, portConfig1);

  // QPN2 can only use ports 2,3 -> ports 3,4 in FlexDeviceId
  packsim::thrift::QpnPortConfig portConfig2;
  portConfig2.srcPorts() = std::vector<uint16_t>{2, 3};
  portConfig2.dstPorts() = std::vector<uint16_t>{2, 3};
  endpoint_->setQpnPortConfig(srcQPN2, portConfig2);

  // Execute: Get connections using different entropy values to cycle through
  std::unordered_set<uint16_t> qpn1Ports;
  std::unordered_set<uint16_t> qpn2Ports;

  packsim::thrift::TiebreakSpec tiebreak;
  tiebreak.ecmp() = packsim::thrift::EcmpTiebreakSpec{};

  for (uint16_t entropy = 0; entropy < 10; ++entropy) {
    const auto* conn1 = endpoint_->getPortLevelQPConnection(
        destination, srcQPN1, dstQPN, entropy, tiebreak);
    const auto* conn2 = endpoint_->getPortLevelQPConnection(
        destination, srcQPN2, dstQPN, entropy, tiebreak);

    ASSERT_NE(conn1, nullptr)
        << "no port-level connection for srcQPN1 at entropy " << entropy;
    ASSERT_NE(conn2, nullptr)
        << "no port-level connection for srcQPN2 at entropy " << entropy;
    qpn1Ports.emplace(conn1->srcId.values[kPortIndexInFlexDeviceId]);
    qpn2Ports.emplace(conn2->srcId.values[kPortIndexInFlexDeviceId]);
  }

  // Assert: QPN1 only used ports 1,2 and QPN2 only used ports 3,4
  EXPECT_TRUE(qpn1Ports.contains(1) || qpn1Ports.contains(2));
  EXPECT_FALSE(qpn1Ports.contains(3));
  EXPECT_FALSE(qpn1Ports.contains(4));

  EXPECT_TRUE(qpn2Ports.contains(3) || qpn2Ports.contains(4));
  EXPECT_FALSE(qpn2Ports.contains(1));
  EXPECT_FALSE(qpn2Ports.contains(2));
}

} // namespace packsim
