/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/endpoint/endpoint.h"
#include "packsim/endpoint/oss_tests/endpoint-test-fixture.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/prefix-id-utils.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/node/flex-node.h"
#include "packsim/node/port-reachability.h"
#include "packsim/transport/roce-base.h"

namespace packsim {

TEST_F(EndpointTransportConstructionTest, ConstructsComposableRoce) {
  packsim::thrift::RoceV2CongestionControlConfig congestionControl;
  congestionControl.dummy() = packsim::thrift::DummyRoceV2StrategyConfig{};
  packsim::thrift::RoceV2LossRecoveryConfig lossRecovery;
  lossRecovery.dummy() = packsim::thrift::DummyRoceV2StrategyConfig{};
  packsim::thrift::RoceV2TransportConfig config;
  config.congestionControl() = std::move(congestionControl);
  config.lossRecovery() = std::move(lossRecovery);
  nicConfig_->transport()->roceV2() = std::move(config);
  EXPECT_NE(
      dynamic_cast<RoceTransportBase*>(makeEndpoint()->getTransport()),
      nullptr);
}

// Distinct per-CoS sizes distinguish the sum from any single queue.
TEST_F(EndpointTransportConstructionTest, RxCreditBytesSumEveryCosQueue) {
  // `configureRoceV2Nic`, called from `SetUp`, overwrites `perCosBufferSize`,
  // so the override belongs in the test body.
  nicConfig_->perCosBufferSize() = std::vector<uint32_t>{4096, 8192, 16384};

  EXPECT_EQ(4096u + 8192u + 16384u, makeEndpoint()->getRxBufferBytes());
}

TEST_F(EndpointTransportConstructionTest, RejectsUnsetTransport) {
  nicConfig_->transport() = packsim::thrift::TransportConfig{};
  EXPECT_THAT(
      [&] { makeEndpoint(); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("transport config is unset")));
}

// Fixture for Endpoint::canPortsReach.
class CanPortsReachTest : public EndpointTestBase {
 protected:
  void SetUp() override {
    EndpointTestBase::SetUp();

    switch1Id_ = FlexDeviceId("2:0:1:0:0:0:0:0");
    switch2Id_ = FlexDeviceId("2:0:2:0:0:0:0:0");

    buildTopology();
    reachability_ =
        std::make_unique<PortReachability>(topo_, kPortIndexInFlexDeviceId);

    // Create endpoint with port addresses at kPortIndexInFlexDeviceId, backed
    // by a reachability tracer over the topology above.
    FlexNodeParams params{
        .simCtx = &simCtx_,
        .name = "xpu0",
        .flexId = FlexDeviceId("1:0:0:0:0:0:0:0"),
        .lpmTable = nullptr,
        .topo = &topo_,
        .portReachability = reachability_.get(),
    };
    endpoint_ = std::make_unique<Endpoint>(
        params, nicConfig_.get(), kPortIndexInFlexDeviceId);

    // Switch nodes whose names match the topology; a port's peer reports its
    // first-hop switch via getPeer()->getNode()->getName().
    switch1Node_ = std::make_unique<FlexNode>(&simCtx_, "switch1");
    switch2Node_ = std::make_unique<FlexNode>(&simCtx_, "switch2");

    // Create kNumPorts ports across two switches: the first half on switch1,
    // the second half on switch2.
    for (uint16_t portNum = 1; portNum <= kNumPorts; ++portNum) {
      const bool onSwitch1 = portNum <= kNumPorts / 2;
      createPortWithSwitch(
          portNum,
          onSwitch1 ? switch1Id_ : switch2Id_,
          onSwitch1 ? switch1Node_.get() : switch2Node_.get());
    }
  }

  // The 5-field accel prefix of xpu0.
  static std::vector<uint32_t> accelId() {
    return {1, 0, 0, 0, 0};
  }

  static packsim::thrift::Node makeSwitchNode(
      const std::string& name,
      std::vector<uint32_t> id) {
    packsim::thrift::Node n;
    n.name() = name;
    n.id() = std::move(id);
    n.type() = packsim::thrift::NodeType::SWITCH;
    n.lpmSpec()->validPrefixLengths() = std::vector<uint32_t>{kPrefixLen};
    return n;
  }

  // An accel<->switch link for the given port, plus the switch's LPM next-hop
  // toward xpu0 over it (Equal-Cost Multi-Path across the switch's ports).
  void addLink(
      thrift::LinkId linkId,
      const uint32_t port,
      const std::string& swName) {
    packsim::thrift::FlexLink link;
    link.linkId() = linkId;
    link.node1() = "xpu0";
    link.node2() = swName;
    std::vector<uint32_t> accelAddr = accelId();
    accelAddr.emplace_back(port); // port field at kPortIndexInFlexDeviceId
    accelAddr.emplace_back(0);
    accelAddr.emplace_back(0);
    link.sourceAddress() = accelAddr;
    (*topo_.links())[linkId] = link;
    packsim::thrift::LpmTableEntry& entry =
        (*(*topo_.nodes())[swName]
              .lpmSpec()
              ->prefixTable())[packsim::makePrefixKey(accelId())];
    entry.links()->emplace_back(linkId);
  }

  // Builds the forwarding topology that canPortsReach traces:
  //   Endpoint (xpu0)
  //     Port1 -> Switch1
  //     Port2 -> Switch1
  //     Port3 -> Switch2
  //     Port4 -> Switch2
  // Each switch forwards toward xpu0 over the ports attached to it.
  void buildTopology() {
    topo_.name() = "endpoint_test";
    topo_.fullPrefixLength() = kPrefixLen;
    auto& nodes = *topo_.nodes();

    packsim::thrift::Node accel;
    accel.name() = "xpu0";
    accel.id() = accelId();
    accel.type() = packsim::thrift::NodeType::ACCEL;
    nodes["xpu0"] = accel;
    nodes["switch1"] = makeSwitchNode("switch1", {2, 0, 1, 0, 0});
    nodes["switch2"] = makeSwitchNode("switch2", {2, 0, 2, 0, 0});

    addLink(/*linkId=*/1, /*port=*/1, "switch1");
    addLink(/*linkId=*/2, /*port=*/2, "switch1");
    addLink(/*linkId=*/3, /*port=*/3, "switch2");
    addLink(/*linkId=*/4, /*port=*/4, "switch2");
  }

  void createPortWithSwitch(
      uint16_t portNum,
      const FlexDeviceId& switchId,
      FlexNode* switchNode) {
    // Create port ID (XPU with port number at kPortIndexInFlexDeviceId)
    FlexDeviceId portId = endpoint_->getFlexId();
    portId.values[kPortIndexInFlexDeviceId] = portNum;

    const std::string portSpeed = fmt::format("{}Gbps", kPortSpeedGbps);
    // Create the endpoint-side port
    auto port = std::make_unique<EthernetPort>(&simCtx_, portSpeed, portId);

    // Create the switch-side port, owned by the named switch node so the
    // endpoint-side port can report its first-hop switch.
    auto switchPort =
        std::make_unique<EthernetPort>(&simCtx_, portSpeed, switchId);
    switchPort->setNode(switchNode);

    // Create channel connecting them (100ns delay)
    auto channel = std::make_shared<EthernetChannel>(100);

    port->attach(channel.get());
    switchPort->attach(channel.get());

    // Store pointers before moving
    EthernetPort* portPtr = port.get();

    // Add port to endpoint (this sets up the ports_ map)
    endpoint_->addPort(portPtr, switchId, 0, portNum);

    // Keep ownership of ports and channels to prevent deletion
    ports_.emplace_back(std::move(port));
    switchPorts_.emplace_back(std::move(switchPort));
    channels_.emplace_back(channel);
  }

  // topo_ and reachability_ outlive endpoint_ (declared first) since the
  // endpoint holds pointers to them.
  packsim::thrift::FlexTopology topo_;
  std::unique_ptr<PortReachability> reachability_;
  std::unique_ptr<FlexNode> switch1Node_;
  std::unique_ptr<FlexNode> switch2Node_;
  std::unique_ptr<Endpoint> endpoint_;

  FlexDeviceId switch1Id_;
  FlexDeviceId switch2Id_;

  // Keep ports alive
  std::vector<std::unique_ptr<EthernetPort>> ports_;
  std::vector<std::unique_ptr<EthernetPort>> switchPorts_;
  std::vector<std::shared_ptr<EthernetChannel>> channels_;
};

TEST_F(CanPortsReachTest, SameSwitch) {
  FlexDeviceId srcPort = endpoint_->getFlexId();
  srcPort.values[kPortIndexInFlexDeviceId] = 1;

  FlexDeviceId dstPort = endpoint_->getFlexId();
  dstPort.values[kPortIndexInFlexDeviceId] = 2;

  EXPECT_TRUE(endpoint_->canPortsReach(srcPort, dstPort))
      << "ports 1 and 2 both attach to switch1, so the fabric forwards between them";
}

TEST_F(CanPortsReachTest, DifferentSwitches) {
  FlexDeviceId srcPort = endpoint_->getFlexId();
  srcPort.values[kPortIndexInFlexDeviceId] = 1;

  FlexDeviceId dstPort = endpoint_->getFlexId();
  dstPort.values[kPortIndexInFlexDeviceId] = 3;

  EXPECT_FALSE(endpoint_->canPortsReach(srcPort, dstPort))
      << "port 1 (switch1) has no forwarding path to port 3 (switch2); treating them as reachable would let traffic egress a port that cannot deliver";
}

TEST_F(CanPortsReachTest, SamePort) {
  FlexDeviceId port1 = endpoint_->getFlexId();
  port1.values[kPortIndexInFlexDeviceId] = 1;

  EXPECT_TRUE(endpoint_->canPortsReach(port1, port1))
      << "a port must reach itself; classifying it unreachable would drop self-addressed traffic";
}

TEST_F(CanPortsReachTest, UnwiredSourcePortReachesNothing) {
  // Port number 99 is not one of the wired ports (1-4), so it is absent from
  // ports_ and has no egress port to trace from.
  FlexDeviceId unwiredSrc = endpoint_->getFlexId();
  unwiredSrc.values[kPortIndexInFlexDeviceId] = 99;
  FlexDeviceId dstPort = endpoint_->getFlexId();
  dstPort.values[kPortIndexInFlexDeviceId] = 1;

  EXPECT_FALSE(endpoint_->canPortsReach(unwiredSrc, dstPort))
      << "a srcPort not wired on this endpoint reaches nothing, rather than "
         "tracing from a port it does not own";
}

TEST_F(CanPortsReachTest, MultiplePortsSameSwitch) {
  FlexDeviceId port3 = endpoint_->getFlexId();
  port3.values[kPortIndexInFlexDeviceId] = 3;

  FlexDeviceId port4 = endpoint_->getFlexId();
  port4.values[kPortIndexInFlexDeviceId] = 4;

  EXPECT_TRUE(endpoint_->canPortsReach(port3, port4))
      << "ports 3 and 4 both attach to switch2, so the fabric forwards between them";
}

TEST_F(CanPortsReachTest, CrossSwitchPattern) {
  // Port 1 -> Switch1
  FlexDeviceId port1 = endpoint_->getFlexId();
  port1.values[kPortIndexInFlexDeviceId] = 1;

  // Port 4 -> Switch2
  FlexDeviceId port4 = endpoint_->getFlexId();
  port4.values[kPortIndexInFlexDeviceId] = 4;

  EXPECT_FALSE(endpoint_->canPortsReach(port1, port4))
      << "port 1 (switch1) cannot deliver to port 4 (switch2); no forwarding path exists";

  // A destination accel on another rack has no LPM route from this source port.
  FlexDeviceId crossRackDst = port4;
  crossRackDst.values[kRackIndexInFlexDeviceId] = 2;
  EXPECT_FALSE(endpoint_->canPortsReach(port1, crossRackDst))
      << "a destination accel on another rack has no LPM route from this source port, so it is unreachable";
}

TEST_F(CanPortsReachTest, SymmetricConnectivity) {
  FlexDeviceId port1 = endpoint_->getFlexId();
  port1.values[kPortIndexInFlexDeviceId] = 1;

  FlexDeviceId port2 = endpoint_->getFlexId();
  port2.values[kPortIndexInFlexDeviceId] = 2;

  bool canReach1to2 = endpoint_->canPortsReach(port1, port2);
  bool canReach2to1 = endpoint_->canPortsReach(port2, port1);

  EXPECT_EQ(canReach1to2, canReach2to1)
      << "reachability must be symmetric, else a path usable in one direction would be rejected in the other";
  EXPECT_TRUE(canReach1to2)
      << "ports 1 and 2 share switch1 and must be reachable";
}

// canPortsReach precondition errors need only the NIC config, not the traced
// topology built by CanPortsReachTest::SetUp, so they use a minimal fixture.
class CanPortsReachErrorTest : public EndpointTestBase {};

TEST_F(CanPortsReachErrorTest, NoPortIndex) {
  // Create endpoint without portIndex
  FlexNodeParams params{
      .simCtx = &simCtx_,
      .name = "xpu1",
      .flexId = FlexDeviceId("1:0:0:0:0:0:0:1"),
      .lpmTable = nullptr,
      .topo = nullptr,
  };
  auto endpointNoPortIndex =
      std::make_unique<Endpoint>(params, nicConfig_.get(), std::nullopt);

  FlexDeviceId port1 = endpointNoPortIndex->getFlexId();
  port1.values[kPortIndexInFlexDeviceId] = 1;

  FlexDeviceId port2 = endpointNoPortIndex->getFlexId();
  port2.values[kPortIndexInFlexDeviceId] = 2;

  // Without a port index there is no port field to trace, so canPortsReach
  // cannot answer and must throw rather than guess.
  EXPECT_THAT(
      [&]() { endpointNoPortIndex->canPortsReach(port1, port2); },
      ::testing::ThrowsMessage<std::runtime_error>(
          ::testing::HasSubstr("Port index must be set")));
}

TEST_F(CanPortsReachErrorTest, NoReachabilityTracer) {
  FlexNodeParams params{
      .simCtx = &simCtx_,
      .name = "xpu2",
      .flexId = FlexDeviceId("1:0:0:0:0:0:0:2"),
      .lpmTable = nullptr,
      .topo = nullptr,
  };
  auto endpointNoTracer = std::make_unique<Endpoint>(
      params, nicConfig_.get(), kPortIndexInFlexDeviceId);

  FlexDeviceId port1 = endpointNoTracer->getFlexId();
  port1.values[kPortIndexInFlexDeviceId] = 1;
  FlexDeviceId port2 = endpointNoTracer->getFlexId();
  port2.values[kPortIndexInFlexDeviceId] = 2;

  EXPECT_THAT(
      [&]() { endpointNoTracer->canPortsReach(port1, port2); },
      ::testing::ThrowsMessage<std::runtime_error>(
          ::testing::HasSubstr("requires the topology's reachability tracer")));
}

// Fixture for Endpoint::getNicStatsSummaryResults on a multi-port endpoint
// without a port field, which lists its ports by their index on the node.
class EndpointNicSummaryTest : public EndpointTestBase {
 protected:
  void SetUp() override {
    EndpointTestBase::SetUp();
    FlexNodeParams params{
        .simCtx = &simCtx_,
        .name = "xpu0",
        .flexId = FlexDeviceId("1:0:0:0:0:0:0:0"),
        .lpmTable = nullptr,
        .topo = nullptr,
    };
    endpoint_ = std::make_unique<Endpoint>(params, nicConfig_.get());
  }

  // Adds port `portNum` with `portNum + 1` CoS queues, so the number of CoS
  // entries in its MAC stats identifies the port.
  void addPortMarkedByQueueCount(const uint16_t portNum) {
    FlexDeviceId portId = endpoint_->getFlexId();
    portId.values[kPortIndexInFlexDeviceId] = portNum + 1;
    auto port = std::make_unique<EthernetPort>(
        &simCtx_, fmt::format("{}Gbps", kPortSpeedGbps), portId);
    endpoint_->addPort(
        port.get(), FlexDeviceId("2:0:1:0:0:0:0:0"), portNum, portNum);
    // After addPort, which applies the NIC's single-queue config to the port.
    port->setPortQueueConfig(
        std::vector<packsim::thrift::PortQueueConfig>(portNum + 1));
    ports_.emplace_back(std::move(port));
  }

  std::unique_ptr<Endpoint> endpoint_;
  std::vector<std::unique_ptr<EthernetPort>> ports_;
};

TEST_F(EndpointNicSummaryTest, MacStatsListsPortsInAscendingNodePortIndex) {
  // Neither this insertion order nor its reverse is port order.
  const std::vector<uint16_t> insertionOrder{3, 0, 6, 1, 7, 2, 5, 4};
  for (const uint16_t portNum : insertionOrder) {
    addPortMarkedByQueueCount(portNum);
  }

  const auto summary = endpoint_->getNicStatsSummaryResults();
  std::vector<size_t> cosCountByIndex;
  for (const auto& portMacStats : *summary.macStats()) {
    cosCountByIndex.emplace_back(portMacStats.cosStats()->size());
  }

  // Entry k is port k, which has k + 1 CoS queues.
  const std::vector<size_t> expected{1, 2, 3, 4, 5, 6, 7, 8};
  EXPECT_EQ(cosCountByIndex, expected);
}

// Endpoints use port-granular addressing only when a port index is configured;
// these tests need only the NIC config, not the traced topology.
TEST_F(EndpointAddressingTest, UsesPortLevelAddressesWhenPortIndexIsZero) {
  // Create endpoint with portIndex=0 (valid)
  FlexNodeParams params{
      .simCtx = &simCtx_,
      .name = "xpu_port0",
      .flexId = FlexDeviceId("1:0:1:1:1:0:0:0"),
      .lpmTable = nullptr,
      .topo = nullptr,
  };
  auto endpointWithPortZero =
      std::make_unique<Endpoint>(params, nicConfig_.get(), uint8_t{0});

  auto rocePkt = createPacket<PacketType::ROCE>(100);
  EXPECT_TRUE(endpointWithPortZero->usingPortLevelAddresses(rocePkt))
      << "with a port index configured, RoCEv2 must be addressed at port granularity to reach the selected NIC port";

  auto ipv6Pkt = createPacket<PacketType::IPV6>(100);
  EXPECT_FALSE(endpointWithPortZero->usingPortLevelAddresses(ipv6Pkt))
      << "IPV6 traffic is accel-addressed even when a port index is configured";
}

TEST_F(EndpointAddressingTest, FallsBackToAccelAddressesWhenNoPortIndex) {
  // Create endpoint without portIndex
  FlexNodeParams params{
      .simCtx = &simCtx_,
      .name = "xpu_no_port",
      .flexId = FlexDeviceId("1:0:1:1:1:0:0:0"),
      .lpmTable = nullptr,
      .topo = nullptr,
  };
  auto endpointNoPortIndex =
      std::make_unique<Endpoint>(params, nicConfig_.get(), std::nullopt);

  auto rocePkt = createPacket<PacketType::ROCE>(100);
  EXPECT_FALSE(endpointNoPortIndex->usingPortLevelAddresses(rocePkt))
      << "without a port index, RoCEv2 must fall back to accel-level addressing";
}

} // namespace packsim
