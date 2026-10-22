/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/scheduler.h"
#include "packsim/link-layer/cbfc/cbfc.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/link-layer/oss_tests/lambda-port-observer.h"
#include "packsim/node/flex-node.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/switch/fb-switch.h"
#include "packsim/switch/oss_tests/fb-switch-test-fixture.h"

namespace thrift = packsim::thrift;

using ::testing::AllOf;
using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim::fbswitch::test {

TEST_F(FbSwitchTest, TestInitializesWithMinimalConfiguration) {
  ASSERT_NE(fbSwitch_, nullptr);

  auto const summary = fbSwitch_->getSummary();
  EXPECT_EQ(summary.ingressBuffers()->size(), 1);
}

TEST_F(FbSwitchTest, TestSummaryNumbersBuffersPerDirection) {
  // bufferIdx is not carried by BufferTracker -- getSummary() derives it from
  // list position, so the numbering is part of the getSummary() contract.
  // Assert the whole ascending sequence rather than just "not 0": the latter
  // passes by accident whenever the first buffer is the one being checked.
  constexpr uint32_t numPorts = 2;
  auto config = FbSwitchTestHelpers::createTwoBufferSwitchConfig(numPorts);
  auto lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};
  packsim::FbSwitch twoBufferSwitch(params, &config);

  auto const summary = twoBufferSwitch.getSummary();

  ASSERT_EQ(
      summary.ingressBuffers()->size(),
      FbSwitchTestHelpers::kNumBuffersPerDirection);
  ASSERT_EQ(
      summary.egressBuffers()->size(),
      FbSwitchTestHelpers::kNumBuffersPerDirection);
  for (uint32_t i = 0; i < FbSwitchTestHelpers::kNumBuffersPerDirection; ++i) {
    EXPECT_EQ(*summary.ingressBuffers()->at(i).bufferIdx(), i);
    // Egress restarts at 0 rather than continuing the ingress numbering.
    EXPECT_EQ(*summary.egressBuffers()->at(i).bufferIdx(), i);
  }
}

TEST_F(FbSwitchTest, TestNonRecoverablePacketsThrowOnRoutingErrors) {
  // Setup switch with empty routing table
  auto const emptyLpmTable = FbSwitchTestHelpers::createEmptyLpmTable();
  auto const minimalTopology = FbSwitchTestHelpers::createMinimalTopology();
  packsim::FlexNodeParams const params{
      simContext_.get(),
      switchName_,
      flexDeviceId_,
      &emptyLpmTable,
      &minimalTopology};
  auto testSwitch = std::make_unique<TestFbSwitch>(params, &switchConfig_);

  // Set up a port and add it to the switch
  std::unique_ptr<packsim::EthernetPort> mockPort =
      std::make_unique<MockEthernetPort>(simContext_.get());
  auto* mockPortPtr = mockPort.get();

  packsim::FlexDeviceId const partnerDeviceId({7, 8, 9});
  testSwitch->addPort(mockPort.get(), partnerDeviceId, 0, 0);
  testSwitch->adoptPort(mockPort);

  // Setup packet connection with unreachable destination
  packsim::FlexDeviceId const sourceId({1, 2, 3});
  packsim::FlexDeviceId const destinationId({4, 5, 6});
  packsim::IPConnection const connection{sourceId, destinationId};

  // Test ROCE packet - should throw
  auto rocePacket = FbSwitchTestHelpers::createTestPacketWithConnection(
      packsim::PacketType::ROCE, connection);

  ASSERT_THAT(
      [&]() { testSwitch->routeSwitchPacket(mockPortPtr, rocePacket); },
      ThrowsMessage<packsim::thrift::PackSimRoutingException>(
          "Found no links to use on test_switch (1:2:3::) for destination 4:5:6::"));

  // Test ETHERNET packet - should throw
  auto ethernetPacket = FbSwitchTestHelpers::createTestPacketWithConnection(
      packsim::PacketType::ETHERNET, connection);

  ASSERT_THAT(
      [&]() { testSwitch->routeSwitchPacket(mockPortPtr, ethernetPacket); },
      ThrowsMessage<packsim::thrift::PackSimRoutingException>(
          "Found no links to use on test_switch (1:2:3::) for destination 4:5:6::"));
}

TEST_F(FbSwitchTest, TestRecordPacketDropCreatesEventWithAllFields) {
  // Call recordPacketDrop directly via TestFbSwitch wrapper
  constexpr uint64_t packetBytes = 1500;
  auto const lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  auto const topology = FbSwitchTestHelpers::createMinimalTopology();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, &topology};
  auto testSwitch = std::make_unique<TestFbSwitch>(params, &switchConfig_);

  testSwitch->recordPacketDrop(
      /* reason= */ packsim::switches::thrift::PacketDropReason::NO_ROUTE,
      /* pktType= */ packsim::PacketType::UDP,
      /* bytes= */ packetBytes,
      /* numPkts= */ 2,
      /* portNum */ 0);

  auto const& dropEvents = testSwitch->packetDropStats_;

  // Verify one drop event was recorded
  ASSERT_EQ(dropEvents.size(), 1);

  // Verify all fields are correctly set
  auto const& dropEvent = dropEvents[0];
  EXPECT_EQ(
      dropEvent.dropReason(),
      packsim::switches::thrift::PacketDropReason::NO_ROUTE);
  EXPECT_EQ(dropEvent.packetType(), "UDP");
  EXPECT_EQ(dropEvent.droppedPkts(), 2U);
  EXPECT_EQ(dropEvent.droppedBytes(), 1500U);
  EXPECT_EQ(dropEvent.eventTimeNs(), 0U);
  EXPECT_EQ(dropEvent.portNum(), 0U);
}

TEST_F(FbSwitchTest, TestRecordPacketDropIncludesPortNumber) {
  // Call recordPacketDrop with a port number
  constexpr uint64_t packetBytes = 1500;
  auto const lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  auto const topology = FbSwitchTestHelpers::createMinimalTopology();
  constexpr uint16_t kPortNumber = 42;
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, &topology};
  auto testSwitch = std::make_unique<TestFbSwitch>(params, &switchConfig_);

  testSwitch->recordPacketDrop(
      packsim::switches::thrift::PacketDropReason::LINK_ERROR,
      packsim::PacketType::UDP,
      packetBytes,
      /* numPkts= */ 1,
      kPortNumber);

  auto const& dropEvents = testSwitch->packetDropStats_;

  // Verify one drop event was recorded
  ASSERT_EQ(dropEvents.size(), 1);

  // Verify port number is correctly set
  auto const& dropEvent = dropEvents[0];
  EXPECT_EQ(
      dropEvent.dropReason(),
      packsim::switches::thrift::PacketDropReason::LINK_ERROR);
  EXPECT_EQ(dropEvent.packetType(), "UDP");
  EXPECT_EQ(dropEvent.droppedPkts(), 1U);
  EXPECT_EQ(dropEvent.droppedBytes(), 1500U);
  EXPECT_EQ(dropEvent.eventTimeNs(), 0U);
  EXPECT_EQ(dropEvent.portNum(), 42U); // Verify port number is stored
}

TEST_F(FbSwitchTest, TestPacketDroppedOnSendRecordsDropEvent) {
  // Setup switch
  auto const lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};
  auto testSwitch = std::make_unique<TestFbSwitch>(params, &switchConfig_);

  // Set up a port
  std::unique_ptr<packsim::EthernetPort> mockPort =
      std::make_unique<MockEthernetPort>(simContext_.get());
  auto* mockPortPtr = mockPort.get();
  mockPortPtr->setPortNum(0);

  // Create test packet with specific properties
  packsim::FlexDeviceId const sourceId({1, 2, 3});
  packsim::FlexDeviceId const destinationId({4, 5, 6});
  packsim::IPConnection const connection{sourceId, destinationId};
  auto packet = FbSwitchTestHelpers::createTestPacketWithConnection(
      packsim::PacketType::UDP, connection, /* portNum= */ 1);
  packet->tag.pg = 2; // Set priority group

  constexpr uint8_t queue = 2;
  uint32_t const expectedBytes = packet->getSize();

  // To test onPacketDroppedOnSend, we need the packet to already be in the
  // buffer The method calls wredDrop which calls removePacket, expecting packet
  // is buffered So we first admit the packet
  uint16_t const inPort = packet->tag.port;
  uint8_t const inQueue = packet->tag.pg;
  auto const bufIdx = testSwitch->getBuff(inPort, mockPortPtr->getPortNum());
  testSwitch->ingressBufs_.at(bufIdx).admitPacket(
      inPort, inQueue, expectedBytes);

  // Now call onPacketDroppedOnSend
  testSwitch->onPacketDroppedOnSend(
      *mockPortPtr,
      packet,
      queue,
      packsim::switches::thrift::PacketDropReason::BUFFER_FULL);

  // Verify packet drop event was recorded with correct fields
  auto const& dropEvents = testSwitch->packetDropStats_;
  ASSERT_EQ(dropEvents.size(), 1) << "Expected exactly one drop event";

  auto const& dropEvent = dropEvents[0];
  EXPECT_EQ(
      dropEvent.dropReason(),
      packsim::switches::thrift::PacketDropReason::BUFFER_FULL)
      << "Expected BUFFER_FULL drop reason";
  EXPECT_EQ(dropEvent.packetType(), "UDP") << "Expected UDP packet type";
  EXPECT_EQ(dropEvent.droppedPkts(), 1U) << "Expected 1 dropped packet";
  EXPECT_EQ(dropEvent.droppedBytes(), expectedBytes)
      << "Expected dropped bytes to match packet size + ethernet header";
  EXPECT_EQ(dropEvent.portNum(), 0U);
}

TEST_F(FbSwitchTest, TestAddSwitchPortThrowsWhenPortGroupExhausted) {
  // Create a switch config with a port group that has only 1 port
  thrift::SwitchConfig limitedConfig =
      FbSwitchTestHelpers::createTestSwitchConfig(1);
  limitedConfig.portGroups()["limited_group"] = {0}; // Only one port available

  auto const lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};
  auto testSwitch = std::make_unique<TestFbSwitch>(params, &limitedConfig);

  // Create the first port and add it successfully
  std::unique_ptr<packsim::EthernetPort> ethPort1 =
      std::make_unique<MockEthernetPort>(simContext_.get());
  packsim::FlexDeviceId const partnerId1({7, 8, 9});

  EXPECT_NO_THROW(testSwitch->addSwitchPort(
      ethPort1.get(), "limited_group", partnerId1, 0));
  testSwitch->adoptPort(ethPort1);

  // Try to add a second port to the same port group
  std::unique_ptr<packsim::EthernetPort> ethPort2 =
      std::make_unique<MockEthernetPort>(simContext_.get());
  packsim::FlexDeviceId const partnerId2({10, 11, 12});

  // Should throw because we've exhausted the port group
  EXPECT_THAT(
      [&]() {
        testSwitch->addSwitchPort(
            ethPort2.get(), "limited_group", partnerId2, 1);
      },
      ThrowsMessage<std::runtime_error>(::testing::HasSubstr(
          "Not enough ports (only 1) provided in switch config for port group 'limited_group'")));
}

TEST_F(FbSwitchTest, TestAddSwitchPortThrowsWhenPortAlreadyAssigned) {
  // Create a config with two port groups that share the same port number
  thrift::SwitchConfig duplicateConfig =
      FbSwitchTestHelpers::createTestSwitchConfig(2);
  duplicateConfig.portGroups()["group1"] = {0}; // Port 0
  duplicateConfig.portGroups()["group2"] = {0}; // Same port 0 (invalid!)

  auto const lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};
  auto testSwitch = std::make_unique<TestFbSwitch>(params, &duplicateConfig);

  // Add the first port to group1 (port number 0)
  std::unique_ptr<packsim::EthernetPort> ethPort1 =
      std::make_unique<MockEthernetPort>(simContext_.get());
  packsim::FlexDeviceId const partnerId1({7, 8, 9});

  EXPECT_NO_THROW(
      testSwitch->addSwitchPort(ethPort1.get(), "group1", partnerId1, 0));
  testSwitch->adoptPort(ethPort1);

  // Try to add a second port to group2 (also uses port 0)
  std::unique_ptr<packsim::EthernetPort> ethPort2 =
      std::make_unique<MockEthernetPort>(simContext_.get());
  packsim::FlexDeviceId const partnerId2({10, 11, 12});

  // Should throw because port 0 is already assigned
  EXPECT_THAT(
      [&]() {
        testSwitch->addSwitchPort(ethPort2.get(), "group2", partnerId2, 1);
      },
      ThrowsMessage<std::runtime_error>(::testing::HasSubstr(
          fmt::format("Tried to add port 0 to {} twice", switchName_))));
}

// 65536 truncates to port 0, a valid port, so the range check must run before
// the port number is narrowed to `uint16_t`.
TEST_F(FbSwitchTest, TestAddSwitchPortRejectsPortAboveUint16Range) {
  constexpr uint32_t kFirstPortAboveUint16 = 65536;
  thrift::SwitchConfig config = FbSwitchTestHelpers::createTestSwitchConfig(1);
  config.portGroups()["group"] = {kFirstPortAboveUint16};
  auto const lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};
  TestFbSwitch testSwitch(params, &config);
  MockEthernetPort ethPort(simContext_.get());

  EXPECT_THAT(
      [&]() {
        testSwitch.addSwitchPort(
            &ethPort, "group", packsim::FlexDeviceId({7, 8, 9}), 0);
      },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          fmt::format(
              "Port {} in port group 'group'", kFirstPortAboveUint16))));
  EXPECT_TRUE(testSwitch.getPortToEthPort().empty());
}

// The largest `uint16_t` marks a port that has no port number yet, so it is
// not a usable port number.
TEST_F(FbSwitchTest, TestAddSwitchPortRejectsUnattachedPortNumber) {
  thrift::SwitchConfig config = FbSwitchTestHelpers::createTestSwitchConfig(1);
  config.portGroups()["group"] = {EthernetPort::kUnattachedPort};
  auto const lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};
  TestFbSwitch testSwitch(params, &config);
  MockEthernetPort ethPort(simContext_.get());

  EXPECT_THAT(
      [&]() {
        testSwitch.addSwitchPort(
            &ethPort, "group", packsim::FlexDeviceId({7, 8, 9}), 0);
      },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          fmt::format(
              "Port {} in port group 'group'",
              EthernetPort::kUnattachedPort))));
  EXPECT_TRUE(testSwitch.getPortToEthPort().empty());
}

// A pairMap key names two `uint32_t` port numbers, like every other port number
// in the switch config, so a key whose port does not fit is malformed.
TEST_F(FbSwitchTest, TestRejectsPairMapPortAboveUint32Range) {
  thrift::SwitchConfig config =
      FbSwitchTestHelpers::createTwoBufferSwitchConfig(1);
  config.portToBufMap()->pairMap()["4294967296.0"] = 0;
  auto const lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};

  EXPECT_THAT(
      [&]() { TestFbSwitch testSwitch(params, &config); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Malformed pairMap key '4294967296.0'")));
}

// A buffer config error names the switch and which of its buffers is wrong.
TEST_F(FbSwitchTest, TestBufferConfigErrorNamesSwitchAndBuffer) {
  thrift::SwitchConfig config =
      FbSwitchTestHelpers::createTwoBufferSwitchConfig(1);
  config.egressBufs()->at(1).cellSizeBytes() = 0;
  auto const lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};

  EXPECT_THAT(
      [&]() { TestFbSwitch testSwitch(params, &config); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Switch test_switch: SwitchConfig.egressBufs[1]: "
          "SwitchBufferConfig (egress buffer): cellSizeBytes is 0")));
}

TEST_F(FbSwitchTest, TestPacketDroppedOnReceiveRecordsLinkError) {
  // Setup switch
  auto const lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};
  auto testSwitch = std::make_unique<TestFbSwitch>(params, &switchConfig_);

  // Set up a port
  std::unique_ptr<packsim::EthernetPort> mockPort =
      std::make_unique<MockEthernetPort>(simContext_.get());
  auto* mockPortPtr = mockPort.get();
  constexpr uint16_t portNum = 5;
  mockPortPtr->setPortNum(portNum);

  // Create test packet
  packsim::FlexDeviceId const sourceId({1, 2, 3});
  packsim::FlexDeviceId const destinationId({4, 5, 6});
  packsim::IPConnection const connection{sourceId, destinationId};
  auto packet = FbSwitchTestHelpers::createTestPacketWithConnection(
      packsim::PacketType::UDP, connection, /* portNum= */ 1);

  constexpr uint8_t queue = 0;
  uint32_t const expectedBytes = packet->getSize();

  // Call onPacketDroppedOnReceive - simulates packet dropped due to link error
  // (e.g., BER)
  testSwitch->onPacketDroppedOnReceive(
      *mockPortPtr,
      packet,
      queue,
      packsim::switches::thrift::PacketDropReason::LINK_ERROR);

  // Verify packet drop event was recorded with correct fields
  auto const& dropEvents = testSwitch->packetDropStats_;
  ASSERT_EQ(dropEvents.size(), 1) << "Expected exactly one drop event";

  auto const& dropEvent = dropEvents[0];
  EXPECT_EQ(
      dropEvent.dropReason(),
      packsim::switches::thrift::PacketDropReason::LINK_ERROR)
      << "Expected LINK_ERROR drop reason for receive path drops";
  EXPECT_EQ(dropEvent.packetType(), "UDP") << "Expected UDP packet type";
  EXPECT_EQ(dropEvent.droppedPkts(), 1U) << "Expected 1 dropped packet";
  EXPECT_EQ(dropEvent.droppedBytes(), expectedBytes)
      << "Expected dropped bytes to match packet size (no ethernet header on "
         "receive)";
  EXPECT_EQ(dropEvent.portNum(), 5U);
}

namespace {
// Builds a switch with egress enabled, and ingress unless `useIngress` is
// false, where egress has zero capacity (all admissions fail), wires two ports
// to distinct port numbers, and creates a UDP packet ready to trigger the
// egress-rejection path.
struct EgressRejectFixture {
  static uint16_t constexpr kInPort = 0;
  static uint16_t constexpr kOutPort = 1;

  thrift::SwitchConfig config;
  thrift::LpmTableSpec lpmTable;
  std::unique_ptr<TestFbSwitch> sw;
  packsim::EthernetPort* inEthPortPtr;
  packsim::EthernetPort* outEthPortPtr;
  packsim::IPConnection connection;
  packsim::PacketPtr packet;

  EgressRejectFixture(
      packsim::SimContext* simCtx,
      std::string const& switchName,
      packsim::FlexDeviceId const& flexDeviceId,
      bool fatalOnDrop,
      bool useIngress)
      : connection(
            packsim::FlexDeviceId({1, 2, 3}),
            packsim::FlexDeviceId({4, 5, 6})) {
    constexpr uint32_t numPorts = 2;
    config = FbSwitchTestHelpers::createTestSwitchConfig(numPorts);
    config.enableFatalOnDrop() = fatalOnDrop;
    config.useIngress() = useIngress;
    config.useEgress() = true;

    // Egress buffer: zero capacity everywhere so tryAddPacket always fails.
    // Egress ports populate `ucQueues` (EgressQueueBufferConfig) -- `pgs` is
    // ingress-only.
    thrift::SwitchBufferConfig egressBufferConfig;
    egressBufferConfig.cellSizeBytes() =
        FbSwitchTestHelpers::kDefaultCellSizeBytes;
    egressBufferConfig.totalCells() = 0U;
    egressBufferConfig.headroomPoolCells() = 0U;
    for (uint32_t portIdx = 0; portIdx < numPorts; ++portIdx) {
      thrift::PortBufferConfig portConfig;
      portConfig.port() = portIdx;
      portConfig.isIngrPort() = false;
      portConfig.isEgrPort() = true;
      portConfig.portRsvdCells() = 0U;
      portConfig.portMaxSharedCells() = 0U;
      for (uint32_t q = 0; q < FbSwitchTestHelpers::kDefaultNumPriorityGroups;
           ++q) {
        thrift::EgressQueueBufferConfig qConfig;
        qConfig.cos() = q;
        qConfig.rsvdCells() = 0U;
        qConfig.dynamicSharing() = false;
        qConfig.alpha() = 0.0;
        qConfig.maxSharedCells() = 0U;
        qConfig.resumeHystCells() = 0U;
        portConfig.ucQueues()->emplace_back(std::move(qConfig));
      }
      egressBufferConfig.ports()[portIdx] = std::move(portConfig);
    }
    // egressBufs is a separate list from bufs; sizes must match
    // (XCHECK in FbSwitch ctor).
    config.egressBufs() = {std::move(egressBufferConfig)};

    // With both ingress and egress enabled, FbSwitch reads pairMap instead of
    // singleMap to determine the per-(ingress,egress) port-pair buffer index.
    for (uint32_t inIdx = 0; inIdx < numPorts; ++inIdx) {
      for (uint32_t outIdx = 0; outIdx < numPorts; ++outIdx) {
        auto const key = fmt::format("{}.{}", inIdx, outIdx);
        config.portToBufMap()->pairMap()[key] = 0;
      }
    }

    lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
    packsim::FlexNodeParams const params{
        simCtx, switchName, flexDeviceId, &lpmTable, nullptr};
    sw = std::make_unique<TestFbSwitch>(params, &config);

    // Two ports on distinct port numbers -- inPort != outPort so a
    // wrong-key rollback would target a different queue tracker than the one
    // that admitted the packet.  Ports are owned by the switch after
    // adoptPort; raw pointers remain valid for the lifetime of the fixture.
    std::unique_ptr<packsim::EthernetPort> inEthPort =
        std::make_unique<MockEthernetPort>(simCtx);
    std::unique_ptr<packsim::EthernetPort> outEthPort =
        std::make_unique<MockEthernetPort>(simCtx);
    inEthPortPtr = inEthPort.get();
    outEthPortPtr = outEthPort.get();
    inEthPortPtr->setPortNum(kInPort);
    outEthPortPtr->setPortNum(kOutPort);
    packsim::FlexDeviceId const partnerIn({7, 8, 9});
    packsim::FlexDeviceId const partnerOut({10, 11, 12});
    sw->addPort(inEthPortPtr, partnerIn, /* linkId */ 0, kInPort);
    sw->addPort(outEthPortPtr, partnerOut, /* linkId */ 1, kOutPort);
    sw->adoptPort(inEthPort);
    sw->adoptPort(outEthPort);

    packet = FbSwitchTestHelpers::createTestPacketWithConnection(
        packsim::PacketType::UDP, connection, kInPort);
  }
};

// DSCP CS1, which the switch's default DSCP map puts in queue 1.
constexpr uint8_t kCs1Dscp = 8;
// The queue a remapped DSCP map puts CS1 in instead.
constexpr uint8_t kRemappedQueue = 3;
// Fewer queues than the eight the default DSCP map selects among.
constexpr uint32_t kFewerQueues = 4;

// Give every port entry in `bufCfg` one queue per priority group, so a port
// built from it classifies packets by its DSCP map.
void addQueuePerPriorityGroup(thrift::SwitchBufferConfig& bufCfg) {
  for (auto& [portNum, portCfg] : *bufCfg.ports()) {
    portCfg.queueCfg()->resize(FbSwitchTestHelpers::kDefaultNumPriorityGroups);
  }
}

// A UDP packet that arrives on `inPort` marked DSCP CS1.
packsim::PacketPtr makeCs1Packet(
    packsim::IPConnection const& connection,
    uint16_t inPort) {
  auto packet = FbSwitchTestHelpers::createTestPacketWithConnection(
      packsim::PacketType::UDP, connection, inPort);
  packet.as<packsim::UdpPacket>()->dscp =
      packsim::Ipv6Packet::DscpType::DSCP_CS1;
  return packet;
}

// A switch built from `switchConfig` whose two "test_group" ports, 0 and 1,
// are added through addSwitchPort, which applies each port's queue, filter and
// scheduler settings.
struct TwoPortSwitch {
  TwoPortSwitch(
      packsim::SimContext* simCtx,
      std::string const& switchName,
      packsim::FlexDeviceId const& flexDeviceId,
      thrift::SwitchConfig switchConfig)
      : config(std::move(switchConfig)),
        lpmTable(FbSwitchTestHelpers::createMinimalLpmTable()) {
    packsim::FlexNodeParams const params{
        simCtx, switchName, flexDeviceId, &lpmTable, nullptr};
    sw = std::make_unique<TestFbSwitch>(params, &config);
    std::unique_ptr<packsim::EthernetPort> port0 =
        std::make_unique<MockEthernetPort>(simCtx);
    std::unique_ptr<packsim::EthernetPort> port1 =
        std::make_unique<MockEthernetPort>(simCtx);
    inPort = port0.get();
    outPort = port1.get();
    sw->addSwitchPort(
        inPort, "test_group", packsim::FlexDeviceId({7, 8, 9}), /*linkId=*/0);
    sw->addSwitchPort(
        outPort,
        "test_group",
        packsim::FlexDeviceId({10, 11, 12}),
        /*linkId=*/1);
    sw->adoptPort(port0);
    sw->adoptPort(port1);
  }

  thrift::SwitchConfig config;
  thrift::LpmTableSpec lpmTable;
  std::unique_ptr<TestFbSwitch> sw;
  // Port 0, owned by `sw`.
  packsim::EthernetPort* inPort{nullptr};
  // Port 1, owned by `sw`.
  packsim::EthernetPort* outPort{nullptr};
};
} // namespace

TEST_F(
    FbSwitchTest,
    TestCheckIngressEgressAdmissionRollsBackIngressWithIngressKeysOnEgressFailure) {
  // Regression test: when ingress admits but egress fails, the rollback must
  // remove cells from the ingress buffer using the (inPort, inpCos) keys
  // that were used to admit -- not (outPort, outQueue) which addresses a
  // different queue tracker and leaves the (inPort, inpCos) queue with stuck
  // cells.
  EgressRejectFixture f(
      simContext_.get(),
      switchName_,
      flexDeviceId_,
      /* fatalOnDrop */ false,
      /* useIngress */ true);

  // Default mock ports use a single queue, so getPktCos returns 0 for both
  // inpCos and outQueue -- but the bug manifests via the distinct
  // (inPort=0, outPort=1) port numbers regardless of queue value.
  uint8_t const inpCos = f.inEthPortPtr->getPktCos(f.packet);
  uint8_t const outQueue = f.outEthPortPtr->getPktCos(f.packet);
  auto const bufIdx = f.sw->getBuff(f.kInPort, f.kOutPort);

  // Pre-condition: both ingress queues empty.
  ASSERT_EQ(
      f.sw->ingressBufs_.at(bufIdx).queueStats(f.kInPort, inpCos).usedCells, 0u)
      << "Ingress (inPort, inpCos) queue must start empty";
  ASSERT_EQ(
      f.sw->ingressBufs_.at(bufIdx).queueStats(f.kOutPort, outQueue).usedCells,
      0u)
      << "Ingress (outPort, outQueue) queue must start empty";

  // Trigger the failure path: ingress admits, egress rejects, rollback fires.
  bool const admitted = f.sw->checkIngressEgressAdmission(
      f.inEthPortPtr, f.outEthPortPtr, f.packet);
  EXPECT_FALSE(admitted)
      << "Egress is configured to reject all admissions, so the call should "
         "return false";

  // Post-condition: the (inPort, inpCos) ingress queue must be fully drained.
  // Without the fix, the rollback would have called removePacket(outPort,
  // outQueue, ...), leaving cells stuck on (inPort, inpCos).
  EXPECT_EQ(
      f.sw->ingressBufs_.at(bufIdx).queueStats(f.kInPort, inpCos).usedCells, 0u)
      << "Rollback must remove cells from the (inPort, inpCos) queue that "
         "ingress admit added them to";

  // Sanity: the unrelated (outPort, outQueue) ingress queue should never have
  // been touched.
  EXPECT_EQ(
      f.sw->ingressBufs_.at(bufIdx).queueStats(f.kOutPort, outQueue).usedCells,
      0u)
      << "Rollback must not touch the (outPort, outQueue) ingress queue";

  // Drop accounting: the failed admission must produce a single
  // BUFFER_FULL drop event attributed to the ingress port the packet
  // arrived on. Without this, packets dropped via the
  // checkIngressEgressAdmission path vanish silently from per-port drop
  // telemetry.
  auto const& dropEvents = f.sw->packetDropStats_;
  ASSERT_EQ(dropEvents.size(), 1u)
      << "Expected exactly one drop event for the failed admission";
  auto const& dropEvent = dropEvents[0];
  EXPECT_EQ(
      dropEvent.dropReason(),
      packsim::switches::thrift::PacketDropReason::BUFFER_FULL);
  EXPECT_EQ(dropEvent.packetType(), "UDP");
  EXPECT_EQ(dropEvent.droppedPkts(), 1U);
  EXPECT_EQ(dropEvent.droppedBytes(), f.packet->getSize());
  EXPECT_EQ(dropEvent.portNum(), f.kInPort)
      << "Drop must be attributed to the ingress port the packet arrived on";
}

TEST_F(FbSwitchTest, TestEnableFatalOnDropIncludesBufStateDiagnostics) {
  // When enableFatalOnDrop is true and egress rejects a packet, the throw
  // must include buffer state diagnostics so that drop crashes are actionable.
  EgressRejectFixture f(
      simContext_.get(),
      switchName_,
      flexDeviceId_,
      /* fatalOnDrop */ true,
      /* useIngress */ true);

  EXPECT_THAT(
      [&] {
        f.sw->checkIngressEgressAdmission(
            f.inEthPortPtr, f.outEthPortPtr, f.packet);
      },
      ThrowsMessage<std::runtime_error>(AllOf(
          HasSubstr("egress"),
          HasSubstr("shared="),
          HasSubstr(fmt::format("inPort={}", f.kInPort)),
          HasSubstr(fmt::format("outPort={}", f.kOutPort)))))
      << "Fatal-on-drop must include buffer state diagnostics";
}

TEST_F(FbSwitchTest, ReceiveDropNotifiesSchedulerObserver) {
  auto const lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  auto const topology = FbSwitchTestHelpers::createMinimalTopology();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, &topology};
  auto testSwitch = std::make_unique<TestFbSwitch>(params, &switchConfig_);

  std::unique_ptr<packsim::EthernetPort> mockPort =
      std::make_unique<MockEthernetPort>(simContext_.get());
  auto* mockPortPtr = mockPort.get();
  packsim::FlexDeviceId const partnerDeviceId({7, 8, 9});
  testSwitch->addPort(mockPort.get(), partnerDeviceId, 0, 0);
  testSwitch->adoptPort(mockPort);

  // deviceName/pkt are valid only during the callback; copy the fields out.
  std::optional<std::string> gotDevice;
  std::optional<packsim::FlexDeviceId> gotDeviceId;
  std::optional<uint32_t> gotPort;
  std::optional<packsim::switches::thrift::PacketDropReason> gotReason;
  std::optional<uint32_t> gotQueueDepth;
  scheduler_->setOnPacketDroppedCallback([&](const packsim::DropEvent& e) {
    gotDevice = std::string(e.deviceName);
    gotDeviceId = e.deviceId;
    gotPort = e.port;
    gotReason = e.reason;
    gotQueueDepth = e.queueDepthBytes;
  });

  // Receive-path drops skip buffer accounting (no wredDrop).
  auto pkt = packsim::createPacket<packsim::PacketType::IPV6>(1000);
  testSwitch->onPacketDroppedOnReceive(
      *mockPortPtr,
      pkt,
      /*queue=*/0,
      packsim::switches::thrift::PacketDropReason::LINK_ERROR);

  ASSERT_TRUE(gotDevice.has_value());
  EXPECT_EQ(*gotDevice, switchName_);
  EXPECT_EQ(*gotDeviceId, flexDeviceId_);
  EXPECT_EQ(*gotPort, mockPortPtr->getPortNum());
  EXPECT_EQ(
      *gotReason, packsim::switches::thrift::PacketDropReason::LINK_ERROR);
  EXPECT_EQ(*gotQueueDepth, mockPortPtr->getTotalQueueLength());
}

TEST_F(FbSwitchTest, SendDropNotifiesSchedulerObserver) {
  auto const lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};
  auto testSwitch = std::make_unique<TestFbSwitch>(params, &switchConfig_);

  std::unique_ptr<packsim::EthernetPort> mockPort =
      std::make_unique<MockEthernetPort>(simContext_.get());
  auto* mockPortPtr = mockPort.get();
  mockPortPtr->setPortNum(0);

  // Send-path drops run through wredDrop, which removes buffer cells, so the
  // packet must first be admitted into the ingress buffer with matching keys.
  packsim::FlexDeviceId const sourceId({1, 2, 3});
  packsim::FlexDeviceId const destinationId({4, 5, 6});
  packsim::IPConnection const connection{sourceId, destinationId};
  auto packet = FbSwitchTestHelpers::createTestPacketWithConnection(
      packsim::PacketType::UDP, connection, /*portNum=*/1);
  packet->tag.pg = 2;

  constexpr uint8_t queue = 2;
  uint16_t const inPort = packet->tag.port;
  uint8_t const inQueue = packet->tag.pg;
  uint32_t const expectedBytes = packet->getSize();
  auto const bufIdx = testSwitch->getBuff(inPort, mockPortPtr->getPortNum());
  testSwitch->ingressBufs_.at(bufIdx).admitPacket(
      inPort, inQueue, expectedBytes);

  // deviceName/pkt are valid only during the callback; copy the fields out.
  std::optional<std::string> gotDevice;
  std::optional<packsim::FlexDeviceId> gotDeviceId;
  std::optional<uint32_t> gotPort;
  std::optional<packsim::switches::thrift::PacketDropReason> gotReason;
  std::optional<uint32_t> gotQueueDepth;
  scheduler_->setOnPacketDroppedCallback([&](const packsim::DropEvent& e) {
    gotDevice = std::string(e.deviceName);
    gotDeviceId = e.deviceId;
    gotPort = e.port;
    gotReason = e.reason;
    gotQueueDepth = e.queueDepthBytes;
  });

  testSwitch->onPacketDroppedOnSend(
      *mockPortPtr,
      packet,
      queue,
      packsim::switches::thrift::PacketDropReason::BUFFER_FULL);

  ASSERT_TRUE(gotDevice.has_value());
  EXPECT_EQ(*gotDevice, switchName_);
  EXPECT_EQ(*gotDeviceId, flexDeviceId_);
  EXPECT_EQ(*gotPort, mockPortPtr->getPortNum());
  EXPECT_EQ(
      *gotReason, packsim::switches::thrift::PacketDropReason::BUFFER_FULL);
  EXPECT_EQ(*gotQueueDepth, mockPortPtr->getTotalQueueLength());
}

// The credited receive buffer is per port, not per switch:
// `getLosslessRxCreditBytes` must answer for the port it is given, or a
// lossless VC's whole credit pool is derived from an unrelated port's headroom.
TEST_F(FbSwitchTest, LosslessRxCreditBytesAreReportedPerPort) {
  constexpr uint16_t kPortA = 0;
  constexpr uint16_t kPortB = 1;
  constexpr uint32_t kHdrmCellsA = 100;
  constexpr uint32_t kHdrmCellsB = 250;

  // The fixture's ports are all lossy with zero headroom; give two of them
  // different lossless headroom so a port mix-up cannot go unnoticed.
  for (auto& buf : *switchConfig_.bufs()) {
    for (auto& [portNum, portCfg] : *buf.ports()) {
      if (!*portCfg.isIngrPort() || portCfg.pgs()->empty()) {
        continue;
      }
      if (portNum == kPortA || portNum == kPortB) {
        auto& pg = portCfg.pgs()->at(0);
        pg.lossless() = true;
        pg.maxHdrmCells() = portNum == kPortA ? kHdrmCellsA : kHdrmCellsB;
      }
    }
  }

  packsim::thrift::LpmTableSpec const lpmTable =
      FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};
  auto testSwitch = std::make_unique<TestFbSwitch>(params, &switchConfig_);

  const uint64_t cellSizeBytes = FbSwitchTestHelpers::kDefaultCellSizeBytes;
  EXPECT_EQ(
      kHdrmCellsA * cellSizeBytes,
      testSwitch->getLosslessRxCreditBytes(kPortA));
  EXPECT_EQ(
      kHdrmCellsB * cellSizeBytes,
      testSwitch->getLosslessRxCreditBytes(kPortB));
  EXPECT_NE(
      testSwitch->getLosslessRxCreditBytes(kPortA),
      testSwitch->getLosslessRxCreditBytes(kPortB))
      << "the two ports must be distinguishable, or this test cannot catch a "
         "caller that passes the wrong port number";
}

TEST_F(FbSwitchTest, CreditedHeadroomCannotExceedTheSharedPool) {
  constexpr uint32_t kPort = 0;
  constexpr uint32_t kPoolCells = 300;
  // Two lossless PGs each asking for the whole pool.
  constexpr uint32_t kPerPgHdrmCells = kPoolCells;

  for (auto& buf : *switchConfig_.bufs()) {
    buf.headroomPoolCells() = kPoolCells;
    auto it = buf.ports()->find(kPort);
    ASSERT_NE(it, buf.ports()->end());
    ASSERT_GE(it->second.pgs()->size(), 2u);
    for (size_t i = 0; i < 2; ++i) {
      auto& pg = it->second.pgs()->at(i);
      pg.lossless() = true;
      pg.maxHdrmCells() = kPerPgHdrmCells;
    }
  }

  packsim::thrift::LpmTableSpec const lpmTable =
      FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};
  auto testSwitch = std::make_unique<TestFbSwitch>(params, &switchConfig_);

  EXPECT_EQ(
      kPoolCells * FbSwitchTestHelpers::kDefaultCellSizeBytes,
      testSwitch->getLosslessRxCreditBytes(kPort))
      << "the two PGs' headroom was summed past the pool they share";
}

// A packet counted at reception but discarded on the send path never drains,
// so `onPacketDroppedOnSend` is the only place its credit can be freed. The
// CC_Update reconcile of UEC §5.2.5.7.4 cannot recover it, because R_VC_CC
// already counted the packet and so the comparison finds no lost credits.
TEST_F(FbSwitchTest, PacketDroppedOnSendFreesTheCbfcCredit) {
  packsim::thrift::LpmTableSpec const lpmTable =
      FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};
  auto testSwitch = std::make_unique<TestFbSwitch>(params, &switchConfig_);

  // Two real ports so the ingress lookup is exercised: addSwitchPort assigns
  // "test_group" ports in order, and registers each port in portToEthPort_.
  auto ingressEthPort = std::make_unique<MockEthernetPort>(simContext_.get());
  auto egressEthPort = std::make_unique<MockEthernetPort>(simContext_.get());
  auto* ingressPtr = ingressEthPort.get();
  auto* egressPtr = egressEthPort.get();
  testSwitch->addSwitchPort(
      ingressPtr, "test_group", packsim::FlexDeviceId({9, 9, 1}), 1);
  testSwitch->addSwitchPort(
      egressPtr, "test_group", packsim::FlexDeviceId({9, 9, 2}), 2);

  // enableCbfc stands up the link-layer scheduler, which requires a channel.
  packsim::thrift::FlexLinkDelayConfig delayCfg;
  delayCfg.delayNs() = 0;
  packsim::EthernetChannel channel(delayCfg, 0, 1.0);
  ingressPtr->attach(&channel);
  egressPtr->attach(&channel);

  // CBFC on the ingress port: DSCP CS3 is the one lossless VC.
  packsim::thrift::CbfcConfig cbfcCfg;
  cbfcCfg.numVcs() = 2U;
  cbfcCfg.isLosslessPerVc() = {false, true};
  packsim::thrift::VcMapEntry entry;
  entry.selector() = packsim::thrift::VcMapSelector::DSCP;
  entry.mask() = 0x3FU;
  entry.value() = 0x18U; // DSCP CS3 -> VC1
  entry.vcId() = 1U;
  cbfcCfg.vcMap() = {entry};
  cbfcCfg.expectedCreditSizeBytes() = 64U;
  ingressPtr->enableCbfc(cbfcCfg);
  auto* cbfc = ingressPtr->cbfc();
  ASSERT_NE(cbfc, nullptr);

  packsim::FlexDeviceId const sourceId({1, 2, 3});
  packsim::FlexDeviceId const destinationId({4, 5, 6});
  packsim::IPConnection const connection{sourceId, destinationId};
  auto packet = FbSwitchTestHelpers::createTestPacketWithConnection(
      packsim::PacketType::UDP, connection, ingressPtr->getPortNum());
  packet.as<packsim::UdpPacket>()->setDscpFromCos(3); // CS3 -> lossless VC1
  packet->tag.pg = 1;
  ASSERT_TRUE(cbfc->isLosslessPacket(*packet))
      << "test packet must land on the credit-gated VC or this proves nothing";

  // Reception counts the credit, exactly as FbSwitch::onPacketRcvd does.
  cbfc->onPacketStored(*packet);

  // `FbSwitch::onPacketRcvd` fills `heldCredits` through `heldCreditsOut`; the
  // raw buffer call below does not, so set it here or the drop has nothing to
  // return.
  constexpr uint16_t kHeld = 2;
  packet->heldCredits = kHeld;
  const uint64_t freedBefore = cbfc->stats().creditsFreed;

  // wredDrop expects the packet to be buffered first.
  const uint16_t inPort = packet->tag.port;
  const auto bufIdx = testSwitch->getBuff(inPort, egressPtr->getPortNum());
  testSwitch->ingressBufs_.at(bufIdx).admitPacket(
      inPort, packet->tag.pg, packet->getSize());

  testSwitch->onPacketDroppedOnSend(
      *egressPtr,
      packet,
      /* queue= */ 1,
      packsim::switches::thrift::PacketDropReason::LINK_ERROR);

  EXPECT_EQ(freedBefore + kHeld, cbfc->stats().creditsFreed)
      << "send-path drop must return exactly the credits the buffer held";
}

// `heldCredits` is per hop, and the same packet object travels on to the next
// switch: a switch that does not model an ingress buffer never overwrites the
// field, so a stale count would be read back on its drain path. See the clear
// in `FbSwitch::onPacketSent`. That clear is unconditional, which is why no
// credit-based flow control is configured here.
TEST_F(FbSwitchTest, DepartureClearsTheHeldCreditCount) {
  packsim::thrift::LpmTableSpec const lpmTable =
      FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};
  auto testSwitch = std::make_unique<TestFbSwitch>(params, &switchConfig_);

  auto ingressEthPort = std::make_unique<MockEthernetPort>(simContext_.get());
  auto egressEthPort = std::make_unique<MockEthernetPort>(simContext_.get());
  auto* ingressPtr = ingressEthPort.get();
  auto* egressPtr = egressEthPort.get();
  testSwitch->addSwitchPort(
      ingressPtr, "test_group", packsim::FlexDeviceId({9, 9, 1}), 1);
  testSwitch->addSwitchPort(
      egressPtr, "test_group", packsim::FlexDeviceId({9, 9, 2}), 2);

  packsim::FlexDeviceId const sourceId({1, 2, 3});
  packsim::FlexDeviceId const destinationId({4, 5, 6});
  packsim::IPConnection const connection{sourceId, destinationId};
  auto packet = FbSwitchTestHelpers::createTestPacketWithConnection(
      packsim::PacketType::UDP, connection, ingressPtr->getPortNum());
  packet->tag.pg = 1;
  constexpr uint16_t kHeld = 2;
  packet->heldCredits = kHeld;

  // The drain path removes the packet from the ingress buffer, so it has to be
  // in there first.
  const uint16_t inPort = packet->tag.port;
  const auto bufIdx = testSwitch->getBuff(inPort, egressPtr->getPortNum());
  testSwitch->ingressBufs_.at(bufIdx).admitPacket(
      inPort, packet->tag.pg, packet->getSize());

  testSwitch->onPacketSent(*egressPtr, packet);

  EXPECT_EQ(0u, packet->heldCredits)
      << "a count left on a departing packet is read by the next switch as "
         "its own";
}

TEST_F(FbSwitchTest, UseEgressWithNoEgressBuffersIsRejected) {
  thrift::SwitchConfig config = FbSwitchTestHelpers::createTestSwitchConfig(2);
  config.useEgress() = true;
  auto const lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};

  EXPECT_THAT(
      [&]() { TestFbSwitch testSwitch(params, &config); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          fmt::format(
              "Switch {}: useEgress is set but egressBufs is empty",
              switchName_))));
}

// An egress-only switch models egressBufs alone, so it builds without any
// ingress buffer and takes each port's settings from the egress buffer
// singleMap names.
TEST_F(FbSwitchTest, EgressOnlySwitchTakesPortSettingsFromEgressBuffers) {
  thrift::SwitchConfig config =
      FbSwitchTestHelpers::createTwoBufferSwitchConfig(2);
  config.useIngress() = false;
  config.bufs()->clear();
  for (auto& egressBuf : *config.egressBufs()) {
    addQueuePerPriorityGroup(egressBuf);
  }
  config.egressBufs()->at(0).ports()->at(1).queueFilterCfg()->fieldToQueue() = {
      {kCs1Dscp, kRemappedQueue}};
  packsim::IPConnection const connection{
      packsim::FlexDeviceId({1, 2, 3}), packsim::FlexDeviceId({4, 5, 6})};

  TwoPortSwitch const f(
      simContext_.get(), switchName_, flexDeviceId_, std::move(config));

  EXPECT_EQ(
      f.outPort->getPktCos(makeCs1Packet(connection, f.inPort->getPortNum())),
      kRemappedQueue);
}

// With both directions on, a port that singleMap omits takes its settings from
// the lowest buffer a pairMap key with the port on its ingress side selects.
TEST_F(FbSwitchTest, PairMapOnlySwitchTakesPortSettingsFromItsIngressBuffer) {
  thrift::SwitchConfig config =
      FbSwitchTestHelpers::createTwoBufferSwitchConfig(2);
  config.portToBufMap()->singleMap()->clear();
  config.portToBufMap()->pairMap()["1.0"] = 1;
  config.portToBufMap()->pairMap()["1.1"] = 1;
  for (auto& buf : *config.bufs()) {
    addQueuePerPriorityGroup(buf);
  }
  config.bufs()->at(1).ports()->at(1).queueFilterCfg()->fieldToQueue() = {
      {kCs1Dscp, kRemappedQueue}};
  packsim::IPConnection const connection{
      packsim::FlexDeviceId({1, 2, 3}), packsim::FlexDeviceId({4, 5, 6})};

  TwoPortSwitch const f(
      simContext_.get(), switchName_, flexDeviceId_, std::move(config));

  EXPECT_EQ(
      f.outPort->getPktCos(makeCs1Packet(connection, f.inPort->getPortNum())),
      kRemappedQueue);
}

// Admission charges the egress queue the out port classifies a packet to, so
// departure must release that queue even when the in port's DSCP map put the
// packet in a different PG.
TEST_F(FbSwitchTest, DepartureReleasesTheEgressQueueAdmissionCharged) {
  thrift::SwitchConfig config =
      FbSwitchTestHelpers::createTwoBufferSwitchConfig(2);
  for (auto& buf : *config.bufs()) {
    addQueuePerPriorityGroup(buf);
  }
  config.bufs()->at(0).ports()->at(1).queueFilterCfg()->fieldToQueue() = {
      {kCs1Dscp, kRemappedQueue}};
  TwoPortSwitch const f(
      simContext_.get(), switchName_, flexDeviceId_, std::move(config));
  packsim::IPConnection const connection{
      packsim::FlexDeviceId({1, 2, 3}), packsim::FlexDeviceId({4, 5, 6})};
  auto packet = makeCs1Packet(connection, f.inPort->getPortNum());
  packet->tag.pg = f.inPort->getPktCos(packet);
  ASSERT_NE(packet->tag.pg, kRemappedQueue);
  uint16_t const outPortNum = f.outPort->getPortNum();
  auto const bufIdx = f.sw->getBuff(f.inPort->getPortNum(), outPortNum);
  ASSERT_TRUE(f.sw->checkIngressEgressAdmission(f.inPort, f.outPort, packet));
  ASSERT_GT(
      f.sw->egressBufs_.at(bufIdx)
          .queueStats(outPortNum, kRemappedQueue)
          .usedCells,
      0u);

  f.sw->onPacketSent(*f.outPort, packet);

  EXPECT_EQ(
      f.sw->egressBufs_.at(bufIdx)
          .queueStats(outPortNum, kRemappedQueue)
          .usedCells,
      0u);
}

// A port the switch adds marks a packet by the length of its VOQ_ECN queue.
// Both thresholds at 0 mark a packet however short that queue is.
TEST_F(FbSwitchTest, SwitchPortMarksAPacketSentFromAVoqEcnQueue) {
  thrift::SwitchConfig config = FbSwitchTestHelpers::createTestSwitchConfig(2);
  auto& wredEcnCfg = *config.bufs()
                          ->at(0)
                          .ports()
                          ->at(1)
                          .queueCfg()
                          ->emplace_back()
                          .wredEcnCfg();
  wredEcnCfg.queueProfileCfg() = thrift::QueueProfileType::VOQ_ECN;
  wredEcnCfg.minQSizeECN() = 0U;
  wredEcnCfg.maxQSizeECN() = 0U;
  TwoPortSwitch const f(
      simContext_.get(), switchName_, flexDeviceId_, std::move(config));
  packsim::LambdaPortObserver peerObserver;
  packsim::EthernetPort peer(simContext_.get(), "100Gbps");
  peer.setObserver(&peerObserver);
  thrift::FlexLinkDelayConfig delayCfg;
  delayCfg.delayNs() = 0;
  packsim::EthernetChannel channel(
      delayCfg, /*flapDelayNs=*/0, /*log10Ber=*/1.0);
  f.outPort->attach(&channel);
  peer.attach(&channel);
  packsim::IPConnection const connection{
      packsim::FlexDeviceId({1, 2, 3}), packsim::FlexDeviceId({4, 5, 6})};
  uint16_t const inPortNum = f.inPort->getPortNum();
  auto packet = FbSwitchTestHelpers::createTestPacketWithConnection(
      packsim::PacketType::UDP, connection, inPortNum);
  // The switch releases a sent packet from its ingress buffer, so the packet
  // has to be in there first.
  f.sw->ingressBufs_.at(f.sw->getBuff(inPortNum, f.outPort->getPortNum()))
      .admitPacket(inPortNum, packet->tag.pg, packet->getSize());

  f.outPort->send(packet);
  scheduler_->run();

  EXPECT_EQ(f.outPort->getPortStats(/*cos=*/0).ecnCeMarkedPkts, 1u);
}

// An egress-only switch holds no ingress buffer, so an egress rejection has
// nothing to roll back there and is recorded as one BUFFER_FULL drop.
TEST_F(FbSwitchTest, EgressOnlyRejectionIsRecordedAsADrop) {
  EgressRejectFixture f(
      simContext_.get(),
      switchName_,
      flexDeviceId_,
      /* fatalOnDrop */ false,
      /* useIngress */ false);

  bool const admitted = f.sw->checkIngressEgressAdmission(
      f.inEthPortPtr, f.outEthPortPtr, f.packet);

  EXPECT_FALSE(admitted);
  ASSERT_EQ(f.sw->packetDropStats_.size(), 1u);
  EXPECT_EQ(
      f.sw->packetDropStats_.at(0).dropReason(),
      packsim::switches::thrift::PacketDropReason::BUFFER_FULL);
}

// Headroom credit is a property of the ingress buffer, so a switch that does
// not model one credits nothing, whatever its ingress buffer config lists.
TEST_F(FbSwitchTest, SwitchWithoutIngressBufferingCreditsNoHeadroom) {
  constexpr uint16_t kPort = 0;
  thrift::SwitchConfig config =
      FbSwitchTestHelpers::createTwoBufferSwitchConfig(2);
  config.useIngress() = false;
  auto& pg = config.bufs()->at(0).ports()->at(kPort).pgs()->at(0);
  pg.lossless() = true;
  pg.maxHdrmCells() = 100;
  auto const lpmTable = FbSwitchTestHelpers::createMinimalLpmTable();
  packsim::FlexNodeParams const params{
      simContext_.get(), switchName_, flexDeviceId_, &lpmTable, nullptr};
  TestFbSwitch const testSwitch(params, &config);

  EXPECT_EQ(testSwitch.getLosslessRxCreditBytes(kPort), 0u);
}

// The default DSCP map selects queues up to 7, so a port with eight queues
// needs eight PGs in every ingress buffer that serves it.
TEST_F(FbSwitchTest, IngressBufferWithTooFewPgsForThePortsFilterIsRejected) {
  thrift::SwitchConfig config = FbSwitchTestHelpers::createTestSwitchConfig(2);
  addQueuePerPriorityGroup(config.bufs()->at(0));
  config.bufs()->at(0).ports()->at(1).pgs()->resize(kFewerQueues);

  EXPECT_THAT(
      [&]() {
        TwoPortSwitch const f(
            simContext_.get(), switchName_, flexDeviceId_, std::move(config));
      },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          fmt::format(
              "Switch {}: port 1's queueFilterCfg can select queue 7 for "
              "ingress traffic, so SwitchConfig.bufs[0] needs at least 8 pgs "
              "for the port but has {}",
              switchName_,
              kFewerQueues))));
}

// Egress admission charges the queue the filter selects before the port looks
// it up, so the egress buffer needs a queue for each one the default DSCP map
// selects, including those past the port's own four.
TEST_F(
    FbSwitchTest,
    EgressBufferWithTooFewUcQueuesForThePortsFilterIsRejected) {
  thrift::SwitchConfig config =
      FbSwitchTestHelpers::createTwoBufferSwitchConfig(2);
  config.bufs()->at(0).ports()->at(1).queueCfg()->resize(kFewerQueues);
  config.egressBufs()->at(1).ports()->at(1).ucQueues()->resize(kFewerQueues);

  EXPECT_THAT(
      [&]() {
        TwoPortSwitch const f(
            simContext_.get(), switchName_, flexDeviceId_, std::move(config));
      },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          fmt::format(
              "Switch {}: port 1's queueFilterCfg can select queue 7 for "
              "egress traffic, so SwitchConfig.egressBufs[1] needs at least 8 "
              "ucQueues for the port but has {}",
              switchName_,
              kFewerQueues))));
}

// A single-queue port puts every packet in queue 0, whatever its DSCP map, so
// each egress buffer that serves it needs only one queue for it.
TEST_F(FbSwitchTest, SingleQueuePortNeedsOnlyOneEgressQueue) {
  thrift::SwitchConfig config =
      FbSwitchTestHelpers::createTwoBufferSwitchConfig(2);
  for (auto& egressBuf : *config.egressBufs()) {
    egressBuf.ports()->at(1).ucQueues()->resize(1);
  }

  EXPECT_NO_THROW({
    TwoPortSwitch const f(
        simContext_.get(), switchName_, flexDeviceId_, std::move(config));
  });
}

// An entry that does not serve a direction configures no queues there, so a
// port may be listed without pgs in an ingress buffer that does not take its
// ingress traffic.
TEST_F(FbSwitchTest, IngressBufferEntryThatDoesNotServeThePortNeedsNoPgs) {
  thrift::SwitchConfig config =
      FbSwitchTestHelpers::createTwoBufferSwitchConfig(2);
  auto& portCfg = config.bufs()->at(1).ports()->at(1);
  portCfg.isIngrPort() = false;
  portCfg.pgs()->clear();

  EXPECT_NO_THROW({
    TwoPortSwitch const f(
        simContext_.get(), switchName_, flexDeviceId_, std::move(config));
  });
}

// A port rejects a received packet for a queue it lacks before ingress
// admission, so the queues the default DSCP map selects past the port's own
// four need no PG.
TEST_F(FbSwitchTest, IngressBufferNeedsPgsOnlyForQueuesThePortHas) {
  thrift::SwitchConfig config = FbSwitchTestHelpers::createTestSwitchConfig(2);
  auto& portCfg = config.bufs()->at(0).ports()->at(1);
  portCfg.queueCfg()->resize(kFewerQueues);
  portCfg.pgs()->resize(kFewerQueues);

  EXPECT_NO_THROW({
    TwoPortSwitch const f(
        simContext_.get(), switchName_, flexDeviceId_, std::move(config));
  });
}

// A DSCP map that selects no queue past `kRemappedQueue` needs a PG only for
// each queue up to it, however many queues the port has.
TEST_F(FbSwitchTest, IngressBufferNeedsPgsOnlyForQueuesTheFilterSelects) {
  thrift::SwitchConfig config = FbSwitchTestHelpers::createTestSwitchConfig(2);
  auto& portCfg = config.bufs()->at(0).ports()->at(1);
  portCfg.queueCfg()->resize(FbSwitchTestHelpers::kDefaultNumPriorityGroups);
  portCfg.queueFilterCfg()->fieldToQueue() = {{kCs1Dscp, kRemappedQueue}};
  portCfg.pgs()->resize(kRemappedQueue + 1);

  EXPECT_NO_THROW({
    TwoPortSwitch const f(
        simContext_.get(), switchName_, flexDeviceId_, std::move(config));
  });
}

} // namespace packsim::fbswitch::test
