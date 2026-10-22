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
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/node/flex-node.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

using ::testing::ThrowsMessage;

namespace packsim {

namespace {

thrift::LpmTableSpec createLpmTable(
    std::vector<thrift::LinkId> links,
    thrift::TiebreakSpec tiebreak) {
  thrift::LpmTableSpec lpmTable;

  thrift::LpmTableEntry defaultEntry;
  defaultEntry.links() = std::move(links);
  defaultEntry.tiebreak() = std::move(tiebreak);

  lpmTable.defaultRoute() = std::move(defaultEntry);
  lpmTable.validPrefixLengths() = std::vector<uint32_t>{};
  lpmTable.prefixTable().ensure();

  return lpmTable;
}

thrift::LpmTableSpec createLpmTableWithLinks(
    std::vector<thrift::LinkId> links) {
  thrift::TiebreakSpec tiebreak;
  tiebreak.ecmp() = thrift::EcmpTiebreakSpec{};
  return createLpmTable(std::move(links), std::move(tiebreak));
}

thrift::LpmTableSpec createLpmTableWithPathSpray(
    std::vector<thrift::LinkId> links) {
  thrift::TiebreakSpec tiebreak;
  tiebreak.pathspray() = thrift::PathSprayTiebreakSpec{};
  return createLpmTable(std::move(links), std::move(tiebreak));
}

thrift::LpmTableSpec createLpmTableWithDpr(
    std::vector<thrift::LinkId> links,
    std::vector<uint32_t> dotProduct,
    uint32_t divScale) {
  thrift::TiebreakSpec tiebreak;
  thrift::DprTiebreakSpec dprSpec;
  dprSpec.dotProduct() = std::move(dotProduct);
  dprSpec.divScale() = divScale;
  tiebreak.dpr() = std::move(dprSpec);
  return createLpmTable(std::move(links), std::move(tiebreak));
}

thrift::LpmTableSpec createLpmTableWithFly(std::vector<thrift::LinkId> links) {
  thrift::TiebreakSpec tiebreak;
  thrift::DragonFlyPlusTiebreakSpec flySpec;
  flySpec.maxAdditionalHops() = 0;
  flySpec.secondTieBreak() = "ECMP";
  tiebreak.fly() = std::move(flySpec);
  return createLpmTable(std::move(links), std::move(tiebreak));
}

thrift::FlexTopology createMinimalTopology() {
  thrift::FlexTopology topology;
  return topology;
}

// Build a test packet of the given type with its IPConnection wired up.
// Defaults to UDP; pass PacketType::IPV6 for a non-UDP IPv6 packet. `conn` is
// declared on the Ipv6Packet base, so a single base cast sets it for any
// IPv6-derived type.
template <packsim::PacketType PktType = packsim::PacketType::UDP>
packsim::PacketPtr createTestPacket(
    packsim::FlexDeviceId const& sourceId,
    packsim::FlexDeviceId const& destinationId,
    std::shared_ptr<packsim::IPConnection>& connectionPtr) {
  auto packet = packsim::createPacket<PktType>(100);

  auto connection =
      std::make_unique<packsim::IPConnection>(sourceId, destinationId);
  packet.template as<packsim::Ipv6Packet>()->conn = connection.get();

  // Transfer ownership to shared_ptr to keep connection alive
  connectionPtr = std::shared_ptr<packsim::IPConnection>(connection.release());

  return packet;
}

} // namespace

// Test wrapper class to expose protected methods
class TestableFlexNode : public packsim::FlexNode {
 public:
  using packsim::FlexNode::findOrAddRoute;
  using packsim::FlexNode::FlexNode;
  using packsim::FlexNode::getNumCachedRoutes;
  using packsim::FlexNode::kMaxCachedRoutes;
  using packsim::FlexNode::routePacket;
};

class FlexNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    scheduler_ = std::make_unique<packsim::Scheduler>();
    simContext_ = std::make_unique<packsim::SimContext>(scheduler_.get(), 0);
  }

  // Wire an "up" link on `node`: attach a channel (owned by the fixture) and
  // mark it UP so routePacket's checkLinkStatus accepts the selected link.
  // Returns the local port so a test can assert routePacket's selection.
  packsim::EthernetPort* addWorkingLink(
      TestableFlexNode& node,
      packsim::FlexDeviceId const& partnerId,
      thrift::LinkId linkId) {
    auto ethPort =
        std::make_unique<packsim::EthernetPort>(simContext_.get(), "100Gbps");
    auto* ethPortPtr = ethPort.get();
    node.adoptPort(ethPort);
    node.addPort(ethPortPtr, partnerId, linkId);

    auto channel = std::make_unique<packsim::EthernetChannel>(0);
    ethPortPtr->attach(channel.get());
    auto peerPort =
        std::make_unique<packsim::EthernetPort>(simContext_.get(), "100Gbps");
    peerPort->attach(channel.get());
    ethPortPtr->setChannelState(packsim::ChannelState::UP);

    channels_.emplace_back(std::move(channel));
    peerPorts_.emplace_back(std::move(peerPort));
    return ethPortPtr;
  }

  std::unique_ptr<packsim::Scheduler> scheduler_;
  std::unique_ptr<packsim::SimContext> simContext_;
  std::vector<std::unique_ptr<packsim::EthernetChannel>> channels_;
  std::vector<std::unique_ptr<packsim::EthernetPort>> peerPorts_;
};

TEST_F(FlexNodeTest, TestRoutePacketThrowsExceptionNoLink) {
  auto const lpmTable = createLpmTableWithLinks({});
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};

  TestableFlexNode testNode(params);

  packsim::FlexDeviceId const destinationId({2, 0, 5});
  std::shared_ptr<packsim::IPConnection> connectionPtr;
  auto packet = createTestPacket(nodeId, destinationId, connectionPtr);

  ASSERT_THAT(
      [&]() { testNode.routePacket(packet, std::nullopt); },
      ThrowsMessage<packsim::thrift::PackSimRoutingException>(
          "Found no links to use on test-node (1:0:3::) for destination 2:0:5::"));
}

TEST_F(FlexNodeTest, TestRoutesPacketWithSingleLink) {
  auto const lpmTable = createLpmTableWithLinks({0});
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};

  TestableFlexNode testNode(params);

  std::unique_ptr<packsim::EthernetPort> ethPort =
      std::make_unique<packsim::EthernetPort>(simContext_.get(), "100Gbps");
  auto* ethPortPtr = ethPort.get();
  testNode.adoptPort(ethPort);

  packsim::FlexDeviceId const partnerId({2, 0, 5});
  testNode.addPort(ethPortPtr, partnerId, 0);

  packsim::FlexDeviceId const destinationId({2, 0, 5});
  std::shared_ptr<packsim::IPConnection> connectionPtr;
  auto packet = createTestPacket(nodeId, destinationId, connectionPtr);

  auto* result = testNode.routePacket(packet, std::nullopt);
  ASSERT_EQ(result, ethPortPtr);
}

TEST_F(FlexNodeTest, TestRoutePacketThrowsExceptionNoWorkingLink) {
  auto const lpmTable = createLpmTableWithLinks({0, 1, 2});
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};

  TestableFlexNode testNode(params);

  std::vector<packsim::EthernetPort*> ethPortPtrs;
  std::vector<std::unique_ptr<packsim::EthernetChannel>> channels;

  thrift::LinkId linkId = 0;
  for (uint32_t i = 0; i < 3; ++i) {
    std::unique_ptr<packsim::EthernetPort> ethPort =
        std::make_unique<packsim::EthernetPort>(simContext_.get(), "100Gbps");
    auto* ethPortPtr = ethPort.get();
    testNode.adoptPort(ethPort);

    packsim::FlexDeviceId const partnerId({2, 0, 5 + i});
    testNode.addPort(ethPortPtr, partnerId, linkId++);

    auto channel = std::make_unique<packsim::EthernetChannel>(0);
    ethPortPtr->attach(channel.get());

    auto peerPort =
        std::make_unique<packsim::EthernetPort>(simContext_.get(), "100Gbps");
    peerPort->attach(channel.get());

    ethPortPtr->setChannelState(packsim::ChannelState::DOWN);

    ethPortPtrs.emplace_back(ethPortPtr);
    channels.emplace_back(std::move(channel));
  }

  packsim::FlexDeviceId const destinationId({2, 0, 5});
  std::shared_ptr<packsim::IPConnection> connectionPtr;
  auto packet = createTestPacket(nodeId, destinationId, connectionPtr);

  ASSERT_THAT(
      [&]() { testNode.routePacket(packet, std::nullopt); },
      ThrowsMessage<packsim::thrift::PackSimRoutingException>(
          "None of the links attached to node test-node are working for destination 2:0:5::"));
}

TEST_F(FlexNodeTest, TestInspectionAccessorsReflectWiredPorts) {
  // Verify each inspection accessor returns the state populated by the
  // constructor and addPort.
  auto const lpmTable = createLpmTableWithLinks({0});
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};
  TestableFlexNode testNode(params);

  EXPECT_EQ(testNode.getLpmTable(), &lpmTable);

  std::unique_ptr<packsim::EthernetPort> ethPort =
      std::make_unique<packsim::EthernetPort>(simContext_.get(), "100Gbps");
  auto* ethPortPtr = ethPort.get();
  testNode.adoptPort(ethPort);

  packsim::FlexDeviceId const partnerId({2, 0, 5});
  constexpr uint16_t kPort = 0;
  constexpr thrift::LinkId kLink = 7;
  testNode.addPort(ethPortPtr, partnerId, kLink, kPort);

  ASSERT_EQ(testNode.getPortToEthPort().size(), 1u);
  EXPECT_EQ(testNode.getPortToEthPort().at(kPort), ethPortPtr);

  ASSERT_EQ(testNode.getPortToLink().size(), 1u);
  EXPECT_EQ(testNode.getPortToLink().at(kPort), kLink);

  ASSERT_EQ(testNode.getPortToPartnerFlexId().size(), 1u);
  EXPECT_EQ(
      testNode.getPortToPartnerFlexId().at(kPort).asVec(), partnerId.asVec());
}

TEST_F(FlexNodeTest, TestInspectionAccessorsEmptyBeforeWiring) {
  // A node with no ports added exposes empty port maps but still its LPM spec.
  auto const lpmTable = createLpmTableWithLinks({});
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};
  TestableFlexNode testNode(params);

  EXPECT_EQ(testNode.getLpmTable(), &lpmTable);
  EXPECT_TRUE(testNode.getPortToEthPort().empty());
  EXPECT_TRUE(testNode.getPortToLink().empty());
  EXPECT_TRUE(testNode.getPortToPartnerFlexId().empty());
}

TEST_F(FlexNodeTest, TestPathSprayRoutesNonUdpPacketToFirstLink) {
  // A non-UDP IPv6 packet (e.g. PING) has no srcPort, so the pathspray tiebreak
  // must fall back to the first link instead of casting to UdpPacket.
  auto const lpmTable = createLpmTableWithPathSpray({0, 1});
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};
  TestableFlexNode testNode(params);

  auto* link0Port =
      addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 5}), 0);
  addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 6}), 1);

  packsim::FlexDeviceId const destinationId({2, 0, 5});
  std::shared_ptr<packsim::IPConnection> connectionPtr;
  auto packet = createTestPacket<packsim::PacketType::IPV6>(
      nodeId, destinationId, connectionPtr);

  EXPECT_EQ(testNode.routePacket(packet, std::nullopt), link0Port);
}

TEST_F(FlexNodeTest, TestPathSprayRoutesUdpPacketBySrcPort) {
  // A UDP packet selects links.at(srcPort % numLinks); srcPort 3 over 2 links
  // picks link index 1.
  auto const lpmTable = createLpmTableWithPathSpray({0, 1});
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};
  TestableFlexNode testNode(params);

  addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 5}), 0);
  auto* link1Port =
      addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 6}), 1);

  packsim::FlexDeviceId const destinationId({2, 0, 5});
  std::shared_ptr<packsim::IPConnection> connectionPtr;
  auto packet = createTestPacket(nodeId, destinationId, connectionPtr);
  constexpr uint16_t kSrcPort = 3; // 3 % 2 == 1
  packet.as<packsim::UdpPacket>()->srcPort = kSrcPort;

  EXPECT_EQ(testNode.routePacket(packet, std::nullopt), link1Port);
}

TEST_F(FlexNodeTest, TestRoutesEachDestinationByItsOwnPrefixEntry) {
  // Two /3 prefix entries, one link each, over a default route with both
  // links: every packet must follow its own destination's entry, including
  // after the node has routed to another destination in between.
  auto lpmTable = createLpmTableWithLinks({0, 1});
  lpmTable.validPrefixLengths() = std::vector<uint32_t>{3};
  thrift::LpmTableEntry entryTo5;
  entryTo5.links() = std::vector<thrift::LinkId>{0};
  entryTo5.tiebreak()->ecmp() = thrift::EcmpTiebreakSpec{};
  thrift::LpmTableEntry entryTo6;
  entryTo6.links() = std::vector<thrift::LinkId>{1};
  entryTo6.tiebreak()->ecmp() = thrift::EcmpTiebreakSpec{};
  lpmTable.prefixTable()->emplace("2.0.5", std::move(entryTo5));
  lpmTable.prefixTable()->emplace("2.0.6", std::move(entryTo6));
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};
  TestableFlexNode testNode(params);
  auto* link0Port =
      addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 5}), 0);
  auto* link1Port =
      addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 6}), 1);

  std::shared_ptr<packsim::IPConnection> connectionTo5;
  auto packetTo5 =
      createTestPacket(nodeId, packsim::FlexDeviceId({2, 0, 5}), connectionTo5);
  std::shared_ptr<packsim::IPConnection> connectionTo6;
  auto packetTo6 =
      createTestPacket(nodeId, packsim::FlexDeviceId({2, 0, 6}), connectionTo6);

  EXPECT_EQ(testNode.routePacket(packetTo5, std::nullopt), link0Port);
  EXPECT_EQ(testNode.routePacket(packetTo6, std::nullopt), link1Port);
  EXPECT_EQ(testNode.routePacket(packetTo5, std::nullopt), link0Port);
}

TEST_F(FlexNodeTest, TestRouteCacheStaysBoundedPastItsCapacity) {
  // Route to one more distinct destination than the cache holds: the full
  // cache keeps its routes and the extra destination is routed uncached, still
  // by its own prefix entry.
  auto lpmTable = createLpmTableWithLinks({0, 1});
  lpmTable.validPrefixLengths() = std::vector<uint32_t>{3};
  thrift::LpmTableEntry entryTo5;
  entryTo5.links() = std::vector<thrift::LinkId>{0};
  entryTo5.tiebreak()->ecmp() = thrift::EcmpTiebreakSpec{};
  lpmTable.prefixTable()->emplace("2.0.5", std::move(entryTo5));
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};
  TestableFlexNode testNode(params);
  auto* link0Port =
      addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 5}), 0);
  addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 6}), 1);

  std::shared_ptr<packsim::IPConnection> connection;
  for (uint32_t i = 0; i < TestableFlexNode::kMaxCachedRoutes; ++i) {
    auto packet =
        createTestPacket(nodeId, packsim::FlexDeviceId({3, 0, i}), connection);
    testNode.routePacket(packet, std::nullopt);
  }
  ASSERT_EQ(testNode.getNumCachedRoutes(), TestableFlexNode::kMaxCachedRoutes);

  auto packetTo5 =
      createTestPacket(nodeId, packsim::FlexDeviceId({2, 0, 5}), connection);
  EXPECT_EQ(testNode.routePacket(packetTo5, std::nullopt), link0Port);
  EXPECT_EQ(testNode.getNumCachedRoutes(), TestableFlexNode::kMaxCachedRoutes);
}

TEST_F(FlexNodeTest, TestCachedRouteSurvivesLaterLookups) {
  // Lookups of 1000 new destinations grow the cache through several rehashes;
  // the route returned first must stay readable (ASAN reports a stale one).
  auto lpmTable = createLpmTableWithLinks({0, 1});
  lpmTable.validPrefixLengths() = std::vector<uint32_t>{3};
  thrift::LpmTableEntry entryTo5;
  entryTo5.links() = std::vector<thrift::LinkId>{0};
  entryTo5.tiebreak()->ecmp() = thrift::EcmpTiebreakSpec{};
  lpmTable.prefixTable()->emplace("2.0.5", std::move(entryTo5));
  auto const topology = createMinimalTopology();
  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};
  TestableFlexNode testNode(params);

  const auto* routeTo5 =
      testNode.findOrAddRoute(packsim::FlexDeviceId({2, 0, 5}));
  ASSERT_NE(routeTo5, nullptr);
  for (uint32_t i = 0; i < 1000; ++i) {
    testNode.findOrAddRoute(packsim::FlexDeviceId({3, 0, i}));
  }

  EXPECT_EQ(routeTo5->entry, &lpmTable.prefixTable()->at("2.0.5"));
}

TEST_F(FlexNodeTest, TestNodeWithoutPrefixesRoutesWithoutFillingTheCache) {
  // No prefix lengths, so every destination takes the default route: routing
  // to two destinations must not add a cached route for either.
  auto const lpmTable = createLpmTableWithLinks({0});
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};
  TestableFlexNode testNode(params);
  auto* link0Port =
      addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 5}), 0);

  std::shared_ptr<packsim::IPConnection> connectionTo5;
  auto packetTo5 =
      createTestPacket(nodeId, packsim::FlexDeviceId({2, 0, 5}), connectionTo5);
  std::shared_ptr<packsim::IPConnection> connectionTo6;
  auto packetTo6 =
      createTestPacket(nodeId, packsim::FlexDeviceId({2, 0, 6}), connectionTo6);

  EXPECT_EQ(testNode.routePacket(packetTo5, std::nullopt), link0Port);
  EXPECT_EQ(testNode.routePacket(packetTo6, std::nullopt), link0Port);
  EXPECT_EQ(testNode.getNumCachedRoutes(), 0);
}

TEST_F(FlexNodeTest, TestNodeWithEmptyLpmTableRoutesThroughTheCache) {
  // A node whose config sets no LPM table: no prefixes and a default route
  // without links. It routes as it did before the default-only route existed:
  // the destination's route enters the cache and routing reports no links.
  const thrift::LpmTableSpec lpmTable;
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};
  TestableFlexNode testNode(params);

  std::shared_ptr<packsim::IPConnection> connectionPtr;
  auto packet =
      createTestPacket(nodeId, packsim::FlexDeviceId({2, 0, 5}), connectionPtr);

  EXPECT_THAT(
      [&]() { testNode.routePacket(packet, std::nullopt); },
      ThrowsMessage<packsim::thrift::PackSimRoutingException>(
          "Found no links to use on test-node (1:0:3::) for destination 2:0:5::"));
  EXPECT_EQ(testNode.getNumCachedRoutes(), 1);
}

TEST_F(FlexNodeTest, TestDprTiebreakSelectsLinkByDotProduct) {
  // dprNum = dotProduct . dstId.values = (0,0,1).(2,0,5) = 5; 5 / 1 = 5;
  // 5 % 2 links == 1.
  auto const lpmTable = createLpmTableWithDpr(
      /*links=*/{0, 1}, /*dotProduct=*/{0, 0, 1}, /*divScale=*/1);
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};
  TestableFlexNode testNode(params);

  addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 5}), 0);
  auto* link1Port =
      addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 6}), 1);

  packsim::FlexDeviceId const destinationId({2, 0, 5});
  std::shared_ptr<packsim::IPConnection> connectionPtr;
  auto packet = createTestPacket(nodeId, destinationId, connectionPtr);

  EXPECT_EQ(testNode.routePacket(packet, std::nullopt), link1Port);
}

TEST_F(FlexNodeTest, TestDprTiebreakDotProductAbove32Bits) {
  // dprNum = (0,0,1'000'000'000).(2,0,5) = 5'000'000'000, which needs more than
  // 32 bits; 5'000'000'000 % 3 links == 2. A 32-bit sum wraps to 705'032'704,
  // which would select link 1.
  auto const lpmTable = createLpmTableWithDpr(
      /*links=*/{0, 1, 2},
      /*dotProduct=*/{0, 0, 1'000'000'000},
      /*divScale=*/1);
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};
  TestableFlexNode testNode(params);

  addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 5}), 0);
  addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 6}), 1);
  auto* link2Port =
      addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 7}), 2);

  packsim::FlexDeviceId const destinationId({2, 0, 5});
  std::shared_ptr<packsim::IPConnection> connectionPtr;
  auto packet = createTestPacket(nodeId, destinationId, connectionPtr);

  EXPECT_EQ(testNode.routePacket(packet, std::nullopt), link2Port);
}

TEST_F(FlexNodeTest, TestDprTiebreakWithZeroDivScaleIsRejected) {
  // routePacket divides the dot product by divScale.
  auto const lpmTable = createLpmTableWithDpr(
      /*links=*/{0, 1}, /*dotProduct=*/{0, 0, 1}, /*divScale=*/0);
  auto const topology = createMinimalTopology();
  packsim::FlexNodeParams const params{
      simContext_.get(),
      "test-node",
      packsim::FlexDeviceId({1, 0, 3}),
      &lpmTable,
      &topology};

  EXPECT_THAT(
      [&]() { TestableFlexNode testNode(params); },
      ThrowsMessage<std::invalid_argument>(
          "Node test-node: DprTiebreakSpec.divScale must be positive, got 0"));
}

TEST_F(FlexNodeTest, TestFlyTiebreakSelectsShortestPathByPodIndex) {
  // maxAdditionalHops=0 -> shortest-path ECMP: keep only links whose partner's
  // pod (flexId index 2) matches the destination's pod. Only link 0 (partner
  // pod 5) matches dst pod 5; link 1 (partner pod 9) is filtered out.
  auto const lpmTable = createLpmTableWithFly({0, 1});
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};
  TestableFlexNode testNode(params);

  auto* link0Port =
      addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 5}), 0);
  addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 9}), 1);

  packsim::FlexDeviceId const destinationId({7, 0, 5});
  std::shared_ptr<packsim::IPConnection> connectionPtr;
  auto packet = createTestPacket(nodeId, destinationId, connectionPtr);

  EXPECT_EQ(testNode.routePacket(packet, std::nullopt), link0Port);
}

TEST_F(FlexNodeTest, TestFlyTiebreakThrowsWhenNoLinkReachesTheDestinationPod) {
  // maxAdditionalHops=0 keeps only links whose partner is in the destination's
  // pod. Neither partner (pods 5 and 9) is in pod 7, so routing must report it
  // rather than pick from an empty set.
  auto const lpmTable = createLpmTableWithFly({0, 1});
  auto const topology = createMinimalTopology();

  packsim::FlexDeviceId const nodeId({1, 0, 3});
  packsim::FlexNodeParams const params{
      simContext_.get(), "test-node", nodeId, &lpmTable, &topology};
  TestableFlexNode testNode(params);

  addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 5}), 0);
  addWorkingLink(testNode, packsim::FlexDeviceId({2, 0, 9}), 1);

  packsim::FlexDeviceId const destinationId({7, 0, 7});
  std::shared_ptr<packsim::IPConnection> connectionPtr;
  auto packet = createTestPacket(nodeId, destinationId, connectionPtr);

  EXPECT_THAT(
      [&]() { testNode.routePacket(packet, std::nullopt); },
      ThrowsMessage<packsim::thrift::PackSimRoutingException>(
          "No fly route link on test-node (1:0:3::) reaches the pod of "
          "destination 7:0:7::"));
}

} // namespace packsim
