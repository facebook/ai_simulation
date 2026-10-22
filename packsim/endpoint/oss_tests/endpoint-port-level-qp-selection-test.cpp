/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Selection policy of `Endpoint::getPortLevelQPConnection`: which of a QPN's
// port-level connections a packet is pinned to, given a tiebreak spec. The
// sibling `endpoint-qpn-port-config-test` covers which connections exist; this
// suite covers which one is picked. The last case covers
// `Endpoint::routePacket`'s bypass of that selection for a packet whose
// transport already chose its port.

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/endpoint/endpoint.h"
#include "packsim/endpoint/oss_tests/endpoint-test-fixture.h"
#include "packsim/if/gen-cpp2/qp_pair_config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/node/flex-node.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/packet/roce.h"

namespace packsim {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace {

// 2 src ports x 3 dst ports, shared by the two ECMP ordering/wraparound cases.
constexpr std::array<uint16_t, 2> kSrcPorts = {0, 1};
constexpr std::array<uint16_t, 3> kDstPorts = {0, 1, 2};
constexpr uint16_t kNumConns = kSrcPorts.size() * kDstPorts.size();

// Largest value the 16-bit entropy field can carry.
constexpr uint32_t kMaxEntropy = 65535;

// Draw count for the spray case. `EndpointTestBase` seeds the SimContext RNG
// with a fixed value, so the draw sequence is deterministic at any count --
// this is not a sample, and no probability attaches to a given run. The count
// is large so that coverage is insensitive to *which* stream that is: an RNG
// or seed change hands the test a different deterministic sequence, and a
// sequence this long covers all four connections unless the generator is
// badly broken. A short count would be no less deterministic, just far easier
// to break by accident.
constexpr int kSprayDraws = 4096;

} // namespace

/**
 * A 4-port RoCEv2 endpoint wired to one switch, with reachability filtering
 * off so that every (srcPort, dstPort) pair in a QPN's port config becomes a
 * connection. That makes the connection vector's contents and order fully
 * determined by the port config, which is what the selection assertions below
 * index into.
 *
 * The scheduler / SimContext / RoCEv2 NicConfig arrange comes from
 * `EndpointTestBase`, whose SimContext is constructed with a fixed RNG seed,
 * so the spray draws below are repeatable run to run.
 */
class EndpointPortLevelQpSelectionTest : public EndpointTestBase {
 protected:
  static constexpr uint32_t kSrcQpn = 100;
  static constexpr uint32_t kDstQpn = 300;
  // Sole argument of the `EthernetChannel(uint64_t delayNs)` constructor. The
  // selection paths under test never advance simulated time, so any non-zero
  // propagation delay serves.
  static constexpr uint64_t kChannelDelayNs = 100;

  void SetUp() override {
    EndpointTestBase::SetUp();
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

    // Every port faces the same switch, so all four peer ports share this id
    // on purpose. Nothing here reads a peer port's own id: the dst port the
    // assertions check comes from the QPN's `dstPorts` config (or, for
    // `routePacket`, the packet's `dstNicPort`), never from the peer, so a
    // per-port peer id would add no discrimination.
    const FlexDeviceId switchId("2:0:1:0:0:0:0:0");
    for (uint16_t portNum = 1; portNum <= kNumPorts; ++portNum) {
      createPortWithSwitch(portNum, switchId);
    }

    ecmpTiebreak_.ecmp() = packsim::thrift::EcmpTiebreakSpec{};
    sprayTiebreak_.spray() = packsim::thrift::SprayTiebreakSpec{};
  }

  /**
   * Wire one endpoint port to a switch port over its own channel.
   *
   * @param portNum     1-based FlexDeviceId port id; also the link id.
   * @param switchId    Device id of the switch end of the link.
   */
  void createPortWithSwitch(uint16_t portNum, const FlexDeviceId& switchId) {
    FlexDeviceId portId = endpoint_->getFlexId();
    portId.values[kPortIndexInFlexDeviceId] = portNum;

    const std::string portSpeed = fmt::format("{}Gbps", kPortSpeedGbps);
    auto port = std::make_unique<EthernetPort>(&simCtx_, portSpeed, portId);
    auto switchPort =
        std::make_unique<EthernetPort>(&simCtx_, portSpeed, switchId);
    auto channel = std::make_shared<EthernetChannel>(kChannelDelayNs);

    port->attach(channel.get());
    switchPort->attach(channel.get());

    // One link id per port. `FlexNode::addPort` keys `linkToPort_` and
    // `linkToPartnerFlexId_` by link id, so reusing one id would leave only
    // the last port reachable through those maps. The selection paths under
    // test do not read them, but a fixture that misrepresents the wiring is a
    // trap for the next test added here.
    endpoint_->addPort(port.get(), switchId, /*linkId=*/portNum, portNum);

    ports_.emplace_back(std::move(port));
    switchPorts_.emplace_back(std::move(switchPort));
    channels_.emplace_back(std::move(channel));
  }

  /**
   * Install a port config for `kSrcQpn`.
   *
   * @param srcPorts    0-based source ports; 1-based in the resulting ids.
   * @param dstPorts    0-based destination ports, likewise.
   */
  void setQpnPorts(
      const std::vector<uint16_t>& srcPorts,
      const std::vector<uint16_t>& dstPorts) {
    packsim::thrift::QpnPortConfig portConfig;
    portConfig.srcPorts() = srcPorts;
    portConfig.dstPorts() = dstPorts;
    endpoint_->setQpnPortConfig(kSrcQpn, portConfig);
  }

  /**
   * Select the connection `entropy` and `tiebreak` pick for `kSrcQpn`.
   *
   * `Endpoint::getPortLevelQPConnection` hands back a pointer only so that a
   * packet can hold the selection; on every path it either throws or returns a
   * connection owned by the routing table, never null. Returning a reference
   * puts that contract in the type for the assertions below.
   *
   * @param entropy     Flow entropy driving the ECMP modulo.
   * @param tiebreak    Which selection policy to apply.
   *
   * @return            The selected connection, owned by the routing table.
   */
  const QPConnection& selectConnection(
      uint16_t entropy,
      const packsim::thrift::TiebreakSpec& tiebreak) {
    return *endpoint_->getPortLevelQPConnection(
        destination_, kSrcQpn, kDstQpn, entropy, tiebreak);
  }

  /**
   * Arrange + precondition shared by the two "empty connection set" cases.
   *
   * Configures a QPN whose port config has no source ports, whose build
   * therefore throws and latches an empty routing-table entry.
   */
  void buildEmptyConnectionSetAndExpectThrow() {
    setQpnPorts({}, {0});
    ASSERT_THAT(
        [&] {
          endpoint_->buildQPConnectionTable(destination_, kSrcQpn, kDstQpn);
        },
        ThrowsMessage<std::runtime_error>(
            HasSubstr("No ports available for QPN")));
  }

  /**
   * The (srcPort, dstPort) FlexDeviceId port fields of a connection.
   *
   * @param conn    A selected connection.
   *
   * @return        Its source and destination port numbers.
   */
  static std::pair<int, int> portPair(const QPConnection& conn) {
    return {
        conn.srcId.values[kPortIndexInFlexDeviceId],
        conn.dstId.values[kPortIndexInFlexDeviceId]};
  }

  /**
   * The distinct picks spray makes over `draws` selections.
   *
   * Entropy is held constant so only the uniform draw can vary the pick.
   *
   * @param draws    How many selections to make.
   *
   * @return         The set of distinct (srcPort, dstPort) pairs seen.
   */
  std::set<std::pair<int, int>> drawSprayPicks(const int draws) {
    std::set<std::pair<int, int>> observed;
    for (int draw = 0; draw < draws; ++draw) {
      observed.emplace(
          portPair(selectConnection(/* entropy */ 0, sprayTiebreak_)));
    }
    return observed;
  }

  const FlexDeviceId destination_{"1:0:2:1:0:0:0:0"};

  // Destruction runs in reverse declaration order, and each layer holds a raw
  // pointer down to the next, so they are declared outermost-last:
  //   endpoint_ -> EthernetPort* (registered via addPort)
  //   EthernetPort -> EthernetChannel* (set by attach())
  // ~EthernetPort does real teardown (teardownLlr / teardownWireDriver)
  // and LinkLayerScheduler holds folly::not_null<EthernetChannel*>, so a
  // channel outliving its ports is not optional. The sibling fixtures in
  // link-layer/oss_tests reset their ports in TearDown() for the same
  // reason; declaring channels_ first gets it without a TearDown().
  std::vector<std::shared_ptr<EthernetChannel>> channels_;
  std::vector<std::unique_ptr<EthernetPort>> ports_;
  std::vector<std::unique_ptr<EthernetPort>> switchPorts_;

  std::unique_ptr<Endpoint> endpoint_;

  packsim::thrift::TiebreakSpec ecmpTiebreak_;
  packsim::thrift::TiebreakSpec sprayTiebreak_;
};

TEST_F(
    EndpointPortLevelQpSelectionTest,
    EcmpMapsEntropyOntoConnectionsInOrder) {
  // Arrange: 2 src ports x 3 dst ports = 6 connections, built src-major.
  setQpnPorts(
      {kSrcPorts.begin(), kSrcPorts.end()},
      {kDstPorts.begin(), kDstPorts.end()});
  const std::vector<std::pair<int, int>> expectedByEntropy = {
      {1, 1}, {1, 2}, {1, 3}, {2, 1}, {2, 2}, {2, 3}};
  ASSERT_EQ(expectedByEntropy.size(), kNumConns);

  // Act + Assert: entropy indexes that vector directly.
  for (uint16_t entropy = 0; entropy < expectedByEntropy.size(); ++entropy) {
    EXPECT_EQ(
        portPair(selectConnection(entropy, ecmpTiebreak_)),
        expectedByEntropy[entropy])
        << "at entropy " << entropy;
  }
}

TEST_F(
    EndpointPortLevelQpSelectionTest,
    EcmpWrapsEntropyModuloConnectionCount) {
  // Arrange: kNumConns connections, so entropy and entropy+kNumConns collide.
  setQpnPorts(
      {kSrcPorts.begin(), kSrcPorts.end()},
      {kDstPorts.begin(), kDstPorts.end()});

  // Act
  const QPConnection& base = selectConnection(/* entropy */ 1, ecmpTiebreak_);
  const QPConnection& wrapped = selectConnection(kNumConns + 1, ecmpTiebreak_);
  const QPConnection& neighbour =
      selectConnection(/* entropy */ 2, ecmpTiebreak_);

  // Assert: flow stickiness is object identity, not just an equal port pair.
  EXPECT_EQ(&base, &wrapped);
  EXPECT_NE(&base, &neighbour);
}

TEST_F(EndpointPortLevelQpSelectionTest, EcmpWithOneConnectionIgnoresEntropy) {
  // Arrange: a single src/dst pair collapses the modulus to 1.
  setQpnPorts({2}, {3});

  // Act
  const QPConnection& first = selectConnection(/* entropy */ 0, ecmpTiebreak_);
  const QPConnection& second = selectConnection(kMaxEntropy, ecmpTiebreak_);

  // Assert
  EXPECT_EQ(&first, &second);
  EXPECT_EQ(portPair(first), std::make_pair(3, 4));
}

// Equality covers both properties at once: spray must reach every connection
// of the QPN, and never reach outside it.
TEST_F(
    EndpointPortLevelQpSelectionTest,
    SprayCoversExactlyTheConnectionsOfTheQpn) {
  setQpnPorts({0, 1}, {0, 1});

  // Function-local rather than a namespace-scope `k`-prefixed global: a
  // std::set needs a run-time constructor, which the `k` prefix would imply
  // it does not have.
  const std::set<std::pair<int, int>> allowedSprayPicks = {
      {1, 1}, {1, 2}, {2, 1}, {2, 2}};

  const auto observed = drawSprayPicks(kSprayDraws);

  EXPECT_EQ(observed, allowedSprayPicks);
}

TEST_F(
    EndpointPortLevelQpSelectionTest,
    SprayWithOneConnectionAlwaysReturnsIt) {
  // Arrange: sampleUniform(0, 0) is the degenerate boundary of the spray draw.
  setQpnPorts({1}, {0});

  // Act + Assert: one connection means the draw has exactly one possible
  // outcome, so a single call covers the boundary — repeating it would only
  // re-assert the same thing.
  EXPECT_EQ(
      portPair(selectConnection(/* entropy */ 0, sprayTiebreak_)),
      std::make_pair(2, 1));
}

TEST_F(EndpointPortLevelQpSelectionTest, UnsupportedTiebreakTypeThrows) {
  // Arrange: reflect is a valid TiebreakSpec arm that endpoints cannot honour.
  setQpnPorts({0}, {0});
  packsim::thrift::TiebreakSpec reflectTiebreak;
  reflectTiebreak.reflect() = packsim::thrift::ReflectionTiebreakSpec{};

  // Act + Assert
  EXPECT_THAT(
      [&] {
        endpoint_->getPortLevelQPConnection(
            destination_, kSrcQpn, kDstQpn, /* entropy */ 0, reflectTiebreak);
      },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Unsupported tiebreak type")));
}

TEST_F(EndpointPortLevelQpSelectionTest, UnsetTiebreakThrows) {
  // Arrange: a default-constructed TiebreakSpec has no arm set.
  setQpnPorts(/*srcPorts=*/{0}, /*dstPorts=*/{0});
  const packsim::thrift::TiebreakSpec unsetTiebreak{};

  // Act + Assert
  EXPECT_THAT(
      [&] { selectConnection(/*entropy=*/0, unsetTiebreak); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Unsupported tiebreak type")));
}

TEST_F(
    EndpointPortLevelQpSelectionTest,
    BuildQpConnectionTableWithEmptySrcPortsThrows) {
  buildEmptyConnectionSetAndExpectThrow();
}

TEST_F(
    EndpointPortLevelQpSelectionTest,
    SelectionOnCachedEmptyConnectionSetThrows) {
  // Arrange: the failed build below latches the (empty) routing-table entry
  // for this QPN, so it is cached as "built with zero connections". Wrapped
  // because the helper's ASSERT_* only returns from the helper — without this
  // the Act below would run on an unestablished precondition.
  ASSERT_NO_FATAL_FAILURE(buildEmptyConnectionSetAndExpectThrow());

  // Act + Assert: selection reports the empty set rather than rebuilding.
  EXPECT_THAT(
      [&] {
        endpoint_->getPortLevelQPConnection(
            destination_, kSrcQpn, kDstQpn, /* entropy */ 0, ecmpTiebreak_);
      },
      ThrowsMessage<std::runtime_error>(HasSubstr("should not be empty")));
}

// `Endpoint::routePacket` skips the selection above for a packet whose
// transport already chose its port, so the QPN's port config does not restrict
// it. This is one of two paths around the selection; the other, the per-port
// send function that `Endpoint::addPort` registers, is the one RoCEv2 takes on
// a port-indexed endpoint.
TEST_F(
    EndpointPortLevelQpSelectionTest,
    RoutePacketTakesTransportChosenPortOverPortConfig) {
  // Arrange: the port config allows only 0-based port 0, which the selection
  // above honours. The transport chose 0-based port 3: FlexDeviceId port 4,
  // wired to ports_[3].
  setQpnPorts({0}, {0});
  ASSERT_EQ(
      portPair(selectConnection(/* entropy */ 0, ecmpTiebreak_)),
      std::make_pair(1, 1));
  constexpr uint16_t kChosenPort = 3;
  const QPConnection socketConn(
      endpoint_->getFlexId(), destination_, kSrcQpn, kDstQpn);
  auto pkt = createPacket<PacketType::ROCE>();
  auto* rocePkt = pkt.tryAs<RocePacket>();
  ASSERT_NE(rocePkt, nullptr);
  rocePkt->conn = &socketConn;
  rocePkt->egressNicPort = kChosenPort;
  rocePkt->dstNicPort = kChosenPort;
  ASSERT_TRUE(rocePkt->chosenEgressNicPort().has_value());

  // Act
  EthernetPort* const egress = endpoint_->routePacket(pkt, std::nullopt);

  // Assert
  EXPECT_EQ(egress, ports_[kChosenPort].get());
  EXPECT_EQ(
      portPair(*static_cast<const QPConnection*>(rocePkt->conn)),
      std::make_pair(4, 4));
}

} // namespace packsim
