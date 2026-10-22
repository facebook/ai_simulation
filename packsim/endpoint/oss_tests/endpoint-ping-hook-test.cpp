/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>

#include <gtest/gtest.h>

#include "packsim/endpoint/endpoint.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/node/flex-node.h"
#include "packsim/packet/ping.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/transport/oss_tests/roce-v2-test-config.h"

namespace packsim {
namespace {

// Transport stub that counts onPacketRcvd so a test can assert whether a packet
// reached the transport layer (i.e. was NOT intercepted by the ping hook).
class CountingTransport : public Transport {
 public:
  CountingTransport(
      SimContext* ctx,
      const FlexDeviceId& id,
      const packsim::thrift::NicConfig* nicConfig)
      : Transport(ctx, id, nicConfig) {}

  void onPacketDropped(const PacketPtr& /* pkt */) override {}

  Socket* connect(
      const FlexDeviceId& /* dst */,
      uint16_t /* port */,
      uint32_t /* localQPN */,
      uint32_t /* remoteQPN */,
      uint8_t /* priority */ = 0,
      const packsim::workload::thrift::Workload* /* workload */ =
          nullptr) override {
    return nullptr;
  }

  bool onPacketSent(const PacketPtr& /* pkt */, EthernetPort* /* ethPort */)
      override {
    return true;
  }

  bool onPacketRcvd(const PacketPtr& /* pkt */, EthernetPort* /* ethPort */)
      override {
    onPacketRcvdCount_++;
    return true;
  }

  int getPacketRcvdCount() const {
    return onPacketRcvdCount_;
  }

 private:
  int onPacketRcvdCount_{0};
};

// Exposes transport_ so the fixture can swap in the counting stub.
class TestableEndpoint : public Endpoint {
 public:
  using Endpoint::Endpoint;
  using Endpoint::transport_;
};

// Exercises the endpoint ping hook: Endpoint::onPacketRcvd dispatches PING
// packets to a registered handler before the transport layer, and forwards
// everything else to the transport unchanged.
class EndpointPingHookTest : public ::testing::Test {
 protected:
  void SetUp() override {
    scheduler_.buildNewEngine();

    nicConfig_ = std::make_unique<packsim::thrift::NicConfig>();
    nicConfig_->speed() = "100Gbps";
    RoceV2TestConfig::configureRoceV2Nic(*nicConfig_);
    nicConfig_->queueScheduleCfg() =
        packsim::thrift::SchedulingAlgorithmType::RR;
    nicConfig_->queueFilterCfg() = packsim::thrift::QueueFilterConfig{};
    nicConfig_->queueCfg() = std::vector<packsim::thrift::PortQueueConfig>{};

    topology_ = std::make_unique<packsim::thrift::FlexTopology>();
    topology_->name() = "test_topology";

    FlexNodeParams params{
        .simCtx = &simCtx_,
        .name = "test_endpoint",
        .flexId = FlexDeviceId("1:0:0:0:0:0:0:0"),
        .lpmTable = nullptr,
        .topo = topology_.get(),
    };
    endpoint_ = std::make_unique<TestableEndpoint>(
        params, nicConfig_.get(), std::nullopt);

    endpoint_->transport_ = std::make_unique<CountingTransport>(
        &simCtx_, endpoint_->getFlexId(), nicConfig_.get());

    ethPort_ = std::make_unique<EthernetPort>(&simCtx_, "100Gbps");
  }

  CountingTransport* transport() {
    return static_cast<CountingTransport*>(endpoint_->transport_.get());
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  std::unique_ptr<packsim::thrift::NicConfig> nicConfig_;
  std::unique_ptr<packsim::thrift::FlexTopology> topology_;
  std::unique_ptr<TestableEndpoint> endpoint_;
  std::unique_ptr<EthernetPort> ethPort_;
};

TEST_F(EndpointPingHookTest, PingPacketDispatchedToRegisteredHandler) {
  bool called = false;
  FlexNode* seenNode = nullptr;
  endpoint_->setOnPingReceivedCallback(
      [&](const PacketPtr& /* pkt */, FlexNode* node) {
        called = true;
        seenNode = node;
      });

  endpoint_->onPacketRcvd(
      *ethPort_, createPacket<PacketType::PING>(PingPacket::kPingPayloadSize));

  EXPECT_TRUE(called);
  // The receiving endpoint is handed to the handler as the node.
  EXPECT_EQ(seenNode, static_cast<FlexNode*>(endpoint_.get()));
  // A PING is intercepted before the transport layer.
  EXPECT_EQ(transport()->getPacketRcvdCount(), 0);
}

TEST_F(EndpointPingHookTest, NonPingPacketReachesTransport) {
  bool called = false;
  endpoint_->setOnPingReceivedCallback(
      [&](const PacketPtr& /* pkt */, FlexNode* /* node */) { called = true; });

  endpoint_->onPacketRcvd(*ethPort_, createPacket<PacketType::IPV6>(64));

  EXPECT_FALSE(called);
  EXPECT_EQ(transport()->getPacketRcvdCount(), 1);
}

// A PING can only be on the wire because something put it there, and whatever
// does must install the handler first. Arriving with no handler means the two
// got out of step, so the endpoint aborts rather than dropping the packet
// quietly.
using EndpointPingHookDeathTest = EndpointPingHookTest;

TEST_F(EndpointPingHookDeathTest, PingWithNoHandlerAborts) {
  EXPECT_DEATH(
      endpoint_->onPacketRcvd(
          *ethPort_,
          createPacket<PacketType::PING>(PingPacket::kPingPayloadSize)),
      "PING arrived at endpoint");
}

// An installer that is destroyed before the endpoints clears the handler. A
// PING after that would otherwise reach a destroyed responder.
TEST_F(EndpointPingHookDeathTest, PingAfterHandlerClearedAborts) {
  endpoint_->setOnPingReceivedCallback(
      [](const PacketPtr& /* pkt */, FlexNode* /* node */) {});
  endpoint_->clearOnPingReceivedCallback();

  EXPECT_DEATH(
      endpoint_->onPacketRcvd(
          *ethPort_,
          createPacket<PacketType::PING>(PingPacket::kPingPayloadSize)),
      "PING arrived at endpoint");
}

} // namespace
} // namespace packsim
