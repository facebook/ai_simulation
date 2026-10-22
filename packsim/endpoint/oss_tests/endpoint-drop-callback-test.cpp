/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>
#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "packsim/endpoint/endpoint.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/node/flex-node.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/transport/oss_tests/roce-v2-test-config.h"

namespace packsim {

// Mock transport to track onPacketDropped calls
class MockTransport : public Transport {
 public:
  MockTransport(
      SimContext* ctx,
      const FlexDeviceId& id,
      const packsim::thrift::NicConfig* nicConfig)
      : Transport(ctx, id, nicConfig) {}

  void onPacketDropped(const PacketPtr& /* pkt */) override {
    onPacketDroppedCallCount_++;
  }

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
    return true;
  }

  int getPacketDroppedCallCount() const {
    return onPacketDroppedCallCount_;
  }

 private:
  int onPacketDroppedCallCount_{0};
};

// Test wrapper to expose protected methods for testing
class TestableEndpoint : public Endpoint {
 public:
  using Endpoint::Endpoint;
  using Endpoint::onPacketDroppedOnReceive;
  using Endpoint::onPacketDroppedOnSend;
  using Endpoint::transport_;
};

/**
 * Test fixture for Endpoint drop callback tests.
 */
class EndpointPacketDropTest : public ::testing::Test {
 protected:
  void SetUp() override {
    scheduler_.buildNewEngine();

    // Create minimal NIC config for testing (matching endpoint-test.cpp
    // pattern)
    nicConfig_ = std::make_unique<packsim::thrift::NicConfig>();
    nicConfig_->speed() = "100Gbps";
    RoceV2TestConfig::configureRoceV2Nic(*nicConfig_);
    nicConfig_->queueScheduleCfg() =
        packsim::thrift::SchedulingAlgorithmType::RR;
    nicConfig_->queueFilterCfg() = packsim::thrift::QueueFilterConfig{};
    nicConfig_->queueCfg() = std::vector<packsim::thrift::PortQueueConfig>{};

    // Create minimal topology (required for endpoint initialization)
    topology_ = std::make_unique<packsim::thrift::FlexTopology>();
    topology_->name() = "test_topology";

    // Create endpoint
    FlexNodeParams params{
        .simCtx = &simCtx_,
        .name = "test_endpoint",
        .flexId = FlexDeviceId("1:0:0:0:0:0:0:0"),
        .lpmTable = nullptr,
        .topo = topology_.get(),
    };
    endpoint_ = std::make_unique<TestableEndpoint>(
        params, nicConfig_.get(), std::nullopt);

    // Replace transport with mock to track onPacketDropped calls
    endpoint_->transport_ = std::make_unique<MockTransport>(
        &simCtx_, endpoint_->getFlexId(), nicConfig_.get());

    // Create port
    ethPort_ = std::make_unique<EthernetPort>(&simCtx_, "100Gbps");
  }

  // Fields captured from the most recent onPacketDropped. deviceName is copied
  // out because DropEvent's view is valid only during the callback.
  struct CapturedDrop {
    std::optional<std::string> device;
    std::optional<FlexDeviceId> deviceId;
    std::optional<uint32_t> port;
    std::optional<packsim::switches::thrift::PacketDropReason> reason;
    std::optional<uint32_t> queueDepth;
  };

  // Record the next scheduler-notified drop into drop_.
  void captureDrops() {
    scheduler_.setOnPacketDroppedCallback([this](const DropEvent& e) {
      drop_.device = std::string(e.deviceName);
      drop_.deviceId = e.deviceId;
      drop_.port = e.port;
      drop_.reason = e.reason;
      drop_.queueDepth = e.queueDepthBytes;
    });
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  std::unique_ptr<packsim::thrift::NicConfig> nicConfig_;
  std::unique_ptr<packsim::thrift::FlexTopology> topology_;
  std::unique_ptr<TestableEndpoint> endpoint_;
  std::unique_ptr<EthernetPort> ethPort_;
  CapturedDrop drop_;
};
TEST_F(EndpointPacketDropTest, MultipleConsecutiveSendDrops) {
  auto mockTransport = static_cast<MockTransport*>(endpoint_->transport_.get());
  auto pkt1 = createPacket<PacketType::IPV6>(1000);
  auto pkt2 = createPacket<PacketType::IPV6>(1500);
  auto pkt3 = createPacket<PacketType::IPV6>(2000);

  endpoint_->onPacketDroppedOnSend(
      *ethPort_,
      pkt1,
      0,
      packsim::switches::thrift::PacketDropReason::BUFFER_FULL);
  endpoint_->onPacketDroppedOnSend(
      *ethPort_,
      pkt2,
      1,
      packsim::switches::thrift::PacketDropReason::BUFFER_FULL);
  endpoint_->onPacketDroppedOnSend(
      *ethPort_,
      pkt3,
      2,
      packsim::switches::thrift::PacketDropReason::BUFFER_FULL);

  EXPECT_EQ(mockTransport->getPacketDroppedCallCount(), 3);
}

TEST_F(EndpointPacketDropTest, MultipleConsecutiveReceiveDrops) {
  auto mockTransport = static_cast<MockTransport*>(endpoint_->transport_.get());
  auto pkt1 = createPacket<PacketType::IPV6>(1000);
  auto pkt2 = createPacket<PacketType::IPV6>(1500);
  auto pkt3 = createPacket<PacketType::IPV6>(2000);

  endpoint_->onPacketDroppedOnReceive(
      *ethPort_,
      pkt1,
      0,
      packsim::switches::thrift::PacketDropReason::LINK_ERROR);
  endpoint_->onPacketDroppedOnReceive(
      *ethPort_,
      pkt2,
      0,
      packsim::switches::thrift::PacketDropReason::LINK_ERROR);
  endpoint_->onPacketDroppedOnReceive(
      *ethPort_,
      pkt3,
      0,
      packsim::switches::thrift::PacketDropReason::LINK_ERROR);

  // Verify it's a no-op - transport_->onPacketDropped should NOT be called
  EXPECT_EQ(mockTransport->getPacketDroppedCallCount(), 0);
}

TEST_F(EndpointPacketDropTest, SendDropAcceptsDifferentQueueIds) {
  auto mockTransport = static_cast<MockTransport*>(endpoint_->transport_.get());
  auto pkt = createPacket<PacketType::IPV6>(1000);

  endpoint_->onPacketDroppedOnSend(
      *ethPort_,
      pkt,
      0,
      packsim::switches::thrift::PacketDropReason::BUFFER_FULL);
  endpoint_->onPacketDroppedOnSend(
      *ethPort_,
      pkt,
      1,
      packsim::switches::thrift::PacketDropReason::BUFFER_FULL);
  endpoint_->onPacketDroppedOnSend(
      *ethPort_,
      pkt,
      7,
      packsim::switches::thrift::PacketDropReason::BUFFER_FULL);

  EXPECT_EQ(mockTransport->getPacketDroppedCallCount(), 3);
}

TEST_F(EndpointPacketDropTest, SendDropNotifiesSchedulerObserver) {
  captureDrops();
  auto pkt = createPacket<PacketType::IPV6>(1000);
  endpoint_->onPacketDroppedOnSend(
      *ethPort_,
      pkt,
      /*queue=*/0,
      packsim::switches::thrift::PacketDropReason::BUFFER_FULL);

  ASSERT_TRUE(drop_.device.has_value());
  EXPECT_EQ(*drop_.device, endpoint_->getName());
  EXPECT_EQ(*drop_.deviceId, endpoint_->getFlexId());
  EXPECT_EQ(*drop_.port, ethPort_->getPortNum());
  EXPECT_EQ(
      *drop_.reason, packsim::switches::thrift::PacketDropReason::BUFFER_FULL);
  EXPECT_EQ(*drop_.queueDepth, ethPort_->getTotalQueueLength());
}

TEST_F(EndpointPacketDropTest, ReceiveDropNotifiesSchedulerObserver) {
  captureDrops();
  auto pkt = createPacket<PacketType::IPV6>(1000);
  endpoint_->onPacketDroppedOnReceive(
      *ethPort_,
      pkt,
      /*queue=*/0,
      packsim::switches::thrift::PacketDropReason::LINK_ERROR);

  ASSERT_TRUE(drop_.device.has_value());
  EXPECT_EQ(*drop_.device, endpoint_->getName());
  EXPECT_EQ(*drop_.deviceId, endpoint_->getFlexId());
  EXPECT_EQ(*drop_.port, ethPort_->getPortNum());
  EXPECT_EQ(
      *drop_.reason, packsim::switches::thrift::PacketDropReason::LINK_ERROR);
  EXPECT_EQ(*drop_.queueDepth, ethPort_->getTotalQueueLength());
}

} // namespace packsim
