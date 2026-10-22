/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/packet/pfc.h"
#include "packsim/packet/roce.h"
#include "packsim/transport/congestion-control.h"
#include "packsim/transport/roce-base.h"

namespace packsim {
namespace {

// A transport that doesn't auto-send on socketReady(), so tests drive
// sendBytes()/onPacketRcvd() directly (mirrors error-recovery-stub-test).
class TestRoceTransport final : public RoceTransportBase {
 public:
  using RoceTransportBase::RoceTransportBase;
  // Expose the protected scheduler hook so tests can drive it directly.
  using RoceTransportBase::trySendOnSocket;
  void sendNext() override {}
};

// Minimal CongestionControl for socket-integration tests: grants the full
// requested quota, records the bytes reported sent, and (like DCQCN) emits a
// CNP through its socket for ECN-CE packets. Other hooks stay at their ABC
// defaults.
class StubCongestionControl final : public CongestionControl {
 public:
  explicit StubCongestionControl(
      CongestionControlSocket& socket,
      bool sendBlocked = false)
      : socket_(socket), sendBlocked_(sendBlocked) {}

  size_t adjustQuota(size_t requestedQuota) const override {
    return requestedQuota;
  }
  void onBytesSent(uint32_t dataBytes) override {
    bytesSent_ += dataBytes;
  }
  void onPacketReceived(bool ecnCe) override {
    if (ecnCe) {
      socket_.emitCongestionNotification();
    }
  }
  bool isSendBlocked() const noexcept override {
    return sendBlocked_;
  }
  uint32_t bytesSent() const noexcept {
    return bytesSent_;
  }

 private:
  CongestionControlSocket& socket_;
  bool sendBlocked_{false};
  uint32_t bytesSent_{0};
};

class CongestionControlSocketTest : public ::testing::Test {
 public:
  void SetUp() override {
    nicCfg_.writeEff() = 100;
    nicCfg_.readGbps() = 400;
    nicCfg_.readLatencyNs() = 0;
    nicCfg_.perCosBufferSize() = {1024 * 1024, 1024 * 1024};
    nicCfg_.perCosXoff() = {1024 * 1024, 1024 * 1024};
    nicCfg_.perCosXon() = {1024 * 1024, 1024 * 1024};
    nicCfg_.speed() = "400Gbps";

    transport_ =
        std::make_unique<TestRoceTransport>(&simCtx_, srcId_, &nicCfg_);
    transport_->setSendPacketFn([this](const PacketPtr& pkt) {
      sentPackets_.push_back(pkt);
      return true;
    });
    transport_->setOnMessageAckedCallback([](Message*) {});
    transport_->setOnMessageReceivedCallback([](Message*) {});
  }

  std::unique_ptr<RoceSocketBase> makeSocket(
      const RoceSocketBase::CongestionControlFactory& factory) {
    return std::make_unique<RoceSocketBase>(
        transport_.get(),
        srcId_,
        dstId_,
        /*srcPort=*/1234,
        /*localQPN=*/0,
        /*remoteQPN=*/0,
        /*smtu=*/4200,
        /*workload=*/nullptr,
        /*errorRecoveryFactory=*/nullptr,
        factory);
  }

  std::unique_ptr<RoceSocketBase> makeSocketWithStub(bool sendBlocked = false) {
    auto factory = [this, sendBlocked](
                       CongestionControlSocket& socket,
                       SimContext&) -> std::unique_ptr<CongestionControl> {
      auto stub = std::make_unique<StubCongestionControl>(socket, sendBlocked);
      stubPtr_ = stub.get();
      return stub;
    };
    return makeSocket(factory);
  }

 protected:
  Scheduler scheduler_;
  Tracer::ConditionalMap conditionalMap_;
  SimContext simCtx_{&scheduler_, "SimContext", 0, &conditionalMap_};
  FlexDeviceId srcId_{{1, 0, 0, 1}};
  FlexDeviceId dstId_{{1, 0, 0, 2}};
  packsim::thrift::NicConfig nicCfg_;
  std::unique_ptr<TestRoceTransport> transport_;
  StubCongestionControl* stubPtr_{nullptr};
  std::vector<PacketPtr> sentPackets_;
};

TEST_F(CongestionControlSocketTest, FactoryInstallsCongestionControl) {
  auto socket = makeSocketWithStub();

  ASSERT_NE(stubPtr_, nullptr);
  EXPECT_EQ(socket->getCongestionControl(), stubPtr_);
}

TEST_F(
    CongestionControlSocketTest,
    NullFactoryLeavesCongestionControlDisabled) {
  auto socket = makeSocket(nullptr);

  EXPECT_EQ(socket->getCongestionControl(), nullptr);
}

TEST_F(CongestionControlSocketTest, DcqcnConfigBuildsRealController) {
  // Exercise the real makeSocket factory (not a stub): a DCQCN-configured
  // transport installs a live congestion-control strategy on connect().
  *nicCfg_.dcqcnCfg()->dcqcnEnable() = true;
  RoceTransportConfig roceCfg;
  roceCfg.congestionControl = RoceTransportConfig::CongestionControl::DCQCN;
  const FlexDeviceId dcqcnSrc{{1, 0, 0, 3}};
  const FlexDeviceId dcqcnDst{{1, 0, 0, 4}};
  auto dcqcnTransport = std::make_unique<TestRoceTransport>(
      &simCtx_, dcqcnSrc, &nicCfg_, roceCfg);
  dcqcnTransport->setSendPacketFn([](const PacketPtr&) { return true; });
  dcqcnTransport->setOnPfcStateChangedCallback([](uint32_t, PfcState) {});
  dcqcnTransport->setOnMessageAckedCallback([](Message*) {});
  dcqcnTransport->setOnMessageReceivedCallback([](Message*) {});

  auto* socket = dynamic_cast<RoceSocketBase*>(dcqcnTransport->connect(
      dcqcnDst, /*port=*/1234, /*localQPN=*/0, /*remoteQPN=*/0));
  ASSERT_NE(socket, nullptr);
  EXPECT_NE(socket->getCongestionControl(), nullptr);
}

TEST_F(CongestionControlSocketTest, EmitCongestionNotificationSendsCnp) {
  auto socket = makeSocketWithStub();

  socket->emitCongestionNotification();

  ASSERT_EQ(sentPackets_.size(), 1u);
  EXPECT_TRUE(sentPackets_.front().as<RocePacket>()->isCnp);
}

TEST_F(CongestionControlSocketTest, EcnMarkedPacketEmitsCnp) {
  auto socket = makeSocketWithStub();

  // A received ECN-CE packet triggers a CNP back toward the sender. Use a CNP
  // carrier so the post-dispatch path is inert (onCongestionSignal defaults to
  // a no-op), isolating the ECN->CNP emission.
  auto pkt = createPacket<PacketType::ROCE>(0);
  pkt.as<Ipv6Packet>()->ecn = Ipv6Packet::EcnType::ECN_CE;
  pkt.as<RocePacket>()->isCnp = true;
  socket->onPacketRcvd(pkt);

  ASSERT_EQ(sentPackets_.size(), 1u);
  EXPECT_TRUE(sentPackets_.front().as<RocePacket>()->isCnp);
}

TEST_F(CongestionControlSocketTest, EcnUnmarkedPacketEmitsNoCnp) {
  auto socket = makeSocketWithStub();

  // The complement of EcnMarkedPacketEmitsCnp: a packet without the ECN-CE mark
  // draws no CNP, even with a strategy installed.
  auto pkt = createPacket<PacketType::ROCE>(0);
  pkt.as<Ipv6Packet>()->ecn = Ipv6Packet::EcnType::ECN_NOT_ECT;
  pkt.as<RocePacket>()->isCnp = true;
  socket->onPacketRcvd(pkt);

  EXPECT_TRUE(sentPackets_.empty());
}

TEST_F(CongestionControlSocketTest, NullCongestionControlPacketRcvdEmitsNoCnp) {
  // With no congestion-control strategy installed, onPacketRcvd must not emit a
  // CNP even for an ECN-CE packet (the congestionControl_ null guard).
  auto socket = makeSocket(nullptr);

  auto pkt = createPacket<PacketType::ROCE>(0);
  pkt.as<Ipv6Packet>()->ecn = Ipv6Packet::EcnType::ECN_CE;
  pkt.as<RocePacket>()->isCnp = true;
  socket->onPacketRcvd(pkt);

  EXPECT_TRUE(sentPackets_.empty());
}

TEST_F(CongestionControlSocketTest, SendBytesRoutesThroughCongestionControl) {
  auto socket = makeSocketWithStub();
  Message msg{.sizeBytes = 128, .srcIdx = srcId_, .dstIdx = dstId_};
  socket->send(&msg);

  const auto [madeProgress, sentBytes] = socket->sendBytes(msg.sizeBytes);
  scheduler_.run();

  ASSERT_NE(stubPtr_, nullptr);
  EXPECT_TRUE(madeProgress);
  EXPECT_EQ(sentBytes, msg.sizeBytes);
  EXPECT_EQ(stubPtr_->bytesSent(), msg.sizeBytes);
}

TEST_F(CongestionControlSocketTest, SendBlockedStrategySkipsSend) {
  auto socket = makeSocketWithStub(/*sendBlocked=*/true);
  Message msg{.sizeBytes = 128, .srcIdx = srcId_, .dstIdx = dstId_};
  socket->send(&msg);

  // The scheduler hook must skip a socket whose CC reports it is blocked, even
  // with queued work; nothing is sent.
  EXPECT_FALSE(transport_->trySendOnSocket(socket.get()));
  EXPECT_TRUE(sentPackets_.empty());
}

TEST_F(CongestionControlSocketTest, UnblockedStrategyAllowsSend) {
  auto socket = makeSocketWithStub(/*sendBlocked=*/false);
  Message msg{.sizeBytes = 128, .srcIdx = srcId_, .dstIdx = dstId_};
  socket->send(&msg);

  EXPECT_TRUE(transport_->trySendOnSocket(socket.get()));
  scheduler_.run();
  EXPECT_FALSE(sentPackets_.empty());
}

} // namespace
} // namespace packsim
