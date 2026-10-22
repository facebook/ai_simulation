/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <memory>
#include <optional>

#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/packet/pfc.h"
#include "packsim/transport/congestion-control.h"
#include "packsim/transport/roce-base.h"
#include "packsim/transport/transport.h"

namespace packsim {

// Verifies that DCQCN timers are cancelled when a socket finishes all work,
// allowing the simulation to finish.
//
// Before the fix, scheduler_.run() would never return because DCQCN alpha
// and rate-increase timers reschedule themselves unconditionally, keeping the
// event queue permanently non-empty.
class DcqcnTimerCancelTest : public ::testing::Test {
 public:
  void SetUp() override {
    constexpr int64_t kOneMicrosecondFs = 1'000'000'000;
    scheduler_.setLookaheadTimeFs(kOneMicrosecondFs);

    nicCfg_.writeEff() = 100;
    nicCfg_.readGbps() = 400;
    nicCfg_.readLatencyNs() = 100;
    nicCfg_.perCosBufferSize() = {1024 * 1024 * 1024, 1024 * 1024 * 1024};
    nicCfg_.perCosXoff() = {1000 * 1024 * 1024, 1000 * 1024 * 1024};
    nicCfg_.perCosXon() = {1000 * 1024 * 1024, 1000 * 1024 * 1024};
    nicCfg_.speed() = "400Gbps";
    *nicCfg_.dcqcnCfg()->dcqcnEnable() = true;

    RoceTransportConfig roceCfg;
    roceCfg.congestionControl = RoceTransportConfig::CongestionControl::DCQCN;
    srcTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, srcId_, &nicCfg_, roceCfg);
    dstTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, dstId_, &nicCfg_, roceCfg);

    srcTransport_->setSendPacketFn(
        [this](const PacketPtr& pkt) { return srcSendPacket(pkt); });
    srcTransport_->setOnPfcStateChangedCallback([](uint32_t, PfcState) {});
    srcTransport_->setOnMessageAckedCallback(
        [this](Message*) { recordSrcAck(); });
    // This harness only counts acks at the source and receives at the
    // destination, but a transport must have a subscriber for both directions:
    // the unexercised one is a no-op rather than absent.
    srcTransport_->setOnMessageReceivedCallback([](Message*) {});

    dstTransport_->setSendPacketFn(
        [this](const PacketPtr& pkt) { return dstSendPacket(pkt); });
    dstTransport_->setOnPfcStateChangedCallback([](uint32_t, PfcState) {});
    dstTransport_->setOnMessageReceivedCallback(
        [this](Message*) { ++rcvdCount_; });
    dstTransport_->setOnMessageAckedCallback([](Message*) {});

    srcSocket_ = srcTransport_->connect(dstId_, 1234, 0, 0);
    dstSocket_ = dstTransport_->connect(srcId_, 2345, 0, 0);
  }

  bool srcSendPacket(const PacketPtr& pkt) {
    srcTransport_->onPacketSent(pkt, nullptr);
    simCtx_.schedule(
        Time::fromNs(100),
        &Transport::onPacketRcvd,
        dstTransport_.get(),
        pkt,
        nullptr);
    return true;
  }

  bool dstSendPacket(const PacketPtr& pkt) {
    dstTransport_->onPacketSent(pkt, nullptr);
    simCtx_.schedule(
        Time::fromNs(100),
        &Transport::onPacketRcvd,
        srcTransport_.get(),
        pkt,
        nullptr);
    return true;
  }

  void sendAndRecv(uint32_t sizeBytes) {
    auto msg = std::make_unique<Message>(Message{
        .sizeBytes = sizeBytes,
        .srcIdx = srcId_,
        .dstIdx = dstId_,
        .connectionMessageId = static_cast<uint32_t>(msgs_.size())});
    srcSocket_->send(msg.get());
    dstSocket_->recv(msg.get());
    msgs_.emplace_back(std::move(msg));
  }

 protected:
  // The source's ack bookkeeping, shared by the fixture's ack subscriber and
  // by any test that subscribes its own.
  void recordSrcAck() {
    ++ackedCount_;
  }

  Tracer::ConditionalMap conditionalMap_;
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, "SimContext", 0, &conditionalMap_};
  FlexDeviceId srcId_{{1, 0, 0, 1}};
  FlexDeviceId dstId_{{1, 0, 0, 2}};
  packsim::thrift::NicConfig nicCfg_;

  std::unique_ptr<Transport> srcTransport_;
  std::unique_ptr<Transport> dstTransport_;
  Socket* srcSocket_{};
  Socket* dstSocket_{};

  std::vector<std::unique_ptr<Message>> msgs_;
  uint32_t ackedCount_{0};
  uint32_t rcvdCount_{0};
};

// Core regression test: scheduler_.run() returns naturally because the event
// queue drains after timers are cancelled. Before the fix, this would hang
// indefinitely because DCQCN timers reschedule themselves forever.
TEST_F(DcqcnTimerCancelTest, EventQueueDrainsAfterSingleMessage) {
  simCtx_.scheduleNow([this]() { sendAndRecv(/*sizeBytes=*/4096 * 65); });
  scheduler_.run();

  EXPECT_EQ(ackedCount_, 1) << "sender should have received an ACK";
  EXPECT_EQ(rcvdCount_, 1) << "receiver should have gotten the message";
}

// Verify that timers are not cancelled prematurely when only one side (send
// or recv) has completed but the other has pending work.
TEST_F(DcqcnTimerCancelTest, TimersStayActiveWhileRecvPending) {
  // Post an extra recv on the src socket that will never be satisfied.
  auto recvMsg = std::make_unique<Message>(Message{
      .sizeBytes = 4096,
      .srcIdx = dstId_,
      .dstIdx = srcId_,
      .connectionMessageId = 99});
  srcSocket_->recv(recvMsg.get());
  msgs_.emplace_back(std::move(recvMsg));

  const auto* const srcCc = dynamic_cast<const DcqcnCongestionControl*>(
      dynamic_cast<RoceSocketBase&>(*srcSocket_).getCongestionControl());
  ASSERT_NE(srcCc, nullptr);
  std::optional<uint32_t> rateIncreasesAtAck;
  srcTransport_->setOnMessageAckedCallback([&](Message*) {
    recordSrcAck();
    rateIncreasesAtAck = srcCc->numRateIncreaseEvents();
  });

  simCtx_.scheduleNow([this]() { sendAndRecv(/*sizeBytes=*/4096); });

  // Run for a bounded time — the message completes but the extra recv keeps
  // the src socket's timers alive.
  scheduler_.run(Time::fromUs(500));

  EXPECT_EQ(ackedCount_, 1) << "message should be ACKed";
  EXPECT_EQ(rcvdCount_, 1) << "message should be received";
  // The rate-increase timer (period rpTimerReset, 300us by default) keeps
  // ticking after the send completed.
  ASSERT_TRUE(rateIncreasesAtAck.has_value());
  EXPECT_GT(srcCc->numRateIncreaseEvents(), *rateIncreasesAtAck);
}

} // namespace packsim
