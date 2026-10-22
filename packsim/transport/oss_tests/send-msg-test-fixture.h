/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include <folly/logging/xlog.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/packet/pfc.h"
#include "packsim/packet/roce.h"
#include "packsim/transport/roce-base.h"
#include "packsim/transport/transport.h"
#include "packsim/util/hashes.h"

namespace packsim {

/*
 * Two-endpoint send/receive scaffold shared by the RoCEv2 suites in this
 * directory. `makeTransports` is the seam each subclass overrides to pick its
 * transport.
 */

class TransportTest : public ::testing::Test {
 public:
  void SetUp() override {
    simCtx_.dataRcvd = 0;

    nicCfg_.writeEff() = 100;
    nicCfg_.readGbps() = 400;
    nicCfg_.readLatencyNs() = 100;
    nicCfg_.perCosBufferSize() = {1024 * 1024 * 1024, 1024 * 1024 * 1024};
    nicCfg_.perCosXoff() = {1000 * 1024 * 1024, 1000 * 1024 * 1024};
    nicCfg_.perCosXon() = {1000 * 1024 * 1024, 1000 * 1024 * 1024};
    nicCfg_.speed() = "400Gbps";

    makeTransports();

    srcTransport_->setSendPacketFn(
        [this](const PacketPtr& pkt) { return srcSendPacket(pkt); });
    srcTransport_->setOnPfcStateChangedCallback(
        [this](uint32_t cos, PfcState state) { setSrcPfc(cos, state); });
    srcTransport_->setOnMessageAckedCallback(
        [this](Message* msg) { srcMsgAcked(msg); });

    dstTransport_->setSendPacketFn(
        [this](const PacketPtr& pkt) { return dstSendPacket(pkt); });
    dstTransport_->setOnPfcStateChangedCallback(
        [this](uint32_t cos, PfcState state) { setDstPfc(cos, state); });
    dstTransport_->setOnMessageReceivedCallback(
        [this](Message* msg) { dstMsgRcvd(msg); });

    srcSocket_ = srcTransport_->connect(dstId_, 1234, 0, 0);
    dstSocket_ = dstTransport_->connect(srcId_, 2345, 0, 0);
  }

  virtual void makeTransports() = 0;

  virtual bool srcSendPacket(const PacketPtr& pkt) {
    ++srcSendCallCounter_;
    if (!firstSentLossRecoveryEnabled_.has_value()) {
      firstSentLossRecoveryEnabled_ = pkt.as<RocePacket>()->lossRecoveryEnabled;
    }
    if (localTailDrops_ > 0 && pkt.as<RocePacket>()->isLastPkt()) {
      --localTailDrops_;
      srcTransport_->onPacketDropped(pkt);
      return true;
    }
    srcTransport_->onPacketSent(pkt, nullptr);
    if (dropTailData_ && !srcDropped_ && pkt.as<RocePacket>()->isLastPkt()) {
      srcDropped_ = true;
      return true;
    }
    if ((srcSendCallCounter_ > 3) && lossy_ && !srcDropped_) {
      auto size = pkt->getSize();
      if (size > 300) {
        XLOGF(INFO, "Skipping src packet of size {}", pkt->getSize());
        srcDropped_ = true;
      }
      return true;
    }
    auto delayNs = kDelayNs;
    if (hashFlows_) {
      auto hash = folly5TupleHash(pkt, 0);
      delayNs = 1 + (hash % 100) * kDelayNs;
    }
    simCtx_.schedule(
        Time::fromNs(delayNs),
        &Transport::onPacketRcvd,
        dstTransport_.get(),
        pkt,
        nullptr);
    return true;
  }

  virtual bool dstSendPacket(const PacketPtr& pkt) {
    ++dstSendCallCounter_;
    dstTransport_->onPacketSent(pkt, nullptr);
    if (dropAck_ && !dstDropped_ && pkt.as<RocePacket>()->isAck) {
      dstDropped_ = true;
      return true;
    }
    if ((dstSendCallCounter_ == 5) && lossy_) {
      XLOGF(INFO, "Skipping dst packet of size {}", pkt->getSize());
      return true;
    }
    simCtx_.schedule(
        Time::fromNs(kDelayNs),
        &Transport::onPacketRcvd,
        srcTransport_.get(),
        pkt,
        nullptr);
    return true;
  }

  void setSrcPfc(uint32_t /* cos */, PfcState state) {
    srcPfcState_ = state;
  }

  void setDstPfc(uint32_t /* cos */, PfcState state) {
    dstPfcState_ = state;
  }

  virtual void srcMsgAcked(Message* msg) {
    EXPECT_EQ(msg->connectionMessageId, msg_->connectionMessageId);
    srcAckd_ = true;
    if (dstRcvd_) {
      scheduler_.stop();
    }
  }

  virtual void dstMsgRcvd(Message* msg) {
    EXPECT_EQ(msg->connectionMessageId, msg_->connectionMessageId);
    dstRcvd_ = true;
    if (srcAckd_ || !expectAck_) {
      scheduler_.stop();
    }
  }

  void sendMsg() {
    Message msg{
        .sizeBytes = kMsgSize,
        .srcIdx = srcId_,
        .dstIdx = dstId_,
        .connectionMessageId = 7};
    msg_ = std::make_unique<Message>(msg);
    srcSocket_->send(msg_.get());
    if (postRecv_) {
      dstSocket_->recv(msg_.get());
    }
  }

  void finalCheck() {
    EXPECT_TRUE(srcAckd_ || !expectAck_);
    EXPECT_TRUE(dstRcvd_);
    EXPECT_EQ(simCtx_.dataRcvd, msg_->sizeBytes);
  }

  void scheduleSendMsgAndCheck() {
    simCtx_.scheduleNow(&TransportTest::sendMsg, this);
    scheduler_.run();
    finalCheck();
  }

 protected:
  Scheduler scheduler_;
  Tracer::ConditionalMap conditionalMap_;
  SimContext simCtx_{&scheduler_, "SimContext", 0, &conditionalMap_};
  FlexDeviceId srcId_{{1, 0, 0, 1}};
  FlexDeviceId dstId_{{1, 0, 0, 2}};
  packsim::thrift::NicConfig nicCfg_;

  PfcState srcPfcState_{false};
  PfcState dstPfcState_{false};

  std::unique_ptr<Transport> srcTransport_;
  std::unique_ptr<Transport> dstTransport_;
  Socket* srcSocket_{};
  Socket* dstSocket_{};

  std::unique_ptr<Message> msg_;

  static constexpr uint32_t kMsgSize{4096 * 65};
  static constexpr uint32_t kDelayNs{100};
  bool srcAckd_{false};
  bool dstRcvd_{false};
  bool lossy_{false};
  bool hashFlows_{false};
  uint32_t srcSendCallCounter_{0};
  uint32_t dstSendCallCounter_{0};
  bool srcDropped_{false};
  bool dstDropped_{false};
  bool dropTailData_{false};
  bool dropAck_{false};
  uint32_t localTailDrops_{0};

  bool postRecv_{true};
  bool expectAck_{true};

  // lossRecoveryEnabled as stamped on the first packet the source sent, so a
  // test can assert what the transport's own config produced rather than
  // setting the flag by hand.
  std::optional<bool> firstSentLossRecoveryEnabled_;
};

} // namespace packsim
