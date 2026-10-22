/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <deque>
#include <string>

#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/time.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/link-layer/oss_tests/lambda-port-observer.h"
#include "packsim/packet/pfc.h"

namespace packsim {

struct PacketEvent {
  explicit PacketEvent(const Time now, PacketPtr inPkt)
      : timeNs(now.getNs()), pkt(inPkt) {}
  uint64_t timeNs;
  PacketPtr pkt;
};

class EthernetTest : public ::testing::Test {
 public:
  void SetUp() override {
    scheduler_.buildNewEngine();
    rebuild();
  }

  void TearDown() override {
    recv1Q_.clear();
    recv2Q_.clear();
  }

  void rebuild() {
    delayTime_ = Time::fromNs(delayNs_);
    dataRate_ = DataRate(speed_);

    port1_ = std::make_unique<EthernetPort>(&simCtx_, speed_);
    port1_->setObserver(&observerPort1_);
    port2_ = std::make_unique<EthernetPort>(&simCtx_, speed_);
    port2_->setObserver(&observerPort2_);

    observerPort1_.setOnPacketRcvdCallback(
        [this](EthernetPort& ethPort, const PacketPtr& p) {
          recv1(ethPort, p);
        });
    observerPort1_.setOnPacketSentCallback(
        [this](EthernetPort& ethPort, const PacketPtr& p) {
          sent1(ethPort, p);
        });
    observerPort2_.setOnPacketRcvdCallback(
        [this](EthernetPort& ethPort, const PacketPtr& p) {
          recv2(ethPort, p);
        });
    observerPort2_.setOnPacketSentCallback(
        [this](EthernetPort& ethPort, const PacketPtr& p) {
          sent2(ethPort, p);
        });

    channel_ = std::make_unique<EthernetChannel>(delayNs_);
    port1_->attach(channel_.get());
    port2_->attach(channel_.get());
  }

  void recv1(EthernetPort& /* ethPort */, const PacketPtr& pkt) {
    recv1Q_.emplace_back(simCtx_.now(), pkt);
  }

  void sent1(EthernetPort& /* ethPort */, const PacketPtr& pkt) {
    sent1Q_.emplace_back(simCtx_.now(), pkt);
  }

  void recv2(EthernetPort& /* ethPort */, const PacketPtr& pkt) {
    recv2Q_.emplace_back(simCtx_.now(), pkt);
  }

  void sent2(EthernetPort& /* ethPort */, const PacketPtr& pkt) {
    sent2Q_.emplace_back(simCtx_.now(), pkt);
  }

  void pauseRx(EthernetPort* ethPort, uint8_t q) {
    ethPort->setRxPfcState(q, PfcState::XOFF);
  }

  void resumeRx(EthernetPort* ethPort, uint8_t q) {
    ethPort->setRxPfcState(q, PfcState::XON);
  }

  void assertTxPaused(EthernetPort* ethPort, uint8_t q) {
    auto state = ethPort->pfc().getTxState(q);
    EXPECT_EQ(state, PfcState::XOFF);
  }

  void assertTxNotPaused(EthernetPort* ethPort, uint8_t q) {
    auto state = ethPort->pfc().getTxState(q);
    EXPECT_EQ(state, PfcState::XON);
  }

 protected:
  void sendPacket(EthernetPort* ethPort, uint32_t sizeBytes = 0) {
    auto pkt = createPacket<PacketType::UDP>(sizeBytes);
    simCtx_.scheduleNow(&EthernetPort::send, ethPort, pkt);
  }
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  LambdaPortObserver observerPort1_;
  std::unique_ptr<EthernetPort> port1_;
  LambdaPortObserver observerPort2_;
  std::unique_ptr<EthernetPort> port2_;
  std::unique_ptr<EthernetChannel> channel_;

  std::deque<PacketEvent> recv1Q_;
  std::deque<PacketEvent> recv2Q_;
  std::deque<PacketEvent> sent1Q_;
  std::deque<PacketEvent> sent2Q_;

  packsim::thrift::PortQueueConfig portQueueCfg_;
  std::string speed_{"100Gbps"};
  DataRate dataRate_;
  uint32_t delayNs_{100};
  Time delayTime_;
  uint32_t sendSizeBytes_{1000};
};

TEST_F(EthernetTest, SendOnePacket1To2) {
  sendPacket(port1_.get(), sendSizeBytes_);
  scheduler_.run();

  EXPECT_EQ(recv1Q_.size(), 0);
  EXPECT_EQ(recv2Q_.size(), 1);
  EXPECT_EQ(sent1Q_.size(), 1);
  EXPECT_EQ(sent2Q_.size(), 0);
}

TEST_F(EthernetTest, SendOnePacket2To1) {
  sendPacket(port2_.get(), sendSizeBytes_);
  scheduler_.run();

  EXPECT_EQ(recv1Q_.size(), 1);
  EXPECT_EQ(recv2Q_.size(), 0);
  EXPECT_EQ(sent1Q_.size(), 0);
  EXPECT_EQ(sent2Q_.size(), 1);
}

// The packet reported as sent counts its headers, the Ethernet one included.
TEST_F(EthernetTest, SentPacketSize) {
  sendPacket(port1_.get(), sendSizeBytes_);
  scheduler_.run();

  auto sentEvent = sent1Q_.front();
  EXPECT_EQ(
      sentEvent.pkt->getSize(), sendSizeBytes_ + sentEvent.pkt->headerSize);
}

// The packet handed up to the node keeps its headers: the port does not strip
// the Ethernet one.
TEST_F(EthernetTest, RecvPacketSize) {
  sendPacket(port1_.get(), sendSizeBytes_);
  scheduler_.run();

  auto recvEvent = recv2Q_.front();
  EXPECT_EQ(
      recvEvent.pkt->getSize(), sendSizeBytes_ + recvEvent.pkt->headerSize);
}

// `LinkLayerScheduler`'s calling pattern: `startSerialize` at t=0 returns
// propDelay, the caller advances sim-time by wireTime, then
// `scheduleRemoteReceive` fires the receive event at now + propDelay.
TEST_F(EthernetTest, StartSerializeThenScheduleAfterDelay) {
  auto pkt = createPacket<PacketType::UDP>(sendSizeBytes_);
  const Time wireTime = Time::fromNs(1000);
  const Time expectedRecvTime = wireTime + delayTime_;

  simCtx_.scheduleNow([this, pkt, wireTime]() {
    const Time propDelay = channel_->startSerialize(pkt, port1_.get());
    // Fixture's makeFlexLinkDelayConfig leaves expDelayNs at default 0,
    // so propDelay is deterministic and equals delayTime_ exactly.
    EXPECT_EQ(propDelay, delayTime_);
    simCtx_.schedule(wireTime, [this, pkt, propDelay]() {
      channel_->scheduleRemoteReceive(pkt, port1_.get(), propDelay);
    });
  });
  scheduler_.run();

  ASSERT_EQ(recv2Q_.size(), 1);
  EXPECT_EQ(recv2Q_.front().timeNs, expectedRecvTime.getNs());
  EXPECT_EQ(recv1Q_.size(), 0);
}

// Port 1 pausing its receive queue pauses port 2's transmit once the pause
// frame arrives, and resuming it lets port 2 transmit again.
TEST_F(EthernetTest, PausePort) {
  auto pauseTxTime = dataRate_.bytesToTime(128);
  simCtx_.scheduleNow(&EthernetTest::pauseRx, this, port1_.get(), 0);
  simCtx_.schedule(
      delayTime_ + pauseTxTime,
      &EthernetTest::assertTxPaused,
      this,
      port2_.get(),
      0);
  simCtx_.schedule(delayTime_, &EthernetTest::resumeRx, this, port1_.get(), 0);
  simCtx_.schedule(
      2U * delayTime_ + pauseTxTime,
      &EthernetTest::assertTxNotPaused,
      this,
      port2_.get(),
      0);
  scheduler_.run(10U * delayTime_);
}

} // namespace packsim
