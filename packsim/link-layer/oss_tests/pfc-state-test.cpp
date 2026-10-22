/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/packet/pfc.h"

namespace packsim {

/**
 * A port's receive-side PFC state: what crossing a threshold does to the
 * port's own state, its counters, and the frame it asks the peer to act on.
 */
class PfcStateTest : public ::testing::Test {
 public:
  void SetUp() override {
    scheduler_.buildNewEngine();
    port1_ = std::make_unique<EthernetPort>(&simCtx_, "100Gbps");
    port2_ = std::make_unique<EthernetPort>(&simCtx_, "100Gbps");

    std::vector<packsim::thrift::PortQueueConfig> portQueueCfg(8);
    port1_->setPortQueueConfig(portQueueCfg);
    port2_->setPortQueueConfig(portQueueCfg);

    port1_->setPortNum(0);
    port2_->setPortNum(1);

    channel_ = std::make_unique<EthernetChannel>(100);
    port1_->attach(channel_.get());
    port2_->attach(channel_.get());
  }

 protected:
  // Let every queued frame reach the peer. Well below the 134us refresh
  // period, so no test loops on the refresh timer.
  void drainWire() {
    scheduler_.run(Time::fromUs(5));
  }

  // PFC frames carry no CoS, so the port books them against queue 0. There is
  // no PFC-specific frame counter, so this is queue 0's total and is a PFC
  // frame count only because no test in this fixture sends data traffic.
  uint64_t pfcFramesSent() const {
    return port1_->getPortStats(0).txPkts;
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  std::unique_ptr<EthernetPort> port1_;
  std::unique_ptr<EthernetPort> port2_;
  std::unique_ptr<EthernetChannel> channel_;
};

TEST_F(PfcStateTest, CrossingTheXoffThresholdPausesThePeer) {
  // Act
  port1_->setRxPfcState(0, PfcState::XOFF);
  drainWire();

  // Assert: the port records the pause, and the peer stops transmitting.
  EXPECT_EQ(port1_->pfc().getRxState(0), PfcState::XOFF);
  EXPECT_EQ(port1_->getPortStats(0).rxXoff, 1u);
  EXPECT_EQ(port2_->pfc().getTxState(0), PfcState::XOFF);
}

TEST_F(PfcStateTest, FallingBackBelowTheThresholdReleasesThePeer) {
  // Arrange
  port1_->setRxPfcState(0, PfcState::XOFF);
  drainWire();
  ASSERT_EQ(port2_->pfc().getTxState(0), PfcState::XOFF);

  // Act
  port1_->setRxPfcState(0, PfcState::XON);
  drainWire();

  // Assert
  EXPECT_EQ(port1_->pfc().getRxState(0), PfcState::XON);
  EXPECT_EQ(port1_->getPortStats(0).rxXon, 1u);
  EXPECT_EQ(port2_->pfc().getTxState(0), PfcState::XON);
}

TEST_F(PfcStateTest, RepeatingTheSameStateSendsNothingFurther) {
  // Arrange
  port1_->setRxPfcState(0, PfcState::XOFF);
  drainWire();
  const uint64_t framesAfterFirstPause = pfcFramesSent();
  // Without this the final check compares 0 to 0 and passes even if the first
  // pause never reached the wire.
  ASSERT_EQ(framesAfterFirstPause, 1u);

  // Act: the buffer stays above the threshold, so the tracker says XOFF again.
  port1_->setRxPfcState(0, PfcState::XOFF);
  drainWire();

  // Assert: counted once, and no second frame. Only the refresh timer
  // re-asserts an unchanged pause, and it has not fired yet.
  EXPECT_EQ(port1_->getPortStats(0).rxXoff, 1u);
  EXPECT_EQ(pfcFramesSent(), framesAfterFirstPause);
}

TEST_F(PfcStateTest, ClassesArePausedIndependently) {
  // Act
  port1_->setRxPfcState(0, PfcState::XOFF);
  port1_->setRxPfcState(2, PfcState::XOFF);
  drainWire();

  // Assert: only the classes that crossed are paused, at both ends.
  EXPECT_EQ(port1_->pfc().getRxState(1), PfcState::XON);
  EXPECT_EQ(port2_->pfc().getTxState(0), PfcState::XOFF);
  EXPECT_EQ(port2_->pfc().getTxState(1), PfcState::XON);
  EXPECT_EQ(port2_->pfc().getTxState(2), PfcState::XOFF);
}

TEST_F(PfcStateTest, ClassesCrossingWhileTheWireIsBusyCoalesce) {
  // Act: the first mark finds the wire idle and goes out at once, which leaves
  // the wire busy for the next two -- the ordinary case when several small
  // packets are admitted across different queues back to back.
  port1_->setRxPfcState(0, PfcState::XOFF);
  port1_->setRxPfcState(1, PfcState::XOFF);
  port1_->setRxPfcState(2, PfcState::XOFF);
  drainWire();

  // Assert: two frames, not three -- classes 1 and 2 shared one. All three
  // classes still reach the peer.
  EXPECT_EQ(pfcFramesSent(), 2u);
  EXPECT_EQ(port2_->pfc().getTxState(0), PfcState::XOFF);
  EXPECT_EQ(port2_->pfc().getTxState(1), PfcState::XOFF);
  EXPECT_EQ(port2_->pfc().getTxState(2), PfcState::XOFF);
}

TEST_F(PfcStateTest, APauseSupersededBeforeTransmitNeverReachesTheWire) {
  // Arrange: occupy the wire so the next marks stay pending.
  port1_->setRxPfcState(1, PfcState::XOFF);

  // Act: class 0 crosses the threshold and falls back below it before the
  // wire frees up.
  port1_->setRxPfcState(0, PfcState::XOFF);
  port1_->setRxPfcState(0, PfcState::XON);
  drainWire();

  // Assert: two frames did reach the peer -- the class 1 pause, then the
  // coalesced frame carrying class 0. Without these the checks below hold at
  // t=0 and would pass even if nothing reached the wire at all.
  ASSERT_EQ(pfcFramesSent(), 2u);
  ASSERT_EQ(port2_->pfc().getTxState(1), PfcState::XOFF);

  // The peer was never told to pause class 0: the second frame carried the
  // XON that superseded it. Sending the pause would have idled it for no
  // reason.
  EXPECT_EQ(port2_->getPortStats(0).rxXoff, 0u);
  EXPECT_EQ(port2_->pfc().getTxState(0), PfcState::XON);
}

} // namespace packsim
