/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/eth-channel.h"

#include <cstdint>
#include <deque>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <utility>

#include <folly/portability/GFlags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/link-layer/oss_tests/lambda-port-observer.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

namespace {
// Sets `pdes_threads=4` during member-init, before any test-fixture field
// constructs a `Scheduler`. Pinned rather than left to the default, so the
// test runs on 4 PDES (Parallel Discrete Event Simulation) worker threads
// whatever `useSmallDefaultThreadCount()` resolves the default to. Under
// ThreadSanitizer the instrumentation cost is superlinear in thread count, so
// a large count slows the test until it times out. `=4` keeps multi-thread
// coverage while staying fast, and keeps it the same everywhere.
// Mirrors the sibling `eth-port-test` / LLR fixtures.
struct PinPdesThreads {
  PinPdesThreads() {
    gflags::SetCommandLineOption("pdes_threads", "4");
  }
};

// Build a `FlexLinkDelayConfig` with a fixed propagation delay and no
// exponential jitter (`expDelayNs` stays at its thrift default of 0), so
// the sampled propagation delay is deterministic regardless of RNG seed.
packsim::thrift::FlexLinkDelayConfig makeDelayConfig(const uint64_t delayNs) {
  packsim::thrift::FlexLinkDelayConfig delayCfg;
  delayCfg.delayNs() = delayNs;
  return delayCfg;
}

// A log10Ber value that disables Bit Error Rate (BER) drops: any positive
// log10Ber yields a linear BER > 1, which `doReceive` treats as "no drops".
constexpr double kBerDisabledLog10 = 1.0;

// Payload size (bytes) for test packets. Value is immaterial to the
// channel-routing behavior under test; only that a packet is delivered.
constexpr uint32_t kPayloadBytes = 1000;

// Records a single packet reception: the sim-time it arrived and the packet.
struct ReceiveRecord {
  ReceiveRecord(const Time now, PacketPtr inPkt)
      : timeNs(now.getNs()), pkt(std::move(inPkt)) {}
  uint64_t timeNs;
  PacketPtr pkt;
};
} // namespace

// BER accessors are pure functions of `log10Ber` and need no simulation
// engine, so they are standalone `TEST`s rather than fixture-based.

TEST(EthChannelBerTest, ThreeArgCtorDerivesLinearBerFromLog10Ber) {
  // Header contract: a log10Ber of -6.0 means a linear BER of 1e-6.
  const EthernetChannel channel(
      makeDelayConfig(0), /*flapDelayNs=*/0, /*log10Ber=*/-6.0);

  EXPECT_EQ(channel.getLog10Ber(), -6.0);
  EXPECT_DOUBLE_EQ(channel.getBer(), 1e-6);
}

TEST(EthChannelBerTest, SetBerRecomputesLinearBer) {
  EthernetChannel channel(
      makeDelayConfig(0), /*flapDelayNs=*/0, /*log10Ber=*/-6.0);

  channel.setBer(-3.0);

  EXPECT_EQ(channel.getLog10Ber(), -3.0);
  EXPECT_DOUBLE_EQ(channel.getBer(), 1e-3);
}

TEST(EthChannelBerTest, DelayOnlyCtorLeavesBerDisabled) {
  // The single-argument ctor is for tests / simple callers that only care
  // about propagation delay; it must leave BER drops disabled (header:
  // the BER is ignored when the linear value is > 1).
  const EthernetChannel channel(/*delayNs=*/100);

  EXPECT_GT(channel.getBer(), 1.0);
}

// Fixture for behaviors that need attached ports and a running
// scheduler: port attachment, peer lookup, and packet routing.
class EthChannelTest : public ::testing::Test {
 protected:
  void SetUp() override {
    scheduler_.buildNewEngine();

    port1_ = std::make_unique<EthernetPort>(&simCtx_, kDataRate);
    port1_->setObserver(&observerPort1_);
    port2_ = std::make_unique<EthernetPort>(&simCtx_, kDataRate);
    port2_->setObserver(&observerPort2_);

    observerPort1_.setOnPacketRcvdCallback(
        [this](EthernetPort&, const PacketPtr& p) {
          port1RecvQ_.emplace_back(simCtx_.now(), p);
        });
    observerPort2_.setOnPacketRcvdCallback(
        [this](EthernetPort&, const PacketPtr& p) {
          port2RecvQ_.emplace_back(simCtx_.now(), p);
        });
    observerPort1_.setOnPacketSentCallback(
        [](EthernetPort&, const PacketPtr&) {});
    observerPort2_.setOnPacketSentCallback(
        [](EthernetPort&, const PacketPtr&) {});
  }

  // Member-init order matters: `flagSaver_` snapshots gflag state BEFORE
  // `pinPdesThreads_` overwrites it (so the prior value is restored on
  // teardown); `pinPdesThreads_` then sets `pdes_threads=4` BEFORE
  // `scheduler_` field-init constructs the PDES engine. The receive
  // queues are declared before the ports so they outlive them.
  folly::gflags::FlagSaver flagSaver_;
  PinPdesThreads pinPdesThreads_;
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  std::deque<ReceiveRecord> port1RecvQ_;
  std::deque<ReceiveRecord> port2RecvQ_;
  LambdaPortObserver observerPort1_;
  std::unique_ptr<EthernetPort> port1_;
  LambdaPortObserver observerPort2_;
  std::unique_ptr<EthernetPort> port2_;

  static constexpr auto kDataRate = "100Gbps";
};

TEST_F(EthChannelTest, AttachRejectsThirdPort) {
  EthernetChannel channel(
      makeDelayConfig(0), /*flapDelayNs=*/0, kBerDisabledLog10);
  channel.attach(port1_.get());
  channel.attach(port2_.get());
  EthernetPort port3(&simCtx_, kDataRate);

  EXPECT_THAT(
      [&]() { channel.attach(&port3); },
      ::testing::ThrowsMessage<std::runtime_error>(::testing::HasSubstr(
          "both ends of the channel are already connected")));
}

TEST_F(EthChannelTest, GetPeerReturnsTheOtherEnd) {
  EthernetChannel channel(
      makeDelayConfig(0), /*flapDelayNs=*/0, kBerDisabledLog10);
  channel.attach(port1_.get());
  channel.attach(port2_.get());

  EXPECT_EQ(channel.getPeer(*port1_), port2_.get());
  EXPECT_EQ(channel.getPeer(*port2_), port1_.get());
}

TEST_F(EthChannelTest, GetPeerRejectsAPortThatIsNotAttached) {
  EthernetChannel channel(
      makeDelayConfig(0), /*flapDelayNs=*/0, kBerDisabledLog10);
  channel.attach(port1_.get());
  channel.attach(port2_.get());
  const EthernetPort port3(&simCtx_, kDataRate);

  EXPECT_THAT(
      [&]() { std::ignore = channel.getPeer(port3); },
      ::testing::ThrowsMessage<std::runtime_error>(
          ::testing::HasSubstr("not attached to this channel")));
}

TEST_F(EthChannelTest, GetPeerRejectsAMissingFarEnd) {
  EthernetChannel channel(
      makeDelayConfig(0), /*flapDelayNs=*/0, kBerDisabledLog10);
  channel.attach(port1_.get());

  EXPECT_THAT(
      [&]() { std::ignore = channel.getPeer(*port1_); },
      ::testing::ThrowsMessage<std::runtime_error>(
          ::testing::HasSubstr("no other port is attached")));
}

TEST_F(EthChannelTest, GetPeerRejectsAPortAttachedAtBothEnds) {
  EthernetChannel channel(
      makeDelayConfig(0), /*flapDelayNs=*/0, kBerDisabledLog10);
  channel.attach(port1_.get());
  channel.attach(port1_.get());

  EXPECT_THAT(
      [&]() { std::ignore = channel.getPeer(*port1_); },
      ::testing::ThrowsMessage<std::runtime_error>(
          ::testing::HasSubstr("no other port is attached")));
}

TEST_F(EthChannelTest, ScheduleRemoteReceiveDeliversToPeerPort) {
  EthernetChannel channel(
      makeDelayConfig(0), /*flapDelayNs=*/0, kBerDisabledLog10);
  port1_->attach(&channel);
  port2_->attach(&channel);

  const auto pkt = createPacket<PacketType::UDP>(kPayloadBytes);
  const Time deliveryDelay = Time::fromNs(500);

  simCtx_.scheduleNow([this, &channel, pkt, deliveryDelay]() {
    channel.scheduleRemoteReceive(pkt, port1_.get(), deliveryDelay);
  });
  scheduler_.run();

  ASSERT_EQ(port2RecvQ_.size(), 1u);
  EXPECT_EQ(port2RecvQ_.front().timeNs, deliveryDelay.getNs());
  EXPECT_EQ(port1RecvQ_.size(), 0u);
}

TEST_F(EthChannelTest, ScheduleRemoteReceiveResolvesPeerFromSecondEnd) {
  // When the source is the second-attached port (end 1), the peer must
  // resolve to the first-attached port (end 0).
  EthernetChannel channel(
      makeDelayConfig(0), /*flapDelayNs=*/0, kBerDisabledLog10);
  port1_->attach(&channel);
  port2_->attach(&channel);

  const auto pkt = createPacket<PacketType::UDP>(kPayloadBytes);
  const Time deliveryDelay = Time::fromNs(500);

  simCtx_.scheduleNow([this, &channel, pkt, deliveryDelay]() {
    channel.scheduleRemoteReceive(pkt, port2_.get(), deliveryDelay);
  });
  scheduler_.run();

  ASSERT_EQ(port1RecvQ_.size(), 1u);
  EXPECT_EQ(port1RecvQ_.front().timeNs, deliveryDelay.getNs());
  EXPECT_EQ(port2RecvQ_.size(), 0u);
}

TEST_F(EthChannelTest, SendPacketDeliversToExplicitDestination) {
  // Unlike `scheduleRemoteReceive`, `sendPacket` does NOT resolve a peer;
  // it delivers to the destination argument exactly. Passing the source
  // port as the destination must deliver back to that same port.
  EthernetChannel channel(
      makeDelayConfig(0), /*flapDelayNs=*/0, kBerDisabledLog10);
  port1_->attach(&channel);
  port2_->attach(&channel);

  const auto pkt = createPacket<PacketType::UDP>(kPayloadBytes);
  const Time deliveryDelay = Time::fromNs(500);

  simCtx_.scheduleNow([this, &channel, pkt, deliveryDelay]() {
    channel.sendPacket(pkt, port1_.get(), deliveryDelay);
  });
  scheduler_.run();

  ASSERT_EQ(port1RecvQ_.size(), 1u);
  EXPECT_EQ(port1RecvQ_.front().timeNs, deliveryDelay.getNs());
  EXPECT_EQ(port2RecvQ_.size(), 0u);
}

TEST_F(EthChannelTest, AttachWithFlapDelayBringsChannelDownAfterDelay) {
  constexpr uint64_t kFlapDelayNs = 1000;
  EthernetChannel channel(makeDelayConfig(0), kFlapDelayNs, kBerDisabledLog10);
  port1_->attach(&channel);
  port2_->attach(&channel);
  ASSERT_EQ(port1_->getChannelState(), ChannelState::UP);

  bool wasUpBeforeFlap = false;
  simCtx_.schedule(Time::fromNs(kFlapDelayNs / 2), [this, &wasUpBeforeFlap]() {
    wasUpBeforeFlap = port1_->getChannelState() == ChannelState::UP;
  });
  scheduler_.run();

  EXPECT_TRUE(wasUpBeforeFlap);
  // attach() schedules the flap on each attached port independently, so
  // both ends of the link transition DOWN.
  EXPECT_EQ(port1_->getChannelState(), ChannelState::DOWN);
  EXPECT_EQ(port2_->getChannelState(), ChannelState::DOWN);
}

TEST_F(EthChannelTest, AttachWithoutFlapDelayKeepsChannelUp) {
  // With flapDelayNs == 0, attach() must NOT schedule a link-down event; the
  // channel stays UP for the whole simulation (the negative of the flap path).
  EthernetChannel channel(
      makeDelayConfig(0), /*flapDelayNs=*/0, kBerDisabledLog10);
  port1_->attach(&channel);
  port2_->attach(&channel);

  scheduler_.run();

  EXPECT_EQ(port1_->getChannelState(), ChannelState::UP);
  EXPECT_EQ(port2_->getChannelState(), ChannelState::UP);
}

} // namespace packsim
