/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/link-layer-scheduler.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <folly/memory/not_null.h>
#include <folly/portability/GFlags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/link-layer/cbfc/cbfc.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/link-layer/llr/llr.h"
#include "packsim/link-layer/oss_tests/lambda-port-observer.h"
#include "packsim/link-layer/oss_tests/link-layer-scheduler-test-fixture.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim::wire {
namespace {

// Lifecycle and mid-transmit-teardown tests for `LinkLayerScheduler`, plus the
// `EthernetPort` CBFC behavior that needs a real port on each end of the
// channel: CtlOS preemption and spacing, CC_Update transmission, PortStats
// stamping, the TX gate across a link flap, LLR enabled or disabled on a CBFC
// port, and `enableCbfc` config validation. LLR replay/admission and priority
// polling are covered by `eth-port-test`, `llr-tx-test` and the
// topology-level integration tests.

// PDES event target for the mid-frame drain. The drain must be a scheduled
// event, not a direct call between two `run()`s: `PdesEngine::run()` rewinds
// each context's current time back to the last executed event, so after a run
// that executed nothing `now()` is not the horizon — and `SimStopwatch`
// measures elapsed against it.
void drainOnCbfc(
    folly::not_null<packsim::cbfc::Cbfc*> cbfcModule,
    packsim::PacketPtr pkt) {
  cbfcModule->onPacketDrained(*pkt);
}

void probeCfUpdateCount(
    folly::not_null<std::vector<uint64_t>*> out,
    folly::not_null<packsim::cbfc::Cbfc*> cbfcModule) {
  out->emplace_back(cbfcModule->stats().cfUpdatesRcvd);
}

TEST_F(LinkLayerSchedulerTest, ConstructsIdle) {
  auto driver = makeDriver();
  EXPECT_FALSE(driver->isStarted());
  EXPECT_FALSE(driver->isShuttingDown());
}

TEST_F(LinkLayerSchedulerTest, StartIsIdempotent) {
  auto driver = makeDriver();
  driver->start();
  driver->start();
  EXPECT_TRUE(driver->isStarted());
  driver->shutdown();
  scheduler().run(Time::fromUs(1));
}

TEST_F(LinkLayerSchedulerTest, StartSpawnsTxDriverAndParksOnShutdown) {
  auto driver = makeDriver();
  driver->start();
  // No outbound work on port1_ — the txDriver should park on the idle
  // wait and exit cleanly when shutdown latches.
  scheduler().run(Time::fromUs(1));
  driver->shutdown();
  scheduler().run(Time::fromUs(1));
}

TEST_F(LinkLayerSchedulerTest, ShutdownWithoutStartIsSafe) {
  auto driver = makeDriver();
  driver->shutdown();
  EXPECT_TRUE(driver->isShuttingDown());
}

TEST_F(LinkLayerSchedulerTest, ShutdownIsIdempotent) {
  auto driver = makeDriver();
  driver->start();
  driver->shutdown();
  driver->shutdown();
  EXPECT_TRUE(driver->isShuttingDown());
  scheduler().run(Time::fromUs(1));
}

TEST_F(LinkLayerSchedulerTest, DestructorTriggersShutdown) {
  {
    auto driver = makeDriver();
    driver->start();
    scheduler().run(Time::fromUs(1));
    // No explicit shutdown — the destructor reset()s the owned txDriver
    // SimScopedTask, destroying its frame and cancelling any installed waiter
    // before the SimEvent members' dtor XCHECK (`head_ == nullptr`) runs.
  }
  scheduler().run(Time::fromUs(1));
}

// Tearing down the port's wire driver while a data frame is still
// serializing must destroy the txDriver coroutine — cancelling its in-flight
// wire-time stopwatch + splice race via frame-destroy — rather than abort in
// ~SimEvent / ~SimStopwatch. This is the destroy-based supersede of the
// earlier cooperative mid-transmit shutdown handling.
TEST_F(LinkLayerSchedulerTest, ShutdownDrainsInFlightDataTransmit) {
  int txCount = 0;
  observerPort1_.setOnPacketSentCallback(
      [&txCount](packsim::EthernetPort&, const packsim::PacketPtr&) {
        ++txCount;
      });

  // send() lazily sets up the port's scheduler and runs the txDriver into
  // sendDataFrame, arming the wire-time stopwatch (1000 B @ 100 Gbps ~80 ns).
  auto pkt = packsim::createPacket<packsim::PacketType::IPV6>(1000);
  port1_->send(pkt);

  // Advance well short of ~80 ns so the frame is genuinely still serializing.
  scheduler().run(Time::fromNs(10));
  ASSERT_EQ(txCount, 0)
      << "frame must still be serializing, else the test does not exercise "
         "mid-transmit teardown";

  // ~EthernetPort -> teardownWireDriver -> shutdown() (txDriver_.reset()).
  port1_.reset();

  EXPECT_EQ(txCount, 0)
      << "an in-flight frame must be abandoned on teardown, not finalized";
}

// The CtlOS analogue of ShutdownDrainsInFlightDataTransmit: tear the port
// down while the txDriver is parked on a CtlOS transmit (a CtlOS control frame
// mid-serialization, on `simScopedDelay`). The destroy-based teardown
// cancels that `simScopedDelay` on frame-destroy.
TEST_F(LinkLayerSchedulerTest, ShutdownDrainsInFlightCtlosTransmit) {
  // Enable LLR so the port has a CtlOS source; global timer OFF so the only
  // CtlOS producer is our explicit injection below. skipInitNegotiation=true
  // sets the LLR into local mode so a received LLR_INIT posts an INIT_ECHO.
  packsim::thrift::LlrConfig cfg;
  cfg.ackByteThreshold() = 16384;
  cfg.txBufferMaxBytes() = 512 * 1024;
  cfg.retryTimeoutNs() = 100000;
  cfg.skipInitNegotiation() = true;
  cfg.reInitOnDiscard() = true;
  port1_->enableLlr(cfg, /*enableGlobalTimer=*/false);

  // A received LLR_INIT posts an LLR_INIT_ECHO CtlOS; its onPost wakes
  // the scheduler (signalCtlosAvailable -> broadcastInline), which
  // synchronously resumes the txDriver in THIS stack: it pulls the CtlOS via
  // nextCtlosFrame() and parks on the CtlOS transmit's simScopedDelay (the
  // wire-serialization wait).
  port1_->llr()->rxCtlos(LlrCtlosType::LLR_INIT, /*seq=*/0, /*initData=*/0);

  // Precondition: the CtlOS was pulled for transmit (slots drained) and
  // is still serializing — we have NOT run the scheduler, so the wire-time
  // simScopedDelay has not fired. Else the test would not exercise mid-CtlOS
  // teardown.
  ASSERT_TRUE(port1_->llr()->ctlosSlots().empty())
      << "CtlOS must have been pulled for transmit";

  // Tear down while parked: ~EthernetPort -> teardownLlr ->
  // teardownWireDriver -> shutdown() -> txDriver_.reset() destroys the frame at
  // the simScopedDelay co_await; SimScopedDelay's destructor cancels the
  // scheduled resume.
  port1_.reset();

  // No dangling PDES event survives: running the queue must not resume a
  // destroyed frame (a use-after-free, caught by ASan). With the old plain
  // simDelay the scheduled resume would dangle here.
  scheduler().run(Time::fromUs(1));
}

// Pins that the CBFC TX gate is reinstated after the link-down drain; see the
// clear/reinstall pair in `EthernetPort::setChannelState`.
TEST_F(LinkLayerSchedulerTest, CbfcQueueGateSurvivesALinkFlap) {
  port1_->enableCbfc(makeTestCbfcConfig());
  // The link-down drain reports every dropped packet through this. Leaving it
  // unset is harmless -- an unset handler no-ops -- so it is installed to
  // count the drain, not to avoid a failure.
  int droppedOnFlap = 0;
  observerPort1_.setOnPacketDroppedOnSendCallback(
      [&](packsim::EthernetPort&,
          const packsim::PacketPtr&,
          uint8_t,
          packsim::switches::thrift::PacketDropReason) { ++droppedOnFlap; });
  // No credit bootstrap: the sender's ledger is empty, so every lossless packet
  // fails `Cbfc::canTransmit`.
  auto losslessPkt = packsim::createPacket<packsim::PacketType::UDP>(64);
  losslessPkt.as<packsim::UdpPacket>()->setDscpFromCos(3); // CS3 -> VC1
  ASSERT_TRUE(port1_->cbfc()->isLosslessPacket(*losslessPkt));
  ASSERT_FALSE(port1_->cbfc()->canTransmit(*losslessPkt))
      << "the packet must be credit-starved, or the gate proves nothing";

  port1_->send(losslessPkt);
  ASSERT_EQ(nullptr, port1_->peekNextAppPacket().get())
      << "with the gate installed a starved queue is skipped, so the port has "
         "nothing to offer";

  port1_->setChannelState(packsim::ChannelState::DOWN);
  EXPECT_EQ(1, droppedOnFlap)
      << "the drain must reach the starved queue, which is why the gate comes "
         "off for the duration";
  port1_->setChannelState(packsim::ChannelState::UP);

  auto afterFlap = packsim::createPacket<packsim::PacketType::UDP>(64);
  afterFlap.as<packsim::UdpPacket>()->setDscpFromCos(3);
  port1_->send(afterFlap);
  EXPECT_EQ(nullptr, port1_->peekNextAppPacket().get())
      << "the gate was not reinstated after the drain -- the port is offering "
         "a packet it has no credit for";
}

// UEC 1.0.3 §5.2.7.2: "The MAC layer must be modified to allow for CtlOS
// messages to preempt the transmission of in-progress Ethernet packets."
//
// This test fails if the CBFC wake is ever routed to the scheduler's data
// signal instead of its CtlOS signal (see the wake-signal contract in
// `link-layer-scheduler.h`): a CF_Update announced on `dataAvailable_` stays
// invisible until the data frame drains.
TEST_F(LinkLayerSchedulerTest, CbfcCfUpdatePreemptsInFlightDataFrame) {
  // A 4000-byte UDP payload is 4078 B on the wire (70 B of Ethernet, IPv6 and
  // UDP headers plus the 8 B preamble) and serializes for 326.24 ns at
  // 100 Gbps. The drain fires 100 ns in, which is 1250 B: past §5.2.11.2's
  // 256-byte no-preemption prefix, so the drain point stays valid if that
  // rule is switched on, and 226 ns before the frame completes.
  constexpr uint16_t kDataPayloadBytes = 4000;
  constexpr int64_t kDrainAtNs = 100;
  constexpr int64_t kRunHorizonNs = 600;

  port1_->enableCbfc(makeTestCbfcConfig());
  port2_->enableCbfc(makeTestCbfcConfig());

  // port2 is the CBFC sender on the lossless VC; port1 is the receiver that
  // buffered port2's packet and will drain it mid-frame, owing port2 a
  // CF_Update. Priming port2's ledger keeps the credit return balanced — a
  // return against an empty ledger would wrap S_P_CU cyclically.
  auto losslessPkt = packsim::createPacket<packsim::PacketType::UDP>(0);
  losslessPkt.as<packsim::UdpPacket>()->setDscpFromCos(3); // CS3 -> VC1
  ASSERT_TRUE(port1_->cbfc()->isLosslessPacket(*losslessPkt))
      << "the drained packet must map to the credit-gated VC, otherwise no "
         "CF_Update is produced and this test proves nothing";
  port2_->cbfc()->onTransmit(*losslessPkt);
  port1_->cbfc()->onPacketStored(*losslessPkt);

  // Observation point: a CF_Update is consumed inside `doReceive` and never
  // reaches `rxCallback_`, so read the ordering off port2's counter at the
  // instant the DATA frame lands at port2.
  std::optional<uint64_t> cfUpdatesSeenWhenDataArrived;
  Time dataArrivalTime;
  observerPort2_.setOnPacketRcvdCallback(
      [&](packsim::EthernetPort&, const packsim::PacketPtr&) {
        cfUpdatesSeenWhenDataArrived = port2_->cbfc()->stats().cfUpdatesRcvd;
        dataArrivalTime = ctx().now();
      });

  auto dataPkt =
      packsim::createPacket<packsim::PacketType::UDP>(kDataPayloadBytes);
  dataPkt.as<packsim::UdpPacket>()->setDscpFromCos(0); // best-effort
  const uint32_t dataPhyBytes = dataPkt->getPhyFrameSize();
  port1_->send(dataPkt);

  ctx().schedule(
      Time::fromNs(kDrainAtNs), &drainOnCbfc, port1_->cbfc(), losslessPkt);

  scheduler().run(Time::fromNs(kRunHorizonNs));

  ASSERT_TRUE(cfUpdatesSeenWhenDataArrived.has_value())
      << "the data frame never reached port2";
  EXPECT_EQ(1u, *cfUpdatesSeenWhenDataArrived)
      << "the CF_Update did not preempt the in-flight frame — it was still "
         "queued when the data frame completed";

  // The data frame's arrival slips by exactly the spliced CtlOS's wire time,
  // which is the direct evidence that the 8-byte CtlOS went out *inside* the
  // frame rather than after it.
  const auto rate = packsim::DataRate("100Gbps");
  const Time expected = rate.bytesToTime(
      dataPhyBytes + packsim::CtlosFrame::kCtlosFrameSizeBytes);
  EXPECT_EQ(expected, dataArrivalTime)
      << "arrival did not slip by one CtlOS of wire time";
}

// UEC 1.0.3 §5.2.11.2: "CtlOS are allowed to preempt a packet except: within
// the first 256 bytes of a packet." This is the same setup as
// `CbfcCfUpdatePreemptsInFlightDataFrame` with the drain moved INSIDE that
// window and the rule turned on, so the two tests bracket the boundary: there
// the CF_Update splices, here it must wait for the frame to finish.
TEST_F(LinkLayerSchedulerTest, CtlosMayNotPreemptInsideTheNoPreemptPrefix) {
  // 10 ns at 100 Gbps is 125 B into the frame -- inside the 256 B prefix.
  constexpr uint16_t kDataPayloadBytes = 4000;
  constexpr int64_t kDrainAtNs = 10;
  constexpr int64_t kRunHorizonNs = 600;

  packsim::thrift::CtlosInsertionConfig ctlosCfg;
  ctlosCfg.noPreemptPrefixBytes() = 256;
  port1_->setCtlosInsertionRules(ctlosCfg);
  port1_->enableCbfc(makeTestCbfcConfig());
  port2_->enableCbfc(makeTestCbfcConfig());

  auto losslessPkt = packsim::createPacket<packsim::PacketType::UDP>(0);
  losslessPkt.as<packsim::UdpPacket>()->setDscpFromCos(3); // CS3 -> VC1
  ASSERT_TRUE(port1_->cbfc()->isLosslessPacket(*losslessPkt));
  port2_->cbfc()->onTransmit(*losslessPkt);
  port1_->cbfc()->onPacketStored(*losslessPkt);

  std::optional<uint64_t> cfUpdatesSeenWhenDataArrived;
  Time dataArrivalTime;
  observerPort2_.setOnPacketRcvdCallback(
      [&](packsim::EthernetPort&, const packsim::PacketPtr&) {
        cfUpdatesSeenWhenDataArrived = port2_->cbfc()->stats().cfUpdatesRcvd;
        dataArrivalTime = ctx().now();
      });

  auto dataPkt =
      packsim::createPacket<packsim::PacketType::UDP>(kDataPayloadBytes);
  dataPkt.as<packsim::UdpPacket>()->setDscpFromCos(0); // best-effort
  const uint32_t dataPhyBytes = dataPkt->getPhyFrameSize();
  port1_->send(dataPkt);
  ctx().schedule(
      Time::fromNs(kDrainAtNs), &drainOnCbfc, port1_->cbfc(), losslessPkt);

  scheduler().run(Time::fromNs(kRunHorizonNs));

  ASSERT_TRUE(cfUpdatesSeenWhenDataArrived.has_value())
      << "the data frame never reached port2";
  EXPECT_EQ(0u, *cfUpdatesSeenWhenDataArrived)
      << "the CF_Update spliced inside the no-preempt prefix";
  // The frame's wire time is unslipped, which is the direct evidence that
  // nothing went out inside it.
  const auto rate = packsim::DataRate("100Gbps");
  EXPECT_EQ(rate.bytesToTime(dataPhyBytes), dataArrivalTime)
      << "arrival slipped, so a CtlOS was spliced after all";
  // Deferred, not dropped: the CtlOS goes out at the next opportunity.
  EXPECT_EQ(1u, port2_->cbfc()->stats().cfUpdatesRcvd)
      << "the deferred CF_Update was never sent";
}

// `hasCtlosSource()` decides whether the wire driver runs the CtlOS splice
// path, so it must track exactly the producers `nextCtlosFrame()` polls.
TEST_F(LinkLayerSchedulerTest, PlainPortHasNoCtlosSource) {
  EXPECT_FALSE(port1_->txArbiter().hasCtlosSource());
}

TEST_F(LinkLayerSchedulerTest, LlrIsACtlosSource) {
  packsim::thrift::LlrConfig llrCfg;
  llrCfg.skipInitNegotiation() = true;
  port1_->enableLlr(llrCfg, /*enableGlobalTimer=*/false);

  EXPECT_TRUE(port1_->txArbiter().hasCtlosSource());
}

TEST_F(LinkLayerSchedulerTest, CbfcIsACtlosSource) {
  port1_->enableCbfc(makeTestCbfcConfig());

  EXPECT_TRUE(port1_->txArbiter().hasCtlosSource());
}

TEST_F(
    LinkLayerSchedulerTest,
    DisablingTheOnlyCtlosProducerLeavesNoCtlosSource) {
  packsim::thrift::LlrConfig llrCfg;
  llrCfg.skipInitNegotiation() = true;
  port1_->enableLlr(llrCfg, /*enableGlobalTimer=*/false);
  ASSERT_TRUE(port1_->txArbiter().hasCtlosSource());

  port1_->disableLlr();

  EXPECT_FALSE(port1_->txArbiter().hasCtlosSource());
}

// On a port with neither LLR nor CBFC the driver transmits without the CtlOS
// splice race. Each frame must still arrive one serialization time (frame plus
// inter-frame gap) plus the channel's propagation delay after it starts, and a
// queued frame must start only when the one ahead of it has left the wire.
TEST_F(
    LinkLayerSchedulerTest,
    PlainPortFramesArriveAfterSerializationAndPropagation) {
  constexpr uint64_t kPropDelayNs = 500;
  constexpr int64_t kRunHorizonNs = 2000;
  // Locals are destroyed in reverse order, so both ports detach before the
  // channel and their observers go away.
  packsim::EthernetChannel delayedChannel(kPropDelayNs);
  LambdaPortObserver senderObserver;
  LambdaPortObserver receiverObserver;
  auto sender = std::make_unique<packsim::EthernetPort>(&ctx(), "100Gbps");
  sender->setObserver(&senderObserver);
  auto receiver = std::make_unique<packsim::EthernetPort>(&ctx(), "100Gbps");
  receiver->setObserver(&receiverObserver);
  sender->attach(&delayedChannel);
  receiver->attach(&delayedChannel);
  ASSERT_FALSE(sender->isLlrEnabled());
  ASSERT_FALSE(sender->isCbfcEnabled());

  std::vector<Time> arrivals;
  receiverObserver.setOnPacketRcvdCallback(
      [&](packsim::EthernetPort&, const packsim::PacketPtr&) {
        arrivals.emplace_back(ctx().now());
      });
  const auto first = packsim::createPacket<packsim::PacketType::IPV6>(1000);
  const auto second = packsim::createPacket<packsim::PacketType::IPV6>(1500);
  sender->send(first);
  sender->send(second);

  scheduler().run(Time::fromNs(kRunHorizonNs));

  const auto rate = packsim::DataRate("100Gbps");
  const auto wireTime = [&rate](const packsim::PacketPtr& pkt) {
    return rate.bytesToTime(pkt->getPhyFrameSize()) +
        rate.bytesToTime(pkt->getInterFrameGapSize());
  };
  const Time propDelay = Time::fromNs(kPropDelayNs);
  const std::vector<Time> expected = {
      wireTime(first) + propDelay,
      wireTime(first) + wireTime(second) + propDelay};
  EXPECT_EQ(expected, arrivals);
}

// A driver that parked before its port had a CtlOS producer waits on the data
// signal alone. CBFC enabled afterwards must still get its CF_Update onto the
// wire, rather than strand it until unrelated data happens to wake the driver.
TEST_F(
    LinkLayerSchedulerTest,
    CtlosFromCbfcEnabledAfterThePlainDriverParkedIsSent) {
  // Past CF_min after the enable, so the report is not withheld; the horizon
  // stays inside CF_max, whose refresh would send a report anyway.
  constexpr int64_t kDrainAfterNs = 100;
  constexpr int64_t kFirstFrameHorizonNs = 200;
  constexpr int64_t kRunHorizonNs = 800;

  // The first send builds port1's wire driver while the port has no CtlOS
  // producer; once the frame has left the wire the driver parks idle.
  int framesRcvd = 0;
  observerPort2_.setOnPacketRcvdCallback(
      [&framesRcvd](packsim::EthernetPort&, const packsim::PacketPtr&) {
        ++framesRcvd;
      });
  port1_->send(packsim::createPacket<packsim::PacketType::UDP>(1000));
  scheduler().run(Time::fromNs(kFirstFrameHorizonNs));
  ASSERT_EQ(1, framesRcvd)
      << "the frame must have left the wire so the driver is parked idle";

  port1_->enableCbfc(makeTestCbfcConfig());
  port2_->enableCbfc(makeTestCbfcConfig());
  // port2 sent on the lossless VC and port1 drains the packet, owing port2 a
  // CF_Update.
  auto losslessPkt = packsim::createPacket<packsim::PacketType::UDP>(0);
  losslessPkt.as<packsim::UdpPacket>()->setDscpFromCos(3); // CS3 -> VC1
  ASSERT_TRUE(port1_->cbfc()->isLosslessPacket(*losslessPkt));
  port2_->cbfc()->onTransmit(*losslessPkt);
  port1_->cbfc()->onPacketStored(*losslessPkt);
  ctx().schedule(
      Time::fromNs(kDrainAfterNs), &drainOnCbfc, port1_->cbfc(), losslessPkt);

  scheduler().run(Time::fromNs(kRunHorizonNs));

  EXPECT_EQ(1u, port2_->cbfc()->stats().cfUpdatesRcvd)
      << "the CF_Update was stranded behind the data-only idle wait";
}

// `EthernetPort::getAllPortStats` must stamp the module's counters onto
// `PortStats` unchanged. Asserting equality rather than mere engagement is what
// catches a field dropped or mis-mapped in the thrift stamping.
TEST_F(LinkLayerSchedulerTest, PortStatsMirrorTheCbfcModuleCounters) {
  constexpr int64_t kRunHorizonNs = 600;
  port1_->enableCbfc(makeTestCbfcConfig());
  port2_->enableCbfc(makeTestCbfcConfig());

  auto losslessPkt = packsim::createPacket<packsim::PacketType::UDP>(0);
  losslessPkt.as<packsim::UdpPacket>()->setDscpFromCos(3); // CS3 -> VC1
  port2_->cbfc()->onTransmit(*losslessPkt);
  port1_->cbfc()->onPacketStored(*losslessPkt);
  drainOnCbfc(port1_->cbfc(), losslessPkt);

  scheduler().run(Time::fromNs(kRunHorizonNs));

  const auto portStats = port2_->getAllPortStats();
  ASSERT_FALSE(portStats.empty());
  ASSERT_TRUE(portStats.front().cbfcStats.has_value());
  EXPECT_EQ(
      port2_->cbfc()->stats().cfUpdatesRcvd,
      *portStats.front().cbfcStats->cfUpdatesRcvd());
  EXPECT_EQ(
      port1_->cbfc()->stats().creditsConsumed,
      *port1_->getAllPortStats().front().cbfcStats->creditsConsumed());
}

// UEC 1.0.3 §5.2.11.2: "CtlOS must be limited to less than 2% of link
// bandwidth ... Enforced by guaranteeing a minimum distance of at least 400 B
// between two CtlOS", and the rule holds "even if there are no packets in
// transit and the CtlOS would be separated only by IDLE characters" -- so it
// is wire time, not intervening data bytes. At 100 Gbps 400 B is 32 ns.
//
// PackSim floors CF_min at 800 B, twice the §5.2.11.2 insertion gap at any
// link rate, so for CF_Updates the insertion rule is always subsumed by CBFC's
// own spacing and can never be the constraint that defers one. It still binds
// for LLR CtlOS, which have no CF_min.
//
// The probe at `kLateProbeAtNs` is the one that matters: it pins that a
// deferred CtlOS is re-polled when the spacing budget expires, the guarantee
// `ctlosGate_` exists for (see its declaration in `link-layer-scheduler.h`).
TEST_F(
    LinkLayerSchedulerTest,
    AWithheldCtlosIsReleasedByTheNextEligibilityCheck) {
  constexpr int64_t kSecondDrainAtNs = 100; // past CF_min (64 ns at 100 Gbps)
  constexpr int64_t kEarlyProbeAtNs = 60; // still inside the first window
  constexpr int64_t kLateProbeAtNs = 200; // after the second drain released it
  constexpr int64_t kRunHorizonNs = 400; // well inside CF_max (1310 ns)

  packsim::thrift::CtlosInsertionConfig ctlosCfg;
  ctlosCfg.minSpacingBytes() = 400;
  port1_->setCtlosInsertionRules(ctlosCfg);

  // CF_min at PackSim's floor: 800 B, 64 ns at 100 Gbps.
  auto cbfcCfg = makeTestCbfcConfig();
  cbfcCfg.cfMinTimerBytes() = packsim::cbfc::kCfMinTimerMinBytes;
  port1_->enableCbfc(cbfcCfg);
  port2_->enableCbfc(cbfcCfg);

  auto pkt1 = packsim::createPacket<packsim::PacketType::UDP>(0);
  pkt1.as<packsim::UdpPacket>()->setDscpFromCos(3); // CS3 -> VC1
  auto pkt2 = packsim::createPacket<packsim::PacketType::UDP>(0);
  pkt2.as<packsim::UdpPacket>()->setDscpFromCos(3);
  ASSERT_TRUE(port1_->cbfc()->isLosslessPacket(*pkt1));
  for (const auto& p : {pkt1, pkt2}) {
    port2_->cbfc()->onTransmit(*p);
    port1_->cbfc()->onPacketStored(*p);
  }

  std::vector<uint64_t> counts;
  // Both drains fold into one report: a CF_Update carries R_VC_CF, which is
  // cumulative, not a per-drain delta.
  drainOnCbfc(port1_->cbfc(), pkt1);
  ctx().schedule(
      Time::fromNs(kSecondDrainAtNs), &drainOnCbfc, port1_->cbfc(), pkt2);
  ctx().schedule(
      Time::fromNs(kEarlyProbeAtNs),
      &probeCfUpdateCount,
      &counts,
      port2_->cbfc());
  ctx().schedule(
      Time::fromNs(kLateProbeAtNs),
      &probeCfUpdateCount,
      &counts,
      port2_->cbfc());

  scheduler().run(Time::fromNs(kRunHorizonNs));

  ASSERT_EQ(2u, counts.size()) << "both probes must have fired";
  EXPECT_EQ(0u, counts.at(0))
      << "a report inside the CF_min window must stay withheld -- expiry is "
         "not a generation condition";
  EXPECT_EQ(1u, counts.at(1))
      << "the second drain's eligibility check never released the withheld "
         "report";
}

// The rules are opt-in: existing LLR simulations are calibrated without
// §5.2.11.2, so an unset config must leave the splice path byte-identical.
TEST_F(LinkLayerSchedulerTest, CtlosInsertionRulesAreOffByDefault) {
  constexpr uint16_t kDataPayloadBytes = 4000;
  constexpr int64_t kDrainAtNs = 10; // inside 256 B, but the rule is off
  // Past CF_min (800 B == 64 ns at 100 Gbps).
  constexpr int64_t kSecondDrainAtNs = 80;
  constexpr int64_t kRunHorizonNs = 600;

  port1_->enableCbfc(makeTestCbfcConfig());
  port2_->enableCbfc(makeTestCbfcConfig());

  auto losslessPkt = packsim::createPacket<packsim::PacketType::UDP>(0);
  losslessPkt.as<packsim::UdpPacket>()->setDscpFromCos(3);
  auto losslessPkt2 = packsim::createPacket<packsim::PacketType::UDP>(0);
  losslessPkt2.as<packsim::UdpPacket>()->setDscpFromCos(3);
  for (const auto& p : {losslessPkt, losslessPkt2}) {
    port2_->cbfc()->onTransmit(*p);
    port1_->cbfc()->onPacketStored(*p);
  }

  Time dataArrivalTime;
  observerPort2_.setOnPacketRcvdCallback(
      [&](packsim::EthernetPort&, const packsim::PacketPtr&) {
        dataArrivalTime = ctx().now();
      });

  auto dataPkt =
      packsim::createPacket<packsim::PacketType::UDP>(kDataPayloadBytes);
  dataPkt.as<packsim::UdpPacket>()->setDscpFromCos(0);
  const uint32_t dataPhyBytes = dataPkt->getPhyFrameSize();
  port1_->send(dataPkt);
  ctx().schedule(
      Time::fromNs(kDrainAtNs), &drainOnCbfc, port1_->cbfc(), losslessPkt);
  // The drain above lands inside the CF_min window and nothing re-polls when
  // it closes, so a second drain past the window is what lets the report out.
  // Without it the assertion below would measure CF_min spacing, not
  // splicing.
  ctx().schedule(
      Time::fromNs(kSecondDrainAtNs),
      &drainOnCbfc,
      port1_->cbfc(),
      losslessPkt2);

  scheduler().run(Time::fromNs(kRunHorizonNs));

  const auto rate = packsim::DataRate("100Gbps");
  EXPECT_EQ(
      rate.bytesToTime(
          dataPhyBytes + packsim::CtlosFrame::kCtlosFrameSizeBytes),
      dataArrivalTime)
      << "the CtlOS did not splice, so the rules are not actually off";
}

// A `minSpacingBytes` beyond one maximum-size frame starves LLR ACK generation
// (§5.2.11.2).
TEST_F(LinkLayerSchedulerTest, RejectsCtlosMinSpacingOutsideBounds) {
  using ::testing::HasSubstr;
  using ::testing::ThrowsMessage;
  packsim::thrift::CtlosInsertionConfig cfg;
  cfg.minSpacingBytes() = 9217;
  EXPECT_THAT(
      [&] { port1_->setCtlosInsertionRules(cfg); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("minSpacingBytes")));
}

// A `noPreemptPrefixBytes` at or beyond one maximum-size frame forbids
// preemption outright, contradicting §5.2.7.2.
TEST_F(LinkLayerSchedulerTest, RejectsCtlosNoPreemptPrefixOutsideBounds) {
  using ::testing::HasSubstr;
  using ::testing::ThrowsMessage;
  packsim::thrift::CtlosInsertionConfig cfg;
  cfg.noPreemptPrefixBytes() = 9216;
  EXPECT_THAT(
      [&] { port1_->setCtlosInsertionRules(cfg); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("noPreemptPrefixBytes")));
}

// The values §5.2.11.2 actually prescribes sit inside those bounds.
TEST_F(LinkLayerSchedulerTest, AcceptsTheCtlosInsertionRulesTheSpecPrescribes) {
  packsim::thrift::CtlosInsertionConfig ok;
  ok.minSpacingBytes() = 400;
  ok.noPreemptPrefixBytes() = 256;
  EXPECT_NO_THROW(port1_->setCtlosInsertionRules(ok));
}

// UEC 1.0.3 §5.2.9 rule 3: "CC_Update packets must be LLR-eligible and carry a
// valid sequence number." A CC_Update that reaches the peer without an LLR
// sequence number escapes LLR's discard window, so a replayed frame's credits
// are freed twice.
//
// The frame is pulled from the P=1 MAC control tier by
// `popCcUpdateIfSendable()`, not from a CoS queue: CBFC builds it, so it never
// passes through `TxPacketSource`. Sequencing is what this test pins; the tier
// it is pulled from is covered by the MAC control tests above.
TEST_F(LinkLayerSchedulerTest, CcUpdateIsTransmittedAsAnLlrEligibleFrame) {
  // The CC_msg timer re-arms itself, firing at 1000 ns and again at 2000 ns, so
  // a 1500 ns horizon observes exactly one CC_Update; a longer window would
  // count several. 1000 ns is the Tbl 5-27 floor `makeCbfcConfig` enforces.
  constexpr int64_t kCcMsgTimerNs = 1000;
  constexpr int64_t kRunHorizonNs = 1500;

  packsim::thrift::LlrConfig llrCfg;
  llrCfg.skipInitNegotiation() = true;
  port1_->enableLlr(llrCfg, /*enableGlobalTimer=*/false);

  auto cbfcCfg = makeTestCbfcConfig();
  cbfcCfg.ccMsgTimerNs() = kCcMsgTimerNs;
  port1_->enableCbfc(cbfcCfg);
  port2_->enableCbfc(makeTestCbfcConfig());

  ASSERT_EQ(0u, port1_->llr()->stats().txOk);

  scheduler().run(Time::fromNs(kRunHorizonNs));

  EXPECT_EQ(1u, port2_->cbfc()->stats().ccUpdatesRcvd)
      << "the CC_Update did not reach the peer at all";
  // The discriminator: a CC_Update that bypasses LLR still reaches the peer,
  // so `ccUpdatesRcvd` cannot tell the two paths apart on its own. `txOk`
  // counts LLR-eligible frames transmitted and stays at 0 on the bypass path.
  EXPECT_EQ(1u, port1_->llr()->stats().txOk)
      << "the CC_Update was transmitted without an LLR sequence number, so "
         "LLR's gap filter cannot discard it in the discard window";
}

// The `ccUpdateLlrEligible = false` counterpart of the test above: CC_Update
// kept out of LLR, departing from UEC §5.2.9. Two things must hold together:
// the frame still reaches the peer and is still counted, but takes no sequence
// number. Port stats must count it even though `llrHeader.enabled` is false.
TEST_F(LinkLayerSchedulerTest, CcUpdateIsUnsequencedWhenLlrEligibilityIsOff) {
  constexpr int64_t kCcMsgTimerNs = 1000;
  constexpr int64_t kRunHorizonNs = 1500;

  packsim::thrift::LlrConfig llrCfg;
  llrCfg.skipInitNegotiation() = true;
  port1_->enableLlr(llrCfg, /*enableGlobalTimer=*/false);

  auto cbfcCfg = makeTestCbfcConfig();
  cbfcCfg.ccMsgTimerNs() = kCcMsgTimerNs;
  cbfcCfg.ccUpdateLlrEligible() = false;
  port1_->enableCbfc(cbfcCfg);
  port2_->enableCbfc(makeTestCbfcConfig());

  ASSERT_EQ(0u, port1_->getPortStats(0).txPkts);

  scheduler().run(Time::fromNs(kRunHorizonNs));

  ASSERT_EQ(1u, port2_->cbfc()->stats().ccUpdatesRcvd)
      << "no CC_Update was transmitted, so nothing below was exercised";
  EXPECT_EQ(0u, port1_->llr()->stats().txOk)
      << "the knob is off, so the CC_Update must take no sequence number and "
         "no replay-buffer entry";
  const auto stats = port1_->getPortStats(0);
  EXPECT_EQ(1u, stats.txPkts)
      << "an unsequenced CC_Update still reached the wire and must still be "
         "counted";
  EXPECT_EQ(packsim::EthernetPacket::kMinEthernetFrameSizeBytes, stats.txBytes);
  EXPECT_EQ(1u, port1_->cbfc()->stats().ccUpdatesSent)
      << "the dedicated counter is disjoint from the port totals, not a "
         "substitute for them";
}

// LLR enabled but the TX state machine is not carrying sequence numbers yet.
// `LLR_OFF` is the initial state and is re-entered whenever the peer drops LLR,
// so it is not transient: a CC_Update held here would be held forever. It must
// go out unsequenced instead, which is what the pre-`popCcUpdateIfSendable`
// path did via `pullNextNewFrame`'s `PASSTHROUGH_TX` branch.
TEST_F(LinkLayerSchedulerTest, CcUpdateIsSentUnsequencedWhileLlrIsOff) {
  constexpr int64_t kCcMsgTimerNs = 1000;
  constexpr int64_t kRunHorizonNs = 1500;

  packsim::thrift::LlrConfig llrCfg;
  llrCfg.skipInitNegotiation() = true;
  port1_->enableLlr(llrCfg, /*enableGlobalTimer=*/false);
  // Drop `llr_mode_remote` to put the TX SM back in `LLR_OFF` and keep it
  // there. Letting the INIT handshake stall instead would park the SM in
  // `INIT`, where `initBehavior` is BLOCK and holding the frame is correct —
  // a different branch from the one under test.
  port1_->llr()->setLlrModeRemote(false);

  auto cbfcCfg = makeTestCbfcConfig();
  cbfcCfg.ccMsgTimerNs() = kCcMsgTimerNs;
  port1_->enableCbfc(cbfcCfg);
  port2_->enableCbfc(makeTestCbfcConfig());

  // The SM transition is coroutine-driven, so it lands on the first step
  // rather than inside `setLlrModeRemote`. Step short of the CC_msg tick.
  scheduler().run(Time::fromNs(1));
  ASSERT_EQ(packsim::llr::TxState::LLR_OFF, port1_->llr()->txState());

  scheduler().run(Time::fromNs(kRunHorizonNs));

  EXPECT_EQ(1u, port2_->cbfc()->stats().ccUpdatesRcvd)
      << "the CC_Update was held instead of transmitted: credit "
         "reconciliation does not depend on LLR carrying sequence numbers, "
         "and LLR_OFF persists, so holding here stalls it indefinitely";
  EXPECT_EQ(0u, port1_->llr()->stats().txOk)
      << "no sequence number is available in LLR_OFF, so it must go out "
         "unsequenced";
  EXPECT_EQ(1u, port1_->llr()->stats().txPassthrough)
      << "an unsequenced CC_Update is a passthrough transmit like any other, "
         "so it must show up in the stat that tracks time spent outside "
         "ADVANCE";
}

// The cell-free notify must never fire for a port-generated CC_Update: it
// occupied no upstream buffer cell, so `FbSwitch::onPacketSent` would release a
// cell that was never allocated (the untagged default is port 0 / pg 0) and
// `Endpoint::onPacketSent` would hand the frame to the transport.
TEST_F(LinkLayerSchedulerTest, CcUpdateDoesNotFireTheCellFreeNotify) {
  constexpr int64_t kCcMsgTimerNs = 1000;
  constexpr int64_t kRunHorizonNs = 1500;

  std::vector<packsim::PacketType> notified;
  observerPort1_.setOnPacketSentCallback(
      [&notified](packsim::EthernetPort&, const packsim::PacketPtr& pkt) {
        notified.emplace_back(pkt->pktType);
      });

  auto cbfcCfg = makeTestCbfcConfig();
  cbfcCfg.ccMsgTimerNs() = kCcMsgTimerNs;
  port1_->enableCbfc(cbfcCfg);
  port2_->enableCbfc(makeTestCbfcConfig());

  // Best-effort (not DSCP CS3), so the credit gate does not hold it back.
  auto dataPkt = packsim::createPacket<packsim::PacketType::UDP>(512);
  dataPkt.as<packsim::UdpPacket>()->setDscpFromCos(0);
  port1_->send(dataPkt);

  scheduler().run(Time::fromNs(kRunHorizonNs));

  // Without this the rest passes vacuously: a change that stopped emitting
  // CC_Updates would leave no ETHERNET_CC to wrongly notify about.
  ASSERT_EQ(1u, port2_->cbfc()->stats().ccUpdatesRcvd)
      << "no CC_Update was transmitted, so the guard was never exercised";
  EXPECT_THAT(
      notified,
      ::testing::Not(::testing::Contains(packsim::PacketType::ETHERNET_CC)));
  // Type-scoped, not a blanket suppression: the data frame still notifies.
  EXPECT_THAT(notified, ::testing::ElementsAre(dataPkt->pktType));
}

// `disableLlr` destroys the wire driver that CBFC's wake callbacks point at,
// and the CC_msg timer fires those callbacks every tick. A port that keeps
// CBFC must get a live driver back. Otherwise the first tick after the disable
// wakes freed memory (a heap-use-after-free under ASan) and no CC_Update is
// sent.
TEST_F(LinkLayerSchedulerTest, CcUpdatesKeepFlowingAfterDisableLlr) {
  // One CC_msg tick, at 1000 ns, falls inside the 1500 ns horizon; see
  // `CcUpdateIsTransmittedAsAnLlrEligibleFrame`.
  constexpr int64_t kCcMsgTimerNs = 1000;
  constexpr int64_t kRunHorizonNs = 1500;

  packsim::thrift::LlrConfig llrCfg;
  llrCfg.skipInitNegotiation() = true;
  port1_->enableLlr(llrCfg, /*enableGlobalTimer=*/false);
  auto cbfcCfg = makeTestCbfcConfig();
  cbfcCfg.ccMsgTimerNs() = kCcMsgTimerNs;
  port1_->enableCbfc(cbfcCfg);
  port2_->enableCbfc(makeTestCbfcConfig());
  // Run with LLR still enabled, short of the CC_msg tick. `run()` rewinds to
  // the last executed event, so the horizon below still ends before the second
  // tick at 2000 ns.
  scheduler().run(Time::fromNs(1));

  port1_->disableLlr();
  ASSERT_FALSE(port1_->isLlrEnabled());
  ASSERT_TRUE(port1_->isCbfcEnabled());

  scheduler().run(Time::fromNs(kRunHorizonNs));

  EXPECT_EQ(1u, port2_->cbfc()->stats().ccUpdatesRcvd)
      << "the CC_msg tick after disableLlr reached no live wire driver";
}

// The CtlOS counterpart of the test above: a drain after `disableLlr` fires
// CBFC's CF_Update wake, which must also reach a live driver.
TEST_F(LinkLayerSchedulerTest, CfUpdatesKeepFlowingAfterDisableLlr) {
  // Past CF_min, so the report is not withheld; see
  // `AWithheldCtlosIsReleasedByTheNextEligibilityCheck`. The horizon stays
  // inside CF_max (1310 ns), whose refresh would send a report anyway.
  constexpr int64_t kDrainAtNs = 100;
  constexpr int64_t kRunHorizonNs = 600;

  packsim::thrift::LlrConfig llrCfg;
  llrCfg.skipInitNegotiation() = true;
  port1_->enableLlr(llrCfg, /*enableGlobalTimer=*/false);
  port1_->enableCbfc(makeTestCbfcConfig());
  port2_->enableCbfc(makeTestCbfcConfig());
  port1_->disableLlr();

  // port2 sent on the lossless VC and port1 drains the packet, owing port2 a
  // CF_Update.
  auto losslessPkt = packsim::createPacket<packsim::PacketType::UDP>(0);
  losslessPkt.as<packsim::UdpPacket>()->setDscpFromCos(3); // CS3 -> VC1
  ASSERT_TRUE(port1_->cbfc()->isLosslessPacket(*losslessPkt));
  port2_->cbfc()->onTransmit(*losslessPkt);
  port1_->cbfc()->onPacketStored(*losslessPkt);
  ctx().schedule(
      Time::fromNs(kDrainAtNs), &drainOnCbfc, port1_->cbfc(), losslessPkt);

  scheduler().run(Time::fromNs(kRunHorizonNs));

  EXPECT_EQ(1u, port2_->cbfc()->stats().cfUpdatesRcvd)
      << "the CF_Update owed after disableLlr reached no live wire driver";
}

// LLR's scheduler-wake callback must reach the wire driver even when the
// driver already exists at `enableLlr`, here the one `disableLlr` rebuilt for
// CBFC. Otherwise LLR's first state change finds no subscriber and XCHECKs.
// The driver exposes no wake count, so the abort is the check, plus ASan, which
// reports a callback left on a destroyed driver as a heap-use-after-free; the
// `TxState` assertion confirms the state change, and so the wake, ran.
TEST_F(LinkLayerSchedulerTest, LlrReenabledOnACbfcPortCanWakeTheWireDriver) {
  packsim::thrift::LlrConfig llrCfg;
  llrCfg.skipInitNegotiation() = true;
  port1_->enableLlr(llrCfg, /*enableGlobalTimer=*/false);
  port1_->enableCbfc(makeTestCbfcConfig());
  port1_->disableLlr();

  port1_->enableLlr(llrCfg, /*enableGlobalTimer=*/false);
  // Leaving ADVANCE is a state change, and every state change wakes the
  // driver. The transition is coroutine-driven, so it lands on the first step.
  port1_->llr()->setLlrModeRemote(false);
  scheduler().run(Time::fromNs(1));

  EXPECT_EQ(packsim::llr::TxState::LLR_OFF, port1_->llr()->txState());
}

// The same requirement, checked the same way, when CBFC, whose `enableCbfc`
// builds the wire driver eagerly, is enabled before LLR.
TEST_F(LinkLayerSchedulerTest, LlrEnabledAfterCbfcCanWakeTheWireDriver) {
  port1_->enableCbfc(makeTestCbfcConfig());
  packsim::thrift::LlrConfig llrCfg;
  llrCfg.skipInitNegotiation() = true;
  port1_->enableLlr(llrCfg, /*enableGlobalTimer=*/false);

  port1_->llr()->setLlrModeRemote(false);
  scheduler().run(Time::fromNs(1));

  EXPECT_EQ(packsim::llr::TxState::LLR_OFF, port1_->llr()->txState());
}

// A CBFC link config crosses from 32-bit thrift fields into narrower C++ types,
// so `makeCbfcConfig` validates before it translates. The tests below pin the
// rejections that would otherwise be silent truncations or a wedged
// simulation, one test per validation check so that a failure names exactly
// one field.
TEST_F(LinkLayerSchedulerTest, RejectsNumVcsOutsideTheSupportedRange) {
  // 256 + maxVcs truncates to exactly maxVcs in the `uint8_t` narrowing, so a
  // check made after the narrowing would wave it through.
  const uint32_t maxVcs = cbfc::kCbfcLimits.maxVcs;
  for (const uint32_t bad : {0u, maxVcs + 1, 256 + maxVcs}) {
    auto cfg = makeTestCbfcConfig();
    cfg.numVcs() = bad;
    cfg.isLosslessPerVc() = {true};
    cfg.vcMap()->at(0).vcId() = 0;
    expectRejected(cfg, "numVcs");
  }
}

// CBFC with no lossless VC gates nothing, and then the module's headline
// invariant (losslessVcBufferFullDrops == 0) holds vacuously.
TEST_F(LinkLayerSchedulerTest, RejectsAConfigWithNoLosslessVc) {
  auto cfg = makeTestCbfcConfig();
  cfg.isLosslessPerVc() = {false, false};
  expectRejected(cfg, "marks no VC lossless");
}

// The array is indexed by VC id, so a length that disagrees with `numVcs`
// leaves the VCs past the end ungated. `VcMapper::isLossless` reads false past
// the end, so a short array would be silently best-effort.
TEST_F(LinkLayerSchedulerTest, RejectsALosslessVcArrayThatDoesNotCoverNumVcs) {
  auto cfg = makeTestCbfcConfig();
  cfg.isLosslessPerVc() = {false}; // numVcs is 2
  expectRejected(cfg, "isLosslessPerVc");
}

TEST_F(LinkLayerSchedulerTest, RejectsMoreVcGroupsThanSupported) {
  auto cfg = makeTestCbfcConfig();
  packsim::thrift::VcGrpEntry grp;
  grp.vcIds() = {1};
  grp.limitCredits() = 4;
  cfg.vcGroups() = std::vector<packsim::thrift::VcGrpEntry>(
      size_t{cbfc::kCbfcLimits.maxVcGroups} + 1, grp);
  expectRejected(cfg, "vcGroups");
}

TEST_F(LinkLayerSchedulerTest, RejectsAVcInTwoVcGroups) {
  auto cfg = makeTestCbfcConfig();
  packsim::thrift::VcGrpEntry a;
  a.vcIds() = {1};
  packsim::thrift::VcGrpEntry b;
  b.vcIds() = {1}; // same VC in two groups
  cfg.vcGroups() = {a, b};
  expectRejected(cfg, "more than one group");
}

// Once any group exists, every lossless VC must be in one -- otherwise the
// ungrouped VC silently escapes the group tier.
TEST_F(LinkLayerSchedulerTest, RejectsALosslessVcInNoVcGroup) {
  auto cfg = makeTestCbfcConfig(); // numVcs 2, VC1 lossless
  packsim::thrift::VcGrpEntry grp;
  grp.vcIds() = {0}; // covers the best-effort VC, not the lossless one
  cfg.vcGroups() = {grp};
  expectRejected(cfg, "no VC group claims it");
}

// Each rejected value is a different near miss: one below and one above the
// accepted range, and -16, the low end of UEC §5.2.10.2's range, which is
// legal there but not modelled here.
TEST_F(LinkLayerSchedulerTest, RejectsAPktOvhdOutsideTheSupportedRange) {
  for (const int32_t bad :
       {cbfc::kCbfcLimits.minPktOvhdBytes - 1,
        cbfc::kCbfcLimits.maxPktOvhdBytes + 1,
        -16}) {
    auto cfg = makeTestCbfcConfig();
    cfg.pktOvhdBytes() = bad;
    expectRejected(cfg, "pktOvhdBytes");
  }
}

TEST_F(LinkLayerSchedulerTest, AcceptsTheSupportedPktOvhdBounds) {
  for (const int32_t good :
       {cbfc::kCbfcLimits.minPktOvhdBytes, cbfc::kCbfcLimits.maxPktOvhdBytes}) {
    auto cfg = makeTestCbfcConfig();
    cfg.pktOvhdBytes() = good;
    EXPECT_NO_THROW(port1_->enableCbfc(cfg)) << "pktOvhdBytes=" << good;
    port1_->disableCbfc();
  }
}

// A zero CF_max or CC_msg spacing wedges the sim (see `validateTimers` in
// `cbfc-config.cpp`). One test per field so a failure names which timer
// regressed.
TEST_F(LinkLayerSchedulerTest, RejectsAZeroCfMaxTimer) {
  auto cfg = makeTestCbfcConfig();
  cfg.cfMaxTimerBytes() = 0;
  expectRejected(cfg, "cfMaxTimerBytes");
}

TEST_F(LinkLayerSchedulerTest, RejectsAZeroCcMsgTimer) {
  auto cfg = makeTestCbfcConfig();
  cfg.ccMsgTimerNs() = 0;
  expectRejected(cfg, "ccMsgTimerNs");
}

TEST_F(LinkLayerSchedulerTest, RejectsACfMinTimerBelowTheSupportedFloor) {
  auto cfg = makeTestCbfcConfig();
  cfg.cfMinTimerBytes() = 799; // PackSim floor is 800
  expectRejected(cfg, "cfMinTimerBytes");
}

// Tbl 5-27 states CF_max as a range "with minimum granularity of 16 kB", so a
// value inside the range that does not land on a step of it is still illegal.
TEST_F(LinkLayerSchedulerTest, RejectsACfMaxTimerOffTheSpecGranularity) {
  auto cfg = makeTestCbfcConfig();
  cfg.cfMaxTimerBytes() = packsim::cbfc::kCfMaxTimerMinBytes + 1;
  expectRejected(cfg, "minimum granularity");
}

// A large threshold is a legal configuration: it makes the accumulation
// trigger rare, leaving CF_max to carry the reports.
TEST_F(LinkLayerSchedulerTest, AcceptsALargeCfCreditThreshold) {
  auto cfg = makeTestCbfcConfig();
  cfg.cfCreditThresholdCredits() = 16383;
  EXPECT_NO_THROW(port1_->enableCbfc(cfg));
}

// A rejected config must leave CBFC cleanly off, not half-constructed.
TEST_F(LinkLayerSchedulerTest, ARejectedConfigLeavesCbfcOff) {
  auto cfg = makeTestCbfcConfig();
  cfg.numVcs() = cbfc::kCbfcLimits.maxVcs + 1u;
  expectRejected(cfg, "numVcs");
  EXPECT_EQ(nullptr, port1_->cbfc());
}

// The valid default must still be accepted — a validator that rejects
// everything would pass every test above.
TEST_F(LinkLayerSchedulerTest, AcceptsAValidCbfcConfig) {
  port1_->enableCbfc(makeTestCbfcConfig());
  EXPECT_NE(nullptr, port1_->cbfc());
}

} // namespace
} // namespace packsim::wire
