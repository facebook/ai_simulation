/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <memory>
#include <string>

#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/switch_config_types.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/queues/queue-packet-handler.h"

namespace packsim {
namespace {

using QueueProfileType = packsim::thrift::QueueProfileType;

// Queue occupancies chosen relative to the kMinThreshold/kMaxThreshold pair so
// that every admit/mark decision below is deterministic: at or above the max
// threshold the handler always acts, below the min threshold it never does.
// The two tests that sit mid-curve do reach the handler's Bernoulli draw, but
// only with a 0% drop/mark rate, where the probability is exactly 1.0 or 0.0.
constexpr uint32_t kMinThreshold = 10;
constexpr uint32_t kMaxThreshold = 100;
constexpr uint32_t kBelowMin = 5;
constexpr uint32_t kAboveMax = 200;

/**
 * Fixture for the WRED / ECN / VoqECN / WRED+ECN packet handlers.
 *
 * The averaging period is zero so that the handler's average-queue-length
 * tracker reports whatever setQLen() was last given, with no dependence on
 * simulation time. That keeps the threshold comparisons under test isolated
 * from the exponentially-weighted averaging. The averaging behaviour itself is
 * covered separately at the bottom of this file, which overrides the period.
 */
class WredEcnPacketHandlerTest : public ::testing::Test {
 protected:
  static packsim::thrift::WredEcnConfig buildConfig(
      QueueProfileType type,
      uint32_t dropOrMarkPercentage) {
    packsim::thrift::WredEcnConfig cfg;
    cfg.queueProfileCfg() = type;
    cfg.exp() = 1;
    cfg.averagingPeriod() = 0;
    cfg.minQSizeWRED() = kMinThreshold;
    cfg.maxQSizeWRED() = kMaxThreshold;
    cfg.dropRateWRED() = dropOrMarkPercentage;
    cfg.minQSizeECN() = kMinThreshold;
    cfg.maxQSizeECN() = kMaxThreshold;
    cfg.markRateECN() = dropOrMarkPercentage;
    return cfg;
  }

  std::unique_ptr<QueuePacketHandler> build(
      QueueProfileType type,
      uint32_t dropOrMarkPercentage = 100) {
    return buildHandler(
        &simCtx_,
        /*id=*/0,
        buildConfig(type, dropOrMarkPercentage),
        /*queues=*/nullptr);
  }

  static PacketPtr packetWithEcn(Ipv6Packet::EcnType ecn) {
    auto pkt = createPacket<PacketType::UDP>(/*payloadSize=*/1024);
    pkt.as<UdpPacket>()->ecn = ecn;
    return pkt;
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
};

TEST_F(WredEcnPacketHandlerTest, HandlerIdIsTheQueueId) {
  auto handler = buildHandler(
      &simCtx_,
      /*id=*/7,
      buildConfig(QueueProfileType::WRED, /*dropOrMarkPercentage=*/100),
      /*queues=*/nullptr);

  EXPECT_EQ(handler->qId, 7u);
}

////////////////// WRED

TEST_F(WredEcnPacketHandlerTest, WredAdmitsBelowMinThreshold) {
  auto handler = build(QueueProfileType::WRED);
  handler->setQLen(kBelowMin);

  EXPECT_TRUE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_NOT_ECT)));
}

TEST_F(WredEcnPacketHandlerTest, WredDropsAtOrAboveMaxThreshold) {
  auto handler = build(QueueProfileType::WRED);
  handler->setQLen(kAboveMax);

  EXPECT_FALSE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_NOT_ECT)));
}

TEST_F(WredEcnPacketHandlerTest, WredWithZeroDropRateAdmitsInsideTheCurve) {
  // A 0% drop rate flattens the WRED curve, so a queue between the two
  // thresholds still admits every packet.
  auto handler = build(QueueProfileType::WRED, /*dropOrMarkPercentage=*/0);
  handler->setQLen((kMinThreshold + kMaxThreshold) / 2);

  EXPECT_TRUE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_NOT_ECT)));
}

TEST_F(WredEcnPacketHandlerTest, WredNeverMarksOnScheduling) {
  auto handler = build(QueueProfileType::WRED);
  handler->setQLen(kAboveMax);
  auto pkt = packetWithEcn(Ipv6Packet::EcnType::ECN_ECT0);

  EXPECT_FALSE(handler->handlePacketAfterScheduling(pkt));
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_ECT0);
}

TEST_F(WredEcnPacketHandlerTest, WredSetParamsRetunesTheCurve) {
  auto handler = build(QueueProfileType::WRED);
  auto* wred = dynamic_cast<WREDQueuePacketHandler*>(handler.get());
  ASSERT_NE(wred, nullptr);
  // Raise the thresholds above the occupancy that used to force a drop.
  wred->setParams(
      /*minThres=*/kAboveMax + 1,
      /*maxThres=*/kAboveMax + 2,
      /*dropPer=*/100);
  handler->setQLen(kAboveMax);

  EXPECT_TRUE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_NOT_ECT)));
}

////////////////// ECN

TEST_F(WredEcnPacketHandlerTest, EcnAlwaysAdmitsOnFiltering) {
  auto handler = build(QueueProfileType::ECN);
  handler->setQLen(kAboveMax);

  EXPECT_TRUE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_ECT0)));
}

TEST_F(WredEcnPacketHandlerTest, EcnMarksCongestionAtOrAboveMaxThreshold) {
  auto handler = build(QueueProfileType::ECN);
  handler->setQLen(kAboveMax);
  auto pkt = packetWithEcn(Ipv6Packet::EcnType::ECN_ECT0);

  EXPECT_TRUE(handler->handlePacketAfterScheduling(pkt));
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_CE);
}

TEST_F(WredEcnPacketHandlerTest, EcnLeavesPacketUnmarkedBelowMinThreshold) {
  auto handler = build(QueueProfileType::ECN);
  handler->setQLen(kBelowMin);
  auto pkt = packetWithEcn(Ipv6Packet::EcnType::ECN_ECT0);

  EXPECT_FALSE(handler->handlePacketAfterScheduling(pkt));
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_ECT0);
}

TEST_F(WredEcnPacketHandlerTest, EcnWithZeroMarkRateLeavesTheCurveUnmarked) {
  auto handler = build(QueueProfileType::ECN, /*dropOrMarkPercentage=*/0);
  handler->setQLen((kMinThreshold + kMaxThreshold) / 2);
  auto pkt = packetWithEcn(Ipv6Packet::EcnType::ECN_ECT0);

  EXPECT_FALSE(handler->handlePacketAfterScheduling(pkt));
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_ECT0);
}

TEST_F(WredEcnPacketHandlerTest, EcnSetParamsRetunesTheCurve) {
  auto handler = build(QueueProfileType::ECN);
  auto* ecn = dynamic_cast<ECNQueuePacketHandler*>(handler.get());
  ASSERT_NE(ecn, nullptr);
  ecn->setParams(
      /*minThres=*/kAboveMax + 1,
      /*maxThres=*/kAboveMax + 2,
      /*markPer=*/100);
  handler->setQLen(kAboveMax);
  auto pkt = packetWithEcn(Ipv6Packet::EcnType::ECN_ECT0);

  EXPECT_FALSE(handler->handlePacketAfterScheduling(pkt));
}

////////////////// VoqECN

TEST_F(WredEcnPacketHandlerTest, VoqEcnDefersMarkingToMarkEcnPacket) {
  // The VOQ variant is driven by the caller passing the queue length in, so
  // both of the generic hooks are no-ops.
  auto handler = build(QueueProfileType::VOQ_ECN);
  handler->setQLen(kAboveMax);
  auto pkt = packetWithEcn(Ipv6Packet::EcnType::ECN_ECT0);

  EXPECT_TRUE(handler->handlePacketAfterFiltering(pkt));
  EXPECT_FALSE(handler->handlePacketAfterScheduling(pkt));
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_ECT0);
}

TEST_F(WredEcnPacketHandlerTest, VoqEcnMarksWhenGivenACongestedQueue) {
  auto handler = build(QueueProfileType::VOQ_ECN);
  auto* voq = dynamic_cast<VoqECNQueuePacketHandler*>(handler.get());
  ASSERT_NE(voq, nullptr);
  auto pkt = packetWithEcn(Ipv6Packet::EcnType::ECN_ECT0);

  EXPECT_TRUE(voq->markEcnPacket(pkt, kAboveMax));
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_CE);
}

TEST_F(WredEcnPacketHandlerTest, VoqEcnLeavesShallowQueuesUnmarked) {
  auto handler = build(QueueProfileType::VOQ_ECN);
  auto* voq = dynamic_cast<VoqECNQueuePacketHandler*>(handler.get());
  ASSERT_NE(voq, nullptr);
  auto pkt = packetWithEcn(Ipv6Packet::EcnType::ECN_ECT0);

  EXPECT_FALSE(voq->markEcnPacket(pkt, kBelowMin));
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_ECT0);
}

TEST_F(WredEcnPacketHandlerTest, VoqEcnSetParamsRetunesTheCurve) {
  auto handler = build(QueueProfileType::VOQ_ECN);
  auto* voq = dynamic_cast<VoqECNQueuePacketHandler*>(handler.get());
  ASSERT_NE(voq, nullptr);
  voq->setParams(
      /*minThres=*/kAboveMax + 1,
      /*maxThres=*/kAboveMax + 2,
      /*markPer=*/100);
  auto pkt = packetWithEcn(Ipv6Packet::EcnType::ECN_ECT0);

  EXPECT_FALSE(voq->markEcnPacket(pkt, kAboveMax));
}

////////////////// WRED + ECN

TEST_F(WredEcnPacketHandlerTest, WredEcnDropsNonEctPacketsWhenCongested) {
  // Non-ECT traffic cannot be signalled, so the combined handler falls back to
  // dropping it.
  auto handler = build(QueueProfileType::WRED_ECN);
  handler->setQLen(kAboveMax);

  EXPECT_FALSE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_NOT_ECT)));
}

TEST_F(WredEcnPacketHandlerTest, WredEcnAdmitsEctPacketsWhenCongested) {
  // ECT traffic is admitted and marked later instead of being dropped.
  auto handler = build(QueueProfileType::WRED_ECN);
  handler->setQLen(kAboveMax);

  EXPECT_TRUE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_ECT1)));
}

TEST_F(WredEcnPacketHandlerTest, WredEcnAdmitsNonEctPacketsBelowMinThreshold) {
  auto handler = build(QueueProfileType::WRED_ECN);
  handler->setQLen(kBelowMin);

  EXPECT_TRUE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_NOT_ECT)));
}

TEST_F(WredEcnPacketHandlerTest, WredEcnMarksCongestedEctPacketsOnScheduling) {
  auto handler = build(QueueProfileType::WRED_ECN);
  handler->setQLen(kAboveMax);
  auto pkt = packetWithEcn(Ipv6Packet::EcnType::ECN_ECT0);

  EXPECT_TRUE(handler->handlePacketAfterScheduling(pkt));
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_CE);
}

TEST_F(WredEcnPacketHandlerTest, WredEcnLeavesUncongestedEctPacketsUnmarked) {
  auto handler = build(QueueProfileType::WRED_ECN);
  handler->setQLen(kBelowMin);
  auto pkt = packetWithEcn(Ipv6Packet::EcnType::ECN_ECT0);

  EXPECT_FALSE(handler->handlePacketAfterScheduling(pkt));
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_ECT0);
}

TEST_F(WredEcnPacketHandlerTest, WredEcnDoesNotReportNonEctPacketsAsMarked) {
  // Deep enough that an ECT packet would always be marked.
  auto handler = build(QueueProfileType::WRED_ECN);
  handler->setQLen(kAboveMax);
  auto pkt = packetWithEcn(Ipv6Packet::EcnType::ECN_NOT_ECT);

  EXPECT_FALSE(handler->handlePacketAfterScheduling(pkt));
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_NOT_ECT);
}

TEST_F(WredEcnPacketHandlerTest, WredEcnSetParamsRetunesBothCurves) {
  auto handler = build(QueueProfileType::WRED_ECN);
  auto* wredEcn = dynamic_cast<WREDECNQueuePacketHandler*>(handler.get());
  ASSERT_NE(wredEcn, nullptr);
  wredEcn->setParams(
      /*minThresWRED=*/kAboveMax + 1,
      /*maxThresWRED=*/kAboveMax + 2,
      /*dropPerWRED=*/100,
      /*minThresECN=*/kAboveMax + 1,
      /*maxThresECN=*/kAboveMax + 2,
      /*markPerECN=*/100);
  handler->setQLen(kAboveMax);
  auto pkt = packetWithEcn(Ipv6Packet::EcnType::ECN_ECT0);

  EXPECT_TRUE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_NOT_ECT)));
  EXPECT_FALSE(handler->handlePacketAfterScheduling(pkt));
}

////////////////// Average queue length

TEST_F(WredEcnPacketHandlerTest, ZeroAveragingPeriodTracksQueueLengthExactly) {
  // With no averaging window the tracked value follows setQLen() immediately,
  // which is the property the threshold tests above rely on.
  auto handler = build(QueueProfileType::WRED);

  handler->setQLen(kBelowMin);
  EXPECT_TRUE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_NOT_ECT)));

  handler->setQLen(kAboveMax);
  EXPECT_FALSE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_NOT_ECT)));
}

TEST_F(WredEcnPacketHandlerTest, AveragedQueueLengthLagsBehindASuddenSpike) {
  // With a non-zero averaging period the tracker is an exponentially-weighted
  // moving average seeded at 0, so a queue that jumps straight to kAboveMax is
  // still reported as empty until at least one averaging period has elapsed.
  // The first update lands at a random offset inside the first period, so only
  // "before any period" and "after many periods" are asserted here.
  constexpr uint64_t kPeriodNs = 1000;
  constexpr uint64_t kManyPeriods = 20;
  packsim::thrift::WredEcnConfig cfg =
      buildConfig(QueueProfileType::WRED, /*dropOrMarkPercentage=*/100);
  cfg.averagingPeriod() = kPeriodNs;
  auto handler = buildHandler(&simCtx_, /*id=*/0, cfg, /*queues=*/nullptr);
  handler->setQLen(kAboveMax);

  EXPECT_TRUE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_NOT_ECT)));

  // After enough periods the average has converged onto the real occupancy and
  // the handler starts dropping.
  scheduler_.run(Time::fromNs(kManyPeriods * kPeriodNs));
  EXPECT_FALSE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_NOT_ECT)));
}

TEST_F(WredEcnPacketHandlerTest, FractionalExpWeightsEachUpdate) {
  // exp = 0.5 weights each update by 2^-0.5 (about 0.707): a queue that jumps
  // from empty to kFullQueue averages 707 after one update and 914 after two,
  // both below the 990 min threshold. Later updates settle at 999, above the
  // 991 max threshold.
  constexpr uint64_t kPeriodNs = 1000;
  constexpr uint32_t kFullQueue = 1000;
  packsim::thrift::WredEcnConfig cfg =
      buildConfig(QueueProfileType::WRED, /*dropOrMarkPercentage=*/100);
  cfg.exp() = 0.5;
  cfg.averagingPeriod() = kPeriodNs;
  cfg.minQSizeWRED() = 990;
  cfg.maxQSizeWRED() = 991;
  auto handler = buildHandler(&simCtx_, /*id=*/0, cfg, /*queues=*/nullptr);
  handler->setQLen(kFullQueue);

  // The update times start at a random offset in [0, kPeriodNs], so after two
  // periods either one or two updates have run.
  scheduler_.run(Time::fromNs(2 * kPeriodNs));
  EXPECT_TRUE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_NOT_ECT)));

  scheduler_.run(Time::fromNs(20 * kPeriodNs));
  EXPECT_FALSE(handler->handlePacketAfterFiltering(
      packetWithEcn(Ipv6Packet::EcnType::ECN_NOT_ECT)));
}

TEST_F(
    WredEcnPacketHandlerTest,
    CatchUpAfterManyPeriodsMatchesReadingEveryPeriod) {
  // `stepped` is read once per period; `caughtUp` is read only after all of
  // them, so it applies every missed update in one call. With exp = 3 the
  // average settles within about 100 updates, both when the queue fills and
  // when it drains.
  constexpr uint64_t kPeriodNs = 1;
  constexpr uint64_t kPeriods = 200;
  constexpr uint64_t kFullQueue = 1'000'000;
  WREDAverageQueueLength stepped(
      &simCtx_, /*exp=*/3.0, Time::fromNs(kPeriodNs), Time::fromNs(0));
  WREDAverageQueueLength caughtUp(
      &simCtx_, /*exp=*/3.0, Time::fromNs(kPeriodNs), Time::fromNs(0));
  const auto runPeriods = [&]() {
    for (uint64_t i = 0; i < kPeriods; ++i) {
      scheduler_.run(Time::fromNs(kPeriodNs));
      stepped();
    }
  };

  stepped.setQLen(kFullQueue);
  caughtUp.setQLen(kFullQueue);
  runPeriods();
  // Truncating each update leaves the average 7 below the queue length.
  EXPECT_EQ(stepped(), 999'993u);
  EXPECT_EQ(caughtUp(), stepped());

  stepped.setQLen(0);
  caughtUp.setQLen(0);
  runPeriods();
  EXPECT_EQ(stepped(), 0u);
  EXPECT_EQ(caughtUp(), stepped());
}

} // namespace
} // namespace packsim
