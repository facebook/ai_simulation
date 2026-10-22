/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <array>
#include <cstdint>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <folly/Utility.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/switch_config_types.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/queues/queues.h"
// @oss-disable

namespace packsim {
namespace {

using DscpType = Ipv6Packet::DscpType;
using packsim::thrift::PacketColor;
using packsim::thrift::QueueProfileType;
using packsim::thrift::SchedulingAlgorithmType;
using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

constexpr uint32_t kPayloadBytes = 64;

// Both ECN thresholds of the uncolored VOQ_ECN queues in
// QueuesMarkVoqEcnPacketTest, as a number of the packets `pushPackets` queues.
constexpr uint32_t kVoqMarkThresholdPkts = 2;

// Queues in QueuesMarkVoqEcnPacketTest.ReadsOnlyTheQueueThePacketLeft:
// kCongestedQueue holds kVoqMarkThresholdPkts packets while the packet popped
// from kSenderQueue leaves that queue empty.
constexpr uint32_t kNumVoqEcnQueues = 2;
constexpr uint8_t kCongestedQueue = 0;
constexpr uint8_t kSenderQueue = 1;

// DSCP value whose filter mapping routes a packet to queue i.
constexpr std::array<DscpType, 8> kDscpForQueue = {
    DscpType::DSCP_DEFAULT,
    DscpType::DSCP_CS1,
    DscpType::DSCP_CS2,
    DscpType::DSCP_CS3,
    DscpType::DSCP_CS4,
    DscpType::DSCP_CS5,
    DscpType::DSCP_CS6,
    DscpType::DSCP_CS7};

packsim::thrift::QueueFilterConfig
makeFilterCfg(uint32_t numQueues, uint32_t minBytes, uint32_t maxBytes) {
  packsim::thrift::QueueFilterConfig cfg;
  for (uint8_t queue = 0; queue < numQueues; ++queue) {
    cfg.fieldToQueue()[folly::to_underlying(kDscpForQueue.at(queue))] = queue;
  }
  cfg.minQSizeThreshold() = minBytes;
  cfg.maxQSizeThreshold() = maxBytes;
  return cfg;
}

// Per-queue configs with thresholds large enough that nothing is ever dropped,
// so occupancy is driven purely by what the test pushes and pops.
std::vector<packsim::thrift::PortQueueConfig> makeNoDropQueueCfgs(
    uint32_t numQueues) {
  constexpr uint32_t kHugeThreshold = 1U << 24;
  std::vector<packsim::thrift::PortQueueConfig> cfgs{numQueues};
  for (auto& pqc : cfgs) {
    auto& c = *pqc.wredEcnCfg();
    c.queueProfileCfg() = QueueProfileType::WRED_ECN;
    c.minQSizeWRED() = kHugeThreshold;
    c.maxQSizeWRED() = kHugeThreshold;
    c.dropRateWRED() = 100;
    c.minQSizeECN() = kHugeThreshold;
    c.maxQSizeECN() = kHugeThreshold;
    c.markRateECN() = 100;
  }
  return cfgs;
}

// Per-queue configs that drop every packet that is not ECN-capable: WRED's
// maximum threshold is an empty queue.
std::vector<packsim::thrift::PortQueueConfig> makeDropAllQueueCfgs(
    uint32_t numQueues) {
  auto cfgs = makeNoDropQueueCfgs(numQueues);
  for (auto& pqc : cfgs) {
    auto& c = *pqc.wredEcnCfg();
    c.minQSizeWRED() = 0;
    c.maxQSizeWRED() = 0;
  }
  return cfgs;
}

/**
 * Shared Arrange for the `Queues` suites: a sim context, a drop-free
 * multi-queue `Queues` whose tx thresholds are expressed as whole packets, and
 * helpers that move a known number of packets through it.
 */
class QueuesTestBase : public ::testing::Test {
 protected:
  void SetUp() override {
    pktBytes_ = createPacket<PacketType::UDP>(kPayloadBytes)->getSize();
    ASSERT_GT(pktBytes_, 0u);
  }

  // Reopens at kMinPkts packets queued, closes above kMaxPkts.
  static constexpr uint32_t kMinPkts = 2;
  static constexpr uint32_t kMaxPkts = 4;

  Queues makeQueues(uint32_t numQueues) {
    return Queues(
        &simCtx_,
        makeFilterCfg(numQueues, kMinPkts * pktBytes_, kMaxPkts * pktBytes_),
        SchedulingAlgorithmType::RR,
        makeNoDropQueueCfgs(numQueues));
  }

  // Fails fatally on a mis-filtered or dropped packet; call sites must wrap
  // this in ASSERT_NO_FATAL_FAILURE so a broken Arrange stops the test.
  void pushPackets(Queues& queues, uint8_t q, uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
      auto pkt = createPacket<PacketType::UDP>(kPayloadBytes);
      pkt.as<UdpPacket>()->dscp = kDscpForQueue.at(q);
      const auto [cos, enqueued] = queues.pushPacket(pkt);
      ASSERT_EQ(cos, q);
      ASSERT_TRUE(enqueued);
    }
  }

  // Fails fatally if any pop finds no eligible queue; wrap in
  // ASSERT_NO_FATAL_FAILURE at the call site.
  void popPackets(Queues& queues, uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
      PacketPtr pkt;
      ASSERT_TRUE(queues.popPacket(pkt).has_value());
    }
  }

  Scheduler sched_;
  SimContext simCtx_{&sched_, /*seed=*/0};
  uint32_t pktBytes_{0};
};

/**
 * Exercises the tx-queue-full hysteresis: occupancy above the max threshold
 * closes the queue, and only a drain back to the min threshold reopens it and
 * notifies the producer.
 *
 * Thresholds are expressed as whole packets so a test can land the queue
 * *exactly* on a threshold and pin down which comparison is inclusive.
 */
class QueuesTxAvailabilityTest : public QueuesTestBase {
 protected:
  // Records every reopen notification so a test can assert on both the count
  // and the queue reported.
  void recordCallback(Queues& queues) {
    queues.setOnTxQueueSpaceAvailableCallback(
        [this](uint8_t cos) { reopenedQueues_.emplace_back(cos); });
  }

  std::vector<uint32_t> reopenedQueues_;
};

// The close comparison is strict (`> max`), so sitting exactly on the max
// threshold must leave the queue open to the producer.
TEST_F(QueuesTxAvailabilityTest, QueueStaysOpenAtExactlyTheMaxThreshold) {
  Queues queues = makeQueues(1);
  recordCallback(queues);
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, 0, kMaxPkts));

  queues.sendPendingTxQueueBytes(0);

  EXPECT_EQ(queues.getQueueLengthInBytes(0), kMaxPkts * pktBytes_);
  EXPECT_TRUE(queues.isTxQueueAvailable(0));
  EXPECT_TRUE(reopenedQueues_.empty());
}

TEST_F(QueuesTxAvailabilityTest, QueueClosesOnceOccupancyExceedsMaxThreshold) {
  Queues queues = makeQueues(1);
  recordCallback(queues);
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, 0, kMaxPkts + 1));

  queues.sendPendingTxQueueBytes(0);

  EXPECT_FALSE(queues.isTxQueueAvailable(0));
  EXPECT_TRUE(reopenedQueues_.empty())
      << "closing the queue must not notify the producer";
}

// A queue that never closed has nothing to reopen, so repeated polling while
// it is open must stay silent.
TEST_F(QueuesTxAvailabilityTest, NoCallbackWhileTheQueueNeverClosed) {
  Queues queues = makeQueues(1);
  recordCallback(queues);
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, 0, kMinPkts));

  queues.sendPendingTxQueueBytes(0);
  queues.sendPendingTxQueueBytes(0);
  queues.sendPendingTxQueueBytes(0);

  EXPECT_TRUE(queues.isTxQueueAvailable(0));
  EXPECT_TRUE(reopenedQueues_.empty());
}

// Hysteresis: dropping back under the max is not enough, occupancy has to fall
// all the way to the min threshold.
TEST_F(QueuesTxAvailabilityTest, ClosedQueueStaysClosedJustAboveMinThreshold) {
  Queues queues = makeQueues(1);
  recordCallback(queues);
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, 0, kMaxPkts + 1));
  queues.sendPendingTxQueueBytes(0);
  ASSERT_FALSE(queues.isTxQueueAvailable(0));

  ASSERT_NO_FATAL_FAILURE(popPackets(queues, kMaxPkts + 1 - (kMinPkts + 1)));
  queues.sendPendingTxQueueBytes(0);

  EXPECT_EQ(queues.getQueueLengthInBytes(0), (kMinPkts + 1) * pktBytes_);
  EXPECT_FALSE(queues.isTxQueueAvailable(0));
  EXPECT_TRUE(reopenedQueues_.empty());
}

// The reopen comparison is inclusive (`<= min`), so landing exactly on the min
// threshold is already enough to reopen the queue.
TEST_F(QueuesTxAvailabilityTest, ReopensAtExactlyMinThreshold) {
  Queues queues = makeQueues(1);
  recordCallback(queues);
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, 0, kMaxPkts + 1));
  queues.sendPendingTxQueueBytes(0);
  ASSERT_FALSE(queues.isTxQueueAvailable(0));

  ASSERT_NO_FATAL_FAILURE(popPackets(queues, kMaxPkts + 1 - kMinPkts));
  queues.sendPendingTxQueueBytes(0);

  EXPECT_EQ(queues.getQueueLengthInBytes(0), kMinPkts * pktBytes_);
  EXPECT_TRUE(queues.isTxQueueAvailable(0));
  const std::vector<uint32_t> expected = {0};
  EXPECT_EQ(reopenedQueues_, expected);
}

// The notification is an edge, not a level: once the queue has reopened,
// further polls at the same occupancy must not re-notify the producer.
TEST_F(QueuesTxAvailabilityTest, ReopenNotificationFiresOnlyOnce) {
  Queues queues = makeQueues(1);
  recordCallback(queues);
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, 0, kMaxPkts + 1));
  queues.sendPendingTxQueueBytes(0);
  ASSERT_NO_FATAL_FAILURE(popPackets(queues, kMaxPkts + 1 - kMinPkts));
  queues.sendPendingTxQueueBytes(0);
  ASSERT_EQ(reopenedQueues_.size(), 1u);

  queues.sendPendingTxQueueBytes(0);
  queues.sendPendingTxQueueBytes(0);

  EXPECT_TRUE(queues.isTxQueueAvailable(0));
  EXPECT_EQ(reopenedQueues_.size(), 1u);
}

// Availability is per-CoS state, so a queue that overflowed must not mark its
// peers unavailable, and the reopen notification must name the queue that
// actually recovered.
TEST_F(QueuesTxAvailabilityTest, AvailabilityIsTrackedPerQueue) {
  Queues queues = makeQueues(8);
  recordCallback(queues);
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, 3, kMaxPkts + 1));
  queues.sendPendingTxQueueBytes(3);
  ASSERT_FALSE(queues.isTxQueueAvailable(3));
  ASSERT_TRUE(queues.isTxQueueAvailable(5));

  // Queue 3 is the only occupied queue, so every pop drains it.
  ASSERT_NO_FATAL_FAILURE(popPackets(queues, kMaxPkts + 1 - kMinPkts));
  queues.sendPendingTxQueueBytes(3);

  EXPECT_TRUE(queues.isTxQueueAvailable(3));
  EXPECT_TRUE(queues.isTxQueueAvailable(5));
  const std::vector<uint32_t> expected = {3};
  EXPECT_EQ(reopenedQueues_, expected);
}

using QueuesPopPacketTest = QueuesTestBase;

// On a Queues that has never had a successful push, the pop is refused by the
// `queuedPkts_ == 0` guard before any queue is consulted, and the caller's
// packet slot is left untouched rather than cleared.
TEST_F(QueuesPopPacketTest, PopPacketReportsSentinelBeforeTheFirstPush) {
  Queues queues = makeQueues(8);
  PacketPtr pkt = createPacket<PacketType::UDP>(kPayloadBytes);
  const auto* untouched = pkt.get();

  EXPECT_FALSE(queues.popPacket(pkt).has_value());

  EXPECT_EQ(pkt.get(), untouched);
}

// The same "nothing to pop" situation reached the other way round. popPacket()
// never decrements queuedPkts_ -- only pushPacket() increments it -- so once a
// queue has been pushed to, the counter stays non-zero forever and the guard
// above is never taken again. A drained queue therefore falls through to the
// no-eligible-queue path instead, which DOES clear the caller's packet. The
// two sentinel paths are indistinguishable to a caller reading only the return
// value, so pin the difference rather than leaving it to be discovered.
TEST_F(QueuesPopPacketTest, PopPacketClearsOutputOnAQueueDrainedAfterAPush) {
  Queues queues = makeQueues(8);
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, 3, 1));
  ASSERT_NO_FATAL_FAILURE(popPackets(queues, 1));
  PacketPtr pkt = createPacket<PacketType::UDP>(kPayloadBytes);

  EXPECT_FALSE(queues.popPacket(pkt).has_value());

  EXPECT_EQ(pkt.get(), nullptr)
      << "a drained queue takes the no-eligible-queue path, which clears the "
         "caller's packet, unlike the never-pushed case above";
}

// Packets are queued but the gate rejects every head packet, so the scheduler
// picks nothing; that path must clear the output packet and leave the queue
// intact for a later pop.
TEST_F(QueuesPopPacketTest, PopPacketClearsOutputWhenNoQueueIsEligible) {
  Queues queues = makeQueues(8);
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, 3, 1));
  queues.setTxGate([](const PacketPtr&) { return false; });
  PacketPtr pkt = createPacket<PacketType::UDP>(kPayloadBytes);
  ASSERT_NE(pkt.get(), nullptr);

  EXPECT_FALSE(queues.popPacket(pkt).has_value());

  EXPECT_EQ(pkt.get(), nullptr);
  EXPECT_EQ(queues.getQueueLengthInPkts(3), 1u);
}

// makeQueues() uses WRED_ECN, which never marks a Not-ECT packet whatever the
// queue depth, so the pop must not report one as marked.
TEST_F(QueuesPopPacketTest, PopPacketDoesNotReportANonEctPacketAsMarked) {
  Queues queues = makeQueues(1);
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, 0, 1));
  PacketPtr pkt;
  bool marked = true;

  ASSERT_EQ(queues.popPacket(pkt, &marked), 0u);

  ASSERT_NE(pkt.get(), nullptr);
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_NOT_ECT);
  EXPECT_FALSE(marked);
}

using QueuesPushPacketByMoveTest = QueuesTestBase;

TEST_F(QueuesPushPacketByMoveTest, MovesAnEnqueuedPacketIntoItsQueue) {
  Queues queues = makeQueues(1);
  PacketPtr pkt = createPacket<PacketType::UDP>(kPayloadBytes);
  const auto* packet = pkt.get();

  const auto [cos, enqueued] = queues.pushPacketByMove(pkt);

  ASSERT_TRUE(enqueued);
  EXPECT_EQ(pkt.get(), nullptr);
  PacketPtr queued;
  queues.peekPacketInQueue(cos, &queued);
  EXPECT_EQ(queued.get(), packet);
}

TEST_F(QueuesPushPacketByMoveTest, LeavesADroppedPacketWithTheCaller) {
  Queues queues(
      &simCtx_,
      makeFilterCfg(1, kMinPkts * pktBytes_, kMaxPkts * pktBytes_),
      SchedulingAlgorithmType::RR,
      makeDropAllQueueCfgs(1));
  PacketPtr pkt = createPacket<PacketType::UDP>(kPayloadBytes);
  const auto* packet = pkt.get();

  const auto [cos, enqueued] = queues.pushPacketByMove(pkt);

  ASSERT_FALSE(enqueued);
  EXPECT_EQ(pkt.get(), packet);
  EXPECT_EQ(queues.getQueueLengthInPkts(cos), 0u);
}

/**
 * VOQ_ECN queues for `markVoqEcnPacket`. The uncolored ones
 * (`makeVoqEcnQueues`) set both ECN thresholds to kVoqMarkThresholdPkts
 * packets, so a packet is marked exactly when that many bytes are still on its
 * queue, and no random number is drawn. The colored one
 * (`makeColoredVoqEcnQueue`) sets them to 0 for every color, so any VOQ mark
 * it allowed would be certain.
 */
class QueuesMarkVoqEcnPacketTest : public QueuesTestBase {
 protected:
  Queues makeVoqEcnQueues(uint32_t numQueues) {
    auto cfgs = makeNoDropQueueCfgs(numQueues);
    for (auto& pqc : cfgs) {
      auto& c = *pqc.wredEcnCfg();
      c.queueProfileCfg() = QueueProfileType::VOQ_ECN;
      c.minQSizeECN() = kVoqMarkThresholdPkts * pktBytes_;
      c.maxQSizeECN() = kVoqMarkThresholdPkts * pktBytes_;
    }
    return Queues(
        &simCtx_,
        makeFilterCfg(numQueues, kMinPkts * pktBytes_, kMaxPkts * pktBytes_),
        SchedulingAlgorithmType::RR,
        std::move(cfgs));
  }

  // One queue that colors packets, with a VOQ_ECN handler for every color
  // whose thresholds are 0, so a VOQ mark by any of them would be certain.
  Queues makeColoredVoqEcnQueue() {
    auto filterCfg =
        makeFilterCfg(1, kMinPkts * pktBytes_, kMaxPkts * pktBytes_);
    filterCfg.fieldToColor()[folly::to_underlying(DscpType::DSCP_CS1)] =
        PacketColor::YELLOW;
    auto cfgs = makeNoDropQueueCfgs(1);
    auto colorCfg = *cfgs.at(0).wredEcnCfg();
    colorCfg.queueProfileCfg() = QueueProfileType::VOQ_ECN;
    colorCfg.minQSizeECN() = 0;
    colorCfg.maxQSizeECN() = 0;
    for (const auto color :
         {PacketColor::GREEN, PacketColor::YELLOW, PacketColor::RED}) {
      cfgs.at(0).coloredWredEcnCfgs()[color] = colorCfg;
    }
    return Queues(
        &simCtx_,
        std::move(filterCfg),
        SchedulingAlgorithmType::RR,
        std::move(cfgs));
  }
};

// One packet popped from kVoqMarkThresholdPkts + 1 leaves the threshold queued.
TEST_F(QueuesMarkVoqEcnPacketTest, MarksWhenTheBytesLeftReachTheThreshold) {
  Queues queues = makeVoqEcnQueues(/*numQueues=*/1);
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, 0, kVoqMarkThresholdPkts + 1));
  PacketPtr pkt;
  ASSERT_EQ(queues.popPacket(pkt), 0u);
  bool marked = false;

  queues.markVoqEcnPacket(pkt, 0, &marked);

  EXPECT_TRUE(marked);
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_CE);
}

// The popped packet no longer counts: the queue held the threshold before the
// pop, and one packet fewer after it.
TEST_F(QueuesMarkVoqEcnPacketTest, LeavesThePacketUnmarkedBelowTheThreshold) {
  Queues queues = makeVoqEcnQueues(/*numQueues=*/1);
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, 0, kVoqMarkThresholdPkts));
  PacketPtr pkt;
  ASSERT_EQ(queues.popPacket(pkt), 0u);
  bool marked = true;

  queues.markVoqEcnPacket(pkt, 0, &marked);

  EXPECT_FALSE(marked);
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_NOT_ECT);
}

// `marked` starts as the mark the queue's own profile reported, WRED_ECN in
// makeQueues(), and markVoqEcnPacket leaves it as it is.
TEST_F(QueuesMarkVoqEcnPacketTest, LeavesTheMarkOfAnotherProfileAsItWas) {
  Queues queues = makeQueues(1);
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, 0, kMaxPkts));
  PacketPtr pkt;
  ASSERT_EQ(queues.popPacket(pkt), 0u);
  bool marked = true;

  queues.markVoqEcnPacket(pkt, 0, &marked);

  EXPECT_TRUE(marked);
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_NOT_ECT);
}

// Each queue is its own virtual output queue: the bytes another queue of the
// same port holds do not count.
TEST_F(QueuesMarkVoqEcnPacketTest, ReadsOnlyTheQueueThePacketLeft) {
  Queues queues = makeVoqEcnQueues(kNumVoqEcnQueues);
  ASSERT_NO_FATAL_FAILURE(
      pushPackets(queues, kCongestedQueue, kVoqMarkThresholdPkts));
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, kSenderQueue, /*count=*/1));
  PacketPtr pkt;
  ASSERT_TRUE(queues.popPacketFromQueue(kSenderQueue, pkt));
  bool marked = true;

  queues.markVoqEcnPacket(pkt, kSenderQueue, &marked);

  EXPECT_FALSE(marked);
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_NOT_ECT);
}

// A queue that colors packets is never VOQ-marked, whatever its colors'
// profiles.
TEST_F(QueuesMarkVoqEcnPacketTest, LeavesAPacketFromAColoredQueueAlone) {
  Queues queues = makeColoredVoqEcnQueue();
  ASSERT_NO_FATAL_FAILURE(pushPackets(queues, /*q=*/0, /*count=*/1));
  PacketPtr pkt;
  ASSERT_EQ(queues.popPacket(pkt), 0u);
  bool marked = false;

  queues.markVoqEcnPacket(pkt, 0, &marked);

  EXPECT_FALSE(marked);
  EXPECT_EQ(pkt.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_NOT_ECT);
}

/**
 * A single-queue `Queues` that colors packets: CS1 maps to YELLOW and every
 * other DSCP falls back to GREEN, but the queue configures GREEN only.
 */
class QueuesColorTest : public QueuesTestBase {
 protected:
  Queues makeGreenOnlyQueues() {
    auto filterCfg =
        makeFilterCfg(1, kMinPkts * pktBytes_, kMaxPkts * pktBytes_);
    filterCfg.fieldToColor()[folly::to_underlying(DscpType::DSCP_CS1)] =
        PacketColor::YELLOW;
    auto queueCfgs = makeNoDropQueueCfgs(1);
    queueCfgs.at(0).coloredWredEcnCfgs()[PacketColor::GREEN] =
        *queueCfgs.at(0).wredEcnCfg();
    return Queues(
        &simCtx_,
        std::move(filterCfg),
        SchedulingAlgorithmType::RR,
        std::move(queueCfgs));
  }

  static PacketPtr packetWithDscp(DscpType dscp) {
    auto pkt = createPacket<PacketType::UDP>(kPayloadBytes);
    pkt.as<UdpPacket>()->dscp = dscp;
    return pkt;
  }
};

TEST_F(QueuesColorTest, RejectsAPacketWhoseColorHasNoConfigOnItsQueue) {
  Queues queues = makeGreenOnlyQueues();
  const PacketPtr pkt = packetWithDscp(DscpType::DSCP_CS1);
  const auto push = [&queues, &pkt]() { queues.pushPacket(pkt); };

  EXPECT_THAT(
      push,
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Queue 0 has no coloredWredEcnCfgs entry for color YELLOW")));
  EXPECT_EQ(queues.getQueueLengthInPkts(0), 0u);
}

TEST_F(QueuesColorTest, UnmappedDscpIsColoredGreenAndQueued) {
  Queues queues = makeGreenOnlyQueues();
  const PacketPtr pkt = packetWithDscp(DscpType::DSCP_DEFAULT);

  const auto [cos, enqueued] = queues.pushPacket(pkt);

  EXPECT_EQ(cos, 0u);
  EXPECT_TRUE(enqueued);
  PacketPtr popped;
  EXPECT_EQ(queues.popPacket(popped), 0u);
  EXPECT_EQ(popped.get(), pkt.get());
}

/**
 * `CosQMetaData` charges a bubble for every interval the queue spends empty
 * after it has carried data, measured on the empty -> non-empty transition.
 */
class CosQMetaDataBubbleTest : public ::testing::Test {
 protected:
  PacketPtr makePacket() const {
    return createPacket<PacketType::UDP>(kPayloadBytes);
  }

  CosQMetaData cosQ_;
};

// The interval before the very first packet is not a bubble: the queue was
// idle, not starved.
TEST_F(CosQMetaDataBubbleTest, FirstEnqueueRecordsNoBubble) {
  cosQ_.enqueueTxPacket(makePacket(), Time::fromNs(100));

  EXPECT_EQ(cosQ_.totalBubbleTimeNs, 0u);
  EXPECT_EQ(cosQ_.thisBubbleStartTimeNs, UINT64_MAX);
  EXPECT_GT(cosQ_.queuedTxBytes, 0u);
}

// Draining to empty leaves the bubble open; nothing is charged until data
// returns, so the trailing empty period is never counted.
TEST_F(CosQMetaDataBubbleTest, DrainingToEmptyDoesNotChargeUntilRefill) {
  cosQ_.enqueueTxPacket(makePacket(), Time::fromNs(100));
  PacketPtr popped;
  cosQ_.dequeueTxPacket(popped, Time::fromNs(200));

  EXPECT_EQ(cosQ_.totalBubbleTimeNs, 0u);
  EXPECT_EQ(cosQ_.thisBubbleStartTimeNs, 200u);
}

// The charge condition is strict (`ns > start`), so a refill in the very same
// nanosecond as the drain is a zero-length bubble and costs nothing.
TEST_F(CosQMetaDataBubbleTest, RefillInTheSameNanosecondChargesNoBubble) {
  cosQ_.enqueueTxPacket(makePacket(), Time::fromNs(100));
  PacketPtr popped;
  cosQ_.dequeueTxPacket(popped, Time::fromNs(200));

  cosQ_.enqueueTxPacket(makePacket(), Time::fromNs(200));

  EXPECT_EQ(cosQ_.totalBubbleTimeNs, 0u);
  EXPECT_GT(cosQ_.queuedTxBytes, 0u);
}

TEST_F(CosQMetaDataBubbleTest, BubblesAccumulateAcrossEmptyPeriods) {
  PacketPtr popped;
  cosQ_.enqueueTxPacket(makePacket(), Time::fromNs(100));
  cosQ_.dequeueTxPacket(popped, Time::fromNs(200));
  cosQ_.enqueueTxPacket(makePacket(), Time::fromNs(500));
  ASSERT_EQ(cosQ_.totalBubbleTimeNs, 300u);

  cosQ_.dequeueTxPacket(popped, Time::fromNs(600));
  cosQ_.enqueueTxPacket(makePacket(), Time::fromNs(900));

  EXPECT_EQ(cosQ_.totalBubbleTimeNs, 600u);
}

// A partial drain keeps the queue busy, so no bubble is opened.
TEST_F(CosQMetaDataBubbleTest, PartialDrainRecordsNoBubble) {
  cosQ_.enqueueTxPacket(makePacket(), Time::fromNs(100));
  cosQ_.enqueueTxPacket(makePacket(), Time::fromNs(100));
  PacketPtr popped;
  cosQ_.dequeueTxPacket(popped, Time::fromNs(200));

  cosQ_.enqueueTxPacket(makePacket(), Time::fromNs(500));

  EXPECT_EQ(cosQ_.totalBubbleTimeNs, 0u);
  EXPECT_EQ(cosQ_.thisBubbleStartTimeNs, UINT64_MAX);
}

/**
 * `CosQMetaData::txQueue` keeps `kMinPacketQueueSlots` slots allocated, so a
 * queue that drains to empty and refills reuses one buffer.
 */
class CosQMetaDataStorageTest : public ::testing::Test {
 protected:
  PacketPtr makePacket() const {
    return createPacket<PacketType::UDP>(kPayloadBytes);
  }

  // Enqueues `burstSize` packets, then expects them back in arrival order and
  // the queue empty.
  void expectBurstDrainsInArrivalOrder(uint32_t burstSize) {
    std::vector<PacketPtr> sent;
    sent.reserve(burstSize);
    for (uint32_t i = 0; i < burstSize; ++i) {
      sent.emplace_back(makePacket());
      cosQ_.enqueueTxPacket(sent.back(), Time::fromNs(i));
    }
    for (const auto& expected : sent) {
      PacketPtr popped;
      cosQ_.dequeueTxPacket(popped, Time::fromNs(burstSize));
      EXPECT_EQ(popped.get(), expected.get()) << "burst of " << burstSize;
    }
    EXPECT_TRUE(cosQ_.txQueue.empty()) << "burst of " << burstSize;
  }

  CosQMetaData cosQ_;
};

// A ring buffer that keeps its storage writes each refill into the slot after
// the previous head, so cycling one packet through empty visits the
// `kMinPacketQueueSlots` adjacent slots of one buffer. A buffer freed on drain
// puts every refill at the start of a fresh allocation: either the same
// address each time, or addresses no closer than one whole buffer apart.
TEST_F(CosQMetaDataStorageTest, RefillAfterDrainReusesTheMinimumBuffer) {
  std::set<const PacketPtr*> headSlots;
  PacketPtr popped;
  for (uint32_t i = 0; i < 4 * kMinPacketQueueSlots; ++i) {
    cosQ_.enqueueTxPacket(makePacket(), Time::fromNs(i));
    headSlots.insert(&cosQ_.txQueue.front());
    cosQ_.dequeueTxPacket(popped, Time::fromNs(i));
  }

  ASSERT_EQ(headSlots.size(), kMinPacketQueueSlots);
  EXPECT_EQ(*headSlots.rbegin() - *headSlots.begin(), kMinPacketQueueSlots - 1);
}

TEST_F(CosQMetaDataStorageTest, BurstBelowTheMinimumStaysFirstInFirstOut) {
  expectBurstDrainsInArrivalOrder(kMinPacketQueueSlots - 1);
}

TEST_F(CosQMetaDataStorageTest, BurstFillingTheMinimumStaysFirstInFirstOut) {
  expectBurstDrainsInArrivalOrder(kMinPacketQueueSlots);
}

TEST_F(CosQMetaDataStorageTest, BurstPastTheMinimumStaysFirstInFirstOut) {
  expectBurstDrainsInArrivalOrder(2 * kMinPacketQueueSlots + 1);
}

// A burst after the queue grew past the minimum and drained, shrinking back to
// the minimum, wraps inside the shrunk buffer.
TEST_F(CosQMetaDataStorageTest, BurstAfterAShrinkStaysFirstInFirstOut) {
  expectBurstDrainsInArrivalOrder(2 * kMinPacketQueueSlots + 1);

  expectBurstDrainsInArrivalOrder(kMinPacketQueueSlots + 1);
}

} // namespace
} // namespace packsim
