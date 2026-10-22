/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <array>
#include <cstdint>
#include <optional>
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
#include "packsim/packet/base.h"
#include "packsim/packet/pfc.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/queues/queue-scheduler.h"
#include "packsim/queues/queues.h"
// @oss-disable

namespace packsim {
namespace {

using DscpType = Ipv6Packet::DscpType;
using packsim::thrift::QueueProfileType;
using packsim::thrift::SchedulingAlgorithmType;

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

packsim::thrift::QueueFilterConfig makeFilterCfg(uint32_t numQueues) {
  packsim::thrift::QueueFilterConfig cfg;
  for (uint8_t queue = 0; queue < numQueues; ++queue) {
    cfg.fieldToQueue()[folly::to_underlying(kDscpForQueue.at(queue))] = queue;
  }
  return cfg;
}

// Per-queue configs with thresholds large enough that nothing is ever dropped.
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

// Owns the shared PDES scheduler + sim context so each test states only what is
// unique about its scenario.
class QueueSchedulerTest : public ::testing::Test {
 protected:
  Queues makeQueues(
      uint32_t numQueues,
      SchedulingAlgorithmType schedulerType = SchedulingAlgorithmType::RR) {
    return Queues(
        &simCtx_,
        makeFilterCfg(numQueues),
        schedulerType,
        makeNoDropQueueCfgs(numQueues));
  }

  // Enqueues one packet into queue `q`.
  void occupyQueue(Queues& queues, uint8_t q) {
    auto pkt = createPacket<PacketType::UDP>(/*payloadSize=*/64);
    pkt.as<UdpPacket>()->dscp = kDscpForQueue.at(q);
    const auto [cos, enqueued] = queues.pushPacket(pkt);
    ASSERT_EQ(cos, q);
    ASSERT_TRUE(enqueued);
  }

  // Rejects only queue 1's current head packet, leaving every other occupied
  // queue eligible. Queue 1 must already hold a packet.
  void starveQueueOne(Queues& queues) {
    PacketPtr headOfOne;
    queues.peekPacketInQueue(1, &headOfOne);
    ASSERT_NE(headOfOne.get(), nullptr);
    queues.setTxGate([head = headOfOne.get()](const PacketPtr& pkt) {
      return pkt.get() != head;
    });
  }

  Scheduler sched_;
  SimContext simCtx_{&sched_, /*seed=*/0};
};

// ---- Strict Priority ----

TEST_F(QueueSchedulerTest, SpSelectsHighestPriorityOccupiedQueue) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);
  SpQueueScheduler scheduler(&queues);
  // Higher queue number wins under strict priority.
  EXPECT_EQ(scheduler.selectQueueToPop(), 3u);
}

TEST_F(QueueSchedulerTest, SpReturnsNulloptWhenAllQueuesEmpty) {
  Queues queues = makeQueues(8);
  SpQueueScheduler scheduler(&queues);
  EXPECT_EQ(scheduler.selectQueueToPop(), std::nullopt);
}

TEST_F(QueueSchedulerTest, SpSkipsPausedQueue) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);
  queues.updateTxPfcState(3, PfcState::XOFF);
  SpQueueScheduler scheduler(&queues);
  // Queue 3 is paused, so the next-highest occupied XON queue is chosen.
  EXPECT_EQ(scheduler.selectQueueToPop(), 1u);
}

TEST_F(QueueSchedulerTest, SpReturnsNulloptWhenAllOccupiedQueuesPaused) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);
  queues.updateTxPfcState(1, PfcState::XOFF);
  queues.updateTxPfcState(3, PfcState::XOFF);
  SpQueueScheduler scheduler(&queues);
  EXPECT_EQ(scheduler.selectQueueToPop(), std::nullopt);
}

// ---- Round Robin ----

TEST_F(QueueSchedulerTest, RrRotatesOverOccupiedQueues) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);
  RrQueueScheduler scheduler(&queues);
  // selectQueueToPop only reads occupancy, so repeated calls rotate.
  EXPECT_EQ(scheduler.selectQueueToPop(), 1u);
  EXPECT_EQ(scheduler.selectQueueToPop(), 3u);
  EXPECT_EQ(scheduler.selectQueueToPop(), 1u);
}

TEST_F(QueueSchedulerTest, RrStaysOnSingleOccupiedQueue) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 2);
  RrQueueScheduler scheduler(&queues);
  EXPECT_EQ(scheduler.selectQueueToPop(), 2u);
  EXPECT_EQ(scheduler.selectQueueToPop(), 2u);
}

TEST_F(QueueSchedulerTest, RrReturnsNulloptWhenAllQueuesEmpty) {
  Queues queues = makeQueues(8);
  RrQueueScheduler scheduler(&queues);
  EXPECT_EQ(scheduler.selectQueueToPop(), std::nullopt);
}

TEST_F(QueueSchedulerTest, RrSkipsPausedQueue) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);
  queues.updateTxPfcState(1, PfcState::XOFF);
  RrQueueScheduler scheduler(&queues);
  // Queue 1 is paused, so the rotation lands on the next occupied XON queue.
  EXPECT_EQ(scheduler.selectQueueToPop(), 3u);
}

TEST_F(QueueSchedulerTest, RrReturnsNulloptWhenAllOccupiedQueuesPaused) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);
  queues.updateTxPfcState(1, PfcState::XOFF);
  queues.updateTxPfcState(3, PfcState::XOFF);
  RrQueueScheduler scheduler(&queues);
  EXPECT_EQ(scheduler.selectQueueToPop(), std::nullopt);
}

TEST_F(QueueSchedulerTest, SingleQueueNextQueueSkipsPausedPacket) {
  Queues queues = makeQueues(1);
  occupyQueue(queues, 0);
  queues.updateTxPfcState(0, PfcState::XOFF);

  EXPECT_FALSE(queues.nextQueue().has_value());
}

// LLR admission (UEC 1.0.3 §5.1) and the CBFC credit gate (§5.2) act on the
// packet returned by `EthernetPort::peekNextAppPacket()`, so the
// following `getTxPacket()` must return that same packet.
TEST_F(QueueSchedulerTest, PeekThenPopReturnTheSamePacket) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);

  // Mirrors `EthernetPort::peekNextAppPacket()`.
  const auto peekCos = queues.peekQueue();
  ASSERT_TRUE(peekCos.has_value());
  PacketPtr peeked;
  queues.peekPacketInQueue(*peekCos, &peeked);
  ASSERT_NE(peeked.get(), nullptr);

  // Mirrors `EthernetPort::getTxPacket()`.
  PacketPtr popped;
  const std::optional<uint8_t> poppedCos = queues.popPacket(popped);
  ASSERT_NE(popped.get(), nullptr);
  ASSERT_TRUE(poppedCos.has_value());

  EXPECT_EQ(*peekCos, *poppedCos) << "peek and pop selected different queues";
  EXPECT_EQ(peeked.get(), popped.get())
      << "the packet popped is not the packet that was inspected";
}

// Peeking must be repeatable: it reports a choice without making one.
TEST_F(QueueSchedulerTest, PeekDoesNotRotateTheCursor) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);
  RrQueueScheduler scheduler(&queues);

  EXPECT_EQ(scheduler.peekQueueToPop(), 1u);
  EXPECT_EQ(scheduler.peekQueueToPop(), 1u) << "peeking must not rotate";
}

// Selecting commits the choice, so the following peek reports the next queue.
TEST_F(QueueSchedulerTest, PopRotatesTheCursor) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);
  RrQueueScheduler scheduler(&queues);

  EXPECT_EQ(scheduler.selectQueueToPop(), 1u);

  EXPECT_EQ(scheduler.peekQueueToPop(), 3u) << "the pop should have rotated";
}

// Popping the queue a caller already selected must not select again.
TEST_F(QueueSchedulerTest, PopPacketFromQueueDoesNotRotateTheCursor) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);
  PacketPtr headOfOne;
  queues.peekPacketInQueue(1, &headOfOne);
  ASSERT_NE(headOfOne.get(), nullptr);

  ASSERT_EQ(queues.nextQueue(), 1u);
  PacketPtr popped;
  ASSERT_TRUE(queues.popPacketFromQueue(1, popped));
  EXPECT_EQ(popped.get(), headOfOne.get());
  EXPECT_EQ(queues.getQueueLengthInPkts(1), 1u);
  EXPECT_EQ(queues.peekQueue(), 3u)
      << "popping a chosen queue must not choose again";
}

TEST_F(QueueSchedulerTest, PopPacketFromQueueReportsAnEmptyQueue) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);

  PacketPtr popped;
  EXPECT_FALSE(queues.popPacketFromQueue(2, popped));
  EXPECT_EQ(popped.get(), nullptr);
  EXPECT_EQ(queues.getQueueLengthInPkts(1), 1u);
}

// ---- Weighted Round Robin ----

TEST_F(QueueSchedulerTest, WrrConstructorThrowsOnWeightCountMismatch) {
  Queues queues = makeQueues(8);
  EXPECT_THAT(
      [&] { WrrQueueScheduler(&queues, std::vector<uint32_t>{1, 1, 1}); },
      ::testing::ThrowsMessage<std::invalid_argument>(::testing::HasSubstr(
          "Mismatch between number of queues 8 and weights 3")));
}

TEST_F(QueueSchedulerTest, WrrSetWeightsThrowsOnMismatch) {
  Queues queues = makeQueues(3);
  WrrQueueScheduler scheduler(&queues, std::vector<uint32_t>{1, 1, 1});
  EXPECT_THAT(
      [&] { scheduler.setWeights(std::vector<uint32_t>{1, 1}); },
      ::testing::ThrowsMessage<std::invalid_argument>(::testing::HasSubstr(
          "Mismatch between number of queues 3 and weights 2")));
}

// A mid-round `setWeights` restarts the round from the first queue: queue 0 is
// served next even though the previous selection left the cursor after it.
TEST_F(QueueSchedulerTest, WrrSetWeightsRestartsTheRoundAtTheFirstQueue) {
  Queues queues = makeQueues(3);
  ASSERT_NO_FATAL_FAILURE(occupyQueue(queues, 0));
  ASSERT_NO_FATAL_FAILURE(occupyQueue(queues, 1));
  ASSERT_NO_FATAL_FAILURE(occupyQueue(queues, 2));
  WrrQueueScheduler scheduler(&queues, std::vector<uint32_t>{1, 1, 1});
  ASSERT_EQ(scheduler.selectQueueToPop(), 0u);

  scheduler.setWeights(std::vector<uint32_t>{1, 1, 1});

  EXPECT_EQ(scheduler.selectQueueToPop(), 0u);
}

TEST_F(QueueSchedulerTest, WrrEqualWeightsRoundRobinFairly) {
  Queues queues = makeQueues(3);
  occupyQueue(queues, 0);
  occupyQueue(queues, 1);
  occupyQueue(queues, 2);
  WrrQueueScheduler scheduler(&queues, std::vector<uint32_t>{1, 1, 1});
  std::array<int, 3> counts = {0, 0, 0};
  for (int i = 0; i < 30; ++i) {
    const auto q = scheduler.selectQueueToPop();
    ASSERT_TRUE(q.has_value());
    ASSERT_LT(*q, 3u);
    counts.at(*q)++;
  }
  // Equal weights distribute the 30 selections evenly across the 3 queues.
  const std::array<int, 3> kExpected = {10, 10, 10};
  EXPECT_EQ(counts, kExpected);
}

TEST_F(QueueSchedulerTest, WrrHigherWeightQueueSelectedMoreOften) {
  Queues queues = makeQueues(2);
  occupyQueue(queues, 0);
  occupyQueue(queues, 1);
  WrrQueueScheduler scheduler(&queues, std::vector<uint32_t>{1, 3});
  std::array<int, 2> counts = {0, 0};
  for (int i = 0; i < 8; ++i) {
    const auto q = scheduler.selectQueueToPop();
    ASSERT_TRUE(q.has_value());
    ASSERT_LT(*q, 2u);
    counts.at(*q)++;
  }
  // Queue 1 carries the higher weight, so it is popped more than queue 0.
  EXPECT_GT(counts.at(1), counts.at(0));
}

TEST_F(QueueSchedulerTest, WrrReturnsNulloptWhenAllQueuesEmpty) {
  Queues queues = makeQueues(3);
  // No queue is occupied: the weight-scan must terminate and report no
  // eligible queue, which guards against the descending-weight loop failing
  // to terminate.
  WrrQueueScheduler scheduler(&queues, std::vector<uint32_t>{1, 1, 1});
  EXPECT_EQ(scheduler.selectQueueToPop(), std::nullopt);
}

// Two permanently backlogged queues (selection never pops) with weights 1 and
// 9: every round of 10 serves them once and nine times.
TEST_F(QueueSchedulerTest, WrrServesBackloggedQueuesInTheRatioOfTheirWeights) {
  constexpr uint32_t kSelections = 100;
  Queues queues = makeQueues(2);
  ASSERT_NO_FATAL_FAILURE(occupyQueue(queues, 0));
  ASSERT_NO_FATAL_FAILURE(occupyQueue(queues, 1));
  WrrQueueScheduler scheduler(&queues, std::vector<uint32_t>{1, 9});

  std::array<uint32_t, 2> counts{};
  for (uint32_t i = 0; i < kSelections; ++i) {
    const auto q = scheduler.selectQueueToPop();
    ASSERT_TRUE(q.has_value());
    ASSERT_LT(*q, 2u);
    counts.at(*q)++;
  }

  const std::array<uint32_t, 2> kExpected = {10, 90};
  EXPECT_EQ(counts, kExpected);
}

// A round that ends at the lowest level part-way through the queue index
// order resumes from the top level, so a queue earlier in the order is still
// found once the later ones have emptied.
TEST_F(QueueSchedulerTest, WrrWrapsToTheTopLevelWhenTheRestOfTheRoundIsEmpty) {
  Queues queues = makeQueues(2);
  ASSERT_NO_FATAL_FAILURE(occupyQueue(queues, 0));
  ASSERT_NO_FATAL_FAILURE(occupyQueue(queues, 1));
  WrrQueueScheduler scheduler(&queues, std::vector<uint32_t>{2, 5});
  // Levels 5, 4 and 3 serve queue 1; level 2 serves queue 0 then queue 1;
  // level 1 serves queue 0, leaving the cursor after queue 0 at level 1.
  const std::vector<uint8_t> kFirstRound = {1, 1, 1, 0, 1, 0};
  for (const uint8_t expected : kFirstRound) {
    ASSERT_EQ(scheduler.selectQueueToPop(), expected);
  }
  PacketPtr drained;
  ASSERT_TRUE(queues.popPacketFromQueue(1, drained));

  EXPECT_EQ(scheduler.selectQueueToPop(), 0u);
}

TEST_F(QueueSchedulerTest, WrrConstructorRejectsAZeroWeight) {
  Queues queues = makeQueues(3);
  EXPECT_THAT(
      [&] { WrrQueueScheduler(&queues, std::vector<uint32_t>{1, 0, 1}); },
      ::testing::ThrowsMessage<std::invalid_argument>(::testing::HasSubstr(
          "WRR weight 0 for queue 1 is below the minimum of 1")));
}

TEST_F(QueueSchedulerTest, WrrSetWeightsRejectsAZeroWeight) {
  Queues queues = makeQueues(3);
  WrrQueueScheduler scheduler(&queues, std::vector<uint32_t>{1, 1, 1});
  EXPECT_THAT(
      [&] { scheduler.setWeights(std::vector<uint32_t>{1, 1, 0}); },
      ::testing::ThrowsMessage<std::invalid_argument>(::testing::HasSubstr(
          "WRR weight 0 for queue 2 is below the minimum of 1")));
}

TEST_F(QueueSchedulerTest, WrrSkipsEmptyHigherWeightQueue) {
  Queues queues = makeQueues(2);
  occupyQueue(queues, 0); // only the low-weight queue holds a packet
  WrrQueueScheduler scheduler(&queues, std::vector<uint32_t>{1, 3});
  // Queue 1 has the higher weight but is empty, so queue 0 is served.
  EXPECT_EQ(scheduler.selectQueueToPop(), 0u);
}

// ---- Scheduler built by Queues ----

// `Queues` builds WRR with weights fixed by queue index. A round walks the
// weight levels from the largest weight down to 1, serving once per level
// every queue whose weight reaches it, so queue i is chosen weight times in a
// round of 260, the sum of the weights.
TEST_F(QueueSchedulerTest, QueuesBuildWrrWithTheFixedPerQueueWeights) {
  constexpr uint32_t kRoundLength = 260;
  Queues queues = makeQueues(8, SchedulingAlgorithmType::WRR);
  for (uint8_t q = 0; q < 8; ++q) {
    ASSERT_NO_FATAL_FAILURE(occupyQueue(queues, q));
  }

  std::array<uint32_t, 8> counts{};
  for (uint32_t i = 0; i < kRoundLength; ++i) {
    const auto q = queues.nextQueue();
    ASSERT_TRUE(q.has_value());
    ASSERT_LT(*q, 8u);
    counts.at(*q)++;
  }

  const std::array<uint32_t, 8> kExpected = {1, 9, 10, 20, 30, 40, 50, 100};
  EXPECT_EQ(counts, kExpected);
}

// A port with fewer than eight queues takes the leading weights, 1 and 9, so
// a round of 10 chooses queue 0 once and queue 1 nine times.
TEST_F(QueueSchedulerTest, QueuesBuildWrrOnFewerQueuesWithTheLeadingWeights) {
  Queues queues = makeQueues(2, SchedulingAlgorithmType::WRR);
  ASSERT_NO_FATAL_FAILURE(occupyQueue(queues, 0));
  ASSERT_NO_FATAL_FAILURE(occupyQueue(queues, 1));

  std::array<uint32_t, 2> counts{};
  for (uint32_t i = 0; i < 10; ++i) {
    const auto q = queues.nextQueue();
    ASSERT_TRUE(q.has_value());
    ASSERT_LT(*q, 2u);
    counts.at(*q)++;
  }

  const std::array<uint32_t, 2> kExpected = {1, 9};
  EXPECT_EQ(counts, kExpected);
}

// Id 2 is reserved in `SchedulingAlgorithmType`. A config that still carries
// it must fail when the queues are built, and the error must name the value.
TEST_F(QueueSchedulerTest, QueuesRejectTheReservedSchedulerId) {
  EXPECT_THAT(
      [&] { makeQueues(8, static_cast<SchedulingAlgorithmType>(2)); },
      ::testing::ThrowsMessage<std::runtime_error>(
          ::testing::HasSubstr("Unknown queue scheduler type 2")));
}

TEST_F(QueueSchedulerTest, WithNoTxGateEveryOccupiedQueueIsEligible) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);

  EXPECT_TRUE(queues.isQueueEligible(1));
  EXPECT_TRUE(queues.isQueueEligible(3));
  EXPECT_FALSE(queues.hasTxGateBlockedQueue());
}

TEST_F(QueueSchedulerTest, TxGateMakesTheStarvedQueueIneligible) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);
  starveQueueOne(queues);

  EXPECT_FALSE(queues.isQueueEligible(1));
  EXPECT_TRUE(queues.isQueueEligible(3));
}

// Telemetry has to separate "blocked on credit" from "empty" and "PFC-paused".
TEST_F(QueueSchedulerTest, TxGateBlockedQueueIsReportedForTelemetry) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);
  starveQueueOne(queues);

  EXPECT_TRUE(queues.hasTxGateBlockedQueue());
}

// UEC 1.0.3 §5.2.8.2 (p. 505): "The sender's implementation must prevent
// blocking between VCs."
TEST_F(QueueSchedulerTest, TxGateSkipsAStarvedQueueForAFundedOne) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);
  starveQueueOne(queues);

  const auto peekCos = queues.peekQueue();
  ASSERT_TRUE(peekCos.has_value());
  EXPECT_EQ(3u, *peekCos)
      << "a starved queue must be skipped, not stall the port";
}

// A gate skip must not make peek and pop disagree about the chosen packet.
TEST_F(QueueSchedulerTest, PeekAndPopAgreeAcrossAGateSkip) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);
  starveQueueOne(queues);

  const auto peekCos = queues.peekQueue();
  ASSERT_TRUE(peekCos.has_value());
  PacketPtr peeked;
  queues.peekPacketInQueue(*peekCos, &peeked);
  ASSERT_NE(peeked.get(), nullptr);

  PacketPtr popped;
  EXPECT_EQ(queues.popPacket(popped), 3u);
  ASSERT_NE(popped.get(), nullptr);

  EXPECT_EQ(peeked.get(), popped.get())
      << "the packet popped is not the packet that was inspected";
}

TEST_F(QueueSchedulerTest, ClearingTheTxGateRestoresThePreCbfcAnswer) {
  Queues queues = makeQueues(8);
  occupyQueue(queues, 1);
  occupyQueue(queues, 3);
  starveQueueOne(queues);
  ASSERT_FALSE(queues.isQueueEligible(1));

  queues.clearTxGate();

  EXPECT_TRUE(queues.isQueueEligible(1));
  EXPECT_FALSE(queues.hasTxGateBlockedQueue());
}

} // namespace

} // namespace packsim
