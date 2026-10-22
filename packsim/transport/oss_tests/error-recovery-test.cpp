/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/transport/error-recovery.h"
#include "packsim/transport/roce-send-info.h"

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim {

namespace {
RoceSendInfo makeSendInfo(uint32_t psn, uint32_t packetSize) {
  RoceSendInfo info;
  info.psn = psn;
  info.packetSize = packetSize;
  return info;
}

GoBackNRecovery::Options selectiveGapRepairOptions() {
  return GoBackNRecovery::Options{
      .selectiveGapRepair = true,
  };
}

class TestErrorRecoverySocket final : public ErrorRecoverySocket {
 public:
  struct NackCall {
    uint32_t seqNum;
    uint32_t psn;
    std::optional<uint8_t> rnrBackoffIdx;
  };

  void emitNackPacket(
      uint32_t seqNum,
      uint32_t psn,
      std::optional<uint8_t> rnrBackoffIdx) override {
    nackCalls.emplace_back(seqNum, psn, rnrBackoffIdx);
  }

  void wakeupForSend() override {
    ++wakeupCalls;
  }

  std::vector<NackCall> nackCalls;
  uint32_t wakeupCalls{0};
};

class DefaultErrorRecovery final : public ErrorRecovery {
 public:
  PsnAction checkPsn(uint32_t&, uint32_t, bool) override {
    return PsnAction::ACCEPT;
  }
  void onPacketScheduled(const RoceSendInfo&) override {}
  AckResult onAck(uint32_t) override {
    return {};
  }
  void onNack(uint32_t, std::optional<uint8_t>) override {}
  RetransmitBatch getRetransmitBatch(uint64_t) override {
    return {};
  }
  bool hasRetransmitPending() const noexcept override {
    return false;
  }
};
} // namespace

class GoBackNRecoveryTest : public testing::Test {
 protected:
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  TestErrorRecoverySocket socket_;
};

TEST_F(GoBackNRecoveryTest, OooSupportBuffersAndReportsPsnGap) {
  GoBackNRecovery recovery(
      /*rnrTimeIdx=*/0, socket_, simCtx_, selectiveGapRepairOptions());
  uint32_t expectedPsn = 5;
  EXPECT_EQ(
      recovery.checkPsn(expectedPsn, /*receivedPsn=*/99, /*oooSupport=*/true),
      PsnAction::BUFFER_AND_REQUEST_NACK);
  EXPECT_EQ(expectedPsn, 5u);
}

TEST_F(GoBackNRecoveryTest, OooSupportAdvancesAcrossBufferedPsns) {
  GoBackNRecovery recovery(
      /*rnrTimeIdx=*/0, socket_, simCtx_, selectiveGapRepairOptions());
  uint32_t expectedPsn = 5;

  EXPECT_EQ(
      recovery.checkPsn(expectedPsn, /*receivedPsn=*/7, /*oooSupport=*/true),
      PsnAction::BUFFER_AND_REQUEST_NACK);
  recovery.onMissingMessage(/*seqNum=*/0, expectedPsn);
  EXPECT_EQ(
      recovery.checkPsn(expectedPsn, /*receivedPsn=*/6, /*oooSupport=*/true),
      PsnAction::BUFFER_WITHOUT_ACK);
  EXPECT_EQ(
      recovery.checkPsn(expectedPsn, /*receivedPsn=*/5, /*oooSupport=*/true),
      PsnAction::ACCEPT);
  EXPECT_EQ(expectedPsn, 8u);
}

TEST_F(GoBackNRecoveryTest, OooSupportRequestsNextBufferedGap) {
  GoBackNRecovery recovery(
      /*rnrTimeIdx=*/0, socket_, simCtx_, selectiveGapRepairOptions());
  uint32_t expectedPsn = 5;

  EXPECT_EQ(
      recovery.checkPsn(expectedPsn, /*receivedPsn=*/8, /*oooSupport=*/true),
      PsnAction::BUFFER_AND_REQUEST_NACK);
  recovery.onMissingMessage(/*seqNum=*/0, expectedPsn);
  EXPECT_EQ(
      recovery.checkPsn(expectedPsn, /*receivedPsn=*/5, /*oooSupport=*/true),
      PsnAction::ACCEPT_AND_REQUEST_NACK);
  EXPECT_EQ(expectedPsn, 6u);
}

TEST_F(GoBackNRecoveryTest, SelectiveGapRepairIgnoresTransientReordering) {
  GoBackNRecovery recovery(
      /*rnrTimeIdx=*/0, socket_, simCtx_, selectiveGapRepairOptions());
  uint32_t expectedPsn = 5;

  EXPECT_EQ(
      recovery.checkPsn(expectedPsn, /*receivedPsn=*/6, /*oooSupport=*/true),
      PsnAction::BUFFER_AND_REQUEST_NACK);
  recovery.onMissingMessage(/*seqNum=*/0, expectedPsn);
  recovery.deferAck(/*seqNum=*/9, /*psn=*/6);
  EXPECT_TRUE(socket_.nackCalls.empty());

  EXPECT_EQ(
      recovery.checkPsn(expectedPsn, /*receivedPsn=*/5, /*oooSupport=*/true),
      PsnAction::ACCEPT);
  const auto readyAcks = recovery.takeReadyDeferredAcks(expectedPsn);
  ASSERT_EQ(readyAcks.size(), 1u);
  EXPECT_EQ(readyAcks.front(), std::make_pair(9u, 6u));
  scheduler_.run();
  EXPECT_TRUE(socket_.nackCalls.empty());
}

TEST(ErrorRecoveryTest, IsSendBlockedDefaultsFalse) {
  const DefaultErrorRecovery recovery;

  EXPECT_FALSE(recovery.isSendBlocked());
}

TEST(ErrorRecoveryTest, IsFailedDefaultsFalse) {
  const DefaultErrorRecovery recovery;

  EXPECT_FALSE(recovery.isFailed());
}

TEST_F(GoBackNRecoveryTest, RejectsZeroRetransmitTimeout) {
  auto options = selectiveGapRepairOptions();
  options.retransmitTimeout = Time{};

  EXPECT_THAT(
      [&] {
        [[maybe_unused]] GoBackNRecovery recovery(
            /*rnrTimeIdx=*/0, socket_, simCtx_, options);
      },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Retransmit timeout must be positive")));
}

TEST_F(GoBackNRecoveryTest, RejectsZeroRetransmitTimeoutAttempts) {
  auto options = selectiveGapRepairOptions();
  options.maxRetransmitTimeoutAttempts = 0;

  EXPECT_THAT(
      [&] {
        [[maybe_unused]] GoBackNRecovery recovery(
            /*rnrTimeIdx=*/0, socket_, simCtx_, options);
      },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Maximum retransmit timeout attempts must be positive")));
}

TEST_F(GoBackNRecoveryTest, InOrderAdvancesExpectedPsn) {
  GoBackNRecovery recovery(0, socket_, simCtx_);
  uint32_t expectedPsn = 5;
  EXPECT_EQ(
      recovery.checkPsn(expectedPsn, /*receivedPsn=*/5, /*oooSupport=*/false),
      PsnAction::ACCEPT);
  EXPECT_EQ(expectedPsn, 6u);
}

TEST_F(GoBackNRecoveryTest, GapReturnsSendNack) {
  GoBackNRecovery recovery(0, socket_, simCtx_);
  uint32_t expectedPsn = 5;
  EXPECT_EQ(
      recovery.checkPsn(expectedPsn, /*receivedPsn=*/7, /*oooSupport=*/false),
      PsnAction::SEND_NACK);
  EXPECT_EQ(expectedPsn, 5u); // a gap does not advance the expected PSN
}

TEST_F(GoBackNRecoveryTest, StalePsnRequestsDuplicateAck) {
  GoBackNRecovery recovery(0, socket_, simCtx_);
  uint32_t expectedPsn = 5;
  EXPECT_EQ(
      recovery.checkPsn(expectedPsn, /*receivedPsn=*/3, /*oooSupport=*/false),
      PsnAction::ACK_DUPLICATE);
  EXPECT_EQ(expectedPsn, 5u);
}

TEST_F(GoBackNRecoveryTest, TracksScheduledPacketsButNotRetransmits) {
  GoBackNRecovery recovery(0, socket_, simCtx_);
  EXPECT_FALSE(recovery.hasRetransmitPending());

  recovery.onPacketScheduled(makeSendInfo(/*psn=*/1, /*packetSize=*/100));
  // After scheduling, the replay pointer sits past the queue, so nothing is
  // pending until a NACK rewinds it.
  EXPECT_FALSE(recovery.hasRetransmitPending());

  RoceSendInfo retransmit = makeSendInfo(2, 100);
  retransmit.isRetransmit = true;
  recovery.onPacketScheduled(retransmit); // retransmits are not recorded

  recovery.onNack(1, std::nullopt); // rewinds the replay pointer to the front
  EXPECT_TRUE(recovery.hasRetransmitPending());
  EXPECT_EQ(socket_.wakeupCalls, 1u);
}

TEST_F(GoBackNRecoveryTest, OnNackWithBackoffReplacesPendingWakeup) {
  GoBackNRecovery recovery(/*rnrTimeIdx=*/1, socket_, simCtx_);

  recovery.onNack(/*expectedPsn=*/1, /*rnrBackoffIdx=*/uint8_t{1});
  recovery.onNack(/*expectedPsn=*/1, /*rnrBackoffIdx=*/uint8_t{1});

  EXPECT_TRUE(recovery.isSendBlocked());
  scheduler_.run();
  EXPECT_EQ(socket_.wakeupCalls, 1u);
  EXPECT_FALSE(recovery.isSendBlocked());
}

TEST_F(GoBackNRecoveryTest, SendRnrRequestedSuppressesDuplicateNack) {
  GoBackNRecovery recovery(/*rnrTimeIdx=*/7, socket_, simCtx_);

  recovery.onSendRnrRequested(/*psn=*/9);
  recovery.onSendRnrRequested(/*psn=*/9);

  ASSERT_EQ(socket_.nackCalls.size(), 1u);
  EXPECT_EQ(socket_.nackCalls[0].seqNum, 9u);
  EXPECT_EQ(socket_.nackCalls[0].psn, 9u);
  EXPECT_EQ(socket_.nackCalls[0].rnrBackoffIdx, 7u);
}

TEST_F(GoBackNRecoveryTest, RnrTimeIdxPastTimerTableThrows) {
  // The RNR timer table has 32 entries, so 32 is the first bad index.
  EXPECT_THAT(
      [this]() { GoBackNRecovery(/*rnrTimeIdx=*/32, socket_, simCtx_); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "GoBackNRecovery rnrTimeIdx 32 is out of range: it indexes "
          "kGoBackNRnrFieldToTimeUs, valid range [0, 31]")));
}

TEST_F(GoBackNRecoveryTest, DestructorCancelsPendingCallbacks) {
  {
    GoBackNRecovery recovery(/*rnrTimeIdx=*/1, socket_, simCtx_);
    recovery.onMissingMessage(/*seqNum=*/8, /*psn=*/3);
    recovery.onMissingMessage(/*seqNum=*/8, /*psn=*/3);
    recovery.onNack(/*expectedPsn=*/1, /*rnrBackoffIdx=*/uint8_t{1});
  }

  ASSERT_EQ(socket_.nackCalls.size(), 1u);
  EXPECT_EQ(socket_.nackCalls[0].seqNum, 8u);
  EXPECT_EQ(socket_.nackCalls[0].psn, 3u);
  EXPECT_FALSE(socket_.nackCalls[0].rnrBackoffIdx.has_value());

  scheduler_.run();
  EXPECT_EQ(socket_.nackCalls.size(), 1u);
  EXPECT_EQ(socket_.wakeupCalls, 0u);
}

TEST_F(GoBackNRecoveryTest, OnAckFreesCumulativelyAckedPackets) {
  GoBackNRecovery recovery(0, socket_, simCtx_);
  recovery.onPacketScheduled(makeSendInfo(1, 100));
  recovery.onPacketScheduled(makeSendInfo(2, 100));
  recovery.onPacketScheduled(makeSendInfo(3, 100));

  const AckResult result = recovery.onAck(2); // frees PSNs 1 and 2
  EXPECT_FALSE(result.nextIncompleteSeqNum.has_value());
  EXPECT_FALSE(result.isStale);

  recovery.onNack(3, std::nullopt); // rewind; only PSN 3 remains to replay
  const auto batch = recovery.getRetransmitBatch(/*quota=*/1000);
  ASSERT_EQ(batch.packets.size(), 1u);
  EXPECT_EQ(batch.packets.front().psn, 3u);
  EXPECT_TRUE(batch.packets.front().isRetransmit);
}

TEST_F(GoBackNRecoveryTest, OnAckCompletesCumulativelyCoveredMessages) {
  GoBackNRecovery recovery(
      /*rnrTimeIdx=*/0, socket_, simCtx_, selectiveGapRepairOptions());
  auto firstTail = makeSendInfo(/*psn=*/1, /*packetSize=*/100);
  firstTail.sequenceNum = 0;
  firstTail.isLastPkt = true;
  auto secondTail = makeSendInfo(/*psn=*/2, /*packetSize=*/100);
  secondTail.sequenceNum = 1;
  secondTail.isLastPkt = true;
  recovery.onPacketScheduled(firstTail);
  recovery.onPacketScheduled(secondTail);

  const AckResult result = recovery.onAck(/*psn=*/2);

  EXPECT_EQ(result.nextIncompleteSeqNum, 2u);
}

TEST_F(GoBackNRecoveryTest, CumulativeCompletionWatermarkDoesNotRegress) {
  GoBackNRecovery recovery(
      /*rnrTimeIdx=*/0, socket_, simCtx_, selectiveGapRepairOptions());
  auto laterMessageTail = makeSendInfo(/*psn=*/1, /*packetSize=*/100);
  laterMessageTail.sequenceNum = 5;
  laterMessageTail.isLastPkt = true;
  auto earlierMessageTail = makeSendInfo(/*psn=*/2, /*packetSize=*/100);
  earlierMessageTail.sequenceNum = 3;
  earlierMessageTail.isLastPkt = true;
  recovery.onPacketScheduled(laterMessageTail);
  recovery.onPacketScheduled(earlierMessageTail);

  const AckResult result = recovery.onAck(/*psn=*/2);

  EXPECT_EQ(result.nextIncompleteSeqNum, 6u);
}

TEST_F(GoBackNRecoveryTest, RetransmitTimeoutEventuallyFailsRecovery) {
  GoBackNRecovery recovery(
      /*rnrTimeIdx=*/0,
      socket_,
      simCtx_,
      GoBackNRecovery::Options{
          .selectiveGapRepair = true,
          .retransmitTimeout = Time::fromNs(1),
          .maxRetransmitTimeoutAttempts = 2,
      });
  auto tail = makeSendInfo(/*psn=*/1, /*packetSize=*/100);
  tail.isLastPkt = true;
  recovery.onPacketScheduled(tail);

  EXPECT_FALSE(recovery.isFailed());
  scheduler_.run();

  EXPECT_TRUE(recovery.isFailed());
  EXPECT_GT(socket_.wakeupCalls, 0u);
  const uint32_t wakeupCalls = socket_.wakeupCalls;
  scheduler_.run();
  EXPECT_EQ(socket_.wakeupCalls, wakeupCalls);
}

TEST_F(
    GoBackNRecoveryTest,
    InOrderDeliveryCompletesCumulativelyCoveredMessages) {
  GoBackNRecovery recovery(
      /*rnrTimeIdx=*/0,
      socket_,
      simCtx_,
      GoBackNRecovery::Options{.inOrderDelivery = true});
  auto firstTail = makeSendInfo(/*psn=*/1, /*packetSize=*/100);
  firstTail.sequenceNum = 0;
  firstTail.isLastPkt = true;
  auto secondTail = makeSendInfo(/*psn=*/2, /*packetSize=*/100);
  secondTail.sequenceNum = 1;
  secondTail.isLastPkt = true;
  recovery.onPacketScheduled(firstTail);
  recovery.onPacketScheduled(secondTail);

  const AckResult result = recovery.onAck(/*psn=*/2);

  EXPECT_EQ(result.nextIncompleteSeqNum, 2u);
}

TEST_F(GoBackNRecoveryTest, InOrderDeliveryArmsTailRetransmitTimer) {
  GoBackNRecovery recovery(
      /*rnrTimeIdx=*/0,
      socket_,
      simCtx_,
      GoBackNRecovery::Options{
          .inOrderDelivery = true,
          .retransmitTimeout = Time::fromNs(1),
          .maxRetransmitTimeoutAttempts = 2,
      });
  auto tail = makeSendInfo(/*psn=*/1, /*packetSize=*/100);
  tail.isLastPkt = true;
  recovery.onPacketScheduled(tail);

  scheduler_.run();

  EXPECT_EQ(socket_.wakeupCalls, 2u);
  EXPECT_TRUE(recovery.isFailed());
}

TEST_F(GoBackNRecoveryTest, RnrNackRestartsRetransmitTimeoutCount) {
  GoBackNRecovery recovery(
      /*rnrTimeIdx=*/0,
      socket_,
      simCtx_,
      GoBackNRecovery::Options{
          .inOrderDelivery = true,
          .retransmitTimeout = Time::fromNs(2),
          .maxRetransmitTimeoutAttempts = 2,
      });
  auto tail = makeSendInfo(/*psn=*/1, /*packetSize=*/100);
  tail.isLastPkt = true;
  recovery.onPacketScheduled(tail);
  // The timer fires at 2, 5 and 8 ns. An RNR NACK lands after each of the
  // first two fires, so by 9 ns no two fires have run back to back.
  for (const uint64_t nackTimeNs : {uint64_t{3}, uint64_t{6}}) {
    simCtx_.schedule(Time::fromNs(nackTimeNs), [&recovery] {
      recovery.onNack(/*expectedPsn=*/1, /*rnrBackoffIdx=*/uint8_t{1});
    });
  }
  bool failedAt9Ns = true;
  simCtx_.schedule(Time::fromNs(9), [&] { failedAt9Ns = recovery.isFailed(); });

  scheduler_.run();

  EXPECT_FALSE(failedAt9Ns);
}

TEST_F(GoBackNRecoveryTest, NackAcknowledgingNewPsnsRestartsTimeoutCount) {
  GoBackNRecovery recovery(
      /*rnrTimeIdx=*/0,
      socket_,
      simCtx_,
      GoBackNRecovery::Options{
          .inOrderDelivery = true,
          .retransmitTimeout = Time::fromNs(2),
          .maxRetransmitTimeoutAttempts = 2,
      });
  recovery.onPacketScheduled(makeSendInfo(/*psn=*/1, /*packetSize=*/100));
  recovery.onPacketScheduled(makeSendInfo(/*psn=*/2, /*packetSize=*/100));
  auto tail = makeSendInfo(/*psn=*/3, /*packetSize=*/100);
  tail.isLastPkt = true;
  recovery.onPacketScheduled(tail);
  // The timer fires at 2, 5 and 8 ns. The NACKs after the first two fires
  // acknowledge PSN 1, then PSN 2.
  for (const uint32_t expectedPsn : {2u, 3u}) {
    simCtx_.schedule(
        Time::fromNs(uint64_t{3} * (expectedPsn - 1)),
        [&recovery, expectedPsn] {
          recovery.onNack(expectedPsn, /*rnrBackoffIdx=*/std::nullopt);
        });
  }
  bool failedAt9Ns = true;
  simCtx_.schedule(Time::fromNs(9), [&] { failedAt9Ns = recovery.isFailed(); });

  scheduler_.run();

  EXPECT_FALSE(failedAt9Ns);
}

TEST_F(GoBackNRecoveryTest, NackWithoutProgressStillCountsTowardFailure) {
  GoBackNRecovery recovery(
      /*rnrTimeIdx=*/0,
      socket_,
      simCtx_,
      GoBackNRecovery::Options{
          .inOrderDelivery = true,
          .retransmitTimeout = Time::fromNs(2),
          .maxRetransmitTimeoutAttempts = 2,
      });
  auto tail = makeSendInfo(/*psn=*/1, /*packetSize=*/100);
  tail.isLastPkt = true;
  recovery.onPacketScheduled(tail);
  // Same timing as above, but both NACKs re-request PSN 1, so the fires at 2
  // and 5 ns use up the budget and the one at 8 ns fails the QP.
  for (const uint64_t nackTimeNs : {uint64_t{3}, uint64_t{6}}) {
    simCtx_.schedule(Time::fromNs(nackTimeNs), [&recovery] {
      recovery.onNack(/*expectedPsn=*/1, /*rnrBackoffIdx=*/std::nullopt);
    });
  }
  bool failedAt9Ns = false;
  simCtx_.schedule(Time::fromNs(9), [&] { failedAt9Ns = recovery.isFailed(); });

  scheduler_.run();

  EXPECT_TRUE(failedAt9Ns);
}

TEST_F(
    GoBackNRecoveryTest,
    OooWithoutSelectiveGapRepairArmsNoTimerOrWatermark) {
  GoBackNRecovery recovery(
      /*rnrTimeIdx=*/0,
      socket_,
      simCtx_,
      GoBackNRecovery::Options{.retransmitTimeout = Time::fromNs(1)});
  auto tail = makeSendInfo(/*psn=*/1, /*packetSize=*/100);
  tail.isLastPkt = true;
  recovery.onPacketScheduled(tail);

  scheduler_.run();

  EXPECT_EQ(socket_.wakeupCalls, 0u);
  EXPECT_FALSE(recovery.isFailed());
  EXPECT_FALSE(recovery.onAck(/*psn=*/1).nextIncompleteSeqNum.has_value());
}

TEST_F(GoBackNRecoveryTest, LowerPsnDoesNotRegressAckState) {
  GoBackNRecovery recovery(0, socket_, simCtx_);
  recovery.onPacketScheduled(makeSendInfo(1, 100));
  recovery.onPacketScheduled(makeSendInfo(2, 100));

  recovery.onAck(1);
  const AckResult result = recovery.onAck(0);
  EXPECT_FALSE(result.nextIncompleteSeqNum.has_value());
  EXPECT_FALSE(result.isStale);

  EXPECT_NO_THROW(recovery.onNack(2, std::nullopt));
  const auto batch = recovery.getRetransmitBatch(/*quota=*/1000);
  ASSERT_EQ(batch.packets.size(), 1u);
  EXPECT_EQ(batch.packets.front().psn, 2u);
}

TEST_F(GoBackNRecoveryTest, OnAckThrowsWhenQueueEmpty) {
  GoBackNRecovery recovery(0, socket_, simCtx_);
  EXPECT_THAT(
      [&] { recovery.onAck(1); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Retransmit queue empty during ACK processing")));
}

TEST_F(GoBackNRecoveryTest, OnAckThrowsWhenQueuePsnMismatches) {
  GoBackNRecovery recovery(0, socket_, simCtx_);
  recovery.onPacketScheduled(makeSendInfo(/*psn=*/2, /*packetSize=*/100));

  EXPECT_THAT(
      [&]() { recovery.onAck(/*psn=*/1); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Retransmit queue PSN mismatch: expected 1, got 2")));
  recovery.onNack(/*expectedPsn=*/1, std::nullopt);
  const auto batch = recovery.getRetransmitBatch(/*quota=*/1000);
  ASSERT_EQ(batch.packets.size(), 1u);
  EXPECT_EQ(batch.packets.front().psn, 2u);
}

TEST_F(GoBackNRecoveryTest, GetRetransmitBatchRespectsQuota) {
  GoBackNRecovery recovery(0, socket_, simCtx_);
  recovery.onPacketScheduled(makeSendInfo(1, 100));
  recovery.onPacketScheduled(makeSendInfo(2, 100));
  recovery.onPacketScheduled(makeSendInfo(3, 100));
  recovery.onNack(1, std::nullopt); // rewind the replay pointer to the front

  const auto batch = recovery.getRetransmitBatch(/*quota=*/150);
  ASSERT_EQ(batch.packets.size(), 1u);
  EXPECT_EQ(batch.totalBytes, 100u);
  EXPECT_TRUE(batch.packets.back().callbackOnSend);
  EXPECT_TRUE(recovery.hasRetransmitPending());
  for (const auto& pkt : batch.packets) {
    EXPECT_TRUE(pkt.isRetransmit);
  }
}

TEST_F(GoBackNRecoveryTest, GetRetransmitBatchCountsNackRetransmissions) {
  GoBackNRecovery recovery(0, socket_, simCtx_);
  recovery.onPacketScheduled(makeSendInfo(/*psn=*/1, /*packetSize=*/100));
  recovery.onPacketScheduled(makeSendInfo(/*psn=*/2, /*packetSize=*/200));
  recovery.onNack(/*expectedPsn=*/1, std::nullopt);

  const auto batch = recovery.getRetransmitBatch(/*quota=*/1000);
  const auto stats = recovery.getRetransmitStats();

  ASSERT_EQ(batch.packets.size(), 2u);
  EXPECT_EQ(stats.retxPkts, 2u);
  EXPECT_EQ(stats.retxBytes, 300u);
  EXPECT_EQ(stats.retxByTimer, 0u);
  EXPECT_EQ(stats.retxByNack, 2u);
}

TEST_F(GoBackNRecoveryTest, GetRetransmitBatchAllowsOversizedFirstPacket) {
  GoBackNRecovery recovery(0, socket_, simCtx_);
  recovery.onPacketScheduled(makeSendInfo(1, 200));
  recovery.onPacketScheduled(makeSendInfo(2, 100));
  recovery.onNack(1, std::nullopt);

  const auto batch = recovery.getRetransmitBatch(/*quota=*/150);

  ASSERT_EQ(batch.packets.size(), 1u);
  EXPECT_EQ(batch.packets.front().psn, 1u);
  EXPECT_EQ(batch.totalBytes, 200u);
  EXPECT_TRUE(batch.packets.back().callbackOnSend);
  EXPECT_TRUE(recovery.hasRetransmitPending());
}

TEST_F(GoBackNRecoveryTest, OooModeRepairsOneGapPerNack) {
  GoBackNRecovery recovery(
      /*rnrTimeIdx=*/0, socket_, simCtx_, selectiveGapRepairOptions());
  recovery.onPacketScheduled(makeSendInfo(1, 100));
  recovery.onPacketScheduled(makeSendInfo(2, 100));
  recovery.onPacketScheduled(makeSendInfo(3, 100));
  recovery.onNack(/*expectedPsn=*/1, std::nullopt);

  const auto batch = recovery.getRetransmitBatch(/*quota=*/1000);

  ASSERT_EQ(batch.packets.size(), 1u);
  EXPECT_EQ(batch.packets.front().psn, 1u);
  EXPECT_FALSE(recovery.hasRetransmitPending());
}

TEST_F(GoBackNRecoveryTest, RnrFieldToTimeIndexZeroDoesNotThrow) {
  EXPECT_NO_THROW(GoBackNRecovery::rnrFieldToTime(0));
}

} // namespace packsim
