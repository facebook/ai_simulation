/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/packet/roce.h"
#include "packsim/transport/error-recovery.h"
#include "packsim/transport/oss_tests/roce-socket-test-fixture.h"
#include "packsim/transport/roce-base.h"

namespace packsim {

namespace {

// Sequence/PSN pair as recorded by deferAck and as emitted on the wire.
using AckKey = std::pair<uint32_t, uint32_t>;

} // namespace

// Error-recovery strategy that returns a caller-chosen PsnAction and records
// every receive-path side effect the socket drives from it. Deferred ACKs are
// stored and released wholesale on the next takeReadyDeferredAcks(), which is
// enough to exercise the defer-then-flush handoff without reimplementing a
// real gap-tracking protocol.
class RecordingRecovery final : public ErrorRecovery {
 public:
  explicit RecordingRecovery(PsnAction action) noexcept : action_(action) {}

  void setAction(PsnAction action) noexcept {
    action_ = action;
  }

  PsnAction checkPsn(uint32_t&, uint32_t, bool) override {
    ++checkPsnCalls_;
    return action_;
  }

  void onPacketScheduled(const RoceSendInfo&) override {}

  AckResult onAck(uint32_t) override {
    return {};
  }

  void onNack(uint32_t, std::optional<uint8_t>) override {}

  void onMissingMessage(uint32_t seqNum, uint32_t psn) override {
    missingMessages_.emplace_back(seqNum, psn);
  }

  void deferAck(uint32_t seqNum, uint32_t psn) override {
    deferredAcks_.emplace_back(seqNum, psn);
  }

  std::vector<AckKey> takeReadyDeferredAcks(uint32_t) override {
    ++takeReadyDeferredAckCalls_;
    return std::exchange(deferredAcks_, {});
  }

  RetransmitBatch getRetransmitBatch(uint64_t) override {
    return {};
  }

  bool hasRetransmitPending() const noexcept override {
    return false;
  }

  int checkPsnCalls() const noexcept {
    return checkPsnCalls_;
  }

  int takeReadyDeferredAckCalls() const noexcept {
    return takeReadyDeferredAckCalls_;
  }

  const std::vector<AckKey>& missingMessages() const noexcept {
    return missingMessages_;
  }

  const std::vector<AckKey>& pendingDeferredAcks() const noexcept {
    return deferredAcks_;
  }

 private:
  PsnAction action_;
  int checkPsnCalls_{0};
  int takeReadyDeferredAckCalls_{0};
  std::vector<AckKey> missingMessages_;
  std::vector<AckKey> deferredAcks_;
};

// PsnAction::BUFFER has no case here: it is the one arm whose socket-side
// effect is byte-count completion rather than ACK policy, and it is covered by
// ErrorRecoveryAdpHooksTest.BufferActionProcessesOutOfOrderPacket in
// error-recovery-stub-test.cpp.
class RoceSocketPsnActionTest : public RoceSocketTestBase {
 public:
  void SetUp() override {
    RoceSocketTestBase::SetUp();
    nicCfg_.outofOrderPktSupport() = false;
  }

  // Builds a socket whose recovery strategy is a RecordingRecovery seeded with
  // `action`, and posts one expected receive message so the receive path gets
  // past the expected-message lookup. The fixture keeps ownership so tests can
  // work with a reference.
  RoceSocketBase& makeSocketWithAction(PsnAction action) {
    socket_ = RoceSocketTestBase::makeSocket(
        [this, action](
            RoceSocketBase&, SimContext&) -> std::unique_ptr<ErrorRecovery> {
          auto recovery = std::make_unique<RecordingRecovery>(action);
          recovery_ = recovery.get();
          return recovery;
        });
    socket_->recv(&expectedMsg_);
    return *socket_;
  }

  // The recovery strategy owned by the socket built above.
  RecordingRecovery& recovery() {
    if (recovery_ == nullptr) {
      throw std::logic_error("no RecordingRecovery was installed");
    }
    return *recovery_;
  }

  // A single 64-byte packet completes and retires an expected message, so a
  // test that receives twice must post a second one for the follow-up packet
  // to match.
  void postFollowUpMessage(RoceSocketBase& socket) {
    socket.recv(&followUpMsg_);
  }

  static PacketPtr makeDataPacket(uint32_t seqNum, uint32_t psn, bool ackReq) {
    auto pkt = createPacket<PacketType::ROCE>(kMsgBytes);
    auto* rocePkt = pkt.as<RocePacket>();
    rocePkt->setOpcode(/*isFirstPkt=*/true, /*isLastPkt=*/true);
    rocePkt->ackReq = ackReq;
    rocePkt->sequenceNum = seqNum;
    rocePkt->psn = psn;
    return pkt;
  }

  std::vector<AckKey> sentAcks() const {
    std::vector<AckKey> acks;
    for (const auto& pkt : sentPackets_) {
      const auto* rocePkt = pkt.as<RocePacket>();
      if (rocePkt->isAck) {
        acks.emplace_back(rocePkt->sequenceNum, rocePkt->psn);
      }
    }
    return acks;
  }

 protected:
  static constexpr uint32_t kMsgBytes = 64;
  // Chosen above the socket's initial expectedPsn_ (1) so onPacketRcvd's
  // duplicate pre-check is skipped and isPsnOk runs exactly once per packet.
  static constexpr uint32_t kFreshPsn = 5;
  // Below expectedPsn_ (1), so the duplicate pre-check fires.
  static constexpr uint32_t kDuplicatePsn = 0;
  // The deferred-ACK pair. Both sit above the socket's initial expectedPsn_
  // (1) so neither trips the duplicate pre-check, and the second is lower than
  // the first so the flush order is visible rather than incidental.
  static constexpr uint32_t kDeferredPsn = 7;
  static constexpr uint32_t kFlushingPsn = 6;

  std::unique_ptr<RoceSocketBase> socket_;
  RecordingRecovery* recovery_{nullptr};
  Message expectedMsg_{
      .sizeBytes = kMsgBytes,
      .srcIdx = dstId_,
      .dstIdx = srcId_,
  };
  Message followUpMsg_{
      .sizeBytes = kMsgBytes,
      .srcIdx = dstId_,
      .dstIdx = srcId_,
  };
};

TEST_F(RoceSocketPsnActionTest, AcceptAcksImmediatelyAndConsumesThePacket) {
  auto& socket = makeSocketWithAction(PsnAction::ACCEPT);

  socket.onPacketRcvd(makeDataPacket(/*seqNum=*/0, kFreshPsn, /*ackReq=*/true));

  EXPECT_EQ(recovery().checkPsnCalls(), 1);
  EXPECT_EQ(sentAcks(), (std::vector<AckKey>{{0u, kFreshPsn}}));
  EXPECT_EQ(*socket.getStats().recvdDataPkts(), 1u);
  EXPECT_TRUE(recovery().missingMessages().empty());
}

TEST_F(
    RoceSocketPsnActionTest,
    BufferWithoutAckDefersTheAckInsteadOfSendingIt) {
  auto& socket = makeSocketWithAction(PsnAction::BUFFER_WITHOUT_ACK);

  socket.onPacketRcvd(makeDataPacket(/*seqNum=*/0, kFreshPsn, /*ackReq=*/true));

  // The packet is still consumed -- only its ACK is held back.
  EXPECT_EQ(*socket.getStats().recvdDataPkts(), 1u);
  EXPECT_TRUE(sentAcks().empty());
  EXPECT_EQ(
      recovery().pendingDeferredAcks(), (std::vector<AckKey>{{0u, kFreshPsn}}));
  EXPECT_TRUE(recovery().missingMessages().empty());
  // Deferring must not itself flush the backlog.
  EXPECT_EQ(recovery().takeReadyDeferredAckCalls(), 0);
}

TEST_F(RoceSocketPsnActionTest, BufferWithoutAckOnUnrequestedAckDefersNothing) {
  auto& socket = makeSocketWithAction(PsnAction::BUFFER_WITHOUT_ACK);

  socket.onPacketRcvd(
      makeDataPacket(/*seqNum=*/0, kFreshPsn, /*ackReq=*/false));

  EXPECT_EQ(*socket.getStats().recvdDataPkts(), 1u);
  EXPECT_TRUE(sentAcks().empty());
  EXPECT_TRUE(recovery().pendingDeferredAcks().empty());
}

TEST_F(
    RoceSocketPsnActionTest,
    BufferAndRequestNackDefersTheAckAndRequestsGapRecovery) {
  auto& socket = makeSocketWithAction(PsnAction::BUFFER_AND_REQUEST_NACK);

  socket.onPacketRcvd(makeDataPacket(/*seqNum=*/0, kFreshPsn, /*ackReq=*/true));

  EXPECT_TRUE(sentAcks().empty());
  // The packet is still consumed -- only its ACK is held back.
  EXPECT_EQ(*socket.getStats().recvdDataPkts(), 1u);
  EXPECT_EQ(
      recovery().pendingDeferredAcks(), (std::vector<AckKey>{{0u, kFreshPsn}}));
  // The gap is reported at the socket's still-unadvanced expectedPsn_ (1).
  EXPECT_EQ(recovery().missingMessages(), (std::vector<AckKey>{{0u, 1u}}));
}

TEST_F(
    RoceSocketPsnActionTest,
    AcceptAfterDeferredAckFlushesItAheadOfTheCurrentAck) {
  auto& socket = makeSocketWithAction(PsnAction::BUFFER_WITHOUT_ACK);
  postFollowUpMessage(socket);
  socket.onPacketRcvd(
      makeDataPacket(/*seqNum=*/0, kDeferredPsn, /*ackReq=*/true));
  ASSERT_TRUE(sentAcks().empty());

  recovery().setAction(PsnAction::ACCEPT);
  socket.onPacketRcvd(
      makeDataPacket(/*seqNum=*/1, kFlushingPsn, /*ackReq=*/true));

  // Released deferred ACK first, then the accepted packet's own ACK.
  EXPECT_EQ(
      sentAcks(),
      (std::vector<AckKey>{{0u, kDeferredPsn}, {1u, kFlushingPsn}}));
  EXPECT_TRUE(recovery().pendingDeferredAcks().empty());
  EXPECT_EQ(recovery().takeReadyDeferredAckCalls(), 1);
}

TEST_F(
    RoceSocketPsnActionTest,
    AcceptAndRequestNackFlushesDeferredAcksAndRequestsGapRecovery) {
  auto& socket = makeSocketWithAction(PsnAction::BUFFER_WITHOUT_ACK);
  postFollowUpMessage(socket);
  socket.onPacketRcvd(
      makeDataPacket(/*seqNum=*/0, kDeferredPsn, /*ackReq=*/true));
  ASSERT_TRUE(sentAcks().empty());

  recovery().setAction(PsnAction::ACCEPT_AND_REQUEST_NACK);
  socket.onPacketRcvd(
      makeDataPacket(/*seqNum=*/1, kFlushingPsn, /*ackReq=*/true));

  EXPECT_EQ(
      sentAcks(),
      (std::vector<AckKey>{{0u, kDeferredPsn}, {1u, kFlushingPsn}}));
  EXPECT_EQ(recovery().missingMessages(), (std::vector<AckKey>{{1u, 1u}}));
  EXPECT_EQ(recovery().takeReadyDeferredAckCalls(), 1);
}

TEST_F(
    RoceSocketPsnActionTest,
    SendNackDropsThePacketAndRequestsGapRecoveryWithoutAcking) {
  auto& socket = makeSocketWithAction(PsnAction::SEND_NACK);

  socket.onPacketRcvd(makeDataPacket(/*seqNum=*/0, kFreshPsn, /*ackReq=*/true));

  EXPECT_EQ(*socket.getStats().recvdDataPkts(), 0u);
  EXPECT_TRUE(sentAcks().empty());
  EXPECT_EQ(recovery().missingMessages(), (std::vector<AckKey>{{0u, 1u}}));
  EXPECT_EQ(socket.expectedMsgs(), 1u);
  // A rejected packet must not flush the deferred-ACK backlog.
  EXPECT_EQ(recovery().takeReadyDeferredAckCalls(), 0);
}

TEST_F(RoceSocketPsnActionTest, AckDuplicateReAcksItsOwnPsnExactlyOnce) {
  auto& socket = makeSocketWithAction(PsnAction::ACK_DUPLICATE);

  socket.onPacketRcvd(
      makeDataPacket(/*seqNum=*/0, kDuplicatePsn, /*ackReq=*/true));

  // The duplicate pre-check resolves the packet, so isPsnOk runs once and the
  // ACK is not doubled by the main check.
  EXPECT_EQ(recovery().checkPsnCalls(), 1);
  EXPECT_EQ(sentAcks(), (std::vector<AckKey>{{0u, kDuplicatePsn}}));
  EXPECT_EQ(*socket.getStats().recvdDataPkts(), 0u);
  EXPECT_EQ(recovery().takeReadyDeferredAckCalls(), 0);
}

TEST_F(RoceSocketPsnActionTest, AckDuplicateWithoutAckRequestStaysSilent) {
  auto& socket = makeSocketWithAction(PsnAction::ACK_DUPLICATE);

  socket.onPacketRcvd(
      makeDataPacket(/*seqNum=*/0, kDuplicatePsn, /*ackReq=*/false));

  EXPECT_TRUE(sentPackets_.empty());
  EXPECT_EQ(*socket.getStats().recvdDataPkts(), 0u);
}

TEST_F(RoceSocketPsnActionTest, DropDiscardsThePacketWithNoSideEffects) {
  auto& socket = makeSocketWithAction(PsnAction::DROP);

  socket.onPacketRcvd(makeDataPacket(/*seqNum=*/0, kFreshPsn, /*ackReq=*/true));

  EXPECT_EQ(*socket.getStats().recvdDataPkts(), 0u);
  EXPECT_TRUE(sentPackets_.empty());
  EXPECT_TRUE(recovery().missingMessages().empty());
  EXPECT_TRUE(recovery().pendingDeferredAcks().empty());
  EXPECT_EQ(socket.expectedMsgs(), 1u);
  // A dropped packet must not flush the deferred-ACK backlog.
  EXPECT_EQ(recovery().takeReadyDeferredAckCalls(), 0);
}

} // namespace packsim
