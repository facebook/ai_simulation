/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>
#include <optional>
#include <stdexcept>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/packet/roce.h"
#include "packsim/transport/error-recovery.h"
#include "packsim/transport/roce-base.h"

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim {

class TestRoceTransport final : public RoceTransportBase {
 public:
  using RoceTransportBase::RoceTransportBase;

  void sendNext() override {
    ++sendNextCalls_;
  }

  int sendNextCalls() const noexcept {
    return sendNextCalls_;
  }

 private:
  int sendNextCalls_{0};
};

class StubErrorRecovery final : public ErrorRecovery {
 public:
  struct Config {
    bool useByteCount{false};
    bool overrideAdjustAckPsn{false};
    uint32_t overriddenPsn{0};
    bool failed{false};
    PsnAction psnAction{PsnAction::ACCEPT};
    bool ackIsStale{false};
    std::optional<uint32_t> ackNextIncompleteSeqNum{};
  };

  explicit StubErrorRecovery(Config cfg) : cfg_(cfg) {}

  bool useByteCountCompletion() const noexcept override {
    return cfg_.useByteCount;
  }

  uint32_t adjustAckPsn(uint32_t requestedAckPsn, uint32_t expectedPsn)
      const noexcept override {
    ++adjustAckPsnCalls_;
    lastRequestedAckPsn_ = requestedAckPsn;
    lastExpectedPsn_ = expectedPsn;
    return cfg_.overrideAdjustAckPsn ? cfg_.overriddenPsn : requestedAckPsn;
  }

  bool isFailed() const noexcept override {
    return cfg_.failed;
  }

  void setFailed(bool failed) noexcept {
    cfg_.failed = failed;
  }

  void setAckResult(AckResult result) noexcept {
    cfg_.ackNextIncompleteSeqNum = result.nextIncompleteSeqNum;
    cfg_.ackIsStale = result.isStale;
  }

  int adjustAckPsnCalls() const noexcept {
    return adjustAckPsnCalls_;
  }

  uint32_t lastRequestedAckPsn() const noexcept {
    return lastRequestedAckPsn_;
  }

  uint32_t lastExpectedPsn() const noexcept {
    return lastExpectedPsn_;
  }

  int onPacketScheduledCalls() const noexcept {
    return onPacketScheduledCalls_;
  }

  int onAckCalls() const noexcept {
    return onAckCalls_;
  }

  int onNackCalls() const noexcept {
    return onNackCalls_;
  }

  PsnAction checkPsn(uint32_t& expectedPsn, uint32_t receivedPsn, bool)
      override {
    if (cfg_.psnAction == PsnAction::ACCEPT && receivedPsn == expectedPsn) {
      ++expectedPsn;
    }
    return cfg_.psnAction;
  }
  void onPacketScheduled(const RoceSendInfo&) override {
    ++onPacketScheduledCalls_;
  }
  AckResult onAck(uint32_t) override {
    ++onAckCalls_;
    return AckResult{
        .nextIncompleteSeqNum = cfg_.ackNextIncompleteSeqNum,
        .isStale = cfg_.ackIsStale};
  }
  void onNack(uint32_t, std::optional<uint8_t>) override {
    ++onNackCalls_;
  }
  RetransmitBatch getRetransmitBatch(uint64_t) override {
    return {};
  }
  bool hasRetransmitPending() const noexcept override {
    return false;
  }

 private:
  Config cfg_;
  mutable int adjustAckPsnCalls_{0};
  mutable uint32_t lastRequestedAckPsn_{0};
  mutable uint32_t lastExpectedPsn_{0};
  int onPacketScheduledCalls_{0};
  int onAckCalls_{0};
  int onNackCalls_{0};
};

class ErrorRecoveryAdpHooksTest : public ::testing::Test {
 public:
  void SetUp() override {
    nicCfg_.writeEff() = 100;
    nicCfg_.readGbps() = 400;
    nicCfg_.readLatencyNs() = 0;
    nicCfg_.perCosBufferSize() = {1024 * 1024, 1024 * 1024};
    nicCfg_.perCosXoff() = {1024 * 1024, 1024 * 1024};
    nicCfg_.perCosXon() = {1024 * 1024, 1024 * 1024};
    nicCfg_.speed() = "400Gbps";

    transport_ =
        std::make_unique<TestRoceTransport>(&simCtx_, srcId_, &nicCfg_);
    transport_->setSendPacketFn([this](const PacketPtr& pkt) {
      sentPackets_.push_back(pkt);
      return true;
    });
    transport_->setOnMessageAckedCallback([](Message*) {});
    transport_->setOnMessageReceivedCallback([](Message*) {});
  }

  std::unique_ptr<RoceSocketBase> makeSocket(
      const RoceSocketBase::ErrorRecoveryFactory& factory) {
    auto socket = std::make_unique<RoceSocketBase>(
        transport_.get(),
        srcId_,
        dstId_,
        /*srcPort=*/1234,
        /*localQPN=*/0,
        /*remoteQPN=*/0,
        /*smtu=*/4200,
        /*workload=*/nullptr,
        factory,
        /*congestionControlFactory=*/nullptr);
    // transport->connect normally wires this up, but we bypass it here.
    socket->setOnMessageAckedCallback([this](Message* msg) {
      ackedSequenceNums_.push_back(msg->sequenceNumber);
    });
    socket->setOnMessageReceivedCallback([](Message*) {});
    return socket;
  }

  std::unique_ptr<RoceSocketBase> makeSocketWithStub(
      StubErrorRecovery::Config stubCfg) {
    auto factory = [this, stubCfg](
                       RoceSocketBase&,
                       SimContext&) -> std::unique_ptr<ErrorRecovery> {
      auto stub = std::make_unique<StubErrorRecovery>(stubCfg);
      stubPtr_ = stub.get();
      return stub;
    };
    return makeSocket(factory);
  }

  static PacketPtr makeAckPacket(uint32_t seqNum, uint32_t psn) {
    auto pkt = createPacket<PacketType::ROCE>(0);
    auto* rocePkt = pkt.as<RocePacket>();
    rocePkt->isAck = true;
    rocePkt->sequenceNum = seqNum;
    rocePkt->psn = psn;
    return pkt;
  }

 protected:
  Scheduler scheduler_;
  Tracer::ConditionalMap conditionalMap_;
  SimContext simCtx_{&scheduler_, "SimContext", 0, &conditionalMap_};
  FlexDeviceId srcId_{{1, 0, 0, 1}};
  FlexDeviceId dstId_{{1, 0, 0, 2}};
  packsim::thrift::NicConfig nicCfg_;
  std::unique_ptr<TestRoceTransport> transport_;
  StubErrorRecovery* stubPtr_{nullptr};
  std::vector<PacketPtr> sentPackets_;
  std::vector<uint32_t> ackedSequenceNums_;
};

TEST_F(
    ErrorRecoveryAdpHooksTest,
    OOOEnabledByByteCountCompletionEvenWhenNicConfigDisablesIt) {
  nicCfg_.outofOrderPktSupport() = false;

  auto socket = makeSocketWithStub({.useByteCount = true});

  EXPECT_TRUE(socket->outOfOrderPktSupport());
}

TEST_F(ErrorRecoveryAdpHooksTest, OOOPreservedFromNicConfig) {
  nicCfg_.outofOrderPktSupport() = true;

  auto socket = makeSocketWithStub({.useByteCount = false});

  EXPECT_TRUE(socket->outOfOrderPktSupport());
}

TEST_F(ErrorRecoveryAdpHooksTest, OOODisabledWhenNeitherEnablesIt) {
  nicCfg_.outofOrderPktSupport() = false;

  auto socket = makeSocketWithStub({.useByteCount = false});

  EXPECT_FALSE(socket->outOfOrderPktSupport());
}

TEST_F(ErrorRecoveryAdpHooksTest, AdjustedPsnIsWrittenToOutgoingAck) {
  nicCfg_.outofOrderPktSupport() = false;
  auto socket =
      makeSocketWithStub({.overrideAdjustAckPsn = true, .overriddenPsn = 42});

  Message msg{
      .sizeBytes = 64,
      .srcIdx = dstId_,
      .dstIdx = srcId_,
  };
  socket->recv(&msg);

  auto pkt = createPacket<PacketType::ROCE>(msg.sizeBytes);
  auto* rocePkt = pkt.as<RocePacket>();
  rocePkt->setOpcode(/*isFirstPkt=*/true, /*isLastPkt=*/true);
  rocePkt->ackReq = true;
  rocePkt->psn = 1;
  rocePkt->sequenceNum = 0;

  socket->onPacketRcvd(pkt);

  ASSERT_NE(stubPtr_, nullptr);
  EXPECT_EQ(stubPtr_->adjustAckPsnCalls(), 1);
  EXPECT_EQ(stubPtr_->lastRequestedAckPsn(), 1u);
  EXPECT_EQ(stubPtr_->lastExpectedPsn(), 2u);
  ASSERT_EQ(sentPackets_.size(), 1u);
  EXPECT_TRUE(sentPackets_.front().as<RocePacket>()->isAck);
  EXPECT_EQ(sentPackets_.front().as<RocePacket>()->psn, 42u);
}

TEST_F(ErrorRecoveryAdpHooksTest, AdjustAckPsnNotInvokedForNonAckPackets) {
  nicCfg_.outofOrderPktSupport() = false;
  auto socket = makeSocketWithStub({});

  socket->emitNackPacket(/*seqNum=*/0, /*psn=*/9, std::nullopt);

  ASSERT_NE(stubPtr_, nullptr);
  EXPECT_EQ(stubPtr_->adjustAckPsnCalls(), 0);
  ASSERT_EQ(sentPackets_.size(), 1u);
  EXPECT_TRUE(sentPackets_.front().as<RocePacket>()->isNack);
}

TEST_F(ErrorRecoveryAdpHooksTest, NullRecoveryPreservesNicConfig) {
  nicCfg_.outofOrderPktSupport() = true;
  auto socket = makeSocket(
      [](RoceSocketBase&, SimContext&) -> std::unique_ptr<ErrorRecovery> {
        return nullptr;
      });

  EXPECT_TRUE(socket->outOfOrderPktSupport());
  EXPECT_EQ(socket->getErrorRecovery(), nullptr);
}

TEST_F(ErrorRecoveryAdpHooksTest, FailedRecoveryReportsNoSendProgress) {
  auto socket = makeSocketWithStub({.failed = true});

  Message msg{
      .sizeBytes = 64,
      .srcIdx = srcId_,
      .dstIdx = dstId_,
  };
  socket->send(&msg);
  const auto [madeProgress, sentBytes] = socket->sendBytes(msg.sizeBytes);
  scheduler_.run();

  ASSERT_NE(stubPtr_, nullptr);
  EXPECT_FALSE(madeProgress);
  EXPECT_EQ(sentBytes, 0u);
  EXPECT_EQ(socket->queuedMsgs(), 1u);
  EXPECT_FALSE(socket->hasWork());
  EXPECT_EQ(stubPtr_->onPacketScheduledCalls(), 0);
  EXPECT_TRUE(sentPackets_.empty());
}

TEST_F(ErrorRecoveryAdpHooksTest, HealthyRecoverySendsQueuedPacket) {
  auto socket = makeSocketWithStub({.failed = false});

  Message msg{
      .sizeBytes = 64,
      .srcIdx = srcId_,
      .dstIdx = dstId_,
  };
  socket->send(&msg);
  const auto [madeProgress, sentBytes] = socket->sendBytes(msg.sizeBytes);
  scheduler_.run();

  ASSERT_NE(stubPtr_, nullptr);
  EXPECT_TRUE(madeProgress);
  EXPECT_EQ(sentBytes, msg.sizeBytes);
  EXPECT_EQ(stubPtr_->onPacketScheduledCalls(), 1);
  ASSERT_EQ(sentPackets_.size(), 1u);
  EXPECT_FALSE(sentPackets_.front().as<RocePacket>()->isAck);
}

TEST_F(ErrorRecoveryAdpHooksTest, FailureAfterSchedulingSuppressesPacket) {
  auto socket = makeSocketWithStub({.failed = false});

  Message msg{
      .sizeBytes = 64,
      .srcIdx = srcId_,
      .dstIdx = dstId_,
  };
  socket->send(&msg);
  const auto [madeProgress, sentBytes] = socket->sendBytes(msg.sizeBytes);
  ASSERT_NE(stubPtr_, nullptr);
  stubPtr_->setFailed(true);

  scheduler_.run();

  EXPECT_TRUE(madeProgress);
  EXPECT_EQ(sentBytes, msg.sizeBytes);
  EXPECT_EQ(stubPtr_->onPacketScheduledCalls(), 1);
  EXPECT_TRUE(sentPackets_.empty());
}

TEST_F(ErrorRecoveryAdpHooksTest, FailedRecoveryIgnoresAckPacket) {
  auto socket = makeSocketWithStub({.failed = true});

  auto pkt = createPacket<PacketType::ROCE>(0);
  auto* rocePkt = pkt.as<RocePacket>();
  rocePkt->isAck = true;
  rocePkt->psn = 1;
  rocePkt->sequenceNum = 0;

  socket->onPacketRcvd(pkt);

  ASSERT_NE(stubPtr_, nullptr);
  EXPECT_EQ(stubPtr_->onAckCalls(), 0);
}

TEST_F(ErrorRecoveryAdpHooksTest, FailedRecoveryIgnoresNackPacket) {
  auto socket = makeSocketWithStub({.failed = true});

  auto pkt = createPacket<PacketType::ROCE>(0);
  auto* rocePkt = pkt.as<RocePacket>();
  rocePkt->isNack = true;
  rocePkt->psn = 1;
  rocePkt->nackTimer = UINT8_MAX;

  socket->onPacketRcvd(pkt);

  ASSERT_NE(stubPtr_, nullptr);
  EXPECT_EQ(stubPtr_->onNackCalls(), 0);
}

TEST_F(ErrorRecoveryAdpHooksTest, BufferActionProcessesOutOfOrderPacket) {
  auto socket = makeSocketWithStub(
      {.useByteCount = true, .psnAction = PsnAction::BUFFER});
  Message msg{
      .sizeBytes = 64,
      .srcIdx = dstId_,
      .dstIdx = srcId_,
  };
  socket->recv(&msg);

  auto pkt = createPacket<PacketType::ROCE>(msg.sizeBytes);
  auto* rocePkt = pkt.as<RocePacket>();
  rocePkt->setOpcode(/*isFirstPkt=*/true, /*isLastPkt=*/true);
  rocePkt->ackReq = true;
  rocePkt->psn = 7;
  rocePkt->sequenceNum = 0;

  socket->onPacketRcvd(pkt);

  ASSERT_EQ(sentPackets_.size(), 1u);
  EXPECT_TRUE(sentPackets_.front().as<RocePacket>()->isAck);
  EXPECT_EQ(sentPackets_.front().as<RocePacket>()->psn, 7u);
}

TEST_F(
    ErrorRecoveryAdpHooksTest,
    StaleAckIsCountedAndWakesTransportWithoutCompletingMessage) {
  auto socket = makeSocketWithStub({.ackIsStale = true});

  Message msg{
      .sizeBytes = 64,
      .srcIdx = srcId_,
      .dstIdx = dstId_,
  };
  socket->send(&msg);
  const auto [madeProgress, sentBytes] = socket->sendBytes(msg.sizeBytes);
  scheduler_.run();
  const int sendNextCallsBeforeAck = transport_->sendNextCalls();

  socket->onPacketRcvd(makeAckPacket(/*seqNum=*/0, /*psn=*/1));

  EXPECT_TRUE(madeProgress);
  EXPECT_EQ(sentBytes, msg.sizeBytes);
  EXPECT_TRUE(ackedSequenceNums_.empty());
  EXPECT_EQ(*socket->getStats().recvdAckPkts(), 1u);
  EXPECT_EQ(transport_->sendNextCalls(), sendNextCallsBeforeAck + 1);

  ASSERT_NE(stubPtr_, nullptr);
  stubPtr_->setAckResult({});
  socket->onPacketRcvd(makeAckPacket(/*seqNum=*/0, /*psn=*/1));

  EXPECT_EQ(ackedSequenceNums_, std::vector<uint32_t>{0u});
}

TEST_F(
    ErrorRecoveryAdpHooksTest,
    CumulativeAckWatermarkCompletesMessagesBelowIt) {
  auto socket = makeSocketWithStub({.ackNextIncompleteSeqNum = 2});

  Message m0{.sizeBytes = 64, .srcIdx = srcId_, .dstIdx = dstId_};
  Message m1{.sizeBytes = 64, .srcIdx = srcId_, .dstIdx = dstId_};
  Message m2{.sizeBytes = 64, .srcIdx = srcId_, .dstIdx = dstId_};
  socket->send(&m0);
  socket->send(&m1);
  socket->send(&m2);
  const auto [madeProgress, sentBytes] = socket->sendBytes(192);
  scheduler_.run();

  socket->onPacketRcvd(makeAckPacket(/*seqNum=*/0, /*psn=*/1));

  EXPECT_TRUE(madeProgress);
  EXPECT_EQ(sentBytes, 192u);
  EXPECT_EQ(ackedSequenceNums_, (std::vector<uint32_t>{0u, 1u}));

  ASSERT_NE(stubPtr_, nullptr);
  stubPtr_->setAckResult({});
  socket->onPacketRcvd(makeAckPacket(/*seqNum=*/2, /*psn=*/2));

  EXPECT_EQ(ackedSequenceNums_, (std::vector<uint32_t>{0u, 1u, 2u}));
}

TEST_F(
    ErrorRecoveryAdpHooksTest,
    CumulativeAckFromCompletedMessageStillAdvancesWatermark) {
  auto socket = makeSocketWithStub({.ackNextIncompleteSeqNum = 1});

  Message m0{.sizeBytes = 64, .srcIdx = srcId_, .dstIdx = dstId_};
  Message m1{.sizeBytes = 64, .srcIdx = srcId_, .dstIdx = dstId_};
  Message m2{.sizeBytes = 64, .srcIdx = srcId_, .dstIdx = dstId_};
  socket->send(&m0);
  socket->send(&m1);
  socket->send(&m2);
  const auto [madeProgress, sentBytes] = socket->sendBytes(192);
  scheduler_.run();

  EXPECT_TRUE(madeProgress);
  EXPECT_EQ(sentBytes, 192u);

  socket->onPacketRcvd(makeAckPacket(/*seqNum=*/0, /*psn=*/1));
  EXPECT_EQ(ackedSequenceNums_, (std::vector<uint32_t>{0u}));

  ASSERT_NE(stubPtr_, nullptr);
  stubPtr_->setAckResult({.nextIncompleteSeqNum = 3});
  socket->onPacketRcvd(makeAckPacket(/*seqNum=*/0, /*psn=*/2));

  EXPECT_EQ(ackedSequenceNums_, (std::vector<uint32_t>{0u, 1u, 2u}));
}

TEST_F(ErrorRecoveryAdpHooksTest, CumulativeAckOnEmptySentMsgsIsNoOp) {
  auto socket = makeSocketWithStub({.ackNextIncompleteSeqNum = 5});
  const int sendNextCallsBeforeAck = transport_->sendNextCalls();

  EXPECT_NO_THROW(socket->onPacketRcvd(makeAckPacket(/*seqNum=*/0, /*psn=*/1)));
  EXPECT_TRUE(ackedSequenceNums_.empty());
  EXPECT_EQ(*socket->getStats().recvdAckPkts(), 1u);
  EXPECT_EQ(transport_->sendNextCalls(), sendNextCallsBeforeAck + 1);
}

TEST_F(ErrorRecoveryAdpHooksTest, ExactAckOnEmptySentMsgsThrows) {
  auto socket = makeSocketWithStub({});
  ASSERT_NE(socket, nullptr) << "makeSocketWithStub returned null";
  // @lint-ignore NULLSAFECLANG nullable-dereference -- ASSERT_NE is fatal.
  RoceSocketBase& socketRef = *socket;

  EXPECT_THAT(
      [&]() { socketRef.onPacketRcvd(makeAckPacket(/*seqNum=*/0, /*psn=*/1)); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Received ACK with no outstanding sent messages")));
  EXPECT_TRUE(ackedSequenceNums_.empty());
}

TEST_F(ErrorRecoveryAdpHooksTest, ExactAckWithMismatchedFifoSequenceThrows) {
  auto socket = makeSocketWithStub({});
  ASSERT_NE(socket, nullptr) << "makeSocketWithStub returned null";
  // @lint-ignore NULLSAFECLANG nullable-dereference -- ASSERT_NE is fatal.
  RoceSocketBase& socketRef = *socket;
  Message msg{.sizeBytes = 64, .srcIdx = srcId_, .dstIdx = dstId_};
  socketRef.send(&msg);
  const auto [madeProgress, sentBytes] = socketRef.sendBytes(msg.sizeBytes);
  scheduler_.run();

  ASSERT_TRUE(madeProgress);
  ASSERT_EQ(sentBytes, msg.sizeBytes);
  EXPECT_THAT(
      [&]() { socketRef.onPacketRcvd(makeAckPacket(/*seqNum=*/1, /*psn=*/1)); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Sequence number doesn't match ACK")));
  EXPECT_TRUE(ackedSequenceNums_.empty());
}

TEST_F(ErrorRecoveryAdpHooksTest, ExactAckWithMissingOOOSequenceThrows) {
  nicCfg_.outofOrderPktSupport() = true;
  auto socket = makeSocketWithStub({});
  ASSERT_NE(socket, nullptr) << "makeSocketWithStub returned null";
  // @lint-ignore NULLSAFECLANG nullable-dereference -- ASSERT_NE is fatal.
  RoceSocketBase& socketRef = *socket;
  Message msg{.sizeBytes = 64, .srcIdx = srcId_, .dstIdx = dstId_};
  socketRef.send(&msg);
  const auto [madeProgress, sentBytes] = socketRef.sendBytes(msg.sizeBytes);
  scheduler_.run();

  ASSERT_TRUE(madeProgress);
  ASSERT_EQ(sentBytes, msg.sizeBytes);
  EXPECT_THAT(
      [&]() { socketRef.onPacketRcvd(makeAckPacket(/*seqNum=*/1, /*psn=*/1)); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Expected sequence number not found")));
  EXPECT_TRUE(ackedSequenceNums_.empty());
}

TEST_F(ErrorRecoveryAdpHooksTest, LowerPsnAckCompletesEarlierMessage) {
  nicCfg_.outofOrderPktSupport() = true;
  auto socket = makeSocket([](RoceSocketBase& owner, SimContext& simCtx) {
    return std::make_unique<GoBackNRecovery>(
        /*rnrTimeIdx=*/0, owner, simCtx);
  });

  Message m0{.sizeBytes = 64, .srcIdx = srcId_, .dstIdx = dstId_};
  Message m1{.sizeBytes = 64, .srcIdx = srcId_, .dstIdx = dstId_};
  socket->send(&m0);
  socket->send(&m1);
  const auto [madeProgress, sentBytes] = socket->sendBytes(128);
  scheduler_.run();

  socket->onPacketRcvd(makeAckPacket(/*seqNum=*/1, /*psn=*/2));
  EXPECT_TRUE(ackedSequenceNums_.empty());

  socket->onPacketRcvd(makeAckPacket(/*seqNum=*/0, /*psn=*/1));

  EXPECT_TRUE(madeProgress);
  EXPECT_EQ(sentBytes, 128u);
  EXPECT_EQ(ackedSequenceNums_, (std::vector<uint32_t>{0u, 1u}));
}

} // namespace packsim
