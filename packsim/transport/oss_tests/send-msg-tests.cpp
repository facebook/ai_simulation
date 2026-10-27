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
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/transport_stats_types.h"
#include "packsim/transport/oss_tests/send-msg-test-fixture.h"
#include "packsim/transport/roce-base.h"
#include "packsim/transport/transport.h"

namespace packsim {

class OptimisticRoceTest : public TransportTest {
  void makeTransports() override {
    RoceTransportConfig roceCfg;
    srcTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, srcId_, &nicCfg_, roceCfg);
    dstTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, dstId_, &nicCfg_, roceCfg);
  }
};

class GoBackNRoceTest : public TransportTest {
  void makeTransports() override {
    RoceTransportConfig roceCfg;
    roceCfg.errorRecovery = RoceTransportConfig::ErrorRecovery::GO_BACK_N;
    roceCfg.rnrTimeIdx = 1;
    srcTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, srcId_, &nicCfg_, roceCfg);
    dstTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, dstId_, &nicCfg_, roceCfg);
  }
};

class DcqcnRoceTest : public TransportTest {
  void makeTransports() override {
    *nicCfg_.dcqcnCfg()->dcqcnEnable() = true;
    RoceTransportConfig roceCfg;
    roceCfg.congestionControl = RoceTransportConfig::CongestionControl::DCQCN;
    srcTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, srcId_, &nicCfg_, roceCfg);
    dstTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, dstId_, &nicCfg_, roceCfg);
  }
};

class GoBackNDcqcnRoceTest : public TransportTest {
  void makeTransports() override {
    *nicCfg_.dcqcnCfg()->dcqcnEnable() = true;
    RoceTransportConfig roceCfg;
    roceCfg.errorRecovery = RoceTransportConfig::ErrorRecovery::GO_BACK_N;
    roceCfg.congestionControl = RoceTransportConfig::CongestionControl::DCQCN;
    roceCfg.rnrTimeIdx = 1;
    srcTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, srcId_, &nicCfg_, roceCfg);
    dstTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, dstId_, &nicCfg_, roceCfg);
  }
};

class GoBackNDcqcnOooRoceTest : public TransportTest {
  void makeTransports() override {
    *nicCfg_.dcqcnCfg()->dcqcnEnable() = true;
    nicCfg_.outofOrderPktSupport() = true;
    RoceTransportConfig roceCfg;
    roceCfg.errorRecovery = RoceTransportConfig::ErrorRecovery::GO_BACK_N;
    roceCfg.congestionControl = RoceTransportConfig::CongestionControl::DCQCN;
    roceCfg.rnrTimeIdx = 1;
    roceCfg.selectiveGapRepair = true;
    srcTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, srcId_, &nicCfg_, roceCfg);
    dstTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, dstId_, &nicCfg_, roceCfg);
  }
};

// The receiver accepts every PSN when outofOrderPktSupport is on and selective
// gap repair is off, so a timer-driven tail resend would be taken as new data.
class GoBackNOooRoceTest : public TransportTest {
  void makeTransports() override {
    nicCfg_.outofOrderPktSupport() = true;
    RoceTransportConfig roceCfg;
    roceCfg.errorRecovery = RoceTransportConfig::ErrorRecovery::GO_BACK_N;
    roceCfg.rnrTimeIdx = 1;
    srcTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, srcId_, &nicCfg_, roceCfg);
    dstTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, dstId_, &nicCfg_, roceCfg);
  }
};

class DcqcnEcnCnpTest : public TransportTest {
  void makeTransports() override {
    *nicCfg_.dcqcnCfg()->dcqcnEnable() = true;
    RoceTransportConfig roceCfg;
    roceCfg.congestionControl = RoceTransportConfig::CongestionControl::DCQCN;
    srcTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, srcId_, &nicCfg_, roceCfg);
    dstTransport_ = std::make_unique<RoceTransportBase>(
        &simCtx_, dstId_, &nicCfg_, roceCfg);
  }

  bool srcSendPacket(const PacketPtr& pkt) override {
    auto* rocePkt = pkt.as<RocePacket>();
    if (!rocePkt->isAck && !rocePkt->isCnp && !rocePkt->isNack &&
        ecnMarkCount_ < kEcnMarkLimit) {
      pkt.as<Ipv6Packet>()->ecn = Ipv6Packet::EcnType::ECN_CE;
      ++ecnMarkCount_;
    }
    return TransportTest::srcSendPacket(pkt);
  }

  bool dstSendPacket(const PacketPtr& pkt) override {
    if (pkt.as<RocePacket>()->isCnp) {
      ++cnpCount_;
    }
    return TransportTest::dstSendPacket(pkt);
  }

 protected:
  uint32_t cnpCount_{0};
  uint32_t ecnMarkCount_{0};
  static constexpr uint32_t kEcnMarkLimit{3};
};

// Two messages in flight on one QP, so the first message's ACK can be lost
// while the second is still outstanding.
class GoBackNTwoMessageRoceTest : public GoBackNRoceTest {
 public:
  void srcMsgAcked(Message* msg) override {
    ackedMsgs_.emplace_back(msg);
    stopWhenDone();
  }

  void dstMsgRcvd(Message* msg) override {
    rcvdMsgs_.emplace_back(msg);
    stopWhenDone();
  }

  void sendMsgs() {
    if (srcSocket_ == nullptr || dstSocket_ == nullptr) {
      FAIL() << "SetUp connected no sockets";
    }
    for (uint32_t id = 0; id < kNumMsgs; ++id) {
      const Message msg{
          .sizeBytes = kMsgSize,
          .srcIdx = srcId_,
          .dstIdx = dstId_,
          .connectionMessageId = id};
      auto& queued = msgs_.emplace_back(std::make_unique<Message>(msg));
      srcSocket_->send(queued.get());
      dstSocket_->recv(queued.get());
    }
  }

 protected:
  void stopWhenDone() {
    if (ackedMsgs_.size() == kNumMsgs && rcvdMsgs_.size() == kNumMsgs) {
      scheduler_.stop();
    }
  }

  static constexpr uint32_t kNumMsgs{2};
  std::vector<std::unique_ptr<Message>> msgs_;
  std::vector<Message*> ackedMsgs_;
  std::vector<Message*> rcvdMsgs_;
};

TEST_F(OptimisticRoceTest, SendMessage) {
  scheduleSendMsgAndCheck();
}

// RoCEv2 models SEND/RECV and WRITE but not RDMA READ: a PULL workload's READ,
// posted as a receive with isRead set, must fail at once rather than wait for
// a SEND that never comes.
TEST_F(OptimisticRoceTest, ReadReceiveThrows) {
  Message msg{.sizeBytes = kMsgSize, .srcIdx = srcId_, .dstIdx = dstId_};
  msg.isRead = true;

  EXPECT_THAT(
      [&] { dstSocket_->recv(&msg); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("does not model RDMA READ")));
}

TEST_F(OptimisticRoceTest, LeavesTransportStatsUnset) {
  const auto summary = srcTransport_->getNicStatsSummaryResults();

  EXPECT_EQ(
      summary.nicStats()->transport()->getType(),
      packsim::thrift::TransportStats::Type::__EMPTY__);
}

TEST_F(GoBackNRoceTest, SendMessage) {
  scheduleSendMsgAndCheck();
}

// The two tests below pin the wiring that lets a switch tell a recoverable
// drop from a fatal one: RoceV2 lossRecovery goBackN -> ErrorRecovery::
// GO_BACK_N -> a non-null errorRecovery_ on the socket -> lossRecoveryEnabled
// stamped at encap -> isRetransmittingTransport. Asserting on a real
// transport, not a hand-set flag, is what makes that chain a test rather than
// a restatement.
TEST_F(GoBackNRoceTest, StampsLossRecoveryEnabledOnSentPackets) {
  scheduleSendMsgAndCheck();

  ASSERT_TRUE(firstSentLossRecoveryEnabled_.has_value())
      << "the fixture sent no packet, so there was nothing to inspect";
  EXPECT_TRUE(*firstSentLossRecoveryEnabled_)
      << "a goBackN QP retransmits, so its packets must be marked recoverable";
}

TEST_F(OptimisticRoceTest, LeavesLossRecoveryEnabledUnsetOnSentPackets) {
  scheduleSendMsgAndCheck();

  ASSERT_TRUE(firstSentLossRecoveryEnabled_.has_value())
      << "the fixture sent no packet, so there was nothing to inspect";
  EXPECT_FALSE(*firstSentLossRecoveryEnabled_)
      << "a QP with dummy loss recovery never retransmits, so losing its "
         "packet stays unrecoverable";
}

TEST_F(DcqcnRoceTest, SendMessage) {
  scheduleSendMsgAndCheck();
}

TEST_F(GoBackNDcqcnRoceTest, SendMessage) {
  scheduleSendMsgAndCheck();
}

/** ECN → CNP flow: verify CNP generation when data packets are ECN-marked **/
TEST_F(DcqcnEcnCnpTest, EcnMarkedPacketTriggersCnp) {
  scheduleSendMsgAndCheck();
  EXPECT_GT(cnpCount_, 0u);
}

// The test's send hook marks the first data packets CE before handing them to
// the sender's transport, as the NIC's own egress queue would; the sender sees
// them leave marked and the receiver sees them arrive marked.
TEST_F(DcqcnEcnCnpTest, EcnMarkedDataCountsAsSentAndReceivedEcn) {
  scheduleSendMsgAndCheck();

  const uint8_t dataCos = *nicCfg_.dataCos();
  EXPECT_EQ(*srcSocket_->getStats().sentEcnPkts(), kEcnMarkLimit);
  EXPECT_EQ(
      *srcTransport_->getNicStatsSummaryResults()
           .nicStats()
           ->perCosStats()
           ->at(dataCos)
           .sentEcnPkts(),
      kEcnMarkLimit);
  EXPECT_EQ(*dstSocket_->getStats().recvdEcnPkts(), kEcnMarkLimit);
  EXPECT_EQ(*dstSocket_->getStats().sentEcnPkts(), 0);
}

/** Only for loss-tolerant transports **/
TEST_F(GoBackNRoceTest, SendMessageLossy) {
  lossy_ = true;
  scheduleSendMsgAndCheck();

  const auto summary = srcTransport_->getNicStatsSummaryResults();
  const auto& transportStats = *summary.nicStats()->transport();
  ASSERT_EQ(
      transportStats.getType(), packsim::thrift::TransportStats::Type::roceV2);
  const auto& roceV2Stats = transportStats.roceV2().value();
  EXPECT_GT(*roceV2Stats.retxPkts(), 0u);
  EXPECT_GT(*roceV2Stats.retxBytes(), 0u);
  EXPECT_EQ(*roceV2Stats.retxByTimer(), 0u);
  EXPECT_EQ(*roceV2Stats.retxByNack(), *roceV2Stats.retxPkts());
}

// An in-order receiver sees no gap when a message's last packet is lost, so
// only the sender's tail timer can recover it.
TEST_F(GoBackNRoceTest, RecoversDroppedTailDataPacket) {
  scheduler_.setLookaheadTimeFs(Time::fromMs(2).getFs());
  dropTailData_ = true;
  scheduleSendMsgAndCheck();

  const auto summary = srcTransport_->getNicStatsSummaryResults();
  const auto& transportStats = *summary.nicStats()->transport();
  ASSERT_EQ(
      transportStats.getType(), packsim::thrift::TransportStats::Type::roceV2);
  const auto& roceV2Stats = transportStats.roceV2().value();
  EXPECT_GT(*roceV2Stats.retxByTimer(), 0u);
  EXPECT_EQ(*roceV2Stats.retxByNack(), 0u);
  EXPECT_EQ(*roceV2Stats.retxPkts(), *roceV2Stats.retxByTimer());
}

TEST_F(GoBackNRoceTest, RecoversDroppedFinalAck) {
  scheduler_.setLookaheadTimeFs(Time::fromMs(2).getFs());
  dropAck_ = true;
  scheduleSendMsgAndCheck();

  const auto summary = srcTransport_->getNicStatsSummaryResults();
  const auto& transportStats = *summary.nicStats()->transport();
  ASSERT_EQ(
      transportStats.getType(), packsim::thrift::TransportStats::Type::roceV2);
  const auto& roceV2Stats = transportStats.roceV2().value();
  EXPECT_GT(*roceV2Stats.retxByTimer(), 0u);
  EXPECT_EQ(*roceV2Stats.retxByNack(), 0u);
  EXPECT_EQ(*roceV2Stats.retxPkts(), *roceV2Stats.retxByTimer());
}

TEST_F(GoBackNTwoMessageRoceTest, LaterAckCompletesMessageWhoseAckWasLost) {
  dropAck_ = true;
  simCtx_.scheduleNow(&GoBackNTwoMessageRoceTest::sendMsgs, this);
  scheduler_.run();

  const std::vector<Message*> sentInOrder{msgs_[0].get(), msgs_[1].get()};
  EXPECT_EQ(ackedMsgs_, sentInOrder);
  EXPECT_EQ(rcvdMsgs_, sentInOrder);
  EXPECT_EQ(simCtx_.dataRcvd, kNumMsgs * kMsgSize);
  const auto summary = srcTransport_->getNicStatsSummaryResults();
  const auto& transportStats = *summary.nicStats()->transport();
  ASSERT_EQ(
      transportStats.getType(), packsim::thrift::TransportStats::Type::roceV2);
  EXPECT_EQ(*transportStats.roceV2().value().retxPkts(), 0u);
}

TEST_F(GoBackNOooRoceTest, DroppedFinalAckArmsNoRetransmitTimer) {
  scheduler_.setLookaheadTimeFs(Time::fromMs(2).getFs());
  dropAck_ = true;
  simCtx_.scheduleNow(&TransportTest::sendMsg, this);
  scheduler_.run(Time::fromMs(10));

  EXPECT_FALSE(srcAckd_);
  EXPECT_TRUE(dstRcvd_);
  EXPECT_EQ(simCtx_.dataRcvd, kMsgSize);
  const auto summary = srcTransport_->getNicStatsSummaryResults();
  const auto& transportStats = *summary.nicStats()->transport();
  ASSERT_EQ(
      transportStats.getType(), packsim::thrift::TransportStats::Type::roceV2);
  EXPECT_EQ(*transportStats.roceV2().value().retxPkts(), 0u);
}

// With selective gap repair off, no gap NACK or retransmit timer resends the
// dropped packet, and the tail's ACK removes it from the sender's retransmit
// queue: the behaviour GoBackNLrConfig.selectiveGapRepair documents for its
// false setting.
TEST_F(GoBackNOooRoceTest, DroppedMiddlePacketIsNeverRetransmitted) {
  scheduler_.setLookaheadTimeFs(Time::fromMs(2).getFs());
  lossy_ = true;
  simCtx_.scheduleNow(&TransportTest::sendMsg, this);
  scheduler_.run(Time::fromMs(5));

  EXPECT_TRUE(srcDropped_);
  EXPECT_TRUE(srcAckd_);
  EXPECT_FALSE(dstRcvd_);
  EXPECT_LT(simCtx_.dataRcvd, kMsgSize);
  const auto summary = srcTransport_->getNicStatsSummaryResults();
  const auto& transportStats = *summary.nicStats()->transport();
  ASSERT_EQ(
      transportStats.getType(), packsim::thrift::TransportStats::Type::roceV2);
  EXPECT_EQ(*transportStats.roceV2().value().retxPkts(), 0u);
}

TEST_F(GoBackNDcqcnRoceTest, SendMessageLossy) {
  lossy_ = true;
  scheduleSendMsgAndCheck();
}

TEST_F(GoBackNDcqcnOooRoceTest, RecoversDroppedMiddlePacket) {
  lossy_ = true;
  scheduleSendMsgAndCheck();
}

TEST_F(GoBackNDcqcnOooRoceTest, RecoversDroppedTailDataPacket) {
  scheduler_.setLookaheadTimeFs(Time::fromMs(2).getFs());
  dropTailData_ = true;
  scheduleSendMsgAndCheck();

  const auto summary = srcTransport_->getNicStatsSummaryResults();
  const auto& transportStats = *summary.nicStats()->transport();
  ASSERT_EQ(
      transportStats.getType(), packsim::thrift::TransportStats::Type::roceV2);
  const auto& roceV2Stats = transportStats.roceV2().value();
  EXPECT_GT(*roceV2Stats.retxByTimer(), 0u);
  EXPECT_EQ(*roceV2Stats.retxByNack(), 0u);
  EXPECT_EQ(*roceV2Stats.retxPkts(), *roceV2Stats.retxByTimer());
}

TEST_F(GoBackNDcqcnOooRoceTest, RecoversDroppedFinalAck) {
  scheduler_.setLookaheadTimeFs(Time::fromMs(2).getFs());
  dropAck_ = true;
  scheduleSendMsgAndCheck();
}

TEST_F(GoBackNDcqcnOooRoceTest, RecoversLocallyDroppedCallbackPackets) {
  scheduler_.setLookaheadTimeFs(Time::fromMs(10).getFs());
  localTailDrops_ = 8;
  scheduleSendMsgAndCheck();
}
} // namespace packsim
