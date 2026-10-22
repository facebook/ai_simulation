/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/transport/roce-base.h"

#include <memory>
#include <optional>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/kernel/device-id.h"

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim {

// Transport subclass that suppresses automatic sendNext on socketReady,
// allowing tests to control when sendNext runs.
class TestableRoceTransport : public RoceTransportBase {
 public:
  using RoceTransportBase::RoceTransportBase;

  // Expose internals for testing.
  using RoceTransportBase::activeRoceSockets_;
  using RoceTransportBase::activeSends_;
  using RoceTransportBase::maxActiveSends_;
  using RoceTransportBase::nextSocketIndex_;
  using RoceTransportBase::trySendOnSocket;

  void sendNext() override {
    if (autoSend_) {
      RoceTransportBase::sendNext();
    }
  }

  void realSendNext() {
    RoceTransportBase::sendNext();
  }

  bool autoSend_{false};
};

class SendNextTest : public ::testing::Test {
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
        std::make_unique<TestableRoceTransport>(&simCtx_, srcId_, &nicCfg_);

    transport_->setSendPacketFn([this](const PacketPtr&) {
      sentPacketCount_++;
      return true;
    });
    transport_->setOnMessageAckedCallback([](Message*) {});
    transport_->setOnMessageReceivedCallback([](Message*) {});
  }

 protected:
  Scheduler scheduler_;
  Tracer::ConditionalMap conditionalMap_;
  SimContext simCtx_{&scheduler_, "SimContext", 0, &conditionalMap_};
  FlexDeviceId srcId_{{1, 0, 0, 1}};

  packsim::thrift::NicConfig nicCfg_;
  std::unique_ptr<TestableRoceTransport> transport_;
  uint32_t sentPacketCount_{0};
  std::vector<std::unique_ptr<Message>> messages_;

  RoceSocketBase* connectSocket(const FlexDeviceId& dst, uint32_t qpn) {
    return static_cast<RoceSocketBase*>(
        transport_->connect(dst, 1234, qpn, qpn));
  }

  void queueMessage(RoceSocketBase* socket, uint32_t sizeBytes) {
    auto msg = std::make_unique<Message>();
    msg->sizeBytes = sizeBytes;
    msg->srcIdx = srcId_;
    msg->dstIdx = socket->getDstId();
    socket->send(msg.get());
    messages_.emplace_back(std::move(msg));
  }
};

// -- hasWork tests --

TEST_F(SendNextTest, HasWorkReturnsFalseWhenEmpty) {
  auto* socket = connectSocket(FlexDeviceId{{1, 0, 0, 2}}, 0);
  EXPECT_FALSE(socket->hasWork());
}

TEST_F(SendNextTest, HasWorkReturnsTrueWithQueuedMessages) {
  auto* socket = connectSocket(FlexDeviceId{{1, 0, 0, 2}}, 0);
  queueMessage(socket, 1000);
  EXPECT_TRUE(socket->hasWork());
}

// -- trySendOnSocket tests --

TEST_F(SendNextTest, TrySendReturnsFalseWhenNoWork) {
  auto* socket = connectSocket(FlexDeviceId{{1, 0, 0, 2}}, 0);
  EXPECT_FALSE(transport_->trySendOnSocket(socket));
}

TEST_F(SendNextTest, TrySendReturnsTrueWhenMessagesQueued) {
  auto* socket = connectSocket(FlexDeviceId{{1, 0, 0, 2}}, 0);
  queueMessage(socket, 1000);
  EXPECT_TRUE(transport_->trySendOnSocket(socket));
}

TEST_F(SendNextTest, TrySendDrainsQueuedMessage) {
  auto* socket = connectSocket(FlexDeviceId{{1, 0, 0, 2}}, 0);
  queueMessage(socket, 1000);

  transport_->trySendOnSocket(socket);

  EXPECT_EQ(socket->queuedMsgs(), 0);
  EXPECT_FALSE(socket->hasWork());
}

TEST_F(SendNextTest, RnrBackoffDefersQueuedMessage) {
  RoceTransportConfig roceCfg;
  roceCfg.errorRecovery = RoceTransportConfig::ErrorRecovery::GO_BACK_N;
  roceCfg.rnrTimeIdx = 0;
  TestableRoceTransport transport(&simCtx_, srcId_, &nicCfg_, roceCfg);
  transport.setSendPacketFn([this](const PacketPtr&) {
    ++sentPacketCount_;
    return true;
  });
  transport.setOnMessageAckedCallback([](Message*) {});
  transport.setOnMessageReceivedCallback([](Message*) {});

  auto* socket = static_cast<RoceSocketBase*>(
      transport.connect(FlexDeviceId{{1, 0, 0, 2}}, 1234, 0, 0));
  auto* recovery = socket->getErrorRecovery();
  ASSERT_NE(recovery, nullptr);
  recovery->onNack(
      /*expectedPsn=*/1, /*rnrBackoffIdx=*/std::optional<uint8_t>{0});
  transport.autoSend_ = true;

  Message message;
  message.sizeBytes = 1000;
  message.srcIdx = srcId_;
  message.dstIdx = socket->getDstId();
  socket->send(&message);

  EXPECT_EQ(socket->queuedMsgs(), 1u);
  EXPECT_EQ(sentPacketCount_, 0u);
}

// -- sendNext tests --

TEST_F(SendNextTest, SendNextStopsAtMaxActiveSends) {
  transport_->maxActiveSends_ = 1;
  auto* s1 = connectSocket(FlexDeviceId{{1, 0, 0, 2}}, 0);
  auto* s2 = connectSocket(FlexDeviceId{{1, 0, 0, 3}}, 1);
  queueMessage(s1, 1000);
  queueMessage(s2, 1000);

  transport_->realSendNext();

  EXPECT_EQ(transport_->activeSends_, 1)
      << "Should stop after reaching maxActiveSends";
}

TEST_F(SendNextTest, SendNextServesAllSockets) {
  auto* s1 = connectSocket(FlexDeviceId{{1, 0, 0, 2}}, 0);
  auto* s2 = connectSocket(FlexDeviceId{{1, 0, 0, 3}}, 1);
  auto* s3 = connectSocket(FlexDeviceId{{1, 0, 0, 4}}, 2);
  queueMessage(s1, 1000);
  queueMessage(s2, 1000);
  queueMessage(s3, 1000);

  transport_->realSendNext();

  EXPECT_EQ(transport_->activeSends_, 3)
      << "All three sockets should have sent";
}

TEST_F(SendNextTest, SendNextRemovesIdleSockets) {
  auto* s1 = connectSocket(FlexDeviceId{{1, 0, 0, 2}}, 0);
  queueMessage(s1, 1000);

  ASSERT_EQ(transport_->activeRoceSockets_.size(), 1);

  transport_->realSendNext();

  EXPECT_TRUE(transport_->activeRoceSockets_.empty())
      << "Socket with no remaining work should be removed from active set";
}

TEST_F(SendNextTest, SendNextKeepsSocketsWithRemainingWork) {
  auto* s1 = connectSocket(FlexDeviceId{{1, 0, 0, 2}}, 0);
  // Queue multiple messages — first sendNext sends one quota chunk,
  // second message remains.
  queueMessage(s1, 1000);
  queueMessage(s1, 1000);

  transport_->realSendNext();

  // After one quota chunk, the second message is still queued but the socket
  // was already served. It should remain in the active set for next round.
  // Note: both messages fit in one quota (65536 default), so both are sent.
  // Use a message larger than the quota to test partial sends.
  EXPECT_FALSE(s1->hasWork()) << "Both small messages fit in one quota chunk";
}

TEST_F(SendNextTest, SendNextKeepsSocketWithLargeMessage) {
  // byteQuota_ is 65536 by default. With maxActiveSends_=1, only one quota
  // chunk is sent, leaving the rest of the message queued.
  transport_->maxActiveSends_ = 1;
  auto* s1 = connectSocket(FlexDeviceId{{1, 0, 0, 2}}, 0);
  queueMessage(s1, 200000);

  transport_->realSendNext();

  EXPECT_TRUE(s1->hasWork())
      << "Large message should not be fully sent in one quota chunk";
  EXPECT_FALSE(transport_->activeRoceSockets_.empty())
      << "Socket with remaining work should stay in active set";
}

TEST_F(SendNextTest, SendNextSetsNextSocketIndex) {
  auto* s1 = connectSocket(FlexDeviceId{{1, 0, 0, 2}}, 0);
  auto* s2 = connectSocket(FlexDeviceId{{1, 0, 0, 3}}, 1);
  queueMessage(s1, 1000);
  queueMessage(s2, 1000);

  transport_->realSendNext();

  // After serving all sockets, nextSocketIndex_ should be set or nullopt
  // depending on whether any sockets remain in the active set.
  // Both small messages are fully sent, so active set should be empty.
  EXPECT_FALSE(transport_->nextSocketIndex_.has_value())
      << "No remaining sockets means no next index";
}

TEST_F(SendNextTest, SendNextExitsWhenAllSocketsBlocked) {
  // Connect sockets but don't queue any messages.
  // Manually add them to active set to simulate blocked state.
  auto* s1 = connectSocket(FlexDeviceId{{1, 0, 0, 2}}, 0);
  auto* s2 = connectSocket(FlexDeviceId{{1, 0, 0, 3}}, 1);

  // Manually add to active set — no messages queued, so send() won't
  // trigger socketReady. This simulates sockets stuck with no work.
  transport_->activeRoceSockets_[0] = s1;
  transport_->activeRoceSockets_[1] = s2;

  transport_->realSendNext();

  // Both sockets have no work, so they should be removed.
  EXPECT_TRUE(transport_->activeRoceSockets_.empty())
      << "Sockets with no work should all be removed";
  EXPECT_EQ(transport_->activeSends_, 0) << "No sends should have occurred";
}

TEST_F(SendNextTest, SendNextThrowsWhenNoSocketsExist) {
  // No connect() calls, so sockets_ is empty.
  // But activeRoceSockets_ is also empty, so the loop won't execute.
  // The throw only fires if activeSends_ < maxActiveSends_ and sockets_ empty.
  EXPECT_THAT(
      [&] { transport_->realSendNext(); },
      ThrowsMessage<std::runtime_error>(HasSubstr("No sockets present")));
}

} // namespace packsim
