/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>
#include <type_traits>

#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/transport/roce-base.h"
#include "packsim/transport/transport.h"

namespace packsim {

// A test transport that doesn't automatically send on socketReady().
// This allows us to manually control when sendBytes is called.
class TestRoceTransport : public RoceTransportBase {
 public:
  using RoceTransportBase::RoceTransportBase;

  void sendNext() override {
    // Do nothing - prevent automatic sending so tests can call sendBytes
    // directly
  }
};

// Test fixture for RoceSocketBase::sendBytes quota tracking
class RoceBaseQuotaTest : public ::testing::Test {
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

    transport_->setSendPacketFn([](const PacketPtr&) { return true; });
    transport_->setOnMessageAckedCallback([](Message*) {});
    transport_->setOnMessageReceivedCallback([](Message*) {});

    socket_ =
        static_cast<RoceSocketBase*>(transport_->connect(dstId_, 1234, 0, 0));
  }

 protected:
  Scheduler scheduler_;
  Tracer::ConditionalMap conditionalMap_;
  SimContext simCtx_{&scheduler_, "SimContext", 0, &conditionalMap_};
  FlexDeviceId srcId_{{1, 0, 0, 1}};
  FlexDeviceId dstId_{{1, 0, 0, 2}};
  packsim::thrift::NicConfig nicCfg_;

  std::unique_ptr<TestRoceTransport> transport_;
  RoceSocketBase* socket_{nullptr};

  std::vector<std::unique_ptr<Message>> messages_;

  void queueMessage(uint32_t sizeBytes) {
    auto msg = std::make_unique<Message>();
    msg->sizeBytes = sizeBytes;
    msg->srcIdx = srcId_;
    msg->dstIdx = dstId_;
    msg->sequenceNumber = static_cast<uint32_t>(messages_.size());
    msg->connectionMessageId = static_cast<uint32_t>(messages_.size());
    socket_->send(msg.get());
    messages_.emplace_back(std::move(msg));
  }
};

// Test that sendBytes respects the quota when a single message exceeds quota
TEST_F(RoceBaseQuotaTest, SingleMessageExceedsQuota) {
  queueMessage(10000);

  auto [_, sentBytes] = socket_->sendBytes(5000);

  EXPECT_EQ(sentBytes, 5000)
      << "Should send exactly quota bytes when message exceeds quota";
}

// Test that sendBytes respects the quota when processing multiple messages.
// This is the specific bug that was fixed: when multiple messages are queued,
// the loop was comparing against the original quota instead of remaining quota.
TEST_F(RoceBaseQuotaTest, MultipleMessagesRespectQuota) {
  queueMessage(3000);
  queueMessage(3000);
  queueMessage(3000);
  // Total: 9000 bytes, quota: 5000
  // Before fix: would send 3000 + 3000 = 6000 bytes (exceeds quota!)
  // After fix: sends 3000 + 2000 = 5000 bytes (respects quota)

  auto [_, sentBytes] = socket_->sendBytes(5000);

  EXPECT_EQ(sentBytes, 5000)
      << "Should send exactly 5000 bytes (3000 + 2000 partial)";
}

// Test that sendBytes sends exactly the quota when messages fit perfectly
TEST_F(RoceBaseQuotaTest, MessagesExactlyFillQuota) {
  queueMessage(2500);
  queueMessage(2500);

  auto [_, sentBytes] = socket_->sendBytes(5000);

  EXPECT_EQ(sentBytes, 5000)
      << "Should send all bytes when messages exactly fill quota";
}

// Test that sendBytes sends all bytes when quota exceeds total message size
TEST_F(RoceBaseQuotaTest, QuotaExceedsTotalMessages) {
  queueMessage(1000);
  queueMessage(2000);

  auto [_, sentBytes] = socket_->sendBytes(10000);

  EXPECT_EQ(sentBytes, 3000)
      << "Should send all available bytes (3000), not the full quota (10000)";
}

// Test with many small messages to stress the quota tracking
TEST_F(RoceBaseQuotaTest, ManySmallMessagesRespectQuota) {
  for (int i = 0; i < 10; ++i) {
    queueMessage(1000);
  }

  auto [_, sentBytes] = socket_->sendBytes(4500);

  EXPECT_EQ(sentBytes, 4500)
      << "Should send exactly 4500 bytes (4 full messages + 500 partial)";
}

// Test edge case: quota of zero should not send any bytes
TEST_F(RoceBaseQuotaTest, ZeroQuota) {
  queueMessage(1000);

  auto [_, sentBytes] = socket_->sendBytes(0);

  EXPECT_EQ(sentBytes, 0) << "Zero quota should send zero bytes";
}

// Test edge case: quota of 1 byte
TEST_F(RoceBaseQuotaTest, MinimalQuota) {
  queueMessage(1000);

  auto [_, sentBytes] = socket_->sendBytes(1);

  EXPECT_EQ(sentBytes, 1) << "Quota of 1 should send exactly 1 byte";
}

// Test that the first message boundary is handled correctly with remaining
// quota
TEST_F(RoceBaseQuotaTest, FirstMessageLargerThanQuota) {
  queueMessage(8000);
  queueMessage(1000);
  queueMessage(1000);

  auto [_, sentBytes] = socket_->sendBytes(5000);

  EXPECT_EQ(sentBytes, 5000)
      << "Should send exactly quota bytes from first message";
  EXPECT_EQ(socket_->queuedMsgs(), 3)
      << "All messages should still be queued (first partially sent)";
}

// Verify getSockets() exposes the sockets created by connect(). SetUp()
// connects one socket, so the read-only view contains exactly that socket.
TEST_F(RoceBaseQuotaTest, GetSocketsExposesConnectedSocket) {
  const auto& sockets = transport_->getSockets();
  ASSERT_EQ(sockets.size(), 1u);
  EXPECT_EQ(sockets[0].get(), socket_);
}

// RoceTransportBase::verifyComplete() does not check outstanding messages: it
// reports complete while a message is still queued. The straggler bullet in
// README.md states this.
TEST_F(RoceBaseQuotaTest, VerifyCompleteIgnoresQueuedMessages) {
  queueMessage(1000);
  ASSERT_EQ(socket_->queuedMsgs(), 1u);

  EXPECT_TRUE(transport_->verifyComplete())
      << "RoceTransportBase::verifyComplete() now reports a queued message as "
         "outstanding; update the completion-check sentence of the straggler "
         "bullet in README.md, the verifyComplete() comment in "
         "transport/roce-base.h and the verifySimulationMetrics() comment in "
         "simulator/packsim-simulation.cpp";
}

// RoceTransportBase does not override Transport::logWorstIncompleteQP(), so a
// RoCEv2 stall that --packsim_detect_stall stops names no QP: taking the
// member's address through RoceTransportBase still yields the base class's
// pointer type.
TEST(RoceBaseHooksTest, LogWorstIncompleteQpKeepsTheEmptyBaseHook) {
  EXPECT_TRUE((std::is_same_v<
               decltype(&RoceTransportBase::logWorstIncompleteQP),
               decltype(&Transport::logWorstIncompleteQP)>))
      << "RoceTransportBase now overrides logWorstIncompleteQP(); update the "
         "straggler bullet in README.md, the --packsim_detect_stall help and "
         "the stall ERR line in simulator/packsim-simulation.cpp";
}

} // namespace packsim
