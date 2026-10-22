/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>
#include <utility>

#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/transport/roce-base.h"
#include "packsim/transport/transport.h"

namespace packsim {

/**
 * Test that workload tagging works correctly:
 * 1. Messages can have workload pointer set
 * 2. Sockets capture workload from first Message
 * 3. Socket::getWorkload() returns correct workload
 */
class WorkloadTaggingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    simCtx_.dataRcvd = 0;

    nicCfg_.writeEff() = 100;
    nicCfg_.readGbps() = 400;
    nicCfg_.readLatencyNs() = 100;
    nicCfg_.perCosBufferSize() = {1024 * 1024 * 1024, 1024 * 1024 * 1024};
    nicCfg_.perCosXoff() = {1000 * 1024 * 1024, 1000 * 1024 * 1024};
    nicCfg_.perCosXon() = {1000 * 1024 * 1024, 1000 * 1024 * 1024};
    nicCfg_.speed() = "400Gbps";
    nicCfg_.dataCos() = 0;
    nicCfg_.ackCos() = 0;
    nicCfg_.mtu() = 4096;
    nicCfg_.outofOrderPktSupport() = false;

    // Setup DCQCN config
    packsim::thrift::DcqcnConfig dcqcnCfg;
    dcqcnCfg.npCnpPrio() = 0;
    nicCfg_.dcqcnCfg() = dcqcnCfg;

    packsim::thrift::RoceV2TransportConfig roceConfig;
    roceConfig.congestionControl()->dummy() =
        packsim::thrift::DummyRoceV2StrategyConfig{};
    roceConfig.lossRecovery()->dummy() =
        packsim::thrift::DummyRoceV2StrategyConfig{};
    packsim::thrift::TransportConfig transportConfig;
    transportConfig.roceV2() = std::move(roceConfig);
    nicCfg_.transport() = std::move(transportConfig);

    srcId_ = FlexDeviceId({0, 0, 0});
    dstId_ = FlexDeviceId({0, 0, 1});

    transport_ =
        std::make_unique<RoceTransportBase>(&simCtx_, srcId_, &nicCfg_);
  }

  // Helper function to create messages with workload pointer
  std::unique_ptr<Message> createMessage(
      uint32_t sizeBytes,
      uint32_t localQPN,
      uint32_t remoteQPN,
      const packsim::workload::thrift::Workload* workload,
      bool isRecv = false) {
    auto msg = std::make_unique<Message>();
    msg->sizeBytes = sizeBytes;
    msg->srcRank = isRecv ? 1 : 0;
    msg->dstRank = isRecv ? 0 : 1;
    msg->srcIdx = isRecv ? dstId_ : srcId_;
    msg->dstIdx = isRecv ? srcId_ : dstId_;
    msg->localQPN = localQPN;
    msg->remoteQPN = remoteQPN;
    msg->workloadId = workload ? *workload->workloadId() : UINT32_MAX;
    msg->workload = workload;
    return msg;
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  packsim::thrift::NicConfig nicCfg_;
  FlexDeviceId srcId_;
  FlexDeviceId dstId_;
  std::unique_ptr<RoceTransportBase> transport_;
};

TEST_F(WorkloadTaggingTest, SocketCapturesWorkloadFromSend) {
  // Test that socket captures workload from send() and makes it accessible
  packsim::workload::thrift::Workload testWorkload;
  testWorkload.workloadId() = 10;
  testWorkload.name() = "send_workload";

  auto msg = createMessage(1024, 100, 200, &testWorkload);
  auto* socket = transport_->connect(dstId_, 1234, 100, 200);

  // Initially, socket should not have workload
  EXPECT_EQ(socket->getWorkload(), nullptr);

  // After send, socket should capture and retain workload
  socket->send(msg.get());
  EXPECT_EQ(socket->getWorkload(), &testWorkload);
  EXPECT_EQ(*socket->getWorkload()->workloadId(), 10);
  EXPECT_EQ(*socket->getWorkload()->name(), "send_workload");
}

TEST_F(WorkloadTaggingTest, SocketCapturesWorkloadFromRecv) {
  // Test that socket also captures workload from recv() path
  packsim::workload::thrift::Workload testWorkload;
  testWorkload.workloadId() = 20;
  testWorkload.name() = "recv_workload";

  auto msg = createMessage(2048, 300, 400, &testWorkload, true);
  auto* socket = transport_->connect(dstId_, 5678, 300, 400);

  EXPECT_EQ(socket->getWorkload(), nullptr);

  // Recv path should also capture workload
  socket->recv(msg.get());
  EXPECT_EQ(socket->getWorkload(), &testWorkload);
  EXPECT_EQ(*socket->getWorkload()->workloadId(), 20);
}

TEST_F(WorkloadTaggingTest, MultipleSocketsHaveIndependentWorkloads) {
  // Test that different sockets maintain different workloads (isolation)
  packsim::workload::thrift::Workload workload1;
  workload1.workloadId() = 10;
  workload1.name() = "workload_1";

  packsim::workload::thrift::Workload workload2;
  workload2.workloadId() = 20;
  workload2.name() = "workload_2";

  auto* socket1 = transport_->connect(dstId_, 1000, 100, 200);
  auto* socket2 = transport_->connect(dstId_, 2000, 300, 400);

  auto msg1 = createMessage(1024, 100, 200, &workload1);
  auto msg2 = createMessage(2048, 300, 400, &workload2);

  socket1->send(msg1.get());
  socket2->send(msg2.get());

  // Each socket should have its own workload
  EXPECT_EQ(socket1->getWorkload(), &workload1);
  EXPECT_EQ(socket2->getWorkload(), &workload2);
  EXPECT_NE(socket1->getWorkload(), socket2->getWorkload());
}

TEST_F(WorkloadTaggingTest, WorkloadTypeAccessibleForTransportLogic) {
  // Test the key use case: checking workload type for transport-specific logic
  packsim::workload::thrift::Workload testWorkload;
  testWorkload.workloadId() = 123;
  testWorkload.name() = "all_to_all_workload";

  // Set workload type to AllToAll
  packsim::workload::thrift::WorkloadSpecificConfig workloadConfig;
  packsim::workload::thrift::AllToAll allToAll;
  workloadConfig.allToAll() = allToAll;
  testWorkload.config() = workloadConfig;

  auto msg = createMessage(1024, 700, 800, &testWorkload);
  auto* socket = transport_->connect(dstId_, 7777, 700, 800);
  socket->send(msg.get());

  // Verify we can check workload type for transport-specific behavior
  auto* workload = socket->getWorkload();
  ASSERT_NE(workload, nullptr);
  EXPECT_EQ(
      workload->config()->getType(),
      packsim::workload::thrift::WorkloadSpecificConfig::Type::allToAll);
  EXPECT_EQ(workload, &testWorkload);
}

TEST_F(WorkloadTaggingTest, NullWorkloadHandledGracefully) {
  // Test edge case: message with null workload pointer
  auto msg = createMessage(512, 900, 1000, nullptr);
  auto* socket = transport_->connect(dstId_, 8888, 900, 1000);

  socket->send(msg.get());

  // Socket should remain nullptr if message has no workload
  EXPECT_EQ(socket->getWorkload(), nullptr);
}

} // namespace packsim
