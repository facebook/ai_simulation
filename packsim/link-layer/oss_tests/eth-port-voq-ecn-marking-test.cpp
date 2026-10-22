/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/eth-port.h"

#include <cstdint>
#include <memory>

#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/switch_config_types.h"
#include "packsim/if/gen-cpp2/topology_types.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/oss_tests/lambda-port-observer.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

namespace {
// Payload of every packet a test sends.
constexpr uint32_t kPayloadBytes = 1000;
} // namespace

/**
 * Two ports on a lossless, zero-delay channel. `port_` transmits from one
 * VOQ_ECN queue whose two ECN thresholds are equal, so a packet is marked
 * exactly when at least that many bytes are still queued once it leaves, and
 * no random number is drawn. `peer_` only receives.
 */
class EthernetPortVoqEcnMarkingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    port_->setObserver(&portObserver_);
    peer_->setObserver(&peerObserver_);
    thrift::FlexLinkDelayConfig delayCfg;
    delayCfg.delayNs() = 0;
    channel_ = std::make_unique<EthernetChannel>(
        delayCfg, /*flapDelayNs=*/0, /*log10Ber=*/1.0);
    port_->attach(channel_.get());
    peer_->attach(channel_.get());
  }

  // Gives `port_` one VOQ_ECN queue with both ECN thresholds at
  // `markThresholdBytes`.
  void configureVoqEcnQueue(uint32_t markThresholdBytes) {
    thrift::PortQueueConfig queueCfg;
    auto& wredEcnCfg = *queueCfg.wredEcnCfg();
    wredEcnCfg.queueProfileCfg() = thrift::QueueProfileType::VOQ_ECN;
    wredEcnCfg.minQSizeECN() = markThresholdBytes;
    wredEcnCfg.maxQSizeECN() = markThresholdBytes;
    port_->setPortQueueConfig({queueCfg});
  }

  void sendPackets(uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
      port_->send(createPacket<PacketType::IPV6>(kPayloadBytes));
    }
  }

  uint64_t getNumMarkedPkts() const {
    return port_->getPortStats(/*cos=*/0).ecnCeMarkedPkts;
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, /*seed=*/0};
  std::unique_ptr<EthernetChannel> channel_;
  // Declared before the ports so they are destroyed after them: a port holds
  // a non-owning pointer to its observer.
  LambdaPortObserver portObserver_;
  LambdaPortObserver peerObserver_;
  std::unique_ptr<EthernetPort> port_{
      std::make_unique<EthernetPort>(&simCtx_, "100Gbps")};
  std::unique_ptr<EthernetPort> peer_{
      std::make_unique<EthernetPort>(&simCtx_, "100Gbps")};
};

// With both thresholds at 0, a packet is marked however short the queue is.
TEST_F(EthernetPortVoqEcnMarkingTest, EnabledPortMarksAPacketItSends) {
  configureVoqEcnQueue(/*markThresholdBytes=*/0);
  port_->enableVoqEcnMarking();

  sendPackets(/*count=*/1);
  scheduler_.run();

  EXPECT_EQ(getNumMarkedPkts(), 1u);
}

// Only a switch enables the marking, so an endpoint's VOQ_ECN queue marks
// nothing.
TEST_F(EthernetPortVoqEcnMarkingTest, PortLeftDisabledMarksNothing) {
  configureVoqEcnQueue(/*markThresholdBytes=*/0);

  sendPackets(/*count=*/1);
  scheduler_.run();

  EXPECT_EQ(getNumMarkedPkts(), 0u);
}

// A switch enables the marking before it configures the port's queues, and
// every queue configuration replaces the port's `Queues`.
TEST_F(EthernetPortVoqEcnMarkingTest, MarkingEnabledBeforeQueueConfigStaysOn) {
  port_->enableVoqEcnMarking();
  configureVoqEcnQueue(/*markThresholdBytes=*/0);

  sendPackets(/*count=*/1);
  scheduler_.run();

  EXPECT_EQ(getNumMarkedPkts(), 1u);
}

// The packets a link-down drain drops leave the queue length the marking
// reads, so after the link comes back a packet sent alone leaves an empty
// queue behind and is not marked.
TEST_F(EthernetPortVoqEcnMarkingTest, LinkDownDrainLeavesNoBytesToMarkBy) {
  configureVoqEcnQueue(/*markThresholdBytes=*/1);
  port_->enableVoqEcnMarking();
  // Without running the scheduler, so all but the packet on the wire are
  // still queued when the link goes down.
  sendPackets(/*count=*/3);
  ASSERT_GT(port_->getQueueLength(/*cos=*/0), 0u);
  port_->setChannelState(ChannelState::DOWN);
  port_->setChannelState(ChannelState::UP);

  sendPackets(/*count=*/1);
  scheduler_.run();

  EXPECT_EQ(getNumMarkedPkts(), 0u);
}

} // namespace packsim
