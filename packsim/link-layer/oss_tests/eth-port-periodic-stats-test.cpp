/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/eth-port.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/switch_config_types.h"
#include "packsim/if/gen-cpp2/topology_types.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/oss_tests/lambda-port-observer.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

// Two ports joined by a lossless, zero-delay channel, each with eight queues
// and DSCP CS1 mapped to queue 1, so a CS1 frame is counted under a class of
// service other than 0 by the periodic byte trace that
// `EthernetPort::enablePeriodicStats` starts.
class EthernetPortPeriodicStatsTest : public ::testing::Test {
 protected:
  static constexpr uint8_t kNumQueues = 8;
  // DSCP CS1, as `Ipv6Packet::DscpType::DSCP_CS1`.
  static constexpr uint8_t kCs1Dscp = 8;
  static constexpr uint8_t kCs1Queue = 1;
  static constexpr uint64_t kTraceIntervalNs = 1000;
  static constexpr uint16_t kSenderPortNum = 1;
  static constexpr uint16_t kReceiverPortNum = 2;
  static constexpr uint32_t kFrameSizeBytes = 1000;

  void SetUp() override {
    scheduler_.buildNewEngine();
    configurePort(*sender_, kSenderPortNum);
    configurePort(*receiver_, kReceiverPortNum);
    sender_->setObserver(&senderObserver_);
    receiver_->setObserver(&receiverObserver_);
    thrift::FlexLinkDelayConfig delayCfg;
    delayCfg.delayNs() = 0;
    channel_ = std::make_unique<EthernetChannel>(
        delayCfg, /*flapDelayNs=*/0, /*log10Ber=*/1.0);
    sender_->attach(channel_.get());
    receiver_->attach(channel_.get());
  }

  static void configurePort(EthernetPort& port, uint16_t portNum) {
    port.setPortNum(portNum);
    port.setPortQueueConfig(std::vector<thrift::PortQueueConfig>(kNumQueues));
    thrift::QueueFilterConfig filterCfg;
    filterCfg.fieldToQueue() = {{kCs1Dscp, kCs1Queue}};
    port.setQueueFilterConfig(filterCfg);
  }

  static PacketPtr makeCs1Frame() {
    auto frame = createPacket<PacketType::IPV6>(kFrameSizeBytes);
    frame.as<Ipv6Packet>()->dscp = Ipv6Packet::DscpType::DSCP_CS1;
    return frame;
  }

  // Sum of every sample the named trace recorded; 0 if it recorded none.
  double traceTotal(const std::string& name) const {
    const auto& traces = simCtx_.tracer.traces();
    const auto it = traces.find(name);
    if (it == traces.end()) {
      return 0;
    }
    double total = 0;
    for (const auto& sample : it->second) {
      total += sample.value;
    }
    return total;
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  std::unique_ptr<EthernetChannel> channel_;
  // Declared before the ports so they are destroyed after them: a port holds
  // a non-owning pointer to its observer.
  LambdaPortObserver senderObserver_;
  LambdaPortObserver receiverObserver_;
  std::unique_ptr<EthernetPort> sender_{
      std::make_unique<EthernetPort>(&simCtx_, "100Gbps")};
  std::unique_ptr<EthernetPort> receiver_{
      std::make_unique<EthernetPort>(&simCtx_, "100Gbps")};
};

TEST_F(EthernetPortPeriodicStatsTest, SentBytesOfEveryClassOfServiceAreTraced) {
  auto frame = makeCs1Frame();
  ASSERT_EQ(sender_->getPktCos(frame), kCs1Queue);
  const auto frameBytes = static_cast<double>(frame->getSize());
  sender_->enablePeriodicStats("sender", kTraceIntervalNs);

  sender_->send(frame);
  scheduler_.run(Time::fromNs(2 * kTraceIntervalNs));

  EXPECT_DOUBLE_EQ(
      traceTotal(fmt::format("TXBytes_sender_Port_{}", kSenderPortNum)),
      frameBytes);
}

TEST_F(
    EthernetPortPeriodicStatsTest,
    ReceivedBytesOfEveryClassOfServiceAreTraced) {
  auto frame = makeCs1Frame();
  ASSERT_EQ(receiver_->getPktCos(frame), kCs1Queue);
  const auto frameBytes = static_cast<double>(frame->getSize());
  receiver_->enablePeriodicStats("receiver", kTraceIntervalNs);

  sender_->send(frame);
  scheduler_.run(Time::fromNs(2 * kTraceIntervalNs));

  EXPECT_DOUBLE_EQ(
      traceTotal(fmt::format("RXBytes_receiver_Port_{}", kReceiverPortNum)),
      frameBytes);
}

} // namespace packsim
