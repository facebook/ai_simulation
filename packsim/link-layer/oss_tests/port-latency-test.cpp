/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/endpoint/runtime/workload-node.h"
#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/topology/flex-topology.h"
#include "packsim/util/builders.h"

namespace packsim {
namespace {

// Helper function to run a simulation and return completion time
uint64_t runSimulationWithNicLatencies(
    uint32_t txLatencyNs,
    uint32_t rxLatencyNs) {
  Scheduler scheduler;
  scheduler.buildNewEngine();
  SimContext simCtx{&scheduler, 0};
  constexpr uint32_t msgSize = 1000;
  constexpr uint32_t numAccel = 128;

  auto simCfg = packsim::buildFourZoneClosConfig(numAccel);

  // Set Tx and Rx latencies on the NIC config
  simCfg.nicConfigMap()->at("default").txLatencyNs() = txLatencyNs;
  simCfg.nicConfigMap()->at("default").rxLatencyNs() = rxLatencyNs;

  auto flexTopo = std::make_unique<packsim::FlexTopology>(
      &scheduler, &simCtx.random, simCfg);

  auto src = flexTopo->getWorkloadNode(0);
  auto dst = flexTopo->getWorkloadNode(8);

  auto srcId = src->getFlexId();
  auto dstId = dst->getFlexId();

  auto msg = std::make_unique<packsim::Message>();
  msg->sizeBytes = msgSize;
  msg->connectionMessageId = 0;
  msg->srcRank = 0;
  msg->dstRank = 8;
  msg->srcIdx = srcId;
  msg->dstIdx = dstId;
  msg->localQPN = 0;
  msg->remoteQPN = 0;

  uint64_t startTimeNs = simCtx.nowNs();
  simCtx.scheduleNow(&WorkloadNode::makeSendRequest, src, msg.get());
  simCtx.scheduleNow(&WorkloadNode::makeReceiveRequest, dst, msg.get());

  scheduler.run();
  uint64_t completionTimeNs = simCtx.nowNs();

  return completionTimeNs - startTimeNs;
}

} // namespace

class PortLatencyTest : public ::testing::Test {
 public:
  void SetUp() override {
    scheduler_.buildNewEngine();
    simCtx_ = std::make_unique<SimContext>(&scheduler_, 0);
  }

  FlexDeviceId getMockFlexId() {
    return FlexDeviceId({1, 2, 3, 4});
  }

  void TearDown() override {
    simCtx_.reset();
    nicConfigs_.clear();
  }

 protected:
  Scheduler scheduler_;
  std::unique_ptr<SimContext> simCtx_;
  std::vector<std::unique_ptr<packsim::thrift::NicConfig>> nicConfigs_;
};

class MockEthernetPort : public EthernetPort {
 public:
  explicit MockEthernetPort(SimContext* simCtx)
      : EthernetPort(simCtx, "200Gbps") {}

  void doSend(PacketPtr /* pkt */) override {
    doSendCalled = true;
  }

  void doReceive(PacketPtr /* pkt */) override {
    doReceiveCalled = true;
  }

  bool doSendCalled = false;
  bool doReceiveCalled = false;
};

TEST_F(PortLatencyTest, PacketSendImmediatePathTest) {
  MockEthernetPort mockPort(simCtx_.get());
  auto pkt = createPacket<PacketType::UDP>(1024);

  // no delay configured so it should send immediately
  mockPort.send(pkt);
  EXPECT_EQ(mockPort.doSendCalled, true);
}

TEST_F(PortLatencyTest, PacketSendDelayedTest) {
  MockEthernetPort mockPort(simCtx_.get());
  auto pkt = createPacket<PacketType::UDP>(1024);

  mockPort.setTxLatencyNs(1000);
  mockPort.send(pkt);
  EXPECT_FALSE(mockPort.doSendCalled);

  // Advance the scheduler to (almost) execute the delayed event
  scheduler_.run(Time::fromNs(1000));
  EXPECT_FALSE(mockPort.doSendCalled);

  // Advance the scheduler to execute the delayed event
  scheduler_.run(Time::fromNs(1));
  EXPECT_TRUE(mockPort.doSendCalled);
}

TEST_F(PortLatencyTest, PacketReceiveImmediateTest) {
  MockEthernetPort mockPort(simCtx_.get());
  auto pkt = createPacket<PacketType::UDP>(1024);

  // no delay configured so it should receive immediately
  mockPort.receive(pkt);
  EXPECT_TRUE(mockPort.doReceiveCalled);
}

TEST_F(PortLatencyTest, PacketReceiveDelayedTest) {
  MockEthernetPort mockPort(simCtx_.get());
  auto pkt = createPacket<PacketType::UDP>(1024);

  mockPort.setRxLatencyNs(500);
  mockPort.receive(pkt);
  EXPECT_FALSE(mockPort.doReceiveCalled);

  // Advance the scheduler to (almost) execute the delayed event
  scheduler_.run(Time::fromNs(500));
  EXPECT_FALSE(mockPort.doReceiveCalled);

  // Advance the scheduler to execute the delayed event
  scheduler_.run(Time::fromNs(1));
  EXPECT_TRUE(mockPort.doReceiveCalled);
}

// End-to-end test that verifies Tx and Rx latencies are applied through the
// full simulation This test measures the baseline runtime with zero latencies,
// then measures again with configured latencies, and verifies the delta matches
// the expected latency increase.
TEST(PortLatencyE2ETest, LatencyIsAppliedEndToEnd) {
  constexpr uint32_t txLatencyNs = 5000; // 5 microseconds
  constexpr uint32_t rxLatencyNs = 3000; // 3 microseconds
  uint32_t expectedLatencyIncrease = 2 *
      (txLatencyNs + rxLatencyNs); // 2 for 1 egress and 1 ingress per packet

  // Run baseline with zero latencies to measure all other contributing factors
  // (switch delays, serialization, propagation, transport protocol overhead,
  // etc.)
  uint64_t baselineRuntimeNs = runSimulationWithNicLatencies(0, 0);

  // Run with configured Tx and Rx latencies
  uint64_t runtimeWithLatenciesNs =
      runSimulationWithNicLatencies(txLatencyNs, rxLatencyNs);

  // Calculate the actual increase in runtime
  uint64_t actualLatencyIncrease = runtimeWithLatenciesNs - baselineRuntimeNs;

  // The simulation is deterministic, so the increase matches the expected one
  // exactly, with no tolerance.
  EXPECT_EQ(actualLatencyIncrease, expectedLatencyIncrease)
      << "Baseline runtime: " << baselineRuntimeNs << "ns, "
      << "Runtime with latencies: " << runtimeWithLatenciesNs << "ns, "
      << "Expected increase: " << expectedLatencyIncrease << "ns, "
      << "Actual increase: " << actualLatencyIncrease << "ns";
}

} // namespace packsim
