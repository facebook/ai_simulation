/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <tuple>
#include <vector>

#include <folly/Utility.h>
#include <folly/logging/xlog.h>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/packet/pfc.h"
#include "packsim/queues/queue-scheduler.h"
#include "packsim/queues/queues.h"

namespace packsim {

namespace {

packsim::thrift::QueueFilterConfig getFilterCfg() {
  packsim::thrift::QueueFilterConfig filterCfg;
  filterCfg.field() = packsim::thrift::QueueFilterField::DSCP;
  filterCfg.fieldToQueue()[folly::to_underlying(
      Ipv6Packet::DscpType::DSCP_DEFAULT)] = 0;
  filterCfg
      .fieldToQueue()[folly::to_underlying(Ipv6Packet::DscpType::DSCP_CS1)] = 1;
  filterCfg
      .fieldToQueue()[folly::to_underlying(Ipv6Packet::DscpType::DSCP_CS2)] = 2;
  filterCfg
      .fieldToQueue()[folly::to_underlying(Ipv6Packet::DscpType::DSCP_CS3)] = 3;
  filterCfg
      .fieldToQueue()[folly::to_underlying(Ipv6Packet::DscpType::DSCP_CS4)] = 4;
  filterCfg
      .fieldToQueue()[folly::to_underlying(Ipv6Packet::DscpType::DSCP_CS5)] = 5;
  filterCfg
      .fieldToQueue()[folly::to_underlying(Ipv6Packet::DscpType::DSCP_CS6)] = 6;
  filterCfg
      .fieldToQueue()[folly::to_underlying(Ipv6Packet::DscpType::DSCP_CS7)] = 7;
  return filterCfg;
}

} // namespace

class SwitchQueuesTest : public ::testing::TestWithParam<double> {
 public:
  void SetUp() override {
    std::vector<packsim::thrift::PortQueueConfig> handlerType;
    handlerType.resize(8);
    for (auto& handler : handlerType) {
      handler.wredEcnCfg()->queueProfileCfg() =
          packsim::thrift::QueueProfileType::NONE;
    }

    queues_ = std::make_unique<Queues>(
        &simCtx_,
        getFilterCfg(),
        packsim::thrift::SchedulingAlgorithmType::RR,
        handlerType);
  }

  // a function that returns all queues' lengths in a vector for comparison
  std::vector<size_t> getAllQueueLength() {
    std::vector<size_t> vec;
    vec.reserve(queues_->getNumQueues());
    for (uint8_t i = 0; i < queues_->getNumQueues(); i++) {
      vec.emplace_back(queues_->getQueueLengthInPkts(i));
    }
    return vec;
  }

 protected:
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  std::unique_ptr<Queues> queues_;
};

TEST_P(SwitchQueuesTest, TestQueuesCreated) {
  EXPECT_EQ(queues_->getNumQueues(), 8);
}

TEST_P(SwitchQueuesTest, TestFilter) {
  // create a supported dscp value and an unsuppported value (should be put in
  // cos == 0)
  auto pkt = createPacket<PacketType::UDP>(0);
  pkt.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS2;
  uint8_t outQueue;
  bool enqueue;
  std::tie(outQueue, enqueue) = queues_->pushPacket(pkt);
  EXPECT_EQ(outQueue, 2);

  auto pkt2 = createPacket<PacketType::UDP>(0);
  pkt.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS2;
  std::tie(outQueue, enqueue) = queues_->pushPacket(pkt2);
  EXPECT_EQ(outQueue, 0);

  // queue lengths should be [1, 0, 1, 0, 0, ...]
  std::vector<size_t> actual_lengths = getAllQueueLength();
  std::vector<size_t> exp_lengths = {1, 0, 1, 0, 0, 0, 0, 0};
  ASSERT_EQ(actual_lengths.size(), exp_lengths.size());
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }
}

TEST_P(SwitchQueuesTest, TestRRScheduler) {
  // Test RR scheduling with 8 queues:

  // (1) insert cos == 1, cos == 2, cos == 3, queue lengths should be
  // [0, 1, 1, 1, 0, ...]
  auto pkt = createPacket<PacketType::UDP>(0);
  pkt.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS1;
  uint8_t outQueue;
  bool enqueue;
  std::tie(outQueue, enqueue) = queues_->pushPacket(pkt);
  EXPECT_EQ(outQueue, 1);

  auto pkt2 = createPacket<PacketType::UDP>(0);
  pkt2.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS2;
  uint8_t outQueue2;
  std::tie(outQueue2, enqueue) = queues_->pushPacket(pkt2);
  EXPECT_EQ(outQueue2, 2);

  auto pkt3 = createPacket<PacketType::UDP>(0);
  pkt3.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS3;
  uint8_t outQueue3;
  std::tie(outQueue3, enqueue) = queues_->pushPacket(pkt3);
  EXPECT_EQ(outQueue3, 3);

  std::vector<size_t> exp_lengths = {0, 1, 1, 1, 0, 0, 0, 0};
  std::vector<size_t> actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  // (2) pop once, queue lengths should be [0, 0, 1, 1, 0, ...]
  PacketPtr poppedPkt;
  queues_->popPacket(poppedPkt);
  exp_lengths = {0, 0, 1, 1, 0, 0, 0, 0};
  actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  // (3) pop once, queue lengths should be [0, 0, 0, 1, 0, ...]
  queues_->popPacket(poppedPkt);
  exp_lengths = {0, 0, 0, 1, 0, 0, 0, 0};
  actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  // (4) insert cos == 1, then pop, queue lengths should be [0, 1, 0, 0, 0, ...]
  auto pkt4 = createPacket<PacketType::UDP>(0);
  pkt4.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS1;
  queues_->pushPacket(pkt4);
  queues_->popPacket(poppedPkt);
  exp_lengths = {0, 1, 0, 0, 0, 0, 0, 0};
  actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  // (5) insert cos == 7, then pop, queue lengths should be [0, 1, 0, 0, 0, ...]
  auto pkt5 = createPacket<PacketType::UDP>(0);
  pkt5.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS7;
  queues_->pushPacket(pkt5);
  queues_->popPacket(poppedPkt);
  exp_lengths = {0, 1, 0, 0, 0, 0, 0, 0};
  actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  // (6) insert cos == 6, then pop, queue lengths should be
  // [0, 0, 0, 0, 0, 0, 1, 0]
  auto pkt6 = createPacket<PacketType::UDP>(0);
  pkt6.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS6;
  queues_->pushPacket(pkt6);
  queues_->popPacket(poppedPkt);
  exp_lengths = {0, 0, 0, 0, 0, 0, 1, 0};
  actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }
}

TEST_P(SwitchQueuesTest, TestWRRScheduler) {
  std::vector<packsim::thrift::PortQueueConfig> handlerType;
  handlerType.resize(8);
  for (auto& handler : handlerType) {
    handler.wredEcnCfg()->queueProfileCfg() =
        packsim::thrift::QueueProfileType::NONE;
  }

  queues_ = std::make_unique<Queues>(
      &simCtx_,
      getFilterCfg(),
      packsim::thrift::SchedulingAlgorithmType::WRR,
      handlerType);

  // followed the test example here:
  // http://kb.linuxvirtualserver.org/wiki/Weighted_Round-Robin_Scheduling
  std::vector<uint32_t> vec = {1, 4, 3, 2, 1, 1, 1, 1}; // set the weights
  dynamic_cast<WrrQueueScheduler&>(*queues_->scheduler_).setWeights(vec);

  // first push 4 pkts with cos == 1, 3 pkts with cos == 2, 2 pkts with cos == 3
  std::vector<PacketPtr> packets_in;
  for (uint32_t i = 0; i < 4; i++) {
    auto pkt = createPacket<PacketType::UDP>(0);
    auto udpPkt = pkt.as<PacketType::UDP>();
    udpPkt->dscp = Ipv6Packet::DscpType::DSCP_CS1;
    packets_in.emplace_back(pkt);
  }

  for (uint32_t i = 0; i < 3; i++) {
    auto pkt = createPacket<PacketType::UDP>(0);
    auto udpPkt = pkt.as<PacketType::UDP>();
    udpPkt->dscp = Ipv6Packet::DscpType::DSCP_CS2;
    packets_in.emplace_back(pkt);
  }

  for (uint32_t i = 0; i < 2; i++) {
    auto pkt = createPacket<PacketType::UDP>(0);
    auto udpPkt = pkt.as<PacketType::UDP>();
    udpPkt->dscp = Ipv6Packet::DscpType::DSCP_CS3;
    packets_in.emplace_back(pkt);
  }
  EXPECT_EQ(packets_in.size(), 9);

  for (uint32_t i = 0; i < packets_in.size(); i++) {
    queues_->pushPacket(packets_in.at(i));
  }

  // first confirm all packets are enqueued currectly
  std::vector<size_t> exp_lengths = {0, 4, 3, 2, 0, 0, 0, 0};
  std::vector<size_t> actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  // take them out to another vector, and check the DSCP values
  std::vector<PacketPtr> packets_out;
  for (uint32_t i = 0; i < 9; i++) {
    PacketPtr p;
    queues_->popPacket(p);
    ASSERT_TRUE(p != nullptr) << "queue underran at pop " << i;
    XLOGF(
        DBG,
        "this packet has dscp value = {}",
        magic_enum::enum_name(p.as<Ipv6Packet>()->dscp));
    packets_out.emplace_back(p);
  }

  // create a vector of expected values
  std::vector<Ipv6Packet::DscpType> expected_dscp_values;
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS1);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS1);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS2);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS1);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS2);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS3);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS1);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS2);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS3);

  for (uint32_t i = 0; i < expected_dscp_values.size(); i++) {
    const auto& pkt = packets_out.at(i);
    EXPECT_EQ(pkt.as<Ipv6Packet>()->dscp, expected_dscp_values.at(i));
  }
}

TEST_P(SwitchQueuesTest, TestWRRSchedulerWithPause) {
  std::vector<packsim::thrift::PortQueueConfig> handlerType;
  handlerType.resize(8);
  for (auto& handler : handlerType) {
    handler.wredEcnCfg()->queueProfileCfg() =
        packsim::thrift::QueueProfileType::NONE;
  }

  queues_ = std::make_unique<Queues>(
      &simCtx_,
      getFilterCfg(),
      packsim::thrift::SchedulingAlgorithmType::WRR,
      handlerType);

  // Same as the above test, but set queue 2 to be paused (XOFF)
  // after popped all other packets, resume queue 2 to be XON
  // then pop again
  std::vector<uint32_t> vec = {1, 4, 3, 2, 1, 1, 1, 1}; // set the weights
  dynamic_cast<WrrQueueScheduler&>(*queues_->scheduler_).setWeights(vec);

  // first push 4 pkts with cos == 1, 3 pkts with cos == 2, 2 pkts with cos == 3
  std::vector<PacketPtr> packets_in;
  for (uint32_t i = 0; i < 4; i++) {
    auto pkt = createPacket<PacketType::UDP>(0);
    pkt.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS1;
    packets_in.emplace_back(pkt);
  }

  for (uint32_t i = 0; i < 3; i++) {
    auto pkt = createPacket<PacketType::UDP>(0);
    pkt.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS2;
    packets_in.emplace_back(pkt);
  }

  for (uint32_t i = 0; i < 2; i++) {
    auto pkt = createPacket<PacketType::UDP>(0);
    pkt.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS3;
    packets_in.emplace_back(pkt);
  }
  EXPECT_EQ(packets_in.size(), 9);

  for (uint32_t i = 0; i < packets_in.size(); i++) {
    queues_->pushPacket(packets_in.at(i));
  }

  // first confirm all packets are enqueued currectly
  std::vector<size_t> exp_lengths = {0, 4, 3, 2, 0, 0, 0, 0};
  std::vector<size_t> actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  // pause queue 2 (COS == 2) before popping
  queues_->updateTxPfcState(2, PfcState::XOFF);

  // take 6 packets to another vector, resume tx for queue 2, then take 3
  // check the packets' DSCP values in the vector
  std::vector<PacketPtr> packets_out;
  for (uint32_t i = 0; i < 6; i++) {
    PacketPtr p;
    queues_->popPacket(p);
    if (p == nullptr) {
      XLOGF(DBG, "got null ptr");
    }
    packets_out.emplace_back(p);
  }
  queues_->updateTxPfcState(2, PfcState::XON);
  for (uint32_t i = 0; i < 3; i++) {
    PacketPtr p;
    queues_->popPacket(p);
    if (p == nullptr) {
      XLOGF(DBG, "got null ptr");
    }
    packets_out.emplace_back(p);
  }

  // create a vector of expected values
  std::vector<Ipv6Packet::DscpType> expected_dscp_values;
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS1);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS1);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS1);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS3);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS1);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS3);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS2);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS2);
  expected_dscp_values.emplace_back(Ipv6Packet::DscpType::DSCP_CS2);

  for (uint32_t i = 0; i < expected_dscp_values.size(); i++) {
    const auto& pkt = packets_out.at(i);
    if (pkt == nullptr) {
      XLOGF(DBG, "packet {} is null", i);
    }
    EXPECT_EQ(pkt.as<Ipv6Packet>()->dscp, expected_dscp_values.at(i));
  }
}

// The tests above drain their bursts before a round ends. Here queues 0 and 1
// stay backlogged for the whole run, so every round completes: with the
// default weights 1 and 9 for those queues, 100 pops take 10 from queue 0 and
// 90 from queue 1.
TEST_P(SwitchQueuesTest, TestWRRSchedulerBackloggedQueuesFollowTheirWeights) {
  constexpr uint32_t kPops = 100;
  std::vector<packsim::thrift::PortQueueConfig> handlerType;
  handlerType.resize(8);
  for (auto& handler : handlerType) {
    handler.wredEcnCfg()->queueProfileCfg() =
        packsim::thrift::QueueProfileType::NONE;
  }
  queues_ = std::make_unique<Queues>(
      &simCtx_,
      getFilterCfg(),
      packsim::thrift::SchedulingAlgorithmType::WRR,
      handlerType);
  for (const auto dscp :
       {Ipv6Packet::DscpType::DSCP_DEFAULT, Ipv6Packet::DscpType::DSCP_CS1}) {
    for (uint32_t i = 0; i < kPops; ++i) {
      auto pkt = createPacket<PacketType::UDP>(0);
      pkt.as<PacketType::UDP>()->dscp = dscp;
      queues_->pushPacket(pkt);
    }
  }

  std::array<uint32_t, 2> pops{};
  for (uint32_t i = 0; i < kPops; ++i) {
    PacketPtr p;
    const auto cos = queues_->popPacket(p);
    ASSERT_TRUE(cos.has_value()) << "queue underran at pop " << i;
    ASSERT_LT(*cos, 2u);
    pops.at(*cos)++;
  }

  const std::array<uint32_t, 2> kExpected = {10, 90};
  EXPECT_EQ(pops, kExpected);
}

TEST_P(SwitchQueuesTest, TestWRRSchedulerSpecialCase) {
  std::vector<packsim::thrift::PortQueueConfig> handlerType;
  handlerType.resize(8);
  for (auto& handler : handlerType) {
    handler.wredEcnCfg()->queueProfileCfg() =
        packsim::thrift::QueueProfileType::NONE;
  }

  queues_ = std::make_unique<Queues>(
      &simCtx_,
      getFilterCfg(),
      packsim::thrift::SchedulingAlgorithmType::WRR,
      handlerType);

  std::vector<uint32_t> vec = {1, 5, 1, 3, 1, 1, 1, 1}; // set the weights
  dynamic_cast<WrrQueueScheduler&>(*queues_->scheduler_).setWeights(vec);

  // case 1: first we push a packet with COS == 6, then pop this packet
  // then we push a packet with COS == 1 (weight 5), then pop
  // the scheduler should be able to jump to weight 5 and find the packet
  auto pkt = createPacket<PacketType::UDP>(0);
  auto udpPkt = pkt.as<PacketType::UDP>();
  udpPkt->dscp = Ipv6Packet::DscpType::DSCP_CS6;
  queues_->pushPacket(pkt);

  PacketPtr p;
  queues_->popPacket(p);
  EXPECT_NE(p, nullptr);

  auto pkt2 = createPacket<PacketType::UDP>(0);
  auto udpPkt2 = pkt2.as<PacketType::UDP>();
  udpPkt2->dscp = Ipv6Packet::DscpType::DSCP_CS1;

  queues_->pushPacket(pkt2);
  PacketPtr p2;
  queues_->popPacket(p2);

  EXPECT_NE(p2, nullptr);

  // case 2: first we push a packet with COS == 7 (last queue), then pop this
  // packet then we push a packet with COS == 3 (weight 3), then pop the
  // scheduler should be able to jump to weight 3 and find the packet
  auto pkt3 = createPacket<PacketType::UDP>(0);
  auto udpPkt3 = pkt3.as<PacketType::UDP>();
  udpPkt3->dscp = Ipv6Packet::DscpType::DSCP_CS7;
  queues_->pushPacket(pkt3);

  PacketPtr p3;
  queues_->popPacket(p3);
  EXPECT_NE(p3, nullptr);

  auto pkt4 = createPacket<PacketType::UDP>(0);
  auto udpPkt4 = pkt4.as<PacketType::UDP>();
  udpPkt4->dscp = Ipv6Packet::DscpType::DSCP_CS3;
  queues_->pushPacket(pkt4);

  PacketPtr p4;
  queues_->popPacket(p4);

  EXPECT_NE(p4, nullptr);
}

TEST_P(SwitchQueuesTest, TestWRRSchedulerSingleWeight) {
  std::vector<packsim::thrift::PortQueueConfig> handlerType;
  handlerType.resize(8);
  for (auto& handler : handlerType) {
    handler.wredEcnCfg()->queueProfileCfg() =
        packsim::thrift::QueueProfileType::NONE;
  }

  queues_ = std::make_unique<Queues>(
      &simCtx_,
      getFilterCfg(),
      packsim::thrift::SchedulingAlgorithmType::WRR,
      handlerType);

  std::vector<uint32_t> vec = {1, 1, 1, 1, 1, 1, 1, 1}; // set the weights
  dynamic_cast<WrrQueueScheduler&>(*queues_->scheduler_).setWeights(vec);

  // push 3 packets and pop 4 times
  for (uint32_t i = 0; i < 3; i++) {
    auto pkt = createPacket<PacketType::UDP>(0);
    auto udpPkt = pkt.as<PacketType::UDP>();
    udpPkt->dscp = Ipv6Packet::DscpType::DSCP_DEFAULT;
    queues_->pushPacket(pkt);
  }

  PacketPtr pkt;
  for (int i = 0; i < 3; i++) {
    queues_->popPacket(pkt);
    EXPECT_NE(pkt, nullptr);
  }
  queues_->popPacket(pkt);
  EXPECT_EQ(pkt, nullptr);

  // replicate the RR tests here, if all weights are the same, should get the
  // same results

  std::vector<size_t> exp_lengths = {0, 0, 0, 0, 0, 0, 0, 0};
  std::vector<size_t> actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  // (1) insert cos == 1, cos == 2, cos == 3, queue lengths should be
  // [0, 1, 1, 1, 0, ...]
  auto pkt5 = createPacket<PacketType::UDP>(0);
  pkt5.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS1;
  queues_->pushPacket(pkt5);

  auto pkt6 = createPacket<PacketType::UDP>(0);
  pkt6.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS2;
  queues_->pushPacket(pkt6);

  auto pkt7 = createPacket<PacketType::UDP>(0);
  pkt7.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS3;
  queues_->pushPacket(pkt7);

  exp_lengths = {0, 1, 1, 1, 0, 0, 0, 0};
  actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  // (2) pop once, queue lengths should be [0, 0, 1, 1, 0, ...]
  PacketPtr p;
  queues_->popPacket(p);
  exp_lengths = {0, 0, 1, 1, 0, 0, 0, 0};
  actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  // (3) pop once, queue lengths should be [0, 0, 0, 1, 0, ...]
  queues_->popPacket(p);
  exp_lengths = {0, 0, 0, 1, 0, 0, 0, 0};
  actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  // (4) insert cos == 1, then pop, queue lengths should be [0, 1, 0, 0, 0, ...]
  auto pkt8 = createPacket<PacketType::UDP>(0);
  pkt8.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS1;
  queues_->pushPacket(pkt8);
  queues_->popPacket(p);
  exp_lengths = {0, 1, 0, 0, 0, 0, 0, 0};
  actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  // (5) insert cos == 7, then pop, queue lengths should be [0, 1, 0, 0, 0, ...]
  auto pkt9 = createPacket<PacketType::UDP>(0);
  pkt9.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS7;
  queues_->pushPacket(pkt9);
  queues_->popPacket(p);
  exp_lengths = {0, 1, 0, 0, 0, 0, 0, 0};
  actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  // (6) insert cos == 6, then pop, queue lengths should be
  // [0, 0, 0, 0, 0, 0, 1, 0]
  auto pkt10 = createPacket<PacketType::UDP>(0);
  pkt10.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS6;
  queues_->pushPacket(pkt10);
  queues_->popPacket(p);
  exp_lengths = {0, 0, 0, 0, 0, 0, 1, 0};
  actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }
}

TEST_P(SwitchQueuesTest, TestSPScheduler) {
  std::vector<packsim::thrift::PortQueueConfig> handlerType;
  handlerType.resize(8);
  for (auto& handler : handlerType) {
    handler.wredEcnCfg()->queueProfileCfg() =
        packsim::thrift::QueueProfileType::NONE;
  }

  queues_ = std::make_unique<Queues>(
      &simCtx_,
      getFilterCfg(),
      packsim::thrift::SchedulingAlgorithmType::SP,
      handlerType);

  for (uint32_t i = 0; i < 1; i++) {
    auto pkt = createPacket<PacketType::UDP>(0);
    pkt.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS7;
    queues_->pushPacket(pkt);
  }

  for (uint32_t i = 0; i < 1; i++) {
    auto pkt = createPacket<PacketType::UDP>(0);
    pkt.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS4;
    queues_->pushPacket(pkt);
  }

  for (uint32_t i = 0; i < 2; i++) {
    auto pkt = createPacket<PacketType::UDP>(0);
    pkt.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_CS1;
    queues_->pushPacket(pkt);
  }

  for (uint32_t i = 0; i < 1; i++) {
    auto pkt = createPacket<PacketType::UDP>(0);
    pkt.as<PacketType::UDP>()->dscp = Ipv6Packet::DscpType::DSCP_DEFAULT;
    queues_->pushPacket(pkt);
  }
  std::vector<size_t> exp_lengths = {1, 2, 0, 0, 1, 0, 0, 1};

  std::vector<size_t> actual_lengths = getAllQueueLength();
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  PacketPtr pkt;
  auto outQueue = queues_->popPacket(pkt);
  EXPECT_EQ(outQueue, 7);
  actual_lengths = getAllQueueLength();
  exp_lengths = {1, 2, 0, 0, 1, 0, 0, 0};
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  outQueue = queues_->popPacket(pkt);
  EXPECT_EQ(outQueue, 4);
  actual_lengths = getAllQueueLength();
  exp_lengths = {1, 2, 0, 0, 0, 0, 0, 0};
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  outQueue = queues_->popPacket(pkt);
  EXPECT_EQ(outQueue, 1);
  actual_lengths = getAllQueueLength();
  exp_lengths = {1, 1, 0, 0, 0, 0, 0, 0};
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  outQueue = queues_->popPacket(pkt);
  EXPECT_EQ(outQueue, 1);
  actual_lengths = getAllQueueLength();
  exp_lengths = {1, 0, 0, 0, 0, 0, 0, 0};
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }

  outQueue = queues_->popPacket(pkt);
  EXPECT_EQ(outQueue, 0);
  actual_lengths = getAllQueueLength();
  exp_lengths = {0, 0, 0, 0, 0, 0, 0, 0};
  for (uint32_t i = 0; i < actual_lengths.size(); i++) {
    EXPECT_EQ(exp_lengths.at(i), actual_lengths.at(i));
  }
}

INSTANTIATE_TEST_SUITE_P(
    SwitchQueuesTestSweep,
    SwitchQueuesTest,
    ::testing::Values(80));

} // namespace packsim
