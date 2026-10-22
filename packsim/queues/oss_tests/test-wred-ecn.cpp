/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <stdint.h>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <folly/Utility.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/switch_config_types.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/queues/queues.h"
// @oss-disable

using packsim::Queues;
using packsim::thrift::QueueProfileType;
using packsim::thrift::SchedulingAlgorithmType;

namespace packsim {

namespace {

packsim::thrift::QueueFilterConfig getFilterCfg() {
  packsim::thrift::QueueFilterConfig filterCfg;
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

TEST(WredEcnTest, TestWREDECN) {
  Scheduler scheduler;
  SimContext simCtx(&scheduler, 0);
  // Test:
  //  - Add three packets to queue, pushing over ECN threshold. First is not ECT
  //  marked, second and third are
  //  - Run simulation for 1 ms to ensure avg length updates
  //  - Try to push a fourth packet, not ECT, ensure it's dropped
  //  - Pop first and second packets
  //  - Ensure ECN_CE marked only on second
  //  - Run simulation for 1 ms to ensure avg length updates
  //  - Pop final packet
  //  - Ensure ECN_CE not marked, but still ECT marked
  //  - Check that queue is now empty
  std::vector<packsim::thrift::PortQueueConfig> portQueueCfgs{1};
  auto& cfg = *portQueueCfgs.at(0).wredEcnCfg();
  cfg.queueProfileCfg() = QueueProfileType::WRED_ECN;
  cfg.minQSizeWRED() = 8192;
  cfg.maxQSizeWRED() = 8192;
  cfg.dropRateWRED() = 100;
  cfg.minQSizeECN() = 8192;
  cfg.maxQSizeECN() = 8192;
  cfg.markRateECN() = 100;

  auto q = Queues(
      &simCtx, getFilterCfg(), SchedulingAlgorithmType::RR, portQueueCfgs);
  auto pkt0 = createPacket<PacketType::UDP>(4200);
  auto pkt1 = createPacket<PacketType::UDP>(4200);
  auto pkt2 = createPacket<PacketType::UDP>(4200);
  auto pkt3 = createPacket<PacketType::UDP>(4200);

  pkt0.as<UdpPacket>()->ecn = Ipv6Packet::EcnType::ECN_NOT_ECT;
  pkt3.as<UdpPacket>()->ecn = Ipv6Packet::EcnType::ECN_NOT_ECT;

  pkt1.as<UdpPacket>()->ecn = Ipv6Packet::EcnType::ECN_ECT1;
  pkt2.as<UdpPacket>()->ecn = Ipv6Packet::EcnType::ECN_ECT1;

  auto [cos0, enq0] = q.pushPacket(pkt0);
  auto [cos1, enq1] = q.pushPacket(pkt1);
  auto [cos2, enq2] = q.pushPacket(pkt2);

  EXPECT_TRUE(enq0);
  EXPECT_TRUE(enq1);
  EXPECT_TRUE(enq2);

  scheduler.run(Time::fromMs(1));

  auto [cos3, enq3] = q.pushPacket(pkt3);
  EXPECT_FALSE(enq3);

  PacketPtr pktOut;

  q.popPacket(pktOut);
  EXPECT_EQ(pktOut.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_NOT_ECT);

  q.popPacket(pktOut);
  EXPECT_EQ(pktOut.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_CE);

  scheduler.run(Time::fromMs(1));

  q.popPacket(pktOut);
  EXPECT_EQ(pktOut.as<UdpPacket>()->ecn, Ipv6Packet::EcnType::ECN_ECT1);

  EXPECT_EQ(q.getTotalQueueLengthInBytes(), 0);
}

// A rare condition when the queue length is max threshold is min threshold
// causes a crash.  This test confirms that the case is handled.
TEST(WredEcnTest, TestAtThreshold) {
  Scheduler scheduler;
  SimContext simCtx(&scheduler, 0);

  //  - Add packet to queue, to be at queue threshold.
  //  - See if it crashes.
  // make this be solvable by a single packet.
  constexpr uint32_t ecnQueueThreshold = 4200;
  std::vector<packsim::thrift::PortQueueConfig> portQueueCfgs{1};
  auto& cfg = *portQueueCfgs.at(0).wredEcnCfg();
  cfg.queueProfileCfg() = QueueProfileType::WRED_ECN;
  cfg.minQSizeWRED() = 8192;
  cfg.maxQSizeWRED() = 8192;
  cfg.dropRateWRED() = 100;
  cfg.minQSizeECN() = ecnQueueThreshold;
  cfg.maxQSizeECN() = ecnQueueThreshold;
  cfg.markRateECN() = 100;

  auto q = Queues(
      &simCtx, getFilterCfg(), SchedulingAlgorithmType::RR, portQueueCfgs);
  // packet size must total ecnQueueThreshold
  auto pkt0 = createPacket<PacketType::UDP>(ecnQueueThreshold - 40);
  pkt0.as<UdpPacket>()->ecn = Ipv6Packet::EcnType::ECN_ECT1;

  auto [cos0, enq0] = q.pushPacket(pkt0);

  EXPECT_TRUE(enq0);

  scheduler.run(Time::fromMs(1));

  PacketPtr pktOut;

  q.popPacket(pktOut);
  EXPECT_EQ(q.getTotalQueueLengthInBytes(), 0);
}

TEST(WredEcnTest, TestWREDECNWithColors) {
  Scheduler scheduler;
  SimContext simCtx(&scheduler, 0);

  // Map first three DSCP values to queue 0, but color them differently
  auto filterCfg = getFilterCfg();
  filterCfg.fieldToQueue()[folly::to_underlying(
      Ipv6Packet::DscpType::DSCP_DEFAULT)] = 0;
  filterCfg
      .fieldToQueue()[folly::to_underlying(Ipv6Packet::DscpType::DSCP_CS1)] = 0;
  filterCfg
      .fieldToQueue()[folly::to_underlying(Ipv6Packet::DscpType::DSCP_CS2)] = 0;

  filterCfg.fieldToColor()[folly::to_underlying(
      Ipv6Packet::DscpType::DSCP_DEFAULT)] =
      packsim::thrift::PacketColor::GREEN;
  filterCfg
      .fieldToColor()[folly::to_underlying(Ipv6Packet::DscpType::DSCP_CS1)] =
      packsim::thrift::PacketColor::YELLOW;
  filterCfg
      .fieldToColor()[folly::to_underlying(Ipv6Packet::DscpType::DSCP_CS2)] =
      packsim::thrift::PacketColor::RED;

  packsim::thrift::WredEcnConfig greenCfg, yellowCfg, redCfg;
  greenCfg.queueProfileCfg() = QueueProfileType::WRED_ECN;
  greenCfg.minQSizeWRED() = 8192;
  greenCfg.maxQSizeWRED() = 8192;
  greenCfg.dropRateWRED() = 100;
  greenCfg.minQSizeECN() = 8192;
  greenCfg.maxQSizeECN() = 8192;
  greenCfg.markRateECN() = 100;

  yellowCfg.queueProfileCfg() = QueueProfileType::WRED_ECN;
  yellowCfg.minQSizeWRED() = 4096;
  yellowCfg.maxQSizeWRED() = 4096;
  yellowCfg.dropRateWRED() = 100;
  yellowCfg.minQSizeECN() = 4096;
  yellowCfg.maxQSizeECN() = 4096;
  yellowCfg.markRateECN() = 100;

  redCfg.queueProfileCfg() = QueueProfileType::WRED_ECN;
  redCfg.minQSizeWRED() = 1024;
  redCfg.maxQSizeWRED() = 1024;
  redCfg.dropRateWRED() = 100;
  redCfg.minQSizeECN() = 1024;
  redCfg.maxQSizeECN() = 1024;
  redCfg.markRateECN() = 100;

  std::vector<packsim::thrift::PortQueueConfig> portQueueCfgs{1};
  portQueueCfgs.at(0)
      .coloredWredEcnCfgs()[packsim::thrift::PacketColor::GREEN] = greenCfg;
  portQueueCfgs.at(0)
      .coloredWredEcnCfgs()[packsim::thrift::PacketColor::YELLOW] = yellowCfg;
  portQueueCfgs.at(0).coloredWredEcnCfgs()[packsim::thrift::PacketColor::RED] =
      redCfg;

  auto q =
      Queues(&simCtx, filterCfg, SchedulingAlgorithmType::RR, portQueueCfgs);

  auto greenPkt1 = createPacket<PacketType::UDP>(4096);
  auto greenPkt2 = createPacket<PacketType::UDP>(4096);
  auto yellowPkt1 = createPacket<PacketType::UDP>(4096);
  auto yellowPkt2 = createPacket<PacketType::UDP>(4096);
  auto redPkt1 = createPacket<PacketType::UDP>(2048);
  auto redPkt2 = createPacket<PacketType::UDP>(2048);

  greenPkt1.as<UdpPacket>()->dscp = Ipv6Packet::DscpType::DSCP_DEFAULT;
  greenPkt2.as<UdpPacket>()->dscp = Ipv6Packet::DscpType::DSCP_DEFAULT;
  yellowPkt1.as<UdpPacket>()->dscp = Ipv6Packet::DscpType::DSCP_CS1;
  yellowPkt2.as<UdpPacket>()->dscp = Ipv6Packet::DscpType::DSCP_CS1;
  redPkt1.as<UdpPacket>()->dscp = Ipv6Packet::DscpType::DSCP_CS2;
  redPkt2.as<UdpPacket>()->dscp = Ipv6Packet::DscpType::DSCP_CS2;

  greenPkt1.as<UdpPacket>()->ecn = Ipv6Packet::EcnType::ECN_NOT_ECT;
  greenPkt2.as<UdpPacket>()->ecn = Ipv6Packet::EcnType::ECN_NOT_ECT;
  yellowPkt1.as<UdpPacket>()->ecn = Ipv6Packet::EcnType::ECN_NOT_ECT;
  yellowPkt2.as<UdpPacket>()->ecn = Ipv6Packet::EcnType::ECN_NOT_ECT;
  redPkt1.as<UdpPacket>()->ecn = Ipv6Packet::EcnType::ECN_NOT_ECT;
  redPkt2.as<UdpPacket>()->ecn = Ipv6Packet::EcnType::ECN_NOT_ECT;

  auto [cos, enq] = q.pushPacket(redPkt1);
  EXPECT_TRUE(enq);
  scheduler.run(Time::fromMs(1));
  std::tie(cos, enq) = q.pushPacket(redPkt2);
  EXPECT_FALSE(enq);

  std::tie(cos, enq) = q.pushPacket(yellowPkt1);
  EXPECT_TRUE(enq);
  scheduler.run(Time::fromMs(1));
  std::tie(cos, enq) = q.pushPacket(yellowPkt2);
  EXPECT_FALSE(enq);

  std::tie(cos, enq) = q.pushPacket(greenPkt1);
  EXPECT_TRUE(enq);
  scheduler.run(Time::fromMs(1));
  std::tie(cos, enq) = q.pushPacket(greenPkt2);
  EXPECT_FALSE(enq);
}
} // namespace packsim
