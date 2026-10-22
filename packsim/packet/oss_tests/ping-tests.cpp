/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/packet/allocator/GlobalManager.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ping.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/packet/type_mapper.h"

namespace packsim {

// Compile-time contract: the two PacketType<->PingPacket mappings added with
// this type must stay in sync in both directions.
static_assert(
    std::is_same_v<PacketTypeMapper<PacketType::PING>::type, PingPacket>);
static_assert(getPacketTypeEnum<PingPacket>() == PacketType::PING);

TEST(PingPacketTest, CreateRequest) {
  PingPacket pkt(PingType::REQUEST, 42);
  EXPECT_EQ(pkt.pktType, PacketType::PING);
  EXPECT_EQ(pkt.pingType, PingType::REQUEST);
  EXPECT_EQ(pkt.pingId, 42u);
  EXPECT_EQ(pkt.sentTimeFs, 0);
  EXPECT_GT(pkt.getSize(), 0u);
}

TEST(PingPacketTest, Reset) {
  PingPacket pkt(PingType::REPLY, 123);
  pkt.sentTimeFs = 5000;
  pkt.payloadSize = 999;
  pkt.reset();
  EXPECT_EQ(pkt.pktType, PacketType::PING);
  EXPECT_EQ(pkt.pingType, PingType::REQUEST);
  EXPECT_EQ(pkt.pingId, 0u);
  EXPECT_EQ(pkt.sentTimeFs, 0);
  EXPECT_EQ(pkt.payloadSize, PingPacket::kPingPayloadSize)
      << "reset() must restore the fixed ping payload size so a pooled packet is reused fresh";
  EXPECT_EQ(pkt.getSize(), PingPacket().getSize());
}

TEST(PingPacketTest, PoolRecyclingYieldsFreshPacket) {
  // Exercises PingPacket's round trip through the pool: take a ping packet,
  // mutate it, return it, then confirm a freshly taken ping packet is in
  // fresh-construction state.
  auto* returned = GlobalManager::takePacket<PacketType::PING>();
  ASSERT_NE(returned, nullptr);
  returned->pingType = PingType::REPLY;
  returned->pingId = 7;
  returned->sentTimeFs = 12345;
  GlobalManager::returnPacket(returned);

  auto pkt = createPacket<PacketType::PING>(PingPacket::kPingPayloadSize);
  auto* fresh = pkt.as<PacketType::PING>();
  EXPECT_EQ(fresh->pktType, PacketType::PING);
  EXPECT_EQ(fresh->pingType, PingType::REQUEST);
  EXPECT_EQ(fresh->pingId, 0u);
  EXPECT_EQ(fresh->sentTimeFs, 0);
  EXPECT_EQ(fresh->payloadSize, PingPacket::kPingPayloadSize);
}

TEST(PingPacketTest, TraceHopsRecorded) {
  PingPacket pkt(PingType::REQUEST, 1);
  pkt.traceEnabled = true;

  // Simulate recording 3 hops.
  PingHopInfo hop1;
  hop1.deviceName = "RTSW_z0_r0";
  hop1.inPort = 0;
  hop1.queueDepthBytes = 1024;
  hop1.simTimeFs = 100'000'000;

  PingHopInfo hop2;
  hop2.deviceName = "SPINE_z0";
  hop2.inPort = 4;
  hop2.queueDepthBytes = 0;
  hop2.simTimeFs = 200'000'000;

  PingHopInfo hop3;
  hop3.deviceName = "RTSW_z0_r1";
  hop3.inPort = 12;
  hop3.queueDepthBytes = 512;
  hop3.simTimeFs = 300'000'000;

  pkt.hops.emplace_back(std::move(hop1));
  pkt.hops.emplace_back(std::move(hop2));
  pkt.hops.emplace_back(std::move(hop3));

  ASSERT_EQ(pkt.hops.size(), 3u);
  EXPECT_EQ(pkt.hops[0].deviceName, "RTSW_z0_r0");
  EXPECT_EQ(pkt.hops[1].deviceName, "SPINE_z0");
  EXPECT_EQ(pkt.hops[2].deviceName, "RTSW_z0_r1");
  EXPECT_EQ(pkt.hops[0].queueDepthBytes, 1024u);
  EXPECT_EQ(pkt.hops[2].inPort, 12u);
}

TEST(PingPacketTest, ResetClearsTraceFields) {
  PingPacket pkt(PingType::REQUEST, 42);
  pkt.traceEnabled = true;
  pkt.sentTimeFs = 1'000'000;
  pkt.hops.emplace_back(PingHopInfo{.deviceName = "sw0"});

  pkt.reset();

  EXPECT_FALSE(pkt.traceEnabled);
  EXPECT_EQ(pkt.sentTimeFs, 0);
  EXPECT_TRUE(pkt.hops.empty());
  EXPECT_EQ(pkt.pingType, PingType::REQUEST);
  EXPECT_EQ(pkt.pingId, 0u);
}

} // namespace packsim
