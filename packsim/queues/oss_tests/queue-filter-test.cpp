/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <stdexcept>
#include <string>

#include <folly/Utility.h>
#include <folly/lang/Hint.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/switch_config_types.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/queues/queue-filter.h"
// @oss-disable

namespace packsim {
namespace {

using DscpType = Ipv6Packet::DscpType;
using packsim::thrift::PacketColor;
using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

uint8_t dscpKey(DscpType dscp) {
  return folly::to_underlying(dscp);
}

packsim::thrift::QueueFilterConfig makeQueueFilterCfg() {
  packsim::thrift::QueueFilterConfig cfg;
  cfg.fieldToQueue()[dscpKey(DscpType::DSCP_CS1)] = 1;
  cfg.fieldToQueue()[dscpKey(DscpType::DSCP_CS3)] = 3;
  return cfg;
}

PacketPtr makeUdpPacket(DscpType dscp) {
  auto pkt = createPacket<PacketType::UDP>(/*payloadSize=*/1000);
  pkt.as<UdpPacket>()->dscp = dscp;
  return pkt;
}

TEST(DscpQueueFilterTest, SelectsConfiguredQueueForMappedDscp) {
  const DscpQueueFilter filter(makeQueueFilterCfg());
  EXPECT_EQ(filter.selectQueueToPush(makeUdpPacket(DscpType::DSCP_CS1)), 1u);
  EXPECT_EQ(filter.selectQueueToPush(makeUdpPacket(DscpType::DSCP_CS3)), 3u);
}

TEST(DscpQueueFilterTest, ReturnsDefaultQueueForUnmappedDscp) {
  const DscpQueueFilter filter(makeQueueFilterCfg());
  // DSCP_CS5 is not present in the filter config.
  EXPECT_EQ(filter.selectQueueToPush(makeUdpPacket(DscpType::DSCP_CS5)), 0u);
}

TEST(DscpQueueFilterTest, ReturnsDefaultQueueForNonIpv6Packet) {
  const DscpQueueFilter filter(makeQueueFilterCfg());
  // ETHERNET_PFC sorts below IPV6, so `tryAs<Ipv6Packet>()` yields nullptr.
  auto pkt = createPacket<PacketType::ETHERNET_PFC>();
  EXPECT_EQ(filter.selectQueueToPush(pkt), 0u);
}

TEST(DscpQueueFilterTest, ColorizeAppliesConfiguredColor) {
  packsim::thrift::QueueFilterConfig cfg = makeQueueFilterCfg();
  cfg.fieldToColor()[dscpKey(DscpType::DSCP_CS2)] = PacketColor::RED;
  const DscpQueueFilter filter(cfg);
  auto pkt = makeUdpPacket(DscpType::DSCP_CS2);
  filter.colorize(pkt);
  EXPECT_EQ(pkt.as<EthernetPacket>()->tag.color, PacketColor::RED);
}

TEST(DscpQueueFilterTest, ColorizeDefaultsToGreenForUnmappedDscp) {
  packsim::thrift::QueueFilterConfig cfg = makeQueueFilterCfg();
  cfg.fieldToColor()[dscpKey(DscpType::DSCP_CS2)] = PacketColor::RED;
  const DscpQueueFilter filter(cfg);
  auto pkt = makeUdpPacket(DscpType::DSCP_CS5); // no color mapping
  // Pre-seed a non-default color so the assertion proves colorize actually
  // wrote GREEN rather than passing on the packet's default.
  pkt.as<EthernetPacket>()->tag.color = PacketColor::RED;
  filter.colorize(pkt);
  EXPECT_EQ(pkt.as<EthernetPacket>()->tag.color, PacketColor::GREEN);
}

// DSCP is 6 bits, so 64 is the smallest key no packet can carry.
constexpr uint8_t kFirstNonDscpKey = 64;

TEST(DscpQueueFilterTest, ColorizeDefaultsToGreenForNonIpv6Packet) {
  // Every DSCP is RED, so only skipping the DSCP lookup yields GREEN.
  packsim::thrift::QueueFilterConfig cfg = makeQueueFilterCfg();
  for (uint8_t dscp = 0; dscp < kFirstNonDscpKey; ++dscp) {
    cfg.fieldToColor()[dscp] = PacketColor::RED;
  }
  const DscpQueueFilter filter(cfg);
  auto pkt = createPacket<PacketType::ETHERNET_PFC>();
  pkt.as<EthernetPacket>()->tag.color = PacketColor::RED;
  filter.colorize(pkt);
  EXPECT_EQ(pkt.as<EthernetPacket>()->tag.color, PacketColor::GREEN);
}

TEST(DscpQueueFilterTest, AcceptsTheLargestDscpKey) {
  constexpr uint8_t kLargestDscp = kFirstNonDscpKey - 1;
  packsim::thrift::QueueFilterConfig cfg = makeQueueFilterCfg();
  cfg.fieldToQueue()[kLargestDscp] = 2;
  cfg.fieldToColor()[kLargestDscp] = PacketColor::RED;
  EXPECT_NO_THROW(DscpQueueFilter{cfg});
}

TEST(DscpQueueFilterTest, RejectsQueueMappingForNonDscpKey) {
  packsim::thrift::QueueFilterConfig cfg = makeQueueFilterCfg();
  cfg.fieldToQueue()[kFirstNonDscpKey] = 1;
  EXPECT_THAT(
      [&cfg]() { const DscpQueueFilter filter(cfg); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("QueueFilterConfig.fieldToQueue has DSCP key 64")));
}

TEST(DscpQueueFilterTest, RejectsColorMappingForNonDscpKey) {
  packsim::thrift::QueueFilterConfig cfg = makeQueueFilterCfg();
  cfg.fieldToColor()[kFirstNonDscpKey] = PacketColor::RED;
  EXPECT_THAT(
      [&cfg]() { const DscpQueueFilter filter(cfg); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("QueueFilterConfig.fieldToColor has DSCP key 64")));
}

} // namespace
} // namespace packsim
