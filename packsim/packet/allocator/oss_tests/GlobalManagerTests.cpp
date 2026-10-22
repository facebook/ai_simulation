/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>
#include <typeinfo>

#include <gtest/gtest.h>

#include "packsim/packet/allocator/GlobalManager.h"
#include "packsim/packet/base.h"
#include "packsim/packet/roce.h"

namespace packsim {

TEST(GlobalManagerTests, TestTakeAndReturn) {
  auto* ethernetPfcPacketPkt =
      packsim::GlobalManager::takePacket<packsim::EthernetPfcPacket>();
  auto* udpPacketPkt =
      packsim::GlobalManager::takePacket<packsim::PacketType::UDP>();

  ASSERT_EQ(ethernetPfcPacketPkt->pktType, packsim::PacketType::ETHERNET_PFC);
  ASSERT_EQ(udpPacketPkt->pktType, packsim::PacketType::UDP);

  packsim::GlobalManager::returnPacket(ethernetPfcPacketPkt);
  packsim::GlobalManager::returnPacket(udpPacketPkt);
}

// `pktType` is a mutable public field, and callers do overwrite it -- e.g.
// switch/oss_tests/fb-switch-test.cpp allocates a UdpPacket and forces
// PacketType::ROCE onto it to drive a RoCE code path. The pool must file the
// packet under what it *is*, not what the field claims: filing this UdpPacket
// allocation in RocePacket's pool would hand it back from
// takePacket<RocePacket>(), and writing RocePacket's members (psn, opcode,
// the two std::optionals ...) would run off the end of the smaller object.
TEST(GlobalManagerTests, FilesPacketByDynamicTypeNotPktTypeField) {
  auto* udp = packsim::GlobalManager::takePacket<packsim::PacketType::UDP>();
  udp->pktType = packsim::PacketType::ROCE;
  packsim::GlobalManager::returnPacket(udp);

  // The mislabelled packet must not surface from RocePacket's pool.
  auto* roce = packsim::GlobalManager::takePacket<packsim::PacketType::ROCE>();
  EXPECT_NE(static_cast<void*>(roce), static_cast<void*>(udp));
  EXPECT_EQ(typeid(*roce), typeid(packsim::RocePacket));

  // It must still be at the top of the pool it was actually allocated from.
  auto* udpAgain =
      packsim::GlobalManager::takePacket<packsim::PacketType::UDP>();
  EXPECT_EQ(static_cast<void*>(udpAgain), static_cast<void*>(udp));

  packsim::GlobalManager::returnPacket(roce);
  packsim::GlobalManager::returnPacket(udpAgain);
}

} // namespace packsim
