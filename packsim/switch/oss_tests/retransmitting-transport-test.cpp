/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <vector>

#include <folly/Utility.h>
#include <gtest/gtest.h>

#include "packsim/packet/base.h"
#include "packsim/packet/roce.h"
#include "packsim/switch/retransmitting-transport.h"

namespace packsim {

// The type-only half gates a drop-vs-throw decision on the packet path, so it
// must fold at compile time.
static_assert(!isAlwaysRetransmittingType(PacketType::ROCE));

TEST(IsAlwaysRetransmittingType, TypesWithoutUnconditionalRetransmitExcluded) {
  // Every PacketType outside the always-retransmitting list belongs here. ROCE
  // sits here because whether it retransmits depends on config, not on the
  // type.
  const std::vector<PacketType> notRetransmitting = {
      PacketType::RAW,
      PacketType::ETHERNET,
      PacketType::ETHERNET_PFC,
      PacketType::IPV6,
      PacketType::UDP,
      PacketType::ROCE,
      PacketType::PING,
      PacketType::ETHERNET_CC,
      PacketType::CTLOS,
      PacketType::CTLOS_CF_UPDATE,
      PacketType::CTLOS_LLR};

  for (const auto pktType : notRetransmitting) {
    const int pktTypeValue = folly::to_underlying(pktType);
    EXPECT_FALSE(isAlwaysRetransmittingType(pktType))
        << "packet type " << pktTypeValue
        << " does not retransmit unconditionally";
  }
}

TEST(IsRetransmittingTransport, RoceWithLossRecoveryIsRecoverable) {
  RocePacket pkt;
  pkt.lossRecoveryEnabled = true;
  EXPECT_TRUE(isRetransmittingTransport(pkt))
      << "a RoCEv2 QP configured with goBackN will retransmit a lost packet, "
         "so dropping it is recoverable";
}

TEST(IsRetransmittingTransport, RoceWithoutLossRecoveryIsUnrecoverable) {
  RocePacket pkt;
  pkt.lossRecoveryEnabled = false;
  EXPECT_FALSE(isRetransmittingTransport(pkt))
      << "a RoCEv2 QP configured with dummy loss recovery never retransmits, "
         "so a drop is unrecoverable and must stay fatal";
}

TEST(IsRetransmittingTransport, NonRetransmittingPacketIsUnrecoverable) {
  UdpPacket udp;
  EXPECT_FALSE(isRetransmittingTransport(udp));
}

TEST(IsRetransmittingTransport, IgnoresAPktTypeThatLiesAboutTheObject) {
  // `pktType` is a mutable public field and callers do overwrite it —
  // fb-switch-test.cpp in this directory allocates a UdpPacket and stamps
  // PacketType::ROCE on it to drive a RoCE code path. Classifying on the tag
  // and casting would read `lossRecoveryEnabled` off an object that has no
  // such member; the predicate asks the object what it is instead.
  UdpPacket notReallyRoce;
  notReallyRoce.pktType = PacketType::ROCE;
  EXPECT_FALSE(isRetransmittingTransport(notReallyRoce));
}

} // namespace packsim
