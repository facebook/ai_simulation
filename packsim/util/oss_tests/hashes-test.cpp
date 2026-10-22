/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/kernel/device-id.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/util/hashes.h"

namespace packsim {

// --------------------------------------------------------------------------
// Packet-based hashes (folly5TupleHash)
// --------------------------------------------------------------------------

class HashesPacketTest : public ::testing::Test {
 protected:
  // srcId()/dstId() on the packet dereference `conn`, so the connection must
  // outlive every packet built from this fixture.
  FlexDeviceId srcId_{std::vector<uint32_t>{1, 2, 3}};
  FlexDeviceId dstId_{std::vector<uint32_t>{4, 5, 6}};
  IPConnection conn_{srcId_, dstId_};

  PacketPtr makeUdp(uint16_t srcPort, uint16_t dstPort) {
    auto packet = createPacket<PacketType::UDP>(/*payloadSize=*/1000);
    auto* udp = packet.as<UdpPacket>();
    udp->conn = &conn_;
    udp->srcPort = srcPort;
    udp->dstPort = dstPort;
    return packet;
  }
};

TEST_F(HashesPacketTest, Folly5TupleHashPacketMatchesFieldOverload) {
  constexpr uint32_t kSalt = 0xABCDEF;
  const auto packet = makeUdp(/*srcPort=*/1234, /*dstPort=*/5678);
  EXPECT_EQ(
      folly5TupleHash(packet, kSalt),
      folly5TupleHash(srcId_, dstId_, 1234, 5678, kSalt));
}

TEST_F(HashesPacketTest, Folly5TupleHashChangesWithSalt) {
  EXPECT_NE(
      folly5TupleHash(srcId_, dstId_, 1234, 5678, 1),
      folly5TupleHash(srcId_, dstId_, 1234, 5678, 2));
}

TEST_F(HashesPacketTest, Folly5TupleHashIsSensitiveToDevicesAndPorts) {
  constexpr uint32_t kSalt = 3;
  const uint64_t base = folly5TupleHash(srcId_, dstId_, 1234, 5678, kSalt);
  EXPECT_NE(base, folly5TupleHash(dstId_, srcId_, 1234, 5678, kSalt)); // ids
  EXPECT_NE(base, folly5TupleHash(srcId_, dstId_, 5678, 1234, kSalt)); // ports
}

} // namespace packsim
