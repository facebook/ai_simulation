/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/kernel/device-id.h"
#include "packsim/packet/base.h"
#include "packsim/packet/roce.h"

namespace packsim {

TEST(BthOpcodeToString, MapsEachKnownOpcode) {
  EXPECT_STREQ(bthOpcodeToString(BthOpcode::SEND_FIRST), "SEND_FIRST");
  EXPECT_STREQ(bthOpcodeToString(BthOpcode::SEND_MIDDLE), "SEND_MIDDLE");
  EXPECT_STREQ(bthOpcodeToString(BthOpcode::SEND_LAST), "SEND_LAST");
  EXPECT_STREQ(
      bthOpcodeToString(BthOpcode::SEND_FIRST_AND_LAST), "SEND_FIRST_AND_LAST");
  EXPECT_STREQ(bthOpcodeToString(BthOpcode::CNP), "CNP");
}

TEST(BthOpcodeToString, UnknownOpcodeFallsBackToUnknown) {
  EXPECT_STREQ(bthOpcodeToString(BthOpcode{200}), "UNKNOWN");
}

TEST(RocePacketSetOpcode, FirstAndLastPacket) {
  RocePacket pkt;
  pkt.setOpcode(/*isFirstPkt=*/true, /*isLastPkt=*/true);
  EXPECT_EQ(pkt.opcode, BthOpcode::SEND_FIRST_AND_LAST);
  EXPECT_TRUE(pkt.isLastPkt());
}

TEST(RocePacketSetOpcode, FirstPacketOnly) {
  RocePacket pkt;
  pkt.setOpcode(/*isFirstPkt=*/true, /*isLastPkt=*/false);
  EXPECT_EQ(pkt.opcode, BthOpcode::SEND_FIRST);
  EXPECT_FALSE(pkt.isLastPkt());
}

TEST(RocePacketSetOpcode, LastPacketOnly) {
  RocePacket pkt;
  pkt.setOpcode(/*isFirstPkt=*/false, /*isLastPkt=*/true);
  EXPECT_EQ(pkt.opcode, BthOpcode::SEND_LAST);
  EXPECT_TRUE(pkt.isLastPkt());
}

TEST(RocePacketSetOpcode, MiddlePacket) {
  RocePacket pkt;
  pkt.setOpcode(/*isFirstPkt=*/false, /*isLastPkt=*/false);
  EXPECT_EQ(pkt.opcode, BthOpcode::SEND_MIDDLE);
  EXPECT_FALSE(pkt.isLastPkt());
}

TEST(RocePacketIsLastPkt, CnpCountsAsLast) {
  RocePacket pkt;
  pkt.opcode = BthOpcode::CNP;
  EXPECT_TRUE(pkt.isLastPkt());
}

TEST(RocePacketQpn, ReadsQpnsFromConnection) {
  const FlexDeviceId src(std::vector<uint32_t>{1, 0, 0, 0, 0, 0, 0, 1});
  const FlexDeviceId dst(std::vector<uint32_t>{2, 0, 0, 0, 0, 0, 0, 2});
  const QPConnection conn(src, dst, /*srcQPN=*/111, /*dstQPN=*/222);
  RocePacket pkt;
  pkt.conn = &conn;
  EXPECT_EQ(pkt.srcQPN(), 111u);
  EXPECT_EQ(pkt.dstQPN(), 222u);
}

TEST(RocePacketReset, RestoresRoceFieldDefaults) {
  RocePacket pkt;
  // Set every RoCE-owned field to a non-default value so reset() is exercised
  // for all of them.
  pkt.psn = 5;
  pkt.immData = 11;
  pkt.sequenceNum = 12;
  pkt.opcode = BthOpcode::SEND_LAST;
  pkt.nackTimer = 3;
  pkt.isAck = true;
  pkt.isNack = true;
  pkt.isCnp = true;
  pkt.ackReq = true;
  pkt.isWrite = true;
  pkt.isWriteImm = true;
  pkt.sendOrWriteData = 13;
  pkt.egressNicPort = 3;
  pkt.dstNicPort = 5;

  pkt.reset();

  EXPECT_EQ(pkt.psn, 0u);
  EXPECT_EQ(pkt.immData, 0u);
  EXPECT_EQ(pkt.sequenceNum, 0u);
  EXPECT_EQ(pkt.opcode, BthOpcode::SEND_FIRST);
  EXPECT_EQ(pkt.nackTimer, UINT8_MAX);
  EXPECT_FALSE(pkt.isAck);
  EXPECT_FALSE(pkt.isNack);
  EXPECT_FALSE(pkt.isCnp);
  EXPECT_FALSE(pkt.ackReq);
  EXPECT_FALSE(pkt.isWrite);
  EXPECT_FALSE(pkt.isWriteImm);
  EXPECT_EQ(pkt.sendOrWriteData, 0u);
  EXPECT_FALSE(pkt.egressNicPort.has_value());
  EXPECT_FALSE(pkt.dstNicPort.has_value());
}

} // namespace packsim
