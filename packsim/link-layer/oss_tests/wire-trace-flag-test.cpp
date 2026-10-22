/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/wire-trace-flag.h"

#include <utility>

#include <gtest/gtest.h>

#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {
namespace {

// (a) Non-Ethernet packet — neither `is<>` branch matches, so the helper
// returns defaults (type=-1, seq=0).
TEST(LlrTraceFieldsTest, ExtractFromNonEthernetReturnsDefaults) {
  auto pkt = createPacket<PacketType::UDP>(100);
  const auto fields = LlrTraceFields::extractFrom(pkt);
  EXPECT_EQ(fields.type, -1);
  EXPECT_EQ(fields.seq, 0u);
}

// (b) Ethernet packet with `llrHeader.enabled=false` — gated branch
// skipped, defaults returned.
TEST(LlrTraceFieldsTest, ExtractFromEthernetLlrDisabledReturnsDefaults) {
  auto pkt = createPacket<PacketType::ETHERNET>(100);
  // Default-constructed LlrHeader has enabled=false; assert that
  // explicitly so the test fails fast if the default ever changes.
  ASSERT_FALSE(pkt.as<EthernetPacket>()->llrHeader.enabled);
  const auto fields = LlrTraceFields::extractFrom(pkt);
  EXPECT_EQ(fields.type, -1);
  EXPECT_EQ(fields.seq, 0u);
}

// (c) An LLR data frame is an ordinary Ethernet frame carrying a sequence
// number, and reports the DATA column value.
TEST(LlrTraceFieldsTest, ExtractFromAnLlrDataFrameReportsDataAndSeq) {
  auto pkt = createPacket<PacketType::ETHERNET>(100);
  auto* eth = pkt.as<EthernetPacket>();
  eth->llrHeader.enabled = true;
  eth->llrHeader.seqNum = 42u;

  const auto fields = LlrTraceFields::extractFrom(pkt);
  EXPECT_EQ(fields.type, 0) << "0 is the DATA column value";
  EXPECT_EQ(fields.seq, 42u);
}

// (d) Each LLR ordered set reports its own column value. The expected values
// are written as literals rather than cast from `LlrCtlosType`, because the
// encoding is deliberately independent of the enum's order; see `traceType`
// in wire-trace-flag.cpp.
TEST(LlrTraceFieldsTest, ExtractFromAnLlrCtlosFrameReportsItsType) {
  const std::pair<LlrCtlosType, int> cases[] = {
      {LlrCtlosType::LLR_ACK, 1},
      {LlrCtlosType::LLR_NACK, 2},
      {LlrCtlosType::LLR_INIT, 3},
      {LlrCtlosType::LLR_INIT_ECHO, 4},
  };
  for (const auto& [type, expected] : cases) {
    auto pkt = createPacket<PacketType::CTLOS_LLR>(0);
    auto* ctlos = pkt.as<LlrCtlosFrame>();
    ctlos->payload = {type, /*seq=*/7u, /*initData=*/0};

    const auto fields = LlrTraceFields::extractFrom(pkt);
    EXPECT_EQ(fields.type, expected) << "type " << static_cast<int>(type);
    EXPECT_EQ(fields.seq, 7u);
  }
}

// (e) A CBFC ordered set is a `CtlosFrame` but not an LLR one. This case is
// what proves `extractFrom` tests for `LlrCtlosFrame` itself: a test against
// the shared `CtlosFrame` base would claim this frame as an LLR one.
TEST(LlrTraceFieldsTest, ExtractFromACfUpdateFrameReportsNoLlrState) {
  auto pkt = createPacket<PacketType::CTLOS_CF_UPDATE>(0);

  const auto fields = LlrTraceFields::extractFrom(pkt);
  EXPECT_EQ(fields.type, -1);
  EXPECT_EQ(fields.seq, 0u);
}

} // namespace
} // namespace packsim
