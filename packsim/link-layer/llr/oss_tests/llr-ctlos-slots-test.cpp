/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/llr/llr-ctlos-slots.h"

#include <gtest/gtest.h>

namespace packsim::llr {
namespace {

class LlrCtlosSlotsTest : public ::testing::Test {
 protected:
  // EthernetPort wires this on every production instance
  // (eth-port.cpp `setOnPostCallback`), so a directly-built one has to
  // satisfy the same contract. Tests asserting on posts install their own.
  void SetUp() override {
    slots_.setOnPostCallback([] {});
  }

  LlrCtlosSlots slots_;
};

// Empty queue: popHighest returns nullopt.
TEST_F(LlrCtlosSlotsTest, EmptyReturnsNullopt) {
  EXPECT_TRUE(slots_.empty());
  EXPECT_FALSE(slots_.popHighest().has_value());
}

// Single LLR_ACK post and pop.
TEST_F(LlrCtlosSlotsTest, SingleAckRoundtrip) {
  slots_.postAck(42);
  EXPECT_FALSE(slots_.empty());
  EXPECT_TRUE(slots_.hasAckNack());

  auto frame = slots_.popHighest();
  ASSERT_TRUE(frame.has_value());
  EXPECT_EQ(frame->type, LlrCtlosType::LLR_ACK);
  EXPECT_EQ(frame->seq, 42u);
  EXPECT_TRUE(slots_.empty());
}

// Spec Tbl 5-12 priority: LLR_INIT_ECHO > LLR_INIT > LLR_ACK/LLR_NACK.
// At most one pending per priority level — see header docstring for the
// per-level rationale.
TEST_F(LlrCtlosSlotsTest, PriorityOrderEchoFirst) {
  slots_.postAck(1);
  slots_.postInit(2, /*initData=*/0xAA);
  slots_.postInitEcho(3, /*initData=*/0xBB);
  EXPECT_TRUE(slots_.hasInitEcho());
  EXPECT_TRUE(slots_.hasInit());
  EXPECT_TRUE(slots_.hasAckNack());

  // Pop in priority order.
  auto first = slots_.popHighest();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first->type, LlrCtlosType::LLR_INIT_ECHO);
  EXPECT_EQ(first->seq, 3u);

  auto second = slots_.popHighest();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->type, LlrCtlosType::LLR_INIT);
  EXPECT_EQ(second->seq, 2u);

  auto third = slots_.popHighest();
  ASSERT_TRUE(third.has_value());
  EXPECT_EQ(third->type, LlrCtlosType::LLR_ACK);
  EXPECT_EQ(third->seq, 1u);

  EXPECT_TRUE(slots_.empty());
}

// Spec §5.1.9.1.3 — successive LLR_ACK posts coalesce: only the latest
// cumulative seq is sent. The slot-based buffer encodes this as an
// overwrite.
TEST_F(LlrCtlosSlotsTest, AckPostOverwritesLatestSeq) {
  slots_.postAck(10);
  slots_.postAck(20);
  slots_.postAck(30);
  EXPECT_TRUE(slots_.hasAckNack());

  auto frame = slots_.popHighest();
  ASSERT_TRUE(frame.has_value());
  EXPECT_EQ(frame->type, LlrCtlosType::LLR_ACK);
  EXPECT_EQ(frame->seq, 30u);
  EXPECT_TRUE(slots_.empty()) << "three posts must overwrite, not accumulate";
}

// Spec §5.1.6 — the AN-SM (SEND_ACKS ⊕ NACK_SENT) guarantees ACK and
// NACK are mutually exclusive. The shared AckNack slot encodes this:
// a NACK post overwrites any stale ACK, and vice versa (the latter
// case cannot legitimately happen at runtime — the AN-SM does not
// re-post an ACK after NACK_SENT until the missing seq arrives —
// but the slot's overwrite semantics still produce a well-defined
// behavior).
TEST_F(LlrCtlosSlotsTest, NackOverwritesPendingAck) {
  slots_.postAck(10);
  slots_.postNack(20);
  EXPECT_TRUE(slots_.hasAckNack());

  auto frame = slots_.popHighest();
  ASSERT_TRUE(frame.has_value());
  EXPECT_EQ(frame->type, LlrCtlosType::LLR_NACK);
  EXPECT_EQ(frame->seq, 20u);
  EXPECT_TRUE(slots_.empty());
}

// Successive LLR_INIT posts overwrite — only the latest
// `init_data` value matters per spec §5.1.4. The periodic
// `Llr::initCtlosLoop` fires every 4 × `ctlosTargetSpacingBytes` of wire
// time; if a previous INIT is still pending when the next tick fires, the
// new one supersedes it.
TEST_F(LlrCtlosSlotsTest, InitPostOverwritesLatestData) {
  slots_.postInit(1, 0xAA);
  slots_.postInit(2, 0xBB);
  EXPECT_TRUE(slots_.hasInit());

  auto frame = slots_.popHighest();
  ASSERT_TRUE(frame.has_value());
  EXPECT_EQ(frame->type, LlrCtlosType::LLR_INIT);
  EXPECT_EQ(frame->seq, 2u);
  EXPECT_EQ(frame->initData, 0xBBu);
  EXPECT_TRUE(slots_.empty()) << "second post must overwrite, not accumulate";
}

// Successive LLR_INIT_ECHO posts overwrite — the peer cares only
// that an echo was sent in response to its most recent INIT (per spec
// §5.1.6 it stops sending INITs after receiving any echo).
TEST_F(LlrCtlosSlotsTest, InitEchoPostOverwritesLatest) {
  slots_.postInitEcho(1, 0xAA);
  slots_.postInitEcho(2, 0xBB);
  EXPECT_TRUE(slots_.hasInitEcho());

  auto frame = slots_.popHighest();
  ASSERT_TRUE(frame.has_value());
  EXPECT_EQ(frame->type, LlrCtlosType::LLR_INIT_ECHO);
  EXPECT_EQ(frame->seq, 2u);
  EXPECT_EQ(frame->initData, 0xBBu);
  EXPECT_TRUE(slots_.empty()) << "second post must overwrite, not accumulate";
}

// LLR_INIT_ECHOs preserve init_seq AND init_data per spec p. 477 §5.1.9.1.2.
TEST_F(LlrCtlosSlotsTest, InitEchoPreservesData) {
  slots_.postInitEcho(0x12345, 0xCAFE);
  auto frame = slots_.popHighest();
  ASSERT_TRUE(frame.has_value());
  EXPECT_EQ(frame->type, LlrCtlosType::LLR_INIT_ECHO);
  EXPECT_EQ(frame->seq, 0x12345u);
  EXPECT_EQ(frame->initData, 0xCAFEu);
}

TEST_F(LlrCtlosSlotsTest, PostAckFiresOnPostCallback) {
  int count = 0;
  slots_.setOnPostCallback([&]() { ++count; });
  slots_.postAck(7);
  EXPECT_EQ(count, 1);
}

TEST_F(LlrCtlosSlotsTest, PostInitFiresOnPostCallback) {
  int count = 0;
  slots_.setOnPostCallback([&]() { ++count; });
  slots_.postInit(8, 0);
  EXPECT_EQ(count, 1);
}

TEST_F(LlrCtlosSlotsTest, PostInitEchoFiresOnPostCallback) {
  int count = 0;
  slots_.setOnPostCallback([&]() { ++count; });
  slots_.postInitEcho(9, 0);
  EXPECT_EQ(count, 1);
}

// Overwriting an in-slot ACK with a newer ACK still fires — the seq
// changed and the consumer may want a wake to drain the latest value.
TEST_F(LlrCtlosSlotsTest, OverwriteAckFiresOnPostCallback) {
  int count = 0;
  slots_.postAck(7);
  slots_.setOnPostCallback([&]() { ++count; });
  slots_.postAck(11);
  EXPECT_EQ(count, 1);
}

TEST_F(LlrCtlosSlotsTest, PostNackFiresOnPostCallback) {
  int count = 0;
  slots_.setOnPostCallback([&]() { ++count; });
  slots_.postNack(10);
  EXPECT_EQ(count, 1);
}

// The onPost subscriber is required: posting without one fails the XCHECK in
// `LlrCtlosSlots::onPost`.
TEST(LlrCtlosSlotsDeathTest, PostWithoutCallbackAborts) {
  LlrCtlosSlots slots;
  EXPECT_DEATH(slots.postAck(1), "no post subscriber registered");
}

// The bound is structural: one slot per priority level (INIT_ECHO, INIT and
// the shared ACK/NACK slot), so one post at each level fills every slot and
// further posts overwrite within their level.
TEST_F(LlrCtlosSlotsTest, MaxDepthIsThree) {
  slots_.postInitEcho(1, 0);
  slots_.postInit(2, 0);
  slots_.postAck(3);
  EXPECT_TRUE(slots_.hasInitEcho());
  EXPECT_TRUE(slots_.hasInit());
  EXPECT_TRUE(slots_.hasAckNack());

  // Posting more of any type overwrites within its slot; there is no fourth
  // slot to fill, which the three pops plus `empty()` below demonstrate.
  slots_.postAck(4);
  slots_.postInit(5, 0xFF);
  slots_.postInitEcho(6, 0xEE);

  // Pop in priority — each slot returns its latest value.
  auto first = slots_.popHighest();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first->type, LlrCtlosType::LLR_INIT_ECHO);
  EXPECT_EQ(first->seq, 6u);

  auto second = slots_.popHighest();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->type, LlrCtlosType::LLR_INIT);
  EXPECT_EQ(second->seq, 5u);

  auto third = slots_.popHighest();
  ASSERT_TRUE(third.has_value());
  EXPECT_EQ(third->type, LlrCtlosType::LLR_ACK);
  EXPECT_EQ(third->seq, 4u);

  EXPECT_TRUE(slots_.empty());
}

} // namespace
} // namespace packsim::llr
