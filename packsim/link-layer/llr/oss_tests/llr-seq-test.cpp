/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/llr/llr-seq.h"

#include <gtest/gtest.h>

namespace packsim::llr {
namespace {

// LlrSeq::kSeqMod / LlrSeq::kSeqHalf constants match spec values.
TEST(LlrSeqTest, ConstantsMatchSpec) {
  // UEC §5.1.5 p. 470: tx_seq is 20-bit, range 0 to 1048575.
  EXPECT_EQ(LlrSeq::kSeqMod, 1u << 20);
  EXPECT_EQ(LlrSeq::kSeqMod, 1048576u);
  // UEC §5.1.8 p. 476: half-space cutoff for ack_nack_seq_ok validation.
  EXPECT_EQ(LlrSeq::kSeqHalf, 1u << 19);
  EXPECT_EQ(LlrSeq::kSeqHalf, 524288u);
}

// seqInc wraps from 0xFFFFF to 0.
TEST(LlrSeqTest, SeqIncWraps) {
  EXPECT_EQ(LlrSeq::seqInc(0u), 1u);
  EXPECT_EQ(LlrSeq::seqInc(100u), 101u);
  EXPECT_EQ(LlrSeq::seqInc(0xFFFFEu), 0xFFFFFu);
  EXPECT_EQ(LlrSeq::seqInc(0xFFFFFu), 0u);
}

// seqDec wraps from 0 to 0xFFFFF.
TEST(LlrSeqTest, SeqDecWraps) {
  EXPECT_EQ(LlrSeq::seqDec(1u), 0u);
  EXPECT_EQ(LlrSeq::seqDec(0xFFFFFu), 0xFFFFEu);
  EXPECT_EQ(LlrSeq::seqDec(0u), 0xFFFFFu);
}

// Spec p. 471 Tbl 5-10 invariant: with tx_seq=0 and acked_seq=0xFFFFF
// (post-reset state per spec), outstanding_seq must be zero.
TEST(LlrSeqTest, OutstandingSeqAfterResetIsZero) {
  EXPECT_EQ(LlrSeq::outstandingSeq(/*txSeq=*/0u, /*ackedSeq=*/0xFFFFFu), 0u);
}

// outstandingSeq tracks unacked count after one frame TX.
TEST(LlrSeqTest, OutstandingSeqAfterOneTx) {
  // Send one frame: tx_seq → 1, acked_seq stays at 0xFFFFF.
  EXPECT_EQ(LlrSeq::outstandingSeq(/*txSeq=*/1u, /*ackedSeq=*/0xFFFFFu), 1u);
  // Then ACK: acked_seq → 0 (frame had seq 0). outstanding = 0.
  EXPECT_EQ(LlrSeq::outstandingSeq(/*txSeq=*/1u, /*ackedSeq=*/0u), 0u);
}

// outstandingSeq handles wraparound correctly.
TEST(LlrSeqTest, OutstandingSeqWraparound) {
  // Many frames sent; tx_seq has wrapped past 0.
  // tx_seq = 5, acked_seq = 0xFFFFE → 5 unacked frames (0xFFFFF, 0, 1, 2, 3, 4)
  // = (5 - 1) - 0xFFFFE mod 2^20 = 4 - 0xFFFFE mod 2^20
  //   = (4 + 2^20 - 0xFFFFE) mod 2^20
  //   = (4 + 0x100000 - 0xFFFFE) mod 2^20
  //   = (4 + 2) mod 2^20 = 6
  EXPECT_EQ(LlrSeq::outstandingSeq(/*txSeq=*/5u, /*ackedSeq=*/0xFFFFEu), 6u);
}

// ackNackSeqOk returns TRUE for a valid in-window seq.
TEST(LlrSeqTest, AckNackSeqOkValid) {
  // tx_seq = 100, acked_seq = 50. Window is [50, 99].
  EXPECT_TRUE(LlrSeq::ackNackSeqOk(/*ackNackSeq=*/50u, 100u, 50u));
  EXPECT_TRUE(LlrSeq::ackNackSeqOk(/*ackNackSeq=*/75u, 100u, 50u));
  EXPECT_TRUE(LlrSeq::ackNackSeqOk(/*ackNackSeq=*/99u, 100u, 50u));
}

// ackNackSeqOk returns FALSE for out-of-window seqs.
TEST(LlrSeqTest, AckNackSeqOkInvalid) {
  // tx_seq = 100, acked_seq = 50. Window is [50, 99].
  // 100 is the next-to-be-sent — out of window.
  EXPECT_FALSE(LlrSeq::ackNackSeqOk(/*ackNackSeq=*/100u, 100u, 50u));
  // 49 is "before" acked_seq — out of window.
  EXPECT_FALSE(LlrSeq::ackNackSeqOk(/*ackNackSeq=*/49u, 100u, 50u));
}

// ackNackSeqOk handles wraparound: window spans the wrap point.
TEST(LlrSeqTest, AckNackSeqOkWraparound) {
  // tx_seq = 5, acked_seq = 0xFFFFE → window is [0xFFFFE, 0xFFFFF, 0, 1, 2, 3,
  // 4].
  EXPECT_TRUE(LlrSeq::ackNackSeqOk(0xFFFFEu, 5u, 0xFFFFEu));
  EXPECT_TRUE(LlrSeq::ackNackSeqOk(0xFFFFFu, 5u, 0xFFFFEu));
  EXPECT_TRUE(LlrSeq::ackNackSeqOk(0u, 5u, 0xFFFFEu));
  EXPECT_TRUE(LlrSeq::ackNackSeqOk(4u, 5u, 0xFFFFEu));
  // 5 is tx_seq itself — out of window.
  EXPECT_FALSE(LlrSeq::ackNackSeqOk(5u, 5u, 0xFFFFEu));
  // 0xFFFFD is before acked_seq — out of window.
  EXPECT_FALSE(LlrSeq::ackNackSeqOk(0xFFFFDu, 5u, 0xFFFFEu));
}

// isBehind: spec p. 476 (Tbl 5-11) duplicate_frame computation.
TEST(LlrSeqTest, IsBehindWithoutWrap) {
  // next_rx_seq = 100. Frames with seq 0-99 are "behind" (duplicates).
  EXPECT_TRUE(LlrSeq::isBehind(/*frameSeq=*/0u, /*nextRxSeq=*/100u));
  EXPECT_TRUE(LlrSeq::isBehind(50u, 100u));
  EXPECT_TRUE(LlrSeq::isBehind(99u, 100u));
  // 100 is expected (caller checks expected_frame separately, but isBehind
  // returns true since (100 - 100) % mod = 0, which is <= half).
  EXPECT_TRUE(LlrSeq::isBehind(100u, 100u));
  // 101 is "ahead" — not behind.
  EXPECT_FALSE(LlrSeq::isBehind(101u, 100u));
  // At exactly the half-space boundary, the spec formula `<= 524288`
  // includes the boundary as "behind".
  EXPECT_TRUE(LlrSeq::isBehind(100u + LlrSeq::kSeqHalf, 100u));
  // Distance just under half — unambiguously "ahead" (modular subtraction
  // gives 524289 > LlrSeq::kSeqHalf, so isBehind returns FALSE).
  EXPECT_FALSE(LlrSeq::isBehind(100u + LlrSeq::kSeqHalf - 1u, 100u));
}

// isBehind with wraparound: next_rx_seq is just past zero.
TEST(LlrSeqTest, IsBehindWithWrap) {
  // next_rx_seq = 5 (just wrapped). 0xFFFF0..0xFFFFF and 0..4 are behind.
  EXPECT_TRUE(LlrSeq::isBehind(0xFFFFFu, 5u));
  EXPECT_TRUE(LlrSeq::isBehind(0xFFFF0u, 5u));
  EXPECT_TRUE(LlrSeq::isBehind(0u, 5u));
  EXPECT_TRUE(LlrSeq::isBehind(4u, 5u));
  // 6 is ahead (missing).
  EXPECT_FALSE(LlrSeq::isBehind(6u, 5u));
}

} // namespace
} // namespace packsim::llr
