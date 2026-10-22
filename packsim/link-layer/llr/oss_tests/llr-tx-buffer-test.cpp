/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/llr/llr-tx-buffer.h"

#include <cstdint>
#include <utility>

#include <gtest/gtest.h>

#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim::llr {
namespace {

// Simple packet factory: creates an Ipv6Packet of `size` bytes total.
PacketPtr makePacket(uint32_t size) {
  return createPacket<PacketType::IPV6>(/*payloadSize=*/size);
}

// Initial state: empty buffer, acked_seq = 0xFFFFF (spec p. 471 reset).
TEST(LlrTxBufferTest, InitialState) {
  LlrTxBuffer buf{1024};
  EXPECT_EQ(buf.outstandingSeq(), 0u);
  EXPECT_EQ(buf.outstandingData(), 0u);
  EXPECT_EQ(buf.ackedSeq(), 0xFFFFFu); // spec-mandated reset value
  EXPECT_FALSE(buf.snapshotUnackedRange().has_value());
}

// tryAdd succeeds when within capacity and updates outstanding counters.
TEST(LlrTxBufferTest, AddFrameUpdatesOutstandingCounters) {
  LlrTxBuffer buf{8192};
  auto pkt = makePacket(100);
  const uint32_t pktSize = pkt->getSize();
  EXPECT_TRUE(buf.tryAdd(0u, std::move(pkt)));
  EXPECT_EQ(buf.outstandingSeq(), 1u);
  EXPECT_EQ(buf.outstandingData(), pktSize);
}

// tryAdd fails when over capacity; state unchanged.
TEST(LlrTxBufferTest, AddOverCapacityRejected) {
  // First packet's getSize() includes the IPv6 header; capacity = one
  // packet's total size minus a small margin so the second add overflows.
  auto sizingPkt = makePacket(100);
  const uint32_t pktSize = sizingPkt->getSize();
  // capacity slightly above one packet, below two
  LlrTxBuffer buf{pktSize + 10};
  EXPECT_TRUE(buf.tryAdd(0u, std::move(sizingPkt)));
  // Second add would push currentBytes_ over capacity.
  EXPECT_FALSE(buf.tryAdd(1u, makePacket(100)));
  EXPECT_EQ(buf.outstandingSeq(), 1u);
}

// removeUpTo with non-advancing seq is a no-op.
TEST(LlrTxBufferTest, RemoveUpToNonAdvancingIsNoop) {
  LlrTxBuffer buf{1024};
  buf.tryAdd(0u, makePacket(100));
  buf.setAckedSeq(0xFFFFFu);
  // ack_nack_seq == ackedSeq() → no advance.
  buf.removeUpTo(0xFFFFFu);
  EXPECT_EQ(buf.ackedSeq(), 0xFFFFFu);
  EXPECT_EQ(buf.outstandingSeq(), 1u);
}

// removeUpTo advances acked_seq and removes entries.
TEST(LlrTxBufferTest, RemoveUpToAdvancesAckedSeqAndRemovesEntries) {
  LlrTxBuffer buf{8192};
  buf.tryAdd(0u, makePacket(100));
  buf.tryAdd(1u, makePacket(200));

  buf.removeUpTo(1u); // ACKs both seq 0 and seq 1

  EXPECT_EQ(buf.ackedSeq(), 1u);
  EXPECT_EQ(buf.outstandingSeq(), 0u);
  EXPECT_EQ(buf.outstandingData(), 0u);
}

// Partial ACK removes only entries up to ackNackSeq.
TEST(LlrTxBufferTest, PartialAckRemovesOnlyAckedEntries) {
  LlrTxBuffer buf{8192};
  auto pkt2 = makePacket(300);
  const uint32_t pkt2Size = pkt2->getSize();
  buf.tryAdd(0u, makePacket(100));
  buf.tryAdd(1u, makePacket(200));
  buf.tryAdd(2u, std::move(pkt2));

  buf.removeUpTo(1u); // ACKs seq 0 and seq 1; seq 2 remains

  EXPECT_EQ(buf.ackedSeq(), 1u);
  EXPECT_EQ(buf.outstandingSeq(), 1u);
  EXPECT_EQ(buf.outstandingData(), pkt2Size);
  EXPECT_TRUE(buf.isUnacked(2u));
}

// discardAll empties the buffer without changing acked_seq.
TEST(LlrTxBufferTest, DiscardAllEmptiesBufferWithoutChangingAckedSeq) {
  LlrTxBuffer buf{1024};
  buf.tryAdd(0u, makePacket(100));
  buf.tryAdd(1u, makePacket(200));
  const uint32_t prevAcked = buf.ackedSeq();

  buf.discardAll();

  EXPECT_EQ(buf.outstandingSeq(), 0u);
  EXPECT_EQ(buf.outstandingData(), 0u);
  EXPECT_EQ(buf.ackedSeq(), prevAcked);
}

// snapshotUnackedRange returns nullopt when empty, valid range otherwise.
TEST(LlrTxBufferTest, SnapshotRange) {
  LlrTxBuffer buf{1024};
  EXPECT_FALSE(buf.snapshotUnackedRange().has_value());

  buf.tryAdd(5u, makePacket(100));
  buf.tryAdd(6u, makePacket(100));
  buf.tryAdd(7u, makePacket(100));
  // ackedSeq is 0xFFFFF (initial), so firstSeq = seqInc(0xFFFFF) = 0.
  // But the buffer's lowest seq is 5.
  // The design says firstSeq = seqInc(ackedSeq); the iteration uses lookup
  // which returns nullptr for absent seqs, so the consumer skips them.
  auto snap = buf.snapshotUnackedRange();
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->firstSeq, 0u); // seqInc(0xFFFFF)
  EXPECT_EQ(snap->lastSeq, 7u);

  // After setting acked_seq = 4, firstSeq = 5.
  buf.setAckedSeq(4u);
  snap = buf.snapshotUnackedRange();
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->firstSeq, 5u);
  EXPECT_EQ(snap->lastSeq, 7u);
}

// snapshotUnackedRange returns the modular-last seq, not the highest
// uint32 key, when the buffer straddles the 0xFFFFF → 0 wrap boundary.
// Without this, REPLAY would iterate firstSeq..0xFFFFF and skip the
// post-wrap entries.
TEST(LlrTxBufferTest, SnapshotRangeAcrossWrap) {
  LlrTxBuffer buf{1024};
  // Simulate tx_seq advancing past 0xFFFFF while acked_seq trails behind.
  buf.setAckedSeq(0xFFFF0u);
  buf.tryAdd(0xFFFF1u, makePacket(50));
  buf.tryAdd(0xFFFFFu, makePacket(50));
  buf.tryAdd(0u, makePacket(50));
  buf.tryAdd(2u, makePacket(50));

  auto snap = buf.snapshotUnackedRange();
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->firstSeq, 0xFFFF1u); // seqInc(0xFFFF0)
  // Modular-last = most recently added = 2. Bug would report 0xFFFFF here.
  EXPECT_EQ(snap->lastSeq, 2u);
}

// isUnacked reflects buffer membership.
TEST(LlrTxBufferTest, IsUnacked) {
  LlrTxBuffer buf{1024};
  buf.tryAdd(10u, makePacket(100));
  EXPECT_TRUE(buf.isUnacked(10u));
  EXPECT_FALSE(buf.isUnacked(11u));
  EXPECT_FALSE(buf.isUnacked(9u));
}

// Peak counters track high-water occupancy and never decrease, even
// after `removeUpTo` drains entries. Backs the
// `txBufferPeak{Packets,Bytes}` thrift fields.
TEST(LlrTxBufferTest, TryAddTracksPeakSeqAndPeakBytes) {
  LlrTxBuffer buf{8192};
  EXPECT_EQ(buf.peakSeq(), 0u);
  EXPECT_EQ(buf.peakBytes(), 0u);

  auto pkt1 = makePacket(100);
  const uint32_t pkt1Size = pkt1->getSize();
  buf.tryAdd(0u, std::move(pkt1));
  EXPECT_EQ(buf.peakSeq(), 1u);
  EXPECT_EQ(buf.peakBytes(), pkt1Size);

  auto pkt2 = makePacket(200);
  const uint32_t pkt2Size = pkt2->getSize();
  buf.tryAdd(1u, std::move(pkt2));
  EXPECT_EQ(buf.peakSeq(), 2u);
  EXPECT_EQ(buf.peakBytes(), pkt1Size + pkt2Size);

  // removeUpTo drains entries but peaks must not decrease.
  buf.removeUpTo(1u);
  EXPECT_EQ(buf.outstandingSeq(), 0u);
  EXPECT_EQ(buf.outstandingData(), 0u);
  EXPECT_EQ(buf.peakSeq(), 2u);
  EXPECT_EQ(buf.peakBytes(), pkt1Size + pkt2Size);
}

// tryAdd that fails (over-capacity) must NOT update peak counters.
TEST(LlrTxBufferTest, FailedTryAddDoesNotAdvancePeak) {
  auto sizingPkt = makePacket(100);
  const uint32_t pktSize = sizingPkt->getSize();
  LlrTxBuffer buf{pktSize + 10};

  ASSERT_TRUE(buf.tryAdd(0u, std::move(sizingPkt)));
  EXPECT_EQ(buf.peakSeq(), 1u);
  EXPECT_EQ(buf.peakBytes(), pktSize);

  // This add would push currentBytes_ over capacity and return false.
  ASSERT_FALSE(buf.tryAdd(1u, makePacket(100)));
  // Peaks unchanged.
  EXPECT_EQ(buf.peakSeq(), 1u);
  EXPECT_EQ(buf.peakBytes(), pktSize);
}

} // namespace
} // namespace packsim::llr
