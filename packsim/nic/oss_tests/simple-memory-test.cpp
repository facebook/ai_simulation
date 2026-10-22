/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstddef>
#include <cstdint>

#include <gtest/gtest.h>

#include "packsim/nic/simple-memory.h"

namespace packsim {

// --------------------------------------------------------------------------
// Construction + accessors
// --------------------------------------------------------------------------

TEST(SimpleMemory, SymmetricConstructorExposesReadParams) {
  SimpleMemory memory(
      /* readWriteBytesPerNs= */ 4.0, /* readWriteLatencyNs= */ 250.0);
  EXPECT_DOUBLE_EQ(memory.getReadBytesPerNs(), 4.0);
  EXPECT_DOUBLE_EQ(memory.getReadLatencyNs(), 250.0);
}

TEST(SimpleMemory, AsymmetricConstructorExposesReadParams) {
  SimpleMemory memory(
      /* readBytesPerNs= */ 4.0,
      /* writeBytesPerNs= */ 2.0,
      /* readLatencyNs= */ 100.0,
      /* writeLatencyNs= */ 200.0);
  // The read accessors must return the read side, not the write side.
  EXPECT_DOUBLE_EQ(memory.getReadBytesPerNs(), 4.0);
  EXPECT_DOUBLE_EQ(memory.getReadLatencyNs(), 100.0);
}

// --------------------------------------------------------------------------
// computePacketFetchTimeNs = latency + size / bandwidth (truncated to ns)
// --------------------------------------------------------------------------

TEST(SimpleMemory, ComputePacketFetchTimeAddsLatencyAndTransfer) {
  SimpleMemory memory(/* readWriteBytesPerNs= */ 2.0,
                      /* readWriteLatencyNs= */ 500.0);
  // 500 latency + 1000 bytes / 2 bytes-per-ns = 500 + 500 = 1000.
  EXPECT_EQ(memory.computePacketFetchTimeNs(1000), 1000u);
}

TEST(SimpleMemory, ComputePacketFetchTimeTruncatesFractionalNanoseconds) {
  SimpleMemory memory(/* readWriteBytesPerNs= */ 3.0,
                      /* readWriteLatencyNs= */ 500.0);
  // 500 + 1000/3 = 833.33..., truncated to 833 by the uint64_t cast.
  EXPECT_EQ(memory.computePacketFetchTimeNs(1000), 833u);
}

// --------------------------------------------------------------------------
// computeMinQueueDepthForLink = ceil(fetchTime / per-packet transmit time)
// --------------------------------------------------------------------------

TEST(SimpleMemory, ComputeMinQueueDepthZeroLinkReturnsOne) {
  SimpleMemory memory(/* readWriteBytesPerNs= */ 1.0,
                      /* readWriteLatencyNs= */ 500.0);
  // A zero-bandwidth link would divide by zero; the guard returns 1 instead.
  EXPECT_EQ(memory.computeMinQueueDepthForLink(/* linkBps= */ 0, 1500), 1u);
}

TEST(SimpleMemory, ComputeMinQueueDepthCountsPacketsPerFetch) {
  // fetchTime(1000) = 9000 latency + 1000 bytes / 1 byte-per-ns = 10000 ns.
  SimpleMemory memory(/* readWriteBytesPerNs= */ 1.0,
                      /* readWriteLatencyNs= */ 9000.0);

  // 8 Gbps: 1000 bytes (8000 bits) transmit in 8000 / 8 = 1000 ns.
  // ceil(10000 / 1000) = 10 packets fit in one fetch window.
  EXPECT_EQ(
      memory.computeMinQueueDepthForLink(/* linkBps= */ 8'000'000'000, 1000),
      10u);

  // 2 Gbps: transmit takes 8000 / 2 = 4000 ns. ceil(10000 / 4000) = 3.
  EXPECT_EQ(
      memory.computeMinQueueDepthForLink(/* linkBps= */ 2'000'000'000, 1000),
      3u);
}

// --------------------------------------------------------------------------
// getReadDelayNs: latency + bandwidth, with back-to-back requests pipelining
// --------------------------------------------------------------------------

TEST(SimpleMemory, ReadDelayIncludesLatencyAndBandwidth) {
  SimpleMemory memory(/* readWriteBytesPerNs= */ 2.0,
                      /* readWriteLatencyNs= */ 500.0);
  // First read from idle memory: latency + 1000 / 2 bandwidth = 500 + 500.
  EXPECT_EQ(
      memory.getReadDelayNs(/* readSize= */ 1000, /* timeNs= */ 0), 1000u);
}

TEST(SimpleMemory, BackToBackReadsQueueAndHideLatency) {
  SimpleMemory memory(/* readWriteBytesPerNs= */ 2.0,
                      /* readWriteLatencyNs= */ 500.0);
  // First read completes at 1000 ns (latency + bandwidth).
  EXPECT_EQ(memory.getReadDelayNs(1000, /* timeNs= */ 0), 1000u);
  // A second read issued at the same time queues behind the first: it only
  // adds the 500 ns bandwidth cost, since the latency is hidden by the
  // in-flight read.
  EXPECT_EQ(memory.getReadDelayNs(1000, /* timeNs= */ 0), 1500u);
}

TEST(SimpleMemory, ReadAfterCompletionPaysLatencyAgain) {
  SimpleMemory memory(/* readWriteBytesPerNs= */ 2.0,
                      /* readWriteLatencyNs= */ 500.0);
  // First read completes at 1000 ns.
  EXPECT_EQ(memory.getReadDelayNs(1000, /* timeNs= */ 0), 1000u);
  // A read issued well after the first completes pays the full latency again:
  // latency + bandwidth = 1000 ns.
  EXPECT_EQ(memory.getReadDelayNs(1000, /* timeNs= */ 2000), 1000u);
}

TEST(SimpleMemory, SuppliedReadLatencyReplacesTheConfiguredOne) {
  SimpleMemory memory(/* readWriteBytesPerNs= */ 2.0,
                      /* readWriteLatencyNs= */ 500.0);
  // A read that brings its own 700 ns latency pays that instead of the
  // configured 500 ns: 700 + 1000 / 2 = 1200 ns.
  EXPECT_EQ(
      memory.getReadDelayNs(
          /* readSize= */ 1000, /* timeNs= */ 0, /* latencyNs= */ 700.0),
      1200u);
}

TEST(SimpleMemory, ShortSuppliedLatencyStillQueuesBehindTheReadInFlight) {
  SimpleMemory memory(/* readWriteBytesPerNs= */ 2.0,
                      /* readWriteLatencyNs= */ 500.0);
  // A 700 ns read completes at 1200 ns.
  EXPECT_EQ(memory.getReadDelayNs(1000, /* timeNs= */ 0, 700.0), 1200u);
  // A 100 ns read issued at the same time would finish at 600 ns on its own,
  // but reads complete in the order they were issued: it adds its 500 ns
  // bandwidth cost behind the first and completes at 1700 ns.
  EXPECT_EQ(memory.getReadDelayNs(1000, /* timeNs= */ 0, 100.0), 1700u);
}

TEST(SimpleMemory, ZeroCostReadGetsMinimumOneNsDelay) {
  // With zero latency and a zero-size read the raw delay would be 0 ns, leaving
  // the completion time equal to the previous one; the model bumps it to a
  // minimum of 1 ns so two packets never become available on the wire at the
  // exact same instant.
  SimpleMemory memory(/* readWriteBytesPerNs= */ 1.0,
                      /* readWriteLatencyNs= */ 0.0);
  EXPECT_EQ(memory.getReadDelayNs(/* readSize= */ 0, /* timeNs= */ 0), 1u);
}

// --------------------------------------------------------------------------
// getWriteDelayNs: mirrors reads but uses the write params + separate state
// --------------------------------------------------------------------------

TEST(SimpleMemory, WriteDelayUsesWriteParameters) {
  // Read side (bw 4, latency 100) is faster than the write side (bw 2, lat
  // 500).
  SimpleMemory memory(
      /* readBytesPerNs= */ 4.0,
      /* writeBytesPerNs= */ 2.0,
      /* readLatencyNs= */ 100.0,
      /* writeLatencyNs= */ 500.0);
  // Uses the write params: 500 latency + 1000 / 2 = 1000 ns (the read params
  // would give 100 + 1000 / 4 = 350 ns).
  EXPECT_EQ(
      memory.getWriteDelayNs(/* writeSize= */ 1000, /* timeNs= */ 0), 1000u);
}

TEST(SimpleMemory, BackToBackWritesQueueAndHideLatency) {
  SimpleMemory memory(/* readWriteBytesPerNs= */ 2.0,
                      /* readWriteLatencyNs= */ 500.0);
  // First write completes at 1000 ns (latency + bandwidth).
  EXPECT_EQ(memory.getWriteDelayNs(1000, /* timeNs= */ 0), 1000u);
  // A second write issued at the same time queues behind the first: it only
  // adds the 500 ns bandwidth cost, since the latency is hidden by the
  // in-flight write.
  EXPECT_EQ(memory.getWriteDelayNs(1000, /* timeNs= */ 0), 1500u);
}

TEST(SimpleMemory, SuppliedWriteLatencyReplacesTheConfiguredOne) {
  SimpleMemory memory(/* readWriteBytesPerNs= */ 2.0,
                      /* readWriteLatencyNs= */ 500.0);
  // A write that brings its own 600 ns latency pays that instead of the
  // configured 500 ns: 600 + 1000 / 2 = 1100 ns.
  EXPECT_EQ(
      memory.getWriteDelayNs(
          /* writeSize= */ 1000, /* timeNs= */ 0, /* latencyNs= */ 600.0),
      1100u);
}

TEST(SimpleMemory, ShortSuppliedLatencyStillQueuesBehindTheWriteInFlight) {
  SimpleMemory memory(/* readWriteBytesPerNs= */ 2.0,
                      /* readWriteLatencyNs= */ 500.0);
  // A 600 ns write completes at 1100 ns.
  EXPECT_EQ(memory.getWriteDelayNs(1000, /* timeNs= */ 0, 600.0), 1100u);
  // A 100 ns write issued at the same time would complete at 600 ns on its
  // own, but writes complete in the order they were issued: it adds its 500 ns
  // bandwidth cost behind the first and completes at 1600 ns.
  EXPECT_EQ(memory.getWriteDelayNs(1000, /* timeNs= */ 0, 100.0), 1600u);
}

TEST(SimpleMemory, ReadAndWriteDelaysTrackStateIndependently) {
  SimpleMemory memory(/* readWriteBytesPerNs= */ 2.0,
                      /* readWriteLatencyNs= */ 500.0);
  // Issue a read, then a write at the same instant. The write must be treated
  // as fresh (1000 ns) rather than queued behind the read, proving read and
  // write completion times are tracked separately.
  EXPECT_EQ(memory.getReadDelayNs(1000, /* timeNs= */ 0), 1000u);
  EXPECT_EQ(memory.getWriteDelayNs(1000, /* timeNs= */ 0), 1000u);
}

} // namespace packsim
