/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/llr/llr.h"

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include <folly/portability/GFlags.h>
#include <gtest/gtest.h>

#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/link-layer/llr/llr-types.h"
#include "packsim/link-layer/llr/oss_tests/llr-test-helpers.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim::llr {
namespace {

// Byte cap large enough that neither `outstanding_data_max` nor
// `txBufferMaxBytes` can fire: ~1 GiB dwarfs the few-KiB frames these
// tests push, so the gate under test is the only one that can trip.
constexpr uint64_t kUnboundedByteCap = 1ull << 30;

// Exercises `Llr::admissionDecision` / `Llr::admit` — the per-state
// admission rules of UEC §5.1.5 p. 473. `llr-tx-test` covers LLR_OFF,
// INIT+BLOCK and INIT+DISCARD; this file covers the remaining arms:
// INIT+BEST_EFFORT, all three FLUSH behaviors, the REPLAY refusal, and
// the three independent ADVANCE capacity gates (`outstanding_seq_max`,
// `outstanding_data_max`, and the replay buffer's own byte capacity).
class LlrAdmissionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Pin pdes_threads=4 — see `eth-port-test.cpp` PinPdesThreads
    // for the rationale (default thread count, ThreadSanitizer timeout).
    gflags::SetCommandLineOption("pdes_threads", "4");
    schedulerOpt_.emplace();
    ctxOpt_.emplace(&schedulerOpt_.value(), "llr-admission-test", 0, nullptr);
  }

  void TearDown() override {
    ctxOpt_.reset();
    schedulerOpt_.reset();
  }

  Scheduler& scheduler() {
    return schedulerOpt_.value();
  }

  SimContext& ctx() {
    return ctxOpt_.value();
  }

  folly::gflags::FlagSaver flagSaver_;
  std::optional<Scheduler> schedulerOpt_;
  std::optional<SimContext> ctxOpt_;

  std::vector<PacketPtr> sentData_;
  test::TestTxPacketSource source_;

  LlrConfig defaultConfig() const {
    LlrConfig c;
    // Long enough that no spec timer fires inside a test window unless
    // the test deliberately advances past it.
    c.replayTimerMax = Time::fromMs(1);
    c.dataAgeTimerMax = Time::fromMs(1);
    c.pcsLostStatusTimerMax = Time::fromUs(10);
    c.replayCtMax = kReplayCtMaxNoLimit;
    c.initBehavior = FrameBehavior::BLOCK;
    c.flushBehavior = FrameBehavior::BLOCK;
    c.reInitOnDiscard = true;
    // No background LLR_ACK emission — admission assertions must not
    // race a periodic CtlOS tick.
    c.enableGlobalTimer = false;
    return c;
  }

  PacketPtr makePacket(uint32_t payloadSize) {
    return createPacket<PacketType::IPV6>(payloadSize);
  }

  // On-the-wire byte count the admission gates account for. Probed from
  // a real packet so the byte-cap tests stay correct if the packet
  // header size changes.
  uint32_t frameBytes(uint32_t payloadSize) {
    return makePacket(payloadSize)->getSize();
  }

  bool txAndCapture(Llr& llr, PacketPtr pkt) {
    return test::txAndCapture(llr, source_, std::move(pkt), sentData_);
  }

  // LLR_OFF → INIT (llr_mode_remote ON) → ADVANCE (matching INIT_ECHO).
  void driveToAdvance(Llr& llr) {
    llr.setLlrModeRemote(/*on=*/true);
    scheduler().run(Time::fromUs(1));
    ASSERT_EQ(llr.txState(), TxState::INIT);
    llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, /*seq=*/0u, /*initData=*/0);
    scheduler().run(Time::fromUs(1));
    ASSERT_EQ(llr.txState(), TxState::ADVANCE);
  }

  // ADVANCE → FLUSH via pcs_lost_status_timer expiry (needs no buffered
  // frame, unlike the data_age path). Requires reInitOnDiscard=false so
  // `txRunFlush` parks on `managementReset` instead of bouncing to INIT
  // — FLUSH-state admission is only observable while parked.
  void driveToParkedFlush(Llr& llr) {
    ASSERT_EQ(llr.txState(), TxState::ADVANCE);
    llr.onPcsStatusChange(/*up=*/false);
    scheduler().run(Time::fromUs(11)); // past pcsLostStatusTimerMax=10us
    ASSERT_EQ(llr.txState(), TxState::FLUSH);
  }
};

// INIT + BEST_EFFORT: the frame is transmitted unprotected rather than
// blocked or dropped — no seq is consumed and nothing lands in the
// replay buffer (UEC §5.1.5 p. 473 "pass through as LLR_INELIGIBLE").
TEST_F(LlrAdmissionTest, InitBestEffortPassesThroughWithoutConsumingSeq) {
  auto config = defaultConfig();
  config.initBehavior = FrameBehavior::BEST_EFFORT;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  llr.setTxPacketSource(&source_);
  llr.start();
  llr.setLlrModeRemote(/*on=*/true);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::INIT);

  EXPECT_EQ(
      llr.admissionDecision(frameBytes(100)), AdmissionResult::PASSTHROUGH_TX);

  source_.enqueue(makePacket(100));
  auto pulled = llr.pullNextNewFrame();
  ASSERT_TRUE(pulled.has_value());
  EXPECT_EQ(pulled->type, FrameType::LLR_INELIGIBLE);
  EXPECT_EQ(pulled->seq, 0u);
  EXPECT_EQ(llr.stats().txPassthrough, 1u);
  EXPECT_EQ(llr.stats().txOk, 0u);
  EXPECT_EQ(llr.txSeq(), 0u);
  EXPECT_EQ(llr.outstandingSeq(), 0u);

  llr.shutdown();
  scheduler().run();
}

// FLUSH + BLOCK: frame stays in the upstream queue for a later retry.
TEST_F(LlrAdmissionTest, FlushBlockLeavesPacketQueued) {
  auto config = defaultConfig();
  config.flushBehavior = FrameBehavior::BLOCK;
  config.reInitOnDiscard = false;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  llr.setTxPacketSource(&source_);
  llr.start();
  ASSERT_NO_FATAL_FAILURE(driveToAdvance(llr));
  ASSERT_NO_FATAL_FAILURE(driveToParkedFlush(llr));

  EXPECT_EQ(llr.admissionDecision(frameBytes(100)), AdmissionResult::BLOCK);
  EXPECT_FALSE(txAndCapture(llr, makePacket(100)));
  EXPECT_EQ(source_.size(), 1u);
  EXPECT_EQ(llr.stats().txDiscard, 0u);
  EXPECT_EQ(llr.stats().txPassthrough, 0u);

  llr.shutdown();
  scheduler().run();
}

// FLUSH + DISCARD: frame is consumed off the upstream queue and counted
// in tx_discards (UEC §5.1.11 Tbl 5-13).
TEST_F(LlrAdmissionTest, FlushDiscardDropsPacketAndCounts) {
  auto config = defaultConfig();
  config.flushBehavior = FrameBehavior::DISCARD;
  config.reInitOnDiscard = false;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  llr.setTxPacketSource(&source_);
  llr.start();
  ASSERT_NO_FATAL_FAILURE(driveToAdvance(llr));
  ASSERT_NO_FATAL_FAILURE(driveToParkedFlush(llr));

  EXPECT_EQ(llr.admissionDecision(frameBytes(100)), AdmissionResult::DROP);
  EXPECT_FALSE(txAndCapture(llr, makePacket(100)));
  EXPECT_EQ(source_.size(), 0u);
  EXPECT_EQ(llr.stats().txDiscard, 1u);
  EXPECT_EQ(sentData_.size(), 0u);

  llr.shutdown();
  scheduler().run();
}

// FLUSH + BEST_EFFORT: transmitted unprotected. The replay buffer was
// emptied by `txRunFlush`'s discardAll, and a pass-through must not
// refill it.
TEST_F(LlrAdmissionTest, FlushBestEffortPassesThroughWithEmptyBuffer) {
  auto config = defaultConfig();
  config.flushBehavior = FrameBehavior::BEST_EFFORT;
  config.reInitOnDiscard = false;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  llr.setTxPacketSource(&source_);
  llr.start();
  ASSERT_NO_FATAL_FAILURE(driveToAdvance(llr));

  // One admitted frame before FLUSH, so discardAll has something to drop.
  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  ASSERT_EQ(llr.outstandingSeq(), 1u);
  ASSERT_NO_FATAL_FAILURE(driveToParkedFlush(llr));
  ASSERT_EQ(llr.outstandingSeq(), 0u);

  EXPECT_EQ(
      llr.admissionDecision(frameBytes(100)), AdmissionResult::PASSTHROUGH_TX);
  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  EXPECT_EQ(llr.stats().txPassthrough, 1u);
  EXPECT_EQ(llr.outstandingSeq(), 0u);
  EXPECT_EQ(llr.outstandingData(), 0u);

  llr.shutdown();
  scheduler().run();
}

// REPLAY refuses new frames unconditionally (spec p. 473: "TX LLR will
// not accept a new LLR-desired frame ... if the state machine is not in
// the ADVANCE state") — even though the buffer is nowhere near its caps.
TEST_F(LlrAdmissionTest, ReplayStateBlocksNewFrameDespiteSpareCapacity) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  llr.setTxPacketSource(&source_);
  llr.start();
  ASSERT_NO_FATAL_FAILURE(driveToAdvance(llr));

  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  // ack_nack_seq = next_rx_seq - 1 = seqDec(0): valid, but does not
  // advance acked_seq, so the frame stays buffered for REPLAY.
  llr.rxCtlos(LlrCtlosType::LLR_NACK, /*seq=*/0xFFFFFu, /*initData=*/0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::REPLAY);

  const uint32_t seqBefore = llr.txSeq();
  EXPECT_EQ(llr.admissionDecision(frameBytes(100)), AdmissionResult::BLOCK);
  EXPECT_FALSE(txAndCapture(llr, makePacket(100)));
  EXPECT_EQ(source_.size(), 1u);
  EXPECT_EQ(llr.txSeq(), seqBefore);
  EXPECT_EQ(llr.outstandingSeq(), 1u);

  test::pullAllReplayFrames(llr, sentData_);
  llr.notifyReplayBatchComplete();
  scheduler().run(Time::fromUs(1));
  llr.shutdown();
  scheduler().run();
}

// `outstanding_seq_max` gate: admission is refused once outstanding_seq
// REACHES the cap (the comparison is `>=`), independently of the byte
// caps, which are left effectively unbounded here.
TEST_F(LlrAdmissionTest, OutstandingSeqMaxBlocksOnReachingCap) {
  auto config = defaultConfig();
  config.outstandingSeqMax = 2;
  config.outstandingDataMax = kUnboundedByteCap;
  config.txBufferMaxBytes = kUnboundedByteCap;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  llr.setTxPacketSource(&source_);
  llr.start();
  ASSERT_NO_FATAL_FAILURE(driveToAdvance(llr));

  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  ASSERT_EQ(llr.outstandingSeq(), 1u);
  // One below the cap still admits.
  EXPECT_EQ(
      llr.admissionDecision(frameBytes(100)), AdmissionResult::ADMIT_AND_TX);
  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  ASSERT_EQ(llr.outstandingSeq(), 2u);

  // At the cap: blocked even for a zero-byte frame, proving the seq gate
  // is what fired.
  EXPECT_EQ(llr.admissionDecision(0), AdmissionResult::BLOCK);
  EXPECT_FALSE(txAndCapture(llr, makePacket(100)));
  EXPECT_EQ(llr.txSeq(), 2u);
  EXPECT_EQ(llr.stats().txOk, 2u);

  llr.shutdown();
  scheduler().run();
}

// An LLR_ACK that advances acked_seq frees a slot, so a frame blocked by
// `outstanding_seq_max` is admitted on the next attempt with the next seq.
TEST_F(LlrAdmissionTest, AckFreeingSeqSlotReopensAdmission) {
  auto config = defaultConfig();
  config.outstandingSeqMax = 2;
  config.outstandingDataMax = kUnboundedByteCap;
  config.txBufferMaxBytes = kUnboundedByteCap;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  llr.setTxPacketSource(&source_);
  llr.start();
  ASSERT_NO_FATAL_FAILURE(driveToAdvance(llr));

  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  EXPECT_FALSE(txAndCapture(llr, makePacket(100)));
  ASSERT_EQ(source_.size(), 1u); // the blocked packet is still queued

  // ACK seq 0 frees the first buffered frame.
  llr.rxCtlos(LlrCtlosType::LLR_ACK, /*seq=*/0u, /*initData=*/0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.outstandingSeq(), 1u);

  auto pulled = llr.pullNextNewFrame();
  ASSERT_TRUE(pulled.has_value());
  EXPECT_EQ(pulled->type, FrameType::LLR_ELIGIBLE);
  EXPECT_EQ(pulled->seq, 2u);
  EXPECT_EQ(llr.outstandingSeq(), 2u);
  EXPECT_EQ(source_.size(), 0u);

  llr.shutdown();
  scheduler().run();
}

// `outstanding_data_max` gate: a frame that makes in-flight bytes exactly
// equal to the cap is admitted (the comparison is `>`, not `>=`); the
// next byte over is refused. The buffer's own capacity is unbounded here
// so only the spec admission limit can fire.
TEST_F(LlrAdmissionTest, OutstandingDataMaxAdmitsExactFitAndBlocksOverflow) {
  const uint32_t bytes = frameBytes(100);
  auto config = defaultConfig();
  config.outstandingDataMax = 2 * bytes;
  config.txBufferMaxBytes = kUnboundedByteCap;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  llr.setTxPacketSource(&source_);
  llr.start();
  ASSERT_NO_FATAL_FAILURE(driveToAdvance(llr));

  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  ASSERT_EQ(llr.outstandingData(), bytes);
  // Exactly filling the cap is allowed.
  EXPECT_EQ(llr.admissionDecision(bytes), AdmissionResult::ADMIT_AND_TX);
  // One byte more is not.
  EXPECT_EQ(llr.admissionDecision(bytes + 1), AdmissionResult::BLOCK);

  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  ASSERT_EQ(llr.outstandingData(), 2ull * bytes);
  EXPECT_EQ(llr.admissionDecision(1), AdmissionResult::BLOCK);
  EXPECT_FALSE(txAndCapture(llr, makePacket(100)));
  EXPECT_EQ(llr.stats().txOk, 2u);

  llr.shutdown();
  scheduler().run();
}

// The gate uses the CANDIDATE frame's size, not just what is already
// buffered: with an empty buffer an oversized frame is still refused.
TEST_F(LlrAdmissionTest, OversizedFrameBlockedAgainstEmptyBuffer) {
  const uint32_t bytes = frameBytes(100);
  auto config = defaultConfig();
  config.outstandingDataMax = 2 * bytes;
  config.txBufferMaxBytes = kUnboundedByteCap;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  llr.setTxPacketSource(&source_);
  llr.start();
  ASSERT_NO_FATAL_FAILURE(driveToAdvance(llr));

  ASSERT_EQ(llr.outstandingData(), 0u);
  EXPECT_EQ(llr.admissionDecision(2 * bytes + 1), AdmissionResult::BLOCK);

  llr.shutdown();
  scheduler().run();
}

// `admissionDecision` is a pure query: repeated calls on both arms leave
// the seq counter, the in-flight byte count and the stats untouched, so
// the other tests may probe it freely before calling `admit`.
TEST_F(LlrAdmissionTest, AdmissionDecisionIsSideEffectFree) {
  const uint32_t bytes = frameBytes(100);
  auto config = defaultConfig();
  config.outstandingDataMax = 2 * bytes;
  config.txBufferMaxBytes = kUnboundedByteCap;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  llr.setTxPacketSource(&source_);
  llr.start();
  ASSERT_NO_FATAL_FAILURE(driveToAdvance(llr));

  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(llr.admissionDecision(bytes), AdmissionResult::ADMIT_AND_TX);
    EXPECT_EQ(llr.admissionDecision(2 * bytes + 1), AdmissionResult::BLOCK);
  }
  EXPECT_EQ(llr.outstandingData(), 0u);
  EXPECT_EQ(llr.outstandingSeq(), 0u);
  EXPECT_EQ(llr.txSeq(), 0u);
  EXPECT_EQ(llr.stats().txOk, 0u);
  EXPECT_EQ(llr.stats().txPassthrough, 0u);
  EXPECT_EQ(llr.stats().txDiscard, 0u);

  llr.shutdown();
  scheduler().run();
}

// The replay-buffer capacity is a SEPARATE gate from the spec admission
// limit: with `outstanding_data_max` unbounded, `txBufferMaxBytes` alone
// must still block.
TEST_F(LlrAdmissionTest, TxBufferMaxBytesBlocksIndependentlyOfDataMax) {
  const uint32_t bytes = frameBytes(100);
  auto config = defaultConfig();
  config.outstandingDataMax = kUnboundedByteCap;
  config.txBufferMaxBytes = 2 * bytes;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  llr.setTxPacketSource(&source_);
  llr.start();
  ASSERT_NO_FATAL_FAILURE(driveToAdvance(llr));

  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  ASSERT_EQ(llr.outstandingData(), 2ull * bytes);

  EXPECT_EQ(llr.admissionDecision(1), AdmissionResult::BLOCK);
  EXPECT_FALSE(txAndCapture(llr, makePacket(100)));
  EXPECT_EQ(llr.stats().txOk, 2u);
  EXPECT_EQ(source_.size(), 1u);

  llr.shutdown();
  scheduler().run();
}

// `admit()` assigns consecutive seqs and accumulates bytes across
// differently-sized frames — the accounting the byte gates read.
TEST_F(LlrAdmissionTest, AdmitAssignsConsecutiveSeqsAndAccumulatesBytes) {
  auto config = defaultConfig();
  config.outstandingSeqMax = 8;
  config.outstandingDataMax = kUnboundedByteCap;
  config.txBufferMaxBytes = kUnboundedByteCap;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  llr.setTxPacketSource(&source_);
  llr.start();
  ASSERT_NO_FATAL_FAILURE(driveToAdvance(llr));

  const uint64_t expectedBytes =
      frameBytes(100) + frameBytes(200) + frameBytes(300);
  for (const uint32_t payload : {100u, 200u, 300u}) {
    source_.enqueue(makePacket(payload));
    auto pulled = llr.pullNextNewFrame();
    ASSERT_TRUE(pulled.has_value());
    EXPECT_EQ(pulled->type, FrameType::LLR_ELIGIBLE);
    llr.notifyNewFrameTransmitted();
  }

  EXPECT_EQ(llr.txSeq(), 3u);
  EXPECT_EQ(llr.outstandingSeq(), 3u);
  EXPECT_EQ(llr.outstandingData(), expectedBytes);
  EXPECT_EQ(llr.stats().txOk, 3u);

  llr.shutdown();
  scheduler().run();
}

} // namespace
} // namespace packsim::llr
