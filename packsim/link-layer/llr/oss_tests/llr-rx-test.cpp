/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/llr/llr.h"

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

class LlrRxTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // MUST set the gflag BEFORE the engine reads it. `Scheduler`'s
    // default constructor already built one from the default
    // `FLAGS_pdes_threads=0` during member init; rebuild here so the test
    // runs on 4 threads whatever that default resolves to. With a large
    // thread count the 1ns-lookahead PDES overhead makes every sim step
    // glacial.
    gflags::SetCommandLineOption("pdes_threads", "4");
    scheduler_.buildNewEngine();
  }

  folly::gflags::FlagSaver flagSaver_;
  Scheduler scheduler_;
  SimContext ctx_{&scheduler_, "llr-rx-test", 0, nullptr};

  std::vector<PacketPtr> sentData_;
  std::vector<LlrCtlosPayload> sentCtlos_;
  std::vector<PacketPtr> deliveredFrames_;
  test::TestTxPacketSource source_;

  // Mirrors `EthernetPort::deliverData`: `Llr` filters and the caller
  // delivers, so these tests exercise the same split production does.
  void rxAndDeliver(
      Llr& llr,
      PacketPtr pkt,
      const FrameType frameType,
      const FcsStatus fcsStatus,
      const uint32_t frameSeq) {
    if (llr.rxFrame(pkt, frameType, fcsStatus, frameSeq) ==
        Llr::RxDisposition::DELIVER) {
      deliveredFrames_.emplace_back(std::move(pkt));
    }
  }

  // Wire the test's `TestTxPacketSource` into `Llr` so
  // `pullNextNewFrame()` sees the test-enqueued packets. Must be
  // called BEFORE `start()`.
  void wireSource(Llr& llr) {
    llr.setTxPacketSource(&source_);
  }

  // Install a CtlOS-capture hook on the Llr's LlrCtlosSlots. Drains
  // every post into `sentCtlos_`. Per-type stat counters and the
  // INIT_ECHO AN-SM transition fire at post time inside `Llr`, so this
  // helper needs no wire-side notify hook to match production.
  void bindCtlosCapture(Llr& llr) {
    llr.ctlosSlots().setOnPostCallback([this, p = &llr]() {
      while (auto f = p->ctlosSlots().popHighest()) {
        sentCtlos_.emplace_back(*f);
      }
    });
  }

  LlrConfig defaultConfig() const {
    LlrConfig c;
    c.replayTimerMax = Time::fromUs(10);
    c.pcsLostStatusTimerMax = Time::fromUs(10);
    c.dataAgeTimerMax = Time::fromUs(10);
    c.replayCtMax = 3;
    c.initBehavior = FrameBehavior::BLOCK;
    c.flushBehavior = FrameBehavior::BLOCK;
    c.reInitOnDiscard = true;
    return c;
  }

  PacketPtr makePacket(uint32_t size) {
    return createPacket<PacketType::IPV6>(size);
  }

  // Convenience: enqueue + pull one frame through the pull API.
  bool txAndCapture(Llr& llr, PacketPtr pkt) {
    return test::txAndCapture(llr, source_, std::move(pkt), sentData_);
  }

  // Convenience: get to ADVANCE state quickly.
  void bringToAdvance(Llr& llr) {
    llr.start();
    llr.setLlrModeLocal(true);
    llr.setLlrModeRemote(true);
    scheduler_.run(Time::fromUs(1));
    ASSERT_EQ(llr.txState(), TxState::INIT);
    llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, /*seq=*/0u, 0);
    scheduler_.run(Time::fromUs(1));
    ASSERT_EQ(llr.txState(), TxState::ADVANCE);
  }
};

// LLR_INELIGIBLE frame is delivered to MAC client unconditionally.
TEST_F(LlrRxTest, IneligibleFrameDelivered) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  scheduler_.run(Time::fromUs(1));

  rxAndDeliver(
      llr,
      makePacket(100),
      FrameType::LLR_INELIGIBLE,
      FcsStatus::GOOD,
      /*frameSeq=*/0u);
  EXPECT_EQ(deliveredFrames_.size(), 1u);
  EXPECT_EQ(llr.stats().rxOk, 1u);
  llr.shutdown();
  scheduler_.run();
}

// LLR_ELIGIBLE frame before next_rx_seq_vld is dropped silently.
TEST_F(LlrRxTest, EligibleFrameWithoutValidNextRxSeqDropped) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  scheduler_.run(Time::fromUs(1));
  // Have not received any LLR_INIT, so nextRxSeqVld is false.
  rxAndDeliver(
      llr,
      makePacket(100),
      FrameType::LLR_ELIGIBLE,
      FcsStatus::GOOD,
      /*frameSeq=*/0u);
  EXPECT_EQ(deliveredFrames_.size(), 0u);
  llr.shutdown();
  scheduler_.run();
}

// LLR_INIT received with llr_mode_local=ON sets next_rx_seq, marks valid,
// posts INIT_ECHO. Subsequent LLR_ELIGIBLE frames are processed.
TEST_F(LlrRxTest, LlrInitSetsNextRxSeqAndEchoes) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeLocal(true);
  scheduler_.run(Time::fromUs(1));

  llr.rxCtlos(LlrCtlosType::LLR_INIT, /*seq=*/42u, /*initData=*/0xCAFEu);
  scheduler_.run(Time::fromUs(1));

  EXPECT_EQ(llr.stats().rxInitCtlos, 1u);
  // INIT_ECHO should have been posted onto ctlosSlots by `rxCtlos`. The
  // ackCtlosLoop will then emit periodic LLR_ACKs (since SEND_ACKS state),
  // so we look for the INIT_ECHO specifically.
  ASSERT_FALSE(sentCtlos_.empty());
  EXPECT_EQ(sentCtlos_[0].type, LlrCtlosType::LLR_INIT_ECHO);
  EXPECT_EQ(sentCtlos_[0].seq, 42u);
  EXPECT_EQ(sentCtlos_[0].initData, 0xCAFEu);

  // Now an LLR-eligible frame at seq 42 should be expected.
  rxAndDeliver(
      llr, makePacket(100), FrameType::LLR_ELIGIBLE, FcsStatus::GOOD, 42u);
  EXPECT_EQ(deliveredFrames_.size(), 1u);
  EXPECT_EQ(llr.stats().rxExpectedSeqGood, 1u);
  llr.shutdown();
  scheduler_.run();
}

// LLR_INIT received with llr_mode_local=OFF is ignored (no INIT_ECHO).
TEST_F(LlrRxTest, LlrInitIgnoredWithModeLocalOff) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeLocal(false);
  scheduler_.run(Time::fromUs(1));

  llr.rxCtlos(LlrCtlosType::LLR_INIT, /*seq=*/42u, 0);
  scheduler_.run(Time::fromUs(1));
  EXPECT_EQ(llr.stats().rxInitCtlos, 1u); // counter still increments
  EXPECT_EQ(sentCtlos_.size(), 0u); // no INIT_ECHO
  llr.shutdown();
  scheduler_.run();
}

// Expected frame with GOOD FCS: delivered, next_rx_seq incremented.
TEST_F(LlrRxTest, ExpectedFrameGoodDeliveredAndAdvances) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeLocal(true);
  scheduler_.run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT, /*seq=*/0u, 0);
  scheduler_.run(Time::fromUs(1));

  rxAndDeliver(
      llr, makePacket(100), FrameType::LLR_ELIGIBLE, FcsStatus::GOOD, 0u);
  EXPECT_EQ(deliveredFrames_.size(), 1u);
  EXPECT_EQ(llr.stats().rxExpectedSeqGood, 1u);

  // Next frame at seq 1 also expected; at seq 2 would be missing.
  rxAndDeliver(
      llr, makePacket(100), FrameType::LLR_ELIGIBLE, FcsStatus::GOOD, 1u);
  EXPECT_EQ(deliveredFrames_.size(), 2u);
  llr.shutdown();
  scheduler_.run();
}

// Expected frame with POISONED FCS: delivered (per spec p. 478), next_rx_seq
// advances. Counted in rxPoisoned + rxExpectedSeqPoisoned.
TEST_F(LlrRxTest, ExpectedFramePoisonedDelivered) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeLocal(true);
  scheduler_.run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT, 0u, 0);
  scheduler_.run(Time::fromUs(1));

  rxAndDeliver(
      llr, makePacket(100), FrameType::LLR_ELIGIBLE, FcsStatus::POISONED, 0u);
  EXPECT_EQ(deliveredFrames_.size(), 1u);
  EXPECT_EQ(llr.stats().rxPoisoned, 1u);
  EXPECT_EQ(llr.stats().rxExpectedSeqPoisoned, 1u);
  llr.shutdown();
  scheduler_.run();
}

// Expected frame with BAD FCS: NOT delivered, next_rx_seq does NOT advance,
// expected_frame_bad pulse drives ACK/NACK SM into SEND_NACK.
TEST_F(LlrRxTest, ExpectedFrameBadDoesNotDeliverPulsesNack) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeLocal(true);
  scheduler_.run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT, 0u, 0);
  scheduler_.run(Time::fromUs(1));
  // Wait for ACK/NACK SM to enter SEND_ACKS (initEchoSent fires inline from
  // `rxCtlos` LLR_INIT after INIT_ECHO is posted).
  scheduler_.run(Time::fromUs(1));
  ASSERT_EQ(llr.ackNackState(), AckNackState::SEND_ACKS);

  rxAndDeliver(
      llr, makePacket(100), FrameType::LLR_ELIGIBLE, FcsStatus::BAD, 0u);
  EXPECT_EQ(deliveredFrames_.size(), 0u);
  EXPECT_EQ(llr.stats().rxExpectedSeqBad, 1u);
  scheduler_.run(Time::fromUs(1));
  // ACK/NACK SM should be in NACK_SENT (transitioned through SEND_NACK).
  EXPECT_EQ(llr.ackNackState(), AckNackState::NACK_SENT);
  // An LLR_NACK should have been emitted on the wire.
  bool foundNack = false;
  for (const auto& f : sentCtlos_) {
    if (f.type == LlrCtlosType::LLR_NACK) {
      foundNack = true;
      EXPECT_EQ(f.seq, 0xFFFFFu); // seqDec(nextRxSeq=0) wraps
    }
  }
  EXPECT_TRUE(foundNack);
  llr.shutdown();
  scheduler_.run();
}

// Missing frame (seq ahead of next_rx_seq): pulses missing_frame, drops
// silently, no delivery. ACK/NACK SM enters SEND_NACK.
TEST_F(LlrRxTest, MissingFrameDropsAndPulsesNack) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeLocal(true);
  scheduler_.run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT, 0u, 0);
  scheduler_.run(Time::fromUs(1));

  // Inject seq 5 when expecting seq 0.
  rxAndDeliver(
      llr, makePacket(100), FrameType::LLR_ELIGIBLE, FcsStatus::GOOD, 5u);
  EXPECT_EQ(deliveredFrames_.size(), 0u);
  EXPECT_EQ(llr.stats().rxMissingSeq, 1u);
  scheduler_.run(Time::fromUs(1));
  EXPECT_EQ(llr.ackNackState(), AckNackState::NACK_SENT);
  llr.shutdown();
  scheduler_.run();
}

// Duplicate frame (seq behind next_rx_seq): dropped, NO NACK fired.
// Spec interpretation: duplicates are silently consumed.
TEST_F(LlrRxTest, DuplicateFrameDoesNotTriggerNack) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeLocal(true);
  scheduler_.run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT, /*seq=*/10u, 0);
  scheduler_.run(Time::fromUs(1));

  // Inject seq 5 (behind) — duplicate.
  rxAndDeliver(
      llr, makePacket(100), FrameType::LLR_ELIGIBLE, FcsStatus::GOOD, 5u);
  EXPECT_EQ(deliveredFrames_.size(), 0u);
  EXPECT_EQ(llr.stats().rxDuplicateSeq, 1u);
  scheduler_.run(Time::fromUs(1));
  // ACK/NACK SM remains in SEND_ACKS (no NACK fired).
  EXPECT_EQ(llr.ackNackState(), AckNackState::SEND_ACKS);
  llr.shutdown();
  scheduler_.run();
}

// LLR_ACK with valid seq advances acked_seq, frees buffer entries.
TEST_F(LlrRxTest, LlrAckAdvancesBufferAndAckedSeq) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  bringToAdvance(llr);

  // Pull three frames; tx_seq=3, buffer has seq 0,1,2.
  txAndCapture(llr, makePacket(100));
  txAndCapture(llr, makePacket(100));
  txAndCapture(llr, makePacket(100));
  scheduler_.run(Time::fromUs(1));
  ASSERT_EQ(llr.txSeq(), 3u);

  // ACK seq 1 (valid): acks frames 0 and 1; buffer keeps frame 2.
  llr.rxCtlos(LlrCtlosType::LLR_ACK, /*seq=*/1u, 0);
  scheduler_.run(Time::fromUs(1));
  EXPECT_EQ(llr.stats().rxAckCtlos, 1u);
  EXPECT_EQ(llr.stats().rxAckNackSeqError, 0u);
  llr.shutdown();
  scheduler_.run();
}

// Spec §5.1.8: "If ack_nack_seq_ok is TRUE and ack_nack_seq is NOT
// equal to acked_seq then the LLR_ACK/LLR_NACK is acknowledging some
// frames in the replay buffer." A duplicate LLR_ACK (ack_nack_seq_ok
// TRUE but equal to acked_seq) must NOT advance the buffer (no-op).
TEST_F(LlrRxTest, DuplicateLlrAckDoesNotAdvanceBuffer) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  bringToAdvance(llr);

  txAndCapture(llr, makePacket(100));
  txAndCapture(llr, makePacket(100));
  scheduler_.run(Time::fromUs(1));
  ASSERT_EQ(llr.txSeq(), 2u);

  // Advance acked_seq to 0 via first ACK.
  llr.rxCtlos(LlrCtlosType::LLR_ACK, /*seq=*/0u, 0);
  scheduler_.run(Time::fromUs(1));
  ASSERT_EQ(llr.stats().rxAckCtlos, 1u);

  // Duplicate ACK with same seq — ack_nack_seq_ok TRUE but no advance.
  llr.rxCtlos(LlrCtlosType::LLR_ACK, /*seq=*/0u, 0);
  scheduler_.run(Time::fromUs(1));
  EXPECT_EQ(llr.stats().rxAckCtlos, 2u); // counter still increments
  EXPECT_EQ(llr.stats().rxAckNackSeqError, 0u); // ack_nack_seq_ok TRUE
  // Buffer should still hold seq 1 (frame at index 0 was acked; frame
  // at index 1 was never acked). No spurious removal from the
  // duplicate ACK.
  llr.shutdown();
  scheduler_.run();
}

// LLR_ACK with invalid seq increments rx_ack_nack_seq_error and drops.
TEST_F(LlrRxTest, LlrAckInvalidSeqCounted) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  bringToAdvance(llr);
  txAndCapture(llr, makePacket(100));
  scheduler_.run(Time::fromUs(1));
  ASSERT_EQ(llr.txSeq(), 1u);

  // ACK seq 99 (way ahead of tx_seq=1) — invalid.
  llr.rxCtlos(LlrCtlosType::LLR_ACK, /*seq=*/99u, 0);
  scheduler_.run(Time::fromUs(1));
  EXPECT_EQ(llr.stats().rxAckNackSeqError, 1u);
  llr.shutdown();
  scheduler_.run();
}

// LLR_NACK with valid seq sets nack_received, broadcasts nackReceived,
// and triggers ADVANCE → REPLAY.
TEST_F(LlrRxTest, LlrNackTriggersReplay) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  bringToAdvance(llr);
  txAndCapture(llr, makePacket(100));
  txAndCapture(llr, makePacket(100));
  scheduler_.run(Time::fromUs(1));

  // NACK seq 0 (valid): valid because tx_seq=2 and acked_seq=0xFFFFF.
  llr.rxCtlos(LlrCtlosType::LLR_NACK, /*seq=*/0u, 0);
  scheduler_.run(Time::fromUs(1));
  EXPECT_EQ(llr.stats().rxNackCtlos, 1u);
  // Drain the REPLAY snapshot and signal batch completion so the TX SM
  // can transition REPLAY → ADVANCE (spec §5.1.5 `replay_done`).
  test::pullAllReplayFrames(llr, sentData_);
  llr.notifyReplayBatchComplete();
  scheduler_.run(Time::fromUs(1));
  EXPECT_EQ(llr.txState(), TxState::ADVANCE);
  EXPECT_GE(llr.stats().txReplay, 1u);
  llr.shutdown();
  scheduler_.run();
}

// LLR_NACK that advances acked_seq frees buffer entries (per spec p. 476).
TEST_F(LlrRxTest, LlrNackAdvancingAckedSeqFreesBuffer) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  bringToAdvance(llr);
  txAndCapture(llr, makePacket(100)); // seq 0
  txAndCapture(llr, makePacket(100)); // seq 1
  txAndCapture(llr, makePacket(100)); // seq 2
  scheduler_.run(Time::fromUs(1));

  // NACK seq 1: implicitly ACKs seq 0 and 1, then NACKs seq 2 onward.
  llr.rxCtlos(LlrCtlosType::LLR_NACK, /*seq=*/1u, 0);
  scheduler_.run(Time::fromUs(1));
  EXPECT_EQ(llr.stats().rxNackCtlos, 1u);
  llr.shutdown();
  scheduler_.run();
}

// ackCtlosLoop emits periodic LLR_ACKs while in SEND_ACKS state.
TEST_F(LlrRxTest, AckCtlosLoopEmitsPeriodically) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeLocal(true);
  scheduler_.run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT, /*seq=*/5u, 0);
  scheduler_.run(Time::fromUs(1));
  // The AN-SM is already in SEND_ACKS: `rxCtlos(LLR_INIT)` enters it inline
  // after posting INIT_ECHO.
  scheduler_.run(Time::fromUs(1));
  ASSERT_EQ(llr.ackNackState(), AckNackState::SEND_ACKS);

  // Receive the expected frame (seq=5) to satisfy the periodic ACK loop's
  // `firstFrameReceived_` gate — the gate suppresses periodic emission on
  // idle ports until real traffic begins. 100 bytes is well under the
  // 8192-byte sync-ACK threshold so the sync trigger does not fire here.
  rxAndDeliver(
      llr, makePacket(100), FrameType::LLR_ELIGIBLE, FcsStatus::GOOD, 5u);

  // Run for a while; expect multiple LLR_ACKs in addition to the INIT_ECHO.
  scheduler_.run(Time::fromUs(1));
  std::size_t ackCount = 0;
  for (const auto& f : sentCtlos_) {
    if (f.type == LlrCtlosType::LLR_ACK) {
      ackCount++;
      EXPECT_EQ(f.seq, 5u); // seqDec(nextRxSeq=6 after rxFrame) = 5
    }
  }
  EXPECT_GE(ackCount, 1u);
  llr.shutdown();
  scheduler_.run();
}

// Round-trip: missing → SEND_NACK → NACK_SENT, then expected frame →
// NACK_SENT → SEND_ACKS resumes ACK emission.
TEST_F(LlrRxTest, NackSentToSendAcksOnExpectedFrame) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeLocal(true);
  scheduler_.run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT, /*seq=*/0u, 0);
  scheduler_.run(Time::fromUs(1));
  scheduler_.run(Time::fromUs(1));
  ASSERT_EQ(llr.ackNackState(), AckNackState::SEND_ACKS);

  // Inject missing frame.
  rxAndDeliver(
      llr, makePacket(100), FrameType::LLR_ELIGIBLE, FcsStatus::GOOD, 5u);
  scheduler_.run(Time::fromUs(1));
  ASSERT_EQ(llr.ackNackState(), AckNackState::NACK_SENT);

  // Now inject the expected frame (seq 0). expected_frame_ok pulse drives
  // NACK_SENT → SEND_ACKS.
  rxAndDeliver(
      llr, makePacket(100), FrameType::LLR_ELIGIBLE, FcsStatus::GOOD, 0u);
  scheduler_.run(Time::fromUs(1));
  EXPECT_EQ(llr.ackNackState(), AckNackState::SEND_ACKS);
  EXPECT_EQ(deliveredFrames_.size(), 1u);
  llr.shutdown();
  scheduler_.run();
}

// Spec §5.1.6 Fig 5-4 "Any state → OFF" edge — trigger 1 of 2:
// `setLlrModeLocal(false)` drives the AN-SM to OFF from any non-OFF
// state. Also clears `next_rx_seq_vld` per Tbl 5-11.
TEST_F(LlrRxTest, AnSmGoesOffOnSetLlrModeLocalFalse) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeLocal(true);
  scheduler_.run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT, /*seq=*/0u, 0);
  scheduler_.run(Time::fromUs(1));
  scheduler_.run(Time::fromUs(1));
  ASSERT_EQ(llr.ackNackState(), AckNackState::SEND_ACKS);
  ASSERT_TRUE(llr.nextRxSeqVld());

  // Toggle llr_mode_local OFF → AN-SM must transition to OFF.
  llr.setLlrModeLocal(false);
  EXPECT_EQ(llr.ackNackState(), AckNackState::OFF);
  EXPECT_FALSE(llr.nextRxSeqVld());

  // Re-toggling false → false is a no-op (already OFF).
  llr.setLlrModeLocal(false);
  EXPECT_EQ(llr.ackNackState(), AckNackState::OFF);

  llr.shutdown();
  scheduler_.run();
}

// Variant of the previous test: drive the SM into NACK_SENT first (a
// state with pending work), then verify setLlrModeLocal(false) still
// resets it cleanly (no stuck pending NACK).
TEST_F(LlrRxTest, AnSmGoesOffFromNackSentOnSetLlrModeLocalFalse) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeLocal(true);
  scheduler_.run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT, /*seq=*/0u, 0);
  scheduler_.run(Time::fromUs(1));
  scheduler_.run(Time::fromUs(1));
  ASSERT_EQ(llr.ackNackState(), AckNackState::SEND_ACKS);

  // Drive into NACK_SENT via a missing-frame gap.
  rxAndDeliver(
      llr, makePacket(100), FrameType::LLR_ELIGIBLE, FcsStatus::GOOD, 5u);
  scheduler_.run(Time::fromUs(1));
  ASSERT_EQ(llr.ackNackState(), AckNackState::NACK_SENT);

  llr.setLlrModeLocal(false);
  EXPECT_EQ(llr.ackNackState(), AckNackState::OFF);
  EXPECT_FALSE(llr.nextRxSeqVld());
  llr.shutdown();
  scheduler_.run();
}

// Spec §5.1.6 Fig 5-4 "Any state → OFF" edge — trigger 2 of 2:
// `managementReset()` drives the AN-SM to OFF synchronously, in
// addition to driving the TX SM to LLR_OFF via the existing
// `managementResetRequested_` broadcast.
TEST_F(LlrRxTest, AnSmGoesOffOnManagementReset) {
  Llr llr(&ctx_, defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeLocal(true);
  scheduler_.run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT, /*seq=*/0u, 0);
  scheduler_.run(Time::fromUs(1));
  scheduler_.run(Time::fromUs(1));
  ASSERT_EQ(llr.ackNackState(), AckNackState::SEND_ACKS);
  ASSERT_TRUE(llr.nextRxSeqVld());

  llr.managementReset();
  EXPECT_EQ(llr.ackNackState(), AckNackState::OFF);
  EXPECT_FALSE(llr.nextRxSeqVld());
  llr.shutdown();
  scheduler_.run();
}

} // namespace
} // namespace packsim::llr
