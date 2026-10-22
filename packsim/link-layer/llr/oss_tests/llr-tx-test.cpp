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

class LlrTxTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Pin pdes_threads=4 — see `eth-port-test.cpp` PinPdesThreads
    // for the rationale (default thread count, ThreadSanitizer timeout).
    gflags::SetCommandLineOption("pdes_threads", "4");
    schedulerOpt_.emplace();
    ctxOpt_.emplace(&schedulerOpt_.value(), "llr-tx-test", 0, nullptr);
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

  // Capture every wire emission so tests can assert on counts/contents.
  // Populated by `txAndCapture` (for new frames pulled via
  // `pullNextNewFrame`) and by the test helpers that pull REPLAY
  // frames. The CtlOS capture hook drains `Llr::ctlosSlots` into
  // `sentCtlos_` at post time.
  std::vector<PacketPtr> sentData_;
  std::vector<LlrCtlosPayload> sentCtlos_;
  test::TestTxPacketSource source_;

  // Wire the test's `TestTxPacketSource` into `Llr` so
  // `pullNextNewFrame()` sees the test-enqueued packets. Must be
  // called BEFORE `start()` per the production wiring order in
  // `EthernetPort::enableLlr`.
  void wireSource(Llr& llr) {
    llr.setTxPacketSource(&source_);
  }

  // Install a CtlOS-capture hook (see `bindCtlosCapture` in llr-rx-test).
  void bindCtlosCapture(Llr& llr) {
    llr.ctlosSlots().setOnPostCallback([this, p = &llr]() {
      while (auto f = p->ctlosSlots().popHighest()) {
        sentCtlos_.emplace_back(*f);
      }
    });
  }

  LlrConfig defaultConfig() const {
    LlrConfig c;
    // Timer maxes are short enough that PDES (1ns lookahead) can drain
    // them on shutdown without taking forever, but long enough that they
    // don't spontaneously fire during the short test run windows.
    c.replayTimerMax = Time::fromUs(10);
    c.pcsLostStatusTimerMax = Time::fromUs(10);
    c.dataAgeTimerMax = Time::fromUs(10);
    c.replayCtMax = 3;
    c.initBehavior = FrameBehavior::BLOCK;
    c.flushBehavior = FrameBehavior::BLOCK;
    c.reInitOnDiscard = true;
    // 800 Gbps and an 8192-byte target spacing by default, so one LLR_INIT
    // period (4 × the spacing) is ~328ns.
    return c;
  }

  PacketPtr makePacket(uint32_t size) {
    return createPacket<PacketType::IPV6>(/*payloadSize=*/size);
  }

  // Convenience wrapper around the shared helper that uses this
  // fixture's source + capture buffer.
  bool txAndCapture(Llr& llr, PacketPtr pkt) {
    return test::txAndCapture(llr, source_, std::move(pkt), sentData_);
  }
};

// Initial state: TX SM is in LLR_OFF; ackNack SM is in OFF.
TEST_F(LlrTxTest, InitialStateIsLlrOff) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  scheduler().run(Time::fromUs(1));
  EXPECT_EQ(llr.txState(), TxState::LLR_OFF);
  EXPECT_EQ(llr.ackNackState(), AckNackState::OFF);
  llr.shutdown();
  scheduler().run();
}

// setLlrModeRemote(true) drives LLR_OFF → INIT.
TEST_F(LlrTxTest, ModeRemoteOnTransitionsToInit) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  EXPECT_EQ(llr.txState(), TxState::INIT);
  llr.shutdown();
  scheduler().run();
}

// INIT → ADVANCE on rxCtlos(LLR_INIT_ECHO) with seq matching tx_seq.
TEST_F(LlrTxTest, InitEchoTransitionsToAdvance) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::INIT);

  // tx_seq is 0 after reset; INIT_ECHO with seq=0 matches.
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, /*seq=*/0u, /*initData=*/0);
  scheduler().run(Time::fromUs(1));
  EXPECT_EQ(llr.txState(), TxState::ADVANCE);
  llr.shutdown();
  scheduler().run();
}

// INIT_ECHO with mismatched seq is ignored (no transition).
// Spec p. 477 §5.1.9.1.2: "If the values are equal, init_echo_received
// is pulsed."
TEST_F(LlrTxTest, MismatchedInitEchoIgnored) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::INIT);

  // tx_seq is 0; send INIT_ECHO with seq=42 (mismatched).
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, /*seq=*/42u, /*initData=*/0);
  scheduler().run(Time::fromUs(1));
  EXPECT_EQ(llr.txState(), TxState::INIT); // still INIT
  llr.shutdown();
  scheduler().run();
}

// initCtlosLoop emits LLR_INIT periodically while in INIT state.
TEST_F(LlrTxTest, InitCtlosLoopEmitsPeriodically) {
  auto config = defaultConfig();
  // LLR_INIT period = 4 × ctlos_target_spacing per UEC §5.1.9.1.1 —
  // with the test's defaultConfig that comes out to a sub-µs interval
  // at the configured link rate. Over a 2µs window we expect at least
  // 1-3 LLR_INITs depending on the initCtlosLoop dispatch ordering;
  // use a conservative ≥1 to avoid timing flakiness.
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::INIT);

  scheduler().run(Time::fromUs(1));
  EXPECT_GE(sentCtlos_.size(), 1u);
  for (const auto& frame : sentCtlos_) {
    EXPECT_EQ(frame.type, LlrCtlosType::LLR_INIT);
  }
  llr.shutdown();
  scheduler().run();
}

// pullNextNewFrame in LLR_OFF state passes through (BEST_EFFORT).
// `txPassthrough` bumps on the LLR_OFF pass-through path.
TEST_F(LlrTxTest, PullInLlrOffPassesThrough) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  scheduler().run(Time::fromUs(1));
  EXPECT_EQ(llr.txState(), TxState::LLR_OFF);

  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  EXPECT_EQ(sentData_.size(), 1u);
  EXPECT_EQ(llr.stats().txPassthrough, 1u);
  llr.shutdown();
  scheduler().run();
}

// pullNextNewFrame in INIT with initBehavior=BLOCK returns nullopt; the
// packet stays in the upstream queue.
TEST_F(LlrTxTest, PullInInitBlocked) {
  auto config = defaultConfig();
  config.initBehavior = FrameBehavior::BLOCK;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::INIT);

  EXPECT_FALSE(txAndCapture(llr, makePacket(100)));
  EXPECT_EQ(sentData_.size(), 0u);
  // BLOCK semantics: packet stays queued for a later retry.
  EXPECT_EQ(source_.size(), 1u);
  llr.shutdown();
  scheduler().run();
}

// pullNextNewFrame in INIT with initBehavior=DISCARD pops the packet
// and bumps txDiscard.
TEST_F(LlrTxTest, PullInInitDiscarded) {
  auto config = defaultConfig();
  config.initBehavior = FrameBehavior::DISCARD;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));

  EXPECT_FALSE(txAndCapture(llr, makePacket(100)));
  EXPECT_EQ(sentData_.size(), 0u);
  EXPECT_EQ(llr.stats().txDiscard, 1u);
  EXPECT_EQ(source_.size(), 0u);
  llr.shutdown();
  scheduler().run();
}

// pullNextNewFrame in ADVANCE admits and stores in buffer; tx_seq increments.
TEST_F(LlrTxTest, PullInAdvanceAdmitsAndAdvancesSeq) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, /*seq=*/0u, 0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);

  EXPECT_EQ(llr.txSeq(), 0u);
  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  EXPECT_EQ(llr.txSeq(), 1u);
  EXPECT_EQ(sentData_.size(), 1u);
  EXPECT_EQ(llr.stats().txOk, 1u);

  EXPECT_TRUE(txAndCapture(llr, makePacket(200)));
  EXPECT_EQ(llr.txSeq(), 2u);
  llr.shutdown();
  scheduler().run();
}

// ADVANCE → REPLAY → ADVANCE on receipt of an LLR_NACK. The wire-side
// drain happens via `pullAllReplayFrames` which mirrors the scheduler's
// per-frame `pullNextReplayFrame` loop.
TEST_F(LlrTxTest, NackReceivedTransitionsThroughReplay) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromNs(100));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, 0u, 0);
  scheduler().run(Time::fromNs(100));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);

  // Send one frame (seq=0) so there's something to replay.
  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  scheduler().run(Time::fromNs(100));
  const auto sentBeforeNack = sentData_.size();

  // Per spec §5.1.9.1.4, LLR_NACK carries `ack_nack_seq = next_rx_seq - 1`.
  // Receiver expecting seq 0 (the frame we just sent) → NACK seq =
  // seqDec(0) = 0xFFFFF. This satisfies ack_nack_seq_ok without
  // advancing acked_seq, so the buffer retains the frame for REPLAY.
  llr.rxCtlos(LlrCtlosType::LLR_NACK, /*seq=*/0xFFFFFu, 0);
  scheduler().run(Time::fromNs(100));
  // Now in REPLAY: drain the snapshot the way the scheduler does, then
  // signal batch completion so the TX SM transitions REPLAY → ADVANCE.
  const auto replayed = test::pullAllReplayFrames(llr, sentData_);
  EXPECT_GE(replayed, 1u);
  llr.notifyReplayBatchComplete();
  scheduler().run(Time::fromNs(100));
  EXPECT_EQ(llr.txState(), TxState::ADVANCE);
  // Replay re-sent the buffered frame.
  EXPECT_GT(sentData_.size(), sentBeforeNack);
  EXPECT_GE(llr.stats().txReplay, 1u);
  llr.shutdown();
  scheduler().run();
}

// ADVANCE → FLUSH on data_age_timer_expired (with reInitOnDiscard=true →
// FLUSH → INIT). We send a frame to arm both timers (rule 2 for both),
// then advance past the shorter of the two — data_age is configured
// shorter than replay so the FLUSH-trigger fires before any REPLAY entry.
TEST_F(LlrTxTest, DataAgeTimerExpiredFlushesAndReinits) {
  auto config = defaultConfig();
  config.reInitOnDiscard = true;
  // Replay timer never fires within the test window.
  config.replayTimerMax = Time::fromMs(1);
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, 0u, 0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);

  // Pull one frame so notifyNewFrameTransmitted arms data_age_timer (rule 2).
  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  scheduler().run(Time::fromUs(1));
  // Advance past dataAgeTimerMax (10us) to trigger the timer.
  scheduler().run(Time::fromUs(11));
  // FLUSH → INIT (per reInitOnDiscard=true).
  EXPECT_EQ(llr.txState(), TxState::INIT);
  llr.shutdown();
  scheduler().run();
}

// ADVANCE → FLUSH stays in FLUSH when reInitOnDiscard=false until
// managementReset(). Then FLUSH → LLR_OFF.
TEST_F(LlrTxTest, FlushWaitsForManagementResetWhenNotReinitOnDiscard) {
  auto config = defaultConfig();
  config.reInitOnDiscard = false;
  // Replay timer never fires within the test window.
  config.replayTimerMax = Time::fromMs(1);
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, 0u, 0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);

  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  scheduler().run(Time::fromUs(1));
  scheduler().run(Time::fromUs(11)); // past dataAgeTimerMax=10us
  // FLUSH stays put (no auto-recover).
  EXPECT_EQ(llr.txState(), TxState::FLUSH);

  llr.managementReset();
  scheduler().run(Time::fromUs(1));
  EXPECT_EQ(llr.txState(), TxState::LLR_OFF);
  llr.shutdown();
  scheduler().run();
}

// replay_timer expired with replayCt < replayCtMax → REPLAY (not FLUSH).
// Disable data_age so only replay_timer fires; verify the ADVANCE→REPLAY
// edge fires and the SM transitions back through REPLAY to ADVANCE.
TEST_F(LlrTxTest, ReplayTimerWithLowCountGoesToReplay) {
  auto config = defaultConfig();
  config.replayCtMax = 5;
  // Data-age timer never fires within the test window.
  config.dataAgeTimerMax = Time::fromMs(1);
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, 0u, 0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);
  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  scheduler().run(Time::fromUs(1));

  // Advance past replayTimerMax (10us) to trigger the timer.
  scheduler().run(Time::fromUs(11));
  // Drain REPLAY frames (mirrors the scheduler) and signal completion.
  test::pullAllReplayFrames(llr, sentData_);
  llr.notifyReplayBatchComplete();
  scheduler().run(Time::fromUs(1));
  // REPLAY → ADVANCE (no FLUSH because replayCt was 0 < 5).
  EXPECT_EQ(llr.txState(), TxState::ADVANCE);
  llr.shutdown();
  scheduler().run();
}

// replayCtMax=0xFF (all-ones) means "no maximum"; replay_timer expiry
// always goes to REPLAY, never FLUSH. Each iteration drains the REPLAY
// snapshot (firing per-frame TX-completes that re-arm rule 3) then
// signals batch completion to drop back to ADVANCE.
TEST_F(LlrTxTest, ReplayCtMaxAllOnesNeverFlushesOnReplayTimer) {
  auto config = defaultConfig();
  config.replayCtMax = 0xFF;
  // Data-age timer never fires within the test window.
  config.dataAgeTimerMax = Time::fromMs(1);
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, 0u, 0);
  scheduler().run(Time::fromUs(1));
  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  scheduler().run(Time::fromUs(1));

  // Each iteration: advance past the timer max → REPLAY enters; drain the
  // REPLAY batch so we return to ADVANCE for the next iteration's fire.
  for (int i = 0; i < 10; ++i) {
    scheduler().run(Time::fromUs(11));
    test::pullAllReplayFrames(llr, sentData_);
    llr.notifyReplayBatchComplete();
    scheduler().run(Time::fromUs(1));
    EXPECT_NE(llr.txState(), TxState::FLUSH)
        << "iteration " << i << ": replayCtMax=0xFF should never FLUSH";
  }
  llr.shutdown();
  scheduler().run();
}

// pcs_lost_status_timer_expired → FLUSH unconditionally (independent of
// replayCt). Drop PCS status to arm the timer (rule 2 — activated when
// PCS_status==FALSE in an active state); then advance past max to fire.
TEST_F(LlrTxTest, PcsLostStatusTimerExpiredFlushes) {
  auto config = defaultConfig();
  config.reInitOnDiscard = false;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, 0u, 0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);

  llr.onPcsStatusChange(false); // arm pcs_lost_status_timer
  scheduler().run(Time::fromUs(11)); // past pcsLostStatusTimerMax=10us
  EXPECT_EQ(llr.txState(), TxState::FLUSH);
  llr.shutdown();
  scheduler().run();
}

// LLR_status accessor matches spec: TRUE in ADVANCE/REPLAY, FALSE elsewhere.
TEST_F(LlrTxTest, LlrStatusReflectsState) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  EXPECT_FALSE(llr.llrStatus()); // LLR_OFF
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  EXPECT_FALSE(llr.llrStatus()); // INIT
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, 0u, 0);
  scheduler().run(Time::fromUs(1));
  EXPECT_TRUE(llr.llrStatus()); // ADVANCE
  llr.shutdown();
  scheduler().run();
}

// skipInitNegotiation: TX SM starts in ADVANCE, ACK/NACK SM in SEND_ACKS,
// no INIT/INIT_ECHO exchange needed. The pull API immediately admits the
// next packet under ADVANCE rules.
TEST_F(LlrTxTest, SkipInitNegotiationStartsInAdvance) {
  auto config = defaultConfig();
  config.skipInitNegotiation = true;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  scheduler().run(Time::fromUs(1));

  // No mode setters called, but SM is already in post-handshake states.
  EXPECT_EQ(llr.txState(), TxState::ADVANCE);
  EXPECT_EQ(llr.ackNackState(), AckNackState::SEND_ACKS);

  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  EXPECT_EQ(llr.txSeq(), 1u);
  llr.shutdown();
  scheduler().run();
}

// With skipInitNegotiation, start() turns llr_mode_remote ON itself, so a
// change made before start() loses nothing and is accepted.
TEST_F(LlrTxTest, SkipInitNegotiationAcceptsModeRemoteBeforeStart) {
  auto config = defaultConfig();
  config.skipInitNegotiation = true;
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.setLlrModeRemote(/*on=*/true);
  llr.start();
  scheduler().run(Time::fromUs(1));

  EXPECT_EQ(llr.txState(), TxState::ADVANCE);
  llr.shutdown();
  scheduler().run();
}

class LlrTxDeathTest : public LlrTxTest {};

// Without skipInitNegotiation, LLR_OFF waits for a change of llr_mode_remote,
// so a change made before start() would be lost.
TEST_F(LlrTxDeathTest, ModeRemoteChangeBeforeStartAborts) {
  Llr llr(&ctx(), defaultConfig());
  EXPECT_DEATH(
      llr.setLlrModeRemote(/*on=*/true),
      "changed llr_mode_remote before start");
}

// A shutdown() before start() latches, so nothing would cancel the timers a
// later start() arms.
TEST_F(LlrTxDeathTest, StartAfterShutdownAborts) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  llr.shutdown();
  EXPECT_DEATH(llr.start(), "after shutdown");
}

// shutdown is idempotent and clean.
TEST_F(LlrTxTest, ShutdownIsCleanAndIdempotent) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  llr.shutdown();
  llr.shutdown(); // second call is no-op
  scheduler().run();
  SUCCEED();
}

// Spec §5.1.5 Fig 5-3: INIT → LLR_OFF on llr_mode_remote OFF. (Missing-
// transition coverage; previously only LLR_OFF → INIT was tested.)
TEST_F(LlrTxTest, InitToLlrOffOnModeRemoteOff) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::INIT);

  llr.setLlrModeRemote(false);
  scheduler().run(Time::fromUs(1));
  EXPECT_EQ(llr.txState(), TxState::LLR_OFF);
  llr.shutdown();
  scheduler().run();
}

// Spec §5.1.5 Fig 5-3: ADVANCE → LLR_OFF on llr_mode_remote OFF.
// The collectAny in txRunAdvance includes llrModeRemoteChanged_; the
// post-await check transitions to LLR_OFF when !llrModeRemote_.
TEST_F(LlrTxTest, AdvanceToLlrOffOnModeRemoteOff) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, 0u, 0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);

  llr.setLlrModeRemote(false);
  scheduler().run(Time::fromUs(1));
  EXPECT_EQ(llr.txState(), TxState::LLR_OFF);
  llr.shutdown();
  scheduler().run();
}

// Spec §5.1.5 Tbl 5-10: an llr_mode_remote change sets `tx_seq` to 0 and
// `acked_seq` to all-ones, so no frame is outstanding afterwards. Frames still
// unacknowledged when the mode goes off must leave the replay buffer, or the
// renegotiated ADVANCE hands their sequence numbers out a second time.
TEST_F(LlrTxTest, ReinitAfterModeRemoteToggleAdmitsFromSeqZero) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(/*on=*/true);
  scheduler().run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, /*seq=*/0u, /*initData=*/0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);
  // Seqs 0 and 1, never acknowledged.
  ASSERT_TRUE(txAndCapture(llr, makePacket(100)));
  ASSERT_TRUE(txAndCapture(llr, makePacket(100)));

  llr.setLlrModeRemote(/*on=*/false);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::LLR_OFF);
  llr.setLlrModeRemote(/*on=*/true);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::INIT);
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, /*seq=*/0u, /*initData=*/0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);
  source_.enqueue(makePacket(100));
  const auto pulled = llr.pullNextNewFrame();

  ASSERT_TRUE(pulled.has_value());
  EXPECT_EQ(pulled->seq, 0u);
  EXPECT_EQ(llr.outstandingSeq(), 1u);
  llr.shutdown();
  scheduler().run();
}

// A frame admitted after the mode goes off, while the TX SM is still in
// ADVANCE, takes the restarted seq 0. The next change, the one that turns the
// mode back on, must discard it too, or the renegotiated ADVANCE hands seq 0
// out a second time.
TEST_F(LlrTxTest, FrameAdmittedBeforeLlrOffIsDiscardedWhenModeRemoteReturns) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(/*on=*/true);
  scheduler().run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, /*seq=*/0u, /*initData=*/0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);

  llr.setLlrModeRemote(/*on=*/false);
  // The TX SM has not run yet, so this is admitted under ADVANCE, as seq 0.
  ASSERT_TRUE(txAndCapture(llr, makePacket(100)));
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::LLR_OFF);
  ASSERT_EQ(llr.outstandingSeq(), 1u);

  llr.setLlrModeRemote(/*on=*/true);
  EXPECT_EQ(llr.outstandingSeq(), 0u);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::INIT);
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, /*seq=*/0u, /*initData=*/0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);
  source_.enqueue(makePacket(100));
  const auto pulled = llr.pullNextNewFrame();

  ASSERT_TRUE(pulled.has_value());
  EXPECT_EQ(pulled->seq, 0u);
  llr.shutdown();
  scheduler().run();
}

// The replay_timer and data_age_timer armed for frames that an llr_mode_remote
// change discards stop with them (rule 1 of each, UEC §5.1.5 Tbl 5-10: held at
// zero whenever outstanding_seq is zero). The mode is restored before the TX SM
// runs, so the test does not depend on INIT entry, which stops the same timers.
// Left running, they would send the TX SM to FLUSH or REPLAY over frames that
// no longer exist.
TEST_F(LlrTxTest, ModeRemoteToggleStopsTimersOfDiscardedFrames) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(/*on=*/true);
  scheduler().run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, /*seq=*/0u, /*initData=*/0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);
  // Arms replay_timer and data_age_timer (rule 2 of each).
  ASSERT_TRUE(txAndCapture(llr, makePacket(100)));

  llr.setLlrModeRemote(/*on=*/false);
  llr.setLlrModeRemote(/*on=*/true);
  // Past replayTimerMax and dataAgeTimerMax (both 10us).
  scheduler().run(Time::fromUs(11));

  EXPECT_EQ(llr.stats().txFlushEntries, 0u);
  EXPECT_EQ(llr.stats().txReplayBatches, 0u);
  llr.shutdown();
  scheduler().run();
}

// An llr_mode_remote change in the middle of a REPLAY batch discards the frames
// the batch had yet to resend, so the batch has nothing left: the wire driver
// pulls no more replay frames and sees the snapshot drained, which is what lets
// it report the batch complete once the frames on the wire finish.
TEST_F(LlrTxTest, ModeRemoteOffDuringReplayLeavesNothingToReplay) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(/*on=*/true);
  scheduler().run(Time::fromNs(100));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, /*seq=*/0u, /*initData=*/0);
  scheduler().run(Time::fromNs(100));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);
  ASSERT_TRUE(txAndCapture(llr, makePacket(100)));
  ASSERT_TRUE(txAndCapture(llr, makePacket(100)));
  // ack_nack_seq = seqDec(0) asks for both frames again without acknowledging
  // either.
  llr.rxCtlos(LlrCtlosType::LLR_NACK, /*seq=*/0xFFFFFu, /*initData=*/0);
  scheduler().run(Time::fromNs(100));
  ASSERT_EQ(llr.txState(), TxState::REPLAY);
  // Resends seq 0; seq 1 is still to come.
  ASSERT_TRUE(llr.pullNextReplayFrame().has_value());

  llr.setLlrModeRemote(/*on=*/false);

  EXPECT_TRUE(llr.isReplaySnapshotDrained());
  EXPECT_FALSE(llr.pullNextReplayFrame().has_value());
  llr.shutdown();
  scheduler().run();
}

// Spec §5.1.8 (last paragraph): nack_received is "set to FALSE when the
// state machine is in the INIT state". A NACK arriving during INIT must
// be ignored — otherwise the next INIT → ADVANCE transition would see
// nackReceived_=true and trigger a spurious REPLAY.
//
// Test: drive SM to INIT, inject a NACK via rxCtlos, drive INIT →
// ADVANCE, verify SM stays in ADVANCE (no spurious REPLAY).
TEST_F(LlrTxTest, NackDuringInitDoesNotPersistToAdvance) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::INIT);

  // NACK during INIT. ack_nack_seq_ok requires seq in [acked_seq,
  // tx_seq]. After llr_mode_remote=true, tx_seq=0 and acked_seq=0xFFFFF
  // (all-ones). Seq=0xFFFFF is in-range.
  llr.rxCtlos(LlrCtlosType::LLR_NACK, 0xFFFFFu, 0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::INIT);

  // Drive INIT → ADVANCE. Without the §5.1.8 gate, nackReceived_=true
  // from above would trigger ADVANCE → REPLAY immediately.
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, 0u, 0);
  scheduler().run(Time::fromUs(1));
  EXPECT_EQ(llr.txState(), TxState::ADVANCE)
      << "NACK during INIT must not trigger REPLAY on ADVANCE entry";
  // No replays should have happened (txBuffer was empty in INIT).
  EXPECT_EQ(llr.stats().txReplay, 0u);
  llr.shutdown();
  scheduler().run();
}

// Companion to NackDuringInitDoesNotPersistToAdvance: NACK arriving
// DURING a FLUSH cycle is cleared by txRunInit's defensive entry-clear
// on the subsequent FLUSH→INIT transition. Tests the leftover-from-
// prior-state path.
//
// Construction: use reInitOnDiscard=false so the SM parks in FLUSH and
// gives us a clean window to inject the NACK via rxCtlos; then
// managementReset → LLR_OFF → toggle llr_mode_remote → INIT, where
// `txRunInit` clears nackReceived_ defensively.
TEST_F(LlrTxTest, NackBeforeFlushReinitIsClearedOnInitEntry) {
  auto config = defaultConfig();
  config.reInitOnDiscard = false; // stay in FLUSH so we can inject NACK
  // Replay timer never fires within the test window.
  config.replayTimerMax = Time::fromMs(1);
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, 0u, 0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);

  // Pull a frame so data_age_timer arms (rule 2); seq=0 lands in the
  // replay buffer. We'll use that seq for the NACK so ack_nack_seq_ok
  // returns true during FLUSH.
  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  scheduler().run(Time::fromUs(1));
  scheduler().run(Time::fromUs(11)); // past dataAgeTimerMax=10us → FLUSH
  ASSERT_EQ(llr.txState(), TxState::FLUSH);

  // NACK during FLUSH: `txState_ != INIT` so the handler sets
  // nackReceived_=true. ack_nack_seq_ok holds since seq 0 is in
  // (ackedSeq=0xFFFFF, txSeq-1=0].
  llr.rxCtlos(LlrCtlosType::LLR_NACK, /*seq=*/0u, 0);

  // Drive FLUSH → LLR_OFF → INIT (txRunInit's defensive clear runs here).
  llr.managementReset();
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::LLR_OFF);
  llr.setLlrModeRemote(false);
  scheduler().run(Time::fromUs(1));
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::INIT);

  // INIT_ECHO → ADVANCE. Without the entry-clear, nackReceived_=true
  // would persist and trigger ADVANCE → REPLAY.
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, 0u, 0);
  scheduler().run(Time::fromUs(1));
  EXPECT_EQ(llr.txState(), TxState::ADVANCE)
      << "leftover NACK from prior cycle must not survive INIT entry";
  EXPECT_EQ(llr.stats().txReplay, 0u);
  llr.shutdown();
  scheduler().run();
}

// shutdown() tears the coroutines down synchronously on the calling stack:
// it `reset()`s the owned SimScopedTask members, destroying each parked
// coroutine frame at its `co_await` suspension point (the frame unwind
// self-cancels every armed waiter via the collectAny teardown and the
// self-cancelling SimEvent/SimSignal/SimStopwatch awaitables). No sim time
// advances and no event is left undrained — verified below.
TEST_F(LlrTxTest, ShutdownDrainsSynchronouslyWithoutAdvancingTime) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, 0u, 0);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);

  // Capture sim time before shutdown.
  const auto preShutdownNs = ctx().nowNs();
  llr.shutdown();
  // No scheduler.run() between shutdown() and the assertion: reset()ing the
  // owned SimScopedTask members destroys every parked frame synchronously on
  // the calling stack, so the drain completes without advancing sim time.
  EXPECT_EQ(ctx().nowNs(), preShutdownNs)
      << "shutdown() must not advance sim time";

  // After shutdown's drain, a final scheduler.run() processes any
  // remaining queued events (e.g., timer cancellations). No XCHECK
  // should fire from undrained event waiters.
  scheduler().run();
  SUCCEED();
}

// Destroy-based teardown: shutdown() while the TX SM is parked in REPLAY
// waiting on `replayBatchComplete_` (a bare `co_await` of the self-cancelling
// SimSignal awaitable) must tear the coroutine down cleanly — `reset()`ing the
// owning `txStateMachine_` destroys the dispatcher frame and cascade-destroys
// `txRunReplay` at its await, leaving no dangling signal waiter (ASan-checked).
TEST_F(LlrTxTest, ShutdownWhileParkedInReplayIsClean) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromNs(100));
  llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, 0u, 0);
  scheduler().run(Time::fromNs(100));
  ASSERT_EQ(llr.txState(), TxState::ADVANCE);

  // One outstanding frame, then NACK it (ack_nack_seq = seqDec(0) = 0xFFFFF,
  // which is ack_nack_seq_ok without advancing acked_seq) to enter REPLAY.
  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  scheduler().run(Time::fromNs(100));
  llr.rxCtlos(LlrCtlosType::LLR_NACK, /*seq=*/0xFFFFFu, 0);
  scheduler().run(Time::fromNs(100));
  ASSERT_EQ(llr.txState(), TxState::REPLAY);

  // Drain the snapshot the way the scheduler would, but do NOT call
  // `notifyReplayBatchComplete()`: the TX SM is now parked in `txRunReplay`'s
  // bare `co_await replayBatchComplete_.wait()`, awaiting `replay_done`.
  const auto replayed = test::pullAllReplayFrames(llr, sentData_);
  EXPECT_GE(replayed, 1u);
  ASSERT_EQ(llr.txState(), TxState::REPLAY) << "must shut down WHILE parked";

  // Tear down mid-wait: reset destroys the parked frame; the explicit timer
  // cancels clear any synchronously-armed resume. No crash / leak / dangling.
  llr.shutdown();
  scheduler().run();
  SUCCEED();
}

} // namespace
} // namespace packsim::llr
