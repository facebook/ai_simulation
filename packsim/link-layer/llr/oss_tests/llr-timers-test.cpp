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

class LlrTimersTest : public ::testing::Test {
 protected:
  void SetUp() override {
    gflags::SetCommandLineOption("pdes_threads", "4");
    schedulerOpt_.emplace();
    ctxOpt_.emplace(&schedulerOpt_.value(), "llr-timers", 0, nullptr);
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
  std::vector<LlrCtlosPayload> sentCtlos_;
  test::TestTxPacketSource source_;

  // The test drives wire-side TX completions explicitly via
  // `test::txAndCapture` (which fires `notifyNewFrameTransmitted`) and
  // `test::pullAllReplayFrames` (which fires `notifyReplayFrameTransmitted`
  // per frame). `notifyReplayBatchComplete` is NOT fired automatically —
  // tests that need it call it explicitly.

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
    c.replayTimerMax = Time::fromUs(10);
    c.pcsLostStatusTimerMax = Time::fromUs(10);
    c.dataAgeTimerMax = Time::fromUs(10);
    c.replayCtMax = 0xFF; // no max — focus on timer expiry, not flush
    c.initBehavior = FrameBehavior::BLOCK;
    c.flushBehavior = FrameBehavior::BLOCK;
    c.reInitOnDiscard = true;
    return c;
  }

  PacketPtr makePacket(uint32_t size) {
    return createPacket<PacketType::IPV6>(size);
  }

  bool txAndCapture(Llr& llr, PacketPtr pkt) {
    return test::txAndCapture(llr, source_, std::move(pkt), sentData_);
  }

  void bringToAdvance(Llr& llr) {
    llr.start();
    llr.setLlrModeLocal(true);
    llr.setLlrModeRemote(true);
    scheduler().run(Time::fromUs(1));
    llr.rxCtlos(LlrCtlosType::LLR_INIT_ECHO, 0u, 0);
    scheduler().run(Time::fromUs(1));
    ASSERT_EQ(llr.txState(), TxState::ADVANCE);
  }
};

// replay_timer rule 1: held at zero while outstanding_seq == 0. With no
// frames sent, the timer never fires (replay_timer_expired stays false).
TEST_F(LlrTimersTest, ReplayTimerHeldWhenBufferEmpty) {
  Llr llr(&ctx(), defaultConfig());
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  bringToAdvance(llr);
  // No frames sent; advance time well past replayTimerMax.
  scheduler().run(Time::fromUs(50)); // 5x the timer max
  // TX SM stays in ADVANCE — timer never fired.
  EXPECT_EQ(llr.txState(), TxState::ADVANCE);
  llr.shutdown();
  scheduler().run();
}

// replay_timer rule 2: activated on first frame TX. After replayTimerMax
// elapses without ACK, timer fires → REPLAY. Drain the snapshot and
// release `replay_done` so the SM returns to ADVANCE.
TEST_F(LlrTimersTest, ReplayTimerFiresAfterMaxWithoutAck) {
  auto config = defaultConfig();
  // Disable data_age_timer (very long) so only replay_timer fires.
  config.dataAgeTimerMax = Time::fromMs(1);
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  bringToAdvance(llr);

  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  scheduler().run(Time::fromUs(1));
  const auto sentBeforeTimer = sentData_.size();

  // Advance just past replayTimerMax (10us) to trigger ONE expiration.
  // We deliberately stop before the second expiration window: each
  // `notifyReplayFrameTransmitted` we fire from `pullAllReplayFrames`
  // re-arms the stopwatch (rule 3); a second expiration during this
  // run would park another `txRunReplay` cycle.
  scheduler().run(Time::fromUs(12));
  // Drain replay frames and signal completion so the SM can transition
  // out of REPLAY (spec §5.1.5 `replay_done`).
  test::pullAllReplayFrames(llr, sentData_);
  llr.notifyReplayBatchComplete();
  scheduler().run(Time::fromUs(1));
  // At least one replay should have fired.
  EXPECT_GE(llr.stats().txReplay, 1u);
  // After REPLAY, back in ADVANCE.
  EXPECT_EQ(llr.txState(), TxState::ADVANCE);
  // More frames sent (the original + one or more replays).
  EXPECT_GT(sentData_.size(), sentBeforeTimer);
  llr.shutdown();
  scheduler().run();
}

// replay_timer rule 4: LLR_ACK that *advances acked_seq* resets the timer.
// With ACKs that keep advancing the buffer (sending fresh frames between
// ACKs), the timer never reaches max.
TEST_F(LlrTimersTest, ReplayTimerResetByAdvancingAck) {
  auto config = defaultConfig();
  config.dataAgeTimerMax = Time::fromMs(1); // disable data_age path
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  bringToAdvance(llr);

  // Each iteration: send a fresh frame, advance time 4us (< 10us timer max),
  // ACK it (advances acked_seq → resets timer per rule 4).
  for (uint32_t i = 0; i < 4; ++i) {
    EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
    scheduler().run(Time::fromUs(4));
    llr.rxCtlos(LlrCtlosType::LLR_ACK, /*seq=*/i, 0);
    scheduler().run(Time::fromUs(1));
  }
  // No replays should have fired (timer kept getting reset).
  EXPECT_EQ(llr.stats().txReplay, 0u);
  llr.shutdown();
  scheduler().run();
}

// data_age_timer rule 4: expires after dataAgeTimerMax with no buffer
// advance. With replayCtMax=0xFF (no replay-count flush), data_age is
// the only path to FLUSH.
TEST_F(LlrTimersTest, DataAgeTimerExpiresFlushes) {
  auto config = defaultConfig();
  config.reInitOnDiscard = false; // stay in FLUSH for assertion
  // Make replay_timer much longer so only data_age can fire.
  config.replayTimerMax = Time::fromUs(100);
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  bringToAdvance(llr);

  EXPECT_TRUE(txAndCapture(llr, makePacket(100)));
  scheduler().run(Time::fromUs(1));
  // Advance well past dataAgeTimerMax (10us). With no ACKs, data_age
  // fires → FLUSH (with reInitOnDiscard=false, stays in FLUSH).
  scheduler().run(Time::fromUs(20));
  EXPECT_EQ(llr.txState(), TxState::FLUSH);
  llr.shutdown();
  scheduler().run();
}

// pcs_lost_status_timer: only runs in ADVANCE/REPLAY when PCS_status=FALSE.
// Triggers FLUSH when expired.
TEST_F(LlrTimersTest, PcsLostStatusTimerFlushesOnExpiry) {
  auto config = defaultConfig();
  config.reInitOnDiscard = false; // don't auto-reinit; wait in FLUSH
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  bringToAdvance(llr);

  // Drop PCS link.
  llr.onPcsStatusChange(false);
  // Advance past pcsLostStatusTimerMax (10us) → FLUSH.
  scheduler().run(Time::fromUs(20));
  EXPECT_EQ(llr.txState(), TxState::FLUSH);
  llr.shutdown();
  scheduler().run();
}

// pcs_lost_status_timer rule 1: stopped in non-active states. With LLR
// in INIT, the timer never fires even if PCS_status=FALSE.
TEST_F(LlrTimersTest, PcsLostStatusTimerStoppedInInit) {
  auto config = defaultConfig();
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  llr.start();
  llr.setLlrModeLocal(true);
  llr.setLlrModeRemote(true);
  scheduler().run(Time::fromUs(1));
  ASSERT_EQ(llr.txState(), TxState::INIT);

  // PCS goes down; timer should NOT activate (we're in INIT, not ADVANCE).
  llr.onPcsStatusChange(false);
  scheduler().run(Time::fromUs(20)); // past timer max
  // Still in INIT (no FLUSH triggered).
  EXPECT_EQ(llr.txState(), TxState::INIT);
  llr.shutdown();
  scheduler().run();
}

// pcs_lost_status_timer: PCS comes back up before timer fires → no flush.
TEST_F(LlrTimersTest, PcsRecoveryBeforeTimerExpires) {
  auto config = defaultConfig();
  Llr llr(&ctx(), config);
  test::wireNoopSchedulerWake(llr);
  bindCtlosCapture(llr);
  wireSource(llr);
  bringToAdvance(llr);

  // PCS down, then up before timer max.
  llr.onPcsStatusChange(false);
  scheduler().run(Time::fromUs(5)); // half the timer max
  llr.onPcsStatusChange(true);
  scheduler().run(Time::fromUs(20)); // past what would have been expiry
  // Should still be ADVANCE.
  EXPECT_EQ(llr.txState(), TxState::ADVANCE);
  llr.shutdown();
  scheduler().run();
}

} // namespace
} // namespace packsim::llr
