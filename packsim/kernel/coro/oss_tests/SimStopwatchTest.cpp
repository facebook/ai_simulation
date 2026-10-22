/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimStopwatch.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/kernel/coro/SimCancellableDelay.h"
#include "packsim/kernel/coro/SimScopedTask.h"
#include "packsim/kernel/coro/SimSignal.h"
#include "packsim/kernel/coro/SimTask.h"
#include "packsim/kernel/coro/SimWaitFirst.h"
#include "packsim/kernel/coro/oss_tests/fixtures/CoroTestFixture.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

using packsim::Scheduler;
using packsim::SimContext;
using packsim::Time;
using packsim::coro::SimCancellableDelay;
using packsim::coro::SimScopedTask;
using packsim::coro::SimSignal;
using packsim::coro::SimStopwatch;
using packsim::coro::SimTask;
using packsim::coro::simWaitFirst;
using packsim::coro::tests::CoroTestFixture;

namespace packsim::coro {

// Pre-builds a Scheduler + SimContext per test so each test body can focus on
// the scenario rather than re-doing fixture wiring. Mirrors the pattern used
// by SimWaitFirstFixture / SimCoordRoutedBarrierFixture.
class SimStopwatchFixture : public CoroTestFixture {
 protected:
  void SetUp() override {
    CoroTestFixture::SetUp();
    scheduler_ = std::make_unique<Scheduler>();
    ctx_ = std::make_unique<SimContext>(
        scheduler_.get(), "stopwatch", /*seed=*/0, /*conditionalMap=*/nullptr);
  }

  std::unique_ptr<Scheduler> scheduler_;
  std::unique_ptr<SimContext> ctx_;
};

using SimStopwatchTest = SimStopwatchFixture;

// Fresh stopwatch starts in STOPPED state.
TEST_F(SimStopwatchTest, InitialStateIsStopped) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  EXPECT_FALSE(sw.running());
  EXPECT_FALSE(sw.expired());
}

// start() from STOPPED transitions to RUNNING; expiration fires at maxDuration.
TEST_F(SimStopwatchTest, StartFromStoppedExpiresAtMaxDuration) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  std::optional<uint64_t> wakeNs;

  auto task = [&]() -> SimTask {
    sw.start();
    co_await sw.expiration();
    wakeNs = ctx_->nowNs();
  };

  ctx_->scheduleNow([&]() { task(); });
  scheduler_->run();

  ASSERT_TRUE(wakeNs.has_value());
  EXPECT_EQ(*wakeNs, 100);
  EXPECT_TRUE(sw.expired());
  EXPECT_FALSE(sw.running());
}

// reset() while RUNNING re-zeros the elapsed time. Without reset, the wake
// would fire at start+maxDuration; with reset at t=50, it fires at 50+max.
TEST_F(SimStopwatchTest, ResetWhileRunningRezerosElapsed) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  std::optional<uint64_t> wakeNs;

  auto task = [&]() -> SimTask {
    sw.start();
    co_await sw.expiration();
    wakeNs = ctx_->nowNs();
  };

  ctx_->scheduleNow([&]() { task(); });
  // At t=50 (halfway through), reset. New deadline becomes 50+100=150.
  ctx_->schedule(Time::fromNs(50), [&]() { sw.reset(); });

  scheduler_->run();

  ASSERT_TRUE(wakeNs.has_value());
  EXPECT_EQ(*wakeNs, 150);
}

// stop() while RUNNING removes the scheduled resume. The waiter stays
// suspended (no resume fires). To verify, race the expiration against a
// timeout: the timeout wins because the stopwatch never expires.
TEST_F(SimStopwatchTest, StopWhileRunningPreventsExpiration) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  SimCancellableDelay timeout;
  std::optional<std::size_t> winner;

  auto task = [&]() -> SimTask {
    sw.start();
    winner = co_await simWaitFirst(
        sw.expiration(), timeout.awaitDelay(ctx_.get(), Time::fromNs(200)));
  };

  ctx_->scheduleNow([&]() { task(); });
  // At t=50, stop the stopwatch — its scheduled resume at t=100 is removed.
  // The race resolves on the timeout(200) instead.
  ctx_->schedule(Time::fromNs(50), [&]() { sw.stop(); });

  scheduler_->run();

  ASSERT_TRUE(winner.has_value());
  EXPECT_EQ(*winner, 1u); // timeout won
  EXPECT_EQ(ctx_->nowNs(), 200);
  EXPECT_FALSE(sw.running());
  EXPECT_FALSE(sw.expired());
}

// reset() from EXPIRED re-arms the stopwatch. Models replay_timer rule 6:
// expired → ACK arrives → re-arm.
TEST_F(SimStopwatchTest, ResetFromExpiredRearms) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  std::vector<uint64_t> wakeNs;

  auto task = [&]() -> SimTask {
    sw.start();
    co_await sw.expiration(); // wakes at t=100
    wakeNs.emplace_back(ctx_->nowNs());
    sw.reset(); // re-arm from t=100
    co_await sw.expiration(); // wakes at t=200
    wakeNs.emplace_back(ctx_->nowNs());
  };

  ctx_->scheduleNow([&]() { task(); });
  scheduler_->run();

  const std::vector<uint64_t> expected = {100, 200};
  EXPECT_EQ(wakeNs, expected);
}

// start() from RUNNING is a no-op. The deadline stays at the original
// schedule time, NOT the second start.
TEST_F(SimStopwatchTest, StartFromRunningIsNoOp) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  std::optional<uint64_t> wakeNs;

  auto task = [&]() -> SimTask {
    sw.start();
    co_await sw.expiration();
    wakeNs = ctx_->nowNs();
  };

  ctx_->scheduleNow([&]() { task(); });
  // At t=50, call start() again — should NOT re-schedule. Wake stays at 100.
  ctx_->schedule(Time::fromNs(50), [&]() { sw.start(); });

  scheduler_->run();

  ASSERT_TRUE(wakeNs.has_value());
  EXPECT_EQ(*wakeNs, 100);
}

// expiration() with await_ready=true: if already EXPIRED before the consumer
// awaits, the consumer resumes immediately without suspending.
TEST_F(SimStopwatchTest, ExpirationReadyImmediatelyIfAlreadyExpired) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(50));
  bool firstResumed = false;
  bool secondResumed = false;

  auto task = [&]() -> SimTask {
    sw.start();
    co_await sw.expiration();
    firstResumed = true;
    // sw is now EXPIRED. A second await must immediately resume.
    co_await sw.expiration();
    secondResumed = true;
  };

  ctx_->scheduleNow([&]() { task(); });
  scheduler_->run();

  EXPECT_TRUE(firstResumed);
  EXPECT_TRUE(secondResumed);
  EXPECT_EQ(ctx_->nowNs(), 50);
}

// SimWaitFirst integration: stopwatch wins the race; other operand cancelled.
TEST_F(SimStopwatchTest, SimWaitFirstStopwatchWins) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(50));
  SimCancellableDelay timeout;
  std::optional<std::size_t> winner;

  auto task = [&]() -> SimTask {
    sw.start();
    winner = co_await simWaitFirst(
        sw.expiration(), timeout.awaitDelay(ctx_.get(), Time::fromNs(100)));
  };

  ctx_->scheduleNow([&]() { task(); });
  scheduler_->run();

  ASSERT_TRUE(winner.has_value());
  EXPECT_EQ(*winner, 0u); // stopwatch.expiration() won
  EXPECT_EQ(ctx_->nowNs(), 50);
  EXPECT_TRUE(sw.expired());
}

// SimWaitFirst integration: other operand wins; loser-cancel destroys the
// stopwatch's suspended waiter via `cancelWaiterOnly()` but leaves the
// stopwatch RUNNING with its scheduled resume armed. Lets multiple
// stopwatches share a single `simWaitFirst` race without losers being
// silently stopped (the motivating use case: LLR's replay/data_age/
// pcs_lost_status timers share `txRunAdvance` as their awaiter while
// being armed by independent rule call sites).
TEST_F(SimStopwatchTest, SimWaitFirstLoserCancelKeepsStopwatchRunning) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  SimCancellableDelay timeout;
  std::optional<std::size_t> winner;

  auto task = [&]() -> SimTask {
    sw.start();
    winner = co_await simWaitFirst(
        sw.expiration(), timeout.awaitDelay(ctx_.get(), Time::fromNs(50)));
  };

  ctx_->scheduleNow([&]() { task(); });
  // Run past the timeout (50ns) but before the stopwatch's deadline (100ns)
  // so we can verify the stopwatch is still RUNNING after the loser-cancel.
  scheduler_->run(Time::fromNs(75));

  ASSERT_TRUE(winner.has_value());
  EXPECT_EQ(*winner, 1u); // timeout won
  EXPECT_EQ(ctx_->nowNs(), 75);
  EXPECT_TRUE(sw.running());
  EXPECT_FALSE(sw.expired());

  // Full owner-shutdown teardown so the destructor's XCHECKs are
  // satisfied — `cancelWaiterOnly()` alone is not sufficient when the
  // scheduled resume is still armed.
  sw.cancel();
}

// `expiration()` with no waiter present: when the scheduled resume fires
// while no one is awaiting, the stopwatch silently transitions to
// EXPIRED. A subsequent `co_await sw.expiration()` then resolves
// immediately via `await_ready=true`. This is the level-triggered
// semantic that `txRunAdvance` relies on to observe timer expirations
// that fired while the SM was parked in REPLAY/FLUSH.
TEST_F(SimStopwatchTest, ExpirationFiresSilentlyWhenNoWaiter) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  sw.start();
  // No awaiter installed; advance past deadline.
  scheduler_->run(Time::fromNs(150));
  EXPECT_FALSE(sw.running());
  EXPECT_TRUE(sw.expired());
  // No teardown needed — the resume lambda cleared `scheduledResume_`
  // and there is no `suspendedHandle_`, so both destructor XCHECKs pass.
}

// `cancelWaiterOnly()` from STOPPED with no waiter installed is a no-op:
// state stays STOPPED, no XCHECKs trip.
TEST_F(SimStopwatchTest, CancelWaiterOnlyFromStoppedWithNoWaiterIsNoop) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  sw.cancelWaiterOnly();
  EXPECT_FALSE(sw.running());
  EXPECT_FALSE(sw.expired());
}

// `cancelWaiterOnly()` from RUNNING with no waiter installed preserves
// the RUNNING state and the scheduled resume — confirming the method
// truly only touches the waiter slot. `sw.cancel()` at the end tears
// down the scheduled resume so the destructor passes.
TEST_F(SimStopwatchTest, CancelWaiterOnlyFromRunningWithNoWaiterIsNoop) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  sw.start();
  sw.cancelWaiterOnly();
  EXPECT_TRUE(sw.running());
  sw.cancel();
}

// `cancel()` full teardown: destroys the suspended waiter, cancels the
// scheduled resume, and transitions to STOPPED. The waiter never resumes
// (its frame is destroyed at the suspension point).
TEST_F(SimStopwatchTest, CancelFromRunningWithWaiterDestroysAndStops) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  bool resumed = false;

  auto task = [&]() -> SimTask {
    sw.start();
    co_await sw.expiration();
    resumed = true; // unreachable — frame destroyed by sw.cancel()
  };

  ctx_->scheduleNow([&]() { task(); });
  // Let the task suspend on `sw.expiration()`.
  scheduler_->run(Time::fromNs(50));
  EXPECT_TRUE(sw.running());

  // Full teardown — should destroy the suspended frame and stop the
  // stopwatch in one call.
  sw.cancel();
  EXPECT_FALSE(sw.running());
  EXPECT_FALSE(sw.expired());

  // Drain any remaining events — the resumed callback must not fire.
  scheduler_->run();
  EXPECT_FALSE(resumed);
}

// Multi-stopwatch `simWaitFirst` race: the loser's waiter is dropped via
// `cancelWaiterOnly()` so the losing stopwatch keeps RUNNING and fires
// EXPIRED at its own deadline. Mirrors the LLR `txRunAdvance` shape
// where the three spec timers are co-awaited and any winner must not
// silently stop the others.
TEST_F(SimStopwatchTest, MultiStopwatchRaceLeavesLosersRunning) {
  SimStopwatch swFast(ctx_.get(), Time::fromNs(50));
  SimStopwatch swSlow(ctx_.get(), Time::fromNs(200));
  std::optional<std::size_t> winner;

  auto task = [&]() -> SimTask {
    swFast.start();
    swSlow.start();
    winner = co_await simWaitFirst(swFast.expiration(), swSlow.expiration());
  };

  ctx_->scheduleNow([&]() { task(); });
  // Run past the fast deadline but well before the slow deadline so we
  // can observe `swSlow` still RUNNING after `swFast` wins.
  scheduler_->run(Time::fromNs(100));

  ASSERT_TRUE(winner.has_value());
  EXPECT_EQ(*winner, 0u); // swFast won
  EXPECT_TRUE(swFast.expired());
  EXPECT_TRUE(swSlow.running()); // not stopped by the loser-cancel
  EXPECT_FALSE(swSlow.expired());

  // Drain to the slow deadline; swSlow silently transitions to EXPIRED.
  scheduler_->run();
  EXPECT_TRUE(swSlow.expired());
}

// stop() before any waiter is installed: subsequent expiration() suspends
// indefinitely (no scheduled resume; stopwatch in STOPPED state). Verify by
// racing against a fixed timeout — the timeout wins.
TEST_F(SimStopwatchTest, ExpirationFromStoppedNeverWakes) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  SimCancellableDelay timeout;
  std::optional<std::size_t> winner;

  auto task = [&]() -> SimTask {
    // Never start; immediately race expiration against a timeout.
    winner = co_await simWaitFirst(
        sw.expiration(), timeout.awaitDelay(ctx_.get(), Time::fromNs(50)));
  };

  ctx_->scheduleNow([&]() { task(); });
  scheduler_->run();

  ASSERT_TRUE(winner.has_value());
  EXPECT_EQ(*winner, 1u); // timeout won; stopwatch never woke
  EXPECT_EQ(ctx_->nowNs(), 50);
}

// startOneShot: schedules a custom-duration delay, returns awaitable. After
// resolution the stopwatch is EXPIRED.
TEST_F(SimStopwatchTest, StartOneShotFiresAtCustomDuration) {
  // maxDuration is 100, but startOneShot uses 30 instead.
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  std::optional<uint64_t> wakeNs;

  auto task = [&]() -> SimTask {
    co_await sw.startOneShot(Time::fromNs(30));
    wakeNs = ctx_->nowNs();
  };

  ctx_->scheduleNow([&]() { task(); });
  scheduler_->run();

  ASSERT_TRUE(wakeNs.has_value());
  EXPECT_EQ(*wakeNs, 30);
  EXPECT_TRUE(sw.expired());
}

// The documented happy-path for iterative one-shot consumers: after each
// `co_await sw.startOneShot(...)` resolves the stopwatch is EXPIRED, and the
// next iteration calls `startOneShot(...)` again with a fresh duration. The
// EXPIRED → RUNNING transition via startOneShot() is permissive (in contrast
// to start(), which XCHECKs from EXPIRED).
TEST_F(SimStopwatchTest, StartOneShotIterativeReuseAcrossExpirations) {
  SimStopwatch sw(ctx_.get(), /*maxDuration=*/Time::fromNs(1));
  std::vector<uint64_t> wakeNs;

  auto task = [&]() -> SimTask {
    for (int i = 0; i < 3; ++i) {
      co_await sw.startOneShot(Time::fromNs(30));
      wakeNs.emplace_back(ctx_->nowNs());
    }
  };

  ctx_->scheduleNow([&]() { task(); });
  scheduler_->run();

  const std::vector<uint64_t> expected = {30, 60, 90};
  EXPECT_EQ(wakeNs, expected);
  EXPECT_TRUE(sw.expired());
}

// stop() before destruction: clean teardown, no XCHECK firing.
TEST_F(SimStopwatchTest, StopBeforeDestructionIsClean) {
  EXPECT_NO_FATAL_FAILURE({
    SimStopwatch sw(ctx_.get(), Time::fromNs(100));
    sw.start();
    sw.stop(); // remove scheduled resume; clean for destruction
    // Destructor's XCHECKs see no waiter and no queued resume.
  });
  // Scheduler had a queued resume that was cancelled before scheduler.run();
  // the run-queue should be empty so sim time stays at zero.
  EXPECT_EQ(ctx_->nowNs(), 0);
}

// Spec scenario: replay_timer rule 4 — LLR_ACK frees buffer → reset to zero.
// Models the actual LLR use case: a stopwatch + a "buffer advanced" signal
// raced via SimWaitFirst, with reset() on the signal-wake.
TEST_F(SimStopwatchTest, SpecReplayTimerRule4ResetOnAck) {
  SimStopwatch replayTimer(ctx_.get(), Time::fromNs(100));
  SimSignal bufferAdvanced(ctx_.get());

  // Simulate ACKs arriving at t=30 and t=70. Each ACK should reset the
  // timer — so the timer never expires while ACKs keep coming. The timer
  // expires only after the last ACK + maxDuration.
  std::vector<uint64_t> ackTimes = {30, 70};
  for (uint64_t t : ackTimes) {
    ctx_->schedule(Time::fromNs(t), [&]() { bufferAdvanced.signal(); });
  }

  std::optional<uint64_t> expiredNs;

  auto loop = [&]() -> SimTask {
    replayTimer.start();
    while (true) {
      const std::size_t winner = co_await simWaitFirst(
          replayTimer.expiration(), bufferAdvanced.wait());
      if (winner == 0) {
        // Timer expired.
        expiredNs = ctx_->nowNs();
        co_return;
      }
      // ACK arrived → reset timer.
      replayTimer.reset();
    }
  };

  ctx_->scheduleNow([&]() { loop(); });
  scheduler_->run();

  // Last ACK at t=70 → timer resets → expires at t=70+100=170.
  ASSERT_TRUE(expiredNs.has_value());
  EXPECT_EQ(*expiredNs, 170);
}

// pause() while RUNNING saves elapsed; resume() re-arms with the remainder.
// A 100ns stopwatch paused at t=30 and resumed at t=80 should expire at
// t=80 + (100-30) = 150.
TEST_F(SimStopwatchTest, PauseResumeCompletesOriginalBudget) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  std::optional<uint64_t> wakeNs;

  auto task = [&]() -> SimTask {
    sw.start();
    co_await sw.expiration();
    wakeNs = ctx_->nowNs();
  };

  ctx_->scheduleNow([&]() { task(); });
  ctx_->schedule(Time::fromNs(30), [&]() { sw.pause(); });
  ctx_->schedule(Time::fromNs(80), [&]() { sw.resume(); });

  scheduler_->run();

  ASSERT_TRUE(wakeNs.has_value());
  EXPECT_EQ(*wakeNs, 150); // 80 (resume) + 70 (remaining budget)
}

// Multiple pause/resume cycles accumulate elapsed correctly. A 100ns
// stopwatch paused at 20 (elapsed=20), resumed at 50 (resumes from
// elapsed=20), paused at 70 (elapsed=40), resumed at 90 (resumes from
// elapsed=40) should expire at 90 + (100-40) = 150.
TEST_F(SimStopwatchTest, MultiplePauseResumeCycles) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  std::optional<uint64_t> wakeNs;

  auto task = [&]() -> SimTask {
    sw.start();
    co_await sw.expiration();
    wakeNs = ctx_->nowNs();
  };

  ctx_->scheduleNow([&]() { task(); });
  ctx_->schedule(Time::fromNs(20), [&]() { sw.pause(); });
  ctx_->schedule(Time::fromNs(50), [&]() { sw.resume(); });
  ctx_->schedule(Time::fromNs(70), [&]() { sw.pause(); });
  ctx_->schedule(Time::fromNs(90), [&]() { sw.resume(); });

  scheduler_->run();

  ASSERT_TRUE(wakeNs.has_value());
  EXPECT_EQ(*wakeNs, 150); // 90 (resume) + 60 (remaining budget after 40 used)
}

// pause() with startOneShot — the per-iteration budget pattern used by
// the wire scheduler. A 4ns startOneShot paused at t=2, resumed at t=10
// should expire at t=10 + (4-2) = 12.
TEST_F(SimStopwatchTest, PauseResumeWorksWithStartOneShot) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(0));
  std::optional<uint64_t> wakeNs;

  auto task = [&]() -> SimTask {
    co_await sw.startOneShot(Time::fromNs(4));
    wakeNs = ctx_->nowNs();
  };

  ctx_->scheduleNow([&]() { task(); });
  ctx_->schedule(Time::fromNs(2), [&]() { sw.pause(); });
  ctx_->schedule(Time::fromNs(10), [&]() { sw.resume(); });

  scheduler_->run();

  ASSERT_TRUE(wakeNs.has_value());
  EXPECT_EQ(*wakeNs, 12);
}

// pause() is idempotent — calling pause() while already PAUSED is a no-op.
TEST_F(SimStopwatchTest, PauseFromPausedIsNoOp) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  std::optional<uint64_t> wakeNs;

  auto task = [&]() -> SimTask {
    sw.start();
    co_await sw.expiration();
    wakeNs = ctx_->nowNs();
  };

  ctx_->scheduleNow([&]() { task(); });
  ctx_->schedule(Time::fromNs(30), [&]() {
    sw.pause();
    sw.pause(); // no-op
    sw.pause(); // no-op
  });
  ctx_->schedule(Time::fromNs(80), [&]() { sw.resume(); });

  scheduler_->run();

  ASSERT_TRUE(wakeNs.has_value());
  EXPECT_EQ(*wakeNs, 150);
}

// reset() from PAUSED re-arms from zero — discards saved elapsed and
// targets the full maxDuration (in contrast to resume(), which
// completes the original budget from where pause() left off).
TEST_F(SimStopwatchTest, ResetFromPausedDiscardsElapsedAndRearms) {
  SimStopwatch sw(ctx_.get(), Time::fromNs(100));
  std::optional<uint64_t> wakeNs;

  auto task = [&]() -> SimTask {
    sw.start();
    co_await sw.expiration();
    wakeNs = ctx_->nowNs();
  };

  ctx_->scheduleNow([&]() { task(); });
  ctx_->schedule(Time::fromNs(30), [&]() { sw.pause(); });
  ctx_->schedule(Time::fromNs(50), [&]() { sw.reset(); });

  scheduler_->run();

  ASSERT_TRUE(wakeNs.has_value());
  EXPECT_EQ(*wakeNs, 150); // 50 (reset) + 100 (full maxDuration, NOT 70)
}

// Destroy-safe bare await: destroying a frame parked on a bare
// `co_await timer.expiration()` (inside a SimScopedTask, e.g. via reset())
// clears the stopwatch's suspended handle via `~SimStopwatch::Awaitable` so
// the still-scheduled resume can't wake the freed frame. The handle clear
// deliberately PRESERVES the scheduled resume (loser-keeps-counting), so the
// owner still cancels the timer (as `Llr::shutdown()` does) — proven by the
// timer still RUNNING after reset(), then STOPPED after cancel(), with no UAF
// on run() and no `~SimStopwatch` abort at scope exit.
TEST_F(SimStopwatchTest, BareAwaitDestroyedWhileSuspendedClearsWaiter) {
  SimStopwatch timer{ctx_.get(), Time::fromNs(1000)};
  timer.start();

  auto body = [&]() -> SimScopedTask { co_await timer.expiration(); };
  SimScopedTask task = body();
  ASSERT_TRUE(timer.running());

  task.reset();
  // ~Awaitable cleared the suspended handle but left the timer running
  // (scheduled resume preserved — the loser-keeps-counting contract).
  EXPECT_TRUE(timer.running());

  timer.cancel(); // owner clears the scheduled resume (Llr::shutdown analogue)
  EXPECT_FALSE(timer.running());

  scheduler_->run();
  EXPECT_EQ(ctx_->nowNs(), 0);
}

} // namespace packsim::coro
