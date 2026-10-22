/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimDelay.h"

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/kernel/coro/SimCancellableDelay.h"
#include "packsim/kernel/coro/SimTask.h"
#include "packsim/kernel/coro/oss_tests/fixtures/CoroTestFixture.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

using packsim::Scheduler;
using packsim::SimContext;
using packsim::Time;
using packsim::coro::SimCancellableDelay;
using packsim::coro::simDelay;
using packsim::coro::SimTask;
using packsim::coro::simYield;
using packsim::coro::tests::CoroTestFixture;

namespace packsim::coro {

using SimDelayTest = CoroTestFixture;

// Single delay advances time by the requested amount, exactly.
TEST_F(SimDelayTest, SingleDelayAdvancesTimeExactly) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "single", 0, nullptr);

  std::optional<uint64_t> timeAtResumeNs;

  auto task = [&]() -> SimTask {
    co_await simDelay(&ctx, Time::fromNs(123));
    timeAtResumeNs = ctx.nowNs();
  };

  ctx.scheduleNow([&]() { task(); });
  scheduler.run();

  ASSERT_TRUE(timeAtResumeNs.has_value());
  EXPECT_EQ(*timeAtResumeNs, 123);
  EXPECT_EQ(ctx.nowNs(), 123);
}

// Drain a queue of variable-size packets with cumulative serialization delay.
// This is the canonical TX-pump pattern.
TEST_F(SimDelayTest, CumulativeDelayInLoop) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "tx-pump", 0, nullptr);

  // Synthetic per-size delay multiplier — each "packet size unit" turns
  // into kPerSizeDelayNs ns of serialization delay.
  constexpr uint64_t kPerSizeDelayNs = 10;

  // Packet "sizes" (delay multipliers) and the expected wake times.
  const std::vector<uint64_t> packetSizes = {1, 3, 2, 5};
  std::vector<uint64_t> wakeTimesNs;
  wakeTimesNs.reserve(packetSizes.size());

  auto txPump = [&]() -> SimTask {
    for (const uint64_t size : packetSizes) {
      co_await simDelay(&ctx, Time::fromNs(kPerSizeDelayNs * size));
      wakeTimesNs.emplace_back(ctx.nowNs());
    }
  };

  ctx.scheduleNow([&]() { txPump(); });
  scheduler.run();

  // Compute expected cumulative wake times directly from packetSizes so
  // the assertion can't drift if packetSizes is edited.
  std::vector<uint64_t> expected;
  expected.reserve(packetSizes.size());
  uint64_t cumulativeNs = 0;
  for (const uint64_t size : packetSizes) {
    cumulativeNs += kPerSizeDelayNs * size;
    expected.emplace_back(cumulativeNs);
  }
  EXPECT_EQ(wakeTimesNs, expected);
}

// Zero-delay simDelay is equivalent to simYield: schedules immediately but
// goes through the event queue (preventing stack overflow).
TEST_F(SimDelayTest, ZeroDelayIsEquivalentToSimYield) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "zero", 0, nullptr);

  int counter = 0;
  auto task = [&]() -> SimTask {
    co_await simDelay(&ctx, Time::fromNs(0));
    counter++;
  };

  ctx.scheduleNow([&]() { task(); });
  scheduler.run();

  EXPECT_EQ(counter, 1);
  EXPECT_EQ(ctx.nowNs(), 0);
}

// Mixed delay + yield in one coroutine.
TEST_F(SimDelayTest, MixedDelayAndYield) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "mixed", 0, nullptr);

  std::vector<uint64_t> wakeTimesNs;

  auto task = [&]() -> SimTask {
    co_await simYield(&ctx);
    wakeTimesNs.emplace_back(ctx.nowNs());
    co_await simDelay(&ctx, Time::fromNs(50));
    wakeTimesNs.emplace_back(ctx.nowNs());
    co_await simYield(&ctx);
    wakeTimesNs.emplace_back(ctx.nowNs());
    co_await simDelay(&ctx, Time::fromNs(100));
    wakeTimesNs.emplace_back(ctx.nowNs());
  };

  ctx.scheduleNow([&]() { task(); });
  scheduler.run();

  const std::vector<uint64_t> expected = {0, 50, 50, 150};
  EXPECT_EQ(wakeTimesNs, expected);
}

// Microsecond delays compose correctly — verifies time-unit conversion.
TEST_F(SimDelayTest, MicrosecondsConvertCorrectly) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "us", 0, nullptr);

  std::optional<uint64_t> timeAtResumeNs;
  auto task = [&]() -> SimTask {
    co_await simDelay(&ctx, Time::fromUs(2));
    timeAtResumeNs = ctx.nowNs();
  };

  ctx.scheduleNow([&]() { task(); });
  scheduler.run();

  // 2us = 2000ns.
  ASSERT_TRUE(timeAtResumeNs.has_value());
  EXPECT_EQ(*timeAtResumeNs, 2000);
}

// SimCancellableDelay: default-constructed is safe to cancel (no-op).
TEST_F(SimDelayTest, CancelEmptyStateIsNoOp) {
  SimCancellableDelay handle;
  ASSERT_FALSE(handle.hasInFlight()); // pre-condition: nothing in flight
  handle.cancel(); // must not crash, no UB
  EXPECT_FALSE(handle.hasInFlight());
  // Calling again is also safe.
  handle.cancel();
  EXPECT_FALSE(handle.hasInFlight());
}

// SimCancellableDelay: scheduled then naturally fires. Handle is cleared.
TEST_F(SimDelayTest, CancellableNaturalFireClearsState) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "natural", 0, nullptr);

  SimCancellableDelay handle;
  bool hasResumed = false;

  auto task = [&]() -> SimTask {
    co_await handle.awaitDelay(&ctx, Time::fromNs(50));
    hasResumed = true;
  };

  ctx.scheduleNow([&]() { task(); });
  scheduler.run();

  EXPECT_TRUE(hasResumed);
  EXPECT_EQ(ctx.nowNs(), 50);
  EXPECT_FALSE(handle.hasInFlight()); // cleared on resume
}

// SimCancellableDelay: cancel before fire. Coroutine never resumes; the
// frame is destroyed by cancel(). Verified by checking the resume action
// did not run AND the scheduler dispatched only the scheduleNow + cancel
// events (never the cancelled resume).
TEST_F(SimDelayTest, CancelBeforeFirePreventsResume) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-before", 0, nullptr);

  const uint64_t baselineEvents = scheduler.getTotalEvents();
  SimCancellableDelay handle;
  bool hasResumed = false;

  auto task = [&]() -> SimTask {
    co_await handle.awaitDelay(&ctx, Time::fromNs(100));
    hasResumed = true; // must NOT execute
  };

  // Schedule the cancel to happen at t=50 (before the t=100 resume).
  ctx.scheduleNow([&]() { task(); });
  ctx.schedule(Time::fromNs(50), [&]() { handle.cancel(); });

  scheduler.run();

  EXPECT_FALSE(hasResumed);
  EXPECT_FALSE(handle.hasInFlight());
  // Direct evidence of the bit-identical-PDES-queue claim: only the
  // scheduleNow lambda + cancel lambda were executed. The cancelled t=100
  // resume must NOT have contributed to the executed-event count.
  constexpr uint64_t kExpectedDispatchedEvents =
      2; // scheduleNow + cancel-trigger, NOT the cancelled resume
  EXPECT_EQ(
      scheduler.getTotalEvents() - baselineEvents, kExpectedDispatchedEvents);
}

// SimCancellableDelay: cancel after natural fire is a no-op. The handle was
// already cleared by the resume callback; cancel() sees no recorded coroutine
// and skips.
TEST_F(SimDelayTest, CancelAfterFireIsNoOp) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-after", 0, nullptr);

  SimCancellableDelay handle;
  bool hasResumed = false;

  auto task = [&]() -> SimTask {
    co_await handle.awaitDelay(&ctx, Time::fromNs(50));
    hasResumed = true;
  };

  ctx.scheduleNow([&]() { task(); });
  // Cancel scheduled AFTER the natural resume time.
  ctx.schedule(Time::fromNs(100), [&]() { handle.cancel(); });

  scheduler.run();

  EXPECT_TRUE(hasResumed);
  EXPECT_EQ(ctx.nowNs(), 100); // scheduler ran past the cancel event too
  EXPECT_FALSE(handle.hasInFlight());
}

// SimCancellableDelay: re-await on the same handle in a loop. Each iteration
// the handle is updated with the new event handle / coroutine_handle.
TEST_F(SimDelayTest, ReuseStateInLoop) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "reuse", 0, nullptr);

  SimCancellableDelay handle;
  std::vector<uint64_t> wakeTimesNs;

  auto task = [&]() -> SimTask {
    for (int i = 0; i < 3; ++i) {
      co_await handle.awaitDelay(&ctx, Time::fromNs(25));
      wakeTimesNs.emplace_back(ctx.nowNs());
    }
  };

  ctx.scheduleNow([&]() { task(); });
  scheduler.run();

  const std::vector<uint64_t> expected = {25, 50, 75};
  EXPECT_EQ(wakeTimesNs, expected);
  // After natural completion of the loop, handle is cleared.
  EXPECT_FALSE(handle.hasInFlight());
}

// SimCancellableDelay: cancel mid-loop. The current iteration's resume is
// cancelled; the coroutine frame is destroyed. The loop counter records how
// far we got.
//
// PDES timing assumption: same-tick events fire in scheduling order; the
// cancel at t=125 is dispatched before the resume that was scheduled at
// t=150 by the third co_await. If a future PDES change reorders same-tick
// dispatch, ASSERT_GE/EXPECT_EQ below catch it (and the message hints at
// "scheduler timing regression" so the cause is obvious).
TEST_F(SimDelayTest, CancelMidLoopStopsLoop) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-mid", 0, nullptr);

  SimCancellableDelay handle;
  int iterations = 0;
  // Modest cap above the expected count (3) — terminates fast on regression
  // if cancel() ever silently fails to interrupt the coroutine.
  constexpr int kMaxIterations = 10;

  auto task = [&]() -> SimTask {
    while (iterations < kMaxIterations) {
      iterations++;
      co_await handle.awaitDelay(&ctx, Time::fromNs(50));
    }
  };

  ctx.scheduleNow([&]() { task(); });
  // Cancel after ~125ns: should let 2 iterations complete (at 50 and 100ns),
  // then cancel the 3rd iteration's pending resume (scheduled at 150ns).
  ctx.schedule(Time::fromNs(125), [&]() { handle.cancel(); });

  scheduler.run();

  // First call: iterations becomes 1, await scheduled at 50.
  // Resume at 50: iterations becomes 2, await scheduled at 100.
  // Resume at 100: iterations becomes 3, await scheduled at 150.
  // Cancel at 125: cancels the 150 resume.
  ASSERT_LT(iterations, kMaxIterations)
      << "Loop hit safety cap — cancel() did not stop the coroutine";
  ASSERT_GE(iterations, 2)
      << "Cancellation arrived before at least 2 resumes fired — likely a "
         "scheduler timing regression";
  EXPECT_EQ(iterations, 3);
}

// SimCancellableDelay: zero-delay path with a cancel scheduled BEFORE the
// task. Verifies the cancel is a no-op against an empty handle, and the
// task's own zero-delay resume still fires normally afterwards.
//
// Companion to CancellableZeroDelayCancelAfterTaskArmsIt below, which
// exercises the reverse same-tick ordering (task armed first, cancel runs
// after, in time to remove the queued resume).
TEST_F(SimDelayTest, CancellableZeroDelayCancelScheduledBeforeTaskIsNoOp) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "zero-cancel", 0, nullptr);

  SimCancellableDelay handle;
  bool hasResumed = false;

  auto task = [&]() -> SimTask {
    co_await handle.awaitDelay(&ctx, Time::fromNs(0));
    hasResumed = true;
  };

  // Both scheduled at t=0; cancel was scheduled FIRST (before task()).
  ctx.scheduleNow([&]() { handle.cancel(); });
  ctx.scheduleNow([&]() { task(); });
  scheduler.run();

  // Order matters: cancel was scheduled before task(), but task() schedules
  // its resume only after running. So cancel runs first (no-op since handle
  // is empty), then task() runs and schedules resume at t=0, which fires.
  EXPECT_TRUE(hasResumed);
}

// SimCancellableDelay: same-tick zero-delay cancel-after-arm. The cancel
// is scheduled BEFORE the task in the same in-running PDES tick, so by the
// time the cancel callback runs, the handle has an in-flight zero-delay
// resume queued at the same logical tick. cancel() must remove the queued
// resume from PDES AND destroy the suspended frame, even though both the
// cancel and the resume are at t=0 — the harder "cancel races a same-tick
// zero-delay resume" case that exercises the dispatcher's isActive-skip
// path.
//
// CancelBeforeFirePreventsResume covers the easier different-tick case
// (cancel at t=50 vs resume at t=100); this test covers the same-tick
// variant where PDES insertion-order tie-break is in play.
//
// Why a wrapper event: pre-run scheduleNow uses kDefaultCtxIdx for sortId
// (largest possible value), but in-running ctx-shortcut scheduling uses
// ctx->ctxIdx (small). Mixing them means in-running events always tie-break
// before pre-run events. To exercise "cancel runs before same-tick resume",
// both must be scheduled from the same in-running context so they share
// the in-context ctxIdx and tie-break by nextEventId. The wrapper schedules
// cancel first (sortId N), then runs task() which schedules resume via
// await_suspend's in-context shortcut (sortId N+1) — cancel pops first.
TEST_F(SimDelayTest, CancellableZeroDelayCancelAfterTaskArmsIt) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "zero-cancel-after", 0, nullptr);

  const uint64_t baselineEvents = scheduler.getTotalEvents();
  SimCancellableDelay handle;
  bool hasResumed = false;

  auto task = [&]() -> SimTask {
    co_await handle.awaitDelay(&ctx, Time::fromNs(0));
    hasResumed = true; // must NOT execute
  };

  ctx.scheduleNow([&]() {
    // Order matters: schedule cancel FIRST (smaller in-context sortId),
    // then arm the handle by starting the task (which in turn schedules
    // the same-tick resume with a larger sortId via await_suspend).
    ctx.scheduleNow([&]() {
      EXPECT_TRUE(handle.hasInFlight())
          << "task ran first; handle should be armed before cancel";
      handle.cancel();
      EXPECT_FALSE(handle.hasInFlight())
          << "cancel must clear the handle synchronously";
    });
    task();
  });
  scheduler.run();

  EXPECT_FALSE(hasResumed) << "cancel must skip the same-tick resume";
  // Direct evidence: wrapper + cancel = 2 executed events. The cancelled
  // t=0 resume must NOT have contributed to the executed-event count.
  constexpr uint64_t kExpectedDispatchedEvents =
      2; // wrapper + cancel, NOT the cancelled same-tick resume
  EXPECT_EQ(
      scheduler.getTotalEvents() - baselineEvents, kExpectedDispatchedEvents);
}

// Calling cancel() from inside the coroutine body after being resumed must
// be a no-op — natural-fire clears the handle BEFORE resuming.
TEST_F(SimDelayTest, CancelFromInsideCoroutineBodyAfterResumeIsNoOp) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-inside", 0, nullptr);
  SimCancellableDelay handle;
  int resumeCount = 0;

  auto task = [&]() -> SimTask {
    co_await handle.awaitDelay(&ctx, Time::fromNs(50));
    resumeCount++;
    // EXPECT_* (not ASSERT_*): ASSERT_* expands to a bare `return;` which
    // is invalid inside a coroutine body (must be `co_return`).
    EXPECT_FALSE(
        handle.hasInFlight()); // pre-condition: cleared by natural fire
    handle.cancel();
    EXPECT_FALSE(handle.hasInFlight());
  };

  ctx.scheduleNow([&]() { task(); });
  scheduler.run();

  EXPECT_EQ(resumeCount, 1);
  EXPECT_FALSE(handle.hasInFlight());
}

// folly::not_null contract: factory functions throw std::invalid_argument
// when a runtime-null pointer is passed for a not_null parameter (folly's
// default null handler — see folly/memory/not_null-inl.h's
// default_null_handler::handle_throw). The throw fires in the
// parameter-binding frame (before our factory body runs), so these tests
// are belt-and-suspenders against a future signature regression (e.g.,
// someone "simplifies" the signature to a raw `T*`). The `std::ignore =`
// suppresses the factory's [[nodiscard]] in the unreachable path after
// not_null's throw.
TEST_F(SimDelayTest, SimDelayRejectsNullCtx) {
  SimContext* nullCtx = nullptr;
  EXPECT_THROW(
      std::ignore = simDelay(nullCtx, Time::fromNs(1)), std::invalid_argument);
  EXPECT_THROW(std::ignore = simYield(nullCtx), std::invalid_argument);
}

TEST_F(SimDelayTest, SimCancellableDelayRejectsNullCtx) {
  SimContext* nullCtx = nullptr;
  SimCancellableDelay delay;
  EXPECT_THROW(
      std::ignore = delay.awaitDelay(nullCtx, Time::fromNs(1)),
      std::invalid_argument);
}

} // namespace packsim::coro
