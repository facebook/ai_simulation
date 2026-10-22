/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/WaiterState.h"

#include <folly/Utility.h>
#include <gtest/gtest.h>

#include "packsim/kernel/coro/SimTask.h"
#include "packsim/kernel/coro/oss_tests/fixtures/CoroTestFixture.h"
#include "packsim/kernel/coro/oss_tests/fixtures/WaiterTestAwaitable.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"

using packsim::Scheduler;
using packsim::SimContext;
using packsim::coro::ResumeMode;
using packsim::coro::SimTask;
using packsim::coro::detail::WaiterState;
using packsim::coro::tests::CoroTestFixture;
using packsim::coro::tests::WaiterTestAwaitable;

namespace packsim::coro {

using WaiterStateTest = CoroTestFixture;

// Default-constructed WaiterState has no installed waiter.
TEST_F(WaiterStateTest, ConstructionDoesNotInstallWaiter) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "construction", 0, nullptr);
  WaiterState waiter(&ctx, ResumeMode::DIRECT);
  EXPECT_FALSE(waiter.has());
}

// install() makes has() report true; resume via notify() clears it.
TEST_F(WaiterStateTest, InstallSetsHasAndNotifyClearsIt) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "install-direct", 0, nullptr);
  WaiterState waiter(&ctx, ResumeMode::DIRECT);
  bool resumed = false;

  auto task = [&]() -> SimTask {
    co_await WaiterTestAwaitable{waiter, resumed};
  };

  ctx.scheduleNow([&]() {
    task();
    EXPECT_TRUE(waiter.has()) << "task suspended; waiter should be installed";
    waiter.notify();
    EXPECT_FALSE(waiter.has())
        << "direct notify resumes synchronously and clears state";
    EXPECT_TRUE(resumed);
  });
  scheduler.run();
}

// Direct-mode notify resumes the coroutine synchronously inside notify(),
// without going through the PDES queue. Verified by checking resumed-flag
// state at the call site immediately after notify() returns.
TEST_F(WaiterStateTest, NotifyDirectModeResumesSynchronously) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "direct-resume", 0, nullptr);
  WaiterState waiter(&ctx, ResumeMode::DIRECT);
  bool resumed = false;

  auto task = [&]() -> SimTask {
    co_await WaiterTestAwaitable{waiter, resumed};
  };

  const uint64_t baselineEvents = scheduler.getTotalEvents();
  ctx.scheduleNow([&]() {
    task();
    EXPECT_FALSE(resumed) << "must not have resumed before notify";
    waiter.notify();
    EXPECT_TRUE(resumed) << "direct mode resumes inside notify()";
  });
  scheduler.run();

  // Only the wrapper scheduleNow event ran; direct resume did NOT add an
  // additional PDES event for the resume.
  EXPECT_EQ(scheduler.getTotalEvents() - baselineEvents, 1u);
}

// Deferred-mode notify schedules the resume through PDES rather than
// resuming inline. The resumed flag flips only after scheduler.run()
// dispatches the queued event.
TEST_F(WaiterStateTest, NotifyDeferredModeResumesViaScheduler) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "deferred-resume", 0, nullptr);
  WaiterState waiter(&ctx, ResumeMode::DEFERRED);
  bool resumed = false;

  auto task = [&]() -> SimTask {
    co_await WaiterTestAwaitable{waiter, resumed};
  };

  const uint64_t baselineEvents = scheduler.getTotalEvents();
  ctx.scheduleNow([&]() {
    task();
    waiter.notify();
    EXPECT_FALSE(resumed) << "deferred mode does NOT resume inside notify()";
    EXPECT_TRUE(waiter.has())
        << "waiter handle remains until the deferred resume fires";
  });
  scheduler.run();

  EXPECT_TRUE(resumed) << "deferred resume must fire when scheduler runs it";
  EXPECT_FALSE(waiter.has()) << "resume callback clears the handle";
  // Wrapper scheduleNow + the deferred resume = 2 events.
  EXPECT_EQ(scheduler.getTotalEvents() - baselineEvents, 2u);
}

// notify() with no installed waiter is a no-op. Catches a regression where
// a stray notify() before install would crash or schedule a phantom event.
TEST_F(WaiterStateTest, NotifyWithNoWaiterIsNoOp) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "notify-empty", 0, nullptr);

  for (const ResumeMode mode : {ResumeMode::DIRECT, ResumeMode::DEFERRED}) {
    WaiterState waiter(&ctx, mode);
    const uint64_t baselineEvents = scheduler.getTotalEvents();
    waiter.notify(); // must not crash, must not schedule anything
    scheduler.run();
    EXPECT_FALSE(waiter.has());
    EXPECT_EQ(scheduler.getTotalEvents() - baselineEvents, 0u)
        << "no PDES events should be scheduled by notify-on-empty (mode="
        << folly::to_underlying(mode) << ")";
  }
}

// cancel() on a fresh WaiterState is safe; calling it twice is also safe.
TEST_F(WaiterStateTest, CancelEmptyIsNoOp) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-empty", 0, nullptr);
  WaiterState waiter(&ctx, ResumeMode::DIRECT);
  EXPECT_FALSE(waiter.has());
  waiter.cancel(); // safe on empty state
  EXPECT_FALSE(waiter.has());
  waiter.cancel(); // idempotent
  EXPECT_FALSE(waiter.has());
}

// cancel() on an installed waiter destroys the suspended frame and clears
// state without resuming. Verified via the resumed flag staying false.
TEST_F(WaiterStateTest, CancelDestroysSuspendedFrameAndClearsState) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-installed", 0, nullptr);
  WaiterState waiter(&ctx, ResumeMode::DIRECT);
  bool resumed = false;

  auto task = [&]() -> SimTask {
    co_await WaiterTestAwaitable{waiter, resumed};
  };

  task(); // eager-start: installs the waiter and suspends
  EXPECT_TRUE(waiter.has());
  waiter.cancel();
  EXPECT_FALSE(waiter.has()) << "cancel must clear waiter state synchronously";
  EXPECT_FALSE(resumed) << "cancel must not resume the coroutine";
}

// cancel() during the deferred-resume pending window removes the queued
// event from PDES. Without this race-safe removal, the queued lambda would
// fire against a destroyed coroutine frame (UAF).
TEST_F(WaiterStateTest, CancelRemovesQueuedDeferredResumeFromPdes) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-deferred", 0, nullptr);
  WaiterState waiter(&ctx, ResumeMode::DEFERRED);
  bool resumed = false;

  auto task = [&]() -> SimTask {
    co_await WaiterTestAwaitable{waiter, resumed};
  };

  const uint64_t baselineEvents = scheduler.getTotalEvents();
  ctx.scheduleNow([&]() {
    task();
    waiter.notify(); // queues a deferred resume
    EXPECT_TRUE(waiter.has())
        << "deferred resume queued; waiter still installed";
    waiter.cancel(); // must remove the queued event AND destroy the frame
    EXPECT_FALSE(waiter.has());
  });
  scheduler.run();

  EXPECT_FALSE(resumed) << "cancel must skip the queued deferred resume";
  // Only the wrapper event ran; the deferred resume was dequeued by cancel.
  EXPECT_EQ(scheduler.getTotalEvents() - baselineEvents, 1u);
}

// Multiple deferred-mode notify() calls before the first deferred resume
// fires are absorbed into a single resume event — the consumer picks up
// any remaining state after waking. Catches a regression where each
// notify() would queue a duplicate resume and crash on second dispatch.
TEST_F(WaiterStateTest, MultipleNotifyInDeferredAbsorbsToOneResume) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "absorb", 0, nullptr);
  WaiterState waiter(&ctx, ResumeMode::DEFERRED);
  bool resumed = false;

  auto task = [&]() -> SimTask {
    co_await WaiterTestAwaitable{waiter, resumed};
  };

  const uint64_t baselineEvents = scheduler.getTotalEvents();
  ctx.scheduleNow([&]() {
    task();
    waiter.notify();
    waiter.notify();
    waiter.notify(); // three notifies, only one resume should be queued
  });
  scheduler.run();

  EXPECT_TRUE(resumed);
  // Wrapper + exactly one deferred resume = 2 events (NOT 4).
  EXPECT_EQ(scheduler.getTotalEvents() - baselineEvents, 2u);
}

} // namespace packsim::coro
