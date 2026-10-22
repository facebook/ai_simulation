/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimSignal.h"

#include <cstdint>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/kernel/coro/SimDelay.h"
#include "packsim/kernel/coro/SimScopedTask.h"
#include "packsim/kernel/coro/SimTask.h"
#include "packsim/kernel/coro/oss_tests/fixtures/CoroTestFixture.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

using packsim::Scheduler;
using packsim::SimContext;
using packsim::Time;
using packsim::coro::ResumeMode;
using packsim::coro::simDelay;
using packsim::coro::SimScopedTask;
using packsim::coro::SimSignal;
using packsim::coro::SimTask;
using packsim::coro::tests::CoroTestFixture;

namespace packsim::coro {

using SimSignalTest = CoroTestFixture;

// Reactive loop: signal arrives, coroutine resumes once per signal.
TEST_F(SimSignalTest, ReactiveLoop) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "reactive", 0, nullptr);

  SimSignal sig(&ctx);
  int wakeups = 0;

  // Producer: signals every 100ns, 5 times.
  auto producer = [&]() -> SimTask {
    for (int i = 0; i < 5; ++i) {
      co_await simDelay(&ctx, Time::fromNs(100));
      sig.signal();
    }
  };

  // Consumer: waits for each signal.
  auto consumer = [&]() -> SimTask {
    for (int i = 0; i < 5; ++i) {
      co_await sig.wait();
      wakeups++;
    }
  };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.scheduleNow([&]() { producer(); });
  scheduler.run();

  EXPECT_EQ(wakeups, 5);
  EXPECT_EQ(ctx.nowNs(), 500);
}

// Signal accumulation: producer fires 3 times before consumer waits.
// All 3 are consumed without suspending.
TEST_F(SimSignalTest, AccumulatedSignals) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "accum", 0, nullptr);

  SimSignal sig(&ctx);
  int wakeups = 0;

  // Producer fires immediately at t=0.
  sig.signal();
  sig.signal();
  sig.signal();
  EXPECT_EQ(sig.pendingSignals(), 3);

  // Consumer wakes up at t=200ns and consumes all 3 without further delay.
  auto consumer = [&]() -> SimTask {
    co_await simDelay(&ctx, Time::fromNs(200));
    for (int i = 0; i < 3; ++i) {
      co_await sig.wait();
      wakeups++;
    }
  };

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  EXPECT_EQ(wakeups, 3);
  EXPECT_EQ(sig.pendingSignals(), 0);
  // Consumer wakes once at 200ns; subsequent wait()s return immediately.
  EXPECT_EQ(ctx.nowNs(), 200);
}

// Direct resume: signal() inside the producer's callback resumes the
// consumer synchronously, all in one PDES event tick.
TEST_F(SimSignalTest, DirectResumeKeepsSameTick) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "direct", 0, nullptr);

  SimSignal sig(&ctx, ResumeMode::DIRECT);
  std::vector<uint64_t> wakeTimes;

  // Producer fires at t=50ns.
  ctx.schedule(Time::fromNs(50), [&]() { sig.signal(); });

  auto consumer = [&]() -> SimTask {
    co_await sig.wait();
    wakeTimes.emplace_back(ctx.nowNs());
  };

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  ASSERT_EQ(wakeTimes.size(), 1u);
  // Direct resume: consumer wakes at the same tick the signal fires.
  EXPECT_EQ(wakeTimes[0], 50);
}

// Deferred resume (ResumeMode::DEFERRED): signal() schedules a scheduleNow
// resume — same logical time, but a separate event in the queue.
TEST_F(SimSignalTest, DeferredResumeAlsoSameTick) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "deferred", 0, nullptr);

  SimSignal sig(&ctx, ResumeMode::DEFERRED);
  std::optional<uint64_t> wakeTime;

  ctx.schedule(Time::fromNs(50), [&]() { sig.signal(); });

  auto consumer = [&]() -> SimTask {
    co_await sig.wait();
    wakeTime = ctx.nowNs();
  };

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  ASSERT_TRUE(wakeTime.has_value());
  EXPECT_EQ(*wakeTime, 50);
}

// Cancellable timer pattern: generation counter inside the coroutine
// allows the loop to exit when an external cancel happens.
TEST_F(SimSignalTest, GenerationCounterCancellation) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel", 0, nullptr);

  SimSignal sig(&ctx);
  int currentGen = 1;
  int iterations = 0;

  auto periodicLoop = [&](const int myGen) -> SimTask {
    while (currentGen == myGen) {
      iterations++;
      co_await simDelay(&ctx, Time::fromNs(50));
    }
  };

  ctx.scheduleNow([&]() { periodicLoop(1); });
  ctx.schedule(Time::fromNs(125), [&]() { currentGen = 2; });

  scheduler.run();

  // Iterations at t=0, t=50, t=100. At t=125 cancel; t=150 wakes, sees
  // generation mismatch, exits loop.
  EXPECT_EQ(iterations, 3);
}

// pendingSignals() reports accurate count across consumer drain.
TEST_F(SimSignalTest, PendingSignalsReportedAccurately) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "pending", 0, nullptr);

  SimSignal sig(&ctx);
  EXPECT_EQ(sig.pendingSignals(), 0);

  for (int i = 1; i <= 5; ++i) {
    sig.signal();
    EXPECT_EQ(sig.pendingSignals(), i);
  }

  // Consume 3 of 5.
  int consumed = 0;
  auto consumer = [&]() -> SimTask {
    for (int i = 0; i < 3; ++i) {
      co_await sig.wait();
      consumed++;
    }
  };

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  EXPECT_EQ(consumed, 3);
  EXPECT_EQ(sig.pendingSignals(), 2);
}

// hasWaiter() reflects suspension state.
TEST_F(SimSignalTest, HasWaiterReflectsSuspensionState) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "haswaiter", 0, nullptr);

  SimSignal sig(&ctx);
  EXPECT_FALSE(sig.hasWaiter());

  auto consumer = [&]() -> SimTask { co_await sig.wait(); };

  // Pre-signal: consumer suspends, hasWaiter becomes true.
  ctx.scheduleNow([&]() { consumer(); });
  // Step the scheduler so the coroutine starts and suspends.
  ctx.schedule(Time::fromNs(10), [&]() {
    EXPECT_TRUE(sig.hasWaiter());
    sig.signal();
  });
  ctx.schedule(Time::fromNs(20), [&]() { EXPECT_FALSE(sig.hasWaiter()); });

  scheduler.run();
}

// Constructor accepts ResumeMode parameter; both modes work.
TEST_F(SimSignalTest, ConstructorDirectResumeParam) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "ctor", 0, nullptr);

  SimSignal sigDirect(&ctx, ResumeMode::DIRECT);
  SimSignal sigDeferred(&ctx, ResumeMode::DEFERRED);

  // Both should accept signals without crashing.
  sigDirect.signal();
  sigDeferred.signal();

  EXPECT_EQ(sigDirect.pendingSignals(), 1);
  EXPECT_EQ(sigDeferred.pendingSignals(), 1);
}

// Default ResumeMode::DIRECT: resume happens synchronously inside signal().
TEST_F(SimSignalTest, DefaultDirectResumeIsTrue) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "default", 0, nullptr);

  SimSignal sig(&ctx);
  bool resumedInSignalCallback = false;

  auto consumer = [&]() -> SimTask {
    co_await sig.wait();
    resumedInSignalCallback = true;
  };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() {
    EXPECT_FALSE(resumedInSignalCallback);
    sig.signal();
    EXPECT_TRUE(resumedInSignalCallback);
  });

  scheduler.run();
}

// With ResumeMode::DEFERRED, cancel() must remove the in-flight deferred
// resume from PDES AND destroy the suspended frame — otherwise the
// scheduler fires the callback on a freed handle (use-after-destroy).
TEST_F(SimSignalTest, CancelDuringInFlightDeferredResume) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-in-flight", 0, nullptr);

  SimSignal sig(&ctx, ResumeMode::DEFERRED);
  bool consumerResumed = false;

  auto consumer = [&]() -> SimTask {
    co_await sig.wait();
    consumerResumed = true;
  };

  // Start the consumer so it suspends on wait().
  ctx.scheduleNow([&]() { consumer(); });

  ctx.schedule(Time::fromNs(10), [&]() {
    EXPECT_TRUE(sig.hasWaiter());
    sig.signal();
    EXPECT_TRUE(sig.hasWaiter())
        << "deferred signal() keeps waiter_ set for cancel() to find";
    sig.cancel();
    EXPECT_FALSE(sig.hasWaiter());
  });

  scheduler.run();
  EXPECT_FALSE(consumerResumed);
}

// cancel() with no in-flight waiter is a safe no-op. Covers the default
// Event::cancel path and the empty-waiter_ branch.
TEST_F(SimSignalTest, CancelWithoutWaiterIsNoOp) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-noop", 0, nullptr);

  SimSignal sig(&ctx, ResumeMode::DEFERRED);

  // No waiter, no scheduled event — cancel() should be a pure no-op.
  sig.cancel();
  EXPECT_FALSE(sig.hasWaiter());

  // Still valid to wait/signal after cancel().
  int wakeups = 0;
  auto consumer = [&]() -> SimTask {
    co_await sig.wait();
    wakeups++;
  };
  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() { sig.signal(); });
  scheduler.run();

  EXPECT_EQ(wakeups, 1);
}

// cancel() is idempotent — safe to call multiple times in sequence. The
// second call must see cleared state and do nothing.
TEST_F(SimSignalTest, CancelIsIdempotent) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-idem", 0, nullptr);

  SimSignal sig(&ctx, ResumeMode::DEFERRED);

  auto consumer = [&]() -> SimTask { co_await sig.wait(); };
  ctx.scheduleNow([&]() { consumer(); });

  ctx.schedule(Time::fromNs(10), [&]() {
    sig.signal();
    sig.cancel();
    sig.cancel(); // second call must not crash or double-destroy
    sig.cancel(); // third for good measure
    EXPECT_FALSE(sig.hasWaiter());
  });

  scheduler.run();
}

// After a natural-fire deferred resume, cancel() must be a safe no-op —
// scheduledResume_ was cleared by the fire lambda, so cancel() on the
// default event handle is a no-op and waiter_ is empty so no destroy path.
TEST_F(SimSignalTest, CancelAfterNaturalFireIsNoOp) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-after-fire", 0, nullptr);

  SimSignal sig(&ctx, ResumeMode::DEFERRED);
  bool consumerResumed = false;

  auto consumer = [&]() -> SimTask {
    co_await sig.wait();
    consumerResumed = true;
  };
  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() { sig.signal(); });
  // Cancel AFTER the scheduled lambda has had a chance to fire.
  ctx.schedule(Time::fromNs(20), [&]() {
    EXPECT_TRUE(consumerResumed) << "deferred resume should have fired by now";
    sig.cancel(); // must be safe: scheduledResume_ already cleared
  });

  scheduler.run();
  EXPECT_TRUE(consumerResumed);
}

// Pins the deferred-resume self-clear ordering: the fire lambda clears
// waiter_/scheduledResume_ BEFORE h.resume(), so the resumed coroutine can
// immediately co_await the same primitive again without tripping the
// single-waiter XCHECK in await_suspend.
TEST_F(SimSignalTest, NestedAwaitAfterResumeReinstallsWaiter) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "nested-await", 0, nullptr);

  SimSignal sig(&ctx, ResumeMode::DEFERRED);
  int wakeups = 0;

  auto consumer = [&]() -> SimTask {
    co_await sig.wait(); // first wait — suspends, then deferred-resumes
    wakeups++;
    co_await sig.wait(); // immediately re-await; must not trip XCHECK
    wakeups++;
  };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() { sig.signal(); });
  ctx.schedule(Time::fromNs(20), [&]() { sig.signal(); });

  scheduler.run();

  EXPECT_EQ(wakeups, 2);
}

// Pure teardown path: consumer suspends on wait(), producer never signals,
// owner calls cancel() to destroy the primitive. This is the scenario
// `SimSignal::cancel()`'s documentation describes.
TEST_F(SimSignalTest, CancelWithSuspendedWaiterNoSignalDestroysFrame) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "teardown-no-signal", 0, nullptr);

  SimSignal sig(&ctx, ResumeMode::DEFERRED);
  bool consumerResumed = false;

  auto consumer = [&]() -> SimTask {
    co_await sig.wait();
    consumerResumed = true;
  };

  ctx.scheduleNow([&]() { consumer(); });
  // No signal() ever fires. Owner just tears down mid-flight.
  ctx.schedule(Time::fromNs(10), [&]() {
    ASSERT_TRUE(sig.hasWaiter());
    sig.cancel();
    EXPECT_FALSE(sig.hasWaiter());
  });

  scheduler.run();
  EXPECT_FALSE(consumerResumed);
}

// Multiple signal()s while a deferred resume is already pending must
// schedule ONE resume, not N (otherwise the second lambda fires on a
// destroyed frame). Guarded by scheduledResume_.isActive() in signal().
TEST_F(SimSignalTest, DeferredMultipleSignalsScheduleOnlyOneResume) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "multi-signal", 0, nullptr);

  SimSignal sig(&ctx, ResumeMode::DEFERRED);
  int wakeups = 0;

  auto consumer = [&]() -> SimTask {
    co_await sig.wait();
    wakeups++;
  };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() {
    sig.signal();
    sig.signal();
    sig.signal();
    sig.signal();
  });

  scheduler.run();

  EXPECT_EQ(wakeups, 1);
  EXPECT_EQ(sig.pendingSignals(), 3);
}

// SimSignal accumulates signals across cancel; a fresh waiter drains
// previously-accumulated pending signals via await_ready without
// suspending. Pins the documented "reuse after accumulation" path of
// SimSignal::cancel — see the cancel() docstring's "Does NOT clear
// pendingSignals_" paragraph.
TEST_F(SimSignalTest, FreshWaiterDrainsAccumulatedPendingSignals) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "fresh-waiter-drains-pending", 0, nullptr);

  SimSignal sig(&ctx);
  // Accumulate 3 signals before any waiter is installed.
  sig.signal();
  sig.signal();
  sig.signal();
  EXPECT_EQ(sig.pendingSignals(), 3u);
  EXPECT_FALSE(sig.hasWaiter());

  // Cancel on a no-waiter SimSignal is a safe no-op AND does not clear
  // pendingSignals_ (this is the documented reuse contract).
  sig.cancel();
  EXPECT_EQ(sig.pendingSignals(), 3u);

  // Fresh waiter consumes all 3 accumulated signals via await_ready.
  // Each co_await sig.wait() returns synchronously without suspending —
  // verified by checking that ctx.nowNs() never advances.
  int wakeups = 0;
  auto consumer = [&]() -> SimTask {
    for (int i = 0; i < 3; ++i) {
      co_await sig.wait();
      wakeups++;
    }
  };

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  EXPECT_EQ(wakeups, 3);
  EXPECT_EQ(sig.pendingSignals(), 0u);
  // No simulation time elapsed: every wait() returned via await_ready.
  EXPECT_EQ(ctx.nowNs(), 0);
}

// Destroy-safe bare await: destroying a frame parked on a bare
// `co_await sig.wait()` (inside a SimScopedTask, e.g. via reset()) must detach
// the waiter via `~SimSignal::Awaitable` — clearing the installed handle so
// `~SimSignal` does not abort. This is the bare-await counterpart to the
// simWaitFirst-harness teardown coverage.
TEST_F(SimSignalTest, BareAwaitDestroyedWhileSuspendedDetaches) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "bare-detach", 0, nullptr);

  SimSignal sig(&ctx);

  auto body = [&]() -> SimScopedTask { co_await sig.wait(); };
  SimScopedTask task = body();
  ASSERT_TRUE(sig.hasWaiter());

  task.reset();
  EXPECT_FALSE(sig.hasWaiter());
}

} // namespace packsim::coro
