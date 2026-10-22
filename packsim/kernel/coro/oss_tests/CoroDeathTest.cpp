/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <coroutine>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>

#include <gtest/gtest.h>

#include "packsim/kernel/coro/ResumeMode.h"
#include "packsim/kernel/coro/SimCancellableDelay.h"
#include "packsim/kernel/coro/SimCoordRoutedBarrier.h"
#include "packsim/kernel/coro/SimEvent.h"
#include "packsim/kernel/coro/SimFuture.h"
#include "packsim/kernel/coro/SimQueue.h"
#include "packsim/kernel/coro/SimSignal.h"
#include "packsim/kernel/coro/SimStopwatch.h"
#include "packsim/kernel/coro/SimTask.h"
#include "packsim/kernel/coro/WaiterState.h"
#include "packsim/kernel/coro/oss_tests/fixtures/CoroTestFixture.h"
#include "packsim/kernel/coro/oss_tests/fixtures/WaiterTestAwaitable.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

using packsim::coro::detail::WaiterState;
using packsim::coro::tests::CoroTestFixture;
using packsim::coro::tests::WaiterTestAwaitable;

namespace packsim::coro {

// Every death test of the coroutine primitives lives in this binary.
//
// Each one pins a contract that a primitive enforces by ending the process: an
// always-on XCHECK in a destructor, in `await_suspend` or in a one-shot API (a
// second waiter, destroying or resetting a primitive that still holds a
// suspended waiter, a second `resolve()`, a SimStopwatch call from the wrong
// state), or the `std::terminate()` that SimTask and SimResultTask call when an
// exception escapes the coroutine body. No failure reaches the caller as an
// exception, so EXPECT_THROW has nothing to catch; only a death test, which
// runs the statement in a child process, can observe the abort and still
// report on it. The XCHECKs are compiled into every build mode, so these are
// EXPECT_DEATH rather than EXPECT_DEBUG_DEATH, which in an opt build runs the
// statement inside the test process itself.
//
// Of the checks a statement can trip, each matcher fits only the one under
// test. Without that check the statement usually still dies, later, in a
// destructor that finds what the missing check let through; a matcher that
// also fits that destructor's message would keep the test green.
//
// Every death test forks the test process, which costs time and hands the
// child whatever state the parent holds. Keeping them all here keeps the forks
// out of the primitives' own test binaries. A lint check flags a death test
// in any binary, so each one carries a NOLINT for it.
//
// The fixture builds no Scheduler, so the test process is still
// single-threaded when it forks: a Scheduler starts its PDES worker threads in
// its constructor, threads do not survive a fork, and ThreadSanitizer refuses
// to start threads in a child forked from a multi-threaded process. A
// statement that needs a Scheduler builds its own, after the fork, which is
// also why it may run it; a Scheduler built before the fork would wait forever
// in run() for workers the child does not have.
using SimTaskDeathTest = CoroTestFixture;
using SimCancellableDelayDeathTest = CoroTestFixture;
using WaiterStateDeathTest = CoroTestFixture;
using SimSignalDeathTest = CoroTestFixture;
using SimEventDeathTest = CoroTestFixture;
using SimFutureDeathTest = CoroTestFixture;
using SimQueueDeathTest = CoroTestFixture;
using SimStopwatchDeathTest = CoroTestFixture;
using SimCoordRoutedBarrierDeathTest = CoroTestFixture;

// SimResultTask<void>'s fatal-only error contract: an uncaught exception
// inside the coroutine body calls std::terminate(). Mirrors the SimTask
// contract test below.
TEST_F(SimTaskDeathTest, VoidTaskUnhandledExceptionTerminates) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        std::set_terminate([]() {
          std::cerr << "VOID_TASK_TERMINATE_HANDLER_INVOKED\n";
          std::abort();
        });
        auto worker = []() -> SimResultTask<void> {
          throw std::runtime_error("fatal");
          co_return; // unreachable; required to make this a coroutine
        };
        // Bind the [[nodiscard]] result; eager start runs the body,
        // which throws and terminates before this line completes.
        auto t = worker();
      },
      "VOID_TASK_TERMINATE_HANDLER_INVOKED");
}

// SimTask's fatal-only error contract: an uncaught exception inside the
// coroutine body calls std::terminate(). EXPECT_DEATH verifies the process
// aborts. Recoverable errors must be handled in-coroutine via try/catch.
//
// Match strategy: install our own std::terminate handler that writes a
// marker we own, instead of matching the runtime's diagnostic banner
// (which is libstdc++/libc++/MSVC-specific). The death-test child process
// isolates the set_terminate mutation from the parent test process, so no
// cleanup is required. If unhandled_exception ever stops calling
// std::terminate (e.g., someone "fixes" it to swallow), the marker will
// not appear and the test fails for the right reason.
TEST_F(SimTaskDeathTest, UnhandledExceptionTerminates) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        std::set_terminate([]() {
          std::cerr << "SIMTASK_TERMINATE_HANDLER_INVOKED\n";
          std::abort();
        });
        auto task = []() -> SimTask {
          throw std::runtime_error("fatal");
          co_return; // unreachable; required to make the lambda a coroutine
        };
        task(); // eager-start runs the body, which throws
      },
      "SIMTASK_TERMINATE_HANDLER_INVOKED");
}

// Re-awaiting on a SimCancellableDelay that already has an in-flight delay
// aborts.
TEST_F(SimCancellableDelayDeathTest, DoubleAwaitTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "double-await", 0, nullptr);
        SimCancellableDelay handle;
        auto taskA = [&]() -> SimTask {
          co_await handle.awaitDelay(&ctx, Time::fromNs(100));
        };
        auto taskB = [&]() -> SimTask {
          co_await handle.awaitDelay(&ctx, Time::fromNs(100));
        };
        taskA();
        taskB(); // trips XCHECK
      },
      // Match the await_suspend XCHECK's specific banner so an unrelated
      // "in-flight" diagnostic from another site won't satisfy this regex.
      "awaitDelay await_suspend with an in-flight delay");
}

// Destroying a SimCancellableDelay while a delay is in flight aborts (catches
// the "forgot to call cancel() before teardown" bug).
TEST_F(SimCancellableDelayDeathTest, DestroyWithInFlightTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "destroy-in-flight", 0, nullptr);
        {
          SimCancellableDelay handle;
          auto task = [&]() -> SimTask {
            co_await handle.awaitDelay(&ctx, Time::fromNs(100));
          };
          task();
        } // ~SimCancellableDelay trips XCHECK
      },
      // Match the destructor XCHECK's specific banner.
      "SimCancellableDelay destroyed with an in-flight delay");
}

// Destroying a WaiterState with an installed waiter trips the destructor
// XCHECK.
TEST_F(WaiterStateDeathTest, DestroyWithSuspendedWaiterTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "destroy-with-waiter", 0, nullptr);
        bool resumed = false;
        {
          WaiterState waiter(&ctx, ResumeMode::DIRECT);
          auto task = [&]() -> SimTask {
            // Parenthesize the brace-init to hide its comma from the
            // EXPECT_DEATH macro preprocessor (only `()` groups commas).
            co_await (WaiterTestAwaitable{waiter, resumed});
          };
          task(); // installs waiter; never resumed/cancelled
        } // ~WaiterState trips XCHECK
      },
      "WaiterState destroyed with a suspended waiter");
}

// Concurrent waiters on a SimSignal trip the always-on XCHECK in
// await_suspend.
TEST_F(SimSignalDeathTest, DoubleAwaitTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "double-await", 0, nullptr);
        SimSignal sig(&ctx);
        auto taskA = [&]() -> SimTask { co_await sig.wait(); };
        auto taskB = [&]() -> SimTask { co_await sig.wait(); };
        taskA();
        taskB(); // trips XCHECK
      },
      "concurrent waiters");
}

// Destroying a SimSignal with a suspended waiter trips the always-on XCHECK
// in ~SimSignal.
TEST_F(SimSignalDeathTest, DestroyWithSuspendedWaiterTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "destroy-with-waiter", 0, nullptr);
        {
          SimSignal sig(&ctx);
          auto task = [&]() -> SimTask { co_await sig.wait(); };
          task();
        } // ~SimSignal trips XCHECK
      },
      "SimSignal destroyed with a suspended waiter");
}

// Destroying a SimEvent with installed waiters trips the always-on XCHECK in
// ~SimEvent (the waiters' coroutine frames would leak).
TEST_F(SimEventDeathTest, DestructorAbortsOnInstalledWaiters) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(
            &scheduler,
            "death-installed",
            /*seed=*/0,
            /*conditionalMap=*/nullptr);
        SimEvent ev(&ctx);
        auto task = [&]() -> SimTask { co_await ev.wait(); };
        // Eager-start runs the body to the suspension point and installs
        // the waiter — no scheduler.run() needed.
        task();
        // ev falls out of scope here with a waiter still installed —
        // destructor's XCHECK fires.
      },
      "destroyed with installed waiters");
}

// Concurrent waiters on SimFuture<void> trip the always-on XCHECK.
TEST_F(SimFutureDeathTest, VoidDoubleAwaitTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "void-double-await", 0, nullptr);
        SimFuture<void> f(&ctx);
        auto taskA = [&]() -> SimTask { co_await f; };
        auto taskB = [&]() -> SimTask { co_await f; };
        taskA();
        taskB(); // trips XCHECK
      },
      "SimFuture<void> already has a suspended waiter");
}

// Destroying a SimFuture<void> with a suspended waiter trips ~SimFuture's
// always-on XCHECK.
TEST_F(SimFutureDeathTest, VoidDestroyWithSuspendedWaiterTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "void-destroy-waiter", 0, nullptr);
        {
          SimFuture<void> f(&ctx);
          auto task = [&]() -> SimTask { co_await f; };
          task();
        } // ~SimFuture<void> trips XCHECK
      },
      "SimFuture<void> destroyed with a suspended waiter");
}

// Concurrent waiters on SimFuture<int> trip the always-on XCHECK.
TEST_F(SimFutureDeathTest, IntDoubleAwaitTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "int-double-await", 0, nullptr);
        SimFuture<int> f(&ctx);
        auto taskA = [&]() -> SimTask { (void)co_await f; };
        auto taskB = [&]() -> SimTask { (void)co_await f; };
        taskA();
        taskB(); // trips XCHECK
      },
      "SimFuture<T> already has a suspended waiter");
}

// Destroying a SimFuture<int> with a suspended waiter trips the XCHECK in
// the value-template specialization's destructor.
TEST_F(SimFutureDeathTest, IntDestroyWithSuspendedWaiterTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "int-destroy-waiter", 0, nullptr);
        {
          SimFuture<int> f(&ctx);
          auto task = [&]() -> SimTask { (void)co_await f; };
          task();
        } // ~SimFuture<int> trips XCHECK
      },
      "SimFuture<T> destroyed with a suspended waiter");
}

// reset() while a waiter is suspended trips SimFuture<void>::reset's XCHECK.
TEST_F(SimFutureDeathTest, VoidResetWithSuspendedWaiterTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "void-reset-waiter", 0, nullptr);
        SimFuture<void> f(&ctx);
        auto task = [&]() -> SimTask { co_await f; };
        task();
        f.reset(); // trips XCHECK
      },
      "reset\\(\\) called");
}

// reset() while a waiter is suspended trips SimFuture<T>::reset's XCHECK.
TEST_F(SimFutureDeathTest, IntResetWithSuspendedWaiterTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "int-reset-waiter", 0, nullptr);
        SimFuture<int> f(&ctx);
        auto task = [&]() -> SimTask { (void)co_await f; };
        task();
        f.reset(); // trips XCHECK
      },
      "reset\\(\\) called");
}

// One-shot contract: a second resolve(T) before consume/reset trips XCHECK.
TEST_F(SimFutureDeathTest, IntDoubleResolveTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "int-double-resolve", 0, nullptr);
        SimFuture<int> f(&ctx);
        f.resolve(1);
        f.resolve(2); // trips XCHECK; would silently overwrite
      },
      "called twice");
}

// SimFuture<void> one-shot per consume cycle: a second resolve() before
// the first is consumed (or reset()) trips XCHECK.
TEST_F(SimFutureDeathTest, VoidDoubleResolveTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "void-double-resolve", 0, nullptr);
        SimFuture<void> f(&ctx);
        f.resolve();
        f.resolve(); // trips XCHECK
      },
      "called twice");
}

// Concurrent consumers of a SimQueue trip the always-on XCHECK in
// await_suspend.
TEST_F(SimQueueDeathTest, DoubleAwaitTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "double-consumer", 0, nullptr);
        SimQueue<int> q(&ctx);
        auto taskA = [&]() -> SimTask { (void)co_await q.pop(); };
        auto taskB = [&]() -> SimTask { (void)co_await q.pop(); };
        taskA();
        taskB(); // trips XCHECK
      },
      "SimQueue already has a suspended consumer");
}

// Destroying a SimQueue with a suspended consumer trips ~SimQueue's
// always-on XCHECK.
TEST_F(SimQueueDeathTest, DestroyWithSuspendedConsumerTripsXCHECK) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(&scheduler, "destroy-consumer", 0, nullptr);
        {
          SimQueue<int> q(&ctx);
          auto task = [&]() -> SimTask { (void)co_await q.pop(); };
          task();
        } // ~SimQueue trips XCHECK
      },
      "SimQueue destroyed with a suspended consumer");
}

// Destroying a SimStopwatch with a suspended waiter trips the first of
// ~SimStopwatch's always-on XCHECKs.
TEST_F(SimStopwatchDeathTest, DestructorAbortsOnSuspendedWaiter) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(
            &scheduler, "death-waiter", /*seed=*/0, /*conditionalMap=*/nullptr);
        SimStopwatch sw(&ctx, Time::fromNs(100));
        auto task = [&]() -> SimTask { co_await sw.expiration(); };
        // Eager-start runs the body to the suspension point and installs
        // the waiter — no scheduler.run() needed.
        task();
        // sw falls out of scope here with the waiter still installed —
        // destructor's XCHECK fires.
      },
      "SimStopwatch destroyed with a suspended waiter");
}

// Destroying a running SimStopwatch, whose expiration resume is still queued,
// trips the second of ~SimStopwatch's always-on XCHECKs.
TEST_F(SimStopwatchDeathTest, DestructorAbortsOnQueuedResume) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(
            &scheduler, "death-resume", /*seed=*/0, /*conditionalMap=*/nullptr);
        {
          SimStopwatch sw(&ctx, Time::fromNs(100));
          sw.start();
          // Destructor runs here with the scheduled resume still queued —
          // second XCHECK fires (no waiter, but `isActive()` is true).
        }
      },
      "queued resume");
}

// start() from EXPIRED aborts: re-arming an expired stopwatch takes reset().
TEST_F(SimStopwatchDeathTest, StartFromExpiredAborts) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(
            &scheduler,
            "death-start-expired",
            /*seed=*/0,
            /*conditionalMap=*/nullptr);
        SimStopwatch sw(&ctx, Time::fromNs(50));
        auto task = [&]() -> SimTask {
          sw.start();
          co_await sw.expiration(); // → EXPIRED
          sw.start(); // XCHECK: cannot start() from EXPIRED.
        };
        ctx.scheduleNow([&]() { task(); });
        scheduler.run();
      },
      "from EXPIRED");
}

// The pause() and resume() matchers below include the originating state
// (state=0=STOPPED, state=3=EXPIRED per the State enum order
// {STOPPED,RUNNING,PAUSED,EXPIRED}) so a misfire from the wrong state
// would not match a generic substring and the test would correctly fail.
TEST_F(SimStopwatchDeathTest, PauseFromStoppedAborts) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(
            &scheduler,
            "death-pause-stopped",
            /*seed=*/0,
            /*conditionalMap=*/nullptr);
        SimStopwatch sw(&ctx, Time::fromNs(50));
        sw.pause(); // XCHECK: pause() from STOPPED.
      },
      "requires RUNNING or PAUSED, got state=0");
}

TEST_F(SimStopwatchDeathTest, PauseFromExpiredAborts) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(
            &scheduler,
            "death-pause-expired",
            /*seed=*/0,
            /*conditionalMap=*/nullptr);
        SimStopwatch sw(&ctx, Time::fromNs(50));
        auto task = [&]() -> SimTask {
          sw.start();
          co_await sw.expiration(); // → EXPIRED
          sw.pause(); // XCHECK: pause() from EXPIRED.
        };
        ctx.scheduleNow([&]() { task(); });
        scheduler.run();
      },
      "requires RUNNING or PAUSED, got state=3");
}

TEST_F(SimStopwatchDeathTest, ResumeFromExpiredAborts) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(
            &scheduler,
            "death-resume-expired",
            /*seed=*/0,
            /*conditionalMap=*/nullptr);
        SimStopwatch sw(&ctx, Time::fromNs(50));
        auto task = [&]() -> SimTask {
          sw.start();
          co_await sw.expiration(); // → EXPIRED
          sw.resume(); // XCHECK: resume() from EXPIRED.
        };
        ctx.scheduleNow([&]() { task(); });
        scheduler.run();
      },
      "requires PAUSED or RUNNING, got state=3");
}

TEST_F(SimStopwatchDeathTest, ResumeFromStoppedAborts) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  EXPECT_DEATH(
      {
        Scheduler scheduler;
        SimContext ctx(
            &scheduler,
            "death-resume-stopped",
            /*seed=*/0,
            /*conditionalMap=*/nullptr);
        SimStopwatch sw(&ctx, Time::fromNs(50));
        sw.resume(); // XCHECK: resume() from STOPPED.
      },
      "requires PAUSED or RUNNING, got state=0");
}

// Destructor aborts loudly if a SimCoordRoutedBarrier is dropped while arrivals
// are still pending — pins the always-on XCHECK contract documented in
// SimCoordRoutedBarrier.h. Without this test, a future refactor could weaken or
// remove the XCHECK and CI wouldn't catch it; the silent regression
// would re-introduce the UAF risk (queued postArrival callbacks
// dereferencing a freed barrier).
//
// The statement runs its own Scheduler to queue the pending arrival before the
// barrier falls out of scope.
TEST_F(SimCoordRoutedBarrierDeathTest, DestructorAbortsOnPendingArrivals) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  ASSERT_DEATH(
      {
        Scheduler localScheduler;
        SimContext localCoord(&localScheduler, "coord");
        SimContext localPart(&localScheduler, "participant");

        // Arrange: 3-participant barrier with only 1 arrival queued.
        SimCoordRoutedBarrier barrier(&localCoord, 3);
        auto task = [&]() -> SimTask { co_await barrier.arrive(&localPart); };
        localPart.scheduleNow([&]() { task(); });
        localScheduler.run(); // arrivals_.size() == 1 after this

        // `barrier` falls out of scope here; ~SimCoordRoutedBarrier() XCHECK
        // fires.
      },
      "SimCoordRoutedBarrier destroyed with .* pending arrivals");
}

// Destructor also aborts when a participant has suspended (await_suspend
// queued postArrival on coord) but coord hasn't run that callback yet —
// arrivals_ is still empty, but inFlight_ counter detects the queued
// reference. Without the inFlight_ check this would be a silent UAF on
// the next coord tick (the queued lambda holds raw `this`).
//
// Drives the state directly: call await_suspend with a noop_coroutine
// handle and never run the scheduler. await_suspend increments inFlight_
// and queues postArrival on coord; scheduler never runs, so postArrival
// stays queued and arrivals_ stays empty. Going through scheduler.run()
// (with any duration) would let coord drain its inbox and process the
// callback, decrementing inFlight_ back to zero.
TEST_F(
    SimCoordRoutedBarrierDeathTest,
    DestructorAbortsOnQueuedButUnfiredPostArrival) {
  // NOLINTNEXTLINE(facebook-gtest-expect-death)
  ASSERT_DEATH(
      {
        Scheduler localScheduler;
        SimContext localCoord(&localScheduler, "coord");
        SimContext localPart(&localScheduler, "participant");

        SimCoordRoutedBarrier barrier(&localCoord, 3);

        // Drive await_suspend manually: this increments inFlight_ to 1
        // and queues postArrival on coord. The noop_coroutine handle is
        // a real (no-op) coroutine handle that satisfies the API; it
        // would never actually be resumed because we never run coord's
        // inbox. arrivals_ stays empty (postArrival never executes).
        barrier.arrive(&localPart).await_suspend(std::noop_coroutine());

        // `barrier` falls out of scope here; destructor's inFlight_
        // XCHECK fires before any read of arrivals_.
      },
      "in-flight postArrival callbacks");
}

} // namespace packsim::coro
