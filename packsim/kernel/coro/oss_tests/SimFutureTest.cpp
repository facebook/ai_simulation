/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimFuture.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "packsim/kernel/coro/SimDelay.h"
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
using packsim::coro::SimFuture;
using packsim::coro::SimTask;
using packsim::coro::tests::CoroTestFixture;

namespace packsim::coro {

using SimFutureTest = CoroTestFixture;

// Void future: send-and-wait pattern.
TEST_F(SimFutureTest, VoidSendAndWait) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "void", 0, nullptr);

  SimFuture<void> ackFuture(&ctx);
  bool resumed = false;
  std::optional<uint64_t> resumeTime;

  // Producer resolves at t=150ns.
  ctx.schedule(Time::fromNs(150), [&]() { ackFuture.resolve(); });

  auto consumer = [&]() -> SimTask {
    co_await ackFuture;
    resumed = true;
    resumeTime = ctx.nowNs();
  };

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  EXPECT_TRUE(resumed);
  ASSERT_TRUE(resumeTime.has_value());
  EXPECT_EQ(*resumeTime, 150);
  // SimFuture<void>::await_resume auto-clears resolved_ on consume.
  EXPECT_FALSE(ackFuture.isResolved());
}

// Already-resolved future returns immediately without suspension.
TEST_F(SimFutureTest, VoidAlreadyResolvedDoesNotSuspend) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "preresolved", 0, nullptr);

  SimFuture<void> f(&ctx);
  f.resolve(); // resolve before any waiter

  bool resumed = false;
  std::optional<uint64_t> resumeTime;

  auto consumer = [&]() -> SimTask {
    co_await f;
    resumed = true;
    resumeTime = ctx.nowNs();
  };

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  EXPECT_TRUE(resumed);
  // No suspension means no time advancement.
  ASSERT_TRUE(resumeTime.has_value());
  EXPECT_EQ(*resumeTime, 0);
}

// Value future: future carries a computed result, moved out on resume.
TEST_F(SimFutureTest, IntResolveDeliversValueToWaiter) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "int", 0, nullptr);

  SimFuture<int> f(&ctx);
  int receivedValue = -1;

  ctx.schedule(Time::fromNs(80), [&]() { f.resolve(42); });

  auto consumer = [&]() -> SimTask { receivedValue = co_await f; };

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  EXPECT_EQ(receivedValue, 42);
  // SimFuture<T>::await_resume auto-clears value_ on consume; isResolved()
  // reflects "has unconsumed value", not "did resolve() ever fire".
  EXPECT_FALSE(f.isResolved());
}

// Move-only value via future (unique_ptr).
TEST_F(SimFutureTest, MoveOnlyValueFuture) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "moveonly", 0, nullptr);

  SimFuture<std::unique_ptr<std::string>> f(&ctx);
  std::string receivedString;

  ctx.schedule(Time::fromNs(50), [&]() {
    f.resolve(std::make_unique<std::string>("hello"));
  });

  auto consumer = [&]() -> SimTask {
    auto ptr = co_await f;
    receivedString = *ptr;
  };

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  EXPECT_EQ(receivedString, "hello");
}

// Reset and reuse: resolve, await, reset, resolve again.
TEST_F(SimFutureTest, ResetAndReuse) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "reuse", 0, nullptr);

  SimFuture<int> f(&ctx);
  int firstValue = -1;
  std::optional<uint64_t> firstReadAtNs;
  int secondValue = -1;
  std::optional<uint64_t> secondReadAtNs;

  // First resolve at t=50; consumer waits 40ns; second resolve at t=100.
  // (No explicit reset() needed: SimFuture<T>::await_resume auto-clears
  // value_ on consume, returning the future to the unresolved state.)
  ctx.schedule(Time::fromNs(50), [&]() { f.resolve(7); });
  ctx.schedule(Time::fromNs(100), [&]() { f.resolve(8); });

  auto consumer = [&]() -> SimTask {
    firstValue = co_await f;
    firstReadAtNs = ctx.nowNs();
    co_await simDelay(&ctx, Time::fromNs(40));
    secondValue = co_await f;
    secondReadAtNs = ctx.nowNs();
  };

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  EXPECT_EQ(firstValue, 7);
  ASSERT_TRUE(firstReadAtNs.has_value());
  EXPECT_EQ(*firstReadAtNs, 50);
  EXPECT_EQ(secondValue, 8);
  ASSERT_TRUE(secondReadAtNs.has_value());
  EXPECT_EQ(*secondReadAtNs, 100);
}

// hasWaiter() reflects suspension state.
TEST_F(SimFutureTest, HasWaiterReflectsState) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "haswaiter", 0, nullptr);

  SimFuture<void> f(&ctx);
  EXPECT_FALSE(f.hasWaiter());

  auto consumer = [&]() -> SimTask { co_await f; };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() {
    EXPECT_TRUE(f.hasWaiter());
    f.resolve();
  });
  ctx.schedule(Time::fromNs(20), [&]() { EXPECT_FALSE(f.hasWaiter()); });

  scheduler.run();
}

// isResolved() and resolution stickiness.
TEST_F(SimFutureTest, IsResolvedTracksState) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "isresolved", 0, nullptr);

  SimFuture<void> f(&ctx);
  EXPECT_FALSE(f.isResolved());

  f.resolve();
  EXPECT_TRUE(f.isResolved());

  f.reset();
  EXPECT_FALSE(f.isResolved());
}

// Default ResumeMode::DIRECT: resume happens synchronously inside resolve().
TEST_F(SimFutureTest, DefaultDirectResumeIsTrue) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "direct", 0, nullptr);

  SimFuture<void> f(&ctx);
  bool resumedSync = false;

  auto consumer = [&]() -> SimTask {
    co_await f;
    resumedSync = true;
  };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() {
    f.resolve();
    EXPECT_TRUE(resumedSync);
  });

  scheduler.run();
}

// With ResumeMode::DEFERRED, cancel() must remove the in-flight deferred
// resume from PDES AND destroy the suspended frame.
TEST_F(SimFutureTest, CancelDuringInFlightDeferredResume) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-in-flight", 0, nullptr);

  SimFuture<void> f(&ctx, ResumeMode::DEFERRED);
  bool consumerResumed = false;

  auto consumer = [&]() -> SimTask {
    co_await f;
    consumerResumed = true;
  };

  ctx.scheduleNow([&]() { consumer(); });

  ctx.schedule(Time::fromNs(10), [&]() {
    EXPECT_TRUE(f.hasWaiter());
    f.resolve();
    EXPECT_TRUE(f.hasWaiter())
        << "deferred resolve() keeps waiter_ set for cancel() to find";
    f.cancel();
    EXPECT_FALSE(f.hasWaiter());
  });

  scheduler.run();
  EXPECT_FALSE(consumerResumed);
}

// cancel() with no in-flight waiter is a safe no-op.
TEST_F(SimFutureTest, CancelWithoutWaiterIsNoOp) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-noop", 0, nullptr);

  SimFuture<void> f(&ctx, ResumeMode::DEFERRED);
  f.cancel();
  EXPECT_FALSE(f.hasWaiter());

  // Still valid to use after cancel().
  bool resumed = false;
  auto consumer = [&]() -> SimTask {
    co_await f;
    resumed = true;
  };
  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() { f.resolve(); });
  scheduler.run();

  EXPECT_TRUE(resumed);
}

// cancel() is idempotent — safe to call multiple times in sequence.
TEST_F(SimFutureTest, CancelIsIdempotent) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-idem", 0, nullptr);

  SimFuture<void> f(&ctx, ResumeMode::DEFERRED);
  auto consumer = [&]() -> SimTask { co_await f; };
  ctx.scheduleNow([&]() { consumer(); });

  ctx.schedule(Time::fromNs(10), [&]() {
    f.resolve();
    f.cancel();
    f.cancel(); // must not crash or double-destroy
    f.cancel();
    EXPECT_FALSE(f.hasWaiter());
  });

  scheduler.run();
}

// cancel() does NOT clear resolved_ — producer's resolution is preserved.
TEST_F(SimFutureTest, CancelPreservesResolvedState) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-preserve", 0, nullptr);

  SimFuture<void> f(&ctx, ResumeMode::DEFERRED);
  auto consumer = [&]() -> SimTask { co_await f; };
  ctx.scheduleNow([&]() { consumer(); });

  ctx.schedule(Time::fromNs(10), [&]() {
    f.resolve();
    EXPECT_TRUE(f.isResolved());
    f.cancel();
    EXPECT_TRUE(f.isResolved())
        << "cancel() must not clear resolved_; use reset() for that";
  });

  scheduler.run();
}

// Same shape as CancelDuringInFlightDeferredResume but for SimFuture<int> —
// pins the value-template's independent cancel() implementation.
TEST_F(SimFutureTest, CancelDuringInFlightDeferredResumeForIntValue) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-int-in-flight", 0, nullptr);

  SimFuture<int> f(&ctx, ResumeMode::DEFERRED);
  bool consumerResumed = false;
  int receivedValue = -1;

  auto consumer = [&]() -> SimTask {
    receivedValue = co_await f;
    consumerResumed = true;
  };

  ctx.scheduleNow([&]() { consumer(); });

  ctx.schedule(Time::fromNs(10), [&]() {
    EXPECT_TRUE(f.hasWaiter());
    f.resolve(42);
    EXPECT_TRUE(f.hasWaiter())
        << "deferred resolve() keeps waiter_ set for cancel() to find";
    f.cancel();
    EXPECT_FALSE(f.hasWaiter());
  });

  scheduler.run();

  EXPECT_FALSE(consumerResumed);
  EXPECT_EQ(receivedValue, -1);
}

// cancel() tears down a suspended consumer mid-flight: after resolve()
// followed by cancel() (in the deferred-resume window), the consumer
// never resumes.
TEST_F(SimFutureTest, CancelTearsDownSuspendedConsumer) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-teardown", 0, nullptr);

  SimFuture<int> f(&ctx, ResumeMode::DEFERRED);
  int result = -1;

  // The consumer lambda must outlive any SimTask coroutine it creates.
  // If declared inside a scheduled callback, the lambda dies before the
  // coroutine resumes, causing stack-use-after-return on `this` capture.
  auto consumer = [&]() -> SimTask { result = co_await f; };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() {
    f.resolve(42);
    f.cancel();
    EXPECT_FALSE(f.hasWaiter());
  });

  scheduler.run();

  EXPECT_EQ(result, -1) << "consumer was cancelled before resume fired";
}

// After cancel() + reset(), a fresh consumer can use the same SimFuture
// from a clean unresolved state.
TEST_F(SimFutureTest, ResetAfterCancelAllowsReuseForNewConsumer) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-reset-reuse", 0, nullptr);

  SimFuture<int> f(&ctx, ResumeMode::DEFERRED);
  int result = -1;

  // Pre-condition: cancel() + reset() leaves the future in clean state.
  f.resolve(42);
  f.cancel();
  f.reset();
  ASSERT_FALSE(f.isResolved());
  ASSERT_FALSE(f.hasWaiter());

  auto consumer = [&]() -> SimTask { result = co_await f; };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() { f.resolve(99); });

  scheduler.run();

  EXPECT_EQ(result, 99);
}

// SimFuture<T>::await_resume auto-clears value_ after move; a subsequent
// resolve() can fire without tripping the double-resolve XCHECK and the
// next consumer reads the new value (not a moved-from one).
TEST_F(SimFutureTest, IntAwaitResumeClearsValue) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "int-auto-clear", 0, nullptr);

  SimFuture<int> f(&ctx);
  f.resolve(7);
  EXPECT_TRUE(f.isResolved());

  int firstValue = -1;
  int secondValue = -1;

  auto consumer = [&]() -> SimTask {
    firstValue = co_await f;
    EXPECT_FALSE(f.isResolved()) << "value_ must be cleared after consume";
    co_await simDelay(&ctx, Time::fromNs(10));
    secondValue = co_await f;
  };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(5), [&]() { f.resolve(42); });

  scheduler.run();

  EXPECT_EQ(firstValue, 7);
  EXPECT_EQ(secondValue, 42);
}

// After a natural-fire deferred resume, cancel() must be a safe no-op —
// scheduledResume_ was cleared by the fire lambda, so cancel() on the
// default event handle is a no-op and waiter_ is empty so no destroy path.
TEST_F(SimFutureTest, CancelAfterNaturalFireIsNoOp) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-after-fire", 0, nullptr);

  SimFuture<void> f(&ctx, ResumeMode::DEFERRED);
  bool consumerResumed = false;

  auto consumer = [&]() -> SimTask {
    co_await f;
    consumerResumed = true;
  };
  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() { f.resolve(); });
  ctx.schedule(Time::fromNs(20), [&]() {
    EXPECT_TRUE(consumerResumed);
    f.cancel(); // must be safe: scheduledResume_ already cleared
  });

  scheduler.run();
  EXPECT_TRUE(consumerResumed);
}

// SimFuture<void>::await_resume auto-clears resolved_, mirroring <T>'s
// auto-clear-on-consume semantic. A subsequent co_await suspends until
// the next resolve().
TEST_F(SimFutureTest, VoidAwaitResumeClearsResolved) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "void-auto-clear", 0, nullptr);

  SimFuture<void> f(&ctx);
  f.resolve();
  EXPECT_TRUE(f.isResolved());

  int wakeups = 0;
  auto consumer = [&]() -> SimTask {
    co_await f;
    wakeups++;
    EXPECT_FALSE(f.isResolved()) << "resolved_ must be cleared after consume";
    co_await simDelay(&ctx, Time::fromNs(10));
    co_await f;
    wakeups++;
  };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(5), [&]() { f.resolve(); });

  scheduler.run();

  EXPECT_EQ(wakeups, 2);
}

// Pins consume-aware semantics in isolation: after consume, a second
// co_await on the same future suspends (because value_ was cleared) and
// only wakes when a new value arrives.
TEST_F(SimFutureTest, IntDoubleConsumeWithoutResolveSuspends) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "int-double-consume", 0, nullptr);

  SimFuture<int> f(&ctx);
  int firstValue = -1;
  int secondValue = -1;
  std::optional<uint64_t> secondReadAtNs;

  auto consumer = [&]() -> SimTask {
    firstValue = co_await f; // resolves at t=10, returns 7
    secondValue = co_await f; // suspends until t=50, returns 42
    secondReadAtNs = ctx.nowNs();
  };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() { f.resolve(7); });
  ctx.schedule(Time::fromNs(50), [&]() { f.resolve(42); });

  scheduler.run();

  EXPECT_EQ(firstValue, 7);
  EXPECT_EQ(secondValue, 42);
  ASSERT_TRUE(secondReadAtNs.has_value());
  EXPECT_EQ(*secondReadAtNs, 50);
}

// Pure teardown path on SimFuture<void>: consumer suspends, producer never
// resolves, owner calls cancel() to destroy the primitive. This is the
// scenario the "Teardown:" doc paragraph in SimFuture.h is written for.
TEST_F(SimFutureTest, VoidCancelWithSuspendedWaiterNoResolveDestroysFrame) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "void-teardown-no-resolve", 0, nullptr);

  SimFuture<void> f(&ctx, ResumeMode::DEFERRED);
  bool consumerResumed = false;

  auto consumer = [&]() -> SimTask {
    co_await f;
    consumerResumed = true;
  };

  ctx.scheduleNow([&]() { consumer(); });
  // No resolve() ever fires. Owner just tears down mid-flight.
  ctx.schedule(Time::fromNs(10), [&]() {
    ASSERT_TRUE(f.hasWaiter());
    f.cancel();
    EXPECT_FALSE(f.hasWaiter());
  });

  scheduler.run();
  EXPECT_FALSE(consumerResumed);
}

// Pure teardown path on SimFuture<int>: same shape but for the value
// specialization's independent cancel() implementation.
TEST_F(SimFutureTest, IntCancelWithSuspendedWaiterNoResolveDestroysFrame) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "int-teardown-no-resolve", 0, nullptr);

  SimFuture<int> f(&ctx, ResumeMode::DEFERRED);
  bool consumerResumed = false;
  int receivedValue = -1;

  auto consumer = [&]() -> SimTask {
    receivedValue = co_await f;
    consumerResumed = true;
  };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() {
    ASSERT_TRUE(f.hasWaiter());
    f.cancel();
    EXPECT_FALSE(f.hasWaiter());
  });

  scheduler.run();
  EXPECT_FALSE(consumerResumed);
  EXPECT_EQ(receivedValue, -1);
}

// After a natural-fire deferred resume on SimFuture<int>, cancel() must
// be a safe no-op (the value-template's independent cancel path).
TEST_F(SimFutureTest, CancelAfterNaturalFireIsNoOpForIntValue) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "int-cancel-after-fire", 0, nullptr);

  SimFuture<int> f(&ctx, ResumeMode::DEFERRED);
  int receivedValue = -1;

  auto consumer = [&]() -> SimTask { receivedValue = co_await f; };
  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() { f.resolve(99); });
  ctx.schedule(Time::fromNs(20), [&]() {
    EXPECT_EQ(receivedValue, 99);
    f.cancel(); // must be safe: scheduledResume_ already cleared
  });

  scheduler.run();
  EXPECT_EQ(receivedValue, 99);
}

} // namespace packsim::coro
