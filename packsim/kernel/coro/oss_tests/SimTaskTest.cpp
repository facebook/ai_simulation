/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimTask.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/kernel/coro/SimDelay.h"
#include "packsim/kernel/coro/oss_tests/fixtures/CoroTestFixture.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

using packsim::Scheduler;
using packsim::SimContext;
using packsim::Time;
using packsim::coro::simDelay;
using packsim::coro::SimResultTask;
using packsim::coro::SimTask;
using packsim::coro::simYield;
using packsim::coro::tests::CoroTestFixture;

namespace packsim::coro {

using SimTaskTest = CoroTestFixture;

// Eager start: coroutine body executes immediately on invocation, before the
// scheduler is even run. Verified by recording state inside the coroutine
// before the first co_await.
TEST_F(SimTaskTest, EagerStartRunsBeforeFirstAwait) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "eager", 0, nullptr);

  bool hasRunBeforeAwait = false;
  bool hasResumedAfterAwait = false;

  auto task = [&]() -> SimTask {
    hasRunBeforeAwait = true;
    co_await simDelay(&ctx, Time::fromNs(10));
    hasResumedAfterAwait = true;
  };

  // Invocation alone runs the body up to the first co_await.
  task();
  EXPECT_TRUE(hasRunBeforeAwait);
  EXPECT_FALSE(hasResumedAfterAwait);

  scheduler.run();
  EXPECT_TRUE(hasResumedAfterAwait);
  EXPECT_EQ(ctx.nowNs(), 10);
}

// Co_return ends the coroutine without further suspension. Frame is freed
// (verified indirectly: a follow-up scheduler.run() does not crash from a
// dangling resume).
TEST_F(SimTaskTest, CoReturnEndsCoroutine) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "early-return", 0, nullptr);

  int reachedCheckpoint = 0;

  auto task = [&]() -> SimTask {
    reachedCheckpoint = 1;
    co_await simDelay(&ctx, Time::fromNs(50));
    reachedCheckpoint = 2;
    co_return;
  };

  ctx.scheduleNow([&]() { task(); });
  scheduler.run();

  EXPECT_EQ(reachedCheckpoint, 2);
}

// Multi-step pipeline with verified timing. Replaces the callback chain:
//   step1() { ...; schedule(d1, &step2); }
//   step2() { ...; schedule(d2, &step3); }
TEST_F(SimTaskTest, MultiStepPipelineAdvancesTimeMonotonically) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "pipeline", 0, nullptr);

  std::vector<std::pair<std::string, uint64_t>> log;

  auto pipeline = [&]() -> SimTask {
    log.emplace_back("start", ctx.nowNs());
    co_await simDelay(&ctx, Time::fromNs(50));
    log.emplace_back("step1_done", ctx.nowNs());
    co_await simDelay(&ctx, Time::fromNs(100));
    log.emplace_back("step2_done", ctx.nowNs());
    co_await simDelay(&ctx, Time::fromNs(200));
    log.emplace_back("step3_done", ctx.nowNs());
  };

  ctx.scheduleNow([&]() { pipeline(); });
  scheduler.run();

  const std::vector<std::pair<std::string, uint64_t>> expected = {
      {"start", 0},
      {"step1_done", 50},
      {"step2_done", 150},
      {"step3_done", 350},
  };
  EXPECT_EQ(log, expected);
  EXPECT_EQ(ctx.nowNs(), 350);
}

// Multiple concurrent coroutines on the same context interleave by
// scheduled time, not by invocation order. This test pins the PDES same-tick
// FIFO insertion-order contract — the {'A',0},{'B',0} ordering at t=0 reflects
// invocation order, and the rest of the sequence is determined by absolute
// scheduled time. If the PDES scheduler ever changes same-tick semantics,
// the regression is obvious here.
TEST_F(SimTaskTest, ConcurrentCoroutinesInterleaveByScheduledTime) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "concurrent", 0, nullptr);

  std::vector<std::pair<char, uint64_t>> log;

  auto taskA = [&]() -> SimTask {
    log.emplace_back('A', ctx.nowNs());
    co_await simDelay(&ctx, Time::fromNs(30));
    log.emplace_back('A', ctx.nowNs());
    co_await simDelay(&ctx, Time::fromNs(30));
    log.emplace_back('A', ctx.nowNs());
  };

  auto taskB = [&]() -> SimTask {
    log.emplace_back('B', ctx.nowNs());
    co_await simDelay(&ctx, Time::fromNs(20));
    log.emplace_back('B', ctx.nowNs());
    co_await simDelay(&ctx, Time::fromNs(20));
    log.emplace_back('B', ctx.nowNs());
  };

  ctx.scheduleNow([&]() { taskA(); });
  ctx.scheduleNow([&]() { taskB(); });
  scheduler.run();

  // Both start at t=0 (insertion order). Then B@20, A@30, B@40, A@60.
  const std::vector<std::pair<char, uint64_t>> expected = {
      {'A', 0},
      {'B', 0},
      {'B', 20},
      {'A', 30},
      {'B', 40},
      {'A', 60},
  };
  EXPECT_EQ(log, expected);
}

// simYield: many iterations at the same logical time without advancing it.
// Validates the "scheduleNow" semantic and avoids stack overflow.
TEST_F(SimTaskTest, SimYieldChainedWorkDoesNotAdvanceTime) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "yielder", 0, nullptr);

  int counter = 0;
  constexpr int kTargetCount = 1000;

  auto task = [&]() -> SimTask {
    for (int i = 0; i < kTargetCount; ++i) {
      counter++;
      co_await simYield(&ctx);
    }
  };

  ctx.scheduleNow([&]() { task(); });
  scheduler.run();

  EXPECT_EQ(counter, kTargetCount);
  EXPECT_EQ(ctx.nowNs(), 0);
}

// Coroutine that completes synchronously (no co_await) returns a SimTask
// whose frame is destroyed immediately by suspend_never on final_suspend.
TEST_F(SimTaskTest, SyncCompletionFreesFrameImmediately) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "sync", 0, nullptr);

  int counter = 0;
  auto task = [&]() -> SimTask {
    counter++;
    co_return;
  };

  task(); // runs to completion synchronously, frame destroyed
  EXPECT_EQ(counter, 1);

  // Scheduler has no pending events from the destroyed coroutine.
  scheduler.run();
  EXPECT_EQ(counter, 1);
  EXPECT_EQ(ctx.nowNs(), 0);
}

// Capture-by-reference works because the lambda outlives the coroutine
// (lambda is on the stack of the test, coroutine references the lambda's
// captures via the lambda's `this`).
TEST_F(SimTaskTest, CaptureByReferenceInCoroutine) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "capture", 0, nullptr);

  int sharedCounter = 0;

  auto task = [&]() -> SimTask {
    for (int i = 0; i < 3; ++i) {
      sharedCounter += 10;
      co_await simDelay(&ctx, Time::fromNs(100));
    }
  };

  ctx.scheduleNow([&]() { task(); });
  scheduler.run();

  EXPECT_EQ(sharedCounter, 30);
  EXPECT_EQ(ctx.nowNs(), 300);
}

// ============================================================================
// SimResultTask<T> — value-returning variant.
// ============================================================================

// Synchronous Task<int>: body completes without internal co_await; co_await
// reads the stored value without suspending the parent.
TEST_F(SimTaskTest, ValueTaskSyncReturn) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "ret-sync", 0, nullptr);

  int result = -1;
  auto runner = [&]() -> SimTask {
    auto producer = []() -> SimResultTask<int> { co_return 42; };
    result = co_await producer();
  };

  ctx.scheduleNow([&]() { runner(); });
  scheduler.run();
  EXPECT_EQ(result, 42);
  EXPECT_EQ(ctx.nowNs(), 0);
}

// Async Task<int>: body suspends on a delay before co_return. Parent
// suspends until final_suspend symmetric-transfers control back.
TEST_F(SimTaskTest, ValueTaskAsyncReturn) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "ret-async", 0, nullptr);

  int result = -1;
  auto runner = [&]() -> SimTask {
    auto producer = [&]() -> SimResultTask<int> {
      co_await simDelay(&ctx, Time::fromNs(100));
      co_return 7;
    };
    result = co_await producer();
  };

  ctx.scheduleNow([&]() { runner(); });
  scheduler.run();
  EXPECT_EQ(result, 7);
  EXPECT_EQ(ctx.nowNs(), 100);
}

// Move-only return type: unique_ptr round-trips through co_return.
TEST_F(SimTaskTest, ValueTaskMoveOnlyType) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "ret-uniqueptr", 0, nullptr);

  std::unique_ptr<int> result;
  auto runner = [&]() -> SimTask {
    auto producer = []() -> SimResultTask<std::unique_ptr<int>> {
      co_return std::make_unique<int>(99);
    };
    result = co_await producer();
  };

  ctx.scheduleNow([&]() { runner(); });
  scheduler.run();
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(*result, 99);
}

// Chained Task<T>: outer awaits inner, both return values, deltas compose
// cleanly across the await.
TEST_F(SimTaskTest, ValueTaskChainedReturns) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "ret-chained", 0, nullptr);

  int finalResult = -1;
  auto runner = [&]() -> SimTask {
    auto inner = [&]() -> SimResultTask<int> {
      co_await simDelay(&ctx, Time::fromNs(10));
      co_return 5;
    };
    auto outer = [&]() -> SimResultTask<int> {
      int x = co_await inner();
      co_await simDelay(&ctx, Time::fromNs(20));
      co_return x * 2;
    };
    finalResult = co_await outer();
  };

  ctx.scheduleNow([&]() { runner(); });
  scheduler.run();
  EXPECT_EQ(finalResult, 10);
  EXPECT_EQ(ctx.nowNs(), 30);
}

// Sync-completion happy path for the destructor: a producer that runs
// to completion synchronously is held at final_suspend with
// handle_.done()==true. When the SimResultTask goes out of scope
// unawaited, the destructor's XDCHECK(handle_.done()) passes and
// destroy() runs. Verifies the destructor's clean-shutdown path; the
// drop-hazard XDCHECK firing path (suspended body) is not covered here
// because XDCHECK is debug-only and is silently elided in opt builds
// where this test runs (see destructor comment).
TEST_F(SimTaskTest, ValueTaskUnawaitedSyncCompletedFrameDestroyed) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "ret-unawaited", 0, nullptr);

  int sideEffect = 0;
  auto runner = [&]() -> SimTask {
    auto producer = [&]() -> SimResultTask<int> {
      sideEffect = 42;
      co_return 1;
    };
    {
      auto t = producer(); // body ran (sideEffect set), value held
      // Destructor runs at end of scope, destroying the frame.
    }
    co_await simYield(&ctx);
  };

  ctx.scheduleNow([&]() { runner(); });
  scheduler.run();
  EXPECT_EQ(sideEffect, 42);
}

// Move-construction transfers handle ownership: after b = std::move(a),
// awaiting b reads the value (a is now empty). Exercises the
// SimResultTask(SimResultTask&&) constructor and the std::exchange
// pattern that nulls the moved-from handle.
TEST_F(SimTaskTest, ValueTaskMoveConstructed) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "ret-move", 0, nullptr);

  std::optional<int> got;
  auto runner = [&]() -> SimTask {
    auto producer = [&]() -> SimResultTask<int> {
      co_await simDelay(&ctx, Time::fromNs(10));
      co_return 7;
    };
    SimResultTask<int> a = producer();
    SimResultTask<int> b = std::move(a);
    got = co_await std::move(b);
  };

  ctx.scheduleNow([&]() { runner(); });
  scheduler.run();
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, 7);
  EXPECT_EQ(ctx.nowNs(), 10);
}

// ============================================================================
// SimResultTask<void> — value-less awaitable variant.
// ============================================================================

// Synchronous void task: body completes without internal co_await. The await
// expression resumes immediately because handle_.done() is true on entry.
TEST_F(SimTaskTest, VoidTaskSyncReturn) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "void-sync", 0, nullptr);

  bool ranToCompletion = false;
  auto runner = [&]() -> SimTask {
    auto worker = [&]() -> SimResultTask<void> {
      ranToCompletion = true;
      co_return;
    };
    co_await worker();
  };

  ctx.scheduleNow([&]() { runner(); });
  scheduler.run();
  EXPECT_TRUE(ranToCompletion);
  EXPECT_EQ(ctx.nowNs(), 0);
}

// Async void task: body suspends on a delay before completing. Parent suspends
// until final_suspend symmetric-transfers control back.
TEST_F(SimTaskTest, VoidTaskAsyncReturn) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "void-async", 0, nullptr);

  bool ranToCompletion = false;
  auto runner = [&]() -> SimTask {
    auto worker = [&]() -> SimResultTask<void> {
      co_await simDelay(&ctx, Time::fromNs(100));
      ranToCompletion = true;
    };
    co_await worker();
  };

  ctx.scheduleNow([&]() { runner(); });
  scheduler.run();
  EXPECT_TRUE(ranToCompletion);
  EXPECT_EQ(ctx.nowNs(), 100);
}

// Fall-off-end completion: body has no explicit co_return, but the coroutine
// is still required to terminate cleanly because return_void is invoked
// implicitly when the body falls off the end.
TEST_F(SimTaskTest, VoidTaskFallOffEnd) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "void-falloff", 0, nullptr);

  bool reachedEnd = false;
  auto runner = [&]() -> SimTask {
    auto worker = [&]() -> SimResultTask<void> {
      co_await simDelay(&ctx, Time::fromNs(50));
      reachedEnd = true;
      // No co_return — falls off the end.
    };
    co_await worker();
  };

  ctx.scheduleNow([&]() { runner(); });
  scheduler.run();
  EXPECT_TRUE(reachedEnd);
  EXPECT_EQ(ctx.nowNs(), 50);
}

// Sequential composition: a parent awaits two void helpers in series; total
// elapsed time is the sum of the helpers' delays, proving each await blocks
// until the previous helper's body completes.
TEST_F(SimTaskTest, VoidTaskSequentialComposition) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "void-seq", 0, nullptr);

  std::vector<uint64_t> completionTimesNs;
  auto runner = [&]() -> SimTask {
    auto worker = [&](uint64_t delayNs) -> SimResultTask<void> {
      co_await simDelay(&ctx, Time::fromNs(delayNs));
      completionTimesNs.emplace_back(ctx.nowNs());
    };
    co_await worker(30);
    co_await worker(70);
  };

  ctx.scheduleNow([&]() { runner(); });
  scheduler.run();
  const std::vector<uint64_t> expected = {30, 100};
  EXPECT_EQ(completionTimesNs, expected);
  EXPECT_EQ(ctx.nowNs(), 100);
}

// Sync-completion happy path for the destructor: a worker that runs to
// completion synchronously is held at final_suspend with
// handle_.done()==true. When the SimResultTask<void> goes out of scope
// unawaited, the destructor's XDCHECK(handle_.done()) passes and
// destroy() runs. The drop-hazard XDCHECK firing path (suspended body)
// is not covered here because XDCHECK is debug-only and is silently
// elided in opt builds where this test runs.
TEST_F(SimTaskTest, VoidTaskUnawaitedSyncCompletedFrameDestroyed) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "void-unawaited", 0, nullptr);

  int sideEffect = 0;
  auto runner = [&]() -> SimTask {
    auto worker = [&]() -> SimResultTask<void> {
      sideEffect = 7;
      co_return;
    };
    {
      auto t = worker(); // body ran (sideEffect set), held at final_suspend
      // Destructor runs at end of scope, destroying the frame.
    }
    co_await simYield(&ctx);
  };

  ctx.scheduleNow([&]() { runner(); });
  scheduler.run();
  EXPECT_EQ(sideEffect, 7);
}

// Move-construction transfers handle ownership: after b = std::move(a),
// awaiting b runs the worker (a is now empty). Mirrors
// `ValueTaskMoveConstructed` for the void specialization.
TEST_F(SimTaskTest, VoidTaskMoveConstructed) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "void-move", 0, nullptr);

  bool ranToCompletion = false;
  auto runner = [&]() -> SimTask {
    auto worker = [&]() -> SimResultTask<void> {
      co_await simDelay(&ctx, Time::fromNs(10));
      ranToCompletion = true;
    };
    SimResultTask<void> a = worker();
    SimResultTask<void> b = std::move(a);
    co_await std::move(b);
  };

  ctx.scheduleNow([&]() { runner(); });
  scheduler.run();
  EXPECT_TRUE(ranToCompletion);
  EXPECT_EQ(ctx.nowNs(), 10);
}

// Self-move-assignment is a no-op (the `this != &other` guard short-
// circuits the destroy + steal). The worker still runs to completion
// when awaited afterward. Indirect through a reference to dodge clang's
// -Wself-move on a direct `t = std::move(t)`.
TEST_F(SimTaskTest, VoidTaskSelfMoveAssignmentIsNoOp) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "void-self-move", 0, nullptr);

  bool ranToCompletion = false;
  auto runner = [&]() -> SimTask {
    auto worker = [&]() -> SimResultTask<void> {
      ranToCompletion = true;
      co_return;
    };
    auto t = worker();
    SimResultTask<void>& tref = t;
    t = std::move(tref); // exercises the self-assignment guard.
    co_await std::move(t);
  };

  ctx.scheduleNow([&]() { runner(); });
  scheduler.run();
  EXPECT_TRUE(ranToCompletion);
}

// 3-level chain: a top-level SimTask awaits a middle SimResultTask<void>
// that awaits an inner SimResultTask<void>. Both inner levels suspend on
// simDelay. Verifies symmetric-transfer through two final_suspend hops
// (the canonical LLR state-machine dispatcher shape).
TEST_F(SimTaskTest, VoidTaskThreeLevelChainSymmetricTransfer) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "void-3level", 0, nullptr);

  std::vector<std::pair<std::string, uint64_t>> log;
  auto runner = [&]() -> SimTask {
    auto inner = [&]() -> SimResultTask<void> {
      log.emplace_back("inner_pre", ctx.nowNs());
      co_await simDelay(&ctx, Time::fromNs(20));
      log.emplace_back("inner_post", ctx.nowNs());
    };
    auto middle = [&]() -> SimResultTask<void> {
      log.emplace_back("middle_pre", ctx.nowNs());
      co_await inner();
      log.emplace_back("middle_post", ctx.nowNs());
      co_await simDelay(&ctx, Time::fromNs(30));
      log.emplace_back("middle_done", ctx.nowNs());
    };
    log.emplace_back("top_pre", ctx.nowNs());
    co_await middle();
    log.emplace_back("top_post", ctx.nowNs());
  };

  ctx.scheduleNow([&]() { runner(); });
  scheduler.run();

  const std::vector<std::pair<std::string, uint64_t>> expected = {
      {"top_pre", 0},
      {"middle_pre", 0},
      {"inner_pre", 0},
      {"inner_post", 20},
      {"middle_post", 20},
      {"middle_done", 50},
      {"top_post", 50},
  };
  EXPECT_EQ(log, expected);
}

} // namespace packsim::coro
