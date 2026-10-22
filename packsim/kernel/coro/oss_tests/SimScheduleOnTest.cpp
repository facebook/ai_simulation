/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimScheduleOn.h"

#include <atomic>
#include <cstdint>
#include <optional>
#include <vector>

#include <folly/portability/GFlags.h>
#include <gtest/gtest.h>

#include "packsim/kernel/coro/SimDelay.h"
#include "packsim/kernel/coro/SimSignal.h"
#include "packsim/kernel/coro/SimTask.h"
#include "packsim/kernel/coro/oss_tests/fixtures/CoroTestFixture.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

using packsim::Scheduler;
using packsim::SimContext;
using packsim::Time;
using packsim::coro::simDelay;
using packsim::coro::simScheduleOn;
using packsim::coro::SimSignal;
using packsim::coro::SimTask;
using packsim::coro::tests::CoroTestFixture;

namespace packsim::coro {

// 1us in femtoseconds — used as the engine lookahead for cross-context tests.
// Larger than zero so cross-context scheduling has room to advance time.
constexpr uint64_t kOneMicrosecondFs = 1'000'000'000;

// Same value expressed in nanoseconds — the resume-time observed by tests
// after a single cross-context migration with the lookahead set above.
// Derived from kOneMicrosecondFs so the two cannot drift out of sync if
// either is changed.
constexpr uint64_t kLookaheadNs = kOneMicrosecondFs / 1'000'000;
static_assert(
    kLookaheadNs * 1'000'000 == kOneMicrosecondFs,
    "kLookaheadNs must be the ns-equivalent of kOneMicrosecondFs (1us)");

using SimScheduleOnTest = CoroTestFixture;

// Multi-threaded fixture for tests that exercise cross-context migration
// across PDES worker threads. Overrides the parent fixture's pdes_threads=1
// with pdes_threads=8.
//
// Calls CoroTestFixture::SetUp() first (defensive — picks up any future
// base-fixture setup steps automatically) and then overwrites
// pdes_threads to 8. SetCommandLineOption is idempotent so the "1 → 8"
// transition is a cheap no-op cost. The base's flagSaver_ member
// saves/restores the gflag on fixture teardown.
class SimScheduleOnMultiThreadTest : public CoroTestFixture {
 protected:
  void SetUp() override {
    CoroTestFixture::SetUp();
    gflags::SetCommandLineOption("pdes_threads", "8");
  }
};

// Single-thread cross-context migration: coroutine moves from ctxA to ctxB.
// Lookahead delay is added by scheduleRemote.
TEST_F(SimScheduleOnTest, BasicMigration) {
  Scheduler scheduler;
  // Set lookahead larger than 0 so cross-context scheduling has room.
  scheduler.setLookaheadTimeFs(kOneMicrosecondFs);
  SimContext ctxA(&scheduler, "ctxA", 0, nullptr);
  SimContext ctxB(&scheduler, "ctxB", 0, nullptr);

  // Per-stage delays for the timeline below.
  constexpr uint64_t kPreMigrateDelayNs = 10;
  constexpr uint64_t kPostMigrateDelayNs = 20;

  std::vector<std::pair<std::string, uint64_t>> log;
  log.reserve(4);

  auto task = [&]() -> SimTask {
    log.emplace_back("on_A_start", ctxA.nowNs());
    co_await simDelay(&ctxA, Time::fromNs(kPreMigrateDelayNs));
    log.emplace_back("on_A_after_delay", ctxA.nowNs());

    co_await simScheduleOn(&ctxB);
    log.emplace_back("on_B_after_migrate", ctxB.nowNs());

    co_await simDelay(&ctxB, Time::fromNs(kPostMigrateDelayNs));
    log.emplace_back("on_B_after_delay", ctxB.nowNs());
  };

  ctxA.scheduleNow([&]() { task(); });
  scheduler.run();

  // Expected timeline — names in 'expected' lay out the arithmetic so the
  // relationship between delays and timestamps is explicit (rather than
  // bare 1010, 1030 magic numbers).
  const std::vector<std::pair<std::string, uint64_t>> expected = {
      {"on_A_start", 0},
      {"on_A_after_delay", kPreMigrateDelayNs},
      {"on_B_after_migrate", kPreMigrateDelayNs + kLookaheadNs},
      {"on_B_after_delay",
       kPreMigrateDelayNs + kLookaheadNs + kPostMigrateDelayNs},
  };
  EXPECT_EQ(log, expected);
}

// Migrate back and forth between two contexts.
TEST_F(SimScheduleOnTest, BackAndForthMigration) {
  Scheduler scheduler;
  scheduler.setLookaheadTimeFs(kOneMicrosecondFs);
  SimContext ctxA(&scheduler, "A", 0, nullptr);
  SimContext ctxB(&scheduler, "B", 0, nullptr);

  // Number of A→B→A round-trips. The coroutine increments aCount once
  // before each migration to B, and once more after the loop, so the
  // expected counts are kRoundTrips+1 on A and kRoundTrips on B.
  constexpr int kRoundTrips = 3;

  int aCount = 0;
  int bCount = 0;

  auto task = [&]() -> SimTask {
    for (int i = 0; i < kRoundTrips; ++i) {
      aCount++;
      co_await simScheduleOn(&ctxB);
      bCount++;
      co_await simScheduleOn(&ctxA);
    }
    aCount++;
  };

  ctxA.scheduleNow([&]() { task(); });
  scheduler.run();

  EXPECT_EQ(aCount, kRoundTrips + 1);
  EXPECT_EQ(bCount, kRoundTrips);
}

// Multi-thread (PDES_THREADS=8): N coroutines on ctxA all migrate to ctxB.
// Since PDES guarantees per-context single-thread execution, ctxA's events
// run on one thread, ctxB's on another. The migrate operation enqueues
// resume on the other thread's queue.
TEST_F(SimScheduleOnMultiThreadTest, MultiThreadMigrationCountsCorrect) {
  Scheduler scheduler;
  scheduler.setLookaheadTimeFs(kOneMicrosecondFs);
  SimContext ctxA(&scheduler, "A", 0, nullptr);
  SimContext ctxB(&scheduler, "B", 0, nullptr);

  constexpr int kNumTasks = 100;
  std::atomic<int> resumeCount{0};

  auto task = [&]() -> SimTask {
    co_await simScheduleOn(&ctxB);
    resumeCount.fetch_add(1, std::memory_order_relaxed);
  };

  for (int i = 0; i < kNumTasks; ++i) {
    ctxA.scheduleNow([&]() { task(); });
  }

  scheduler.run();

  EXPECT_EQ(resumeCount.load(std::memory_order_relaxed), kNumTasks);
}

// Bidirectional multi-thread stress: half the tasks migrate ctxA→ctxB and
// the other half ctxB→ctxA, simultaneously, under PDES_THREADS=8.
// Stronger contention shape than the one-way MultiThreadMigrationCountsCorrect.
TEST_F(SimScheduleOnMultiThreadTest, MultiThreadBidirectionalNoLostWakeups) {
  Scheduler scheduler;
  scheduler.setLookaheadTimeFs(kOneMicrosecondFs);
  SimContext ctxA(&scheduler, "A", 0, nullptr);
  SimContext ctxB(&scheduler, "B", 0, nullptr);

  constexpr int kNumPerDirection = 50;
  std::atomic<int> aToBCount{0};
  std::atomic<int> bToACount{0};

  auto aToB = [&]() -> SimTask {
    co_await simScheduleOn(&ctxB);
    aToBCount.fetch_add(1, std::memory_order_relaxed);
  };
  auto bToA = [&]() -> SimTask {
    co_await simScheduleOn(&ctxA);
    bToACount.fetch_add(1, std::memory_order_relaxed);
  };

  for (int i = 0; i < kNumPerDirection; ++i) {
    ctxA.scheduleNow([&]() { aToB(); });
    ctxB.scheduleNow([&]() { bToA(); });
  }

  scheduler.run();

  EXPECT_EQ(aToBCount.load(std::memory_order_relaxed), kNumPerDirection);
  EXPECT_EQ(bToACount.load(std::memory_order_relaxed), kNumPerDirection);
}

// Multi-thread stress: repeat to catch any wakeup races. Each iteration
// schedules N migrations and verifies the count.
TEST_F(SimScheduleOnMultiThreadTest, MultiThreadStressNoLostWakeups) {
  constexpr int kIterations = 50;
  constexpr int kTasksPerIteration = 32;

  for (int iter = 0; iter < kIterations; ++iter) {
    Scheduler scheduler;
    scheduler.setLookaheadTimeFs(kOneMicrosecondFs);
    SimContext ctxA(&scheduler, "A", 0, nullptr);
    SimContext ctxB(&scheduler, "B", 0, nullptr);

    std::atomic<int> count{0};
    auto task = [&]() -> SimTask {
      co_await simScheduleOn(&ctxB);
      count.fetch_add(1, std::memory_order_relaxed);
    };

    for (int i = 0; i < kTasksPerIteration; ++i) {
      ctxA.scheduleNow([&]() { task(); });
    }

    scheduler.run();
    EXPECT_EQ(count.load(std::memory_order_relaxed), kTasksPerIteration)
        << "Iteration " << iter << " lost a wakeup";
  }
}

// simScheduleOn from a context to itself: still goes through scheduleRemote
// (with lookahead). Documents that there is no same-context optimization
// in the awaiter itself — callers should avoid simScheduleOn(currentCtx).
//
// Contrast with simYield(ctx): a plain yield on the same context defers to
// the next dispatch with zero time advance. simScheduleOn(currentCtx) pays
// the lookahead even though no actual migration happens — that is the
// footgun the header docstring warns about.
TEST_F(SimScheduleOnTest, SameContextStillAddsLookaheadDelay) {
  Scheduler scheduler;
  scheduler.setLookaheadTimeFs(kOneMicrosecondFs);
  SimContext ctx(&scheduler, "self", 0, nullptr);

  // optional (not -1 sentinel): explicit "did the coroutine resume?" check
  // and unambiguous about the resume time we observed.
  std::optional<uint64_t> resumeTimeNs;
  auto task = [&]() -> SimTask {
    co_await simScheduleOn(&ctx); // no-op semantically, but adds lookahead
    resumeTimeNs = ctx.nowNs();
  };

  ctx.scheduleNow([&]() { task(); });
  scheduler.run();

  ASSERT_TRUE(resumeTimeNs.has_value()) << "coroutine never resumed";
  // Even self-migration adds lookahead — documented behavior.
  EXPECT_EQ(*resumeTimeNs, kLookaheadNs);
}

// Cross-context coordination: SimSignal is per-context, so to wait on a
// signal that lives on ctxA from a coroutine running on ctxB, the
// coroutine first migrates to ctxA via simScheduleOn, then awaits the
// signal there. Pins the documented "migrate first, then call the
// per-context primitive" pattern.
TEST_F(SimScheduleOnTest, CrossContextSimSignalViaSimScheduleOn) {
  Scheduler scheduler;
  scheduler.setLookaheadTimeFs(kOneMicrosecondFs);
  SimContext ctxA(&scheduler, "A", 0, nullptr);
  SimContext ctxB(&scheduler, "B", 0, nullptr);

  // SimSignal lives on ctxA.
  SimSignal sig(&ctxA);
  std::optional<uint64_t> consumerWakeNs;

  // Consumer starts on ctxB, migrates to ctxA, awaits signal there.
  auto consumer = [&]() -> SimTask {
    co_await simScheduleOn(&ctxA);
    co_await sig.wait();
    consumerWakeNs = ctxA.nowNs();
  };

  // Producer fires the signal on ctxA after a delay long enough that the
  // consumer has already migrated and is waiting (migration costs one
  // lookahead, so 2 * lookahead is a safe upper bound).
  constexpr uint64_t kSignalFireDelayNs = 2 * kLookaheadNs;
  ctxA.schedule(Time::fromNs(kSignalFireDelayNs), [&]() { sig.signal(); });

  ctxB.scheduleNow([&]() { consumer(); });
  scheduler.run();

  // Assert the wake time, not just that it resumed — a regression that
  // dropped the lookahead delay (e.g., simScheduleOn becoming a no-op
  // for same-context, or scheduleRemote stopping its lookahead add)
  // would still resume the consumer, just at the wrong logical time.
  ASSERT_TRUE(consumerWakeNs.has_value()) << "consumer never resumed";
  EXPECT_EQ(*consumerWakeNs, kSignalFireDelayNs)
      << "consumer should wake exactly when the signal fires on ctxA";
}

} // namespace packsim::coro
