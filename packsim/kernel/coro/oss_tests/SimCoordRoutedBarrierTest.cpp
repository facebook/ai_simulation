/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimCoordRoutedBarrier.h"

#include <array>
#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <vector>

#include <folly/portability/GFlags.h>
#include <gmock/gmock.h>
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
using packsim::coro::SimCoordRoutedBarrier;
using packsim::coro::simDelay;
using packsim::coro::SimTask;
using packsim::coro::tests::CoroTestFixture;
using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim::coro {

// 1 microsecond expressed in femtoseconds — used as the lookahead in
// multi-thread tests so cross-context hops are visible at microsecond
// resolution.
constexpr uint64_t kOneMicrosecondFs = 1'000'000'000;

// Single-thread fixture pins the per-context lookahead to exactly 1ns
// (in fs) so the 2-hop coord round-trip takes exactly 2ns regardless of
// any future change to `kDefaultLookaheadFs` in kernel/scheduler.cpp or to
// the lookahead a simulation derives from its link delays.
constexpr uint64_t kSingleThreadLookaheadFs = 1'000'000;
constexpr uint64_t kSingleThreadTwoLookaheadHopsNs = 2;

// 8 worker threads: small enough for deterministic output ordering,
// large enough to surface cross-thread interactions that pdes_threads=1
// would mask. Stored as a string for SetCommandLineOption's API.
constexpr const char* kMultiThreadPdesWorkers = "8";

// Single-thread fixture for SimCoordRoutedBarrier tests. Pre-builds a
// Scheduler, coord context, and one participant context (the per-test setup
// that was previously duplicated across 6 of 7 tests). Scheduler is heap-
// allocated (not a fixture member) because PDES Scheduler::Scheduler()
// reads FLAGS_pdes_threads at construction; CoroTestFixture::SetUp()
// sets that flag, so the Scheduler MUST be constructed AFTER SetUp().
//
// Multi-context test creates its own participant vector and uses the
// SimCoordRoutedBarrierMultiThreadFixture below.
class SimCoordRoutedBarrierFixture : public CoroTestFixture {
 protected:
  void SetUp() override {
    CoroTestFixture::SetUp();
    scheduler_ = std::make_unique<Scheduler>();
    // Pin lookahead so single-thread tests' kSingleThreadTwoLookaheadHopsNs
    // assumption is independent of the default value.
    scheduler_->setLookaheadTimeFs(kSingleThreadLookaheadFs);
    coordCtx_ = std::make_unique<SimContext>(scheduler_.get(), "coord");
    participantCtx_ =
        std::make_unique<SimContext>(scheduler_.get(), "participant");
  }

  std::unique_ptr<Scheduler> scheduler_;
  std::unique_ptr<SimContext> coordCtx_;
  std::unique_ptr<SimContext> participantCtx_;
};
using SimCoordRoutedBarrierTest = SimCoordRoutedBarrierFixture;

// Multi-thread fixture for tests that exercise cross-context aggregation
// across PDES worker threads. Inherits CoroTestFixture's pdes_threads=1
// setup then overrides to 8. Pattern matches SimScheduleOnTest.
//
// Provides Scheduler (with 1us lookahead pre-set) and coord context;
// participant contexts are created per-test because their count varies.
class SimCoordRoutedBarrierMultiThreadFixture : public CoroTestFixture {
 protected:
  void SetUp() override {
    CoroTestFixture::SetUp();
    gflags::SetCommandLineOption("pdes_threads", kMultiThreadPdesWorkers);
    scheduler_ = std::make_unique<Scheduler>();
    scheduler_->setLookaheadTimeFs(kOneMicrosecondFs);
    coordCtx_ = std::make_unique<SimContext>(scheduler_.get(), "coord");
  }

  std::unique_ptr<Scheduler> scheduler_;
  std::unique_ptr<SimContext> coordCtx_;
};
using SimCoordRoutedBarrierMultiThreadTest =
    SimCoordRoutedBarrierMultiThreadFixture;

// Construction throws on numParticipants == 0 — guards against silently
// broken barriers (would never release). Validates exception MESSAGE so a
// future refactor adding another throw in the constructor doesn't pass
// this test for the wrong reason.
TEST_F(SimCoordRoutedBarrierTest, ConstructionRejectsNonPositiveCount) {
  EXPECT_THAT(
      [&]() { SimCoordRoutedBarrier barrier(coordCtx_.get(), 0); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("numParticipants must be > 0")));
}

// Single-participant barrier: one arrive triggers immediate release. Even
// so, resume is delayed by 2 × lookahead because of the coord round-trip.
TEST_F(
    SimCoordRoutedBarrierTest,
    SingleParticipantReleasesAfterTwoLookaheadHops) {
  SimCoordRoutedBarrier barrier(coordCtx_.get(), 1);
  std::optional<uint64_t> resumeTimeNs;

  auto task = [&]() -> SimTask {
    co_await barrier.arrive(participantCtx_.get());
    resumeTimeNs = participantCtx_->nowNs();
  };

  participantCtx_->scheduleNow([&]() { task(); });
  scheduler_->run();

  ASSERT_TRUE(resumeTimeNs.has_value());
  EXPECT_EQ(*resumeTimeNs, kSingleThreadTwoLookaheadHopsNs);
}

// Three coroutines on the same context arriving at a 3-participant
// barrier: all three release together at the same time (last-arriver
// triggers dispatch).
TEST_F(
    SimCoordRoutedBarrierTest,
    MultipleParticipantsSameContextReleaseTogether) {
  // Per-participant pre-arrival work; participant N waits 50 * (N+1) ns.
  // Slowest participant determines barrier release time.
  constexpr uint64_t kPerParticipantWorkNs = 50;
  constexpr uint32_t kNumParticipants = 3;

  SimCoordRoutedBarrier barrier(coordCtx_.get(), kNumParticipants);
  std::vector<uint64_t> wakeTimes;
  wakeTimes.reserve(kNumParticipants);

  auto participantTask = [&](const uint32_t participantNum) -> SimTask {
    co_await simDelay(
        participantCtx_.get(),
        Time::fromNs(kPerParticipantWorkNs * (participantNum + 1)));
    co_await barrier.arrive(participantCtx_.get());
    wakeTimes.emplace_back(participantCtx_->nowNs());
  };

  for (uint32_t i = 0; i < kNumParticipants; ++i) {
    participantCtx_->scheduleNow([&, i]() { participantTask(i); });
  }
  scheduler_->run();

  // Slowest participant arrives at t = 50 * kNumParticipants ns, then 2
  // lookahead hops.
  constexpr uint64_t expectedWake = kPerParticipantWorkNs * kNumParticipants +
      kSingleThreadTwoLookaheadHopsNs;
  const std::vector<uint64_t> expected(kNumParticipants, expectedWake);
  EXPECT_EQ(wakeTimes, expected);
}

// Iterative pattern: same SimCoordRoutedBarrier instance reused across multiple
// barrier cycles. Validates arrivals_.clear() lets the barrier rearm.
// Asserts both the iteration count AND the final time — the former
// catches "barrier never rearms" (test would fail with count=0); the
// latter catches "barrier rearms but releases early" (count would be
// right but timing would drift).
TEST_F(SimCoordRoutedBarrierTest, IterativeBarrierReuseSameInstance) {
  constexpr uint32_t kIterations = 4;
  constexpr uint32_t kParticipants = 2;
  constexpr uint64_t kPerIterationWorkNs = 10;
  // Per iteration: kPerIterationWorkNs of local work + 2ns barrier
  // round-trip (kSingleThreadTwoLookaheadHopsNs).
  constexpr uint64_t kPerIterationTotalNs =
      kPerIterationWorkNs + kSingleThreadTwoLookaheadHopsNs;

  SimCoordRoutedBarrier barrier(coordCtx_.get(), kParticipants);
  uint32_t completedIterations = 0;

  auto participantTask = [&]() -> SimTask {
    for (uint32_t i = 0; i < kIterations; ++i) {
      co_await simDelay(
          participantCtx_.get(), Time::fromNs(kPerIterationWorkNs));
      co_await barrier.arrive(participantCtx_.get());
    }
    completedIterations++;
  };

  for (uint32_t p = 0; p < kParticipants; ++p) {
    participantCtx_->scheduleNow([&]() { participantTask(); });
  }
  scheduler_->run();

  EXPECT_EQ(completedIterations, kParticipants);
  EXPECT_EQ(participantCtx_->nowNs(), kIterations * kPerIterationTotalNs);
}

// 2 of 3 participants arrive — neither resumes. After the 3rd arrives,
// all three resume together.
TEST_F(SimCoordRoutedBarrierTest, PartialArrivalsDoNotResume) {
  constexpr uint32_t kNumParticipants = 3;

  SimCoordRoutedBarrier barrier(coordCtx_.get(), kNumParticipants);
  // std::array (not std::vector<bool>) — vector<bool> is the bit-packed
  // specialization with quirks; std::array is unambiguous when size is
  // compile-time known. Default-init = all false.
  std::array<bool, kNumParticipants> resumed{};

  auto participantTask = [&](const uint32_t idx) -> SimTask {
    co_await barrier.arrive(participantCtx_.get());
    resumed[idx] = true;
  };

  // Submit kNumParticipants - 1 arrivals; barrier should not release.
  for (uint32_t i = 0; i < kNumParticipants - 1; ++i) {
    participantCtx_->scheduleNow([&, i]() { participantTask(i); });
  }
  scheduler_->run();

  constexpr std::array<bool, kNumParticipants> noneResumedYet{};
  EXPECT_EQ(resumed, noneResumedYet);

  // Last participant arrives — releases all kNumParticipants together.
  participantCtx_->scheduleNow(
      [&]() { participantTask(kNumParticipants - 1); });
  scheduler_->run();

  std::array<bool, kNumParticipants> allResumed{};
  allResumed.fill(true);
  EXPECT_EQ(resumed, allResumed);
}

// Multiple SimCoordRoutedBarriers share one coord context — validates the
// "coord-as-shared-infrastructure" pattern. Two independent barriers
// running concurrently must not interfere. Plain ints (not atomics)
// because the test fixture pins pdes_threads=1; all callbacks run on a
// single PDES worker thread.
TEST_F(SimCoordRoutedBarrierTest, MultipleBarriersShareSameCoordContext) {
  SimCoordRoutedBarrier barrierA(coordCtx_.get(), 2);
  SimCoordRoutedBarrier barrierB(coordCtx_.get(), 2);

  int aReleased = 0;
  int bReleased = 0;

  auto taskA = [&]() -> SimTask {
    co_await barrierA.arrive(participantCtx_.get());
    ++aReleased;
  };
  auto taskB = [&]() -> SimTask {
    co_await barrierB.arrive(participantCtx_.get());
    ++bReleased;
  };

  participantCtx_->scheduleNow([&]() { taskA(); });
  participantCtx_->scheduleNow([&]() { taskA(); });
  participantCtx_->scheduleNow([&]() { taskB(); });
  participantCtx_->scheduleNow([&]() { taskB(); });
  scheduler_->run();

  EXPECT_EQ(aReleased, 2);
  EXPECT_EQ(bReleased, 2);
}

// Multi-context, multi-thread: 4 participants across 4 SimContexts under
// pdes_threads=8 with 1us lookahead (pre-set in fixture). All resume at
// coord round-trip time = 2us. Asserts in nanoseconds (not microseconds)
// so a regression drifting the resume time by < 1us would still fail
// loudly.
TEST_F(
    SimCoordRoutedBarrierMultiThreadTest,
    MultiContextParticipantsReleaseTogether) {
  constexpr uint32_t kNumParticipants = 4;
  std::vector<std::unique_ptr<SimContext>> participantCtxs;
  participantCtxs.reserve(kNumParticipants);
  for (uint32_t i = 0; i < kNumParticipants; ++i) {
    // Vary seed by index so each context has a distinct random stream.
    participantCtxs.emplace_back(
        std::make_unique<SimContext>(
            scheduler_.get(),
            "participant",
            /*seed=*/i + 1,
            /*conditionalMap=*/nullptr));
  }

  SimCoordRoutedBarrier barrier(coordCtx_.get(), kNumParticipants);
  // std::array (not std::vector) — kNumParticipants is compile-time
  // known, and std::vector<std::atomic<T>> is non-portable
  // (std::atomic<T> is neither copyable nor movable, violating
  // std::vector<T> requirements).
  constexpr uint64_t kNotResumed = std::numeric_limits<uint64_t>::max();
  std::array<std::atomic<uint64_t>, kNumParticipants> resumeTimesNs{};
  for (auto& t : resumeTimesNs) {
    t.store(kNotResumed);
  }

  auto participantTask = [&](SimContext* myCtx, const uint32_t idx) -> SimTask {
    co_await barrier.arrive(myCtx);
    resumeTimesNs[idx].store(myCtx->nowNs());
  };

  for (uint32_t i = 0; i < kNumParticipants; ++i) {
    SimContext* ctx = participantCtxs.at(i).get();
    ctx->scheduleNow([&, ctx, i]() { participantTask(ctx, i); });
  }
  scheduler_->run();

  // 2 lookahead hops at 1us each = 2000ns.
  constexpr uint64_t kTwoLookaheadHopsNs = 2000;
  for (uint32_t i = 0; i < kNumParticipants; ++i) {
    EXPECT_EQ(resumeTimesNs[i].load(), kTwoLookaheadHopsNs)
        << "participant " << i
        << " should resume at 2000ns (2 * 1us lookahead hops)";
  }
}

// N=1 boundary: a single-participant barrier reused across multiple
// iterations. Pins that the immediate-release threshold (1 of 1)
// re-arms cleanly between iterations — easy to break with off-by-one
// in the rearm path.
TEST_F(SimCoordRoutedBarrierTest, SingleParticipantIterativeReuse) {
  constexpr uint32_t kIterations = 4;
  // Per iteration: 2ns barrier round-trip (2 lookahead hops at 1ns).
  constexpr uint64_t kPerIterationTotalNs = kSingleThreadTwoLookaheadHopsNs;

  SimCoordRoutedBarrier barrier(coordCtx_.get(), 1);
  uint32_t completedIterations = 0;

  auto participantTask = [&]() -> SimTask {
    for (uint32_t i = 0; i < kIterations; ++i) {
      co_await barrier.arrive(participantCtx_.get());
      completedIterations++;
    }
  };

  participantCtx_->scheduleNow([&]() { participantTask(); });
  scheduler_->run();

  EXPECT_EQ(completedIterations, kIterations);
  EXPECT_EQ(participantCtx_->nowNs(), kIterations * kPerIterationTotalNs);
}

// Heterogeneous barriers (N=2 and N=3) sharing one coord context.
// Sharper validation of the "coord-as-shared-infrastructure" pattern:
// proves the coord routes arrivals to the right barrier instance based
// on the pointer in the scheduleRemote bind, not on any per-barrier
// implicit assumption about N.
TEST_F(SimCoordRoutedBarrierTest, MixedNBarriersShareSameCoordContext) {
  SimCoordRoutedBarrier barrierA(coordCtx_.get(), 2);
  SimCoordRoutedBarrier barrierB(coordCtx_.get(), 3);

  int aReleased = 0;
  int bReleased = 0;

  auto taskA = [&]() -> SimTask {
    co_await barrierA.arrive(participantCtx_.get());
    ++aReleased;
  };
  auto taskB = [&]() -> SimTask {
    co_await barrierB.arrive(participantCtx_.get());
    ++bReleased;
  };

  // 2 arrivals for barrierA, 3 for barrierB. Both should release.
  participantCtx_->scheduleNow([&]() { taskA(); });
  participantCtx_->scheduleNow([&]() { taskA(); });
  participantCtx_->scheduleNow([&]() { taskB(); });
  participantCtx_->scheduleNow([&]() { taskB(); });
  participantCtx_->scheduleNow([&]() { taskB(); });
  scheduler_->run();

  EXPECT_EQ(aReleased, 2);
  EXPECT_EQ(bReleased, 3);
}

// Multi-context iteration loop under pdes_threads=8: combines the
// rearm-after-release path with cross-thread aggregation. The strongest
// test of the coord-context pattern's two main claims (reuse + thread
// safety) running together.
TEST_F(SimCoordRoutedBarrierMultiThreadTest, MultiContextIterativeReuse) {
  constexpr uint32_t kNumParticipants = 4;
  constexpr uint32_t kIterations = 3;
  // Per iteration: 2us barrier round-trip (2 hops at 1us lookahead).
  constexpr uint64_t kPerIterationTotalNs = 2000;

  std::vector<std::unique_ptr<SimContext>> participantCtxs;
  participantCtxs.reserve(kNumParticipants);
  for (uint32_t i = 0; i < kNumParticipants; ++i) {
    participantCtxs.emplace_back(
        std::make_unique<SimContext>(
            scheduler_.get(),
            "participant",
            /*seed=*/i + 1,
            /*conditionalMap=*/nullptr));
  }

  SimCoordRoutedBarrier barrier(coordCtx_.get(), kNumParticipants);
  // Same default-init + explicit .store() pattern as resumeTimesNs in
  // MultiContextParticipantsReleaseTogether. Pre-C++20 std::atomic<T>{}
  // leaves the value indeterminate; an explicit store(0) makes the
  // initial value well-defined regardless of toolchain version.
  std::array<std::atomic<uint32_t>, kNumParticipants> completedIters{};
  for (auto& c : completedIters) {
    c.store(0);
  }

  auto participantTask = [&](SimContext* myCtx, const uint32_t idx) -> SimTask {
    for (uint32_t i = 0; i < kIterations; ++i) {
      co_await barrier.arrive(myCtx);
      completedIters[idx].fetch_add(1);
    }
  };

  for (uint32_t i = 0; i < kNumParticipants; ++i) {
    SimContext* ctx = participantCtxs.at(i).get();
    ctx->scheduleNow([&, ctx, i]() { participantTask(ctx, i); });
  }
  scheduler_->run();

  for (uint32_t i = 0; i < kNumParticipants; ++i) {
    EXPECT_EQ(completedIters[i].load(), kIterations)
        << "participant " << i << " should complete all " << kIterations
        << " iterations";
    EXPECT_EQ(
        participantCtxs.at(i)->nowNs(), kIterations * kPerIterationTotalNs)
        << "participant " << i << " final time should be "
        << (kIterations * kPerIterationTotalNs) << "ns";
  }
}

} // namespace packsim::coro
