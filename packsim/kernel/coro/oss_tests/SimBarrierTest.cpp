/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimBarrier.h"

#include <array>
#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <tuple>
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
using packsim::coro::SimBarrier;
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

// Single-thread tests rely on the raw Scheduler default lookahead (1ns,
// from kDefaultLookaheadFs in kernel/scheduler.cpp). Each cross-context
// hop adds 1ns of sim time. The flat N-way SimBarrier dispatches
// resumes directly from the last-to-arrive participant (no coord
// round-trip) — so the round-trip is 1 hop, 1ns total in single-thread
// tests (vs the 2ns a coord-routed N-1-N design would take).
constexpr uint64_t kSingleThreadOneLookaheadHopNs = 1;

// 8 worker threads: small enough for deterministic output ordering,
// large enough to surface cross-thread interactions that pdes_threads=1
// would mask. Stored as a string for SetCommandLineOption's API.
constexpr const char* kMultiThreadPdesWorkers = "8";

// Single-thread fixture for SimBarrier tests. Pre-builds a Scheduler
// and one participant context. Scheduler is held by std::unique_ptr
// (constructed in SetUp(), not as a stack-initialized data member)
// because PDES Scheduler::Scheduler() reads FLAGS_pdes_threads at
// construction; CoroTestFixture::SetUp() sets that flag, so the
// Scheduler MUST be constructed AFTER SetUp().
//
// SimBarrier self-coordinates via atomics — no coord context needed.
// Multi-context tests create their own participant vector and use
// SimBarrierMultiThreadFixture below.
class SimBarrierFixture : public CoroTestFixture {
 protected:
  void SetUp() override {
    CoroTestFixture::SetUp();
    scheduler_ = std::make_unique<Scheduler>();
    participantCtx_ =
        std::make_unique<SimContext>(scheduler_.get(), "participant");
  }

  std::unique_ptr<Scheduler> scheduler_;
  std::unique_ptr<SimContext> participantCtx_;
};
using SimBarrierTest = SimBarrierFixture;

// Multi-thread fixture for tests that exercise cross-context aggregation
// across PDES worker threads. Inherits CoroTestFixture's pdes_threads=1
// setup then overrides to 8. Pattern matches SimScheduleOnTest.
//
// Provides Scheduler with 1us lookahead pre-set; participant contexts
// are created per-test because their count varies.
class SimBarrierMultiThreadFixture : public CoroTestFixture {
 protected:
  void SetUp() override {
    CoroTestFixture::SetUp();
    gflags::SetCommandLineOption("pdes_threads", kMultiThreadPdesWorkers);
    scheduler_ = std::make_unique<Scheduler>();
    scheduler_->setLookaheadTimeFs(kOneMicrosecondFs);
  }

  std::unique_ptr<Scheduler> scheduler_;
};
using SimBarrierMultiThreadTest = SimBarrierMultiThreadFixture;

// Construction throws on numParticipants == 0 — guards against silently
// broken barriers (would never release). Validates exception MESSAGE so a
// future refactor adding another throw in the constructor doesn't pass
// this test for the wrong reason.
TEST_F(SimBarrierTest, ConstructionRejectsNonPositiveCount) {
  EXPECT_THAT(
      []() { std::ignore = std::make_shared<SimBarrier>(0); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("numParticipants must be > 0")));
}

// arrive() throws std::bad_weak_ptr when called on a SimBarrier that
// wasn't constructed via std::make_shared. This is the primary structural
// guard in the shared_ptr lifetime model: stack-allocated barriers fail
// loudly at the first arrive() call instead of silently UAFing later.
TEST_F(SimBarrierTest, ArriveOnNonSharedBarrierThrowsBadWeakPtr) {
  // Intentional stack allocation: the whole point of this test is to
  // verify that arrive() throws std::bad_weak_ptr when the barrier
  // isn't owned by a shared_ptr. clang-tidy's blanket rule against
  // stack-allocating enable_shared_from_this subclasses doesn't
  // distinguish "unsafe in prod" from "the test IS the guard".
  // NOLINTNEXTLINE(facebook-hte-UnmanagedEnableSharedFromThis)
  SimBarrier stackBarrier(/*numParticipants=*/2);
  // EXPECT_THAT + Throws lets us discard arrive()'s [[nodiscard]] result
  // inside the lambda body without the lambda's noreturn semantics tripping
  // the warning at the macro expansion site.
  EXPECT_THAT(
      [&]() { std::ignore = stackBarrier.arrive(participantCtx_.get()); },
      ::testing::Throws<std::bad_weak_ptr>());
}

// Single-participant barrier: one arrive triggers immediate release.
// Resume happens 1 × lookahead after the dispatcher fires (vs the 2 ×
// lookahead of the original coord-routed design).
TEST_F(SimBarrierTest, SingleParticipantReleasesAfterOneLookaheadHop) {
  auto barrier = std::make_shared<SimBarrier>(1);
  std::optional<uint64_t> resumeTimeNs;

  auto task = [&]() -> SimTask {
    co_await barrier->arrive(participantCtx_.get());
    resumeTimeNs = participantCtx_->nowNs();
  };

  participantCtx_->scheduleNow([&]() { task(); });
  scheduler_->run();

  ASSERT_TRUE(resumeTimeNs.has_value());
  EXPECT_EQ(*resumeTimeNs, kSingleThreadOneLookaheadHopNs);
}

// Three coroutines on the same context arriving at a 3-participant
// barrier: all three release together at the same time (last-arriver
// triggers dispatch).
TEST_F(SimBarrierTest, MultipleParticipantsSameContextReleaseTogether) {
  // Per-participant pre-arrival work; participant N waits 50 * (N+1) ns.
  // Slowest participant determines barrier release time.
  constexpr uint64_t kPerParticipantWorkNs = 50;
  constexpr uint32_t kNumParticipants = 3;

  auto barrier = std::make_shared<SimBarrier>(kNumParticipants);
  std::vector<uint64_t> wakeTimes;
  wakeTimes.reserve(kNumParticipants);

  auto participantTask = [&](const uint32_t participantNum) -> SimTask {
    co_await simDelay(
        participantCtx_.get(),
        Time::fromNs(kPerParticipantWorkNs * (participantNum + 1)));
    co_await barrier->arrive(participantCtx_.get());
    wakeTimes.emplace_back(participantCtx_->nowNs());
  };

  for (uint32_t i = 0; i < kNumParticipants; ++i) {
    participantCtx_->scheduleNow([&, i]() { participantTask(i); });
  }
  scheduler_->run();

  // Slowest participant arrives at t = 50 * kNumParticipants ns, then 1
  // lookahead hop.
  constexpr uint64_t expectedWake =
      kPerParticipantWorkNs * kNumParticipants + kSingleThreadOneLookaheadHopNs;
  const std::vector<uint64_t> expected(kNumParticipants, expectedWake);
  EXPECT_EQ(wakeTimes, expected);
}

// Iterative pattern: same SimBarrier instance reused across multiple
// barrier cycles. Validates the dispatcher's reset of arrived_ lets the
// barrier rearm. Asserts both the iteration count AND the final time —
// the former catches "barrier never rearms" (test would fail with
// count=0); the latter catches "barrier rearms but releases early"
// (count would be right but timing would drift).
TEST_F(SimBarrierTest, IterativeBarrierReuseSameInstance) {
  constexpr uint32_t kIterations = 4;
  constexpr uint32_t kParticipants = 2;
  constexpr uint64_t kPerIterationWorkNs = 10;
  // Per iteration: kPerIterationWorkNs of local work + 1ns barrier
  // round-trip (kSingleThreadOneLookaheadHopNs).
  constexpr uint64_t kPerIterationTotalNs =
      kPerIterationWorkNs + kSingleThreadOneLookaheadHopNs;

  auto barrier = std::make_shared<SimBarrier>(kParticipants);
  uint32_t completedIterations = 0;

  auto participantTask = [&]() -> SimTask {
    for (uint32_t i = 0; i < kIterations; ++i) {
      co_await simDelay(
          participantCtx_.get(), Time::fromNs(kPerIterationWorkNs));
      co_await barrier->arrive(participantCtx_.get());
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
TEST_F(SimBarrierTest, PartialArrivalsDoNotResume) {
  constexpr uint32_t kNumParticipants = 3;

  auto barrier = std::make_shared<SimBarrier>(kNumParticipants);
  // std::array (not std::vector<bool>) — vector<bool> is the bit-packed
  // specialization with quirks; std::array is unambiguous when size is
  // compile-time known. Default-init = all false.
  std::array<bool, kNumParticipants> resumed{};

  auto participantTask = [&](const uint32_t idx) -> SimTask {
    co_await barrier->arrive(participantCtx_.get());
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

// Two independent SimBarriers running concurrently must not interfere —
// each has its own atomic counter and waiter queue. Plain ints (not
// atomics) because the test fixture pins pdes_threads=1; all callbacks
// run on a single PDES worker thread.
TEST_F(SimBarrierTest, MultipleBarriersIndependent) {
  auto barrierA = std::make_shared<SimBarrier>(2);
  auto barrierB = std::make_shared<SimBarrier>(2);

  int aReleased = 0;
  int bReleased = 0;

  auto taskA = [&]() -> SimTask {
    co_await barrierA->arrive(participantCtx_.get());
    ++aReleased;
  };
  auto taskB = [&]() -> SimTask {
    co_await barrierB->arrive(participantCtx_.get());
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
// 1 lookahead = 1us. Asserts in nanoseconds (not microseconds) so a
// regression drifting the resume time would still fail loudly.
TEST_F(SimBarrierMultiThreadTest, MultiContextParticipantsReleaseTogether) {
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

  auto barrier = std::make_shared<SimBarrier>(kNumParticipants);
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
    co_await barrier->arrive(myCtx);
    resumeTimesNs[idx].store(myCtx->nowNs());
  };

  for (uint32_t i = 0; i < kNumParticipants; ++i) {
    SimContext* ctx = participantCtxs.at(i).get();
    ctx->scheduleNow([&, ctx, i]() { participantTask(ctx, i); });
  }
  scheduler_->run();

  // 1 lookahead hop at 1us = 1000ns.
  constexpr uint64_t kOneLookaheadHopNs = 1000;
  for (uint32_t i = 0; i < kNumParticipants; ++i) {
    EXPECT_EQ(resumeTimesNs[i].load(), kOneLookaheadHopNs)
        << "participant " << i
        << " should resume at 1000ns (1 * 1us lookahead hop)";
  }
}

// N=1 boundary: a single-participant barrier reused across multiple
// iterations. Pins that the immediate-release threshold (1 of 1)
// re-arms cleanly between iterations — easy to break with off-by-one
// in the rearm path.
TEST_F(SimBarrierTest, SingleParticipantIterativeReuse) {
  constexpr uint32_t kIterations = 4;
  // Per iteration: 1ns barrier round-trip (1 lookahead hop at 1ns).
  constexpr uint64_t kPerIterationTotalNs = kSingleThreadOneLookaheadHopNs;

  auto barrier = std::make_shared<SimBarrier>(1);
  int completedIterations = 0;

  auto participantTask = [&]() -> SimTask {
    for (uint32_t i = 0; i < kIterations; ++i) {
      co_await barrier->arrive(participantCtx_.get());
    }
    completedIterations++;
  };

  participantCtx_->scheduleNow([&]() { participantTask(); });
  scheduler_->run();

  EXPECT_EQ(completedIterations, 1);
  EXPECT_EQ(participantCtx_->nowNs(), kIterations * kPerIterationTotalNs);
}

// Heterogeneous barriers (N=2 and N=3) running independently — pins
// that each barrier's atomic counter is private to that instance and
// the dispatcher condition (count == N) uses the right N for each.
TEST_F(SimBarrierTest, MixedNBarriersIndependent) {
  auto barrierA = std::make_shared<SimBarrier>(2);
  auto barrierB = std::make_shared<SimBarrier>(3);

  int aReleased = 0;
  int bReleased = 0;

  auto taskA = [&]() -> SimTask {
    co_await barrierA->arrive(participantCtx_.get());
    ++aReleased;
  };
  auto taskB = [&]() -> SimTask {
    co_await barrierB->arrive(participantCtx_.get());
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
// test of the flat N-way design's two main claims (reuse + thread
// safety) running together.
TEST_F(SimBarrierMultiThreadTest, MultiContextIterativeReuse) {
  constexpr uint32_t kNumParticipants = 4;
  constexpr uint32_t kIterations = 3;
  // Per iteration: 1us barrier round-trip (1 hop at 1us lookahead).
  constexpr uint64_t kPerIterationTotalNs = 1000;

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

  auto barrier = std::make_shared<SimBarrier>(kNumParticipants);
  // Same default-init + explicit .store() pattern as resumeTimesNs above.
  // Pre-C++20 std::atomic<T>{} leaves the value indeterminate; an
  // explicit store(0) makes the initial value well-defined regardless
  // of toolchain version.
  std::array<std::atomic<uint32_t>, kNumParticipants> completedIters{};
  for (auto& c : completedIters) {
    c.store(0);
  }

  auto participantTask = [&](SimContext* myCtx, const uint32_t idx) -> SimTask {
    for (uint32_t i = 0; i < kIterations; ++i) {
      co_await barrier->arrive(myCtx);
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

// Validates the shared_ptr lifetime contract: after all participants
// resume and drop their references, the barrier auto-destructs. We
// observe destruction by capturing a weak_ptr alongside the shared_ptr,
// running the simulation, dropping our own ref, and asserting the
// weak_ptr has expired.
TEST_F(SimBarrierTest, BarrierAutoDestructsAfterParticipantsComplete) {
  constexpr uint32_t kNumParticipants = 3;
  std::weak_ptr<SimBarrier> weakBarrier;

  {
    auto barrier = std::make_shared<SimBarrier>(kNumParticipants);
    weakBarrier = barrier;

    auto participantTask = [barrier, this]() -> SimTask {
      co_await barrier->arrive(participantCtx_.get());
    };

    for (uint32_t i = 0; i < kNumParticipants; ++i) {
      participantCtx_->scheduleNow([&]() { participantTask(); });
    }
    scheduler_->run();

    // After run(): the per-participant Awaitables have all released
    // their strong refs (one per arrive() that completed). Two refs
    // remain — the local `barrier` and the single capture inside
    // `participantTask` (one lambda shared by all N inner launchers).
    // Both go out of scope at the closing `}` below, dropping the
    // refcount to 0 and destructing the barrier.
  }

  EXPECT_TRUE(weakBarrier.expired())
      << "Barrier should auto-destruct after all participants complete "
         "and the user's shared_ptr drops.";
}

} // namespace packsim::coro
