/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimWaitFirst.h"

#include <memory>
#include <optional>

#include <gtest/gtest.h>

#include "packsim/kernel/coro/SimCancellableDelay.h"
#include "packsim/kernel/coro/SimEvent.h"
#include "packsim/kernel/coro/SimScopedTask.h"
#include "packsim/kernel/coro/SimSignal.h"
#include "packsim/kernel/coro/SimStopwatch.h"
#include "packsim/kernel/coro/SimTask.h"
#include "packsim/kernel/coro/oss_tests/fixtures/CoroTestFixture.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

using packsim::Scheduler;
using packsim::SimContext;
using packsim::Time;
using packsim::coro::collectAny;
using packsim::coro::SimCancellableDelay;
using packsim::coro::SimEvent;
using packsim::coro::SimEventMode;
using packsim::coro::SimScopedTask;
using packsim::coro::SimSignal;
using packsim::coro::SimStopwatch;
using packsim::coro::SimTask;
using packsim::coro::simWaitFirst;
using packsim::coro::SimWaitFirstAwaitable;
using packsim::coro::tests::CoroTestFixture;

namespace packsim::coro {

// Single-thread fixture for SimWaitFirst tests. Same shape as
// SimBarrierFixture: scheduler + one participant context constructed in
// SetUp() (after CoroTestFixture::SetUp() sets FLAGS_pdes_threads).
class SimWaitFirstFixture : public CoroTestFixture {
 protected:
  void SetUp() override {
    CoroTestFixture::SetUp();
    scheduler_ = std::make_unique<Scheduler>();
    ctx_ = std::make_unique<SimContext>(scheduler_.get(), "ctx");
  }

  std::unique_ptr<Scheduler> scheduler_;
  std::unique_ptr<SimContext> ctx_;
};
using SimWaitFirstTest = SimWaitFirstFixture;

// ============================================================
// Two-operand races
// ============================================================

// Delay vs signal where signal fires first. Index 1 (signal) wins.
TEST_F(SimWaitFirstTest, TwoOperandsSignalWinsBeforeDelay) {
  SimCancellableDelay delay;
  SimSignal sig{ctx_.get()};
  std::optional<size_t> winner;

  auto task = [&]() -> SimTask {
    winner = co_await simWaitFirst(
        delay.awaitDelay(ctx_.get(), Time::fromNs(1000)), sig.wait());
  };

  ctx_->scheduleNow([&] { task(); });
  // Signal fires at t=10ns, well before delay's t=1000ns expiration.
  ctx_->schedule(Time::fromNs(10), [&] { sig.signal(); });
  scheduler_->run();

  ASSERT_TRUE(winner.has_value());
  EXPECT_EQ(*winner, 1u);
  // Delay was cancelled — confirm SimCancellableDelay is in a clean
  // state (otherwise its destructor's XCHECK on hasInFlight() will fire).
  EXPECT_FALSE(delay.hasInFlight());
  EXPECT_FALSE(sig.hasWaiter());
}

// Delay vs signal where the delay fires first. Index 0 (delay) wins.
TEST_F(SimWaitFirstTest, TwoOperandsDelayWinsBeforeSignal) {
  SimCancellableDelay delay;
  SimSignal sig{ctx_.get()};
  std::optional<size_t> winner;

  auto task = [&]() -> SimTask {
    winner = co_await simWaitFirst(
        delay.awaitDelay(ctx_.get(), Time::fromNs(50)), sig.wait());
  };

  ctx_->scheduleNow([&] { task(); });
  // Signal scheduled at t=1000ns, well after delay's t=50ns expiration.
  ctx_->schedule(Time::fromNs(1000), [&] { sig.signal(); });
  scheduler_->run();

  ASSERT_TRUE(winner.has_value());
  EXPECT_EQ(*winner, 0u);
  EXPECT_FALSE(delay.hasInFlight());
  EXPECT_FALSE(sig.hasWaiter());
}

// ============================================================
// Variadic — three operands
// ============================================================

// 3 operands, middle one (signal) wins. Validates the parameter pack
// expansion handles >2 operands and the canceller array sizes correctly.
TEST_F(SimWaitFirstTest, ThreeOperandsMiddleWins) {
  SimCancellableDelay delayA;
  SimSignal sig{ctx_.get()};
  SimCancellableDelay delayB;
  std::optional<size_t> winner;

  auto task = [&]() -> SimTask {
    winner = co_await simWaitFirst(
        delayA.awaitDelay(ctx_.get(), Time::fromNs(1000)),
        sig.wait(),
        delayB.awaitDelay(ctx_.get(), Time::fromNs(1000)));
  };

  ctx_->scheduleNow([&] { task(); });
  ctx_->schedule(Time::fromNs(10), [&] { sig.signal(); });
  scheduler_->run();

  ASSERT_TRUE(winner.has_value());
  EXPECT_EQ(*winner, 1u);
  EXPECT_FALSE(delayA.hasInFlight());
  EXPECT_FALSE(delayB.hasInFlight());
  EXPECT_FALSE(sig.hasWaiter());
}

// ============================================================
// Tie-breaking — same-tick race resolution
// ============================================================

// When two delays expire at the same tick, the lower-index operand
// wins (operand-order tie-break, deterministic in single-threaded PDES).
TEST_F(SimWaitFirstTest, SameTickRaceLowerIndexWins) {
  SimCancellableDelay delayA;
  SimCancellableDelay delayB;
  std::optional<size_t> winner;

  auto task = [&]() -> SimTask {
    winner = co_await simWaitFirst(
        delayA.awaitDelay(ctx_.get(), Time::fromNs(100)),
        delayB.awaitDelay(ctx_.get(), Time::fromNs(100)));
  };

  ctx_->scheduleNow([&] { task(); });
  scheduler_->run();

  ASSERT_TRUE(winner.has_value());
  // PDES dispatches events in insertion order at the same tick;
  // delayA was inserted before delayB during armAll's sequential
  // expansion, so delayA's resume callback fires first.
  EXPECT_EQ(*winner, 0u);
  EXPECT_FALSE(delayA.hasInFlight());
  EXPECT_FALSE(delayB.hasInFlight());
}

// ============================================================
// Iterative reuse — same primitives, multiple SimWaitFirst rounds
// ============================================================

// Same delay+signal primitives reused across 3 races. Validates that
// SimWaitFirst leaves primitives in a clean state after each round so
// they can be re-armed.
TEST_F(SimWaitFirstTest, IterativeReuseAcrossRounds) {
  SimCancellableDelay delay;
  SimSignal sig{ctx_.get()};
  std::vector<size_t> winners;

  // Three rounds: signal wins, delay wins, signal wins.
  auto task = [&]() -> SimTask {
    // Round 1: signal at t=10, delay at 1000 — signal wins.
    winners.emplace_back(
        co_await simWaitFirst(
            delay.awaitDelay(ctx_.get(), Time::fromNs(1000)), sig.wait()));
    // Round 2: signal at t=2000, delay at 100 — delay wins.
    winners.emplace_back(
        co_await simWaitFirst(
            delay.awaitDelay(ctx_.get(), Time::fromNs(100)), sig.wait()));
    // Round 3: signal at t=3000, delay at 5000 — signal wins.
    winners.emplace_back(
        co_await simWaitFirst(
            delay.awaitDelay(ctx_.get(), Time::fromNs(5000)), sig.wait()));
  };

  ctx_->scheduleNow([&] { task(); });
  ctx_->schedule(Time::fromNs(10), [&] { sig.signal(); });
  ctx_->schedule(Time::fromNs(2000), [&] { sig.signal(); });
  ctx_->schedule(Time::fromNs(3000), [&] { sig.signal(); });
  scheduler_->run();

  // signal won round 1, delay won round 2, signal won round 3
  const std::vector<size_t> expected{1u, 0u, 1u};
  EXPECT_EQ(winners, expected);
  EXPECT_FALSE(delay.hasInFlight());
  EXPECT_FALSE(sig.hasWaiter());
}

// ============================================================
// Cancellation correctness — losers don't fire after race resolves
// ============================================================

// After a race resolves, the loser delay's scheduled resume must not
// fire later. Validate by tracking sim-time at which the parent task
// completes — should be the winner's resolution time, not the loser's.
TEST_F(SimWaitFirstTest, LoserDelayDoesNotFireAfterResolution) {
  SimCancellableDelay delayA;
  SimCancellableDelay delayB;
  std::optional<uint64_t> resolvedAtNs;
  int postResolutionRunCount = 0;

  auto task = [&]() -> SimTask {
    auto idx = co_await simWaitFirst(
        delayA.awaitDelay(ctx_.get(), Time::fromNs(50)),
        delayB.awaitDelay(ctx_.get(), Time::fromNs(5000)));
    resolvedAtNs = ctx_->nowNs();
    EXPECT_EQ(idx, 0u);
    // Counter (vs a bool) catches the double-resume case if a future
    // regression lets delayB's resume fire after the race resolved.
    ++postResolutionRunCount;
  };

  ctx_->scheduleNow([&] { task(); });
  scheduler_->run();

  ASSERT_TRUE(resolvedAtNs.has_value());
  EXPECT_EQ(postResolutionRunCount, 1);
  // Resolved at delayA's expiration (50ns), NOT delayB's.
  EXPECT_EQ(*resolvedAtNs, 50);
  // Final sim-time should also not have advanced to delayB's expiration —
  // confirms delayB's event was actually removed from PDES.
  EXPECT_LT(ctx_->nowNs(), 5000);
  EXPECT_FALSE(delayA.hasInFlight());
  EXPECT_FALSE(delayB.hasInFlight());
}

// ============================================================
// Pre-pending signal — signal arrives before SimWaitFirst is awaited
// ============================================================

// Signal already has a pending count when SimWaitFirst is co_awaited.
// The signal operand's await_ready should return true; signal wins
// immediately (no PDES round-trip).
TEST_F(SimWaitFirstTest, PrePendingSignalWinsImmediately) {
  SimCancellableDelay delay;
  SimSignal sig{ctx_.get()};
  std::optional<size_t> winner;
  std::optional<uint64_t> resolvedAtNs;

  auto task = [&]() -> SimTask {
    sig.signal(); // pre-arrive — pendingSignals_ = 1
    winner = co_await simWaitFirst(
        delay.awaitDelay(ctx_.get(), Time::fromNs(5000)), sig.wait());
    resolvedAtNs = ctx_->nowNs();
  };

  ctx_->scheduleNow([&] { task(); });
  scheduler_->run();

  ASSERT_TRUE(winner.has_value());
  EXPECT_EQ(*winner, 1u);
  // Signal wins; the child wraps the signal's wait() in a co_await.
  // SimSignal::Awaitable::await_ready returns true, so the child
  // doesn't actually suspend on the signal — it runs straight to
  // state.resolve(1). Total elapsed: 0 sim-time.
  ASSERT_TRUE(resolvedAtNs.has_value());
  EXPECT_EQ(*resolvedAtNs, 0);
  EXPECT_FALSE(delay.hasInFlight());
  EXPECT_FALSE(sig.hasWaiter());
  EXPECT_EQ(sig.pendingSignals(), 0u);
}

// Same scenario as above, but the pre-pending signal is at index 0 (NOT
// the last operand). Locks in the deferred-parent-resume fix: without
// that fix, the eager child at index 0 resolves synchronously inside
// armAll, parent resumes mid-loop, parent destructs *this, then the
// remaining armOne<I> calls write to freed memory (UAF). With the fix,
// resolve() detects armingInProgress and defers parent.resume() until
// after armAll returns.
TEST_F(SimWaitFirstTest, PrePendingSignalAtIndex0InThreeOperandRace) {
  SimCancellableDelay delay1;
  SimCancellableDelay delay2;
  SimSignal sig{ctx_.get()};
  std::optional<size_t> winner;

  auto task = [&]() -> SimTask {
    sig.signal();
    winner = co_await simWaitFirst(
        sig.wait(), // index 0 — non-last; pre-pending → await_ready=true
        delay1.awaitDelay(ctx_.get(), Time::fromNs(5000)),
        delay2.awaitDelay(ctx_.get(), Time::fromNs(10000)));
  };

  ctx_->scheduleNow([&] { task(); });
  scheduler_->run();

  ASSERT_TRUE(winner.has_value());
  EXPECT_EQ(*winner, 0u);
  // Both delays must have been cleaned up (loser-cancel) without UAF.
  EXPECT_FALSE(delay1.hasInFlight());
  EXPECT_FALSE(delay2.hasInFlight());
  EXPECT_FALSE(sig.hasWaiter());
  EXPECT_EQ(sig.pendingSignals(), 0u);
}

// ============================================================
// Mixed: signal wins twice in a row with delay re-armed each round
// ============================================================

// Stress test: 5 sequential rounds, all won by signal. Validates that
// repeated cancellation of delays leaves the SimCancellableDelay in a
// reusable state (no XCHECK fires, no leaks).
TEST_F(SimWaitFirstTest, RepeatedSignalWinsLeaveDelayReusable) {
  SimCancellableDelay delay;
  SimSignal sig{ctx_.get()};
  std::vector<size_t> winners;
  constexpr size_t kRounds = 5;

  auto task = [&]() -> SimTask {
    for (size_t i = 0; i < kRounds; ++i) {
      winners.emplace_back(
          co_await simWaitFirst(
              delay.awaitDelay(ctx_.get(), Time::fromNs(1'000'000)),
              sig.wait()));
    }
  };

  ctx_->scheduleNow([&] { task(); });
  for (size_t i = 0; i < kRounds; ++i) {
    // Schedule each signal slightly after the previous round resolves.
    ctx_->schedule(Time::fromNs((i + 1) * 100), [&] { sig.signal(); });
  }
  scheduler_->run();

  // Constructed-expected comparison gives full actual-vs-expected
  // diagnostics on failure (preferred over per-element loops).
  const std::vector<size_t> expected(kRounds, 1u);
  EXPECT_EQ(winners, expected);
  EXPECT_FALSE(delay.hasInFlight());
  EXPECT_FALSE(sig.hasWaiter());
}

// ============================================================
// collectAny — folly::coro-aligned alias for simWaitFirst.
// ============================================================

// `collectAny` is a thin alias for `simWaitFirst` with identical
// semantics. Type-identity (alias produces the same template
// instantiation for any arity) is enforced at compile time by the
// static_assert below. The runtime smoke tests catch the gross
// failure mode (alias broken / returns wrong index in a happy-path
// race); behavioral divergence in edge cases (cancellation ordering,
// exception propagation) would NOT be caught here — the
// simWaitFirst* test suite remains the source of truth for those.
static_assert(
    std::is_same_v<
        decltype(collectAny(
            std::declval<SimSignal::Awaitable>(),
            std::declval<SimSignal::Awaitable>())),
        decltype(simWaitFirst(
            std::declval<SimSignal::Awaitable>(),
            std::declval<SimSignal::Awaitable>()))>,
    "collectAny must produce the same awaitable type as simWaitFirst "
    "for the homogeneous 2-operand case.");
// 3-operand mixed-type case: covers the variadic-arity + heterogeneous-
// operand path that the canonical LLR usage takes (race a SimSignal
// against two delays).
static_assert(
    std::is_same_v<
        decltype(collectAny(
            std::declval<SimSignal::Awaitable>(),
            std::declval<SimCancellableDelay::Awaitable>(),
            std::declval<SimCancellableDelay::Awaitable>())),
        decltype(simWaitFirst(
            std::declval<SimSignal::Awaitable>(),
            std::declval<SimCancellableDelay::Awaitable>(),
            std::declval<SimCancellableDelay::Awaitable>()))>,
    "collectAny must produce the same awaitable type as simWaitFirst "
    "for the heterogeneous N=3 case.");

// collectAny — same race shape as `TwoOperandsSignalWinsBeforeDelay`
// at the top of the file: signal fires before delay → index 1 wins.
// Constants match the original to make alias-symmetry obvious.
TEST_F(SimWaitFirstTest, CollectAnySignalWinsBeforeDelay) {
  SimCancellableDelay delay;
  SimSignal sig{ctx_.get()};
  std::optional<size_t> winner;

  auto task = [&]() -> SimTask {
    winner = co_await collectAny(
        delay.awaitDelay(ctx_.get(), Time::fromNs(1000)), sig.wait());
  };

  ctx_->scheduleNow([&] { task(); });
  ctx_->schedule(Time::fromNs(10), [&] { sig.signal(); });
  scheduler_->run();

  ASSERT_TRUE(winner.has_value());
  EXPECT_EQ(*winner, 1u);
  EXPECT_FALSE(delay.hasInFlight());
  EXPECT_FALSE(sig.hasWaiter());
  EXPECT_EQ(ctx_->nowNs(), 10);
}

// collectAny with delay-wins-first. Constants match
// `TwoOperandsDelayWinsBeforeSignal` for alias-symmetry.
TEST_F(SimWaitFirstTest, CollectAnyDelayWinsBeforeSignal) {
  SimCancellableDelay delay;
  SimSignal sig{ctx_.get()};
  std::optional<size_t> winner;

  auto task = [&]() -> SimTask {
    winner = co_await collectAny(
        delay.awaitDelay(ctx_.get(), Time::fromNs(50)), sig.wait());
  };

  ctx_->scheduleNow([&] { task(); });
  // Schedule signal AFTER the delay resolves — delay should win.
  ctx_->schedule(Time::fromNs(1000), [&] { sig.signal(); });
  scheduler_->run();

  ASSERT_TRUE(winner.has_value());
  EXPECT_EQ(*winner, 0u);
  EXPECT_FALSE(delay.hasInFlight());
  EXPECT_FALSE(sig.hasWaiter());
}

// ============================================================
// Destroy-time teardown — frame destroyed while the race is armed
// ============================================================
//
// These exercise ~SimWaitFirstAwaitable's teardown path: a SimScopedTask is
// torn down (reset()) while still suspended in an UNRESOLVED simWaitFirst.
// Every operand must be fully cancelled so the operands' own destructors
// (which XCHECK no installed waiter / no scheduled resume) do not abort, and
// no scheduled PDES event survives to resume the destroyed frame.

// Two cancellable-delay operands: tearing down the armed race cancels both,
// so each delay is clean and no scheduled resume fires into the dead frame.
TEST_F(SimWaitFirstTest, TeardownWhileArmedCancelsDelayOperands) {
  SimCancellableDelay delayA;
  SimCancellableDelay delayB;

  // Name the coroutine lambda so its closure outlives the frame (an inline
  // temporary lambda's closure dies at the end of the statement).
  auto body = [&]() -> SimScopedTask {
    co_await simWaitFirst(
        delayA.awaitDelay(ctx_.get(), Time::fromNs(1000)),
        delayB.awaitDelay(ctx_.get(), Time::fromNs(2000)));
  };
  SimScopedTask task = body();

  ASSERT_TRUE(delayA.hasInFlight());
  ASSERT_TRUE(delayB.hasInFlight());

  task.reset(); // destroy the coroutine mid-race

  EXPECT_FALSE(delayA.hasInFlight());
  EXPECT_FALSE(delayB.hasInFlight());
  // No scheduled resume survives: running the (now-empty) queue must not
  // resume a destroyed frame and time must not jump to a delay deadline.
  scheduler_->run();
  EXPECT_EQ(ctx_->nowNs(), 0);
}

// SimStopwatch operand: teardown must FULLY cancel it (clear the scheduled
// resume), not just drop the waiter — otherwise ~SimStopwatch's XCHECK fires
// at scope exit. This is the case the SimStopwatch teardown override exists
// for.
TEST_F(SimWaitFirstTest, TeardownWhileArmedFullyCancelsStopwatchOperand) {
  SimStopwatch timer{ctx_.get(), Time::fromNs(1000)};
  timer.start();
  SimCancellableDelay delay;

  auto body = [&]() -> SimScopedTask {
    co_await simWaitFirst(
        timer.expiration(), delay.awaitDelay(ctx_.get(), Time::fromNs(2000)));
  };
  SimScopedTask task = body();

  ASSERT_TRUE(timer.running());
  ASSERT_TRUE(delay.hasInFlight());

  task.reset();

  // Stopwatch is back to a clean STOPPED state with no scheduled resume — the
  // decisive proof is that ~SimStopwatch (at scope exit) does not abort.
  EXPECT_FALSE(timer.running());
  EXPECT_FALSE(delay.hasInFlight());
}

// SimEvent operands: tearing down the armed race must leave every event with
// no installed waiter, else ~SimEvent aborts at scope exit. Three SimEvents
// (two PULSE + one LATCHED) exercise the teardown-cancel path over SimEvent
// operands generically. (`LinkLayerScheduler`'s own idle wait now races just
// `dataAvailable_` + `ctlosAvailable_`; shutdown is handled by destroying the
// txDriver frame, not by a `shutdownRequested_` operand.)
TEST_F(SimWaitFirstTest, TeardownWhileArmedCancelsEventOperands) {
  SimEvent pulseA{ctx_.get()};
  SimEvent pulseB{ctx_.get()};
  SimEvent latched{ctx_.get(), SimEventMode::LATCHED};

  auto body = [&]() -> SimScopedTask {
    co_await simWaitFirst(pulseA.wait(), pulseB.wait(), latched.wait());
  };
  SimScopedTask task = body();

  task.reset();

  // All three events have empty waiter lists — verified by ~SimEvent not
  // aborting as the events leave scope at the end of this test.
  SUCCEED();
}

} // namespace packsim::coro
