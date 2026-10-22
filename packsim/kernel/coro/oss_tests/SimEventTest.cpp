/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimEvent.h"

#include <memory>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/kernel/coro/SimCancellableDelay.h"
#include "packsim/kernel/coro/SimScopedTask.h"
#include "packsim/kernel/coro/SimSignal.h"
#include "packsim/kernel/coro/SimTask.h"
#include "packsim/kernel/coro/SimWaitFirst.h"
#include "packsim/kernel/coro/oss_tests/fixtures/CoroTestFixture.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

using packsim::Scheduler;
using packsim::SimContext;
using packsim::Time;
using packsim::coro::SimCancellableDelay;
using packsim::coro::SimEvent;
using packsim::coro::SimEventMode;
using packsim::coro::SimScopedTask;
using packsim::coro::SimSignal;
using packsim::coro::SimTask;
using packsim::coro::simWaitFirst;
using packsim::coro::tests::CoroTestFixture;

namespace packsim::coro {

// Shared fixture: every test in the regular suite uses the same Scheduler +
// SimContext shape, so build them once in SetUp() instead of repeating two
// lines per test. Mirrors SimStopwatchFixture and the other sibling test
// fixtures.
class SimEventFixture : public CoroTestFixture {
 protected:
  void SetUp() override {
    CoroTestFixture::SetUp();
    scheduler_ = std::make_unique<Scheduler>();
    ctx_ = std::make_unique<SimContext>(
        scheduler_.get(), "event", /*seed=*/0, /*conditionalMap=*/nullptr);
  }

  std::unique_ptr<Scheduler> scheduler_;
  std::unique_ptr<SimContext> ctx_;
};

using SimEventTest = SimEventFixture;

// Pulse mode: broadcast wakes a single waiter.
TEST_F(SimEventTest, PulseSingleWaiterWakes) {
  SimEvent ev(ctx_.get());
  bool resumed = false;

  auto task = [&]() -> SimTask {
    co_await ev.wait();
    resumed = true;
  };

  ctx_->scheduleNow([&]() { task(); });
  ctx_->schedule(Time::fromNs(50), [&]() { ev.broadcast(); });
  scheduler_->run();

  EXPECT_TRUE(resumed);
  EXPECT_EQ(ctx_->nowNs(), 50);
}

// Pulse mode: broadcast wakes ALL current waiters in one call.
TEST_F(SimEventTest, PulseMultipleWaitersAllWake) {
  SimEvent ev(ctx_.get());
  constexpr int kNumWaiters = 5;
  int resumeCount = 0;

  auto waiter = [&]() -> SimTask {
    co_await ev.wait();
    ++resumeCount;
  };

  for (int i = 0; i < kNumWaiters; ++i) {
    ctx_->scheduleNow([&]() { waiter(); });
  }
  ctx_->schedule(Time::fromNs(50), [&]() { ev.broadcast(); });
  scheduler_->run();

  EXPECT_EQ(resumeCount, kNumWaiters);
}

// Pulse mode: latecomers (wait() called after broadcast) suspend; second
// broadcast is needed to wake them.
TEST_F(SimEventTest, PulseLatecomerNeedsAnotherBroadcast) {
  SimEvent ev(ctx_.get());
  std::vector<uint64_t> wakeNs;

  // First waiter installs at t=0, wakes on broadcast at t=50.
  auto firstWaiter = [&]() -> SimTask {
    co_await ev.wait();
    wakeNs.emplace_back(ctx_->nowNs());
  };
  // Second waiter installs at t=100 (AFTER the first broadcast); only wakes
  // on the second broadcast at t=150.
  auto lateWaiter = [&]() -> SimTask {
    co_await ev.wait();
    wakeNs.emplace_back(ctx_->nowNs());
  };

  ctx_->scheduleNow([&]() { firstWaiter(); });
  ctx_->schedule(Time::fromNs(50), [&]() { ev.broadcast(); });
  ctx_->schedule(Time::fromNs(100), [&]() { lateWaiter(); });
  ctx_->schedule(Time::fromNs(150), [&]() { ev.broadcast(); });
  scheduler_->run();

  const std::vector<uint64_t> expected = {50, 150};
  EXPECT_EQ(wakeNs, expected);
}

// Pulse mode: an empty broadcast (no waiters) is a no-op AND leaves no
// observable state change — `isLatched()` stays false (Pulse mode never
// latches), and the scheduler has nothing to dispatch (run() returns
// immediately at sim-time 0).
TEST_F(SimEventTest, PulseEmptyBroadcastIsNoOp) {
  SimEvent ev(ctx_.get());
  EXPECT_FALSE(ev.isLatched());
  ev.broadcast(); // nothing to wake — no-op
  EXPECT_FALSE(ev.isLatched());
  scheduler_->run();
  EXPECT_FALSE(ev.isLatched());
  EXPECT_EQ(ctx_->nowNs(), 0);
}

// Latched mode: broadcast sets the bit; subsequent wait() resumes
// immediately without suspending.
TEST_F(SimEventTest, LatchedSubsequentWaitResumesImmediately) {
  SimEvent ev(ctx_.get(), SimEventMode::LATCHED);
  ev.broadcast(); // set bit (no waiters yet, so just sets the bit)

  bool resumed = false;
  auto task = [&]() -> SimTask {
    co_await ev.wait(); // bit is set → await_ready=true → no suspend
    resumed = true;
  };

  ctx_->scheduleNow([&]() { task(); });
  scheduler_->run();

  EXPECT_TRUE(resumed);
  EXPECT_TRUE(ev.isLatched());
}

// Latched mode: bit persists across multiple wait() calls until reset.
TEST_F(SimEventTest, LatchedBitPersistsUntilReset) {
  SimEvent ev(ctx_.get(), SimEventMode::LATCHED);
  ev.broadcast();
  EXPECT_TRUE(ev.isLatched());

  int resumeCount = 0;
  auto task = [&]() -> SimTask {
    for (int i = 0; i < 3; ++i) {
      co_await ev.wait();
      ++resumeCount;
    }
  };

  ctx_->scheduleNow([&]() { task(); });
  scheduler_->run();

  EXPECT_EQ(resumeCount, 3); // each wait resumed immediately
  EXPECT_TRUE(ev.isLatched());

  // After reset, a fresh wait would suspend.
  ev.reset();
  EXPECT_FALSE(ev.isLatched());
}

// Latched mode: broadcast also wakes any currently-suspended waiter (i.e.,
// the bit transition wakes pre-existing waiters too, not just new ones).
TEST_F(SimEventTest, LatchedBroadcastWakesSuspendedWaiter) {
  SimEvent ev(ctx_.get(), SimEventMode::LATCHED);
  bool resumed = false;
  std::optional<uint64_t> wakeNs;

  auto task = [&]() -> SimTask {
    co_await ev.wait(); // suspends — bit is not yet set
    resumed = true;
    wakeNs = ctx_->nowNs();
  };

  ctx_->scheduleNow([&]() { task(); });
  ctx_->schedule(Time::fromNs(50), [&]() { ev.broadcast(); });
  scheduler_->run();

  EXPECT_TRUE(resumed);
  ASSERT_TRUE(wakeNs.has_value());
  EXPECT_EQ(*wakeNs, 50);
  EXPECT_TRUE(ev.isLatched());
}

// Latched mode: a single broadcast() call has TWO observable effects on the
// same SimEvent — (1) it wakes the currently-suspended waiter AND (2) sets
// the sticky bit so a subsequent wait() returns immediately. This combined
// test verifies both effects in one flow (the existing sibling tests cover
// each effect individually, but not the conjunction).
TEST_F(SimEventTest, LatchedBroadcastWakesAndSetsBit) {
  SimEvent ev(ctx_.get(), SimEventMode::LATCHED);
  std::optional<uint64_t> firstWakeNs;
  std::optional<uint64_t> secondWakeNs;

  auto firstWaiter = [&]() -> SimTask {
    co_await ev.wait(); // suspends — bit not set
    firstWakeNs = ctx_->nowNs();
  };
  // Installs AFTER the broadcast — observes the bit set, returns
  // immediately without suspending.
  auto secondWaiter = [&]() -> SimTask {
    co_await ev.wait();
    secondWakeNs = ctx_->nowNs();
  };

  ctx_->scheduleNow([&]() { firstWaiter(); });
  ctx_->schedule(Time::fromNs(50), [&]() { ev.broadcast(); });
  ctx_->schedule(Time::fromNs(100), [&]() { secondWaiter(); });
  scheduler_->run();

  // Effect 1: pre-existing waiter woke at t=50 (broadcast time).
  ASSERT_TRUE(firstWakeNs.has_value());
  EXPECT_EQ(*firstWakeNs, 50);
  // Effect 2: late waiter installed at t=100 saw the bit set, resumed
  // immediately (same tick).
  ASSERT_TRUE(secondWakeNs.has_value());
  EXPECT_EQ(*secondWakeNs, 100);
  EXPECT_TRUE(ev.isLatched());
}

// SimWaitFirst integration: SimEvent wins (broadcast fires before timeout).
TEST_F(SimEventTest, SimWaitFirstEventWins) {
  SimEvent ev(ctx_.get());
  SimCancellableDelay timeout;
  std::optional<std::size_t> winner;

  auto task = [&]() -> SimTask {
    winner = co_await simWaitFirst(
        ev.wait(), timeout.awaitDelay(ctx_.get(), Time::fromNs(100)));
  };

  ctx_->scheduleNow([&]() { task(); });
  ctx_->schedule(Time::fromNs(50), [&]() { ev.broadcast(); });
  scheduler_->run();

  ASSERT_TRUE(winner.has_value());
  EXPECT_EQ(*winner, 0u); // ev.wait() won
  EXPECT_EQ(ctx_->nowNs(), 50);
}

// SimWaitFirst integration: SimEvent loses; per-waiter loser-cancel removes
// only this waiter from the list, leaving siblings unaffected.
TEST_F(SimEventTest, SimWaitFirstLoserCancelLeavesSiblingsAlone) {
  SimEvent ev(ctx_.get());
  bool sibling1Resumed = false;
  bool sibling2Resumed = false;
  std::optional<std::size_t> loserWinner;

  // Sibling waiters that should NOT be cancelled when the loser-cancel
  // fires on a different waiter.
  auto sibling1 = [&]() -> SimTask {
    co_await ev.wait();
    sibling1Resumed = true;
  };
  auto sibling2 = [&]() -> SimTask {
    co_await ev.wait();
    sibling2Resumed = true;
  };

  // Loser: races ev.wait() against a short timeout. Timeout wins; the
  // loser's ev.wait() waiter is per-waiter cancelled.
  SimCancellableDelay loserTimeout;
  auto loser = [&]() -> SimTask {
    loserWinner = co_await simWaitFirst(
        ev.wait(), loserTimeout.awaitDelay(ctx_.get(), Time::fromNs(25)));
  };

  ctx_->scheduleNow([&]() { sibling1(); });
  ctx_->scheduleNow([&]() { sibling2(); });
  ctx_->scheduleNow([&]() { loser(); });
  // Broadcast happens AFTER the loser's timeout fires. Siblings should wake.
  ctx_->schedule(Time::fromNs(100), [&]() { ev.broadcast(); });
  scheduler_->run();

  ASSERT_TRUE(loserWinner.has_value());
  EXPECT_EQ(*loserWinner, 1u); // timeout won
  EXPECT_TRUE(sibling1Resumed);
  EXPECT_TRUE(sibling2Resumed);
}

// SimWaitFirst integration: many waiters all racing the same event; one
// fires the broadcast, others just wait.
TEST_F(SimEventTest, SimWaitFirstManyWaitersOneBroadcastWakesAll) {
  SimEvent ev(ctx_.get());
  constexpr size_t kNumWaiters = 8;
  size_t resumeCount = 0;
  std::vector<SimCancellableDelay> timeouts(kNumWaiters);

  auto waiter = [&](size_t i) -> SimTask {
    [[maybe_unused]] auto winner = co_await simWaitFirst(
        ev.wait(), timeouts[i].awaitDelay(ctx_.get(), Time::fromNs(1000)));
    ++resumeCount;
  };

  for (size_t i = 0; i < kNumWaiters; ++i) {
    ctx_->scheduleNow([&, i]() { waiter(i); });
  }
  ctx_->schedule(Time::fromNs(50), [&]() { ev.broadcast(); });
  scheduler_->run();

  EXPECT_EQ(resumeCount, kNumWaiters);
  EXPECT_EQ(ctx_->nowNs(), 50);
}

// cancelWaiter() unlink branch coverage: head waiter loses while siblings
// remain. Exercises the `head_ == aw` branch of cancelWaiter()'s unlink.
// Setup: install three waiters in order (waiter1, waiter2, waiter3 — pushed
// to list head, so list-order is waiter3 -> waiter2 -> waiter1). Cancel
// waiter3 (the head) via SimWaitFirst loser-cancel; verify waiter1 and
// waiter2 still wake on broadcast.
TEST_F(SimEventTest, CancelWaiterUnlinksHead) {
  SimEvent ev(ctx_.get());
  bool waiter1Resumed = false;
  bool waiter2Resumed = false;
  std::optional<std::size_t> waiter3Winner;
  SimCancellableDelay timeout;

  auto waiter1 = [&]() -> SimTask {
    co_await ev.wait();
    waiter1Resumed = true;
  };
  auto waiter2 = [&]() -> SimTask {
    co_await ev.wait();
    waiter2Resumed = true;
  };
  // waiter3 races ev.wait() against an early timeout — timeout wins, so
  // waiter3's installed Awaitable (currently at list head) is cancelled.
  auto waiter3 = [&]() -> SimTask {
    waiter3Winner = co_await simWaitFirst(
        ev.wait(), timeout.awaitDelay(ctx_.get(), Time::fromNs(25)));
  };

  ctx_->scheduleNow([&]() { waiter1(); });
  ctx_->scheduleNow([&]() { waiter2(); });
  ctx_->scheduleNow([&]() { waiter3(); }); // head
  ctx_->schedule(Time::fromNs(100), [&]() { ev.broadcast(); });
  scheduler_->run();

  ASSERT_TRUE(waiter3Winner.has_value());
  EXPECT_EQ(*waiter3Winner, 1u); // timeout won; head was unlinked
  EXPECT_TRUE(waiter1Resumed);
  EXPECT_TRUE(waiter2Resumed);
}

// cancelWaiter() unlink branch coverage: middle waiter loses with both
// neighbors present. Exercises the `prev != nullptr && next != nullptr`
// branch of cancelWaiter()'s unlink.
// Setup: install three waiters; cancel the middle one via SimWaitFirst.
TEST_F(SimEventTest, CancelWaiterUnlinksMiddle) {
  SimEvent ev(ctx_.get());
  bool waiter1Resumed = false;
  bool waiter3Resumed = false;
  std::optional<std::size_t> waiter2Winner;
  SimCancellableDelay timeout;

  auto waiter1 = [&]() -> SimTask {
    co_await ev.wait();
    waiter1Resumed = true;
  };
  auto waiter2 = [&]() -> SimTask {
    waiter2Winner = co_await simWaitFirst(
        ev.wait(), timeout.awaitDelay(ctx_.get(), Time::fromNs(25)));
  };
  auto waiter3 = [&]() -> SimTask {
    co_await ev.wait();
    waiter3Resumed = true;
  };

  // Order: waiter1 first (becomes tail), waiter2 second (middle), waiter3
  // third (becomes head).
  ctx_->scheduleNow([&]() { waiter1(); });
  ctx_->scheduleNow([&]() { waiter2(); });
  ctx_->scheduleNow([&]() { waiter3(); });
  ctx_->schedule(Time::fromNs(100), [&]() { ev.broadcast(); });
  scheduler_->run();

  ASSERT_TRUE(waiter2Winner.has_value());
  EXPECT_EQ(*waiter2Winner, 1u); // timeout won; middle was unlinked
  EXPECT_TRUE(waiter1Resumed);
  EXPECT_TRUE(waiter3Resumed);
}

// cancelWaiter() unlink branch coverage: tail waiter loses with only a
// prev neighbor. Exercises the `prev != nullptr && next == nullptr`
// branch of cancelWaiter()'s unlink.
TEST_F(SimEventTest, CancelWaiterUnlinksTail) {
  SimEvent ev(ctx_.get());
  bool waiter2Resumed = false;
  bool waiter3Resumed = false;
  std::optional<std::size_t> waiter1Winner;
  SimCancellableDelay timeout;

  // waiter1 installs first → becomes tail of the LIFO list.
  auto waiter1 = [&]() -> SimTask {
    waiter1Winner = co_await simWaitFirst(
        ev.wait(), timeout.awaitDelay(ctx_.get(), Time::fromNs(25)));
  };
  auto waiter2 = [&]() -> SimTask {
    co_await ev.wait();
    waiter2Resumed = true;
  };
  auto waiter3 = [&]() -> SimTask {
    co_await ev.wait();
    waiter3Resumed = true;
  };

  ctx_->scheduleNow([&]() { waiter1(); }); // tail
  ctx_->scheduleNow([&]() { waiter2(); });
  ctx_->scheduleNow([&]() { waiter3(); }); // head
  ctx_->schedule(Time::fromNs(100), [&]() { ev.broadcast(); });
  scheduler_->run();

  ASSERT_TRUE(waiter1Winner.has_value());
  EXPECT_EQ(*waiter1Winner, 1u); // timeout won; tail was unlinked
  EXPECT_TRUE(waiter2Resumed);
  EXPECT_TRUE(waiter3Resumed);
}

// cancelWaiter() unlink branch coverage: single-element list (head_ == aw,
// next == nullptr, prev == nullptr). After cancellation, head_ must become
// nullptr and a subsequent broadcast() must be a no-op.
TEST_F(SimEventTest, CancelWaiterUnlinksSingleElement) {
  SimEvent ev(ctx_.get());
  std::optional<std::size_t> waiterWinner;
  SimCancellableDelay timeout;

  auto loneWaiter = [&]() -> SimTask {
    waiterWinner = co_await simWaitFirst(
        ev.wait(), timeout.awaitDelay(ctx_.get(), Time::fromNs(25)));
  };

  ctx_->scheduleNow([&]() { loneWaiter(); });
  // Broadcast AFTER the timeout fires — list should already be empty, so
  // broadcast is a clean no-op (no leftover waiter to wake or to leak).
  ctx_->schedule(Time::fromNs(100), [&]() { ev.broadcast(); });
  scheduler_->run();

  ASSERT_TRUE(waiterWinner.has_value());
  EXPECT_EQ(*waiterWinner, 1u); // timeout won; sole waiter was unlinked
  // No XCHECK fires at ev's destruction → list is empty.
}

// cancelWaiter() during the "broadcast fired, deferred resume queued but
// not yet dispatched" window: broadcast() detaches the waiter from head_
// and queues a deferred resume lambda holding `aw` raw; before the
// scheduler dispatches that lambda, cancelWaiter() must cancel the
// queued event handle — otherwise the dispatched lambda would dereference
// `aw->handle` and `aw->scheduledResume` after the coroutine frame has
// been destroyed.
//
// Triggers the race by issuing `broadcast()` then `signal()` from the
// SAME scheduled callback at t=50 (insertion order: broadcast first,
// signal second). Inside the callback:
//   1. broadcast() detaches event's awaiter, queues deferred resume at t=50
//   2. signal() resolves the SimWaitFirst race synchronously (signal wins
//      by insertion order); loser-cancel calls cancelWaiter on event's
//      detached Awaitable.
// cancelWaiter must cancel() the queued event handle so the post-callback
// scheduler dispatch does not fire on the destroyed Awaitable. An
// AddressSanitizer build would catch a missed cancel() as
// stack-use-after-scope.
TEST_F(SimEventTest, CancelWaiterCancelsQueuedDeferredResume) {
  SimEvent ev(ctx_.get());
  SimSignal sig{ctx_.get()};
  std::optional<std::size_t> winner;

  auto waiter = [&]() -> SimTask {
    winner = co_await simWaitFirst(ev.wait(), sig.wait());
  };

  ctx_->scheduleNow([&]() { waiter(); });
  ctx_->schedule(Time::fromNs(50), [&]() {
    ev.broadcast(); // detach + queue deferred resume at t=50
    sig.signal(); // synchronously resolves the race; signal wins
  });
  scheduler_->run();

  ASSERT_TRUE(winner.has_value());
  EXPECT_EQ(*winner, 1u);
  EXPECT_FALSE(sig.hasWaiter());
  // ev's destruction passes the XCHECK (head_ == nullptr); the cancelled
  // deferred resume did NOT fire on the destroyed frame.
}

// Spec scenario: enterFlush in Latched mode wakes timer-loop coroutines
// that are between awaits. The first broadcast occurs while the loop is
// processing some other event; the loop's next wait() observes isLatched()
// and exits.
TEST_F(SimEventTest, SpecEnterFlushLatchedSemantics) {
  SimEvent enterFlush(ctx_.get(), SimEventMode::LATCHED);
  SimEvent otherWork(ctx_.get()); // some other event the loop also waits on

  bool exited = false;

  // Loop: wakes on otherWork or enterFlush; after each wake re-checks
  // enterFlush.isLatched() and exits if set.
  auto loop = [&]() -> SimTask {
    while (true) {
      co_await simWaitFirst(otherWork.wait(), enterFlush.wait());
      if (enterFlush.isLatched()) {
        exited = true;
        co_return;
      }
      // Otherwise process otherWork and loop.
    }
  };

  ctx_->scheduleNow([&]() { loop(); });
  // At t=10, broadcast otherWork — loop wakes, sees no enterFlush, loops.
  ctx_->schedule(Time::fromNs(10), [&]() { otherWork.broadcast(); });
  // At t=20, broadcast enterFlush — loop wakes, sees enterFlush, exits.
  ctx_->schedule(Time::fromNs(20), [&]() { enterFlush.broadcast(); });
  scheduler_->run();

  EXPECT_TRUE(exited);
}

// Latched: reset() clears the bit but does NOT wake or affect installed
// waiters (they stay suspended until next broadcast).
TEST_F(SimEventTest, LatchedResetDoesNotWakeWaiter) {
  SimEvent ev(ctx_.get(), SimEventMode::LATCHED);
  bool resumed = false;
  std::optional<std::size_t> winner;
  SimCancellableDelay timeout;

  auto task = [&]() -> SimTask {
    winner = co_await simWaitFirst(
        ev.wait(), timeout.awaitDelay(ctx_.get(), Time::fromNs(100)));
    resumed = true;
  };

  ctx_->scheduleNow([&]() { task(); });
  // At t=20, set the bit.
  ctx_->schedule(Time::fromNs(20), [&]() { ev.broadcast(); });
  // At t=30 (after the broadcast already woke task), reset the bit.
  ctx_->schedule(Time::fromNs(30), [&]() { ev.reset(); });
  scheduler_->run();

  EXPECT_TRUE(resumed);
  ASSERT_TRUE(winner.has_value());
  // The task woke at t=20 via the broadcast — NOT at the timeout (100).
  EXPECT_EQ(*winner, 0u);
  EXPECT_FALSE(ev.isLatched()); // reset cleared the bit
}

// broadcastInline: resumes waiters synchronously on the calling stack.
// After broadcastInline returns, every waiter has run through to its
// next suspension or co_return — no scheduler.run() needed. Also
// verifies the latched bit is set (Latched mode).
TEST_F(SimEventTest, LatchedBroadcastInlineResumesSynchronouslyAndSetsBit) {
  SimEvent ev(ctx_.get(), SimEventMode::LATCHED);
  int resumeCount = 0;

  auto waiter = [&]() -> SimTask {
    co_await ev.wait();
    ++resumeCount;
  };

  ctx_->scheduleNow([&]() { waiter(); });
  ctx_->scheduleNow([&]() { waiter(); });
  ctx_->scheduleNow([&]() { waiter(); });
  scheduler_->run(Time::fromNs(1)); // get all waiters installed

  EXPECT_EQ(resumeCount, 0);
  ev.broadcastInline();
  // All three waiters resumed synchronously — no scheduler.run() between
  // broadcastInline() and the assertion.
  EXPECT_EQ(resumeCount, 3);
  EXPECT_TRUE(ev.isLatched());
}

// Pulse-mode broadcastInline must NOT set a sticky bit — Pulse mode has
// no bit. Verifies the mode_ == LATCHED branch is correctly skipped, so
// a future refactor of the mode dispatch can't silently regress.
TEST_F(SimEventTest, PulseBroadcastInlineResumesSynchronouslyAndDoesNotLatch) {
  SimEvent ev(ctx_.get(), SimEventMode::PULSE);
  int resumeCount = 0;

  auto waiter = [&]() -> SimTask {
    co_await ev.wait();
    ++resumeCount;
  };

  ctx_->scheduleNow([&]() { waiter(); });
  ctx_->scheduleNow([&]() { waiter(); });
  scheduler_->run(Time::fromNs(1));

  EXPECT_EQ(resumeCount, 0);
  ev.broadcastInline();
  EXPECT_EQ(resumeCount, 2);
  EXPECT_FALSE(ev.isLatched()); // Pulse mode never latches.
}

// broadcastInline allows clean destruction immediately after — no need
// to drain via scheduler. (broadcast() with deferred resumes would leak.)
TEST_F(SimEventTest, LatchedBroadcastInlineAllowsImmediateDestruction) {
  bool ran = false;
  {
    SimEvent ev(ctx_.get(), SimEventMode::LATCHED);
    auto waiter = [&]() -> SimTask {
      co_await ev.wait();
      ran = true;
    };
    ctx_->scheduleNow([&]() { waiter(); });
    scheduler_->run(Time::fromNs(1)); // install waiter
    ev.broadcastInline(); // resume synchronously
    // ev destructs here without scheduler.run() — should not XCHECK.
  }
  EXPECT_TRUE(ran);
}

// Re-entrancy: a waiter whose body fires broadcastInline() on the same
// event during its own resume sees an empty waiter list (head_ was
// nulled BEFORE the resume loop) and the nested call is a no-op. The
// outer broadcast continues iterating its snapshot.
TEST_F(SimEventTest, LatchedBroadcastInlineReEntrantIsSafe) {
  SimEvent ev(ctx_.get(), SimEventMode::LATCHED);
  int resumeCount = 0;
  auto waiter = [&]() -> SimTask {
    co_await ev.wait();
    ++resumeCount;
    ev.broadcastInline(); // re-entrant; head is empty, should no-op.
  };
  ctx_->scheduleNow([&]() { waiter(); });
  ctx_->scheduleNow([&]() { waiter(); });
  scheduler_->run(Time::fromNs(1));
  ev.broadcastInline();
  EXPECT_EQ(resumeCount, 2); // both waiters resumed exactly once.
  EXPECT_TRUE(ev.isLatched()); // bit set.
}

// SimEvent destructor catches the "destroyed with installed waiters" bug
// (we'd leak coroutine frames). Verified by ensuring a clean shutdown via
// broadcast doesn't trip the XCHECK.
TEST_F(SimEventTest, CleanShutdownByBroadcastBeforeDestruction) {
  bool ran = false;
  {
    SimEvent ev(ctx_.get());
    auto task = [&]() -> SimTask {
      co_await ev.wait();
      ran = true;
    };
    ctx_->scheduleNow([&]() { task(); });
    // Broadcast before destruction so the waiter resumes and exits cleanly.
    ctx_->schedule(Time::fromNs(10), [&]() { ev.broadcast(); });
    scheduler_->run();
  }
  EXPECT_TRUE(ran);
}

// Destroy-safe bare await: destroying a frame parked on a bare
// `co_await event.wait()` (inside a SimScopedTask, e.g. via reset()) must
// unlink the intrusive waiter via `~SimEvent::Awaitable`. SimEvent exposes no
// public waiter-count accessor (and we don't add one just for a test), so the
// unlink is asserted behaviorally: a `broadcast()` after the reset walks the
// waiter list — if the destroyed Awaitable were still linked, broadcast would
// dereference the freed node (a use-after-free caught by ASan) and try to
// resume the dead frame. A correctly-unlinked waiter leaves an empty list, so
// the body never resumes and `~SimEvent` (scope exit) sees `head_ == nullptr`.
TEST_F(SimEventTest, BareAwaitDestroyedWhileSuspendedUnlinks) {
  SimEvent pulse{ctx_.get()};
  bool resumed = false;

  auto body = [&]() -> SimScopedTask {
    co_await pulse.wait();
    resumed = true; // only reached if the waiter is resumed
  };
  SimScopedTask task = body();
  ASSERT_FALSE(resumed) << "must be parked on the await before reset";

  task.reset(); // destroy the frame mid-wait → ~SimEvent::Awaitable unlinks

  // Broadcast walks the (now-empty) list: no UAF on a dangling node, and the
  // destroyed frame is not resumed.
  pulse.broadcast();
  scheduler_->run();
  EXPECT_FALSE(resumed)
      << "a destroyed frame must not be resumed by a later broadcast";
}

} // namespace packsim::coro
