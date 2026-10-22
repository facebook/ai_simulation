/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimQueue.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

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
using packsim::coro::SimQueue;
using packsim::coro::SimTask;
using packsim::coro::tests::CoroTestFixture;

namespace packsim::coro {

using SimQueueTest = CoroTestFixture;

// TX-pump pattern: producer enqueues, consumer drains with per-item delay.
TEST_F(SimQueueTest, TxPumpDrain) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "txpump", 0, nullptr);

  SimQueue<int> q(&ctx);
  std::vector<int> received;

  // Pre-load queue.
  for (int i = 0; i < 4; ++i) {
    q.push(i * 10);
  }

  auto consumer = [&]() -> SimTask {
    while (!q.empty()) {
      const int item = co_await q.pop();
      received.emplace_back(item);
      co_await simDelay(&ctx, Time::fromNs(20));
    }
  };

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  const std::vector<int> expected = {0, 10, 20, 30};
  EXPECT_EQ(received, expected);
  // 4 items × 20ns serialization each = 80ns total.
  EXPECT_EQ(ctx.nowNs(), 80);
}

// Consumer waits when queue is empty; producer push wakes it.
TEST_F(SimQueueTest, ConsumerWaitsForProducer) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "wait", 0, nullptr);

  SimQueue<int> q(&ctx);
  int received = -1;
  std::optional<uint64_t> receiveTime;

  // Producer pushes at t=200.
  ctx.schedule(Time::fromNs(200), [&]() { q.push(99); });

  auto consumer = [&]() -> SimTask {
    received = co_await q.pop();
    receiveTime = ctx.nowNs();
  };

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  EXPECT_EQ(received, 99);
  ASSERT_TRUE(receiveTime.has_value());
  EXPECT_EQ(*receiveTime, 200);
}

// Move-only payload (unique_ptr).
TEST_F(SimQueueTest, MoveOnlyPayload) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "moveonly", 0, nullptr);

  SimQueue<std::unique_ptr<std::string>> q(&ctx);
  std::vector<std::string> received;

  q.push(std::make_unique<std::string>("alpha"));
  q.push(std::make_unique<std::string>("beta"));

  auto consumer = [&]() -> SimTask {
    while (!q.empty()) {
      const auto ptr = co_await q.pop();
      received.emplace_back(*ptr);
    }
  };

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  const std::vector<std::string> expected = {"alpha", "beta"};
  EXPECT_EQ(received, expected);
}

// Multiple pushes accumulate; drain consumes in FIFO order.
TEST_F(SimQueueTest, FifoOrder) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "fifo", 0, nullptr);

  SimQueue<int> q(&ctx);
  std::vector<int> received;

  for (int i = 0; i < 10; ++i) {
    q.push(i);
  }
  EXPECT_EQ(q.size(), 10u);

  auto consumer = [&]() -> SimTask {
    while (!q.empty()) {
      received.emplace_back(co_await q.pop());
    }
  };

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  std::vector<int> expected;
  expected.reserve(10);
  for (int i = 0; i < 10; ++i) {
    expected.emplace_back(i);
  }
  EXPECT_EQ(received, expected);
  EXPECT_TRUE(q.empty());
}

// Sliding-window pattern: producer pushes one at a time, consumer waits
// after each pop. Verifies that suspended consumer is resumed by push.
TEST_F(SimQueueTest, ProducerWakesSuspendedConsumer) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "sliding", 0, nullptr);

  SimQueue<int> q(&ctx);
  std::vector<std::pair<int, uint64_t>> events;

  auto consumer = [&]() -> SimTask {
    for (int i = 0; i < 3; ++i) {
      const int item = co_await q.pop();
      events.emplace_back(item, ctx.nowNs());
    }
  };

  // Push at staggered times.
  ctx.schedule(Time::fromNs(50), [&]() { q.push(1); });
  ctx.schedule(Time::fromNs(120), [&]() { q.push(2); });
  ctx.schedule(Time::fromNs(200), [&]() { q.push(3); });

  ctx.scheduleNow([&]() { consumer(); });
  scheduler.run();

  const std::vector<std::pair<int, uint64_t>> expected = {
      {1, 50}, {2, 120}, {3, 200}};
  EXPECT_EQ(events, expected);
}

// hasWaiter() reflects consumer suspension.
TEST_F(SimQueueTest, HasWaiterTracksConsumer) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "haswaiter", 0, nullptr);

  SimQueue<int> q(&ctx);
  EXPECT_FALSE(q.hasWaiter());

  auto consumer = [&]() -> SimTask { co_await q.pop(); };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() {
    EXPECT_TRUE(q.hasWaiter());
    q.push(42);
  });
  ctx.schedule(Time::fromNs(20), [&]() { EXPECT_FALSE(q.hasWaiter()); });

  scheduler.run();
}

// Default ResumeMode::DEFERRED: consumer resume is deferred through scheduleNow
// (sequenced after push() returns), bounding stack depth on long chains.
TEST_F(SimQueueTest, DefaultDirectResumeIsFalse) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "direct-default", 0, nullptr);

  SimQueue<int> q(&ctx);
  bool consumedSync = false;

  auto consumer = [&]() -> SimTask {
    co_await q.pop();
    consumedSync = true;
  };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() {
    q.push(7);
    EXPECT_FALSE(consumedSync);
  });
  ctx.schedule(Time::fromNs(20), [&]() { EXPECT_TRUE(consumedSync); });

  scheduler.run();
}

// Empty queue with no waiter is harmless.
TEST_F(SimQueueTest, EmptyQueueNoWaiterDestructible) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "empty", 0, nullptr);

  {
    SimQueue<int> q(&ctx);
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0u);
    EXPECT_FALSE(q.hasWaiter());
  }
  // Destruction without waiter is fine.
}

// Push then drain (no waiter) — queue accumulates items.
TEST_F(SimQueueTest, PushWithoutConsumerAccumulates) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "accum", 0, nullptr);

  SimQueue<int> q(&ctx);
  for (int i = 0; i < 5; ++i) {
    q.push(i);
  }

  EXPECT_EQ(q.size(), 5u);
  EXPECT_FALSE(q.empty());
  EXPECT_FALSE(q.hasWaiter());
}

// With ResumeMode::DEFERRED, cancel() must remove the in-flight deferred
// resume from PDES AND destroy the suspended consumer.
TEST_F(SimQueueTest, CancelDuringInFlightDeferredResume) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-in-flight", 0, nullptr);

  SimQueue<int> q(&ctx, ResumeMode::DEFERRED);
  bool consumerResumed = false;

  auto consumer = [&]() -> SimTask {
    (void)co_await q.pop();
    consumerResumed = true;
  };

  ctx.scheduleNow([&]() { consumer(); });

  ctx.schedule(Time::fromNs(10), [&]() {
    EXPECT_TRUE(q.hasWaiter());
    q.push(42);
    EXPECT_TRUE(q.hasWaiter())
        << "deferred push() keeps waiter_ set for cancel() to find";
    q.cancel();
    EXPECT_FALSE(q.hasWaiter());
  });

  scheduler.run();
  EXPECT_FALSE(consumerResumed);
}

// cancel() with no in-flight consumer is a safe no-op.
TEST_F(SimQueueTest, CancelWithoutWaiterIsNoOp) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-noop", 0, nullptr);

  SimQueue<int> q(&ctx, ResumeMode::DEFERRED);
  q.cancel();
  EXPECT_FALSE(q.hasWaiter());

  // Still valid to use after cancel().
  int received = -1;
  auto consumer = [&]() -> SimTask { received = co_await q.pop(); };
  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() { q.push(7); });
  scheduler.run();

  EXPECT_EQ(received, 7);
}

// cancel() is idempotent — safe to call multiple times in sequence.
TEST_F(SimQueueTest, CancelIsIdempotent) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-idem", 0, nullptr);

  SimQueue<int> q(&ctx, ResumeMode::DEFERRED);
  auto consumer = [&]() -> SimTask { (void)co_await q.pop(); };
  ctx.scheduleNow([&]() { consumer(); });

  ctx.schedule(Time::fromNs(10), [&]() {
    q.push(1);
    q.cancel();
    q.cancel(); // must not crash or double-destroy
    q.cancel();
    EXPECT_FALSE(q.hasWaiter());
  });

  scheduler.run();
}

// cancel() does NOT drop queued items — a later consumer can still pop
// them. cancel() only tears down the suspended consumer.
TEST_F(SimQueueTest, CancelPreservesQueuedItems) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-preserve", 0, nullptr);

  SimQueue<int> q(&ctx, ResumeMode::DEFERRED);
  std::vector<int> drained;

  // Both consumer lambdas must outlive any SimTask coroutine they create.
  // If declared inside a scheduled callback, the lambda dies before the
  // coroutine resumes, causing stack-use-after-return on `this` capture.
  auto consumer1 = [&]() -> SimTask { (void)co_await q.pop(); };
  auto consumer2 = [&]() -> SimTask {
    for (int i = 0; i < 3; ++i) {
      drained.emplace_back(co_await q.pop());
    }
  };

  // Consumer 1 suspends, then gets cancelled after items are pushed.
  ctx.scheduleNow([&]() { consumer1(); });

  ctx.schedule(Time::fromNs(10), [&]() {
    q.push(1);
    q.push(2);
    q.push(3);
    EXPECT_EQ(q.size(), 3u);
    q.cancel();
    EXPECT_EQ(q.size(), 3u) << "cancel() must not drop queued items";
  });

  // Consumer 2 starts later and drains all 3.
  ctx.schedule(Time::fromNs(20), [&]() { consumer2(); });

  scheduler.run();

  const std::vector<int> expected = {1, 2, 3};
  EXPECT_EQ(drained, expected);
}

// After a natural-fire deferred resume, cancel() must be a safe no-op.
TEST_F(SimQueueTest, CancelAfterNaturalFireIsNoOp) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "cancel-after-fire", 0, nullptr);

  SimQueue<int> q(&ctx, ResumeMode::DEFERRED);
  bool consumerResumed = false;

  auto consumer = [&]() -> SimTask {
    (void)co_await q.pop();
    consumerResumed = true;
  };
  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() { q.push(1); });
  // Cancel AFTER the scheduled lambda has had a chance to fire.
  ctx.schedule(Time::fromNs(20), [&]() {
    EXPECT_TRUE(consumerResumed) << "deferred resume should have fired by now";
    q.cancel(); // must be safe: scheduledResume_ already cleared
  });

  scheduler.run();
  EXPECT_TRUE(consumerResumed);
}

// Pure teardown path: consumer suspends on pop(), producer never pushes,
// owner calls cancel() to destroy the primitive. This is the scenario the
// "Teardown:" doc paragraph in SimQueue.h is written for.
TEST_F(SimQueueTest, CancelWithSuspendedConsumerNoPushDestroysFrame) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "teardown-no-push", 0, nullptr);

  SimQueue<int> q(&ctx, ResumeMode::DEFERRED);
  bool consumerResumed = false;

  auto consumer = [&]() -> SimTask {
    (void)co_await q.pop();
    consumerResumed = true;
  };

  ctx.scheduleNow([&]() { consumer(); });
  // No push() ever fires. Owner just tears down mid-flight.
  ctx.schedule(Time::fromNs(10), [&]() {
    ASSERT_TRUE(q.hasWaiter());
    q.cancel();
    EXPECT_FALSE(q.hasWaiter());
  });

  scheduler.run();
  EXPECT_FALSE(consumerResumed);
}

// Sanity check that ResumeMode::DIRECT does not overflow the stack for a
// moderate chain depth (1000) under the CI default stack size. "Bounded"
// here means the caller has statically determined the chain depth — this
// test only asserts that 1000 sequential push+pop pairs in one coroutine
// complete without stack overflow or ASan failure. The limit is not
// 1000; this is the chosen sample point.
TEST_F(SimQueueTest, DirectResume1000PushPopCompletesWithoutStackOverflow) {
  Scheduler scheduler;
  SimContext ctx(&scheduler, "direct-bounded", 0, nullptr);

  SimQueue<int> q(&ctx, ResumeMode::DIRECT);
  constexpr int kN = 1000;
  int sum = 0;

  auto consumer = [&]() -> SimTask {
    for (int i = 0; i < kN; ++i) {
      sum += co_await q.pop();
    }
  };

  ctx.scheduleNow([&]() { consumer(); });
  ctx.schedule(Time::fromNs(10), [&]() {
    for (int i = 0; i < kN; ++i) {
      q.push(i + 1);
    }
  });

  scheduler.run();

  EXPECT_EQ(sum, kN * (kN + 1) / 2);
}

} // namespace packsim::coro
