/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimScopedTask.h"

#include <memory>
#include <utility>

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
using packsim::coro::SimScopedTask;
using packsim::coro::tests::CoroTestFixture;

namespace packsim::coro {

// Single-thread fixture (same shape as SimWaitFirstFixture): scheduler + one
// participant context constructed after CoroTestFixture pins pdes_threads=1.
class SimScopedTaskFixture : public CoroTestFixture {
 protected:
  void SetUp() override {
    CoroTestFixture::SetUp();
    scheduler_ = std::make_unique<Scheduler>();
    ctx_ = std::make_unique<SimContext>(scheduler_.get(), "ctx");
  }

  std::unique_ptr<Scheduler> scheduler_;
  std::unique_ptr<SimContext> ctx_;
};
using SimScopedTaskTest = SimScopedTaskFixture;

// A body with no co_await runs to completion on eager start and parks at
// final_suspend (frame retained, done()==true) without any scheduler run.
TEST_F(SimScopedTaskTest, SyncBodyCompletesEagerlyAndIsDone) {
  bool ran = false;
  auto make = [&]() -> SimScopedTask {
    ran = true;
    co_return;
  };

  SimScopedTask task = make();

  EXPECT_TRUE(ran);
  EXPECT_TRUE(task.valid());
  EXPECT_TRUE(task.done());
  // Destructor destroys the completed frame — no crash, no leak.
}

// An async body suspends on a delay (done()==false), then completes when the
// scheduler dispatches the resume.
TEST_F(SimScopedTaskTest, AsyncBodyCompletesAfterSchedulerRun) {
  // Arbitrary positive delay to suspend the body until the scheduler runs.
  constexpr uint64_t kDelayNs = 100;

  bool finished = false;
  auto make = [&]() -> SimScopedTask {
    co_await simDelay(ctx_.get(), Time::fromNs(kDelayNs));
    finished = true;
  };

  SimScopedTask task = make();
  EXPECT_TRUE(task.valid());
  EXPECT_FALSE(task.done());
  EXPECT_FALSE(finished);

  scheduler_->run();

  EXPECT_TRUE(finished);
  EXPECT_TRUE(task.done());
}

// reset() destroys the owned frame and becomes empty; calling it again is a
// safe no-op (no double-free).
TEST_F(SimScopedTaskTest, ResetReleasesFrameAndIsIdempotent) {
  auto make = [&]() -> SimScopedTask { co_return; };

  SimScopedTask task = make();
  ASSERT_TRUE(task.valid());

  task.reset();
  EXPECT_FALSE(task.valid());
  EXPECT_FALSE(task.done());

  task.reset();
  EXPECT_FALSE(task.valid());
}

// Move construction transfers frame ownership; the moved-from task is empty.
TEST_F(SimScopedTaskTest, MoveConstructTransfersOwnership) {
  auto make = [&]() -> SimScopedTask { co_return; };

  SimScopedTask src = make();
  ASSERT_TRUE(src.valid());

  SimScopedTask dst = std::move(src);

  // Intentionally inspect the moved-from object: the move ctor leaves it empty
  // via `std::exchange(handle_, {})`, so reading `valid()` here is well-defined
  // and verifies the documented moved-from-is-empty contract (which is what
  // makes a moved-from task safe to destroy without a double-free).
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_FALSE(src.valid());
  EXPECT_TRUE(dst.valid());
  EXPECT_TRUE(dst.done());
}

// Move assignment destroys the destination's existing frame, then takes the
// source's — the `member_ = makeDriver();` pattern an owner uses in start().
TEST_F(SimScopedTaskTest, MoveAssignReplacesExistingFrame) {
  int completions = 0;
  auto make = [&]() -> SimScopedTask {
    ++completions;
    co_return;
  };

  SimScopedTask task = make();
  ASSERT_TRUE(task.valid());
  EXPECT_EQ(completions, 1);

  task = make();

  EXPECT_TRUE(task.valid());
  EXPECT_EQ(completions, 2);
}

// A default-constructed task owns nothing; reset()/dtor are safe no-ops.
TEST_F(SimScopedTaskTest, DefaultConstructedIsEmpty) {
  SimScopedTask task;

  EXPECT_FALSE(task.valid());
  EXPECT_FALSE(task.done());

  task.reset();
  EXPECT_FALSE(task.valid());
}

} // namespace packsim::coro
