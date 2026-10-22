/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimScopedDelay.h"

#include <memory>

#include <gtest/gtest.h>

#include "packsim/kernel/coro/SimScopedTask.h"
#include "packsim/kernel/coro/SimTask.h"
#include "packsim/kernel/coro/oss_tests/fixtures/CoroTestFixture.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

using packsim::Scheduler;
using packsim::SimContext;
using packsim::Time;
using packsim::coro::SimResultTask;
using packsim::coro::simScopedDelay;
using packsim::coro::SimScopedTask;
using packsim::coro::tests::CoroTestFixture;

namespace packsim::coro {

class SimScopedDelayFixture : public CoroTestFixture {
 protected:
  void SetUp() override {
    CoroTestFixture::SetUp();
    scheduler_ = std::make_unique<Scheduler>();
    ctx_ = std::make_unique<SimContext>(scheduler_.get(), "ctx");
  }

  std::unique_ptr<Scheduler> scheduler_;
  std::unique_ptr<SimContext> ctx_;
};
using SimScopedDelayTest = SimScopedDelayFixture;

// Normal path: the delay fires after `delay` and resumes the coroutine.
TEST_F(SimScopedDelayTest, FiresAfterDelay) {
  // Delay applied to the scoped coroutine; the resume-time assertion below
  // must observe exactly this value.
  constexpr uint64_t kDelayNs = 100;

  bool resumed = false;
  // Name the coroutine lambda so the closure (which holds the [&] captures)
  // outlives the scheduler run that resumes the coroutine — an inline temporary
  // lambda would be destroyed at the end of this statement, dangling the
  // captures the resumed body touches (stack-use-after-scope).
  auto body = [&]() -> SimScopedTask {
    co_await simScopedDelay(ctx_.get(), Time::fromNs(kDelayNs));
    resumed = true;
  };
  SimScopedTask task = body();

  EXPECT_FALSE(resumed);
  scheduler_->run();

  EXPECT_TRUE(resumed);
  EXPECT_EQ(ctx_->nowNs(), kDelayNs);
}

// Teardown path: destroying the frame while parked on the delay cancels the
// scheduled resume — running the (now-empty) queue must not resume the
// destroyed frame, and sim-time must not advance to the delay's deadline.
TEST_F(SimScopedDelayTest, TeardownCancelsScheduledResume) {
  // Delay long enough that the frame is parked when torn down; the resume
  // must be cancelled so sim-time never reaches this deadline.
  constexpr uint64_t kDelayNs = 1000;

  bool resumed = false;
  auto body = [&]() -> SimScopedTask {
    co_await simScopedDelay(ctx_.get(), Time::fromNs(kDelayNs));
    resumed = true; // must NOT run after teardown
  };
  SimScopedTask task = body();

  task.reset(); // destroy the frame mid-delay

  scheduler_->run();
  EXPECT_FALSE(resumed);
  EXPECT_EQ(ctx_->nowNs(), 0);
}

// Cascade: a SimScopedTask whose body co_awaits a SimResultTask<void> child
// that is itself parked on a scoped delay. reset()ing the parent destroys the
// parent frame, which cascades into the child (~SimResultTask -> destroy) and
// then the child's SimScopedDelay (cancel). Proves (a) the relaxed
// SimResultTask dtor permits cascade-destroy of a co_await'd running child
// without aborting, and (b) the scoped delay's event is cancelled.
TEST_F(SimScopedDelayTest, CascadeThroughSimResultTaskTearsDownChild) {
  // Delay long enough that the child is parked when the parent is torn down;
  // the cascade must cancel it so sim-time never reaches this deadline.
  constexpr uint64_t kDelayNs = 1000;

  bool childAfter = false;
  bool parentAfter = false;

  auto child = [&]() -> SimResultTask<void> {
    co_await simScopedDelay(ctx_.get(), Time::fromNs(kDelayNs));
    childAfter = true;
  };
  auto parentBody = [&]() -> SimScopedTask {
    co_await child();
    parentAfter = true;
  };
  SimScopedTask parent = parentBody();

  // parent is suspended awaiting child; child is suspended on the scoped delay.
  parent.reset();

  scheduler_->run();
  EXPECT_FALSE(childAfter);
  EXPECT_FALSE(parentAfter);
  EXPECT_EQ(ctx_->nowNs(), 0);
}

} // namespace packsim::coro
