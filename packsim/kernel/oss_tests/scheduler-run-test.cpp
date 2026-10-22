/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <stdexcept>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/duration.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/time.h"

namespace packsim {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

// Scheduler::runUntil, with events that record their delay in fs. A 10 fs
// lookahead puts frame boundaries at 0, 10, 20 fs, ...
class SchedulerRunTest : public ::testing::Test {
 protected:
  void SetUp() override {
    sched_.setLookaheadTimeFs(10);
  }

  // Schedule an event `delayFs` after timeNow() that appends `delayFs` to
  // executedDelaysFs_.
  void recordAfter(const uint64_t delayFs) {
    sched_.scheduleAfter(&ctx_, Duration(delayFs), [this, delayFs]() {
      executedDelaysFs_.emplace_back(delayFs);
    });
  }

  // Scheduler under test.
  Scheduler sched_;
  // Context the recorded events run on.
  Scheduler::SimulationContext ctx_;
  // Delays, in fs, of the recorded events that ran, in order.
  std::vector<uint64_t> executedDelaysFs_;
};

TEST_F(SchedulerRunTest, RunUntilStopsAtAnAbsoluteTime) {
  // run(Time(15)) from 10 fs would end at 25 fs and run the event at 20 fs.
  recordAfter(5);
  recordAfter(12);
  recordAfter(20);

  sched_.runUntil(Time(10));
  sched_.runUntil(Time(15));

  EXPECT_EQ(executedDelaysFs_, (std::vector<uint64_t>{5, 12}));
  EXPECT_EQ(sched_.timeNow(), Time(15));
}

TEST_F(SchedulerRunTest, RunUntilLeavesTheEventAtTheEndPending) {
  recordAfter(5);
  recordAfter(10);

  sched_.runUntil(Time(10));
  EXPECT_EQ(executedDelaysFs_, (std::vector<uint64_t>{5}));
  EXPECT_EQ(sched_.timeNow(), Time(10));

  sched_.runUntil(Time(11));
  EXPECT_EQ(executedDelaysFs_, (std::vector<uint64_t>{5, 10}));
  EXPECT_EQ(sched_.timeNow(), Time(11));
}

TEST_F(SchedulerRunTest, RunUntilNowRunsNothing) {
  // A hand-rolled run(end - now) is run(Time(0)) here, which runs the event.
  recordAfter(5);

  sched_.runUntil(sched_.timeNow());

  EXPECT_TRUE(executedDelaysFs_.empty());
  EXPECT_EQ(sched_.timeNow(), Time(0));
}

TEST_F(SchedulerRunTest, RunUntilPastTimeThrows) {
  sched_.runUntil(Time(10));

  EXPECT_THAT(
      [this] { sched_.runUntil(Time(5)); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("before the current time")));
  EXPECT_EQ(sched_.timeNow(), Time(10));
}

TEST_F(SchedulerRunTest, RunUntilFromAnEventHasNoEffect) {
  // Without the running check, timeNow() inside the event reads 5 fs and the
  // past end throws out of run().
  sched_.scheduleAfter(
      &ctx_, Duration(5), [this]() { sched_.runUntil(Time(0)); });

  EXPECT_NO_THROW(sched_.run());
  EXPECT_EQ(sched_.timeNow(), Time(5));
}

} // namespace packsim
