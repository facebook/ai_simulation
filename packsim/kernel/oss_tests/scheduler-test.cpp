/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/switches_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

// Exercises the Scheduler pause/breakpoint pass-throughs, which delegate to
// PdesEngine. A small lookahead of 10fs puts frame boundaries at 0, 10, 20,
// 30..., so events scheduled at 5/15/25fs each land in their own frame.

TEST(SchedulerTest, BreakpointSetOps) {
  Scheduler sched;
  EXPECT_TRUE(sched.getBreakpointsFs().empty());

  sched.addBreakpointFs(30);
  sched.addBreakpointFs(10);
  sched.addBreakpointFs(20);
  EXPECT_EQ(sched.getBreakpointsFs(), (std::set<uint64_t>{10, 20, 30}));

  sched.removeBreakpointFs(20);
  EXPECT_EQ(sched.getBreakpointsFs(), (std::set<uint64_t>{10, 30}));

  sched.clearAllBreakpoints();
  EXPECT_TRUE(sched.getBreakpointsFs().empty());
}

TEST(SchedulerTest, RequestPauseAndResume) {
  Scheduler sched;
  sched.setLookaheadTimeFs(10);

  Scheduler::SimulationContext ctx;
  std::vector<uint64_t> executed;
  sched.scheduleAfter(
      &ctx, Time(5), [&executed]() { executed.emplace_back(5); });
  sched.scheduleAfter(
      &ctx, Time(15), [&executed]() { executed.emplace_back(15); });
  sched.scheduleAfter(
      &ctx, Time(25), [&executed]() { executed.emplace_back(25); });

  // Pause request before run: the engine stops after the first frame, so only
  // the event at 5fs runs.
  sched.requestPause();
  sched.run();
  EXPECT_TRUE(sched.isNotRunning());
  EXPECT_EQ(executed, (std::vector<uint64_t>{5}));
  EXPECT_EQ(sched.getLastEventTimeFs(), 5);

  // Resume to completion.
  sched.run();
  EXPECT_EQ(executed, (std::vector<uint64_t>{5, 15, 25}));
  EXPECT_EQ(sched.getLastEventTimeFs(), 25);
}

TEST(SchedulerTest, ClearPauseRequest) {
  Scheduler sched;
  sched.setLookaheadTimeFs(10);

  Scheduler::SimulationContext ctx;
  std::vector<uint64_t> executed;
  sched.scheduleAfter(
      &ctx, Time(5), [&executed]() { executed.emplace_back(5); });
  sched.scheduleAfter(
      &ctx, Time(15), [&executed]() { executed.emplace_back(15); });

  // A pause request that is cleared before run must not stop the simulation.
  sched.requestPause();
  sched.clearPauseRequest();
  sched.run();
  EXPECT_EQ(executed, (std::vector<uint64_t>{5, 15}));
}

TEST(SchedulerTest, PauseRequestNotCarriedAcrossRuns) {
  Scheduler sched;
  sched.setLookaheadTimeFs(10);

  // A pause requested when there is nothing to run must not persist: run() over
  // an empty queue never reaches the pause check, so the request has to be
  // cleared on exit. Otherwise the next run() would pause on its first frame
  // even though no new request was issued.
  sched.requestPause();
  sched.run();

  Scheduler::SimulationContext ctx;
  std::vector<uint64_t> executed;
  sched.scheduleAfter(
      &ctx, Time(5), [&executed]() { executed.emplace_back(5); });
  sched.scheduleAfter(
      &ctx, Time(15), [&executed]() { executed.emplace_back(15); });

  sched.run();
  EXPECT_EQ(executed, (std::vector<uint64_t>{5, 15}));
}

TEST(SchedulerTest, OnPacketDroppedCallbackInvoked) {
  Scheduler sched;
  bool called = false;
  std::string seenDevice;
  sched.setOnPacketDroppedCallback([&](const DropEvent& event) {
    called = true;
    seenDevice = event.deviceName;
  });

  const PacketPtr pkt{};
  const FlexDeviceId deviceId({0, 0, 0, 0, 0, 0, 0, 1});
  // Named so the string deviceName binds to plainly outlives onPacketDropped().
  const std::string deviceName = "RTSW_z0_r0";
  sched.onPacketDropped(
      DropEvent{
          deviceName,
          deviceId,
          /*port=*/5,
          switches::thrift::PacketDropReason::WRED,
          &pkt,
          /*queueDepthBytes=*/4096,
          /*simTimeFs=*/1'000'000'000});

  EXPECT_TRUE(called);
  EXPECT_EQ(seenDevice, "RTSW_z0_r0");
}

TEST(SchedulerTest, OnPacketDroppedWithNoSubscriberIsNoOp) {
  Scheduler sched;
  // No subscriber registered: onPacketDropped must be a safe no-op, not a
  // crash.
  const PacketPtr pkt{};
  const FlexDeviceId deviceId({0, 0, 0, 0, 0, 0, 0, 1});
  const std::string deviceName = "RTSW_z0_r0";
  sched.onPacketDropped(
      DropEvent{
          deviceName,
          deviceId,
          /*port=*/0,
          switches::thrift::PacketDropReason::WRED,
          &pkt,
          /*queueDepthBytes=*/0,
          /*simTimeFs=*/0});
}

TEST(SchedulerTest, SimContextReportsRegisteredScheduler) {
  // A SimContext registers with the Scheduler it is constructed with and hands
  // it back via getScheduler() — the bridge the endpoint uses to reach the
  // sim-wide scheduler (getSimCtx()->getScheduler()) during a run.
  Scheduler sched;
  SimContext ctx(&sched, "ping-ctx");
  EXPECT_EQ(ctx.getScheduler(), &sched);
}

} // namespace packsim
