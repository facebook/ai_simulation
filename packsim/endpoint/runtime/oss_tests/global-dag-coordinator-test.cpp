/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>

#include <gmock/gmock-function-mocker.h>
#include <gtest/gtest.h>

#include "packsim/endpoint/runtime/global-dag-coordinator.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

using ::testing::HasSubstr;
using ::testing::MockFunction;
using ::testing::ThrowsMessage;

namespace packsim {

TEST(GlobalDagCoordinator, TestTriggerIterations) {
  Scheduler scheduler;

  GlobalDagCoordinator coordinator(
      &scheduler, /* numDags= */ 2, /* minIters= */ 3);

  auto node1 = std::make_unique<SimContext>(&scheduler, "node1");
  auto node2 = std::make_unique<SimContext>(&scheduler, "node2");
  auto node3 = std::make_unique<SimContext>(&scheduler, "node3");
  auto node4 = std::make_unique<SimContext>(&scheduler, "node4");

  coordinator.addDagNode(/* dagId= */ 0, node1.get());
  coordinator.addDagNode(/* dagId= */ 0, node2.get());

  coordinator.addDagNode(/* dagId= */ 1, node3.get());
  coordinator.addDagNode(/* dagId= */ 1, node4.get());

  MockFunction<void(uint32_t /* iter # */)> startNextIter1;
  MockFunction<void(uint32_t /* iter # */)> startNextIter2;
  MockFunction<void(uint32_t /* iter # */)> startNextIter3;
  MockFunction<void(uint32_t /* iter # */)> startNextIter4;
  MockFunction<void()> allDagsFinished;
  EXPECT_CALL(allDagsFinished, Call()).Times(1);
  coordinator.setOnAllDagsFinishedCallback(allDagsFinished.AsStdFunction());

  // node1, DAG 0
  EXPECT_CALL(startNextIter1, Call(1)).Times(1);
  EXPECT_CALL(startNextIter1, Call(2)).Times(1);
  EXPECT_CALL(startNextIter1, Call(3)).Times(1);
  EXPECT_CALL(startNextIter1, Call(4)).Times(1);
  EXPECT_CALL(startNextIter1, Call(5)).Times(1);

  // node2, DAG 0
  EXPECT_CALL(startNextIter2, Call(1)).Times(1);
  EXPECT_CALL(startNextIter2, Call(2)).Times(1);
  EXPECT_CALL(startNextIter2, Call(3)).Times(1);
  EXPECT_CALL(startNextIter2, Call(4)).Times(1);
  EXPECT_CALL(startNextIter2, Call(5)).Times(1);

  // node3, DAG 1
  EXPECT_CALL(startNextIter3, Call(1)).Times(1);
  EXPECT_CALL(startNextIter3, Call(2)).Times(1);

  // node4, DAG 1
  EXPECT_CALL(startNextIter4, Call(1)).Times(1);
  EXPECT_CALL(startNextIter4, Call(2)).Times(1);

  coordinator.arrive(
      /* dagId= */ 0, node1.get(), startNextIter1.AsStdFunction());
  coordinator.arrive(
      /* dagId= */ 0, node2.get(), startNextIter2.AsStdFunction());

  coordinator.arrive(
      /* dagId= */ 0, node1.get(), startNextIter1.AsStdFunction());
  coordinator.arrive(
      /* dagId= */ 0, node2.get(), startNextIter2.AsStdFunction());

  coordinator.arrive(
      /* dagId= */ 1, node3.get(), startNextIter3.AsStdFunction());
  coordinator.arrive(
      /* dagId= */ 1, node4.get(), startNextIter4.AsStdFunction());

  coordinator.arrive(
      /* dagId= */ 0, node1.get(), startNextIter1.AsStdFunction());
  coordinator.arrive(
      /* dagId= */ 0, node2.get(), startNextIter2.AsStdFunction());

  coordinator.arrive(
      /* dagId= */ 0, node1.get(), startNextIter1.AsStdFunction());
  coordinator.arrive(
      /* dagId= */ 0, node2.get(), startNextIter2.AsStdFunction());

  coordinator.arrive(
      /* dagId= */ 1, node3.get(), startNextIter3.AsStdFunction());
  coordinator.arrive(
      /* dagId= */ 1, node4.get(), startNextIter4.AsStdFunction());

  coordinator.arrive(
      /* dagId= */ 0, node1.get(), startNextIter1.AsStdFunction());
  coordinator.arrive(
      /* dagId= */ 0, node2.get(), startNextIter2.AsStdFunction());

  coordinator.arrive(
      /* dagId= */ 1, node3.get(), startNextIter3.AsStdFunction());
  coordinator.arrive(
      /* dagId= */ 1, node4.get(), startNextIter4.AsStdFunction());

  coordinator.arrive(
      /* dagId= */ 0, node1.get(), startNextIter1.AsStdFunction());
  coordinator.arrive(
      /* dagId= */ 0, node2.get(), startNextIter2.AsStdFunction());

  scheduler.run();
}

TEST(GlobalDagCoordinator, TestInvalidContext) {
  Scheduler scheduler;

  GlobalDagCoordinator coordinator(
      &scheduler, /* numDags= */ 2, /* minIters= */ 3);

  auto node1 = std::make_unique<SimContext>(&scheduler, "node1");
  auto node2 = std::make_unique<SimContext>(&scheduler, "node2");
  auto node3 = std::make_unique<SimContext>(&scheduler, "node3");
  auto node4 = std::make_unique<SimContext>(&scheduler, "node4");

  coordinator.addDagNode(/* dagId= */ 0, node1.get());
  coordinator.addDagNode(/* dagId= */ 0, node2.get());

  coordinator.addDagNode(/* dagId= */ 1, node3.get());
  coordinator.addDagNode(/* dagId= */ 1, node4.get());

  EXPECT_THAT(
      [&]() { coordinator.addDagNode(/* dagId= */ 5, node3.get()); },
      ThrowsMessage<std::runtime_error>(HasSubstr("Invalid DAG id")));

  MockFunction<void(uint32_t /* iter # */)> startNextIter1;
  MockFunction<void(uint32_t /* iter # */)> startNextIter2;
  MockFunction<void(uint32_t /* iter # */)> startNextIter3;
  MockFunction<void(uint32_t /* iter # */)> startNextIter4;

  EXPECT_THAT(
      [&]() {
        // node3 should not arrive at DAG 0
        coordinator.arrive(
            /* dagId= */ 0, node3.get(), startNextIter1.AsStdFunction());

        scheduler.run();
      },
      ThrowsMessage<std::runtime_error>(HasSubstr("Invalid context")));

  EXPECT_THAT(
      [&]() {
        // node1 should not arrive at DAG 1
        coordinator.arrive(
            /* dagId= */ 1, node1.get(), startNextIter3.AsStdFunction());

        scheduler.run();
      },
      ThrowsMessage<std::runtime_error>(HasSubstr("Invalid context")));
}

TEST(GlobalDagCoordinator, TestAllDagsFinishedWaitsForTheSlowestDag) {
  constexpr uint32_t kMinIters = 3;
  // Each DAG 1 iteration spends this long before its node arrives, against the
  // two one-lookahead hops (1 ns each by default) of a DAG 0 iteration.
  constexpr uint64_t kDag1IterationDelayNs = 10;
  Scheduler scheduler;

  GlobalDagCoordinator coordinator(
      &scheduler, /* numDags= */ 2, /* minIters= */ kMinIters);

  SimContext node1(&scheduler, "node1");
  SimContext node2(&scheduler, "node2");

  coordinator.addDagNode(/* dagId= */ 0, &node1);
  coordinator.addDagNode(/* dagId= */ 1, &node2);

  // Iterations each DAG had completed when it was last restarted. The nodes
  // and the coordinator run on PDES threads, so the callbacks only record what
  // they see and the test thread asserts after the run.
  std::array<std::atomic<uint32_t>, 2> itersAtLastRestart{};
  std::array<std::atomic<uint32_t>, 2> itersAtAllFinished{};
  std::atomic<uint32_t> allFinishedCount{0};
  std::atomic<bool> restartedAfterAllFinished{false};
  coordinator.setOnAllDagsFinishedCallback([&] {
    itersAtAllFinished[0] = itersAtLastRestart[0].load();
    itersAtAllFinished[1] = itersAtLastRestart[1].load();
    ++allFinishedCount;
  });

  std::function<void(uint32_t)> restartDag0;
  std::function<void(uint32_t)> restartDag1;
  restartDag0 = [&](uint32_t const iters) {
    if (allFinishedCount.load() != 0) {
      restartedAfterAllFinished = true;
    }
    itersAtLastRestart[0] = iters;
    coordinator.arrive(/* dagId= */ 0, &node1, restartDag0);
  };
  restartDag1 = [&](uint32_t const iters) {
    if (allFinishedCount.load() != 0) {
      restartedAfterAllFinished = true;
    }
    itersAtLastRestart[1] = iters;
    node2.schedule(Time::fromNs(kDag1IterationDelayNs), [&] {
      coordinator.arrive(/* dagId= */ 1, &node2, restartDag1);
    });
  };

  coordinator.arrive(/* dagId= */ 0, &node1, restartDag0);
  coordinator.arrive(/* dagId= */ 1, &node2, restartDag1);
  scheduler.run();

  EXPECT_EQ(allFinishedCount.load(), 1u);
  EXPECT_FALSE(restartedAfterAllFinished.load())
      << "a DAG restarted after every DAG finished";
  // A DAG's last iteration is the one after its last restart.
  EXPECT_EQ(itersAtAllFinished[1].load() + 1, kMinIters);
  EXPECT_GT(itersAtAllFinished[0].load() + 1, kMinIters)
      << "DAG 0 repeats until DAG 1 completes minIters";
}

TEST(GlobalDagCoordinator, ArrivingTwiceInOneIterationThrows) {
  Scheduler scheduler;
  GlobalDagCoordinator coordinator(
      &scheduler, /* numDags= */ 1, /* minIters= */ 2);
  SimContext node1(&scheduler, "node1");
  SimContext node2(&scheduler, "node2");
  coordinator.addDagNode(/* dagId= */ 0, &node1);
  coordinator.addDagNode(/* dagId= */ 0, &node2);

  EXPECT_THAT(
      [&]() {
        coordinator.arrive(/* dagId= */ 0, &node1, [](uint32_t) {});
        coordinator.arrive(/* dagId= */ 0, &node1, [](uint32_t) {});
        scheduler.run();
      },
      ThrowsMessage<std::runtime_error>(HasSubstr("arrived twice")));
}

TEST(GlobalDagCoordinator, ArrivingAtAFinishedDagThrows) {
  Scheduler scheduler;
  GlobalDagCoordinator coordinator(
      &scheduler, /* numDags= */ 1, /* minIters= */ 1);
  SimContext node(&scheduler, "node");
  coordinator.addDagNode(/* dagId= */ 0, &node);
  coordinator.setOnAllDagsFinishedCallback([] {});
  coordinator.arrive(/* dagId= */ 0, &node, [](uint32_t) {});
  scheduler.run();

  EXPECT_THAT(
      [&]() {
        coordinator.arrive(/* dagId= */ 0, &node, [](uint32_t) {});
        scheduler.run();
      },
      ThrowsMessage<std::runtime_error>(HasSubstr("already finished")));
}

TEST(GlobalDagCoordinator, ArriveRejectsAnUnknownDagId) {
  Scheduler scheduler;
  GlobalDagCoordinator coordinator(
      &scheduler, /* numDags= */ 1, /* minIters= */ 1);
  SimContext node(&scheduler, "node");
  coordinator.addDagNode(/* dagId= */ 0, &node);

  EXPECT_THAT(
      [&]() { coordinator.arrive(/* dagId= */ 3, &node, [](uint32_t) {}); },
      ThrowsMessage<std::runtime_error>(HasSubstr("Invalid DAG id 3")));
}

TEST(GlobalDagCoordinator, TestDagWithoutNodesDoesNotHoldBackOtherDags) {
  Scheduler scheduler;

  GlobalDagCoordinator coordinator(
      &scheduler, /* numDags= */ 2, /* minIters= */ 2);

  SimContext node1(&scheduler, "node1");
  // DAG 1 gets no nodes, so it never completes an iteration.
  coordinator.addDagNode(/* dagId= */ 0, &node1);

  MockFunction<void()> allDagsFinished;
  EXPECT_CALL(allDagsFinished, Call()).Times(1);
  coordinator.setOnAllDagsFinishedCallback(allDagsFinished.AsStdFunction());

  MockFunction<void(uint32_t /* iter # */)> startNextIter;
  EXPECT_CALL(startNextIter, Call(1)).WillOnce([&](uint32_t /* iter # */) {
    coordinator.arrive(
        /* dagId= */ 0, &node1, startNextIter.AsStdFunction());
  });

  coordinator.arrive(
      /* dagId= */ 0, &node1, startNextIter.AsStdFunction());
  scheduler.run();
}

} // namespace packsim
