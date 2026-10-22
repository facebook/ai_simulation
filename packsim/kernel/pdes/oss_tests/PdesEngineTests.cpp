/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <folly/system/HardwareConcurrency.h>
#include <gflags/gflags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/pdes/PdesEngine.h"

using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim::pdes {

namespace {

// Event body for the tests whose event throws. A named function rather than an
// inline lambda so it can carry [[noreturn]]: C++20 has no syntax for that
// attribute on a lambda.
[[noreturn]] void throwFromContext1Event() {
  throw std::runtime_error("Test func throws in context1.");
}

// Event bodies for the tests where two contexts throw in the same frame.
[[noreturn]] void throwFromContext1At7() {
  throw std::runtime_error("context1 throws at 7");
}

[[noreturn]] void throwFromContext2At5() {
  throw std::runtime_error("context2 throws at 5");
}

class RingNode : public SimulationContext {
 public:
  RingNode(
      PdesEngine* enginePtr,
      size_t id,
      std::vector<RingNode*> const& nodePtrs)
      : enginePtr_(enginePtr), id_(id), nodePtrs_(nodePtrs) {}

  virtual ~RingNode() = default;

  Event scheduleInitialEvent(uint64_t const initialDelayFs) {
    return enginePtr_->scheduleAt(
        nodePtrs_.at(id_),
        enginePtr_->nowFs(nodePtrs_.at(id_)) + initialDelayFs,
        [this]() {
          auto const dstId = (id_ + 1) % nodePtrs_.size();
          sendMessage(dstId, id_);
        });
  }

  virtual void sendMessage(size_t dstId, size_t message) {
    enginePtr_->scheduleAt(
        nodePtrs_.at(dstId),
        enginePtr_->nowFs(nodePtrs_.at(id_)) + getLatencyFs(id_, dstId),
        [dst = nodePtrs_.at(dstId), message]() {
          dst->receiveMessage(message);
        });
  }

  virtual void receiveMessage(size_t message) {
    if (message == id_) {
      finishTimeFs_ = enginePtr_->nowFs(nodePtrs_.at(id_));
      return;
    }
    // Forward the message to the next node when you get it.
    auto const dstId = (id_ + 1) % nodePtrs_.size();
    enginePtr_->scheduleAt(
        nodePtrs_.at(id_),
        enginePtr_->nowFs(nodePtrs_.at(id_)),
        [this, dstId, message]() { sendMessage(dstId, message); });
  }

  static uint64_t getLatencyFs(size_t /* srcId */, size_t dstId) {
    return 100 + dstId;
  }

  std::optional<uint64_t> getFinishTimeFs() const {
    return finishTimeFs_;
  }

 protected:
  PdesEngine* enginePtr_;
  size_t const id_;
  std::vector<RingNode*> const& nodePtrs_;
  std::optional<uint64_t> finishTimeFs_;
};

class RingNodeContextless : public RingNode {
 public:
  RingNodeContextless(
      PdesEngine* enginePtr,
      size_t id,
      std::vector<RingNode*> const& nodePtrs)
      : RingNode(enginePtr, id, nodePtrs) {}

  virtual void sendMessage(size_t dstId, size_t message) override {
    enginePtr_->scheduleAt(
        nodePtrs_.at(dstId),
        enginePtr_->nowFs() + getLatencyFs(id_, dstId),
        [dst = nodePtrs_.at(dstId), message]() {
          dst->receiveMessage(message);
        });
  }

  virtual void receiveMessage(size_t message) override {
    if (message == id_) {
      finishTimeFs_ = enginePtr_->nowFs();
      return;
    }
    // Forward the message to the next node when you get it.
    auto const dstId = (id_ + 1) % nodePtrs_.size();
    enginePtr_->schedule(getProcessingLatencyFs(), [this, dstId, message]() {
      enginePtr_->scheduleNow(
          [this, dstId, message]() { sendMessage(dstId, message); });
    });
  }

  static uint64_t getProcessingLatencyFs() {
    return 5;
  }
};

// Counts its copies, so a test can tell an argument that was copied from one
// that was moved.
class CopyCounter {
 public:
  explicit CopyCounter(int* copies) : copies_(copies) {}
  CopyCounter(const CopyCounter& other) : copies_(other.copies_) {
    ++*copies_;
  }
  CopyCounter(CopyCounter&& other) noexcept = default;
  CopyCounter& operator=(const CopyCounter&) = delete;
  CopyCounter& operator=(CopyCounter&&) = delete;

 private:
  int* copies_;
};

// Event body that keeps its argument, and so takes it by value.
void keepCounter(std::optional<CopyCounter>* kept, CopyCounter counter) {
  kept->emplace(std::move(counter));
}
} // namespace

TEST(PdesEngine, PdesEngineRingTransmissionModel) {
  constexpr auto kLookaheadTimeFs = 100;
  constexpr size_t kNumNodes = 100;
  constexpr auto kNumThreads = 20;
  constexpr auto kInitialDelayFs = 50;

  auto pdes = PdesEngine(kLookaheadTimeFs, kNumThreads);

  EXPECT_EQ(pdes.getLookaheadTimeFs(), kLookaheadTimeFs);

  // Create nodes for the simulation.
  std::vector<std::unique_ptr<RingNode>> nodes;
  std::vector<RingNode*> nodePtrs;
  nodes.reserve(kNumNodes);
  for (size_t i = 0; i < kNumNodes; ++i) {
    nodes.emplace_back(
        std::make_unique<RingNode>(&pdes, /* id= */ i, nodePtrs));
    nodePtrs.emplace_back(nodes.back().get());
  }

  for (auto& node : nodes) {
    node->scheduleInitialEvent(kInitialDelayFs);
  }

  pdes.run();

  constexpr auto kExpectedEndToEndDelay = 50 /* initial delay */ +
      (100 * 100) /* constant propagation latency */ +
      ((99 * 100) /
       2) /* additonal propagation latency, i.e., 0 + 1 + ... + 99 */;

  for (size_t i = 0; i < kNumNodes; ++i) {
    EXPECT_EQ(nodes[i]->getFinishTimeFs(), kExpectedEndToEndDelay);
  }

  std::unordered_map<size_t, int> contextIds;
  for (size_t i = 0; i < kNumNodes; ++i) {
    contextIds[pdes.contextToThreadNum(nodes[i].get())]++;
  }

  EXPECT_EQ(contextIds.size(), kNumThreads);
  for (size_t i = 0; i < kNumThreads; ++i) {
    EXPECT_EQ(contextIds[i], kNumNodes / kNumThreads);
  }
}

TEST(PdesEngine, PdesEngineRingTransmissionModelContextless) {
  constexpr auto kLookaheadTimeFs = 100;
  constexpr size_t kNumNodes = 100;
  constexpr auto kNumThreads = 20;
  constexpr auto kInitialDelayFs = 50;

  auto pdes = PdesEngine(kLookaheadTimeFs, kNumThreads);

  EXPECT_EQ(pdes.getLookaheadTimeFs(), kLookaheadTimeFs);

  // Create nodes for the simulation.
  std::vector<std::unique_ptr<RingNodeContextless>> nodes;
  std::vector<RingNode*> nodePtrs;
  nodes.reserve(kNumNodes);
  for (size_t i = 0; i < kNumNodes; ++i) {
    nodes.emplace_back(
        std::make_unique<RingNodeContextless>(&pdes, /* id= */ i, nodePtrs));
    nodePtrs.emplace_back(nodes.back().get());
  }

  for (auto& node : nodes) {
    node->scheduleInitialEvent(kInitialDelayFs);
  }

  pdes.run();

  constexpr auto kExpectedEndToEndDelay = 50 /* initial delay */ +
      (100 * 100) /* constant propagation latency */ +
      (99 * 5) /* constant processing latency (except for receiver) */ +
      ((99 * 100) /
       2) /* additonal propagation latency, i.e., 0 + 1 + ... + 99 */;

  for (size_t i = 0; i < kNumNodes; ++i) {
    EXPECT_EQ(nodes[i]->getFinishTimeFs(), kExpectedEndToEndDelay);
  }
}

TEST(PdesEngine, PdesEngineRingTransmissionModelRunLessThanLookahead) {
  constexpr auto kLookaheadTimeFs = 100;
  constexpr size_t kNumNodes = 100;
  constexpr auto kNumThreads = 20;
  constexpr auto kInitialDelayFs = 50;

  auto pdes = PdesEngine(kLookaheadTimeFs, kNumThreads);

  EXPECT_EQ(pdes.getLookaheadTimeFs(), kLookaheadTimeFs);

  // Create nodes for the simulation.
  std::vector<std::unique_ptr<RingNode>> nodes;
  std::vector<RingNode*> nodePtrs;
  nodes.reserve(kNumNodes);
  for (size_t i = 0; i < kNumNodes; ++i) {
    nodes.emplace_back(
        std::make_unique<RingNode>(&pdes, /* id= */ i, nodePtrs));
    nodePtrs.emplace_back(nodes.back().get());
  }

  EXPECT_EQ(pdes.getEventCount(), 0);

  std::vector<Event> initialEvents;
  initialEvents.reserve(nodes.size());
  for (auto& node : nodes) {
    initialEvents.emplace_back(node->scheduleInitialEvent(kInitialDelayFs));
  }

  EXPECT_EQ(pdes.getEventCount(), kNumNodes);

  pdes.run(/* durationFs= */ 20);

  for (auto& initialEvent : initialEvents) {
    EXPECT_EQ(initialEvent.getEventTimeFs() - pdes.nowFs(), 50 - 20);
  }

  auto firstStopEvent =
      pdes.scheduleAt(nodes[0].get(), 80, [&pdes]() { pdes.stop(); });

  auto secondStopEvent =
      pdes.scheduleAt(nodes[0].get(), 180, [&pdes]() { pdes.stop(); });

  auto thirdStopEvent =
      pdes.scheduleAt(nodes[0].get(), 1000, [&pdes]() { pdes.stop(); });

  EXPECT_TRUE(firstStopEvent.isActive());
  EXPECT_TRUE(secondStopEvent.isActive());
  EXPECT_TRUE(thirdStopEvent.isActive());

  pdes.run();

  EXPECT_FALSE(firstStopEvent.isActive());
  EXPECT_TRUE(secondStopEvent.isActive());
  EXPECT_TRUE(thirdStopEvent.isActive());

  for (auto& node : nodes) {
    EXPECT_EQ(pdes.nowFs(node.get()), 80);
  }

  secondStopEvent.cancel();

  EXPECT_FALSE(secondStopEvent.isActive());
  EXPECT_TRUE(thirdStopEvent.isActive());

  pdes.run(/* durationFs= */ 950);

  EXPECT_FALSE(thirdStopEvent.isActive());

  // Stop is called at 1000fs, but other contexts can run until the end of their
  // frame, which is at 1030fs (80 + 950). The final event in that time occurs
  // at 1025fs.
  for (auto& node : nodes) {
    EXPECT_EQ(pdes.nowFs(node.get()), 1025);
  }

  pdes.run();

  EXPECT_FALSE(firstStopEvent.isActive());
  EXPECT_FALSE(secondStopEvent.isActive());
  EXPECT_FALSE(thirdStopEvent.isActive());

  constexpr auto kExpectedEndToEndDelay = 50 /* initial delay */ +
      (100 * 100) /* constant propagation latency */ +
      ((99 * 100) /
       2) /* additonal propagation latency, i.e., 0 + 1 + ... + 99 */;

  for (size_t i = 0; i < kNumNodes; ++i) {
    EXPECT_EQ(nodes[i]->getFinishTimeFs(), kExpectedEndToEndDelay);
  }
}

TEST(PdesEngine, PdesEngineInvalidLookahead) {
  EXPECT_THAT(
      []() { PdesEngine(0); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Lookahead time must be a positive value.")));
}

TEST(PdesEngine, SetLookaheadTimeRejectsZero) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 100);

  EXPECT_THAT(
      [&pdes]() { pdes.setLookaheadTimeFs(0); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Lookahead time must be a positive value.")));
}

TEST(PdesEngine, PdesEngineNumThreads) {
  auto const hardwareCpuCount = folly::available_concurrency();
  {
    auto pdes = PdesEngine(/* lookaheadTimeFs= */ 100, /* numThreads= */ 10);
    EXPECT_THAT(pdes.numThreads_, 10);
  }
  {
    auto pdes = PdesEngine(/* lookaheadTimeFs= */ 100, /* numThreads= */ 0);
    EXPECT_THAT(pdes.numThreads_, hardwareCpuCount);
  }
}

TEST(PdesEngine, PdesEngineAlreadyRunning) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 100, /* numThreads= */ 10);

  pdes.isRunning_ = true;
  pdes.isStopping_ = true;

  // Since the engine is already running, calling run again will return
  // immediately without affecting any system state.
  pdes.run();

  EXPECT_TRUE(pdes.isStopping_);
  EXPECT_TRUE(pdes.isRunning_);
}

TEST(PdesEngine, PdesEngineRunFrameInvalidDurationZero) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 100, /* numThreads= */ 10);

  EXPECT_THAT(
      [&pdes]() { pdes.runFrame(/* durationFs= */ 0); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Cannot run durations that are non-positive.")));
}

TEST(PdesEngine, PdesEngineRunFrameInvalidDurationGreaterThanLookahead) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 100, /* numThreads= */ 10);

  EXPECT_THAT(
      [&pdes]() { pdes.runFrame(/* durationFs= */ 110); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Cannot run longer than lookahead time in one shot.")));
}

TEST(PdesEngine, PdesEngineRunFrameInvalidDurationOverflow) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 100, /* numThreads= */ 10);

  pdes.nextFrameTimeFs_ = std::numeric_limits<uint64_t>::max() - 20;

  EXPECT_THAT(
      [&pdes]() { pdes.runFrame(/* durationFs= */ 50); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Time overflow, cannot run simulation any further.")));
}

TEST(PdesEngine, PdesEngineRunInvalidDurationOverflow) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 100, /* numThreads= */ 10);

  pdes.nextFrameTimeFs_ = std::numeric_limits<uint64_t>::max() - 20;

  EXPECT_THAT(
      [&pdes]() { pdes.run(/* durationFs= */ 50); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Time overflow, cannot run simulation any further.")));
  EXPECT_FALSE(pdes.isRunning_);
}

TEST(PdesEngine, PdesEngineCausalityParadox) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 100, /* numThreads= */ 2);

  SimulationContext context1;
  SimulationContext context2;

  pdes.scheduleAt(&context1, /* timeFs= */ 0, []() { /* do nothing */ });
  pdes.scheduleAt(&context2, /* timeFs= */ 0, []() { /* do nothing */ });

  context1.currentTimeFs = 20;
  context2.currentTimeFs = 20;

  EXPECT_THAT(
      [&pdes]() { pdes.run(); },
      ThrowsMessage<std::runtime_error>(HasSubstr("Causality Paradox")));
}

TEST(PdesEngine, EventIsActiveWhileItsCallbackRuns) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
  SimulationContext context;
  Event handle;
  bool activeDuringCallback = false;
  handle = pdes.scheduleAt(&context, /* timeFs= */ 15, [&]() {
    activeDuringCallback = handle.isActive();
  });

  pdes.run();

  EXPECT_TRUE(activeDuringCallback);
  EXPECT_FALSE(handle.isActive());
}

TEST(PdesEngine, PdesEngineScheduleOnCurrentContextInThePastThrows) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
  SimulationContext context;
  // At time 15 the event schedules another one on its own context at time 5.
  pdes.scheduleAt(&context, /* timeFs= */ 15, [&pdes, &context]() {
    pdes.scheduleAt(&context, /* timeFs= */ 5, []() {});
  });

  EXPECT_THAT(
      [&pdes]() { pdes.run(); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Cannot schedule an event in the past")));
}

TEST(PdesEngine, PdesEngineDelayOverflowThrows) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
  SimulationContext context;
  pdes.scheduleAt(&context, /* timeFs= */ 15, [&pdes]() {
    pdes.schedule(std::numeric_limits<uint64_t>::max() - 9, []() {});
  });

  EXPECT_THAT(
      [&pdes]() { pdes.run(); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Time overflow, cannot schedule the event.")));
}

TEST(PdesEngine, PdesEngineScheduleOnAnotherContextInThePastThrows) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
  SimulationContext context;
  context.currentTimeFs = 20;

  EXPECT_THAT(
      [&]() { pdes.scheduleAt(&context, /* timeFs= */ 5, []() {}); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Cannot schedule an event in the past")));
}

TEST(PdesEngine, PdesEngineScheduleRelativeOutsideThePoolThrows) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);

  EXPECT_THAT(
      [&pdes]() { pdes.schedule(/* delayFs= */ 5, []() {}); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("not part of the PDES thread pool")));
}

TEST(PdesEngine, PdesEngineScheduleRelativeFromAnotherEnginesPoolThrows) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
  auto otherPdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
  SimulationContext context;
  // A worker thread of `pdes` is not part of `otherPdes`'s pool, so it has no
  // current context there to schedule relative to.
  pdes.scheduleAt(&context, /* timeFs= */ 5, [&otherPdes]() {
    otherPdes.schedule(/* delayFs= */ 5, []() {});
  });

  EXPECT_THAT(
      [&pdes]() { pdes.run(); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("not part of the PDES thread pool")));
  EXPECT_EQ(otherPdes.getEventCount(), 0);
}

TEST(PdesEngine, PdesEngineFuncThrows) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);

  SimulationContext context1;
  SimulationContext context2;

  pdes.scheduleAt(&context1, /* timeFs= */ 0, []() { /* do nothing */ });
  pdes.scheduleAt(&context2, /* timeFs= */ 0, []() { /* do nothing */ });

  pdes.scheduleAt(&context1, /* timeFs= */ 15, throwFromContext1Event);

  // If we run, context 1 should throw in frame 2.
  EXPECT_THAT(
      [&pdes]() { pdes.run(); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Test func throws in context1.")));

  EXPECT_EQ(pdes.nowFs(&context1), 15);
  EXPECT_EQ(pdes.nowFs(&context2), 15);
}

TEST(PdesEngine, PdesEngineRethrowsEarliestOfSeveralFrameExceptions) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
  // Contexts go to threads round-robin as they are first seen, so these two
  // run on different threads within the same frame.
  SimulationContext context1;
  SimulationContext context2;
  pdes.scheduleAt(&context1, /* timeFs= */ 7, throwFromContext1At7);
  pdes.scheduleAt(&context2, /* timeFs= */ 5, throwFromContext2At5);

  EXPECT_THAT(
      [&pdes]() { pdes.run(); },
      ThrowsMessage<std::runtime_error>(HasSubstr("context2 throws at 5")));
}

TEST(PdesEngine, PdesEngineRethrowsEarliestExceptionOfContextsOnOneThread) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 1);
  // Both contexts run on the one thread, context1 first, so the later event's
  // exception is raised first and context2 runs after it.
  SimulationContext context1;
  SimulationContext context2;
  pdes.scheduleAt(&context1, /* timeFs= */ 7, throwFromContext1At7);
  pdes.scheduleAt(&context2, /* timeFs= */ 5, throwFromContext2At5);

  EXPECT_THAT(
      [&pdes]() { pdes.run(); },
      ThrowsMessage<std::runtime_error>(HasSubstr("context2 throws at 5")));
}

TEST(PdesEngine, PdesEngineDoesNotRethrowAnEarlierFramesException) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
  SimulationContext context1;
  SimulationContext context2;
  pdes.scheduleAt(&context1, /* timeFs= */ 7, throwFromContext1At7);
  pdes.scheduleAt(&context2, /* timeFs= */ 5, throwFromContext2At5);
  ASSERT_THAT(
      [&pdes]() { pdes.run(); },
      ThrowsMessage<std::runtime_error>(HasSubstr("context2 throws at 5")));

  bool ranLaterEvent = false;
  pdes.scheduleAt(&context1, /* timeFs= */ 20, [&ranLaterEvent]() {
    ranLaterEvent = true;
  });

  EXPECT_NO_THROW(pdes.run());
  EXPECT_TRUE(ranLaterEvent);
}

TEST(PdesEngine, PdesEngineHandleToThrowingEventGoesStale) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
  SimulationContext context;
  const Event handle =
      pdes.scheduleAt(&context, /* timeFs= */ 5, throwFromContext1Event);

  EXPECT_THAT(
      [&pdes]() { pdes.run(); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Test func throws in context1.")));

  // Reading the handle must not touch freed memory (ASAN would report it), and
  // cancelling it must not revive or disturb a recycled event.
  EXPECT_FALSE(handle.isActive());
  handle.cancel();
  EXPECT_FALSE(handle.isActive());
}

TEST(PdesEngine, PdesEngineRunsAMoveOnlyCallable) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10);
  SimulationContext context;
  int valueSeen = 0;
  auto value = std::make_unique<int>(42);
  pdes.scheduleAt(
      &context,
      /* timeFs= */ 5,
      [&valueSeen, value = std::move(value)]() { valueSeen = *value; });

  pdes.run();

  EXPECT_EQ(valueSeen, 42);
}

TEST(PdesEngine, PdesEngineMovesAStoredArgumentIntoAByValueCallee) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10);
  SimulationContext context;
  int copies = 0;
  std::optional<CopyCounter> kept;
  pdes.scheduleAt(
      &context, /* timeFs= */ 5, &keepCounter, &kept, CopyCounter(&copies));

  pdes.run();

  ASSERT_TRUE(kept.has_value());
  EXPECT_EQ(copies, 0);
}

TEST(PdesEngine, PdesEngineMovesAStoredArgumentIntoAByValueCalleeOnItsContext) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10);
  SimulationContext context;
  int copies = 0;
  std::optional<CopyCounter> kept;
  pdes.scheduleAt(&context, /* timeFs= */ 5, [&pdes, &copies, &kept]() {
    pdes.schedule(
        /* delayFs= */ 1, &keepCounter, &kept, CopyCounter(&copies));
  });

  pdes.run();

  ASSERT_TRUE(kept.has_value());
  EXPECT_EQ(copies, 0);
}

TEST(PdesEngine, PdesEngineFuncExecutionOrder) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10);

  SimulationContext context;

  std::vector<std::string> funcNames;

  pdes.scheduleAt(&context, /* timeFs= */ 0, [&funcNames]() {
    funcNames.emplace_back("func1");
  });
  pdes.scheduleAt(&context, /* timeFs= */ 0, [&funcNames]() {
    funcNames.emplace_back("func2");
  });
  pdes.scheduleAt(&context, /* timeFs= */ 0, [&funcNames]() {
    funcNames.emplace_back("func3");
  });

  pdes.run();

  EXPECT_THAT(funcNames, ElementsAre("func1", "func2", "func3"));
}

TEST(PdesEngine, SameTimeEventsRunInSortIdOrderRatherThanQueueOrder) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 100, /* numThreads= */ 1);
  SimulationContext context;
  std::vector<std::string> ranEvents;
  // Scheduled between runs, so it enters the context's queue when the frame
  // starts, under a sort id above any a context gives.
  pdes.scheduleAt(&context, /* timeFs= */ 10, [&ranEvents]() {
    ranEvents.emplace_back("scheduled between runs");
  });
  pdes.scheduleAt(&context, /* timeFs= */ 5, [&pdes, &context, &ranEvents]() {
    // Queued after the event above and at the same time, under the context's
    // own, lower sort id.
    pdes.scheduleAt(&context, /* timeFs= */ 10, [&ranEvents]() {
      ranEvents.emplace_back("scheduled by the context");
    });
  });

  pdes.run();

  EXPECT_THAT(
      ranEvents,
      ElementsAre("scheduled by the context", "scheduled between runs"));
}

TEST(PdesEngine, CancelledEventIsSkippedWhenItReachesTheFront) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 100, /* numThreads= */ 1);
  SimulationContext context;
  std::vector<std::string> ranEvents;
  Event cancelled;
  pdes.scheduleAt(&context, /* timeFs= */ 10, [&ranEvents, &cancelled]() {
    ranEvents.emplace_back("canceller");
    cancelled.cancel();
  });
  cancelled = pdes.scheduleAt(&context, /* timeFs= */ 20, [&ranEvents]() {
    ranEvents.emplace_back("cancelled");
  });
  pdes.scheduleAt(&context, /* timeFs= */ 30, [&ranEvents]() {
    ranEvents.emplace_back("after the cancelled one");
  });

  pdes.run();

  EXPECT_THAT(ranEvents, ElementsAre("canceller", "after the cancelled one"));
}

TEST(PdesEngine, PdesEngineCheckTimeAdvancementDurationZero) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 100, /* numThreads= */ 10);

  SimulationContext context;
  pdes.scheduleAt(&context, /* timeFs= */ 10, []() { /* do nothing */ });

  EXPECT_EQ(pdes.nowFs(&context), 0);

  // Duration is not provided, this means simulation must stop at 100 fs since
  // that's the frame time for last event.
  pdes.run();

  EXPECT_EQ(pdes.nowFs(&context), 10);

  // Time should be universally correct outside run call
  EXPECT_EQ(pdes.nowFs(), 10);
}

TEST(PdesEngine, PdesEngineCheckTimeAdvancementDurationGreaterThanZero) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 100, /* numThreads= */ 10);

  SimulationContext context;
  auto eventPtr1 =
      pdes.scheduleAt(&context, /* timeFs= */ 10, []() { /* do nothing */ });
  auto eventPtr2 =
      pdes.scheduleAt(&context, /* timeFs= */ 60, []() { /* do nothing */ });
  auto eventPtr3 =
      pdes.scheduleAt(&context, /* timeFs= */ 90, []() { /* do nothing */ });

  EXPECT_EQ(pdes.nowFs(&context), 0);

  EXPECT_TRUE(eventPtr1.isActive());
  EXPECT_TRUE(eventPtr2.isActive());
  EXPECT_TRUE(eventPtr3.isActive());

  // Duration is provided, this means simulation must stop at current time +
  // duration exactly.
  pdes.run(/* durationFs= */ 80);

  EXPECT_EQ(pdes.nowFs(&context), 80);

  EXPECT_FALSE(eventPtr1.isActive());
  EXPECT_FALSE(eventPtr2.isActive());
  EXPECT_TRUE(eventPtr3.isActive());
}

TEST(PdesEngine, PdesEngineContextlessValidateContext) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 1, /* numThreads= */ 10);

  SimulationContext context1;
  SimulationContext context2;

  pdes.scheduleAt(&context1, /* timeFs= */ 10, [&]() {
    EXPECT_EQ(pdes.getCurrentThread()->currentContext, &context1);
    pdes.scheduleAt(&context2, /* timeFs= */ 15, [&]() {
      EXPECT_EQ(pdes.getCurrentThread()->currentContext, &context2);
    });
  });

  pdes.scheduleAt(&context2, /* timeFs= */ 10, [&]() {
    EXPECT_EQ(pdes.getCurrentThread()->currentContext, &context2);
    pdes.scheduleNow([&]() {
      EXPECT_EQ(pdes.getCurrentThread()->currentContext, &context2);
      pdes.scheduleNow([&]() {
        EXPECT_EQ(pdes.getCurrentThread()->currentContext, &context2);
        pdes.scheduleNow([&]() {
          EXPECT_EQ(pdes.getCurrentThread()->currentContext, &context2);
          pdes.scheduleAt(&context1, /* timeFs= */ 50, [&]() {
            EXPECT_EQ(pdes.getCurrentThread()->currentContext, &context1);
          });
        });
      });
    });
    pdes.schedule(/* delayFs= */ 10, [&]() {
      EXPECT_EQ(pdes.getCurrentThread()->currentContext, &context2);
      pdes.schedule(/* delayFs= */ 10, [&]() {
        EXPECT_EQ(pdes.getCurrentThread()->currentContext, &context2);
        pdes.schedule(/* delayFs= */ 10, [&]() {
          EXPECT_EQ(pdes.getCurrentThread()->currentContext, &context2);
          pdes.scheduleAt(&context1, /* timeFs= */ 50, [&]() {
            EXPECT_EQ(pdes.getCurrentThread()->currentContext, &context1);
          });
        });
      });
    });
  });

  pdes.run();
}

TEST(PdesEngine, PdesEngineRunsAContextAnotherEngineAlreadyAssigned) {
  SimulationContext context;
  auto firstPdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
  firstPdes.registerContext(&context);
  auto secondPdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
  bool ran = false;
  secondPdes.scheduleAt(&context, /* timeFs= */ 5, [&ran]() { ran = true; });

  secondPdes.run();

  EXPECT_TRUE(ran);
}

TEST(PdesEngine, PdesEngineRunsAContextAfterItsFirstEngineIsGone) {
  // Mirrors Scheduler::buildNewEngine, which hands existing contexts to a
  // fresh engine.
  SimulationContext context;
  {
    auto firstPdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
    firstPdes.scheduleAt(&context, /* timeFs= */ 5, []() {});
    firstPdes.run();
  }
  auto secondPdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
  bool ran = false;
  secondPdes.scheduleAt(&context, /* timeFs= */ 20, [&ran]() { ran = true; });

  secondPdes.run();

  EXPECT_TRUE(ran);
}

TEST(PdesEngine, RebalanceEvents) {
  class MockPdesEngine : public PdesEngine {
   public:
    explicit MockPdesEngine(uint64_t lookaheadTimeFs, size_t numThreads = 0)
        : PdesEngine(lookaheadTimeFs, numThreads) {}

    PdesEngine::time_point getTime() const override {
      if (!overrideTime) {
        return PdesEngine::getTime();
      }
      return overrideTimeVal;
    }
    bool overrideTime{false};
    PdesEngine::time_point overrideTimeVal;
  };

  constexpr int kNumThreads = 4;
  constexpr int kNumContexts = 100;
  auto pdes =
      MockPdesEngine(/* lookaheadTimeFs= */ 100, /* numThreads= */ kNumThreads);

  std::vector<SimulationContext> contexts(kNumContexts);

  // Pathological scheduling of events; there are four threads, only schedule
  // real work on thread 0
  uint64_t val = 0;
  for (size_t ctx = 0; ctx < kNumContexts; ctx++) {
    auto context = &contexts[ctx];
    if ((ctx % kNumThreads) == 0) {
      pdes.scheduleAt(
          /*context=*/context,
          /*timeFs=*/ctx,
          [&val]() {
            for (uint64_t i = 1; i < 1'000'000; i++) {
              val += i;
              val *= i;
              val /= i;
              val -= i;
              val >>= (i % 32);
            }
          });
    } else {
      pdes.scheduleAt(
          /*context=*/context,
          /*timeFs=*/ctx,
          []() {
            /* do nothing */
          });
    }
  }

  pdes.run();

  double totalTimeBefore = 0;
  auto timesBeforeRebalance = pdes.getThreadCpuWorkTimes();
  for (size_t i = 1; i < kNumThreads; i++) {
    totalTimeBefore += timesBeforeRebalance.at(i);
    EXPECT_GT(timesBeforeRebalance.at(0), timesBeforeRebalance.at(i));
  }
  double ratioBefore =
      timesBeforeRebalance.at(0) / totalTimeBefore / kNumThreads;
  EXPECT_GT(ratioBefore, FLAGS_pdes_rebalance_ratio);

  // Need to advance time past FLAGS_pdes_rebalance_seconds to trigger
  // rebalancing
  uint64_t advanceTime = std::round(2 * FLAGS_pdes_rebalance_seconds);
  pdes.overrideTimeVal = pdes.getTime() + std::chrono::seconds(advanceTime);
  pdes.overrideTime = true;

  EXPECT_TRUE(pdes.shouldRebalance());

  std::vector<size_t> threadIdxBefore;
  threadIdxBefore.reserve(contexts.size());
  for (const auto& context : contexts) {
    threadIdxBefore.emplace_back(context.threadIdx.load());
  }

  // Set FLAGS_pdes_integrate_balance to true so we don't set the per-context
  // CPU work time to 0. We can then use this to check that rebalanceing
  // happened in a sane way. The FlagSaver restores it when the test scope
  // exits, including on an early EXPECT/ASSERT failure below.
  const gflags::FlagSaver flagSaver;
  FLAGS_pdes_integrate_balance = true;

  pdes.rebalanceContexts();
  auto timesAfterRebalance = pdes.getThreadCpuWorkTimes();

  // Expect time to go down on the first thread and up on all the others
  EXPECT_GT(timesBeforeRebalance.at(0), timesAfterRebalance.at(0));

  double totalTimeAfter = timesAfterRebalance.at(0);
  for (size_t i = 1; i < kNumThreads; i++) {
    EXPECT_GT(timesAfterRebalance.at(i), timesBeforeRebalance.at(i));
    totalTimeAfter += timesAfterRebalance.at(i);
  }
  double ratioAfter = timesAfterRebalance.at(0) / totalTimeAfter / kNumThreads;
  EXPECT_LT(ratioAfter, ratioBefore);

  // Every context records the thread it was moved to, which is what the
  // workers check before running it.
  size_t numMoved = 0;
  for (size_t ctx = 0; ctx < kNumContexts; ctx++) {
    numMoved += contexts[ctx].threadIdx.load() != threadIdxBefore[ctx] ? 1 : 0;
  }
  EXPECT_GT(numMoved, 0);
  for (size_t thread = 0; thread < pdes.threadPool_.size(); thread++) {
    const auto lockedContexts = pdes.threadPool_[thread]->contexts.rlock();
    for (const auto* const context : *lockedContexts) {
      EXPECT_EQ(context->threadIdx.load(), thread) << context->nameWithPtr();
    }
  }
  constexpr uint64_t kSecondRunStartFs = 1'000'000;
  for (size_t ctx = 0; ctx < kNumContexts; ctx++) {
    pdes.scheduleAt(
        /*context=*/&contexts[ctx],
        /*timeFs=*/kSecondRunStartFs + ctx,
        []() {
          /* do nothing */
        });
  }
  EXPECT_NO_THROW(pdes.run());
}

TEST(PdesEngine, PdesEnginePauseAndResume) {
  // Arrange: 3 events at times 5, 15, 25 with lookahead=10.
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 1);

  SimulationContext context;
  std::vector<uint64_t> executedTimes;

  pdes.scheduleAt(&context, /* timeFs= */ 5, [&executedTimes]() {
    executedTimes.emplace_back(5);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 15, [&executedTimes]() {
    executedTimes.emplace_back(15);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 25, [&executedTimes]() {
    executedTimes.emplace_back(25);
  });

  // Act: Request pause before run, so engine pauses after first frame.
  pdes.requestPause();
  pdes.run();

  // Assert: Only the first frame's events should have executed.
  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{5}));
  EXPECT_TRUE(pdes.isNotRunning());

  // Act: Clear pause and run again to completion.
  pdes.run();

  // Assert: All events should have executed now.
  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{5, 15, 25}));
}

TEST(PdesEngine, PdesEngineBreakpointPause) {
  // Arrange: 3 events at times 5, 15, 25 with lookahead=10.
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 1);

  SimulationContext context;
  std::vector<uint64_t> executedTimes;

  pdes.scheduleAt(&context, /* timeFs= */ 5, [&executedTimes]() {
    executedTimes.emplace_back(5);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 15, [&executedTimes]() {
    executedTimes.emplace_back(15);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 25, [&executedTimes]() {
    executedTimes.emplace_back(25);
  });

  // Act: Set breakpoint at 15fs and run. The engine checks
  // lastEventTimeFs_ >= breakpointFs_ after each frame, so it will pause
  // after the frame where the event at time 15 executes.
  pdes.addBreakpointFs(15);
  pdes.run();

  // Assert: Events at 5 and 15 should execute, but not 25.
  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{5, 15}));
  EXPECT_TRUE(pdes.isNotRunning());

  // Act: Clear breakpoint and run to completion.
  pdes.clearAllBreakpoints();
  pdes.run();

  // Assert: All events should have executed.
  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{5, 15, 25}));
}

TEST(PdesEngine, PdesEngineRemoveBreakpoint) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 1);

  SimulationContext context;
  std::vector<uint64_t> executedTimes;

  pdes.scheduleAt(&context, /* timeFs= */ 5, [&executedTimes]() {
    executedTimes.emplace_back(5);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 15, [&executedTimes]() {
    executedTimes.emplace_back(15);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 25, [&executedTimes]() {
    executedTimes.emplace_back(25);
  });

  // Add breakpoint then remove it — engine should run to completion.
  pdes.addBreakpointFs(15);
  pdes.removeBreakpointFs(15);
  pdes.run();

  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{5, 15, 25}));
}

TEST(PdesEngine, PdesEngineClearPauseRequest) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 1);

  SimulationContext context;
  std::vector<uint64_t> executedTimes;

  pdes.scheduleAt(&context, /* timeFs= */ 5, [&executedTimes]() {
    executedTimes.emplace_back(5);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 15, [&executedTimes]() {
    executedTimes.emplace_back(15);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 25, [&executedTimes]() {
    executedTimes.emplace_back(25);
  });

  // Request pause then clear it — engine should run to completion.
  pdes.requestPause();
  pdes.clearPauseRequest();
  pdes.run();

  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{5, 15, 25}));
}

TEST(PdesEngine, PdesEngineGetBreakpoints) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 1);

  EXPECT_TRUE(pdes.getBreakpointsFs().empty());

  pdes.addBreakpointFs(30);
  pdes.addBreakpointFs(10);
  pdes.addBreakpointFs(20);

  const std::set<uint64_t> expected{10, 20, 30};
  EXPECT_EQ(pdes.getBreakpointsFs(), expected);

  pdes.removeBreakpointFs(20);
  const std::set<uint64_t> expectedAfterRemove{10, 30};
  EXPECT_EQ(pdes.getBreakpointsFs(), expectedAfterRemove);

  pdes.clearAllBreakpoints();
  EXPECT_TRUE(pdes.getBreakpointsFs().empty());
}

TEST(PdesEngine, PdesEngineMultiThreadedPause) {
  // Use 2 threads with enough contexts and events across multiple frames.
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);

  SimulationContext ctx1;
  SimulationContext ctx2;
  std::atomic<int> eventCount{0};

  // Frame [0, 10): events at 5 on ctx1 and 7 on ctx2
  pdes.scheduleAt(&ctx1, /* timeFs= */ 5, [&eventCount]() { eventCount++; });
  pdes.scheduleAt(&ctx2, /* timeFs= */ 7, [&eventCount]() { eventCount++; });
  // Frame [10, 20): events at 15 on ctx1 and 17 on ctx2
  pdes.scheduleAt(&ctx1, /* timeFs= */ 15, [&eventCount]() { eventCount++; });
  pdes.scheduleAt(&ctx2, /* timeFs= */ 17, [&eventCount]() { eventCount++; });

  // Request pause — should pause after first frame completes.
  pdes.requestPause();
  pdes.run();

  // First frame [0,10) executed both events (at 5 and 7); engine is paused.
  EXPECT_TRUE(pdes.isNotRunning());
  EXPECT_EQ(eventCount.load(), 2);

  // Continue to completion (the two events in frame [10,20)).
  pdes.run();
  EXPECT_EQ(eventCount.load(), 4);
}

TEST(PdesEngine, PdesEngineMultipleSequentialBreakpoints) {
  // Breakpoints trigger after a frame completes when lastEventTimeFs_ >=
  // breakpointFs. With lookahead=10, frame boundaries are 0, 10, 20, 30...
  // Set breakpoints at event times to trigger at the end of their frames.
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 1);

  SimulationContext context;
  std::vector<uint64_t> executedTimes;

  pdes.scheduleAt(&context, /* timeFs= */ 5, [&executedTimes]() {
    executedTimes.emplace_back(5);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 15, [&executedTimes]() {
    executedTimes.emplace_back(15);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 25, [&executedTimes]() {
    executedTimes.emplace_back(25);
  });

  // Breakpoint at 5: triggers after frame [0,10) when lastEventTimeFs_=5 >= 5.
  // Breakpoint at 15: triggers after frame [10,20) when lastEventTimeFs_=15
  // >= 15.
  pdes.addBreakpointFs(5);
  pdes.addBreakpointFs(15);
  pdes.run();

  // First breakpoint at 5 consumed after frame [0,10).
  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{5}));
  EXPECT_EQ(pdes.getBreakpointsFs(), std::set<uint64_t>{15});

  pdes.run();

  // Second breakpoint at 15 consumed after frame [10,20).
  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{5, 15}));
  EXPECT_TRUE(pdes.getBreakpointsFs().empty());

  pdes.run();

  // All events completed.
  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{5, 15, 25}));
}

TEST(PdesEngine, PdesEngineMultipleBreakpointsInOneFrame) {
  // Several breakpoints packed within a single frame must trigger exactly one
  // pause and all be consumed at once, rather than lingering and pausing on
  // later frames at times past the user's breakpoint values.
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 1);

  SimulationContext context;
  std::vector<uint64_t> executedTimes;

  pdes.scheduleAt(&context, /* timeFs= */ 8, [&executedTimes]() {
    executedTimes.emplace_back(8);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 25, [&executedTimes]() {
    executedTimes.emplace_back(25);
  });

  // Breakpoints at 3, 5, 7 all fall inside frame [0,10); after that frame
  // lastEventTimeFs_ = 8 is >= all three, so a single run() pauses once and
  // consumes all of them.
  pdes.addBreakpointFs(3);
  pdes.addBreakpointFs(5);
  pdes.addBreakpointFs(7);
  pdes.run();

  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{8}));
  EXPECT_TRUE(pdes.getBreakpointsFs().empty());
  EXPECT_TRUE(pdes.isNotRunning());

  // Resume: no breakpoints linger, so it runs to completion.
  pdes.run();
  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{8, 25}));
}

TEST(PdesEngine, PdesEnginePauseAndBreakpointInteraction) {
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 1);

  SimulationContext context;
  std::vector<uint64_t> executedTimes;

  pdes.scheduleAt(&context, /* timeFs= */ 5, [&executedTimes]() {
    executedTimes.emplace_back(5);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 15, [&executedTimes]() {
    executedTimes.emplace_back(15);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 25, [&executedTimes]() {
    executedTimes.emplace_back(25);
  });

  // Set both pause request AND a breakpoint. Pause request takes priority
  // and the breakpoint at 15 should NOT be consumed.
  pdes.requestPause();
  pdes.addBreakpointFs(15);
  pdes.run();

  // Paused after first frame from requestPause.
  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{5}));
  // Breakpoint should still be set (not consumed by requestPause).
  EXPECT_EQ(pdes.getBreakpointsFs(), std::set<uint64_t>{15});

  // Run again — breakpoint at 15 should trigger.
  pdes.run();

  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{5, 15}));
  EXPECT_TRUE(pdes.getBreakpointsFs().empty());

  // Run to completion.
  pdes.run();
  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{5, 15, 25}));
}

TEST(PdesEngine, PdesEnginePauseWithBoundedDuration) {
  // Test pause during run(durationFs) — verifies the wasPaused fix that
  // prevents lastEventTimeFs_ from being advanced to finishTimeFs.
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 1);

  SimulationContext context;
  std::vector<uint64_t> executedTimes;

  pdes.scheduleAt(&context, /* timeFs= */ 5, [&executedTimes]() {
    executedTimes.emplace_back(5);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 15, [&executedTimes]() {
    executedTimes.emplace_back(15);
  });
  pdes.scheduleAt(&context, /* timeFs= */ 25, [&executedTimes]() {
    executedTimes.emplace_back(25);
  });

  // Breakpoint at 5 triggers after frame [0,10) when lastEventTimeFs_=5 >= 5.
  pdes.addBreakpointFs(5);
  pdes.run(/* durationFs= */ 30);

  // Should pause at breakpoint, NOT advance lastEventTimeFs_ to 30.
  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{5}));
  EXPECT_TRUE(pdes.isNotRunning());

  // Resume with remaining duration — engine should complete remaining events.
  pdes.run(/* durationFs= */ 25);

  EXPECT_EQ(executedTimes, (std::vector<uint64_t>{5, 15, 25}));
}

TEST(PdesEngine, PrintExitStatsReportsEveryWorkerThread) {
  const gflags::FlagSaver flagSaver;
  ASSERT_FALSE(
      gflags::SetCommandLineOption("pdes_print_cpu_load", "true").empty());
  auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
  SimulationContext context;
  pdes.scheduleAt(&context, /* timeFs= */ 5, []() {});
  pdes.run();

  testing::internal::CaptureStdout();
  pdes.printExitStats();
  const std::string output = testing::internal::GetCapturedStdout();

  EXPECT_THAT(output, HasSubstr("CPU Time for thread 0 contexts: "));
  EXPECT_THAT(output, HasSubstr("CPU Time for thread 1 contexts: "));
}

// The stats read every registered context, and owners commonly destroy their
// contexts before the engine.
TEST(PdesEngine, DestructorDoesNotPrintExitStats) {
  const gflags::FlagSaver flagSaver;
  ASSERT_FALSE(
      gflags::SetCommandLineOption("pdes_print_cpu_load", "true").empty());
  auto pdes = std::make_unique<PdesEngine>(
      /* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
  auto context = std::make_unique<SimulationContext>();
  pdes->scheduleAt(context.get(), /* timeFs= */ 5, []() {});
  pdes->run();
  context.reset();

  testing::internal::CaptureStdout();
  pdes.reset();
  EXPECT_EQ(testing::internal::GetCapturedStdout(), "");
}

} // namespace packsim::pdes
