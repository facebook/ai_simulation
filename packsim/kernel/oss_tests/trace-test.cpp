/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <stdexcept>
#include <string>

#include <folly/container/F14Map.h>
#include <folly/json/dynamic.h>
#include <folly/json/json.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/kernel/trace.h"

namespace packsim {
namespace {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

// Tracer is reached through SimContext::tracer, which is how every production
// caller uses it, so the fixture builds a real Scheduler + SimContext rather
// than constructing a Tracer standalone.
class TracerTest : public ::testing::Test {
 protected:
  Scheduler scheduler_;
  SimContext ctx_{&scheduler_, "tracer-test"};

  Tracer& tracer() {
    return ctx_.tracer;
  }
};

TEST_F(TracerTest, TraceStartsEmpty) {
  EXPECT_TRUE(tracer().traces().empty());
}

TEST_F(TracerTest, TraceRecordsValueUnderItsName) {
  tracer().trace("qwnd", 1.5);

  const auto& traces = tracer().traces();
  ASSERT_EQ(1u, traces.size());
  ASSERT_TRUE(traces.contains("qwnd"));
  ASSERT_EQ(1u, traces.at("qwnd").size());
  EXPECT_DOUBLE_EQ(1.5, traces.at("qwnd").front().value);
}

TEST_F(TracerTest, RepeatedTracesAccumulateInOrder) {
  tracer().trace("bw", 10.0);
  tracer().trace("bw", 20.0);
  tracer().trace("bw", 30.0);

  std::vector<double> values;
  for (const auto& sample : tracer().traces().at("bw")) {
    values.emplace_back(sample.value);
  }

  const std::vector<double> expected{10.0, 20.0, 30.0};
  EXPECT_EQ(expected, values);
}

TEST_F(TracerTest, DistinctNamesGetSeparateSeries) {
  tracer().trace("a", 1.0);
  tracer().trace("b", 2.0);
  tracer().trace("a", 3.0);

  const auto& traces = tracer().traces();
  ASSERT_EQ(2u, traces.size());
  EXPECT_EQ(2u, traces.at("a").size());
  EXPECT_EQ(1u, traces.at("b").size());
}

// TraceValue::timeNs is stamped from the simulation clock, not wall clock, so
// a sample taken from a scheduled event carries that event's simulated time.
TEST_F(TracerTest, TraceStampsSimulationTimeNotWallClock) {
  constexpr uint64_t kEventTimeNs = 1000;
  ctx_.schedule(
      Time::fromNs(kEventTimeNs), [this]() { ctx_.tracer.trace("late", 7.0); });

  tracer().trace("early", 1.0);
  ASSERT_EQ(0u, tracer().traces().at("early").front().timeNs);

  scheduler_.run();

  EXPECT_EQ(kEventTimeNs, tracer().traces().at("late").front().timeNs);
}

TEST_F(TracerTest, SnapToIntervalRejectsAnIntervalUnderOneNanosecond) {
  EXPECT_THAT(
      [this] {
        tracer().startLogPeriodicSnapToInterval(
            "series", Time::fromPs(999), [] { return 1.0; });
      },
      ThrowsMessage<std::invalid_argument>(HasSubstr("at least 1 ns")));
}

TEST_F(TracerTest, SnapshotIsEmptyWithoutRegistrations) {
  EXPECT_TRUE(tracer().getSnapshot().empty());
}

TEST_F(TracerTest, SnapshotEvaluatesRegisteredFunctionsOnEachCall) {
  double backing = 4.0;
  tracer().registerSnapshot("depth", [&backing]() { return backing; });

  EXPECT_DOUBLE_EQ(4.0, tracer().getSnapshot().at("depth"));

  backing = 9.0;
  EXPECT_DOUBLE_EQ(9.0, tracer().getSnapshot().at("depth"));
}

TEST_F(TracerTest, RegisteringSameSnapshotNameReplacesPrevious) {
  tracer().registerSnapshot("v", []() { return 1.0; });
  tracer().registerSnapshot("v", []() { return 2.0; });

  const auto snap = tracer().getSnapshot();
  ASSERT_EQ(1u, snap.size());
  EXPECT_DOUBLE_EQ(2.0, snap.at("v"));
}

TEST_F(TracerTest, StartEventPopulatesIdentifyingFields) {
  auto* event = tracer().startEvent("send", "transport", /*pid=*/3, /*tid=*/7);

  ASSERT_NE(nullptr, event);
  EXPECT_EQ("send", event->name);
  EXPECT_EQ("transport", event->category);
  EXPECT_EQ(3u, event->pid);
  EXPECT_EQ(7u, event->tid);
}

// A fresh event is open-ended; end() closes it exactly once.
TEST_F(TracerTest, EndClosesAnOpenEvent) {
  auto* event = tracer().startEvent("send", "transport", 0, 0);
  ASSERT_EQ(UINT64_MAX, event->endTimeUs);

  event->end(Time::fromUs(5));

  EXPECT_EQ(5u, event->endTimeUs);
}

TEST_F(TracerTest, EndingAnAlreadyEndedEventThrows) {
  auto* event = tracer().startEvent("send", "transport", 0, 0);
  event->end(Time::fromUs(5));

  EXPECT_THAT(
      [&event]() { event->end(Time::fromUs(6)); },
      ThrowsMessage<std::runtime_error>(HasSubstr("already ended")));
}

// The (pid, tid) pair models one serial lane, so a second event may not open
// on that lane while the previous one is still running.
TEST_F(TracerTest, StartEventThrowsWhilePriorEventOnSameLaneIsOpen) {
  tracer().startEvent("first", "transport", /*pid=*/1, /*tid=*/2);

  EXPECT_THAT(
      [this]() { tracer().startEvent("second", "transport", 1, 2); },
      ThrowsMessage<std::runtime_error>(HasSubstr("is not yet complete")));
}

TEST_F(TracerTest, StartEventSucceedsOnSameLaneAfterPriorEventEnds) {
  auto* first = tracer().startEvent("first", "transport", 1, 2);
  first->end(Time::fromUs(1));

  EXPECT_NO_THROW(tracer().startEvent("second", "transport", 1, 2));
}

TEST_F(TracerTest, StartEventSucceedsOnADifferentLane) {
  tracer().startEvent("first", "transport", /*pid=*/1, /*tid=*/2);

  EXPECT_NO_THROW(
      tracer().startEvent("other", "transport", /*pid=*/1, /*tid=*/3));
}

// getTraceEvents() emits the Chrome trace duration pair: a "B" record carrying
// the args and a matching "E" record.
TEST_F(TracerTest, TraceEventsEmitBeginAndEndRecords) {
  auto* event = tracer().startEvent(
      "send",
      "transport",
      /*pid=*/1,
      /*tid=*/2,
      folly::dynamic::object("k", 5));
  event->end(Time::fromUs(9));

  const folly::dynamic events = tracer().getTraceEvents();

  ASSERT_TRUE(events.isArray());
  ASSERT_EQ(2u, events.size());

  const auto& begin = events[0];
  EXPECT_EQ("send", begin["name"].asString());
  EXPECT_EQ("transport", begin["cat"].asString());
  EXPECT_EQ("B", begin["ph"].asString());
  EXPECT_EQ(1u, begin["pid"].asInt());
  EXPECT_EQ(2u, begin["tid"].asInt());
  EXPECT_EQ(5, begin["args"]["k"].asInt());

  const auto& end = events[1];
  EXPECT_EQ("send", end["name"].asString());
  EXPECT_EQ("E", end["ph"].asString());
  EXPECT_EQ(9u, end["ts"].asInt());
  // Args ride only on the begin record.
  EXPECT_FALSE(end.contains("args"));
}

// Sorting the keys makes the expected text independent of folly::dynamic's
// hash-map iteration order.
TEST_F(TracerTest, TraceEventsSerializeACompletedEventAsBeginAndEnd) {
  auto* event = tracer().startEvent(
      "send",
      "transport",
      /*pid=*/1,
      /*tid=*/2,
      folly::dynamic::object("k", 5));
  event->end(Time::fromUs(9));

  folly::json::serialization_opts opts;
  opts.sort_keys = true;

  EXPECT_EQ(
      R"([{"args":{"k":5},"cat":"transport","name":"send","ph":"B","pid":1,"tid":2,"ts":0},)"
      R"({"cat":"transport","name":"send","ph":"E","pid":1,"tid":2,"ts":9}])",
      folly::json::serialize(tracer().getTraceEvents(), opts));
}

// Timestamps are truncated to whole microseconds, so a phase shorter than 1us
// can start and end on the same "ts". Its records must still be a "B"
// followed by an "E".
TEST_F(TracerTest, TraceEventsEndAPhaseThatStartsAndEndsInTheSameMicrosecond) {
  constexpr uint64_t kStartNs = 5200;
  constexpr uint64_t kEndNs = 5700;
  constexpr int64_t kTsUs = 5;

  Tracer::TraceEvent* event = nullptr;
  ctx_.schedule(Time::fromNs(kStartNs), [this, &event]() {
    event = ctx_.tracer.startEvent(
        "send",
        "transport",
        /*pid=*/1,
        /*tid=*/2,
        folly::dynamic::object("k", 5));
  });
  scheduler_.run();
  ASSERT_NE(nullptr, event);
  event->end(Time::fromNs(kEndNs));

  const folly::dynamic events = tracer().getTraceEvents();

  ASSERT_EQ(2u, events.size());
  EXPECT_EQ("B", events[0]["ph"].asString());
  EXPECT_EQ(kTsUs, events[0]["ts"].asInt());
  EXPECT_EQ(5, events[0]["args"]["k"].asInt());
  EXPECT_EQ("E", events[1]["ph"].asString());
  EXPECT_EQ(kTsUs, events[1]["ts"].asInt());
  EXPECT_FALSE(events[1].contains("args"));
}

// An event that has not ended has no end time, so only its "B" record is
// emitted and the event stays open. Events on other lanes still get both
// records.
TEST_F(TracerTest, TraceEventsLeaveANeverEndedEventOpen) {
  tracer().startEvent("open", "transport", /*pid=*/1, /*tid=*/2);
  auto* done = tracer().startEvent("done", "transport", /*pid=*/1, /*tid=*/3);
  done->end(Time::fromUs(4));

  const folly::dynamic events = tracer().getTraceEvents();

  ASSERT_EQ(3u, events.size());
  EXPECT_EQ("open", events[0]["name"].asString());
  EXPECT_EQ("B", events[0]["ph"].asString());
  EXPECT_EQ(0, events[0]["ts"].asInt());
  EXPECT_EQ("done", events[1]["name"].asString());
  EXPECT_EQ("B", events[1]["ph"].asString());
  EXPECT_EQ("done", events[2]["name"].asString());
  EXPECT_EQ("E", events[2]["ph"].asString());
  EXPECT_EQ(4, events[2]["ts"].asInt());
}

// startTimeUs and endTimeUs both feed the "ts" field of the emitted Chrome
// trace records, and that field is defined in microseconds. Advancing the
// clock to a time where nanoseconds and microseconds differ is what makes this
// assertion meaningful - at t=0 the two units coincide and a unit bug is
// invisible.
TEST_F(TracerTest, TraceEventTimestampsAreBothInMicroseconds) {
  constexpr uint64_t kStartNs = 5000;
  constexpr uint64_t kStartUs = 5;

  Tracer::TraceEvent* event = nullptr;
  ctx_.schedule(Time::fromNs(kStartNs), [this, &event]() {
    event = ctx_.tracer.startEvent("send", "transport", 0, 0);
  });
  scheduler_.run();

  ASSERT_NE(nullptr, event);
  EXPECT_EQ(kStartUs, event->startTimeUs);

  event->end(Time::fromUs(9));
  EXPECT_EQ(9u, event->endTimeUs);
  EXPECT_LT(event->startTimeUs, event->endTimeUs);
}

TEST_F(TracerTest, TraceEventsIsEmptyWithoutEvents) {
  const folly::dynamic events = tracer().getTraceEvents();

  ASSERT_TRUE(events.isArray());
  EXPECT_EQ(0u, events.size());
}

// logPeriodicConditional needs the category->interval map that SimContext only
// supplies through its four-argument constructor. This fixture's context was
// built without one, so the call must fail loudly rather than silently drop
// the trace registration.
TEST_F(TracerTest, ConditionalTraceWithoutMapThrows) {
  EXPECT_THAT(
      [this]() {
        tracer().logPeriodicConditional("cat", "name", []() { return 1.0; });
      },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("no conditional map was provided")));
}

TEST_F(TracerTest, TryCancelPeriodicIgnoresUnknownName) {
  EXPECT_NO_THROW(tracer().tryCancelPeriodic("never-registered"));
}

// cancelPeriodic() uses .at(), so unlike tryCancelPeriodic() it is the
// caller's job to know the trace exists. Only the type is asserted here: the
// message comes from the standard library's map::at, not from packsim, so
// pinning its text would couple this test to the libstdc++ implementation.
TEST_F(TracerTest, CancelPeriodicThrowsOnUnknownName) {
  EXPECT_THROW(tracer().cancelPeriodic("never-registered"), std::out_of_range);
}

// Conditional traces need a SimContext built with a category->interval map,
// which TracerTest's context deliberately lacks. These cases therefore get
// their own fixture rather than reusing TracerTest and ignoring its members.
class ConditionalTracerTest : public ::testing::Test {
 protected:
  // Tracer::logPeriodicConditional() reads the map value as nanoseconds
  // (trace.cpp: `Time::fromNs(it->second)`), so this is 1us expressed in ns.
  static constexpr uint64_t kConditionalIntervalNs = 1000;

  const Tracer::ConditionalMap conditionalMap_{{"cc", kConditionalIntervalNs}};
  Scheduler scheduler_;
  SimContext ctx_{&scheduler_, "conditional", /*seed=*/0, &conditionalMap_};

  Tracer& tracer() {
    return ctx_.tracer;
  }
};

TEST_F(ConditionalTracerTest, RegistersWhenCategoryIsMapped) {
  tracer().logPeriodicConditional("cc", "qwnd", []() { return 2.0; });

  // Snapping to the interval from t=0 logs immediately.
  ASSERT_TRUE(tracer().traces().contains("qwnd"));
  EXPECT_DOUBLE_EQ(2.0, tracer().traces().at("qwnd").front().value);
  EXPECT_NO_THROW(tracer().cancelPeriodic("qwnd"));
}

TEST_F(ConditionalTracerTest, DropsUnmappedCategory) {
  tracer().logPeriodicConditional("absent", "qwnd", []() { return 2.0; });

  EXPECT_TRUE(tracer().traces().empty());
}

TEST(ConditionalTracerConstructionTest, ZeroIntervalIsRejected) {
  const Tracer::ConditionalMap conditionalMap{{"cc", 0}};
  Scheduler scheduler;

  EXPECT_THAT(
      [&] {
        SimContext ctx(
            &scheduler, "zero-interval", /*seed=*/0, &conditionalMap);
      },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Simulation.conditionalTraceIntervalsNs['cc'] is 0")));
}

} // namespace
} // namespace packsim
