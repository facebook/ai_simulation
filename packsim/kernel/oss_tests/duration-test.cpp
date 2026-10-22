/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <stdexcept>
#include <type_traits>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/duration.h"
#include "packsim/kernel/time.h"

namespace packsim {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

// A point and a span never convert into each other, so mixing them does not
// compile.
static_assert(!std::is_convertible_v<Time, Duration>);
static_assert(!std::is_convertible_v<Duration, Time>);
static_assert(!std::is_constructible_v<Duration, Time>);
static_assert(!std::is_constructible_v<Time, Duration>);
static_assert(!std::is_convertible_v<uint64_t, Duration>);

// Passed by value like the uint64_t it wraps.
static_assert(std::is_trivially_copyable_v<Duration>);
static_assert(sizeof(Duration) == sizeof(uint64_t));

// Arithmetic between durations works in constant expressions.
static_assert(
    Duration::fromNs(1) + Duration::fromPs(500) == Duration(1'500'000));
static_assert(
    Duration::fromUs(1) - Duration::fromNs(1) == Duration::fromNs(999));
static_assert(Duration::fromPs(999) < Duration::fromNs(1));

TEST(DurationTest, DefaultIsZero) {
  EXPECT_TRUE(Duration().isZero());
  EXPECT_EQ(Duration(), Duration(0));
}

TEST(DurationTest, NonZeroLengthIsNotZero) {
  EXPECT_FALSE(Duration(1).isZero());
}

TEST(DurationTest, NamedConstructorsScaleToFemtoseconds) {
  EXPECT_EQ(Duration::fromPs(1).getFs(), 1'000u);
  EXPECT_EQ(Duration::fromNs(1).getFs(), 1'000'000u);
  EXPECT_EQ(Duration::fromUs(1).getFs(), 1'000'000'000u);
  EXPECT_EQ(Duration::fromMs(1).getFs(), 1'000'000'000'000u);
  EXPECT_EQ(Duration::fromSec(1).getFs(), 1'000'000'000'000'000u);
}

TEST(DurationTest, GettersTruncateToCoarserUnit) {
  // Just under 2 us, so rounding would give the next unit at every scale.
  const Duration d(1'999'999'999);
  EXPECT_EQ(d.getPs(), 1'999'999u);
  EXPECT_EQ(d.getNs(), 1'999u);
  EXPECT_EQ(d.getUs(), 1u);
}

TEST(DurationTest, Add) {
  EXPECT_EQ(Duration::fromNs(5) + Duration::fromNs(10), Duration::fromNs(15));
}

TEST(DurationTest, PlusAssign) {
  Duration d = Duration::fromNs(10);
  d += Duration::fromNs(5);
  EXPECT_EQ(d, Duration::fromNs(15));
}

TEST(DurationTest, Subtract) {
  EXPECT_EQ(Duration::fromNs(11) - Duration::fromNs(5), Duration::fromNs(6));
}

TEST(DurationTest, SubtractEqualLengthsIsZero) {
  EXPECT_EQ(Duration::fromNs(5) - Duration::fromNs(5), Duration());
}

TEST(DurationTest, MinusAssign) {
  Duration d = Duration::fromNs(10);
  d -= Duration::fromNs(3);
  EXPECT_EQ(d, Duration::fromNs(7));
}

TEST(DurationTest, SubtractUnderflowThrows) {
  EXPECT_THAT(
      [] { Duration::fromNs(5) - Duration::fromNs(10); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Duration difference is negative")));
}

TEST(DurationTest, MinusAssignUnderflowThrowsAndKeepsValue) {
  Duration d = Duration::fromNs(5);
  EXPECT_THAT(
      [&d] { d -= Duration::fromNs(6); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Duration difference is negative")));
  EXPECT_EQ(d, Duration::fromNs(5));
}

TEST(DurationTest, OrdersByLength) {
  EXPECT_LT(Duration::fromPs(999), Duration::fromNs(1));
  EXPECT_LE(Duration::fromNs(1), Duration::fromPs(1'000));
  EXPECT_GT(Duration::fromNs(2), Duration::fromNs(1));
  EXPECT_NE(Duration::fromNs(1), Duration::fromNs(2));
}

TEST(DurationTest, TimePlusDurationIsLaterTime) {
  EXPECT_EQ(Time::fromNs(10) + Duration::fromNs(5), Time::fromNs(15));
}

TEST(DurationTest, TimeMinusDurationIsEarlierTime) {
  EXPECT_EQ(Time::fromNs(10) - Duration::fromNs(4), Time::fromNs(6));
}

TEST(DurationTest, TimeMinusLongerDurationThrows) {
  EXPECT_THAT(
      [] { Time::fromNs(5) - Duration::fromNs(6); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Time difference is negative")));
}

TEST(DurationTest, BetweenIsTheSpanFromStartToEnd) {
  EXPECT_EQ(
      Duration::between(Time::fromNs(5), Time::fromNs(12)),
      Duration::fromNs(7));
}

TEST(DurationTest, BetweenThrowsWhenEndIsBeforeStart) {
  EXPECT_THAT(
      [] { Duration::between(Time::fromNs(12), Time::fromNs(5)); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Time difference is negative")));
}

} // namespace packsim
