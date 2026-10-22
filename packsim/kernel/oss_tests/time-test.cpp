/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <stdexcept>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/time.h"

namespace packsim {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

TEST(PackSimTimeTests, AddTime) {
  Time t1(5);
  Time t2(10);
  Time t3 = t1 + t2;
  EXPECT_EQ(t3.getFs(), 15);
}

TEST(PackSimTimeTests, SubtractTime) {
  Time t1(5);
  Time t2(11);
  Time t3 = t2 - t1;
  EXPECT_EQ(t3.getFs(), 6);
}

TEST(PackSimTimeTests, ScaleTime) {
  Time t1(5);
  Time t2 = t1 * 3U;
  EXPECT_EQ(t2.getFs(), 15);
}

TEST(PackSimTimeTests, SubtractUnderflowThrows) {
  EXPECT_THAT(
      [] { Time(5) - Time(10); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Time difference is negative")));
}

TEST(PackSimTimeTests, PlusAssign) {
  Time t(10);
  t += Time(5);
  EXPECT_EQ(t.getFs(), 15);
}

TEST(PackSimTimeTests, MinusAssign) {
  Time t(10);
  t -= Time(3);
  EXPECT_EQ(t.getFs(), 7);
}

TEST(PackSimTimeTests, MinusAssignUnderflowThrows) {
  EXPECT_THAT(
      [] {
        Time t(5);
        t -= Time(6);
      },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Time difference is negative")));
}

TEST(PackSimTimeTests, ScalarMultiplyIsCommutative) {
  EXPECT_EQ((Time(5) * 3U).getFs(), 15);
  EXPECT_EQ((3U * Time(5)).getFs(), 15);
}

TEST(PackSimTimeTests, DivideByScalar) {
  EXPECT_EQ((Time(15) / 3U).getFs(), 5);
}

TEST(PackSimTimeTests, DivideByTimeYieldsRatio) {
  EXPECT_DOUBLE_EQ(Time(15) / Time(5), 3.0);
}

TEST(PackSimTimeTests, LessThan) {
  EXPECT_TRUE(Time(5) < Time(10));
  EXPECT_FALSE(Time(10) < Time(5));
}

TEST(PackSimTimeTests, GreaterThan) {
  EXPECT_TRUE(Time(10) > Time(5));
  EXPECT_FALSE(Time(5) > Time(10));
}

TEST(PackSimTimeTests, LessThanOrEqual) {
  EXPECT_TRUE(Time(5) <= Time(5));
  EXPECT_TRUE(Time(5) <= Time(6));
  EXPECT_FALSE(Time(6) <= Time(5));
}

TEST(PackSimTimeTests, GreaterThanOrEqual) {
  EXPECT_TRUE(Time(5) >= Time(5));
  EXPECT_TRUE(Time(6) >= Time(5));
  EXPECT_FALSE(Time(5) >= Time(6));
}

TEST(PackSimTimeTests, Equality) {
  EXPECT_TRUE(Time(5) == Time(5));
  EXPECT_FALSE(Time(5) == Time(6));
}

TEST(PackSimTimeTests, IsZero) {
  EXPECT_TRUE(Time().isZero());
  EXPECT_TRUE(Time(0).isZero());
  EXPECT_FALSE(Time(1).isZero());
}

TEST(PackSimTimeTests, NamedConstructorsScaleToFemtoseconds) {
  EXPECT_EQ(Time::fromPs(1).getFs(), 1'000u);
  EXPECT_EQ(Time::fromNs(1).getFs(), 1'000'000u);
  EXPECT_EQ(Time::fromUs(1).getFs(), 1'000'000'000u);
  EXPECT_EQ(Time::fromMs(1).getFs(), 1'000'000'000'000u);
  EXPECT_EQ(Time::fromSec(1).getFs(), 1'000'000'000'000'000u);
}

TEST(PackSimTimeTests, GettersTruncateToCoarserUnit) {
  const Time t = Time::fromNs(1) + Time::fromPs(500); // 1.5 ns
  EXPECT_EQ(t.getNs(), 1u);
  EXPECT_EQ(t.getPs(), 1'500u);
}

} // namespace packsim
