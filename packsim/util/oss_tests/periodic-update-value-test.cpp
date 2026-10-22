/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <stdexcept>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/util/periodic-update-value.h"

namespace packsim {
namespace {

// Minimal concrete PeriodicUpdateValue for exercising the base refill loop:
// each update bumps the value by one.
class CountingPeriodicValue : public PeriodicUpdateValue<int64_t> {
 public:
  using PeriodicUpdateValue<int64_t>::PeriodicUpdateValue;

 protected:
  void updateValueOnce() override {
    value_ += 1;
  }
};

// Rate + period chosen so each period adds exactly 100 tokens:
// bitRate * periodNs / (8 bits/byte * 1e9 ns/s) = 8e8 * 1000 / 8e9 = 100.
constexpr uint64_t kBitRateBps = 800'000'000;

// Owns the scheduler/context shared by every case. run() is only exercised by
// the cases that schedule an observation event; the read-at-t=0 cases compute
// lazily on read and never call run(), so the lookahead set here is harmless
// for them.
class PeriodicUpdateValueTest : public ::testing::Test {
 protected:
  void SetUp() override {
    sched_.setLookaheadTimeFs(Time::fromNs(1000).getFs());
  }

  Scheduler sched_;
  SimContext simCtx_{&sched_, /*seed=*/0};
};

// Adds a device id and a bucket factory so each case declares only the
// parameters it actually varies (initial balance, max, negative-token policy).
class TokenBucketWithCbTest : public PeriodicUpdateValueTest {
 protected:
  TokenBucketWithCb
  makeBucket(double initialValue, double maxValue, bool allowNegativeTokens) {
    return TokenBucketWithCb(
        &simCtx_,
        initialValue,
        DataRate(kBitRateBps),
        maxValue,
        /*period=*/Time::fromNs(1000),
        /*startTime=*/Time(0),
        id_,
        allowNegativeTokens);
  }

  FlexDeviceId id_;
};

// ---- PeriodicUpdateValue base ----

TEST_F(PeriodicUpdateValueTest, ReturnsInitialValueBeforeFirstPeriodElapses) {
  CountingPeriodicValue value(
      &simCtx_,
      /*initialValue=*/5,
      /*period=*/Time::fromNs(100),
      /*startTime=*/Time(0));
  // now (0) < startTime + period, so no update fires.
  EXPECT_EQ(value(), 5);
}

TEST_F(PeriodicUpdateValueTest, UpdatesOncePerElapsedPeriod) {
  CountingPeriodicValue value(
      &simCtx_,
      /*initialValue=*/0,
      /*period=*/Time::fromNs(100),
      /*startTime=*/Time(0));
  int64_t observed = -1;
  bool ran = false;
  sched_.scheduleAfter(&simCtx_, Time::fromNs(250), [&]() {
    observed = value();
    ran = true;
  });
  sched_.run();
  ASSERT_TRUE(ran);
  // 250ns / 100ns period = 2 elapsed periods.
  EXPECT_EQ(observed, 2);
}

TEST_F(PeriodicUpdateValueTest, SetValueOverridesCurrentValue) {
  CountingPeriodicValue value(
      &simCtx_,
      /*initialValue=*/0,
      /*period=*/Time::fromNs(100),
      /*startTime=*/Time(0));
  value.setValue(42);
  EXPECT_EQ(value(), 42);
}

TEST_F(PeriodicUpdateValueTest, ZeroPeriodUpdatesOnEveryRead) {
  CountingPeriodicValue value(
      &simCtx_, /*initialValue=*/0, /*period=*/Time(0), /*startTime=*/Time(0));
  // With a zero period the early-return guard (now < now) is false, so each
  // read performs exactly one update.
  EXPECT_EQ(value(), 1);
  EXPECT_EQ(value(), 2);
}

// ---- TokenBucketWithCb ----

TEST_F(TokenBucketWithCbTest, AddIncreasesBalance) {
  auto bucket = makeBucket(
      /*initialValue=*/50.0, /*maxValue=*/1e9, /*allowNegativeTokens=*/false);
  bucket += 25.0;
  // Read at t=0: before one period elapses, no refill occurs.
  EXPECT_DOUBLE_EQ(bucket(), 75.0);
}

TEST_F(
    TokenBucketWithCbTest,
    SubtractThrowsWhenBalanceGoesNegativeAndDisallowed) {
  auto bucket = makeBucket(
      /*initialValue=*/50.0, /*maxValue=*/1e9, /*allowNegativeTokens=*/false);
  EXPECT_THAT(
      [&] { bucket -= 100.0; },
      ::testing::ThrowsMessage<std::runtime_error>(
          ::testing::HasSubstr("Not enough tokens")));
}

TEST_F(TokenBucketWithCbTest, SubtractAllowsNegativeBalanceWhenConfigured) {
  auto bucket = makeBucket(
      /*initialValue=*/50.0, /*maxValue=*/1e9, /*allowNegativeTokens=*/true);
  bucket -= 100.0;
  EXPECT_DOUBLE_EQ(bucket(), -50.0);
}

TEST_F(TokenBucketWithCbTest, GetRateReturnsConstructedRate) {
  auto bucket = makeBucket(
      /*initialValue=*/0.0, /*maxValue=*/1e9, /*allowNegativeTokens=*/false);
  EXPECT_EQ(bucket.getRate(), DataRate(kBitRateBps));
}

TEST_F(TokenBucketWithCbTest, SetRateUpdatesGetRate) {
  auto bucket = makeBucket(
      /*initialValue=*/0.0, /*maxValue=*/1e9, /*allowNegativeTokens=*/false);
  bucket.setRate(DataRate(kBitRateBps / 2));
  EXPECT_EQ(bucket.getRate(), DataRate(kBitRateBps / 2));
}

TEST_F(TokenBucketWithCbTest, RefillAccumulatesTokensOverElapsedPeriods) {
  auto bucket = makeBucket(
      /*initialValue=*/0.0, /*maxValue=*/1e9, /*allowNegativeTokens=*/false);
  double observed = -1.0;
  bool ran = false;
  sched_.scheduleAfter(&simCtx_, Time::fromNs(3000), [&]() {
    observed = bucket();
    ran = true;
  });
  sched_.run();
  ASSERT_TRUE(ran);
  // 3 periods * 100 tokens/period.
  EXPECT_DOUBLE_EQ(observed, 300.0);
}

TEST_F(TokenBucketWithCbTest, RefillClampsBalanceAtMaxValue) {
  auto bucket = makeBucket(
      /*initialValue=*/0.0, /*maxValue=*/250.0, /*allowNegativeTokens=*/false);
  double observed = -1.0;
  bool ran = false;
  sched_.scheduleAfter(&simCtx_, Time::fromNs(3000), [&]() {
    observed = bucket();
    ran = true;
  });
  sched_.run();
  ASSERT_TRUE(ran);
  // Would reach 300 but is capped at maxValue.
  EXPECT_DOUBLE_EQ(observed, 250.0);
}

TEST_F(TokenBucketWithCbTest, RefillCarriesSubPeriodRemainderAcrossReads) {
  auto bucket = makeBucket(
      /*initialValue=*/0.0, /*maxValue=*/1e9, /*allowNegativeTokens=*/false);
  double first = -1.0;
  double second = -1.0;
  bool ranFirst = false;
  bool ranSecond = false;
  // At 2500ns: 2 full periods -> 200; lastUpdate advances to 2000ns, carrying
  // the 500ns remainder (not to 2500ns).
  sched_.scheduleAfter(&simCtx_, Time::fromNs(2500), [&]() {
    first = bucket();
    ranFirst = true;
  });
  // At 3100ns: (3100-2000)/1000 = 1 more period -> 300. A "lastUpdate = now"
  // regression would instead read (3100-2500)/1000 = 0 and stay at 200.
  sched_.scheduleAfter(&simCtx_, Time::fromNs(3100), [&]() {
    second = bucket();
    ranSecond = true;
  });
  sched_.run();
  ASSERT_TRUE(ranFirst);
  ASSERT_TRUE(ranSecond);
  EXPECT_DOUBLE_EQ(first, 200.0);
  EXPECT_DOUBLE_EQ(second, 300.0);
}

TEST_F(TokenBucketWithCbTest, SetRateChangesRefillRate) {
  auto bucket = makeBucket(
      /*initialValue=*/0.0, /*maxValue=*/1e9, /*allowNegativeTokens=*/false);
  bucket.setRate(DataRate(kBitRateBps / 2)); // 50 tokens/period
  double observed = -1.0;
  bool ran = false;
  sched_.scheduleAfter(&simCtx_, Time::fromNs(3000), [&]() {
    observed = bucket();
    ran = true;
  });
  sched_.run();
  ASSERT_TRUE(ran);
  // 3 periods * 50 tokens/period.
  EXPECT_DOUBLE_EQ(observed, 150.0);
}

TEST_F(TokenBucketWithCbTest, FiresCallbackWhenRefillReachesTriggerLevel) {
  auto bucket = makeBucket(
      /*initialValue=*/0.0, /*maxValue=*/1e9, /*allowNegativeTokens=*/false);
  bool fired = false;
  bucket.setTriggerLevel(250.0);
  bucket.setOnBytesAvailableCallback(
      [&](const FlexDeviceId&) { fired = true; });
  sched_.run();
  // The bucket schedules its callback for when the balance would reach the
  // trigger level: ceil(250 / 100) = 3 periods -> 3000ns.
  EXPECT_TRUE(fired);
}

} // namespace
} // namespace packsim
