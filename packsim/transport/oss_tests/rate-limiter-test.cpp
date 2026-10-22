/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <memory>

#include <gtest/gtest.h>

#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/device-id.h"
#include "packsim/transport/rate-limiter.h"

namespace packsim {

class RateLimiterTest : public ::testing::TestWithParam<double> {
 public:
  void SetUp() override {
    rateLimiter_ = std::make_unique<TokenBucketRateLimiter>(
        &simCtx_,
        65536,
        DataRate("50Mbps"),
        kRefreshPeriodNs,
        0,
        true,
        FlexDeviceId({1, 0, 0, 20}));
  }

  void checkExpectedToken(double expectedToken) {
    EXPECT_DOUBLE_EQ(expectedToken, rateLimiter_->getMaxBytesAllowed());
  }

 protected:
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  std::unique_ptr<TokenBucketRateLimiter> rateLimiter_;
  static constexpr uint64_t kRefreshPeriodNs{1000};
};

TEST_P(RateLimiterTest, TokenBucketTestRefreshToken) {
  EXPECT_EQ(rateLimiter_->getRateLimiterType(), RateLimiterType::TOKEN_BUCKET);
  checkExpectedToken(0);
  scheduler_.scheduleAfter(
      &simCtx_,
      Time::fromNs(kRefreshPeriodNs),
      &RateLimiterTest::checkExpectedToken,
      this,
      (6.25));
  scheduler_.run(Time::fromUs(3.0));
  checkExpectedToken(18.75);
}

TEST_P(RateLimiterTest, TokenBucketTestConsumeToken) {
  EXPECT_EQ(rateLimiter_->getRateLimiterType(), RateLimiterType::TOKEN_BUCKET);
  checkExpectedToken(0);
  scheduler_.scheduleAfter(
      &simCtx_,
      Time::fromNs(kRefreshPeriodNs),
      &RateLimiterTest::checkExpectedToken,
      this,
      (6.25));
  scheduler_.run(Time::fromUs(3));
  checkExpectedToken(18.75);
  rateLimiter_->consumeTokens(4);
  checkExpectedToken(14.75);
}

INSTANTIATE_TEST_SUITE_P(
    RateLimiterTestSweep,
    RateLimiterTest,
    ::testing::Values(80));

// Test that TokenBucketRateLimiter can be created mid-simulation without
// crashing. This tests the fix for a bug where creating a rate limiter after
// simulation time had advanced would cause "Time difference is negative"
// exceptions when setting a callback.
class RateLimiterMidSimulationTest : public ::testing::Test {
 protected:
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
};

TEST_F(
    RateLimiterMidSimulationTest,
    CreateRateLimiterMidSimulationWithCallback) {
  // Advance simulation to 10 milliseconds
  scheduler_.run(Time::fromMs(10));

  // Create a rate limiter at this later time - this should not throw
  auto rateLimiter = std::make_unique<TokenBucketRateLimiter>(
      &simCtx_,
      65536, // bucket size
      DataRate("400Gbps"), // rate
      1000, // refresh period ns
      500, // init refresh delay ns (splay)
      true, // enable
      FlexDeviceId({1, 0, 0, 20}));

  // Track if callback was invoked
  bool callbackInvoked = false;
  FlexDeviceId callbackId;

  // Set a callback - this is where the bug manifested (in rescheduleNotify)
  // Before the fix, this would throw "Time difference is negative"
  EXPECT_NO_THROW(rateLimiter->setOnBytesAvailableCallback(
      [&callbackInvoked, &callbackId](const FlexDeviceId& id) {
        callbackInvoked = true;
        callbackId = id;
      }));

  // Set trigger level to request a callback when tokens are available
  rateLimiter->setNumBytesAvailable(1000);

  // Run simulation forward - callback should fire once tokens accumulate
  scheduler_.run(Time::fromMs(1));

  // Verify callback was invoked with correct ID
  EXPECT_TRUE(callbackInvoked)
      << "Callback should have been invoked after tokens accumulated";
  EXPECT_EQ(callbackId, FlexDeviceId({1, 0, 0, 20}))
      << "Callback should receive the correct FlexDeviceId";

  // Verify tokens accumulated correctly
  // 400Gbps = 50GB/s = 50 bytes/ns. Over 1ms = 50,000,000 bytes (capped at
  // bucket size)
  double tokens = rateLimiter->getMaxBytesAllowed();
  EXPECT_GT(tokens, 0) << "Should have accumulated tokens after refresh";
  EXPECT_LE(tokens, 65536) << "Tokens should be capped at bucket size";
}

TEST_F(
    RateLimiterMidSimulationTest,
    CreateRateLimiterAtVeryLateSimulationTime) {
  // Advance simulation to 1 second (a very late time)
  scheduler_.run(Time::fromSec(1));

  // Create a rate limiter at this late time - should still work
  auto rateLimiter = std::make_unique<TokenBucketRateLimiter>(
      &simCtx_,
      65536,
      DataRate("100Gbps"),
      1000,
      1000, // init refresh delay = refresh period
      true,
      FlexDeviceId({2, 0, 0, 30}));

  bool callbackInvoked = false;

  // This should not throw even at late simulation times
  EXPECT_NO_THROW(rateLimiter->setOnBytesAvailableCallback(
      [&callbackInvoked](const FlexDeviceId&) { callbackInvoked = true; }));

  rateLimiter->setNumBytesAvailable(100);

  // Advance a small amount - callback should fire
  scheduler_.run(Time::fromUs(100));

  EXPECT_TRUE(callbackInvoked)
      << "Callback should work even when rate limiter is created 1 second "
      << "into simulation";

  // Verify token accumulation is correct
  double tokens = rateLimiter->getMaxBytesAllowed();
  EXPECT_GT(tokens, 0) << "Should have accumulated tokens";
}

TEST_F(RateLimiterMidSimulationTest, CreateRateLimiterWithZeroDelay) {
  // Advance simulation to 5 milliseconds
  scheduler_.run(Time::fromMs(5));

  // Create a rate limiter with zero init refresh delay (startTime = now())
  auto rateLimiter = std::make_unique<TokenBucketRateLimiter>(
      &simCtx_,
      65536,
      DataRate("100Gbps"),
      1000, // refresh period ns
      0, // zero init refresh delay
      true,
      FlexDeviceId({3, 0, 0, 40}));

  bool callbackInvoked = false;

  // Should not throw with zero delay
  EXPECT_NO_THROW(rateLimiter->setOnBytesAvailableCallback(
      [&callbackInvoked](const FlexDeviceId&) { callbackInvoked = true; }));

  rateLimiter->setNumBytesAvailable(100);

  // Advance simulation - callback should fire
  scheduler_.run(Time::fromUs(10));

  EXPECT_TRUE(callbackInvoked)
      << "Callback should work with zero init refresh delay";

  double tokens = rateLimiter->getMaxBytesAllowed();
  EXPECT_GT(tokens, 0) << "Should have accumulated tokens with zero delay";
}

// A fresh bucket with no tokens, whose bytes-available subscriber is replaced
// while a notification is pending.
class RateLimiterSubscriberTest : public RateLimiterMidSimulationTest {
 protected:
  // 100 Gbps fills 12500 bytes per 1000 ns refresh, so the notification for
  // kTriggerBytes is due at the first refresh.
  static constexpr uint32_t kTriggerBytes{1000};
  TokenBucketRateLimiter rateLimiter_{
      &simCtx_,
      /* bucketSize= */ 65536,
      DataRate("100Gbps"),
      /* refreshPeriodNs= */ 1000,
      /* initRefreshDelayNs= */ 0,
      /* en= */ true,
      FlexDeviceId({3, 0, 0, 40})};
};

TEST_F(RateLimiterSubscriberTest, UnsubscribingWithdrawsAPendingNotification) {
  int firstSubscriberCalls = 0;
  rateLimiter_.setOnBytesAvailableCallback(
      [&firstSubscriberCalls](const FlexDeviceId&) { ++firstSubscriberCalls; });
  rateLimiter_.setNumBytesAvailable(kTriggerBytes);

  rateLimiter_.setOnBytesAvailableCallback(nullptr);
  scheduler_.run(Time::fromUs(10));

  EXPECT_EQ(firstSubscriberCalls, 0);
}

TEST_F(RateLimiterSubscriberTest, ResubscribingSchedulesTheNotificationAgain) {
  int firstSubscriberCalls = 0;
  int secondSubscriberCalls = 0;
  rateLimiter_.setOnBytesAvailableCallback(
      [&firstSubscriberCalls](const FlexDeviceId&) { ++firstSubscriberCalls; });
  rateLimiter_.setNumBytesAvailable(kTriggerBytes);

  rateLimiter_.setOnBytesAvailableCallback(nullptr);
  rateLimiter_.setOnBytesAvailableCallback(
      [&secondSubscriberCalls](const FlexDeviceId&) {
        ++secondSubscriberCalls;
      });
  scheduler_.run(Time::fromUs(10));

  EXPECT_EQ(firstSubscriberCalls, 0);
  EXPECT_EQ(secondSubscriberCalls, 1);
}

}; // namespace packsim
