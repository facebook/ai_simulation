/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/transport/rate-limiter.h"

namespace packsim {

RateLimiter::RateLimiter() {};

RateLimiter::~RateLimiter() {};

TokenBucketRateLimiter::TokenBucketRateLimiter(
    SimContext* simCtx,
    const uint32_t bucketSize,
    const DataRate& rate,
    const double refreshPeriodNs,
    const double initRefreshDelayNs,
    bool /* enable */,
    const FlexDeviceId& id,
    const bool allowNegativeTokens)
    : RateLimiter(),
      tb_( // Start with 0 tokens:
          simCtx,
          0,
          // Rate of token fill:
          rate,
          // Max # of tokens:
          bucketSize,
          // Refresh period:
          Time::fromNs(refreshPeriodNs),
          // Start time relative to current simulation time (not time 0).
          // This is critical for sockets created dynamically during simulation,
          // not just at initialization.
          simCtx->now() + Time::fromNs(initRefreshDelayNs),
          // FlexId for callback:
          id,
          allowNegativeTokens) {};

RateLimiterType TokenBucketRateLimiter::getRateLimiterType() {
  return RateLimiterType::TOKEN_BUCKET;
}

void TokenBucketRateLimiter::consumeTokens(const uint32_t tokens) {
  tb_ -= tokens;
}

void TokenBucketRateLimiter::setRateParam(DataRate rate) {
  tb_.setRate(std::move(rate));
}

DataRate TokenBucketRateLimiter::getRateParam() {
  return tb_.getRate();
}

void TokenBucketRateLimiter::addTokens() {
  tb_.updateValueOnce();
  tb_.rescheduleNotify();
}

double TokenBucketRateLimiter::getMaxBytesAllowed() {
  return tb_();
}

void TokenBucketRateLimiter::setOnBytesAvailableCallback(
    const OnBytesAvailableCallback& cb) {
  tb_.setOnBytesAvailableCallback(cb);
}

void TokenBucketRateLimiter::setNumBytesAvailable(const uint32_t numBytes) {
  tb_.setTriggerLevel(numBytes);
}

void TokenBucketRateLimiter::cancelRefreshEvent() {
  tb_.cancelNotify();
}

} // namespace packsim
