/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>

#include "packsim/kernel/device-id.h"
#include "packsim/util/periodic-update-value.h"

namespace packsim {

enum class RateLimiterType : uint8_t {
  NONE = 0,
  TOKEN_BUCKET = 1,
};

// base class of queue packet handler
class RateLimiter {
 public:
  explicit RateLimiter();
  RateLimiter(const RateLimiter&) = delete;
  RateLimiter& operator=(const RateLimiter&) = delete;
  RateLimiter(RateLimiter&&) = delete;
  RateLimiter& operator=(RateLimiter&&) = delete;
  virtual double getMaxBytesAllowed() = 0;
  virtual ~RateLimiter() = 0;
  virtual RateLimiterType getRateLimiterType() = 0;
};

class TokenBucketRateLimiter : public RateLimiter {
 public:
  TokenBucketRateLimiter(
      SimContext* simCtx,
      uint32_t bucketSize,
      const DataRate& rate,
      double refreshPeriodNs,
      bool en,
      const FlexDeviceId& id,
      bool allowNegativeTokens = false)
      : TokenBucketRateLimiter(
            simCtx,
            bucketSize,
            rate,
            refreshPeriodNs,
            refreshPeriodNs,
            en,
            id,
            allowNegativeTokens) {}
  TokenBucketRateLimiter(
      SimContext* simCtx,
      uint32_t bucketSize,
      const DataRate& rate,
      double refreshPeriodNs,
      double initRefreshDelayNs,
      bool en,
      const FlexDeviceId& id,
      bool allowNegativeTokens = false);
  double getMaxBytesAllowed() override;
  virtual ~TokenBucketRateLimiter() override {}
  TokenBucketRateLimiter(const TokenBucketRateLimiter&) = delete;
  TokenBucketRateLimiter& operator=(const TokenBucketRateLimiter&) = delete;
  TokenBucketRateLimiter(TokenBucketRateLimiter&&) = delete;
  TokenBucketRateLimiter& operator=(TokenBucketRateLimiter&&) = delete;
  RateLimiterType getRateLimiterType() override;
  void consumeTokens(uint32_t tokens);
  void setRateParam(DataRate rate);
  DataRate getRateParam();
  void addTokens();
  using OnBytesAvailableCallback = std::function<void(const FlexDeviceId&)>;
  void setOnBytesAvailableCallback(const OnBytesAvailableCallback& cb);
  void setNumBytesAvailable(uint32_t numBytes);
  void cancelRefreshEvent();

 private:
  TokenBucketWithCb tb_;
};

} // namespace packsim
