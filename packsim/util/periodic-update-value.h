/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <functional>
#include <stdexcept>

#include <folly/logging/xlog.h>

#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

namespace packsim {

// A value that updates periodically based on simulation time.
// The startTime parameter specifies when periodic updates begin. For objects
// created mid-simulation, pass simCtx->now() + delay, not just delay relative
// to time 0. This ensures correct behavior when the object is created after
// simulation has already progressed.
template <typename T>
class PeriodicUpdateValue {
 public:
  PeriodicUpdateValue(
      SimContext* simCtx,
      T initialValue,
      const Time& period,
      const Time& startTime)
      : simCtx_(simCtx),
        period_(period),
        lastUpdateTime_(startTime),
        value_(initialValue) {}
  virtual ~PeriodicUpdateValue() = default;

  const T& operator()() {
    auto now = simCtx_->now();
    if (now < (lastUpdateTime_ + period_)) {
      return value_;
    }
    if (period_.getNs() > 0) {
      uint64_t n = std::floor((now - lastUpdateTime_) / period_);
      updateValueNTimes(n);
      lastUpdateTime_ += n * period_;
    } else {
      updateValueOnce();
      lastUpdateTime_ = now;
    }

    return value_;
  }

  void setValue(const T& v) {
    value_ = v;
  }

 protected:
  virtual void updateValueOnce() = 0;
  virtual void updateValueNTimes(uint64_t n) {
    for (uint64_t i = 0; i < n; i++) {
      updateValueOnce();
    }
  }
  SimContext* const simCtx_;
  Time period_;
  Time lastUpdateTime_;
  T value_;
};

class TokenBucketWithCb : public PeriodicUpdateValue<double> {
 public:
  using OnBytesAvailableCallback = std::function<void(const FlexDeviceId&)>;

  TokenBucketWithCb(
      SimContext* simCtx,
      double initialValue,
      const DataRate& rate,
      double maxValue,
      const Time period,
      const Time startTime,
      const FlexDeviceId& id,
      bool allowNegativeTokens)
      : PeriodicUpdateValue<double>(simCtx, initialValue, period, startTime),
        maxValue_(maxValue),
        id_(id),
        allowNegativeTokens_(allowNegativeTokens) {
    setRate(rate);
    rescheduleNotify();
  }

  TokenBucketWithCb& operator+=(const double& other) {
    value_ += other;
    rescheduleNotify();
    return *this;
  }

  TokenBucketWithCb& operator-=(const double& other) {
    value_ -= other;
    if (value_ < 0 && !allowNegativeTokens_) {
      throw std::runtime_error("Not enough tokens");
    }
    rescheduleNotify();
    return *this;
  }

  void setRate(DataRate rate) {
    incrementBy_ =
        rate.getBps() * period_.getNs() / (kBitsPerByte * kNsPerSecond);
    rate_ = std::move(rate);
    rescheduleNotify();
  }

  const DataRate& getRate() const noexcept {
    return rate_;
  }

  void setOnBytesAvailableCallback(const OnBytesAvailableCallback& cb) {
    onBytesAvailable_ = cb;
    hasBytesAvailableSubscriber_ = static_cast<bool>(cb);
    if (!hasBytesAvailableSubscriber_) {
      // Unsubscribing withdraws a pending notification, so that a later
      // subscriber gets one scheduled afresh.
      cancelNotify();
      nextCbTime_ = Time();
      return;
    }
    rescheduleNotify();
  }

  void setTriggerLevel(double level) {
    triggerLevel_ = level;
    rescheduleNotify();
  }

  void onBytesAvailable() {
    XCHECK(onBytesAvailable_) << "no bytes-available subscriber registered";
    onBytesAvailable_(id_);
  }

  void cancelNotify() {
    notifyEvent_.cancel();
  }

  // Don't need to update callback time here, since it already accounted for
  // regular increments
  void updateValueOnce() override {
    value_ = std::min(value_ + incrementBy_, maxValue_);
  }

  void updateValueNTimes(uint64_t n) override {
    value_ = std::min(value_ + n * incrementBy_, maxValue_);
  }

  void rescheduleNotify() {
    // No subscriber is a real state: the constructor and the rate and level
    // setters reschedule before setOnBytesAvailableCallback() has run.
    if ((value_ >= triggerLevel_) || !hasBytesAvailableSubscriber_) {
      return;
    }
    double reqToks = triggerLevel_ - value_;
    double reqIncs = std::ceil(reqToks / incrementBy_);
    Time trigTime = lastUpdateTime_ + reqIncs * period_;

    if (trigTime != nextCbTime_) {
      notifyEvent_.cancel();
      // Defensive: if trigTime is in the past, trigger immediately.
      Time now = simCtx_->now();
      Time delta = (trigTime <= now) ? Time::fromNs(0) : (trigTime - now);
      notifyEvent_ =
          simCtx_->schedule(delta, &TokenBucketWithCb::onBytesAvailable, this);
      nextCbTime_ = trigTime;
    }
  }

 private:
  // Bits in a byte.
  static constexpr double kBitsPerByte = 8.0;
  // Nanoseconds in one second.
  static constexpr double kNsPerSecond = 1'000'000'000.0;

 protected:
  double incrementBy_{0};
  double maxValue_{0};
  double triggerLevel_{0};
  FlexDeviceId id_;
  bool allowNegativeTokens_{false};

  DataRate rate_;
  Time nextCbTime_;
  // Pending onBytesAvailable() event; re-armed when the computed trigger time
  // moves, cancelled by cancelNotify().
  Scheduler::EventHandle notifyEvent_;
  OnBytesAvailableCallback onBytesAvailable_;
  // Whether onBytesAvailable_ holds a subscriber; rescheduleNotify() reads this
  // instead of the member, which only the setter and the raiser touch.
  bool hasBytesAvailableSubscriber_{false};
};

} // namespace packsim
