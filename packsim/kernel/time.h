/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <compare>
#include <cstdint>
#include <stdexcept>
#include <type_traits>

#include <folly/CPortability.h>

namespace packsim {

class Time {
 public:
  Time() = default;

  /**
   * Time of `fs` femtoseconds.
   *
   * @param fs  Duration in femtoseconds.
   */
  explicit Time(const uint64_t fs) noexcept : timeFs_(fs) {}

  /**
   * Time of `ps` picoseconds. The femtosecond product wraps modulo 2^64 fs
   * (about 5.1 hours).
   *
   * @param ps  Duration in picoseconds.
   *
   * @return    The same duration as a Time.
   */
  FOLLY_ALWAYS_INLINE static Time fromPs(const uint64_t ps) noexcept {
    return Time(ps * 1'000ULL);
  }

  /**
   * Time of `ns` nanoseconds. The femtosecond product wraps modulo 2^64 fs
   * (about 5.1 hours).
   *
   * @param ns  Duration in nanoseconds.
   *
   * @return    The same duration as a Time.
   */
  FOLLY_ALWAYS_INLINE static Time fromNs(const uint64_t ns) noexcept {
    return Time(ns * 1'000'000ULL);
  }

  /**
   * Time of `us` microseconds. The femtosecond product wraps modulo 2^64 fs
   * (about 5.1 hours).
   *
   * @param us  Duration in microseconds.
   *
   * @return    The same duration as a Time.
   */
  FOLLY_ALWAYS_INLINE static Time fromUs(const uint64_t us) noexcept {
    return Time(us * 1'000'000'000ULL);
  }

  /**
   * Time of `ms` milliseconds. The femtosecond product wraps modulo 2^64 fs
   * (about 5.1 hours).
   *
   * @param ms  Duration in milliseconds.
   *
   * @return    The same duration as a Time.
   */
  FOLLY_ALWAYS_INLINE static Time fromMs(const uint64_t ms) noexcept {
    return Time(ms * 1'000'000'000'000ULL);
  }

  /**
   * Time of `sec` seconds. The femtosecond product wraps modulo 2^64 fs
   * (about 5.1 hours).
   *
   * @param sec  Duration in seconds.
   *
   * @return     The same duration as a Time.
   */
  FOLLY_ALWAYS_INLINE static Time fromSec(const uint64_t sec) noexcept {
    return Time(sec * 1'000'000'000'000'000ULL);
  }

  friend Time operator+(const Time lhs, const Time rhs) {
    return Time(lhs.timeFs_ + rhs.timeFs_);
  }

  friend Time operator-(const Time lhs, const Time rhs) {
    if (rhs.timeFs_ > lhs.timeFs_) {
      throw std::runtime_error("Time difference is negative");
    }
    return Time(lhs.timeFs_ - rhs.timeFs_);
  }

  Time& operator+=(const Time& rhs) {
    timeFs_ += rhs.timeFs_;
    return *this;
  }

  Time& operator-=(const Time& rhs) {
    if (rhs.timeFs_ > timeFs_) {
      throw std::runtime_error("Time difference is negative");
    }
    timeFs_ -= rhs.timeFs_;
    return *this;
  }

  template <class T>
    requires std::is_arithmetic_v<T>
  friend Time operator*(const Time lhs, const T& rhs) {
    return Time(lhs.timeFs_ * rhs);
  }

  template <class T>
    requires std::is_arithmetic_v<T>
  friend Time operator*(const T& lhs, const Time rhs) {
    return Time(lhs * rhs.timeFs_);
  }

  template <class T>
    requires std::is_arithmetic_v<T>
  friend Time operator/(const Time lhs, const T& rhs) {
    return Time(lhs.timeFs_ / rhs);
  }

  double operator/(const Time& rhs) const {
    return static_cast<double>(timeFs_) / rhs.timeFs_;
  }

  template <class T>
  Time& operator*=(const T& rhs) {
    timeFs_ *= rhs;
    return *this;
  }

  template <class T>
  Time& operator/=(const T& rhs) {
    timeFs_ /= rhs;
    return *this;
  }

  // Times compare by their single member, so the defaults order them by
  // duration and supply every comparison operator.
  friend bool operator==(const Time& lhs, const Time& rhs) noexcept = default;
  friend auto operator<=>(const Time& lhs, const Time& rhs) noexcept = default;

  bool isZero() const noexcept {
    return timeFs_ == 0;
  }

  // Duration in femtoseconds.
  FOLLY_ALWAYS_INLINE uint64_t getFs() const noexcept {
    return timeFs_;
  }

  // Whole picoseconds, truncated toward zero.
  FOLLY_ALWAYS_INLINE uint64_t getPs() const noexcept {
    return timeFs_ / 1'000ULL;
  }

  // Whole nanoseconds, truncated toward zero.
  FOLLY_ALWAYS_INLINE uint64_t getNs() const noexcept {
    return timeFs_ / 1'000'000ULL;
  }

  // Whole microseconds, truncated toward zero.
  FOLLY_ALWAYS_INLINE uint64_t getUs() const noexcept {
    return timeFs_ / 1'000'000'000ULL;
  }

 private:
  uint64_t timeFs_{0};
};

} // namespace packsim
