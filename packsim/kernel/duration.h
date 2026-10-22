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

#include "packsim/kernel/time.h"

namespace packsim {

/**
 * A span of simulated time in femtoseconds, never negative. `Time` is a point
 * on the simulation clock and `Duration` the distance between two points.
 * Neither converts to the other, implicitly or explicitly, so passing one
 * where the other is meant does not compile. They meet only through
 * `Time + Duration`, `Time - Duration` and `Duration::between`.
 *
 * Factories and addition wrap modulo 2^64 fs (about 5.1 hours), as `Time`'s
 * do. A subtraction that would go below zero throws.
 *
 * Example usage:
 *
 *      const Duration delay = Duration::fromNs(100);
 *      const Time drainAt = simCtx.now() + delay;
 *      simCtx.schedule(delay, &Port::drain, port);
 *      // Later, from another event on simCtx:
 *      const Duration left = Duration::between(simCtx.now(), drainAt);
 */
class Duration {
 public:
  // Zero length.
  constexpr Duration() noexcept = default;

  /**
   * Duration of `fs` femtoseconds.
   *
   * @param fs  Length in femtoseconds.
   */
  explicit constexpr Duration(const uint64_t fs) noexcept : durationFs_(fs) {}

  /**
   * Duration of `ps` picoseconds. The femtosecond product wraps modulo 2^64
   * fs, as in `Time::fromPs`.
   *
   * @param ps  Length in picoseconds.
   *
   * @return    The same length as a Duration.
   */
  static constexpr Duration fromPs(const uint64_t ps) noexcept {
    return Duration(ps * kFsPerPs);
  }

  /**
   * Duration of `ns` nanoseconds. The femtosecond product wraps modulo 2^64
   * fs, as in `Time::fromNs`.
   *
   * @param ns  Length in nanoseconds.
   *
   * @return    The same length as a Duration.
   */
  static constexpr Duration fromNs(const uint64_t ns) noexcept {
    return Duration(ns * kFsPerNs);
  }

  /**
   * Duration of `us` microseconds. The femtosecond product wraps modulo 2^64
   * fs, as in `Time::fromUs`.
   *
   * @param us  Length in microseconds.
   *
   * @return    The same length as a Duration.
   */
  static constexpr Duration fromUs(const uint64_t us) noexcept {
    return Duration(us * kFsPerUs);
  }

  /**
   * Duration of `ms` milliseconds. The femtosecond product wraps modulo 2^64
   * fs, as in `Time::fromMs`.
   *
   * @param ms  Length in milliseconds.
   *
   * @return    The same length as a Duration.
   */
  static constexpr Duration fromMs(const uint64_t ms) noexcept {
    return Duration(ms * kFsPerMs);
  }

  /**
   * Duration of `sec` seconds. The femtosecond product wraps modulo 2^64 fs,
   * as in `Time::fromSec`.
   *
   * @param sec  Length in seconds.
   *
   * @return     The same length as a Duration.
   */
  static constexpr Duration fromSec(const uint64_t sec) noexcept {
    return Duration(sec * kFsPerSec);
  }

  /**
   * The span from `start` to `end`, the typed form of `end - start` (which
   * yields a `Time`).
   *
   * @param start  Earlier point.
   * @param end    Later point.
   *
   * @return       Length of the span.
   *
   * @throw std::runtime_error if `end` is before `start`.
   */
  static Duration between(const Time start, const Time end) {
    return Duration((end - start).getFs());
  }

  // Length in femtoseconds.
  constexpr uint64_t getFs() const noexcept {
    return durationFs_;
  }

  // Whole picoseconds, truncated toward zero.
  constexpr uint64_t getPs() const noexcept {
    return durationFs_ / kFsPerPs;
  }

  // Whole nanoseconds, truncated toward zero.
  constexpr uint64_t getNs() const noexcept {
    return durationFs_ / kFsPerNs;
  }

  // Whole microseconds, truncated toward zero.
  constexpr uint64_t getUs() const noexcept {
    return durationFs_ / kFsPerUs;
  }

  // Whether the length is zero.
  constexpr bool isZero() const noexcept {
    return durationFs_ == 0;
  }

  // Ordered by length; also declares `==`.
  constexpr auto operator<=>(const Duration&) const noexcept = default;

  /**
   * Sum of two durations; wraps modulo 2^64 fs, as `Time` addition does.
   *
   * @param lhs  First duration.
   * @param rhs  Second duration.
   *
   * @return     The combined length.
   */
  friend constexpr Duration operator+(
      const Duration lhs,
      const Duration rhs) noexcept {
    return Duration(lhs.durationFs_ + rhs.durationFs_);
  }

  /**
   * `lhs` shortened by `rhs`.
   *
   * @param lhs  Duration to shorten.
   * @param rhs  Length to take off.
   *
   * @return     The remaining length.
   *
   * @throw std::runtime_error if `rhs` is longer than `lhs`.
   */
  friend constexpr Duration operator-(const Duration lhs, const Duration rhs) {
    if (rhs.durationFs_ > lhs.durationFs_) {
      throw std::runtime_error("Duration difference is negative");
    }
    return Duration(lhs.durationFs_ - rhs.durationFs_);
  }

  /**
   * Add `rhs` in place; wraps like `operator+`.
   *
   * @param rhs  Duration to add.
   *
   * @return     This duration.
   */
  constexpr Duration& operator+=(const Duration rhs) noexcept {
    durationFs_ += rhs.durationFs_;
    return *this;
  }

  /**
   * Take `rhs` off in place.
   *
   * @param rhs  Length to take off.
   *
   * @return     This duration.
   *
   * @throw std::runtime_error if `rhs` is longer than this duration, which is
   *        then left unchanged.
   */
  constexpr Duration& operator-=(const Duration rhs) {
    *this = *this - rhs;
    return *this;
  }

  /**
   * The point `rhs` after `lhs`, computed with `Time` addition, so it wraps
   * as `Time` does.
   *
   * @param lhs  Starting point.
   * @param rhs  Length to move forward.
   *
   * @return     The later point.
   */
  friend Time operator+(const Time lhs, const Duration rhs) noexcept {
    return lhs + Time(rhs.durationFs_);
  }

  /**
   * The point `rhs` before `lhs`, computed with `Time` subtraction.
   *
   * @param lhs  Starting point.
   * @param rhs  Length to move back.
   *
   * @return     The earlier point.
   *
   * @throw std::runtime_error if `rhs` reaches back before time zero.
   */
  friend Time operator-(const Time lhs, const Duration rhs) {
    return lhs - Time(rhs.durationFs_);
  }

 private:
  // Femtoseconds per picosecond, nanosecond, microsecond, millisecond and
  // second.
  static constexpr uint64_t kFsPerPs = 1'000ULL;
  static constexpr uint64_t kFsPerNs = 1'000'000ULL;
  static constexpr uint64_t kFsPerUs = 1'000'000'000ULL;
  static constexpr uint64_t kFsPerMs = 1'000'000'000'000ULL;
  static constexpr uint64_t kFsPerSec = 1'000'000'000'000'000ULL;

  // Length in femtoseconds.
  uint64_t durationFs_{0};
};

} // namespace packsim
