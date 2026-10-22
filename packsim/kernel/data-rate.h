/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cmath>
#include <cstdint>
#include <string>

#include <folly/CPortability.h>

#include "packsim/kernel/time.h"

namespace packsim {

class DataRate {
 public:
  DataRate() noexcept {
    setBps(1);
  }
  /**
   * Parses a rate such as "100Gbps" or "12.5MBps".
   *
   * @param s  An amount in fixed notation (digits with at most one '.', no
   *           sign, exponent or whitespace) followed by one of the
   *           case-sensitive units Gbps, GBps, Mbps or MBps. The amount is
   *           read as the nearest double and multiplied by the unit's bits per
   *           second in double precision, and the product is truncated toward
   *           zero, so below about 2^52 bps (4.5 Pbps) the result can be 1 bps
   *           below the exact rate ("4.1Gbps" gives 4099999999), and above that
   *           it can be off by up to about 3 kbps either way.
   *
   * @throw std::invalid_argument if `s` is not of that form, or its rate,
   *        computed that way, is 2^64 bits per second or more (a rate within
   *        about 3 kbps below 2^64 can round up to it).
   */
  explicit DataRate(const std::string& s);
  explicit DataRate(uint64_t bps) noexcept {
    setBps(bps);
  }

  DataRate(const DataRate& other) noexcept {
    setBps(other.bps_);
  }

  DataRate(DataRate&& other) noexcept {
    setBps(other.bps_);
  }

  DataRate& operator=(const DataRate& other) noexcept {
    setBps(other.bps_);
    return *this;
  }

  DataRate& operator=(DataRate&& other) noexcept {
    swap(*this, other);
    return *this;
  }

  ~DataRate() = default;

  // Rate in bits per second.
  FOLLY_ALWAYS_INLINE uint64_t getBps() const noexcept {
    return bps_;
  }

  FOLLY_ALWAYS_INLINE Time bitsToTime(double bits) const noexcept {
    return Time(static_cast<uint64_t>(std::llround(bits * bitsToFs_)));
  }

  FOLLY_ALWAYS_INLINE Time bytesToTime(double bytes) const noexcept {
    return bitsToTime(bytes * kBitsPerByte);
  }

  FOLLY_ALWAYS_INLINE friend bool operator<(
      const DataRate& lhs,
      const DataRate& rhs) noexcept {
    return lhs.bps_ < rhs.bps_;
  }

  FOLLY_ALWAYS_INLINE friend bool operator>(
      const DataRate& lhs,
      const DataRate& rhs) noexcept {
    return rhs.bps_ < lhs.bps_;
  }

  FOLLY_ALWAYS_INLINE friend bool operator<=(
      const DataRate& lhs,
      const DataRate& rhs) noexcept {
    return !(lhs.bps_ > rhs.bps_);
  }

  FOLLY_ALWAYS_INLINE friend bool operator>=(
      const DataRate& lhs,
      const DataRate& rhs) noexcept {
    return !(lhs.bps_ < rhs.bps_);
  }

  FOLLY_ALWAYS_INLINE friend bool operator==(
      const DataRate& lhs,
      const DataRate& rhs) noexcept {
    return lhs.bps_ == rhs.bps_;
  }

  friend bool operator!=(const DataRate& lhs, const DataRate& rhs) noexcept {
    return !(lhs == rhs);
  }

  friend void swap(DataRate& first, DataRate& second) noexcept {
    std::swap(first.bps_, second.bps_);
    std::swap(first.bitsToFs_, second.bitsToFs_);
  }

 private:
  // Femtoseconds per second, used to convert a bit rate into per-bit duration.
  static constexpr double kFemtosecondsPerSecond = 1e15;
  static constexpr double kBitsPerByte = 8.0;

  void setBps(uint64_t bps) {
    bps_ = bps;
    bitsToFs_ = kFemtosecondsPerSecond / bps_;
  }
  uint64_t bps_{1};
  double bitsToFs_{kFemtosecondsPerSecond};
};

} // namespace packsim
