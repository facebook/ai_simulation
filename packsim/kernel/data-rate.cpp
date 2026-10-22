/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/data-rate.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

#include <fmt/format.h>

namespace packsim {

namespace {

// A unit the string constructor accepts.
struct RateUnit {
  // Text the rate string must end with. Matched case-sensitively, because the
  // case of the 'b' is what separates bits (Gbps) from bytes (GBps).
  std::string_view suffix;
  // Bits per second that one unit of the amount stands for.
  double bpsPerUnit;
};

constexpr std::array<RateUnit, 4> kRateUnits{{
    {"Gbps", 1e9},
    {"GBps", 8e9},
    {"Mbps", 1e6},
    {"MBps", 8e6},
}};

// 2^64, the first bit rate a uint64_t cannot hold.
constexpr double kBpsLimit = 0x1p64;

// The value of the first `length` characters of `s` if all of them form a
// non-negative decimal number in fixed notation, e.g. "100" or "12.5": 0 for
// an amount too small for a double, +infinity for one too large, and nothing
// if the characters are not of that form.
std::optional<double> parseAmount(
    const std::string& s,
    const size_t length) noexcept {
  if (length == 0 || s.front() == '-') {
    return std::nullopt;
  }
  double amount = 0;
  const char* const begin = s.data();
  const char* const end = begin + length;
  const auto [parsedEnd, error] =
      std::from_chars(begin, end, amount, std::chars_format::fixed);
  if (parsedEnd != end) {
    return std::nullopt;
  }
  if (error == std::errc::result_out_of_range) {
    // Without an exponent, only an amount below one can underflow.
    const bool isBelowOne = std::all_of(
        begin, std::find(begin, end, '.'), [](char c) { return c == '0'; });
    return isBelowOne ? 0.0 : std::numeric_limits<double>::infinity();
  }
  if (error != std::errc{} || !std::isfinite(amount)) {
    return std::nullopt;
  }
  return amount;
}

// The bits per second of `s`, which ends in `unit`'s suffix.
uint64_t parseBps(const std::string& s, const RateUnit& unit) {
  const auto amount = parseAmount(s, s.size() - unit.suffix.size());
  if (!amount) {
    throw std::invalid_argument(
        fmt::format(
            "Data rate '{}' must be an amount in fixed notation (digits with at most one '.', no sign, exponent or spaces) followed by its unit",
            s));
  }
  const double bps = *amount * unit.bpsPerUnit;
  if (bps >= kBpsLimit) {
    throw std::invalid_argument(
        fmt::format(
            "Data rate '{}' is too large: its bit rate rounds to 2^64 bits per second or more",
            s));
  }
  return static_cast<uint64_t>(bps);
}

} // namespace

DataRate::DataRate(const std::string& s) {
  for (const auto& unit : kRateUnits) {
    if (s.ends_with(unit.suffix)) {
      setBps(parseBps(s, unit));
      return;
    }
  }
  if (parseAmount(s, s.size())) {
    throw std::invalid_argument(
        fmt::format("No units provided for data rate '{}'", s));
  }
  throw std::invalid_argument(
      fmt::format(
          "Unsupported rate suffix in data rate '{}'; expected Gbps, GBps, Mbps or MBps",
          s));
}

} // namespace packsim
