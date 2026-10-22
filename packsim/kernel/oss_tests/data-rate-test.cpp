/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <array>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/data-rate.h"

namespace packsim {
namespace {

using ::testing::AllOf;
using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

// Mis-cased spellings of the four supported suffixes, one per casing class:
// all-lower, all-upper, and a swapped-case variant of the b/B that carries the
// bits-vs-bytes distinction.
constexpr std::array<std::string_view, 5> kMisCasedSuffixes{
    "gbps",
    "GBPS",
    "gBps",
    "mbps",
    "MBPS"};

TEST(DataRateTests, TestBytesConversion) {
  auto bps = DataRate("100Gbps");
  auto t1MB = bps.bytesToTime(1000 * 1000);
  EXPECT_EQ(t1MB.getUs(), 80);
}

TEST(DataRateTests, TestBitsConversion) {
  auto bps = DataRate("100Gbps");
  auto t1MB = bps.bitsToTime(8 * 1000 * 1000);
  EXPECT_EQ(t1MB.getNs(), Time::fromUs(80).getNs());
}

TEST(DataRateTests, StringConstructorParsesUnits) {
  EXPECT_EQ(DataRate("100Gbps").getBps(), 100'000'000'000ULL);
  EXPECT_EQ(DataRate("1GBps").getBps(), 8'000'000'000ULL); // bytes/s = *8
  EXPECT_EQ(DataRate("500Mbps").getBps(), 500'000'000ULL);
  EXPECT_EQ(DataRate("2MBps").getBps(), 16'000'000ULL); // bytes/s = *8
}

TEST(DataRateTests, StringConstructorRejectsMissingUnits) {
  EXPECT_THAT(
      [] { DataRate("100"); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("No units provided")));
}

TEST(DataRateTests, StringConstructorRejectsUnknownSuffix) {
  EXPECT_THAT(
      [] { DataRate("100Xbps"); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Unsupported rate suffix")));
}

// Suffix matching here is deliberately case-sensitive: the only thing
// separating Gbps (bits) from GBps (bytes) is the case of the 'b', so
// case-folding the comparison would silently conflate the two and scale every
// affected rate by 8. Rejecting mis-cased suffixes keeps that error loud.
TEST(DataRateTests, StringConstructorRejectsMisCasedSuffix) {
  for (const auto suffix : kMisCasedSuffixes) {
    SCOPED_TRACE(suffix);
    EXPECT_THAT(
        [suffix] { DataRate(std::string("100").append(suffix)); },
        ThrowsMessage<std::invalid_argument>(
            HasSubstr("Unsupported rate suffix")));
  }
}

TEST(DataRateTests, StringConstructorParsesFractionalAmount) {
  EXPECT_EQ(DataRate("12.5Gbps").getBps(), 12'500'000'000ULL);
}

TEST(DataRateTests, StringConstructorRejectsAmountWithTwoPoints) {
  EXPECT_THAT(
      [] { DataRate("1.2.3Gbps"); },
      ThrowsMessage<std::invalid_argument>(AllOf(
          HasSubstr("must be an amount in fixed notation"),
          HasSubstr("'1.2.3Gbps'"))));
}

TEST(DataRateTests, StringConstructorRejectsNegativeAmount) {
  EXPECT_THAT(
      [] { DataRate("-5Gbps"); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("must be an amount in fixed notation")));
}

TEST(DataRateTests, StringConstructorRejectsUnitWithoutAmount) {
  EXPECT_THAT(
      [] { DataRate("Gbps"); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("must be an amount in fixed notation")));
}

TEST(DataRateTests, StringConstructorRejectsExponentNotation) {
  EXPECT_THAT(
      [] { DataRate("1e3Mbps"); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("must be an amount in fixed notation")));
}

TEST(DataRateTests, StringConstructorRejectsRateBeyondUint64) {
  EXPECT_THAT(
      [] { DataRate("20000000000GBps"); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("too large")));
}

TEST(DataRateTests, StringConstructorRejectsNonFiniteAmount) {
  for (const std::string rate : {"nanGbps", "infGbps"}) {
    SCOPED_TRACE(rate);
    EXPECT_THAT(
        [&rate] { DataRate{rate}; },
        ThrowsMessage<std::invalid_argument>(
            HasSubstr("must be an amount in fixed notation")));
  }
}

TEST(DataRateTests, StringConstructorRejectsRateRoundingUpToTwoToThe64) {
  EXPECT_THAT(
      [] { DataRate("18446744073.709551615Gbps"); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("too large")));
}

TEST(DataRateTests, StringConstructorAcceptsRateJustBelowTwoToThe64) {
  EXPECT_EQ(
      DataRate("18446744073.70955Gbps").getBps(),
      18'446'744'073'709'549'568ULL);
}

TEST(DataRateTests, StringConstructorComputesTheRateInDouble) {
  EXPECT_EQ(DataRate("2.132077Gbps").getBps(), 2'132'077'000ULL);
  EXPECT_EQ(DataRate("33.829542Gbps").getBps(), 33'829'541'999ULL);
}

TEST(DataRateTests, StringConstructorTruncatesTheDoubleProduct) {
  EXPECT_EQ(DataRate("4.1Gbps").getBps(), 4'099'999'999ULL);
}

TEST(DataRateTests, StringConstructorRejectsPositiveSign) {
  EXPECT_THAT(
      [] { DataRate("+5Gbps"); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("must be an amount in fixed notation")));
}

TEST(DataRateTests, StringConstructorRejectsWhitespaceAroundAmount) {
  for (const std::string rate : {"5 Gbps", " 5Gbps"}) {
    SCOPED_TRACE(rate);
    EXPECT_THAT(
        [&rate] { DataRate{rate}; },
        ThrowsMessage<std::invalid_argument>(
            HasSubstr("must be an amount in fixed notation")));
  }
}

TEST(DataRateTests, StringConstructorRejectsHexAmount) {
  EXPECT_THAT(
      [] { DataRate("0x10Gbps"); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("must be an amount in fixed notation")));
}

TEST(DataRateTests, StringConstructorReportsAnUnsupportedSuffixWithoutAUnit) {
  for (const std::string rate : {"", "1.2.3"}) {
    SCOPED_TRACE(rate);
    EXPECT_THAT(
        [&rate] { DataRate{rate}; },
        ThrowsMessage<std::invalid_argument>(
            HasSubstr("Unsupported rate suffix")));
  }
}

TEST(DataRateTests, StringConstructorRejectsAnAmountTooLargeForADouble) {
  const std::string rate = "1" + std::string(400, '0') + "Gbps";
  EXPECT_THAT(
      [&rate] { DataRate{rate}; },
      ThrowsMessage<std::invalid_argument>(HasSubstr("too large")));
}

TEST(DataRateTests, StringConstructorTruncatesAnAmountTooSmallForADouble) {
  const std::string rate = "0." + std::string(400, '0') + "1Gbps";
  EXPECT_EQ(DataRate(rate).getBps(), 0ULL);
}

TEST(DataRateTests, DefaultConstructorIsOneBps) {
  EXPECT_EQ(DataRate().getBps(), 1ULL);
}

TEST(DataRateTests, BpsConstructorStoresValue) {
  EXPECT_EQ(DataRate(uint64_t{5000}).getBps(), 5000ULL);
}

TEST(DataRateTests, LessThanOrdersByBitRate) {
  EXPECT_LT(DataRate(uint64_t{100}), DataRate(uint64_t{200}));
}

TEST(DataRateTests, GreaterThanOrdersByBitRate) {
  EXPECT_GT(DataRate(uint64_t{200}), DataRate(uint64_t{100}));
}

TEST(DataRateTests, LessOrEqualHoldsForEqualAndSmaller) {
  EXPECT_LE(DataRate(uint64_t{100}), DataRate(uint64_t{100}));
  EXPECT_LE(DataRate(uint64_t{100}), DataRate(uint64_t{200}));
}

TEST(DataRateTests, GreaterOrEqualHoldsForEqualAndLarger) {
  EXPECT_GE(DataRate(uint64_t{100}), DataRate(uint64_t{100}));
  EXPECT_GE(DataRate(uint64_t{200}), DataRate(uint64_t{100}));
}

TEST(DataRateTests, EqualityMatchesSameBitRate) {
  EXPECT_EQ(DataRate(uint64_t{100}), DataRate(uint64_t{100}));
}

TEST(DataRateTests, InequalityDetectsDifferentBitRate) {
  EXPECT_NE(DataRate(uint64_t{100}), DataRate(uint64_t{200}));
}

} // namespace
} // namespace packsim
