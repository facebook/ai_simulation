/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/Conv.h>
#include <folly/IPAddress.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/device-id.h"

namespace packsim {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

TEST(FlexDeviceId, DefaultIsUninitializedAndZeroed) {
  const FlexDeviceId id;
  EXPECT_FALSE(id.isInitialized());
  const std::vector<uint32_t> zeros(FlexDeviceId::kNumFlexDeviceIdFields, 0);
  EXPECT_EQ(id.asVec(), zeros);
}

TEST(FlexDeviceId, VectorConstructorPadsTrailingFieldsWithZero) {
  const FlexDeviceId id(std::vector<uint32_t>{1, 2, 3});
  EXPECT_TRUE(id.isInitialized());
  const std::vector<uint32_t> expected{1, 2, 3, 0, 0, 0, 0, 0};
  EXPECT_EQ(id.asVec(), expected);
}

TEST(FlexDeviceId, EmptyVectorIsUninitialized) {
  const FlexDeviceId id(std::vector<uint32_t>{});
  EXPECT_FALSE(id.isInitialized());
}

TEST(FlexDeviceId, TooManyValuesThrows) {
  const std::vector<uint32_t> tooMany(
      FlexDeviceId::kNumFlexDeviceIdFields + 1, 1);
  EXPECT_THAT(
      [&] { FlexDeviceId{tooMany}; },
      ThrowsMessage<std::runtime_error>(HasSubstr("Too many values")));
}

TEST(FlexDeviceId, ValueAboveHextetRangeThrows) {
  EXPECT_THAT(
      [] { FlexDeviceId{std::vector<uint32_t>{65536}}; },
      ThrowsMessage<folly::ConversionError>(
          HasSubstr("Overflow during arithmetic conversion")));
}

TEST(FlexDeviceId, FieldsCoverTheFullHextetRange) {
  const FlexDeviceId high(std::vector<uint32_t>{65535, 32768});
  const std::vector<uint32_t> expected{65535, 32768, 0, 0, 0, 0, 0, 0};
  EXPECT_EQ(high.asVec(), expected);
  EXPECT_EQ(high.toString(), "ffff:8000::");
  EXPECT_EQ(FlexDeviceId("ffff:8000::"), high);
  EXPECT_TRUE(high > FlexDeviceId(std::vector<uint32_t>{65535, 32767}));
}

TEST(FlexDeviceId, SerializeRoundTripsThroughIpv6) {
  const FlexDeviceId original(std::vector<uint32_t>{1, 2, 3, 4, 5, 6, 7, 8});
  const folly::IPAddressV6 ipv6(original.serialize());
  const FlexDeviceId roundTripped(ipv6);
  EXPECT_EQ(roundTripped, original);
}

TEST(FlexDeviceId, ToStringRoundTripsThroughStringConstructor) {
  const FlexDeviceId original(
      std::vector<uint32_t>{10, 20, 30, 40, 50, 60, 70, 80});
  const FlexDeviceId reparsed(original.toString().c_str());
  EXPECT_EQ(reparsed, original);
}

TEST(FlexDeviceId, GreaterThanIgnoresFirstField) {
  // operator> compares fields starting at index 1, so a difference confined to
  // index 0 leaves the two IDs neither greater than the other.
  const FlexDeviceId a(std::vector<uint32_t>{0, 5, 5, 5, 5, 5, 5, 5});
  const FlexDeviceId b(std::vector<uint32_t>{9, 5, 5, 5, 5, 5, 5, 5});
  EXPECT_FALSE(a > b);
  EXPECT_FALSE(b > a);
  // ...but operator== DOES consider field 0, so they are not equal.
  EXPECT_FALSE(a == b);
  EXPECT_TRUE(a != b);
}

TEST(FlexDeviceId, GreaterThanComparesFromSecondField) {
  const FlexDeviceId smaller(std::vector<uint32_t>{0, 1, 0, 0, 0, 0, 0, 0});
  const FlexDeviceId larger(std::vector<uint32_t>{0, 2, 0, 0, 0, 0, 0, 0});
  EXPECT_TRUE(larger > smaller);
  EXPECT_FALSE(smaller > larger);
}

TEST(FlexDeviceId, EqualIdsHashEquallyAndWorkAsMapKeys) {
  const FlexDeviceId key(std::vector<uint32_t>{1, 1, 2, 3, 5, 8, 13, 21});
  const FlexDeviceId same(std::vector<uint32_t>{1, 1, 2, 3, 5, 8, 13, 21});
  EXPECT_EQ(std::hash<FlexDeviceId>{}(key), std::hash<FlexDeviceId>{}(same));

  std::unordered_map<FlexDeviceId, int> byId;
  byId[key] = 42;
  EXPECT_EQ(byId.at(same), 42);
}

TEST(FlexDeviceId, MalformedStringThrows) {
  EXPECT_THAT(
      [] { FlexDeviceId{"not-an-ip-address"}; },
      ThrowsMessage<folly::IPAddressFormatException>(
          HasSubstr("not-an-ip-address")));
}

} // namespace packsim
