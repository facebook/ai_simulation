/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <stdexcept>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/ceil-divide.h"

namespace packsim {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

TEST(CeilDivide32, ReturnsCeilingWhenWithinRange) {
  EXPECT_EQ(ceilDivide32(10, 3), 4u);
  EXPECT_EQ(ceilDivide32(9, 3), 3u);
  EXPECT_EQ(ceilDivide32(0, 7), 0u);
}

TEST(CeilDivide32, ResultExactlyUint32MaxDoesNotThrow) {
  EXPECT_EQ(ceilDivide32(UINT32_MAX, 1), UINT32_MAX);
  // UINT64_MAX == (2^32 + 1) * (2^32 - 1), so the quotient is exactly
  // UINT32_MAX even though the numerator is the largest 64-bit value.
  EXPECT_EQ(ceilDivide32(UINT64_MAX, (uint64_t{1} << 32) + 1), UINT32_MAX);
}

TEST(CeilDivide32, OverflowThrowsRuntimeError) {
  // One past UINT32_MAX cannot fit in a uint32_t result.
  EXPECT_THAT(
      [] { ceilDivide32(uint64_t{UINT32_MAX} + 1, 1); },
      ThrowsMessage<std::runtime_error>(HasSubstr("overflowed")));
}

} // namespace packsim
