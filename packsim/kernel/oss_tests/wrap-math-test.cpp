/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <vector>

#include <gtest/gtest.h>

#include "packsim/kernel/wrap-math.h"

namespace packsim {

const std::vector<uint16_t> aTests16 = {0, 5, 37, (1 << 15), UINT16_MAX};
const std::vector<uint32_t> aTests32 = {0, 5, 37, (1U << 31), UINT32_MAX};
const std::vector<uint64_t> aTests64 = {0, 5, 37, (1UL << 63), UINT64_MAX};

TEST(WrapMath, Uint16Equal) {
  for (uint16_t a : aTests16) {
    uint16_t b = a;
    EXPECT_FALSE(after(a, b));
    EXPECT_FALSE(after(b, a));

    EXPECT_TRUE(afterOrEqual(a, b));
    EXPECT_TRUE(afterOrEqual(b, a));

    EXPECT_FALSE(before(a, b));
    EXPECT_FALSE(before(b, a));

    EXPECT_TRUE(beforeOrEqual(a, b));
    EXPECT_TRUE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint32Equal) {
  for (uint32_t a : aTests32) {
    uint32_t b = a;
    EXPECT_FALSE(after(a, b));
    EXPECT_FALSE(after(b, a));

    EXPECT_TRUE(afterOrEqual(a, b));
    EXPECT_TRUE(afterOrEqual(b, a));

    EXPECT_FALSE(before(a, b));
    EXPECT_FALSE(before(b, a));

    EXPECT_TRUE(beforeOrEqual(a, b));
    EXPECT_TRUE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint64Equal) {
  for (uint64_t a : aTests64) {
    uint64_t b = a;
    EXPECT_FALSE(after(a, b));
    EXPECT_FALSE(after(b, a));

    EXPECT_TRUE(afterOrEqual(a, b));
    EXPECT_TRUE(afterOrEqual(b, a));

    EXPECT_FALSE(before(a, b));
    EXPECT_FALSE(before(b, a));

    EXPECT_TRUE(beforeOrEqual(a, b));
    EXPECT_TRUE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint16OffByOne) {
  for (uint16_t a : aTests16) {
    uint16_t b = a - 1;
    EXPECT_TRUE(after(a, b));
    EXPECT_FALSE(after(b, a));

    EXPECT_TRUE(afterOrEqual(a, b));
    EXPECT_FALSE(afterOrEqual(b, a));

    EXPECT_FALSE(before(a, b));
    EXPECT_TRUE(before(b, a));

    EXPECT_FALSE(beforeOrEqual(a, b));
    EXPECT_TRUE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint32OffByOne) {
  for (uint32_t a : aTests32) {
    uint32_t b = a - 1;
    EXPECT_TRUE(after(a, b));
    EXPECT_FALSE(after(b, a));

    EXPECT_TRUE(afterOrEqual(a, b));
    EXPECT_FALSE(afterOrEqual(b, a));

    EXPECT_FALSE(before(a, b));
    EXPECT_TRUE(before(b, a));

    EXPECT_FALSE(beforeOrEqual(a, b));
    EXPECT_TRUE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint64OffByOne) {
  for (uint64_t a : aTests64) {
    uint64_t b = a - 1;
    EXPECT_TRUE(after(a, b));
    EXPECT_FALSE(after(b, a));

    EXPECT_TRUE(afterOrEqual(a, b));
    EXPECT_FALSE(afterOrEqual(b, a));

    EXPECT_FALSE(before(a, b));
    EXPECT_TRUE(before(b, a));

    EXPECT_FALSE(beforeOrEqual(a, b));
    EXPECT_TRUE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint16Wrap) {
  for (uint16_t a : aTests16) {
    uint16_t b = a - (1 << 14);
    EXPECT_TRUE(after(a, b));
    EXPECT_FALSE(after(b, a));

    EXPECT_TRUE(afterOrEqual(a, b));
    EXPECT_FALSE(afterOrEqual(b, a));

    EXPECT_FALSE(before(a, b));
    EXPECT_TRUE(before(b, a));

    EXPECT_FALSE(beforeOrEqual(a, b));
    EXPECT_TRUE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint32Wrap) {
  for (uint32_t a : aTests32) {
    uint32_t b = a - (1U << 30);
    EXPECT_TRUE(after(a, b));
    EXPECT_FALSE(after(b, a));

    EXPECT_TRUE(afterOrEqual(a, b));
    EXPECT_FALSE(afterOrEqual(b, a));

    EXPECT_FALSE(before(a, b));
    EXPECT_TRUE(before(b, a));

    EXPECT_FALSE(beforeOrEqual(a, b));
    EXPECT_TRUE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint64Wrap) {
  for (uint64_t a : aTests64) {
    uint64_t b = a - (1UL << 62);
    EXPECT_TRUE(after(a, b));
    EXPECT_FALSE(after(b, a));

    EXPECT_TRUE(afterOrEqual(a, b));
    EXPECT_FALSE(afterOrEqual(b, a));

    EXPECT_FALSE(before(a, b));
    EXPECT_TRUE(before(b, a));

    EXPECT_FALSE(beforeOrEqual(a, b));
    EXPECT_TRUE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint16FullWrapMinusOne) {
  // Here, b is 2^15 - 1 greater than a, so we expect that b > a (i.e. haven't
  // wrapped yet)
  for (uint16_t a : aTests16) {
    uint16_t b = a + (1 << 15) - 1;

    EXPECT_FALSE(after(a, b));
    EXPECT_TRUE(after(b, a));

    EXPECT_FALSE(afterOrEqual(a, b));
    EXPECT_TRUE(afterOrEqual(b, a));

    EXPECT_TRUE(before(a, b));
    EXPECT_FALSE(before(b, a));

    EXPECT_TRUE(beforeOrEqual(a, b));
    EXPECT_FALSE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint16FullWrap) {
  // Here, b is 2^15 greater than a, so we expect that a > b /and/ b > a -- i.e.
  // things are undefined here. But also a >= b and b >= a will both be false.
  for (uint16_t a : aTests16) {
    uint16_t b = a + (1 << 15);

    EXPECT_TRUE(after(a, b));
    EXPECT_TRUE(after(b, a));

    EXPECT_FALSE(afterOrEqual(a, b));
    EXPECT_FALSE(afterOrEqual(b, a));

    EXPECT_TRUE(before(a, b));
    EXPECT_TRUE(before(b, a));

    EXPECT_FALSE(beforeOrEqual(a, b));
    EXPECT_FALSE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint16FullWrapPlusOne) {
  // Here, b is 2^15 + 1 greater than a, so we expect that a > b (i.e. we've
  // fully wrapped around)
  for (uint16_t a : aTests16) {
    uint16_t b = a + (1 << 15) + 1;

    EXPECT_TRUE(after(a, b));
    EXPECT_FALSE(after(b, a));

    EXPECT_TRUE(afterOrEqual(a, b));
    EXPECT_FALSE(afterOrEqual(b, a));

    EXPECT_FALSE(before(a, b));
    EXPECT_TRUE(before(b, a));

    EXPECT_FALSE(beforeOrEqual(a, b));
    EXPECT_TRUE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint32FullWrapMinusOne) {
  // Here, b is 2^31 - 1 greater than a, so we expect that b > a (i.e. haven't
  // wrapped yet)
  for (uint32_t a : aTests32) {
    uint32_t b = a + (1U << 31) - 1;

    EXPECT_FALSE(after(a, b));
    EXPECT_TRUE(after(b, a));

    EXPECT_FALSE(afterOrEqual(a, b));
    EXPECT_TRUE(afterOrEqual(b, a));

    EXPECT_TRUE(before(a, b));
    EXPECT_FALSE(before(b, a));

    EXPECT_TRUE(beforeOrEqual(a, b));
    EXPECT_FALSE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint32FullWrap) {
  // Here, b is 2^31 greater than a, so we expect that a > b /and/ b > a -- i.e.
  // things are undefined here. But also a >= b and b >= a will both be false.
  for (uint32_t a : aTests32) {
    uint32_t b = a + (1U << 31);

    EXPECT_TRUE(after(a, b));
    EXPECT_TRUE(after(b, a));

    EXPECT_FALSE(afterOrEqual(a, b));
    EXPECT_FALSE(afterOrEqual(b, a));

    EXPECT_TRUE(before(a, b));
    EXPECT_TRUE(before(b, a));

    EXPECT_FALSE(beforeOrEqual(a, b));
    EXPECT_FALSE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint32FullWrapPlusOne) {
  // Here, b is 2^31 + 1 greater than a, so we expect that a > b (i.e. we've
  // fully wrapped around)
  for (uint32_t a : aTests32) {
    uint32_t b = a + (1U << 31) + 1;

    EXPECT_TRUE(after(a, b));
    EXPECT_FALSE(after(b, a));

    EXPECT_TRUE(afterOrEqual(a, b));
    EXPECT_FALSE(afterOrEqual(b, a));

    EXPECT_FALSE(before(a, b));
    EXPECT_TRUE(before(b, a));

    EXPECT_FALSE(beforeOrEqual(a, b));
    EXPECT_TRUE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint64FullWrapMinusOne) {
  // Here, b is 2^63 - 1 greater than a, so we expect that b > a (i.e. haven't
  // wrapped yet)
  for (uint64_t a : aTests64) {
    uint64_t b = a + (1UL << 63) - 1;

    EXPECT_FALSE(after(a, b));
    EXPECT_TRUE(after(b, a));

    EXPECT_FALSE(afterOrEqual(a, b));
    EXPECT_TRUE(afterOrEqual(b, a));

    EXPECT_TRUE(before(a, b));
    EXPECT_FALSE(before(b, a));

    EXPECT_TRUE(beforeOrEqual(a, b));
    EXPECT_FALSE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint64FullWrap) {
  // Here, b is 2^63 greater than a, so we expect that a > b /and/ b > a -- i.e.
  // things are undefined here. But also a >= b and b >= a will both be false.
  for (uint64_t a : aTests64) {
    uint64_t b = a + (1UL << 63);

    EXPECT_TRUE(after(a, b));
    EXPECT_TRUE(after(b, a));

    EXPECT_FALSE(afterOrEqual(a, b));
    EXPECT_FALSE(afterOrEqual(b, a));

    EXPECT_TRUE(before(a, b));
    EXPECT_TRUE(before(b, a));

    EXPECT_FALSE(beforeOrEqual(a, b));
    EXPECT_FALSE(beforeOrEqual(b, a));
  }
}

TEST(WrapMath, Uint64FullWrapPlusOne) {
  // Here, b is 2^63 + 1 greater than a, so we expect that a > b (i.e. we've
  // fully wrapped around)
  for (uint64_t a : aTests64) {
    uint64_t b = a + (1UL << 63) + 1;

    EXPECT_TRUE(after(a, b));
    EXPECT_FALSE(after(b, a));

    EXPECT_TRUE(afterOrEqual(a, b));
    EXPECT_FALSE(afterOrEqual(b, a));

    EXPECT_FALSE(before(a, b));
    EXPECT_TRUE(before(b, a));

    EXPECT_FALSE(beforeOrEqual(a, b));
    EXPECT_TRUE(beforeOrEqual(b, a));
  }
}

} // namespace packsim
