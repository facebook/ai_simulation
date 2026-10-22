/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/kernel/prefix-id-utils.h"

namespace packsim {

TEST(MakePrefixKey, JoinsInitializerListWithDots) {
  EXPECT_EQ(makePrefixKey({1, 2, 3, 4}), "1.2.3.4");
}

TEST(MakePrefixKey, SingleElementHasNoTrailingDot) {
  EXPECT_EQ(makePrefixKey({5}), "5");
}

TEST(MakePrefixKey, VectorOverloadMatchesInitializerList) {
  const std::vector<uint32_t> vals{7, 8, 9};
  EXPECT_EQ(makePrefixKey(vals), "7.8.9");
  EXPECT_EQ(makePrefixKey(vals), makePrefixKey({7, 8, 9}));
}

TEST(MakePrefixKey, EmptyVectorIsEmptyString) {
  const std::vector<uint32_t> empty;
  EXPECT_EQ(makePrefixKey(empty), "");
}

} // namespace packsim
