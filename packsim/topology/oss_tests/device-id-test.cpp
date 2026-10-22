/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/kernel/device-id.h"

namespace packsim {

TEST(DeviceIdTests, FlexIdToFromVector) {
  std::vector<std::vector<uint32_t>> vecs = {
      {1},
      {555, 12345},
      {2, 0, 3853},
      {12, 24, 16300, 1, 5},
      {5, 0, 0, 0, 0, 1}};
  for (const auto& vec : vecs) {
    FlexDeviceId devId(vec);
    auto newVec = devId.asVec();
    newVec.resize(vec.size());
    EXPECT_EQ(vec, newVec);
  }
}

TEST(DeviceIdTests, FlexIdToString) {
  FlexDeviceId devId({1});
  EXPECT_EQ(devId.toString(), "1::");
  FlexDeviceId devId2({123, 17, 0, 1});
  EXPECT_EQ(devId2.toString(), "7b:11:0:1::");
}

} // namespace packsim
