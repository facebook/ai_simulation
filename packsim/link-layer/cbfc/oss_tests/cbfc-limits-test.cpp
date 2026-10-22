/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/cbfc-limits.h"

#include <gtest/gtest.h>

namespace packsim::cbfc {

TEST(CbfcLimitsTest, UecLimitsFollowTheSpec) {
  // Arrange
  constexpr CbfcLimits kExpected{
      .maxVcs = 32,
      .maxVcGroups = 32,
      .maxVcsPerGroup = 32,
      .minPktOvhdBytes = 0,
      .maxPktOvhdBytes = 127,
      .pktOvhdStepBytes = 1,
      .defaultCreditSizeBytes = 64,
  };

  // Act + Assert
  EXPECT_EQ(CbfcLimits::uec(), kExpected);
  // UEC 1.0.3 §5.2.10.1's must-support credit sizes are the powers of two
  // from 32 to 2048.
  constexpr uint32_t kCredit = CbfcLimits::uec().defaultCreditSizeBytes;
  EXPECT_TRUE(
      kCredit >= 32 && kCredit <= 2048 && (kCredit & (kCredit - 1)) == 0);
}

TEST(CbfcLimitsTest, PktOvhdIsAcceptedOnlyOnTheStepGrid) {
  // Arrange
  constexpr CbfcLimits kGrid{
      .maxVcs = 1,
      .maxVcGroups = 1,
      .maxVcsPerGroup = 1,
      .minPktOvhdBytes = 8,
      .maxPktOvhdBytes = 40,
      .pktOvhdStepBytes = 8,
      .defaultCreditSizeBytes = 64,
  };

  // Act + Assert
  EXPECT_TRUE(kGrid.isPktOvhdSupported(8));
  EXPECT_TRUE(kGrid.isPktOvhdSupported(40));
  EXPECT_FALSE(kGrid.isPktOvhdSupported(0)) << "below the minimum";
  EXPECT_FALSE(kGrid.isPktOvhdSupported(12)) << "off the step grid";
  EXPECT_FALSE(kGrid.isPktOvhdSupported(48)) << "above the maximum";
}

} // namespace packsim::cbfc
