/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/vc-mapper.h"

#include <gtest/gtest.h>

#include "packsim/link-layer/cbfc/cbfc-types.h"

namespace packsim::cbfc {

namespace {
// VC1's rule matches DSCP CS3 (0x18) exactly; VC3's rule matches DSCP
// 0x20-0x27 (CS4 and the AF4x codepoints).
CbfcConfig makeConfig() {
  CbfcConfig cfg;
  cfg.numVcs = 4;
  cfg.losslessVc = {false, true, false, true};
  cfg.vcMap = {
      VcMapRule{VcSelector::DSCP, /*mask=*/0x3F, /*value=*/0x18, /*vcId=*/1},
      VcMapRule{VcSelector::PCP_DEI, /*mask=*/0x7, /*value=*/0x5, /*vcId=*/2},
      VcMapRule{VcSelector::DSCP, /*mask=*/0x38, /*value=*/0x20, /*vcId=*/3},
  };
  return cfg;
}
} // namespace

TEST(VcMapperTest, ExactDscpMatchSelectsVc) {
  const VcMapper mapper(makeConfig());
  EXPECT_EQ(
      std::optional<uint8_t>(1), mapper.vcFor(/*dscp=*/0x18, /*pcpDei=*/0));
}

TEST(VcMapperTest, PcpDeiMatchSelectsVc) {
  const VcMapper mapper(makeConfig());
  EXPECT_EQ(
      std::optional<uint8_t>(2), mapper.vcFor(/*dscp=*/0, /*pcpDei=*/0x5));
}

TEST(VcMapperTest, MaskMatchesClassOfDscpValues) {
  const VcMapper mapper(makeConfig());
  // AF41 is DSCP 0x22, and (0x22 & 0x38) == 0x20, so the third rule assigns
  // VC3.
  EXPECT_EQ(
      std::optional<uint8_t>(3), mapper.vcFor(/*dscp=*/0x22, /*pcpDei=*/0));
}

TEST(VcMapperTest, FirstMatchingRuleWins) {
  CbfcConfig cfg;
  cfg.numVcs = 2;
  cfg.losslessVc = {true, true};
  cfg.vcMap = {
      VcMapRule{VcSelector::DSCP, 0x3F, 0x18, /*vcId=*/0},
      VcMapRule{VcSelector::DSCP, 0x38, 0x18, /*vcId=*/1},
  };
  const VcMapper mapper(cfg);
  // 0x18 matches both rules; the first (VC0) wins.
  EXPECT_EQ(std::optional<uint8_t>(0), mapper.vcFor(0x18, 0));
}

TEST(VcMapperTest, NoMatchIsBestEffort) {
  const VcMapper mapper(makeConfig());
  EXPECT_EQ(std::nullopt, mapper.vcFor(/*dscp=*/0x01, /*pcpDei=*/0x0));
}

TEST(VcMapperTest, IsLosslessReflectsConfig) {
  const VcMapper mapper(makeConfig());
  EXPECT_FALSE(mapper.isLossless(0));
  EXPECT_TRUE(mapper.isLossless(1));
  EXPECT_FALSE(mapper.isLossless(2));
  EXPECT_TRUE(mapper.isLossless(3));
  // Out-of-range VC is best-effort, not a crash.
  EXPECT_FALSE(mapper.isLossless(99));
}

} // namespace packsim::cbfc
