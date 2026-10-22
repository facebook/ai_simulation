/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/cbfc-config.h"

#include <cstdint>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/topology_types.h"
#include "packsim/kernel/time.h"
#include "packsim/link-layer/cbfc/cbfc-types.h"

namespace packsim::cbfc {
namespace {

// 100 Gbps, the rate the CBFC link tests elsewhere in the tree use. Only
// carried through to `CbfcConfig::linkBps`, never validated.
constexpr uint64_t kTestLinkBps = 100'000'000'000ull;

class CbfcConfigTest : public ::testing::Test {
 protected:
  // numVcs 2 with VC1 lossless, one DSCP CS3 -> VC1 rule, explicit 64 B
  // credits. Every other field is left at its thrift default, which is valid.
  static packsim::thrift::CbfcConfig makeValidConfig() {
    packsim::thrift::CbfcConfig cfg;
    cfg.numVcs() = 2;
    cfg.isLosslessPerVc() = {false, true};
    packsim::thrift::VcMapEntry entry;
    entry.selector() = packsim::thrift::VcMapSelector::DSCP;
    entry.mask() = 0x3F;
    entry.value() = 0x18;
    entry.vcId() = 1;
    cfg.vcMap() = {entry};
    cfg.expectedCreditSizeBytes() = 64;
    return cfg;
  }

  // Matching the whole rendered message keeps each rejection pinned to the
  // check it covers, and to the value that check reports, rather than to
  // "something threw".
  static void expectRejected(
      const packsim::thrift::CbfcConfig& cfg,
      const std::string& expectedText) {
    EXPECT_THAT(
        [&] { std::ignore = makeCbfcConfig(cfg, kTestLinkBps); },
        ::testing::ThrowsMessage<std::invalid_argument>(
            ::testing::HasSubstr(expectedText)));
  }
};

TEST_F(CbfcConfigTest, TranslatesEveryScalarFieldIntoTheRuntimeConfig) {
  // Arrange. Every field is set away from its thrift default so each
  // assertion below is on a value that was carried across, not on a default
  // that both sides happen to share.
  auto cfg = makeValidConfig();
  cfg.pktOvhdBytes() = 32;
  cfg.cfMinTimerBytes() = 1'024;
  cfg.cfMaxTimerBytes() = 2 * kCfMaxTimerMinBytes;
  cfg.ccMsgTimerNs() = 5'000;
  cfg.cfCreditThresholdCredits() = 7;
  cfg.overshootReserveCredits() = 9;
  cfg.vcMinCredits() = {3, 5};

  // Act.
  const CbfcConfig out = makeCbfcConfig(cfg, kTestLinkBps);

  // Assert.
  EXPECT_EQ(2, out.numVcs);
  EXPECT_EQ(std::vector<bool>({false, true}), out.losslessVc);
  EXPECT_EQ(64u, out.creditSizeBytes);
  EXPECT_EQ(32u, out.pktOvhdBytes);
  EXPECT_EQ(1'024u, out.cfMinTimerBytes);
  EXPECT_EQ(2u * kCfMaxTimerMinBytes, out.cfMaxTimerBytes);
  EXPECT_EQ(Time::fromNs(5'000), out.ccMsgTimer);
  EXPECT_EQ(7u, out.cfCreditThresholdCredits);
  EXPECT_EQ(9u, out.overshootReserveCredits);
  EXPECT_EQ(std::vector<uint64_t>({3u, 5u}), out.vcMinCredits);
  // The struct's own default is 800 Gbps, so this pins that the caller's rate
  // reaches the runtime config rather than being silently dropped.
  EXPECT_EQ(kTestLinkBps, out.linkBps);
}

TEST_F(CbfcConfigTest, TranslatesTheVcMapRulesIntoTheRuntimeConfig) {
  // Arrange.
  const auto cfg = makeValidConfig();

  // Act.
  const CbfcConfig out = makeCbfcConfig(cfg, kTestLinkBps);

  // Assert.
  ASSERT_EQ(1u, out.vcMap.size());
  EXPECT_EQ(VcSelector::DSCP, out.vcMap.at(0).selector);
  EXPECT_EQ(0x3Fu, out.vcMap.at(0).mask);
  EXPECT_EQ(0x18u, out.vcMap.at(0).value);
  EXPECT_EQ(1u, out.vcMap.at(0).vcId);
}

// Group ids are the entries' indices, so the whole tier has to be checked
// through the translated struct: nothing downstream reports it.
TEST_F(CbfcConfigTest, TranslatesVcGroupsIntoTheRuntimeConfig) {
  // Arrange. One group covering both VCs, which satisfies "every lossless VC
  // belongs to a group" once any group exists.
  auto cfg = makeValidConfig();
  packsim::thrift::VcGrpEntry group;
  group.vcIds() = {0, 1};
  group.limitCredits() = 4'096;
  cfg.vcGroups() = {group};

  // Act.
  const CbfcConfig out = makeCbfcConfig(cfg, kTestLinkBps);

  // Assert.
  ASSERT_EQ(1u, out.vcGroups.size());
  EXPECT_EQ(std::vector<uint8_t>({0, 1}), out.vcGroups.at(0).vcIds);
  EXPECT_EQ(4'096u, out.vcGroups.at(0).limitCredits);
}

// 0 is the thrift "derive from the receiver" sentinel. The runtime config must
// never hold it: the credit charge divides by this field.
TEST_F(CbfcConfigTest, ResolvesTheZeroCreditSizeSentinelToTheDefaultSize) {
  // Arrange.
  auto cfg = makeValidConfig();
  cfg.expectedCreditSizeBytes() = 0;

  // Act.
  const CbfcConfig out = makeCbfcConfig(cfg, kTestLinkBps);

  // Assert.
  EXPECT_EQ(kCbfcLimits.defaultCreditSizeBytes, out.creditSizeBytes);
}

// The ceiling is the 12-bit S_CreditSize wire field, so the largest
// expressible size must survive translation unchanged rather than be rounded
// or rejected off by one.
TEST_F(CbfcConfigTest, AcceptsTheLargestCreditSizeTheWireFieldCanCarry) {
  // Arrange.
  auto cfg = makeValidConfig();
  cfg.expectedCreditSizeBytes() = kMaxCreditSizeBytes;

  // Act.
  const CbfcConfig out = makeCbfcConfig(cfg, kTestLinkBps);

  // Assert.
  EXPECT_EQ(kMaxCreditSizeBytes, out.creditSizeBytes);
}

// One past the 12-bit S_CreditSize field.
TEST_F(CbfcConfigTest, RejectsACreditSizeOnePastTheWireFieldCeiling) {
  // Arrange.
  auto cfg = makeValidConfig();
  cfg.expectedCreditSizeBytes() = kMaxCreditSizeBytes + 1;

  // Act + Assert.
  expectRejected(
      cfg,
      "CbfcConfig.expectedCreditSizeBytes (4096) is outside 0..4095 "
      "(0 = derive)");
}

// The rule's VC id subscripts the per-VC state, so an id at or past numVcs
// must not reach the runtime config.
TEST_F(CbfcConfigTest, RejectsAVcMapRuleTargetingAVcIdAtNumVcs) {
  // Arrange.
  auto cfg = makeValidConfig(); // numVcs is 2, so 2 is one past the end
  cfg.vcMap()->at(0).vcId() = 2;

  // Act + Assert.
  expectRejected(cfg, "CbfcConfig.vcMap has VC id 2 outside 0..1");
}

// DSCP is 6 bits: 0x3F is the widest legal mask and 0x40 the first value that
// cannot be matched, so a rule carrying it can never fire.
TEST_F(CbfcConfigTest, RejectsADscpVcMapValueAboveTheSixBitField) {
  // Arrange.
  auto cfg = makeValidConfig();
  cfg.vcMap()->at(0).value() = 0x40;

  // Act + Assert.
  expectRejected(
      cfg, "CbfcConfig.vcMap value 64 is outside 0..63 for selector DSCP");
}

// The width the rule is checked against is selector-dependent: PCP/DEI is 4
// bits, so the 0x3F mask that is legal for DSCP is out of range here.
TEST_F(CbfcConfigTest, RejectsADscpWidthMaskOnAPcpDeiRule) {
  // Arrange. `value` is pulled inside the 4-bit field so the mask is the only
  // out-of-range field and the check order cannot decide the outcome.
  auto cfg = makeValidConfig();
  cfg.vcMap()->at(0).selector() = packsim::thrift::VcMapSelector::PCP_DEI;
  cfg.vcMap()->at(0).value() = 0x4;

  // Act + Assert.
  expectRejected(
      cfg, "CbfcConfig.vcMap mask 63 is outside 0..15 for selector PCP_DEI");
}

// The array is indexed by VC id. A length that disagrees with numVcs would
// leave the VCs past its end with no reserve and no diagnostic.
TEST_F(CbfcConfigTest, RejectsAVcMinCreditsArrayThatDoesNotCoverNumVcs) {
  // Arrange.
  auto cfg = makeValidConfig(); // numVcs is 2
  cfg.vcMinCredits() = {4};

  // Act + Assert.
  expectRejected(cfg, "CbfcConfig.vcMinCredits has 1 entries but numVcs is 2");
}

// Empty is the documented "no VC has a reserve" default and must stay exempt
// from the length check.
TEST_F(CbfcConfigTest, AcceptsAnEmptyVcMinCreditsAsTheNoReserveDefault) {
  // Arrange.
  auto cfg = makeValidConfig();
  cfg.vcMinCredits() = {};

  // Act.
  const CbfcConfig out = makeCbfcConfig(cfg, kTestLinkBps);

  // Assert.
  EXPECT_TRUE(out.vcMinCredits.empty());
}

// PackSim floors CF_min at 800 B of wire time.
TEST_F(CbfcConfigTest, RejectsACfMinTimerBelowTheSupportedFloor) {
  // Arrange.
  auto cfg = makeValidConfig();
  cfg.cfMinTimerBytes() = kCfMinTimerMinBytes - 1;

  // Act + Assert.
  expectRejected(
      cfg,
      "CbfcConfig.cfMinTimerBytes (799) is below PackSim's floor of 800 B of "
      "wire time");
}

// CF_min is the floor on CF_Update spacing and CF_max the ceiling, so
// inverting them makes the pair unsatisfiable: reports would come out at the
// CF_min spacing while the config claims CF_max.
TEST_F(CbfcConfigTest, RejectsACfMinTimerAboveCfMax) {
  // Arrange. CF_max stays in range and on the 16 kB granularity, so this can
  // only trip the inversion check.
  auto cfg = makeValidConfig();
  cfg.cfMaxTimerBytes() = kCfMaxTimerMinBytes;
  cfg.cfMinTimerBytes() = kCfMaxTimerMinBytes + 1;

  // Act + Assert.
  expectRejected(
      cfg,
      "CbfcConfig.cfMinTimerBytes (16385) exceeds cfMaxTimerBytes (16384)");
}

// Tbl 5-27 caps CF_max at 1 MB. The existing coverage is all at the low end,
// so a regression that dropped the upper bound would go unnoticed.
TEST_F(CbfcConfigTest, RejectsACfMaxTimerAboveTheSpecCeiling) {
  // Arrange. Still a multiple of the 16 kB granularity, so only the range
  // check can reject it.
  auto cfg = makeValidConfig();
  cfg.cfMaxTimerBytes() = kCfMaxTimerMaxBytes + kCfMaxTimerMinBytes;

  // Act + Assert.
  expectRejected(
      cfg,
      "CbfcConfig.cfMaxTimerBytes (1064960) is outside the UEC 1.0.3 "
      "Tbl 5-27 range 16384..1048576 B of wire time");
}

// PackSim caps CC_msg at 250 ms.
TEST_F(CbfcConfigTest, RejectsACcMsgTimerAboveTheSupportedCeiling) {
  // Arrange.
  auto cfg = makeValidConfig();
  cfg.ccMsgTimerNs() = kCcMsgTimerMaxNs + 1;

  // Act + Assert.
  expectRejected(
      cfg,
      "CbfcConfig.ccMsgTimerNs (250000001) is outside the range 1000 ns (UEC "
      "1.0.3 Tbl 5-27 floor) to 250000000 ns (PackSim ceiling)");
}

// 1 us is the Tbl 5-27 floor and must translate exactly rather than be
// rejected off by one.
TEST_F(CbfcConfigTest, AcceptsTheSmallestCcMsgTimerTheSpecAllows) {
  // Arrange.
  auto cfg = makeValidConfig();
  cfg.ccMsgTimerNs() = kCcMsgTimerMinNs;

  // Act.
  const CbfcConfig out = makeCbfcConfig(cfg, kTestLinkBps);

  // Assert.
  EXPECT_EQ(Time::fromNs(kCcMsgTimerMinNs), out.ccMsgTimer);
}

// 250 ms in ns is PackSim's ceiling and the largest value the conversion to
// `Time` has to carry.
TEST_F(CbfcConfigTest, AcceptsTheLargestSupportedCcMsgTimer) {
  // Arrange.
  auto cfg = makeValidConfig();
  cfg.ccMsgTimerNs() = kCcMsgTimerMaxNs;

  // Act.
  const CbfcConfig out = makeCbfcConfig(cfg, kTestLinkBps);

  // Assert.
  EXPECT_EQ(Time::fromNs(kCcMsgTimerMaxNs), out.ccMsgTimer);
}

} // namespace
} // namespace packsim::cbfc
