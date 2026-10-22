/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/config-builders.h"

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

namespace packsim {

// The weights a DPR tiebreak puts on a fabric tier decide how traffic is split
// there, so these tests read the numbers out of the built topology rather than
// only checking which tiebreak flavour was selected.
class FlexTopologyTiebreakTest : public testing::Test {
 protected:
  // One zone holding a single rack of one host, so the accelerator tier stays
  // small and every test can name its switches directly.
  ClosTopologySpec topo_{
      .accelPerHost = 2,
      .accelPerRack = 2,
      .accelPerZone = 2,
      .zones = 1,
      .numCtsws = 1,
      .numAtsws = 1,
      .nominalRacksPerZone = 64};

  ClosLinks links_{
      .accelToRtsw = ClosLinkSpec{.speed = "200Gbps", .delayNs = 500},
      .rtswToCtsw =
          ClosLinkSpec{.speed = "200Gbps", .delayNs = 1000, .numLinks = 1},
      .ctswToAtsw =
          ClosLinkSpec{.speed = "400Gbps", .delayNs = 1000, .numLinks = 1}};

  uint32_t planes_{1};

  packsim::thrift::Simulation build(const std::string& routingStr) {
    return addFlexTopology(
        packsim::thrift::Simulation(), links_, topo_, routingStr, planes_);
  }

  // Throws when the route picked some other tiebreak flavour, which fails the
  // test with the flavour named: reading the dpr arm of a union holding
  // something else would instead raise an opaque thrift bad_field_access.
  static const packsim::thrift::DprTiebreakSpec& dprOfDefaultRoute(
      const packsim::thrift::Simulation& sim,
      const std::string& nodeName) {
    const auto& tiebreak = *sim.flex_topology()
                                ->nodes()
                                ->at(nodeName)
                                .lpmSpec()
                                ->defaultRoute()
                                ->tiebreak();
    if (tiebreak.getType() != packsim::thrift::TiebreakSpec::Type::dpr) {
      throw std::logic_error(
          fmt::format("default route of {} has no DPR tiebreak", nodeName));
    }
    return *tiebreak.dpr();
  }
};

TEST_F(FlexTopologyTiebreakTest, RackUplinkWeightsCarryTheAcceleratorFanout) {
  // The accelerator count per host is the only caller-supplied number in the
  // rack uplink weight vector; getting it wrong reweights every rack uplink.
  topo_.accelPerHost = 4;
  topo_.accelPerRack = 4;
  topo_.accelPerZone = 4;

  const auto sim = build("DPR");

  const auto& dpr = dprOfDefaultRoute(sim, "RTSW_z0_r0");
  EXPECT_EQ(*dpr.dotProduct(), (std::vector<uint32_t>{0, 0, 0, 4, 1}));
  // The rack uplink is never oversubscription-scaled, unlike the CTSW uplink.
  EXPECT_EQ(*dpr.divScale(), 1);
}

TEST_F(FlexTopologyTiebreakTest, SpineUplinkWeightsSelectThePlaneCoordinate) {
  // The weight vector is a constant in the builder, so the value itself is not
  // the interesting part: what this pins is the plumbing, that the CTSW default
  // route ends up carrying a DPR weighting whose only non-zero coordinate is
  // the third one, the plane index of the destination prefix.
  const auto sim = build("DPR");

  const auto& dpr = dprOfDefaultRoute(sim, "CTSW_z0_p0_c0");
  EXPECT_EQ(*dpr.dotProduct(), (std::vector<uint32_t>{0, 0, 1}));
}

TEST_F(FlexTopologyTiebreakTest, SpineUplinkScaleIsNominalRacksPerUplink) {
  // The fixture's nominal zone is 64 racks wide; the scale is how many of those
  // racks share one CTSW uplink. Four uplinks per ATSW across four ATSWs in the
  // plane is 16 uplinks, so each carries 4 racks' worth of traffic.
  links_.ctswToAtsw.numLinks = 4;
  topo_.numAtsws = 4;

  const auto sim = build("DPR");

  const auto& dpr = dprOfDefaultRoute(sim, "CTSW_z0_p0_c0");
  EXPECT_EQ(*dpr.divScale(), 4);
}

TEST_F(FlexTopologyTiebreakTest, SpineUplinkScaleCountsOnlyOnePlanesAtsws) {
  // Eight ATSWs reached over four links each would be 32 uplinks per CTSW, and
  // 64 / 32 is 2. Splitting them over two planes leaves each CTSW only the 16
  // uplinks of its own plane, so 4 here is what shows the divisor counts one
  // plane's ATSWs rather than all of them.
  links_.ctswToAtsw.numLinks = 4;
  topo_.numAtsws = 8;
  topo_.numCtsws = 2;
  planes_ = 2;

  const auto sim = build("DPR");

  const auto& dpr = dprOfDefaultRoute(sim, "CTSW_z0_p0_c0");
  EXPECT_EQ(*dpr.divScale(), 4);
}

TEST_F(FlexTopologyTiebreakTest, SpineUplinkScaleIsAtLeastOneOnAWideSpine) {
  // A spine with more uplinks than the nominal zone has racks would truncate
  // the per-uplink rack count to zero, and DPR divides by it.
  links_.ctswToAtsw.numLinks = 4;
  topo_.numAtsws = 100;

  const auto sim = build("DPR");

  const auto& dpr = dprOfDefaultRoute(sim, "CTSW_z0_p0_c0");
  EXPECT_EQ(*dpr.divScale(), 1);
}

TEST_F(
    FlexTopologyTiebreakTest,
    ASpineWithNoUplinksIsBuiltWithoutDividingByZero) {
  // The uplink count is the divisor of the oversubscription ratio, so a
  // spineless topology has to be guarded. An unguarded divide raises SIGFPE
  // and kills the process, so simply reaching the assertions below is the
  // claim being made; they only confirm the spine really was left unwired.
  links_.ctswToAtsw.numLinks = 0;

  const auto sim = build("DPR");

  // The rest of the fabric is still cabled, so an empty link map would make the
  // ATSW scan below pass vacuously.
  const auto& allLinks = *sim.flex_topology()->links();
  ASSERT_FALSE(allLinks.empty());
  for (const auto& [linkId, link] : allLinks) {
    EXPECT_NE(link.node2()->rfind("ATSW_", 0), 0u) << "link " << linkId;
  }
  const auto& ctsw = sim.flex_topology()->nodes()->at("CTSW_z0_p0_c0");
  EXPECT_TRUE(ctsw.lpmSpec()->defaultRoute()->links()->empty());
}

TEST_F(FlexTopologyTiebreakTest, DprSendsTrafficBackDownTheArrivalPlane) {
  // Under DPR the ATSW must reflect a packet down the plane it came up, so its
  // downlink tiebreak is a reflection rather than a hash.
  const auto sim = build("DPR");

  const auto& atsw = sim.flex_topology()->nodes()->at("ATSW_p0_a0");
  ASSERT_EQ(atsw.lpmSpec()->prefixTable()->size(), 1);
  const auto& route = atsw.lpmSpec()->prefixTable()->begin()->second;
  EXPECT_EQ(
      route.tiebreak()->getType(),
      packsim::thrift::TiebreakSpec::Type::reflect);
}

// The routing mode is matched case-insensitively, so a lower-case spelling
// selects the same tiebreak as the upper-case one instead of being rejected as
// an unsupported mode. dprOfDefaultRoute throws unless the DPR arm was taken,
// so reaching the assertion is itself the claim.
TEST_F(FlexTopologyTiebreakTest, RoutingModeIsMatchedRegardlessOfCase) {
  const auto sim = build("dpr");

  const auto& dpr = dprOfDefaultRoute(sim, "RTSW_z0_r0");
  EXPECT_EQ(*dpr.dotProduct(), (std::vector<uint32_t>{0, 0, 0, 2, 1}));
}

} // namespace packsim
