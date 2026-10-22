/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/config-builders.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using packsim::addFlexTopology;
using packsim::ClosLinks;
using packsim::ClosLinkSpec;
using packsim::ClosTopologySpec;
using packsim::switchParamsToConfig;
using packsim::wedge100CfgToThrift;

namespace packsim {

namespace {

// A switch whose port counts divide evenly by its buffer count, which is the
// precondition switchParamsToConfig enforces before sizing anything.
packsim::thrift::SwitchParams makeSwitchParams() {
  packsim::thrift::SwitchParams cfg;
  cfg.isWedge100() = false;
  cfg.numBuffs() = 2U;
  cfg.numUplinks() = 8U;
  cfg.numDownlinks() = 8U;
  cfg.cellSizeBytes() = 254U;
  cfg.bufSizeBytes() = 64UL * 1024 * 1024;
  cfg.rsvdBytesPerPort() = 4200U;
  cfg.xonHystBytes() = 8U * 1024;
  cfg.hdrmBytesPerPortUl() = 64U * 1024;
  cfg.hdrmBytesPerPortDl() = 32U * 1024;
  cfg.hdrmPoolPerBuff() = 0U;
  cfg.alpha() = 0.25;
  cfg.egrAlpha() = 0.25;
  cfg.queueProfileType() = packsim::thrift::QueueProfileType::NONE;
  cfg.queueSchedulerType() = packsim::thrift::SchedulingAlgorithmType::RR;
  cfg.numQueues() = 1U;
  // One entry per queue: the builder rejects a mismatch outright.
  cfg.isLosslessPerPg() = {false};
  cfg.dynShareEn() = true;
  cfg.egrDynShareEn() = true;
  cfg.rsvdBytesPerPortPg() = 4200U;
  cfg.rsvdBytesPerEgrQueue() = 4200U;
  cfg.egrResumeHystBytes() = 8U * 1024;
  return cfg;
}

// The smallest Clos topology addFlexTopology will build: one zone, one rack
// of one host, two accelerators, and one CTSW/ATSW per plane.
ClosTopologySpec makeTopologySpec() {
  ClosTopologySpec topo;
  topo.accelPerHost = 2;
  topo.accelPerRack = 2;
  topo.accelPerZone = 2;
  topo.zones = 1;
  topo.numCtsws = 1;
  topo.numAtsws = 1;
  return topo;
}

ClosLinks makeLinks() {
  ClosLinks links;
  links.accelToRtsw = ClosLinkSpec{.speed = "200Gbps", .delayNs = 500};
  links.rtswToCtsw = ClosLinkSpec{.speed = "200Gbps", .delayNs = 1000};
  links.ctswToAtsw = ClosLinkSpec{.speed = "400Gbps", .delayNs = 1000};
  return links;
}

packsim::thrift::Simulation buildTopology(
    const std::string& routingStr = "MIXED",
    uint32_t planes = 1) {
  return addFlexTopology(
      packsim::thrift::Simulation(),
      makeLinks(),
      makeTopologySpec(),
      routingStr,
      planes);
}

// How many links of the built topology land on `node`'s `portGroup`.
uint32_t countPortGroupLinks(
    const packsim::thrift::Simulation& sim,
    const std::string& node,
    const std::string& portGroup) {
  uint32_t count = 0;
  for (const auto& [linkId, link] : *sim.flex_topology()->links()) {
    if ((*link.node1() == node && *link.node1PortGroup() == portGroup) ||
        (*link.node2() == node && *link.node2PortGroup() == portGroup)) {
      ++count;
    }
  }
  return count;
}

// How many nodes of a tier the built topology holds, keyed by the name prefix
// the builders use ("Accel_", "RTSW_", "CTSW_", "ATSW_").
size_t countNodes(
    const packsim::thrift::Simulation& sim,
    const std::string& prefix) {
  size_t count = 0;
  for (const auto& [name, node] : *sim.flex_topology()->nodes()) {
    if (name.rfind(prefix, 0) == 0) {
      ++count;
    }
  }
  return count;
}

} // namespace

TEST(SwitchParamsToConfigTest, OneConfigPerBuffer) {
  const auto cfg = switchParamsToConfig(makeSwitchParams());

  EXPECT_EQ(cfg.bufs()->size(), 2U);
}

TEST(SwitchParamsToConfigTest, EachBufferGetsAnEqualShareOfTheSwitchBytes) {
  // The switch's bytes are divided by the buffer count and only then rounded up
  // to whole cells. Rounding first and dividing after would give a different
  // (larger) per-buffer pool, so the expectation is computed in that order from
  // the fixture's own inputs rather than read off the implementation.
  const auto params = makeSwitchParams();
  const uint64_t bytesPerBuffer = *params.bufSizeBytes() / *params.numBuffs();
  const uint64_t cellSize = *params.cellSizeBytes();
  const uint64_t expectedCellsPerBuffer =
      (bytesPerBuffer + cellSize - 1) / cellSize;

  const auto cfg = switchParamsToConfig(params);

  EXPECT_EQ(*cfg.bufs()->at(0).totalCells(), expectedCellsPerBuffer);
  EXPECT_EQ(*cfg.bufs()->at(1).totalCells(), expectedCellsPerBuffer);
}

TEST(SwitchParamsToConfigTest, TheCellSizeIsCarriedOntoEveryBuffer) {
  auto params = makeSwitchParams();
  params.cellSizeBytes() = 512U;

  const auto cfg = switchParamsToConfig(params);

  for (const auto& buf : *cfg.bufs()) {
    EXPECT_EQ(*buf.cellSizeBytes(), 512U);
  }
}

TEST(SwitchParamsToConfigTest, PortCountsMustDivideByTheBufferCount) {
  // An uneven split would leave one buffer serving more ports than another,
  // which the per-buffer maths below has no way to express.
  auto params = makeSwitchParams();
  params.numBuffs() = 3U;

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::invalid_argument>(testing::StrEq(
          "numUplinks (8) and numDownlinks (8) must each be divisible by "
          "numBuffs (3)")));
}

TEST(SwitchParamsToConfigTest, AnUnevenUplinkSplitIsRejected) {
  // The downlinks split evenly over the two buffers, but the third uplink
  // would have no buffer.
  auto params = makeSwitchParams();
  params.numUplinks() = 3U;

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::invalid_argument>(testing::StrEq(
          "numUplinks (3) and numDownlinks (8) must each be divisible by "
          "numBuffs (2)")));
}

TEST(SwitchParamsToConfigTest, AnUnevenDownlinkSplitIsRejected) {
  // The uplinks split evenly over the two buffers, but the third downlink
  // would have no buffer.
  auto params = makeSwitchParams();
  params.numDownlinks() = 3U;

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::invalid_argument>(testing::StrEq(
          "numUplinks (8) and numDownlinks (3) must each be divisible by "
          "numBuffs (2)")));
}

TEST(SwitchParamsToConfigTest, FewerPortsThanBuffersAreRejected) {
  // With dynamic sharing off, a port's static share is the shared cells
  // divided by the ports per buffer, of which there would be none.
  auto params = makeSwitchParams();
  params.numUplinks() = 1U;
  params.numDownlinks() = 0U;
  params.dynShareEn() = false;

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::invalid_argument>(testing::StrEq(
          "numUplinks (1) and numDownlinks (0) must each be divisible by "
          "numBuffs (2)")));
}

// Lossy mode (no lossless PG) has no headroom pool, but its reserved cells must
// still fit in the buffer.
TEST(SwitchParamsToConfigTest, LossyReservedCellsAboveTheBufferAreRejected) {
  auto params = makeSwitchParams();
  ASSERT_EQ(params.isLosslessPerPg()->at(0), false) << "precondition: lossy";
  // 32 MiB per buffer is 132105 cells of 254 B; 8 ports reserving 4 MiB each
  // (16514 cells) need 132112.
  params.rsvdBytesPerPort() = 4U * 1024 * 1024;

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::invalid_argument>(testing::HasSubstr(
          "Headroom pool (0 cells) plus reserved cells (16514 per port x 8 "
          "ports) exceed the 132105 cells of each buffer")));
}

// Port numbers are `uint16_t`, so a switch with more ports than that range
// holds is rejected before any port is numbered.
TEST(SwitchParamsToConfigTest, MorePortsThanAUint16HoldsAreRejected) {
  auto params = makeSwitchParams();
  params.numUplinks() = 32768U;
  params.numDownlinks() = 32768U;

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::range_error>(testing::HasSubstr("65536")));
}

// A port-count sum that does not fit 32 bits is rejected too, rather than
// wrapping to a small port count that passes the per-buffer divisibility check.
TEST(SwitchParamsToConfigTest, PortCountsWhoseSumExceeds32BitsAreRejected) {
  auto params = makeSwitchParams();
  params.numBuffs() = 1U;
  params.numUplinks() = std::numeric_limits<uint32_t>::max();
  params.numDownlinks() = 1U;

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::range_error>(
          testing::HasSubstr("4294967296")));
}

// Cell counts are 32-bit config fields, so a buffer with more cells than that
// is rejected rather than truncated.
TEST(SwitchParamsToConfigTest, ABufferWithMoreCellsThanAUint32HoldsIsRejected) {
  auto params = makeSwitchParams();
  params.cellSizeBytes() = 1U;
  params.bufSizeBytes() = uint64_t{1} << 34;

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::runtime_error>(
          testing::HasSubstr("ceilDivide32 overflowed")));
}

TEST(SwitchParamsToConfigTest, AWedge100SwitchTakesItsOwnBuilder) {
  // The wedge100 path is a separate sizing model, so the generic builder has
  // to hand off rather than apply its own. The hand-off is observed through an
  // error only the wedge100 builder raises -- the generic path has no opinion
  // about lossless PGs, so seeing this message proves the delegation happened.
  auto params = makeSwitchParams();
  params.isWedge100() = true;
  params.isLosslessPerPg() = {true};

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::runtime_error>(
          testing::HasSubstr("Wedge100 lossless mode not supported")));
  EXPECT_THAT(
      [&params]() { wedge100CfgToThrift(params); },
      testing::ThrowsMessage<std::runtime_error>(
          testing::HasSubstr("Wedge100 lossless mode not supported")));
}

TEST(SwitchParamsToConfigTest, ZeroBuffersAreRejected) {
  // Every per-buffer figure below is divided by the buffer count.
  auto params = makeSwitchParams();
  params.numBuffs() = 0;

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::invalid_argument>(
          testing::StrEq("numBuffs must be greater than 0")));
}

TEST(SwitchParamsToConfigTest, ASwitchWithNoPortsIsRejected) {
  // A static share of the buffer is the shared cells divided by the ports.
  auto params = makeSwitchParams();
  params.numUplinks() = 0;
  params.numDownlinks() = 0;

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::invalid_argument>(
          testing::StrEq("numUplinks + numDownlinks must be greater than 0")));
}

TEST(SwitchParamsToConfigTest, AZeroCellSizeIsRejected) {
  // Buffer sizes are converted to cells by dividing by the cell size.
  auto params = makeSwitchParams();
  params.cellSizeBytes() = 0;

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::invalid_argument>(
          testing::StrEq("cellSizeBytes must be greater than 0")));
}

TEST(SwitchParamsToConfigTest, DropAndMarkPercentagesAreCopiedIntoTheQueues) {
  // Neither value is WredEcnConfig's default of 100, so a field the builder
  // leaves unset fails the test.
  auto params = makeSwitchParams();
  params.dropProbWRED() = 40;
  params.markProbECN() = 15;

  const auto cfg = switchParamsToConfig(params);

  const auto& wredEcnCfg =
      *cfg.bufs()->at(0).ports()->at(0).queueCfg()->at(0).wredEcnCfg();
  EXPECT_EQ(*wredEcnCfg.dropRateWRED(), 40U);
  EXPECT_EQ(*wredEcnCfg.markRateECN(), 15U);
}

TEST(SwitchParamsToConfigTest, AOneHundredPercentageIsAccepted) {
  auto params = makeSwitchParams();
  params.dropProbWRED() = 100;
  params.markProbECN() = 100;

  EXPECT_NO_THROW(switchParamsToConfig(params));
}

// WredEcnConfig holds whole percentages, and a fraction such as 0.5 is more
// likely a probability than half a percent.
TEST(SwitchParamsToConfigTest, AFractionalPercentageIsRejected) {
  auto params = makeSwitchParams();
  params.markProbECN() = 0.5;

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::invalid_argument>(testing::StrEq(
          "SwitchParams.markProbECN must be a whole percentage from 0 to "
          "100, got 0.5")));
}

TEST(SwitchParamsToConfigTest, APercentageAboveOneHundredIsRejected) {
  auto params = makeSwitchParams();
  params.dropProbWRED() = 101;

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::invalid_argument>(testing::StrEq(
          "SwitchParams.dropProbWRED must be a whole percentage from 0 to "
          "100, got 101")));
}

TEST(SwitchParamsToConfigTest, ANegativePercentageIsRejected) {
  auto params = makeSwitchParams();
  params.markProbECN() = -1;

  EXPECT_THAT(
      [&params]() { switchParamsToConfig(params); },
      testing::ThrowsMessage<std::invalid_argument>(testing::StrEq(
          "SwitchParams.markProbECN must be a whole percentage from 0 to "
          "100, got -1")));
}

TEST(Wedge100CfgToThriftTest, ABufferCountOtherThanFourIsRejected) {
  // The wedge100 port map assigns every ingress and egress port pair to one
  // of exactly four buffers.
  auto params = makeSwitchParams();
  params.isWedge100() = true;
  params.numBuffs() = 2;

  EXPECT_THAT(
      [&params]() { wedge100CfgToThrift(params); },
      testing::ThrowsMessage<std::invalid_argument>(
          testing::StrEq("wedge100 requires numBuffs == 4")));
}

TEST(Wedge100CfgToThriftTest, AZeroCellSizeIsRejected) {
  auto params = makeSwitchParams();
  params.isWedge100() = true;
  params.numBuffs() = 4;
  params.cellSizeBytes() = 0;

  EXPECT_THAT(
      [&params]() { wedge100CfgToThrift(params); },
      testing::ThrowsMessage<std::invalid_argument>(
          testing::StrEq("cellSizeBytes must be greater than 0")));
}

TEST(Wedge100CfgToThriftTest, PercentagesAreCopiedIntoIngressPortQueues) {
  // Buffer 0 takes port 0 on both sides; its queue configs come from the
  // ingress side.
  auto params = makeSwitchParams();
  params.isWedge100() = true;
  params.numBuffs() = 4;
  params.dropProbWRED() = 40;
  params.markProbECN() = 15;

  const auto cfg = wedge100CfgToThrift(params);

  const auto& wredEcnCfg =
      *cfg.bufs()->at(0).ports()->at(0).queueCfg()->at(0).wredEcnCfg();
  EXPECT_EQ(*wredEcnCfg.dropRateWRED(), 40U);
  EXPECT_EQ(*wredEcnCfg.markRateECN(), 15U);
}

TEST(Wedge100CfgToThriftTest, PercentagesAreCopiedIntoEgressOnlyPortQueues) {
  // Buffer 0 takes port 32 on its egress side only, where the builder writes
  // the queue configs separately.
  auto params = makeSwitchParams();
  params.isWedge100() = true;
  params.numBuffs() = 4;
  params.dropProbWRED() = 40;
  params.markProbECN() = 15;

  const auto cfg = wedge100CfgToThrift(params);

  const auto& wredEcnCfg =
      *cfg.bufs()->at(0).ports()->at(32).queueCfg()->at(0).wredEcnCfg();
  EXPECT_EQ(*wredEcnCfg.dropRateWRED(), 40U);
  EXPECT_EQ(*wredEcnCfg.markRateECN(), 15U);
}

TEST(Wedge100CfgToThriftTest, AFractionalPercentageIsRejected) {
  auto params = makeSwitchParams();
  params.isWedge100() = true;
  params.numBuffs() = 4;
  params.dropProbWRED() = 0.5;

  EXPECT_THAT(
      [&params]() { wedge100CfgToThrift(params); },
      testing::ThrowsMessage<std::invalid_argument>(testing::StrEq(
          "SwitchParams.dropProbWRED must be a whole percentage from 0 to "
          "100, got 0.5")));
}

TEST(AddFlexTopologyTest, ZeroAcceleratorsPerHostAreRejected) {
  // Hosts per rack is accelPerRack / accelPerHost.
  auto topo = makeTopologySpec();
  topo.accelPerHost = 0;

  EXPECT_THAT(
      [&topo]() {
        addFlexTopology(
            packsim::thrift::Simulation(), makeLinks(), topo, "MIXED", 1);
      },
      testing::ThrowsMessage<std::invalid_argument>(
          testing::HasSubstr("got accelPerHost 0, accelPerRack 2, planes 1")));
}

TEST(AddFlexTopologyTest, ARackSmallerThanAHostIsRejected) {
  // A rack must hold at least one host, or racks per zone divides by zero.
  auto topo = makeTopologySpec();
  topo.accelPerRack = 1;

  EXPECT_THAT(
      [&topo]() {
        addFlexTopology(
            packsim::thrift::Simulation(), makeLinks(), topo, "MIXED", 1);
      },
      testing::ThrowsMessage<std::invalid_argument>(
          testing::HasSubstr("got accelPerHost 2, accelPerRack 1, planes 1")));
}

TEST(AddFlexTopologyTest, ZeroPlanesAreRejected) {
  // Switches per plane divide the switch counts by the plane count.
  EXPECT_THAT(
      []() { buildTopology("MIXED", /*planes=*/0); },
      testing::ThrowsMessage<std::invalid_argument>(
          testing::HasSubstr("got accelPerHost 2, accelPerRack 2, planes 0")));
}

TEST(WiredSpinePortsTest, MatchesTheLinksAddFlexTopologyWires) {
  // Four zones of 8 racks, 8 CTSWs per zone and 4 ATSWs, over 2 planes.
  const ClosTopologySpec topo;
  auto links = makeLinks();
  links.ctswToAtsw.numLinks = 2;
  constexpr uint32_t kPlanes = 2;

  const ClosSpinePorts ports = wiredSpinePorts(links, topo, kPlanes);
  const auto sim = addFlexTopology(
      packsim::thrift::Simulation(), links, topo, "MIXED", kPlanes);

  EXPECT_EQ(ports.ctsw.uplinks, 4u);
  EXPECT_EQ(ports.ctsw.downlinks, 8u);
  EXPECT_EQ(ports.atsw.uplinks, 0u);
  EXPECT_EQ(ports.atsw.downlinks, 32u);
  EXPECT_EQ(
      countPortGroupLinks(sim, "CTSW_z0_p0_c0", "uplinks"), ports.ctsw.uplinks);
  EXPECT_EQ(
      countPortGroupLinks(sim, "CTSW_z0_p0_c0", "downlinks"),
      ports.ctsw.downlinks);
  EXPECT_EQ(
      countPortGroupLinks(sim, "ATSW_p0_a0", "downlinks"),
      ports.atsw.downlinks);
}

TEST(WiredSpinePortsTest, RejectsTheTopologiesAddFlexTopologyRejects) {
  auto topo = makeTopologySpec();
  topo.accelPerHost = 0;

  EXPECT_THAT(
      [&topo]() { wiredSpinePorts(makeLinks(), topo, /*planes=*/1); },
      testing::ThrowsMessage<std::invalid_argument>(
          testing::HasSubstr("got accelPerHost 0, accelPerRack 2, planes 1")));
}

TEST(AddFlexTopologyTest, EveryTierIsPopulatedAtTheRequestedSize) {
  const auto sim = buildTopology();

  EXPECT_EQ(countNodes(sim, "Accel_"), 2);
  EXPECT_EQ(countNodes(sim, "RTSW_"), 1);
  EXPECT_EQ(countNodes(sim, "CTSW_"), 1);
  EXPECT_EQ(countNodes(sim, "ATSW_"), 1);
}

TEST(AddFlexTopologyTest, TheCallersSimulationIsNotModified) {
  // Callers layer this onto a simulation they keep, so the builder has to
  // return a copy rather than mutate its argument.
  packsim::thrift::Simulation original;

  const auto built =
      addFlexTopology(original, makeLinks(), makeTopologySpec(), "MIXED", 1);

  EXPECT_TRUE(original.flex_topology()->nodes()->empty());
  EXPECT_FALSE(built.flex_topology()->nodes()->empty());
}

TEST(AddFlexTopologyTest, EveryAcceleratorIsWiredToItsRackSwitch) {
  const auto sim = buildTopology();

  size_t accelToRtsw = 0;
  for (const auto& [linkId, link] : *sim.flex_topology()->links()) {
    if (link.node1()->rfind("Accel_", 0) == 0 &&
        link.node2()->rfind("RTSW_", 0) == 0) {
      ++accelToRtsw;
    }
  }
  EXPECT_EQ(accelToRtsw, 2);
}

TEST(AddFlexTopologyTest, LinkIdsAreUniqueAcrossTheWholeFabric) {
  // The link id is the key of the links map and is also written into each
  // node's LPM entries, so a collision would silently reroute traffic.
  const auto sim = buildTopology();

  for (const auto& [linkId, link] : *sim.flex_topology()->links()) {
    EXPECT_EQ(linkId, *link.linkId());
  }
  EXPECT_FALSE(sim.flex_topology()->links()->empty());
}

TEST(AddFlexTopologyTest, EveryLinkCarriesItsTiersSpeedAndDelay) {
  const auto sim = buildTopology();

  ASSERT_FALSE(sim.flex_topology()->links()->empty());
  for (const auto& [linkId, link] : *sim.flex_topology()->links()) {
    EXPECT_FALSE(link.speed()->empty());
    // A link with no delay config reaches the simulator as a zero-latency
    // wire, which silently changes every result.
    EXPECT_GT(*link.delayConfig()->delayNs(), 0);
  }
}

TEST(AddFlexTopologyTest, DprRoutingPutsADotProductTiebreakOnTheRackUplink) {
  const auto sim = buildTopology("DPR");

  const auto& rtsw = sim.flex_topology()->nodes()->at("RTSW_z0_r0");
  EXPECT_EQ(
      rtsw.lpmSpec()->defaultRoute()->tiebreak()->getType(),
      packsim::thrift::TiebreakSpec::Type::dpr);
}

TEST(AddFlexTopologyTest, EcmpRoutingPutsAnEcmpTiebreakOnTheRackUplink) {
  const auto sim = buildTopology("ECMP");

  const auto& rtsw = sim.flex_topology()->nodes()->at("RTSW_z0_r0");
  EXPECT_EQ(
      rtsw.lpmSpec()->defaultRoute()->tiebreak()->getType(),
      packsim::thrift::TiebreakSpec::Type::ecmp);
}

TEST(AddFlexTopologyTest, MixedRoutingTakesDprUpFromTheRack) {
  // "MIXED" is DPR at the rack uplink and ECMP above it -- that split is the
  // whole point of the mode, so both halves are pinned.
  const auto sim = buildTopology("MIXED");

  const auto& rtsw = sim.flex_topology()->nodes()->at("RTSW_z0_r0");
  const auto& ctsw = sim.flex_topology()->nodes()->at("CTSW_z0_p0_c0");
  EXPECT_EQ(
      rtsw.lpmSpec()->defaultRoute()->tiebreak()->getType(),
      packsim::thrift::TiebreakSpec::Type::dpr);
  EXPECT_EQ(
      ctsw.lpmSpec()->defaultRoute()->tiebreak()->getType(),
      packsim::thrift::TiebreakSpec::Type::ecmp);
}

TEST(AddFlexTopologyTest, AnUnsupportedRoutingModeIsRejected) {
  // Falling back to a default here would build a fabric that routes
  // differently from what the caller asked for, with no warning.
  EXPECT_THAT(
      []() { buildTopology("ROUND_ROBIN"); },
      testing::ThrowsMessage<std::runtime_error>(
          testing::HasSubstr("not supported by flex topology")));
}

TEST(AddFlexTopologyTest, TheSpineIsSplitAcrossThePlanes) {
  auto topo = makeTopologySpec();
  topo.numCtsws = 4;
  topo.numAtsws = 4;

  const auto sim = addFlexTopology(
      packsim::thrift::Simulation(), makeLinks(), topo, "MIXED", 2);

  // Four switches over two planes is two per plane, and the names carry the
  // plane index so the two halves stay distinguishable.
  EXPECT_EQ(countNodes(sim, "CTSW_"), 4);
  EXPECT_EQ(countNodes(sim, "ATSW_"), 4);
  EXPECT_TRUE(sim.flex_topology()->nodes()->contains("ATSW_p1_a1"));
}

TEST(AddFlexTopologyTest, EveryAcceleratorGetsAnEndpointConfig) {
  // The endpoint config is what binds a NIC to its links; an accelerator
  // without one is unreachable.
  const auto sim = buildTopology();

  // Without this the loop below would pass on a topology with no
  // accelerators at all.
  ASSERT_EQ(countNodes(sim, "Accel_"), 2);
  for (const auto& [name, node] : *sim.flex_topology()->nodes()) {
    if (name.rfind("Accel_", 0) != 0) {
      continue;
    }
    ASSERT_EQ(node.endpointCfgs()->size(), 1);
    EXPECT_FALSE(node.endpointCfgs()->at(0).linkIds()->empty());
  }
}

TEST(AddFlexTopologyTest, TheRackSwitchGetsAPrefixRouteToEachAccelerator) {
  const auto sim = buildTopology();

  const auto& rtsw = sim.flex_topology()->nodes()->at("RTSW_z0_r0");
  EXPECT_EQ(rtsw.lpmSpec()->prefixTable()->size(), 2);
  EXPECT_EQ(*rtsw.lpmSpec()->validPrefixLengths(), std::vector<uint32_t>{5});
}

} // namespace packsim
