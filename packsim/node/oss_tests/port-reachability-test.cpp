/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "packsim/kernel/device-id.h"
#include "packsim/kernel/prefix-id-utils.h"
#include "packsim/node/port-reachability.h"

namespace packsim {
namespace {

// Index of the port field within a FlexDeviceId on these fabrics (matches
// `EndpointConfig::portIdx`).
constexpr uint8_t kPortFieldIdx = 5;
constexpr int kPrefixLen = 5;

std::vector<uint32_t> accelId(uint32_t accel) {
  return {1, 0, 0, accel, 0};
}

// Accel-granularity address (no port field) for the dstAccelAddress argument.
FlexDeviceId accelAddr(uint32_t accel) {
  return FlexDeviceId(std::vector<uint32_t>{1, 0, 0, accel, 0});
}

std::vector<uint32_t> switchId(uint32_t plane) {
  return {2, 0, plane, 0, 1};
}

packsim::thrift::Node makeAccel(uint32_t accel) {
  packsim::thrift::Node n;
  n.name() = fmt::format("Accel_{}", accel);
  n.id() = accelId(accel);
  n.type() = packsim::thrift::NodeType::ACCEL;
  return n;
}

packsim::thrift::Node makeSwitch(
    const std::string& name,
    std::vector<uint32_t> id) {
  packsim::thrift::Node n;
  n.name() = name;
  n.id() = std::move(id);
  n.type() = packsim::thrift::NodeType::SWITCH;
  n.lpmSpec()->validPrefixLengths() = std::vector<uint32_t>{kPrefixLen};
  return n;
}

void addRoute(
    packsim::thrift::Node& sw,
    const std::vector<uint32_t>& dstAccel,
    packsim::thrift::LinkId linkId) {
  packsim::thrift::LpmTableEntry entry;
  entry.links() = std::vector<packsim::thrift::LinkId>{linkId};
  (*sw.lpmSpec()->prefixTable())[packsim::makePrefixKey(dstAccel)] = entry;
}

packsim::thrift::FlexLink makeAccelSwitchLink(
    packsim::thrift::LinkId linkId,
    uint32_t accel,
    uint32_t port,
    const std::string& swName,
    const std::vector<uint32_t>& swAddr) {
  packsim::thrift::FlexLink link;
  link.linkId() = linkId;
  link.node1() = fmt::format("Accel_{}", accel);
  link.node2() = swName;
  std::vector<uint32_t> srcAddr = accelId(accel);
  srcAddr.emplace_back(port + 1);
  srcAddr.emplace_back(0);
  srcAddr.emplace_back(0);
  link.sourceAddress() = srcAddr;
  link.destinationAddress() = swAddr;
  return link;
}

// Flat multiplane: each accel has one port per plane; accel port P wires to
// LEAF_P; each LEAF has an LPM prefix route to every accel. PLANAR by
// construction (a packet egressing port P reaches the dst accel only on port
// P).
packsim::thrift::FlexTopology buildFlatMultiplane(
    uint32_t numAccels,
    uint32_t numPlanes) {
  packsim::thrift::FlexTopology topo;
  topo.name() = "flat_multiplane_test";
  topo.fullPrefixLength() = kPrefixLen;
  auto& nodes = *topo.nodes();
  auto& links = *topo.links();

  for (uint32_t a = 0; a < numAccels; ++a) {
    nodes[fmt::format("Accel_{}", a)] = makeAccel(a);
  }
  for (uint32_t p = 0; p < numPlanes; ++p) {
    nodes[fmt::format("LEAF_{}", p)] =
        makeSwitch(fmt::format("LEAF_{}", p), switchId(p));
  }

  packsim::thrift::LinkId linkId = 0;
  for (uint32_t a = 0; a < numAccels; ++a) {
    for (uint32_t p = 0; p < numPlanes; ++p) {
      const std::string swName = fmt::format("LEAF_{}", p);
      std::vector<uint32_t> swAddr = switchId(p);
      swAddr.insert(swAddr.end(), {0, 0, 0});
      links[linkId] = makeAccelSwitchLink(linkId, a, p, swName, swAddr);
      addRoute(nodes[swName], accelId(a), linkId);
      ++linkId;
    }
  }
  return topo;
}

// Meshed: a single switch wired to every accel port; the switch can forward to
// each accel on all of its ports. MESHED by construction.
packsim::thrift::FlexTopology buildMeshed(
    uint32_t numAccels,
    uint32_t numPorts) {
  packsim::thrift::FlexTopology topo;
  topo.name() = "meshed_test";
  topo.fullPrefixLength() = kPrefixLen;
  auto& nodes = *topo.nodes();
  auto& links = *topo.links();

  for (uint32_t a = 0; a < numAccels; ++a) {
    nodes[fmt::format("Accel_{}", a)] = makeAccel(a);
  }
  nodes["SW"] = makeSwitch("SW", {2, 0, 0, 0, 1});
  std::vector<uint32_t> swAddr = {2, 0, 0, 0, 1, 0, 0, 0};

  packsim::thrift::LinkId linkId = 0;
  for (uint32_t a = 0; a < numAccels; ++a) {
    for (uint32_t port = 0; port < numPorts; ++port) {
      links[linkId] = makeAccelSwitchLink(linkId, a, port, "SW", swAddr);
      // Every accel port is an LPM next-hop for that accel (meshed).
      packsim::thrift::LpmTableEntry& entry =
          (*nodes["SW"].lpmSpec()->prefixTable())[packsim::makePrefixKey(
              accelId(a))];
      entry.links()->emplace_back(linkId);
      ++linkId;
    }
  }
  return topo;
}

packsim::thrift::FlexLink makeSwitchLink(
    packsim::thrift::LinkId linkId,
    const std::string& node1,
    const std::string& node2) {
  packsim::thrift::FlexLink link;
  link.linkId() = linkId;
  link.node1() = node1;
  link.node2() = node2;
  // Switch-to-switch endpoint addresses are not consulted by the tracer (only
  // accel-side addresses of leaf links are), but set them for realism.
  link.sourceAddress() = std::vector<uint32_t>{2, 0, 0, 0, 1, 0, 0, 0};
  link.destinationAddress() = std::vector<uint32_t>{2, 0, 0, 0, 1, 0, 0, 0};
  return link;
}

void addRouteLink(
    packsim::thrift::Node& sw,
    const std::vector<uint32_t>& dstAccel,
    packsim::thrift::LinkId linkId) {
  packsim::thrift::LpmTableEntry& entry =
      (*sw.lpmSpec()->prefixTable())[packsim::makePrefixKey(dstAccel)];
  entry.links()->emplace_back(linkId);
}

std::vector<uint32_t> accelIdAt(uint32_t rack, uint32_t idx) {
  return {1, 0, rack, idx, 0};
}

FlexDeviceId accelAddrAt(uint32_t rack, uint32_t idx) {
  return FlexDeviceId(std::vector<uint32_t>{1, 0, rack, idx, 0});
}

// Multi-level multiplane: one CLOS per plane. Each plane p has one leaf switch
// per accel (LEAF_p_a, wired to accel a's port p) and one spine (SPINE_p). A
// packet to another accel travels leaf -> spine -> leaf -> accel (multi-hop).
// PLANAR by construction: the trace stays on plane p and reaches the
// destination only on port p.
packsim::thrift::FlexTopology buildMultiLevelMultiplane(
    uint32_t numAccels,
    uint32_t numPlanes) {
  packsim::thrift::FlexTopology topo;
  topo.name() = "multilevel_multiplane_test";
  topo.fullPrefixLength() = kPrefixLen;
  auto& nodes = *topo.nodes();
  auto& links = *topo.links();

  for (uint32_t a = 0; a < numAccels; ++a) {
    nodes[fmt::format("Accel_{}", a)] = makeAccel(a);
  }

  packsim::thrift::LinkId linkId = 0;
  for (uint32_t p = 0; p < numPlanes; ++p) {
    const std::string spine = fmt::format("SPINE_{}", p);
    nodes[spine] = makeSwitch(spine, {3, 0, p, 0, 1});

    std::map<uint32_t, std::string> leaf;
    for (uint32_t a = 0; a < numAccels; ++a) {
      leaf[a] = fmt::format("LEAF_{}_{}", p, a);
      nodes[leaf[a]] = makeSwitch(leaf[a], {2, 0, p, a, 1});
      const std::vector<uint32_t> leafAddr = {2, 0, p, a, 1, 0, 0, 0};
      links[linkId] = makeAccelSwitchLink(linkId, a, p, leaf[a], leafAddr);
      addRoute(nodes[leaf[a]], accelId(a), linkId); // own accel: direct
      ++linkId;
    }

    std::map<uint32_t, packsim::thrift::LinkId> leafToSpine;
    for (uint32_t a = 0; a < numAccels; ++a) {
      links[linkId] = makeSwitchLink(linkId, leaf[a], spine);
      leafToSpine[a] = linkId;
      ++linkId;
    }

    for (uint32_t a = 0; a < numAccels; ++a) {
      // Spine forwards each accel down its own leaf.
      addRoute(nodes[spine], accelId(a), leafToSpine[a]);
      // Each leaf forwards other accels up to the spine.
      for (uint32_t d = 0; d < numAccels; ++d) {
        if (d != a) {
          addRoute(nodes[leaf[a]], accelId(d), leafToSpine[a]);
        }
      }
    }
  }
  return topo;
}

// A two-switch cycle (SW_A <-> SW_B) where SW_B's route to the destination
// includes both the destination leaf and a link back to SW_A. Without
// loop-safety the BFS would revisit SW_A forever; with it the walk terminates
// and still reports the destination port.
packsim::thrift::FlexTopology buildCyclicFabric() {
  packsim::thrift::FlexTopology topo;
  topo.name() = "cyclic_test";
  topo.fullPrefixLength() = kPrefixLen;
  auto& nodes = *topo.nodes();
  auto& links = *topo.links();

  nodes["Accel_0"] = makeAccel(0);
  nodes["Accel_1"] = makeAccel(1);
  nodes["SW_A"] = makeSwitch("SW_A", {2, 0, 0, 0, 1});
  nodes["SW_B"] = makeSwitch("SW_B", {2, 0, 1, 0, 1});

  links[0] = makeSwitchLink(0, "SW_A", "SW_B");
  links[1] = makeAccelSwitchLink(
      1, /*accel=*/1, /*port=*/0, "SW_B", {2, 0, 1, 0, 1, 0, 0, 0});

  addRoute(nodes["SW_A"], accelId(1), 0); // SW_A -> SW_B
  addRoute(nodes["SW_B"], accelId(1), 1); // SW_B -> accel1 leaf
  addRouteLink(
      nodes["SW_B"], accelId(1), 0); // ... and back to SW_A (the cycle)
  return topo;
}

// Two accels in different racks reachable through one plane. The tracer decides
// reachability from the actual forwarding, not from rack membership.
packsim::thrift::FlexTopology buildTwoRack() {
  packsim::thrift::FlexTopology topo;
  topo.name() = "two_rack_test";
  topo.fullPrefixLength() = kPrefixLen;
  auto& nodes = *topo.nodes();
  auto& links = *topo.links();

  for (uint32_t rack = 0; rack < 2; ++rack) {
    packsim::thrift::Node accel;
    accel.name() = fmt::format("Accel_{}", rack);
    accel.id() = accelIdAt(rack, /*idx=*/0);
    accel.type() = packsim::thrift::NodeType::ACCEL;
    nodes[*accel.name()] = accel;
  }
  nodes["LEAF_0"] = makeSwitch("LEAF_0", {2, 0, 0, 0, 1});
  const std::vector<uint32_t> swAddr = {2, 0, 0, 0, 1, 0, 0, 0};

  packsim::thrift::LinkId linkId = 0;
  for (uint32_t rack = 0; rack < 2; ++rack) {
    packsim::thrift::FlexLink link;
    link.linkId() = linkId;
    link.node1() = fmt::format("Accel_{}", rack);
    link.node2() = "LEAF_0";
    std::vector<uint32_t> src = accelIdAt(rack, 0);
    src.emplace_back(/*port+1=*/1);
    src.emplace_back(0);
    src.emplace_back(0);
    link.sourceAddress() = src;
    link.destinationAddress() = swAddr;
    links[linkId] = link;
    addRoute(nodes["LEAF_0"], accelIdAt(rack, 0), linkId);
    ++linkId;
  }
  return topo;
}

// --- Per-port-keyed fabric --------------------------------------------------
// Unlike the per-accel fabrics above, these switches key their LPM by the full
// per-port address (validPrefixLengths={6}, the port field inside the matched
// prefix), and the cross-rack backbone is all-to-all. This is the convention a
// masked-accel query cannot match (the zeroed port field never equals a real
// port key), so only a full-address trace finds these routes.

constexpr int kPerPortPrefixLen = 6;

packsim::thrift::Node makePerPortSwitch(
    const std::string& name,
    std::vector<uint32_t> id) {
  packsim::thrift::Node n;
  n.name() = name;
  n.id() = std::move(id);
  n.type() = packsim::thrift::NodeType::SWITCH;
  n.lpmSpec()->validPrefixLengths() = std::vector<uint32_t>{kPerPortPrefixLen};
  return n;
}

// Full per-port prefix key {1, 0, rack, accel, 0, port+1}.
std::vector<uint32_t> portPrefix(uint32_t rack, uint32_t accel, uint32_t port) {
  return {1, 0, rack, accel, 0, port + 1};
}

void addPortRoute(
    packsim::thrift::Node& sw,
    const std::vector<uint32_t>& portKey,
    packsim::thrift::LinkId linkId) {
  packsim::thrift::LpmTableEntry& entry =
      (*sw.lpmSpec()->prefixTable())[packsim::makePrefixKey(portKey)];
  entry.links()->emplace_back(linkId);
}

packsim::thrift::FlexLink makePerPortAccelLink(
    packsim::thrift::LinkId linkId,
    uint32_t rack,
    uint32_t accel,
    uint32_t port,
    const std::string& swName) {
  packsim::thrift::FlexLink link;
  link.linkId() = linkId;
  link.node1() = fmt::format("Accel_{}_{}", rack, accel);
  link.node2() = swName;
  link.sourceAddress() =
      std::vector<uint32_t>{1, 0, rack, accel, 0, port + 1, 0, 0};
  link.destinationAddress() =
      std::vector<uint32_t>{2, 0, rack, port, 1, 0, 0, 0};
  return link;
}

// 2 racks x accelPerRack accels x 2 planes; accel port p wires to SW_rack_p.
// Intra-rack planes are isolated (no switch<->switch link within a rack), so an
// intra-rack packet reaches the destination only on the same port (PLANAR).
// The cross-rack backbone is all-to-all (every rack-0 switch links every rack-1
// switch), so a cross-rack packet can land on either remote port (MESHED).
packsim::thrift::FlexTopology buildPerPortMultiplane(uint32_t accelPerRack) {
  constexpr uint32_t kNumRacks = 2;
  constexpr uint32_t kNumPlanes = 2;
  packsim::thrift::FlexTopology topo;
  topo.name() = "per_port_multiplane_test";
  topo.fullPrefixLength() = kPerPortPrefixLen;
  auto& nodes = *topo.nodes();
  auto& links = *topo.links();

  for (uint32_t r = 0; r < kNumRacks; ++r) {
    for (uint32_t a = 0; a < accelPerRack; ++a) {
      packsim::thrift::Node accel;
      accel.name() = fmt::format("Accel_{}_{}", r, a);
      accel.id() = std::vector<uint32_t>{1, 0, r, a, 0};
      accel.type() = packsim::thrift::NodeType::ACCEL;
      nodes[*accel.name()] = accel;
    }
    for (uint32_t p = 0; p < kNumPlanes; ++p) {
      const std::string sw = fmt::format("SW_{}_{}", r, p);
      nodes[sw] = makePerPortSwitch(sw, {2, 0, r, p, 1});
    }
  }

  packsim::thrift::LinkId linkId = 0;
  // accel port p -> SW_rack_p; that switch routes the accel's port p down it.
  for (uint32_t r = 0; r < kNumRacks; ++r) {
    for (uint32_t a = 0; a < accelPerRack; ++a) {
      for (uint32_t p = 0; p < kNumPlanes; ++p) {
        const std::string sw = fmt::format("SW_{}_{}", r, p);
        links[linkId] = makePerPortAccelLink(linkId, r, a, p, sw);
        addPortRoute(nodes[sw], portPrefix(r, a, p), linkId);
        ++linkId;
      }
    }
  }
  // All-to-all cross-rack backbone: each rack-0 switch learns every rack-1
  // accel port (and vice versa) via its inter-rack link to the switch that
  // owns that port.
  for (uint32_t p0 = 0; p0 < kNumPlanes; ++p0) {
    for (uint32_t p1 = 0; p1 < kNumPlanes; ++p1) {
      const std::string sw0 = fmt::format("SW_0_{}", p0);
      const std::string sw1 = fmt::format("SW_1_{}", p1);
      const packsim::thrift::LinkId link01 = linkId++;
      links[link01] = makeSwitchLink(link01, sw0, sw1);
      for (uint32_t a = 0; a < accelPerRack; ++a) {
        addPortRoute(nodes[sw0], portPrefix(1, a, p1), link01);
        addPortRoute(nodes[sw1], portPrefix(0, a, p0), link01);
      }
    }
  }
  return topo;
}

// --- Leaf/spine fabric routed by default route ------------------------------
// A leaf keys only its own rack's accel ports and sends every other
// destination up a default route bundling several links to each spine; a
// spine keys each rack's prefix (validPrefixLengths={3}) onto the bundle down
// to that rack's leaf.

constexpr int kRackPrefixLen = 3;

// 2 racks x 2 accels x 2 ports; accel ports wire to their rack's leaf, and each
// leaf has 3 links to each of 2 spines.
packsim::thrift::FlexTopology buildLeafSpine() {
  constexpr uint32_t kNumRacks = 2;
  constexpr uint32_t kAccelPerRack = 2;
  constexpr uint32_t kNumPorts = 2;
  constexpr uint32_t kNumSpines = 2;
  constexpr uint32_t kLinksPerLeafSpinePair = 3;
  packsim::thrift::FlexTopology topo;
  topo.name() = "leaf_spine_test";
  topo.fullPrefixLength() = kPerPortPrefixLen;
  auto& nodes = *topo.nodes();
  auto& links = *topo.links();

  packsim::thrift::LinkId linkId = 0;
  for (uint32_t r = 0; r < kNumRacks; ++r) {
    const std::string leaf = fmt::format("LEAF_{}", r);
    nodes[leaf] = makePerPortSwitch(leaf, {2, 0, r, 0, 1});
    for (uint32_t a = 0; a < kAccelPerRack; ++a) {
      packsim::thrift::Node accel;
      accel.name() = fmt::format("Accel_{}_{}", r, a);
      accel.id() = std::vector<uint32_t>{1, 0, r, a, 0};
      accel.type() = packsim::thrift::NodeType::ACCEL;
      nodes[*accel.name()] = accel;
      for (uint32_t p = 0; p < kNumPorts; ++p) {
        links[linkId] = makePerPortAccelLink(linkId, r, a, p, leaf);
        addPortRoute(nodes[leaf], portPrefix(r, a, p), linkId);
        ++linkId;
      }
    }
  }
  for (uint32_t s = 0; s < kNumSpines; ++s) {
    const std::string spine = fmt::format("SPINE_{}", s);
    nodes[spine] = makeSwitch(spine, {3, 0, 0, s, 1});
    nodes[spine].lpmSpec()->validPrefixLengths() =
        std::vector<uint32_t>{kRackPrefixLen};
    for (uint32_t r = 0; r < kNumRacks; ++r) {
      const std::string leaf = fmt::format("LEAF_{}", r);
      for (uint32_t i = 0; i < kLinksPerLeafSpinePair; ++i) {
        links[linkId] = makeSwitchLink(linkId, leaf, spine);
        nodes[leaf].lpmSpec()->defaultRoute()->links()->emplace_back(linkId);
        addPortRoute(nodes[spine], {1, 0, r}, linkId);
        ++linkId;
      }
    }
  }
  return topo;
}

TEST(PortReachabilityTest, LeafDefaultRouteReachesEveryCrossRackPort) {
  const auto topo = buildLeafSpine();
  // A rack-1 port misses LEAF_0's port entries, climbs its default route to
  // either spine, and comes down the rack-1 bundle to LEAF_1, which delivers on
  // that port: every port of the destination is reachable (MESHED).
  const std::set<uint16_t> expected{0, 1};
  EXPECT_EQ(
      PortReachability(topo, kPortFieldIdx)
          .dstPortsFrom("LEAF_0", accelAddrAt(/*rack=*/1, /*idx=*/1)),
      expected)
      << "a cross-rack destination is reached through the leaf's default route "
         "and the spines' rack prefixes, on every one of its ports";
}

TEST(PortReachabilityTest, PerPortIntraRackIsPlanar) {
  const auto topo = buildPerPortMultiplane(/*accelPerRack=*/2);
  // Egress port p (first hop SW_0_p) reaches a same-rack accel only on port p:
  // the planes are isolated within a rack.
  for (uint16_t p = 0; p < 2; ++p) {
    const std::set<uint16_t> expected{p};
    EXPECT_EQ(
        PortReachability(topo, kPortFieldIdx)
            .dstPortsFrom(
                fmt::format("SW_0_{}", p), accelAddrAt(/*rack=*/0, /*idx=*/1)),
        expected)
        << "per-port intra-rack wiring is planar: entering plane " << p
        << " must reach the same-rack destination only on port " << p;
  }
}

TEST(PortReachabilityTest, PerPortCrossRackIsMeshed) {
  const auto topo = buildPerPortMultiplane(/*accelPerRack=*/2);
  // Egress port 0 (first hop SW_0_0) reaches a cross-rack accel on BOTH ports
  // via the all-to-all backbone -- the per-port keys the masked-accel query
  // could not match.
  const std::set<uint16_t> expected{0, 1};
  EXPECT_EQ(
      PortReachability(topo, kPortFieldIdx)
          .dstPortsFrom("SW_0_0", accelAddrAt(/*rack=*/1, /*idx=*/0)),
      expected)
      << "per-port cross-rack wiring is meshed: an all-to-all backbone reaches "
         "the destination on every port";
}

TEST(PortReachabilityTest, FlatMultiplaneIsPlanarPerPort) {
  const auto topo = buildFlatMultiplane(/*numAccels=*/2, /*numPlanes=*/4);
  // A packet egressing accel-0 port P (first hop LEAF_P) reaches accel-1 only
  // on the same port P.
  for (uint16_t p = 0; p < 4; ++p) {
    const std::set<uint16_t> expected{p};
    EXPECT_EQ(
        PortReachability(topo, kPortFieldIdx)
            .dstPortsFrom(fmt::format("LEAF_{}", p), accelAddr(1)),
        expected)
        << "flat multiplane must be planar: first hop LEAF_" << p
        << " should reach the destination accel only on port " << p;
  }
}

TEST(PortReachabilityTest, MeshedReachesAllPorts) {
  const auto topo = buildMeshed(/*numAccels=*/2, /*numPorts=*/3);
  const std::set<uint16_t> expected{0, 1, 2};
  EXPECT_EQ(
      PortReachability(topo, kPortFieldIdx).dstPortsFrom("SW", accelAddr(1)),
      expected)
      << "meshed wiring must report every destination port as reachable";
}

TEST(PortReachabilityTest, SinglePlaneReachesOnlyPortZero) {
  const auto topo = buildFlatMultiplane(/*numAccels=*/2, /*numPlanes=*/1);
  const std::set<uint16_t> expected{0};
  EXPECT_EQ(
      PortReachability(topo, kPortFieldIdx)
          .dstPortsFrom("LEAF_0", accelAddr(1)),
      expected)
      << "single-plane fabric has only port 0 to reach the destination";
}

TEST(PortReachabilityTest, UnknownFirstHopSwitchReachesNothing) {
  const auto topo = buildFlatMultiplane(/*numAccels=*/2, /*numPlanes=*/2);
  EXPECT_TRUE(PortReachability(topo, kPortFieldIdx)
                  .dstPortsFrom("LEAF_does_not_exist", accelAddr(1))
                  .empty())
      << "an unknown first-hop switch reaches no destination port";
}

TEST(PortReachabilityTest, NonSwitchFirstHopReachesNothing) {
  // A first hop that names an existing non-switch node (e.g. an accel) has no
  // LPM table to trace, so it must return empty -- not dereference a missing
  // lpmSpec -- matching the documented unknown-first-hop behavior.
  const auto topo = buildFlatMultiplane(/*numAccels=*/2, /*numPlanes=*/2);
  EXPECT_TRUE(PortReachability(topo, kPortFieldIdx)
                  .dstPortsFrom("Accel_0", accelAddr(1))
                  .empty())
      << "a non-switch first-hop node reaches no destination port";
}

TEST(PortReachabilityTest, MultiLevelMultiplaneIsPlanarViaMultiHop) {
  const auto topo = buildMultiLevelMultiplane(/*numAccels=*/2, /*numPlanes=*/4);
  // Source accel-0 enters plane p at its leaf LEAF_p_0; the packet reaches
  // accel-1 only after leaf -> spine -> leaf, and only on port p.
  for (uint16_t p = 0; p < 4; ++p) {
    const std::set<uint16_t> expected{p};
    EXPECT_EQ(
        PortReachability(topo, kPortFieldIdx)
            .dstPortsFrom(fmt::format("LEAF_{}_0", p), accelAddr(1)),
        expected)
        << "multi-hop plane fabric must stay planar: entering plane " << p
        << " must reach the destination only on port " << p;
  }
}

TEST(PortReachabilityTest, SinglePlaneClosReachesOnlyPortZero) {
  // A single-plane CLOS (one port per accel, multi-hop) reaches only port 0.
  const auto topo = buildMultiLevelMultiplane(/*numAccels=*/2, /*numPlanes=*/1);
  const std::set<uint16_t> expected{0};
  EXPECT_EQ(
      PortReachability(topo, kPortFieldIdx)
          .dstPortsFrom("LEAF_0_0", accelAddr(1)),
      expected)
      << "single-plane CLOS reaches the destination only on its one port";
}

TEST(PortReachabilityTest, CyclicFabricTerminatesAtDestinationPort) {
  const auto topo = buildCyclicFabric();
  const std::set<uint16_t> expected{0};
  EXPECT_EQ(
      PortReachability(topo, kPortFieldIdx).dstPortsFrom("SW_A", accelAddr(1)),
      expected)
      << "a routing cycle must not loop forever and must still find the "
         "destination port";
}

TEST(PortReachabilityTest, CrossRackReachableViaForwarding) {
  const auto topo = buildTwoRack();
  const std::set<uint16_t> expected{0};
  EXPECT_EQ(
      PortReachability(topo, kPortFieldIdx)
          .dstPortsFrom("LEAF_0", accelAddrAt(/*rack=*/1, /*idx=*/0)),
      expected)
      << "cross-rack reachability must be decided by forwarding, not by a "
         "rack-number short-circuit";
}

} // namespace
} // namespace packsim
