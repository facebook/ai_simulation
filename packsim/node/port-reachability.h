/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include <folly/container/F14Map.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/device-id.h"

namespace packsim {

/**
 * Per-port reachability over a FlexTopology, computed by tracing the fabric's
 * own forwarding (links plus per-node LPM tables). Used to decide a
 * multi-port transport's planar-vs-meshed destination-port assignment and to
 * filter unreachable (srcPort, dstPort) pairs. Assumes that, for a given
 * destination, routing may pick any link of each matched LPM route, as the
 * `ecmp` and `spray` tiebreaks do. Where a tiebreak uses only part of that
 * set -- `fly`'s shortest-path filter, or `dpr` and `reflect`, which fix one
 * link per destination or per ingress link -- the result can include ports
 * routing never uses.
 *
 * Built once per (topology, portFieldIdx) -- FlexTopology owns the instance
 * every endpoint shares. The constructor indexes nodes, precomputes each
 * accel's ports, and resolves every switch LPM route to its distinct next hops,
 * so a trace visits each next hop once however many links the route spreads
 * over: an ECMP default route of many links up to a few spines costs one step
 * per spine. The query runs O(QP-pairs) times during large all-to-all setup.
 * All state is const after construction, so concurrent dstPortsFrom queries
 * are safe.
 *
 * topo must outlive this instance and must not be mutated after construction:
 * the indices hold raw pointers into its node, link and LPM containers.
 */
class PortReachability {
 public:
  PortReachability(
      const packsim::thrift::FlexTopology& topo,
      uint8_t portFieldIdx);

  /**
   * The destination NIC port numbers (0-based, the same space as egressNicPort)
   * at the destination accel reachable from firstHopSwitch. Walks real links
   * and per-switch LPM from firstHopSwitch toward the destination accel, taking
   * the union over every link of each matched LPM route, whatever its
   * tiebreak, and stopping at accel leaves.
   *
   * The source port's wiring is PLANAR iff this set is exactly that port's own
   * number; any additional port means MESHED. Whether a destination is planar
   * or meshed depends on the source-to-destination path, so this is traced per
   * (firstHopSwitch, destination) rather than precomputed per switch.
   *
   * @param firstHopSwitch   Node name of the switch directly wired to the
   *                         source egress port (the trace start).
   * @param dstAccelAddress  Address identifying the destination accel; only its
   *                         accel prefix (the leading portFieldIdx fields) is
   *                         consulted.
   *
   * @return                 Reachable destination port numbers; empty if
   *                         firstHopSwitch is not a switch node.
   */
  [[nodiscard]] std::set<uint16_t> dstPortsFrom(
      const std::string& firstHopSwitch,
      const FlexDeviceId& dstAccelAddress) const;

 private:
  using NodesByName =
      folly::F14FastMap<std::string, const packsim::thrift::Node*>;

  // Where one switch LPM route leads, with its links already followed.
  struct RouteHops {
    // The distinct switches the route's links reach.
    std::vector<const packsim::thrift::Node*> switches;
    // The accel-side address of each route link that lands on an accel.
    std::vector<const std::vector<uint32_t>*> accelAddresses;
  };
  // Every switch's LPM routes, keyed by the address of the route entry inside
  // topo, which is the reference FlexNode::lpmEntryFor returns.
  using HopsByRoute =
      folly::F14FastMap<const packsim::thrift::LpmTableEntry*, RouteHops>;

  // The destination accel's own port numbers, precomputed at construction. The
  // returned reference is valid for the lifetime of this instance.
  const std::vector<uint16_t>& destAccelPorts(
      const FlexDeviceId& dstAccelAddress) const;

  // Breadth-first walk from firstHopSwitch toward dstPortId, adding every port
  // of dstPortId's accel that the walk lands on to reached.
  void collectReachedPorts(
      const packsim::thrift::Node& firstHopSwitch,
      const FlexDeviceId& dstPortId,
      std::set<uint16_t>& reached) const;

  static NodesByName buildNodesByName(
      const packsim::thrift::FlexTopology& topo);
  static folly::F14FastMap<FlexDeviceId, std::vector<uint16_t>>
  buildAccelPortsByPrefix(
      const packsim::thrift::FlexTopology& topo,
      uint8_t portFieldIdx);
  static HopsByRoute buildHopsByRoute(
      const packsim::thrift::FlexTopology& topo,
      const NodesByName& nodesByName);

  // Index of the port field within a FlexDeviceId on this fabric, fixed per
  // topology's addressing scheme.
  const uint8_t portFieldIdx_;
  const NodesByName nodesByName_;
  // Each accel prefix's own ports, keyed by prefix; precomputed once so
  // dstPortsFrom needs no per-query link scan.
  const folly::F14FastMap<FlexDeviceId, std::vector<uint16_t>>
      accelPortsByPrefix_;
  const HopsByRoute hopsByRoute_;
};

} // namespace packsim
