/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/node/port-reachability.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <folly/Conv.h>
#include <folly/container/F14Set.h>

#include "packsim/node/flex-node.h"

namespace packsim {

namespace {

// A link's accel-side endpoint address: the port-level address on the accel end
// (carrying the real nic and port fields), as opposed to the switch end which
// carries the switch id. node1 holds sourceAddress, node2 holds
// destinationAddress.
const std::vector<uint32_t>& accelSideAddress(
    const packsim::thrift::FlexLink& link,
    const bool accelIsNode1) {
  return accelIsNode1 ? *link.sourceAddress() : *link.destinationAddress();
}

// Whether an accel-side address belongs to the destination addressed by dst:
// every field before the port field matches (region/dc/rack/accel/nic), leaving
// the port field free -- that is exactly what reachability discovers.
bool accelSideIsDst(
    const std::vector<uint32_t>& accelAddr,
    const FlexDeviceId& dst,
    const uint8_t portFieldIdx) {
  if (accelAddr.size() <= portFieldIdx) {
    return false;
  }
  for (uint8_t i = 0; i < portFieldIdx; ++i) {
    if (accelAddr[i] != dst.values.at(i)) {
      return false;
    }
  }
  return true;
}

// The 0-based port number carried by an accel-side address (stored as port+1; 0
// means unset).
std::optional<uint16_t> portOf(
    const std::vector<uint32_t>& accelAddr,
    const uint8_t portFieldIdx) {
  if (accelAddr.size() <= portFieldIdx || accelAddr[portFieldIdx] == 0) {
    return std::nullopt;
  }
  return folly::to<uint16_t>(accelAddr[portFieldIdx] - 1);
}

// The full destination port address used as the routing key: the accel address
// with its port field set to port+1 (FlexDeviceId port fields are 1-based; 0
// means unset). Tracing on this full address mirrors how FlexNode::routePacket
// keys its LPM lookups, so the trace matches the LPM entry routing would use.
FlexDeviceId dstPortAddress(
    const FlexDeviceId& dstAccelAddress,
    const uint8_t portFieldIdx,
    const uint16_t port) {
  FlexDeviceId id = dstAccelAddress;
  // port was read from a 1-based uint16_t field, so port + 1 fits the field.
  uint16_t& portField = id.values.at(portFieldIdx);
  portField = port;
  ++portField;
  return id;
}

// Accel-prefix key: a FlexDeviceId with the port field and beyond zeroed, so
// two addresses share a key iff their [0, portFieldIdx) fields match.
FlexDeviceId accelPrefixKey(
    const FlexDeviceId& accel,
    const uint8_t portFieldIdx) {
  FlexDeviceId key = accel;
  for (size_t i = portFieldIdx;
       std::cmp_less(i, FlexDeviceId::kNumFlexDeviceIdFields);
       ++i) {
    key.values.at(i) = 0;
  }
  return key;
}

} // namespace

PortReachability::NodesByName PortReachability::buildNodesByName(
    const packsim::thrift::FlexTopology& topo) {
  const auto& nodes = *topo.nodes();
  NodesByName byName;
  byName.reserve(nodes.size());
  for (const auto& [name, node] : nodes) {
    byName.emplace(name, &node);
  }
  return byName;
}

folly::F14FastMap<FlexDeviceId, std::vector<uint16_t>>
PortReachability::buildAccelPortsByPrefix(
    const packsim::thrift::FlexTopology& topo,
    const uint8_t portFieldIdx) {
  // One pass over all links. Each accel port is wired to a single link, so
  // every port is recorded exactly once.
  folly::F14FastMap<FlexDeviceId, std::vector<uint16_t>> byPrefix;
  for (const auto& [linkId, link] : *topo.links()) {
    for (const bool accelIsNode1 : {true, false}) {
      const auto& addr = accelSideAddress(link, accelIsNode1);
      if (const auto port = portOf(addr, portFieldIdx)) {
        byPrefix[accelPrefixKey(FlexDeviceId(addr), portFieldIdx)].emplace_back(
            *port);
      }
    }
  }
  return byPrefix;
}

PortReachability::HopsByRoute PortReachability::buildHopsByRoute(
    const packsim::thrift::FlexTopology& topo,
    const NodesByName& nodesByName) {
  // Follows each of a route's links once, from the switch the route is on.
  const auto resolve = [&](const packsim::thrift::LpmTableEntry& route,
                           const std::string& switchName) {
    RouteHops hops;
    for (const auto& linkId : *route.links()) {
      const auto& link = topo.links()->at(linkId);
      const bool switchIsNode1 = (*link.node1() == switchName);
      const auto* next =
          nodesByName.at(switchIsNode1 ? *link.node2() : *link.node1());
      const auto nextType = *next->type();
      if (nextType == packsim::thrift::NodeType::SWITCH) {
        // An ECMP group holds many links to few switches, so this stays short.
        if (std::find(hops.switches.begin(), hops.switches.end(), next) ==
            hops.switches.end()) {
          hops.switches.emplace_back(next);
        }
      } else if (nextType == packsim::thrift::NodeType::ACCEL) {
        // Only accel leaves carry destination ports; gating on node type stops
        // a non-accel endpoint whose address shares the destination's prefix
        // from being misread as a reachable port.
        hops.accelAddresses.emplace_back(
            &accelSideAddress(link, /*accelIsNode1=*/!switchIsNode1));
      }
    }
    return hops;
  };

  HopsByRoute hopsByRoute;
  for (const auto& [name, node] : *topo.nodes()) {
    if (*node.type() != packsim::thrift::NodeType::SWITCH) {
      continue;
    }
    const auto& lpm = *node.lpmSpec();
    hopsByRoute.emplace(
        &*lpm.defaultRoute(), resolve(*lpm.defaultRoute(), name));
    for (const auto& [prefix, route] : *lpm.prefixTable()) {
      hopsByRoute.emplace(&route, resolve(route, name));
    }
  }
  return hopsByRoute;
}

PortReachability::PortReachability(
    const packsim::thrift::FlexTopology& topo,
    const uint8_t portFieldIdx)
    : portFieldIdx_(portFieldIdx),
      nodesByName_(buildNodesByName(topo)),
      accelPortsByPrefix_(buildAccelPortsByPrefix(topo, portFieldIdx)),
      hopsByRoute_(buildHopsByRoute(topo, nodesByName_)) {}

const std::vector<uint16_t>& PortReachability::destAccelPorts(
    const FlexDeviceId& dstAccelAddress) const {
  static const std::vector<uint16_t> kEmpty;
  const auto it =
      accelPortsByPrefix_.find(accelPrefixKey(dstAccelAddress, portFieldIdx_));
  return it == accelPortsByPrefix_.end() ? kEmpty : it->second;
}

void PortReachability::collectReachedPorts(
    const packsim::thrift::Node& firstHopSwitch,
    const FlexDeviceId& dstPortId,
    std::set<uint16_t>& reached) const {
  // Only switches are enqueued, marked seen at enqueue, so the walk is
  // loop-safe.
  std::deque<const packsim::thrift::Node*> frontier{&firstHopSwitch};
  folly::F14FastSet<const packsim::thrift::Node*> seen{&firstHopSwitch};
  while (!frontier.empty()) {
    const auto* switchNode = frontier.front();
    frontier.pop_front();
    // The route FlexNode::routePacket would match for a real packet to
    // dstPortId at this switch.
    const RouteHops& hops = hopsByRoute_.at(
        &FlexNode::lpmEntryFor(*switchNode->lpmSpec(), dstPortId));
    for (const auto* next : hops.switches) {
      if (seen.emplace(next).second) {
        frontier.emplace_back(next);
      }
    }
    // dstPortId shares every field before the port field with its accel, so it
    // identifies the destination accel's own links.
    for (const auto* accelAddr : hops.accelAddresses) {
      if (!accelSideIsDst(*accelAddr, dstPortId, portFieldIdx_)) {
        continue;
      }
      if (const auto port = portOf(*accelAddr, portFieldIdx_)) {
        reached.emplace(*port);
      }
    }
  }
}

std::set<uint16_t> PortReachability::dstPortsFrom(
    const std::string& firstHopSwitch,
    const FlexDeviceId& dstAccelAddress) const {
  std::set<uint16_t> dstPorts;
  // Documented contract: empty unless firstHopSwitch names a switch node.
  const auto it = nodesByName_.find(firstHopSwitch);
  if (it == nodesByName_.end() ||
      *it->second->type() != packsim::thrift::NodeType::SWITCH) {
    return dstPorts;
  }

  // Probe each of the destination accel's ports with its full port address,
  // keyed exactly as FlexNode::routePacket keys real packets. The union
  // reflects the true wiring: PLANAR when isolated per plane, MESHED when a
  // port reaches several.
  for (const uint16_t port : destAccelPorts(dstAccelAddress)) {
    collectReachedPorts(
        *it->second,
        dstPortAddress(dstAccelAddress, portFieldIdx_, port),
        dstPorts);
  }
  return dstPorts;
}

} // namespace packsim
