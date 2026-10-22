/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/node/flex-node.h"

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/String.h>
#include <folly/container/small_vector.h>
#include <folly/hash/Hash.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp/util/EnumUtils.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/kernel/prefix-id-utils.h"
#include "packsim/kernel/random.h"
#include "packsim/link-layer/eth-port.h"
// @oss-disable
#include "packsim/util/ascii-table.h"
#include "packsim/util/hashes.h"

namespace packsim {

namespace {

// Candidate links the `fly` tiebreak's shortest-path filter keeps inline. An
// arbitrary bound: a route with more links still works, it just allocates.
constexpr size_t kFlyInlineLinks = 16;

// Index into FlexDeviceId.values of the pod the `fly` tiebreak compares.
constexpr size_t kFlyPodIdx = 2;

/**
 * Whether topology-specific link narrowing can change any route resolved at
 * `nodeId`.
 *
 * @param topo    The topology routes are resolved against; null on a node
 *                built without one.
 * @param nodeId  FlexDeviceId of the routing node.
 *
 * @return        True iff `buildRoute` can narrow a route at this node.
 */
bool canNarrowRoutesAt(
    [[maybe_unused]] const packsim::thrift::FlexTopology* topo,
    [[maybe_unused]] const FlexDeviceId& nodeId) {
  // @oss-disable
      // @oss-disable
  return false; // @oss-enable
}

/**
 * Throw the routing error that FbSwitch's routing recovery path catches.
 *
 * @param message        The full error text.
 * @param nodeName       The node that could not route the packet.
 * @param destinationId  The packet's destination.
 *
 * @throw packsim::thrift::PackSimRoutingException always.
 */
[[noreturn]] void throwRoutingError(
    std::string message,
    const std::string& nodeName,
    const FlexDeviceId& destinationId) {
  packsim::thrift::PackSimRoutingException ex;
  ex.message() = std::move(message);
  ex.nodeName() = nodeName;
  ex.destinationId() = destinationId.toString();
  throw std::move(ex);
}

} // namespace

FlexNode::FlexNode(const FlexNodeParams& params)
    : simCtx_(params.simCtx),
      name_(params.name),
      flexId_(params.flexId),
      lpmTable_(params.lpmTable),
      topo_(params.topo),
      defaultOnlyRoute_(makeDefaultOnlyRoute()) {
  XLOGF(DBG, "Setting LPM Table on node {}", name_);
  printLpmTable();
  validateDprDivScales();

  routingSalt_ = simCtx_->random.sampleUniform<uint32_t>();
  hashSeeds_.at(0) = simCtx_->random.sampleUniform<uint32_t>();
  hashSeeds_.at(1) = simCtx_->random.sampleUniform<uint32_t>();
}

FlexNode::FlexNode(SimContext* simCtx, const std::string& name)
    : simCtx_(simCtx), name_(name), lpmTable_(nullptr), topo_(nullptr) {
  routingSalt_ = simCtx_->random.sampleUniform<uint32_t>();
  hashSeeds_.at(0) = simCtx_->random.sampleUniform<uint32_t>();
  hashSeeds_.at(1) = simCtx_->random.sampleUniform<uint32_t>();
}

void FlexNode::addPort(
    EthernetPort* ethPort,
    const FlexDeviceId& partnerId,
    const packsim::thrift::LinkId linkId,
    const uint16_t portNum) {
  linkToPort_[linkId] = portNum;
  portToLink_[portNum] = linkId;
  portToEthPort_[portNum] = ethPort;
  linkToEthPort_[linkId] = ethPort;
  linkToPartnerFlexId_[linkId] = partnerId;
  portToPartnerFlexId_[portNum] = partnerId;
  ethPort->setPortNum(portNum);
}

void FlexNode::addPort(
    EthernetPort* ethPort,
    const FlexDeviceId& partnerId,
    const packsim::thrift::LinkId linkId) {
  addPort(ethPort, partnerId, linkId, numPorts_);
}

std::optional<size_t> FlexNode::findReflection(
    const std::optional<packsim::thrift::LinkId> srcLink) {
  if (!srcLink.has_value()) {
    return std::nullopt;
  }
  const auto it = reflectionMap_.find(*srcLink);
  if (it != reflectionMap_.end()) {
    return it->second;
  }
  std::optional<size_t> idx;

  for (const auto& e : *lpmTable_->prefixTable()) {
    auto& links = *e.second.links();
    for (size_t i = 0; i < links.size(); ++i) {
      if (*srcLink == links.at(i)) {
        idx = i;
        break;
      }
    }
    if (idx.has_value()) {
      break;
    }
  }
  reflectionMap_[*srcLink] = idx;
  return idx;
}

void FlexNode::validateDprDivScales() const {
  if (!lpmTable_) {
    return;
  }
  const auto validate = [this](const packsim::thrift::LpmTableEntry& entry) {
    const auto& tiebreak = *entry.tiebreak();
    if (tiebreak.getType() == packsim::thrift::TiebreakSpec::Type::dpr &&
        *tiebreak.dpr()->divScale() < 1) {
      throw std::invalid_argument(
          fmt::format(
              "Node {}: DprTiebreakSpec.divScale must be positive, got {}",
              name_,
              *tiebreak.dpr()->divScale()));
    }
  };
  for (const auto& [route, entry] : *lpmTable_->prefixTable()) {
    validate(entry);
  }
  validate(*lpmTable_->defaultRoute());
}

void FlexNode::printLpmTable() const {
  if (!lpmTable_) {
    XLOGF(DBG, "[LPM TABLE] Node {} has NULL lpmTable", name_);
    return;
  }

  std::ostringstream output;
  output << "\n[LPM TABLE] Node: " << name_
         << ", FlexId: " << flexId_.toString() << "\n";

  // Add valid prefix lengths
  output << "  Valid prefix lengths: ";
  if (!lpmTable_->validPrefixLengths()->empty()) {
    output << "[" << folly::join(", ", *lpmTable_->validPrefixLengths())
           << "]\n";
  } else {
    output << "(empty)\n";
  }

  // Add prefix table entries
  output << "  Prefix table entries:\n";
  if (!lpmTable_->prefixTable()->empty()) {
    AsciiTable table({"Route", "Links", "Tiebreak"});

    for (const auto& [route, entry] : *lpmTable_->prefixTable()) {
      table.addRow(
          {folly::join(", ", route),
           folly::join(", ", entry.links().value()),
           apache::thrift::util::enumNameSafe(entry.tiebreak()->getType())});
    }

    output << table.toString() << "\n";
  } else {
    output << "    (empty)\n";
  }

  // Add default route
  output << "  Default route:\n";
  AsciiTable defaultTable({"Route", "Links", "Tiebreak"});
  defaultTable.addRow(
      {"default",
       lpmTable_->defaultRoute()->links()->empty()
           ? "(no links)"
           : folly::join(", ", *lpmTable_->defaultRoute()->links()),
       apache::thrift::util::enumNameSafe(
           lpmTable_->defaultRoute()->tiebreak()->getType())});
  output << defaultTable.toString();

  XLOGF(DBG, "{}", output.str());
}

const packsim::thrift::LpmTableEntry& FlexNode::lpmEntryFor(
    const packsim::thrift::LpmTableSpec& lpm,
    const FlexDeviceId& dstId) {
  if (!lpm.validPrefixLengths()->empty()) {
    const uint32_t maxLen = lpm.validPrefixLengths()->at(0);
    std::vector<uint32_t> vals(maxLen, 0);
    for (uint32_t i = 0; i < maxLen; ++i) {
      vals.at(i) = dstId.values.at(i);
    }
    for (const uint32_t len : *lpm.validPrefixLengths()) {
      vals.resize(len);
      const auto it = lpm.prefixTable()->find(packsim::makePrefixKey(vals));
      if (it == lpm.prefixTable()->end()) {
        continue;
      }
      return it->second;
    }
  }
  return *lpm.defaultRoute();
}

const packsim::thrift::LpmTableEntry* FlexNode::getLpmEntry(
    const FlexDeviceId& dstId) {
  if (const auto* route = findOrAddRoute(dstId)) {
    return route->entry;
  }
  return &lpmEntryFor(*lpmTable_, dstId);
}

size_t FlexNode::RouteKeyHash::operator()(
    const FlexDeviceId& dstId) const noexcept {
  const auto& fields = dstId.values;
  static_assert(std::tuple_size_v<std::remove_cvref_t<decltype(fields)>> == 8);
  uint64_t low = 0;
  uint64_t high = 0;
  for (size_t i = 0; i < 4; ++i) {
    low = (low << 16) | fields[i];
    high = (high << 16) | fields[i + 4];
  }
  return folly::hash::hash_128_to_64(low, high);
}

FlexNode::CachedRoute FlexNode::buildRoute(const FlexDeviceId& dstId) const {
  CachedRoute route{.entry = &lpmEntryFor(*lpmTable_, dstId)};
  // @oss-disable
      // @oss-disable
  return route;
}

std::optional<FlexNode::CachedRoute> FlexNode::makeDefaultOnlyRoute() const {
  // `lpmEntryFor` tries prefixes only when the table lists prefix lengths. A
  // default route without links (a node whose config sets no default route)
  // takes the cached path, where routePacket reports the missing route.
  if (lpmTable_ == nullptr || !lpmTable_->validPrefixLengths()->empty() ||
      lpmTable_->defaultRoute()->links()->empty() ||
      canNarrowRoutesAt(topo_, flexId_)) {
    return std::nullopt;
  }
  return CachedRoute{.entry = &*lpmTable_->defaultRoute()};
}

const FlexNode::CachedRoute* FOLLY_NULLABLE
FlexNode::findOrAddRoute(const FlexDeviceId& dstId) {
  if (defaultOnlyRoute_.has_value()) {
    return &*defaultOnlyRoute_;
  }
  if (const auto it = routes_.find(dstId); it != routes_.end()) {
    return &it->second;
  }
  if (routes_.size() >= kMaxCachedRoutes) {
    return nullptr;
  }
  return &routes_.try_emplace(dstId, buildRoute(dstId)).first->second;
}

EthernetPort* FlexNode::routePacket(
    const PacketPtr& pkt,
    const std::optional<packsim::thrift::LinkId> srcLink) {
  // Use Ipv6Packet to access dstId — all IPv6-derived
  // packets (UDP, RoCE, PING, etc.) share this base class.
  const auto* ipv6Pkt = pkt.as<Ipv6Packet>();
  const auto& dstId = ipv6Pkt->dstId();
  // A destination the full cache has no room for is routed uncached.
  std::optional<CachedRoute> uncachedRoute;
  const CachedRoute* route = findOrAddRoute(dstId);
  if (route == nullptr) {
    route = &uncachedRoute.emplace(buildRoute(dstId));
  }
  const auto* entry = route->entry.unwrap();
  const auto& links = route->links();
  const size_t numLinks = links.size();

  if (numLinks == 0) {
    throwRoutingError(
        fmt::format(
            "Found no links to use on {} ({}) for destination {}",
            name_,
            flexId_.toString(),
            ipv6Pkt->dstId().toString()),
        name_,
        ipv6Pkt->dstId());
  } else if (numLinks == 1) {
    return linkToEthPort_.at(links.at(0));
  }

  packsim::thrift::LinkId selLink;
  const auto& tiebreak = *entry->tiebreak();
  switch (tiebreak.getType()) {
    case packsim::thrift::TiebreakSpec::Type::ecmp: {
      selLink =
          links.at(packsim::folly5TupleHash(pkt, routingSalt_) % numLinks);
    } break;
    case packsim::thrift::TiebreakSpec::Type::dpr: {
      const auto& dprSpec = *entry->tiebreak()->dpr();
      const auto& dot = *dprSpec.dotProduct();
      const uint32_t divScale = *dprSpec.divScale();
      uint64_t dprNum = 0;
      for (size_t i = 0; i < dot.size(); ++i) {
        dprNum += uint64_t{dot.at(i)} * ipv6Pkt->dstId().values.at(i);
      }
      dprNum /= divScale;
      selLink = links.at(dprNum % numLinks);
    } break;
    case packsim::thrift::TiebreakSpec::Type::reflect: {
      // With no source link, or one that no LPM entry lists, the index is
      // SIZE_MAX % numLinks.
      const size_t idx =
          findReflection(srcLink).value_or(std::numeric_limits<size_t>::max());
      selLink = links.at(idx % numLinks);
    } break;
    // @oss-disable
      // @oss-disable
      // @oss-disable
    // @oss-disable
    case packsim::thrift::TiebreakSpec::Type::spray: {
      // Select random link
      const auto rngHash =
          simCtx_->random.sampleUniform<size_t>(0, numLinks - 1);
      selLink = links.at(rngHash);
    } break;
    case packsim::thrift::TiebreakSpec::Type::fly: {
      auto& flySpec = *entry->tiebreak()->fly();
      if (*flySpec.maxAdditionalHops() > 1) {
        throw std::runtime_error("Only support exactly one extra hop");
      }
      if (*flySpec.secondTieBreak() != "ECMP") {
        throw std::runtime_error("Only support ECMP secondary tiebreak");
      }

      // decide whether to use shortest path
      bool goShortest = false;
      if (*flySpec.maxAdditionalHops() == 0) {
        goShortest = true;
      } else {
        const FlexDeviceId peerFlexId = srcLink.has_value()
            ? linkToPartnerFlexId_[*srcLink]
            : FlexDeviceId{};
        if (flexId_.values.at(0) == peerFlexId.values.at(0)) {
          goShortest = true;
        }
      }

      if (goShortest) {
        // ECMP over shortest path links
        // compare peer id's pod number with dst's pod number
        const auto dstPod = ipv6Pkt->dstId().values.at(kFlyPodIdx);
        folly::small_vector<packsim::thrift::LinkId, kFlyInlineLinks>
            shortestLinks;
        for (const auto link : links) {
          if (linkToPartnerFlexId_.at(link).values.at(kFlyPodIdx) == dstPod) {
            shortestLinks.emplace_back(link);
          }
        }
        if (shortestLinks.empty()) {
          throwRoutingError(
              fmt::format(
                  "No fly route link on {} ({}) reaches the pod of destination {}",
                  name_,
                  flexId_.toString(),
                  ipv6Pkt->dstId().toString()),
              name_,
              ipv6Pkt->dstId());
        }
        selLink = shortestLinks.at(
            packsim::folly5TupleHash(pkt, routingSalt_) % shortestLinks.size());
      } else {
        // ECMP over all links
        selLink =
            links.at(packsim::folly5TupleHash(pkt, routingSalt_) % numLinks);
      }
    } break;
    case packsim::thrift::TiebreakSpec::Type::pathspray: {
      // srcPort exists only on UdpPacket; non-UDP IPv6 packets (e.g. PING) have
      // none, so fall back to the first link. `is<>()` answers from the
      // packet's own `pktType` rather than by RTTI, and
      // `MatcherAgreesWithDynamicCast` is what keeps that answer in step with
      // the class hierarchy.
      if (!pkt.is<UdpPacket>()) {
        selLink = links.at(0);
        break;
      }
      auto* udpPkt = pkt.as<UdpPacket>();
      const auto srcPort = udpPkt->srcPort;
      selLink = links.at(srcPort % numLinks);
      XLOGF(
          DBG9,
          "Src {} Dst {} Selected link: {} srcPort {} numLinks {}",
          ipv6Pkt->srcId().toString(),
          ipv6Pkt->dstId().toString(),
          selLink,
          srcPort,
          numLinks);
    } break;
    case packsim::thrift::TiebreakSpec::Type::__EMPTY__:
    default: {
      throw std::runtime_error("No tiebreak set");
    }
  }

  EthernetPort* selPort = linkToEthPort_.at(selLink);
  if (selPort->getChannelState() != ChannelState::UP) { // selected link is down
    // Iterate over links and add only working links to the vector
    std::vector<packsim::thrift::LinkId> workingLinks;
    workingLinks.reserve(entry->links().value().size());
    for (const auto& linkId : entry->links().value()) {
      if (checkLinkStatus(linkId)) {
        workingLinks.emplace_back(linkId);
      }
    }

    const size_t numWorkingLinks = workingLinks.size();
    if (numWorkingLinks == 0) {
      throwRoutingError(
          fmt::format(
              "None of the links attached to node {} are working for destination {}",
              name_,
              ipv6Pkt->dstId().toString()),
          name_,
          ipv6Pkt->dstId());
    }

    // Spray: re-select randomly from working links
    if (tiebreak.spray().has_value()) {
      const auto rngHash =
          simCtx_->random.sampleUniform<size_t>(0, numWorkingLinks - 1);
      selLink = workingLinks.at(rngHash);
    } else {
      // Fallback ECMP; use ECMP to select a link among working links
      selLink = workingLinks.at(
          packsim::folly5TupleHash(pkt, routingSalt_) % numWorkingLinks);
    }
    selPort = linkToEthPort_.at(selLink);
  }

  return selPort;
}

inline bool FlexNode::checkLinkStatus(
    const packsim::thrift::LinkId linkId) const {
  return linkToEthPort_.at(linkId)->getChannelState() == ChannelState::UP;
}

bool FlexNode::routeAndSendPacket(const PacketPtr& pkt) {
  auto ethPort = routePacket(pkt, std::nullopt);

  ethPort->send(pkt);
  return true;
}

} // namespace packsim
