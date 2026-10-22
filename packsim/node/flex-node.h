/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/CppAttributes.h>
#include <folly/container/F14Map.h>
#include <folly/memory/not_null.h>

// @oss-disable
#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

class PortReachability;

struct FlexNodeParams {
  SimContext* simCtx;
  const std::string& name;
  const FlexDeviceId& flexId;
  const packsim::thrift::LpmTableSpec* lpmTable;
  const packsim::thrift::FlexTopology* topo;
  // Reachability tracer over topo, shared by every endpoint (FlexTopology owns
  // it); null for nodes that never trace reachability.
  const PortReachability* portReachability{nullptr};
};

class FlexNode {
 public:
  /**
   * Reports the bytes of receive buffer this node credits to lossless traffic
   * on `portNum`. Covers the port headroom pool only, not the whole input
   * buffer; `bootstrapCbfcCredits` sizes the partner's sender credit pool from
   * it.
   *
   * @param portNum  Port whose credited receive buffer is reported.
   *
   * @return         Credited bytes. Zero means the node advertises no credited
   *                 space, so a lossless VC on the link can never transmit.
   */
  [[nodiscard]] virtual uint64_t getLosslessRxCreditBytes(
      uint16_t /* portNum */) const {
    return 0;
  }

  /**
   * Reports the granularity, in bytes, in which this node allocates its
   * receive buffer. The CBFC credit size must equal this granularity: a frame
   * smaller than a cell still occupies a whole cell, so a smaller credit size
   * grants the sender more credits than the buffer can hold.
   *
   * @return  Allocation granularity in bytes, or 0 for a node whose receive
   *          buffer is byte-limited rather than quantized, such as a NIC. Zero
   *          imposes no constraint on the credit size.
   */
  [[nodiscard]] virtual uint32_t getRxCellSizeBytes() const {
    return 0;
  }

  explicit FlexNode(const FlexNodeParams& params);

  explicit FlexNode(SimContext* simCtx, const std::string& name);

  virtual ~FlexNode() = default;

  virtual void addPort(
      EthernetPort* ethPort,
      const FlexDeviceId& partnerId,
      packsim::thrift::LinkId linkId,
      uint16_t portNum);

  /**
   * Wire `ethPort` to its partner and number it automatically: the port number
   * is `numPorts_`, the count of ports this node has taken with `adoptPort` so
   * far, so a port adopted before this call counts itself.
   *
   * @param ethPort    The port to wire.
   * @param partnerId  FlexDeviceId of the node on the other end.
   * @param linkId     Link the port belongs to.
   */
  virtual void addPort(
      EthernetPort* ethPort,
      const FlexDeviceId& partnerId,
      packsim::thrift::LinkId linkId);

  /**
   * Take ownership of `ethPort` and bind it to this node. Wiring it to a
   * partner is `addPort`'s job.
   *
   * @param ethPort  The port to own; moved from.
   *
   * @throw std::runtime_error if `ethPort` runs in another SimContext, or if
   *        the node has no port number left for it.
   */
  virtual void adoptPort(std::unique_ptr<EthernetPort>& ethPort) {
    ethPort->setNode(this);
    if (ethPort->getSimCtx() != simCtx_) {
      throw std::runtime_error(
          "EthernetPort must have the same SimContext as FlexNode");
    }
    if (numPorts_ + 1 >= EthernetPort::kUnattachedPort) {
      throw std::runtime_error(
          fmt::format(
              "Node {} has more ports than usable port numbers", name_));
    }
    ethPorts_.emplace_back(std::move(ethPort));
    ++numPorts_;
  }

  const std::string& getName() const noexcept {
    return name_;
  }

  SimContext* getSimCtx() const noexcept {
    return simCtx_;
  }

  const FlexDeviceId& getFlexId() const noexcept {
    return flexId_;
  }

  // LPM routing table (may be null).
  const packsim::thrift::LpmTableSpec* getLpmTable() const {
    return lpmTable_;
  }

  // Port → partner FlexDeviceId mapping.
  const folly::F14VectorMap<uint16_t, FlexDeviceId>& getPortToPartnerFlexId()
      const {
    return portToPartnerFlexId_;
  }

  // Port → link ID mapping.
  const folly::F14FastMap<uint16_t, packsim::thrift::LinkId>& getPortToLink()
      const {
    return portToLink_;
  }

  // Port → EthernetPort mapping.
  const folly::F14FastMap<uint16_t, EthernetPort*>& getPortToEthPort() const {
    return portToEthPort_;
  }

  uint32_t getPortQueueLength(uint16_t port, uint8_t cos) const {
    return portToEthPort_.at(port)->getQueueLength(cos);
  }

  uint32_t getTotalPortQueueLength(uint16_t port) const {
    return portToEthPort_.at(port)->getTotalQueueLength();
  }

  void doSnapshot(uint64_t intervalNs) {
    takeSnapshot();
    simCtx_->schedule(
        Time::fromNs(intervalNs), &FlexNode::doSnapshot, this, intervalNs);
  }

  /**
   * Longest-prefix-match lookup of dstId against an LPM table, falling back to
   * the default route. Shared by per-node routing (getLpmEntry) and the
   * topology reachability tracer (PortReachability) so both select the LPM
   * entry identically.
   *
   * @param lpm    The LPM table to match against.
   * @param dstId  The destination address to match.
   *
   * @return       The matching LPM entry, or the table's default route. Always
   *               a valid reference (never null).
   */
  static const packsim::thrift::LpmTableEntry& lpmEntryFor(
      const packsim::thrift::LpmTableSpec& lpm,
      const FlexDeviceId& dstId);

  /**
   * Route a packet through LPM routing and send it on the selected port.
   * Public so a ping handler installed via
   * `Endpoint::setOnPingReceivedCallback` can route a packet it injects: the
   * injection is scheduled via the sim context and runs on the event loop,
   * preserving the frame-boundary safety model.
   *
   * @param pkt  The packet to route and send.
   *
   * @return     True if the packet was sent successfully.
   */
  virtual bool routeAndSendPacket(const PacketPtr& pkt);

 protected:
  virtual EthernetPort* routePacket(
      const PacketPtr& pkt,
      std::optional<packsim::thrift::LinkId> srcLink);
  // The LPM entry for `dstId`; points into lpmTable_. Read from the route cache
  // (see findOrAddRoute), so a first lookup also runs that route's narrowing.
  const packsim::thrift::LpmTableEntry* getLpmEntry(const FlexDeviceId& dstId);
  // The LPM entry for one destination and the links routePacket picks from.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
  struct CachedRoute {
    // The prefix-table entry, or the default route, the destination matched;
    // points into lpmTable_. No default initializer: folly::not_null deletes
    // its default constructor, and buildRoute always sets it.
    folly::not_null<const packsim::thrift::LpmTableEntry*> entry;
    // The entry's links narrowed for this destination; set only when routing
    // narrows them, so the cache holds no copy of an entry's own links.
    std::optional<std::vector<packsim::thrift::LinkId>> narrowedLinks;

    // The links routePacket picks from.
    const std::vector<packsim::thrift::LinkId>& links() const {
      return narrowedLinks ? *narrowedLinks : *entry->links();
    }
  };
  /**
   * The cached route to `dstId`, added on its first lookup while the cache has
   * room: a route depends only on the LPM table and the topology, which do not
   * change during a run. A node routes only on its own context, so the cache
   * takes no lock. Routes are never removed and the cache is node-based, so a
   * returned route stays valid for the node's lifetime.
   *
   * On a node where every destination takes the default route unchanged (see
   * `defaultOnlyRoute_`), returns that one route and leaves the cache empty.
   *
   * @param dstId  The destination address.
   *
   * @return       The route, or nullptr once the cache holds kMaxCachedRoutes
   *               routes and `dstId` is not one of them (use buildRoute).
   */
  const CachedRoute* FOLLY_NULLABLE findOrAddRoute(const FlexDeviceId& dstId);
  /**
   * Compute the route to `dstId` without caching it.
   *
   * @param dstId  The destination address.
   *
   * @return       The LPM entry `dstId` matches, and the entry's links narrowed
   *               for `dstId` when routing narrows them.
   */
  CachedRoute buildRoute(const FlexDeviceId& dstId) const;
  /**
   * The route every destination takes when no destination can differ: the
   * LPM table lists no prefix lengths, so `lpmEntryFor` always returns the
   * default route, and no link narrowing can apply at this node.
   *
   * @return  That route, or std::nullopt when routes can differ by
   *          destination, the default route has no links, or the node has
   *          no LPM table.
   */
  std::optional<CachedRoute> makeDefaultOnlyRoute() const;
  // Number of routes the route cache holds; at most kMaxCachedRoutes.
  size_t getNumCachedRoutes() const noexcept {
    return routes_.size();
  }
  // Most routes the route cache holds, about 300 KB per node, so that memory
  // stays bounded however many destinations a node routes to: once full, it
  // keeps the routes it has and other destinations are routed uncached.
  static constexpr size_t kMaxCachedRoutes = 4096;
  void printLpmTable() const;
  // Throws std::invalid_argument if a DPR tiebreak in lpmTable_ has a
  // divScale below 1; routePacket divides by it.
  void validateDprDivScales() const;

  virtual void takeSnapshot() {}
  std::optional<size_t> findReflection(
      std::optional<packsim::thrift::LinkId> srcLink);
  // return true if link is "up"; false if link is "down"
  bool checkLinkStatus(packsim::thrift::LinkId linkId) const;

 protected:
  SimContext* const simCtx_;
  const std::string name_;

  const FlexDeviceId flexId_;
  packsim::thrift::LpmTableSpec const* const lpmTable_;
  packsim::thrift::FlexTopology const* const topo_;
  // @oss-disable

  folly::F14FastMap<packsim::thrift::LinkId, uint16_t> linkToPort_;
  folly::F14FastMap<uint16_t, packsim::thrift::LinkId> portToLink_;
  folly::F14FastMap<uint16_t, EthernetPort*> portToEthPort_;
  folly::F14FastMap<packsim::thrift::LinkId, EthernetPort*> linkToEthPort_;
  folly::F14FastMap<packsim::thrift::LinkId, FlexDeviceId> linkToPartnerFlexId_;
  // A vector map, so iteration order depends only on insertion order, not on
  // the key's hash, and anything that prints the map prints a stable order.
  folly::F14VectorMap<uint16_t, FlexDeviceId> portToPartnerFlexId_;
  std::vector<std::unique_ptr<EthernetPort>> ethPorts_;
  // `ethPorts_.size()`, kept below `EthernetPort::kUnattachedPort`.
  uint16_t numPorts_{0};

  folly::F14FastMap<packsim::thrift::LinkId, std::optional<size_t>>
      reflectionMap_;
  // Hashes the eight fields FlexDeviceId::operator== compares, packed into two
  // 64-bit words: cheaper per routed packet than the element-wise
  // std::hash<FlexDeviceId>. routes_ is never iterated, so the order this hash
  // gives it never shows.
  struct RouteKeyHash {
    size_t operator()(const FlexDeviceId& dstId) const noexcept;
  };
  // Routes by destination; see findOrAddRoute. Node-based so that a returned
  // route stays put when later lookups add routes.
  folly::F14NodeMap<FlexDeviceId, CachedRoute, RouteKeyHash> routes_;
  // Set at construction when every destination takes the default route
  // unchanged (see makeDefaultOnlyRoute): findOrAddRoute then returns it and
  // routes_ stays empty, sparing a hash lookup per packet and up to
  // kMaxCachedRoutes copies of one route.
  const std::optional<CachedRoute> defaultOnlyRoute_;
  uint32_t routingSalt_{0};
  // Seeds for hash-based path selection. Drawn at construction whether or not
  // anything uses them, so the random stream, and every result drawn from it,
  // does not depend on which routing features are in use.
  std::array<uint32_t, 2> hashSeeds_{};
};

} // namespace packsim
