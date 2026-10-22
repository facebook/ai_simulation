/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include <folly/CppAttributes.h>
#include <folly/container/F14Map.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/qp_pair_config_types.h"
#include "packsim/if/gen-cpp2/results_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/link-layer/port-observer.h"
#include "packsim/node/flex-node.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/transport/transport.h"

namespace packsim {

class PortReachability;

class Endpoint : public FlexNode, public IPortObserver {
 public:
  Endpoint(
      const FlexNodeParams& params,
      const packsim::thrift::NicConfig* nicConfig,
      std::optional<uint8_t> portIndex = std::nullopt);

  void makeSendRequest(packsim::Message* msg);
  void makeReceiveRequest(packsim::Message* msg);
  packsim::thrift::NicSummary getNicStatsSummaryResults() const;

  void onMessageReceived(packsim::Message* msg);
  void onMessageAcked(packsim::Message* msg);
  void onPingReceived(const PacketPtr& pkt);
  void onPacketSent(EthernetPort& ethPort, const PacketPtr& pkt) override;
  void onLinkUp(EthernetPort* ethPort);

  /**
   * Handle packet drops on the send path (queue overflow).
   *
   * Notifies the transport layer about packet drop so it can handle
   * retransmission.
   *
   * @param ethPort  The port where the drop occurred.
   * @param pkt      The packet that was dropped.
   * @param queue    The queue where the drop occurred.
   * @param reason   The reason for the drop.
   */
  void onPacketDroppedOnSend(
      EthernetPort& ethPort,
      const PacketPtr& pkt,
      uint8_t queue,
      packsim::switches::thrift::PacketDropReason reason) override;

  /**
   * Handle packet drops on the receive path (BER, channel down).
   *
   * Packets dropped on receive shouldn't trigger retransmission logic on this
   * endpoint - the sender should handle it. These drops are ignored at the
   * endpoint level.
   *
   * @param ethPort  The port where the drop occurred.
   * @param pkt      The packet that was dropped.
   * @param queue    The queue where the drop occurred.
   * @param reason   The reason the drop occurred.
   */
  void onPacketDroppedOnReceive(
      EthernetPort& ethPort,
      const PacketPtr& pkt,
      uint8_t queue,
      packsim::switches::thrift::PacketDropReason reason) override;

  void onPacketRcvd(EthernetPort& ethPort, const PacketPtr& pkt) override;

  void addPort(
      EthernetPort* ethPort,
      const FlexDeviceId& partnerId,
      packsim::thrift::LinkId linkId) override {
    FlexNode::addPort(ethPort, partnerId, linkId);
  }

  void addPort(
      EthernetPort* ethPort,
      const FlexDeviceId& partnerId,
      packsim::thrift::LinkId linkId,
      uint16_t portNum) override;

  EthernetPort* routePacket(
      const PacketPtr& pkt,
      std::optional<packsim::thrift::LinkId> srcLink) override;

  const folly::F14FastMap<FlexDeviceId, EthernetPort*>& getPorts()
      const noexcept {
    return ports_;
  }

  /**
   * Whether a packet egressing the local egress port srcPort can be delivered
   * to dstPort, decided by tracing the fabric's own forwarding (links plus
   * per-node LPM) from srcPort's first-hop switch to the destination accel.
   * Assumes that, for a given destination, routing may pick any link of each
   * matched LPM route (as `ecmp` and `spray` do). Where a tiebreak uses only
   * part of that set (for example `fly`'s shortest-path filter), it may return
   * true for a dstPort that routing never delivers to from srcPort.
   *
   * @param srcPort  A local egress port of this endpoint. A port not wired on
   *                 this endpoint reaches nothing (returns false).
   * @param dstPort  The destination port; its accel prefix selects the target
   *                 accel and its port field is the port checked for delivery.
   *
   * @return         True iff the trace reaches dstPort from srcPort's
   *                 first-hop switch.
   *
   * @throw  std::runtime_error if the port index is unset or the endpoint has
   *         no reachability tracer.
   */
  bool canPortsReach(const FlexDeviceId& srcPort, const FlexDeviceId& dstPort)
      const;

  const FlexDeviceId& getFlexId() const noexcept {
    return flexId_;
  }

  packsim::Transport* getTransport() const noexcept {
    return transport_.get();
  }

  bool usingPortLevelAddresses(const PacketPtr& pkt) const noexcept {
    return portIndex_.has_value() && usesPortLevelAddressing(pkt->pktType);
  }

  bool allowReachableRoutesOnly() const {
    return nicConfig_->allowReachableRoutesOnly().value();
  }

  packsim::thrift::NicSize getNicSize() const {
    return *nicConfig_->nicSize();
  }

  /**
   * Sum of the `NicConfig.perCosBufferSize` entries, in bytes: the
   * receive space of this endpoint's NIC across every CoS queue. `NicRxBuffer`
   * does not enforce it and has no headroom tier, so on a CBFC link all of it
   * is credited space.
   */
  [[nodiscard]] uint64_t getRxBufferBytes() const {
    uint64_t totalBytes = 0;
    for (const uint32_t sz : *nicConfig_->perCosBufferSize()) {
      totalBytes += sz;
    }
    return totalBytes;
  }

  /* Callbacks */
  using OnMessageReceivedCallback = std::function<void(packsim::Message*)>;
  using OnMessageAckedCallback = std::function<void(packsim::Message*)>;
  void setOnMessageReceivedCallback(const OnMessageReceivedCallback& cb) {
    onMessageReceived_ = cb;
  }

  void setOnMessageAckedCallback(const OnMessageAckedCallback& cb) {
    onMessageAcked_ = cb;
  }

  // Handler invoked (before the transport layer) when a PING packet arrives.
  // Installed by whatever injects PINGs; nothing in the simulator core does. A
  // PING reaching an endpoint with no handler is a wiring error and aborts
  // rather than being dropped. Type-erased, so the endpoint depends on neither
  // the installer nor the scheduler.
  using OnPingReceivedCallback =
      std::function<void(const PacketPtr&, FlexNode*)>;
  void setOnPingReceivedCallback(OnPingReceivedCallback handler) {
    onPingReceived_ = std::move(handler);
  }
  void clearOnPingReceivedCallback() {
    onPingReceived_ = nullptr;
  }

  /**
   * Select the connection a packet of a QP takes, from one of this endpoint's
   * ports to one of the destination NIC's ports. The QP's connection table is
   * built on first use.
   *
   * @param destination  The destination NIC.
   * @param srcQPN       Source queue pair number; keys the connection table.
   * @param dstQPN       Destination queue pair number.
   * @param entropy      Packet entropy, which ecmp hashes over the connections.
   * @param tiebreak     The route's tiebreak: ecmp hashes the entropy, spray
   *                     picks a connection at random.
   *
   * @return             The selected connection, owned by this endpoint.
   * @throw              std::runtime_error if the QP has no connection, if its
   *                     connection table cannot be built, or for a tiebreak
   *                     other than ecmp and spray.
   */
  const QPConnection* getPortLevelQPConnection(
      const FlexDeviceId& destination,
      const uint32_t srcQPN,
      const uint32_t dstQPN,
      const uint16_t entropy,
      const packsim::thrift::TiebreakSpec& tiebreak);

  void buildQPConnectionTable(
      const FlexDeviceId& destination,
      const uint32_t srcQPN,
      const uint32_t dstQPN);

  // Set allowed port restrictions for a specific QPN using QpnPortConfig thrift
  void setQpnPortConfig(
      uint32_t localQPN,
      const packsim::thrift::QpnPortConfig& portConfig);

  // Get port restrictions for a specific QPN (returns nullptr if not set)
  const packsim::thrift::QpnPortConfig* getQpnPortConfig(
      uint32_t localQPN) const;

  // Store a workload pointer for port config lookups.
  // Port configs are looked up directly from the workload's map on demand,
  // avoiding the cost of copying all entries to each endpoint.
  void onWorkloadPortConfigs(
      const packsim::workload::thrift::Workload* workload);

 protected:
  std::unique_ptr<Transport> transport_;

  const packsim::thrift::NicConfig* nicConfig_;

  // Callbacks
  OnMessageReceivedCallback onMessageReceived_;
  OnMessageAckedCallback onMessageAcked_;
  OnPingReceivedCallback onPingReceived_;

 private:
  // FlexDeviceId of a port --> the physical port (EthernetPort)
  // Only populated if the Endpoint is on an XPU
  folly::F14FastMap<FlexDeviceId, EthernetPort*> ports_;
  // index into FlexDeviceId.values array that indicates the port field
  std::optional<uint8_t> portIndex_;

  /**
   * A port's 0-based NIC egress port: the port field of its address, which is
   * 1-based, minus 1.
   *
   * @param ethPort  A port of this endpoint.
   *
   * @return         The port's egress NIC port number.
   *
   * NOTE: Requires portIndex_ to be set.
   */
  uint16_t getEgressNicPort(const EthernetPort& ethPort) const;

  /**
   * The number NicSummary.macStats lists a port of this endpoint under.
   *
   * @param ethPort  A port of this endpoint.
   *
   * @return         The port's 0-based NIC egress port (its address's port
   *                 field minus 1, the numbering PortPairBytes and the
   *                 transport use) when the endpoint has a port field;
   *                 otherwise the port's index on its node.
   */
  uint16_t getMacStatsPortNum(const EthernetPort& ethPort) const;
  // srcQPN -> available QPConnections
  // QPN is unique per endpoint, so it alone is sufficient to identify the
  // QPConnections for a given connection.
  folly::F14FastMap<uint32_t, std::vector<std::unique_ptr<QPConnection>>>
      sourceRoutingTable_;

  // Per-QPN port restrictions: localQPN -> allowed ports
  // QPN is unique per endpoint, so it alone is sufficient to identify the
  // port restrictions for a given connection.
  // Port configs are looked up from the workload pointer (workload_), set
  // through onWorkloadPortConfigs, to avoid copying every entry to each
  // endpoint. The local map is only filled by setQpnPortConfig (tests).
  folly::F14FastMap<uint32_t, packsim::thrift::QpnPortConfig> qpnPortConfigs_;
  const packsim::workload::thrift::Workload* workload_{nullptr};

  // Cache of QPConnections created by transport-controlled egressNicPort
  // routing. Key encodes srcQPN, srcPort, dstPort.
  folly::F14FastMap<uint64_t, std::unique_ptr<QPConnection>>
      egressNicPortConnections_;

  // The topology's reachability tracer, shared by every endpoint and owned by
  // FlexTopology; null when the endpoint was built without one.
  const PortReachability* const portReachability_;

  // Helper to initialize QPN port config from workload if available
  void initQpnPortConfigFromWorkload(Message* msg);

  // Route a packet using the transport-controlled egressNicPort, bypassing
  // ECMP and QpnPortConfig.
  EthernetPort* routePacketByEgressNicPort(
      const PacketPtr& pkt,
      uint16_t egressNicPort);

  // Whether the `EthernetPort` wired to a 0-based egress NIC port number has
  // its link up. Used by multi-port transports to steer RTO failover away
  // from a down port.
  bool isEgressPortLive(uint16_t egressPort) const;

  /**
   * The destination NIC port numbers (0-based) at dst reachable from a local
   * egress port, by tracing the fabric's forwarding. Backs canPortsReach and
   * the meshed-wiring detection used by multi-port transports.
   *
   * @param srcPort  A local egress port to trace from.
   * @param dst      The destination; any port field is ignored (reachability is
   *                 traced to the destination accel).
   *
   * @return         The reachable destination port numbers; empty if srcPort is
   *                 not a wired local port.
   *
   * @throw  std::runtime_error if the port index or reachability tracer is
   *         unset.
   */
  std::set<uint16_t> computeReachableDstPorts(
      const FlexDeviceId& srcPort,
      const FlexDeviceId& dst) const;
};

} // namespace packsim
