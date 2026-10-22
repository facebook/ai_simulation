/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include <folly/container/F14Map.h>
#include <folly/hash/Hash.h>
#include <folly/memory/not_null.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/link-layer/port-observer.h"
#include "packsim/node/flex-node.h"
#include "packsim/packet/pfc.h"
#include "packsim/switch/simple-switch-buffer.h"

namespace packsim {

class IIncidentLookup;

class FbSwitch : public FlexNode, public IPortObserver {
 public:
  FbSwitch(
      const FlexNodeParams& params,
      const packsim::thrift::SwitchConfig* cfg);
  ~FbSwitch() override = default;
  FbSwitch(const FbSwitch&) = delete;
  FbSwitch& operator=(const FbSwitch&) = delete;
  FbSwitch(FbSwitch&&) = delete;
  FbSwitch& operator=(FbSwitch&&) = delete;

  const FlexDeviceId& getFlexDeviceId() const noexcept;

  // `IPortObserver`. Admits the frame into the ingress buffer, which is
  // what receiving one means for a switch.
  void onPacketRcvd(EthernetPort& ethPort, const PacketPtr& pkt) override;

  // `IPortObserver`. As `onPacketRcvd`, moving the port's handle into the
  // routing event instead of copying it.
  void onPacketRcvdByMove(EthernetPort& ethPort, PacketPtr&& pkt) override;

  void onPacketSent(EthernetPort& ethPort, const PacketPtr& pkt) override;

  /**
   * Update internal state and stats when a packet is dropped on send.
   * This happens due to down links and buffer full.
   *
   * @param  ethPort  The port where the drop occurred.
   * @param  pkt      Packet being dropped.
   * @param  queue    Which PG queue this packet is in.
   * @param  reason   The reason for the packet drop.
   *
   */
  void onPacketDroppedOnSend(
      EthernetPort& ethPort,
      const PacketPtr& pkt,
      uint8_t queue,
      packsim::switches::thrift::PacketDropReason reason) override;

  /**
   * Update internal state and stats when a packet is dropped on receive.
   * This happens due to down links and bit error during transmission.
   *
   * @param  ethPort  The port where the drop occurred.
   * @param  pkt      Packet being dropped.
   * @param  queue    Which PG queue this packet is in.
   * @param  reason   The reason for the packet drop.
   *
   */
  void onPacketDroppedOnReceive(
      EthernetPort& ethPort,
      const PacketPtr& pkt,
      uint8_t queue,
      packsim::switches::thrift::PacketDropReason reason) override;
  void packetMarked(
      EthernetPort* ethPort,
      const PacketPtr& pkt,
      uint8_t queue,
      packsim::switches::thrift::PacketDropReason reason);

  virtual packsim::switches::thrift::Switch getSummary();
  uint8_t getBuff(uint16_t inpPort, uint16_t outPort) const;

  void setRxPfcState(uint16_t port, uint8_t pg, PfcState state);

  virtual void addSwitchPort(
      folly::not_null<EthernetPort*> ethPort,
      std::string portGroup,
      FlexDeviceId partnerId,
      packsim::thrift::LinkId linkId);

  void adoptPort(std::unique_ptr<EthernetPort>& ethPort) override;

  // Set the incident lookup for drop attribution. Nothing in this repository
  // sets one, so `lookupIncidentId()` returns nullopt.
  void setIncidentLookup(const IIncidentLookup* lookup);

  const std::vector<std::unique_ptr<EthernetPort>>& getAllPorts()
      const noexcept {
    return ethPorts_;
  }

  uint16_t getPortNumFromLinkId(packsim::thrift::LinkId linkId) const;

  // Every port served by one of this switch's ingress buffers. Empty when the
  // switch is configured without ingress buffering.
  std::vector<uint16_t> getIngressPortNums() const;

  uint64_t getCellsInQueue(uint8_t bufferIdx, uint16_t port, uint8_t pg) const;

  // Credits only the port's lossless ingress headroom: `maxHdrmCells` summed
  // over the port's lossless priority groups, capped per buffer by
  // `headroomPoolCells`, and 0 on a switch without ingress buffering. Contract:
  // `FlexNode::getLosslessRxCreditBytes`.
  [[nodiscard]] uint64_t getLosslessRxCreditBytes(
      uint16_t portNum) const override;

  [[nodiscard]] uint32_t getRxCellSizeBytes() const override {
    return cellSize_;
  }
  uint64_t getPortTxBytes(uint16_t port, uint8_t pg) const;

  std::vector<std::tuple<Time, packsim::switches::thrift::Switch>>&
  getSnapshots() {
    return snapshots_;
  }

  void clearSnapshots() {
    snapshots_.clear();
    snapshots_.shrink_to_fit();
  }

 protected:
  // Marks every ingress queue tracker on `portNum` as CBFC-managed: headroom
  // is restricted to lossless traffic and released exactly per packet. The
  // ordering requirement is stated at the `addSwitchPort` call site.
  void markIngressCbfcManaged(uint16_t portNum);

  virtual void routeSwitchPacket(EthernetPort* inEthPort, PacketPtr pkt);
  virtual bool checkIngressEgressAdmission(
      folly::not_null<EthernetPort*> inEthPort,
      folly::not_null<EthernetPort*> outEthPort,
      const PacketPtr& pkt);
  void takeSnapshot() override {
    snapshots_.emplace_back(simCtx_->now(), getSummary());
  }

  virtual void sendFromPort(EthernetPort* outEthPort, PacketPtr pkt);

  /**
   * Find which buffer holds a port's queue, filter and scheduler settings, as
   * an index into `bufs`, or into `egressBufs` on an egress-only switch: the
   * port's `portToBufMap.singleMap` entry or, on a switch that uses both
   * directions, the lowest index that a pairMap key with the port on its
   * ingress side selects.
   *
   * @param portNum  The switch port number.
   *
   * @return         The buffer index, or std::nullopt if portToBufMap does not
   *                 map the port.
   */
  std::optional<uint8_t> findPortBufferIndex(uint16_t portNum) const;

  /**
   * Get a port's entry in the buffer `findPortBufferIndex` picks.
   *
   * @param portNum  The switch port number.
   *
   * @return         The port's PortBufferConfig in that buffer.
   *
   * @throw std::invalid_argument  If portToBufMap maps the port to no buffer,
   *                               or to one that does not exist or has no
   *                               entry for the port.
   */
  const packsim::thrift::PortBufferConfig& getPortBufferConfig(
      uint16_t portNum) const;

  /**
   * Check that every buffer entry serving a port in one direction configures
   * a queue for each queue the port's filter can charge to that buffer.
   *
   * @param portNum     The switch port number.
   * @param filterCfg   The port's queue filter, default DSCP map applied.
   * @param numQueues   The number of queues the port has.
   * @param useIngress  Whether to check the pgs of the `bufs` entries with
   *                    isIngrPort rather than the ucQueues of the `egressBufs`
   *                    entries with isEgrPort.
   *
   * @throw std::invalid_argument  If such an entry holds too few queues.
   */
  void requireBufferQueues(
      uint16_t portNum,
      const packsim::thrift::QueueFilterConfig& filterCfg,
      uint8_t numQueues,
      bool useIngress) const;

  uint32_t cellSize_ = 254;
  uint32_t pipelineDelayNs_ = 1250;
  uint32_t expDelayNs_ = 0;
  const packsim::thrift::SwitchConfig* cfg_;

  // Buffer index by the ingress port of an ingress-only switch or the egress
  // port of an egress-only switch.
  folly::F14FastMap<uint32_t, uint8_t> portBuff_;
  // Buffer index by (ingress port, egress port) on a switch that uses both.
  folly::F14FastMap<
      std::pair<uint32_t, uint32_t>,
      uint8_t,
      folly::hasher<std::pair<uint32_t, uint32_t>>>
      portPairBuff_;

  std::vector<packsim::SimpleSwitchBuffer> ingressBufs_;
  std::vector<packsim::SimpleSwitchBuffer> egressBufs_;

  folly::F14FastMap<std::string, size_t> portGroupCounters_;

  std::unordered_set<uint16_t> portsAssigned_;

  // Track packet drops by reason and type
  std::vector<packsim::switches::thrift::PacketDropEvent> packetDropStats_;

  // Non-owning pointer to incident lookup for drop attribution.
  const IIncidentLookup* incidentLookup_{nullptr};

  // Look up the active incident ID for the given port, if any.
  std::optional<uint32_t> lookupIncidentId(uint16_t portNum) const;

  /**
   * Records a packet drop event for statistics tracking.
   *
   * @param reason      The reason for the packet drop (e.g., NO_ROUTE,
   *                    BUFFER_FULL)
   * @param pktType     The packet type being dropped
   * @param bytes       The size of the dropped packet in bytes
   * @param numPkts     The number of packets dropped
   * @param portNum     The port number where the packet was dropped
   * @param incidentId  The incident this drop is attributed to, if any
   */
  void recordPacketDrop(
      packsim::switches::thrift::PacketDropReason reason,
      PacketType pktType,
      uint64_t bytes,
      uint64_t numPkts,
      uint16_t portNum,
      std::optional<uint32_t> incidentId = std::nullopt);

  bool useIngress_ = true;
  bool useEgress_ = false;

  std::vector<std::tuple<Time, packsim::switches::thrift::Switch>> snapshots_;
};

} // namespace packsim
