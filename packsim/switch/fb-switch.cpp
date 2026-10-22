/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/switch/fb-switch.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/Conv.h>
#include <folly/container/F14Map.h>
#include <folly/logging/xlog.h>
#include <folly/memory/not_null.h>
#include <magic_enum/magic_enum.hpp>

// @oss-disable
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/link-layer/cbfc/cbfc.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ping.h"
#include "packsim/switch/retransmitting-transport.h"

namespace packsim {

namespace {

constexpr uint32_t kNoIncidentId = 0;

const folly::F14FastMap<uint8_t, uint8_t> kDscpToCosMapDefault =
    {{48, 6}, {56, 7}, {40, 5}, {32, 4}, {24, 3}, {16, 2}, {8, 1}, {0, 0}};

// A queue filter returns a `uint8_t`, so every queue it selects is below this.
constexpr uint32_t kQueueIndexLimit = 256;

// Highest queue below `queueLimit` that a port with `numQueues` queues and
// filter `filterCfg` selects for a packet. A single-queue port selects queue 0
// for every packet, as a multi-queue port does for a DSCP its fieldToQueue
// does not list.
uint8_t getHighestQueue(
    const packsim::thrift::QueueFilterConfig& filterCfg,
    const uint8_t numQueues,
    const uint32_t queueLimit) {
  uint8_t highestQueue = 0;
  if (numQueues <= 1) {
    return highestQueue;
  }
  for (const auto& [dscp, queue] : *filterCfg.fieldToQueue()) {
    if (queue < queueLimit) {
      highestQueue = std::max(highestQueue, queue);
    }
  }
  return highestQueue;
}

std::string formatBufState(
    const char* label,
    SimpleSwitchBuffer& buf,
    const uint16_t port,
    const uint8_t queue) {
  const auto& bs = buf.bufferStats();
  const auto& ps = buf.portStats(port);
  const auto& qs = buf.queueStats(port, queue);
  return fmt::format(
      "{}: buf shared={}/{} hdrm={}/{} | "
      "port {} used={} rsvd={}/{} shared={}/{} hdrm={}/{} | "
      "queue {}.{} used={} rsvd={}/{} shared={}/{} hdrm={}/{} "
      "drops={} watermark={}",
      label,
      bs.usedSharedCells,
      bs.usedSharedCells + bs.availSharedCells,
      bs.usedHdrmCells,
      bs.usedHdrmCells + bs.availHdrmCells,
      port,
      ps.usedCells,
      ps.usedPortRsvdCells,
      ps.usedPortRsvdCells + ps.availPortRsvdCells,
      ps.usedSharedCells,
      ps.usedSharedCells + ps.availSharedCells,
      ps.usedHdrmCells,
      ps.usedHdrmCells + ps.availHdrmCells,
      port,
      queue,
      qs.usedCells,
      qs.usedQueueRsvdCells,
      qs.usedQueueRsvdCells + qs.availQueueRsvdCells,
      qs.usedSharedCells,
      qs.usedSharedCells + qs.availSharedCells,
      qs.usedHdrmCells,
      qs.usedHdrmCells + qs.availHdrmCells,
      qs.droppedPkts,
      qs.watermarkCells);
}

} // namespace

FbSwitch::FbSwitch(
    const FlexNodeParams& params,
    const packsim::thrift::SwitchConfig* cfg)
    : FlexNode(params) {
  cfg_ = cfg;
  printLpmTable();
  const auto pfcCb = [this](uint16_t port, uint8_t pg, PfcState state) {
    setRxPfcState(port, pg, state);
  };

  useIngress_ = *cfg_->useIngress();
  useEgress_ = *cfg_->useEgress();
  if (useIngress_ && cfg_->bufs()->empty()) {
    throw std::invalid_argument(
        fmt::format("Switch {}: useIngress is set but bufs is empty", name_));
  }
  if (useEgress_ && cfg_->egressBufs()->empty()) {
    throw std::invalid_argument(
        fmt::format(
            "Switch {}: useEgress is set but egressBufs is empty", name_));
  }

  // if the switch use both ingress and egress, make sure both buffer
  // configs are present
  if (useIngress_ && useEgress_) {
    XCHECK(
        cfg_->bufs()->size() == cfg_->egressBufs()->size(),
        "Ingress and egress buffers do not match!");
  }

  // A buffer config error names the switch and the buffer's index.
  if (useIngress_) {
    ingressBufs_.reserve(cfg_->bufs()->size());
    for (const auto& bufCfg : *cfg_->bufs()) {
      try {
        auto& buf =
            ingressBufs_.emplace_back(simCtx_, bufCfg, /*useIngress=*/true);
        buf.setOnPfcStateChangedCallback(pfcCb);
      } catch (const std::invalid_argument& e) {
        throw std::invalid_argument(
            fmt::format(
                "Switch {}: SwitchConfig.bufs[{}]: {}",
                name_,
                ingressBufs_.size(),
                e.what()));
      }
    }
  }

  if (useEgress_) {
    egressBufs_.reserve(cfg_->egressBufs()->size());
    for (const auto& bufCfg : *cfg_->egressBufs()) {
      try {
        egressBufs_.emplace_back(simCtx_, bufCfg, /*useIngress=*/false);
      } catch (const std::invalid_argument& e) {
        throw std::invalid_argument(
            fmt::format(
                "Switch {}: SwitchConfig.egressBufs[{}]: {}",
                name_,
                egressBufs_.size(),
                e.what()));
      }
    }
  }

  const auto& portToBufMap = *cfg_->portToBufMap();
  if (useIngress_ && useEgress_) {
    for (const auto& [portPairKey, portPairBuff] : *portToBufMap.pairMap()) {
      // Key is "ingrPort.egrPort" string (e.g., "0.5" for ingress 0, egress 5)
      const auto dotPos = portPairKey.find('.');
      if (dotPos == std::string::npos) {
        throw std::runtime_error(
            fmt::format(
                "Malformed pairMap key '{}' for switch={}: missing '.' separator",
                portPairKey,
                name_));
      }
      try {
        const uint32_t inpPortNum =
            folly::to<uint32_t>(portPairKey.substr(0, dotPos));
        const uint32_t outPortNum =
            folly::to<uint32_t>(portPairKey.substr(dotPos + 1));
        portPairBuff_[{inpPortNum, outPortNum}] = portPairBuff;
      } catch (const std::exception& e) {
        throw std::runtime_error(
            fmt::format(
                "Malformed pairMap key '{}' for switch={}: {}",
                portPairKey,
                name_,
                e.what()));
      }
    }
  } else if (useIngress_ || useEgress_) {
    portBuff_.insert(
        portToBufMap.singleMap()->begin(), portToBufMap.singleMap()->end());
  } else {
    throw std::runtime_error(
        fmt::format(
            "Incorrect Port# to Buffer Mapping config for switch={}", name_));
  }

  // An egress-only switch has no ingress buffers; it takes the cell size from
  // its egress buffers.
  const auto& cellSizeBufs = useIngress_ ? *cfg_->bufs() : *cfg_->egressBufs();
  cellSize_ = *cellSizeBufs.at(0).cellSizeBytes();
  for (const auto& buf : cellSizeBufs) {
    if (folly::to<uint32_t>(*buf.cellSizeBytes()) != cellSize_) {
      throw std::runtime_error("Buffers must all have the same cell size");
    }
  }

  pipelineDelayNs_ = *cfg_->pipelineDelayNs();
  expDelayNs_ = *cfg_->expDelayNs();

  // @oss-disable
      // @oss-disable
}

std::vector<uint16_t> FbSwitch::getIngressPortNums() const {
  std::vector<uint16_t> ports;
  for (const auto& buf : ingressBufs_) {
    const auto bufPorts = buf.getPortNums();
    ports.insert(ports.end(), bufPorts.begin(), bufPorts.end());
  }
  return ports;
}

uint16_t FbSwitch::getPortNumFromLinkId(
    const packsim::thrift::LinkId linkId) const {
  const auto it = linkToPort_.find(linkId);
  if (it == linkToPort_.end()) {
    throw std::runtime_error("Link not found");
  }
  return it->second;
}

void FbSwitch::setRxPfcState(
    const uint16_t port,
    const uint8_t pg,
    const PfcState state) {
  portToEthPort_.at(port)->setRxPfcState(pg, state);
}

void FbSwitch::onPacketRcvd(EthernetPort& ethPort, const PacketPtr& pkt) {
  onPacketRcvdByMove(ethPort, PacketPtr(pkt));
}

void FbSwitch::onPacketRcvdByMove(EthernetPort& ethPort, PacketPtr&& pkt) {
  // R_VC_CC must be counted synchronously at reception, not after
  // `pipelineDelayNs_` (UEC §5.2.5.7.1). Delaying it lets an in-band CC_Update
  // reach the §5.2.5.7.4 reconcile with this packet still uncounted, so
  // `LostCredits` comes out positive and the credits are returned twice. On an
  // LLR link only in-order frames reach here, so the frames UEC §5.2.9 rule 2
  // keeps out of R_VC_CC never arrive.
  if (auto* cbfc = ethPort.cbfc()) {
    cbfc->onPacketStored(*pkt);
  }
  simCtx_->schedule(
      Time::fromNs(
          pipelineDelayNs_ + simCtx_->random.sampleExponential(expDelayNs_)),
      &FbSwitch::routeSwitchPacket,
      this,
      &ethPort,
      std::move(pkt));
}

bool FbSwitch::checkIngressEgressAdmission(
    folly::not_null<EthernetPort*> inEthPort,
    folly::not_null<EthernetPort*> outEthPort,
    const PacketPtr& pkt) {
  const uint16_t inPort = inEthPort->getPortNum();
  // pkt->getSize() already includes the Ethernet framing — no manual
  // +kEthernetHeaderSize here (would double-count and trigger premature
  // BUFFER_FULL drops).
  const uint32_t pktBytes = pkt->getSize();
  const uint16_t outPort = outEthPort->getPortNum();
  const uint8_t outQueue = outEthPort->getPktCos(pkt);
  const uint8_t inpCos = inEthPort->getPktCos(pkt);
  const uint8_t inpPg = inpCos; // assume 1:1 mapping for simplicity
  const auto bufIdx = getBuff(inPort, outPort);
  bool ingressAdmit = true;
  bool egressAdmit = true;
  if (useIngress_) {
    auto& ingressBuf = ingressBufs_.at(bufIdx);
    // This model credits only headroom (`getLosslessRxCreditBytes`), so on a
    // CBFC-managed queue only lossless traffic may reach it. See
    // `AdmitOptions` for what `heldCreditsOut` records.
    const auto trafficClass = (inEthPort->cbfc() != nullptr &&
                               inEthPort->cbfc()->isLosslessPacket(*pkt))
        ? BufferTrafficClass::LOSSLESS
        : BufferTrafficClass::LOSSY;
    ingressAdmit = ingressBuf.admitPacket(
        inPort,
        inpCos,
        pktBytes,
        {.trafficClass = trafficClass, .heldCreditsOut = &pkt->heldCredits});
  }
  if (useEgress_ && ingressAdmit) {
    auto& egressBuf = egressBufs_.at(bufIdx);
    egressAdmit = egressBuf.admitPacket(outPort, outQueue, pktBytes);
  }
  if (useIngress_ && ingressAdmit && !egressAdmit) {
    auto& ingressBuf = ingressBufs_.at(bufIdx);
    // Ingress port admitted the packet, but egress port did not.
    // Free up the memory in the ingress port buffer.
    // The released count is ignored here: this packet is about to be
    // BUFFER_FULL-dropped, and the `onPacketDrained` call below frees its whole
    // credit cost rather than the headroom share.
    std::ignore =
        ingressBuf.removePacket(inPort, inpCos, pktBytes, pkt->heldCredits);
  }
  const bool admit = ingressAdmit && egressAdmit;

  if (!admit) {
    recordPacketDrop(
        packsim::switches::thrift::PacketDropReason::BUFFER_FULL,
        pkt->pktType,
        pkt->getSize(),
        /* numPkts */ 1,
        inPort);
    // A BUFFER_FULL drop on a lossless VC is a flow-control correctness
    // violation: credits should have throttled the sender before the buffer
    // overran (UEC §5.2.5.6.1). Recorded on the receiving port's CBFC module so
    // the violation is observable in telemetry and tests.
    if (auto* cbfc = inEthPort->cbfc()) {
      if (cbfc->isLosslessPacket(*pkt)) {
        cbfc->onLosslessVcBufferFullDrop();
      }
      // A dropped packet never drains, so its reception-time credit has to be
      // freed here or it leaks for the rest of the run (UEC §5.2.5.7.1).
      cbfc->onPacketDrained(*pkt);
    }
    if (*cfg_->enableFatalOnDrop()) {
      const auto detail = (useIngress_ && !ingressAdmit)
          ? formatBufState("ingress", ingressBufs_.at(bufIdx), inPort, inpCos)
          : formatBufState("egress", egressBufs_.at(bufIdx), outPort, outQueue);
      throw std::runtime_error(
          fmt::format(
              "{} Dropping packet: pg={}, pktBytes={}, "
              "inPort={}, outPort={}, t={}ns\n  {}",
              name_,
              inpPg,
              pktBytes,
              inPort,
              outPort,
              simCtx_->nowNs(),
              detail));
    } else {
      XLOGF(
          DBG, "{} Dropping packet from port {}, pg={}", name_, inPort, inpPg);
      return false;
    }
  }
  // Model choice: only headroom is credited, so credit for the cells a packet
  // occupies outside headroom returns at enqueue and the headroom share is held
  // until dequeue. `pkt->heldCredits` is that held share; see
  // `Cbfc::onCreditsHeld`.
  if (auto* cbfc = inEthPort->cbfc(); cbfc != nullptr) {
    cbfc->onCreditsHeld(*pkt, pkt->heldCredits);
  }

  XLOGF(
      DBG,
      "At time={}, at switch={} Incoming packet from port {}, pg={}, size={}",
      simCtx_->nowNs(),
      name_,
      inPort,
      inpPg,
      pktBytes);
  return true;
}

void FbSwitch::onPacketSent(EthernetPort& ethPort, const PacketPtr& pkt) {
  const auto outPort = ethPort.getPortNum();
  const uint16_t inPort = pkt->tag.port;
  const uint8_t inpPg = pkt->tag.pg;
  const uint32_t pktBytes = pkt->getSize();
  const auto bufIdx = getBuff(inPort, outPort);
  // `removePacket` clamps the release against the headroom it still holds, so
  // the credit return below must use what came back, not `pkt->heldCredits`.
  uint16_t releasedHdrmCells = pkt->heldCredits;
  if (useIngress_) {
    releasedHdrmCells = ingressBufs_.at(bufIdx).removePacket(
        inPort, inpPg, pktBytes, pkt->heldCredits);
  }
  if (useEgress_) {
    // Admission charged the queue this port classifies the packet to, which
    // differs from the ingress PG when the two ports' DSCP maps differ.
    const uint8_t outQueue = ethPort.getPktCos(pkt);
    // Egress is never CBFC-managed, so it holds no credit to return.
    std::ignore =
        egressBufs_.at(bufIdx).removePacket(outPort, outQueue, pktBytes);
  }
  // The packet has drained, so free the credit on its ingress port, `inPort`;
  // that port returns the credit upstream via a CF_Update (UEC §5.2.5.7.2).
  // A port missing from `portToEthPort_` never had an `EthernetPort`, so it
  // never had CBFC and holds no credit; tests drive switches with unregistered
  // ingress ports.
  if (portToEthPort_.contains(inPort)) {
    if (auto* cbfc = portToEthPort_.at(inPort)->cbfc()) {
      cbfc->onCreditsFreed(*pkt, releasedHdrmCells);
    }
  }
  // `heldCredits` is per-hop and this switch's share is now settled. A
  // downstream switch that models no ingress buffer never overwrites the field,
  // so a stale count would make it release credits it never took.
  pkt->heldCredits = 0;
  XLOGF(
      DBG,
      "At time={}, at switch={} Outgoing packet from port {}, pg={}, size={}",
      simCtx_->nowNs(),
      name_,
      outPort,
      inpPg,
      pktBytes);
}

void FbSwitch::onPacketDroppedOnSend(
    EthernetPort& ethPort,
    const PacketPtr& pkt,
    const uint8_t queue,
    const packsim::switches::thrift::PacketDropReason reason) {
  const auto outPort = ethPort.getPortNum();
  const uint16_t inPort = pkt->tag.port;
  const uint8_t inpPg = pkt->tag.pg;
  const uint32_t pktBytes = pkt->getSize();
  const auto outQueue = queue;

  const auto bufIdx = getBuff(inPort, outPort);
  // `wredDrop` clamps the release, so free what came back, not
  // `pkt->heldCredits`.
  uint16_t releasedHdrmCells = pkt->heldCredits;
  if (useIngress_) {
    releasedHdrmCells = ingressBufs_.at(bufIdx).wredDrop(
        inPort, inpPg, pktBytes, pkt->heldCredits);
  }
  if (useEgress_) {
    // Egress is never CBFC-managed, so it holds no credit to return.
    std::ignore = egressBufs_.at(bufIdx).wredDrop(outPort, outQueue, pktBytes);
  }
  // This packet was admitted but never drains, so its outstanding headroom
  // credit has to be freed here: the UEC §5.2.5.7.4 reconcile cannot recover it
  // because R_VC_CC already counted the packet, so it finds no lost credits.
  if (portToEthPort_.contains(inPort)) {
    if (auto* cbfc = portToEthPort_.at(inPort)->cbfc()) {
      cbfc->onCreditsFreed(*pkt, releasedHdrmCells);
    }
  }

  // Record packet drop event for statistics tracking (send path drops)
  const auto incidentId = lookupIncidentId(outPort);
  recordPacketDrop(
      reason,
      pkt->pktType,
      pktBytes,
      /* numPkts= */ 1,
      outPort,
      incidentId);

  XLOGF(
      DBG,
      "At time={}, at switch={} dropped outgoing packet from port {}, queue={}, size={}",
      simCtx_->nowNs(),
      name_,
      outPort,
      outQueue,
      pktBytes);

  simCtx_->getScheduler()->onPacketDropped(
      DropEvent{
          name_,
          flexId_,
          outPort,
          reason,
          &pkt,
          ethPort.getTotalQueueLength(),
          simCtx_->nowFs()});
}

void FbSwitch::onPacketDroppedOnReceive(
    EthernetPort& ethPort,
    const PacketPtr& pkt,
    uint8_t /* queue */,
    const packsim::switches::thrift::PacketDropReason reason) {
  const auto port = ethPort.getPortNum();
  const uint32_t pktBytes = pkt->getSize();

  // Record packet drop event for statistics tracking (receive path drops)
  // Note: Do NOT call wredDrop on buffers because packet was dropped before
  // entering the switch
  const auto incidentId = lookupIncidentId(port);
  recordPacketDrop(
      reason,
      pkt->pktType,
      pktBytes,
      /* numPkts= */ 1,
      port,
      incidentId);

  XLOGF(
      DBG,
      "At time={}, at switch={} dropped incoming packet at port {}, size={}",
      simCtx_->nowNs(),
      name_,
      port,
      pktBytes);

  simCtx_->getScheduler()->onPacketDropped(
      DropEvent{
          name_,
          flexId_,
          port,
          reason,
          &pkt,
          ethPort.getTotalQueueLength(),
          simCtx_->nowFs()});
}

void FbSwitch::packetMarked(
    EthernetPort* ethPort,
    const PacketPtr& pkt,
    const uint8_t outQueue,
    packsim::switches::thrift::PacketDropReason /* reason */) {
  const uint16_t inPort = pkt->tag.port;
  const uint16_t outPort = ethPort->getPortNum();
  const uint8_t bufIdx = getBuff(inPort, outPort);

  if (useIngress_) {
    ingressBufs_.at(bufIdx).ecnMark(inPort, pkt->tag.pg);
  }
  if (useEgress_) {
    egressBufs_.at(bufIdx).ecnMark(outPort, outQueue);
  }
  XLOGF(
      DBG,
      "At time={}, at switch={} marked outgoing packet from port {}, queue={}, size={}",
      simCtx_->nowNs(),
      name_,
      outPort,
      outQueue,
      pkt->getSize());
}

packsim::switches::thrift::Switch FbSwitch::getSummary() {
  packsim::switches::thrift::Switch sum;
  sum.name() = name_;
  sum.flexId() = flexId_.asVec();

  for (size_t i = 0; i < ethPorts_.size(); ++i) {
    const auto ethPort = ethPorts_.at(i).get();
    const uint16_t localPort = ethPort->getPortNum();
    const uint16_t peerPortNum = ethPort->getPeerPortNum();
    // The summary maps store port numbers as `i32`, and a port that was never
    // attached is stored there as -1.
    const int32_t portNum =
        localPort == EthernetPort::kUnattachedPort ? -1 : localPort;
    const int32_t partnerPortNum =
        peerPortNum == EthernetPort::kUnattachedPort ? -1 : peerPortNum;
    for (const auto& ps : ethPort->getAllPortStats()) {
      sum.intfStats()[portNum].emplace_back(ps.toThrift());
    }
    sum.portPartners()[portNum] = ethPort->getPeerFlexDeviceId().asVec();
    sum.portPartnerPortNums()[portNum] = partnerPortNum;
  }

  // bufferIdx restarts at 0 per direction: it is the buffer's position within
  // `ingressBuffers`/`egressBuffers`, which is how consumers of the result
  // derive it. Stamped here rather than stored on the tracker, which has no
  // notion of its own position: a stored copy would be a second source of
  // truth that could drift from list order.
  uint32_t ingressBufferIdx = 0;
  for (const auto& buf : ingressBufs_) {
    auto& serBuf = sum.ingressBuffers()->emplace_back(buf.serializeState());
    serBuf.bufferIdx() = ingressBufferIdx++;
  }
  uint32_t egressBufferIdx = 0;
  for (const auto& buf : egressBufs_) {
    auto& serBuf = sum.egressBuffers()->emplace_back(buf.serializeState());
    serBuf.bufferIdx() = egressBufferIdx++;
  }

  // @oss-disable

  sum.packetDropEvents() = packetDropStats_;

  return sum;
}

void FbSwitch::adoptPort(std::unique_ptr<EthernetPort>& ethPort) {
  ethPort->setOnPacketMarkedCallback(
      [this](
          EthernetPort* markedPort,
          const PacketPtr& pkt,
          uint8_t queue,
          packsim::switches::thrift::PacketDropReason reason) {
        packetMarked(markedPort, pkt, queue, reason);
      });

  ethPort->setObserver(this);
  FlexNode::adoptPort(ethPort);
}

void FbSwitch::addSwitchPort(
    folly::not_null<EthernetPort*> ethPort,
    std::string portGroup,
    FlexDeviceId partnerId,
    const packsim::thrift::LinkId linkId) {
  const auto& groupPorts = cfg_->portGroups()->at(portGroup);
  const size_t counter = portGroupCounters_[portGroup]++;
  if (counter >= groupPorts.size()) {
    throw std::runtime_error(
        fmt::format(
            "Not enough ports (only {}) provided in switch config for port group '{}' to support the links the flex topology requests.",
            groupPorts.size(),
            portGroup));
  }
  const uint32_t configPort = groupPorts.at(counter);
  if (configPort >= EthernetPort::kUnattachedPort) {
    throw std::invalid_argument(
        fmt::format(
            "Port {} in port group '{}' of switch {} is outside [0, {})",
            configPort,
            portGroup,
            name_,
            EthernetPort::kUnattachedPort));
  }
  const auto portNum = static_cast<uint16_t>(configPort);
  if (portsAssigned_.contains(portNum)) {
    throw std::runtime_error(
        fmt::format("Tried to add port {} to {} twice", portNum, name_));
  }
  portsAssigned_.emplace(portNum);
  FlexNode::addPort(ethPort, partnerId, linkId, portNum);

  ethPort->setPortNum(portNum);
  ethPort->enableVoqEcnMarking();

  const auto& portCfg = getPortBufferConfig(portNum);
  auto queueFilterCfg = *portCfg.queueFilterCfg();
  if (queueFilterCfg.fieldToQueue()->empty()) {
    queueFilterCfg.fieldToQueue() = kDscpToCosMapDefault;
  }
  ethPort->setQueueFilterConfig(queueFilterCfg);
  ethPort->setPortQueueConfig(*portCfg.queueCfg());
  ethPort->setQueueSchedulerConfig(*portCfg.queueScheduleCfg());
  if (useIngress_) {
    requireBufferQueues(
        portNum, queueFilterCfg, ethPort->getNumQueues(), /*useIngress=*/true);
  }
  if (useEgress_) {
    requireBufferQueues(
        portNum, queueFilterCfg, ethPort->getNumQueues(), /*useIngress=*/false);
  }

  // Ordering requirement: CBFC must already be enabled on `ethPort` —
  // FlexTopology does so in connectNodes, before this call. Enabling it
  // afterwards leaves the port's queue trackers unmanaged, so headroom stays
  // open to lossy traffic and credit releases are never clamped to headroom.
  if (useIngress_ && ethPort->isCbfcEnabled()) {
    markIngressCbfcManaged(portNum);
  }
}

void FbSwitch::markIngressCbfcManaged(const uint16_t portNum) {
  for (auto& buf : ingressBufs_) {
    if (buf.hasPort(portNum)) {
      buf.setPortCbfcManaged(portNum);
    }
  }
}

uint64_t FbSwitch::getLosslessRxCreditBytes(const uint16_t portNum) const {
  if (!useIngress_) {
    return 0;
  }
  // Read from the config, not the live trackers, so this is independent of
  // buffer-init order. Only headroom is credited in this model; reserved and
  // shared space is uncredited and returns its credits at enqueue.
  uint64_t cells = 0;
  const auto& bufs =
      *folly::not_null<const packsim::thrift::SwitchConfig*>(cfg_)->bufs();
  for (const auto& bufCfg : bufs) {
    const auto portIt = bufCfg.ports()->find(portNum);
    if (portIt == bufCfg.ports()->end() || !*portIt->second.isIngrPort()) {
      continue;
    }
    uint64_t bufCells = 0;
    for (const auto& pg : *portIt->second.pgs()) {
      if (*pg.lossless()) {
        bufCells += *pg.maxHdrmCells();
      }
    }
    // `headroomPoolCells` is one ceiling shared by every PG in the buffer, so
    // it binds the sum, not each term. Clamping per PG would advertise N times
    // the headroom that exists and let the sender credit into a lossless
    // BUFFER_FULL drop.
    cells += std::min<uint64_t>(bufCells, *bufCfg.headroomPoolCells());
  }
  return cells * cellSize_;
}

void FbSwitch::sendFromPort(EthernetPort* outEthPort, PacketPtr pkt) {
  outEthPort->send(std::move(pkt));
}

uint64_t FbSwitch::getCellsInQueue(
    const uint8_t bufferIdx,
    const uint16_t port,
    const uint8_t pg) const {
  const auto& buf = egressBufs_.at(bufferIdx);
  if (!buf.hasPort(port)) {
    return 0;
  }
  return buf.queueStats(port, pg).usedCells;
}

uint64_t FbSwitch::getPortTxBytes(const uint16_t port, const uint8_t pg) const {
  const auto it = portToEthPort_.find(port);
  PortStats ps{};
  if (it != portToEthPort_.end()) {
    ps = it->second->getPortStats(pg);
  }
  return ps.txBytes;
}

const FlexDeviceId& FbSwitch::getFlexDeviceId() const noexcept {
  return flexId_;
}

void FbSwitch::setIncidentLookup(const IIncidentLookup* lookup) {
  incidentLookup_ = lookup;
}

std::optional<uint32_t> FbSwitch::lookupIncidentId(
    const uint16_t portNum) const {
  if (!incidentLookup_) {
    return std::nullopt;
  }
  const auto it = portToLink_.find(portNum);
  if (it == portToLink_.end()) {
    return std::nullopt;
  }
  // @oss-disable
  return std::nullopt; // @oss-enable
}

void FbSwitch::recordPacketDrop(
    const packsim::switches::thrift::PacketDropReason reason,
    const PacketType pktType,
    const uint64_t bytes,
    const uint64_t numPkts,
    const uint16_t portNum,
    std::optional<uint32_t> incidentId) {
  packsim::switches::thrift::PacketDropEvent drop;
  drop.dropReason() = reason;
  drop.packetType() = std::string(magic_enum::enum_name(pktType));
  drop.droppedPkts() = numPkts;
  drop.droppedBytes() = bytes;
  drop.eventTimeNs() = simCtx_->now().getNs();
  drop.portNum() = portNum;
  drop.incidentId() = incidentId.value_or(kNoIncidentId);
  packetDropStats_.emplace_back(std::move(drop));
}

void FbSwitch::routeSwitchPacket(EthernetPort* inEthPort, PacketPtr pkt) {
  auto ipPtr = pkt.as<Ipv6Packet>();
  ipPtr->hopLimit--;

  // Add tag for input port and PG
  pkt->tag.port = inEthPort->getPortNum();
  pkt->tag.pg = inEthPort->getPktCos(pkt);

  // Record this hop when the ping asks for a trace.
  if (pkt->pktType == PacketType::PING) {
    auto* ping = pkt.as<PingPacket>();
    if (ping->traceEnabled) {
      ping->hops.emplace_back(
          PingHopInfo{
              .deviceName = name_,
              .deviceId = flexId_,
              .inPort = pkt->tag.port,
              .queueDepthBytes = inEthPort->getTotalQueueLength(),
              .simTimeFs = simCtx_->nowFs()});
    }
  }

  const auto srcLinkId = portToLink_.at(pkt->tag.port);

  EthernetPort* outEthPort = nullptr;
  try {
    outEthPort = FlexNode::routePacket(pkt, srcLinkId);
  } catch (const packsim::thrift::PackSimRoutingException& e) {
    // A no-route packet on a retransmitting transport is a recoverable drop;
    // other packet types have no recovery and stay fatal.
    if (isRetransmittingTransport(*pkt)) {
      // Drop packet and log warning
      XLOGF(
          DBG,
          "{}. Dropping {} packet at time={}, from port={}, pg={}, ",
          *e.message(),
          magic_enum::enum_name(pkt->pktType),
          simCtx_->nowNs(),
          pkt->tag.port,
          pkt->tag.pg);
      recordPacketDrop(
          /* reason= */ packsim::switches::thrift::PacketDropReason::NO_ROUTE,
          /* pktType= */ pkt->pktType,
          /* bytes= */ pkt->getSize(),
          /* numPkts= */ 1,
          /* portNum= */ pkt->tag.port);
      // The packet never reached ingress admission, so the whole credit cost
      // `onPacketStored` counted at reception is still outstanding and has to
      // be freed here or it leaks for the rest of the run (UEC §5.2.5.7.1).
      if (auto* cbfc = inEthPort->cbfc()) {
        cbfc->onPacketDrained(*pkt);
      }
      return;
    }
    XLOGF(
        ERR,
        "{}. Dropping {} packet at time={}, from port={}, pg={}, ",
        *e.message(),
        magic_enum::enum_name(pkt->pktType),
        simCtx_->nowNs(),
        pkt->tag.port,
        pkt->tag.pg);
    throw;
  }

  if (checkIngressEgressAdmission(inEthPort, outEthPort, pkt)) {
    sendFromPort(outEthPort, std::move(pkt));
  }
}

uint8_t FbSwitch::getBuff(const uint16_t inpPort, const uint16_t outPort)
    const {
  if (useIngress_ && useEgress_) {
    return portPairBuff_.at({inpPort, outPort});
  }
  return portBuff_.at(useIngress_ ? inpPort : outPort);
}

std::optional<uint8_t> FbSwitch::findPortBufferIndex(
    const uint16_t portNum) const {
  const auto& singleMap = *cfg_->portToBufMap()->singleMap();
  if (const auto it = singleMap.find(portNum); it != singleMap.end()) {
    return it->second;
  }
  std::optional<uint8_t> bufIdx;
  if (useIngress_ && useEgress_) {
    for (const auto& [ports, pairBufIdx] : portPairBuff_) {
      if (ports.first == portNum &&
          (!bufIdx.has_value() || pairBufIdx < *bufIdx)) {
        bufIdx = pairBufIdx;
      }
    }
  }
  return bufIdx;
}

const packsim::thrift::PortBufferConfig& FbSwitch::getPortBufferConfig(
    const uint16_t portNum) const {
  const auto& bufCfgs = useIngress_ ? *cfg_->bufs() : *cfg_->egressBufs();
  const char* const bufsName = useIngress_ ? "bufs" : "egressBufs";
  const auto bufIdx = findPortBufferIndex(portNum);
  if (!bufIdx.has_value()) {
    throw std::invalid_argument(
        fmt::format(
            "Switch {}: portToBufMap maps port {} to no buffer",
            name_,
            portNum));
  }
  if (*bufIdx >= bufCfgs.size()) {
    throw std::invalid_argument(
        fmt::format(
            "Switch {}: portToBufMap maps port {} to buffer {}, but {} has {} "
            "entries",
            name_,
            portNum,
            *bufIdx,
            bufsName,
            bufCfgs.size()));
  }
  const auto& ports = *bufCfgs.at(*bufIdx).ports();
  const auto portIt = ports.find(portNum);
  if (portIt == ports.end()) {
    throw std::invalid_argument(
        fmt::format(
            "Switch {}: {}[{}] has no entry for port {}",
            name_,
            bufsName,
            *bufIdx,
            portNum));
  }
  return portIt->second;
}

void FbSwitch::requireBufferQueues(
    const uint16_t portNum,
    const packsim::thrift::QueueFilterConfig& filterCfg,
    const uint8_t numQueues,
    const bool useIngress) const {
  // A received packet whose queue the port lacks fails the port's own
  // counters (`EthernetPort::getStatsFor`) before ingress admission, but
  // egress admission charges the queue before the port looks it up.
  const uint8_t highestQueue = getHighestQueue(
      filterCfg, numQueues, useIngress ? numQueues : kQueueIndexLimit);
  const auto& switchCfg =
      *folly::not_null<const packsim::thrift::SwitchConfig*>(cfg_);
  const auto& bufCfgs =
      useIngress ? *switchCfg.bufs() : *switchCfg.egressBufs();
  for (size_t bufIdx = 0; bufIdx < bufCfgs.size(); ++bufIdx) {
    const auto& ports = *bufCfgs.at(bufIdx).ports();
    const auto portIt = ports.find(portNum);
    // An entry configures queues only in the directions it serves; a builder
    // may also list a port, without pgs, in an ingress buffer that does not
    // take its ingress traffic.
    if (portIt == ports.end() ||
        !(useIngress ? *portIt->second.isIngrPort()
                     : *portIt->second.isEgrPort())) {
      continue;
    }
    const size_t numConfiguredQueues = useIngress
        ? portIt->second.pgs()->size()
        : portIt->second.ucQueues()->size();
    if (numConfiguredQueues <= highestQueue) {
      // More ucQueues alone would not do when the port lacks the queue too:
      // its own queue lookup would then fail at the first such packet.
      const std::string portQueuesNote =
          !useIngress && highestQueue >= numQueues
          ? fmt::format(
                "; the port itself has {} queues, so either map no DSCP past "
                "queue {} or give the port more queueCfg entries",
                numQueues,
                numQueues - 1)
          : std::string{};
      throw std::invalid_argument(
          fmt::format(
              "Switch {}: port {}'s queueFilterCfg can select queue {} for {} "
              "traffic, so SwitchConfig.{}[{}] needs at least {} {} for the "
              "port but has {} (an empty fieldToQueue maps DSCP 8n to queue n "
              "on a switch port){}",
              name_,
              portNum,
              highestQueue,
              useIngress ? "ingress" : "egress",
              useIngress ? "bufs" : "egressBufs",
              bufIdx,
              highestQueue + 1,
              useIngress ? "pgs" : "ucQueues",
              numConfiguredQueues,
              portQueuesNote));
    }
  }
}

} // namespace packsim
