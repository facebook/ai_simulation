/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/endpoint/endpoint.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/Conv.h>
#include <folly/ScopeGuard.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "packsim/link-layer/cbfc/cbfc.h"
#include "packsim/link-layer/cbfc/nic-rx-credit-bridge.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/node/port-reachability.h"
#include "packsim/transport/transport-factory.h"

namespace packsim {
Endpoint::Endpoint(
    const FlexNodeParams& params,
    const packsim::thrift::NicConfig* nicConfig,
    std::optional<uint8_t> portIndex)
    : FlexNode(params),
      nicConfig_(nicConfig),
      portIndex_(portIndex),
      portReachability_(params.portReachability) {
  transport_ = packsim::makeTransport(
      TransportParams{
          .simCtx = simCtx_,
          .flexId = flexId_,
          .nicConfig = *nicConfig_,
          .endpointName = getName()});

  transport_->setOnMessageReceivedCallback(
      [this](Message* msg) { onMessageReceived(msg); });
  transport_->setOnMessageAckedCallback(
      [this](Message* msg) { onMessageAcked(msg); });
  transport_->setOnWorkloadPortConfigsCallback(
      [this](const packsim::workload::thrift::Workload* workload) {
        onWorkloadPortConfigs(workload);
      });
  transport_->name_ = name_;
}

void Endpoint::initQpnPortConfigFromWorkload(Message* msg) {
  if (msg->workload && !msg->workload->qpnPortConfigs()->empty()) {
    // Initialize ALL port configs from the workload, not just the one for
    // this message's QPN. This ensures port configs are available before any
    // packet routing happens, avoiding race conditions where packets use
    // QPNs that haven't been explicitly sent yet (e.g., ACKs).
    onWorkloadPortConfigs(msg->workload);
  }
}

void Endpoint::makeSendRequest(Message* msg) {
  XLOGF(
      DBG,
      "Endpoint::makeSendRequest: {} ports={} msg_size={}",
      getName(),
      ports_.size(),
      msg->sizeBytes);

  XLOGF(
      DBG7,
      "Endpoint::makeSendRequest: Accel {} ({}), Send {} bytes to {} (msg: '{}')",
      getName(),
      msg->srcIdx.toString(),
      msg->sizeBytes,
      msg->dstIdx.toString(),
      msg->toString());

  initQpnPortConfigFromWorkload(msg);

  transport_
      ->findOrMakeSocket(
          msg->dstIdx, msg->localQPN, msg->remoteQPN, msg->workload)
      ->send(msg);
}

void Endpoint::makeReceiveRequest(Message* msg) {
  XLOGF(
      DBG7,
      "Endpoint::makeReceiveRequest: Accel {} ({}), Recv {} bytes from {} (msg: '{}')",
      getName(),
      msg->dstIdx.toString(),
      msg->sizeBytes,
      msg->srcIdx.toString(),
      msg->toString());

  initQpnPortConfigFromWorkload(msg);

  transport_
      ->findOrMakeSocket(
          msg->srcIdx, msg->localQPN, msg->remoteQPN, msg->workload)
      ->recv(msg);
}

void Endpoint::addPort(
    EthernetPort* ethPort,
    const FlexDeviceId& partnerId,
    const packsim::thrift::LinkId linkId,
    const uint16_t portNum) {
  FlexNode::addPort(ethPort, partnerId, linkId, portNum);
  ports_[ethPort->getFlexDeviceId()] = ethPort;
  ethPort->setPortIndex(portIndex_);

  // With a port index every port is registered with the transport, whatever
  // the transport, so RoceTransportBase does know it owns several ports;
  // without one only the first port is. RoCEv2 simply pins each QP pair to
  // one registered port -- see the multi-port section of transport/DESIGN.md.
  // What addAvailableNicPort and setPortTopology add on top of that is
  // reachability- and liveness-aware per-packet plane selection, for a
  // transport that overrides them; both default to no-ops on Transport.
  if (portIndex_.has_value()) {
    const uint8_t portIdx = portIndex_.value();
    const uint16_t egressPort = getEgressNicPort(*ethPort);
    transport_->addAvailableNicPort(egressPort);
    if (egressPort == 0) {
      // Registered once: both callbacks are transport-wide and resolve any
      // egress port number.
      transport_->setPortTopology(
          Transport::PortTopology{
              .reachableDstPorts =
                  [this, portIdx](
                      uint16_t srcPort, // 0-based
                      const FlexDeviceId& dst) -> std::set<uint16_t> {
                // FlexDeviceId port values are 1-based.
                FlexDeviceId srcPortId = getFlexId();
                uint16_t& portField = srcPortId.values[portIdx];
                portField = srcPort;
                ++portField;
                return computeReachableDstPorts(srcPortId, dst);
              },
              .isEgressPortLive = [this](uint16_t port) -> bool {
                return isEgressPortLive(port); // 0-based
              }});
    }
    transport_->addPort(
        egressPort, ethPort, [this, egressPort](const PacketPtr& pkt) {
          auto* egressEthPort = routePacketByEgressNicPort(pkt, egressPort);
          egressEthPort->send(pkt);
          return true;
        });
  } else if (ports_.size() == 1) {
    // Single-port topology (e.g., CLOS with numPlanes=1).
    // Wire the one port to plane 0 using generic ECMP routing.
    transport_->addPort(0, ethPort, [this](const PacketPtr& pkt) {
      return routeAndSendPacket(pkt);
    });
  }

  auto& queueSchedulerCfg = *nicConfig_->queueScheduleCfg();
  auto& queueFilterCfg = *nicConfig_->queueFilterCfg();
  auto& portQueueCfg = *nicConfig_->queueCfg();
  const uint32_t txLatencyNs = *nicConfig_->txLatencyNs();
  const uint32_t rxLatencyNs = *nicConfig_->rxLatencyNs();

  ethPort->setQueueSchedulerConfig(queueSchedulerCfg);
  ethPort->setQueueFilterConfig(queueFilterCfg);
  ethPort->setPortQueueConfig(portQueueCfg);
  ethPort->setTxLatencyNs(txLatencyNs);
  ethPort->setRxLatencyNs(rxLatencyNs);
  ethPort->setObserver(this);

  ethPort->setOnLinkUpCallback(
      [this](EthernetPort* upPort) { onLinkUp(upPort); });

  transport_->setSendPacketFn(
      [this](const PacketPtr& pkt) { return routeAndSendPacket(pkt); });

  ethPort->setOnTxQueueSpaceAvailableCallback(
      [this](uint8_t cos) { transport_->sendPendingData(cos); });
}

std::set<uint16_t> Endpoint::computeReachableDstPorts(
    const FlexDeviceId& srcPort,
    const FlexDeviceId& dst) const {
  if (!portIndex_.has_value()) {
    throw std::runtime_error(
        fmt::format(
            "Port index must be set to determine reachability for ports {} -> {}",
            srcPort.toString(),
            dst.toString()));
  }
  const uint8_t portIndex = portIndex_.value();
  if (portReachability_ == nullptr) {
    throw std::runtime_error(
        "computeReachableDstPorts requires the topology's reachability tracer");
  }

  // srcPort is one of this endpoint's egress ports; its first-hop switch is the
  // node on the remote end of that port. Trace the fabric's forwarding
  // from there toward the destination accel.
  const auto it = ports_.find(srcPort);
  if (it == ports_.end()) {
    return {};
  }
  auto* const peer = it->second->getPeer();
  auto* const switchNode = peer->getNode();
  // Invariant: production topology wiring routes every port through
  // FlexTopology::connectNodes -> FlexNode/FbSwitch::adoptPort, which calls
  // setNode, so the switch-side port's node is always set outside unit tests.
  XDCHECK(switchNode != nullptr)
      << "the first-hop switch port's node is set by topology wiring "
         "(FlexTopology::connectNodes -> adoptPort -> setNode)";
  const auto& firstHopSwitch = switchNode->getName();
  // Reachability is to the destination accel, so clear any port field in dst:
  // a port-qualified destination must yield the same reachable set as the bare
  // accel (this is the single place every caller's dst is normalized).
  FlexDeviceId dstAccel = dst;
  dstAccel.values.at(portIndex) = 0;
  return portReachability_->dstPortsFrom(firstHopSwitch, dstAccel);
}

bool Endpoint::canPortsReach(
    const FlexDeviceId& srcPort,
    const FlexDeviceId& dstPort) const {
  const std::set<uint16_t> reached = computeReachableDstPorts(srcPort, dstPort);
  // FlexDeviceId ports are 1-based (0 = unset); reachable set is 0-based.
  // computeReachableDstPorts threw above if portIndex_ is unset, so .value() is
  // safe here.
  uint16_t dstPortNumber = dstPort.values.at(portIndex_.value());
  if (dstPortNumber == 0) {
    return false;
  }
  --dstPortNumber;
  return reached.contains(dstPortNumber);
}

EthernetPort* Endpoint::routePacket(
    const PacketPtr& pkt,
    const std::optional<packsim::thrift::LinkId> srcLink) {
  if (!usingPortLevelAddresses(pkt)) {
    return FlexNode::routePacket(pkt, srcLink);
  } else if (ports_.empty()) {
    throw std::runtime_error(
        fmt::format(
            "Not a switch, and found no ports on the Endpoint ({}).",
            getName()));
  }

  // Transport-controlled port routing bypasses ECMP/QpnPortConfig. Asking the
  // packet rather than probing for each transport's packet type keeps the
  // endpoint out of the business of knowing which types carry a choice, and
  // costs one virtual call instead of a dynamic_cast per candidate type.
  auto* const udpPkt = pkt.as<UdpPacket>();
  if (const auto egressNicPort = udpPkt->chosenEgressNicPort()) {
    return routePacketByEgressNicPort(pkt, *egressNicPort);
  }

  const auto& destinationId = udpPkt->dstId();
  const auto& lpmEntry = getLpmEntry(destinationId);
  const auto& tiebreak = *lpmEntry->tiebreak();
  const uint16_t entropy = udpPkt->srcPort;

  const QPConnection* conn = static_cast<const QPConnection*>(udpPkt->conn);
  udpPkt->socketLevelConn = udpPkt->conn;

  udpPkt->conn = getPortLevelQPConnection(
      destinationId, conn->srcQPN, conn->dstQPN, entropy, tiebreak);

  const auto it = ports_.find(udpPkt->conn->srcId);

  if (it == ports_.end()) {
    std::vector<std::string> portAddresses;
    for (const auto& [portId, ethPort] : ports_) {
      portAddresses.emplace_back(portId.toString());
    }
    const std::string errorMsg = fmt::format(
        "({}, {}): Should have found a port with FlexDeviceID: {}. "
        "Available ports on NIC {}: [{}]",
        getName(),
        getFlexId().toString(),
        udpPkt->conn->srcId.toString(),
        getName(),
        fmt::join(portAddresses, ", "));
    XLOGF(ERR, "{}", errorMsg);
    throw std::runtime_error(errorMsg);
  }

  return it->second;
}

const QPConnection* Endpoint::getPortLevelQPConnection(
    const FlexDeviceId& destination,
    const uint32_t srcQPN,
    const uint32_t dstQPN,
    const uint16_t entropy,
    const packsim::thrift::TiebreakSpec& tiebreak) {
  if (!sourceRoutingTable_.contains(srcQPN)) {
    buildQPConnectionTable(destination, srcQPN, dstQPN);
  }
  const std::vector<std::unique_ptr<QPConnection>>& conns =
      sourceRoutingTable_.at(srcQPN);

  if (conns.empty()) {
    throw std::runtime_error(
        fmt::format(
            "Available QPConnections from src: {} to dst: {} should not be empty.",
            flexId_.toString(),
            destination.toString()));
  }

  switch (tiebreak.getType()) {
    case packsim::thrift::TiebreakSpec::Type::ecmp: {
      return conns.at(entropy % conns.size()).get();
    }
    case packsim::thrift::TiebreakSpec::Type::spray: {
      const auto rng = simCtx_->random.sampleUniform<uint32_t>(
          0, static_cast<uint32_t>(conns.size() - 1));
      return conns.at(rng).get();
    }
    case packsim::thrift::TiebreakSpec::Type::__EMPTY__:
    case packsim::thrift::TiebreakSpec::Type::dpr:
    // @oss-disable
    case packsim::thrift::TiebreakSpec::Type::reflect:
    case packsim::thrift::TiebreakSpec::Type::fly:
    case packsim::thrift::TiebreakSpec::Type::pathspray:
      break;
  }
  throw std::runtime_error(
      fmt::format(
          "Unsupported tiebreak type {}",
          apache::thrift::util::enumNameSafe(tiebreak.getType())));
}

void Endpoint::buildQPConnectionTable(
    const FlexDeviceId& destination,
    const uint32_t srcQPN,
    const uint32_t dstQPN) {
  const size_t numPorts = ports_.size();
  FlexDeviceId s = getFlexId();
  FlexDeviceId d = destination;

  if (!portIndex_.has_value()) {
    throw std::runtime_error("Port index must be set");
  } else if (portIndex_.value() <= 1) {
    throw std::runtime_error("Port index must be greater than 1");
  } else if (numPorts == 0) {
    throw std::runtime_error("No ports found");
  }
  XLOGF(
      DBG,
      "Building connection table on {} for source: {} and destination: {}, portIndex: {}, srcQPN: {}, dstQPN: {}",
      getName(),
      flexId_.toString(),
      destination.toString(),
      portIndex_.value(),
      srcQPN,
      dstQPN);

  const uint8_t portIndex = portIndex_.value();
  std::vector<std::unique_ptr<QPConnection>>& conns =
      sourceRoutingTable_[srcQPN];

  // Get the port restrictions for this QPN. They come from the workload's QP
  // templates and list the ports the QPN may use (typically a small subset of
  // the NIC's ports). A port-level address with no port config for the QPN is
  // a configuration error and throws below.
  const packsim::thrift::QpnPortConfig* portConfig = getQpnPortConfig(srcQPN);

  std::vector<uint16_t> srcPortList;
  std::vector<uint16_t> dstPortList;

  if (portConfig == nullptr) {
    const size_t localQpnConfigCount = qpnPortConfigs_.size();
    const size_t workloadQpnConfigCount =
        workload_ ? workload_->qpnPortConfigs()->size() : 0;
    XLOGF(
        ERR,
        "buildQPConnectionTable: No port config for srcQPN {}. "
        "Available QPN configs: {} local, {} workload",
        srcQPN,
        localQpnConfigCount,
        workloadQpnConfigCount);
    const auto errMesg = fmt::format(
        "QPN port configs are expected when using port level addresses. "
        "Review QP templates input. NIC: {}, s: {} d: {}, sQPN {} dQPN {}. "
        "srcQPN={} not found in {} local + {} workload configs.",
        getName(),
        s.toString(),
        d.toString(),
        srcQPN,
        dstQPN,
        srcQPN,
        localQpnConfigCount,
        workloadQpnConfigCount);
    throw std::runtime_error(errMesg);
  } else {
    XLOGF(
        DBG,
        "{}: port config was ok for NIC: {}, s: {} d: {}, sQPN {} dQPN {}",
        __func__,
        getName(),
        s.toString(),
        d.toString(),
        srcQPN,
        dstQPN);
  }

  // Use the explicit port restrictions from the QP-template input.
  // That input uses 0-based port indices, but FlexDeviceIds are 1-based, so
  // add 1 to every port index here because these are used to create
  // FlexDeviceIds.
  srcPortList.reserve(portConfig->srcPorts()->size());
  for (const uint16_t zeroBasedSrcPort : *portConfig->srcPorts()) {
    srcPortList.emplace_back(folly::to<uint16_t>(zeroBasedSrcPort + 1));
  }
  dstPortList.reserve(portConfig->dstPorts()->size());
  for (const uint16_t zeroBasedDstPort : *portConfig->dstPorts()) {
    dstPortList.emplace_back(folly::to<uint16_t>(zeroBasedDstPort + 1));
  }

  if (srcPortList.empty() || dstPortList.empty()) {
    throw std::runtime_error(
        fmt::format(
            "No ports available for QPN {} -> {}. Check your configuration and provide at least 1 port for the QP to use. srcPorts.size()={}, dstPorts.size()={}",
            srcQPN,
            destination.toString(),
            srcPortList.size(),
            dstPortList.size()));
  }

  // Build all possible paths
  // Optionally filter out paths that are not reachable
  for (const uint16_t srcPort : srcPortList) {
    for (const uint16_t dstPort : dstPortList) {
      s.values[portIndex] = srcPort;
      d.values[portIndex] = dstPort;
      if (allowReachableRoutesOnly() && !canPortsReach(s, d)) {
        XLOGF(
            DBG,
            "Port pair {} -> {} is not reachable",
            s.toString(),
            d.toString());
        continue;
      }
      conns.emplace_back(std::make_unique<QPConnection>(s, d, srcQPN, dstQPN));
    }
  }

  XLOGF(
      DBG,
      "Built {} reachable connections for {} -> {} (srcQPN={})",
      conns.size(),
      flexId_.toString(),
      destination.toString(),
      srcQPN);
}

void Endpoint::setQpnPortConfig(
    const uint32_t localQPN,
    const packsim::thrift::QpnPortConfig& portConfig) {
  if (portConfig.srcPorts()->empty()) {
    XLOGF(
        WARN,
        "srcPorts is empty for QPN {} on endpoint {}",
        localQPN,
        getName());
  }
  if (portConfig.dstPorts()->empty()) {
    XLOGF(
        WARN,
        "dstPorts is empty for QPN {} on endpoint {}",
        localQPN,
        getName());
  }

  qpnPortConfigs_[localQPN] = portConfig;

  XLOGF(
      DBG,
      "Set QPN port config: endpoint={} localQPN={} srcPorts=[{}] dstPorts=[{}]",
      getName(),
      localQPN,
      fmt::join(*portConfig.srcPorts(), ","),
      fmt::join(*portConfig.dstPorts(), ","));
}

const packsim::thrift::QpnPortConfig* FOLLY_NULLABLE
Endpoint::getQpnPortConfig(const uint32_t localQPN) const {
  // Check local map first (used by setQpnPortConfig in tests)
  const auto it = qpnPortConfigs_.find(localQPN);
  if (it != qpnPortConfigs_.end()) {
    return &it->second;
  }
  // Then the workload's own map, which avoids copying every config to each
  // endpoint.
  if (workload_) {
    const auto wit = workload_->qpnPortConfigs()->find(localQPN);
    if (wit != workload_->qpnPortConfigs()->end()) {
      return &wit->second;
    }
  }
  XLOGF(DBG, "No port config for QPN {} (using legacy mode)", localQPN);
  return nullptr;
}

void Endpoint::onWorkloadPortConfigs(
    const packsim::workload::thrift::Workload* workload) {
  if (workload_ || !workload || workload->qpnPortConfigs()->empty()) {
    return;
  }
  workload_ = workload;
  XLOGF(
      DBG,
      "onWorkloadPortConfigs: Stored workload pointer with {} port configs for endpoint {}",
      workload->qpnPortConfigs()->size(),
      getName());
}

packsim::thrift::NicSummary Endpoint::getNicStatsSummaryResults() const {
  auto summary = transport_->getNicStatsSummaryResults();
  // Use ports_ map instead of ethPorts_ vector since Endpoint doesn't own the
  // ports. The map's iteration order is unspecified and differs between
  // builds, and readers take the list index as the port, so emit the ports in
  // ascending getMacStatsPortNum(). The port's index on its node, unique among
  // the node's ports, breaks any tie, so the order never depends on the map.
  std::vector<std::pair<uint16_t, const EthernetPort*>> portsByNum;
  portsByNum.reserve(ports_.size());
  for (const auto& [portId, ethPort] : ports_) {
    portsByNum.emplace_back(getMacStatsPortNum(*ethPort), ethPort);
  }
  std::ranges::sort(portsByNum, {}, [](const auto& entry) {
    return std::pair{entry.first, entry.second->getPortNum()};
  });
  auto macStats = summary.macStats();
  macStats->reserve(portsByNum.size());
  for (const auto& [portNum, ethPort] : portsByNum) {
    auto& portMacStats = macStats->emplace_back();
    for (const auto& cosStats : ethPort->getAllPortStats()) {
      portMacStats.cosStats()->emplace_back(cosStats.toThrift());
    }
  }
  return summary;
}

uint16_t Endpoint::getEgressNicPort(const EthernetPort& ethPort) const {
  // FlexDeviceId port values are 1-based; NIC egress ports are 0-based.
  uint16_t egressPort = ethPort.getFlexDeviceId().values[portIndex_.value()];
  --egressPort;
  return egressPort;
}

uint16_t Endpoint::getMacStatsPortNum(const EthernetPort& ethPort) const {
  return portIndex_.has_value() ? getEgressNicPort(ethPort)
                                : ethPort.getPortNum();
}

// The transport is wired to these in the constructor, before any subscriber
// exists, so the lambda defers the read to fire time. XCHECK rather than a
// silent guard: an endpoint that completes a message with nobody listening has
// lost that completion, which is a wiring bug rather than a configuration.
void Endpoint::onMessageReceived(Message* msg) {
  XCHECK(onMessageReceived_)
      << "no message-received subscriber on endpoint " << getName();
  onMessageReceived_(msg);
}

// A PING that reaches an endpoint with no handler installed is a bug in
// whoever put it on the wire, so this asserts rather than dropping it. An
// installer destroyed before the endpoints must clear the handler, which turns
// a late PING into this abort instead of a call into a destroyed responder.
void Endpoint::onPingReceived(const PacketPtr& pkt) {
  XCHECK(onPingReceived_) << "PING arrived at endpoint " << getName()
                          << " with no handler";
  onPingReceived_(pkt, this);
}

void Endpoint::onMessageAcked(Message* msg) {
  XCHECK(onMessageAcked_) << "no message-acked subscriber on endpoint "
                          << getName();
  onMessageAcked_(msg);
}

void Endpoint::onPacketSent(EthernetPort& ethPort, const PacketPtr& pkt) {
  transport_->onPacketSent(pkt, &ethPort);
}

void Endpoint::onLinkUp(EthernetPort* ethPort) {
  transport_->onLinkUp(ethPort);
}

bool Endpoint::isEgressPortLive(const uint16_t egressPort) const {
  if (!portIndex_.has_value()) {
    return true; // single-port: no alternative port, treat as up
  }
  // egressPort is 0-based; FlexDeviceId port values are 1-based.
  FlexDeviceId portId = getFlexId();
  uint16_t& portField = portId.values[portIndex_.value()];
  portField = egressPort;
  ++portField;
  const auto it = ports_.find(portId);
  // Unknown port: do not over-restrict (treat as up).
  return it == ports_.end() ||
      it->second->getChannelState() == ChannelState::UP;
}

void Endpoint::onPacketDroppedOnSend(
    EthernetPort& ethPort,
    const PacketPtr& pkt,
    uint8_t /* queue */,
    packsim::switches::thrift::PacketDropReason reason) {
  transport_->onPacketDropped(pkt);
  simCtx_->getScheduler()->onPacketDropped(
      DropEvent{
          getName(),
          getFlexId(),
          ethPort.getPortNum(),
          reason,
          &pkt,
          ethPort.getTotalQueueLength(),
          simCtx_->nowFs()});
}

void Endpoint::onPacketDroppedOnReceive(
    EthernetPort& ethPort,
    const PacketPtr& pkt,
    uint8_t /* queue */,
    packsim::switches::thrift::PacketDropReason reason) {
  simCtx_->getScheduler()->onPacketDropped(
      DropEvent{
          getName(),
          getFlexId(),
          ethPort.getPortNum(),
          reason,
          &pkt,
          ethPort.getTotalQueueLength(),
          simCtx_->nowFs()});
}

namespace {

/**
 * Settle one wire arrival's CBFC credit as `Endpoint::onPacketRcvd` unwinds:
 * free it now, unless a receive buffer claimed the packet and its drain will
 * free it instead.
 *
 * `arrivalWasCounted` is captured at entry, so enabling CBFC part way through
 * dispatch cannot free a credit that was never counted. The module to report
 * to is re-read from `rxPort->cbfc()` here rather than captured at entry, so
 * disabling CBFC part way through dispatch cannot report into a destroyed
 * module.
 *
 * `creditBridge` is disarmed before any early return: `arm` aborts if one is
 * still pending when the next arrival arms.
 */
void settleCbfcArrival(
    EthernetPort* FOLLY_NONNULL rxPort,
    const PacketPtr& pkt,
    cbfc::NicRxCreditBridge* creditBridge,
    const bool arrivalWasCounted) {
  const bool claimed =
      creditBridge != nullptr && creditBridge->disarmAndWasClaimed();
  if (!arrivalWasCounted) {
    return;
  }
  auto* cbfcAtExit = rxPort->cbfc();
  if (cbfcAtExit == nullptr) {
    return; // the module that counted the arrival is gone
  }
  if (claimed) {
    return; // a receive buffer took it; the drain will free the credit
  }
  cbfcAtExit->onPacketDrained(*pkt);
}

} // namespace

void Endpoint::onPacketRcvd(EthernetPort& ethPort, const PacketPtr& pkt) {
  pkt->tag.port = ethPort.getPortNum();
  pkt->tag.pg = 0;
  // CBFC receiver accounting (UEC §5.2.5.7.1/.7.2). The upstream switch spent a
  // credit forwarding this frame, so this device counts R_VC_CC unconditionally
  // on arrival, as `FbSwitch::onPacketRcvd` does. The scope guard below settles
  // that credit on every exit path; see `settleCbfcArrival` for how, and
  // `cbfc::NicRxCreditBridge` for which arrivals a buffer claims.
  //
  // This must stay above the PING branch and ahead of transport dispatch so it
  // runs once per wire arrival. An early return placed above it leaves R_VC_CC
  // short of the sender's S_VC_CC, stranding the credit at the sender until a
  // CC_Update reconcile hands it back (§5.2.5.7.4), and forfeiting it for the
  // rest of the run once that gap exceeds `cbfc::kMaxLostCredits`.
  auto* creditBridge = ethPort.rxCreditBridge();
  bool arrivalWasCounted = false;
  if (auto* cbfc = ethPort.cbfc(); cbfc != nullptr) {
    cbfc->onPacketStored(*pkt);
    arrivalWasCounted = true;
    if (creditBridge != nullptr) {
      creditBridge->arm(*pkt);
    }
  }
  SCOPE_EXIT {
    settleCbfcArrival(&ethPort, pkt, creditBridge, arrivalWasCounted);
  };

  // Intercept PING before the transport layer and hand it to the registered
  // handler; onPingReceived aborts if none is registered.
  if (pkt->pktType == PacketType::PING) {
    onPingReceived(pkt);
    return;
  }

  transport_->onPacketRcvd(pkt, &ethPort);
}

EthernetPort* Endpoint::routePacketByEgressNicPort(
    const PacketPtr& pkt,
    const uint16_t egressNicPort) {
  const auto udpPkt = pkt.as<PacketType::UDP>();
  const auto* conn = static_cast<const QPConnection*>(udpPkt->conn);
  udpPkt->socketLevelConn = udpPkt->conn;

  const uint8_t portIndex = portIndex_.value();
  // egressNicPort and dstNicPort are 0-based; FlexDeviceId is 1-based. A packet
  // whose transport decorrelates the two lands on its own destination port;
  // anything else lands on the port number it egresses from.
  uint16_t srcNicPort = egressNicPort;
  ++srcNicPort;
  uint16_t dstNicPort = udpPkt->chosenDstNicPort().value_or(egressNicPort);
  ++dstNicPort;

  // Build a cache key from srcQPN and the port pair. srcQPN is unique per
  // endpoint, so it alone identifies the QP pair; the two 16-bit ports fill the
  // low half of the word, so no field is truncated or overlapped.
  const uint64_t key = (uint64_t{conn->srcQPN} << 32) |
      (uint64_t{srcNicPort} << 16) | dstNicPort;

  auto it = egressNicPortConnections_.find(key);
  if (it == egressNicPortConnections_.end()) {
    FlexDeviceId srcId = conn->srcId;
    FlexDeviceId dstId = conn->dstId;
    srcId.values[portIndex] = srcNicPort;
    dstId.values[portIndex] = dstNicPort;
    auto newConn = std::make_unique<QPConnection>(
        srcId, dstId, conn->srcQPN, conn->dstQPN);
    it = egressNicPortConnections_.emplace(key, std::move(newConn)).first;
  }

  udpPkt->conn = it->second.get();

  const auto portIt = ports_.find(udpPkt->conn->srcId);
  if (portIt == ports_.end()) {
    std::vector<std::string> portAddresses;
    for (const auto& [pid, value] : ports_) {
      portAddresses.emplace_back(pid.toString());
    }
    throw std::runtime_error(
        fmt::format(
            "({}, {}): egressNicPort {} mapped to srcId {} not found in ports. "
            "Available: [{}]",
            getName(),
            getFlexId().toString(),
            egressNicPort,
            udpPkt->conn->srcId.toString(),
            fmt::join(portAddresses, ", ")));
  }

  return portIt->second;
}

} // namespace packsim
