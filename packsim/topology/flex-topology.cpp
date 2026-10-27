/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/topology/flex-topology.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>

#include <fmt/format.h>
#include <folly/container/detail/F14Policy.h>
#include <folly/logging/xlog.h>
#include <folly/memory/not_null.h>

// @oss-disable
// @oss-disable
#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/nic_config_types.h"
#include "packsim/if/gen-cpp2/roce_v2_config_types.h"
#include "packsim/if/gen-cpp2/topology_types.h"
#include "packsim/if/gen-cpp2/transport_config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/prefix-id-utils.h"
#include "packsim/link-layer/cbfc/cbfc-credit-tracker.h"
#include "packsim/link-layer/cbfc/cbfc-sender-ledger.h"
#include "packsim/link-layer/cbfc/cbfc.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/node/port-reachability.h"
// @oss-disable
#include "packsim/switch/fb-switch.h"

namespace packsim {

namespace {

/**
 * One end of a CBFC link: a port and the node that owns it.
 *
 * `node` must own `port`. The bootstrap reads a partner's receive buffer from
 * `node` indexed by `port`'s port number, so a mismatched pair silently yields
 * the credit pool of an unrelated port.
 */
struct CbfcLinkEnd {
  folly::not_null<EthernetPort*> port;
  folly::not_null<FlexNode*> node;
};

/**
 * Resolve the single CreditSize both ends of a link will use.
 *
 * CreditSize is a property of the LINK, not of a direction: each end uses it
 * both to charge what it sends and to count what it receives, so both must
 * resolve to the same number or the reconcile loop reports phantom leaks.
 *
 * The receiver's cell size decides it, because the credit pool is derived from
 * that buffer and a sub-cell frame still occupies a whole cell. A receiver
 * that does not quantize -- a byte-limited NIC buffer -- reports 0 and
 * constrains nothing; when neither end quantizes the configured value stands,
 * defaulting to `kCbfcLimits.defaultCreditSizeBytes`.
 *
 * @param end1                  One end of the link.
 * @param end2                  The other end.
 * @param configuredCreditSizeBytes
 *                              The link's thrift `expectedCreditSizeBytes`,
 *                              0 for "derive from the receiver". Must be the
 *                              raw
 *                              thrift value: `Cbfc::creditSizeBytes()` has
 *                              already replaced the 0 with the default,
 *                              which would hide the sentinel.
 * @return The credit size both ends will be set to, in bytes.
 * @throw std::invalid_argument if the two ends quantize differently, or if a
 *        non-zero configured credit size disagrees with the receiver's cell
 *        size (CreditSize != CellSize is not modelled).
 */
uint32_t resolveLinkCreditSizeBytes(
    const CbfcLinkEnd& end1,
    const CbfcLinkEnd& end2,
    uint32_t configuredCreditSizeBytes) {
  // Fall back to the module's resolved value only when nothing quantizes.
  const uint32_t fallbackCreditSize = end1.port->cbfc()->creditSizeBytes();
  std::optional<uint32_t> resolvedCellSize;
  // Names the end that contributed `resolvedCellSize` in the mismatch throw.
  std::string resolvedFrom;
  for (const auto& end : {end1, end2}) {
    const auto& port = end.port;
    const uint32_t cellSizeBytes = end.node->getRxCellSizeBytes();
    if (cellSizeBytes == 0) {
      continue;
    }
    if (resolvedCellSize.has_value() && *resolvedCellSize != cellSizeBytes) {
      throw std::invalid_argument(
          fmt::format(
              "CBFC link's two ends allocate their receive buffers in "
              "different units: {} uses {} B and {} uses {} B, so no single "
              "credit size can equal both. CreditSize is per link, not per "
              "direction. Give every receiver on the link the same "
              "cellSizeBytes.",
              resolvedFrom,
              *resolvedCellSize,
              port->getFlexDeviceId().toString(),
              cellSizeBytes));
    }
    // CreditSize != CellSize is NOT SUPPORTED. UEC §5.2.5.8 permits
    // CreditSize <= CellSize (example: CellSize 80, CreditSize 64, a 100-cell
    // buffer advertising 100 credits, not 125), but PackSim treats one credit
    // as one cell throughout: the pool is `creditBytes / creditSizeBytes`,
    // which over-credits the sender the moment the two differ; the switch
    // records a cell count as the credits it holds (`Packet::heldCredits`)
    // with no byte-domain conversion; and there is no excess-credit return
    // path.
    //
    // A configured 0 means "derive from the receiver", so it is not a
    // disagreement.
    if (configuredCreditSizeBytes > 0 &&
        cellSizeBytes != configuredCreditSizeBytes) {
      throw std::invalid_argument(
          fmt::format(
              "CBFC link at {}: configured creditSizeBytes {} does not match "
              "the receiver's {} B cell. PackSim requires CreditSize == "
              "CellSize; a smaller credit quantum (UEC §5.2.5.8) is not "
              "modelled. Set creditSizeBytes to 0 to derive it from the "
              "receiver, or to {} to match.",
              port->getFlexDeviceId().toString(),
              configuredCreditSizeBytes,
              cellSizeBytes,
              cellSizeBytes));
    }
    resolvedCellSize = cellSizeBytes;
    resolvedFrom = port->getFlexDeviceId().toString();
  }
  return resolvedCellSize.value_or(fallbackCreditSize);
}

/**
 * Sim-init CBFC credit bootstrap for one direction — the shortcut that
 * replaces the LLDP credit negotiation (UEC §5.2.8).
 *
 * A sender's credit limit is its PARTNER'S credited receive buffer: UEC
 * §5.2.8.2 says "Receiver.TotalCredits = Receiver(Input buffer size) /
 * Receiver(cell size)", and this model narrows "input buffer" to the port's
 * headroom pool (`FlexNode::getLosslessRxCreditBytes`), a model choice: the
 * reserved and shared regions are uncredited and return their credits at
 * enqueue.
 *
 * The pool is therefore per direction and asymmetric: a switch advertises its
 * lossless PG headroom, an accel endpoint its whole NIC RX buffer, which has
 * no headroom tier. A pool above `CbfcSenderLedger::kMaxPortCreditLimit` is
 * capped there, with a warning.
 *
 * @param local            The end whose sender limit is being initialized.
 * @param partner          The end whose credited receive buffer supplies that
 *                         limit.
 * @param creditSizeBytes  The resolved per-link credit size.
 * @throw std::invalid_argument if the partner advertises less than one credit,
 *        or the reservations exceed the derived pool.
 */
void bootstrapOneDirection(
    const CbfcLinkEnd& local,
    const CbfcLinkEnd& partner,
    const uint32_t creditSizeBytes) {
  // The sender ledger's bound, stricter than UEC Tbl 5-27's 2^19-1. Capping a
  // larger pool leaves part of the partner's buffer uncredited but never
  // overruns it.
  constexpr uint64_t kMaxTotalCredits =
      cbfc::CbfcSenderLedger::kMaxPortCreditLimit;

  const auto& port = local.port;
  const auto& partnerPort = partner.port;
  const auto& partnerNode = partner.node;
  auto* cbfc = port->cbfc();
  // Index the partner's buffer by the PARTNER's port number: port numbers live
  // in each node's own space -- FbSwitch draws them from its port groups,
  // FlexNode counts its ports -- so the local number would read an
  // unrelated port's headroom on any link the two ends number differently,
  // which is every switch-to-switch link. `makeLink` adds both ports before
  // calling this, so both numbers are final here.
  //
  // The credited bytes are per port and deliberately not divided across a
  // multi-port endpoint's ports. That is exact, not an N-way over-commit:
  // `RoceTransportBase` keys `queueToMemoryPerPort_` by port number and builds
  // each entry from the full `perCosBufferSize`, so an N-port endpoint really
  // does own N full-size receive buffers.
  const uint64_t creditBytes =
      partnerNode->getLosslessRxCreditBytes(partnerPort->getPortNum());
  if (creditBytes < creditSizeBytes) {
    throw std::invalid_argument(
        fmt::format(
            "CBFC link {} -> {}: the partner's port {} advertises {} B of "
            "credited lossless receive buffer, below one {} B credit. A "
            "lossless VC with no credits can never transmit — size the "
            "receiver's headroom to absorb a credit round trip at line rate.",
            port->getFlexDeviceId().toString(),
            partnerPort->getFlexDeviceId().toString(),
            partnerPort->getPortNum(),
            creditBytes,
            creditSizeBytes));
  }
  // §5.2.8.2's "input buffer size / cell size": `resolveLinkCreditSizeBytes`
  // guarantees CreditSize == CellSize, so this division yields a cell count.
  // It stops being correct if that equality is ever relaxed.
  const uint64_t uncappedCredits = creditBytes / creditSizeBytes;
  const uint64_t totalCredits = std::min(uncappedCredits, kMaxTotalCredits);
  if (uncappedCredits > kMaxTotalCredits) {
    XLOGF(
        WARN,
        "CBFC link {} -> {}: the partner's port {} advertises {} credits of "
        "lossless receive buffer, above the {} a sender's credit ledger can "
        "hold. Using the cap -- the sender will not use the whole advertised "
        "buffer.",
        port->getFlexDeviceId().toString(),
        partnerPort->getFlexDeviceId().toString(),
        partnerPort->getPortNum(),
        uncappedCredits,
        kMaxTotalCredits);
  }
  cbfc->initSenderPortLimit(totalCredits);
  if (!cbfc->initSenderReservations()) {
    // Left unthrown, this presents as a hung simulation rather than an error.
    throw std::invalid_argument(
        fmt::format(
            "CBFC device {}: vcMinCredits + overshootReserveCredits exceed "
            "the {} credit pool derived from the partner's advertised "
            "headroom; VCs without a reserve could never transmit.",
            port->getFlexDeviceId().toString(),
            totalCredits));
  }
}

/**
 * Sim-init CBFC credit bootstrap: resolve the link's credit size, install it
 * on both modules, then size each direction's sender limit from its partner's
 * advertised receive buffer.
 *
 * @param end1        One end of the link.
 * @param end2        The other end.
 * @param cbfcConfig  The link's thrift config; its raw
 *                    `expectedCreditSizeBytes` is the configured size, 0
 *                    meaning "derive from the receiver".
 * @throw std::invalid_argument if a partner advertises less than one credit,
 *        or the two ends quantize their receive buffers differently, or the
 *        resolved credit size violates a bound the configured value was
 *        checked against.
 * @throw std::runtime_error if the resolved credit size is 0, which is an
 *        internal invariant violation rather than a config error.
 */
void bootstrapCbfcCredits(
    const CbfcLinkEnd& end1,
    const CbfcLinkEnd& end2,
    const packsim::thrift::CbfcConfig& cbfcConfig) {
  const uint32_t creditSizeBytes = resolveLinkCreditSizeBytes(
      end1, end2, *cbfcConfig.expectedCreditSizeBytes());
  // Must precede `setCreditSize`, which throws its own, link-less
  // `std::invalid_argument` on 0. A 0 reaching `bootstrapOneDirection` would
  // divide by zero: its `creditBytes < creditSizeBytes` guard reads `x < 0` on
  // unsigned and never fires.
  if (creditSizeBytes == 0) {
    throw std::runtime_error(
        fmt::format(
            "CBFC link {} -> {}: resolved credit size is 0 B. The sentinel "
            "should have been replaced by the receiver's cell size or the "
            "default credit size before bootstrap.",
            end1.port->getFlexDeviceId().toString(),
            end2.port->getFlexDeviceId().toString()));
  }
  // Re-check the resolved size: `validateCbfcConfig` only saw the CONFIGURED
  // value, and a configured 0 resolves here to a receiver cell size that never
  // passed it -- possibly one outside the 12-bit S_CreditSize wire field.
  cbfc::CbfcCreditTracker::validateCreditSize(
      creditSizeBytes,
      fmt::format(
          "CBFC link {} -> {}",
          end1.port->getFlexDeviceId().toString(),
          end2.port->getFlexDeviceId().toString()));
  end1.port->cbfc()->setCreditSize(creditSizeBytes);
  end2.port->cbfc()->setCreditSize(creditSizeBytes);

  bootstrapOneDirection(end1, end2, creditSizeBytes);
  bootstrapOneDirection(end2, end1, creditSizeBytes);
}

// How a NIC config's RoCEv2 Go-Back-N receiver treats a PSN it already has.
enum class GoBackNAckKind {
  // The config does not run RoCEv2 Go-Back-N.
  NOT_GO_BACK_N,
  // Accepts every PSN, so a resent packet reads as new data.
  ACCEPTS_EVERY_PSN,
  // ACKs cumulatively and re-ACKs a PSN it already has.
  CUMULATIVE,
};

GoBackNAckKind getGoBackNAckKind(
    const packsim::thrift::NicConfig& nic) noexcept {
  const auto& transport = *nic.transport();
  if (transport.getType() != packsim::thrift::TransportConfig::Type::roceV2) {
    return GoBackNAckKind::NOT_GO_BACK_N;
  }
  const auto& lossRecovery = *transport.roceV2()->lossRecovery();
  if (lossRecovery.getType() !=
      packsim::thrift::RoceV2LossRecoveryConfig::Type::goBackN) {
    return GoBackNAckKind::NOT_GO_BACK_N;
  }
  const bool selectiveGapRepair = *lossRecovery.goBackN()->selectiveGapRepair();
  return *nic.outofOrderPktSupport() && !selectiveGapRepair
      ? GoBackNAckKind::ACCEPTS_EVERY_PSN
      : GoBackNAckKind::CUMULATIVE;
}

// Names of the NIC configs the accelerators' endpoints use, sorted.
std::set<std::string> getAccelNicConfigNames(
    const packsim::thrift::FlexTopology& topo) {
  std::set<std::string> names;
  for (const auto& [_, node] : *topo.nodes()) {
    if (*node.type() != packsim::thrift::NodeType::ACCEL) {
      continue;
    }
    for (const auto& endpointCfg : *node.endpointCfgs()) {
      names.emplace(*endpointCfg.nicConfig());
    }
  }
  return names;
}

// The one reachability tracer every endpoint shares, over the port field the
// accels' port-addressed endpoints use (fixed per topology, see
// PortReachability); null when no endpoint addresses ports.
//
// @throw std::invalid_argument if two endpoints use different port indices.
std::unique_ptr<const PortReachability> buildPortReachability(
    const packsim::thrift::FlexTopology& topo) {
  std::optional<uint8_t> topoPortIdx;
  for (const auto& [name, node] : *topo.nodes()) {
    if (*node.type() != packsim::thrift::NodeType::ACCEL) {
      continue;
    }
    for (const auto& endpointCfg : *node.endpointCfgs()) {
      // A port index of 0 means the endpoint has no port field.
      const uint8_t portIdx = *endpointCfg.portIdx();
      if (portIdx == 0) {
        continue;
      }
      if (topoPortIdx.has_value() && *topoPortIdx != portIdx) {
        throw std::invalid_argument(
            fmt::format(
                "Accel {} has an endpoint with port index {}, but another "
                "endpoint uses {}. Every port-addressed endpoint must share "
                "one port index, since one reachability tracer serves the "
                "topology.",
                name,
                portIdx,
                *topoPortIdx));
      }
      topoPortIdx = portIdx;
    }
  }
  if (!topoPortIdx.has_value()) {
    return nullptr;
  }
  return std::make_unique<const PortReachability>(topo, *topoPortIdx);
}

} // namespace

FlexTopology::FlexTopology(
    Scheduler* scheduler,
    Random* random,
    packsim::thrift::Simulation& sim)
    : simCfg_(&sim), scheduler_(scheduler), random_(random) {
  topo_ = &(*sim.flex_topology());
  portReachability_ = buildPortReachability(*topo_);

  buildNodes(sim);
  if (const auto mismatch = describeGoBackNAckMismatch(sim)) {
    XLOGF(WARN, "{}", *mismatch);
  }

  const auto& nicCfg = *sim.nic();
  for (const auto& pair : *topo_->links()) {
    makeLink(pair.second, nicCfg);
  }

  printDebugInfo();

  // A value that is not positive, including the default -1, disables
  // snapshots.
  const int64_t snapshotIntervalNs = sim.snapshotIntervalNs().value();
  if (snapshotIntervalNs <= 0) {
    return;
  }
  const auto snapInterval = static_cast<uint64_t>(snapshotIntervalNs);

  // taking snapshots, read the relevant nodes from config and store them
  const auto& snapshotNodes = sim.snapshotNodes().value();

  // if it's empty, take all switches
  if (snapshotNodes.empty()) {
    for (size_t switchIdx = 0; switchIdx < switches_.size(); ++switchIdx) {
      switches_.at(switchIdx)->doSnapshot(snapInterval);
    }
  }

  for (const auto& nodeName : snapshotNodes) {
    const auto it = nodes_.find(nodeName);
    if (it == nodes_.end()) {
      throw std::runtime_error(
          fmt::format("Node {} is not in nodes_", nodeName));
    }
    it->second->doSnapshot(snapInterval);
  }
}

void FlexTopology::buildNodes(const packsim::thrift::Simulation& sim) {
  auto& nicCfg = *sim.nic();
  for (auto& pair : *topo_->nodes()) {
    const auto& name = pair.first;
    auto& node = pair.second;
    const auto& role = *node.role();
    const auto& type = *node.type();

    // Ensure the valid prefix length list is in descending order
    auto& table = *node.lpmSpec();
    std::sort(
        table.validPrefixLengths()->begin(),
        table.validPrefixLengths()->end(),
        std::greater<>());

    const auto flexId = FlexDeviceId(*node.id());
    switch (type) {
      case packsim::thrift::NodeType::ACCEL:
        accelFlexIdQueue_.push(flexId);
        flexIdToName_[flexId] = pair.first;
        break;
      case packsim::thrift::NodeType::SWITCH:
        const auto& config = sim.switchConfigMap()->at(role);
        nodes_[name] = buildSwitch(name, config, flexId, &table);
        break;
    }
  }

  while (!accelFlexIdQueue_.empty()) {
    const auto flexId = accelFlexIdQueue_.top();
    const auto& name = flexIdToName_[flexId];
    const auto& node = (*topo_->nodes())[name];
    nodes_[name] = buildWorkloadNode(node, nicCfg, *sim.nicConfigMap());
    accelFlexIdQueue_.pop();
  }
}

void FlexTopology::printDebugInfo() const {
  if (!XLOG_IS_ON(DBG)) {
    return;
  }

  XLOGF(DBG, "========== FlexTopology Debug Info ==========");
  XLOGF(DBG, "Total Workload Nodes: {}", workloadNodes_.size());
  XLOGF(DBG, "Total Switches: {}", switches_.size());

  // Print out each NIC's port level addresses
  for (const auto& wkldNode : workloadNodes_) {
    const auto& nodeName = wkldNode->getName();
    const auto& nodeFlexId = wkldNode->getFlexId();

    XLOGF(
        DBG,
        "\n--- WorkloadNode: {} (FlexId: {}) ---",
        nodeName,
        nodeFlexId.toString());

    const auto& endpoints = wkldNode->getAllEndpoints();
    XLOGF(DBG, "  Number of Endpoints (NICs): {}", endpoints.size());

    for (size_t epIdx = 0; epIdx < endpoints.size(); ++epIdx) {
      const auto& endpoint = endpoints[epIdx];
      const auto& epFlexId = endpoint->getFlexId();

      XLOGF(DBG, "  Endpoint {}: FlexId {}", epIdx, epFlexId.toString());

      const auto& ports = endpoint->getPorts();
      XLOGF(DBG, "    Number of Ports: {}", ports.size());

      for (const auto& [portFlexId, ethPort] : ports) {
        const auto& peerId = ethPort->getPeerFlexDeviceId();
        const auto portNum = ethPort->getPortNum();
        XLOGF(
            DBG,
            "      Port {}: FlexId {} -> Remote FlexId {}",
            portNum,
            portFlexId.toString(),
            peerId.toString());
      }
    }
  }

  XLOGF(DBG, "========== End of FlexTopology Debug Info ==========\n");
}

std::optional<std::string> FlexTopology::describeGoBackNAckMismatch(
    const packsim::thrift::Simulation& sim) {
  const auto& nicMap = *sim.nicConfigMap();
  std::optional<std::string> acceptsEveryPsn;
  std::optional<std::string> cumulative;
  for (const auto& name : getAccelNicConfigNames(*sim.flex_topology())) {
    const auto it = nicMap.find(name);
    if (it == nicMap.end()) {
      continue;
    }
    switch (getGoBackNAckKind(it->second)) {
      case GoBackNAckKind::NOT_GO_BACK_N:
        break;
      case GoBackNAckKind::ACCEPTS_EVERY_PSN:
        acceptsEveryPsn = name;
        break;
      case GoBackNAckKind::CUMULATIVE:
        cumulative = name;
        break;
    }
  }
  if (!acceptsEveryPsn.has_value() || !cumulative.has_value()) {
    return std::nullopt;
  }
  return fmt::format(
      "NicConfig '{0}' runs RoCEv2 goBackN with outofOrderPktSupport on and "
      "selectiveGapRepair off, so it accepts every PSN, but NicConfig '{1}' "
      "runs goBackN with cumulative ACKs. Go-Back-N takes these settings from "
      "the local NIC and assumes the peer matches, so a QP from a '{1}' "
      "endpoint to a '{0}' endpoint can resend a tail packet that '{0}' takes "
      "as new data. Turn on selectiveGapRepair for '{0}', or give both the "
      "same outofOrderPktSupport.",
      *acceptsEveryPsn,
      *cumulative);
}

bool FlexTopology::checkOutofOrderPktReq(
    const FlexDeviceId& devId,
    const packsim::thrift::LpmTableSpec& table) {
  const packsim::thrift::LpmTableEntry* entry = &(*table.defaultRoute());
  auto vals = devId.asVec();
  for (const auto& len : *table.validPrefixLengths()) {
    vals.resize(len);
    const auto it = table.prefixTable()->find(packsim::makePrefixKey(vals));
    if (it == table.prefixTable()->end()) {
      continue;
    }
    entry = &(it->second);
    break;
  }
  auto& tiebreak = *entry->tiebreak();

  const bool isSprayEnabled = tiebreak.spray().has_value();
  // Tiebreak modes that reorder packets need out-of-order arrival support.
  // @oss-disable
  return isSprayEnabled; // @oss-enable
}

WorkloadNode* FlexTopology::buildWorkloadNode(
    const packsim::thrift::Node& node,
    const packsim::thrift::NicConfig& nicCfg,
    const std::map<std::string, packsim::thrift::NicConfig>& nicMap) {
  const std::string& name = *node.name();
  const auto accelId = workloadNodes_.size();
  const auto accelDevId = FlexDeviceId(*node.id());

  accelDevIds_.emplace_back(accelDevId);
  devIdToId_[accelDevId] = accelId;
  nodeDevIds_[name] = accelDevId;

  const auto& table = *node.lpmSpec();
  const bool outofOrderPktReq = checkOutofOrderPktReq(accelDevId, table);

  // Check if out of order packet arrival is supported by NIC
  // if out of order packet arrival is required
  if (outofOrderPktReq) {
    const bool nicOutofOrderSupport = (*nicCfg.outofOrderPktSupport());
    if (outofOrderPktReq != nicOutofOrderSupport) {
      XLOGF(ERR, "NIC does not support out of order packet arrival");
    }
  }
  auto simCtx = std::make_unique<SimContext>(
      scheduler_,
      name,
      random_->sampleUniform<uint64_t>(),
      &(*simCfg_->conditionalTraceIntervalsNs()));
  FlexNodeParams params{
      .simCtx = simCtx.get(),
      .name = name,
      .flexId = accelDevId,
      .lpmTable = &table,
      .topo = topo_,
      .portReachability = portReachability_.get(),
  };

  const packsim::thrift::NetworkLevelFeatureConfig* networkLevelFeatureConfig =
      nullptr;
  if (simCfg_->networkLevelFeatureConfig().has_value()) {
    networkLevelFeatureConfig = &simCfg_->networkLevelFeatureConfig().value();
  }

  auto accel = std::make_unique<WorkloadNode>( // @oss-enable
  // @oss-disable
      params,
      node,
      nicMap,
      // @oss-disable
      networkLevelFeatureConfig); // @oss-enable
      // @oss-disable
  simCtxs_.emplace_back(std::move(simCtx));
  for (const auto& e : accel->getAllEndpoints()) {
    endpoints_.emplace_back(e.get());
  }
  auto ptr = accel.get();
  workloadNodes_.emplace_back(std::move(accel));
  return ptr;
}

FbSwitch* FlexTopology::buildSwitch(
    const std::string& name,
    const packsim::thrift::SwitchConfig& cfg,
    const FlexDeviceId& devId,
    const packsim::thrift::LpmTableSpec* lpmTable) {
  auto simCtx = std::make_unique<SimContext>(
      scheduler_,
      name,
      random_->sampleUniform<uint64_t>(),
      &(*simCfg_->conditionalTraceIntervalsNs()));
  FlexNodeParams params{
      .simCtx = simCtx.get(),
      .name = name,
      .flexId = devId,
      .lpmTable = lpmTable,
      .topo = topo_,
  };
  auto sw = std::make_unique<FbSwitch>(params, &cfg);
  simCtxs_.emplace_back(std::move(simCtx));

  auto ptr = sw.get();
  nodeDevIds_[name] = devId;
  switches_.emplace_back(std::move(sw));
  return ptr;
}

std::pair<EthernetPort*, EthernetPort*> FlexTopology::connectNodes(
    FlexNode* n1,
    FlexNode* n2,
    const packsim::thrift::FlexLink& linkCfg) {
  auto* ctx1 = n1->getSimCtx();
  auto* ctx2 = n2->getSimCtx();
  auto port1U = std::make_unique<EthernetPort>(
      ctx1, *linkCfg.speed(), FlexDeviceId(*linkCfg.sourceAddress()));
  auto port2U = std::make_unique<EthernetPort>(
      ctx2, *linkCfg.speed(), FlexDeviceId(*linkCfg.destinationAddress()));
  auto port1 = port1U.get();
  auto port2 = port2U.get();

  // Move ownership of unique_ptr to the node
  n1->adoptPort(port1U);
  n2->adoptPort(port2U);

  auto channel = std::make_unique<EthernetChannel>(
      *linkCfg.delayConfig(), *linkCfg.linkFlapDelayNs(), *linkCfg.log10Ber());

  port1->attach(channel.get());
  port2->attach(channel.get());
  channels_.emplace_back(std::move(channel));

  const auto state = *linkCfg.status() == packsim::thrift::LinkStatus::UP
      ? ChannelState::UP
      : ChannelState::DOWN;
  port1->setChannelState(state);
  port2->setChannelState(state);

  // Install the CtlOS insertion rules (UEC §5.2.11.2) before the LLR and CBFC
  // blocks below: both call `setupLinkLayerScheduler`, which hands the rules
  // the port already holds to the new `LinkLayerScheduler` and starts it.
  // The rules have no defined behaviour on an already-transmitting port.
  if (linkCfg.ctlosConfig().has_value()) {
    const auto& ctlosCfg = linkCfg.ctlosConfig().value();
    port1->setCtlosInsertionRules(ctlosCfg);
    port2->setCtlosInsertionRules(ctlosCfg);
  }

  // Enable LLR if configured
  if (linkCfg.llrConfig().has_value()) {
    const auto& llrCfg = linkCfg.llrConfig().value();
    port1->enableLlr(llrCfg, /*enableGlobalTimer=*/true);
    port2->enableLlr(llrCfg, /*enableGlobalTimer=*/true);
  }

  // Enable CBFC if configured (UEC 1.0.3 §5.2).
  if (linkCfg.cbfcConfig().has_value()) {
    const auto& cbfcCfg = linkCfg.cbfcConfig().value();
    port1->enableCbfc(cbfcCfg);
    port2->enableCbfc(cbfcCfg);

    // The credit bootstrap runs later, from `makeLink`: it indexes the
    // partner's buffer by port number, and `setPortNum` does not happen until
    // `addSwitchPort`/`addPort`.
  }

  return {port1, port2};
}

void FlexTopology::makeLink(
    const packsim::thrift::FlexLink& link,
    const packsim::thrift::NicConfig& nicCfg) {
  const std::string& name1 = *link.node1();
  const std::string& name2 = *link.node2();
  const FlexDeviceId& id1 = nodeDevIds_.at(name1);
  const FlexDeviceId& id2 = nodeDevIds_.at(name2);
  FlexNode* node1 = nodes_.at(name1);
  FlexNode* node2 = nodes_.at(name2);

  const auto linkPorts = connectNodes(node1, node2, link);
  EthernetPort* port1 = linkPorts.first;
  EthernetPort* port2 = linkPorts.second;

  linkPorts_[*link.linkId()] = std::make_pair(port1, port2);

  // If port level address on the link is not provided,
  // then assign the port the XPU address
  if (!port1->getFlexDeviceId().isInitialized()) {
    port1->setFlexDeviceId(nodeDevIds_.at(name1));
    const auto& portId1 = port1->getFlexDeviceId();
    if (portId1 != id1) {
      throw std::runtime_error(
          fmt::format("{} != {}", portId1.toString(), id1.toString()));
    }
  }
  if (!port2->getFlexDeviceId().isInitialized()) {
    port2->setFlexDeviceId(nodeDevIds_.at(name2));
    const auto& portId2 = port2->getFlexDeviceId();
    if (portId2 != id2) {
      throw std::runtime_error(
          fmt::format("{} != {}", portId2.toString(), id2.toString()));
    }
  }

  auto accel1 = dynamic_cast<WorkloadNode*>(node1);
  auto accel2 = dynamic_cast<WorkloadNode*>(node2);
  auto sw1 = dynamic_cast<FbSwitch*>(node1);
  auto sw2 = dynamic_cast<FbSwitch*>(node2);
  auto& group1 = *link.node1PortGroup();
  auto& group2 = *link.node2PortGroup();

  if (accel1 != nullptr) {
    accel1->addPort(port1, id2, *link.linkId());
  } else {
    if (sw1 == nullptr) {
      throw std::runtime_error("Node 1 is not an endpoint or a switch");
    }
    sw1->addSwitchPort(port1, group1, id2, *link.linkId());
  }
  if (accel2 != nullptr) {
    accel2->addPort(port2, id1, *link.linkId());
  } else {
    if (sw2 == nullptr) {
      throw std::runtime_error("Node 2 is not an endpoint or a switch");
    }
    sw2->addSwitchPort(port2, group2, id1, *link.linkId());
  }

  // The credit bootstrap must follow the `addPort`/`addSwitchPort` calls
  // above: only then do both ports have the port numbers it indexes by.
  if (link.cbfcConfig().has_value()) {
    // Unreachable while `connectNodes` enables CBFC on both ends. A port
    // gating on a pool that was never bootstrapped refuses every lossless
    // packet, which presents as a stalled link, not as a setup error.
    if (!port1->isCbfcEnabled() || !port2->isCbfcEnabled()) {
      throw std::runtime_error(
          fmt::format(
              "Link {} has a cbfcConfig but CBFC is enabled on only one end "
              "({}: {}, {}: {}). Bootstrapping one end would leave the other "
              "gating on an unset credit pool.",
              *link.linkId(),
              port1->getFlexDeviceId().toString(),
              port1->isCbfcEnabled(),
              port2->getFlexDeviceId().toString(),
              port2->isCbfcEnabled()));
    }
    bootstrapCbfcCredits(
        CbfcLinkEnd{port1, node1},
        CbfcLinkEnd{port2, node2},
        *link.cbfcConfig());
  }

  // A value that is not positive, including the default -1, leaves periodic
  // stats off.
  const int64_t snapshotIntervalNs =
      nicCfg.netDeviceSnapshotIntervalNs().value();
  const uint64_t statsIntervalNs =
      snapshotIntervalNs > 0 ? static_cast<uint64_t>(snapshotIntervalNs) : 0;
  port1->enablePeriodicStats(
      fmt::format("{}_{}", name1, name2), statsIntervalNs);
  port2->enablePeriodicStats(
      fmt::format("{}_{}", name2, name1), statsIntervalNs);

  XLOGF(
      DBG, "{} -> {}: {} -> {}", name1, name2, id1.toString(), id2.toString());
}

WorkloadNode* FlexTopology::getWorkloadNode(const size_t id) const {
  return workloadNodes_.at(id).get();
}

WorkloadNode* FlexTopology::getWorkloadNode(const FlexDeviceId& id) const {
  const auto it = devIdToId_.find(id);
  if (it == devIdToId_.end()) {
    throw std::runtime_error(
        fmt::format("Device {} not found in topology", id.toString()));
  } else if (it->second >= workloadNodes_.size()) {
    throw std::runtime_error(
        fmt::format(
            "WorkloadNode {} not found in workload nodes. Should be at index {}. Workload nodes size: {}",
            id.toString(),
            it->second,
            workloadNodes_.size()));
  }
  return workloadNodes_.at(it->second).get();
}

std::vector<packsim::thrift::Snapshot> FlexTopology::serializeSnapshots() {
  std::map<uint64_t, Snapshot> snapMap;
  for (auto& switchNode : switches_) {
    for (auto& [time, snap] : switchNode->getSnapshots()) {
      snapMap[time.getNs()].switches.emplace_back(std::move(snap));
    }
    switchNode->clearSnapshots();
  }
  for (auto& workloadNode : workloadNodes_) {
    for (auto& [time, snap] : workloadNode->getSnapshots()) {
      for (auto& nic : snap) {
        snapMap[time.getNs()].nics.emplace_back(std::move(nic));
      }
    }
    workloadNode->clearSnapshots();
  }

  std::vector<packsim::thrift::Snapshot> snaps;
  snaps.reserve(snapMap.size());
  for (auto& [timeNs, snap] : snapMap) {
    snap.timeNs = timeNs;
    snaps.emplace_back(snap.toThrift());
  }
  return snaps;
}

size_t FlexTopology::getNumEndpoints() {
  return workloadNodes_.size();
}

} // namespace packsim
