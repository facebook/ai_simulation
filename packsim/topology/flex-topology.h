/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include <folly/CppAttributes.h>
#include <folly/container/F14Map.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/endpoint/runtime/workload-node.h"
#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/results_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/node/port-reachability.h"
#include "packsim/switch/fb-switch.h"

namespace packsim {

struct Snapshot {
  uint64_t timeNs{};
  std::vector<packsim::switches::thrift::Switch> switches;
  std::vector<packsim::thrift::NicSummary> nics;

  packsim::thrift::Snapshot toThrift() {
    packsim::thrift::Snapshot snap;
    snap.timeNs() = timeNs;
    snap.switchStats() = std::move(switches);
    snap.nicStats() = std::move(nics);
    return snap;
  }
};

class FlexTopology {
 public:
  explicit FlexTopology(
      Scheduler* scheduler,
      Random* random,
      packsim::thrift::Simulation& sim);

  WorkloadNode* getWorkloadNode(size_t id) const;

  WorkloadNode* getWorkloadNode(const FlexDeviceId& id) const;

  const std::vector<std::unique_ptr<FbSwitch>>& getAllSwitches()
      const noexcept {
    return switches_;
  }

  const std::vector<Endpoint*>& getAllEndpoints() const noexcept {
    return endpoints_;
  }

  const std::vector<std::unique_ptr<WorkloadNode>>& getAllWorkloadNodes()
      const noexcept {
    return workloadNodes_;
  }

  size_t getNumEndpoints();

  std::vector<packsim::thrift::Snapshot> serializeSnapshots();

  /**
   * The two ports a link joins.
   *
   * @param linkId  The link's id in the topology config.
   *
   * @return        The port on the link's `node1`, then the one on its
   *                `node2`; neither is null.
   *
   * @throw std::out_of_range if `linkId` names no link in this topology.
   */
  std::pair<EthernetPort*, EthernetPort*> getLinkPorts(
      packsim::thrift::LinkId linkId) const {
    return linkPorts_.at(linkId);
  }

  /** Print out debug level logs for the topology. */
  void printDebugInfo() const;

  /**
   * Find two NIC configs, among those the accelerators' endpoints use, that
   * would give a RoCEv2 Go-Back-N QP ends that disagree on ACKs: one accepts
   * every PSN (outofOrderPktSupport on, selectiveGapRepair off), the other's
   * ACKs are cumulative (outofOrderPktSupport off, or selectiveGapRepair on).
   * GoBackNRecovery takes these settings from the local NIC and assumes the
   * peer matches, so a QP from the second kind to the first arms a retransmit
   * timer whose tail resends the first kind takes as new data.
   *
   * @param sim  Simulation config whose `nicConfigMap` is filled in.
   *
   * @return     A message naming one config of each kind, or std::nullopt
   *             when the Go-Back-N configs in use do not mix the two.
   */
  static std::optional<std::string> describeGoBackNAckMismatch(
      const packsim::thrift::Simulation& sim);

  std::vector<std::unique_ptr<SimContext>> simCtxs_;

 private:
  void buildNodes(const packsim::thrift::Simulation& sim);
  std::pair<EthernetPort*, EthernetPort*> connectNodes(
      FlexNode* n1,
      FlexNode* n2,
      const packsim::thrift::FlexLink& linkCfg);
  WorkloadNode* buildWorkloadNode(
      const packsim::thrift::Node& node,
      const packsim::thrift::NicConfig& nicCfg,
      const std::map<std::string, packsim::thrift::NicConfig>& nicMap);

  FbSwitch* buildSwitch(
      const std::string& name,
      const packsim::thrift::SwitchConfig& cfg,
      const FlexDeviceId& devId,
      const packsim::thrift::LpmTableSpec* lpmTable);

  void makeLink(
      const packsim::thrift::FlexLink& link,
      const packsim::thrift::NicConfig& nicCfg);

  /**
   * Check if out-of-order message arrival is required for accelerators.
   *
   * @param devId The device identifier
   * @param table The LPM table specification
   * @return True if out-of-order packet handling is required
   */
  bool checkOutofOrderPktReq(
      const FlexDeviceId& devId,
      const packsim::thrift::LpmTableSpec& table);

  const packsim::thrift::Simulation* FOLLY_NONNULL const simCfg_;
  Scheduler* const scheduler_;
  Random* const random_;
  folly::F14FastMap<std::string, FlexNode*> nodes_;
  std::vector<std::unique_ptr<EthernetChannel>> channels_;

  packsim::thrift::FlexTopology* topo_;
  // Reachability tracer over topo_ shared by every endpoint; declared before
  // workloadNodes_ so it outlives them. Null when no endpoint addresses ports.
  std::unique_ptr<const PortReachability> portReachability_;
  folly::F14FastMap<std::string, FlexDeviceId> nodeDevIds_;
  std::vector<FlexDeviceId> accelDevIds_;
  folly::F14FastMap<FlexDeviceId, size_t> devIdToId_;
  std::vector<std::unique_ptr<WorkloadNode>> workloadNodes_;
  std::vector<Endpoint*> endpoints_;

  // priority queue to re-order the endpoints
  std::priority_queue<
      FlexDeviceId,
      std::vector<FlexDeviceId>,
      std::greater<FlexDeviceId>>
      accelFlexIdQueue_;
  // map flexId to (endpoint) node name
  folly::F14FastMap<FlexDeviceId, std::string> flexIdToName_;

  // Each link's two ports, in `node1`, `node2` order.
  folly::F14FastMap<
      packsim::thrift::LinkId,
      std::pair<EthernetPort*, EthernetPort*>>
      linkPorts_;

  std::vector<std::unique_ptr<FbSwitch>> switches_;

  // store the relevant endpoints and switches for snapshotting
  // create this in constructor
  std::vector<WorkloadNode*> snapshotWorkloadNodes_;
  std::vector<FbSwitch*> snapshotSwitches_;
  std::vector<Snapshot> snapshots_;
};

} // namespace packsim
