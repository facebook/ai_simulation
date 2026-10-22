/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include <folly/CppAttributes.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include "packsim/endpoint/endpoint.h"
#include "packsim/endpoint/runtime/collective-barrier.h"
#include "packsim/endpoint/runtime/dependency-tracker.h"
#include "packsim/endpoint/runtime/global-dag-coordinator.h"
#include "packsim/endpoint/runtime/message-tracker.h"
#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/time.h"
#include "packsim/node/flex-node.h"
#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

/**
 * WorkloadNode represents a node in the network that hosts workloads.
 */
class WorkloadNode : public FlexNode {
 public:
  /**
   * @param params             FlexNode construction parameters
   * @param nodeCfg            Node configuration from thrift
   * @param nicMap             Map of NIC (Network Interface Card)
   * configurations
   * @param networkLevelFeatureConfig Optional network-level feature settings
   * @throw std::invalid_argument if an endpoint lists no NIC id, or ids for
   *        different addresses, or a portIdx out of range.
   * @throw std::runtime_error if an endpoint's nicConfig is not in nicMap.
   * @throw std::out_of_range if a node without endpointCfgs finds no
   *        kDefaultNicConfig entry in nicMap.
   */
  WorkloadNode(
      const FlexNodeParams& params,
      const packsim::thrift::Node& nodeCfg,
      const std::map<std::string, packsim::thrift::NicConfig>& nicMap,
      const packsim::thrift::NetworkLevelFeatureConfig*
          networkLevelFeatureConfig = nullptr);

  void addPort(
      EthernetPort* ethPort,
      const FlexDeviceId& partnerId,
      packsim::thrift::LinkId linkId) override {
    FlexNode::addPort(ethPort, partnerId, linkId);
  }

  /**
   * Add the port to the endpoint whose `linkIds` list `linkId`. A node
   * configured without any `linkIds` must have a single endpoint, which takes
   * every port.
   *
   * @param ethPort    Port to add
   * @param partnerId  ID of the partner device
   * @param linkId     Link identifier
   * @param portNum    Port number
   * @throw            std::invalid_argument if no endpoint lists `linkId` and
   *                   the node lists links or has several endpoints.
   */
  void addPort(
      EthernetPort* ethPort,
      const FlexDeviceId& partnerId,
      packsim::thrift::LinkId linkId,
      uint16_t portNum) override;

  /**
   * @param msg Message to send
   */
  void makeSendRequest(packsim::Message* FOLLY_NONNULL msg);

  /**
   * @param msg Message to receive
   */
  void makeReceiveRequest(packsim::Message* FOLLY_NONNULL msg);

  /**
   * @param msg Message that was received
   */
  void onMessageReceived(packsim::Message* msg);

  /**
   * @param msg Message that was acknowledged
   */
  void onMessageAcked(packsim::Message* msg);

  using SubmitMessageFn = std::function<void(Message*)>;
  using OnRankCompleteCallback = std::function<void(uint32_t, uint32_t)>;

  /**
   * @return Tuple of (send callback, receive callback, rank complete callback)
   */
  std::tuple<SubmitMessageFn, SubmitMessageFn, OnRankCompleteCallback>
  genCallbacks();

  // Get all endpoints managed by this node.
  const std::vector<std::unique_ptr<Endpoint>>& getAllEndpoints() const {
    return endpoints_;
  }

  // Get endpoint by index.
  Endpoint* getEndpoint(uint32_t idx) const {
    return endpoints_.at(idx).get();
  }

  /**
   * An accel endpoint has no headroom tier: `NicRxBuffer` reserves no
   * headroom and counts bytes rather than cells, so unlike the base contract
   * the whole RX buffer is credited space.
   *
   * `portNum` is ignored. Every port under an endpoint gets its own
   * `NicRxBuffer` sized from that endpoint's `NicConfig`, and only the first
   * endpoint is consulted, so a node whose endpoints were built from different
   * `NicConfig`s reports the first one's buffer size for every port.
   *
   * TODO: split per VC once a multi-lossless-VC endpoint config exists;
   * today every endpoint carries a single lossless VC.
   */
  [[nodiscard]] uint64_t getLosslessRxCreditBytes(
      uint16_t /* portNum */) const override {
    return endpoints_.empty() ? 0 : endpoints_.front()->getRxBufferBytes();
  }

  using OnAllWorkloadsCompleteCallback =
      std::function<void(const FlexDeviceId&)>;

  // Set callback invoked when a workload completes.
  void setOnAllWorkloadsCompleteCallback(
      const OnAllWorkloadsCompleteCallback& cb) {
    onAllWorkloadsComplete_ = cb;
  }

  /**
   * @param workloadId ID of the completed workload
   * @param rank       Rank that completed
   */
  void onWorkloadComplete(uint32_t workloadId, uint32_t rank);

  // Raise the node's own completion event, which reports that no workload is
  // left pending. Distinct from onWorkloadComplete above: that one is this
  // node's subscriber for a single rank finishing.
  void onAllWorkloadsComplete();

  /**
   * Build a new workload owned by this endpoint.
   *
   * @param wCfg      Workload configuration
   * @param rank      Rank number
   * @param iteration Iteration count (default 1)
   */
  void buildWorkloadRank(
      const packsim::workload::thrift::Workload& wCfg,
      const uint32_t rank,
      const uint32_t iteration = 1);

  /**
   * Eagerly create a socket (QP) on the specified endpoint.
   * Triggers the existing transport-state construction and QP registration
   * path.
   *
   * @param endpointId Endpoint to create the socket on
   * @param dstId      Destination device ID
   * @param localQPN   Local Queue Pair Number
   * @param remoteQPN  Remote Queue Pair Number
   * @param workload   Optional workload configuration
   * @return           Created socket
   */
  Socket* eagerCreateSocket(
      uint32_t endpointId,
      const FlexDeviceId& dstId,
      uint32_t localQPN,
      uint32_t remoteQPN,
      const packsim::workload::thrift::Workload* workload);

  using OnMessageReceivedCallback = std::function<void(packsim::Message*)>;
  using OnMessageAckedCallback = std::function<void(packsim::Message*)>;

  // Set callback invoked when a message is received.
  void setOnMessageReceivedCallback(const OnMessageReceivedCallback& cb) {
    onMessageReceived_ = cb;
  }

  // Set callback invoked when a message is acknowledged.
  void setOnMessageAckedCallback(const OnMessageAckedCallback& cb) {
    onMessageAcked_ = cb;
  }

  /**
   * @return Vector of all workload ranks managed by this node
   */
  std::vector<packsim::WorkloadRank*> getWorkloads() const {
    std::vector<packsim::WorkloadRank*> ret;
    ret.reserve(workloads_.size());
    for (const auto& [_, workload] : workloads_) {
      ret.emplace_back(workload.get());
    }
    return ret;
  }

  // Capture current NIC statistics snapshot.
  virtual void takeSnapshot() override {
    std::vector<packsim::thrift::NicSummary> nicSummaries;
    nicSummaries.reserve(endpoints_.size());
    for (const auto& ep : endpoints_) {
      nicSummaries.emplace_back(ep->getNicStatsSummaryResults());
    }
    snapshots_.emplace_back(simCtx_->now(), std::move(nicSummaries));
  }

  // Get all captured snapshots.
  std::vector<std::tuple<Time, std::vector<packsim::thrift::NicSummary>>>&
  getSnapshots() {
    return snapshots_;
  }

  // Clear all captured snapshots and release memory.
  void clearSnapshots() {
    snapshots_.clear();
    snapshots_.shrink_to_fit();
  }

  /**
   * @param depInfos             Dependency information
   * @param globalDagCoordinator Global DAG coordinator
   */
  void setDependencyInfos(
      DependencyInfos* FOLLY_NONNULL depInfos,
      GlobalDagCoordinator* FOLLY_NONNULL globalDagCoordinator);

  /**
   * Start the workloads on this node if they have no DAG dependencies or if
   * they are not part of any DAGs.
   * NOTE: This should be only called once at the beginning of the simulation.
   */
  void startWorkloads();

  /**
   * Schedule a workload to start at a certain delay relative to now.
   *
   * @param workloadId ID of the workload to run
   * @param delayNs    Delay in nanoseconds (default 0)
   */
  void runWorkload(const uint32_t workloadId, const uint64_t delayNs = 0);

  /**
   * For the given workload ID, setup a collective barrier.
   *
   * @param workloadId ID of the workload
   * @param barrier    Collective barrier instance
   */
  void setCollectiveBarrier(
      const uint32_t workloadId,
      std::unique_ptr<CollectiveBarrier> barrier);

  /**
   * Provide read-only access to all the stored rank results.
   *
   * @return Vector of per-rank workload results
   */
  std::vector<packsim::thrift::PerRankWorkloadResult> const&
  getWorkloadRankResults() const {
    return workloadRankResults_;
  }

  // Get per-message tracker (nullptr when tracking is disabled).
  const MessageTracker* messageTracker() const {
    return messageTracker_.get();
  }

 protected:
  /**
   * Called once per rank, right after `buildWorkloadRank` builds it and before
   * its expected bytes are counted. The base does nothing.
   *
   * @param rank  The rank just built; this node owns it.
   * @param wCfg  Its workload configuration, which outlives the rank.
   */
  virtual void onRankBuilt(
      WorkloadRank& /*rank*/,
      const packsim::workload::thrift::Workload& /*wCfg*/) {}

  /**
   * Post @p msg on the send path. The base hands it to the endpoint it names
   * and records the socket it used against its workload.
   *
   * @param msg Message to send
   */
  virtual void submitSend(Message* FOLLY_NONNULL msg);

  /**
   * Post @p msg on the receive path. The base hands it to the endpoint it
   * names and records the socket it used against its workload.
   *
   * @param msg Message to receive
   */
  virtual void submitReceive(Message* FOLLY_NONNULL msg);

  /**
   * Map a message delivered by an endpoint to the one its workload is waiting
   * for. The base returns @p msg unchanged.
   *
   * @param msg The delivered message
   * @return    The message to deliver upward, or nullptr to deliver nothing yet
   */
  virtual Message* FOLLY_NULLABLE resolveReceived(Message* FOLLY_NONNULL msg) {
    return msg;
  }

  /**
   * Map an acknowledged message to the one its workload is waiting for. The
   * base returns @p msg unchanged.
   *
   * @param msg The acknowledged message
   * @return    The message to acknowledge upward, or nullptr for none yet
   */
  virtual Message* FOLLY_NULLABLE resolveAcked(Message* FOLLY_NONNULL msg) {
    return msg;
  }

  /**
   * Record that @p workloadId opened a socket, so it is unregistered when that
   * workload completes.
   *
   * @param workloadId Workload that owns the socket
   * @param endpointId Endpoint the socket is on
   * @param peer       The socket's remote device
   * @param localQPN   The socket's local Queue Pair Number
   */
  void trackWorkloadSocket(
      uint32_t workloadId,
      uint32_t endpointId,
      const FlexDeviceId& peer,
      uint32_t localQPN);

 private:
  // Test fixture that verifies rank-specific ACK routing and generic fallback.
  friend class WorkloadNodeAckRoutingTest;

  std::vector<std::unique_ptr<Endpoint>> endpoints_;

  folly::F14FastMap<uint32_t, std::unique_ptr<packsim::WorkloadRank>>
      workloads_;

  // Workload ranks that have been added to the node but not yet finished.
  folly::F14FastSet<uint32_t /* workload ID */> pendingWorkloads_;

  // Track the sockets created by various workloads. This is used to unregister
  // sockets once the workload completes.
  folly::F14FastMap<
      uint32_t /* workload ID */,
      folly::F14FastSet<std::tuple<
          uint32_t /* endpointId */,
          FlexDeviceId /* dstIdx */,
          uint32_t /* localQPN */>>>
      workloadSocketInfos_;

  folly::F14FastMap<packsim::thrift::LinkId, uint32_t> linkToEndpoint_;

  OnMessageReceivedCallback onMessageReceived_;

  OnMessageAckedCallback onMessageAcked_;

  OnAllWorkloadsCompleteCallback onAllWorkloadsComplete_;

  std::vector<std::tuple<Time, std::vector<packsim::thrift::NicSummary>>>
      snapshots_;

  std::unique_ptr<DependencyTracker> dt_;

  // A barrier is used for each workload that has a rank placed on this workload
  // node to coordinate workload level events through a leader rank.
  folly::
      F14FastMap<uint32_t /* workload ID */, std::unique_ptr<CollectiveBarrier>>
          workloadBarrier_;

  // Per rank thrift results for all the ranks under this node. In case of
  // multiple iterations, this will hold the results for all iterations of all
  // ranks under this node.
  std::vector<packsim::thrift::PerRankWorkloadResult> workloadRankResults_;

  // Network-level feature configuration (per-message tracking, etc.)
  const packsim::thrift::NetworkLevelFeatureConfig* networkLevelFeatureConfig_{
      nullptr};

  bool enablePerMessageTracking_{false};

  std::unique_ptr<MessageTracker> messageTracker_;
};

} // namespace packsim
