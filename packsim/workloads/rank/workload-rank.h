/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <folly/container/F14Map.h>
#include <folly/portability/GTestProd.h>

#include "packsim/if/gen-cpp2/qp_pair_config_types.h"
#include "packsim/if/gen-cpp2/results_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/transport/transport.h"
#include "packsim/workloads/parallel_qp/channel_manager.h"

namespace packsim {

constexpr uint32_t kNotifyMsgSize = 4;
// connectionMessageId of the notify messages. Per-message tracking keys a
// message by its ranks and this id, so it sits at the top of the id space,
// above the ids workloads give their data messages.
constexpr uint32_t kNotifyConnectionMessageId =
    std::numeric_limits<uint32_t>::max();
// How long a PULL-mode rank waits after its last READ before raising
// rank-complete, so that, on a transport that models RDMA READ, the
// responder's read-response messages can close on the initiator's ACKs
// (1 ms).
constexpr uint64_t kPullDrainDelayNs = 1'000'000;

class WorkloadRank;
struct WorkloadRankParams {
  // The rank calls this to hand a message to the node that owns it.
  using SubmitMessageFn = std::function<void(Message*)>;

  // The rank fires these upward when one of its requests completes. Same
  // signature as SubmitMessageFn, deliberately distinct types: these travel
  // the opposite direction.
  using OnRequestAckedCallback = std::function<void(Message*)>;
  using OnRequestReceivedCallback = std::function<void(Message*)>;
  using OnRankCompleteCallback = std::function<void(uint32_t, uint32_t)>;
  // Factory that builds a child WorkloadRank of any type. buildWorkloadRank
  // seeds itself here so composite ranks (BatchRank, ArRecursiveRank) can build
  // children without a link-time dependency back on builder.cpp — that
  // back-call is what previously kept them in builder's build rule.
  using WorkloadRankFactory = std::function<std::unique_ptr<WorkloadRank>(
      SimContext*,
      uint32_t,
      const packsim::workload::thrift::Workload&,
      uint32_t,
      const SubmitMessageFn&,
      const SubmitMessageFn&,
      const OnRankCompleteCallback&,
      WorkloadRank*)>;
  SimContext* simCtx;
  uint32_t workloadId{0};
  const packsim::workload::thrift::Workload& workCfg;
  uint32_t rank{0};
  const SubmitMessageFn& smcb;
  const SubmitMessageFn& rmcb;
  const OnRankCompleteCallback& rccb;
  WorkloadRank* parent{nullptr};
  WorkloadRankFactory factory{};
};

class WorkloadRank {
 public:
  explicit WorkloadRank(const WorkloadRankParams& params);
  virtual ~WorkloadRank() = default;
  virtual void run() = 0;
  void requestReceived(Message* msg);
  void requestAcked(Message* msg);
  /**
   * Attempt to consume a transport ACK owned by a rank-specific sub-pipeline.
   *
   * @param msg  A transport-completed message for this workload.
   *
   * @return     True when the sub-pipeline consumed the ACK.
   */
  virtual bool tryHandleTransportAck(Message* /*msg*/) {
    return false;
  }
  virtual uint64_t getExpectedBytes() = 0;
  virtual bool complete() = 0;
  virtual void progress() {}
  void notify(uint32_t toRank);
  void postRecvNotify(uint32_t fromRank);
  bool isNotifyMsg(Message* msg) const;
  void notifyReceived(uint32_t fromRank);
  void notifyAcked(uint32_t toRank);
  bool checkNotifyReceived(uint32_t fromRank);
  bool checkNotifyAcked(uint32_t fromRank);

  std::vector<uint32_t> getPeers() const;

  WorkloadRank* getParent() const noexcept {
    return parent_;
  }

  SimContext* getSimCtx() const noexcept {
    return simCtx_;
  }

  // Also stamps the iteration on the notify receives the constructor already
  // posted, since they were created before it was known.
  void setIteration(const uint32_t iter) {
    iteration_ = iter;
    for (auto& [_, msgs] : recvNotifyMessages_) {
      for (auto& msg : msgs) {
        msg->iteration = iter;
      }
    }
    if (channelManager_ != nullptr) {
      channelManager_->setIteration(iter);
    }
  }

  packsim::thrift::PerRankWorkloadResult getThriftResult() const {
    packsim::thrift::PerRankWorkloadResult res;
    res.startTimeNs() = startTimeNs_;
    res.endTimeNs() = endTimeNs_;
    res.workloadId() = workloadId_;
    res.rank() = rank_;
    res.name() = workloadName_;
    res.iteration() = iteration_;
    return res;
  }

 protected:
  // Raise the rank-complete event. Protected because every concrete rank
  // completes through it; the member is read nowhere else.
  void onRankComplete(uint32_t workloadId, uint32_t rank);

  /**
   * Run `start` once this rank's start delay (`startTimeDelayNs_`, from the
   * workload's staggered-start settings) has passed: in an event scheduled
   * that far ahead, or at once when the delay is 0, so that a workload without
   * a stagger gets no extra event.
   *
   * @param start  Callable that starts the rank's traffic.
   */
  template <typename TStart>
  void runAfterStartDelay(TStart&& start) {
    if (startTimeDelayNs_ == 0) {
      start();
      return;
    }
    simCtx_->schedule(
        Time::fromNs(startTimeDelayNs_), std::forward<TStart>(start));
  }

  std::unique_ptr<Message> send(
      uint32_t toRank,
      uint32_t size,
      uint32_t chunkId,
      uint32_t endpointId = 0,
      uint64_t sendDelayNs = 0,
      bool writeImm = false,
      bool write = false,
      uint32_t writeImmData = 0,
      uint64_t dataPtr = 0);

  // NIC-aware send that uses specific source and destination NICs
  // for multi-NIC configurations
  std::unique_ptr<Message> sendWithNic(
      uint32_t toRank,
      uint32_t size,
      uint32_t chunkId,
      uint32_t srcNicIdx,
      uint32_t dstNicIdx,
      uint32_t endpointId = 0,
      uint64_t sendDelayNs = 0,
      bool writeImm = false,
      bool write = false,
      uint32_t writeImmData = 0,
      uint64_t dataPtr = 0);

  std::unique_ptr<Message> recv(
      uint32_t fromRank,
      uint32_t size,
      uint32_t chunkId,
      uint32_t endpointId = 0,
      uint64_t sendDelayNs = 0,
      bool isRead = false,
      uint64_t dataPtr = 0);

  const packsim::workload::thrift::QPNPair& getDataQPNs(uint32_t peer) const;
  const packsim::workload::thrift::QPNPair& getNotifyQPNs(uint32_t peer) const;
  // NIC pair owning the notify QP to `peer` (srcNicIdx is this rank's NIC,
  // dstNicIdx the peer's): the first NIC pair, as in getNotifyQPNs. Legacy
  // qpnPairMap QPs live on NIC 0, so they report (0, 0).
  packsim::thrift::NicPair getNotifyNicPair(uint32_t peer) const;

  // NIC-aware QPN accessors for multi-NIC configurations
  // Uses srcNicIdx and dstNicIdx to lookup in the 4-level nicQpnPairMap
  const packsim::workload::thrift::QPNPair&
  getDataQPNs(uint32_t peer, uint32_t srcNicIdx, uint32_t dstNicIdx) const;
  const packsim::workload::thrift::QPNPair&
  getNotifyQPNs(uint32_t peer, uint32_t srcNicIdx, uint32_t dstNicIdx) const;

  // Get the total number of data QPs available for a given peer
  // Returns count of all QPs across all NIC pairs (from nicQpnPairMap)
  // or falls back to legacy qpnPairMap count
  uint32_t getTotalDataQPCount(uint32_t peer) const;

  // Get data QPN pair by flat index along with the NIC indices.
  // Returns the QPN pair and populates srcNicIdx/dstNicIdx with the
  // NIC indices from the sender's perspective (srcNicIdx = sender's NIC,
  // dstNicIdx = receiver's NIC).
  // NOTE: Requires NIC-aware mode (nicQpnPairMap must be populated).
  // Throws runtime_error if nicQpnPairMap is empty.
  const packsim::workload::thrift::QPNPair& getDataQPNByIndexWithNic(
      uint32_t peer,
      uint32_t qpIndex,
      uint32_t& srcNicIdx,
      uint32_t& dstNicIdx) const;

  // Round-robin send across all available QPs for the given peer.
  // Uses qpIndex to select which QP to use (typically chunk ID or counter).
  // The actual QP is selected as: qpIndex % getTotalDataQPCount(peer)
  // NOTE: Requires NIC-aware mode (nicQpnPairMap must be populated).
  // Use send() for legacy qpnPairMap mode.
  std::unique_ptr<Message> sendWithQPIndex(
      uint32_t toRank,
      uint32_t size,
      uint32_t chunkId,
      uint32_t qpIndex,
      uint64_t sendDelayNs = 0,
      bool writeImm = false,
      bool write = false,
      uint32_t writeImmData = 0,
      uint64_t dataPtr = 0);

  // Round-robin recv across all available QPs for the given peer.
  // Uses qpIndex to select which QP to use (typically chunk ID or counter).
  // The actual QP is selected as: qpIndex % getTotalDataQPCount(peer)
  // NOTE: Requires NIC-aware mode (nicQpnPairMap must be populated).
  // Use recv() for legacy qpnPairMap mode.
  std::unique_ptr<Message> recvWithQPIndex(
      uint32_t fromRank,
      uint32_t size,
      uint32_t chunkId,
      uint32_t qpIndex,
      uint64_t sendDelayNs = 0,
      bool isRead = false,
      uint64_t dataPtr = 0);

  // Check if NIC-aware mode is active (nicQpnPairMap is populated)
  bool isNicAwareModeActive() const;

  /**
   * Rejects a workload config field that must be at least 1 but is 0, with
   * the message form the config fix-up uses: "Workload <name>: <type> <field>
   * must be at least 1, got 0".
   *
   * @param workloadType  Workload type named in the message, e.g. "Incast".
   * @param field         Config field named in the message, e.g. "numChunks".
   * @param value         The field's configured value.
   *
   * @throw std::invalid_argument if value is 0.
   */
  void requirePositiveField(
      const std::string& workloadType,
      const std::string& field,
      uint32_t value) const;

  /**
   * Rejects a workload with fewer than two ranks, in requirePositiveField's
   * message form: "Workload <name>: <type> numEndpoints must be at least 2,
   * got <numEndpoints>".
   *
   * @param workloadType  Workload type named in the message, e.g. "Ring".
   *
   * @throw std::invalid_argument if numEndpoints is below 2.
   */
  void requireAtLeastTwoRanks(const std::string& workloadType) const;

  uint32_t getDataQPCountFromTotal(size_t totalQPs) const;

  // Populate qpSelectionByPeer_ once so the QPN accessors avoid a
  // std::map<RankPair> lookup per packet. Owner ranks (parent_ == nullptr)
  // only.
  void buildQpSelection();
  // The memoized nicQpnPairMap entry for (rank_, peer); nullptr if absent.
  const packsim::workload::thrift::NicPairToQPNList* qpnsByNicPairFor(
      uint32_t peer) const;

  // Composite children delegate transport operations to their root owner.
  const packsim::workload::thrift::Workload& getRoutingWorkloadConfig() const;
  bool hasChannelManagerInHierarchy() const;

  std::unique_ptr<Message> sendWithQPN(
      uint32_t toRank,
      uint32_t size,
      uint32_t chunkId,
      uint32_t localQPN,
      uint32_t remoteQPN,
      uint32_t endpointId = 0,
      uint32_t dstEndpointId = 0,
      uint64_t sendDelayNs = 0,
      bool writeImm = false,
      bool write = false,
      uint32_t writeImmData = 0,
      uint32_t sendOrWriteData = 0,
      uint64_t dataPtr = 0);

  std::unique_ptr<Message> recvWithQPN(
      uint32_t fromRank,
      uint32_t size,
      uint32_t chunkId,
      uint32_t localQPN,
      uint32_t remoteQPN,
      uint32_t endpointId = 0,
      uint32_t dstEndpointId = 0,
      uint64_t sendDelayNs = 0,
      bool isRead = false,
      uint64_t dataPtr = 0);

  uint64_t getTimeNs();

  /**
   * Check if the peer GPU is in the same NV domain as the current GPU.
   * Membership comes only from `Workload.rankToNvDomain`: when that map is
   * unset or empty, this is false for every peer, this rank included.
   *
   * @param peerRank           Rank Id of the peer GPU
   * @return                   True if the peer GPU is in the same NV domain.
   * @throw std::runtime_error if the map is non-empty, `peerRank` is not this
   *                           rank, and either rank is missing from the map.
   */
  bool isInSameNvDomain(uint32_t peerRank) const;

  WorkloadRankParams::OnRankCompleteCallback onRankComplete_;
  WorkloadRankParams::SubmitMessageFn sendMessageFn_;
  WorkloadRankParams::SubmitMessageFn recvMessageFn_;

  Tracer::TraceEvent* traceEvent_{};

  std::unique_ptr<ChannelManager> channelManager_;

  // A peer's NIC-pair -> QPN-list map and its total data-QP count, resolved
  // once from nicQpnPairMap so the hot send/recv path skips the
  // std::map<RankPair> find.
  struct QpSelection {
    const packsim::workload::thrift::NicPairToQPNList* qpnsByNicPair{nullptr};
    uint32_t totalDataQpCount{0};
  };
  // Resolved QpSelection for each peer rank.
  folly::F14FastMap<uint32_t, QpSelection> qpSelectionByPeer_;

 public:
  void setOnRequestAckedCallback(
      const WorkloadRankParams::OnRequestAckedCallback& cb);
  void setOnRequestReceivedCallback(
      const WorkloadRankParams::OnRequestReceivedCallback& cb);
  SimContext* const simCtx_;
  const packsim::workload::thrift::Workload* workCfg_;
  WorkloadRank* parent_{nullptr};
  folly::F14FastMap<uint32_t, std::deque<std::unique_ptr<Message>>>
      sendNotifyMessages_;
  folly::F14FastMap<uint32_t, std::deque<std::unique_ptr<Message>>>
      recvNotifyMessages_;
  folly::F14FastMap<uint32_t, uint32_t> notifyAckedCount_;
  folly::F14FastMap<uint32_t, uint32_t> notifyRecvCount_;
  std::string workloadName_;
  uint32_t workloadId_;
  uint32_t numRanks_;
  uint32_t rank_;
  uint64_t startTimeNs_{0};
  uint64_t endTimeNs_{};
  // for random splay, specify Max splay time
  uint64_t startTimeRandomSplayNs_{100};
  // for explicit splay
  uint64_t startTimeExplicitSplayNs_{UINT64_MAX};
  // actual start time delay, assigned based on the above two values
  uint64_t startTimeDelayNs_{0};
  uint32_t iteration_{1};

  // Raise the request-acked / request-received events. Public because a
  // composite rank delivers these to the child it is currently driving; the
  // members below are read nowhere but here.
  void onRequestAcked(Message* msg);
  void onRequestReceived(Message* msg);

 protected:
  WorkloadRankParams::OnRequestAckedCallback onRequestAcked_;
  WorkloadRankParams::OnRequestReceivedCallback onRequestReceived_;

 public:
  FOLLY_GTEST_FRIEND_TEST(
      WorkloadRankGetDataQPNsTest,
      GetDataQPNsReturnsCorrectQPNPair);
  FOLLY_GTEST_FRIEND_TEST(
      WorkloadRankGetDataQPNsTest,
      GetDataQPNsReturnsDifferentPeerQPNs);
  FOLLY_GTEST_FRIEND_TEST(
      WorkloadRankGetDataQPNsTest,
      NicAwareGetDataQPNsReturnsCorrectQPNPair);
  FOLLY_GTEST_FRIEND_TEST(
      WorkloadRankGetDataQPNsTest,
      NicAwareGetDataQPNsReturnsTheRequestedNicPairsQPN);
  FOLLY_GTEST_FRIEND_TEST(
      WorkloadRankGetDataQPNsTest,
      GetTotalDataQPCountReturnsCorrectCount);
  FOLLY_GTEST_FRIEND_TEST(
      WorkloadRankGetDataQPNsTest,
      IsNicAwareModeActiveReturnsTrue);
  FOLLY_GTEST_FRIEND_TEST(
      WorkloadRankGetDataQPNsTest,
      GetDataQPNByIndexThrowsWhenOutOfRange);
};

} // namespace packsim
