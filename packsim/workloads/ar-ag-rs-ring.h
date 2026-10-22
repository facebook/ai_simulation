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
#include <vector>

#include <folly/container/F14Map.h>
#include <folly/portability/GTestProd.h>

#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

class RingRank : public WorkloadRank {
 public:
  /**
   * Construct a RingRank for ring-based collective communication.
   *
   * @param params    Workload rank parameters including rank, config,
   *                  and callbacks
   *
   * @throw           std::invalid_argument for a workload that is not
   *                  all-reduce, all-gather or reduce-scatter, a numChunks of
   *                  0, a NIC selection mode outside the enum, or a workload
   *                  all-NIC mode cannot serve (see initializeNicPaths())
   */
  explicit RingRank(const WorkloadRankParams& params);

  /**
   * Start the ring collective operation for this rank.
   */
  void run() override;

  /**
   * Handle a received message from the left neighbor.
   *
   * @param msg    The received message
   */
  void requestReceived(Message* msg);

  /**
   * Handle acknowledgment of a sent message to the right neighbor.
   *
   * @param msg    The acknowledged message
   */
  void requestAcked(Message* msg);

  /**
   * Get the expected number of bytes to receive from the network.
   *
   * Returns 0 for EXIT and INTERIOR ranks whose left neighbor is in the
   * same NV domain (skipRecv_=true), since those ranks receive data over
   * NVLink instead of the network.
   *
   * Example: An EXIT rank (rank 3 in a 2-domain ring) has skipRecv_=true
   * because its left neighbor (rank 2) is in the same NV domain, so it
   * returns 0 — all its input arrives via NVLink, not the network.
   *
   * @return    Expected bytes; 0 if this rank skips receiving
   */
  uint64_t getExpectedBytes() override;

  /**
   * Check whether this rank has completed the collective operation.
   *
   * @return    true if all sends and receives are done
   */
  bool complete() override;

 protected:
  struct RingNicPath {
    // Queue pair number on the sending endpoint.
    uint32_t localQpn;
    // Queue pair number on the receiving endpoint.
    uint32_t remoteQpn;
    // Sending endpoint index within the source rank.
    uint32_t endpointId;
    // Receiving endpoint index within the destination rank.
    uint32_t dstEndpointId;
  };
  using RingNicPathCandidates = std::vector<std::vector<RingNicPath>>;

  /**
   * Attempt to send queued chunks up to the max in-flight window.
   *
   * @param begin    If true, record the start time
   */
  void trySendChunks(bool begin = false);

  // Returns true if any rank in this workload has NV domain skipping active.
  bool hasNvDomainSkipping() const;

  /**
   * Build deterministic NIC paths for a sender-direction rank pair.
   *
   * @param srcRank    Source rank of the ring edge.
   * @param dstRank    Destination rank of the ring edge.
   *
   * @return           One compatible path per source NIC, indexed by source
   *                   NIC.
   * @throw            std::invalid_argument when the edge cannot use every
   *                   source and destination NIC exactly once.
   */
  std::vector<RingNicPath> buildNicPaths(uint32_t srcRank, uint32_t dstRank)
      const;

  /**
   * Read and validate the number of NICs on a ring edge.
   *
   * @param srcRank    Source rank of the ring edge.
   * @param dstRank    Destination rank of the ring edge.
   *
   * @return           The common source and destination NIC count.
   * @throw            std::invalid_argument when endpoint mappings are absent,
   *                   empty, or have different NIC counts.
   */
  size_t getNicCountForEdge(uint32_t srcRank, uint32_t dstRank) const;

  /**
   * Collect all configured QP-backed path candidates for a ring edge.
   *
   * @param srcRank    Source rank of the ring edge.
   * @param dstRank    Destination rank of the ring edge.
   * @param nicCount   Validated NIC count for both ranks.
   *
   * @return           Candidate paths grouped by source NIC.
   * @throw            std::invalid_argument when the QP map is missing or
   *                   contains an invalid NIC pair.
   */
  RingNicPathCandidates buildNicPathCandidates(
      uint32_t srcRank,
      uint32_t dstRank,
      size_t nicCount) const;

  /**
   * Select a deterministic complete matching from NIC path candidates.
   *
   * @param candidatesBySrcNic    Candidate paths grouped by source NIC.
   * @param srcRank               Source rank used in validation errors.
   * @param dstRank               Destination rank used in validation errors.
   *
   * @return                      One path per source NIC, indexed by source
   *                              NIC.
   * @throw                       std::invalid_argument when no complete
   *                              matching exists.
   */
  static std::vector<RingNicPath> selectNicPathMatching(
      const RingNicPathCandidates& candidatesBySrcNic,
      uint32_t srcRank,
      uint32_t dstRank);

  /**
   * Set the chunk size, chunk counts and in-flight window from the workload's
   * collective config, and for all-reduce and all-gather the NIC selection
   * mode.
   *
   * @throw std::invalid_argument for a workload that is not all-reduce,
   *        all-gather or reduce-scatter, or a numChunks of 0.
   */
  void initFromCollectiveConfig();

  /**
   * Set the chunk size, chunk counts and in-flight window from a ring
   * collective's config.
   *
   * @param cfg       The all-reduce, all-gather or reduce-scatter config.
   * @param numSteps  Ring steps the collective takes: 2 * (numRanks_ - 1) for
   *                  all-reduce, numRanks_ - 1 for all-gather and
   *                  reduce-scatter.
   *
   * @throw           std::invalid_argument if the config's numChunks or
   *                  maxChunksInFlight is 0.
   */
  template <typename TCollectiveCfg>
  void initChunks(const TCollectiveCfg& cfg, uint64_t numSteps);

  /**
   * Set up the NIC paths the NIC selection mode needs: none for SINGLE_NIC,
   * one per NIC for ALL_AVAILABLE_NICS.
   *
   * @throw std::invalid_argument for a mode outside the enum, or when
   *        initializeNicPaths() rejects the workload.
   */
  void applyNicSelectionMode();

  /**
   * Validate and initialize the paths used by this rank.
   *
   * @throw std::invalid_argument when all-NIC mode is incompatible with the
   *        workload's routing or pipeline settings.
   */
  void initializeNicPaths();

  /**
   * Post one send using the selected NIC path.
   *
   * @param chunkId    Ring chunk identifier.
   *
   * @return           The posted send message.
   */
  std::unique_ptr<Message> sendChunk(uint32_t chunkId);

  /**
   * Post one receive using the selected NIC path.
   *
   * @param chunkId    Ring chunk identifier.
   *
   * @return           The posted receive message.
   */
  std::unique_ptr<Message> recvChunk(uint32_t chunkId);

  bool started_{false};
  uint32_t left_;
  uint32_t right_;
  // Size of one chunk.
  uint32_t chunkSizeBytes_{0};
  // Chunks per ring step: the collective config's numChunks.
  uint32_t localChunks_{0};
  // Chunks sent over the whole collective: localChunks_ per step, over
  // 2 * (numRanks_ - 1) steps for all-reduce and numRanks_ - 1 otherwise.
  uint32_t totalChunks_{0};
  uint32_t sentChunks_{0};
  uint32_t chunksInFlight_{0};
  uint32_t maxChunksInFlight_{0};
  uint32_t queuedChunks_{0};
  uint32_t postedRecvs_{0};
  folly::F14FastMap<uint32_t, std::unique_ptr<Message>> sendMsgs_;
  folly::F14FastMap<uint32_t, std::unique_ptr<Message>> recvMsgs_;
  // Policy for assigning ring chunks to NIC paths.
  workload::thrift::RingNicSelectionMode nicSelectionMode_{
      workload::thrift::RingNicSelectionMode::SINGLE_NIC};
  // Sender-direction NIC paths indexed by source NIC.
  std::vector<RingNicPath> sendNicPaths_;
  // Receiver-direction NIC paths indexed by the upstream source NIC.
  std::vector<RingNicPath> recvNicPaths_;
  // Skip sending to right neighbor when it is in the same NV domain,
  // as intra-domain data transfer is assumed to happen over NVLink.
  // Rank roles: ENTRY (skipSend only), EXIT (skipRecv only),
  //             INTERIOR (both skip), FULL (neither skip).
  bool skipSend_{false};
  // Skip receiving from left neighbor when it is in the same NV domain
  bool skipRecv_{false};

  // =============== Friend Tests ===============
  FOLLY_GTEST_FRIEND_TEST(RingRankNvDomainTest, NeighborsSkipSameNvDomain);
  FOLLY_GTEST_FRIEND_TEST(RingRankNvDomainTest, AllDifferentDomainsNoSkipping);
  FOLLY_GTEST_FRIEND_TEST(RingRankNvDomainTest, EntryRankPostsRecvsOnly);
  FOLLY_GTEST_FRIEND_TEST(RingRankNvDomainTest, ExitRankQueuesAllChunksNoRecvs);
  FOLLY_GTEST_FRIEND_TEST(
      RingRankNvDomainTest,
      RequestReceivedSkipsSendForEntryRank);
  FOLLY_GTEST_FRIEND_TEST(
      RingRankNvDomainTest,
      TrySendChunksSendsUpToMaxInFlight);
  FOLLY_GTEST_FRIEND_TEST(
      RingRankNvDomainTest,
      RequestReceivedFullRankQueuesAndPostsRecv);
  FOLLY_GTEST_FRIEND_TEST(
      RingRankNvDomainTest,
      RequestAckedCompletesWhenAllSendsDone);
  FOLLY_GTEST_FRIEND_TEST(RingRankNvDomainTest, RequestAckedTriggersMoreSends);
  FOLLY_GTEST_FRIEND_TEST(
      RingRankNvDomainTest,
      RequestAckedRemovesMatchingOutOfOrderMessage);
  FOLLY_GTEST_FRIEND_TEST(
      RingRankNvDomainTest,
      RequestReceivedRemovesMatchingOutOfOrderMessage);
  FOLLY_GTEST_FRIEND_TEST(
      RingRankNvDomainTest,
      EntryRankPrePostsAllRecvsWithNvDomain);
  FOLLY_GTEST_FRIEND_TEST(
      RingRankNvDomainTest,
      SingleRankWithNvDomainsCompletesImmediately);
};

} // namespace packsim
