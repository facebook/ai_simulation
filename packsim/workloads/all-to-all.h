/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

#include <folly/container/F14Map.h>
#include <folly/container/vector_bool.h>
#include <folly/portability/GTestProd.h>

// @oss-disable
#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

class AllToAllRank : public WorkloadRank {
  friend class WorkloadRankTestHarness;

 public:
  explicit AllToAllRank(const WorkloadRankParams& params);
  ~AllToAllRank() override;

  // Rule of Five: explicitly define move/copy operations
  AllToAllRank(const AllToAllRank&) = delete;
  AllToAllRank& operator=(const AllToAllRank&) = delete;
  AllToAllRank(AllToAllRank&&) = delete;
  AllToAllRank& operator=(AllToAllRank&&) = delete;

  void run() override;
  virtual void requestReceived(Message* msg);
  virtual void requestAcked(Message* msg);
  uint64_t getExpectedBytes() override;
  bool complete() override;
  void progress() override;

 protected:
  virtual void sendNotifies();
  virtual uint32_t getSendSize(uint32_t /* dstRank */, uint32_t /* chunk */)
      const {
    return messageSize_;
  }

  virtual uint64_t getSendDelay(uint32_t /* dstRank */, uint32_t /* chunkId */)
      const {
    // Send delay is always 0, unless it's a vectorized AllToAll communication
    // where this function is overridden
    return 0;
  }

  virtual uint32_t getRecvSize(uint32_t /* srcRank */, uint32_t /* chunk */)
      const {
    return messageSize_;
  }

  /**
   * Check if workload is completed and either finalize or continue progress.
   * Helper function in requestReceived/requestAcked.
   */
  void checkCompletionAndContinue();

  /**
   * Check if the peer GPU is in the same rail as the current GPU.
   *
   * @param peerRank           Rank Id of the peer GPU
   * @return                   True if the peer GPU is in the same rail.
   */
  bool isInSameRail(uint32_t peerRank) const;

  /**
   * Whether this rank sends or receives any bytes. `run()` completes a rank
   * without traffic at once, without exchanging notifies, and
   * `getExpectedBytes()` is 0 for it.
   *
   * @return                   True if a peer the rank exchanges messages with
   *                           has a non-zero send or receive size for a chunk.
   */
  bool hasTraffic() const;

  /**
   * Whether this rank exchanges nothing with a peer: the peer is this rank, is
   * in its NV domain per `Workload.rankToNvDomain`, or, with PXN, is on
   * another rail.
   *
   * @param peerRank           Rank Id of the peer GPU
   * @return                   True if the rank posts no receive from, sends
   *                           nothing to and notifies nothing to the peer.
   */
  bool isPeerSkipped(uint32_t peerRank) const;

  std::vector<folly::F14FastMap<uint32_t, std::unique_ptr<Message>>> sendMsgs_;
  std::vector<folly::F14FastMap<uint32_t, std::unique_ptr<Message>>> recvMsgs_;

  uint32_t messageSize_;
  uint32_t numChunks_;
  uint32_t maxChunksInFlight_;
  uint32_t remRecvs_{0};
  uint32_t remSends_{0};
  bool notifiesSent_{false};
  std::vector<uint32_t> nextChunk_;
  // Per-peer QP index for send-side round-robin, persistent across progress()
  std::vector<uint32_t> sendQpNumToUse_;
  folly::vector_bool<> notifyRcvd_;
  // Whether to use PXN for rail-based topology
  bool usePxn_ = false;
  // @oss-disable
  // # of GPUs per NV domain. Affects traffic only with PXN, where it groups
  // ranks into rails and scales each message.
  uint32_t numGpusPerNvDomain_;

  // =============== Friend Tests ===============
  FOLLY_GTEST_FRIEND_TEST(AllToAllWorkloadPxnRankTest, Validate);
  FOLLY_GTEST_FRIEND_TEST(
      AllToAllRankNvDomainTest,
      TestUnsetMappingIgnoresNumGpusPerNvDomain);
  FOLLY_GTEST_FRIEND_TEST(
      AllToAllRankNvDomainTest,
      TestExplicitNvDomainMapping);
  FOLLY_GTEST_FRIEND_TEST(
      AllToAllRankNvDomainTest,
      TestExplicitMappingWithMissingRank);
};

} // namespace packsim
