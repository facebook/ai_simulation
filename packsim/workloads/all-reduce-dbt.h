/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <vector>

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

struct BinaryMessageTree {
  uint32_t root{};
  std::vector<uint32_t> parent;
  std::vector<std::vector<uint32_t>> children;
  std::vector<uint32_t> leaves;
};

const BinaryMessageTree& buildDoubleBinaryTree(uint32_t N, bool oddLeaves);

class AllReduceDbtRank : public WorkloadRank {
 public:
  explicit AllReduceDbtRank(const WorkloadRankParams& params);
  void run() override;
  void requestReceived(Message* msg);
  void requestAcked(Message* msg);
  uint64_t getExpectedBytes() override;
  bool complete() override;
  void progress() override;

 protected:
  void postNotifies();
  void postInitialSends();
  struct TreeInfo {
    bool amRoot{};
    uint32_t parent{};
    std::vector<uint32_t> children;
    folly::F14FastMap<uint32_t, std::deque<std::unique_ptr<Message>>> sends;
    folly::F14FastMap<uint32_t, std::deque<std::unique_ptr<Message>>> recvs;
    folly::F14FastMap<uint32_t, uint32_t> upChunkRecvCounts;
  };
  TreeInfo oddInfo_;
  TreeInfo evenInfo_;

  std::vector<uint32_t> chunkSizes_;
  folly::F14FastSet<uint32_t> allPeers_;
  folly::F14FastSet<uint32_t> notifyPending_;

  uint64_t completeSends_{0};
  uint64_t completeRecvs_{0};
  uint64_t expectedSends_{0};
  uint64_t expectedRecvs_{0};
};
} // namespace packsim
