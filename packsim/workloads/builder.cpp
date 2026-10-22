/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/builder.h"

#include <cmath>
#include <cstdint>
#include <functional>
#include <memory>
#include <set>
#include <stdexcept>
#include <vector>

#include <fmt/core.h>
#include <folly/Utility.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/workload_types.h"
// @oss-disable
#include "packsim/workloads/ag-rs-recursive.h"
#include "packsim/workloads/all-reduce-dbt.h"
#include "packsim/workloads/all-reduce-rhd.h"
#include "packsim/workloads/all-to-all-push-pull.h"
#include "packsim/workloads/all-to-all-vec.h"
#include "packsim/workloads/all-to-all.h"
#include "packsim/workloads/ar-ag-rs-ring.h"
#include "packsim/workloads/batch.h"
#include "packsim/workloads/compute.h"
// @oss-disable
#include "packsim/workloads/incast.h"
#include "packsim/workloads/permutation-push-pull.h"
#include "packsim/workloads/permutation.h"

using packsim::workload::thrift::AllReduceAlgorithm;
using packsim::workload::thrift::WorkloadSpecificConfig;

namespace packsim {

namespace {

std::vector<std::vector<uint32_t>> genA2APeers(const uint32_t numRanks) {
  std::vector<std::vector<uint32_t>> peers{numRanks};
  for (uint32_t i = 0; i < numRanks; i++) {
    peers[i].reserve(numRanks - 1);
    for (uint32_t j = i + 1; j < numRanks; j++) {
      if (i != j) {
        peers[i].emplace_back(j);
      }
    }
  }
  return peers;
}

std::vector<std::vector<uint32_t>> genPermutationPeers(
    const packsim::workload::thrift::Workload& w) {
  const uint32_t numRanks = *w.numEndpoints();
  const auto& dstRanks = *w.config()->permutation()->dstRanks();
  if (dstRanks.size() != numRanks) {
    throw std::invalid_argument(
        fmt::format(
            "Permutation workload '{}' has {} dstRanks entries but {} endpoints",
            *w.name(),
            dstRanks.size(),
            numRanks));
  }
  std::vector<std::vector<uint32_t>> peers{numRanks};
  for (uint32_t i = 0; i < numRanks; i++) {
    peers[i].reserve(2);
  }
  for (uint32_t i = 0; i < numRanks; i++) {
    const auto dst = dstRanks[i];
    if (dst >= numRanks) {
      throw std::invalid_argument(
          fmt::format(
              "Permutation workload '{}' has dstRanks[{}] = {}, outside [0, {})",
              *w.name(),
              i,
              dst,
              numRanks));
    }
    peers[i].emplace_back(dst);
    peers[dst].emplace_back(i);
  }
  return peers;
}

std::vector<std::vector<uint32_t>> genRingPeers(const uint32_t numRanks) {
  std::vector<std::vector<uint32_t>> peers{numRanks};
  for (uint32_t i = 0; i < numRanks; i++) {
    peers[i].reserve(2);
    const auto right = (i + 1) % numRanks;
    peers[i].emplace_back(right);
    peers[right].emplace_back(i);
  }
  return peers;
}

std::vector<std::vector<uint32_t>> genDBTPeers(const uint32_t numRanks) {
  std::vector<std::set<uint32_t>> peerSets{numRanks};
  const BinaryMessageTree& even =
      buildDoubleBinaryTree(numRanks, /*oddLeaves=*/false);
  const BinaryMessageTree& odd =
      buildDoubleBinaryTree(numRanks, /*oddLeaves=*/true);

  for (uint32_t rank = 0; rank < numRanks; rank++) {
    for (const auto& tree : {even, odd}) {
      const auto parent = tree.parent.at(rank);
      if (parent < numRanks) {
        peerSets[rank].emplace(parent);
        peerSets[parent].emplace(rank);
      }
      for (const auto child : tree.children.at(rank)) {
        peerSets[rank].emplace(child);
        peerSets[child].emplace(rank);
      }
    }
  }

  std::vector<std::vector<uint32_t>> peers{numRanks};
  for (uint32_t rank = 0; rank < numRanks; rank++) {
    peers[rank].reserve(peerSets[rank].size());
    for (const auto& peer : peerSets[rank]) {
      peers[rank].emplace_back(peer);
    }
  }

  return peers;
}

std::vector<std::vector<uint32_t>> genRHDPeers(const uint32_t numRanks) {
  std::vector<std::vector<uint32_t>> peers{numRanks};
  const uint32_t nSteps = log2(numRanks);
  if ((1U << nSteps) != numRanks) {
    throw std::invalid_argument("Number of ranks must be a power of 2");
  }

  for (uint32_t rank = 0; rank < numRanks; rank++) {
    peers[rank].reserve(nSteps);
  }
  for (uint32_t rank = 0; rank < numRanks; rank++) {
    for (uint32_t step = 0; step < nSteps; step++) {
      const uint32_t dist = 1U << step;
      const uint32_t pos = (rank / dist) % 2;
      // Only do left-hand nodes in each pair
      if (pos == 0) {
        const uint32_t peer = rank + dist;
        peers[rank].emplace_back(peer);
        peers[peer].emplace_back(rank);
      }
    }
  }

  return peers;
}

std::vector<std::vector<uint32_t>> genIncastPeers(
    const uint32_t numRanks,
    const uint32_t numSrcs) {
  std::vector<std::vector<uint32_t>> peers{numRanks};
  peers[0].reserve(numRanks - 1);
  for (uint32_t i = 1; i < numSrcs + 1; i++) {
    peers[0].emplace_back(i);
    peers[i].emplace_back(0);
  }
  return peers;
}

std::vector<std::vector<uint32_t>> genArAgRsPeers(
    const uint32_t numRanks,
    const packsim::workload::thrift::AllReduceAlgorithm alg) {
  switch (alg) {
    case packsim::workload::thrift::AllReduceAlgorithm::RING:
      return genRingPeers(numRanks);
    case packsim::workload::thrift::AllReduceAlgorithm::DOUBLE_BINARY_TREE:
      return genDBTPeers(numRanks);
    case packsim::workload::thrift::AllReduceAlgorithm::RECURSIVE_MSG_OPT:
    case packsim::workload::thrift::AllReduceAlgorithm::RECURSIVE_DIST_OPT:
      return genRHDPeers(numRanks);
    case packsim::workload::thrift::AllReduceAlgorithm::UNKNOWN:
      break;
  }
  // Also reached by a value outside the enum.
  throw std::invalid_argument("Unsupported algorithm");
}

std::vector<std::vector<uint32_t>> genBatchPeers(
    const packsim::workload::thrift::Workload& w) {
  const auto N = *w.numEndpoints();
  std::vector<std::set<uint32_t>> peers(N);
  for (const auto& child : *w.config()->batch()->children()) {
    const auto childPeers = getRankPeers(child);
    // Compute children have no communication peers.
    if (childPeers.empty()) {
      continue;
    }
    for (uint32_t i = 0; i < N; i++) {
      for (const auto& peer : childPeers.at(i)) {
        peers[i].emplace(peer);
      }
    }
  }
  std::vector<std::vector<uint32_t>> ret(N);
  for (uint32_t i = 0; i < N; i++) {
    ret[i].reserve(peers[i].size());
    for (const auto& peer : peers[i]) {
      ret[i].emplace_back(peer);
    }
  }
  return ret;
}

} // namespace

std::unique_ptr<WorkloadRank> buildWorkloadRank(
    SimContext* simCtx,
    const uint32_t workloadId,
    const packsim::workload::thrift::Workload& w,
    const uint32_t rank,
    const std::function<void(Message*)>& smcb,
    const std::function<void(Message*)>& rmcb,
    const std::function<void(uint32_t, uint32_t)>& rccb,
    WorkloadRank* parent) {
  const WorkloadRankParams params{
      .simCtx = simCtx,
      .workloadId = workloadId,
      .workCfg = w,
      .rank = rank,
      .smcb = smcb,
      .rmcb = rmcb,
      .rccb = rccb,
      .parent = parent,
      .factory = buildWorkloadRank,
  };
  switch (w.config()->getType()) {
    case WorkloadSpecificConfig::Type::allToAll: {
      const auto& a2aCfg = *w.config()->allToAll();
      const bool hasVectorizedSizes = a2aCfg.vectorizedSizesBytes()->size() > 0;
      const bool usesPushPullMode =
          *a2aCfg.workloadMode() == workload::thrift::WorkloadMode::PUSH ||
          *a2aCfg.workloadMode() == workload::thrift::WorkloadMode::PULL;

      if (hasVectorizedSizes && usesPushPullMode) {
        throw std::invalid_argument(
            "AllToAll workload cannot have both vectorizedSizesBytes and "
            "non-SENDRCV workloadMode set. PUSH/PULL modes are not supported "
            "for vectorized AllToAll workloads.");
      }

      if (hasVectorizedSizes) {
        return std::make_unique<VectorizedAllToAllRank>(params);
      } else if (usesPushPullMode) {
        return std::make_unique<AllToAllPushPullRank>(params);
      } else {
        return std::make_unique<AllToAllRank>(params);
      }
    }
    case WorkloadSpecificConfig::Type::incast:
      return std::make_unique<IncastRank>(params);
    case WorkloadSpecificConfig::Type::permutation: {
      const auto& permCfg = *w.config()->permutation();
      const bool usesPushPullMode =
          *permCfg.workloadMode() == workload::thrift::WorkloadMode::PUSH ||
          *permCfg.workloadMode() == workload::thrift::WorkloadMode::PULL;
      if (usesPushPullMode) {
        return std::make_unique<PermutationPushPullRank>(params);
      }
      return std::make_unique<PermutationRank>(params);
    }
    case WorkloadSpecificConfig::Type::allReduce:
      switch (*w.config()->allReduce()->algorithm()) {
        case AllReduceAlgorithm::RING:
          return std::make_unique<RingRank>(params);
        case AllReduceAlgorithm::DOUBLE_BINARY_TREE:
          return std::make_unique<AllReduceDbtRank>(params);
        case AllReduceAlgorithm::RECURSIVE_MSG_OPT:
        case AllReduceAlgorithm::RECURSIVE_DIST_OPT:
          return std::make_unique<ArRecursiveRank>(params);
        case AllReduceAlgorithm::UNKNOWN:
          break;
      }
      // Also reached by a value outside the enum.
      throw std::invalid_argument("Unsupported all-reduce algorithm for ranks");

    case WorkloadSpecificConfig::Type::allGather:
      switch (*w.config()->allGather()->algorithm()) {
        case AllReduceAlgorithm::RECURSIVE_MSG_OPT:
        case AllReduceAlgorithm::RECURSIVE_DIST_OPT:
          return std::make_unique<AgRsRecursiveRank>(params);
        case AllReduceAlgorithm::RING:
          return std::make_unique<RingRank>(params);
        case AllReduceAlgorithm::DOUBLE_BINARY_TREE:
        case AllReduceAlgorithm::UNKNOWN:
          break;
      }
      // Also reached by a value outside the enum.
      throw std::invalid_argument("Unsupported all-gather algorithm for ranks");
    case WorkloadSpecificConfig::Type::reduceScatter:
      switch (*w.config()->reduceScatter()->algorithm()) {
        case AllReduceAlgorithm::RECURSIVE_MSG_OPT:
        case AllReduceAlgorithm::RECURSIVE_DIST_OPT:
          return std::make_unique<AgRsRecursiveRank>(params);
        case AllReduceAlgorithm::RING:
          return std::make_unique<RingRank>(params);
        case AllReduceAlgorithm::DOUBLE_BINARY_TREE:
        case AllReduceAlgorithm::UNKNOWN:
          break;
      }
      // Also reached by a value outside the enum.
      throw std::invalid_argument(
          "Unsupported reduce-scatter algorithm for ranks");
    // @oss-disable
      // @oss-disable
          // @oss-disable
    case WorkloadSpecificConfig::Type::batch:
      return std::make_unique<BatchRank>(params);
    case WorkloadSpecificConfig::Type::compute:
      return std::make_unique<ComputeRank>(params);
    // @oss-disable
      // @oss-disable
    case WorkloadSpecificConfig::Type::__EMPTY__:
    // @oss-disable
    // @oss-disable
      break;
  }
  throw std::invalid_argument(
      fmt::format(
          "Unsupported workload type: {}",
          folly::to_underlying(w.config()->getType())));
}

std::vector<std::vector<uint32_t>> getRankPeers(
    const packsim::workload::thrift::Workload& w) {
  switch (w.config()->getType()) {
    case WorkloadSpecificConfig::Type::allToAll:
      return genA2APeers(*w.numEndpoints());
    case WorkloadSpecificConfig::Type::incast:
      return genIncastPeers(
          *w.numEndpoints(), IncastRank::resolveNumSources(w));
    case WorkloadSpecificConfig::Type::permutation:
      return genPermutationPeers(w);
    case WorkloadSpecificConfig::Type::allReduce:
      return genArAgRsPeers(
          *w.numEndpoints(), *w.config()->allReduce()->algorithm());
    case WorkloadSpecificConfig::Type::allGather:
      return genArAgRsPeers(
          *w.numEndpoints(), *w.config()->allGather()->algorithm());
    case WorkloadSpecificConfig::Type::reduceScatter:
      return genArAgRsPeers(
          *w.numEndpoints(), *w.config()->reduceScatter()->algorithm());
    case WorkloadSpecificConfig::Type::batch:
      return genBatchPeers(w);
    case WorkloadSpecificConfig::Type::compute:
      return {};
    // @oss-disable
      // @oss-disable
    case WorkloadSpecificConfig::Type::__EMPTY__:
    // @oss-disable
    // @oss-disable
    // @oss-disable
      break;
  }
  throw std::invalid_argument(
      fmt::format(
          "Unsupported workload type: {}",
          folly::to_underlying(w.config()->getType())));
}

} // namespace packsim
