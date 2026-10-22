/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/all-reduce-dbt.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/Conv.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <folly/logging/xlog.h>

namespace packsim {

const BinaryMessageTree& buildDoubleBinaryTree(
    const uint32_t N,
    const bool oddLeaves) {
  static folly::
      F14FastMap<std::pair<uint32_t, bool>, std::unique_ptr<BinaryMessageTree>>
          cache;
  const auto cacheKey = std::make_pair(N, oddLeaves);
  const auto it = cache.find(cacheKey);
  if (it != cache.end()) {
    return *it->second;
  }

  auto ptr = std::make_unique<BinaryMessageTree>();
  auto& tree = *ptr;
  cache[cacheKey] = std::move(ptr);

  tree.parent.resize(N, UINT32_MAX);
  tree.children.resize(N);

  uint32_t rootChild;
  uint32_t k = N / 2;

  if (oddLeaves) {
    tree.root = 0;
    rootChild = k;
  } else {
    tree.root = N - 1;
    rootChild = tree.root - k;
  }

  tree.parent.at(tree.root) = UINT32_MAX;
  tree.children.at(tree.root).emplace_back(rootChild);
  tree.parent.at(rootChild) = tree.root;

  std::vector<uint32_t> nodes{rootChild};

  while (k > 1) {
    k /= 2;
    std::vector<uint32_t> newNodes;
    newNodes.reserve(nodes.size() * 2);
    for (const auto& node : nodes) {
      const std::vector<uint32_t> children{node - k, node + k};
      for (const auto& child : children) {
        tree.children.at(node).emplace_back(child);
        tree.parent.at(child) = node;
        newNodes.emplace_back(child);
      }
    }
    nodes = newNodes;
  }
  tree.leaves = nodes;
  return tree;
}

AllReduceDbtRank::AllReduceDbtRank(const WorkloadRankParams& params)
    : WorkloadRank(params) {
  // A one-rank tree would make its root its own child.
  requireAtLeastTwoRanks("AllReduceDbt");
  // Double binary tree requires power-of-2 number of ranks
  const auto nSteps = static_cast<uint32_t>(log2(numRanks_));
  if ((1u << nSteps) != numRanks_) {
    throw std::invalid_argument(
        fmt::format(
            "AllReduceDbt: numRanks_ ({}) must be a power of 2", numRanks_));
  }

  const auto& cfg = *workCfg_->config()->allReduce();
  const auto numChunks = *cfg.numChunks();
  requirePositiveField("AllReduceDbt", "numChunks", numChunks);
  this->setOnRequestAckedCallback([this](Message* msg) { requestAcked(msg); });
  this->setOnRequestReceivedCallback(
      [this](Message* msg) { requestReceived(msg); });

  for (const bool& oddLeaves : {false, true}) {
    const auto& tree = buildDoubleBinaryTree(numRanks_, oddLeaves);
    TreeInfo& info = oddLeaves ? oddInfo_ : evenInfo_;
    info.amRoot = tree.root == rank_;
    info.parent = tree.parent.at(rank_);
    info.children = tree.children.at(rank_);
    for (const auto& child : info.children) {
      allPeers_.emplace(child);
    }

    if (!info.amRoot) {
      // Account for expected sends up tree
      expectedSends_ += numChunks;
      // Account for expected recvs from parent
      expectedRecvs_ += numChunks;
      allPeers_.emplace(info.parent);
    }
    // Account for expected sends down tree
    expectedSends_ += numChunks * info.children.size();

    // Account for receives from children
    expectedRecvs_ += numChunks * info.children.size();
  }

  if (*cfg.sizeBytes() % 2 != 0) {
    throw std::invalid_argument("AllReduceDbt: sizeBytes must be even");
  }
  const uint32_t treeBytes = folly::to<uint32_t>(*cfg.sizeBytes() / 2);
  const auto chunkSizeBytes = treeBytes / numChunks;
  chunkSizes_.resize(numChunks, chunkSizeBytes);
  chunkSizes_.at(numChunks - 1) = treeBytes - (numChunks - 1) * chunkSizeBytes;
}

void AllReduceDbtRank::run() {
  traceEvent_ = simCtx_->tracer.startEvent(
      "AllReduceDbtRank", "Workloads", workloadId_, rank_);
  const size_t numChunks = chunkSizes_.size();
  // Receive upwards pass chunks
  for (uint32_t i = 0; i < numChunks; i++) {
    const uint32_t chunkSize = chunkSizes_.at(i);
    for (const auto& child : oddInfo_.children) {
      const uint32_t chunkId = 2 * i + 1; // Odd tree chunks are odd #s
      oddInfo_.recvs[child].emplace_back(recv(child, chunkSize, chunkId));
    }
    for (const auto& child : evenInfo_.children) {
      const uint32_t chunkId = 2 * i; // Even tree chunks are even #s
      evenInfo_.recvs[child].emplace_back(recv(child, chunkSize, chunkId));
    }
  }
  // Receive downwards pass chunks
  for (uint32_t i = 0; i < numChunks; i++) {
    const uint32_t chunkSize = chunkSizes_.at(i);
    if (!oddInfo_.amRoot) {
      const auto parent = oddInfo_.parent;
      const uint32_t chunkId = 2 * i + 1; // Odd tree chunks are odd #s
      oddInfo_.recvs[parent].emplace_back(recv(parent, chunkSize, chunkId));
    }
    if (!evenInfo_.amRoot) {
      const auto parent = evenInfo_.parent;
      const uint32_t chunkId = 2 * i; // Even tree chunks are even #s
      evenInfo_.recvs[parent].emplace_back(recv(parent, chunkSize, chunkId));
    }
  }

  notifyPending_ = allPeers_;
  simCtx_->schedule(
      Time::fromNs(startTimeDelayNs_), &AllReduceDbtRank::postNotifies, this);
}

void AllReduceDbtRank::postNotifies() {
  startTimeNs_ = getTimeNs();
  for (const auto& peer : allPeers_) {
    notify(peer);
  }
}

void AllReduceDbtRank::progress() {
  // Wait until all notifies are received before posting initial sends
  if (notifyPending_.empty()) {
    return;
  }

  folly::F14FastSet<uint32_t> newNotifies;
  for (const auto& rank : notifyPending_) {
    if (checkNotifyReceived(rank)) {
      newNotifies.emplace(rank);
    }
  }

  for (const auto& rank : newNotifies) {
    notifyPending_.erase(rank);
  }

  if (notifyPending_.empty()) {
    postInitialSends();
  }
}

void AllReduceDbtRank::postInitialSends() {
  const size_t numChunks = chunkSizes_.size();
  auto& leafTreeInfo = (rank_ % 2 == 1) ? oddInfo_ : evenInfo_;
  const auto parent = leafTreeInfo.parent;
  const uint32_t chunkOffset = rank_ % 2;
  for (uint32_t i = 0; i < numChunks; i++) {
    const uint32_t chunkSize = chunkSizes_.at(i);
    const uint32_t chunkId = 2 * i + chunkOffset;
    leafTreeInfo.sends[parent].emplace_back(send(parent, chunkSize, chunkId));
  }
}

void AllReduceDbtRank::requestReceived(Message* msg) {
  if (isNotifyMsg(msg)) {
    notifyReceived(msg->srcRank);
    return;
  }
  TreeInfo& treeInfo =
      (msg->connectionMessageId % 2) == 1 ? oddInfo_ : evenInfo_;
  const auto srcRank = msg->srcRank;
  const auto chunkId = msg->connectionMessageId;
  XLOGF(
      DBG,
      "Received message from {}->{} at {}",
      msg->srcRank,
      msg->dstRank,
      rank_);
  auto& recvQ = treeInfo.recvs.at(srcRank);
  if (recvQ.empty()) {
    throw std::runtime_error("Empty recvQ");
  }
  const auto expChunkId = recvQ.front()->connectionMessageId;
  if (chunkId != expChunkId) {
    throw std::runtime_error(
        fmt::format(
            "Received out of order recv CQE: {} != {}", chunkId, expChunkId));
  }

  if (msg->srcRank == treeInfo.parent) {
    for (const auto& child : treeInfo.children) {
      treeInfo.sends[child].emplace_back(send(child, msg->sizeBytes, chunkId));
    }
  } else if (
      std::find(
          treeInfo.children.begin(), treeInfo.children.end(), msg->srcRank) !=
      treeInfo.children.end()) {
    // Message from child, see if we've gotten this chunk from all children
    treeInfo.upChunkRecvCounts[chunkId]++;
    if (treeInfo.upChunkRecvCounts[chunkId] == treeInfo.children.size()) {
      // Received this chunkId from all children; if we're root, send reduced
      // chunk back to all children, otherwise forward to parent
      if (treeInfo.amRoot) {
        for (const auto& child : treeInfo.children) {
          treeInfo.sends[child].emplace_back(
              send(child, msg->sizeBytes, chunkId));
        }
      } else {
        treeInfo.sends[treeInfo.parent].emplace_back(
            send(treeInfo.parent, msg->sizeBytes, chunkId));
      }
    }
  } else {
    throw std::runtime_error(
        fmt::format(
            "Rank {} got chunk {} from unexpected rank {}",
            rank_,
            chunkId,
            msg->srcRank));
  }

  recvQ.pop_front();
  ++completeRecvs_;
  if (complete()) {
    endTimeNs_ = getTimeNs();
    traceEvent_->end(simCtx_->now());
    onRankComplete(workloadId_, rank_);
  }
}

void AllReduceDbtRank::requestAcked(Message* msg) {
  if (isNotifyMsg(msg)) {
    notifyAcked(msg->dstRank);
    return;
  }
  TreeInfo& treeInfo =
      (msg->connectionMessageId % 2) == 1 ? oddInfo_ : evenInfo_;
  auto& sendQ = treeInfo.sends.at(msg->dstRank);
  if (sendQ.empty()) {
    throw std::runtime_error("Empty sendQ");
  }
  if (msg->connectionMessageId != sendQ.front()->connectionMessageId) {
    throw std::runtime_error(
        fmt::format(
            "Received out of order send CQE: {} != {}",
            msg->connectionMessageId,
            sendQ.front()->connectionMessageId));
  }

  sendQ.pop_front();
  ++completeSends_;
  if (complete()) {
    endTimeNs_ = getTimeNs();
    traceEvent_->end(simCtx_->now());
    onRankComplete(workloadId_, rank_);
  }
}

bool AllReduceDbtRank::complete() {
  return (completeSends_ == expectedSends_) &&
      (completeRecvs_ == expectedRecvs_);
}

uint64_t AllReduceDbtRank::getExpectedBytes() {
  const auto& cfg = *workCfg_->config()->allReduce();
  return *cfg.sizeBytes() * (expectedRecvs_ / chunkSizes_.size()) / 2 +
      allPeers_.size() * kNotifyMsgSize;
}

} // namespace packsim
