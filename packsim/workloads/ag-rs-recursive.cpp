/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/ag-rs-recursive.h"

#include <cmath>
#include <cstdint>
#include <functional>
#include <limits>
#include <stdexcept>

#include <fmt/format.h>
#include <folly/Conv.h>

using packsim::workload::thrift::AllReduceAlgorithm;
using packsim::workload::thrift::WorkloadSpecificConfig;

namespace packsim {
AgRsRecursiveRank::AgRsRecursiveRank(const WorkloadRankParams& params)
    : WorkloadRank(params) {
  // With one rank `nSteps_` is 0, but `run()` reads the first step's peer and
  // receive.
  requireAtLeastTwoRanks("Recursive");
  wkldType_ = workCfg_->config()->getType();

  nSteps_ = log2(numRanks_);
  if ((1U << nSteps_) != numRanks_) {
    throw std::invalid_argument("Number of ranks must be a power of 2");
  }

  this->setOnRequestAckedCallback([this](Message* msg) { requestAcked(msg); });
  this->setOnRequestReceivedCallback(
      [this](Message* msg) { requestReceived(msg); });

  uint64_t inBytes, outBytes;
  uint64_t firstStepMsgSize, largestStepMsgSize;
  if (wkldType_ == WorkloadSpecificConfig::Type::allGather) {
    auto agCfg = *workCfg_->config()->allGather();
    algorithm_ = *agCfg.algorithm();
    outBytes = *agCfg.sizeBytes();
    inBytes = outBytes / numRanks_;
    sizeBytes_ = inBytes;
    stepMsgs_ = 1;
    firstStepMsgSize = inBytes;
    // In message optimized mode the message doubles every step, so the last
    // step sends half of the gathered buffer.
    largestStepMsgSize = algorithm_ == AllReduceAlgorithm::RECURSIVE_MSG_OPT
        ? outBytes / 2
        : inBytes;

    if (inBytes * numRanks_ != outBytes) {
      throw std::invalid_argument("sizeBytes must be divisible by numRanks");
    }
  } else if (wkldType_ == WorkloadSpecificConfig::Type::reduceScatter) {
    auto rsCfg = *workCfg_->config()->reduceScatter();
    algorithm_ = *rsCfg.algorithm();
    inBytes = *rsCfg.sizeBytes();
    outBytes = inBytes / numRanks_;
    sizeBytes_ = outBytes;

    // In distance optimized mode, we send N/2 non-contiguous blocks at the
    // first step and halve the # of blocks each step. In message optimized
    // mode, we send one message per step but double the size each time.
    stepMsgs_ = algorithm_ == AllReduceAlgorithm::RECURSIVE_DIST_OPT
        ? numRanks_ / 2
        : 1U;
    firstStepMsgSize = algorithm_ == AllReduceAlgorithm::RECURSIVE_DIST_OPT
        ? outBytes
        : inBytes / 2;
    largestStepMsgSize = firstStepMsgSize;

    if (outBytes * numRanks_ != inBytes) {
      throw std::invalid_argument("sizeBytes must be divisible by numRanks");
    }
  } else {
    throw std::invalid_argument("Unsupported workload type");
  }

  if (algorithm_ != AllReduceAlgorithm::RECURSIVE_MSG_OPT &&
      algorithm_ != AllReduceAlgorithm::RECURSIVE_DIST_OPT) {
    throw std::invalid_argument("Unsupported algorithm");
  }

  if (largestStepMsgSize > std::numeric_limits<uint32_t>::max()) {
    throw std::invalid_argument(
        fmt::format(
            "Step message of {} bytes does not fit in 32 bits",
            largestStepMsgSize));
  }
  stepMsgSize_ = folly::to<uint32_t>(firstStepMsgSize);

  // Precompute peers and distances for reuse
  peers_.resize(nSteps_);
  dists_.resize(nSteps_);
  bool farthestFirst = (wkldType_ == WorkloadSpecificConfig::Type::allGather) &&
      (algorithm_ == AllReduceAlgorithm::RECURSIVE_DIST_OPT);
  farthestFirst |= (wkldType_ == WorkloadSpecificConfig::Type::reduceScatter) &&
      (algorithm_ == AllReduceAlgorithm::RECURSIVE_MSG_OPT);
  for (uint32_t i = 0; i < nSteps_; i++) {
    dists_[i] = farthestFirst ? numRanks_ / (2U << i) : (1U << i);
    uint32_t pos = (rank_ / dists_[i]) % 2;
    peers_[i] = pos == 0 ? rank_ + dists_[i] : rank_ - dists_[i];
    peerToStep_[peers_[i]] = i;
  }

  // Size the send and receive message trackers
  ctrlRcvd_.resize(nSteps_, false);
  sends_.resize(nSteps_);
  recvs_.resize(nSteps_);
}

void AgRsRecursiveRank::run() {
  // Post receives for first step
  for (uint32_t j = 0; j < stepMsgs_; j++) {
    recvs_[step_].emplace_back(recv(peers_[0], stepMsgSize_, j));
  }

  runAfterStartDelay([this] {
    // Send control message that indicates RTR to first peer
    notify(peers_[0]);
    startTimeNs_ = getTimeNs();
    progress();
  });
}

void AgRsRecursiveRank::requestReceived(Message* msg) {
  if (isNotifyMsg(msg)) {
    notifyReceived(msg->srcRank);
  } else {
    ++completedRecvsThisStep_;
    if (recvs_.at(step_).empty()) {
      throw std::runtime_error("Received message without Recv WQE posted");
    }
    auto& expMsg = recvs_.at(step_).front();
    if (expMsg->connectionMessageId != msg->connectionMessageId) {
      throw std::runtime_error(
          "Received message with wrong connection message ID");
    }
    recvs_.at(step_).pop_front();
  }

  progress();
}

void AgRsRecursiveRank::requestAcked(Message* msg) {
  if (isNotifyMsg(msg)) {
    notifyAcked(msg->dstRank);
  } else {
    ++completedSendsThisStep_;
    if (sends_.at(step_).empty()) {
      throw std::runtime_error("Received ack without Send WQE posted");
    }
    auto& expMsg = sends_.at(step_).front();
    if (expMsg->connectionMessageId != msg->connectionMessageId) {
      throw std::runtime_error("Received ack with wrong connection message ID");
    }
    sends_.at(step_).pop_front();
  }
  progress();
}

void AgRsRecursiveRank::progress() {
  if (complete_) {
    return;
  }
  bool stepStarted = sendsPostedThisStep_ > 0;
  bool sendsComplete = completedSendsThisStep_ == stepMsgs_;
  bool recvsComplete = completedRecvsThisStep_ == stepMsgs_;

  if (!ctrlRcvd_.at(step_)) {
    ctrlRcvd_.at(step_) = checkNotifyReceived(peers_.at(step_));
  }

  if (!stepStarted && ctrlRcvd_.at(step_) && (step_ < nSteps_)) {
    // Send messages for this step
    for (uint32_t j = 0; j < stepMsgs_; j++) {
      auto peer = peers_.at(step_);
      sends_.at(step_).emplace_back(send(peer, stepMsgSize_, j));
      sendsPostedThisStep_++;
    }
  } else if (stepStarted && sendsComplete && recvsComplete) {
    // Step is complete, move to next step
    ++step_;
    if (step_ == nSteps_) {
      // Final step reached; complete workload
      complete_ = true;
      endTimeNs_ = getTimeNs();
      onRankComplete(workloadId_, rank_);
      return;
    }

    // Otherwise, start next step
    auto peer = peers_.at(step_);
    sendsPostedThisStep_ = 0;
    completedSendsThisStep_ = 0;
    completedRecvsThisStep_ = 0;
    if (wkldType_ == WorkloadSpecificConfig::Type::allGather) {
      // All Gather
      if (algorithm_ == AllReduceAlgorithm::RECURSIVE_DIST_OPT) {
        stepMsgs_ *= 2;
      } else {
        stepMsgSize_ *= 2;
      }
    } else {
      // Reduce Scatter
      if (algorithm_ == AllReduceAlgorithm::RECURSIVE_DIST_OPT) {
        stepMsgs_ /= 2;
      } else {
        stepMsgSize_ /= 2;
      }
    }
    // Pre-post recvs for this step
    for (uint32_t j = 0; j < stepMsgs_; j++) {
      recvs_[step_].emplace_back(recv(peer, stepMsgSize_, j));
    }
    notify(peer);
  }
}

uint64_t AgRsRecursiveRank::getExpectedBytes() {
  return (numRanks_ - 1) * sizeBytes_ + nSteps_ * kNotifyMsgSize;
}

bool AgRsRecursiveRank::complete() {
  return complete_;
}
} // namespace packsim
