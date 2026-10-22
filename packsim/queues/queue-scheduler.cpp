/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/queues/queue-scheduler.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>

#include <fmt/core.h>
#include <fmt/format.h>
#include <folly/memory/not_null.h>

#include "packsim/queues/queues.h"

namespace packsim {

QueueScheduler::QueueScheduler(const Queues* queues) : queues_(queues) {
  numQueues_ = queues->getNumQueues();
}

// RR scheduler
RrQueueScheduler::RrQueueScheduler(const Queues* queues)
    : QueueScheduler(queues) {}

std::optional<uint8_t> RrQueueScheduler::scanFrom(uint8_t startId) const {
  for (uint8_t i = 0; i < numQueues_; ++i) {
    const uint8_t qId = (startId + i) % numQueues_;
    if (queues_->isQueueEligible(qId)) {
      return qId;
    }
  }
  return std::nullopt;
}

std::optional<uint8_t> RrQueueScheduler::selectQueueToPop() {
  const auto queueSelected = scanFrom(rrId_);
  if (queueSelected.has_value()) {
    rrId_ = *queueSelected;
    ++rrId_;
  }
  return queueSelected;
}

std::optional<uint8_t> RrQueueScheduler::peekQueueToPop() const {
  return scanFrom(rrId_);
}

// WRR scheduler
WrrQueueScheduler::WrrQueueScheduler(
    const Queues* queues,
    const std::vector<uint32_t>& weights)
    : QueueScheduler(queues) {
  setWeights(weights);
}

// The scan resumes at the current weight level `weightId_` and round-robin
// position `rrId_`, not at `maxWeight_`. At each level it takes the first
// eligible queue in index order whose weight is at least that level;
// otherwise the level drops by one and the walk restarts at queue 0. Levels
// run from `maxWeight_` down to `kMinWeight`, so a backlogged queue is served
// once per level its weight reaches: exactly `weight` times per round.
WrrQueueScheduler::Selection WrrQueueScheduler::scan() const {
  Selection sel;

  // two cases:
  // if current weight == kMinWeight, then scan until weight == kMinWeight and
  // index == rrId - 1 (potentially roll over to max weight)
  // if current weight > kMinWeight, then scan down to weight == kMinWeight,
  // which every queue reaches
  const uint32_t startWeight = weightId_;
  uint32_t w = startWeight;
  uint8_t i = rrId_;
  bool visited = false;

  while (true) {
    // terminate at i == numQueues_
    while (i < numQueues_) {
      if (queues_->isQueueEligible(i) && (weights_.at(i) >= w)) {
        sel.queue = i;
        sel.nextRrId = i;
        ++sel.nextRrId;
        break;
      }
      ++i;
    }

    if (sel.queue.has_value()) {
      sel.weightLevel = w;
      break;
    }
    if (w > kMinWeight) {
      --w;
    } else if (startWeight == kMinWeight && !visited) {
      // special case, when start at weight kMinWeight and start qId > 0,
      // it first roll over to max weight, and rr from 0 to qId - 1
      w = maxWeight_;
      visited = true;
    } else {
      // Every weight level down to kMinWeight has been scanned.
      break;
    }
    i = 0;
  }

  return sel;
}

std::optional<uint8_t> WrrQueueScheduler::selectQueueToPop() {
  const Selection sel = scan();
  if (sel.queue.has_value()) {
    rrId_ = sel.nextRrId;
    weightId_ = sel.weightLevel;
  }
  return sel.queue;
}

std::optional<uint8_t> WrrQueueScheduler::peekQueueToPop() const {
  return scan().queue;
}

void WrrQueueScheduler::setWeights(const std::vector<uint32_t>& weights) {
  if (weights.size() != numQueues_) {
    throw std::invalid_argument(
        fmt::format(
            "Mismatch between number of queues {} and weights {}",
            numQueues_,
            weights.size()));
  }
  for (std::size_t queue = 0; queue < weights.size(); ++queue) {
    if (weights[queue] < kMinWeight) {
      throw std::invalid_argument(
          fmt::format(
              "WRR weight {} for queue {} is below the minimum of {}: such a "
              "queue would never be served",
              weights[queue],
              queue,
              kMinWeight));
    }
  }
  weights_ = weights;
  maxWeight_ = *std::max_element(weights.begin(), weights.end());
  weightId_ = maxWeight_;
  rrId_ = 0;
}

// SP scheduler
SpQueueScheduler::SpQueueScheduler(const Queues* queues)
    : QueueScheduler(queues) {}

std::optional<uint8_t> SpQueueScheduler::selectQueueToPop() {
  return peekQueueToPop();
}

std::optional<uint8_t> SpQueueScheduler::peekQueueToPop() const {
  for (uint8_t queNum = numQueues_; queNum > 0;) {
    --queNum;
    if (queues_->isQueueEligible(queNum)) {
      return queNum;
    }
  }
  return std::nullopt;
}

} // namespace packsim
