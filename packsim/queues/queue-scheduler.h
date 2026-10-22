/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include <folly/memory/not_null.h>

namespace packsim {

class Queues;

/**
 * Base class of queue scheduler.
 */
class QueueScheduler {
 public:
  /**
   * @param queues Pointer to access queue information
   */
  explicit QueueScheduler(const Queues* queues);
  virtual ~QueueScheduler() = default;

  /**
   * Selects the queue to pop from next and commits that choice, advancing any
   * round-robin cursor past the returned queue. Call once per packet actually
   * taken; to inspect a packet before taking it, use `peekQueueToPop()`.
   *
   * @return Queue index to pop, or `std::nullopt` if no queue is eligible.
   */
  virtual std::optional<uint8_t> selectQueueToPop() = 0;

  /**
   * Reports the choice `selectQueueToPop()` would make without advancing the
   * cursor, so a caller can inspect the head packet without redirecting the
   * following pop to a different queue.
   *
   * @return Queue index that would be popped, or `std::nullopt` if no queue
   *         is eligible.
   */
  [[nodiscard]] virtual std::optional<uint8_t> peekQueueToPop() const = 0;

 protected:
  // Not owned; must outlive the scheduler.
  folly::not_null<const Queues*> queues_;
  uint8_t numQueues_;
};

/**
 * Subclass of scheduler that does Strict Priority (SP).
 * In SP, higher queue number has higher priority.
 * For example, Queue 7 has higher priority than 6, and 6 is higher than 5.
 */
class SpQueueScheduler : public QueueScheduler {
 public:
  /**
   * @param queues Pointer to access queue information
   */
  explicit SpQueueScheduler(const Queues* queues);

  /**
   * @return Queue index to pop, or `std::nullopt` when none is eligible.
   */
  std::optional<uint8_t> selectQueueToPop() override;

  /**
   * SP holds no cursor, so peeking and selecting are the same scan.
   *
   * @return  Queue to pop from, or `std::nullopt` when none is eligible.
   */
  [[nodiscard]] std::optional<uint8_t> peekQueueToPop() const override;
};

/**
 * Subclass of scheduler that does Round-Robin.
 */
class RrQueueScheduler : public QueueScheduler {
 public:
  /**
   * @param queues Pointer to access queue information
   */
  explicit RrQueueScheduler(const Queues* queues);

  /**
   * @return Queue index to pop, or `std::nullopt` when none is eligible.
   */
  std::optional<uint8_t> selectQueueToPop() override;

  /**
   * @return  Queue to pop from without moving the cursor, or `std::nullopt`
   *          when none is eligible.
   */
  [[nodiscard]] std::optional<uint8_t> peekQueueToPop() const override;

 private:
  /**
   * Round-robin scan starting at `startId`. Shared by `selectQueueToPop()` and
   * `peekQueueToPop()` so the two cannot disagree.
   *
   * @param startId  Queue id to begin the scan at.
   * @return         First eligible queue found, or `std::nullopt` when none
   *                 is.
   */
  [[nodiscard]] std::optional<uint8_t> scanFrom(uint8_t startId) const;

  uint8_t rrId_{0};
};

/**
 * Subclass of scheduler that does Weighted Round-Robin.
 */
class WrrQueueScheduler : public QueueScheduler {
 public:
  // Smallest accepted weight. A round walks the weight levels from the largest
  // weight down to this one, so a queue of weight w is served w times.
  static constexpr uint32_t kMinWeight = 1;

  /**
   * @param queues  Pointer to access queue information
   * @param weights Weight vector for each queue
   * @throw std::invalid_argument if `weights` does not have one entry per
   *        queue.
   * @throw std::invalid_argument if a weight is below `kMinWeight`.
   */
  WrrQueueScheduler(const Queues* queues, const std::vector<uint32_t>& weights);

  /**
   * @return Queue index to pop, or `std::nullopt` when none is eligible.
   */
  std::optional<uint8_t> selectQueueToPop() override;

  /**
   * @return  Queue to pop from without committing the cursor or weight
   *          state, or `std::nullopt` when none is eligible.
   */
  [[nodiscard]] std::optional<uint8_t> peekQueueToPop() const override;

  /**
   * Replace the weights and restart the round: the next selection starts at
   * the largest weight and the first queue.
   *
   * @param weights Weight vector for each queue
   * @throw std::invalid_argument if `weights` does not have one entry per
   *        queue.
   * @throw std::invalid_argument if a weight is below `kMinWeight`.
   */
  void setWeights(const std::vector<uint32_t>& weights);

 private:
  /**
   * A WRR choice plus the cursor state (`rrId_`, `weightId_`) it implies.
   * `selectQueueToPop()` commits that state; `peekQueueToPop()` discards it.
   */
  struct Selection {
    // Chosen queue index, or `std::nullopt` when no queue is eligible.
    std::optional<uint8_t> queue;
    // Value copied into `rrId_` on commit. It is the chosen index plus one, so
    // it can equal `numQueues_`; the next scan then finds nothing at its
    // starting weight level and drops to the next level.
    uint8_t nextRrId{0};
    // Weight level the queue was chosen at, from `kMinWeight` to `maxWeight_`.
    // Copied into `weightId_` on commit.
    uint32_t weightLevel{0};
  };
  [[nodiscard]] Selection scan() const;

  uint8_t rrId_{0};
  // Current weight level, from `kMinWeight` up to `maxWeight_`.
  uint32_t weightId_{0};
  uint32_t maxWeight_{0};
  std::vector<uint32_t> weights_;
};
} // namespace packsim
