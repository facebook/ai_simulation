/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <cstdint>
#include <optional>

namespace packsim::cbfc {

/**
 * Which CC_Update ranges (UEC 1.0.3 Tbl 5-22: VC 0..15, VC 16..31) owe a
 * report. Each CC_msg tick queues every range the link has; the port pops
 * them one at a time, round-robin, so a timer that ticks faster than the
 * port drains cannot starve the upper range.
 *
 * Queueing an already-queued range is a no-op: a CC_Update carries cumulative
 * counters read at pop time, so a later report makes an earlier one
 * redundant.
 *
 * NOTE: Not thread-safe; used from one `Cbfc` on its own `SimContext`.
 */
class CcRangeScheduler {
 public:
  // Ranges UEC defines: one per 16 VCs, up to 32 VCs.
  static constexpr uint8_t kMaxRanges = 2;

  /**
   * @param numRanges  CC_Update ranges the link needs, 1..kMaxRanges.
   *
   * @throw std::invalid_argument  If `numRanges` is outside that range.
   */
  explicit CcRangeScheduler(uint8_t numRanges);

  // Queue every range, as one CC_msg tick does.
  void queueAll() noexcept;

  // Whether any range is queued.
  [[nodiscard]] bool hasQueued() const noexcept;

  /**
   * Take the next queued range, scanning round-robin from the one after the
   * range last taken.
   *
   * @return  The range index (0 for VC 0..15, 1 for VC 16..31), or nullopt
   *          when none is queued.
   */
  [[nodiscard]] std::optional<uint8_t> pop() noexcept;

  // Drop every queued range and restart the scan at range 0.
  void clear() noexcept;

 private:
  uint8_t numRanges_;
  std::array<bool, kMaxRanges> queued_{};
  // Where the next pop starts scanning.
  uint8_t next_{0};
};

} // namespace packsim::cbfc
