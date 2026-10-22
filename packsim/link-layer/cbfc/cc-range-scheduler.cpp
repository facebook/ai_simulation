/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/cc-range-scheduler.h"

#include <stdexcept>

#include <fmt/format.h>

namespace packsim::cbfc {

CcRangeScheduler::CcRangeScheduler(const uint8_t numRanges)
    : numRanges_(numRanges) {
  if (numRanges_ < 1 || numRanges_ > kMaxRanges) {
    throw std::invalid_argument(
        fmt::format(
            "CcRangeScheduler: {} CC_Update ranges requested; UEC defines "
            "1..{}.",
            int{numRanges_},
            int{kMaxRanges}));
  }
}

void CcRangeScheduler::queueAll() noexcept {
  for (uint8_t r = 0; r < numRanges_; ++r) {
    queued_[r] = true;
  }
}

bool CcRangeScheduler::hasQueued() const noexcept {
  for (uint8_t r = 0; r < numRanges_; ++r) {
    if (queued_[r]) {
      return true;
    }
  }
  return false;
}

std::optional<uint8_t> CcRangeScheduler::pop() noexcept {
  for (uint8_t i = 0; i < numRanges_; ++i) {
    const uint8_t r = (next_ + i) % numRanges_;
    if (queued_[r]) {
      queued_[r] = false;
      next_ = (r + 1) % numRanges_;
      return r;
    }
  }
  return std::nullopt;
}

void CcRangeScheduler::clear() noexcept {
  queued_.fill(false);
  next_ = 0;
}

} // namespace packsim::cbfc
