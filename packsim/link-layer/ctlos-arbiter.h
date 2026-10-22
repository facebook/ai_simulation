/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <optional>
#include <span>

#include <folly/logging/xlog.h>

#include "packsim/link-layer/ctlos-source.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

/**
 * Chooses which CtlOS producer transmits next on one link.
 *
 * UEC 1.0.3 §5.1.9.1.5 (p.486): "If credit-based flow control is also active,
 * arbitration between CBFC CtlOS transmission and LLR CtlOS transmission
 * should employ an algorithm that avoids starvation of either. To that end,
 * round robin is recommended." This is that round robin. Priority *within* a
 * producer stays the producer's business — Table 5-12's ordering of the four
 * LLR CtlOS is enforced by `llr::LlrCtlosSlots`, not here.
 *
 * The arbiter stores no producers, only the rotation cursor: the caller hands
 * the live set to each `popNextRr()`. The caller's ownership stays the single
 * source of truth for which producers exist, so one torn down between calls
 * cannot leave a stale pointer here.
 *
 * NOTE: not thread-safe. It lives on one PDES context.
 */
class CtlosArbiter {
 public:
  /**
   * Take the next CtlOS frame, round-robin across `sources`.
   *
   * The cursor advances past whichever source produced and persists across
   * calls, so a producer that always has work cannot hold the wire against one
   * that sometimes does.
   *
   * @param sources  The port's producer slots, in an order that does not vary
   *                 between calls: the cursor indexes them, so a slot that
   *                 moves silently hands its turn to someone else. A slot is
   *                 null while its producer is disabled, and is skipped.
   *
   * @return         The frame, or `nullopt` when no producer has one ready.
   */
  [[nodiscard]] std::optional<PacketPtr> popNextRr(
      std::span<ICtlosSource* const> sources) {
    for (std::size_t i = 0; i < sources.size(); ++i) {
      const std::size_t idx = (cursor_ + i) % sources.size();
      auto* source = sources[idx];
      if (source == nullptr || !source->hasPendingCtlos()) {
        continue;
      }
      auto frame = source->popCtlos();
      // `hasPendingCtlos()` is contractually the predicate `popCtlos()`
      // applies, so a source that declines here has broken that contract and
      // has just consumed a wake with nothing to show for it.
      XCHECK(frame.has_value())
          << "CtlosArbiter: source reported a pending CtlOS then declined to "
             "produce one; hasPendingCtlos() and popCtlos() disagree";
      cursor_ = (idx + 1) % sources.size();
      return frame;
    }
    return std::nullopt;
  }

 private:
  // Slot to start the next sweep from. Persists across calls; that persistence
  // is the whole anti-starvation mechanism, and the only state here.
  std::size_t cursor_{0};
};

} // namespace packsim
