/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <optional>

#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

/**
 * A producer of CtlOS frames on one port's link.
 *
 * Two protocols emit them: link-layer retry and credit-based flow control.
 * They share one insertion point, so `CtlosArbiter` picks between them and
 * this is the interface it selects over. What a frame carries, and when a
 * protocol decides one is due, stays with that protocol.
 *
 * `hasPendingCtlos()` must be the exact predicate `popCtlos()` applies. A
 * source that answers true and then returns `nullopt` consumes a wake and
 * sends nothing, and because the CtlOS wake is a pulse event the arbiter may
 * have no later signal to retry on — the frame strands on an otherwise idle
 * link. A source withholding work reports `false` and waits for its own next
 * event.
 */
class ICtlosSource {
 public:
  virtual ~ICtlosSource() = default;

  /**
   * Whether `popCtlos()` would return a frame if called right now.
   *
   * @return  True iff a frame is available this instant.
   */
  [[nodiscard]] virtual bool hasPendingCtlos() const = 0;

  /**
   * Take the next CtlOS frame.
   *
   * Only called when `hasPendingCtlos()` is true, and returning `nullopt`
   * then is a contract violation; see the class docstring.
   *
   * @return  The frame to transmit.
   */
  [[nodiscard]] virtual std::optional<PacketPtr> popCtlos() = 0;
};

} // namespace packsim
