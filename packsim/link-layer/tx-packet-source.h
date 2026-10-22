/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

/**
 * Head-of-line packet source for a port's transmit queues.
 *
 * The two methods are the port's own TX-queue API: peek the head-of-line
 * application packet, then pop it. They are read-then-pop rather than a
 * single pop so a caller can test the packet against an admission rule and
 * only then commit. `llr::Llr` uses that to check LLR admission, and
 * `EthernetPort` uses it directly for the CBFC credit gate and for
 * plain non-LLR egress.
 *
 * It is an interface, rather than a direct call on `EthernetPort`, so the
 * LLR code does not depend on the port that owns it and the two have no
 * dependency cycle.
 */
class TxPacketSource {
 public:
  virtual ~TxPacketSource() = default;

  /**
   * Peek the next eligible TX packet without popping. Returns the
   * packet at the head of the next-eligible CoS queue, or
   * a null `PacketPtr{}` if no queue has work right now. `const` — no
   * observable side effects (the caller must call `getTxPacket()` to
   * commit the pop).
   *
   * @return The packet at the head of the next-eligible CoS queue, or
   *         a null `PacketPtr{}` if no queue has work right now.
   */
  [[nodiscard]] virtual PacketPtr peekNextAppPacket() const = 0;

  /**
   * Pop the next packet into `packet`. Side effects (caller-visible):
   * VOQ-based ECN marking, mark callback, ECN/CE accounting.
   *
   * @param packet Reference to store the popped packet.
   */
  virtual void getTxPacket(PacketPtr& packet) = 0;
};

} // namespace packsim
