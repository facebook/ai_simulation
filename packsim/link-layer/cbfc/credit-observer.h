/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace packsim {

struct Packet;

namespace cbfc {

/**
 * Receives store/drain notifications from a receiver buffer so the CBFC
 * receiver can update its per-VC credit counters (UEC 1.0.3 §5.2.5.7).
 *
 * Receiver buffers (`SimpleSwitchBuffer`, `NicRxBuffer`) stay CBFC-agnostic:
 * they forward the packet at their enqueue/dequeue points, and the observer
 * maps it to a VC and a credit count. That count is cell-based for the switch
 * buffer and byte-based for the NIC buffer.
 */
class ICreditObserver {
 public:
  virtual ~ICreditObserver() = default;

  /** A packet was stored into the receiver buffer (§5.2.5.7.1). */
  virtual void onPacketStored(const Packet& pkt) = 0;

  /** A packet drained out of the receiver buffer (§5.2.5.7.2). */
  virtual void onPacketDrained(const Packet& pkt) = 0;
};

} // namespace cbfc
} // namespace packsim
