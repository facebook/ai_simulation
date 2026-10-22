/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "packsim/packet/base.h"
#include "packsim/packet/roce.h"

namespace packsim {

/**
 * Whether a packet type retransmits lost packets no matter how it is
 * configured.
 *
 * A property of the type alone, so it folds at compile time. RoCEv2 is
 * deliberately absent: it retransmits only when loss recovery is configured,
 * which this cannot see. Prefer `isRetransmittingTransport` unless you
 * genuinely mean "always, regardless of config".
 *
 * @param pktType    Packet type to classify.
 *
 * @return           True if the type always retransmits.
 */
[[nodiscard]] constexpr bool isAlwaysRetransmittingType(
    [[maybe_unused]] const PacketType pktType) noexcept {
  // @oss-disable
      // @oss-disable
      // @oss-disable
      // @oss-disable
      // @oss-disable
      // @oss-disable
      // @oss-disable
      // @oss-disable
  return false; // @oss-enable
}

/**
 * Whether this packet belongs to a transport that will retransmit it if it is
 * lost.
 *
 * Such a transport recovers a lost packet by retransmitting it, so dropping
 * one costs latency but keeps the flow correct. A packet with no recovery path
 * is unrecoverable and its loss must be surfaced rather than silently dropped.
 *
 * Two ways to qualify. A type from `isAlwaysRetransmittingType` always does.
 * RoCEv2 does only when its `lossRecovery` union selects `goBackN` rather than
 * `dummy` -- a property of the sending QP, not of the packet type -- so the
 * sender stamps `RocePacket::lossRecoveryEnabled` at encap and this reads it
 * back. Without that, every RoCEv2 drop looked unrecoverable and turned a
 * routable-again packet into a fatal error.
 *
 * @param pkt    Packet to classify.
 *
 * @return       True if the packet's transport retransmits it when lost.
 */
[[nodiscard]] inline bool isRetransmittingTransport(
    const Packet& pkt) noexcept {
  if (isAlwaysRetransmittingType(pkt.pktType)) {
    return true;
  }
  // dynamic_cast, not a `pktType == ROCE` test plus static_cast: pktType is a
  // mutable public field that callers do overwrite -- fb-switch-test.cpp in
  // switch/oss_tests stamps ROCE on a UdpPacket to drive a RoCE path -- so the
  // tag alone is not proof of the dynamic type and casting on it would read a
  // member that is not there.
  // This runs only after a routing failure, so the check costs nothing that
  // matters.
  const auto* rocePkt = dynamic_cast<const RocePacket*>(&pkt);
  return rocePkt != nullptr && rocePkt->lossRecoveryEnabled;
}

} // namespace packsim
