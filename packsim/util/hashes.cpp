/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/hashes.h"

#include <folly/hash/Hash.h>

#include "packsim/kernel/device-id.h"

namespace packsim {
uint64_t folly5TupleHash(const PacketPtr& p, const uint32_t salt) {
  auto udp = p.as<UdpPacket>();
  return folly::hash::hash_combine(
      udp->srcId(), udp->dstId(), udp->srcPort, udp->dstPort, salt);
}

uint64_t folly5TupleHash(
    const FlexDeviceId& src,
    const FlexDeviceId& dst,
    const uint16_t srcPort,
    const uint16_t dstPort,
    const uint32_t salt) {
  return folly::hash::hash_combine(src, dst, srcPort, dstPort, salt);
}

} // namespace packsim
