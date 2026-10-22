/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/packet/base.h"

#include <algorithm>

#include <folly/Utility.h>

namespace packsim {

uint32_t Packet::getSize() const {
  return headerSize + payloadSize;
}

uint32_t EthernetPacket::getSize() const {
  return std::max(Packet::getSize(), kMinEthernetFrameSizeBytes);
}

void Ipv6Packet::setDscpFromCos(const uint32_t cos) {
  static constexpr std::array<DscpType, 8> kDscpForCos{
      {DscpType::DSCP_DEFAULT,
       DscpType::DSCP_CS1,
       DscpType::DSCP_CS2,
       DscpType::DSCP_CS3,
       DscpType::DSCP_CS4,
       DscpType::DSCP_CS5,
       DscpType::DSCP_CS6,
       DscpType::DSCP_CS7}};
  dscp = kDscpForCos.at(cos);
}

uint8_t Ipv6Packet::getDscpVal() const {
  return folly::to_underlying(dscp);
}

} // namespace packsim
