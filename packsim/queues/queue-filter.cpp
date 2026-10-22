/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/queues/queue-filter.h"

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

#include <fmt/format.h>
#include <folly/container/F14Map.h>
#include <folly/container/detail/F14Policy.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

namespace {
// Queue index used when a packet does not match any configured filter rule.
constexpr uint8_t kDefaultQueueIndex = 0;
} // namespace

QueueFilter::~QueueFilter() = default;

DscpQueueFilter::DscpQueueFilter(
    const packsim::thrift::QueueFilterConfig& filterCfg)
    : filterCfg_(filterCfg) {
  for (const auto& [dscp, queue] : *filterCfg_.fieldToQueue()) {
    requireDscpKey("fieldToQueue", dscp);
  }
  for (const auto& [dscp, color] : *filterCfg_.fieldToColor()) {
    requireDscpKey("fieldToColor", dscp);
  }
}

void DscpQueueFilter::requireDscpKey(
    const std::string& mapName,
    const uint32_t dscp) {
  if (dscp >= kNumDscpValues) {
    throw std::invalid_argument(
        fmt::format(
            "QueueFilterConfig.{} has DSCP key {}, but DSCP is a 6-bit value "
            "below {}.",
            mapName,
            dscp,
            kNumDscpValues));
  }
}

uint8_t DscpQueueFilter::selectQueueToPush(const PacketPtr& pkt) const {
  const auto* ipv6 = pkt.tryAs<Ipv6Packet>();
  if (!ipv6) {
    return kDefaultQueueIndex;
  }
  const auto it = filterCfg_.fieldToQueue()->find(ipv6->getDscpVal());
  return (it != filterCfg_.fieldToQueue()->end()) ? it->second
                                                  : kDefaultQueueIndex;
}

void DscpQueueFilter::colorize(const PacketPtr& pkt) const {
  // A non-IPv6 packet carries no DSCP, so it gets the unmapped-DSCP color.
  packsim::thrift::PacketColor color{packsim::thrift::PacketColor::GREEN};
  if (const auto* ipv6 = pkt.tryAs<Ipv6Packet>()) {
    const auto itc = filterCfg_.fieldToColor()->find(ipv6->getDscpVal());
    if (itc != filterCfg_.fieldToColor()->end()) {
      color = itc->second;
    }
  }
  pkt->tag.color = color;
}

} // namespace packsim
