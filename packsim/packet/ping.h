/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "packsim/kernel/device-id.h"
#include "packsim/packet/base.h"

namespace packsim {

enum class PingType : uint8_t {
  REQUEST,
  REPLY,
};

// One switch hop, recorded when the request's traceEnabled is set.
struct PingHopInfo {
  // Name of the switch this hop traversed.
  std::string deviceName;
  // FlexId of that switch.
  FlexDeviceId deviceId{};
  // Ingress port the packet arrived on at this hop.
  uint16_t inPort{0};
  // Ingress-port queue occupancy when the packet transited, in bytes.
  uint32_t queueDepthBytes{0};
  // Simulation time the packet transited this hop, in femtoseconds.
  uint64_t simTimeFs{0};
};

/**
 * A ping (echo request/reply matched by pingId), routed at the IPv6/Ethernet
 * layer via LPM. The simulator never sends one itself: a caller injects it and
 * answers it through `Endpoint::setOnPingReceivedCallback`.
 */
struct PingPacket : public Ipv6Packet {
  // This implementation of PingPacket defaults to 64-byte pings.
  static constexpr uint32_t kPingPayloadSize = 64;

  PingPacket() : Ipv6Packet(PacketType::PING, kPingPayloadSize) {}

  explicit PingPacket(PingType type, uint32_t id)
      : Ipv6Packet(PacketType::PING, kPingPayloadSize),
        pingType(type),
        pingId(id) {}

  ~PingPacket() override = default;

  void reset() override {
    Ipv6Packet::reset();
    payloadSize = kPingPayloadSize;
    pingType = PingType::REQUEST;
    pingId = 0;
    sentTimeFs = 0;
    traceEnabled = false;
    hops.clear();
  }

  // Whether this is a request or reply.
  PingType pingType{PingType::REQUEST};

  // Unique identifier matching request to reply.
  uint32_t pingId{0};

  // Timestamp when the request was sent (femtoseconds).
  uint64_t sentTimeFs{0};

  // When true, switches record hop info as the packet traverses.
  bool traceEnabled{false};

  // Hop-by-hop trace (populated only when traceEnabled is true).
  std::vector<PingHopInfo> hops;
};

} // namespace packsim
