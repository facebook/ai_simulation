/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <type_traits>

#include "packsim/packet/base.h"
#include "packsim/packet/ping.h"
#include "packsim/packet/roce.h"

namespace packsim {

namespace detail {
// Dependent on T, so the primary-template static_assert below only fires when
// PacketEnumMapper is actually instantiated for an unmapped type.
template <typename>
inline constexpr bool kPacketTypeIsMapped = false;
} // namespace detail

/**
 * Select a type based on the enum. This allows us to map enum to type at
 * compile time.
 *
 * Specialise this next to the packet type itself rather than here — see
 * PacketEnumMapper.
 */
template <PacketType T>
struct PacketTypeMapper;

/**
 * Select an enum based on the type. This allows us to map type to enum at
 * compile time. The inverse of PacketTypeMapper; specialise the two together.
 *
 * A packet type declares its own mapping in the header that defines it, so
 * this header stays independent of the transports. `packet/base.h`,
 * `packet/roce.h` and `packet/ping.h` types are mapped below because this
 * header already owns them; every other packet type maps itself.
 */
template <typename T>
struct PacketEnumMapper {
  static_assert(
      detail::kPacketTypeIsMapped<T>,
      "Unsupported packet type. Specialise PacketTypeMapper and "
      "PacketEnumMapper for it in the header that defines it.");
};

template <>
struct PacketTypeMapper<PacketType::RAW> {
  using type = Packet;
};
template <>
struct PacketEnumMapper<Packet> {
  static constexpr PacketType value = PacketType::RAW;
};

template <>
struct PacketTypeMapper<PacketType::ETHERNET> {
  using type = EthernetPacket;
};
template <>
struct PacketEnumMapper<EthernetPacket> {
  static constexpr PacketType value = PacketType::ETHERNET;
};

// Mapped for type tests only: `MacControlFrame`'s constructor is protected, so
// `is<MacControlFrame>()` / `as<>` work but
// `createPacket<PacketType::MAC_CONTROL>()` does not compile. A MAC control
// frame is always a specific kind.
template <>
struct PacketTypeMapper<PacketType::MAC_CONTROL> {
  using type = MacControlFrame;
};
template <>
struct PacketEnumMapper<MacControlFrame> {
  static constexpr PacketType value = PacketType::MAC_CONTROL;
};

template <>
struct PacketTypeMapper<PacketType::ETHERNET_PFC> {
  using type = EthernetPfcPacket;
};
template <>
struct PacketEnumMapper<EthernetPfcPacket> {
  static constexpr PacketType value = PacketType::ETHERNET_PFC;
};

template <>
struct PacketTypeMapper<PacketType::ETHERNET_CC> {
  using type = EthernetCcPacket;
};
template <>
struct PacketEnumMapper<EthernetCcPacket> {
  static constexpr PacketType value = PacketType::ETHERNET_CC;
};

// Mapped for type tests only: `CtlosFrame`'s constructor is protected, so
// `is<CtlosFrame>()` / `as<>` work but `createPacket<PacketType::CTLOS>()`
// does not compile. An ordered set must be a specific kind.
template <>
struct PacketTypeMapper<PacketType::CTLOS> {
  using type = CtlosFrame;
};
template <>
struct PacketEnumMapper<CtlosFrame> {
  static constexpr PacketType value = PacketType::CTLOS;
};

template <>
struct PacketTypeMapper<PacketType::CTLOS_CF_UPDATE> {
  using type = CfUpdateFrame;
};
template <>
struct PacketEnumMapper<CfUpdateFrame> {
  static constexpr PacketType value = PacketType::CTLOS_CF_UPDATE;
};

template <>
struct PacketTypeMapper<PacketType::CTLOS_LLR> {
  using type = LlrCtlosFrame;
};
template <>
struct PacketEnumMapper<LlrCtlosFrame> {
  static constexpr PacketType value = PacketType::CTLOS_LLR;
};

template <>
struct PacketTypeMapper<PacketType::IPV6> {
  using type = Ipv6Packet;
};
template <>
struct PacketEnumMapper<Ipv6Packet> {
  static constexpr PacketType value = PacketType::IPV6;
};

template <>
struct PacketTypeMapper<PacketType::UDP> {
  using type = UdpPacket;
};
template <>
struct PacketEnumMapper<UdpPacket> {
  static constexpr PacketType value = PacketType::UDP;
};

template <>
struct PacketTypeMapper<PacketType::ROCE> {
  using type = RocePacket;
};
template <>
struct PacketEnumMapper<RocePacket> {
  static constexpr PacketType value = PacketType::ROCE;
};

template <>
struct PacketTypeMapper<PacketType::PING> {
  using type = PingPacket;
};
template <>
struct PacketEnumMapper<PingPacket> {
  static constexpr PacketType value = PacketType::PING;
};

/**
 * The PacketType enum value a C++ packet type maps to.
 *
 * @return The PacketType corresponding to PacketCppType.
 */
template <
    typename PacketCppType,
    typename T = std::remove_cvref_t<PacketCppType>>
constexpr PacketType getPacketTypeEnum() {
  return PacketEnumMapper<T>::value;
}

/**
 * Whether a runtime `PacketType` denotes `T` -- that is, whether a packet
 * carrying that type is a `T`, or something derived from it.
 *
 * The default answers by exact match, which is correct for every leaf type. A
 * type that others derive from specialises this with the enum range its family
 * occupies, so the test stays a constexpr compare rather than a
 * `dynamic_cast`. `PacketPtr::is()`, `PacketPtr::tryAs()` and `packetIs()` are
 * the callers.
 *
 * Specialise this beside the type's `PacketTypeMapper` / `PacketEnumMapper`,
 * for the same reason those live together: a caller can only ask about a type
 * it can name, and naming it means including the header that defines it, so
 * the specialisation is always visible wherever it is usable.
 *
 * A specialisation encodes where its family sits in `PacketType`, so it is
 * wrong the moment a type is added in the wrong place. What holds it to the
 * class hierarchy is `MatcherAgreesWithDynamicCast` in
 * `packet/oss_tests/packet-tests.cpp`, which instantiates every mapped type and
 * requires each matcher to return exactly what `dynamic_cast` does.
 */
template <typename T>
struct PacketTypeMatcher {
  static constexpr bool matches(PacketType t) noexcept {
    return t == PacketEnumMapper<T>::value;
  }
};

// The root: every packet is a `Packet`.
template <>
struct PacketTypeMatcher<Packet> {
  static constexpr bool matches(PacketType /*t*/) noexcept {
    return true;
  }
};

template <>
struct PacketTypeMatcher<CtlosFrame> {
  static constexpr bool matches(PacketType t) noexcept {
    return t >= PacketType::CTLOS && t <= PacketType::CTLOS_LLR;
  }
};

template <>
struct PacketTypeMatcher<EthernetPacket> {
  static constexpr bool matches(PacketType t) noexcept {
    return t >= PacketType::ETHERNET;
  }
};

template <>
struct PacketTypeMatcher<MacControlFrame> {
  static constexpr bool matches(PacketType t) noexcept {
    return t >= PacketType::MAC_CONTROL && t <= PacketType::ETHERNET_CC;
  }
};

template <>
struct PacketTypeMatcher<Ipv6Packet> {
  static constexpr bool matches(PacketType t) noexcept {
    return t >= PacketType::IPV6;
  }
};

// Excludes `PING` rather than ending at a named upper bound: `PingPacket` is
// the one type at or after `UDP` that derives from `Ipv6Packet` instead of
// `UdpPacket`. Naming the exception rather than the last member also keeps
// this correct in the open-source export, which strips the transport types
// between `ROCE` and `PING` from the enum.
template <>
struct PacketTypeMatcher<UdpPacket> {
  static constexpr bool matches(PacketType t) noexcept {
    return t >= PacketType::UDP && t != PacketType::PING;
  }
};

/**
 * Whether `pkt` is a `T`. The reference-taking counterpart of
 * `PacketPtr::is()`, for the observers that take a `const Packet&` because
 * they look at a packet without owning it.
 *
 * @param pkt  The packet to test.
 *
 * @return     True iff `pkt` is a `T` or derived from one.
 */
template <typename T>
[[nodiscard]] constexpr bool packetIs(const Packet& pkt) noexcept {
  return PacketTypeMatcher<T>::matches(pkt.pktType);
}

} // namespace packsim
