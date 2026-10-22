/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include <fmt/core.h>
#include <folly/CPortability.h>

#include "packsim/kernel/device-id.h"
#include "packsim/packet/base.h"

namespace packsim {

/** Base Transport Header opcode for RoCE packets. */
enum class BthOpcode : uint8_t {
  SEND_FIRST = 0,
  SEND_MIDDLE = 1,
  SEND_LAST = 2,
  SEND_FIRST_AND_LAST = 3,
  CNP = 129, // 0x81
};

/**
 * Convert BthOpcode to string representation.
 *
 * @param opcode The opcode to convert.
 * @return       String name of the opcode, or "UNKNOWN" if not recognized.
 */
FOLLY_ALWAYS_INLINE const char* bthOpcodeToString(BthOpcode opcode) {
  switch (opcode) {
    case BthOpcode::SEND_FIRST:
      return "SEND_FIRST";
    case BthOpcode::SEND_MIDDLE:
      return "SEND_MIDDLE";
    case BthOpcode::SEND_LAST:
      return "SEND_LAST";
    case BthOpcode::SEND_FIRST_AND_LAST:
      return "SEND_FIRST_AND_LAST";
    case BthOpcode::CNP:
      return "CNP";
    default:
      return "UNKNOWN";
  }
}

/** RDMA over Converged Ethernet packet. */
struct RocePacket : public UdpPacket {
  explicit RocePacket() : UdpPacket(PacketType::ROCE, 0) {
    headerSize += RocePacket::kRoceHeaderSize;
  }

  explicit RocePacket(uint16_t ps) : UdpPacket(PacketType::ROCE, ps) {
    headerSize += RocePacket::kRoceHeaderSize;
  }

  explicit RocePacket(PacketType pt, uint16_t ps) : UdpPacket(pt, ps) {
    headerSize += RocePacket::kRoceHeaderSize;
  }
  ~RocePacket() override = default;

  /**
   * Set the BTH opcode based on packet position.
   *
   * @param isFirstPkt Whether this is the first packet in the message.
   * @param isLastPkt  Whether this is the last packet in the message.
   */
  void setOpcode(bool isFirstPkt, bool isLastPkt);

  /**
   * Check if this is the last packet in the message.
   *
   * @return True if this is the last packet.
   */
  bool isLastPkt() const;

  /** Reset packet to initial state. */
  virtual void reset() override {
    UdpPacket::reset();
    headerSize += kRoceHeaderSize;
    psn = 0;
    immData = 0;
    opcode = BthOpcode::SEND_FIRST;
    sequenceNum = 0;
    nackTimer = UINT8_MAX;
    isAck = false;
    isNack = false;
    isCnp = false;
    ackReq = false;
    isWrite = false;
    isWriteImm = false;
    sendOrWriteData = 0;
    lossRecoveryEnabled = false;
    egressNicPort = std::nullopt;
    dstNicPort = std::nullopt;
  }

  // Get source Queue Pair Number from connection.
  uint32_t srcQPN() const noexcept {
    return static_cast<QPConnection const*>(conn)->srcQPN;
  }

  // Get destination Queue Pair Number from connection.
  uint32_t dstQPN() const noexcept {
    return static_cast<QPConnection const*>(conn)->dstQPN;
  }

  static constexpr uint32_t kRoceHeaderSize = 24;

  // Packet Sequence Number
  uint32_t psn{0};
  // Immediate data field
  uint32_t immData{0};
  // Flow-level sequence number
  uint32_t sequenceNum{};
  // Base Transport Header opcode
  BthOpcode opcode{BthOpcode::SEND_FIRST};
  // NACK retransmission timer
  uint8_t nackTimer{UINT8_MAX};

  // Flags
  bool isAck{false};
  bool isNack{false};
  bool isCnp{false};
  bool ackReq{false};
  bool isWrite{false};
  bool isWriteImm{false};
  uint32_t sendOrWriteData{0};

  // True when the sending QP has loss recovery configured (RoceV2
  // lossRecovery selects goBackN, not dummy), so this packet will be
  // retransmitted if it is lost. Stamped at encap because a switch seeing the
  // packet has no access to the sender's transport config; read back by
  // isRetransmittingTransport in switch/retransmitting-transport.h.
  bool lossRecoveryEnabled{false};

  // Physical NIC ports selected by the transport. Port numbers are 0-based.
  std::optional<uint16_t> egressNicPort;
  std::optional<uint16_t> dstNicPort;

  [[nodiscard]] std::optional<uint16_t> chosenEgressNicPort()
      const noexcept override {
    return egressNicPort;
  }

  [[nodiscard]] std::optional<uint16_t> chosenDstNicPort()
      const noexcept override {
    return dstNicPort;
  }
};

} // namespace packsim

// fmt::formatter specialization for QPConnection
template <>
struct fmt::formatter<packsim::QPConnection> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const packsim::QPConnection& conn, FormatContext& ctx) const {
    return fmt::format_to(
        ctx.out(),
        "src={}, dst={}, srcQPN={}, dstQPN={}",
        conn.srcId.toString(),
        conn.dstId.toString(),
        conn.srcQPN,
        conn.dstQPN);
  }
};

// fmt::formatter specialization for RocePacket
template <>
struct fmt::formatter<packsim::RocePacket> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const packsim::RocePacket& pkt, FormatContext& ctx) const {
    return fmt::format_to(
        ctx.out(),
        "RocePacket{{{}, psn={}, seqNum={}, "
        "opcode={}, payloadSize={}, headerSize={}, "
        "isAck={}, isNack={}, isCnp={}, ackReq={}, isWrite={}, isWriteImm={}, "
        "immData={}, nackTimer={}, sendOrWriteData={}}}",
        pkt.conn
            ? fmt::format(
                  "{}", *static_cast<const packsim::QPConnection*>(pkt.conn))
            : "conn=null",
        pkt.psn,
        pkt.sequenceNum,
        packsim::bthOpcodeToString(pkt.opcode),
        pkt.payloadSize,
        pkt.headerSize,
        pkt.isAck,
        pkt.isNack,
        pkt.isCnp,
        pkt.ackReq,
        pkt.isWrite,
        pkt.isWriteImm,
        pkt.immData,
        pkt.nackTimer,
        pkt.sendOrWriteData);
  }
};
