/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <bitset>
#include <cstdint>
#include <optional>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/switch_config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/packet/llr-ctlos-types.h"
#include "packsim/packet/pfc.h"

namespace packsim {

enum class PacketType : uint8_t {
  // Non-IPv6 packet types (do NOT inherit from Ipv6Packet).
  // These MUST have enum values < IPV6.
  RAW,
  // PCS-level Control Ordered Sets (UEC §5.2.11): they preempt the data
  // stream rather than riding it, so they are not Ethernet frames.
  CTLOS, // The family base; names `CtlosFrame` for `is<>`/`as<>`
  CTLOS_CF_UPDATE, // CBFC CF_Update (UEC §5.2.6.1); see `CfUpdateFrame`
  CTLOS_LLR, // LLR ACK/NACK/INIT/INIT_ECHO; see `LlrCtlosFrame`

  ETHERNET,
  // MAC control frames (UEC 1.0.3 §5.2.9). `MAC_CONTROL` is the family base,
  // mapped for `is<>`/`as<>` only — an instance is always a specific kind.
  MAC_CONTROL,
  ETHERNET_PFC,
  ETHERNET_CC, // CBFC CC_Update MAC control frame (UEC §5.2.6.2)

  // @oss-disable

  // === IPv6 boundary ===
  // All packet types >= IPV6 MUST inherit from Ipv6Packet.
  // This ordering is relied upon by `PacketTypeMatcher<Ipv6Packet>`
  // (packet/type_mapper.h), which tests `pktType >= PacketType::IPV6` instead
  // of a dynamic_cast whenever `PacketPtr::tryAs<Ipv6Packet>()` runs, e.g. in
  // DscpQueueFilter::selectQueueToPush().
  // `MatcherAgreesWithDynamicCast` in packet/oss_tests/packet-tests.cpp
  // enforces this invariant against the class hierarchy.
  IPV6,

  // IPv6-derived packet types (inherit from Ipv6Packet).
  // These MUST have enum values >= IPV6.
  //
  // Nothing depends on the numeric values -- this is a plain C++ enum, absent
  // from every thrift file in the tree, and the only casts of it are into log
  // messages -- so adding or removing a type, which renumbers the ones after
  // it, is safe. What must hold either way is the IPV6 ordering invariant
  // above.
  UDP,
  ROCE,
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  PING,
};

/**
 * Whether a packet type carries per-port rather than per-device addressing.
 *
 * Extracted from Endpoint and EthernetPort, which both gate their
 * multi-port address handling on it and previously spelled the same predicate
 * out separately. One definition means the two cannot drift.
 *
 * @param type  Packet type to classify.
 *
 * @return      True if the type uses port-level addressing.
 */
[[nodiscard]] constexpr bool usesPortLevelAddressing(
    const PacketType type) noexcept {
  if (type == PacketType::ROCE) {
    return true;
  }
  // @oss-disable
      // @oss-disable
      // @oss-disable
  return false; // @oss-enable
}

/**
 * The Link Level Retry sequence number stamped on a data frame.
 *
 * The number rides in the Ethernet preamble, so it adds nothing to the frame
 * size. A receiver draws only one distinction, eligible or ineligible, and
 * `enabled` records it. There is no frame-type field, because the four LLR
 * control types are `LlrCtlosFrame`s rather than Ethernet frames.
 */
struct LlrHeader {
  // True when this frame was assigned a sequence number and may be replayed.
  bool enabled{false};
  uint32_t seqNum{0};
};

/**
 * CBFC (Credit-Based Flow Control, UEC §5.2) CC_Update message type
 * (Tbl 5-22): which 16-VC range the carried S_VC_CC values cover.
 *
 * VC_16_31 is generated only for a link configured with more than 16 VCs.
 */
enum class CbfcCcMsgType : uint8_t {
  VC_0_15 = 1, // 0x01: S_VC_CC for VC[0..15]
  VC_16_31 = 2, // 0x02: S_VC_CC for VC[16..31]
};

// Number of S_VC_CC counter slots in one CC_Update frame. UEC Tbl 5-22 fixes
// the Data field at 40 bytes carrying 16 counters of 20 bits each.
inline constexpr uint8_t kCcUpdateVcsPerMsg = 16;

/**
 * Payload of a CF_Update Control Ordered Set (UEC §5.2.6.1 Tbl 5-21): two
 * (VC index, R_VC_CF) credit-freed reports.
 *
 * Modeled as struct fields, not bit-packed wire bytes, like the LLR CtlOS
 * payloads. Keep the fields size-descending: interleaving the uint16 counters
 * with the uint8 VC indices pads this from 6 bytes to 8.
 */
struct CbfcCfUpdatePayload {
  // R_VC_CF[vc1], the receiver's credit-freed counter. The wire field
  // `CF1_count` is 15 bits (UEC Tbl 5-21), so it wraps at 2^15.
  uint16_t cf1{0};
  // R_VC_CF[vc2], carried in the 15-bit `CF2_count` field with the same wrap.
  uint16_t cf2{0};
  // VC the `cf1` report names. `CF1_VC_index` is 5 bits: range 0-31.
  uint8_t vc1{0};
  // VC the `cf2` report names. `CF2_VC_index` is 5 bits: range 0-31.
  uint8_t vc2{0};
};

struct Packet {
  explicit Packet() : pktType(PacketType::RAW) {}

  explicit Packet(uint16_t ps)
      : pktType(PacketType::RAW), headerSize(0), payloadSize(ps) {}

  explicit Packet(PacketType pt, uint16_t ps)
      : pktType(pt), headerSize(0), payloadSize(ps) {}
  virtual ~Packet() = default;

  Packet(const Packet&) = default;
  Packet& operator=(const Packet&) = default;
  Packet(Packet&&) = default;
  Packet& operator=(Packet&&) = default;

  virtual uint32_t getSize() const;

  /**
   * PHY-layer frame size: bytes that must be serialized to put this
   * frame on the wire, from preamble through the last bit of the FCS.
   * Used to compute `txTime` — the offset at which the receiver has
   * observed the last bit and can begin forwarding (sender_now + txTime
   * + propagation_delay).
   *
   * Does NOT include the inter-frame gap. The IFG is post-frame, after
   * the receiver has the entire frame; it concerns when the *sender*
   * may begin the next frame, not when the *receiver* gets this one.
   * See `getInterFrameGapSize()`.
   *
   * Default = `getSize()` (no PHY overhead). `EthernetPacket` adds the
   * 8-byte preamble; a `CtlosFrame` is a preamble-equivalent ordered set
   * and stays at `getSize()`.
   *
   * @return PHY-layer frame size in bytes (preamble + MAC frame).
   */
  virtual uint32_t getPhyFrameSize() const {
    return getSize();
  }

  /**
   * PHY-layer post-frame gap in bytes: the inter-frame gap the sender
   * must wait after this frame before beginning the next. The receiver
   * does not observe this — it is a purely sender-side cycle constraint.
   * `txCompleteTime = bytesToTime(getPhyFrameSize() +
   * getInterFrameGapSize())`.
   *
   * Default = 0 (no gap). `EthernetPacket` is documented as the standard
   * 12-byte IFG for full Ethernet frames; see the override for the
   * current PDES-race workaround (returns 0). A `CtlosFrame` is an inline
   * ordered set and carries no IFG of its own.
   *
   * @return Inter-frame gap size in bytes.
   */
  virtual uint32_t getInterFrameGapSize() const {
    return 0;
  }

  virtual void reset() {
    headerSize = 0;
    payloadSize = 0;
    heldCredits = 0;
  }

  struct Tag {
    uint16_t port{0};
    uint8_t pg{0};
    packsim::thrift::PacketColor color{packsim::thrift::PacketColor::GREEN};
  };

  Tag tag;
  // CBFC credits held between switch admission and departure. Link bootstrap
  // enforces `CreditSize == CellSize`, so this is also the cells to return to
  // the credited region. Packets are pooled, so `reset()` must clear it or a
  // recycled packet releases credits it never held.
  uint16_t heldCredits{0};
  PacketType pktType;
  // Header size in bytes
  uint16_t headerSize{0};
  // Payload size in bytes
  uint32_t payloadSize{0};
};

struct EthernetPacket : Packet {
  explicit EthernetPacket() : Packet(PacketType::ETHERNET, 0) {
    headerSize += EthernetPacket::kEthernetHeaderSize;
  }

  explicit EthernetPacket(uint16_t ps) : Packet(PacketType::ETHERNET, ps) {
    headerSize += EthernetPacket::kEthernetHeaderSize;
  }

  explicit EthernetPacket(PacketType pt, uint16_t ps) : Packet(pt, ps) {
    headerSize += EthernetPacket::kEthernetHeaderSize;
  }
  virtual ~EthernetPacket() override = default;

  EthernetPacket(const EthernetPacket&) = default;
  EthernetPacket& operator=(const EthernetPacket&) = default;
  EthernetPacket(EthernetPacket&&) = default;
  EthernetPacket& operator=(EthernetPacket&&) = default;
  uint32_t getSize() const override;

  /**
   * @return The preamble plus the in-buffer MAC frame, in bytes.
   */
  uint32_t getPhyFrameSize() const override {
    return getSize() + kPreambleSize;
  }

  /**
   * Returns 0 rather than the standard 12-byte gap, to avoid a PDES race
   * described in the body.
   *
   * @return Inter-frame gap in bytes.
   */
  uint32_t getInterFrameGapSize() const override {
    // FIXME: this should return `kIfgSize`, but the spec-correct 12-byte gap
    // exposes a pre-existing race in the multi-threaded PDES switch buffer
    // accounting:
    //
    //   - txCompleteTime in eth-port.cpp = bytesToTime(phyFrame) +
    //     bytesToTime(IFG). Shifting completion later by ~1ns at 100Gbps.
    //   - That tiny shift pushes FbSwitch::onPacketSent into the same
    //     PDES lookahead window (1000ns) as the next hop's
    //     routeSwitchPacket on a different worker thread.
    //   - routeSwitchPacket writes pkt->tag.{port,pg}; onPacketSent reads
    //     them. Concurrent read/write yields wrong queue identity →
    //     QueueTracker::removePacket throws "Don't have enough used
    //     cells" because the actual cells live on a different queue.
    //
    // Returning 0 keeps the two events far enough apart to avoid the race.
    //
    // The proper fix is to schedule FbSwitch::onPacketSent and the next hop's
    // FbSwitch::routeSwitchPacket into different PDES lookahead windows so
    // they never run concurrently, most likely by enforcing a minimum gap
    // larger than the lookahead between transmit completion and the
    // receiver's routing event. Snapshotting the metadata would paper over
    // the race rather than fix it. Once that lands, this method can return
    // kIfgSize.
    return 0;
  }

  void reset() override {
    Packet::reset();
    headerSize += EthernetPacket::kEthernetHeaderSize;
    llrHeader = LlrHeader{};
  }

  // IEEE 802.3 minimum frame, which a short frame is padded up to.
  static constexpr uint32_t kMinEthernetFrameSizeBytes = 64;
  static constexpr uint32_t kPreambleSize = 8;
  // IEEE 802.3 inter-frame gap, in bytes.
  static constexpr uint32_t kIfgSize = 12;
  static constexpr uint32_t kEthertypeSize = 2;
  static constexpr uint32_t kMacAddrSize = 6;
  static constexpr uint32_t kVlanTagSize = 4;
  static constexpr uint32_t kTrailerSize = 4;
  // In-buffer Ethernet header: 2 MACs + ethertype + VLAN + FCS = 22 bytes.
  // Preamble and IFG are PHY-layer; accounted for separately by
  // `getPhyFrameSize()` (preamble + frame) and `getInterFrameGapSize()`
  // (post-frame IFG).
  static constexpr uint32_t kEthernetHeaderSize =
      2 * kMacAddrSize + kEthertypeSize + kVlanTagSize + kTrailerSize;

  LlrHeader llrHeader;
};

/**
 * Base for the MAC control frames — PFC pause and CBFC CC_Update.
 *
 * A MAC control frame is terminated by the link peer's MAC rather than
 * forwarded: it never occupies an ingress buffer, so it consumes no CBFC
 * credit (UEC 1.0.3 §5.2.9 — PFC belongs to a best-effort VC, and CC_Update is
 * best-effort and "does not use credits"). It is not "Ethernet header + a
 * control header" either; the whole frame is a standard 64-byte minimum
 * Ethernet frame padded to that length whatever the opcode payload says, so
 * `headerSize` does not decompose and stays 0. PHY-layer additions come from
 * `EthernetPacket::getPhyFrameSize()` / `getInterFrameGapSize()`.
 *
 * LLR treatment is NOT shared and belongs to each subclass: §5.2.9 makes PFC
 * LLR-ineligible and CC_Update LLR-eligible with a sequence number.
 */
struct MacControlFrame : EthernetPacket {
 public:
  ~MacControlFrame() override = default;

  /**
   * The fixed MAC control frame size. `final` so no subclass reports another.
   *
   * The inherited `max(headerSize + payloadSize, 64)` is not enough:
   * `createPacket` assigns `payloadSize` after `reset()`, bypassing the
   * constructor, so a size passed there would otherwise be reported. Any
   * payload size is recorded on the packet and never observed.
   *
   * @return  The fixed frame size in bytes.
   */
  uint32_t getSize() const final {
    return kMinEthernetFrameSizeBytes;
  }

 protected:
  // Protected: a MAC control frame is always a specific kind, so
  // `createPacket<PacketType::MAC_CONTROL>()` must not compile.
  explicit MacControlFrame(PacketType pt) : EthernetPacket(pt, 0) {
    headerSize = 0;
  }
};

struct EthernetPfcPacket : MacControlFrame {
 public:
  explicit EthernetPfcPacket() : MacControlFrame(PacketType::ETHERNET_PFC) {}
  ~EthernetPfcPacket() override = default;

  void reset() override {
    // Skip EthernetPacket::reset() — see `MacControlFrame` on frame semantics.
    Packet::reset();
    pfcClassEnable = 0b00000000;
    pfcClassPauseQuanta.fill(65535);
  }

  std::bitset<kNumPfcClasses> pfcClassEnable{0b00000000};
  std::array<uint16_t, kNumPfcClasses> pfcClassPauseQuanta{};
};

/**
 * CBFC CC_Update message (UEC §5.2.6.2): a 64-byte in-band MAC control frame
 * carrying the S_VC_CC consumed-credit values for one VC range, used to
 * reclaim credits leaked by link-error drops (§5.2.5.6.4).
 *
 * Sizing and the credit exemption come from `MacControlFrame`; UEC Tbl 5-22
 * pins this at the same 64-byte MAC frame.
 */
struct EthernetCcPacket : MacControlFrame {
 public:
  // No payload-size constructor: the counters live in members, not a payload.
  explicit EthernetCcPacket() : MacControlFrame(PacketType::ETHERNET_CC) {}
  ~EthernetCcPacket() override = default;

  void reset() override {
    // Skip EthernetPacket::reset() — see `MacControlFrame` on frame semantics.
    Packet::reset();
    // Unlike PFC, a CC_Update is LLR-eligible by default (UEC §5.2.9), so it
    // is stamped with a sequence number by `Llr::admit`. A pooled frame
    // therefore comes back carrying one, and the pool is shared with ports
    // that have LLR disabled.
    llrHeader = LlrHeader{};
    msgType = CbfcCcMsgType::VC_0_15;
    sVcCc.fill(0);
  }

  // Which 16-VC range `sVcCc` reports on (UEC Tbl 5-22 Msg Type field).
  CbfcCcMsgType msgType{CbfcCcMsgType::VC_0_15};
  // S_VC_CC consumed-credit counters, one per VC of the range `msgType`
  // selects, in ascending VC order. Each counter is 20 bits wide and wraps at
  // 2^20 (UEC Tbl 5-18); an unused or best-effort VC reports 0 (UEC Tbl 5-22).
  std::array<uint32_t, kCcUpdateVcsPerMsg> sVcCc{};
};

/**
 * Base for PCS-level Control Ordered Sets (UEC 1.0.3 §5.2.11): 8 bytes that
 * preempt a data frame rather than riding one.
 *
 * Not a MAC frame -- no Ethernet header, preamble or inter-frame gap -- so
 * `Packet`'s defaults (`getPhyFrameSize() == getSize()`,
 * `getInterFrameGapSize() == 0`) already describe it.
 *
 * Carries no payload: each subclass adds the fields for its own ordered-set
 * kind. CBFC's is `CfUpdateFrame` and LLR's is `LlrCtlosFrame`.
 */
struct CtlosFrame : Packet {
  // UEC §5.2.11 ordered-set size.
  static constexpr uint32_t kCtlosFrameSizeBytes = 8;

  ~CtlosFrame() override = default;

  CtlosFrame(const CtlosFrame&) = default;
  CtlosFrame& operator=(const CtlosFrame&) = default;
  CtlosFrame(CtlosFrame&&) = default;
  CtlosFrame& operator=(CtlosFrame&&) = default;

  /**
   * The fixed size of every Control Ordered Set. `final` so no subclass can
   * report another size.
   *
   * `headerSize` and `payloadSize` are ignored, so a payload size passed to
   * `createPacket<PacketType::CTLOS_CF_UPDATE>(n)` is recorded on the packet
   * but never observed: both `getSize()` and `getPhyFrameSize()` report 8
   * bytes for any `n`. Pass 0.
   *
   * @return  The fixed ordered-set size in bytes.
   */
  uint32_t getSize() const final {
    return kCtlosFrameSizeBytes;
  }

 protected:
  explicit CtlosFrame(PacketType pt) : Packet(pt, 0) {}
};

/**
 * CBFC's CF_Update ordered set (UEC §5.2.6.1): two (VC index, R_VC_CF)
 * credit-freed reports carried inside the 8 bytes.
 */
struct CfUpdateFrame : CtlosFrame {
  explicit CfUpdateFrame() : CtlosFrame(PacketType::CTLOS_CF_UPDATE) {}
  ~CfUpdateFrame() override = default;

  CfUpdateFrame(const CfUpdateFrame&) = default;
  CfUpdateFrame& operator=(const CfUpdateFrame&) = default;
  CfUpdateFrame(CfUpdateFrame&&) = default;
  CfUpdateFrame& operator=(CfUpdateFrame&&) = default;

  void reset() override {
    CtlosFrame::reset();
    cfUpdate = CbfcCfUpdatePayload{};
  }

  // Both reports are always present on the wire: UEC Tbl 5-21 gives neither a
  // validity bit, so there is no encoding for a CF_Update naming one VC.
  CbfcCfUpdatePayload cfUpdate;
};

/**
 * An LLR Control Ordered Set on the wire: ACK, NACK, LLR_INIT or
 * LLR_INIT_ECHO (UEC §5.1.9).
 *
 * All four share this one class and are told apart by `payload.type`. A class
 * per type would buy nothing, because no code dispatches on the C++ type:
 * `LlrCtlosSlots` already arbitrates on the runtime type, and the receiver
 * passes it straight to `Llr::rxCtlos`.
 */
struct LlrCtlosFrame : CtlosFrame {
  explicit LlrCtlosFrame() : CtlosFrame(PacketType::CTLOS_LLR) {}
  ~LlrCtlosFrame() override = default;

  LlrCtlosFrame(const LlrCtlosFrame&) = default;
  LlrCtlosFrame& operator=(const LlrCtlosFrame&) = default;
  LlrCtlosFrame(LlrCtlosFrame&&) = default;
  LlrCtlosFrame& operator=(LlrCtlosFrame&&) = default;

  void reset() override {
    CtlosFrame::reset();
    payload = LlrCtlosPayload{};
  }

  LlrCtlosPayload payload;
};

struct IPConnection {
  FlexDeviceId srcId;
  FlexDeviceId dstId;
};

struct QPConnection : public IPConnection {
  QPConnection(
      const FlexDeviceId& srcId,
      const FlexDeviceId& dstId,
      uint32_t srcQPN,
      uint32_t dstQPN)
      : IPConnection(srcId, dstId), srcQPN(srcQPN), dstQPN(dstQPN) {}
  uint32_t srcQPN;
  uint32_t dstQPN;
};

struct Ipv6Packet : public EthernetPacket {
  explicit Ipv6Packet() : EthernetPacket(PacketType::IPV6, 0) {
    headerSize += Ipv6Packet::kIpv6HeaderSize;
  }

  explicit Ipv6Packet(uint16_t ps) : EthernetPacket(PacketType::IPV6, ps) {
    headerSize += Ipv6Packet::kIpv6HeaderSize;
  }

  explicit Ipv6Packet(PacketType pt, uint16_t ps) : EthernetPacket(pt, ps) {
    headerSize += Ipv6Packet::kIpv6HeaderSize;
  }
  ~Ipv6Packet() override = default;

  Ipv6Packet(const Ipv6Packet&) = default;
  Ipv6Packet& operator=(const Ipv6Packet&) = default;
  Ipv6Packet(Ipv6Packet&&) = default;
  Ipv6Packet& operator=(Ipv6Packet&&) = default;

  // Differentiated Services codepoints, carried in the upper six bits of the
  // Traffic Class byte: the class selectors of RFC 2474, the assured
  // forwarding classes of RFC 2597 and expedited forwarding from RFC 3246.
  enum class DscpType : uint8_t {
    DSCP_DEFAULT = 0,
    DSCP_CS1 = 8,
    DSCP_CS2 = 16,
    DSCP_CS3 = 24,
    DSCP_CS4 = 32,
    DSCP_CS5 = 40,
    DSCP_CS6 = 48,
    DSCP_CS7 = 56,
    // AF<class><drop precedence> is 8 * class + 2 * drop precedence.
    DSCP_AF11 = 10,
    DSCP_AF12 = 12,
    DSCP_AF13 = 14,
    DSCP_AF21 = 18,
    DSCP_AF22 = 20,
    DSCP_AF23 = 22,
    DSCP_AF31 = 26,
    DSCP_AF32 = 28,
    DSCP_AF33 = 30,
    DSCP_AF41 = 34,
    DSCP_AF42 = 36,
    DSCP_AF43 = 38,
    DSCP_EF = 46,
  };

  // Explicit Congestion Notification codepoints, carried in the low two bits
  // of the Traffic Class byte (RFC 3168 section 5).
  enum class EcnType : uint8_t {
    ECN_NOT_ECT = 0b00,
    ECN_ECT1 = 0b01,
    ECN_ECT0 = 0b10,
    ECN_CE = 0b11,
  };

  void setDscpFromCos(uint32_t cos);

  uint8_t getDscpVal() const;

  void reset() override {
    EthernetPacket::reset();
    headerSize += kIpv6HeaderSize;
    dscp = DscpType::DSCP_DEFAULT;
    ecn = EcnType::ECN_NOT_ECT;
    hopLimit = 255;
    conn = nullptr;
    socketLevelConn = nullptr;
  }

  const FlexDeviceId& srcId() const noexcept {
    return conn->srcId;
  }

  const FlexDeviceId& dstId() const noexcept {
    return conn->dstId;
  }

  static constexpr uint32_t kIpv6HeaderSize = 40;

  IPConnection const* conn{};
  IPConnection const* socketLevelConn{};
  DscpType dscp{DscpType::DSCP_DEFAULT};
  EcnType ecn{EcnType::ECN_NOT_ECT};
  uint8_t hopLimit{255};
};

struct UdpPacket : public Ipv6Packet {
  explicit UdpPacket() : Ipv6Packet(PacketType::UDP, 0) {
    headerSize += UdpPacket::kUdpHeaderSize;
  }

  explicit UdpPacket(uint16_t ps) : Ipv6Packet(PacketType::UDP, ps) {
    headerSize += UdpPacket::kUdpHeaderSize;
  }

  explicit UdpPacket(PacketType pt, uint16_t ps) : Ipv6Packet(pt, ps) {
    headerSize += UdpPacket::kUdpHeaderSize;
  }
  ~UdpPacket() override = default;

  UdpPacket(const UdpPacket&) = default;
  UdpPacket& operator=(const UdpPacket&) = default;
  UdpPacket(UdpPacket&&) = default;
  UdpPacket& operator=(UdpPacket&&) = default;

  static constexpr uint32_t kUdpHeaderSize = 8;

  void reset() override {
    Ipv6Packet::reset();
    headerSize += kUdpHeaderSize;
    srcPort = 0;
    dstPort = 0;
  }

  /**
   * The 0-based physical NIC port this packet's transport chose to send it
   * from, if that transport selects ports itself.
   *
   * Endpoint routing consults this before falling back to its own ECMP path: a
   * transport that picks an egress plane per packet has already decided, and
   * stamps the choice on the packets it builds. Answering on the packet is what
   * lets the endpoint stay ignorant of which packet types carry a choice.
   *
   * @return  The chosen egress NIC port, or nullopt if this packet carries no
   *          transport-level port selection.
   */
  [[nodiscard]] virtual std::optional<uint16_t> chosenEgressNicPort()
      const noexcept {
    return std::nullopt;
  }

  /**
   * The 0-based destination NIC port this packet's transport chose, if any.
   *
   * @return  The chosen destination NIC port, or nullopt to land on the same
   *          port number the packet egresses from.
   */
  [[nodiscard]] virtual std::optional<uint16_t> chosenDstNicPort()
      const noexcept {
    return std::nullopt;
  }

  uint16_t srcPort{0};
  uint16_t dstPort{0};
};

} // namespace packsim
