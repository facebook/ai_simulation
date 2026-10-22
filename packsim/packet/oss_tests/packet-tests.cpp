/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <array>
#include <bitset>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <type_traits>

#include <folly/Utility.h>
#include <gtest/gtest.h>

#include "packsim/packet/allocator/GlobalManager.h"
#include "packsim/packet/base.h"
#include "packsim/packet/llr-ctlos-types.h"
#include "packsim/packet/ping.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/packet/roce.h"
#include "packsim/packet/type_mapper.h"

namespace packsim {
namespace {

TEST(PacketTest, MakeAPacket) {
  UdpPacket udpPkt(0);
  // In-buffer Ethernet header is 22 bytes (PHY-layer preamble + IFG are
  // wire-only — accounted for separately), IPv6 header is 40 bytes, UDP
  // header is 8 bytes — totaling 70. With zero payload that exceeds the
  // 64B Ethernet minimum so getSize() returns the natural sum.
  EXPECT_EQ(udpPkt.getSize(), 70);
  // PHY frame size adds the 8-byte preamble before the MAC frame → 78.
  EXPECT_EQ(udpPkt.getPhyFrameSize(), 78);
  // FIXME: should be 12 (standard Ethernet IFG), but is
  // forced to 0 to work around a multi-threaded PDES race in switch
  // buffer accounting. See EthernetPacket::getInterFrameGapSize() in
  // packet/base.h for the full explanation. When the race is fixed,
  // restore the expectation to 12.
  EXPECT_EQ(udpPkt.getInterFrameGapSize(), 0);
}

TEST(PacketTest, SetDscpFromCosMapsCosToDscpValue) {
  UdpPacket pkt(0);
  pkt.setDscpFromCos(0);
  EXPECT_EQ(pkt.getDscpVal(), 0x00);
  pkt.setDscpFromCos(3);
  EXPECT_EQ(pkt.getDscpVal(), 0x18); // DSCP_CS3
  pkt.setDscpFromCos(7);
  EXPECT_EQ(pkt.getDscpVal(), 0x38); // DSCP_CS7
}

TEST(PacketTest, SetDscpFromCosThrowsWhenCosOutOfRange) {
  UdpPacket pkt(0);
  EXPECT_THROW(pkt.setDscpFromCos(8), std::out_of_range);
}

TEST(PacketTest, PfcPacketPadsToMinEthernetFrame) {
  EthernetPfcPacket pfc;
  // headerSize 0 + payload 0, padded up to the 64B Ethernet minimum.
  EXPECT_EQ(pfc.getSize(), 64u);
}

TEST(PacketTest, LlrCtlosFrameIsAnEightByteOrderedSet) {
  LlrCtlosFrame ctlos;

  EXPECT_EQ(CtlosFrame::kCtlosFrameSizeBytes, ctlos.getSize());
}

TEST(PacketTest, Ipv6ResetRestoresDefaults) {
  UdpPacket pkt(50);
  pkt.setDscpFromCos(5);
  pkt.ecn = Ipv6Packet::EcnType::ECN_CE;
  pkt.hopLimit = 10;
  pkt.srcPort = 1234;

  pkt.reset();

  EXPECT_EQ(pkt.getDscpVal(), 0x00); // DSCP_DEFAULT
  EXPECT_EQ(pkt.ecn, Ipv6Packet::EcnType::ECN_NOT_ECT);
  EXPECT_EQ(pkt.hopLimit, 255);
  EXPECT_EQ(pkt.srcPort, 0);
  EXPECT_EQ(pkt.conn, nullptr);
}

TEST(PacketTest, EcnCodepointsFollowRfc3168) {
  EXPECT_EQ(folly::to_underlying(Ipv6Packet::EcnType::ECN_NOT_ECT), 0b00);
  EXPECT_EQ(folly::to_underlying(Ipv6Packet::EcnType::ECN_ECT1), 0b01);
  EXPECT_EQ(folly::to_underlying(Ipv6Packet::EcnType::ECN_ECT0), 0b10);
  EXPECT_EQ(folly::to_underlying(Ipv6Packet::EcnType::ECN_CE), 0b11);
}

TEST(PacketTest, PfcResetRestoresPauseQuanta) {
  EthernetPfcPacket pfc;
  pfc.pfcClassEnable = 0xFF;
  pfc.pfcClassPauseQuanta.fill(0);

  pfc.reset();

  EXPECT_EQ(pfc.pfcClassEnable.to_ulong(), 0u);
  for (const uint16_t quanta : pfc.pfcClassPauseQuanta) {
    EXPECT_EQ(quanta, 65535);
  }
}

// ── PHY framing ──────────────────────────────────────────────────────────────
//
// getPhyFrameSize() and getInterFrameGapSize() feed the link-layer's txTime
// and txCompleteTime directly, so a wrong answer skews every serialization
// time in the simulation without anything failing. MakeAPacket pins the UDP
// case; these cover the base class and the LLR split.

// The base Packet has no PHY overhead: the defaults are the identity, which is
// what every non-Ethernet packet type inherits. Asserted against the concrete
// 100 rather than against getSize(), which is what the default returns -- that
// comparison is the base implementation restated and cannot fail.
TEST(PacketTest, ABarePacketHasNoPhyOverhead) {
  const Packet pkt(/*ps=*/100);

  EXPECT_EQ(pkt.getPhyFrameSize(), 100u);
  EXPECT_EQ(pkt.getInterFrameGapSize(), 0u);
}

// Concrete counts, not `getSize() + kPreambleSize`: re-deriving the expected
// value with the implementation's own expression would still pass if the
// framing constants or the formula changed together. 22-byte header + 100-byte
// payload = 122, plus the 8-byte preamble = 130. Same numbers as MakeAPacket
// above, which pins 70/78 for the UDP case.
TEST(PacketTest, AnEthernetFrameAddsThePreambleToItsPhySize) {
  const EthernetPacket eth(100);

  EXPECT_EQ(eth.getSize(), 122u);
  EXPECT_EQ(eth.getPhyFrameSize(), 130u);
}

// An ordered set preempts the data stream rather than riding it, so it gets
// neither a preamble nor an inter-frame gap of its own.
TEST(PacketTest, AnLlrCtlosFrameGetsNoPreambleOrGap) {
  LlrCtlosFrame ctlos;

  // Only the PHY numbers are asserted here, because
  // LlrCtlosFrameIsAnEightByteOrderedSet already pins getSize() against the
  // named constant.
  EXPECT_EQ(CtlosFrame::kCtlosFrameSizeBytes, ctlos.getPhyFrameSize());
  EXPECT_EQ(0u, ctlos.getInterFrameGapSize());
}

// An LLR DATA frame is an ordinary Ethernet frame that happens to carry a
// sequence number in its preamble, so enabling LLR must not change any of its
// framing numbers.
TEST(PacketTest, AnLlrDataFrameKeepsNormalEthernetFraming) {
  EthernetPacket eth(100);
  eth.llrHeader.enabled = true;
  eth.llrHeader.seqNum = 42;

  EXPECT_EQ(eth.getSize(), 122u);
  EXPECT_EQ(eth.getPhyFrameSize(), 130u);
}

// FIXME: the IFG is forced to 0 for every Ethernet frame to work
// around a multi-threaded PDES race in switch buffer accounting; see
// EthernetPacket::getInterFrameGapSize() in packet/base.h. Pinned here so the
// workaround is visible and a change to it is deliberate.
//
// Once the race is fixed the gap becomes kIfgSize and this test fails, which
// is exactly when it should.
TEST(PacketTest, EveryEthernetFrameCurrentlyReportsNoInterFrameGap) {
  EthernetPacket ordinary(100);

  EXPECT_EQ(ordinary.getInterFrameGapSize(), 0u);
}

// ── Recycling ────────────────────────────────────────────────────────────────

// Packets are pooled and reused, so reset() has to clear LLR state too --
// otherwise a recycled frame is acknowledged under the previous user's
// sequence number.
TEST(PacketTest, ResetClearsStaleLlrState) {
  EthernetPacket eth(100);
  eth.llrHeader.enabled = true;
  eth.llrHeader.seqNum = 42;

  eth.reset();

  EXPECT_FALSE(eth.llrHeader.enabled);
  EXPECT_EQ(eth.llrHeader.seqNum, 0u);
  // reset() also clears the payload, so the frame falls to the 64-byte
  // Ethernet minimum (the 22-byte header alone is under it) and the PHY size
  // is that plus the 8-byte preamble.
  EXPECT_EQ(eth.getSize(), 64u);
  EXPECT_EQ(eth.getPhyFrameSize(), 72u);
}

// Both halves of Packet::reset() are exercised: headerSize is set first,
// because a freshly constructed Packet already has it at 0 and the reset would
// otherwise be verified on the payload alone.
TEST(PacketTest, ResetClearsTheBasePacketSizes) {
  Packet pkt(/*ps=*/100);
  pkt.headerSize = 20;
  ASSERT_EQ(pkt.getSize(), 120u);

  pkt.reset();

  EXPECT_EQ(pkt.getSize(), 0u);
}

TEST(PacketTest, CcUpdatePacketIsMinimumEthernetFrame) {
  EthernetCcPacket cc;
  // Zero-payload MAC control frame, padded to the Ethernet minimum.
  EXPECT_EQ(EthernetPacket::kMinEthernetFrameSizeBytes, cc.getSize());
  // A CC_Update is a MAC frame, not a PCS ordered set, so it carries a
  // preamble.
  EXPECT_EQ(
      EthernetPacket::kMinEthernetFrameSizeBytes +
          EthernetPacket::kPreambleSize,
      cc.getPhyFrameSize());
}

// A recycled packet that kept a stale msgType would report the wrong VC range
// and reconcile the wrong counters.
TEST(PacketTest, CcUpdateResetRestoresEveryFieldItOwns) {
  EthernetCcPacket cc;
  cc.msgType = CbfcCcMsgType::VC_16_31;
  cc.sVcCc[3] = 800008; // a 20-bit S_VC_CC value
  cc.heldCredits = 4;

  cc.reset();

  EXPECT_EQ(CbfcCcMsgType::VC_0_15, cc.msgType);
  EXPECT_EQ(0u, cc.sVcCc[3]);
  EXPECT_EQ(0u, cc.heldCredits);
  // Still a full MAC control frame, not an 8-byte CtlOS.
  EXPECT_EQ(EthernetPacket::kMinEthernetFrameSizeBytes, cc.getSize());
}

// Packets are pooled. One that kept a prior admission's held count would
// release credits the switch no longer holds for it -- a credit double-free.
TEST(PacketTest, ResetClearsTheHeldCredits) {
  EthernetPacket pkt;
  pkt.heldCredits = 10;

  pkt.reset();

  EXPECT_EQ(0u, pkt.heldCredits);
}

TEST(PacketTest, CfUpdateFrameIsAnEightByteOrderedSet) {
  CfUpdateFrame ctlos;
  ctlos.cfUpdate =
      CbfcCfUpdatePayload{.cf1 = 123, .cf2 = 14203, .vc1 = 4, .vc2 = 11};

  // A CtlOS is a PCS ordered set, not a MAC frame: no Ethernet framing, no
  // preamble, no IFG. The payload rides inside the 8 bytes rather than adding
  // to them.
  EXPECT_EQ(CtlosFrame::kCtlosFrameSizeBytes, ctlos.getSize());
  EXPECT_EQ(CtlosFrame::kCtlosFrameSizeBytes, ctlos.getPhyFrameSize());
  // `CtlosFrame` inherits `Packet`'s 0 default, so unlike `EthernetPacket`'s 0
  // it does not depend on the forced-IFG workaround in
  // `EthernetPacket::getInterFrameGapSize()`.
  EXPECT_EQ(0u, ctlos.getInterFrameGapSize());
}

TEST(PacketTest, CfUpdateFrameSortsBelowTheIpv6Boundary) {
  CfUpdateFrame ctlos;
  EXPECT_EQ(PacketType::CTLOS_CF_UPDATE, ctlos.pktType);
  EXPECT_LT(PacketType::CTLOS_CF_UPDATE, PacketType::IPV6)
      << "CTLOS_CF_UPDATE must sort below the IPv6 boundary; DscpQueueFilter relies on it";
}

TEST(PacketTest, LlrCtlosFrameSortsBelowTheIpv6Boundary) {
  LlrCtlosFrame ctlos;
  EXPECT_EQ(PacketType::CTLOS_LLR, ctlos.pktType);
  EXPECT_LT(PacketType::CTLOS_LLR, PacketType::IPV6)
      << "CTLOS_LLR must sort below the IPv6 boundary; DscpQueueFilter relies on it";
}

TEST(PacketTest, LlrCtlosFrameResetClearsEveryField) {
  LlrCtlosFrame ctlos;
  ctlos.payload = {LlrCtlosType::LLR_INIT_ECHO, /*seq=*/4095, /*initData=*/7};

  ctlos.reset();

  EXPECT_EQ(LlrCtlosType::LLR_ACK, ctlos.payload.type);
  EXPECT_EQ(0u, ctlos.payload.seq);
  EXPECT_EQ(0u, ctlos.payload.initData);
  EXPECT_EQ(CtlosFrame::kCtlosFrameSizeBytes, ctlos.getSize());
}

TEST(PacketTest, LlrCtlosPoolRoundTripYieldsAFreshFrame) {
  auto* returned = GlobalManager::takePacket<PacketType::CTLOS_LLR>();
  ASSERT_NE(returned, nullptr);
  returned->payload = {LlrCtlosType::LLR_NACK, /*seq=*/77, /*initData=*/3};
  returned->heldCredits = 5;
  GlobalManager::returnPacket(returned);

  auto pkt = createPacket<PacketType::CTLOS_LLR>();
  auto* fresh = pkt.as<PacketType::CTLOS_LLR>();

  // The identity check comes first for the reason given in
  // `CcUpdatePoolRoundTripYieldsAFreshFrame`.
  EXPECT_EQ(returned, fresh) << "the pool did not hand back the recycled frame";
  EXPECT_EQ(PacketType::CTLOS_LLR, fresh->pktType);
  EXPECT_EQ(LlrCtlosType::LLR_ACK, fresh->payload.type);
  EXPECT_EQ(0u, fresh->payload.seq);
  EXPECT_EQ(0u, fresh->payload.initData);
  EXPECT_EQ(0u, fresh->heldCredits);
}

TEST(PacketTest, CtlosBaseFixesTheSizeForEverySubclass) {
  CfUpdateFrame cf;
  const CtlosFrame& asBase = cf;
  EXPECT_EQ(CtlosFrame::kCtlosFrameSizeBytes, asBase.getSize());
  EXPECT_EQ(cf.getSize(), asBase.getSize());
  static_assert(
      std::is_base_of_v<CtlosFrame, CfUpdateFrame>,
      "CfUpdateFrame must derive from the CtlOS base");
  static_assert(
      std::is_base_of_v<CtlosFrame, LlrCtlosFrame>,
      "LlrCtlosFrame must derive from the CtlOS base");
}

TEST(PacketTest, MacControlBaseFixesTheSizeForEverySubclass) {
  EthernetPfcPacket pfc;
  EthernetCcPacket cc;
  const MacControlFrame& pfcAsBase = pfc;
  const MacControlFrame& ccAsBase = cc;
  EXPECT_EQ(EthernetPacket::kMinEthernetFrameSizeBytes, pfcAsBase.getSize());
  EXPECT_EQ(EthernetPacket::kMinEthernetFrameSizeBytes, ccAsBase.getSize());
  EXPECT_EQ(pfc.getSize(), pfcAsBase.getSize());
  EXPECT_EQ(cc.getSize(), ccAsBase.getSize());
  static_assert(
      std::is_base_of_v<MacControlFrame, EthernetPfcPacket>,
      "EthernetPfcPacket must derive from the MAC control base");
  static_assert(
      std::is_base_of_v<MacControlFrame, EthernetCcPacket>,
      "EthernetCcPacket must derive from the MAC control base");
}

// `PacketTypeMatcher` answers "is this packet a `T`?" by comparing the runtime
// `PacketType` against the enum range a family occupies, where the code it
// replaces asked `dynamic_cast`. Nothing in the type system ties the two
// together: a packet type declared at the wrong point in the enum still
// compiles, and makes the matcher answer wrong. The tests below are what ties
// them, by running both against a real instance of every mapped type.

// `dynamic_cast` is the reference answer. The matcher and the three public
// entry points built on it must each return exactly what it returns.
template <typename Base, typename Concrete>
void expectAgreesWithDynamicCast() {
  Concrete pkt;
  Packet& asPacket = pkt;
  const bool derives = dynamic_cast<Base*>(&asPacket) != nullptr;
  const auto pktType = folly::to_underlying(asPacket.pktType);

  EXPECT_EQ(derives, PacketTypeMatcher<Base>::matches(asPacket.pktType))
      << "PacketTypeMatcher disagrees with the hierarchy for pktType "
      << pktType;
  EXPECT_EQ(derives, packetIs<Base>(asPacket))
      << "packetIs disagrees with the hierarchy for pktType " << pktType;

  // The pooled packet is what production holds, and it reaches the matcher
  // through `PacketPtr` rather than directly, so it is checked here too.
  const PacketPtr pooled = createPacket<PacketEnumMapper<Concrete>::value>();
  EXPECT_EQ(derives, pooled.is<Base>())
      << "PacketPtr::is disagrees with the hierarchy for pktType " << pktType;
  EXPECT_EQ(derives, pooled.tryAs<Base>() != nullptr)
      << "PacketPtr::tryAs disagrees with the hierarchy for pktType "
      << pktType;
}

// Against every mapped type, not only the family bases: a base checks that its
// range is wide enough, and a leaf checks that the ranges around it are not too
// wide. Only both together catch a type inserted inside another's range.
template <typename Concrete>
void expectAgreesForEveryMappedBase() {
  expectAgreesWithDynamicCast<Packet, Concrete>();
  expectAgreesWithDynamicCast<CtlosFrame, Concrete>();
  expectAgreesWithDynamicCast<CfUpdateFrame, Concrete>();
  expectAgreesWithDynamicCast<LlrCtlosFrame, Concrete>();
  expectAgreesWithDynamicCast<EthernetPacket, Concrete>();
  expectAgreesWithDynamicCast<MacControlFrame, Concrete>();
  expectAgreesWithDynamicCast<EthernetPfcPacket, Concrete>();
  expectAgreesWithDynamicCast<EthernetCcPacket, Concrete>();
  expectAgreesWithDynamicCast<Ipv6Packet, Concrete>();
  expectAgreesWithDynamicCast<UdpPacket, Concrete>();
  expectAgreesWithDynamicCast<RocePacket, Concrete>();
  expectAgreesWithDynamicCast<PingPacket, Concrete>();
}

TEST(PacketTest, MatcherAgreesWithDynamicCast) {
  expectAgreesForEveryMappedBase<Packet>();
  expectAgreesForEveryMappedBase<CfUpdateFrame>();
  expectAgreesForEveryMappedBase<LlrCtlosFrame>();
  expectAgreesForEveryMappedBase<EthernetPacket>();
  expectAgreesForEveryMappedBase<EthernetPfcPacket>();
  expectAgreesForEveryMappedBase<EthernetCcPacket>();
  expectAgreesForEveryMappedBase<Ipv6Packet>();
  expectAgreesForEveryMappedBase<UdpPacket>();
  expectAgreesForEveryMappedBase<RocePacket>();
  expectAgreesForEveryMappedBase<PingPacket>();
}

TEST(PacketTest, CfUpdateFrameResetClearsThePayload) {
  CfUpdateFrame ctlos;
  ctlos.cfUpdate = CbfcCfUpdatePayload{.cf1 = 9, .cf2 = 8, .vc1 = 7, .vc2 = 6};

  ctlos.reset();

  EXPECT_EQ(0u, ctlos.cfUpdate.cf1);
  EXPECT_EQ(0u, ctlos.cfUpdate.cf2);
  EXPECT_EQ(0u, ctlos.cfUpdate.vc1);
  EXPECT_EQ(0u, ctlos.cfUpdate.vc2);
  EXPECT_EQ(CtlosFrame::kCtlosFrameSizeBytes, ctlos.getSize());
}

// Stack-constructed frames skip the production path: `createPacket` draws from
// the pool keyed on the mapped C++ type, calls `reset()`, then assigns
// `pktType` and `payloadSize`. Only a round trip exercises the
// `PacketTypeMapper` / `PacketEnumMapper` entries and the pool binding at
// runtime.
TEST(PacketTest, CcUpdatePoolRoundTripYieldsAFreshFrame) {
  auto* returned = GlobalManager::takePacket<PacketType::ETHERNET_CC>();
  ASSERT_NE(returned, nullptr);
  returned->msgType = CbfcCcMsgType::VC_16_31;
  returned->sVcCc[3] = 800008;
  returned->heldCredits = 4;
  // A CC_Update is pulled through the LLR data path and comes back stamped
  // (UEC §5.2.7.2), and the pool is shared with LLR-disabled ports, so a
  // recycled frame must not keep the previous port's sequence number.
  returned->llrHeader = LlrHeader{.enabled = true, .seqNum = 99};
  GlobalManager::returnPacket(returned);

  auto pkt = createPacket<PacketType::ETHERNET_CC>();
  auto* fresh = pkt.as<PacketType::ETHERNET_CC>();

  // The identity check is what makes the assertions below cover recycling
  // rather than a fresh allocation.
  EXPECT_EQ(returned, fresh) << "the pool did not hand back the recycled frame";
  EXPECT_EQ(PacketType::ETHERNET_CC, fresh->pktType);
  EXPECT_EQ(CbfcCcMsgType::VC_0_15, fresh->msgType);
  EXPECT_EQ(0u, fresh->sVcCc[3]);
  EXPECT_EQ(0u, fresh->heldCredits);
  EXPECT_FALSE(fresh->llrHeader.enabled);
  EXPECT_EQ(0u, fresh->llrHeader.seqNum);
  EXPECT_EQ(EthernetPacket::kMinEthernetFrameSizeBytes, fresh->getSize());
}

TEST(PacketTest, CfUpdatePoolRoundTripYieldsAFreshFrame) {
  auto* returned = GlobalManager::takePacket<PacketType::CTLOS_CF_UPDATE>();
  ASSERT_NE(returned, nullptr);
  returned->cfUpdate =
      CbfcCfUpdatePayload{.cf1 = 123, .cf2 = 14203, .vc1 = 4, .vc2 = 11};
  returned->heldCredits = 9;
  GlobalManager::returnPacket(returned);

  auto pkt = createPacket<PacketType::CTLOS_CF_UPDATE>();
  auto* fresh = pkt.as<PacketType::CTLOS_CF_UPDATE>();

  // Identity first: see `CcUpdatePoolRoundTripYieldsAFreshFrame`.
  EXPECT_EQ(returned, fresh) << "the pool did not hand back the recycled frame";
  EXPECT_EQ(PacketType::CTLOS_CF_UPDATE, fresh->pktType);
  EXPECT_EQ(0u, fresh->cfUpdate.cf1);
  EXPECT_EQ(0u, fresh->cfUpdate.cf2);
  EXPECT_EQ(0u, fresh->cfUpdate.vc1);
  EXPECT_EQ(0u, fresh->cfUpdate.vc2);
  EXPECT_EQ(0u, fresh->heldCredits);
}

// `createPacket` assigns `payloadSize` after `reset()`, bypassing the
// constructor, so only the `final` `getSize()` keeps a caller from turning a
// 64-byte MAC control frame into a 1500-byte one. UEC Tbl 5-22 fixes the
// CC_Update at 64 bytes.
TEST(PacketTest, CcUpdateFromThePoolIgnoresTheRequestedPayloadSize) {
  auto pkt = createPacket<PacketType::ETHERNET_CC>(/* payloadSize= */ 1500);
  auto* cc = pkt.as<PacketType::ETHERNET_CC>();

  EXPECT_EQ(1500u, cc->payloadSize) << "createPacket still records it";
  EXPECT_EQ(EthernetPacket::kMinEthernetFrameSizeBytes, cc->getSize());
  // The PHY size adds the preamble to the pinned size, not to the requested
  // payload.
  EXPECT_EQ(
      EthernetPacket::kMinEthernetFrameSizeBytes +
          EthernetPacket::kPreambleSize,
      cc->getPhyFrameSize());
}

// An ordered set is fixed-width: `createPacket` assigns `payloadSize`
// unconditionally, and `CtlosFrame`'s `final` `getSize()` is what stops that
// from changing what the frame reports on the wire.
TEST(PacketTest, CfUpdateFromThePoolIgnoresTheRequestedPayloadSize) {
  auto pkt = createPacket<PacketType::CTLOS_CF_UPDATE>(/* payloadSize= */ 64);
  auto* frame = pkt.as<PacketType::CTLOS_CF_UPDATE>();

  EXPECT_EQ(64u, frame->payloadSize) << "createPacket still records it";
  EXPECT_EQ(CtlosFrame::kCtlosFrameSizeBytes, frame->getSize());
  EXPECT_EQ(CtlosFrame::kCtlosFrameSizeBytes, frame->getPhyFrameSize())
      << "an ordered set carries no preamble";
  EXPECT_EQ(0u, frame->getInterFrameGapSize())
      << "an ordered set carries no inter-frame gap";
}

} // namespace
} // namespace packsim
