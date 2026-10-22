/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/cbfc-sender-ledger.h"

#include <algorithm>
#include <stdexcept>

#include <fmt/format.h>

#include "packsim/link-layer/cbfc/cbfc-validate.h"

namespace packsim::cbfc {

CbfcSenderLedger::CbfcSenderLedger(uint8_t numVcs)
    : numVcs_(CbfcValidate::checkedNumVcs(numVcs)),
      sPCu_(kCreditWidthBits),
      sVcCc_(numVcs_, CreditCounter(kCreditWidthBits)),
      sVcCf_(numVcs_, CreditCounter(kFreedWidthBits)),
      sVcCu_(numVcs_, CreditCounter(kCreditWidthBits)),
      vcMin_(numVcs_, 0),
      vcMinUsed_(numVcs_, 0),
      vcSharedUsed_(numVcs_, 0),
      vcGrp_(numVcs_, std::nullopt),
      vcGrpLimit_(kLedgerMaxVcGroups, 0),
      vcGrpSharedUsed_(kLedgerMaxVcGroups, 0) {}

void CbfcSenderLedger::setPortCreditLimit(uint64_t totalCredits) {
  // The pool bounds two gaps -- §5.2.4.2's half-modulus window on
  // S_VC_CC/R_VC_CC, and how far R_VC_CF can advance between two received
  // CF_Updates (see the @throw notes in the header) -- but only the freed
  // counter needs a runtime check. It is the tighter bound, and the only one
  // whose failure is unrecoverable: a full-modulus R_VC_CF advance reads as
  // "freed nothing" in `onCfUpdate`, and the §5.2.5.7.4 reconcile compares the
  // CONSUMED counters, which still agree, so nothing detects the loss.
  //
  // Rejecting here is deliberately stricter than Tbl 5-27's cap, and the bound
  // sits far above realistic per-port headroom pools: 2^15 credits of 64 B is
  // 2 MiB.
  static_assert(
      kFreedWidthBits < kCreditWidthBits,
      "the freed counter is assumed to be the tighter bound; if that stops "
      "being true, the consumed counter's half-modulus needs its own check");
  if (totalCredits > kMaxPortCreditLimit) {
    throw std::invalid_argument(
        fmt::format(
            "CBFC port credit limit {} is at or above the {}-bit freed-counter "
            "modulus ({}); a VC holding that many outstanding credits can "
            "advance R_VC_CF by a full modulus between two received "
            "CF_Updates, which reads as no advance at all and loses the "
            "credits with no recovery path.",
            totalCredits,
            int{kFreedWidthBits},
            kMaxPortCreditLimit + 1));
  }
  sPCl_ = totalCredits;
}

void CbfcSenderLedger::checkVc(uint8_t vc) const {
  CbfcValidate::validateVc(vc, numVcs_);
}

void CbfcSenderLedger::assignVcGroup(uint8_t vc, std::optional<uint8_t> grp) {
  checkVc(vc);
  if (grp.has_value() && *grp >= kLedgerMaxVcGroups) {
    throw std::invalid_argument(
        fmt::format(
            "CBFC VC {} is assigned to VC group {}, outside 0..{}.",
            int{vc},
            int{*grp},
            int{kLedgerMaxVcGroups} - 1));
  }
  vcGrp_.at(vc) = grp;
}

void CbfcSenderLedger::setVcGrpLimit(uint8_t grp, uint64_t credits) {
  if (grp >= kLedgerMaxVcGroups) {
    throw std::invalid_argument(
        fmt::format(
            "CBFC VC group {} is outside 0..{}.",
            int{grp},
            int{kLedgerMaxVcGroups} - 1));
  }
  vcGrpLimit_.at(grp) = credits;
}

void CbfcSenderLedger::setVcMinCredits(uint8_t vc, uint64_t credits) {
  checkVc(vc);
  auto& vcMin = vcMin_.at(vc);
  vcMinTotal_ = vcMinTotal_ - vcMin + credits;
  vcMin = credits;
}

void CbfcSenderLedger::setOvershootReserve(uint64_t credits) noexcept {
  overshootReserve_ = credits;
}

uint64_t CbfcSenderLedger::portTransmitLimit() const noexcept {
  const uint64_t limit = sPCl_.value_or(0);
  return limit > overshootReserve_ ? limit - overshootReserve_ : 0;
}

uint64_t CbfcSenderLedger::sharedCreditLimit() const noexcept {
  const uint64_t limit = sPCl_.value_or(0);
  const uint64_t reserved = vcMinTotal_ + overshootReserve_;
  return limit > reserved ? limit - reserved : 0;
}

bool CbfcSenderLedger::reservationsFit() const noexcept {
  // An unconfigured pool is not a fitting one. Reported here rather than from
  // `canTransmit` so it surfaces once at init, where the caller can name the
  // link, instead of as an unexplained per-packet refusal.
  return sPCl_.has_value() && vcMinTotal_ + overshootReserve_ <= *sPCl_;
}

uint64_t CbfcSenderLedger::sharedShareOf(uint8_t vc, uint64_t pktCredits)
    const {
  // Clamped: shrinking a VC's reserve below its live usage is a documented
  // hazard (see the CONTRACT block in the header), and an unsigned wrap here
  // would make `dedicatedRoom` enormous, drive `fromShared` to 0 and silently
  // switch that VC's shared and group gating off.
  const uint64_t vcMinUsed = vcMinUsed_.at(vc);
  const uint64_t vcMin = vcMin_.at(vc);
  const uint64_t dedicatedRoom = vcMin > vcMinUsed ? vcMin - vcMinUsed : 0;
  return pktCredits > dedicatedRoom ? pktCredits - dedicatedRoom : 0;
}

bool CbfcSenderLedger::canTransmit(uint8_t vc, uint64_t pktCredits) const {
  // With no groups assigned and every vcMin_ at 0 the three tiers reduce to
  // exactly the single-pool gate `S_P_CU + c <= S_P_CL`.
  //
  // The port limit binds first even for a packet a VC's own reserve covers:
  // reserves that oversubscribe the pool would otherwise let every VC draw its
  // full reserve and collectively exceed `S_P_CL`, overrunning the receiver.
  // §5.2.5.6.1 allows the extra tiers but is explicit that "the only
  // requirement is that the S_P_CL limit shall be observed". Where
  // `reservationsFit()` holds this clause cannot fire.
  if (sPCu_.value() + pktCredits > portTransmitLimit()) {
    return false;
  }
  const uint64_t fromShared = sharedShareOf(vc, pktCredits);
  if (fromShared == 0) {
    return true; // covered entirely by this VC's own reserve
  }
  const auto grp = vcGrp_.at(vc);
  if (grp.has_value() &&
      vcGrpSharedUsed_.at(*grp) + fromShared > vcGrpLimit_.at(*grp)) {
    return false;
  }
  // Both the group clause above and this one must hold; that is what makes
  // oversubscribed group limits safe.
  return sharedUsed_ + fromShared <= sharedCreditLimit();
}

void CbfcSenderLedger::onTransmit(uint8_t vc, uint64_t pktCredits) {
  sPCu_.add(pktCredits);
  sVcCc_.at(vc).add(pktCredits);
  sVcCu_.at(vc).add(pktCredits);
  // Consume precedence: a VC spends its own reserve first, and only what that
  // reserve does not cover is taken from its VC group's shared credit.
  const uint64_t fromShared = sharedShareOf(vc, pktCredits);
  vcMinUsed_.at(vc) += pktCredits - fromShared;
  vcSharedUsed_.at(vc) += fromShared;
  sharedUsed_ += fromShared;
  const auto grp = vcGrp_.at(vc);
  if (grp.has_value()) {
    vcGrpSharedUsed_.at(*grp) += fromShared;
  }
}

CfUpdateResult CbfcSenderLedger::onCfUpdate(uint8_t vc, uint64_t rVcCf) {
  auto& sVcCf = sVcCf_.at(vc);
  // Mask first: `rVcCf` arrives raw off the wire in a 16-bit field while the
  // counter is 15 bits, so an over-width value would fail the equality check
  // below and fall through as an apparent advance, for what is actually a
  // routine CF_max repeat.
  const uint64_t reported = sVcCf.masked(rVcCf);
  // A repeat of the last applied value is the routine CF_max refresh of a VC
  // that has freed nothing since: no credits to return, and not an error.
  //
  // An advance of exactly 2^15 credits aliases to that same value and is
  // indistinguishable from it -- the 15 transmitted bits carry no wrap
  // indicator, and UEC defines no out-of-band one. Only
  // the port credit limit makes such an advance unreachable; see
  // `setPortCreditLimit`.
  if (reported == sVcCf.value()) {
    return {};
  }
  // §5.2.5.6.3 verbatim: credits_returned = R_VC_CF - S_VC_CF, then
  // S_VC_CF = R_VC_CF -- plain modular subtraction over the counter's FULL
  // range. §5.2.4.2's half-window rule deliberately does not apply: it resolves
  // direction only when direction is unknown. CBFC is point-to-point, so the
  // wire is FIFO and S_VC_CF only ever holds a previously received R_VC_CF, a
  // cumulative count that never decreases; every residue in 0..2^15-1 is
  // therefore an unambiguous forward advance.
  const uint64_t creditsReturned = sVcCf.forwardDistanceTo(reported);
  sVcCf.set(reported);
  // Return precedence mirrors consume: while a VC still holds shared credit,
  // a return frees that first, from both the VC and its VC group, and only once
  // its shared usage is zero does a return release its reserve credit.
  // Shared first also unblocks other VCs soonest, since a VC holding its own
  // reserve blocks nobody. Clamped because the receiver's R_VC_CF is
  // authoritative and may legitimately outrun this local split after a
  // CC_Update reconcile.
  auto& vcSharedUsed = vcSharedUsed_.at(vc);
  const uint64_t fromShared = std::min(creditsReturned, vcSharedUsed);
  vcSharedUsed -= fromShared;
  sharedUsed_ -= fromShared;
  // The VC group half of the return precedence described above. Uses the
  // already-clamped `fromShared`, so it cannot underflow.
  const auto grp = vcGrp_.at(vc);
  if (grp.has_value()) {
    vcGrpSharedUsed_.at(*grp) -= fromShared;
  }
  auto& vcMinUsed = vcMinUsed_.at(vc);
  const uint64_t fromMin = std::min(creditsReturned - fromShared, vcMinUsed);
  vcMinUsed -= fromMin;
  // Decrement the in-use counters by what the tiers actually gave back, not by
  // what the receiver reported. S_P_CU is the sum of the per-tier usages across
  // VCs, so on an over-return -- where R_VC_CF outruns this side's split --
  // subtracting the raw `creditsReturned` would drop the port counter below
  // that sum and let the gate admit credits still in flight. Saturating as
  // well, so a wrapped counter can never read as a full port and stall the
  // link.
  const uint64_t released = fromShared + fromMin;
  sPCu_.subSaturating(released);
  sVcCu_.at(vc).subSaturating(released);
  return {.creditsReturned = creditsReturned};
}

uint64_t CbfcSenderLedger::consumed(uint8_t vc) const {
  return sVcCc_.at(vc).value();
}

uint64_t CbfcSenderLedger::outstandingVc(uint8_t vc) const {
  return sVcCu_.at(vc).value();
}

uint64_t CbfcSenderLedger::outstandingPort() const noexcept {
  return sPCu_.value();
}

} // namespace packsim::cbfc
