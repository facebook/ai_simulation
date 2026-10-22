/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/cbfc-credit-tracker.h"

#include <algorithm>
#include <stdexcept>

#include <fmt/format.h>

#include "packsim/link-layer/cbfc/cbfc-types.h"
#include "packsim/link-layer/cbfc/cbfc-validate.h"

namespace packsim::cbfc {

void CbfcCreditTracker::validateCreditSize(
    const int64_t creditSizeBytes,
    const std::string& context) {
  if (creditSizeBytes < 1 || creditSizeBytes > kMaxCreditSizeBytes) {
    throw std::invalid_argument(
        fmt::format(
            "{}: credit size {} B is outside 1..{}. S_CreditSize is a 12-bit "
            "wire field (Tbl 5-18), so a larger value cannot be advertised to "
            "the sender.",
            context,
            creditSizeBytes,
            kMaxCreditSizeBytes));
  }
}

CbfcCreditTracker::CbfcCreditTracker(uint8_t numVcs)
    : numVcs_(CbfcValidate::checkedNumVcs(numVcs)),
      rVcCc_(numVcs_, CreditCounter(kCreditWidthBits)),
      rVcCf_(numVcs_, CreditCounter(kFreedWidthBits)) {}

void CbfcCreditTracker::checkVc(uint8_t vc) const {
  CbfcValidate::validateVc(vc, numVcs_);
}

void CbfcCreditTracker::onStored(uint8_t vc, uint64_t pktCredits) {
  checkVc(vc);
  rVcCc_.at(vc).add(pktCredits);
}

void CbfcCreditTracker::onDrained(uint8_t vc, uint64_t pktCredits) {
  checkVc(vc);
  rVcCf_.at(vc).add(pktCredits);
}

CcUpdateResult CbfcCreditTracker::onCcUpdate(uint8_t vc, uint64_t sVcCc) {
  checkVc(vc);
  auto& rVcCc = rVcCc_.at(vc);
  // §5.2.5.7.4: LostCredits = S_VC_CC[x] - R_VC_CC[x], applied when it is > 0.
  // Modular subtraction over the counter's FULL range, not §5.2.4.2's
  // half-window: the sender counts a packet on transmit before the receiver
  // counts it on arrival, so S_VC_CC never falls behind R_VC_CC. See
  // `CreditCounter` for the general rule.
  const uint64_t lostCredits = rVcCc.forwardDistanceTo(sVcCc);
  if (lostCredits == 0) {
    return {}; // in sync; nothing to reconcile
  }
  // UEC 1.0.3 §5.2.5.7.4's Implementation Note calls for a report when
  // LostCredits is "less than zero or greater than 2^14-1". This one check
  // covers both: the subtraction is unsigned and modular, so a value that
  // would be negative in signed arithmetic comes back as a large positive
  // residue and trips the same bound.
  if (lostCredits > kMaxLostCredits) {
    // Rejection is sticky: R_VC_CC is not resynced to S_VC_CC, so the gap
    // persists and every later CC_Update on this VC repeats the same oversized
    // LostCredits. §5.2.5.7.4 prescribes no recovery, so none is implemented;
    // `ccUpdateLostCreditsOutOfRangeCount` is the signal that it happened.
    return {.lostCredits = 0, .lostCreditsOutOfRange = true};
  }
  rVcCf_.at(vc).add(lostCredits);
  rVcCc.set(sVcCc);
  return {.lostCredits = lostCredits, .lostCreditsOutOfRange = false};
}

uint64_t CbfcCreditTracker::freed(uint8_t vc) const {
  checkVc(vc);
  return rVcCf_.at(vc).value();
}

uint64_t CbfcCreditTracker::consumed(uint8_t vc) const {
  checkVc(vc);
  return rVcCc_.at(vc).value();
}

} // namespace packsim::cbfc
