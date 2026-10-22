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

#include "packsim/link-layer/cbfc/credit-counter.h"

namespace packsim::cbfc {

// The largest LostCredits value a CC_Update may carry. LostCredits is the gap
// between the credits the sender says it has consumed and the credits the
// receiver counted; see `onCcUpdate`. UEC 1.0.3 §5.2.5.7.4's Implementation
// Note declares any value above 2^14-1 a serious error, so that is the cap.
inline constexpr uint64_t kMaxLostCredits = (uint64_t{1} << 14) - 1;

// The spec cap must stay inside the freed counter's unambiguous half-range,
// or a reconcile could hand back more than a CF_Update can report.
static_assert(kMaxLostCredits < (uint64_t{1} << (kFreedWidthBits - 1)));

/**
 * The outcome of reconciling one sender credit count from a CC_Update
 * (UEC 1.0.3 §5.2.5.7.4).
 */
struct CcUpdateResult {
  // How many credits the reconcile handed back to the sender. This is zero
  // when the two sides already agreed, and also zero when the update was
  // rejected for being out of range.
  uint64_t lostCredits = 0;
  // True when the computed LostCredits exceeded `kMaxLostCredits` and the
  // update was therefore rejected. Neither counter moved, so those credits are
  // forfeited for the rest of the run.
  bool lostCreditsOutOfRange = false;
};

/**
 * The receiver's half of CBFC credit accounting (UEC 1.0.3 §5.2.5.7;
 * Tbl 5-16/5-18).
 *
 * CBFC (Credit-Based Flow Control) lets a sender transmit only while the
 * receiver has advertised buffer space for it. That space is measured in
 * credits, and each virtual channel (VC) is accounted for separately. This
 * class holds the receiver's two counters for every VC:
 *
 *   - a consumed count (R_VC_CC), incremented when a packet is stored;
 *   - a freed count (R_VC_CF), incremented when a packet drains out again.
 *
 * The receiver buffer drives both counters through `ICreditObserver`. The
 * CF_Update generator reads the freed count to tell the sender how many
 * credits it may take back, and `onCcUpdate` reconciles the two sides so that
 * credits lost in transit are recovered rather than stranded.
 *
 * This class is pure arithmetic: it owns no simulation state and schedules
 * nothing.
 */
class CbfcCreditTracker {
 public:
  /**
   * Build the tracker for a link carrying `numVcs` virtual channels.
   *
   * @param numVcs  How many virtual channels this link has.
   * @throw std::invalid_argument if `numVcs` is outside 1..`kUecMaxVcs`.
   */
  explicit CbfcCreditTracker(uint8_t numVcs);

  /**
   * Check that a credit size can be advertised to the link partner.
   *
   * Called twice per link, because the size can change in between: once
   * against the size written in the config, and again at bootstrap against the
   * size resolved from the receiver's buffer cell. A config size of 0 means
   * "derive it", so the first call never sees the size actually used.
   *
   * @param creditSizeBytes  Bytes per credit on this link. Always a real size,
   *                         never the 0 that means "derive it".
   * @param context          Names the config or link in the exception message.
   *
   * @throw std::invalid_argument if `creditSizeBytes` is outside
   *        1..`kMaxCreditSizeBytes`, the range the 12-bit `S_CreditSize` wire
   *        field can carry.
   */
  static void validateCreditSize(
      int64_t creditSizeBytes,
      const std::string& context);

  /**
   * Record that a packet was stored into the receiver buffer (§5.2.5.7.1).
   *
   * @param vc          The virtual channel the packet arrived on.
   * @param pktCredits  How many credits the packet occupies. Added to the
   *                    consumed count for that VC.
   */
  void onStored(uint8_t vc, uint64_t pktCredits);

  /**
   * Record that a packet drained out of the receiver buffer (§5.2.5.7.2). The
   * credits it was holding become eligible to return in a CF_Update.
   *
   * @param vc          The virtual channel the packet left from.
   * @param pktCredits  How many credits the packet frees. Added to the freed
   *                    count for that VC.
   */
  void onDrained(uint8_t vc, uint64_t pktCredits);

  /**
   * Reconcile this receiver's counters against the sender's view of how many
   * credits it has consumed (§5.2.5.7.4).
   *
   * The sender charges itself for a packet on transmit. A packet that never
   * reaches the receiver buffer -- dropped on the link, say -- is never counted
   * on this side, and its credits are stranded: the sender still believes they
   * are in use. A CC_Update carries the sender's consumed count so the receiver
   * can measure that gap and hand the credits back.
   *
   * The arithmetic is the spec's:
   *
   *     LostCredits = S_VC_CC - R_VC_CC
   *     if LostCredits > 0:
   *         R_VC_CF += LostCredits    // give them back to the sender
   *         R_VC_CC  = S_VC_CC        // and resynchronise
   *
   * The subtraction is modular across the counter's full width, so a `sVcCc`
   * behind `R_VC_CC` yields a large positive difference rather than a small
   * one.
   *
   * A LostCredits value above `kMaxLostCredits` is rejected whole and neither
   * counter moves. It is not clamped: a freed count advanced further than one
   * CF_Update can express makes the sender discard that report and every later
   * one for the VC, leaving it permanently unable to transmit.
   *
   * @param vc     The virtual channel this entry refers to.
   * @param sVcCc  The sender's consumed-credit count for that VC, as carried in
   *               the CC_Update.
   *
   * @return       How many credits were reclaimed, and whether the value was
   *               rejected for being out of range.
   */
  [[nodiscard]] CcUpdateResult onCcUpdate(uint8_t vc, uint64_t sVcCc);

  // The receiver's freed-credit count for `vc`. This is the value a CF_Update
  // carries back to the sender.
  [[nodiscard]] uint64_t freed(uint8_t vc) const;
  // The receiver's consumed-credit count for `vc`. Used for reconciliation and
  // for debugging; it is not reported to the sender on its own.
  [[nodiscard]] uint64_t consumed(uint8_t vc) const;

  // How many virtual channels this tracker was built for. Every per-VC index
  // must be below this.
  [[nodiscard]] uint8_t numVcs() const noexcept {
    return numVcs_;
  }

 private:
  /**
   * Reject a VC id at or above `numVcs_`, naming it. Every per-VC entry point
   * calls this first, so a bad id reports the same way as on the sender ledger
   * rather than as a message-less `std::out_of_range` from `.at()`.
   *
   * @param vc  VC id to validate.
   * @throw std::invalid_argument if `vc` is outside 0..numVcs_-1.
   */
  void checkVc(uint8_t vc) const;

  // Fixed at construction. Deliberately not `const`: a const member would
  // delete the implicit assignment operators on an otherwise assignable class.
  uint8_t numVcs_;
  // R_VC_CC[x]: credits consumed per VC, 20-bit cyclic counter (Tbl 5-18).
  // `numVcs_` entries, fixed at construction.
  std::vector<CreditCounter> rVcCc_;
  // R_VC_CF[x]: credits freed per VC, 15-bit cyclic counter (Tbl 5-18). The
  // narrower width is what `CbfcSenderLedger::setPortCreditLimit` bounds the
  // credit pool against. `numVcs_` entries, fixed at construction.
  std::vector<CreditCounter> rVcCf_;
};

} // namespace packsim::cbfc
