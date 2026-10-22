/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "packsim/link-layer/cbfc/cbfc-types.h"
#include "packsim/link-layer/cbfc/credit-counter.h"

namespace packsim::cbfc {

/**
 * Outcome of applying one `R_VC_CF` value from a CF_Update (§5.2.5.6.3).
 */
struct CfUpdateResult {
  // How far `rVcCf` advanced `S_VC_CF`. A CC_Update reconcile (§5.2.5.7.4)
  // lets the receiver free credits this side still counts as in use, so this
  // can exceed what the local tiers released. It is a wake signal, not an
  // exact count: > 0 means a credit-starved VC may now resume.
  uint64_t creditsReturned{0};
};

/**
 * Sender-side CBFC credit ledger (UEC 1.0.3 §5.2.5.6; Tbl 5-17/5-18).
 *
 * Tracks per-VC consumed (`S_VC_CC`), freed (`S_VC_CF`), and in-use
 * (`S_VC_CU`) counters plus the port-level in-use counter (`S_P_CU`), and
 * answers the pre-transmission credit gate (§5.2.5.6.1). Pure logic with no
 * simulation dependencies. Callers must invoke it only for lossless VCs;
 * best-effort VCs bypass the gate (§5.2.5.6.1).
 *
 * The pooled credit limit (`S_P_CL`) is configured from the partner receiver's
 * buffer at sim init, standing in for the LLDP handshake of §5.2.8. There is
 * no per-VC `S_VC_CL` counter: the per-VC split is a local reservation, not a
 * bootstrapped limit.
 */
class CbfcSenderLedger {
 public:
  // Largest pooled port credit limit `setPortCreditLimit` accepts, in credits:
  // 2^15 - 1, one below the 15-bit freed counter's modulus.
  static constexpr uint64_t kMaxPortCreditLimit =
      (uint64_t{1} << kFreedWidthBits) - 1;

  /**
   * Build the sender ledger for a link with `numVcs` virtual channels.
   *
   * @param numVcs  VC count for this link.
   * @throw std::invalid_argument if `numVcs` is outside 1..`kUecMaxVcs`.
   */
  explicit CbfcSenderLedger(uint8_t numVcs);

  // ----- Initialization (sim bootstrap; replaces LLDP §5.2.8) -----
  //
  // CONTRACT: the setters below are init-only. They must run before the first
  // `onTransmit`, and nothing may reconfigure a ledger afterwards.
  // Reconfiguring a live ledger corrupts credit state silently, in two ways:
  //   - Shrinking a VC's reserve below its live `vcMinUsed_` clamps in
  //     `sharedShareOf` rather than underflowing, but the credits still held
  //     under `vcMinUsed_` leave `sharedCreditLimit()` counting room that is in
  //     use.
  //   - Moving a VC between groups while it holds credits decrements the wrong
  //     group's usage in `onCfUpdate` and wedges that group's limit.
  // Making reserves or group membership dynamic requires clamping/migrating the
  // corresponding usage counters first.

  /**
   * Set the pooled port credit limit `S_P_CL`.
   *
   * The receiver advertises one pooled limit per port
   * (`FlexNode::getLosslessRxCreditBytes`), and UEC §5.2.5.2 sanctions the
   * sender splitting it "in an implementation-specific manner ... with
   * combinations of dedicated and shared VC credit limits" -- the per-VC
   * reserve / VC group / shared hierarchy below.
   *
   * @param totalCredits The pooled limit in credits, at most
   *        `kMaxPortCreditLimit`. This is stricter than UEC Tbl 5-27's 2^19-1;
   *        see the implementation for why the freed counter's width, not the
   *        consumed counter's, is the bound that matters.
   * @throw std::invalid_argument if `totalCredits` exceeds
   *        `kMaxPortCreditLimit`. The pool bounds how far R_VC_CF can advance
   *        between two CF_Updates the sender receives, and a full-modulus
   *        advance is indistinguishable from no advance: the credits are lost
   *        with no recovery path in either spec.
   */
  void setPortCreditLimit(uint64_t totalCredits);

  /**
   * Put `vc` in VC group `grp`, or in no group when `grp` is `std::nullopt`.
   *
   * An unassigned VC has no group clause and is gated by the global shared
   * limit alone; that is the no-groups-configured default. The per-group
   * membership cap (`kCbfcLimits.maxVcsPerGroup`) is not checked here because
   * this setter sees one VC at a time; it is enforced in config validation,
   * where the whole group is visible.
   *
   * @param vc  VC id, in 0..numVcs-1.
   * @param grp Group id in 0..kLedgerMaxVcGroups-1, or `std::nullopt` to
   *            leave it ungrouped.
   * @throw std::invalid_argument if `grp` holds a value outside that range.
   */
  void assignVcGroup(uint8_t vc, std::optional<uint8_t> grp);

  /**
   * Burst-absorption limit for VC group `grp`.
   *
   * Group limits are not required to fit inside the shared pool; the global
   * shared limit is checked as well, so they are excluded from
   * `reservationsFit()`.
   *
   * @param grp     Group id in 0..kLedgerMaxVcGroups-1.
   * @param credits The group's limit, in credits.
   * @throw std::invalid_argument if `grp` is outside that range.
   */
  void setVcGrpLimit(uint8_t grp, uint64_t credits);

  /**
   * Reserve `credits` of the pooled limit as VC `vc`'s dedicated minimum.
   *
   * A VC can always transmit into its own reserve regardless of how much of
   * the shared pool other VCs hold, which is what makes lossless VCs
   * non-blocking with respect to each other (UEC §5.2.5.2). The split of the
   * advertised pool into per-VC reserves is implementation-defined (UEC
   * §5.2.5.2), so the values are configured rather than derived.
   *
   * Default 0 for every VC, which reduces the gate to a single pooled limit.
   *
   * @param vc        VC id.
   * @param credits   Credits dedicated to this VC.
   * @throw std::invalid_argument if `vc` is outside 0..numVcs-1.
   */
  void setVcMinCredits(uint8_t vc, uint64_t credits);

  /**
   * Withhold `credits` from the port credit limit and the shared pool to
   * absorb per-packet encapsulation growth between the credit gate and the
   * wire.
   *
   * The gate charges a packet the credits its pre-encapsulation size implies;
   * this reserve covers whatever the egress path adds afterwards, so the growth
   * does not overrun the receiver. Default 0.
   *
   * @param credits   Credits withheld from both `portTransmitLimit()` and
   *                  `sharedCreditLimit()`.
   */
  void setOvershootReserve(uint64_t credits) noexcept;

  /**
   * Shared credits available to VCs beyond their dedicated minimums:
   * `S_P_CL − Σ vcMin − overshoot reserve`, saturating at 0.
   *
   * @return  Shared pool size in credits.
   */
  [[nodiscard]] uint64_t sharedCreditLimit() const noexcept;

  /**
   * Whether the advertised pool covers the sender's own reservations. A false
   * result means the shared pool is empty and VCs without a reserve can never
   * transmit.
   *
   * @return  True iff `setPortCreditLimit` has been called AND
   *          `Σ vcMin + overshoot reserve <= S_P_CL`. An unconfigured pool
   *          reports false so the caller can fail at init rather than refuse
   *          every transmit with no diagnostic.
   */
  [[nodiscard]] bool reservationsFit() const noexcept;

  // ----- Hot path -----

  /**
   * Credit gate (§5.2.5.6.1): may a packet needing `pktCredits` credits be
   * transmitted on lossless VC `vc`?
   *
   * Three tiers, consumed in order: the VC's own reserve first, then its VC
   * group's pool, then the global shared pool. The VC's own reserve is always
   * available to it; anything beyond that must fit BOTH its VC group limit
   * AND the global shared limit, which is what makes group oversubscription
   * safe. `portTransmitLimit()` binds first and
   * unconditionally, whatever the per-VC tiers allow, so §5.2.5.6.1's "the
   * only requirement is that the S_P_CL limit shall be observed" holds.
   *
   * @param vc           VC id.
   * @param pktCredits   Credits the packet requires.
   *
   * @return             True iff the gate admits the packet.
   */
  [[nodiscard]] bool canTransmit(uint8_t vc, uint64_t pktCredits) const;

  /**
   * Account a transmitted packet (§5.2.5.6.2):
   * `S_P_CU += c; S_VC_CC[vc] += c; S_VC_CU[vc] += c`.
   *
   * @param vc           VC id.
   * @param pktCredits   Credits the packet consumed.
   */
  void onTransmit(uint8_t vc, uint64_t pktCredits);

  /**
   * Apply a received CF_Update `R_VC_CF` value for `vc` (§5.2.5.6.3):
   * `credits_returned = R_VC_CF − S_VC_CF; S_P_CU −= …; S_VC_CU[vc] −= …;
   * S_VC_CF[vc] = R_VC_CF`.
   *
   * @param vc      VC id.
   * @param rVcCf   `R_VC_CF[vc]` value carried in the CF_Update.
   *
   * @return        See `CfUpdateResult`.
   */
  [[nodiscard]] CfUpdateResult onCfUpdate(uint8_t vc, uint64_t rVcCf);

  // ----- Accessors (CC_Update generation §5.2.5.6.4, stats) -----

  // `S_VC_CC[vc]` — consumed credits, carried in CC_Update.
  [[nodiscard]] uint64_t consumed(uint8_t vc) const;
  // `S_VC_CU[vc]` — per-VC credits in use.
  [[nodiscard]] uint64_t outstandingVc(uint8_t vc) const;
  // `S_P_CU` — port-level credits in use.
  [[nodiscard]] uint64_t outstandingPort() const noexcept;

  /**
   * Credits the gate may hand out at port level: `S_P_CL` less the overshoot
   * reserve. Bounds `canTransmit` even when a VC's own reserve covers the
   * packet, so §5.2.5.6.1's "the S_P_CL limit shall be observed" still holds on
   * a configuration whose reserves do not fit.
   *
   * @return  Port-level credits the gate may hand out.
   */
  [[nodiscard]] uint64_t portTransmitLimit() const noexcept;
  // VC count this ledger was built for; every VC id argument must be below it.
  [[nodiscard]] uint8_t numVcs() const noexcept {
    return numVcs_;
  }

 private:
  // The part of `pktCredits` on `vc` that its dedicated minimum cannot absorb
  // and so must come from the shared pool. 0 when the reserve covers it all.
  [[nodiscard]] uint64_t sharedShareOf(uint8_t vc, uint64_t pktCredits) const;

  /**
   * Reject a VC id outside 0..numVcs_-1 by name, rather than letting `.at()`
   * surface it as a message-less `std::out_of_range`.
   *
   * @param vc  VC id to validate.
   * @throw std::invalid_argument if `vc` is outside 0..numVcs_-1.
   */
  void checkVc(uint8_t vc) const;

  // Every per-VC and per-group vector declared below is indexed with `.at()`,
  // on the transmit path included: the index is a VC id derived from packet
  // contents by `VcMapper`, so an unchecked out-of-range read would silently
  // corrupt credit state rather than merely return a wrong answer.

  // VC count fixed at construction, 1..`kUecMaxVcs`; bounds every per-VC
  // index.
  uint8_t numVcs_;
  // S_P_CL: pooled port credit limit in credits, below the 15-bit freed-counter
  // modulus. Empty until `setPortCreditLimit` runs.
  //
  // Empty and zero must stay distinguishable. Both leave `portTransmitLimit()`
  // at 0 and refuse every transmit, but only the empty case is a bug, and
  // `reservationsFit()` is the only thing that reports it.
  std::optional<uint64_t> sPCl_;
  // S_P_CU: port-level credits in use, 20-bit cyclic counter (Tbl 5-18).
  // Advanced on transmit, reduced saturating on a CF_Update.
  CreditCounter sPCu_;
  // S_VC_CC[x]: credits consumed per VC, 20-bit cyclic. Advanced on transmit
  // and never reduced; this is the value a CC_Update carries.
  std::vector<CreditCounter> sVcCc_;
  // S_VC_CF[x]: this side's copy of the receiver's freed count per VC, 15-bit
  // cyclic. Assigned from a CF_Update, never incremented locally.
  std::vector<CreditCounter> sVcCf_;
  // S_VC_CU[x]: credits in use per VC, 20-bit cyclic. Advanced on transmit,
  // reduced saturating on a CF_Update.
  std::vector<CreditCounter> sVcCu_;

  // The reservations, and the split of S_P_CU into dedicated, per-group and
  // shared usage. Plain integers, not CreditCounter: local bookkeeping that
  // never crosses the wire, so not subject to the §5.2.4 cyclic widths.
  // Invariant: Σ vcGrpSharedUsed_ == sharedUsed_ restricted to grouped VCs.

  // Credits withheld from both `portTransmitLimit()` and
  // `sharedCreditLimit()` to cover egress encapsulation growth.
  uint64_t overshootReserve_{0};
  // Σ vcMin_, maintained incrementally.
  uint64_t vcMinTotal_{0};
  // Σ vcSharedUsed_.
  uint64_t sharedUsed_{0};
  // Dedicated credits per VC.
  std::vector<uint64_t> vcMin_;
  // Of vcMin_, the portion currently in use.
  std::vector<uint64_t> vcMinUsed_;
  // This VC's share of sharedUsed_.
  std::vector<uint64_t> vcSharedUsed_;
  // VC -> its VC group; `std::nullopt` for a VC in no group.
  std::vector<std::optional<uint8_t>> vcGrp_;
  // Per-group burst-absorption limit.
  std::vector<uint64_t> vcGrpLimit_;
  // Per-group shared credits in use.
  std::vector<uint64_t> vcGrpSharedUsed_;
};

} // namespace packsim::cbfc
