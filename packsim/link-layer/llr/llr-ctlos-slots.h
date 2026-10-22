/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <utility>

#include <folly/logging/xlog.h>

#include "packsim/link-layer/llr/llr-types.h"
#include "packsim/packet/llr-ctlos-types.h"

namespace packsim::llr {

/**
 * The pending LLR Control Ordered Sets on one link — three slots, not a queue.
 *
 * Spec §5.1.9.1.5 Tbl 5-12 defines a strict priority order:
 *
 *   Highest: LLR_INIT_ECHO
 *            LLR_INIT
 *   Lowest:  LLR_ACK or LLR_NACK
 *
 * `popHighest()` walks the priority levels in order. Without this
 * ordering an LLR_INIT_ECHO could be sent behind an already-pending
 * LLR_ACK, delaying the link partner's INIT handshake.
 *
 * Scope: ONLY the four LLR CtlOS types. CBFC is a separate producer, and
 * arbitration between the two is `CtlosArbiter`'s job.
 *
 * ## Why slots
 *
 * Per the spec each priority level holds at most one in-flight CtlOS:
 *
 *   - **LLR_INIT_ECHO**: §5.1.6 — one ECHO per received INIT. The peer
 *     stops sending INITs once it receives an ECHO, so the steady-state
 *     in-flight count is 1.
 *   - **LLR_INIT**: §5.1.4 — periodic, every `4 × ctlosTargetSpacingBytes`
 *     of wire time (see `Llr::Llr`), while TX SM is in INIT. Only the
 *     latest `init_seq` / `init_data` is spec-meaningful (a stale INIT in
 *     flight provides no value).
 *   - **LLR_ACK and LLR_NACK**: §5.1.6 — the AN-SM enforces
 *     mutual-exclusion of ACK and NACK transmission. In `SEND_ACKS`
 *     only ACKs are posted; on a gap-detect the SM atomically
 *     transitions SEND_ACKS → SEND_NACK → NACK_SENT and posts a
 *     single NACK; in `NACK_SENT` no further ACK/NACK is posted
 *     until the missing frame arrives and the SM returns to SEND_ACKS.
 *     Therefore at most ONE of {ACK, NACK} is pending at any time.
 *
 * So this is three `std::optional<LlrCtlosPayload>` slots — one per priority
 * level, with ACK and NACK sharing the lowest (their mutual exclusion is
 * enforced upstream by the AN-SM). The type encodes the bound: there is no
 * fourth slot to fill. Nothing here has a depth or an order within a slot,
 * which is why the accessors are per-slot predicates rather than a count.
 *
 * ## Coalescing — overwrite, not append
 *
 * `postAck` replaces the current ACK seq (cumulative ACKs subsume
 * earlier ones — spec §5.1.9.1.3). `postNack` overwrites whatever
 * was in the AckNack slot (which can only be a stale ACK per the
 * AN-SM mutex). `postInit` / `postInitEcho` overwrite their
 * respective slots — for INIT the latest `init_data` supersedes
 * earlier values; for INIT_ECHO the peer only cares that ONE was
 * sent, so the latest received INIT's seq is the right thing to
 * echo.
 *
 * ## Lifetime
 *
 * Owned by `Llr`. The `onPost` callback is required: each `post*()` call
 * invokes it synchronously, and XCHECKs that one is installed. Whatever the
 * callback captures must outlive this object.
 */
class LlrCtlosSlots {
 public:
  LlrCtlosSlots() noexcept = default;

  LlrCtlosSlots(const LlrCtlosSlots&) = delete;
  LlrCtlosSlots& operator=(const LlrCtlosSlots&) = delete;
  LlrCtlosSlots(LlrCtlosSlots&&) = delete;
  LlrCtlosSlots& operator=(LlrCtlosSlots&&) = delete;
  ~LlrCtlosSlots() = default;

  /**
   * Install a callback invoked synchronously after each successful post.
   * Whatever the callback captures must outlive this object.
   *
   * Required before the first post: `post*()` XCHECKs that a callback is
   * installed.
   */
  using OnPostCallback = std::function<void()>;

  void setOnPostCallback(OnPostCallback cb) noexcept {
    onPost_ = std::move(cb);
  }

  /**
   * Spec p. 477 §5.1.9.1.2: pending LLR_INIT_ECHO. Overwrites any prior
   * echo — the peer cares only that one was sent in response to its
   * most recent INIT. Highest priority.
   */
  void postInitEcho(uint32_t initSeq, uint16_t initData) {
    pendingInitEcho_ = {LlrCtlosType::LLR_INIT_ECHO, initSeq, initData};
    onPost();
  }

  /**
   * Spec p. 477 §5.1.9.1.1: pending LLR_INIT (called from the periodic
   * initCtlosLoop while TX SM is in INIT). Overwrites the prior pending
   * INIT — the latest `init_data` value is the one we want to negotiate.
   * Mid priority.
   */
  void postInit(uint32_t initSeq, uint16_t initData) {
    pendingInit_ = {LlrCtlosType::LLR_INIT, initSeq, initData};
    onPost();
  }

  /**
   * Spec p. 477 §5.1.9.1.3: pending LLR_ACK (called from the periodic
   * ackCtlosLoop and from rxFrame's byte trigger while AN-SM is in
   * SEND_ACKS). Cumulative — overwriting the seq with the latest is the
   * canonical coalescing per the spec.
   * AN-SM guarantees the slot's prior content (if any) was an ACK, not
   * a NACK (NACK_SENT state suppresses ACK posting).
   */
  void postAck(uint32_t ackNackSeq) {
    assertAckNackSlotNotNack("postAck");
    pendingAckNack_ = {LlrCtlosType::LLR_ACK, ackNackSeq, 0};
    onPost();
  }

  /**
   * Spec p. 477 §5.1.9.1.4: pending LLR_NACK (called once on
   * AN-SM transition SEND_ACKS → SEND_NACK → NACK_SENT). Overwrites
   * any stale ACK in the AckNack slot — the AN-SM mutex guarantees no
   * concurrent NACK can already be present.
   */
  void postNack(uint32_t ackNackSeq) {
    assertAckNackSlotNotNack("postNack");
    pendingAckNack_ = {LlrCtlosType::LLR_NACK, ackNackSeq, 0};
    onPost();
  }

  /**
   * Pop the highest-priority pending CtlOS, or `std::nullopt` if all
   * slots are empty. Drain order per Tbl 5-12: INIT_ECHO → INIT →
   * ACK/NACK.
   */
  [[nodiscard]] std::optional<LlrCtlosPayload> popHighest() noexcept {
    if (pendingInitEcho_.has_value()) {
      return std::exchange(pendingInitEcho_, std::nullopt);
    }
    if (pendingInit_.has_value()) {
      return std::exchange(pendingInit_, std::nullopt);
    }
    if (pendingAckNack_.has_value()) {
      return std::exchange(pendingAckNack_, std::nullopt);
    }
    return std::nullopt;
  }

  // True iff no CtlOS is pending in any slot.
  [[nodiscard]] bool empty() const noexcept {
    return !hasInitEcho() && !hasInit() && !hasAckNack();
  }

  // Whether the INIT_ECHO slot holds a report.
  [[nodiscard]] bool hasInitEcho() const noexcept {
    return pendingInitEcho_.has_value();
  }

  // Whether the INIT slot holds a report.
  [[nodiscard]] bool hasInit() const noexcept {
    return pendingInit_.has_value();
  }

  // Whether the shared ACK/NACK slot holds a report. True here before a
  // `postAck` means that ACK coalesces onto a pending one rather than
  // occupying an empty slot.
  [[nodiscard]] bool hasAckNack() const noexcept {
    return pendingAckNack_.has_value();
  }

 private:
  void onPost() const {
    XCHECK(onPost_) << "no post subscriber registered on the CtlOS slots";
    onPost_();
  }

  /**
   * AN-SM (UEC §5.1.6 Fig 5-4) defense-in-depth: when posting into
   * the shared ACK/NACK slot, any prior pending entry must be an ACK
   * (never a NACK). NACK_SENT suppresses further ACK/NACK posting
   * until the missing frame arrives, and the NACK→NACK transition is
   * impossible by spec — so a pending NACK in this slot means the
   * AN-SM mutex broke upstream. Both `postAck` and `postNack`
   * share this invariant; keeping the check in one place ensures the
   * AN-SM contract has a single source of truth.
   */
  void assertAckNackSlotNotNack(const char* caller) const {
    XDCHECK(
        !pendingAckNack_.has_value() ||
        pendingAckNack_->type == LlrCtlosType::LLR_ACK)
        << "AN-SM mutex violation: " << caller << " called with NACK pending";
  }

  std::optional<LlrCtlosPayload> pendingInitEcho_;
  std::optional<LlrCtlosPayload> pendingInit_;
  // ACK and NACK share this slot. The AN-SM (UEC §5.1.6 Fig 5-4)
  // guarantees at most one of them is in-flight at any time — ACK is
  // posted only in SEND_ACKS, NACK only on SEND_ACKS → SEND_NACK →
  // NACK_SENT atomic transition, and no further ACK/NACK is posted
  // until the missing frame arrives and the SM returns to SEND_ACKS.
  std::optional<LlrCtlosPayload> pendingAckNack_;
  OnPostCallback onPost_;
};

} // namespace packsim::llr
