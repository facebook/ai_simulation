/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

namespace packsim::cbfc {

/**
 * A cyclic (wrapping) credit counter (UEC 1.0.3 §5.2.4.2).
 *
 * CBFC credit counters only ever increment and wrap at 2^width, so counters
 * are compared modulo 2^width. §5.2.4.2 caps a valid difference at 2^(width-1)
 * only when the direction of travel is unknown. CBFC is a point-to-point link
 * protocol with a FIFO wire, so the peer's value never falls behind the local
 * copy and callers use `forwardDistanceTo` over the full range: a 10-bit
 * counter sitting at 256 reads a wire value of 200 as a forward distance of
 * 968, not a backward step of 56. The only advance that cannot be represented
 * is an exact multiple of the modulus, which aliases to zero and which no
 * implementation can detect (see `CbfcSenderLedger::onCfUpdate`).
 *
 * Counter names are UEC shorthand: S_/R_ = sender/receiver, _P_/_VC_ =
 * per-port/per-VC, CC/CF/CU/CL = consumed/freed/in-use/limit.
 */
class CreditCounter {
 public:
  /**
   * Construct a counter of the given bit width. Starts at 0.
   *
   * Unchecked precondition: 1 <= widthBits <= 63. Width 0 yields a counter
   * stuck at 0; width >= 64 is undefined behaviour.
   */
  explicit constexpr CreditCounter(uint8_t widthBits) noexcept
      : mask_((uint64_t{1} << widthBits) - 1) {}

  // Current counter value, always in [0, 2^width).
  [[nodiscard]] constexpr uint64_t value() const noexcept {
    return value_;
  }

  /**
   * `v` reduced to this counter's width.
   *
   * Wire fields are wider than the counters they feed -- `R_VC_CF` travels in
   * a 16-bit `CbfcCfUpdatePayload::cf1` but the counter is 15 bits -- so a
   * reported value must be masked before it is compared against `value()`.
   * `add`, `set` and `forwardDistanceTo` mask internally; this exists for
   * callers doing their own comparison.
   */
  [[nodiscard]] constexpr uint64_t masked(const uint64_t v) const noexcept {
    return v & mask_;
  }

  // Modulus 2^width (one past the maximum representable value).
  [[nodiscard]] constexpr uint64_t modulus() const noexcept {
    return mask_ + 1;
  }

  // Increment by `n` credits, wrapping at 2^width.
  constexpr void add(uint64_t n) noexcept {
    value_ = (value_ + n) & mask_;
  }

  /**
   * Decrement by at most the current value, stopping at 0.
   *
   * The in-use counters (S_P_CU / S_VC_CU) hold local occupancy, not wire
   * values, so a CF_Update can return more than they hold: R_VC_CF is
   * authoritative and a CC_Update reconcile can advance it past what this side
   * booked as in use (§5.2.5.6.3). Wrapping that over-return would leave the
   * counter just below the modulus, which the credit gate reads as a fully
   * consumed port -- silently stalling every VC on the link.
   */
  constexpr void subSaturating(uint64_t n) noexcept {
    value_ = n >= value_ ? 0 : (value_ - n) & mask_;
  }

  /**
   * Adopt an absolute value, masked to width. Used to synchronize a counter to
   * a peer's value, e.g. S_VC_CF = R_VC_CF after applying a CF_Update, or
   * R_VC_CC = S_VC_CC after a CC_Update (§5.2.5.6.3 / .7.4).
   */
  constexpr void set(uint64_t v) noexcept {
    value_ = v & mask_;
  }

  /**
   * Cyclic forward distance from this counter to a raw value carried on the
   * wire: `(wireValue - *this)` modulo 2^width, always in [0, 2^width). This
   * is how far the peer's value is *ahead* of this counter, and the basis for
   * `credits_returned` (§5.2.5.6.3) and `LostCredits` (§5.2.5.7.4).
   */
  [[nodiscard]] constexpr uint64_t forwardDistanceTo(
      uint64_t wireValue) const noexcept {
    return ((wireValue & mask_) - value_) & mask_;
  }

  /**
   * Half the modulus of a `widthBits`-wide counter, which is the §5.2.4.2
   * ambiguity point: a cyclic delta at or above this reads as "behind" rather
   * than "ahead".
   */
  [[nodiscard]] static constexpr uint64_t halfModulus(
      const uint8_t widthBits) noexcept {
    return (uint64_t{1} << widthBits) / 2;
  }

 private:
  // 2^width - 1, fixed at construction.
  uint64_t mask_;
  // Counter value in credits; every mutator keeps it in [0, mask_].
  uint64_t value_{0};
};

// Credit counter widths (UEC 1.0.3 Tbl 5-18).
// 20-bit: S_P_CL, S_P_CU, S_VC_CL, S_VC_CU, S_VC_CC, R_VC_CC.
inline constexpr uint8_t kCreditWidthBits = 20;
// 15-bit: S_VC_CF, R_VC_CF.
inline constexpr uint8_t kFreedWidthBits = 15;

static_assert(kCreditWidthBits >= 1 && kCreditWidthBits < 64);
static_assert(kFreedWidthBits >= 1 && kFreedWidthBits < 64);

} // namespace packsim::cbfc
