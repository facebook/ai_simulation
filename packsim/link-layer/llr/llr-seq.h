/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

namespace packsim::llr {

/**
 * 20-bit modular sequence number arithmetic for UEC §5.1 LLR.
 *
 * Per spec p. 471 (Tbl 5-10) and p. 476 (§5.1.8), all LLR sequence numbers
 * are 20-bit values that wrap from 0xFFFFF (1048575) back to 0. Comparisons
 * use modular arithmetic with a 524288 (2^19) half-space cutoff to
 * disambiguate "ahead" vs "behind" in the wrapped space.
 *
 * `LlrSeq` is a stateless namespace-class: all members are `static constexpr`
 * and the class is never instantiated.
 */
class LlrSeq {
 public:
  static constexpr uint32_t kSeqMod = 1u << 20; // 1048576
  static constexpr uint32_t kSeqHalf = 1u << 19; // 524288
  static constexpr uint32_t kSeqMax = kSeqMod - 1; // 0xFFFFF

  // Spec p. 471 (Tbl 5-10, `acked_seq` row): set to all-ones whenever
  // `llr_mode_remote` toggles. Combined with `tx_seq = 0` at the same
  // reset point, the modular formula
  // `outstanding_seq = (tx_seq − 1 − acked_seq) mod 2^20` evaluates to 0
  // — matching the spec invariant that no frames are outstanding at
  // reset.
  static constexpr uint32_t kInitialAckedSeq = kSeqMax;

  /**
   * Spec p. 476 §5.1.8 — the validity check for an incoming
   * LLR_ACK / LLR_NACK's `ack_nack_seq`:
   *
   *   ack_nack_seq_ok =
   *       (((tx_seq - 1) - ack_nack_seq) mod 1048576 <= 524288) &&
   *       ((ack_nack_seq - acked_seq) mod 1048576 < 524288)
   *
   * Used by the rxCtlos LLR_ACK / LLR_NACK handler to validate before
   * applying the seq. If false, the spec says to increment
   * `rx_ack_nack_seq_error` and discard the CtlOS without acting on it.
   *
   * @param ackNackSeq  The `ack_nack_seq` value from the incoming control frame
   * @param txSeq       Current transmit sequence number
   * @param ackedSeq    Last acknowledged sequence number
   * @return            True iff `ackNackSeq` lies within the half-open range
   *                    `[acked_seq, tx_seq - 1]` modulo `kSeqMod`
   */
  [[nodiscard]] static constexpr bool ackNackSeqOk(
      uint32_t ackNackSeq,
      uint32_t txSeq,
      uint32_t ackedSeq) noexcept {
    const uint32_t txMinusOne = (txSeq + kSeqMod - 1) % kSeqMod;
    const uint32_t lhsDelta = (txMinusOne + kSeqMod - ackNackSeq) % kSeqMod;
    const uint32_t rhsDelta = (ackNackSeq + kSeqMod - ackedSeq) % kSeqMod;
    return (lhsDelta <= kSeqHalf) && (rhsDelta < kSeqHalf);
  }

  /**
   * Spec p. 476 (Tbl 5-11): a received `frame_seq` is "behind" the expected
   * `next_rx_seq` (i.e., a duplicate of an already-received frame):
   *
   *   duplicate_frame = next_rx_seq_vld && !expected_frame &&
   *                     ((next_rx_seq - frame_seq) mod 1048576 <= 524288)
   *
   * Caller must check `next_rx_seq_vld && !expected_frame` separately;
   * this function returns the modular-arithmetic component.
   *
   * @param frameSeq    The `frame_seq` value from the incoming frame
   * @param nextRxSeq   Expected next receive sequence number
   * @return            True iff `frameSeq` is behind `nextRxSeq` in modular
   *                    space
   */
  [[nodiscard]] static constexpr bool isBehind(
      uint32_t frameSeq,
      uint32_t nextRxSeq) noexcept {
    return ((nextRxSeq + kSeqMod - frameSeq) % kSeqMod) <= kSeqHalf;
  }

  /**
   * Modular successor: `s + 1 mod 1048576`. Wraps from 0xFFFFF to 0.
   * Used to advance `tx_seq` after a frame TX (spec p. 470 Tbl 5-10) and
   * `next_rx_seq` after a successful RX (spec p. 476 Tbl 5-11).
   *
   * @param s  The sequence number to increment
   * @return   `(s + 1) mod 1048576`
   */
  [[nodiscard]] static constexpr uint32_t seqInc(uint32_t s) noexcept {
    return (s + 1) % kSeqMod;
  }

  /**
   * Modular predecessor: `s - 1 mod 1048576`. Wraps from 0 to 0xFFFFF.
   * Used to compute `next_rx_seq - 1` for the `ack_nack_seq` field of
   * outgoing LLR_ACK / LLR_NACK (spec p. 477 §5.1.9.1.3 / §5.1.9.1.4).
   *
   * @param s  The sequence number to decrement
   * @return   `(s - 1) mod 1048576`
   */
  [[nodiscard]] static constexpr uint32_t seqDec(uint32_t s) noexcept {
    return (s + kSeqMod - 1) % kSeqMod;
  }

  /**
   * Modular subtraction returning the "outstanding count":
   *   outstanding_seq = ((tx_seq - 1) - acked_seq) mod 1048576
   *
   * Per spec p. 470 (Tbl 5-10). With the spec-mandated initial values
   * (tx_seq = 0, acked_seq = 0xFFFFF set when llr_mode_remote becomes ON),
   * this evaluates to 0 — matching the spec invariant that no frames are
   * outstanding immediately after reset.
   *
   * @param txSeq     Current transmit sequence number
   * @param ackedSeq  Last acknowledged sequence number
   * @return          Number of outstanding (unacknowledged) frames
   */
  [[nodiscard]] static constexpr uint32_t outstandingSeq(
      uint32_t txSeq,
      uint32_t ackedSeq) noexcept {
    const uint32_t txMinusOne = (txSeq + kSeqMod - 1) % kSeqMod;
    return (txMinusOne + kSeqMod - ackedSeq) % kSeqMod;
  }

  LlrSeq() = delete;
};

} // namespace packsim::llr
