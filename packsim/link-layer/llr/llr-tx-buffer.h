/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>

#include <folly/logging/xlog.h>

#include "packsim/link-layer/llr/llr-seq.h"
#include "packsim/link-layer/llr/llr-types.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim::llr {

/**
 * Replay buffer for the TX side of LLR (UEC §5.1.5 p. 471).
 *
 * Each LLR-eligible frame transmitted in the ADVANCE state is added here
 * (assigned the current `tx_seq`) and held until acknowledged by an LLR_ACK
 * or LLR_NACK whose `ack_nack_seq` advances `acked_seq` past this frame.
 *
 * ## Sequence-number arithmetic
 *
 * All seq comparisons use `llr-seq.h`'s 20-bit modular helpers. The
 * underlying storage is a `std::map<uint32_t, Entry>` keyed by seq number,
 * but the `outstanding_seq_max ≤ 524288` half-space bound only limits the
 * *number* of entries — it does not prevent the entries from straddling
 * the 0xFFFFF → 0 wrap boundary (e.g., `acked_seq=0xFFFF0`, `tx_seq=0x10`
 * leaves the buffer with entries at {0xFFFF1…0xFFFFF, 0…0xF}). Consumers
 * that need to iterate the buffer in modular order MUST NOT rely on the
 * map's raw-uint32 iteration order — use `snapshotUnackedRange()` and
 * walk the returned [first, last] range via `seqInc()`, which is wrap-safe.
 *
 * ## Lifetime
 *
 * Owned by the `Llr` class.
 */
class LlrTxBuffer {
 public:
  /**
   * @param maxBytes  Replay-buffer storage capacity in bytes, checked apart
   *                  from the spec's `outstanding_data_max`. `tryAdd()`
   *                  returns false if a new frame would exceed it.
   */
  explicit LlrTxBuffer(uint64_t maxBytes) noexcept : maxBytes_(maxBytes) {}

  LlrTxBuffer(const LlrTxBuffer&) = delete;
  LlrTxBuffer& operator=(const LlrTxBuffer&) = delete;
  LlrTxBuffer(LlrTxBuffer&&) = delete;
  LlrTxBuffer& operator=(LlrTxBuffer&&) = delete;
  ~LlrTxBuffer() = default;

  /**
   * Attempt to add `pkt` at sequence number `seq` (the current `tx_seq`).
   * Returns false (and does NOT mutate state) if adding would exceed
   * `maxBytes`.
   */
  bool tryAdd(uint32_t seq, PacketPtr pkt) {
    const uint32_t size = pkt->getSize();
    if (currentBytes_ + size > maxBytes_) {
      return false;
    }
    const auto [it, inserted] =
        entries_.emplace(seq, Entry{std::move(pkt), size});
    // Caller (`Llr::admit`) is the sole arbiter of `tx_seq` and must
    // never reuse a still-unacked seq — duplicate insert here means the
    // 20-bit sequence space wrapped past `outstanding_seq_max` without
    // the spec-mandated ACK advance, which would silently corrupt the
    // replay buffer.
    XCHECK(inserted) << "LLR TX buffer: duplicate seq " << seq;
    currentBytes_ += size;
    // High-water marks, exported as `LlrStats.txBufferPeak{Packets,Bytes}`.
    if (entries_.size() > peakSeq_) {
      peakSeq_ = entries_.size();
    }
    if (currentBytes_ > peakBytes_) {
      peakBytes_ = currentBytes_;
    }
    // Track most-recently-added seq for `snapshotUnackedRange()` —
    // `entries_.rbegin()` is unsafe across the 0xFFFFF → 0 wrap boundary
    // (it returns the highest uint32 key, not the modular-last entry).
    lastAddedSeq_ = seq;
    return true;
  }

  /**
   * Spec p. 476 §5.1.8: "The replay buffer can remove all the frames up to
   * and including the frame whose sequence number is the ack_nack_seq."
   *
   * Removes all entries in the wrap-safe range `(ackedSeq, ackNackSeq]`
   * and updates `acked_seq` to `ackNackSeq`. Iteration is by seq number
   * (not by map iterator) so it is wrap-safe across the 0xFFFFF → 0
   * boundary.
   *
   * Caller MUST have validated `ack_nack_seq_ok` first (per spec). The
   * "ackNackSeq == ackedSeq" duplicate case is handled here as a no-op.
   */
  void removeUpTo(uint32_t ackNackSeq) {
    const uint32_t prevAckedSeq = ackedSeq_;
    if (ackNackSeq == prevAckedSeq) {
      return; // Duplicate ACK; no advance.
    }
    uint32_t seq = LlrSeq::seqInc(prevAckedSeq);
    while (true) {
      auto it = entries_.find(seq);
      if (it != entries_.end()) {
        currentBytes_ -= it->second.size;
        entries_.erase(it);
      }
      if (seq == ackNackSeq) {
        break;
      }
      seq = LlrSeq::seqInc(seq);
    }
    ackedSeq_ = ackNackSeq;
  }

  /**
   * Spec p. 473: "When in the FLUSH state, the LLR discards all frames
   * held in the replay buffer without transmitting them."
   */
  void discardAll() {
    entries_.clear();
    currentBytes_ = 0;
  }

  // True iff seq is currently held in the buffer (unacked).
  [[nodiscard]] bool isUnacked(uint32_t seq) const {
    return entries_.contains(seq);
  }

  // Look up the held frame at seq, or nullptr if not present.
  [[nodiscard]] const PacketPtr* lookup(uint32_t seq) const {
    const auto it = entries_.find(seq);
    return it == entries_.end() ? nullptr : &it->second.packet;
  }

  /**
   * Snapshot the `[firstSeq, lastSeq]` range for REPLAY iteration:
   * `firstSeq = seqInc(acked_seq)`, `lastSeq = lastAddedSeq_`. Consumers
   * walk the range via `seqInc()` and look up each seq with `lookup()`,
   * which returns nullptr for gaps (so concurrent `removeUpTo()` is
   * tolerated).
   *
   * Returns nullopt if the buffer is empty.
   */
  struct SnapshotRange {
    uint32_t firstSeq;
    uint32_t lastSeq;
  };
  [[nodiscard]] std::optional<SnapshotRange> snapshotUnackedRange() const {
    if (entries_.empty()) {
      return std::nullopt;
    }
    return SnapshotRange{
        .firstSeq = LlrSeq::seqInc(ackedSeq_), .lastSeq = lastAddedSeq_};
  }

  // `acked_seq` per spec p. 471 (Tbl 5-10).
  [[nodiscard]] uint32_t ackedSeq() const noexcept {
    return ackedSeq_;
  }

  /**
   * Set `acked_seq` directly. Called by `Llr` on the spec-mandated
   * reset paths per §5.1.5 Tbl 5-10 (acked_seq row): `init_seq − 1`
   * when sending LLR_INIT, and `0xFFFFF` (all-ones) on
   * llr_mode_remote change.
   */
  void setAckedSeq(uint32_t s) noexcept {
    ackedSeq_ = s;
  }

  // `outstanding_seq` per spec p. 471 — number of buffered (unacked)
  // frames.
  [[nodiscard]] size_t outstandingSeq() const noexcept {
    return entries_.size();
  }

  // `outstanding_data` per spec p. 471 — total bytes held.
  [[nodiscard]] uint64_t outstandingData() const noexcept {
    return currentBytes_;
  }

  // Replay-buffer storage capacity in bytes. `Llr::admissionDecision`
  // blocks a frame that would push `outstandingData()` past it, in addition
  // to the spec's `outstanding_data_max` check.
  [[nodiscard]] uint64_t maxBytes() const noexcept {
    return maxBytes_;
  }

  // High-water mark of `outstandingSeq()` since construction.
  [[nodiscard]] size_t peakSeq() const noexcept {
    return peakSeq_;
  }

  // High-water mark of `outstandingData()` since construction.
  [[nodiscard]] uint64_t peakBytes() const noexcept {
    return peakBytes_;
  }

 private:
  struct Entry {
    PacketPtr packet;
    // Cached for fast removeUpTo accounting.
    uint32_t size;
  };

  const uint64_t maxBytes_;

  // Keyed by seq for O(log N) lookup. Iteration order is raw-uint32 and
  // does NOT match modular order across the 0xFFFFF → 0 wrap boundary —
  // consumers iterate via `snapshotUnackedRange()` and `seqInc()`, which
  // is wrap-safe.
  std::map<uint32_t, Entry> entries_;
  uint64_t currentBytes_ = 0;

  // Seq of the most recent successful `tryAdd()`. Used by
  // `snapshotUnackedRange()` to determine `lastSeq` in wrap-safe order.
  // Unread when the buffer is empty, so the initial value is arbitrary.
  uint32_t lastAddedSeq_ = 0;

  uint32_t ackedSeq_ = LlrSeq::kInitialAckedSeq;

  // High-water marks for `outstandingSeq()` / `outstandingData()`.
  // Updated on each successful `tryAdd()`. Never reset — they record
  // peak occupancy over the buffer's lifetime.
  size_t peakSeq_ = 0;
  uint64_t peakBytes_ = 0;
};

} // namespace packsim::llr
