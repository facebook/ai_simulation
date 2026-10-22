/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <deque>
#include <utility>
#include <vector>

#include <folly/logging/xlog.h>

#include "packsim/link-layer/llr/llr.h"
#include "packsim/link-layer/tx-packet-source.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim::llr::test {

/**
 * Trivial in-memory `TxPacketSource` for LLR unit tests. Lets tests
 * enqueue packets and have `Llr::pullNextNewFrame()` consume them via
 * the standard pull API, just like the production
 * `EthernetPort` adapter does.
 */
class TestTxPacketSource : public TxPacketSource {
 public:
  [[nodiscard]] PacketPtr peekNextAppPacket() const override {
    return queue_.empty() ? PacketPtr{} : queue_.front();
  }

  void getTxPacket(PacketPtr& packet) override {
    XCHECK(!queue_.empty()) << "getTxPacket on empty TestTxPacketSource";
    packet = std::move(queue_.front());
    queue_.pop_front();
  }

  void enqueue(PacketPtr pkt) {
    queue_.emplace_back(std::move(pkt));
  }

  [[nodiscard]] std::size_t size() const noexcept {
    return queue_.size();
  }

 private:
  std::deque<PacketPtr> queue_;
};

/**
 * Drive one frame through `Llr` via the pull API, mirroring what the
 * scheduler does in production: enqueue the packet on `source`, call
 * `pullNextNewFrame()` (which applies `admissionDecision`), then
 * dispatch the per-frame TX-complete notification on success.
 * Captures the pulled packet into `sentData` so tests can inspect
 * the wire output in the same way `EthernetPort` would.
 *
 * Returns true iff `pullNextNewFrame()` returned a frame
 * (`admissionDecision` was `ADMIT_AND_TX` or `PASSTHROUGH_TX`).
 *
 * Returns false on the two rejection outcomes:
 *   - BLOCK → `source.size()` is unchanged (still has the packet),
 *     no `stats_` field bumped.
 *   - DROP  → `source.size() == prev - 1` AND
 *     `llr.stats().txDiscard` is incremented.
 * Tests that need to distinguish BLOCK from DROP must inspect
 * `source.size()` and `llr.stats()` post-call.
 */
[[nodiscard]] inline bool txAndCapture(
    Llr& llr,
    TestTxPacketSource& source,
    PacketPtr pkt,
    std::vector<PacketPtr>& sentData) {
  source.enqueue(std::move(pkt));
  auto pulled = llr.pullNextNewFrame();
  if (!pulled.has_value()) {
    return false;
  }
  sentData.emplace_back(std::move(pulled->pkt));
  llr.notifyNewFrameTransmitted();
  return true;
}

/**
 * Drain all currently-pullable REPLAY frames, capturing them and
 * firing the per-frame TX-complete notification. Returns the count
 * of frames pulled (callers may discard — the drain is the point).
 * Tests then typically call `llr.notifyReplayBatchComplete()` to
 * release `txRunReplay` from its `replay_done` await.
 */
inline std::size_t pullAllReplayFrames(
    Llr& llr,
    std::vector<PacketPtr>& sentData) {
  std::size_t count = 0;
  while (auto frame = llr.pullNextReplayFrame()) {
    sentData.emplace_back(std::move(frame->pkt));
    llr.notifyReplayFrameTransmitted();
    ++count;
  }
  return count;
}

/**
 * Install a no-op scheduler-wake subscriber.
 *
 * `EthernetPort::setupLinkLayerScheduler` wires this on every production
 * `Llr`, so a test that builds one directly has to satisfy the same contract.
 * Must run before `start()`, which `setOnSchedulerWakeCallback` enforces.
 */
inline void wireNoopSchedulerWake(Llr& llr) {
  llr.setOnSchedulerWakeCallback([] {});
  // The port also wires the CtlOS slots it owns, from the same setup path.
  llr.ctlosSlots().setOnPostCallback([] {});
}

} // namespace packsim::llr::test
