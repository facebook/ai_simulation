/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <cstdint>
#include <optional>

#include "packsim/link-layer/ctlos-arbiter.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

class EthernetPort;

/**
 * Chooses what a port transmits next.
 *
 * The coroutine wire driver pulls one frame at a time via
 * `nextOutboundFrame()`, transmits it, then reports completion via
 * `onFrameTransmitted()`. The PCS-splice path calls `nextCtlosFrame()` to
 * splice a control ordered set into an in-flight data frame.
 *
 * Priority, highest first: CtlOS, MAC control (PFC then CC_Update), LLR
 * replay, LLR new-frame, non-LLR egress. CtlOS is the only non-interruptible
 * kind, because it is the interrupter.
 *
 * Example usage:
 *
 *      if (auto frame = arbiter.nextOutboundFrame()) {
 *        co_await transmit(frame->pkt);
 *        arbiter.onFrameTransmitted(*frame);
 *      }
 *
 * NOTE: Holds a reference to the port whose queues, statistics and protocol
 * modules it arbitrates over. `Queues` is replaced whenever the port is
 * reconfigured, so the port is consulted rather than its parts cached.
 * Not thread-safe; called only from its own port's event callbacks.
 */
class TxArbiter {
 public:
  /**
   * @param port  The port whose outbound frames this arbiter selects; it owns
   *              the arbiter.
   */
  explicit TxArbiter(EthernetPort& port);

  /**
   * Frame category, which decides what `onFrameTransmitted()` does next.
   */
  enum class TxKind : uint8_t {
    CTL_OS, // Control ordered set: LLR ACK/NACK/INIT* or CBFC CF_Update
    // MAC control frame: PFC pause or CBFC CC_Update. Terminated by the
    // peer's MAC, so never credit-gated (UEC 1.0.3 §5.2.9).
    MAC_CONTROL,
    LLR_REPLAY, // LLR_ELIGIBLE retransmit from replay buffer
    LLR_NEW, // LLR_ELIGIBLE first-transmit (admitted to replay buffer)
    LLR_PASSTHROUGH, // LLR_INELIGIBLE pulled by LLR (BEST_EFFORT or LLR_OFF)
    EGRESS, // Non-LLR mode app data
  };

  /**
   * One frame handed to the wire driver. `pkt`'s LLR header is already
   * stamped for the LLR kinds and any pop-time statistics are already bumped.
   */
  struct OutboundFrame {
    // The driver treats a frame as an immutable handoff (transmit, then
    // notify) and holds it `const`. The members themselves are not `const`,
    // so returning a frame inside `std::optional` moves `pkt` instead of
    // copying the shared pointer.
    PacketPtr pkt;
    TxKind kind;
  };

  /**
   * Pull the next outbound frame in priority order.
   *
   * @return  The frame, or `nullopt` both when no source has work and when
   *          the CBFC credit gate is holding the wire because the next
   *          lossless frame has no credit.
   */
  [[nodiscard]] std::optional<OutboundFrame> nextOutboundFrame();

  /**
   * CtlOS-only variant, for the wire driver's PCS-splice path. Selection is
   * round-robin across the port's registered producers, so neither LLR nor
   * CBFC can hold the wire against the other, and either one alone is enough.
   *
   * @return  The next CtlOS frame, or `nullopt` when neither producer has one
   *          pending.
   */
  [[nodiscard]] std::optional<OutboundFrame> nextCtlosFrame();

  /**
   * Whether the port has a CtlOS producer, i.e. LLR or CBFC is enabled. Without
   * one `nextCtlosFrame()` can only return `nullopt`.
   *
   * @return  True when `nextCtlosFrame()` has at least one producer to poll.
   */
  [[nodiscard]] bool hasCtlosSource() const noexcept;

  /**
   * Report that `frame` finished transmitting. Dispatches by `frame.kind` to
   * bump per-CoS port statistics, fire the upstream transmit notification for
   * first-transmit data frames only, and tell LLR and CBFC what reached the
   * wire.
   *
   * @param frame  The frame returned by an earlier pull.
   */
  void onFrameTransmitted(const OutboundFrame& frame);

 private:
  struct CbfcGateDecision {
    // True means hold the wire: either no app frame is queued, or the next
    // one is on a lossless VC that is out of credits.
    bool blocked{false};
    // The frame the gate inspected and admitted, still sitting in its queue.
    // Null when CBFC is disabled (no inspection happens) or when `blocked`.
    //
    // The caller must confirm its own pop returns THIS packet: the gate and
    // the pop select independently, and a divergence would put an unchecked
    // frame on the wire while charging credits to one that never went.
    PacketPtr inspected;
  };

  /**
   * Apply the CBFC transmit credit gate, shared by the LLR and non-LLR egress
   * pull paths. Best-effort frames pass. Replays bypass this gate entirely,
   * because their credits were consumed on first transmit.
   *
   * @return  Whether the wire is held, and when it is not, the frame whose
   *          credits were checked so the caller can confirm its own pop
   *          selected that same frame.
   */
  [[nodiscard]] CbfcGateDecision cbfcGateNextAppPacket();

  /**
   * The port's CtlOS producer slots, LLR then CBFC. Slot order is fixed, so a
   * producer that is disabled leaves a null behind rather than shifting the
   * other one and silently reassigning its turn.
   *
   * @return  One slot per producer, null while that producer is disabled.
   */
  [[nodiscard]] std::array<ICtlosSource*, 2> getCtlosSources() const noexcept;

  /**
   * Take the pending CC_Update if it can be transmitted now, sequencing it
   * into the LLR replay buffer when configured LLR-eligible.
   *
   * @return  The frame, or `nullopt` when none is pending or UEC §5.2.9
   *          forbids sending it yet.
   */
  [[nodiscard]] std::optional<PacketPtr> popCcUpdateIfSendable();

  /**
   * Shared post-transmit accounting for data frames. Bumps per-CoS transmit
   * counters unconditionally, and fires the upstream transmit notification
   * only for the first transmit of a frame that came from upstream.
   *
   * @param pkt       The transmitted frame.
   * @param isReplay  True for an LLR retransmit, which already notified
   *                  upstream on its first transmit.
   */
  void onWireDataFrameComplete(const PacketPtr& pkt, bool isReplay);

  // The owning port, whose queues and protocol modules are read on every pull.
  EthernetPort& port_;

  // Rotation cursor for round-robin CtlOS selection (UEC §5.1.9.1.5). Holds
  // no producer pointers; `nextCtlosFrame()` passes the live set on every
  // pull.
  CtlosArbiter ctlosArbiter_;

  // REPLAY-state frames popped but not yet transmit-complete. The snapshot
  // itself lives inside `Llr`; this counter is what lets
  // `notifyReplayBatchComplete()` wait until the wire has actually drained
  // the batch. `uint32_t` matches the spec ceiling on outstanding_seq.
  uint32_t replayInFlight_{0};
};

} // namespace packsim
