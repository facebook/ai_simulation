/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/tx-arbiter.h"

#include <algorithm>
#include <array>
#include <utility>

#include <folly/logging/xlog.h>

#include "packsim/link-layer/cbfc/cbfc.h"
#include "packsim/link-layer/ctlos-source.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/link-layer/llr/llr.h"
#include "packsim/link-layer/pfc-controller.h"
#include "packsim/queues/queues.h"

namespace packsim {

TxArbiter::TxArbiter(EthernetPort& port) : port_(port) {}

std::optional<PacketPtr> TxArbiter::popCcUpdateIfSendable() {
  if (!port_.cbfc_ || !port_.cbfc_->hasPendingCcUpdate()) {
    return std::nullopt;
  }
  // Both checks happen BEFORE the pop, because `popCcUpdate()` clears the
  // pending flag and bumps `ccUpdatesSent` — popping a frame we then cannot
  // send would count one that never went out, and lose it.
  bool sequenced = false;
  if (port_.llr_ != nullptr) {
    // §5.2.9 rule 9: no new CC_Update may be transmitted during the replay
    // window. (A CC_Update already in the replay buffer still replays — the
    // rule bars new ones, not retransmission.)
    //
    // This gate is outside the `ccUpdateLlrEligible` test on purpose. The rule
    // is written against every CC_Update, and an ineligible one needs it more,
    // not less: with no sequence number it slips past the peer's discard
    // window, arriving amid frames the peer is still reconstructing from
    // retransmissions.
    if (port_.llr_->txState() == llr::TxState::REPLAY) {
      return std::nullopt;
    }
    if (!port_.cbfc_->ccUpdateLlrEligible()) {
      // Ineligible: no sequence number, no replay-buffer entry, so there is no
      // admission decision to make. Rule 9 above still applied.
      return port_.cbfc_->popCcUpdate();
    }
    // A CC_Update is a fixed-size frame, so its admission can be decided
    // before it exists.
    switch (port_.llr_->admissionDecision(
        EthernetPacket::kMinEthernetFrameSizeBytes)) {
      case llr::AdmissionResult::ADMIT_AND_TX:
        sequenced = true;
        break;
      case llr::AdmissionResult::PASSTHROUGH_TX:
        // The TX state machine is not carrying sequence numbers right now —
        // LLR_OFF, or INIT/FLUSH configured best-effort. Credit reconciliation
        // does not depend on LLR, so the frame goes out unsequenced rather
        // than waiting. LLR_OFF in particular is the initial state and is
        // re-entered whenever the peer drops LLR, so holding here would stall
        // the CC_Update indefinitely.
        port_.llr_->notifyPassthroughTransmitted();
        break;
      case llr::AdmissionResult::BLOCK:
      case llr::AdmissionResult::DROP:
        // Hold, do not discard: the CC_msg timer re-marks the same pending
        // range each tick, so the next snapshot goes out once the state
        // machine leaves the blocking state.
        return std::nullopt;
      case llr::AdmissionResult::UNKNOWN:
        XCHECK(false) << "popCcUpdateIfSendable: admissionDecision returned "
                         "the UNKNOWN sentinel";
        return std::nullopt;
    }
  }
  auto cc = port_.cbfc_->popCcUpdate();
  if (!cc) {
    return std::nullopt;
  }
  if (sequenced) {
    const auto seq = port_.llr_->admit(*cc);
    XCHECK(seq.has_value())
        << "popCcUpdateIfSendable: admission was accepted for a fixed-size "
           "CC_Update but the replay buffer then refused it";
  }
  return cc;
}

TxArbiter::CbfcGateDecision TxArbiter::cbfcGateNextAppPacket() {
  if (!port_.cbfc_) {
    return {}; // CBFC disabled: nothing to gate, and nothing inspected.
  }
  PacketPtr peeked = port_.peekNextAppPacket();
  if (!peeked) {
    // With the queue gate installed the scheduler has already skipped any
    // unfundable queue, so an empty peek means either an empty wire or every
    // occupied queue credit-starved — only the latter is a gate-block. The scan
    // is confined to this idle path.
    if (port_.queues_ && port_.queues_->hasTxGateBlockedQueue()) {
      port_.cbfc_->onGateBlocked();
    }
    return {.blocked = true, .inspected = PacketPtr{}};
  }
  if (!port_.cbfc_->canTransmit(*peeked)) {
    // A ready lossless frame is held for lack of credit.
    port_.cbfc_->onGateBlocked();
    return {.blocked = true, .inspected = PacketPtr{}};
  }
  return {.blocked = false, .inspected = std::move(peeked)};
}

std::optional<TxArbiter::OutboundFrame> TxArbiter::nextOutboundFrame() {
  // Priority order: MAC control (PFC, then CC_Update) > LLR (REPLAY/ADVANCE)
  // > Egress. CtlOS outranks all of them but is pulled via `nextCtlosFrame()`
  // — it has splice semantics and is handled separately by the scheduler.

  // ===== P=1: MAC control frames (UEC 1.0.3 §5.2.9) =====
  //
  // PFC and CC_Update are both terminated by the peer's MAC rather than
  // forwarded, so neither occupies an ingress buffer and neither is
  // credit-gated. They share this tier, ahead of all data, and PFC takes
  // strict priority within it: a pause is time-critical, a credit
  // reconciliation is not.

  // PFC — §5.2.9: LLR-ineligible and best-effort, so it is never stamped and
  // never enters the replay buffer. One frame carries every class whose state
  // changed since the last transmission; `PfcController` also bumps the
  // per-CoS XON/XOFF counters and clears its register.
  if (port_.pfc_.hasPendingFrame()) {
    return OutboundFrame{
        .pkt = port_.pfc_.buildPendingFrame(), .kind = TxKind::MAC_CONTROL};
  }

  // CC_Update — §5.2.9 rule 3 makes it LLR-eligible with a valid sequence
  // number, so unlike PFC it does enter the replay buffer.
  if (auto cc = popCcUpdateIfSendable()) {
    return OutboundFrame{.pkt = std::move(*cc), .kind = TxKind::MAC_CONTROL};
  }

  if (port_.llr_) {
    // P=2a: LLR REPLAY — drain the snapshot one frame per call via
    // `Llr::pullNextReplayFrame`. The helper lazy-snapshots on first
    // call, self-resets on drain, and stamps + bumps replay stats
    // internally.
    if (port_.llr_->txState() == llr::TxState::REPLAY) {
      auto replayFrame = port_.llr_->pullNextReplayFrame();
      if (replayFrame.has_value()) {
        ++replayInFlight_;
        return OutboundFrame{
            .pkt = std::move(replayFrame->pkt),
            .kind = TxKind::LLR_REPLAY,
        };
      }
      // Snapshot drained. If no replays are still on the wire, the
      // batch is done — tell `txRunReplay` to exit REPLAY. Otherwise
      // the last in-flight `onFrameTransmitted` fires the notify.
      if (replayInFlight_ == 0) {
        port_.llr_->notifyReplayBatchComplete();
      }
      return std::nullopt;
    }

    // CBFC credit gate (replays above bypass it — their credits were consumed
    // on first transmit). See `cbfcGateNextAppPacket`.
    const CbfcGateDecision gate = cbfcGateNextAppPacket();
    if (gate.blocked) {
      return std::nullopt;
    }

    // P=2b: LLR new-frame pull — peek + check + pop + admit/passthrough
    // via `Llr::pullNextNewFrame`. The returned `PulledFrame` carries
    // the stamped packet plus its (type, seq) — derive TxKind from the
    // returned type so `onFrameTransmitted` dispatches correctly
    // (`LLR_NEW` arms timers; `LLR_PASSTHROUGH` doesn't).
    auto pulled = port_.llr_->pullNextNewFrame();
    if (!pulled.has_value()) {
      return std::nullopt;
    }
    if (gate.inspected.get() != nullptr) {
      // LLR selects the packet again on its own peek/pop pair; this confirms
      // it landed on the frame whose credits were checked.
      XCHECK_EQ(gate.inspected.get(), pulled->pkt.get())
          << "CBFC gate/pull mismatch: credits were checked against a "
             "different frame than the one being transmitted";
    }
    const TxKind kind = (pulled->type == llr::FrameType::LLR_ELIGIBLE)
        ? TxKind::LLR_NEW
        : TxKind::LLR_PASSTHROUGH;
    return OutboundFrame{.pkt = std::move(pulled->pkt), .kind = kind};
  }

  // P=3: Non-LLR egress. Straight pop from the per-CoS queues, gated by CBFC
  // credits for lossless VCs (best-effort passes).
  const CbfcGateDecision gate = cbfcGateNextAppPacket();
  if (gate.blocked) {
    return std::nullopt;
  }
  PacketPtr pkt;
  port_.getTxPacket(pkt);
  if (!pkt) {
    return std::nullopt;
  }
  if (gate.inspected.get() != nullptr) {
    XCHECK_EQ(gate.inspected.get(), pkt.get())
        << "CBFC gate/pop mismatch: credits were checked against a different "
           "frame than the one being transmitted";
  }
  return OutboundFrame{.pkt = std::move(pkt), .kind = TxKind::EGRESS};
}

std::optional<TxArbiter::OutboundFrame> TxArbiter::nextCtlosFrame() {
  // Round robin across whichever CtlOS producers this port has, per UEC 1.0.3
  // §5.1.9.1.5. Priority *within* LLR (Tbl 5-12: INIT_ECHO, INIT, ACK/NACK) is
  // `LlrCtlosSlots`'s business, not the arbiter's.
  const std::array<ICtlosSource*, 2> sources = getCtlosSources();
  auto pkt = ctlosArbiter_.popNextRr(sources);
  if (!pkt) {
    return std::nullopt;
  }
  // Port stats accounting for CtlOS pops. Splice-pop also flows through here,
  // so direct and spliced paths share one bump site.
  auto& ps0 = port_.portStats_.at(0);
  ++ps0.txPkts;
  ps0.txBytes += (*pkt)->getSize();
  return OutboundFrame{.pkt = std::move(*pkt), .kind = TxKind::CTL_OS};
}

bool TxArbiter::hasCtlosSource() const noexcept {
  const std::array<ICtlosSource*, 2> sources = getCtlosSources();
  return std::any_of(sources.begin(), sources.end(), [](ICtlosSource* source) {
    return source != nullptr;
  });
}

std::array<ICtlosSource*, 2> TxArbiter::getCtlosSources() const noexcept {
  return {port_.llr_.get(), port_.cbfc_.get()};
}

void TxArbiter::onFrameTransmitted(const OutboundFrame& frame) {
  switch (frame.kind) {
    case TxKind::CTL_OS:
      // No-op. Stats already fired at pop time inside `nextCtlosFrame`.
      return;

    case TxKind::MAC_CONTROL: {
      // Both members count toward the port byte totals, as CtlOS frames do.
      // The dedicated counters are disjoint from these and are bumped at pop
      // time — XON/XOFF transitions for PFC, `cbfcStats.ccUpdatesSent` for a
      // CC_Update — so neither frame is counted twice. No
      // `port_.cbfc_->onTransmit()` here: §5.2.9 says a CC_Update uses no
      // credits.
      onWireDataFrameComplete(frame.pkt, /*isReplay=*/false);
      // Sequenced CC_Update only: it holds a replay-buffer entry, so LLR must
      // learn it reached the wire or the replay timer never arms for it. PFC
      // is never stamped, so `llrHeader.enabled` is false for it. The cast is
      // unconditional because every frame on this tier is a `MacControlFrame`,
      // which derives from `EthernetPacket`.
      if (port_.llr_ && frame.pkt.as<EthernetPacket>()->llrHeader.enabled) {
        port_.llr_->notifyNewFrameTransmitted();
      }
      return;
    }

    case TxKind::EGRESS:
    case TxKind::LLR_PASSTHROUGH:
      // Plain transmit completion — stats + the upstream transmit notify
      // notify only. `EGRESS` is non-LLR mode; `LLR_PASSTHROUGH` is
      // LLR_INELIGIBLE (LLR_OFF or BEST_EFFORT pass-through; no buffer entry,
      // no timers to arm). CBFC consumes credits on this first transmit
      // (§5.2.5.6.2; no-op for best-effort VCs).
      if (port_.cbfc_) {
        port_.cbfc_->onTransmit(*frame.pkt);
      }
      onWireDataFrameComplete(frame.pkt, /*isReplay=*/false);
      return;

    case TxKind::LLR_NEW:
      // First transmit of an LLR-eligible frame — CBFC consumes its credits
      // here (not on replay, §5.2.9 rule 1).
      if (port_.cbfc_) {
        port_.cbfc_->onTransmit(*frame.pkt);
      }
      onWireDataFrameComplete(frame.pkt, /*isReplay=*/false);
      port_.llr_->notifyNewFrameTransmitted();
      return;

    case TxKind::LLR_REPLAY:
      onWireDataFrameComplete(frame.pkt, /*isReplay=*/true);
      port_.llr_->notifyReplayFrameTransmitted();
      XDCHECK_GT(replayInFlight_, 0u);
      // Snapshot drained earlier AND this is the last in-flight replay
      // — the batch is done; tell `txRunReplay` to exit REPLAY.
      if (--replayInFlight_ == 0 && port_.llr_->isReplaySnapshotDrained()) {
        port_.llr_->notifyReplayBatchComplete();
      }
      return;
  }
}

void TxArbiter::onWireDataFrameComplete(
    const PacketPtr& pkt,
    const bool isReplay) {
  // Port stats and ECN/TTL accounting bump for every transmission — replay and
  // MAC control included — so wire-throughput metrics match the wire.
  // The transmit notification is the upstream switch's "cell-free" notify and
  // MUST be gated on `!isReplay`: replay frames already triggered it on their
  // first transmit, so re-firing would double-decrement the upstream switch's
  // buffer.
  port_.bumpTxStats(pkt);
  // A MAC control frame is generated by this port's MAC rather than
  // forwarded, so it never occupied an upstream buffer cell and the cell-free
  // notify must not fire for it: `FbSwitch::onPacketSent` would call
  // `removePacket()` against a cell that was never allocated (its tag defaults
  // to port 0 / pg 0), and `Endpoint::onPacketSent` would hand it to the
  // transport.
  if (!isReplay && !pkt.is<MacControlFrame>() && port_.observer_ != nullptr) {
    port_.observer_->onPacketSent(port_, pkt);
  }
}

} // namespace packsim
