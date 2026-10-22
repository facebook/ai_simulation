/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/llr/llr.h"

#include <limits>
#include <utility>

#include <folly/Utility.h>
#include <folly/logging/xlog.h>
#include <folly/portability/GFlags.h>

#include "packsim/kernel/coro/SimWaitFirst.h"
#include "packsim/kernel/data-rate.h"
#include "packsim/link-layer/llr/llr-seq.h"
#include "packsim/link-layer/wire-trace-flag.h"
#include "packsim/packet/base.h"

namespace packsim::llr {

Llr::Llr(folly::not_null<SimContext*> ctx, const LlrConfig& config)
    : ctx_(ctx), config_(config) {
  txBuffer_ = std::make_unique<LlrTxBuffer>(config_.txBufferMaxBytes);
  ctlosSlots_ = std::make_unique<LlrCtlosSlots>();
  // CtlOS wake routing is owned by the port — it installs an
  // `onPost` callback on `ctlosSlots_` after construction (via
  // `EthernetPort::enableLlr`).
  replayTimer_ =
      std::make_unique<coro::SimStopwatch>(ctx_, config_.replayTimerMax);
  dataAgeTimer_ =
      std::make_unique<coro::SimStopwatch>(ctx_, config_.dataAgeTimerMax);
  pcsLostStatusTimer_ =
      std::make_unique<coro::SimStopwatch>(ctx_, config_.pcsLostStatusTimerMax);
  // Periodic CtlOS emission timers. Periods derive from
  // `ctlosTargetSpacingBytes` at the configured link rate (UEC §5.1.3.1.1
  // for ACK; §5.1.9.1.1 mandates `4 × ctlos_target_spacing` for INIT).
  const DataRate bps{config_.linkBps};
  ackCtlosTimer_ = std::make_unique<coro::SimStopwatch>(
      ctx_, bps.bytesToTime(config_.ctlosTargetSpacingBytes));
  initCtlosTimer_ = std::make_unique<coro::SimStopwatch>(
      ctx_, bps.bytesToTime(4u * config_.ctlosTargetSpacingBytes));
}

Llr::~Llr() {
  // RAII teardown, matching LinkLayerScheduler. shutdown() is idempotent
  // (the `shuttingDown_` guard makes a second call a no-op), so calling it
  // here is harmless when the owner already shut down via disableLlr(), and
  // it makes the object self-safe if the owner forgot: it `reset()`s the
  // owned SimScopedTask coroutines (destroying their frames at the current
  // co_await) and cancels the member timers BEFORE those members are
  // destroyed — without it, a synchronously-armed timer's scheduled resume
  // would trip the ~SimStopwatch XCHECK. The destructor body runs before any
  // member is destroyed, so touching the tasks/timers here is well-defined.
  shutdown();
}

void Llr::setOnSchedulerWakeCallback(OnSchedulerWakeCallback onWake) {
  XCHECK(!started_)
      << "setOnSchedulerWakeCallback must be called before start()";
  onSchedulerWake_ = std::move(onWake);
}

void Llr::onSchedulerWake() noexcept {
  XCHECK(onSchedulerWake_) << "no scheduler-wake subscriber registered on LLR";
  onSchedulerWake_();
}

void Llr::start() {
  if (started_) {
    return;
  }
  XCHECK(!shuttingDown_)
      << "start() after shutdown(): every later shutdown() is a no-op, so "
         "nothing would cancel the timers start() arms";
  started_ = true;

  if (config_.skipInitNegotiation) {
    // Start the TX SM in ADVANCE and the AN-SM in SEND_ACKS, as if
    // LLR_INIT / LLR_INIT_ECHO had been exchanged. No spec basis; see
    // LlrConfig::skipInitNegotiation for when this applies.
    txState_ = TxState::ADVANCE;
    ackNackState_ = AckNackState::SEND_ACKS;
    nextRxSeqVld_ = true;
    // nextRxSeq_=0 matches the value that would arrive via LLR_INIT
    // after a fresh tx_seq reset (§5.1.5 Tbl 5-10 tx_seq row: tx_seq=0
    // on llr_mode_remote toggle, and §5.1.9.1.1 INIT carries init_seq
    // = tx_seq).
    nextRxSeq_ = 0;
    llrModeLocal_ = true;
    llrModeRemote_ = true;
  }

  // Spawn the coroutines as owned SimScopedTask members — each eager-starts,
  // suspends on its first co_await, and runs independently until `shutdown()`
  // `reset()`s it (destroying the frame at its suspension point). (AN-SM is
  // implemented as synchronous methods, not a coroutine; the
  // replay/data_age/pcs_lost_status timer expirations are observed directly
  // by `txRunAdvance` via `stopwatch.expiration()`.)
  txStateMachine_ = txStateMachine();
  initCtlosLoop_ = initCtlosLoop();
  ackCtlosLoop_ = ackCtlosLoop();
}

void Llr::shutdown() {
  if (shuttingDown_) {
    return;
  }
  shuttingDown_ = true;
  // Structural teardown. reset() destroys each coroutine's suspended frame
  // at its current co_await; the txStateMachine_ dispatcher cascade-destroys
  // the txRun* helper it is awaiting. Every park point is a destroy-safe
  // `collectAny` race or a bare await of a self-cancelling awaitable, so the
  // frame unwind cancels each armed waiter (the SimEvent/SimSignal awaitables
  // also clear their scheduled resume; the SimStopwatch awaitable clears only
  // the suspended handle, so the CtlOS timers' scheduled resumes are cleared
  // by the explicit cancels below). Reset the tasks BEFORE cancelling the
  // member timers so a frame is never resumed by a timer mid-teardown.
  txStateMachine_.reset();
  initCtlosLoop_.reset();
  ackCtlosLoop_.reset();
  // The frame unwind above clears any waiter a coroutine had parked on a
  // timer, but the replay/data_age/pcs_lost_status timers may have been
  // armed SYNCHRONOUSLY by rule call sites with NO coroutine awaiting them —
  // those scheduled resumes live in the timer (outside any frame), so the
  // unwind cannot reach them. Cancel all five explicitly so the
  // SimStopwatch destructors don't trip on `scheduledResume_.isActive()`.
  replayTimer_->cancel();
  dataAgeTimer_->cancel();
  pcsLostStatusTimer_->cancel();
  ackCtlosTimer_->cancel();
  initCtlosTimer_->cancel();
}

void Llr::setLlrModeLocal(const bool on) {
  const bool wasOn = llrModeLocal_;
  llrModeLocal_ = on;
  // Spec §5.1.6 Fig 5-4 "Any state → OFF" edge: triggered by
  // `llr_mode_local == OFF`. The synchronous AN-SM (see anSm* methods)
  // requires this caller-side wiring — `setLlrModeLocal(false)` must
  // drive the AN-SM to OFF rather than leaving it stuck in
  // SEND_ACKS/NACK_SENT and continuing to process frames.
  if (wasOn && !on) {
    anSmReset();
  }
}

void Llr::setLlrModeRemote(const bool on) {
  if (llrModeRemote_ == on) {
    return;
  }
  XCHECK(started_ || config_.skipInitNegotiation)
      << "setLlrModeRemote changed llr_mode_remote before start(); LLR_OFF "
         "waits for a change, so it would be lost";
  llrModeRemote_ = on;
  // Spec §5.1.5 Tbl 5-10: `tx_seq → 0` (tx_seq row); `acked_seq →
  // all-ones` (acked_seq row) whenever `llr_mode_remote` changes. See
  // `LlrSeq::kInitialAckedSeq` for the all-ones rationale. Together they
  // make `outstanding_seq` 0, so the replay buffer is emptied too: a frame
  // left in it would hold a sequence number the restarted `tx_seq` hands out
  // again.
  txSeq_ = 0;
  txBuffer_->setAckedSeq(LlrSeq::kInitialAckedSeq);
  discardReplayBuffer();
  llrModeRemoteChanged_.broadcast();
}

void Llr::onPcsStatusChange(const bool up) {
  pcsStatus_ = up;
  // Spec UEC §5.1.5 Tbl 5-10 pcs_lost_status_timer rule 2: activated when
  // PCS_status == FALSE. Recompute the timer's running/stopped state now
  // that the input changed.
  if (started_) {
    updatePcsLostStatusTimer();
  }
}

void Llr::managementReset() {
  managementResetRequested_.broadcast();
  // Spec §5.1.6 Fig 5-4 "Any state → OFF" edge: triggered by `reset`.
  // The TX SM observes `managementResetRequested_` from `txRunFlush`
  // (FLUSH state with reInitOnDiscard=false). The AN-SM is independent
  // and gated by `llr_mode_local` per spec §5.1.4 — drive it to OFF
  // synchronously here.
  anSmReset();
}

void Llr::notifyReplayBatchComplete() {
  replayBatchComplete_.signal();
}

void Llr::notifyReplayFrameTransmitted() {
  // Spec UEC §5.1.5 Tbl 5-10 rule 3 says "activated (if not already)
  // when the end of a replay frame is transmitted". The "if not already"
  // means: no-op if the timer is already RUNNING; arm-from-zero if it's
  // STOPPED (e.g., after a rule-5 NACK hold or a rule-6 expiration that
  // `txRunAdvance` has already consumed via `stopwatch.stop()`).
  //
  // If the timer is currently EXPIRED (txRunAdvance hasn't consumed it
  // yet), skip the start — `start()` XCHECKs against EXPIRED, and the
  // pending consumer call will stop the stopwatch then a subsequent
  // rule-3 / rule-4 call will arm.
  if (!replayTimer_->running() && !replayTimer_->expired() &&
      txBuffer_->outstandingSeq() > 0) {
    replayTimer_->start();
  }
}

void Llr::notifyPassthroughTransmitted() {
  ++stats_.txPassthrough;
}

void Llr::notifyNewFrameTransmitted() {
  // Spec UEC §5.1.5 Tbl 5-10 rule 2 for BOTH replay_timer and
  // data_age_timer: "activated (if not already) when end of new LLR
  // frame is transmitted." Arm each stopwatch if it's STOPPED with
  // outstanding data; skip if EXPIRED (rule-6 / rule-4 hold; rule 3
  // arms on the first replay TX) or RUNNING (already armed — the
  // "if not already" clause is a no-op).
  if (txBuffer_->outstandingSeq() == 0) {
    return;
  }
  if (!replayTimer_->running() && !replayTimer_->expired()) {
    replayTimer_->start();
  }
  if (!dataAgeTimer_->running() && !dataAgeTimer_->expired()) {
    dataAgeTimer_->start();
  }
}

AdmissionResult Llr::admissionDecision(const uint32_t bytes) const noexcept {
  // Spec p. 473: behavior depends on TX SM state, plus the per-state
  // `FrameBehavior` config for INIT and FLUSH.
  switch (txState_) {
    case TxState::UNKNOWN:
      XCHECK(false) << "admissionDecision: TX SM in UNKNOWN sentinel state";
      return AdmissionResult::BLOCK;

    case TxState::LLR_OFF:
      return AdmissionResult::PASSTHROUGH_TX;

    case TxState::INIT:
      switch (config_.initBehavior) {
        case FrameBehavior::UNKNOWN:
          XCHECK(false)
              << "admissionDecision: initBehavior is UNKNOWN sentinel";
          return AdmissionResult::BLOCK;
        case FrameBehavior::DISCARD:
          return AdmissionResult::DROP;
        case FrameBehavior::BLOCK:
          return AdmissionResult::BLOCK;
        case FrameBehavior::BEST_EFFORT:
          return AdmissionResult::PASSTHROUGH_TX;
      }
      return AdmissionResult::BLOCK;

    case TxState::FLUSH:
      switch (config_.flushBehavior) {
        case FrameBehavior::UNKNOWN:
          XCHECK(false)
              << "admissionDecision: flushBehavior is UNKNOWN sentinel";
          return AdmissionResult::BLOCK;
        case FrameBehavior::DISCARD:
          return AdmissionResult::DROP;
        case FrameBehavior::BLOCK:
          return AdmissionResult::BLOCK;
        case FrameBehavior::BEST_EFFORT:
          return AdmissionResult::PASSTHROUGH_TX;
      }
      return AdmissionResult::BLOCK;

    case TxState::REPLAY:
      // Spec p. 473: "TX LLR will not accept a new LLR-desired frame ... if
      // the state machine is not in the ADVANCE state."
      return AdmissionResult::BLOCK;

    case TxState::ADVANCE:
      // Admission caps (spec p. 473): all three must hold.
      if (txBuffer_->outstandingSeq() >= config_.outstandingSeqMax) {
        return AdmissionResult::BLOCK;
      }
      // Two byte-count gates intentionally: `outstandingDataMax` is the
      // spec admission limit (UEC §5.1.4 Tbl 5-9 `outstanding_data_max`,
      // the protocol's in-flight bytes cap), while `txBuffer_->maxBytes()`
      // is the implementation-defined replay-buffer storage capacity
      // (UEC §5.1 p. 463). Callers may size them independently — e.g.,
      // buffer slightly larger than the admission limit as a safety
      // margin, or admission throttled below buffer capacity for a flow.
      // Checking both keeps the admit path safe under any consistent
      // configuration.
      if (txBuffer_->outstandingData() + bytes > config_.outstandingDataMax) {
        return AdmissionResult::BLOCK;
      }
      if (txBuffer_->outstandingData() + bytes > txBuffer_->maxBytes()) {
        return AdmissionResult::BLOCK;
      }
      return AdmissionResult::ADMIT_AND_TX;
  }
  return AdmissionResult::BLOCK;
}

void Llr::restartIfOutstandingFrames(coro::SimStopwatch& stopwatch) {
  // Spec UEC §5.1.5 Tbl 5-10: replay_timer rule 4 and data_age_timer
  // rule 3 share the shape "set to zero on ACK / advancing-NACK; if
  // outstanding_seq > 0, re-activate; otherwise held at zero." `stop()`
  // clears both RUNNING and EXPIRED back to STOPPED (so any stale
  // EXPIRED that the consumer hadn't drained yet is cleared too).
  stopwatch.stop();
  if (txBuffer_->outstandingSeq() > 0) {
    stopwatch.start();
  }
}

void Llr::discardReplayBuffer() noexcept {
  txBuffer_->discardAll();
  replayTimer_->stop();
  dataAgeTimer_->stop();
  replaySnap_.reset();
  replaySnapshotExhausted_ = true;
}

std::optional<uint32_t> Llr::admit(const PacketPtr& packet) {
  const uint32_t seq = txSeq_;
  // `admissionDecision` has already checked capacity and this `Llr` is only
  // touched from its own SimContext, but `tryAdd`'s own size accounting is
  // the authoritative final check.
  if (!txBuffer_->tryAdd(seq, packet)) {
    return std::nullopt;
  }
  ++stats_.txOk;
  txSeq_ = LlrSeq::seqInc(txSeq_);
  // Stamp here, not in the caller: a buffered frame whose header still reads
  // ineligible is sequenced for this sender and unsequenced for the peer.
  stampLlrHeader(packet, FrameType::LLR_ELIGIBLE, seq);
  return seq;
}

std::optional<PulledFrame> Llr::pullNextNewFrame() {
  XCHECK(packetSource_ != nullptr)
      << "pullNextNewFrame: setTxPacketSource() not called";
  // Non-null per the XCHECK above; the reference form makes that visible to
  // null-safety checkers.
  auto& source = *packetSource_;
  const PacketPtr peek = source.peekNextAppPacket();
  if (!peek) {
    return std::nullopt; // queue empty
  }
  switch (admissionDecision(peek->getSize())) {
    case AdmissionResult::UNKNOWN:
      XCHECK(false) << "pullNextNewFrame: AdmissionResult is UNKNOWN sentinel";
      return std::nullopt;
    case AdmissionResult::ADMIT_AND_TX: {
      PacketPtr popped;
      source.getTxPacket(popped);
      XCHECK_EQ(peek.get(), popped.get())
          << "pullNextNewFrame: peek/pop mismatch — the admission decision "
             "was made on a different packet than the one being transmitted";
      const auto seq = admit(popped);
      XCHECK(seq.has_value())
          << "pullNextNewFrame: admit failed after admissionDecision accepted";
      return PulledFrame{
          .pkt = std::move(popped),
          .type = FrameType::LLR_ELIGIBLE,
          .seq = *seq,
      };
    }
    case AdmissionResult::PASSTHROUGH_TX: {
      PacketPtr popped;
      source.getTxPacket(popped);
      XCHECK_EQ(peek.get(), popped.get())
          << "pullNextNewFrame: peek/pop mismatch — the admission decision "
             "was made on a different packet than the one being transmitted";
      ++stats_.txPassthrough;
      stampLlrHeader(popped, FrameType::LLR_INELIGIBLE, /*seq=*/0);
      return PulledFrame{
          .pkt = std::move(popped),
          .type = FrameType::LLR_INELIGIBLE,
          .seq = 0,
      };
    }
    case AdmissionResult::DROP: {
      // Pop the packet, bump `txDiscard`, and return `nullopt`.
      PacketPtr popped;
      source.getTxPacket(popped);
      XCHECK_EQ(peek.get(), popped.get())
          << "pullNextNewFrame: peek/pop mismatch — the admission decision "
             "was made on a different packet than the one being transmitted";
      ++stats_.txDiscard;
      return std::nullopt;
    }
    case AdmissionResult::BLOCK:
      // Packet stays in queue. A future ACK/NACK that advances `acked_seq`,
      // or a TX state transition (INIT and FLUSH block by default), wakes the
      // scheduler, whose next pull retries admission.
      return std::nullopt;
  }
  return std::nullopt;
}

void Llr::stampLlrHeader(
    const PacketPtr& pkt,
    const FrameType type,
    const uint32_t seq) {
  auto* eth = pkt.as<EthernetPacket>();
  // Only EthernetPackets carry an LLR header. The wire-layer contract
  // is that LLR is invoked only for ethernet frames — a null `eth`
  // here means an upstream layer queued a wrong-typed packet onto the
  // LLR data path, which would silently bypass header stamping and
  // produce an unack-able frame on the wire.
  XCHECK(eth) << "stampLlrHeader: non-EthernetPacket reached LLR";
  if (type == FrameType::LLR_ELIGIBLE) {
    eth->llrHeader.enabled = true;
    eth->llrHeader.seqNum = seq;
  } else {
    // A pass-through frame carries no sequence number. Receivers consult
    // `enabled` first and read no further, so `seqNum` is left as it is.
    eth->llrHeader.enabled = false;
  }
}

std::optional<ReplayFrame> Llr::pullNextReplayFrame() {
  // Once exhausted in this REPLAY entry, stay null until the next
  // REPLAY entry resets us. Without this gate, a pull after drain would
  // re-snapshot the still-unacked buffer and start the batch again, so a
  // caller that pulls until nullopt (e.g. `test::pullAllReplayFrames`)
  // would never finish. `TxArbiter` does not depend on it: the wire driver
  // pulls one frame at a time, and once the snapshot is drained with no
  // replay frame on the wire, `TxArbiter` fires `notifyReplayBatchComplete`
  // (from `onFrameTransmitted`, or from `nextOutboundFrame` when the peer
  // acknowledged the rest of the snapshot first), which moves the TX SM to
  // ADVANCE before the next pull.
  if (replaySnapshotExhausted_) {
    return std::nullopt;
  }
  // Lazy-init the snapshot on the first call per REPLAY entry. Empty
  // buffer at entry → mark exhausted and return nothing to replay.
  if (!replaySnap_.has_value()) {
    // Spec UEC §5.1.4 Tbl 5-9 caps outstanding_seq at kSeqHalf
    // (2^19 = 524288). Assert BEFORE we touch the snapshot fields so
    // a violation never propagates through `replayNextSeq_`.
    XDCHECK_LE(txBuffer_->outstandingSeq(), LlrSeq::kSeqHalf);
    replaySnap_ = txBuffer_->snapshotUnackedRange();
    if (!replaySnap_.has_value()) {
      replaySnapshotExhausted_ = true;
      return std::nullopt;
    }
    replayNextSeq_ = replaySnap_->firstSeq;
  }
  // Walk forward from `replayNextSeq_`, skipping seqs that `lookup`
  // returns null for (ACKed since snapshot). Each step is either a
  // return or an increment; we terminate when we've consumed or skipped
  // the `lastSeq`.
  while (true) {
    const PacketPtr* pkt = txBuffer_->lookup(replayNextSeq_);
    const bool atLast = (replayNextSeq_ == replaySnap_->lastSeq);
    if (pkt != nullptr) {
      const uint32_t seq = replayNextSeq_;
      PacketPtr frame = *pkt;
      if (atLast) {
        replaySnap_.reset();
        replaySnapshotExhausted_ = true;
      } else {
        replayNextSeq_ = LlrSeq::seqInc(replayNextSeq_);
      }
      // Stamp + stats bump inside the helper so `TxArbiter` doesn't have
      // to duplicate the accounting — matches the `pullNextNewFrame`
      // pattern.
      ++stats_.txReplay;
      stats_.txReplayBytes += frame->getSize();
      stampLlrHeader(frame, FrameType::LLR_ELIGIBLE, seq);
      return ReplayFrame{.pkt = std::move(frame), .seq = seq};
    }
    if (atLast) {
      replaySnap_.reset();
      replaySnapshotExhausted_ = true;
      return std::nullopt;
    }
    replayNextSeq_ = LlrSeq::seqInc(replayNextSeq_);
  }
}

Llr::RxDisposition Llr::rxFrame(
    const PacketPtr& packet,
    const FrameType frameType,
    const FcsStatus fcsStatus,
    const uint32_t frameSeq) {
  // rxFrame is the LLR module's sole data-frame entry point — null here
  // means the wire-side caller violated its contract, and silently
  // returning would lose the frame without advancing `next_rx_seq` or
  // updating any RX counter.
  XCHECK(packet);
  // Universal FCS counter (UEC §5.1.11 Tbl 5-13).
  switch (fcsStatus) {
    case FcsStatus::UNKNOWN:
      XCHECK(false) << "rxFrame: fcsStatus is UNKNOWN sentinel";
      return RxDisposition::DROP;
    case FcsStatus::GOOD:
      ++stats_.rxOk;
      break;
    case FcsStatus::POISONED:
      ++stats_.rxPoisoned;
      break;
    case FcsStatus::BAD:
      ++stats_.rxBad;
      break;
  }

  // LLR_INELIGIBLE frames pass straight through to MAC client (UEC §5.1.7
  // p. 466: "expected LLR-eligible frames and LLR-ineligible frames ...
  // are passed through the RX LLR to the MAC client layer without being
  // filtered").
  if (frameType == FrameType::LLR_INELIGIBLE) {
    return RxDisposition::DELIVER;
  }

  // LLR_ELIGIBLE path. Compute the spec-derived predicates per §5.1.7
  // Tbl 5-11:
  //   expected_frame  = next_rx_seq_vld && (frame_seq == next_rx_seq)
  //   duplicate_frame = next_rx_seq_vld && !expected_frame &&
  //                     ((next_rx_seq - frame_seq) mod 2^20 <= 524288)
  //   missing_frame   = next_rx_seq_vld && !expected_frame && !duplicate
  if (!nextRxSeqVld_) {
    // Spec §5.1.7 Tbl 5-11: all three of expected_frame, duplicate_frame,
    // missing_frame are guarded on `next_rx_seq_vld` — so with vld=FALSE
    // they are all FALSE and no AN-SM signal fires. Drop silently; the
    // link partner's replay_timer (§5.1.9.1.4) retransmits once
    // next_rx_seq is established by an LLR_INIT.
    return RxDisposition::DROP;
  }

  const bool expectedFrame = (frameSeq == nextRxSeq_);
  const bool duplicateFrame =
      !expectedFrame && LlrSeq::isBehind(frameSeq, nextRxSeq_);
  const bool missingFrame = !expectedFrame && !duplicateFrame;

  if (expectedFrame) {
    if (fcsStatus == FcsStatus::GOOD) {
      ++stats_.rxExpectedSeqGood;
    } else if (fcsStatus == FcsStatus::POISONED) {
      ++stats_.rxExpectedSeqPoisoned;
    } else {
      ++stats_.rxExpectedSeqBad;
    }
    if (fcsStatus == FcsStatus::GOOD || fcsStatus == FcsStatus::POISONED) {
      // expected_frame_ok pulse. Increment next_rx_seq and notify the ACK/NACK
      // SM (so it can transition NACK_SENT → SEND_ACKS). Delivery is the
      // caller's, on the `DELIVER` this branch returns below.
      const uint32_t pktBytes = packet->getSize();
      nextRxSeq_ = LlrSeq::seqInc(nextRxSeq_);
      // Spec Fig 5-4 NACK_SENT → SEND_ACKS edge. Synchronous transition
      // so the byte-on-receive ACK check below sees the post-transition
      // state immediately.
      anSmOnExpectedFrameOk();
      // On-receive synchronous ACK trigger. Once `ctlosTargetSpacingBytes`
      // bytes have been received since the last ACK, fire an ACK
      // immediately rather than waiting up to one period for the
      // `ackCtlosLoop` to tick. Kept SEPARATE from the periodic loop:
      // periodic provides keepalive; sync provides the latency the
      // sender's pipelined buffer admission needs to stay full.
      bytesSinceLastAck_ += pktBytes;
      firstFrameReceived_ = true;
      if (FLAGS_packsim_trace_wire_events) {
        XLOGF(
            INFO,
            "TRACE-BYTE T={:014} bytesAfter={} threshold={} ans={} vld={}",
            ctx_->nowNs(),
            bytesSinceLastAck_,
            config_.ctlosTargetSpacingBytes,
            folly::to_underlying(ackNackState_),
            nextRxSeqVld_);
      }
      if (bytesSinceLastAck_ >= config_.ctlosTargetSpacingBytes &&
          ackNackState_ == AckNackState::SEND_ACKS && nextRxSeqVld_) {
        const uint32_t ackSeq = LlrSeq::seqDec(nextRxSeq_);
        postAckTraced(ackSeq, "BYTE");
        ++stats_.txAckCtlos;
        ++stats_.txAckByteTrigger;
        bytesSinceLastAck_ = 0;
      }
    } else {
      // fcsStatus == BAD: expected_frame_bad event. Do NOT deliver, do
      // NOT increment next_rx_seq (same frame will be replayed). Per
      // §5.1.6 Fig 5-4, transitions SEND_ACKS → SEND_NACK → NACK_SENT,
      // which `anSmOnGapDetected()` handles atomically (the SEND_NACK
      // state is instantaneous per §5.1.9.1.4 — post one LLR_NACK
      // then transition to NACK_SENT). The method's own state check
      // enforces "single LLR_NACK per gap" without external gating.
      anSmOnGapDetected();
    }
    return fcsStatus == FcsStatus::BAD ? RxDisposition::DROP
                                       : RxDisposition::DELIVER;
  }

  if (duplicateFrame) {
    ++stats_.rxDuplicateSeq;
    // Per §5.1.6 Fig 5-4, the SEND_ACKS→SEND_NACK edge label is
    // (missing_frame | expected_frame_bad) — duplicate_frame is NOT in
    // the trigger set. Drop silently (counter already incremented).
    return RxDisposition::DROP;
  }

  // missing_frame: pulse the SEND_ACKS → SEND_NACK trigger. Same
  // two-layer gate as expected_frame_bad above (UEC §5.1.6 p. 474).
  if (missingFrame) {
    ++stats_.rxMissingSeq;
    // SEND_ACKS → SEND_NACK → NACK_SENT transition (Fig 5-4). Method
    // self-gates on state, so a missing_frame in NACK_SENT is a no-op
    // (per §5.1.6 "single LLR_NACK per gap"). No external pending-signals
    // gating needed — the state check inside is the single source of truth.
    anSmOnGapDetected();
  }
  return RxDisposition::DROP;
}

void Llr::rxCtlos(
    const LlrCtlosType type,
    const uint32_t seq,
    const uint16_t initData) {
  switch (type) {
    case LlrCtlosType::LLR_INIT:
      ++stats_.rxInitCtlos;
      // Spec §5.1.9.1.1: LLR_INIT processing is gated by
      // llr_mode_local == ON.
      if (!llrModeLocal_) {
        return;
      }
      // Spec §5.1.7 Tbl 5-11 (next_rx_seq row): "set to the value
      // contained in the LLR_INIT Control Ordered Set when an LLR_INIT
      // Control Ordered Set is received". Tbl 5-11 (next_rx_seq_vld
      // row): "Set to TRUE when llr_mode_local is ON and an LLR_INIT
      // Control Ordered Set has been received". INIT_ECHO emission per
      // §5.1.9.1.2.
      nextRxSeq_ = seq;
      nextRxSeqVld_ = true;
      ctlosSlots_->postInitEcho(seq, initData);
      ++stats_.txInitEchoCtlos;
      // Spec Fig 5-4 OFF → SEND_ACKS edge: `llr_mode_local==ON &&
      // init_echo_sent`. Fired at post time (no observable
      // difference from wire-complete: the peer can't send LLR-eligible
      // data until it receives our INIT_ECHO and transitions out of
      // INIT, so the ackCtlosLoop/byte-trigger gates can't actually
      // emit anything during the [post, wire-complete] window).
      anSmOnInitEchoSent();
      break;

    case LlrCtlosType::LLR_INIT_ECHO:
      ++stats_.rxInitEchoCtlos;
      // Spec §5.1.9.1.2: only pulse init_echo_received if init_seq
      // matches the locally-assigned tx_seq. Essential — INIT_ECHOs for
      // stale INITs (sent before a re-init) must not transition out of
      // INIT prematurely.
      if (seq == txSeq_) {
        initEchoReceived_.signal();
      }
      break;

    case LlrCtlosType::LLR_ACK: {
      ++stats_.rxAckCtlos;
      if (!LlrSeq::ackNackSeqOk(seq, txSeq_, txBuffer_->ackedSeq())) {
        ++stats_.rxAckNackSeqError;
        return;
      }
      // Spec §5.1.8: "If ack_nack_seq_ok is TRUE and ack_nack_seq is
      // NOT equal to acked_seq then the LLR_ACK/LLR_NACK is
      // acknowledging some frames in the replay buffer." The equality
      // check rejects duplicate ACKs (ack_nack_seq_ok TRUE but no new
      // frames freed) — avoids a no-op removeUpTo() and the scheduler wake
      // it would trigger.
      if (seq != txBuffer_->ackedSeq()) {
        txBuffer_->removeUpTo(seq);
        // Spec §5.1.5 Tbl 5-10 (replay_ct row): "set to zero whenever an
        // LLR_ACK is received for some unacknowledged frames." Gated by
        // the seq-advance check above, which is the spec qualifier for
        // "for some unacknowledged frames" (§5.1.8). Without this reset,
        // replay_ct grows unbounded across REPLAY exits even when the
        // link is making forward progress, prematurely tripping
        // replay_ct_is_max and entering FLUSH on healthy links under
        // high BER.
        replayCt_ = 0;
        // Spec §5.1.5 Tbl 5-10 rule 4 ("replay_timer set to zero when
        // LLR_ACK acknowledges some frames in the replay buffer; if
        // outstanding_seq is non-zero, activate; otherwise held at zero")
        // and rule 3 for data_age_timer (same shape).
        restartIfOutstandingFrames(*replayTimer_);
        restartIfOutstandingFrames(*dataAgeTimer_);
        // Buffer just freed seq(s). Wake the scheduler unconditionally
        // (cheap Pulse wake; dropped if no waiter parked) so any
        // previously-blocked admission can retry.
        onSchedulerWake();
      }
      // Spec §5.1.5 Tbl 5-10 (nack_received row): set to FALSE on
      // LLR_ACK reception.
      nackReceived_ = false;
      break;
    }

    case LlrCtlosType::LLR_NACK: {
      ++stats_.rxNackCtlos;
      if (!LlrSeq::ackNackSeqOk(seq, txSeq_, txBuffer_->ackedSeq())) {
        ++stats_.rxAckNackSeqError;
        return;
      }
      // Per spec p. 476 §5.1.8: the qualifier "acknowledging some frames"
      // applies symmetrically to LLR_NACK; if seq advances acked_seq, we
      // must free the buffered frames (so the data_age_timer can re-arm
      // per rule 3) before processing the NACK's REPLAY trigger.
      if (seq != txBuffer_->ackedSeq()) {
        txBuffer_->removeUpTo(seq);
        // Spec §5.1.5 Tbl 5-10 data_age_timer rule 3 applies symmetrically
        // to LLR_NACK whose ack_nack_seq advances acked_seq (UEC §5.1.8).
        restartIfOutstandingFrames(*dataAgeTimer_);
        // Buffer just freed seq(s) on NACK-advance. See parallel
        // comment in the LLR_ACK case.
        onSchedulerWake();
      }
      // Spec §5.1.5 Tbl 5-10 rule 5: "replay_timer is set to zero when
      // an LLR_NACK is received. The replay_timer is then held at zero
      // until a replay commences (rule 3) or an LLR_ACK arrives (rule 4)."
      // Stopping the stopwatch here implements the "set to zero" half;
      // the "held until rule 3 or 4" half is enforced naturally because
      // nothing else arms the stopwatch in this window except rule 3
      // (in `notifyReplayFrameTransmitted`) and rule 4 (in `rxCtlos
      // LLR_ACK`), both of which are spec-defined exits.
      //
      // A pipelined NEW frame's TX-complete during this window can fire
      // rule 2 (in `notifyNewFrameTransmitted`), which is allowed under
      // the rules-compose reading of the spec: rule 5's exit list is not
      // presented as exhaustive, and rule 2's "activated (if not already)"
      // can compose with rule 5's "held at zero". Behavioural cost: at
      // most one txTime of phase shift on the next expiration, which is
      // typically <10% of replay_timer_max.
      replayTimer_->stop();
      // Spec §5.1.5 Tbl 5-10 (nack_received row): set to TRUE on
      // LLR_NACK reception. Combined with §5.1.8 (last paragraph):
      // "set to FALSE when the state machine is in the INIT state" —
      // gate the set so an INIT-state NACK is ignored (peer has no
      // reason to NACK during INIT, but if one arrives we must not let
      // it set the flag, which would otherwise trigger a spurious
      // REPLAY on INIT → ADVANCE). txRunInit clears defensively on
      // entry too, covering NACKs left over from a prior ADVANCE/FLUSH.
      if (txState_ != TxState::INIT) {
        nackReceived_ = true;
      }
      nackReceivedEvent_.broadcast();
      break;
    }
  }
}

// ===== Internal helpers =====

void Llr::transitionTxStateTo(const TxState newState) {
  if (txState_ == newState) {
    return;
  }
  XLOGF(
      DBG,
      "LLR TX SM: {} -> {}",
      LlrEnumNames::toString(txState_),
      LlrEnumNames::toString(newState));
  txState_ = newState;
  // Telemetry: per-state entry counters (not spec-mandated; exported as
  // `LlrStats.initCount` / `flushCount`).
  if (newState == TxState::INIT) {
    ++stats_.txInitEntries;
  } else if (newState == TxState::FLUSH) {
    ++stats_.txFlushEntries;
  }
  // Spec UEC §5.1.5 Tbl 5-10 pcs_lost_status_timer rule 1: timer is
  // stopped while in LLR_OFF / INIT / FLUSH (i.e., not ADVANCE or REPLAY).
  // Re-evaluate now that the TX SM state changed.
  updatePcsLostStatusTimer();
  // initCtlos emission is gated to the INIT state per UEC §5.1.9.1.1.
  // Arm the periodic timer on INIT entry; stop on INIT exit so the
  // `initCtlosLoop` waiter parks until the next INIT entry.
  if (newState == TxState::INIT) {
    initCtlosTimer_->reset();
  } else {
    initCtlosTimer_->stop();
  }
  // Reset `pullNextReplayFrame` cursor state on REPLAY entry so the
  // first pull in this entry re-snapshots the unacked range. Without
  // this reset, a previous REPLAY's exhausted-flag would persist and
  // block the new entry's pulls.
  if (newState == TxState::REPLAY) {
    replaySnap_.reset();
    replaySnapshotExhausted_ = false;
  }
  // Every state change is a hint to the scheduler: INIT/REPLAY → ADVANCE
  // unblocks new-frame admission; ADVANCE → REPLAY needs replay frames
  // pulled. Letting `TxArbiter::nextOutboundFrame` self-resolve via a wake
  // is simpler than tracking which transitions matter — wake
  // unconditionally; the arbiter returns nullopt on transitions that
  // genuinely have no work and the scheduler re-parks.
  onSchedulerWake();
}

// ===== Coroutines =====

coro::SimScopedTask Llr::txStateMachine() {
  // Thin dispatcher: each state's behavior is a `txRunXxx()` helper that
  // awaits the relevant events and calls `transitionTxStateTo()` if a
  // transition is warranted. Helpers return no value — the dispatcher
  // relies on `txState_` being updated by `transitionTxStateTo` and reads
  // it on the next iteration to pick the next branch. Shutdown needs no
  // check here: `shutdown()` `reset()`s this owned SimScopedTask, destroying
  // the dispatcher frame at whichever `co_await txRun*()` it is parked on
  // (cascade-destroying that helper).
  while (true) {
    switch (txState_) {
      case TxState::UNKNOWN:
        XCHECK(false) << "txStateMachine: txState_ is UNKNOWN sentinel";
        co_return;
      case TxState::LLR_OFF:
        co_await txRunLlrOff();
        break;
      case TxState::INIT:
        co_await txRunInit();
        break;
      case TxState::ADVANCE:
        co_await txRunAdvance();
        break;
      case TxState::REPLAY:
        co_await txRunReplay();
        break;
      case TxState::FLUSH:
        co_await txRunFlush();
        break;
    }
  }
}

coro::SimResultTask<void> Llr::txRunLlrOff() {
  // Wait for llr_mode_remote → ON. The bare await is destroy-safe (the
  // SimEvent awaitable self-cancels on frame-destroy): shutdown destroys this
  // frame via the dispatcher's reset rather than signalling an exit.
  co_await llrModeRemoteChanged_.wait();
  if (llrModeRemote_) {
    transitionTxStateTo(TxState::INIT);
  }
}

coro::SimResultTask<void> Llr::txRunInit() {
  // Spec §5.1.8 (last paragraph): `nack_received` is "set to FALSE when
  // the state machine is in the INIT state". Clear here so a NACK that
  // arrived during a prior FLUSH cycle can't survive into the next
  // ADVANCE and trigger a spurious REPLAY.
  nackReceived_ = false;
  // Spec §5.1.5 Tbl 5-10 (replay_ct row): `replay_ct = 0` in the INIT
  // state, unconditionally.
  replayCt_ = 0;
  // Spec §5.1.5 Tbl 5-10 rule 1 for replay_timer and data_age_timer:
  // "set to zero and held at zero whenever outstanding_seq is zero".
  // Every path into INIT has already run `discardReplayBuffer()`, so both
  // timers are stopped; re-asserted here in case a new entry path skips it.
  // `stop()` is safe from any state.
  replayTimer_->stop();
  dataAgeTimer_->stop();
  co_await coro::collectAny(
      initEchoReceived_.wait(), llrModeRemoteChanged_.wait());
  if (!llrModeRemote_) {
    transitionTxStateTo(TxState::LLR_OFF);
    co_return;
  }
  // Woken by initEchoReceived (the rxCtlos handler validated init_seq ==
  // tx_seq before signaling), or by a mode change undone before this ran
  // (see `setLlrModeRemote`).
  transitionTxStateTo(TxState::ADVANCE);
}

coro::SimResultTask<void> Llr::txRunAdvance() {
  // Stopwatch awaits are level-triggered: `SimStopwatch::expiration()`'s
  // `await_ready()` returns true while the stopwatch is in EXPIRED state,
  // so a timer that fired while the SM was parked in REPLAY/FLUSH (no
  // waiter installed at that moment) is observed immediately on the next
  // ADVANCE entry.
  //
  // `nackReceived_` is a bool because `nackReceivedEvent_` is Pulse-mode
  // — a broadcast issued while the SM is parked elsewhere is lost. The
  // bool persists across that parking; we short-circuit the await when
  // it's already TRUE so a missed Pulse doesn't strand the SM.
  if (!nackReceived_ && llrModeRemote_) {
    co_await coro::collectAny(
        nackReceivedEvent_.wait(),
        replayTimer_->expiration(),
        dataAgeTimer_->expiration(),
        pcsLostStatusTimer_->expiration(),
        llrModeRemoteChanged_.wait());
  }
  if (!llrModeRemote_) {
    transitionTxStateTo(TxState::LLR_OFF);
    co_return;
  }

  // Re-evaluation block: atomically observe all four edge conditions now.
  // Spec §5.1.5 Fig 5-3 (ADVANCE→FLUSH edge label, verbatim):
  //   pcs_lost_status_timer_expired
  //   | data_age_timer_expired
  //   | (replay_timer_expired & replay_ct_is_max)
  // Check FLUSH-trigger BEFORE REPLAY-trigger: a replay_timer expiry
  // with replay_ct already at max must enter FLUSH, not REPLAY.
  if (pcsLostStatusTimer_->expired() || dataAgeTimer_->expired() ||
      (replayTimer_->expired() && replayCtIsMax())) {
    // Consume the EXPIRED edges so a later FLUSH→INIT→ADVANCE cycle (via
    // reInitOnDiscard=true) doesn't see stale EXPIRED states and
    // immediately re-transition to FLUSH. `stop()` is safe from EXPIRED,
    // STOPPED, or RUNNING — covers the path where (say) replay_timer
    // was RUNNING but data_age_timer fired.
    pcsLostStatusTimer_->stop();
    dataAgeTimer_->stop();
    replayTimer_->stop();
    transitionTxStateTo(TxState::FLUSH);
    co_return;
  }
  // Spec §5.1.5 Fig 5-3 (ADVANCE→REPLAY edge label):
  //   !pcs_lost_status_timer_expired & !data_age_timer_expired
  //   & (nack_received | (replay_timer_expired & !replay_ct_is_max))
  // The !pcs_lost / !data_age guard is satisfied by ordering: the FLUSH
  // block above handles those cases first.
  if (nackReceived_ || (replayTimer_->expired() && !replayCtIsMax())) {
    // Telemetry: classify what triggered this REPLAY entry. NACK takes
    // priority since both flags may be set simultaneously. Useful for
    // diagnosing pathological replay patterns (e.g., timer-driven storms
    // during FLUSH cycles).
    if (nackReceived_) {
      ++stats_.txReplayViaNack;
    } else {
      ++stats_.txReplayViaTimer;
    }
    transitionTxStateTo(TxState::REPLAY);
    // Spec §5.1.5 Tbl 5-10 (nack_received, replay_timer rows): both are
    // consumed on REPLAY entry. data_age_timer and pcs_lost_status_timer
    // are deliberately NOT stopped — they keep running across REPLAY per
    // their spec rules (data_age rule 4 holds at max until FLUSH;
    // pcs_lost_status rule 3 same).
    nackReceived_ = false;
    replayTimer_->stop();
    co_return;
  }
  // Spurious wake; loop and wait again.
}

coro::SimResultTask<void> Llr::txRunReplay() {
  // Wake the scheduler so `TxArbiter::nextOutboundFrame` starts
  // pulling replay frames via `pullNextReplayFrame()`. The
  // replay_timer's rule-3 re-arm fires from
  // `notifyReplayFrameTransmitted()`, which `TxArbiter::onFrameTransmitted`
  // calls at TX-complete time, not here at submit time.
  //
  // Snapshot whether there is work to replay BEFORE waking — we need
  // the answer to decide whether to await `replay_done`. An empty
  // snapshot means no replay frames will be pulled, so no
  // `replayBatchComplete_` signal will fire.
  const bool hasWorkToReplay = txBuffer_->snapshotUnackedRange().has_value();
  onSchedulerWake();

  // Spec §5.1.5 Tbl 5-10 (replay_ct row): "It is incremented by one
  // each time that the REPLAY state is exited." The companion
  // replay_ct_is_max row uses `>=` against replay_ct_max, so any value
  // past the limit is permanently is_max (i.e., "stuck at max" is
  // spec-correct behavior).
  //
  // Saturate at the type max so we never wrap back to 0 (which would
  // silently un-set replay_ct_is_max). The "no maximum" sentinel
  // (`kReplayCtMaxNoLimit`) and the type's max value happen to be the
  // same byte (0xFF) today, but they're semantically distinct — keep
  // the saturation gated on the type max so a future sentinel change
  // doesn't break wrap-around protection.
  if (replayCt_ < std::numeric_limits<uint8_t>::max()) {
    ++replayCt_;
  }
  // Telemetry: cumulative REPLAY batch count (txReplay tracks per-frame
  // retransmits; this tracks SM transitions out of REPLAY).
  ++stats_.txReplayBatches;
  // Spec §5.1.5: `replay_done` = "completed a replay operation" (Tbl
  // 5-13 tx_replay row), i.e., every replay frame has been transmitted
  // on the wire (not merely enqueued downstream). Await batch completion
  // BEFORE transitioning out of REPLAY so the ADVANCE-gated admission
  // path can't let FRESH frames slip in while REPLAY frames for the same
  // seqs are still in flight (would create dual-presence on the wire when
  // the sender pipeline is async). NACKs arriving during the wait set
  // `nackReceived_`; the SM is parked here so they coalesce — on wake we
  // transition to ADVANCE and the next `txRunAdvance` pass re-enters
  // REPLAY for the follow-up batch.
  if (hasWorkToReplay) {
    // Bare await is destroy-safe (the SimSignal awaitable self-cancels on
    // frame-destroy); shutdown destroys this frame via the dispatcher's reset.
    co_await replayBatchComplete_.wait();
  }
  transitionTxStateTo(TxState::ADVANCE);
}

coro::SimResultTask<void> Llr::txRunFlush() {
  // Spec §5.1.5: FLUSH state discards all frames in the replay buffer.
  discardReplayBuffer();
  if (config_.reInitOnDiscard) {
    transitionTxStateTo(TxState::INIT);
    // Spec §5.1.5 Tbl 5-10 (replay_ct row): "set to zero in the INIT
    // state." Reset here on the FLUSH→INIT path; see txRunInit() for
    // the LLR_OFF→INIT path.
    replayCt_ = 0;
    co_return;
  }
  // re_init_on_discard == FALSE: wait for managementReset(). The bare await is
  // destroy-safe (the SimEvent awaitable self-cancels on frame-destroy);
  // shutdown destroys this frame via the dispatcher's reset rather than racing
  // a shutdown operand.
  co_await managementResetRequested_.wait();
  transitionTxStateTo(TxState::LLR_OFF);
}

coro::SimScopedTask Llr::initCtlosLoop() {
  // Periodic LLR_INIT emission while TX SM is in INIT (UEC §5.1.9.1.1).
  // `transitionTxStateTo` arms `initCtlosTimer_` on INIT entry and stops
  // it on INIT exit, so the loop's `expiration()` await parks naturally
  // when not in INIT (no scheduled resume to fire) and resumes only when
  // the next INIT cycle re-arms the timer. The `txState_ == INIT` check
  // below is defensive (e.g., transitionTxStateTo could race with a fire
  // already in flight) — emit only when still in INIT. `shutdown()` tears this
  // loop down by `reset()`ing the owning SimScopedTask; the bare stopwatch
  // await is destroy-safe (the awaitable self-cancels the suspended handle on
  // frame-destroy) and `shutdown()` cancels `initCtlosTimer_` to clear its
  // scheduled resume.
  while (true) {
    co_await initCtlosTimer_->expiration();
    if (txState_ == TxState::INIT) {
      ctlosSlots_->postInit(txSeq_, /*initData=*/0);
      ++stats_.txInitCtlos;
      initCtlosTimer_->reset();
    }
  }
}

// ===== ACK/NACK state machine — UEC §5.1.6 Fig 5-4 =====
//
//   reset OR llr_mode_local==OFF
//                ↓
//   Any state ──→ OFF
//                   │  llr_mode_local==ON && init_echo_sent
//                   ↓
//               SEND_ACKS  ←─── expected_frame_ok ─── NACK_SENT
//                   │                                   ↑
//                   │  missing_frame || expected_frame_bad
//                   ↓                                   │
//               SEND_NACK ──── nack CtlOS sent ────────┘
//
// Implemented as synchronous methods (NOT a coroutine) because the
// `rxFrame()` byte-on-receive ACK gate reads `ackNackState_` immediately
// after an AN-SM transition, in the same synchronous call chain. A
// coroutine SM that defers the transition to a separate PDES event
// would leave that read seeing stale state.

void Llr::anSmOnInitEchoSent() {
  // Spec Fig 5-4 OFF → SEND_ACKS edge: llr_mode_local==ON && init_echo_sent.
  // Called from `rxCtlos` LLR_INIT branch immediately after posting
  // the LLR_INIT_ECHO. Idempotent if not in OFF (the SM may have
  // already advanced in tests that pre-position state).
  if (ackNackState_ != AckNackState::OFF) {
    return;
  }
  if (!llrModeLocal_) {
    return;
  }
  ackNackState_ = AckNackState::SEND_ACKS;
}

void Llr::anSmOnExpectedFrameOk() {
  // Spec Fig 5-4 NACK_SENT → SEND_ACKS edge. Called synchronously from
  // `rxFrame()` when an LLR-eligible frame arrives with `seq == next_rx_seq`
  // (an expected_frame_ok event per §5.1.7).
  if (ackNackState_ != AckNackState::NACK_SENT) {
    return;
  }
  ackNackState_ = AckNackState::SEND_ACKS;
}

void Llr::anSmOnGapDetected() {
  // Spec Fig 5-4 SEND_ACKS → SEND_NACK → NACK_SENT edges, fused.
  // The spec models SEND_NACK as instantaneous (post one LLR_NACK then
  // transition to NACK_SENT); we collapse both edges into a single
  // synchronous call. Trigger is `missing_frame || expected_frame_bad`
  // per Fig 5-4 — both call this method (called from `rxFrame()`).
  if (ackNackState_ != AckNackState::SEND_ACKS) {
    // Per §5.1.6: "a single LLR_NACK" — subsequent gap detections in
    // NACK_SENT are ignored until the gap is filled (the receiver is
    // already waiting for the missing frame).
    return;
  }
  // Spec §5.1.9.1.4: LLR_NACK carries ack_nack_seq = next_rx_seq - 1.
  ctlosSlots_->postNack(LlrSeq::seqDec(nextRxSeq_));
  ++stats_.txNackCtlos;
  ackNackState_ = AckNackState::NACK_SENT;
  // §5.1.3.1.1's "no ACKs in NACK_SENT" rule is satisfied by
  // `ackCtlosLoop`'s per-iteration state gate, not by aborting any
  // in-flight period — that gate naturally suppresses emit while in
  // NACK_SENT and resumes emit the moment the state flips back to
  // SEND_ACKS, preserving ACK pacing across the gap.
}

void Llr::anSmReset() {
  // Spec Fig 5-4 "Any state → OFF" edge: reset OR llr_mode_local==OFF.
  // Also responsible for setting `next_rx_seq_vld` per Tbl 5-11 row:
  // "Set to FALSE whenever the ACK/NACK transmit state machine enters
  // the OFF state."
  ackNackState_ = AckNackState::OFF;
  nextRxSeqVld_ = false;
}

void Llr::postAckTraced(
    const uint32_t ackSeq,
    const char* triggerSrc) noexcept {
  // Sample the slot before posting: `postAck` always fills it, so occupancy
  // beforehand is exactly "this ACK lands on a pending one". That is the
  // question the trace exists to answer — whether the byte-triggered and
  // periodic paths are both emitting for the same ACK. Gated on the flag so
  // the flag-off cost stays one branch.
  if (FLAGS_packsim_trace_wire_events) {
    const bool coalesced = ctlosSlots_->hasAckNack();
    ctlosSlots_->postAck(ackSeq);
    XLOGF(
        INFO,
        "TRACE-ENQ T={:014} src={} seq={} coalesced={}",
        ctx_->nowNs(),
        triggerSrc,
        ackSeq,
        coalesced);
  } else {
    ctlosSlots_->postAck(ackSeq);
  }
}

coro::SimScopedTask Llr::ackCtlosLoop() {
  // Test gate: tests that drive ACK emission manually require zero
  // background activity so observable state matches the assertion
  // sequence. `LlrConfig::enableGlobalTimer` defaults to true.
  if (!config_.enableGlobalTimer) {
    co_return;
  }
  // Spec §5.1.3.1.1: emit one LLR_ACK every `ctlosTargetSpacingBytes`
  // worth of wire time while AN-SM is in SEND_ACKS (suppressed in
  // NACK_SENT). The `firstFrameReceived_` gate suppresses spurious ACKs
  // on idle ports: `skipInitNegotiation` sets `nextRxSeqVld_=true` even
  // before any data arrives, so without this gate an idle port would
  // emit an ACK every period for nothing. `shutdown()` tears this loop down by
  // `reset()`ing the owning SimScopedTask; the bare stopwatch await is
  // destroy-safe (the awaitable self-cancels the suspended handle on
  // frame-destroy) and `shutdown()` cancels `ackCtlosTimer_` to clear its
  // scheduled resume.
  while (true) {
    ackCtlosTimer_->reset();
    co_await ackCtlosTimer_->expiration();
    if (FLAGS_packsim_trace_wire_events) {
      XLOGF(
          INFO,
          "TRACE-PERIODIC T={:014} ans={} vld={} firstFr={} nextRxSeq={} bytesSince={}",
          ctx_->nowNs(),
          folly::to_underlying(ackNackState_),
          nextRxSeqVld_,
          firstFrameReceived_,
          nextRxSeq_,
          bytesSinceLastAck_);
    }
    if (ackNackState_ == AckNackState::SEND_ACKS && nextRxSeqVld_ &&
        firstFrameReceived_) {
      const uint32_t ackSeq = LlrSeq::seqDec(nextRxSeq_);
      postAckTraced(ackSeq, "PERIODIC");
      ++stats_.txAckCtlos;
      ++stats_.txAckPeriodic;
      bytesSinceLastAck_ = 0;
    }
  }
}

void Llr::updatePcsLostStatusTimer() {
  // Spec UEC §5.1.5 Tbl 5-10 rules 1 and 2 for pcs_lost_status_timer.
  // Should the timer be running?
  //   - Rule 1: stopped if TX SM is in LLR_OFF / INIT / FLUSH.
  //   - Rule 2: activated if PCS_status == FALSE (when in an active state).
  // Combined: running iff (txState in {ADVANCE, REPLAY}) AND (!pcsStatus).
  const bool inActiveState =
      (txState_ == TxState::ADVANCE || txState_ == TxState::REPLAY);
  const bool shouldRun = inActiveState && !pcsStatus_;
  if (shouldRun) {
    if (!pcsLostStatusTimer_->running() && !pcsLostStatusTimer_->expired()) {
      pcsLostStatusTimer_->start();
    }
  } else {
    pcsLostStatusTimer_->stop();
  }
}

packsim::switches::thrift::LlrStats Llr::exportThriftStats() const {
  const auto& s = stats_;
  packsim::switches::thrift::LlrStats stats;
  stats.retries() = s.txReplayBatches; // Retry attempts (one per REPLAY entry).
  stats.retryBytes() = s.txReplayBytes; // Bytes retransmitted on replay.
  stats.successes() = s.txOk; // LLR-eligible frames TX'd with good FCS.
  stats.acksSent() = s.txAckCtlos;
  stats.acksRcvd() = s.rxAckCtlos;
  stats.nacksSent() = s.txNackCtlos;
  stats.nacksRcvd() = s.rxNackCtlos;
  stats.txDiscard() = s.txDiscard; // INIT/FLUSH DISCARD-behavior frames.
  // `txBufferDrops` (field 8) is deprecated — the LLR module does not
  // drop admitted frames; backpressure leaves them in the upstream
  // queue. Left at the thrift default (0).
  stats.initCount() = s.txInitEntries; // TX SM entries into INIT.
  stats.flushCount() = s.txFlushEntries; // TX SM entries into FLUSH.
  stats.initsSent() = s.txInitCtlos;
  stats.initsRcvd() = s.rxInitCtlos;
  stats.initEchosSent() = s.txInitEchoCtlos;
  stats.initEchosRcvd() = s.rxInitEchoCtlos;
  // Current + peak replay-buffer occupancy.
  stats.txBufferSize() = outstandingSeq();
  stats.txBufferBytes() = outstandingData();
  stats.txBufferPeakPackets() = outstandingSeqPeak();
  stats.txBufferPeakBytes() = outstandingDataPeak();
  // Underlying enum values are explicit in `llr-types.h` so direct
  // `folly::to_underlying` is stable across enum reorders.
  stats.txState() = folly::to_underlying(txState());
  stats.ackNackState() = folly::to_underlying(ackNackState());
  return stats;
}

PacketPtr Llr::buildCtlosPacket(const LlrCtlosPayload& f) noexcept {
  auto pkt = createPacket<PacketType::CTLOS_LLR>(0);
  // The queue and the frame share one payload type, so this copies the report
  // whole and stays correct if a field is added to it.
  pkt.as<LlrCtlosFrame>()->payload = f;
  return pkt;
}

void Llr::logCounterSummary(const std::string& deviceId, const uint16_t portNum)
    const {
  XLOGF(
      DBG,
      "LLR-COUNT device={} port={} txOk={} txReplay={} txReplayBatches={} txReplayViaNack={} txReplayViaTimer={} txAck={} txAckPer={} txAckByte={} txNack={} rxNack={} rxMissing={} rxDup={}",
      deviceId,
      portNum,
      stats_.txOk,
      stats_.txReplay,
      stats_.txReplayBatches,
      stats_.txReplayViaNack,
      stats_.txReplayViaTimer,
      stats_.txAckCtlos,
      stats_.txAckPeriodic,
      stats_.txAckByteTrigger,
      stats_.txNackCtlos,
      stats_.rxNackCtlos,
      stats_.rxMissingSeq,
      stats_.rxDuplicateSeq);
}

} // namespace packsim::llr
