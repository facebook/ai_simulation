/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/link-layer-scheduler.h"

#include <utility>

#include <folly/Utility.h>
#include <folly/logging/xlog.h>
#include <folly/portability/GFlags.h>

#include "packsim/kernel/coro/SimScopedDelay.h"
#include "packsim/kernel/coro/SimStopwatch.h"
#include "packsim/kernel/coro/SimWaitFirst.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/link-layer/wire-trace-flag.h"
#include "packsim/packet/base.h"

namespace packsim::wire {

LinkLayerScheduler::LinkLayerScheduler(
    folly::not_null<SimContext*> ctx,
    folly::not_null<EthernetChannel*> channel,
    folly::not_null<EthernetPort*> port,
    folly::not_null<TxArbiter*> arbiter,
    DataRate linkRate) noexcept
    : ctx_(ctx),
      channel_(channel),
      port_(port),
      arbiter_(arbiter),
      // std::move keeps unnecessary-copy lint checks quiet; DataRate is
      // trivially small, but those checks fire whatever the type's size.
      linkRate_(std::move(linkRate)),
      dataAvailable_(ctx, coro::SimEventMode::PULSE),
      ctlosAvailable_(ctx, coro::SimEventMode::PULSE),
      // Duration is supplied per arm via `startOneShot`; this max is unused.
      ctlosGate_(ctx, Time{}) {}

LinkLayerScheduler::~LinkLayerScheduler() {
  // Idempotent — no-op if shutdown() was already called by the owner.
  shutdown();
}

void LinkLayerScheduler::start() {
  if (started_) {
    return;
  }
  started_ = true;
  // Eager-start the txDriver and OWN it as a SimScopedTask: shutdown() / the
  // destructor reset() it, destroying the frame and cancelling whatever it is
  // parked on — no cooperative shutdown signal needed.
  txDriver_ = txDriver();
}

void LinkLayerScheduler::shutdown() {
  // Destroy-based teardown: reset() destroys the txDriver frame wherever it is
  // parked; the frame unwind cancels its installed waiters and scheduled PDES
  // events via the awaitables' destructors (simWaitFirst teardown,
  // SimScopedDelay). Idempotent — reset() on an empty / already-reset task is a
  // no-op. Safe with a transmission in flight.
  shuttingDown_ = true;
  txDriver_.reset();
  // The destroyed frame may have been parked on `dataAvailable_` alone.
  parkedOnDataOnly_ = false;
  // Frame teardown detaches the gate's waiter but deliberately leaves its PDES
  // resume queued (the simWaitFirst loser-cancel contract), which ~SimStopwatch
  // asserts against. Must run after reset(), which is what removes the waiter.
  ctlosGate_.stop();
}

void LinkLayerScheduler::signalDataAvailable() noexcept {
  // broadcastInline resumes the parked txDriver synchronously in this
  // stack, skipping the PDES event-scheduling round trip. Safe because
  // the wake path (poll → admit → transmit) does not re-enter the
  // calling producer.
  dataAvailable_.broadcastInline();
}

void LinkLayerScheduler::signalCtlosAvailable() noexcept {
  // A driver parked on `dataAvailable_` alone (no CtlOS producer existed when
  // it parked) is not listening here, so wake it on the signal it does await.
  if (parkedOnDataOnly_) {
    dataAvailable_.broadcastInline();
    return;
  }
  ctlosAvailable_.broadcastInline();
}

namespace {
// Wire-serialization time for `pkt` (preamble + frame + IFG). An exactly-zero
// result (a zero-byte frame) becomes 1 ns so no frame takes zero wire time;
// nonzero sub-ns times, e.g. an 8 B CtlOS at 200 Gbps (320 ps), are kept.
Time computeTxCompleteTime(const PacketPtr& pkt, const DataRate& linkRate) {
  Time t = linkRate.bytesToTime(pkt->getPhyFrameSize()) +
      linkRate.bytesToTime(pkt->getInterFrameGapSize());
  if (t.isZero()) {
    t = Time::fromNs(1);
  }
  return t;
}
} // namespace

coro::SimScopedTask LinkLayerScheduler::txDriver() {
  // Wire-driving loop. Awaits a wake signal; on wake, pulls one frame
  // from the port. CtlOS (highest priority) is pulled first via
  // `nextCtlosFrame()`; everything else flows through
  // `nextOutboundFrame()` (which encodes its own internal priority).
  //
  // No shutdown handling appears here: this is an infinite loop torn down
  // structurally. `shutdown()` reset()s this owned `SimScopedTask`, destroying
  // the frame at whatever `co_await` it is parked on; the awaitables cancel
  // themselves on frame-destroy (the idle `simWaitFirst` / `SimEvent` wait
  // below, the `SimScopedDelay` of an unpreemptible transmit, the wire
  // stopwatch + splice race in `sendDataFrame`). No shutdown operand or
  // per-transmit check is needed.
  while (true) {
    // POLL BEFORE AWAIT: try the port BEFORE awaiting any signal. This
    // recovers from missed Pulse-mode broadcasts that fired while we
    // were mid-transmit (no waiter installed → broadcast lost).
    // §5.2.11.2 spacing gates the pull, not the send: holding a popped CtlOS
    // would idle the wire, whereas skipping the pull lets a data frame go and
    // the CtlOS follow once the budget has elapsed.
    //
    // A port with neither LLR nor CBFC has no CtlOS producer, so nothing posts
    // a CtlOS or pulses `ctlosAvailable_`. Re-read on every iteration so a
    // producer enabled later is picked up at the next frame boundary.
    const bool hasCtlosSource = arbiter_->hasCtlosSource();
    const bool mayPullCtlos = hasCtlosSource && ctlosSpacingElapsed();
    const auto ctlos = mayPullCtlos ? arbiter_->nextCtlosFrame()
                                    : std::optional<TxArbiter::OutboundFrame>{};
    if (ctlos.has_value()) {
      if (FLAGS_packsim_trace_wire_events) {
        XLOGF(INFO, "TRACE-DRV T={:014} ev=POLL-HIT src=CTLOS", ctx_->nowFs());
      }
      // CtlOS does not preempt CtlOS, and nothing else preempts CtlOS, so
      // there is no splice race: serialize, wait, deliver.
      const TxTiming tx = beginTx(ctlos->pkt, TxKind::CTLOS);
      co_await coro::simScopedDelay(ctx_, tx.txComplete);
      endTx(ctlos->pkt, tx.propDelay, TxKind::CTLOS);
      arbiter_->onFrameTransmitted(*ctlos);
      continue;
    }

    const auto frame = arbiter_->nextOutboundFrame();
    if (frame.has_value()) {
      if (FLAGS_packsim_trace_wire_events) {
        XLOGF(
            INFO,
            "TRACE-DRV T={:014} ev=POLL-HIT kind={}",
            ctx_->nowFs(),
            folly::to_underlying(frame->kind));
      }
      if (hasCtlosSource) {
        co_await sendDataFrame(frame->pkt);
      } else {
        // No CtlOS producer, so nothing can splice into this frame. Completes
        // at the time `sendDataFrame`'s wire stopwatch would reach, with the
        // same trace events.
        const TxTiming tx = beginTx(frame->pkt, TxKind::DATA);
        co_await coro::simScopedDelay(ctx_, tx.txComplete);
        endTx(frame->pkt, tx.propDelay, TxKind::DATA);
      }
      arbiter_->onFrameTransmitted(*frame);
      continue;
    }

    if (FLAGS_packsim_trace_wire_events) {
      XLOGF(INFO, "TRACE-DRV T={:014} ev=POLL-EMPTY", ctx_->nowFs());
    }
    // Idle wait. Park until a producer pulses `dataAvailable_` or
    // `ctlosAvailable_`. If the owner reset()s this task while parked here, the
    // frame is destroyed and every operand is teardown-cancelled.
    //
    // When the spacing gate is shut it joins the race, so a CtlOS deferred
    // behind it is released the moment the budget expires (see `ctlosGate_`).
    // The winner is irrelevant here — every wake re-polls from the top.
    //
    // Without a CtlOS producer only data can wake the driver, so it parks on
    // `dataAvailable_` alone. A producer enabled while it is parked there
    // announces its first CtlOS through `dataAvailable_` instead (see
    // `signalCtlosAvailable()`), and the re-poll pulls it.
    if (!hasCtlosSource) {
      parkedOnDataOnly_ = true;
      co_await dataAvailable_.wait();
      parkedOnDataOnly_ = false;
    } else if (mayPullCtlos) {
      co_await coro::simWaitFirst(
          dataAvailable_.wait(), ctlosAvailable_.wait());
    } else {
      co_await coro::simWaitFirst(
          dataAvailable_.wait(),
          ctlosAvailable_.wait(),
          ctlosGate_.startOneShot(ctlosReadyAt_ - ctx_->now()));
      // A losing operand keeps its scheduled resume by contract; clear it so
      // the next `startOneShot` does not trip the single-in-flight XCHECK.
      ctlosGate_.stop();
    }
    if (FLAGS_packsim_trace_wire_events) {
      XLOGF(INFO, "TRACE-DRV T={:014} ev=IDLE-WAKE", ctx_->nowFs());
    }
  }
}

LinkLayerScheduler::TxTiming LinkLayerScheduler::beginTx(
    const PacketPtr& pkt,
    const TxKind kind) {
  const Time txCompleteTime = computeTxCompleteTime(pkt, linkRate_);
  if (FLAGS_packsim_trace_wire_events) {
    XLOGF(
        INFO,
        "TRACE-DRV T={:014} ev=TX-START-{} txComplete={}",
        ctx_->nowFs(),
        traceLabel(kind),
        txCompleteTime.getNs());
  }
  // Sample the propagation delay at serialization start; `endTx` hands it to
  // `scheduleRemoteReceive`.
  return TxTiming{
      .txComplete = txCompleteTime,
      .propDelay = channel_->startSerialize(pkt, port_)};
}

void LinkLayerScheduler::endTx(
    const PacketPtr& pkt,
    const Time propDelay,
    const TxKind kind) {
  channel_->scheduleRemoteReceive(pkt, port_, propDelay);
  if (kind == TxKind::CTLOS) {
    // Start the §5.2.11.2 spacing budget for the next CtlOS from the moment
    // this one leaves the wire.
    ctlosReadyAt_ = ctx_->now() + minCtlosSpacing_;
  }
  if (FLAGS_packsim_trace_wire_events) {
    XLOGF(
        INFO,
        "TRACE-DRV T={:014} ev=TX-DONE-{}",
        ctx_->nowFs(),
        traceLabel(kind));
  }
}

const char* LinkLayerScheduler::traceLabel(const TxKind kind) noexcept {
  switch (kind) {
    case TxKind::CTLOS:
      return "NONPRE";
    case TxKind::DATA:
      return "PRE";
  }
  // Also reached by a value outside the enum.
  return "PRE";
}

coro::SimResultTask<void> LinkLayerScheduler::sendDataFrame(
    const PacketPtr& pkt) {
  // Preemptible transmit (PFC or data). The wire-time stopwatch counts
  // down the local serialization budget. When CtlOS arrives mid-frame,
  // `pause()` the stopwatch, drain every queued CtlOS frame via
  // `arbiter_->nextCtlosFrame()`, then `resume()` — pause/resume preserves
  // elapsed time so the original budget is honored across any number
  // of splices. Once the stopwatch finally expires, `scheduleRemoteReceive`
  // fires with the propagation delay sampled at `startSerialize` time.
  //
  // Drain-all on each wake: when several CtlOS pulses coalesce into a
  // single wake, draining the entire queue before resuming is cheaper
  // and avoids a boundary-condition leak where a popped frame could be
  // thrown away if the wire-time expired on the same PDES timestamp.
  // The propagation delay is sampled up front — held across splices and
  // passed to scheduleRemoteReceive once the wire-time budget is fully
  // consumed.
  const TxTiming tx = beginTx(pkt, TxKind::DATA);

  // `maxDuration` is unused — start() with txComplete as the
  // budget. `pause()` / `resume()` preserve elapsed across splices.
  coro::SimStopwatch wireTimer{ctx_, tx.txComplete};
  wireTimer.start();

  // End of the §5.2.11.2 no-preempt prefix, as an absolute deadline. Wall clock
  // is exact despite the pausable wire stopwatch: splicing is precisely what
  // the prefix forbids, so the wire cannot pause inside this window.
  const Time noPreemptUntil = ctx_->now() + noPreemptPrefix_;

  while (true) {
    // The spacing gate joins the race whenever it is shut, so the burst drain
    // below resumes exactly when the budget expires. Ordering matters: the wire
    // stopwatch stays operand 0 so `winner == 0` still means "frame complete",
    // and both CtlOS operands mean "re-examine the queue".
    std::size_t winner{};
    if (ctlosSpacingElapsed()) {
      winner = co_await coro::simWaitFirst(
          wireTimer.expiration(), ctlosAvailable_.wait());
    } else {
      winner = co_await coro::simWaitFirst(
          wireTimer.expiration(),
          ctlosAvailable_.wait(),
          ctlosGate_.startOneShot(ctlosReadyAt_ - ctx_->now()));
      // See `txDriver`: clear the losing operand's resume before the next arm.
      ctlosGate_.stop();
    }

    if (winner == 0) {
      // Stopwatch expired — wire serialization fully consumed. Commit
      // the remote-receive event for the data frame.
      endTx(pkt, tx.propDelay, TxKind::DATA);
      co_return;
    }

    // CtlOS wake. Pause the wire stopwatch (preserves elapsed), drain
    // every queued CtlOS frame via the port's pull API, then resume.
    wireTimer.pause();

    // §5.2.11.2: a CtlOS may not preempt inside the first 256 B of a packet,
    // and the spacing rule still applies mid-frame. When either forbids the
    // splice, resume immediately — the CtlOS is not consumed and goes out at
    // the next opportunity.
    //
    // The prefix deliberately gets no wake of its own: §5.2.11.2 permits
    // preemption after byte 256 but does not require it, so a CtlOS held inside
    // the prefix rides out the frame and the completion re-poll releases it.
    // The spacing case does get one — the next loop iteration arms
    // `ctlosGate_`.
    if (ctx_->now() < noPreemptUntil || !ctlosSpacingElapsed()) {
      if (FLAGS_packsim_trace_wire_events) {
        XLOGF(INFO, "TRACE-DRV T={:014} ev=SPLICE-DEFER", ctx_->nowFs());
      }
      wireTimer.resume();
      continue;
    }
    if (FLAGS_packsim_trace_wire_events) {
      // One SPLICE-WAKE per wake event (NOT per spliced frame) — a
      // single ctlosAvailable_ pulse may coalesce multiple posts
      // into one wake that drains several frames below.
      XLOGF(INFO, "TRACE-DRV T={:014} ev=SPLICE-WAKE", ctx_->nowFs());
    }

    // §5.2.11.2's spacing is between ANY two CtlOS, so it also bounds this
    // burst drain — otherwise a coalesced wake emits the whole backlog
    // back-to-back. The first iteration is already cleared by the guard above;
    // later ones re-check and leave the remainder queued for the next wake,
    // which the outer loop arms `ctlosGate_` for.
    while (ctlosSpacingElapsed()) {
      const auto ctlosFrame = arbiter_->nextCtlosFrame();
      if (!ctlosFrame.has_value()) {
        break;
      }
      // The same non-preemptible CtlOS transmit as `txDriver`'s, then notify
      // completion.
      const TxTiming ctlosTx = beginTx(ctlosFrame->pkt, TxKind::CTLOS);
      co_await coro::simScopedDelay(ctx_, ctlosTx.txComplete);
      endTx(ctlosFrame->pkt, ctlosTx.propDelay, TxKind::CTLOS);
      arbiter_->onFrameTransmitted(*ctlosFrame);
    }

    wireTimer.resume();
  }
}

} // namespace packsim::wire
