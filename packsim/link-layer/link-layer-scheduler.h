/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <optional>

#include <folly/memory/not_null.h>

#include "packsim/kernel/coro/SimEvent.h"
#include "packsim/kernel/coro/SimScopedTask.h"
#include "packsim/kernel/coro/SimStopwatch.h"
#include "packsim/kernel/coro/SimTask.h"
#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {
class EthernetChannel;
class EthernetPort;
class TxArbiter;
} // namespace packsim

namespace packsim::wire {

/**
 * Coroutine-based wire driver for `EthernetPort`.
 *
 * Owns a single `txDriver` coroutine (an owned `SimScopedTask`) that pulls one
 * frame at a time from `TxArbiter::nextOutboundFrame()` / `nextCtlosFrame()`
 * pull API and pushes it onto the wire. A CtlOS, and a PFC or data frame on a
 * port with no CtlOS producer (neither LLR nor CBFC), cannot be preempted, so
 * the driver transmits it inline: `beginTx`, a delay for the serialization
 * time, `endTx`. On a port with a CtlOS producer a PFC or data frame goes
 * through `sendDataFrame`, which handles PCS splice: when a CtlOS arrives
 * mid-frame, the wire stopwatch pauses, the CtlOS is drained via
 * `nextCtlosFrame()` and transmitted, then the stopwatch resumes.
 *
 * Priority is encoded inside `nextOutboundFrame()` — the scheduler is
 * deliberately ignorant of priority tiers, allowing the port to evolve
 * its source mix (e.g., add a new CoS class) without scheduler changes.
 *
 * Lifetime: owned by `EthernetPort`. Construct, call `start()` (which
 * eager-starts and OWNS the `txDriver` as a `SimScopedTask` member), then
 * `shutdown()` (also called by the destructor) `reset()`s that task —
 * destroying the coroutine frame wherever it is parked. The frame unwind
 * cancels its installed waiters and scheduled PDES events via the awaitables'
 * own destructors (`simWaitFirst` teardown, `SimScopedDelay`), so teardown is
 * safe at ANY suspension point, mid-transmit included. No cooperative shutdown
 * signal is needed and no await site mentions shutdown.
 */
class LinkLayerScheduler final {
 public:
  /**
   * Construct a scheduler bound to `channel` with `port` as its "self"
   * identity. `linkRate` is the link rate used to compute wire-serialization
   * time per frame. All five arguments are required and stay fixed for
   * the scheduler's lifetime. `ctx`, `channel`, `port` and `arbiter` must
   * outlive the scheduler; `channel`, `port` and `arbiter` are owned by the
   * topology / port layer.
   *
   * @param ctx      Simulation context. Must outlive the scheduler.
   * @param channel  Wire medium. Must outlive the scheduler.
   * @param port     Self identity port. Must outlive the scheduler.
   * @param arbiter  Chooses the next frame to transmit. Must outlive the
   *                 scheduler.
   * @param linkRate Link rate for computing wire-serialization time
   */
  LinkLayerScheduler(
      folly::not_null<SimContext*> ctx,
      folly::not_null<EthernetChannel*> channel,
      folly::not_null<EthernetPort*> port,
      folly::not_null<TxArbiter*> arbiter,
      DataRate linkRate) noexcept;

  LinkLayerScheduler(const LinkLayerScheduler&) = delete;
  LinkLayerScheduler& operator=(const LinkLayerScheduler&) = delete;
  LinkLayerScheduler(LinkLayerScheduler&&) = delete;
  LinkLayerScheduler& operator=(LinkLayerScheduler&&) = delete;

  ~LinkLayerScheduler();

  /**
   * Eager-start the `txDriver` coroutine and store it as an owned
   * `SimScopedTask`. Idempotent (a second call is a no-op). The coroutine runs
   * until `shutdown()` (or the destructor) `reset()`s it.
   */
  void start();

  /**
   * Tear down the `txDriver` by `reset()`-ing the owned `SimScopedTask`:
   * `std::coroutine_handle::destroy()` unwinds the frame wherever it is parked
   * and the awaitables' destructors cancel its installed waiters / scheduled
   * PDES events. Safe with a transmission in flight. Idempotent (reset() on an
   * empty / already-reset task is a no-op).
   */
  void shutdown();

  // ===== Wake signals =====
  //
  // Two SimEvents, one per frame category. Each wake pulses exactly one of
  // them (no double-pulse). The txDriver's idle wait awaits both via
  // simWaitFirst -- or only `dataAvailable_` on a port with no CtlOS
  // producer (neither LLR nor CBFC), where nothing pulses the other.
  //
  //  * `dataAvailable_` — pulsed by `signalDataAvailable()`: PFC, LLR data
  //    or the non-LLR egress queue (depending on port mode), and CBFC's
  //    credit return and queued CC_Update. Also pulsed by
  //    `signalCtlosAvailable()` instead of `ctlosAvailable_` while the
  //    driver is parked on `dataAvailable_` alone.
  //
  //  * `ctlosAvailable_` — pulsed only by `signalCtlosAvailable()` (LLR's
  //    CtlOS slots, CBFC's CF_Update). Wakes both the idle wait AND the
  //    PCS-splice race in `sendDataFrame`'s inner `simWaitFirst`
  //    (wireTimer.expiration() vs ctlosAvailable_), so a data-signal pulse
  //    during a data transmission doesn't spuriously splice.

  /**
   * Pulse the data availability signal. Called after a frame that is not a
   * CtlOS may have become sendable: PFC, LLR data or non-LLR egress queue
   * enqueues, and CBFC credit returns or CC_Update.
   */
  void signalDataAvailable() noexcept;

  /**
   * Pulse the CtlOS-specific signal. Wired up by the port's
   * `LlrCtlosSlots::onPost` callback (via `onCtlosPosted`) and by CBFC's
   * CtlOS wake, so any post into the CtlOS slots wakes both the txDriver's
   * idle wait (via simWaitFirst) and any in-flight non-CtlOS transmission's
   * PCS-splice race. When the driver parked before the port had a CtlOS
   * producer it waits on the data signal alone, so that one is pulsed
   * instead; a frame already on the wire then finishes unspliced and the
   * CtlOS goes out right after it.
   */
  void signalCtlosAvailable() noexcept;

  // ===== Test affordances =====

  [[nodiscard]] bool isStarted() const noexcept {
    return started_;
  }

  [[nodiscard]] bool isShuttingDown() const noexcept {
    return shuttingDown_;
  }

 private:
  coro::SimScopedTask txDriver();

  // Which transmit path a frame takes. A CtlOS is never preempted; a PFC or
  // data frame may be spliced into by a CtlOS. Names the frame in the wire
  // trace (`NONPRE` / `PRE`), and a CtlOS also restarts the §5.2.11.2
  // spacing budget when it leaves the wire.
  enum class TxKind { CTLOS, DATA };

  // One frame's serialization, as `beginTx` started it.
  struct TxTiming {
    // Wire-serialization time: how long the transmitter stays busy.
    Time txComplete;
    // Propagation delay sampled at serialization start, handed to `endTx`.
    Time propDelay;
  };

  /**
   * Put `pkt` on the wire: compute its serialization time and start
   * serializing it on the channel. The caller waits `txComplete` (or, in
   * `sendDataFrame`, runs it down on a pausable stopwatch) and then calls
   * `endTx`.
   *
   * @param pkt   The frame to transmit.
   * @param kind  Its transmit path.
   *
   * @return      The frame's serialization time and propagation delay.
   */
  TxTiming beginTx(const PacketPtr& pkt, TxKind kind);

  /**
   * Finish a frame `beginTx` started, once its serialization time has run
   * out: schedule its arrival at the far end and, for a CtlOS, start the
   * §5.2.11.2 spacing budget for the next one.
   *
   * @param pkt        The frame that finished serializing.
   * @param propDelay  `TxTiming::propDelay` from its `beginTx`.
   * @param kind       Its transmit path, as passed to `beginTx`.
   */
  void endTx(const PacketPtr& pkt, Time propDelay, TxKind kind);

  // Trace label of a transmit path: `NONPRE` for a CtlOS, `PRE` otherwise.
  static const char* traceLabel(TxKind kind) noexcept;

  /**
   * Preemptible TX path — used for PFC and data frames on a port with a
   * CtlOS producer (LLR or CBFC). Races the
   * wire-time stopwatch against `ctlosAvailable_`; on every wake,
   * drains the pending CtlOS (splicing each frame into the in-flight
   * wire window) and re-arms the stopwatch with whatever wire-time
   * is left.
   *
   * @param pkt  The data or PFC frame to transmit
   * @return     Awaitable task that completes when serialization finishes
   */
  coro::SimResultTask<void> sendDataFrame(const PacketPtr& pkt);

  const folly::not_null<SimContext*> ctx_;

  // Wire-medium and self-identity. The scheduler holds these so it
  // can call `channel_->startSerialize(pkt, port_)` directly without
  // a callback indirection.
  const folly::not_null<EthernetChannel*> channel_;
  // The channel identifies the transmitting endpoint by port, so the
  // driver needs it for serialization even though frame selection is the
  // arbiter's job.
  const folly::not_null<EthernetPort*> port_;
  const folly::not_null<TxArbiter*> arbiter_;

  // Link rate. Used to compute wire-serialization time per frame
  // (`bytesToTime` on `phyFrameSize` and `interFrameGapSize`).
  const DataRate linkRate_;

  // Pulse: see "Wake signals" above for who broadcasts here.
  coro::SimEvent dataAvailable_;

  // Pulse: CtlOS-specific arrival signal. Wakes the idle wait (via
  // simWaitFirst) and the inlined preemption race during in-flight
  // non-CtlOS transmission.
  coro::SimEvent ctlosAvailable_;

  /**
   * Test UEC 1.0.3 §5.2.11.2's minimum-spacing rule for the next CtlOS.
   *
   * @return  True when another CtlOS may be inserted. Always true while the
   *          rule is off: `ctlosReadyAt_` then never advances past `now`.
   */
  [[nodiscard]] bool ctlosSpacingElapsed() const noexcept {
    return ctx_->now() >= ctlosReadyAt_;
  }

 public:
  /**
   * Enable UEC 1.0.3 §5.2.11.2's CtlOS insertion rules on this port. Both
   * rules default to off (0).
   *
   * MUST be called before `start()`; there is no defined behaviour for
   * changing the rules on a port that is already transmitting.
   *
   * @param minSpacingBytes       Minimum wire distance between two CtlOS.
   *                              §5.2.11.2 requires at least 400 B; 0
   *                              disables the rule.
   * @param noPreemptPrefixBytes  Head-of-packet bytes a CtlOS may not preempt
   *                              inside. §5.2.11.2 requires 256; 0 disables.
   */
  void setCtlosInsertionRules(
      uint32_t minSpacingBytes,
      uint32_t noPreemptPrefixBytes) noexcept {
    minCtlosSpacing_ = linkRate_.bytesToTime(minSpacingBytes);
    noPreemptPrefix_ = linkRate_.bytesToTime(noPreemptPrefixBytes);
  }

 private:
  // Minimum wire distance between two CtlOS, as wire time at `linkRate_`;
  // zero disables the rule. UEC 1.0.3 §5.2.11.2 caps CtlOS at under 2% of
  // link bandwidth "by guaranteeing a minimum distance of at least 400 B
  // between two CtlOS", and holds the rule "even if ... separated only by
  // IDLE characters": this is wire time, not a count of intervening data
  // bytes.
  Time minCtlosSpacing_{};

  // Head of a packet during which a CtlOS may not preempt, as wire time at
  // `linkRate_`; zero disables the rule. UEC 1.0.3 §5.2.11.2: "CtlOS are
  // allowed to preempt a packet except: within the first 256 bytes of a
  // packet."
  Time noPreemptPrefix_{};

  // Earliest time the next CtlOS may leave the wire under §5.2.11.2's spacing
  // rule. Advanced by `endTx` for a CtlOS; zero until the first CtlOS goes
  // out.
  Time ctlosReadyAt_{};

  // Wakes the driver exactly when `ctlosReadyAt_` arrives, as a losing-safe
  // `simWaitFirst` operand. `ctlosAvailable_` is a PULSE event, so a CtlOS
  // already queued when the spacing gate defers a pull has no producer left to
  // re-announce it; without this operand the driver parks and the frame is
  // stranded until unrelated traffic happens to wake it. Armed only through
  // `startOneShot`, which supplies the duration per arm.
  coro::SimStopwatch ctlosGate_;

  // True only while the txDriver is parked on `dataAvailable_` alone, which it
  // does when the port has no CtlOS producer. `signalCtlosAvailable()` then
  // pulses `dataAvailable_`, so a producer enabled since cannot strand a CtlOS.
  bool parkedOnDataOnly_ = false;

  // One-shot false→true; `start()` is idempotent against it.
  bool started_ = false;

  // One-shot false→true; set by `shutdown()`. Backs `isShuttingDown()`.
  bool shuttingDown_ = false;

  // The owned wire-driving coroutine. Declared LAST so it is destroyed
  // (reset) before the `SimEvent` members it may be parked on — though
  // `shutdown()` (called by the destructor) already reset()s it first.
  coro::SimScopedTask txDriver_;
};

} // namespace packsim::wire
