/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>

#include <folly/logging/xlog.h>
#include <folly/memory/not_null.h>

#include "packsim/if/gen-cpp2/switches_types.h"
#include "packsim/kernel/coro/SimEvent.h"
#include "packsim/kernel/coro/SimScopedTask.h"
#include "packsim/kernel/coro/SimSignal.h"
#include "packsim/kernel/coro/SimStopwatch.h"
#include "packsim/kernel/coro/SimTask.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/ctlos-source.h"
#include "packsim/link-layer/llr/llr-ctlos-slots.h"
#include "packsim/link-layer/llr/llr-tx-buffer.h"
#include "packsim/link-layer/llr/llr-types.h"
#include "packsim/link-layer/tx-packet-source.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim::llr {

/**
 * One frame returned by `Llr::pullNextNewFrame()`. The packet's LLR
 * header is already stamped via `stampLlrHeader(pkt, type, seq)` before
 * it leaves the helper. Callers forward `pkt` directly to the wire.
 */
struct PulledFrame {
  PacketPtr pkt;
  // LLR_ELIGIBLE or LLR_INELIGIBLE.
  FrameType type;
  // Valid only when `type == LLR_ELIGIBLE`; 0 otherwise.
  uint32_t seq;
};

/**
 * One frame returned by `Llr::pullNextReplayFrame()`. The packet's LLR
 * header is already stamped as LLR_ELIGIBLE + the buffer-recorded seq.
 * Callers forward `pkt` directly to the wire.
 */
struct ReplayFrame {
  PacketPtr pkt;
  uint32_t seq;
};

/**
 * Link Level Retry (LLR) module per UEC 1.0.3 §5.1.
 *
 * The hot path (per-packet `rxFrame`, `rxCtlos`) is synchronous. The TX
 * state machine and the periodic LLR_INIT / LLR_ACK emitters are
 * coroutines spawned at `start()` and torn down at `shutdown()`; the
 * ACK/NACK SM and the spec timers are not (see "## Coroutines" below).
 *
 * ## Lifecycle
 *
 *   construction → start() → [running] → shutdown() → destruction
 *
 * `start()` spawns the coroutines (as owned `SimScopedTask` members) and
 * brings the SMs into their initial state. With
 * `LlrConfig::skipInitNegotiation` set, as it is by default for a link
 * configured with LLR, that is the post-handshake state: the TX SM begins in
 * ADVANCE. Otherwise the TX SM begins in LLR_OFF; transition to INIT happens
 * when `setLlrModeRemote(true)` is called, which `EthernetPort::enableLlr`
 * does right after `start()`.
 *
 * `shutdown()` tears the coroutines down structurally: it `reset()`s each
 * owned `SimScopedTask`, destroying the suspended frame (and, for the TX SM
 * dispatcher, cascade-destroying the `txRun*` helper it is `co_await`ing).
 * Each coroutine parks only on destroy-safe constructs (`collectAny` races
 * or bare awaits of self-cancelling awaitables), so the frame unwind cancels
 * every armed waiter — no cooperative `shutdownRequested_` flag and no
 * per-frame shutdown check. It then `cancel()`s the member timer stopwatches,
 * which may have been armed synchronously by rule call sites with no coroutine
 * awaiting them (the frame unwind alone cannot cancel those). `shutdown()` is
 * idempotent and `~Llr` calls it, so teardown is RAII-safe even if the owner
 * never calls it explicitly.
 *
 * ## TX path — scheduler pulls
 *
 * Outbound data frames flow via the scheduler-driven pull API:
 * `pullNextNewFrame()` for ADVANCE-state admission and
 * `pullNextReplayFrame()` for REPLAY-state retransmits. Both stamp the
 * LLR header and bump stats internally, so the wire driver
 * (`TxArbiter::nextOutboundFrame`) only has to forward the
 * returned packet to the wire and call back via `notifyNewFrameTransmitted`
 * / `notifyReplayFrameTransmitted` at TX-complete time.
 *
 * `pullNextNewFrame()` reads the upstream per-CoS queues through the
 * `TxPacketSource` interface (see `tx-packet-source.h`). The owning
 * port implements `TxPacketSource` and wires itself in via
 * `setTxPacketSource(this)`. This decouples `Llr` from the concrete
 * `EthernetPort` type, so the LLR code does not depend on the port that owns
 * it.
 *
 * ## Receive side — a filter, not a conduit
 *
 * `rxFrame()` does not deliver anything. It updates the RX counters and the
 * AN-SM and returns an `RxDisposition`; the caller owns the packet throughout
 * and performs the delivery itself. That is what lets an LLR-eligible frame
 * and an ordinary one reach the MAC client by the same path.
 *
 * CtlOS frames do NOT flow through `rxFrame()`. They are posted into
 * `ctlosSlots()` (a `LlrCtlosSlots` owned by this `Llr`) and handed to the
 * wire through `popCtlos()`, which the port's CtlOS arbiter calls when this
 * producer wins its turn. The owning port installs an `onPost` callback
 * on the slots to receive wake notifications.
 *
 * ## Coroutines
 *
 *   - `txStateMachine` — TX SM dispatcher (LLR_OFF / INIT / ADVANCE /
 *     REPLAY / FLUSH per UEC §5.1.5 Fig 5-3). The three spec timers
 *     (replay / data_age / pcs_lost_status, Tbl 5-10) are observed
 *     directly by `txRunAdvance` via each `SimStopwatch::expiration()`
 *     awaitable; their other rules (arm/reset/stop) are implemented
 *     synchronously at the call sites.
 *   - `initCtlosLoop` — periodic LLR_INIT emission while in INIT
 *     (§5.1.9.1.1). Timer is armed by `transitionTxStateTo` on INIT
 *     entry and stopped on INIT exit.
 *   - `ackCtlosLoop` — periodic LLR_ACK emission while AN-SM is in
 *     SEND_ACKS (§5.1.9.1.3). Per-period state-gated emit.
 *
 * The AN-SM (UEC §5.1.6) is NOT a coroutine — it is a set of
 * synchronous methods (`anSmOn*`) called inline from `rxFrame()` and
 * `rxCtlos()` (the latter fires `anSmOnInitEchoSent()` at the moment
 * an LLR_INIT_ECHO is posted onto the `ctlosSlots_`). See the
 * comment on the `anSmOn*` declarations.
 *
 * ## Synchronous hot path
 *
 *   - `rxFrame()` / `rxCtlos()`: full §5.1.7 / §5.1.9 handling.
 *   - `pullNextNewFrame()` / `pullNextReplayFrame()`: called by the wire
 *     driver per frame; share `admissionDecision` + `stampLlrHeader`.
 */
class Llr : public ICtlosSource {
 public:
  /**
   * What the caller should do with a frame it has just handed to
   * `rxFrame()`. `Llr` filters on the receive side and never takes
   * ownership: the caller keeps the packet and performs the delivery
   * itself, so an LLR-eligible frame and an ordinary one reach the MAC
   * client by the same path.
   */
  enum class RxDisposition : uint8_t {
    // Pass the frame to the MAC client: LLR_INELIGIBLE frames, and
    // LLR_ELIGIBLE frames with expected_frame_ok (GOOD or POISONED FCS,
    // expected seq).
    DELIVER,
    // Filtered by LLR (UEC §5.1.7 p. 476): bad FCS, a duplicate or
    // missing sequence number, or a frame arriving before LLR_INIT has
    // established `next_rx_seq`.
    DROP,
  };

  /**
   * @param ctx        Simulation context. MUST outlive this Llr instance
   *                   AND any in-flight coroutine spawned by `start()`.
   * @param config     Configuration values (see `LlrConfig` in
   *                   `llr-types.h`).
   */
  Llr(folly::not_null<SimContext*> ctx, const LlrConfig& config);

  Llr(const Llr&) = delete;
  Llr& operator=(const Llr&) = delete;
  Llr(Llr&&) = delete;
  Llr& operator=(Llr&&) = delete;

  /**
   * Destructor. Calls `shutdown()` (idempotent) for RAII-safe teardown, so an
   * owner that forgot to shut down explicitly does not leave a synchronously-
   * armed timer's scheduled resume to trip the `~SimStopwatch` XCHECK. Owners
   * that drive an explicit lifecycle (e.g. `EthernetPort::disableLlr()`)
   * still call `shutdown()` themselves; the destructor's call is then a no-op.
   */
  ~Llr() override;

  // ===== Lifecycle =====

  /**
   * Spawn all coroutines. Idempotent — second call is a no-op. With
   * `LlrConfig::skipInitNegotiation` it first puts both SMs in their
   * post-handshake states and turns both mode controls ON.
   *
   * Install the scheduler-wake callback first (`setOnSchedulerWakeCallback`
   * XCHECKs this); `setTxPacketSource` and the `ctlosSlots()` post callback
   * only have to precede the first pull or post. `setLlrModeLocal` may come
   * before or after, but a change through `setLlrModeRemote` must come after
   * (see there). XCHECKs if called after `shutdown()` (see `shutdown()`).
   */
  void start();

  /**
   * Tear down all coroutines. `reset()`s each owned `SimScopedTask`
   * (destroying its suspended frame; the TX SM dispatcher cascade-destroys
   * the helper it is awaiting), then `cancel()`s the member timers. After
   * `shutdown()` returns, no further coroutine activity occurs.
   *
   * Idempotent: only the first call acts, even one made before `start()`.
   * Such a call tears nothing down but still latches, so `start()` XCHECKs if
   * it follows: every later `shutdown()`, the destructor's included, would be
   * a no-op, and a timer `start()` armed would trip the `~SimStopwatch`
   * XCHECK when this `Llr` is destroyed.
   */
  void shutdown();

  // ===== Spec-mandated mode controls (UEC §5.1.4 Tbl 5-9) =====

  /**
   * Set `llr_mode_local` (Tbl 5-9). Determines whether the receive side
   * accepts and processes LLR_INIT CtlOS. Persists across coroutine state.
   */
  void setLlrModeLocal(bool on);

  /**
   * Set `llr_mode_remote` (Tbl 5-9). Setting the current value does nothing.
   * A change resets `tx_seq` to 0 and `acked_seq` to all-ones (Tbl 5-10),
   * which leaves no frame outstanding, so it also discards the replay buffer,
   * stops replay_timer and data_age_timer, and leaves a REPLAY batch in
   * progress nothing more to resend.
   *
   * The TX SM learns of a change by being woken and reads the value only
   * when it runs, so a change undone before it runs reaches it as a wake
   * with the value unchanged: INIT then moves to ADVANCE without an
   * LLR_INIT_ECHO, and ADVANCE stays put, neither renegotiating the reset
   * `tx_seq`. Otherwise only LLR_OFF, INIT and ADVANCE wait to be woken:
   *   - false → INIT and ADVANCE move to LLR_OFF. REPLAY gets there through
   *     ADVANCE, which checks the value on entry, once its batch completes;
   *     FLUSH ignores the change (see `managementReset`).
   *   - true → LLR_OFF moves to INIT and the periodic LLR_INIT loop starts.
   *
   * XCHECKs if it changes the value before `start()`, unless
   * `LlrConfig::skipInitNegotiation` is set: LLR_OFF waits for a change
   * rather than reading the value, so an earlier change would be lost and
   * the link would run without LLR. With `skipInitNegotiation`, `start()`
   * turns the mode ON itself.
   */
  void setLlrModeRemote(bool on);

  /**
   * Spec p. 472 (Tbl 5-10): `LLR_status` derives from PCS_status when
   * llr_mode_remote is OFF. PackSim treats PCS_status as TRUE by default.
   */
  void onPcsStatusChange(bool up);

  /**
   * Management intervention for a TX SM parked in FLUSH with
   * `re_init_on_discard == FALSE` (UEC §5.1.5 p. 473: "If the value is
   * FALSE, then the LLR will wait for management intervention"): moves it to
   * LLR_OFF. Only that parked FLUSH waits for the request, so in any other TX
   * state it is lost and the TX SM carries on. In every state it drives the
   * AN-SM to OFF (UEC §5.1.6 Fig 5-4 "reset" edge), which clears
   * `next_rx_seq_vld`.
   *
   * It does not renegotiate. LLR_OFF waits for the next `llr_mode_remote`
   * change, so while that mode stays ON, frames pass through unprotected
   * until `setLlrModeRemote(false)` and then `setLlrModeRemote(true)` move
   * the TX SM to INIT.
   */
  void managementReset();

  /**
   * Wire-adapter notification: the LAST in-flight replay frame's
   * TX-complete fired (i.e., the wire is no longer busy with this REPLAY
   * batch). Used by `txRunReplay` to gate REPLAY → ADVANCE on
   * `replay_done` per UEC §5.1.5.
   */
  void notifyReplayBatchComplete();

  /**
   * Wire-adapter notification: a single replay frame's TX-complete fired.
   * Implements the replay_timer rule 3 trigger ("activated when a replay
   * frame TX completes" — UEC §5.1.5 Tbl 5-10) by arming the stopwatch
   * inline. `TxArbiter::onFrameTransmitted` calls this for every replay
   * frame.
   */
  void notifyReplayFrameTransmitted();

  /**
   * Wire-adapter notification: a single new (non-replay) LLR-eligible data
   * frame's TX-complete fired. Implements replay_timer rule 2 and
   * data_age_timer rule 2 ("activated when the end of a new LLR frame is
   * transmitted" — UEC §5.1.5 Tbl 5-10) by arming both stopwatches inline.
   */
  void notifyNewFrameTransmitted();

  /**
   * Counts a frame the caller is sending LLR_INELIGIBLE because
   * `admissionDecision` returned `PASSTHROUGH_TX`. `pullNextNewFrame` counts
   * its own passthroughs; callers that build their frame instead of pulling it
   * from the CoS queue -- the CBFC CC_Update path -- have to say so here, or
   * `txPassthrough` under-reports how much traffic left the link outside
   * ADVANCE.
   */
  void notifyPassthroughTransmitted();

  // ===== Hot-path methods (synchronous, called per packet) =====

  /**
   * UEC §5.1.7 — RX entry. Updates the RX counters and the AN-SM, and
   * returns whether the caller should deliver the frame:
   *   - LLR_INELIGIBLE: always `DELIVER` (passed through unfiltered).
   *   - LLR_ELIGIBLE: compute expected/duplicate/missing predicates per
   *     Tbl 5-11; `DELIVER` on expected_frame_ok, otherwise `DROP` and
   *     pulse the AN-SM channels accordingly.
   *
   * Drops silently (no NACK) when `next_rx_seq_vld == FALSE` — the link
   * partner's `replay_timer` will trigger replay since we cannot send an
   * LLR_NACK before LLR_INIT establishes `next_rx_seq`.
   */
  [[nodiscard]] RxDisposition rxFrame(
      const PacketPtr& packet,
      FrameType frameType,
      FcsStatus fcsStatus,
      uint32_t frameSeq);

  /**
   * UEC §5.1.9 — CtlOS receive dispatch. Per LlrCtlosType:
   *   - LLR_INIT: validates `llr_mode_local`, sets `next_rx_seq`, posts
   *     LLR_INIT_ECHO.
   *   - LLR_INIT_ECHO: pulses `initEchoReceived_` if `seq == tx_seq` so
   *     the TX SM can transition INIT → ADVANCE.
   *   - LLR_ACK / LLR_NACK: validates `ack_nack_seq_ok` (per §5.1.8); on
   *     advance, calls `txBuffer_->removeUpTo(seq)` and re-arms the
   *     replay/data_age stopwatches inline. LLR_NACK additionally pulses
   *     `nackReceivedEvent_` to trigger REPLAY.
   *
   * Counter increments are unconditional per spec — even ignored CtlOS
   * (e.g., INIT with `llr_mode_local==FALSE`) increments their `rx*Ctlos`
   * counter before being dropped.
   */
  void rxCtlos(LlrCtlosType type, uint32_t seq, uint16_t initData);

  // ===== Observability =====

  /**
   * Read-only stats access (UEC §5.1.11 Tbl 5-13 counters).
   */
  [[nodiscard]] const Stats& stats() const noexcept {
    return stats_;
  }

  /**
   * Project this module onto the thrift result schema: the UEC §5.1.11
   * Tbl 5-13 counters above, plus the replay buffer's current and peak
   * occupancy and the two state-machine states. Those six are live module
   * state rather than counters, which is why the projection belongs here and
   * not on `Stats`. See the `switches.thrift` field comments for per-field
   * semantics.
   */
  [[nodiscard]] packsim::switches::thrift::LlrStats exportThriftStats() const;

  /**
   * Current TX state machine state (UEC §5.1.5 Fig 5-3).
   */
  [[nodiscard]] TxState txState() const noexcept {
    return txState_;
  }

  // ===== State introspection =====

  /**
   * Current ACK/NACK TX state machine state (UEC §5.1.6 Fig 5-4).
   */
  [[nodiscard]] AckNackState ackNackState() const noexcept {
    return ackNackState_;
  }

  /**
   * Current `next_rx_seq_vld` (UEC §5.1.7 Tbl 5-11). For test
   * introspection — set TRUE when an LLR_INIT is received while
   * `llr_mode_local` is ON (`rxCtlos`) or by `start()` when INIT negotiation
   * is skipped, FALSE when AN-SM enters OFF (`anSmReset`).
   */
  [[nodiscard]] bool nextRxSeqVld() const noexcept {
    return nextRxSeqVld_;
  }

  /**
   * Current `tx_seq` (UEC §5.1.5 Tbl 5-10). For test introspection.
   */
  [[nodiscard]] uint32_t txSeq() const noexcept {
    return txSeq_;
  }

  /**
   * True iff `replay_ct` has reached `config.replayCtMax` (per spec Tbl
   * 5-10 p. 472). The sentinel value `kReplayCtMaxNoLimit` (`0xFF`) means
   * "no maximum" — replays never trigger FLUSH on this counter.
   */
  [[nodiscard]] bool replayCtIsMax() const noexcept {
    return (config_.replayCtMax != kReplayCtMaxNoLimit) &&
        (replayCt_ >= config_.replayCtMax);
  }

  /**
   * LLR_status per spec p. 472. TRUE iff TX SM ∈ {ADVANCE, REPLAY}.
   */
  [[nodiscard]] bool llrStatus() const noexcept {
    return txState_ == TxState::ADVANCE || txState_ == TxState::REPLAY;
  }

  /**
   * Number of LLR-eligible frames currently in the replay buffer (i.e.,
   * admitted to LLR but not yet acknowledged). Equal to `outstanding_seq`
   * per spec p. 471. For test introspection.
   *
   * Invariant: `txBuffer_` is constructed during `Llr::Llr()` and reset
   * only by the destructor — so any external caller observing the object
   * is guaranteed `txBuffer_` is non-null. DCHECK so production builds
   * pay no overhead and debug builds catch lifecycle bugs.
   */
  [[nodiscard]] size_t outstandingSeq() const noexcept {
    XDCHECK(txBuffer_);
    return txBuffer_->outstandingSeq();
  }

  /**
   * Bytes currently held in the replay buffer (sum of held frames' sizes).
   * For test introspection. Same `txBuffer_` non-null invariant as
   * `outstandingSeq()`.
   */
  [[nodiscard]] uint64_t outstandingData() const noexcept {
    XDCHECK(txBuffer_);
    return txBuffer_->outstandingData();
  }

  /**
   * High-water marks of `outstandingSeq()` / `outstandingData()` over the
   * buffer's lifetime. `exportThriftStats` reports them as the
   * `txBufferPeak*` thrift fields. Same `txBuffer_` non-null invariant as
   * `outstandingSeq()`. Return types mirror their non-peak counterparts.
   */
  [[nodiscard]] size_t outstandingSeqPeak() const noexcept {
    XDCHECK(txBuffer_);
    return txBuffer_->peakSeq();
  }

  [[nodiscard]] uint64_t outstandingDataPeak() const noexcept {
    XDCHECK(txBuffer_);
    return txBuffer_->peakBytes();
  }

  // ----- ICtlosSource -----
  //
  // `LlrCtlosSlots` holds which report is pending and at what priority; the
  // wire frame is built here, when the arbiter asks, so `ack_nack_seq` is the
  // value current at transmission rather than when the report was queued
  // (UEC §5.1.9.1.3/.4).

  /**
   * Whether an LLR CtlOS is waiting to go out.
   *
   * @return True iff any of the three priority slots is occupied. Three, not
   *         four, because ACK and NACK are mutually exclusive and share one.
   */
  [[nodiscard]] bool hasPendingCtlos() const override {
    return !ctlosSlots_->empty();
  }

  /**
   * Build the highest-priority pending LLR CtlOS (Tbl 5-12 order:
   * INIT_ECHO, then INIT, then ACK/NACK).
   *
   * @return The frame, or nullopt when no slot is occupied.
   */
  [[nodiscard]] std::optional<PacketPtr> popCtlos() override {
    auto payload = ctlosSlots_->popHighest();
    if (!payload) {
      return std::nullopt;
    }
    return buildCtlosPacket(*payload);
  }

  /**
   * Direct access to the LlrCtlosSlots. The owning port uses it to install
   * the `onPost` wake callback; tests also post reports and assert
   * pending state through it. Reference (not pointer): the queue is non-null
   * for the entire Llr lifetime, so a pointer invites no-op null checks.
   */
  LlrCtlosSlots& ctlosSlots() noexcept {
    return *ctlosSlots_;
  }

  // ===== Wire-driver hook =====
  //
  // The wire driver (`LinkLayerScheduler` +
  // `TxArbiter::nextOutboundFrame`) calls two pull helpers
  // (`pullNextReplayFrame` in REPLAY, `pullNextNewFrame` in every other TX
  // state). Both stamp + bump stats internally so `TxArbiter` only
  // forwards the returned packet.
  //
  // The owning port installs a wake callback through
  // `setOnSchedulerWakeCallback` so `Llr` can pulse the scheduler on state
  // changes (REPLAY entry, ACK-frees-buffer, etc.).

  /**
   * Callback fired by `Llr` when a state change calls for the wire
   * driver to be woken (REPLAY entry, ACK-frees-buffer, etc.).
   * The typical adapter binds it to
   * `LinkLayerScheduler::signalDataAvailable`.
   */
  using OnSchedulerWakeCallback = std::function<void()>;

  /**
   * Install the wire-driver wake callback. `Llr` fires it whenever
   * `transitionTxStateTo` changes the TX state, and in these cases:
   *   - `txRunReplay` wakes the scheduler on REPLAY entry so the port
   *     pulls replay frames via `pullNextReplayFrame()`.
   *   - `rxCtlos` after `removeUpTo()` calls `onSchedulerWake()` so any
   *     previously-blocked admission retries. The wake is unconditional
   *     (cheap Pulse signal; no per-port "backpressure" tracking).
   *
   * Required, and must be called BEFORE `start()` (XCHECKs). Unit tests
   * that drive no scheduler install a no-op callback, because
   * `onSchedulerWake()` XCHECKs that one is set.
   *
   * Decoupling `Llr` from the concrete `LinkLayerScheduler` type via a
   * callback (rather than a typed pointer) keeps the LLR code independent
   * of the scheduler: `llr.cpp` does not include `link-layer-scheduler.h`.
   */
  void setOnSchedulerWakeCallback(OnSchedulerWakeCallback onWake);

  /**
   * Log this port's counter totals at DBG.
   *
   * Emitted at DBG rather than INFO so a multi-port sweep does not produce one
   * always-on line per port at teardown. Turn it on with `--logging=DBG` to see
   * how ACKs were triggered: `txAck` counts ACK posts to the CtlOS slots (a
   * pending ACK may be overwritten before it is sent) and is what thrift
   * exports as `acksSent`; it equals `txAckPer` plus `txAckByte`, the posts
   * from the periodic loop and from the rxFrame byte trigger. The thrift
   * `LlrStats` export carries only `txAck`, so this is the only place the
   * split can be read or diffed across versions.
   *
   * @param deviceId  Identifies the port in the log line.
   * @param portNum   Identifies the port in the log line.
   */
  void logCounterSummary(const std::string& deviceId, uint16_t portNum) const;

  /**
   * Wire the upstream TX packet source (the per-CoS application
   * queues). The source's `peekNextAppPacket()` returns the next
   * eligible packet without popping; `getTxPacket()` does the actual
   * pop with all post-pop hooks (VOQ-based ECN marking, mark callback,
   * ECN stats). Set once in `EthernetPort::enableLlr` after
   * `Llr` construction; remains stable for the port's lifetime.
   *
   * Required by `pullNextNewFrame()`. Pass `nullptr` only in unit tests
   * that don't exercise the pull path.
   *
   * @param source  The port's per-CoS application queues. Non-owning.
   */
  void setTxPacketSource(TxPacketSource* FOLLY_NULLABLE source) noexcept {
    packetSource_ = source;
  }

  /**
   * True iff no REPLAY snapshot is left to pull: `pullNextReplayFrame()` has
   * drained it, or `discardReplayBuffer()` emptied the buffer. Used by the
   * wire driver to decide when to fire `notifyReplayBatchComplete()`.
   */
  [[nodiscard]] bool isReplaySnapshotDrained() const noexcept {
    return !replaySnap_.has_value();
  }

  /**
   * 5-way admission decision for a frame of `bytes` size, derived from
   * `txState_` and the configured per-state `FrameBehavior`s
   * (`UNKNOWN` + the four real outcomes). Used by `pullNextNewFrame`, and
   * by `TxArbiter` for CBFC CC_Update frames, to dispatch on state ×
   * config; the caller chooses how to interpret each outcome (see
   * `AdmissionResult` docstring).
   */
  [[nodiscard]] AdmissionResult admissionDecision(
      uint32_t bytes) const noexcept;

  /**
   * Admit a frame to the replay buffer: reserve the current `tx_seq`, store
   * the frame, bump `stats_.txOk`, advance `tx_seq`, and stamp the frame
   * LLR_ELIGIBLE with the reserved sequence number.
   *
   * Stamping is part of admission rather than a separate step the caller
   * performs, because the two must not come apart: a frame sitting in the
   * replay buffer whose header still reads LLR-ineligible is sequenced as far
   * as this sender is concerned and unsequenced as far as the peer is.
   *
   * Most frames arrive here through `pullNextNewFrame`. A CBFC CC_Update calls
   * it directly — UEC 1.0.3 §5.2.9 requires a CC_Update to be LLR-eligible and
   * carry a valid sequence number, but it is built by CBFC rather than pulled
   * from a CoS queue.
   *
   * Pre-condition: the caller has verified ADVANCE state and the
   * `outstandingSeq` / `outstandingData` caps via `admissionDecision()`
   * returning `ADMIT_AND_TX`. A nullopt return means the replay buffer refused
   * the frame and it MUST NOT be transmitted — an unsequenced send leaves the
   * peer waiting on a sequence number that never arrives.
   *
   * @param packet  The frame to admit; stamped in place on success.
   *
   * @return        The reserved sequence number, or nullopt if the replay
   *                buffer is full.
   */
  [[nodiscard]] std::optional<uint32_t> admit(const PacketPtr& packet);

  // ===== Pull primitives (used by the wire driver) =====

  /**
   * Pull the next new (non-replay) frame from `packetSource_`'s CoS queues,
   * dispatching on `admissionDecision`:
   *   - `ADMIT_AND_TX`    → admit + stamp ELIGIBLE; return frame with
   *                         type=LLR_ELIGIBLE, seq=assigned
   *   - `PASSTHROUGH_TX`  → stamp INELIGIBLE; return frame with
   *                         type=LLR_INELIGIBLE, seq=0
   *   - `DROP`            → pop, bump `stats_.txDiscard`, return nullopt
   *   - `BLOCK`           → return nullopt; packet stays in queue
   * Returns `nullopt` when the queue is empty.
   *
   * The returned packet's LLR header is already stamped; callers
   * forward it directly to the wire.
   *
   * Pre-condition: `setTxPacketSource(source)` must have been called
   * (XCHECKs `packetSource_ != nullptr`).
   */
  [[nodiscard]] std::optional<PulledFrame> pullNextNewFrame();

  /**
   * Pull the next REPLAY-state frame from the unacked-range snapshot.
   * Lazy-snapshots `txBuffer_->snapshotUnackedRange()` on first call
   * per REPLAY entry (held in `replaySnap_` + `replayNextSeq_`);
   * skips seqs that `lookup` returns null for (ACKed since snapshot);
   * stamps the LLR header and bumps `stats_.txReplay` /
   * `stats_.txReplayBytes` before returning. Returns `nullopt` when
   * the snapshot is exhausted (cursor auto-resets to nullopt when
   * drained so the next REPLAY entry re-snapshots cleanly).
   *
   * Called once per frame by the wire driver while the TX SM is in
   * REPLAY.
   */
  [[nodiscard]] std::optional<ReplayFrame> pullNextReplayFrame();

  /**
   * Stamp `pkt`'s LlrHeader for a wire emission of `(type, seq)`.
   * Used by `pullNextNewFrame` and `pullNextReplayFrame` so the wire
   * driver receives an already-stamped packet — no LLR-header
   * knowledge required outside this module.
   *
   * For `LLR_ELIGIBLE`: enabled=true, seqNum=seq.
   * For `LLR_INELIGIBLE`: enabled=false (pass-through frame; `seqNum` is
   * left untouched, since it is meaningful only when `enabled==true`).
   */
  static void
  stampLlrHeader(const PacketPtr& pkt, FrameType type, uint32_t seq);

  /**
   * Build the `LlrCtlosFrame` that carries the pending report `f` onto the
   * wire. The function holds no state, so `popCtlos()` calls it on whichever
   * report `LlrCtlosSlots` yields at transmit time.
   */
  [[nodiscard]] static PacketPtr buildCtlosPacket(
      const LlrCtlosPayload& f) noexcept;

 private:
  /**
   * Fire the `onSchedulerWake_` callback; XCHECKs that one is installed.
   * Called from `rxCtlos` after `removeUpTo()` frees buffer space (LLR_ACK
   * or LLR_NACK), from `transitionTxStateTo` on state changes, and from
   * `txRunReplay` on REPLAY entry. The callback's wake is
   * cheap (Pulse signal: dropped if no waiter; otherwise one
   * priority-poll cycle at worst), so we wake on every buffer-free
   * rather than tracking an "was-an-admission-pending" flag.
   */
  void onSchedulerWake() noexcept;

  // ===== Coroutines =====

  /**
   * TX state machine (UEC §5.1.5 Fig 5-3). Runs all transitions among
   * LLR_OFF / INIT / ADVANCE / REPLAY / FLUSH. The body is a thin
   * dispatcher that delegates each state's behavior to a per-state
   * `txRunXxx()` helper. Helpers mutate `txState_` directly via
   * `transitionTxStateTo()` and return no value; the dispatcher's
   * next iteration reads the updated `txState_` to pick the next branch.
   *
   * Owned as a `SimScopedTask` member (`txStateMachine_`); `shutdown()`
   * `reset()`s it, destroying the suspended dispatcher frame — which
   * cascade-destroys the `txRun*` helper it is currently `co_await`ing. No
   * per-iteration shutdown check is needed.
   */
  coro::SimScopedTask txStateMachine();

  /**
   * Per-state TX SM helpers. Each runs the body of one state — awaits
   * the relevant events and calls `transitionTxStateTo()` if a transition
   * is warranted. The dispatcher simply `co_await`s the helper for the
   * current state and lets the `transitionTxStateTo()` side-effect drive
   * the next iteration.
   *
   * Helpers do NOT handle shutdown themselves: teardown is structural —
   * `shutdown()` `reset()`s the owning `txStateMachine_`, destroying the
   * dispatcher frame and (by cascade) the helper currently being
   * `co_await`'d at its `co_await` suspension point. Each helper parks only
   * on destroy-safe constructs (`collectAny` races or bare awaits of
   * self-cancelling awaitables), so the frame unwind cancels every armed
   * waiter cleanly.
   */
  coro::SimResultTask<void> txRunLlrOff();
  coro::SimResultTask<void> txRunInit();
  coro::SimResultTask<void> txRunAdvance();
  coro::SimResultTask<void> txRunReplay();
  coro::SimResultTask<void> txRunFlush();

  /**
   * Periodic LLR_INIT emission while TX SM is in INIT (UEC §5.1.9.1.1
   * p. 477). Period is `4 × ctlosTargetSpacingBytes` translated to time
   * at the configured link rate. Owned as a `SimScopedTask` member
   * (`initCtlosLoop_`); `shutdown()` `reset()`s it.
   */
  coro::SimScopedTask initCtlosLoop();

  /**
   * ===== ACK/NACK state machine (UEC §5.1.6 Fig 5-4) =====
   *
   * The AN-SM is a set of synchronous methods (not a coroutine). Each
   * method mutates `ackNackState_` synchronously inside the caller's
   * stack frame, so any subsequent synchronous code (notably
   * `rxFrame()`'s byte-on-receive ACK trigger) observes the
   * post-transition state immediately. `ackCtlosLoop` polls
   * `ackNackState_` per period and emits only when in SEND_ACKS — no
   * separate wake-up signal is needed.
   *
   * All four state transitions of Fig 5-4 are covered:
   *   - `anSmOnInitEchoSent()`: OFF → SEND_ACKS (when llrModeLocal)
   *   - `anSmOnExpectedFrameOk()`: NACK_SENT → SEND_ACKS
   *   - `anSmOnGapDetected()`: SEND_ACKS → SEND_NACK → NACK_SENT
   *         (single call covers both spec edges; SEND_NACK is
   *         instantaneous per §5.1.9.1.4 — post NACK then move on)
   *   - `anSmReset()`: any state → OFF (on llr_mode_local OFF / reset)
   */
  void anSmOnInitEchoSent();
  void anSmOnExpectedFrameOk();
  void anSmOnGapDetected();
  void anSmReset();

  /**
   * Post an LLR_ACK while emitting a `TRACE-ENQ` line iff
   * `FLAGS_packsim_trace_wire_events` is set. `triggerSrc` is the
   * trace-line src label (e.g., "BYTE" or "PERIODIC") — NOT a
   * network source device.
   */
  void postAckTraced(uint32_t ackSeq, const char* triggerSrc) noexcept;

  /**
   * Periodic LLR_ACK emission while ACK/NACK SM is in SEND_ACKS (UEC
   * §5.1.9.1.3). Period derives from `ctlosTargetSpacingBytes`. Owned as a
   * `SimScopedTask` member (`ackCtlosLoop_`); `shutdown()` `reset()`s it.
   */
  coro::SimScopedTask ackCtlosLoop();

  /**
   * Recompute whether `pcs_lost_status_timer` should be running per its
   * spec rules 1 (stopped in LLR_OFF/INIT/FLUSH) and 2 (activated when
   * PCS_status==FALSE). Called from `transitionTxStateTo` and
   * `onPcsStatusChange`. Rule 3 (expiration → FLUSH) is observed by
   * `txRunAdvance` directly via `pcsLostStatusTimer_->expiration()`.
   */
  void updatePcsLostStatusTimer();

  /**
   * Transition TX state and run side effects: recompute
   * `pcs_lost_status_timer` per rule 1 (stopped in LLR_OFF/INIT/FLUSH);
   * arm/disarm `initCtlosTimer_` on INIT entry/exit per §5.1.9.1.1.
   */
  void transitionTxStateTo(TxState newState);

  /**
   * Implement the "set to zero, then activate if outstanding_seq > 0
   * otherwise held at zero" rule shape used by both replay_timer rule 4
   * and data_age_timer rule 3 (UEC §5.1.5 Tbl 5-10). Stopwatch is passed
   * in to keep `rxCtlos` ACK/NACK handlers symmetric.
   */
  void restartIfOutstandingFrames(coro::SimStopwatch& stopwatch);

  /**
   * Discard every frame in the replay buffer along with the state that tracks
   * them: replay_timer and data_age_timer stop (rule 1 of each, UEC §5.1.5
   * Tbl 5-10: held at zero whenever outstanding_seq is zero), and a REPLAY
   * batch in progress has nothing left to pull. Called on FLUSH entry and on
   * every `llr_mode_remote` change.
   */
  void discardReplayBuffer() noexcept;

  // ===== State =====

  const folly::not_null<SimContext*> ctx_;
  const LlrConfig config_;

  // TX path variables (UEC §5.1.5 Tbl 5-10). The three `*_timer_expired`
  // spec fields are observable directly via `timer_->expired()` — no
  // bool shadow is needed (the SimStopwatch's EXPIRED state persists
  // until an explicit `stop()` or `reset()`).
  TxState txState_ = TxState::LLR_OFF;
  uint32_t txSeq_ = 0;
  uint8_t replayCt_ = 0;
  bool nackReceived_ = false;
  // Default TRUE per spec p. 472.
  bool pcsStatus_ = true;

  // REPLAY snapshot iterator state. `pullNextReplayFrame()` lazy-takes
  // the snapshot from `txBuffer_->snapshotUnackedRange()` on first call
  // per REPLAY entry, walks `replayNextSeq_` through the range
  // (skipping ACKed seqs via `lookup`), and sets `replaySnapshotExhausted_`
  // when the range drains. `transitionTxStateTo(REPLAY)` resets the
  // exhausted flag so the next REPLAY entry re-snapshots cleanly.
  //
  // The exhausted flag matters because the underlying `txBuffer_` is NOT
  // cleared by replay (frames live until ACK'd). Without the flag, lazy
  // re-init after drain would re-snapshot the still-unacked buffer, so a
  // caller that pulls until nullopt would never finish. `TxArbiter` stops
  // pulling on its own: the batch-complete signal moves the TX SM to
  // ADVANCE after the last replay frame. `discardReplayBuffer()` sets the
  // flag too: a discarded buffer leaves nothing to replay.
  std::optional<LlrTxBuffer::SnapshotRange> replaySnap_;
  uint32_t replayNextSeq_ = 0;
  bool replaySnapshotExhausted_ = false;

  // ACK/NACK SM state
  AckNackState ackNackState_ = AckNackState::OFF;

  // RX path variables (UEC §5.1.7 Tbl 5-11)
  uint32_t nextRxSeq_ = 0;
  bool nextRxSeqVld_ = false;

  // True once any LLR-eligible frame has been received. Gates the
  // periodic ACK loop so idle ports (which may have `nextRxSeqVld_`
  // pre-set via `skipInitNegotiation`) don't emit spurious ACKs.
  bool firstFrameReceived_ = false;

  // Bytes received since the last on-receive ACK was emitted. Counts
  // only `expected_frame_ok` frames (GOOD or POISONED FCS, expected
  // seq) — the same frames whose receipt advances `next_rx_seq` per
  // UEC §5.1.7 Tbl 5-11. Drives the on-receive synchronous ACK trigger
  // in `rxFrame`: once the counter crosses `ctlosTargetSpacingBytes`,
  // an ACK is fired immediately without waiting for `ackCtlosLoop`'s
  // next periodic tick. The sync and periodic paths may both emit an
  // ACK for the same `next_rx_seq − 1`; the spec (§5.1.9.1.3) does
  // not require dedup.
  uint64_t bytesSinceLastAck_ = 0;

  // Mode controls (UEC §5.1.4)
  bool llrModeLocal_ = false;
  bool llrModeRemote_ = false;

  Stats stats_;

  // Lifecycle flags
  bool started_ = false;
  bool shuttingDown_ = false;

  // Inter-coroutine event channels.
  //
  // Naming + mode reflects the producer→consumer fan-out. Conventions:
  //   - `SimSignal` for single-consumer channels (signal counts queue;
  //     missed signals are remembered as pending, useful when the
  //     producer may fire before the consumer has armed its await).
  //   - `SimEvent::Pulse` for multi-consumer broadcasts where each
  //     consumer re-checks observable state on every wake (missed
  //     pulses are harmless).
  //
  // Shutdown is NOT a channel: it is structural (`shutdown()` `reset()`s the
  // owned `SimScopedTask` members below, destroying the coroutine frames).
  // There is no `shutdownRequested_` event.
  //
  // Declaration order matters: the `SimScopedTask` members are declared LAST
  // so `~Llr` destroys (resets) them BEFORE these events/signals — though the
  // real teardown is `shutdown()`, which the owner calls first.

  // ===== RX-CtlOS → TX-side (single consumer) =====
  // initEchoReceived: rxCtlos LLR_INIT_ECHO → txStateMachine, drives
  //                   INIT → ADVANCE per Fig 5-3.
  coro::SimSignal initEchoReceived_{ctx_};
  // nackReceived: rxCtlos LLR_NACK → txStateMachine. Pulse (vs Signal)
  //               because the consumer re-checks `nackReceived_` boolean
  //               on every wake; missed broadcasts are recovered via the
  //               boolean.
  coro::SimEvent nackReceivedEvent_{ctx_};

  // ===== Wire adapter → TX SM =====
  // replayBatchComplete: wire adapter signals via
  // `notifyReplayBatchComplete()` when the LAST in-flight replay frame's
  // TX-complete callback fires. `txRunReplay` awaits this in REPLAY's
  // tail to gate REPLAY → ADVANCE until `replay_done` per UEC §5.1.5.
  // Single consumer; SimSignal so missed pulses are remembered as pending.
  coro::SimSignal replayBatchComplete_{ctx_};

  // ===== Operator-level intent events that the TX SM waits on. =====
  // PULSE, and unlike `nackReceivedEvent_` nothing recovers a missed pulse:
  // LLR_OFF and a parked FLUSH wait for the edge without re-reading any
  // state, so a broadcast with no waiter is lost (see `setLlrModeRemote`,
  // `managementReset`).
  coro::SimEvent managementResetRequested_{ctx_};
  coro::SimEvent llrModeRemoteChanged_{ctx_};

  // Owned components.
  std::unique_ptr<LlrTxBuffer> txBuffer_;
  std::unique_ptr<LlrCtlosSlots> ctlosSlots_;

  // Timer stopwatches owned at the Llr scope. Unique pointers because
  // each SimStopwatch needs `ctx_` and a config-derived `maxDuration`
  // which are only available after the constructor's initializer list
  // runs — emplaced in the constructor body. The three spec timers
  // (replay/data_age/pcs_lost_status) are manipulated directly by the
  // synchronous rule call sites; the two CtlOS-emission timers
  // (ack/init) are reset/awaited inside their respective coroutine
  // loops.
  std::unique_ptr<coro::SimStopwatch> replayTimer_;
  std::unique_ptr<coro::SimStopwatch> dataAgeTimer_;
  std::unique_ptr<coro::SimStopwatch> pcsLostStatusTimer_;
  std::unique_ptr<coro::SimStopwatch> ackCtlosTimer_;
  std::unique_ptr<coro::SimStopwatch> initCtlosTimer_;

  // Wire-driver wake callback, set via `setOnSchedulerWakeCallback()`.
  // Required: `onSchedulerWake()` XCHECKs that it is set. Callback-based
  // (vs typed pointer) decouples `Llr` from `wire::LinkLayerScheduler`.
  OnSchedulerWakeCallback onSchedulerWake_;

  // Upstream TX packet source — wired by `setTxPacketSource()` from
  // `EthernetPort::enableLlr()`. Read by `pullNextNewFrame()`
  // to call `packetSource_->peekNextAppPacket()` +
  // `packetSource_->getTxPacket()`. Stays nullptr in unit tests that exercise
  // only Llr internals.
  TxPacketSource* FOLLY_NULLABLE packetSource_ = nullptr;

  // Owned driver coroutines (started in `start()`, torn down in
  // `shutdown()` via `reset()`). Declared LAST so `~Llr` destroys them
  // before any event/signal/timer they may be suspended on — their frame
  // unwind cancels every armed waiter via the destroy-safe `collectAny`
  // races and self-cancelling bare awaitables. `txStateMachine_` owns the
  // dispatcher; resetting it cascade-destroys the `txRun*` helper it is
  // `co_await`ing.
  coro::SimScopedTask txStateMachine_;
  coro::SimScopedTask initCtlosLoop_;
  coro::SimScopedTask ackCtlosLoop_;
};

} // namespace packsim::llr
