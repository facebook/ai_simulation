/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

#include <folly/memory/not_null.h>

#include "packsim/if/gen-cpp2/switches_types.h"
#include "packsim/kernel/coro/SimScopedTask.h"
#include "packsim/kernel/coro/SimStopwatch.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/link-layer/cbfc/cbfc-config.h"
#include "packsim/link-layer/cbfc/cbfc-credit-tracker.h"
#include "packsim/link-layer/cbfc/cbfc-sender-ledger.h"
#include "packsim/link-layer/cbfc/cbfc-types.h"
#include "packsim/link-layer/cbfc/cc-range-scheduler.h"
#include "packsim/link-layer/cbfc/credit-observer.h"
#include "packsim/link-layer/cbfc/vc-mapper.h"
#include "packsim/link-layer/ctlos-source.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim::cbfc {

/**
 * CBFC (Credit-Based Flow Control, UEC 1.0.3 §5.2) per-link module — peer to
 * `llr::Llr`, owned by `EthernetPort`. Plays BOTH link roles:
 *
 *  - Sender (egress): the credit gate (`canTransmit`) + consume (`onTransmit`),
 *    `CF_Update` reception (`rxCfUpdate`), and periodic `CC_Update` generation.
 *  - Receiver (ingress): per-VC credit tracking fed by the receiver buffer's
 *    store/drain events (it IS the `ICreditObserver`), `CF_Update` generation
 *    (it is also an `ICtlosSource`), and `CC_Update` reconciliation
 *    (`rxCcUpdate`).
 *
 * The port drives the module: it calls the credit gate on the TX pull path,
 * pulls `CF_Update` through the `CtlosArbiter` round robin and `popCcUpdate()`
 * directly, feeds RX messages, and registers the wake callbacks that re-pump
 * the wire.
 *
 * Not thread-safe. It lives on one PDES context; call from the sim thread only.
 */
class Cbfc : public ICreditObserver, public ICtlosSource {
 public:
  /**
   * Invoked when the wire should be re-pumped. `setOnWakeCallback` and
   * `setOnCtlosReadyCallback` must stay on separate signals.
   */
  using OnWakeCallback = std::function<void()>;

  /**
   * Invoked when a CF_Update is ready to go out on the CtlOS signal. Same
   * signature as `OnWakeCallback`, deliberately a distinct type so a member or
   * parameter declares which of the two signals it carries.
   */
  using OnCtlosReadyCallback = std::function<void()>;

  /**
   * Build the CBFC module for one link end from the link's configuration.
   *
   * Takes the thrift struct rather than the translated `CbfcConfig`: the
   * translation narrows signed wire values into unsigned ones and turns
   * id-keyed collections into dense vectors, after which an out-of-range field
   * can no longer be diagnosed.
   *
   * @param simCtx   Simulation context; timers are scheduled against it and it
   *                 must outlive this module.
   * @param config   The link's CBFC settings, as configured.
   * @param linkBps  The port's bit rate. Port state rather than link config,
   *                 but the byte-denominated CF_min and CF_max timers cannot
   *                 be converted to wire time without it.
   *
   * @throw std::invalid_argument if any field is out of range. See
   *        `makeCbfcConfig` in cbfc-config.h for what is checked.
   */
  Cbfc(
      folly::not_null<SimContext*> simCtx,
      const packsim::thrift::CbfcConfig& config,
      uint64_t linkBps);
  /**
   * Calls `shutdown()`, so a queued timer event can never outlive the module.
   * Owners driving an explicit lifecycle (`EthernetPort::disableCbfc`)
   * still call `shutdown()` themselves.
   */
  ~Cbfc() override;
  Cbfc(const Cbfc&) = delete;
  Cbfc& operator=(const Cbfc&) = delete;
  Cbfc(Cbfc&&) = delete;
  Cbfc& operator=(Cbfc&&) = delete;

  // ----- Lifecycle -----
  /**
   * Arm the periodic CC_Update and CF_max-refresh timers. Idempotent: a
   * second call while already running arms no second timer chain.
   */
  void start();
  /**
   * Cancel the periodic CC_Update and CF_max timers. Idempotent, and safe
   * before `start()`. Cancellation is required: a queued timer resume holds
   * `this` and would otherwise run against a destroyed module.
   *
   * PRECONDITION: no CBFC loop body may be on the stack. `reset()`ing a
   * coroutine frame that is executing is undefined, and the loops do run
   * port code: `onWake()` / `onCtlosReady()` reach
   * `LinkLayerScheduler::signal*Available`, which resumes the TX driver
   * INLINE via `SimEvent::broadcastInline` rather than deferring it. The
   * precondition holds because the only production caller is
   * `EthernetPort::disableCbfc`, itself reached only from
   * `~EthernetPort`. `NicRxCreditBridge` depends on the same
   * teardown-only constraint; making CBFC runtime-toggleable breaks both.
   */
  void shutdown();

  // ----- Initialization (sim bootstrap; replaces LLDP §5.2.8) -----
  /**
   * Override CreditSize from the receiver buffer at sim-init bootstrap.
   *
   * Only the 0 case is rejected here. A resolved size must also fit the 12-bit
   * S_CreditSize wire field, and that check is the CALLER's job:
   * `bootstrapCbfcCredits` in `flex-topology.cpp` calls
   * `CbfcCreditTracker::validateCreditSize` against the resolved size
   * immediately BEFORE calling this setter.
   *
   * @param creditSizeBytes  Bytes per credit, resolved from the receiver's
   *                         cell size at sim-init bootstrap.
   * @throw std::invalid_argument if `creditSizeBytes` is 0.
   */
  void setCreditSize(uint32_t creditSizeBytes);
  // Bytes per credit in force on this link.
  [[nodiscard]] uint32_t creditSizeBytes() const noexcept {
    return config_.creditSizeBytes;
  }
  /**
   * Initialize the sender credit limits from the partner receiver's buffer.
   *
   * @param totalCredits  The partner receiver buffer's pooled credit limit.
   *
   * @throw std::invalid_argument if `totalCredits` exceeds
   *        `CbfcSenderLedger::kMaxPortCreditLimit` (2^15 - 1), the bound
   *        `CbfcSenderLedger::setPortCreditLimit` enforces so a CF_Update's
   *        advance stays readable.
   */
  void initSenderPortLimit(uint64_t totalCredits);
  /**
   * Apply `config.vcMinCredits`, `config.overshootReserveCredits` and
   * `config.vcGroups` to the sender ledger. Call after `initSenderPortLimit`:
   * the shared pool is what remains of the pooled limit once both are
   * reserved. Group limits deliberately do not enter the feasibility result —
   * the feasibility check covers the per-VC and overshoot reserves only,
   * because group limits may oversubscribe the shared pool.
   *
   * @return  False if the reservations exceed the pooled limit, leaving no
   *          shared pool — the caller should surface this as a misconfigured
   *          link rather than silently starving unreserved VCs.
   */
  [[nodiscard]] bool initSenderReservations();

  // ----- Receiver role: ICreditObserver (fed by the buffer) -----
  void onPacketStored(const Packet& pkt) override;
  void onPacketDrained(const Packet& pkt) override;

  /**
   * Record that the receive buffer is holding `heldCredits` of `pkt`'s cost
   * and free the rest immediately. PackSim credits only a switch's headroom (a
   * model choice; see `FbSwitch::getLosslessRxCreditBytes`), so only credit
   * for cells held there is kept until the packet leaves.
   *
   * Call once at enqueue. The held portion is released later by
   * `onCreditsFreed` when the packet drains or is dropped. A cell count may be
   * passed directly: `resolveLinkCreditSizeBytes` in `flex-topology.cpp`
   * enforces `CreditSize == CellSize`.
   *
   * @param pkt          The packet being enqueued.
   * @param heldCredits  Credits the buffer keeps until the packet leaves.
   *                     Clamped to the packet's whole cost.
   */
  void onCreditsHeld(const Packet& pkt, uint32_t heldCredits);

  /**
   * Free `credits` against `pkt`'s VC (§5.2.5.7.2). No-op for a best-effort
   * packet or a zero count, so an `onCreditsHeld` call that holds the whole
   * packet never dirties a VC or spends a CtlOS slot.
   *
   * @param pkt      The packet whose VC the credits are freed against.
   * @param credits  Credits to free.
   */
  void onCreditsFreed(const Packet& pkt, uint32_t credits);

  /**
   * @param pkt  The packet to classify against this link's VC map.
   * @return     True if `pkt` maps to a lossless, credit-gated VC.
   */
  [[nodiscard]] bool isLosslessPacket(const Packet& pkt) const {
    return losslessVcOf(pkt).has_value();
  }
  /**
   * Record a BUFFER_FULL drop on a lossless VC — a flow-control correctness
   * violation (§5.2.5.6.1). MUST stay 0 when credits are sized correctly.
   */
  void onLosslessVcBufferFullDrop() noexcept {
    ++stats_.losslessVcBufferFullDrops;
  }

  // ----- Sender role: TX credit gate (called on the scheduler pull path) -----
  /**
   * Whether `pkt` may be transmitted now.
   *
   * Best-effort packets always pass; lossless-VC packets are gated on
   * available credits (§5.2.5.6.1). A pure query -- it consumes nothing, so
   * the caller must follow a true result with `onTransmit`.
   *
   * @param pkt  The head-of-line packet the scheduler is offering.
   * @return     True if the credit gate admits it.
   */
  [[nodiscard]] bool canTransmit(const Packet& pkt) const;
  /**
   * Account a transmitted packet's credits (§5.2.5.6.2). No-op for
   * best-effort packets.
   *
   * @param pkt  The packet that was transmitted.
   */
  void onTransmit(const Packet& pkt);
  /**
   * Record that the TX gate held back a ready lossless packet for lack of
   * credit (§5.2.5.6.1). Telemetry only: `canTransmit` is a pure const query,
   * so the port bumps this when it acts on a `false` gate result.
   */
  void onGateBlocked() noexcept {
    ++stats_.gateBlockedCount;
  }

  // ----- CF_Update producer (receiver role; ICtlosSource) -----
  //
  // The payload is built here, at transmit time, rather than when the credits
  // were freed: `packDirtyReports` reads `R_VC_CF` out of the tracker when the
  // arbiter asks, so the report carries everything freed up to that instant.

  /**
   * Whether a CF_Update is ready to go out right now.
   *
   * Ready means some VC's `R_VC_CF` has changed since it was last reported,
   * and the CF_min spacing since the previous CF_Update has elapsed.
   *
   * @return True iff `popCtlos()` would produce a frame.
   */
  [[nodiscard]] bool hasPendingCtlos() const override;

  /**
   * Build a CF_Update from the VCs currently owing a report.
   *
   * Mutating: the reported VCs are marked clean, so their credits are handed
   * out once.
   *
   * @return The frame, or nullopt if nothing is ready.
   */
  [[nodiscard]] std::optional<PacketPtr> popCtlos() override;

  /**
   * Take the next CF_Update payload, if one is ready.
   *
   * The payload step `popCtlos()` builds its frame from: same readiness rule,
   * same mark-clean side effect, stopping short of the wire format.
   *
   * @return The payload, or nullopt if nothing is ready.
   */
  [[nodiscard]] std::optional<CbfcCfUpdatePayload> popCfUpdate();
  /**
   * Build the wire packet for a CF_Update CtlOS payload: a `CfUpdateFrame`,
   * which is an 8-byte PCS ordered set rather than an Ethernet frame.
   */
  [[nodiscard]] static PacketPtr buildCfUpdatePacket(
      const CbfcCfUpdatePayload& payload);

  // ----- CC_Update producer (sender role; port pulls on the data path) ----
  /**
   * Builds the next CC_Update the CC_msg timer has queued, filling in the
   * running per-VC consumed-credit totals for its range, and clears that
   * range's pending flag. A link with more than 16 VCs has two ranges
   * (`VC_0_15`, `VC_16_31`), popped round-robin; see `CcRangeScheduler`.
   *
   * The totals are sampled at pop rather than when the report was queued, so
   * they describe exactly the packets already on the wire. The caller must
   * transmit on the same pull that pops: a packet sent in between would sit
   * ahead of the CC_Update while being absent from its total, and the peer
   * would count its credits twice.
   *
   * @return The pending CC_Update packet, or nullopt if none is due.
   */
  [[nodiscard]] std::optional<PacketPtr> popCcUpdate();

  // Whether a CC_Update is waiting. Lets the port decide whether it can be
  // sent before popping one, since `popCcUpdate()` clears the pending flag and
  // bumps `ccUpdatesSent`.
  [[nodiscard]] bool hasPendingCcUpdate() const noexcept {
    return ccRanges_.hasQueued();
  }

  // Whether CC_Update takes an LLR sequence number and a replay-buffer entry
  // (UEC §5.2.9). See `CbfcConfig::ccUpdateLlrEligible`.
  [[nodiscard]] bool ccUpdateLlrEligible() const noexcept {
    return config_.ccUpdateLlrEligible;
  }

  // ----- RX handlers (called from the port RX path) -----
  // Apply a received CF_Update (sender role, §5.2.5.6.3).
  void rxCfUpdate(const CbfcCfUpdatePayload& payload);
  /**
   * Apply a received CC_Update (receiver role, §5.2.5.7.4).
   *
   * Reconciles this receiver's `R_VC_CC` against the sender's `S_VC_CC`,
   * reclaiming credits leaked by dropped packets. An entry whose implied
   * LostCredits exceeds `kMaxLostCredits` -- 2^14-1, the error threshold in
   * §5.2.5.7.4's Implementation Note -- is rejected whole and counted in
   * `ccUpdateLostCreditsOutOfRangeCount` rather than folded in.
   *
   * @param pkt The received CC_Update frame.
   */
  void rxCcUpdate(const EthernetCcPacket& pkt);

  // Data-path wake: returned credits may unblock a gated VC, or a CC_Update
  // (an ordinary Ethernet packet) is queued for the outbound pull.
  void setOnWakeCallback(OnWakeCallback cb) {
    onWake_ = std::move(cb);
  }

  // CtlOS wake: a CF_Update is ready. This must reach the scheduler's CtlOS
  // signal — it is the only signal the in-flight splice path awaits, so a
  // CF_Update announced on the data signal cannot preempt a frame already on
  // the wire and waits for the frame to drain (UEC 1.0.3 §5.2.7.2).
  void setOnCtlosReadyCallback(OnCtlosReadyCallback cb) {
    onCtlosReady_ = std::move(cb);
  }
  // Cumulative counters for this link, surfaced through `PortStats`.
  [[nodiscard]] const Stats& stats() const noexcept {
    return stats_;
  }
  /**
   * Project this link's counters onto the thrift result schema. See the
   * `switches.thrift` field comments for per-field semantics.
   */
  [[nodiscard]] packsim::switches::thrift::CbfcStats exportThriftStats() const;
  // VCs configured on this link, 1..kCbfcLimits.maxVcs.
  [[nodiscard]] uint8_t numVcs() const noexcept {
    return config_.numVcs;
  }

 private:
  // Delegated to by the public constructor once `makeCbfcConfig` has validated
  // and translated the link config. It must stay private: `validateCbfcConfig`
  // is the only pass over these fields, so a translated config handed in
  // directly would reach the gate unchecked.
  Cbfc(folly::not_null<SimContext*> simCtx, const CbfcConfig& config);

  /**
   * Map a packet to its VC using the DSCP / PCP-DEI rules of UEC 1.0.3
   * §5.2.5.2.
   *
   * @param pkt  The packet to map.
   * @return     The VC id in `0..numVcs-1` when that VC is lossless, or
   *             nullopt when the packet is non-IP, unmapped, or maps to a
   *             best-effort VC.
   */
  [[nodiscard]] std::optional<uint8_t> losslessVcOf(const Packet& pkt) const;
  /**
   * Pack up to `kCfReportsPerUpdate` dirty VCs into a payload, marking the
   * ones it reports clean, from the `cfRrCursor_` round-robin start index.
   *
   * @return The payload, or nullopt if no VC was dirty.
   */
  [[nodiscard]] std::optional<CbfcCfUpdatePayload> packDirtyReports();

  /**
   * Credits a packet costs on this link (UEC 1.0.3 §5.2.5.6.1).
   *
   * @param bytes  The MAC frame size in bytes, before PktOvhd is added.
   * @return       roundup((bytes + pktOvhdBytes) / creditSizeBytes).
   */
  [[nodiscard]] uint32_t pktCreditsFor(uint32_t bytes) const;
  /**
   * Every `config_.ccMsgTimer`, queue the CC_Update range for §5.2.5.6.4 leak
   * recovery.
   */
  coro::SimScopedTask ccMsgLoop();
  // Every `cfMaxSpacing_`, re-dirty each VC that has ever freed credits.
  coro::SimScopedTask cfMaxLoop();
  [[nodiscard]] bool hasDirtyVc() const noexcept;
  void onWake();
  void onCtlosReady();

  // Owning port's simulation context; supplies `now()` and timer scheduling.
  const folly::not_null<SimContext*> simCtx_;
  // Single source of truth for the credit size; non-const because
  // `setCreditSize` resolves `creditSizeBytes` from the receiver buffer at
  // bootstrap.
  CbfcConfig config_;
  // Packet -> VC classification (§5.2.5.2 mask-match on DSCP / PCP-DEI).
  VcMapper mapper_;
  // Sender-side accounting: S_VC_CC consumed per VC against the partner's
  // pooled credit limit, plus the per-VC reserve, overshoot and VC-group
  // reservations.
  CbfcSenderLedger ledger_;
  // Receiver-side accounting: R_VC_CC counted at store, R_VC_CF at drain, and
  // the §5.2.5.7.4 reconcile against a received CC_Update.
  CbfcCreditTracker tracker_;
  // Counters surfaced through `stats()` onto the port's thrift `CbfcStats`.
  Stats stats_;
  // Data-path wake: credits were returned, so a gated VC may now transmit.
  OnWakeCallback onWake_;
  // CtlOS wake: a CF_Update is queued for the ordered-set path. Kept separate
  // from `onWake_` because the two feed different schedulers -- see
  // `setOnCtlosReadyCallback`.
  OnCtlosReadyCallback onCtlosReady_;

  // CF_Update production state (receiver role). cfDirty_[vc] is set when a
  // drain or CC reconcile changes R_VC_CF[vc]; CF_max periodically re-dirties.
  std::vector<bool> cfDirty_;
  // Sticky per-VC "R_VC_CF[vc] has been incremented at least once". The CF_max
  // lost-update refresh keys off this rather than a value test: R_VC_CF is a
  // 15-bit cyclic counter (Tbl 5-18), so an active VC passes through 0 every
  // 2^15 credits, and a lost CF_Update — the case CF_max exists to recover —
  // leaves R_VC_CF unchanged.
  std::vector<bool> cfEverFreed_;
  // One-shot for the unknown-VC error log. Not per VC: the reported index is
  // out of this port's range, so there is no slot to key it on.
  bool cfUnknownVcLogged_{false};
  // One-shot for the discarded-CC_Update-range error log, for the same reason
  // as `cfUnknownVcLogged_`.
  bool ccRangeUnknownLogged_{false};
  // Credits added to R_VC_CF[vc] since that VC was last packed into a
  // CF_Update, for the optional accumulation gate (`cfCreditThresholdCredits`).
  // Deliberately NOT R_VC_CF itself: that is a 15-bit cyclic counter
  // (Tbl 5-18), so a threshold comparison against it would be meaningless
  // across a wrap. Reset where cfDirty_ clears, so the CF_max path consumes
  // residue rather than letting it count toward the next crossing.
  std::vector<uint64_t> cfAccum_;
  // Round-robin start index for the two CF_Update report slots. Persists
  // across calls: a fixed vc=0 start lets two continuously dirty low VCs hold
  // both slots forever and starve every higher VC's credit return. Rotation is
  // a modelling choice — UEC §5.2.5.7.3 does not specify slot selection —
  // and follows `RrQueueScheduler::scanFrom`.
  uint8_t cfRrCursor_{0};
  // ----- CF_min is a filter, not a trigger -----
  //
  // UEC 1.0.3 §5.2.5.7.3 gives the CF_Update generation condition as:
  //
  //     "The R_VC_CF[x] was updated, and the CF_min_timer has expired."
  //
  // Expiry is not itself a generation condition, so no report is owed when the
  // window closes. A VC dirtied inside it stays dirty and is reported by the
  // next eligibility check: a later R_VC_CF change, a CC_Update reconcile
  // reclaim, or the CF_max refresh.
  //
  // An implementation may instead schedule a wake at expiry and emit there,
  // making expiry a de facto trigger. That is a latency optimization, not a
  // conformance requirement, so it is not modelled here; reinstate it only
  // behind a benchmark.
  //
  // The cost of not latching is that CF_max drains only `kCfReportsPerUpdate`
  // VCs per period, so N withheld VCs take ceil(N/2) CF_max periods rather
  // than one. These timers are byte-denominated, so the deferred transmission
  // is that many CF_max BYTES independent of link rate -- 8 MB for a 16-VC
  // port with CF_max at its 1 MB ceiling, and only on an otherwise idle link.
  // `CfMaxDrainsTwoVcsPerPeriodNotAll` pins this.
  //
  // Simulation time at which `popCfUpdate` last handed out a payload, for the
  // CF_min spacing gate. Default-constructed to 0, so the first report is
  // spaced from time 0 rather than from `start()`.
  //
  // §5.2.5.7.3 does not say which event restarts CF_min_timer. Restarting on
  // transmission is what makes it a bound on report RATE, which is what
  // §5.2.11.2's CtlOS bandwidth cap cares about; restarting on every counter
  // update would let a steadily-draining VC defer its report indefinitely.
  Time lastCfSentTime_;
  // Minimum interval between CF_Updates: `config_.cfMinTimerBytes` of wire
  // time at `config_.linkBps`. Always positive: the public constructor runs
  // `makeCbfcConfig`, which rejects a byte count below `kCfMinTimerMinBytes`
  // (a PackSim floor).
  Time cfMinSpacing_;
  // Interval of the CF_max lost-update refresh: `config_.cfMaxTimerBytes` of
  // wire time at `config_.linkBps`. Always positive: the public constructor
  // runs `makeCbfcConfig`, which rejects a byte count below
  // `kCfMaxTimerMinBytes` (UEC 1.0.3 Tbl 5-27).
  Time cfMaxSpacing_;

  // CC_Update production state (sender role): the ranges owing the receiver a
  // report, one per 16 configured VCs.
  //
  // Re-queueing a queued range is the whole of the "newest wins" rule. A
  // CC_Update carries `S_VC_CC`, a cumulative counter read from the ledger at
  // pop time, and the §5.2.5.7.4 reconcile assigns rather than accumulates, so
  // a later report makes every earlier one redundant.
  CcRangeScheduler ccRanges_;

  // `cfMaxTimer_` must be declared after `cfMaxSpacing_`: a `SimStopwatch`
  // takes its duration at construction, and member init follows declaration
  // order.
  coro::SimStopwatch ccMsgTimer_;
  coro::SimStopwatch cfMaxTimer_;

  // Declared LAST so `~Cbfc` destroys them FIRST, unwinding each suspended
  // frame before the timers it is parked on.
  coro::SimScopedTask ccMsgLoop_;
  coro::SimScopedTask cfMaxLoop_;

  // True between `start()` and `shutdown()`. The loops need no `running_`
  // check of their own: `shutdown()` destroys their frames outright, and its
  // teardown-only precondition keeps that from happening mid-body.
  bool running_{false};
  // True only after `shutdown()`, cleared again by `start()`. Distinct from
  // `!running_`, which is also the state of a module that has never been
  // started -- a valid state in which credits must still accumulate and be
  // reported. Only the shut-down case suppresses CF_Update production.
  bool stopped_{false};
};

} // namespace packsim::cbfc
