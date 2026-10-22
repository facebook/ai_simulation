/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <utility>
#include <vector>

#include <folly/container/F14Set.h>
#include <folly/memory/not_null.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/transport/roce-send-info.h"

namespace packsim {

/**
 * Socket-side operations available to error-recovery strategies.
 *
 * The socket owns packet construction and transport wakeups while each
 * strategy owns its protocol state and timer scheduling.
 */
class ErrorRecoverySocket {
 public:
  virtual ~ErrorRecoverySocket() = default;

  ErrorRecoverySocket(const ErrorRecoverySocket&) = delete;
  ErrorRecoverySocket& operator=(const ErrorRecoverySocket&) = delete;
  ErrorRecoverySocket(ErrorRecoverySocket&&) = delete;
  ErrorRecoverySocket& operator=(ErrorRecoverySocket&&) = delete;

  /**
   * Emit one NACK packet without scheduling a resend.
   *
   * @param seqNum         Sequence number of the message containing the gap.
   * @param psn            Expected PSN to include in the NACK.
   * @param rnrBackoffIdx  RNR back-off-table index, or nullopt for none.
   */
  virtual void emitNackPacket(
      uint32_t seqNum,
      uint32_t psn,
      std::optional<uint8_t> rnrBackoffIdx) = 0;

  /** Notify the transport that the socket is ready to send. */
  virtual void wakeupForSend() = 0;

 protected:
  ErrorRecoverySocket() = default;
};

// Result returned by ErrorRecovery::onAck() instructing the socket how to
// complete sent messages.
struct AckResult {
  // The next sequence number that is NOT yet complete. The socket completes
  // every sent message whose sequenceNumber < this value.
  // std::nullopt (default) triggers per-message completion by seqNum. A value
  // requests cumulative completion below that watermark.
  std::optional<uint32_t> nextIncompleteSeqNum;

  // True when the strategy determines that this ACK cannot complete any sent
  // message. The socket skips completion state changes but still accounts for
  // the received packet and performs post-ACK housekeeping. This takes
  // precedence over nextIncompleteSeqNum; a stale result's watermark is
  // ignored.
  bool isStale{false};
};

// Result of a PSN check on the receive path. DROP is first so a
// value-initialized action has the conservative behavior.
enum class PsnAction {
  // PSN is a duplicate or unexpected; drop silently.
  DROP,
  // PSN is an already-received packet; ACK it again if it requested an ACK.
  ACK_DUPLICATE,
  // PSN is acceptable; process the packet normally.
  ACCEPT,
  // Process the packet and request recovery for the next known gap.
  ACCEPT_AND_REQUEST_NACK,
  // Process an out-of-order packet while the strategy tracks selective-repeat
  // state for byte-count completion.
  BUFFER,
  // Process an out-of-order packet but suppress its ACK while a gap remains.
  BUFFER_WITHOUT_ACK,
  // Process an out-of-order packet, suppress its ACK, and request gap recovery.
  BUFFER_AND_REQUEST_NACK,
  // PSN indicates a gap; caller should send a NACK.
  SEND_NACK,
};

/**
 * Abstract base for RoCE error-recovery strategies.
 *
 * Subclasses (e.g. GoBackNRecovery) implement a specific protocol. The owning
 * socket handles I/O primitives (packet emission, scheduling wakeups); the
 * strategy owns its own protocol state (retransmit queue, NACK/RNR timers) and
 * reaches back into the socket via a small set of public helpers
 * (`emitNackPacket`, `wakeupForSend`).
 */
class ErrorRecovery {
 public:
  virtual ~ErrorRecovery() = default;

  ErrorRecovery(const ErrorRecovery&) = delete;
  ErrorRecovery& operator=(const ErrorRecovery&) = delete;
  ErrorRecovery(ErrorRecovery&&) = delete;
  ErrorRecovery& operator=(ErrorRecovery&&) = delete;

  // A batch of packets to retransmit, returned by getRetransmitBatch().
  struct RetransmitBatch {
    std::vector<RoceSendInfo> packets;
    uint64_t totalBytes{0};
  };

  struct RetransmitStats {
    // Total packets returned for retransmission.
    uint64_t retxPkts{0};
    // Total payload bytes returned for retransmission.
    uint64_t retxBytes{0};
    // Packets retransmitted after a timeout.
    uint64_t retxByTimer{0};
    // Packets retransmitted after a NACK.
    uint64_t retxByNack{0};
  };

  /**
   * Whether the socket should skip sending right now (e.g. RNR back-off).
   *
   * @return  True if a back-off is in effect and the socket should defer
   *          sends; false otherwise. Default is false — protocols without
   *          back-off do not override.
   */
  virtual bool isSendBlocked() const {
    return false;
  }

  /**
   * Check if a received PSN is acceptable.
   *
   * @param expectedPsn  The next PSN the receiver expects (may be updated).
   * @param receivedPsn  The PSN of the received packet.
   * @param oooSupport   Whether out-of-order packet support is enabled.
   * @return PsnAction indicating how the caller should proceed.
   */
  virtual PsnAction
  checkPsn(uint32_t& expectedPsn, uint32_t receivedPsn, bool oooSupport) = 0;

  // Override the PSN written into outgoing ACK packets. Default: no change.
  // Strategies that use cumulative ACKs override this to return
  // `expectedPsn - 1` regardless of which packet triggered the ACK.
  virtual uint32_t adjustAckPsn(
      uint32_t requestedAckPsn,
      [[maybe_unused]] uint32_t expectedPsn) const noexcept {
    return requestedAckPsn;
  }

  // True if the strategy needs byte-count message completion (vs. relying
  // on `isLastPkt` arriving in order). Strategies that buffer out-of-order
  // packets at the receiver (e.g. selective repeat) return true; the socket
  // then sets `outOfOrderPktSupport_=true`.
  virtual bool useByteCountCompletion() const noexcept {
    return false;
  }

  /**
   * Record a sent packet for potential retransmission.
   *
   * @param info  The send info of the packet that was scheduled.
   */
  virtual void onPacketScheduled(const RoceSendInfo& info) = 0;

  /**
   * Process an ACK. Returns an AckResult describing which sent messages
   * can now be completed (and whether the ACK should be ignored). The
   * strategy is responsible for its own state updates
   * (e.g. popping its retransmit queue); the socket consults the result
   * only for sentMsgs_ completion.
   *
   * @param psn   The packet sequence number from the ACK.
   *
   * @return      AckResult instructing the socket how to complete messages.
   */
  virtual AckResult onAck(uint32_t psn) = 0;

  /**
   * Process a NACK. The strategy is responsible for scheduling the wakeup
   * immediately or after the requested RNR back-off.
   *
   * @param expectedPsn    The PSN the receiver expected (from the NACK).
   * @param rnrBackoffIdx  RNR back-off-table index, or nullopt for none.
   */
  virtual void onNack(
      uint32_t expectedPsn,
      std::optional<uint8_t> rnrBackoffIdx) = 0;

  /**
   * Handle a PSN gap detected by the receiver. The strategy emits the
   * NACK packet (via `socket->emitNackPacket`) and owns any periodic-resend
   * scheduling. The default implementation does nothing.
   *
   * @param seqNum  The sequence number of the gap-triggering packet.
   * @param psn     The expected PSN to include in the NACK.
   */
  virtual void onMissingMessage(uint32_t, uint32_t) {}

  // Hold an ACK for an out-of-order packet until all preceding PSNs arrive.
  virtual void deferAck(uint32_t, uint32_t) {}

  // Return and remove deferred ACKs now covered by expectedPsn.
  virtual std::vector<std::pair<uint32_t, uint32_t>> takeReadyDeferredAcks(
      uint32_t) {
    return {};
  }

  /**
   * The socket needs to send an RNR (no buffer / no work). Strategy
   * determines the back-off delay and emits the NACK. May be a no-op
   * if a NACK is already scheduled. The default implementation does nothing.
   *
   * @param psn  The PSN to include in the RNR NACK.
   */
  virtual void onSendRnrRequested(uint32_t) {}

  /**
   * Get a batch of packets to retransmit, up to quota bytes.
   *
   * @param quota  Maximum bytes to retransmit.
   * @return Batch of packets to retransmit and total bytes.
   */
  virtual RetransmitBatch getRetransmitBatch(uint64_t quota) = 0;

  // Get cumulative retransmission counters for this recovery strategy.
  virtual RetransmitStats getRetransmitStats() const noexcept {
    return {};
  }

  // Check if there are packets pending retransmission.
  virtual bool hasRetransmitPending() const noexcept = 0;

  // True after the strategy transitions the QP to ERR. The socket stops
  // packet emission and ignores recovery responses, but the transition owner
  // remains responsible for reporting failed work because RoceSocketBase has
  // no error-completion callback. Default: never fails.
  virtual bool isFailed() const noexcept {
    return false;
  }

 protected:
  ErrorRecovery() = default;
};

/**
 * Go-Back-N error recovery component for RoCE sockets.
 *
 * Manages retransmit queue state, PSN gap detection, and retransmission,
 * NACK, and RNR timer scheduling. The owning socket exposes `emitNackPacket`
 * (packet creation) and `wakeupForSend` (signalling) primitives that this
 * component calls back into.
 *
 * Example usage:
 *
 *     auto recovery = GoBackNRecovery(rnrTimeIdx, socket, simCtx);
 *     recovery.onPacketScheduled(info);       // after scheduling a send
 *     auto result = recovery.checkPsn(...);   // on packet receive
 *     auto ack = recovery.onAck(psn);          // on ACK receive
 */
class GoBackNRecovery final : public ErrorRecovery {
 public:
  struct Options {
    // Whether OOO gaps use delayed single-packet repair.
    bool selectiveGapRepair{false};

    // Whether the receiver accepts packets only in PSN order (the NIC's
    // outofOrderPktSupport is off). Taken from the local NIC; the peer is
    // assumed to use the same setting, and FlexTopology warns at startup
    // about NIC configs that break that (describeGoBackNAckMismatch()).
    bool inOrderDelivery{false};

    // Tail/ACK-loss timeout. Armed once an ACK-requesting tail is sent, when
    // inOrderDelivery or selectiveGapRepair is set; otherwise no timer runs.
    Time retransmitTimeout{Time::fromMs(1)};

    // Consecutive timeouts allowed without ACK progress before QP failure. A
    // NACK that acknowledges new PSNs, or an RNR NACK, also restarts the count.
    uint32_t maxRetransmitTimeoutAttempts{1024};
  };

  /**
   * @param rnrTimeIdx  Index into the RNR back-off table; must be below its
   *                    size.
   * @param socket      Owning socket; used to emit NACK packets and signal
   *                    wakeups. Must outlive this component.
   * @param simCtx      Simulation context for scheduling NACK/RNR timers.
   *                    Must outlive this component.
   *
   * @throw std::invalid_argument if rnrTimeIdx is not below the table size.
   */
  GoBackNRecovery(
      uint8_t rnrTimeIdx,
      ErrorRecoverySocket& socket,
      SimContext& simCtx);

  /**
   * Construct Go-Back-N recovery with an explicit recovery policy.
   *
   * @param rnrTimeIdx  Index into the RNR back-off table; must be below its
   *                    size.
   * @param socket      Owning socket; must outlive this component.
   * @param simCtx      Simulation context; must outlive this component.
   * @param options     Delivery-order, selective-repair and retransmit-timeout
   *                    policy.
   *
   * @throw std::invalid_argument if rnrTimeIdx is not below the table size, or
   *        if the options give a zero retransmit timeout or attempt limit.
   */
  GoBackNRecovery(
      uint8_t rnrTimeIdx,
      ErrorRecoverySocket& socket,
      SimContext& simCtx,
      const Options& options);
  ~GoBackNRecovery() override;

  GoBackNRecovery(const GoBackNRecovery&) = delete;
  GoBackNRecovery& operator=(const GoBackNRecovery&) = delete;
  GoBackNRecovery(GoBackNRecovery&&) = delete;
  GoBackNRecovery& operator=(GoBackNRecovery&&) = delete;

  // True while an RNR back-off timer is scheduled (socket should skip).
  bool isSendBlocked() const override;

  /**
   * Check if a received PSN is acceptable.
   *
   * @param expectedPsn  The next PSN the receiver expects.
   * @param receivedPsn  The PSN of the received packet.
   * @param oooSupport   Whether out-of-order packet support is enabled.
   *
   * @return PsnAction indicating how the caller should proceed.
   */
  PsnAction checkPsn(
      uint32_t& expectedPsn,
      uint32_t receivedPsn,
      bool oooSupport) override;

  /**
   * Record a sent packet for potential retransmission.
   * Called after scheduling a send. Retransmits are not recorded.
   *
   * @param info  The send info of the packet that was scheduled.
   */
  void onPacketScheduled(const RoceSendInfo& info) override;

  /**
   * Process an ACK. Frees acknowledged packets from the retransmit queue
   * and returns an AckResult instructing the socket how to complete the
   * matching sent message.
   *
   * For already-observed PSNs (`psn <= lastReceivedPsn_`), skips retransmit
   * queue processing but still requests exact-match completion. A higher-PSN
   * ACK for a later message can advance recovery state before a reordered
   * lower-PSN ACK completes an earlier message.
   * For new PSNs, pops the retransmit queue. When ACKs are cumulative (see
   * acksAreCumulative()), also returns a completion watermark covering every
   * message tail the ACK reaches; otherwise the socket completes the ACKed
   * message by sequence number.
   */
  AckResult onAck(uint32_t psn) override;

  /**
   * Process a NACK. Resets the retransmit pointer so pending packets
   * will be replayed from the gap, then either schedules an RNR back-off
   * timer when `rnrBackoffIdx` is set or wakes the socket immediately.
   */
  void onNack(uint32_t expectedPsn, std::optional<uint8_t> rnrBackoffIdx)
      override;

  // Request recovery for a PSN gap. Selective gap repair delays the first
  // NACK so transient reordering can resolve before recovery starts.
  void onMissingMessage(uint32_t seqNum, uint32_t psn) override;

  void deferAck(uint32_t seqNum, uint32_t psn) override;
  std::vector<std::pair<uint32_t, uint32_t>> takeReadyDeferredAcks(
      uint32_t expectedPsn) override;

  // Emit an RNR NACK (only if no NACK is already scheduled), using the
  // configured RNR back-off delay.
  void onSendRnrRequested(uint32_t psn) override;

  /**
   * Get a batch of packets to retransmit, up to quota bytes.
   * Each returned SendInfo has isRetransmit set to true and
   * callbackOnSend set on the last packet.
   *
   * @param quota  Maximum bytes to retransmit.
   *
   * @return Batch of packets to retransmit and total bytes.
   */
  RetransmitBatch getRetransmitBatch(uint64_t quota) override;

  RetransmitStats getRetransmitStats() const noexcept override {
    return retransmitStats_;
  }

  // Check if there are packets pending retransmission.
  bool hasRetransmitPending() const noexcept override;

  bool isFailed() const noexcept override {
    return failed_;
  }

  // Get the last cumulatively acknowledged PSN for diagnostics.
  uint32_t lastReceivedPsn() const noexcept {
    return lastReceivedPsn_;
  }

  // Convert RNR field value to simulation time.
  static Time rnrFieldToTime(uint8_t rnrField);

 private:
  enum class RetransmitTrigger { NACK, TIMER };

  // True when an ACK implies every earlier PSN arrived: the receiver accepts
  // only in-order PSNs, or selective gap repair holds ACKs until gaps fill.
  // Cumulative completion and the tail/ACK-loss timer both rely on it.
  bool acksAreCumulative() const noexcept {
    return inOrderDelivery_ || selectiveGapRepair_;
  }

  void resetRetransmitTimeout();
  void onRetransmitTimeout();
  void failRecovery();

  // Internal: emit one NACK and (re)schedule the periodic resend.
  void scheduleNackResend(
      uint32_t seqNum,
      uint32_t psn,
      std::optional<uint8_t> rnrBackoffIdx);

  folly::not_null<ErrorRecoverySocket*> socket_;
  folly::not_null<SimContext*> simCtx_;
  std::deque<RoceSendInfo> reTxQ_;
  folly::F14FastSet<uint32_t> bufferedPsns_;
  std::vector<std::pair<uint32_t, uint32_t>> deferredAcks_;
  size_t reTxPtr_{0};
  uint32_t retransmitPacketLimit_{UINT32_MAX};
  Time retransmitTimeout_;
  uint32_t maxRetransmitTimeoutAttempts_;
  uint32_t retransmitTimeoutAttempts_{0};
  uint32_t lastReceivedPsn_{0};
  RetransmitStats retransmitStats_;
  std::optional<RetransmitTrigger> retransmitTrigger_;
  bool selectiveGapRepair_{false};
  bool inOrderDelivery_{false};
  bool failed_{false};
  Scheduler::EventHandle retransmitEvent_;
  Scheduler::EventHandle nackEvent_;
  Scheduler::EventHandle rnrEvent_;
  uint8_t rnrTimeIdx_;
};

} // namespace packsim
