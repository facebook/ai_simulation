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
#include <iterator>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <boost/circular_buffer.hpp>
#include <folly/CPortability.h>
#include <folly/Utility.h>
#include <folly/logging/LogStreamProcessor.h>
#include <folly/logging/xlog.h>
#include <folly/portability/GTestProd.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/switch_config_types.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/packet/base.h"
#include "packsim/packet/pfc.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/queues/queue-filter.h"
#include "packsim/queues/queue-packet-handler.h"
#include "packsim/queues/queue-scheduler.h"

namespace packsim {

// Smallest buffer a packet queue keeps allocated, in packets. The space
// optimized ring buffer shrinks to this minimum when it drains and frees its
// storage when the minimum is 0, so a non-zero minimum is what lets a queue go
// empty <-> non-empty without an allocation. Allocated when the queue is built,
// so every queue, used or not, holds 2 x 16-byte `PacketPtr` = 32 bytes.
constexpr uint32_t kMinPacketQueueSlots = 2;

// The packet ring every queue here uses, built with capacity
// `PacketRing::capacity_type(UINT32_MAX, kMinPacketQueueSlots)`: unbounded, and
// never shrinking below kMinPacketQueueSlots.
using PacketRing = boost::circular_buffer_space_optimized<PacketPtr>;

/**
 * A simple queue that only supports enqueue, dequeue, and peek operations
 * and only keeps track of how many bytes and packets are in the queue.
 */
struct SimplePacketQueue {
  PacketRing queue{PacketRing::capacity_type(UINT32_MAX, kMinPacketQueueSlots)};
  uint32_t queuedPacketBytes{0};

  FOLLY_ALWAYS_INLINE void enqueuePacket(const PacketPtr& pkt) {
    queue.push_back(pkt);
    queuedPacketBytes += pkt->getSize();
  }

  FOLLY_ALWAYS_INLINE void dequeuePacket(PacketPtr* pkt) {
    queuedPacketBytes -= queue.front()->getSize();
    *pkt = std::move(queue.front());
    queue.pop_front();
  }

  FOLLY_ALWAYS_INLINE void peekPacket(PacketPtr* pkt) const {
    *pkt = queue.front();
  }

  uint32_t getCurrentBytes() const noexcept {
    return queuedPacketBytes;
  }

  size_t getCurrentPackets() const noexcept {
    return queue.size();
  }
};

/**
 * Metadata for a Class of Service (CoS) queue.
 */
struct CosQMetaData {
  PacketRing txQueue{
      PacketRing::capacity_type(UINT32_MAX, kMinPacketQueueSlots)};
  uint32_t queuedTxBytes{0};
  uint64_t rxPauseStartTime{0};
  uint64_t txPauseStartTime{0};

  // Track total "bubble" duration in this queue; bubble is characterized as any
  // time after the first data is seen that the queue is empty, not including
  // the time from final dequeue to end of simulation
  uint64_t totalBubbleTimeNs{0};
  uint64_t thisBubbleStartTimeNs{UINT64_MAX};

  PfcState rxPfcState{PfcState::XON};
  PfcState txPfcState{PfcState::XON};

  bool txQueueAvailable{true};

  FOLLY_ALWAYS_INLINE void enqueueTxPacket(PacketPtr pkt, const Time now) {
    // If the queue is empty on enqueue, we might be at the end of a bubble. The
    // only time we won't be is the first time we enqueue data. We log bubbles
    // on the empty -> non-empty transition to make sure we don't count the
    // final empty period before end of simulation.
    if (queuedTxBytes == 0) {
      auto ns = now.getNs();
      if (ns > thisBubbleStartTimeNs) {
        // This will be true every time we go from empty -> not empty, except
        // the first time because thisBubbleStartTimeNs is initialized to
        // UINT64_MAX
        totalBubbleTimeNs += ns - thisBubbleStartTimeNs;
      }
    }
    const uint32_t pktBytes = pkt->getSize();
    txQueue.push_back(std::move(pkt));
    queuedTxBytes += pktBytes;
  }

  FOLLY_ALWAYS_INLINE void dequeueTxPacket(PacketPtr& pkt, const Time now) {
    pkt = std::move(txQueue.front());
    queuedTxBytes -= pkt->getSize();
    txQueue.pop_front();

    // If the queue is now empty, we're at the start of a bubble
    if (queuedTxBytes == 0) {
      thisBubbleStartTimeNs = now.getNs();
    }
  }

  FOLLY_ALWAYS_INLINE void peekTxPacket(PacketPtr* pkt) const {
    *pkt = txQueue.front();
  }
};

/**
 * Manages multiple Class of Service (CoS) queues with filtering, scheduling,
 * and packet handling capabilities.
 */
class Queues : private folly::NonCopyableNonMovable {
 public:
  // Returns the number of queues
  uint8_t getNumQueues() const noexcept {
    return numQueues_;
  }

  /**
   * @param cos Class of Service identifier
   * @return Queue length in packets for the specified CoS
   */
  FOLLY_ALWAYS_INLINE size_t getQueueLengthInPkts(uint8_t cos) const;

  /**
   * @param cos Class of Service identifier
   * @return Queue length in bytes for the specified CoS
   */
  FOLLY_ALWAYS_INLINE uint32_t getQueueLengthInBytes(uint8_t cos) const;

  /**
   * @return Total queue length in bytes across all CoS queues
   */
  FOLLY_ALWAYS_INLINE uint32_t getTotalQueueLengthInBytes() const noexcept;

  /**
   * @param cos Class of Service identifier
   * @return True if the queue is empty
   */
  FOLLY_ALWAYS_INLINE bool isQueueEmpty(uint8_t cos) const;

  /**
   * @param cos    Class of Service identifier
   * @param pkt    Output parameter for the peeked packet
   */
  FOLLY_ALWAYS_INLINE void peekPacketInQueue(uint8_t cos, PacketPtr* pkt) const;

  /**
   * Selects the next eligible queue and advances the scheduler cursor,
   * committing to that queue's head packet. Take the packet with
   * `popPacketFromQueue()`: `popPacket()` would select again. To inspect
   * without committing, use `peekQueue()`.
   *
   * @return The next eligible queue to pop from, or std::nullopt when no queue
   *         is currently eligible.
   */
  FOLLY_ALWAYS_INLINE std::optional<uint8_t> nextQueue();

  /**
   * The queue `nextQueue()` would choose, without advancing the scheduler
   * cursor.
   *
   * @return The next eligible queue to pop from, or std::nullopt when no queue
   *         is currently eligible.
   */
  FOLLY_ALWAYS_INLINE std::optional<uint8_t> peekQueue() const;

  /**
   * Predicate a scheduler consults before considering a queue. Installed by
   * the owning `EthernetPort` when CBFC (Credit-Based Flow Control) is
   * enabled; empty otherwise.
   */
  using TxGate = std::function<bool(const PacketPtr&)>;

  /**
   * Installs a transmit gate, replacing any gate already installed. A queue is
   * then eligible only if its head packet passes the gate.
   *
   * The predicate must be a pure test. `isQueueEligible()` runs it on every
   * scheduling decision, including inspections through `peekQueue()`, so a
   * gate that consumed credit would change the decision the following
   * `popPacket()` makes. Whatever the predicate captures must stay valid until
   * this Queues is destroyed or `clearTxGate()` is called.
   *
   * @param gate  Predicate run on a queue's head packet; returning false makes
   *              that queue ineligible so the scheduler can serve another,
   *              instead of stalling the port behind it.
   */
  void setTxGate(TxGate gate) noexcept {
    txGate_ = std::move(gate);
  }
  // Removes any transmit gate; a queue is then eligible whenever it is
  // occupied and XON.
  void clearTxGate() noexcept {
    txGate_ = nullptr;
  }

  /**
   * Whether `cos` may be scheduled: occupied, not PFC-paused, and — when a gate
   * is installed — its head packet passes the gate.
   */
  [[nodiscard]] FOLLY_ALWAYS_INLINE bool isQueueEligible(uint8_t cos) const;

  /**
   * Whether some queue is occupied and XON but held back only by the gate.
   * Telemetry uses this to tell a queue blocked on credit apart from an empty
   * queue and from a PFC pause.
   *
   * Linear in the number of queues and runs the gate on each occupied, XON
   * one, so keep it off the per-packet path.
   */
  [[nodiscard]] bool hasTxGateBlockedQueue() const;

  /**
   * Push a packet into the appropriate queue.
   *
   * @param pkt Packet to push
   * @return Pair of (queue number, enqueue success flag)
   * @throw std::invalid_argument if packets are colored and the packet's color
   *        has no coloredWredEcnCfgs entry on its queue.
   */
  FOLLY_ALWAYS_INLINE std::pair<uint8_t, bool> pushPacket(const PacketPtr& pkt);

  /**
   * Same as `pushPacket`, except that an enqueued packet is moved into its
   * queue instead of copied, which saves the atomic reference-count update a
   * copy makes. A dropped packet is left in `pkt`.
   *
   * @param pkt Packet to push; null afterwards if it was enqueued
   * @return Pair of (queue number, enqueue success flag)
   * @throw std::invalid_argument if packets are colored and the packet's color
   *        has no coloredWredEcnCfgs entry on its queue.
   */
  FOLLY_ALWAYS_INLINE std::pair<uint8_t, bool> pushPacketByMove(PacketPtr& pkt);

  /**
   * Pop a packet from the next scheduled queue.
   *
   * @param pkt       Output parameter for the popped packet
   * @param ecnMarked Optional output parameter for ECN marking status
   *
   * @return          The queue the packet came from, or `nullopt` when no
   *                  queue is eligible -- either nothing is enqueued, or the
   *                  scheduler skipped every occupied queue (PFC-paused or
   *                  credit-starved). Matches `peekQueue()`.
   */
  FOLLY_ALWAYS_INLINE std::optional<uint8_t> popPacket(
      PacketPtr& pkt,
      bool* ecnMarked = nullptr);

  /**
   * Pop the head packet of queue `cos` without consulting the scheduler, for a
   * caller that already chose `cos` with `nextQueue()`. The packet gets the
   * same post-scheduling handling as in `popPacket()`, but PFC state and the
   * transmit gate are not checked again.
   *
   * @param cos       Queue to pop from
   * @param outPkt    Output parameter for the popped packet; nullptr when
   *                  `cos` is empty
   * @param ecnMarked Optional output parameter for ECN marking status
   *
   * @return          False when `cos` holds no packet
   * @throw std::out_of_range if `cos` is not one of this instance's queues
   */
  FOLLY_ALWAYS_INLINE bool
  popPacketFromQueue(uint8_t cos, PacketPtr& outPkt, bool* ecnMarked = nullptr);

  /**
   * VOQ-based ECN (Explicit Congestion Notification) marking of a packet just
   * popped from queue `qId`: when that queue's profile is VOQ_ECN, the mark
   * curve reads the bytes still queued on `qId`. On a switch port that queue
   * is the virtual output queue (VOQ) for {egress port, queue}, which every
   * ingress port feeds. Does nothing for any other profile, or when packets
   * are colored.
   *
   * @param pkt     The packet `popPacket()` just returned from `qId`.
   * @param qId     The queue `pkt` came from.
   * @param marked  Optional output parameter, set to whether `pkt` was marked.
   *                Left as it was when this does nothing.
   */
  FOLLY_ALWAYS_INLINE void markVoqEcnPacket(
      const PacketPtr& pkt,
      uint8_t qId,
      bool* marked = nullptr) const;

  /**
   * Apply filtering to determine which queue a packet belongs to.
   *
   * @param pkt Packet to filter
   * @return Queue number for the packet
   */
  FOLLY_ALWAYS_INLINE uint8_t filter(const PacketPtr& pkt) const;

  /**
   * @param cos Class of Service identifier
   * @return Transmit pause start time in nanoseconds
   */
  FOLLY_ALWAYS_INLINE uint64_t getTxPauseStartTime(uint8_t cos) const;

  // Set TX pause start time to current simulation time
  FOLLY_ALWAYS_INLINE void setTxPauseStartTime(uint8_t cos);

  /**
   * @param cos Class of Service identifier
   * @return Receive pause start time in nanoseconds
   */
  FOLLY_ALWAYS_INLINE uint64_t getRxPauseStartTime(uint8_t cos) const;

  // Set RX pause start time to current simulation time
  FOLLY_ALWAYS_INLINE void setRxPauseStartTime(uint8_t cos);

  /**
   * Update transmit PFC state for a queue.
   *
   * @param cos   Class of Service identifier
   * @param state New PFC state
   */
  FOLLY_ALWAYS_INLINE void updateTxPfcState(uint8_t cos, PfcState state);

  /**
   * Update receive PFC state for a queue.
   *
   * @param cos   Class of Service identifier
   * @param state New PFC state
   */
  FOLLY_ALWAYS_INLINE void updateRxPfcState(uint8_t cos, PfcState state);

  /**
   * @param cos Class of Service identifier
   * @return Current transmit PFC state
   */
  FOLLY_ALWAYS_INLINE PfcState getTxPfcState(uint8_t cos) const;

  /**
   * @param cos Class of Service identifier
   * @return Current receive PFC state
   */
  FOLLY_ALWAYS_INLINE PfcState getRxPfcState(uint8_t cos) const;

  /**
   * @param pkt Packet to get handler for
   * @return Packet handler for the packet
   * @throw std::invalid_argument if packets are colored and the packet's color
   *        has no coloredWredEcnCfgs entry on its queue.
   */
  QueuePacketHandler* getHandler(const PacketPtr& pkt) const;

  /**
   * @param pkt Packet to get handler for
   * @param cos Class of Service identifier
   * @return Packet handler for the packet and CoS
   * @throw std::invalid_argument if packets are colored and the packet's color
   *        has no coloredWredEcnCfgs entry for `cos`.
   */
  QueuePacketHandler* getHandler(const PacketPtr& pkt, uint8_t cos) const;

  using OnTxQueueSpaceAvailableCallback = std::function<void(uint8_t)>;

  // Set the callback for pending queue data send notifications
  FOLLY_ALWAYS_INLINE void setOnTxQueueSpaceAvailableCallback(
      const OnTxQueueSpaceAvailableCallback& cb) {
    onTxQueueSpaceAvailable_ = cb;
  }

  /**
   * Trigger callback when queue has space available.
   *
   * @param cos Class of Service identifier
   */
  FOLLY_ALWAYS_INLINE void sendPendingTxQueueBytes(uint8_t cos);

  /**
   * @param cos Class of Service identifier
   * @return True if the TX queue has space available
   */
  FOLLY_ALWAYS_INLINE bool isTxQueueAvailable(uint8_t cos);

  /**
   * Construct a basic Queues instance.
   *
   * @param simCtx Simulation context
   */
  /* implicit */ Queues(SimContext* simCtx) : simCtx_(simCtx) {}

  /**
   * Construct a fully configured Queues instance with one queue per entry of
   * `handlerType`.
   *
   * @param simCtx        Simulation context
   * @param filterCfg     Filter configuration
   * @param schedulerType Type of scheduling algorithm
   * @param handlerType   Per-queue packet handler configurations
   * @throw std::runtime_error if `handlerType` has no entries or more than 8,
   *        if `filterCfg.field` is not DSCP, if `schedulerType` is not RR,
   *        WRR or SP, or if a queue's profile type is unknown.
   * @throw std::invalid_argument if `filterCfg.fieldToQueue` or
   *        `filterCfg.fieldToColor` has a DSCP key of 64 or more, or if a
   *        queue's WredEcnConfig has an `exp` or `averagingPeriod` that is
   *        negative or not finite, an `averagingPeriod` of 2^64 or more, or a
   *        `dropRateWRED` or `markRateECN` above 100.
   * @throw std::out_of_range if `filterCfg.fieldToColor` is non-empty and a
   *        queue's `coloredWredEcnCfgs` has a key other than GREEN, YELLOW or
   *        RED.
   */
  Queues(
      SimContext* simCtx,
      packsim::thrift::QueueFilterConfig filterCfg,
      packsim::thrift::SchedulingAlgorithmType schedulerType,
      std::vector<packsim::thrift::PortQueueConfig> handlerType);
  virtual ~Queues() = default;

 private:
  FOLLY_GTEST_FRIEND_TEST(SwitchQueuesTest, TestWRRScheduler);
  FOLLY_GTEST_FRIEND_TEST(SwitchQueuesTest, TestWRRSchedulerWithPause);
  FOLLY_GTEST_FRIEND_TEST(SwitchQueuesTest, TestWRRSchedulerSpecialCase);
  FOLLY_GTEST_FRIEND_TEST(SwitchQueuesTest, TestWRRSchedulerSingleWeight);

  /**
   * The body of `pushPacket` and `pushPacketByMove`: filters and admits `pkt`,
   * and places an admitted packet in its queue, copied from an lvalue or moved
   * from an rvalue. A dropped packet is neither copied nor moved.
   *
   * @param pkt Packet to push
   * @return Pair of (queue number, enqueue success flag)
   */
  template <typename TPacket>
  FOLLY_ALWAYS_INLINE std::pair<uint8_t, bool> pushPacketIfAdmitted(
      TPacket&& pkt);

  SimContext* const simCtx_;

  // cosQueues_[i] is the queue for cos == qId == i
  std::vector<CosQMetaData> cosQueues_;
  std::unique_ptr<QueueFilter> filter_;
  std::unique_ptr<QueueScheduler> scheduler_;
  // Empty means every occupied, XON queue is eligible; see `isQueueEligible()`.
  TxGate txGate_;
  std::vector<std::unique_ptr<QueuePacketHandler>> handlers_;
  std::vector<std::vector<std::unique_ptr<QueuePacketHandler>>>
      coloredHandlers_;

  uint8_t numQueues_{0};
  size_t queuedPkts_{0};
  bool useColor_{false};

  // Raise the tx-queue-space-available event.
  void onTxQueueSpaceAvailable(uint8_t cos) {
    XCHECK(onTxQueueSpaceAvailable_)
        << "no tx-queue-space-available subscriber registered on queues";
    onTxQueueSpaceAvailable_(cos);
  }

  OnTxQueueSpaceAvailableCallback onTxQueueSpaceAvailable_;
  uint32_t minQSizeThreshold_{0};
  uint32_t maxQSizeThreshold_{0};
};

/*** Inline Definitions ***/

template <typename TPacket>
std::pair<uint8_t, bool> Queues::pushPacketIfAdmitted(TPacket&& pkt) {
  if (useColor_) {
    filter_->colorize(pkt);
  }
  const auto cos = filter(pkt);
  auto handler = getHandler(pkt, cos);
  const bool enqueue = handler->handlePacketAfterFiltering(pkt);
  if (enqueue) {
    ++queuedPkts_;
    auto& q = cosQueues_.at(cos);
    q.enqueueTxPacket(std::forward<TPacket>(pkt), simCtx_->now());
    handler->setQLen(q.queuedTxBytes);
  }
  return {cos, enqueue};
}

std::pair<uint8_t, bool> Queues::pushPacket(const PacketPtr& pkt) {
  return pushPacketIfAdmitted(pkt);
}

std::pair<uint8_t, bool> Queues::pushPacketByMove(PacketPtr& pkt) {
  return pushPacketIfAdmitted(std::move(pkt));
}

bool Queues::popPacketFromQueue(
    const uint8_t cos,
    PacketPtr& outPkt,
    bool* ecnMarked) {
  auto& q = cosQueues_.at(cos);
  if (q.txQueue.empty()) {
    outPkt = nullptr;
    return false;
  }
  q.dequeueTxPacket(outPkt, simCtx_->now());
  auto handler = getHandler(outPkt, cos);
  const bool m = handler->handlePacketAfterScheduling(outPkt);
  if (ecnMarked) {
    *ecnMarked = m;
  }
  handler->setQLen(q.queuedTxBytes);
  return true;
}

std::optional<uint8_t> Queues::popPacket(PacketPtr& outPkt, bool* marked) {
  if (queuedPkts_ == 0) {
    return std::nullopt;
  }

  const auto selected = scheduler_->selectQueueToPop();
  if (!selected.has_value()) {
    outPkt = nullptr;
    return std::nullopt;
  }
  const uint8_t cos = *selected;
  if (!popPacketFromQueue(cos, outPkt, marked)) {
    return std::nullopt;
  }
  return cos;
}

void Queues::updateTxPfcState(uint8_t cos, PfcState state) {
  cosQueues_.at(cos).txPfcState = state;
}

void Queues::updateRxPfcState(uint8_t cos, PfcState state) {
  cosQueues_.at(cos).rxPfcState = state;
}

PfcState Queues::getTxPfcState(uint8_t cos) const {
  return cosQueues_.at(cos).txPfcState;
}

PfcState Queues::getRxPfcState(uint8_t cos) const {
  return cosQueues_.at(cos).rxPfcState;
}

uint64_t Queues::getTxPauseStartTime(uint8_t cos) const {
  return cosQueues_.at(cos).txPauseStartTime;
}

void Queues::setTxPauseStartTime(uint8_t cos) {
  cosQueues_.at(cos).txPauseStartTime = simCtx_->nowNs();
}

uint64_t Queues::getRxPauseStartTime(uint8_t cos) const {
  return cosQueues_.at(cos).rxPauseStartTime;
}

void Queues::setRxPauseStartTime(uint8_t cos) {
  cosQueues_.at(cos).rxPauseStartTime = simCtx_->nowNs();
}

size_t Queues::getQueueLengthInPkts(uint8_t cos) const {
  return cosQueues_.at(cos).txQueue.size();
}

uint32_t Queues::getQueueLengthInBytes(uint8_t cos) const {
  return cosQueues_.at(cos).queuedTxBytes;
}

uint32_t Queues::getTotalQueueLengthInBytes() const noexcept {
  uint32_t bytesTotal{0};
  for (const auto& q : cosQueues_) {
    bytesTotal += q.queuedTxBytes;
  }
  return bytesTotal;
}

bool Queues::isQueueEmpty(uint8_t cos) const {
  return (getQueueLengthInBytes(cos) == 0);
}

void Queues::sendPendingTxQueueBytes(uint8_t cos) {
  const bool txQueueAvailable = cosQueues_.at(cos).txQueueAvailable;
  if (txQueueAvailable && getQueueLengthInBytes(cos) > maxQSizeThreshold_) {
    cosQueues_.at(cos).txQueueAvailable = false;
    XLOGF(
        DBG9,
        " Cos: {}, Queue is full: {} {}",
        cos,
        getQueueLengthInBytes(cos),
        cosQueues_.at(cos).txQueueAvailable);
  } else if (
      !txQueueAvailable && getQueueLengthInBytes(cos) <= minQSizeThreshold_) {
    cosQueues_.at(cos).txQueueAvailable = true;
    XLOGF(
        DBG9,
        " Cos: {}, Queue has space: {} {}",
        cos,
        getQueueLengthInBytes(cos),
        cosQueues_.at(cos).txQueueAvailable);
    onTxQueueSpaceAvailable(cos);
  }
}

bool Queues::isTxQueueAvailable(uint8_t cos) {
  return cosQueues_.at(cos).txQueueAvailable;
}

void Queues::peekPacketInQueue(uint8_t cos, PacketPtr* packet) const {
  const auto& q = cosQueues_.at(cos);
  if (!q.txQueue.empty()) {
    cosQueues_.at(cos).peekTxPacket(packet);
  }
}

std::optional<uint8_t> Queues::nextQueue() {
  return scheduler_->selectQueueToPop();
}

inline bool Queues::hasTxGateBlockedQueue() const {
  if (!txGate_) {
    return false;
  }
  // Occupancy, XON and the gate are the only clauses `isQueueEligible()`
  // tests, so with a gate installed an occupied XON queue can be ineligible
  // only because the gate rejected its head packet.
  for (uint8_t cos = 0; cos < numQueues_; ++cos) {
    if (getQueueLengthInPkts(cos) > 0 && getTxPfcState(cos) == PfcState::XON &&
        !isQueueEligible(cos)) {
      return true;
    }
  }
  return false;
}

bool Queues::isQueueEligible(const uint8_t cos) const {
  if (getQueueLengthInPkts(cos) == 0 || getTxPfcState(cos) != PfcState::XON) {
    return false;
  }
  if (!txGate_) {
    return true;
  }
  return txGate_(cosQueues_.at(cos).txQueue.front());
}

std::optional<uint8_t> Queues::peekQueue() const {
  return scheduler_->peekQueueToPop();
}

uint8_t Queues::filter(const PacketPtr& pkt) const {
  return numQueues_ > 1 ? filter_->selectQueueToPush(pkt) : 0;
}

void Queues::markVoqEcnPacket(
    const PacketPtr& pkt,
    const uint8_t qId,
    bool* marked) const {
  if (useColor_) {
    return;
  }
  auto& handler = *handlers_.at(qId);
  if (handler.getQueuePacketHandlerType() != QueuePacketHandlerType::VOQ_ECN) {
    return;
  }
  const bool m = static_cast<VoqECNQueuePacketHandler&>(handler).markEcnPacket(
      pkt, getQueueLengthInBytes(qId));
  if (marked) {
    *marked = m;
  }
}

} // namespace packsim
