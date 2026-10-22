/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include <folly/CPortability.h>
#include <folly/container/F14Map.h>
#include <folly/logging/xlog.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/switches_types.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/packet/pfc.h"
#include "packsim/switch/cell-divider.h"

namespace packsim {

struct SwitchStats {
  // Stands in for "this has not happened yet" in the bubble timestamps below.
  // It is deliberately not a real time: the accounting has to tell a queue
  // that has never held data from one that filled at time zero.
  static constexpr uint64_t kNoTimeNs = UINT64_MAX;

  uint32_t cellSize{0};

  uint32_t bufPkts{0};
  uint32_t watermarkBufPkts{0};

  uint32_t availCells{UINT32_MAX};
  uint32_t usedCells{0};
  uint32_t watermarkCells{0};

  uint32_t availQueueRsvdCells{UINT32_MAX};
  uint32_t usedQueueRsvdCells{0};
  uint32_t watermarkQueueRsvdCells{0};

  uint32_t availPortRsvdCells{UINT32_MAX};
  uint32_t usedPortRsvdCells{0};
  uint32_t watermarkPortRsvdCells{0};

  uint32_t availSharedCells{UINT32_MAX};
  uint32_t usedSharedCells{0};
  uint32_t watermarkSharedCells{0};
  uint32_t watermarkXoffSharedCells{0};

  uint32_t availHdrmCells{UINT32_MAX};
  uint32_t usedHdrmCells{0};
  uint32_t watermarkHdrmCells{0};

  uint64_t admittedPkts{0};
  uint64_t admittedBytes{0};

  uint64_t droppedPkts{0};
  uint64_t droppedBytes{0};

  uint64_t wredDroppedPkts{0};
  uint64_t wredDroppedBytes{0};

  uint64_t ecnMarkedPkts{0};

  // Track total "bubble" duration in this queue; bubble is characterized as any
  // time after the first data is seen that the queue is empty, not including
  // the time from final dequeue to end of simulation
  uint64_t totalBubbleTimeNs{0};
  uint64_t firstDataTimeNs{kNoTimeNs};
  uint64_t thisBubbleStartTimeNs{kNoTimeNs};

  FOLLY_ALWAYS_INLINE void addPacket(
      uint32_t bytes,
      uint32_t totalCells,
      uint32_t queueRsvdCells,
      uint32_t portRsvdCells,
      uint32_t sharedCells,
      uint32_t hdrmCells,
      const Time now);

  FOLLY_ALWAYS_INLINE void removePacket(
      uint32_t totalCells,
      uint32_t queueRsvdCells,
      uint32_t portRsvdCells,
      uint32_t sharedCells,
      uint32_t hdrmCells,
      const Time now);

  FOLLY_ALWAYS_INLINE void droppedPacket(uint32_t bytes);

  FOLLY_ALWAYS_INLINE void wredDroppedPacket(uint32_t bytes);

  /**
   * Snapshot these counters as the wire struct.
   *
   * @param now  The instant the snapshot is taken at. It is the end of the
   *             bubble window whenever cells are still buffered, so it cannot
   *             be recovered from the counters themselves -- see
   *             `lastDataTimeNs` in `BufferStats` for the full contract.
   *
   * @return     The counters, with the bubble window resolved.
   */
  packsim::switches::thrift::BufferStats serializeState(Time now) const;
};

struct PortTracker {
  explicit PortTracker(SimContext* simCtx) : simCtx(simCtx) {}
  explicit PortTracker(
      SimContext* simCtx,
      const packsim::thrift::PortBufferConfig& portCfg);
  void initialize(const packsim::thrift::PortBufferConfig& portCfg);

  packsim::switches::thrift::Port serializeState() const;

  SimContext* simCtx;
  uint16_t portNum{0};
  SwitchStats stats;
};

/**
 * Whether a packet may use the credited PG headroom region. Admission fills PG
 * minimum, then the shared pool, then PG headroom. On a CBFC-managed queue a
 * lossy packet stops after the shared pool: headroom is the only credited
 * region and, as with PFC headroom, is kept for lossless traffic. An unmanaged
 * queue lets any packet reach headroom.
 */
enum class BufferTrafficClass : uint8_t { LOSSY, LOSSLESS };

/**
 * Optional per-packet inputs to `QueueTracker::tryAddPacket` and
 * `SimpleSwitchBuffer::admitPacket`.
 */
struct AdmitOptions {
  BufferTrafficClass trafficClass{BufferTrafficClass::LOSSY};
  // Where to record the cells the buffer holds for this packet until it
  // leaves: the headroom cells on a CBFC-managed queue, the packet's whole
  // cell cost otherwise. A null pointer records nothing.
  uint16_t* heldCreditsOut{nullptr};
};

struct QueueTracker {
  explicit QueueTracker(SimContext* simCtx) : simCtx(simCtx) {}
  explicit QueueTracker(
      SimContext* simCtx,
      const packsim::thrift::PriorityGroupBufferConfig& pgCfg);
  explicit QueueTracker(
      SimContext* simCtx,
      const packsim::thrift::EgressQueueBufferConfig& qCfg);

  void initialize(const packsim::thrift::PriorityGroupBufferConfig& pgCfg);
  void initialize(const packsim::thrift::EgressQueueBufferConfig& eqCfg);

  bool tryAddPacket(uint32_t bytes, AdmitOptions opts = {});

  /**
   * Removes a packet from this queue and reports the cells the caller may
   * return as credit.
   *
   * @param bytes          Size in bytes of the packet leaving the queue.
   * @param heldCredits    Cells recorded for this packet at admission through
   *                       `AdmitOptions::heldCreditsOut`. Honoured only on a
   *                       CBFC-managed queue.
   *
   * @return               Cells the caller may return as credit. On a
   *                       CBFC-managed queue this is `heldCredits` clamped by
   *                       the packet's own cell count and by the headroom the
   *                       tracker still holds, so credit return must use this
   *                       value rather than `heldCredits`. On an unmanaged
   *                       queue it is `heldCredits` unchanged.
   *
   * @throw                std::runtime_error if the queue does not hold enough
   *                       used cells to remove the packet.
   */
  [[nodiscard]] uint16_t removePacket(uint32_t bytes, uint16_t heldCredits = 0);
  void droppedPacket(uint32_t bytes);
  void wredDroppedPacket(uint32_t bytes);
  void ecnMarkedPacket();
  void updatePfc();

  packsim::switches::thrift::Queue serializeState() const;

  FOLLY_ALWAYS_INLINE uint32_t getAvailRsvdCells() const noexcept;
  FOLLY_ALWAYS_INLINE uint32_t getAvailSharedCells() const noexcept;
  FOLLY_ALWAYS_INLINE uint32_t getAvailHdrmCells() const noexcept;

  SimContext* simCtx;
  SwitchStats* bufferStats{};
  SwitchStats* portStats{};
  // Counts the cells a packet takes in this queue. `BufferTracker` sets it
  // from the buffer's cell size, together with `stats.cellSize`.
  CellDivider cellDivider;
  double alpha{0};
  // PFC resume threshold: when set, PFC resumes (XON) when available shared
  // cells reach this value
  std::optional<uint32_t> xonHystCells;
  // Alternative PFC resume mechanism: when set, PFC resumes when buffer usage
  // drops to (xoff_threshold - resumeOffsetCells) instead of using hysteresis
  std::optional<uint32_t> resumeOffsetCells;
  uint16_t portNum{0};
  bool canUsePortRsvd{true};
  bool dynamicSharing{true};
  bool pfc{true};
  bool lossless{true};
  // True only for ingress trackers on a port with CBFC enabled.
  // Set once at port setup, before any traffic, and never cleared.
  bool cbfcManaged{false};
  uint8_t queueNum{0};
  PfcState pfcState{PfcState::XON};

  SwitchStats stats;
};

class BufferTracker {
 public:
  explicit BufferTracker(SimContext* simCtx) : simCtx_(simCtx) {}
  explicit BufferTracker(
      SimContext* simCtx,
      uint32_t cellSize,
      const std::vector<uint16_t>& portNums,
      uint8_t queuesPerPort);

  FOLLY_ALWAYS_INLINE PortTracker& portTracker(uint16_t port) const {
    return *ports_.at(port);
  }

  FOLLY_ALWAYS_INLINE SwitchStats& portStats(uint16_t port) const {
    return portTracker(port).stats;
  }

  FOLLY_ALWAYS_INLINE QueueTracker& queueTracker(uint16_t port, uint8_t queue)
      const {
    XCHECK_LT(queue, queuesPerPort_);
    uint8_t* ptr = reinterpret_cast<uint8_t*>(&portTracker(port)) +
        sizeof(PortTracker) + queue * sizeof(QueueTracker);
    return *reinterpret_cast<QueueTracker*>(ptr);
  }

  FOLLY_ALWAYS_INLINE SwitchStats& queueStats(uint16_t port, uint8_t queue)
      const {
    return queueTracker(port, queue).stats;
  }
  std::vector<uint16_t> getPortNums() const;
  void initPort(
      uint16_t port,
      const packsim::thrift::PortBufferConfig& portCfg);
  void initQueue(
      uint16_t port,
      uint8_t queueNum,
      const packsim::thrift::PriorityGroupBufferConfig& pgCfg);
  void initQueue(
      uint16_t port,
      uint8_t queueNum,
      const packsim::thrift::EgressQueueBufferConfig& eqCfg);

  packsim::switches::thrift::Buffer serializeState() const;

 private:
  /**
   * Serialize one port together with its queues.
   *
   * @param portNum    The port number to serialize.
   *
   * @return           The serialized port, queues included.
   */
  packsim::switches::thrift::Port serializePort(const uint16_t portNum) const;

  friend class SimpleSwitchBuffer;
  void initialize(
      uint32_t cellSize,
      const std::vector<uint16_t>& ports,
      uint8_t queuesPerPort);

  SimContext* const simCtx_;
  uint32_t cellSize_{254};
  uint8_t queuesPerPort_{1};
  std::unique_ptr<uint8_t[]> storage_;
  SwitchStats* bufferStats_{};
  folly::F14FastMap<uint16_t, PortTracker*> ports_;
};

/*** Inline Implementations ***/

void SwitchStats::addPacket(
    uint32_t bytes,
    uint32_t totalCells,
    uint32_t queueRsvdCells,
    uint32_t portRsvdCells,
    uint32_t sharedCells,
    uint32_t hdrmCells,
    const Time now) {
  // If the queue is empty on enqueue, we might be at the end of a bubble. The
  // only time we won't be is the first time we enqueue data. We log bubbles
  // on the empty -> non-empty transition to make sure we don't count the
  // final empty period before end of simulation.
  if (usedCells == 0) {
    auto ns = now.getNs();
    if (firstDataTimeNs == kNoTimeNs) {
      firstDataTimeNs = ns;
    }
    if (ns > thisBubbleStartTimeNs) {
      // This will be true every time we go from empty -> not empty, except
      // the first time, because thisBubbleStartTimeNs is still kNoTimeNs and
      // no real time compares greater than it.
      totalBubbleTimeNs += ns - thisBubbleStartTimeNs;
    }
  }
  ++bufPkts;
  ++admittedPkts;
  admittedBytes += bytes;

  watermarkBufPkts = std::max(watermarkBufPkts, bufPkts);

  availCells -= totalCells;
  usedCells += totalCells;
  watermarkCells = std::max(watermarkCells, usedCells);

  availQueueRsvdCells -= queueRsvdCells;
  usedQueueRsvdCells += queueRsvdCells;
  watermarkQueueRsvdCells =
      std::max(watermarkQueueRsvdCells, usedQueueRsvdCells);

  availPortRsvdCells -= portRsvdCells;
  usedPortRsvdCells += portRsvdCells;
  watermarkPortRsvdCells = std::max(watermarkPortRsvdCells, usedPortRsvdCells);

  availSharedCells -= sharedCells;
  usedSharedCells += sharedCells;
  watermarkSharedCells = std::max(watermarkSharedCells, usedSharedCells);

  availHdrmCells -= hdrmCells;
  usedHdrmCells += hdrmCells;
  watermarkHdrmCells = std::max(watermarkHdrmCells, usedHdrmCells);
}

void SwitchStats::removePacket(
    uint32_t totalCells,
    uint32_t queueRsvdCells,
    uint32_t portRsvdCells,
    uint32_t sharedCells,
    uint32_t hdrmCells,
    const Time now) {
  --bufPkts;

  availCells += totalCells;
  usedCells -= totalCells;

  availQueueRsvdCells += queueRsvdCells;
  usedQueueRsvdCells -= queueRsvdCells;

  availPortRsvdCells += portRsvdCells;
  usedPortRsvdCells -= portRsvdCells;

  availSharedCells += sharedCells;
  usedSharedCells -= sharedCells;

  availHdrmCells += hdrmCells;
  usedHdrmCells -= hdrmCells;

  // If the queue is now empty, we're at the start of a bubble
  if (usedCells == 0) {
    thisBubbleStartTimeNs = now.getNs();
  }
}

void SwitchStats::droppedPacket(uint32_t bytes) {
  ++droppedPkts;
  droppedBytes += bytes;
}

void SwitchStats::wredDroppedPacket(uint32_t bytes) {
  ++wredDroppedPkts;
  wredDroppedBytes += bytes;
}

uint32_t QueueTracker::getAvailRsvdCells() const noexcept {
  return canUsePortRsvd
      ? stats.availQueueRsvdCells + portStats->availPortRsvdCells
      : stats.availQueueRsvdCells;
}

uint32_t QueueTracker::getAvailSharedCells() const noexcept {
  uint32_t availSharedCells = bufferStats->availSharedCells;
  if (dynamicSharing) {
    // Limit is max # cells this queue can use right now, including already
    // buffered data
    const uint32_t limit = std::floor(alpha * availSharedCells);
    availSharedCells =
        (limit >= stats.usedSharedCells) ? limit - stats.usedSharedCells : 0;
  }

  // Apply queue and port limits on max shared cells
  availSharedCells = std::min(availSharedCells, stats.availSharedCells);
  availSharedCells = std::min(availSharedCells, portStats->availSharedCells);
  return availSharedCells;
}

uint32_t QueueTracker::getAvailHdrmCells() const noexcept {
  if (lossless) {
    const auto availHdrmCells =
        std::min(stats.availHdrmCells, portStats->availHdrmCells);
    return std::min(availHdrmCells, bufferStats->availHdrmCells);
  }
  return 0;
}
} // namespace packsim
