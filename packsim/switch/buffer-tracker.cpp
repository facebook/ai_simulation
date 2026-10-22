/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/switch/buffer-tracker.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <vector>

#include <boost/interprocess/mapped_region.hpp>
#include <fmt/format.h>
#include <folly/Math.h>
#include <folly/logging/xlog.h>

namespace packsim {

packsim::switches::thrift::BufferStats SwitchStats::serializeState(
    const Time now) const {
  packsim::switches::thrift::BufferStats serStats;

  serStats.cellSizeBytes() = cellSize;

  serStats.bufPkts() = bufPkts;
  serStats.watermarkBufPkts() = watermarkBufPkts;

  serStats.availCells() = availCells;
  serStats.usedCells() = usedCells;
  serStats.watermarkCells() = watermarkCells;

  serStats.availQueueRsvdCells() = availQueueRsvdCells;
  serStats.usedQueueRsvdCells() = usedQueueRsvdCells;
  serStats.watermarkQueueRsvdCells() = watermarkQueueRsvdCells;

  serStats.availPortRsvdCells() = availPortRsvdCells;
  serStats.usedPortRsvdCells() = usedPortRsvdCells;
  serStats.watermarkPortRsvdCells() = watermarkPortRsvdCells;

  serStats.availSharedCells() = availSharedCells;
  serStats.usedSharedCells() = usedSharedCells;
  serStats.watermarkSharedCells() = watermarkSharedCells;

  serStats.availHdrmCells() = availHdrmCells;
  serStats.usedHdrmCells() = usedHdrmCells;
  serStats.watermarkHdrmCells() = watermarkHdrmCells;

  serStats.admittedPkts() = admittedPkts;
  serStats.admittedBytes() = admittedBytes;

  serStats.droppedPkts() = droppedPkts;
  serStats.droppedBytes() = droppedBytes;

  serStats.wredDroppedPkts() = wredDroppedPkts;
  serStats.wredDroppedBytes() = wredDroppedBytes;

  serStats.ecnMarkedPkts() = ecnMarkedPkts;

  serStats.totalBubbleTimeNs() = totalBubbleTimeNs;

  // Nothing ever arrived, so there is no window to report a fraction over.
  // Both members are still their UINT64_MAX sentinel, and these are signed
  // wire fields, so passing them through would publish -1; report the
  // schema's own defaults instead.
  if (firstDataTimeNs == kNoTimeNs) {
    serStats.firstDataTimeNs() = 0U;
    serStats.lastDataTimeNs() = 0U;
    serStats.bubbleFrac() = 0.0;
    return serStats;
  }

  // Where the queue stopped doing work, which is what the window has to end
  // at for the fraction to mean anything.
  //
  // While cells are still buffered that is `now`: everything since the last
  // refill has been busy time, and it is already excluded from
  // `totalBubbleTimeNs`, so stopping the window earlier would divide a
  // numerator that runs to the last refill by a denominator that does not --
  // which is how the fraction used to exceed 1.
  //
  // Once the queue has drained it is the drain instead, which is what keeps
  // the trailing idle period before the end of the simulation out of the
  // window. `totalBubbleTimeNs` never counts that period either -- a bubble is
  // only charged by the enqueue that closes it -- so the two agree.
  const uint64_t windowEndNs =
      usedCells == 0 ? thisBubbleStartTimeNs : now.getNs();

  serStats.firstDataTimeNs() = firstDataTimeNs;
  serStats.lastDataTimeNs() = windowEndNs;
  const uint64_t bubbleWindowNs = windowEndNs - firstDataTimeNs;
  serStats.bubbleFrac() = bubbleWindowNs == 0
      ? 0.0
      : static_cast<double>(totalBubbleTimeNs) / bubbleWindowNs;

  return serStats;
}

packsim::switches::thrift::Port PortTracker::serializeState() const {
  packsim::switches::thrift::Port serPort;
  serPort.portNum() = portNum;
  serPort.bufStats() = stats.serializeState(simCtx->now());
  return serPort;
}

packsim::switches::thrift::Queue QueueTracker::serializeState() const {
  packsim::switches::thrift::Queue serQ;

  serQ.portNum() = portNum;
  serQ.queueNum() = queueNum;
  serQ.alpha() = alpha;
  if (xonHystCells.has_value()) {
    serQ.xonHystCells() = xonHystCells.value();
  }
  if (resumeOffsetCells.has_value()) {
    serQ.resumeOffsetCells() = resumeOffsetCells.value();
  }
  serQ.canUsePortRsvd() = canUsePortRsvd;
  serQ.dynamicSharing() = dynamicSharing;
  serQ.pfc() = pfc;
  serQ.lossless() = lossless;
  serQ.pfcState() = static_cast<packsim::switches::thrift::PfcState>(pfcState);
  serQ.bufStats() = stats.serializeState(simCtx->now());

  return serQ;
}

packsim::switches::thrift::Port BufferTracker::serializePort(
    const uint16_t portNum) const {
  auto serPort = portTracker(portNum).serializeState();
  for (uint8_t qIdx = 0; qIdx < queuesPerPort_; ++qIdx) {
    serPort.queues()->emplace_back(
        queueTracker(portNum, qIdx).serializeState());
  }
  return serPort;
}

packsim::switches::thrift::Buffer BufferTracker::serializeState() const {
  packsim::switches::thrift::Buffer serBuf;
  serBuf.bufStats() = bufferStats_->serializeState(simCtx_->now());
  // Ascending, because ports_ is a hash map whose iteration order differs
  // between a run and its replay.
  auto portNums = getPortNums();
  std::ranges::sort(portNums);
  for (const auto& portNum : portNums) {
    serBuf.ports()->emplace_back(serializePort(portNum));
  }
  return serBuf;
}

PortTracker::PortTracker(
    SimContext* simCtx,
    const packsim::thrift::PortBufferConfig& portCfg)
    : simCtx(simCtx) {
  initialize(portCfg);
}

void PortTracker::initialize(const packsim::thrift::PortBufferConfig& portCfg) {
  stats.availPortRsvdCells = *portCfg.portRsvdCells();
  stats.availSharedCells = *portCfg.portMaxSharedCells();
}

QueueTracker::QueueTracker(
    SimContext* simCtx,
    const packsim::thrift::PriorityGroupBufferConfig& pgCfg)
    : simCtx(simCtx) {
  initialize(pgCfg);
}

QueueTracker::QueueTracker(
    SimContext* simCtx,
    const packsim::thrift::EgressQueueBufferConfig& eqCfg)
    : simCtx(simCtx) {
  initialize(eqCfg);
}

void QueueTracker::initialize(
    const packsim::thrift::PriorityGroupBufferConfig& pgCfg) {
  alpha = *pgCfg.alpha();
  stats.availQueueRsvdCells = *pgCfg.rsvdCells();
  stats.availSharedCells = *pgCfg.maxSharedCells();
  stats.availHdrmCells = *pgCfg.maxHdrmCells();
  if (pgCfg.xonHystCells().has_value()) {
    xonHystCells = *pgCfg.xonHystCells();
  }
  if (pgCfg.resumeOffsetCells().has_value()) {
    resumeOffsetCells = *pgCfg.resumeOffsetCells();
  }
  if (xonHystCells.has_value() && resumeOffsetCells.has_value()) {
    // raise an exception if both xonHystCells and resumeOffsetCells are set
    throw std::runtime_error(
        "xonHystCells and resumeOffsetCells cannot be set at the same time");
  }
  canUsePortRsvd = *pgCfg.canUsePortRsvd();
  dynamicSharing = *pgCfg.dynamicSharing();
  pfc = *pgCfg.pfc();
  lossless = *pgCfg.lossless();
}

void QueueTracker::initialize(
    const packsim::thrift::EgressQueueBufferConfig& eqCfg) {
  alpha = *eqCfg.alpha();
  stats.availQueueRsvdCells = *eqCfg.rsvdCells();
  stats.availSharedCells = *eqCfg.maxSharedCells();
  dynamicSharing = *eqCfg.dynamicSharing();
  pfc = false;
}

bool QueueTracker::tryAddPacket(const uint32_t bytes, AdmitOptions opts) {
  const uint32_t totalCells = cellDivider.getCellsFromBytes(bytes);
  uint32_t cellsLeft = totalCells;

  // 1. Use queue reserved cells
  const uint32_t qRsvdCells = std::min(cellsLeft, stats.availQueueRsvdCells);
  cellsLeft -= qRsvdCells;

  // 2. Use port reserved cells, if allowed
  uint32_t portRsvdCells{0};
  if ((cellsLeft > 0) && canUsePortRsvd) {
    portRsvdCells = std::min(cellsLeft, portStats->availPortRsvdCells);
    cellsLeft -= portRsvdCells;
  }

  // 3. Use shared cells
  uint32_t sharedCells{0};
  if (cellsLeft > 0) {
    sharedCells = std::min(cellsLeft, getAvailSharedCells());
    cellsLeft -= sharedCells;
  }

  // 4. Finally, use headroom cells. On a CBFC-managed queue this region is the
  // credited space, so only lossless traffic may reach it.
  uint32_t hdrmCells{0};
  if (cellsLeft > 0 &&
      (!cbfcManaged || opts.trafficClass == BufferTrafficClass::LOSSLESS)) {
    hdrmCells = std::min(cellsLeft, getAvailHdrmCells());
    cellsLeft -= hdrmCells;
  }

  if (cellsLeft > 0) {
    // We didn't have space
    return false;
  }

  if (opts.heldCreditsOut != nullptr) {
    // Only headroom is credited on a CBFC-managed queue in this model; an
    // unmanaged queue holds the packet's whole cell cost.
    const uint32_t heldCells = cbfcManaged ? hdrmCells : totalCells;
    // `Packet::heldCredits` is 16 bits, so a large packet against a small cell
    // size can overflow it. Only a CBFC-managed queue returns this count as
    // credit at drain, so only there is an unrepresentable value a correctness
    // failure; refuse the packet rather than abort the simulation.
    if (cbfcManaged && heldCells > std::numeric_limits<uint16_t>::max()) {
      XLOGF(
          ERR,
          "Buffer admission refused on port {} queue {}: {} held cells exceed "
          "the 16-bit Packet::heldCredits field ({} B packet, {} B cells).",
          portNum,
          queueNum,
          heldCells,
          bytes,
          stats.cellSize);
      return false;
    }
    *opts.heldCreditsOut = static_cast<uint16_t>(
        std::min<uint32_t>(heldCells, std::numeric_limits<uint16_t>::max()));
  }

  const auto now = simCtx->now();
  stats.addPacket(
      bytes,
      totalCells,
      qRsvdCells,
      portRsvdCells,
      sharedCells,
      hdrmCells,
      now);
  portStats->addPacket(
      bytes,
      totalCells,
      qRsvdCells,
      portRsvdCells,
      sharedCells,
      hdrmCells,
      now);
  bufferStats->addPacket(
      bytes,
      totalCells,
      qRsvdCells,
      portRsvdCells,
      sharedCells,
      hdrmCells,
      now);
  return true;
}

void QueueTracker::updatePfc() {
  if (pfcState == PfcState::XON) {
    if (stats.usedHdrmCells > 0) {
      // Transition to XOFF: Record current shared buffer high water
      pfcState = PfcState::XOFF;
      stats.watermarkXoffSharedCells = stats.usedSharedCells;
    }
    return;
  }

  // Currently in XOFF state
  if (stats.usedHdrmCells > 0) {
    // Still using headroom, remain in XOFF
    return;
  }

  // No headroom in use, check if we should transition to XON
  bool canTransitionToXon = false;

  if (resumeOffsetCells.has_value()) {
    const uint32_t targetSharedUsage =
        (stats.watermarkXoffSharedCells >= resumeOffsetCells.value())
        ? stats.watermarkXoffSharedCells - resumeOffsetCells.value()
        : 0;
    const uint32_t currentSharedUsage = stats.usedSharedCells;
    // Limit is max # cells this queue can use right now, including already
    // buffered data
    const uint32_t limit = std::floor(alpha * bufferStats->availSharedCells);
    const uint32_t availSharedCells =
        (limit >= stats.usedSharedCells) ? limit - stats.usedSharedCells : 0;
    canTransitionToXon =
        (currentSharedUsage <= targetSharedUsage) && (availSharedCells > 0);

  } else if (xonHystCells.has_value()) {
    uint32_t availSharedCells = bufferStats->availSharedCells;
    if (dynamicSharing) {
      // Limit is max # cells this queue can use right now, including already
      // buffered data
      const uint32_t limit = std::floor(alpha * availSharedCells);
      availSharedCells =
          (limit >= stats.usedSharedCells) ? limit - stats.usedSharedCells : 0;
    }
    canTransitionToXon =
        (availSharedCells >= xonHystCells.value()) && (availSharedCells > 0);
  }

  if (canTransitionToXon) {
    pfcState = PfcState::XON;
    stats.watermarkXoffSharedCells = 0;
  }
}

uint16_t QueueTracker::removePacket(
    const uint32_t bytes,
    const uint16_t heldCredits) {
  const uint32_t totalCells = cellDivider.getCellsFromBytes(bytes);
  if (totalCells > stats.usedCells) {
    throw std::runtime_error(
        fmt::format(
            "Don't have enough used cells to remove this packet: "
            "totalCells={}, usedCells={}, bytes={}, cellSize={}, "
            "usedHdrm={}, usedShared={}, usedPortRsvd={}, usedQueueRsvd={}",
            totalCells,
            stats.usedCells,
            bytes,
            stats.cellSize,
            stats.usedHdrmCells,
            stats.usedSharedCells,
            stats.usedPortRsvdCells,
            stats.usedQueueRsvdCells));
  }

  uint32_t cellsLeft = totalCells;

  // `updatePfc` reads `usedHdrmCells` to decide XON, so a CBFC-managed queue
  // must release exactly the headroom cells this packet took; draining headroom
  // first regardless of which packet is leaving would zero that counter while
  // another packet still occupies headroom. Both arms clamp to
  // `stats.usedHdrmCells`, so a stale `heldCredits` cannot wrap the
  // subtractions below.
  const uint32_t hdrmCells = cbfcManaged
      ? std::min<uint32_t>({heldCredits, cellsLeft, stats.usedHdrmCells})
      : std::min(cellsLeft, stats.usedHdrmCells);
  cellsLeft -= hdrmCells;

  const uint32_t sharedCells = std::min(cellsLeft, stats.usedSharedCells);
  cellsLeft -= sharedCells;

  const uint32_t portRsvdCells = std::min(cellsLeft, stats.usedPortRsvdCells);
  cellsLeft -= portRsvdCells;

  const uint32_t qRsvdCells = std::min(cellsLeft, stats.usedQueueRsvdCells);
  cellsLeft -= qRsvdCells;

  if (cellsLeft) {
    throw std::runtime_error(
        fmt::format(
            "Don't have enough used cells of specific types to remove this packet: "
            "cellsLeft={}, totalCells={}, bytes={}, cellSize={}, "
            "hdrmCells={}, sharedCells={}, portRsvdCells={}, qRsvdCells={}, "
            "usedHdrm={}, usedShared={}, usedPortRsvd={}, usedQueueRsvd={}",
            cellsLeft,
            totalCells,
            bytes,
            stats.cellSize,
            hdrmCells,
            sharedCells,
            portRsvdCells,
            qRsvdCells,
            stats.usedHdrmCells,
            stats.usedSharedCells,
            stats.usedPortRsvdCells,
            stats.usedQueueRsvdCells));
  }

  const auto now = simCtx->now();
  stats.removePacket(
      totalCells, qRsvdCells, portRsvdCells, sharedCells, hdrmCells, now);
  portStats->removePacket(
      totalCells, qRsvdCells, portRsvdCells, sharedCells, hdrmCells, now);
  bufferStats->removePacket(
      totalCells, qRsvdCells, portRsvdCells, sharedCells, hdrmCells, now);
  // The clamp above can release fewer headroom cells than the packet recorded;
  // returning `heldCredits` there would credit the sender for space that never
  // came back.
  return cbfcManaged ? static_cast<uint16_t>(hdrmCells) : heldCredits;
}

void QueueTracker::droppedPacket(const uint32_t bytes) {
  stats.droppedPacket(bytes);
  portStats->droppedPacket(bytes);
  bufferStats->droppedPacket(bytes);
}

void QueueTracker::wredDroppedPacket(const uint32_t bytes) {
  stats.wredDroppedPacket(bytes);
  portStats->wredDroppedPacket(bytes);
  bufferStats->wredDroppedPacket(bytes);
}

void QueueTracker::ecnMarkedPacket() {
  ++stats.ecnMarkedPkts;
  ++portStats->ecnMarkedPkts;
  ++bufferStats->ecnMarkedPkts;
}

/** BufferTracker **/
BufferTracker::BufferTracker(
    SimContext* simCtx,
    const uint32_t cellSize,
    const std::vector<uint16_t>& portNums,
    const uint8_t queuesPerPort)
    : simCtx_(simCtx) {
  initialize(cellSize, portNums, queuesPerPort);
}

void BufferTracker::initialize(
    const uint32_t cellSize,
    const std::vector<uint16_t>& portNums,
    const uint8_t queuesPerPort) {
  const CellDivider cellDivider(cellSize);
  cellSize_ = cellSize;
  queuesPerPort_ = queuesPerPort;
  const size_t numPorts = portNums.size();

  // Typical x86-64 cache line size. Only the switch-wide `bufferStats_` is
  // padded to whole cache lines; the port and queue trackers after it are
  // packed back to back.
  constexpr size_t kCacheLineSizeBytes = 64;
  const size_t statsCacheLines =
      folly::divCeil(sizeof(SwitchStats), kCacheLineSizeBytes);
  const size_t statsSpace = kCacheLineSizeBytes * statsCacheLines;
  const size_t minSize = statsSpace + sizeof(PortTracker) * numPorts +
      sizeof(QueueTracker) * queuesPerPort * numPorts;

  const size_t pageSize = boost::interprocess::mapped_region::get_page_size();
  const size_t pages = folly::divCeil(minSize, pageSize);
  const size_t size = pages * pageSize;

  std::unique_ptr<uint8_t[]> storage =
      std::make_unique_for_overwrite<uint8_t[]>(size);
  std::memset(&storage[0], 0, size);
  storage_ = std::move(storage);

  uint8_t* ptr = &(storage_[0]);
  bufferStats_ = reinterpret_cast<SwitchStats*>(ptr);
  *bufferStats_ = SwitchStats();
  bufferStats_->cellSize = cellSize;
  ptr += statsSpace;

  PortTracker* portPtr;
  QueueTracker* queuePtr;
  for (const auto& portNum : portNums) {
    // Goal is to interleave the PortStats and QueueStats objects in the same
    // general region of memory, so that the PortStats object is as close to all
    // of its QueueStats objects as possible. Do this by walking pointer down
    // the newly created array as we make each PortStats and QueueStats object.
    portPtr = reinterpret_cast<PortTracker*>(ptr);
    ptr += sizeof(PortTracker);
    *portPtr = PortTracker(simCtx_);
    portPtr->portNum = portNum;
    portPtr->stats.cellSize = cellSize;
    ports_[portNum] = portPtr;

    for (uint8_t queueNum = 0; queueNum < queuesPerPort; queueNum++) {
      queuePtr = reinterpret_cast<QueueTracker*>(ptr);
      ptr += sizeof(QueueTracker);
      *queuePtr = QueueTracker(simCtx_);
      queuePtr->portNum = portNum;
      queuePtr->queueNum = queueNum;
      queuePtr->stats.cellSize = cellSize;
      queuePtr->cellDivider = cellDivider;
      queuePtr->bufferStats = bufferStats_;
      queuePtr->portStats = &(portStats(portNum));
    }
  }
}

void BufferTracker::initPort(
    const uint16_t portNum,
    const packsim::thrift::PortBufferConfig& portCfg) {
  portTracker(portNum).initialize(portCfg);
}

void BufferTracker::initQueue(
    const uint16_t portNum,
    const uint8_t queueNum,
    const packsim::thrift::PriorityGroupBufferConfig& pgCfg) {
  queueTracker(portNum, queueNum).initialize(pgCfg);
}

void BufferTracker::initQueue(
    const uint16_t portNum,
    const uint8_t queueNum,
    const packsim::thrift::EgressQueueBufferConfig& eqCfg) {
  queueTracker(portNum, queueNum).initialize(eqCfg);
}

std::vector<uint16_t> BufferTracker::getPortNums() const {
  std::vector<uint16_t> portNums;
  portNums.reserve(ports_.size());
  for (const auto& pair : ports_) {
    portNums.emplace_back(pair.first);
  }
  return portNums;
}

} // namespace packsim
