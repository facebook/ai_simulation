/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/switch/simple-switch-buffer.h"

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>

#include <fmt/format.h>
#include <folly/Conv.h>

namespace packsim {

SimpleSwitchBuffer::SimpleSwitchBuffer(
    SimContext* simCtx,
    const packsim::thrift::SwitchBufferConfig& cfg,
    const bool useIngress)
    : simCtx_(simCtx), bufTracker_(simCtx) {
  const std::string bufferKind = useIngress ? "ingress" : "egress";
  if (*cfg.cellSizeBytes() == 0) {
    throw std::invalid_argument(
        fmt::format(
            "SwitchBufferConfig ({} buffer): cellSizeBytes is 0; it must be at least 1",
            bufferKind));
  }
  // Initialize the BufferTracker:
  buildBufferTracker(cfg, useIngress);

  // Sum the reserved cells, in 64 bits so that the sum cannot wrap:
  uint64_t portRsvdCells = 0;
  uint64_t queueRsvdCells = 0;
  for (const auto& [configPort, portCfg] : *cfg.ports()) {
    const auto portNum = folly::to<uint16_t>(configPort);

    ports_.emplace(portNum);

    bufTracker_.initPort(portNum, portCfg);
    portRsvdCells += *portCfg.portRsvdCells();

    if (portNum != *portCfg.port()) {
      throw std::runtime_error("Key and portCfg port number don't match");
    }
    uint8_t qIdx{0};
    if (useIngress && *portCfg.isIngrPort()) {
      for (const auto& qConfig : *portCfg.pgs()) {
        queueRsvdCells += *qConfig.rsvdCells();
        bufTracker_.initQueue(portNum, qIdx, qConfig);
        ++qIdx;
      }
    } else if (!useIngress && *portCfg.isEgrPort()) {
      for (const auto& qConfig : *portCfg.ucQueues()) {
        queueRsvdCells += *qConfig.rsvdCells();
        bufTracker_.initQueue(portNum, qIdx, qConfig);
        ++qIdx;
      }
    }
  }

  const uint64_t carvedCells =
      uint64_t{*cfg.headroomPoolCells()} + portRsvdCells + queueRsvdCells;
  if (carvedCells > *cfg.totalCells()) {
    throw std::invalid_argument(
        fmt::format(
            "SwitchBufferConfig ({} buffer): headroomPoolCells {} plus portRsvdCells {} plus queue rsvdCells {} is {} cells, more than totalCells {}",
            bufferKind,
            *cfg.headroomPoolCells(),
            portRsvdCells,
            queueRsvdCells,
            carvedCells,
            *cfg.totalCells()));
  }

  bufTracker_.bufferStats_->availCells = *cfg.totalCells();
  bufTracker_.bufferStats_->availHdrmCells = *cfg.headroomPoolCells();
  bufTracker_.bufferStats_->availSharedCells =
      static_cast<uint32_t>(*cfg.totalCells() - carvedCells);
}

void SimpleSwitchBuffer::buildBufferTracker(
    const packsim::thrift::SwitchBufferConfig& cfg,
    const bool useIngress) {
  const uint32_t cellSize = *cfg.cellSizeBytes();
  std::vector<uint16_t> ports;
  ports.reserve(cfg.ports()->size());
  uint8_t maxQueues{0};
  for (const auto& [configPort, portCfg] : *cfg.ports()) {
    ports.emplace_back(folly::to<uint16_t>(configPort));
    const auto numQueues = folly::to<uint8_t>(
        useIngress ? portCfg.pgs()->size() : portCfg.ucQueues()->size());
    maxQueues = std::max(numQueues, maxQueues);
  }

  bufTracker_.initialize(cellSize, ports, maxQueues);
}

uint16_t SimpleSwitchBuffer::wredDrop(
    const uint16_t port,
    const uint8_t queue,
    const uint32_t pktBytes,
    uint16_t heldCredits) {
  const uint16_t releasedHdrmCells =
      removePacket(port, queue, pktBytes, heldCredits);
  queueTracker(port, queue).wredDroppedPacket(pktBytes);
  return releasedHdrmCells;
}

void SimpleSwitchBuffer::setPortCbfcManaged(const uint16_t portNum) {
  // Trackers for PGs this port never uses are marked too; that is harmless
  // because `admitPacket` only ever addresses configured (port, cos) pairs.
  for (uint8_t q = 0; q < bufTracker_.queuesPerPort_; ++q) {
    bufTracker_.queueTracker(portNum, q).cbfcManaged = true;
  }
}

void SimpleSwitchBuffer::ecnMark(const uint16_t port, const uint8_t queue) {
  queueTracker(port, queue).ecnMarkedPacket();
}

} // namespace packsim
