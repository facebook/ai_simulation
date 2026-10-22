/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <vector>

#include <folly/CPortability.h>
#include <folly/container/F14Set.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/switches_types.h"
#include "packsim/packet/pfc.h"
#include "packsim/switch/buffer-tracker.h"

namespace packsim {

/**
 * Manages switch buffer admission control and tracking.
 *
 * Handles packet admission, removal, WRED drops, ECN marking, and PFC state
 * updates for switch ports and queues.
 */
class SimpleSwitchBuffer {
 public:
  /**
   * Constructs a switch buffer manager.
   *
   * @param simCtx      Simulation context
   * @param cfg         Buffer configuration
   * @param useIngress  Whether to use ingress buffer (default true)
   * @throw             std::invalid_argument if `cellSizeBytes` is 0, or if
   *                    `headroomPoolCells` plus every port's `portRsvdCells`
   *                    plus the `rsvdCells` of the queues this buffer uses
   *                    exceeds `totalCells`; std::runtime_error if a port's
   *                    config key does not match its `port` field;
   *                    folly::ConversionError if a port number does not fit
   *                    `uint16_t` or a port has more queues than `uint8_t` can
   *                    count
   */
  explicit SimpleSwitchBuffer(
      SimContext* simCtx,
      const packsim::thrift::SwitchBufferConfig& cfg,
      bool useIngress = true);

  /**
   * Attempts to admit a packet to the buffer.
   *
   * @param port      Port number
   * @param queue     Queue number
   * @param pktBytes  Packet size in bytes
   * @param opts      Optional inputs to this admission; see `AdmitOptions`
   * @return          true if packet was admitted, false if dropped
   */
  FOLLY_ALWAYS_INLINE bool admitPacket(
      uint16_t port,
      uint8_t queue,
      uint32_t pktBytes,
      AdmitOptions opts = {});

  /**
   * Marks every queue tracker on `portNum` as CBFC-managed, enabling the
   * lossless-only headroom rules in `QueueTracker::tryAddPacket`. A port
   * cannot be unmarked, so mark it before the port carries traffic.
   *
   * @param portNum  Port number
   */
  void setPortCbfcManaged(uint16_t portNum);

  /**
   * Removes a packet from the buffer.
   *
   * @param port         Port number
   * @param queue        Queue number
   * @param pktBytes     Packet size in bytes
   * @param heldCredits  Cells recorded in `AdmitOptions::heldCreditsOut` at
   *                     admission
   * @return             Cells the caller may return as credit; see
   *                     `QueueTracker::removePacket`.
   */
  FOLLY_ALWAYS_INLINE uint16_t removePacket(
      uint16_t port,
      uint8_t queue,
      uint32_t pktBytes,
      uint16_t heldCredits = 0);

  /**
   * Records a WRED drop for a packet.
   *
   * @param port         Port number
   * @param queue        Queue number
   * @param pktBytes     Packet size in bytes
   * @param heldCredits  Cells recorded in `AdmitOptions::heldCreditsOut` at
   *                     admission
   * @return             Cells the caller may return as credit; see
   *                     `QueueTracker::removePacket`.
   */
  [[nodiscard]] uint16_t wredDrop(
      uint16_t port,
      uint8_t queue,
      uint32_t pktBytes,
      uint16_t heldCredits = 0);

  /**
   * Marks a packet with ECN.
   *
   * @param port   Port number
   * @param queue  Queue number
   */
  void ecnMark(uint16_t port, uint8_t queue);

  /**
   * Builds the internal buffer tracker from configuration.
   *
   * @param cfg         Buffer configuration
   * @param useIngress  Whether to use ingress buffer
   * @throw             folly::ConversionError if a port number does not fit
   *                    `uint16_t` or a port has more queues than `uint8_t` can
   *                    count
   */
  void buildBufferTracker(
      const packsim::thrift::SwitchBufferConfig& cfg,
      bool useIngress);

  // Serializes current buffer state.
  packsim::switches::thrift::Buffer serializeState() const {
    return bufTracker_.serializeState();
  }

  // Callback arguments: port number, queue number, new PFC state.
  using OnPfcStateChangedCallback =
      std::function<void(uint16_t, uint8_t, PfcState)>;

  // Sets the callback for PFC state changes.
  void setOnPfcStateChangedCallback(const OnPfcStateChangedCallback& cb) {
    onPfcStateChanged_ = cb;
  }

  // Returns list of configured port numbers
  std::vector<uint16_t> getPortNums() const {
    return bufTracker_.getPortNums();
  }

  // Checks if a port is configured
  FOLLY_ALWAYS_INLINE bool hasPort(uint16_t portNum) const noexcept {
    return ports_.contains(portNum);
  }

  // Test access
  friend class DynShareTests;

  // Accessors for test inspection
  FOLLY_ALWAYS_INLINE SwitchStats& bufferStats() noexcept {
    return *bufTracker_.bufferStats_;
  }

  FOLLY_ALWAYS_INLINE PortTracker& portTracker(uint16_t portNum) const {
    return bufTracker_.portTracker(portNum);
  }

  FOLLY_ALWAYS_INLINE SwitchStats& portStats(uint16_t portNum) const {
    return bufTracker_.portStats(portNum);
  }

  FOLLY_ALWAYS_INLINE QueueTracker& queueTracker(
      uint16_t portNum,
      uint8_t queueNum) const {
    return bufTracker_.queueTracker(portNum, queueNum);
  }

  FOLLY_ALWAYS_INLINE SwitchStats& queueStats(
      uint16_t portNum,
      uint8_t queueNum) const {
    return bufTracker_.queueStats(portNum, queueNum);
  }

 private:
  // Simulation context
  SimContext* const simCtx_;

  // Buffer tracking state
  BufferTracker bufTracker_;

  // Callback invoked on PFC state changes
  // Raise the PFC-state-changed event.
  void onPfcStateChanged(uint16_t portNum, uint8_t queueNum, PfcState state) {
    XCHECK(onPfcStateChanged_)
        << "no pfc-state-changed subscriber registered on switch buffer";
    onPfcStateChanged_(portNum, queueNum, state);
  }

  OnPfcStateChangedCallback onPfcStateChanged_;

  // Set of configured port numbers
  folly::F14FastSet<uint16_t> ports_;
};

/** Inline Impls **/
bool SimpleSwitchBuffer::admitPacket(
    uint16_t portNum,
    uint8_t queueNum,
    uint32_t pktBytes,
    AdmitOptions opts) {
  auto& q = queueTracker(portNum, queueNum);
  if (!q.tryAddPacket(pktBytes, opts)) {
    q.droppedPacket(pktBytes);
    return false;
  }
  if (q.pfc) {
    const auto oldPfc = q.pfcState;
    q.updatePfc();
    const auto newPfc = q.pfcState;
    if (oldPfc != newPfc) {
      onPfcStateChanged(portNum, queueNum, newPfc);
    }
  }
  return true;
}

uint16_t SimpleSwitchBuffer::removePacket(
    uint16_t portNum,
    uint8_t queueNum,
    uint32_t pktBytes,
    uint16_t heldCredits) {
  auto& q = queueTracker(portNum, queueNum);
  const uint16_t releasedHdrmCells = q.removePacket(pktBytes, heldCredits);
  if (q.pfc) {
    const auto oldPfc = q.pfcState;
    q.updatePfc();
    const auto newPfc = q.pfcState;
    if (oldPfc != newPfc) {
      onPfcStateChanged(portNum, queueNum, newPfc);
    }
  }
  return releasedHdrmCells;
}
} // namespace packsim
