/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <bitset>
#include <cstdint>
#include <vector>

#include "packsim/kernel/scheduler.h"
#include "packsim/packet/pfc.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

class EthernetPort;

/**
 * Priority Flow Control (IEEE 802.1Qbb) for one port.
 *
 * Owns the port's outbound PFC state and both PFC timers. It does NOT own the
 * per-class pause state itself: that lives in `Queues::CosQueue`, because the
 * queue scheduler consults it to decide eligibility. This class reads and
 * writes it through the port, which is why it holds a port reference
 * rather than a set of injected dependencies -- `Queues` is replaced whenever
 * the port is reconfigured, so a stored pointer would dangle.
 *
 * Example usage:
 *
 *      pfc.setRxState(cos, PfcState::XOFF);   // local buffer filled up
 *      if (pfc.hasPendingFrame()) {
 *        transmit(pfc.buildPendingFrame());
 *      }
 *
 * NOTE: Not thread-safe. Called only from the simulation's event callbacks for
 * its own port.
 */
class PfcController {
 public:
  /**
   * @param port  The port whose queues this controller pauses and resumes; it
   *              owns the controller.
   */
  explicit PfcController(EthernetPort& port);

  /**
   * Resize the per-class timer tables after the port's queue count changes.
   * Discards any armed timer, so it runs only from port reconfiguration.
   *
   * @param numQueues  The port's new CoS queue count.
   */
  void resize(uint8_t numQueues);

  /**
   * Apply an incoming PFC frame: count it per class and pause or resume this
   * port's transmit accordingly.
   *
   * @param pkt  The received frame.
   */
  void receiveFrame(const PacketPtr& pkt);

  /**
   * Bring this port's transmit state for one class into line with what the
   * peer's PFC frame asks for, pausing or resuming accordingly.
   *
   * @param qId     The class to update.
   * @param quanta  Pause duration in 512-bit-time quanta; 0 means resume.
   */
  void updateTxStateFromPeer(uint8_t qId, uint16_t quanta);

  // Resume transmit for `qId` once the peer's pause duration has elapsed.
  void resumeTx(uint8_t qId);

  /**
   * Record that this port's own receive buffer crossed a threshold and mark
   * the class for the next outbound frame. PFC is link-local, so nothing
   * above the MAC is told.
   *
   * @param qId    The class whose local receive state changed.
   * @param state  XOFF to ask the peer to pause, XON to release it.
   */
  void setRxState(uint8_t qId, PfcState state);

  /**
   * Record `state` for `qId` in the port's outbound register, overwriting
   * whatever that class held, and arm or cancel its refresh timer.
   *
   * @param qId    The class to mark.
   * @param state  The state to report to the peer.
   */
  void markState(uint8_t qId, PfcState state);

  // Re-assert an existing pause before the peer's pause duration expires.
  void refreshRxPause(uint8_t qId);

  // Whether any class has state the peer has not been told about yet.
  [[nodiscard]] bool hasPendingFrame() const noexcept {
    return pending_.dirty;
  }

  /**
   * Materialise one frame carrying every class marked since the last
   * transmission, bump those classes' XON/XOFF counters, and clear the
   * register.
   *
   * @return  The frame to transmit. Only call when `hasPendingFrame()`.
   */
  [[nodiscard]] PacketPtr buildPendingFrame();

  /**
   * Discard all PFC state after the link goes down: drop the pending frame,
   * cancel both timer sets, and clear every outstanding pause. The link-down
   * breaks the PFC contract, so an outstanding XOFF must not survive it.
   */
  void onLinkDown();

  PfcState getRxState(uint8_t qId) const;
  PfcState getTxState(uint8_t qId) const;

 private:
  /**
   * The port's single outbound PFC state, per IEEE 802.1Qbb.
   *
   * A PFC frame carries the pause state of every class at once, so the MAC
   * keeps one state register rather than a queue of frames: a new threshold
   * event overwrites its own class in place, and the next frame on the wire
   * reports the latest state of all classes together.
   *
   * `classEnable` is cleared after each transmission. A clear bit means "this
   * class is unchanged", so a class stays paused on the peer until either the
   * refresh timer re-marks it or an XON does.
   */
  struct Pending {
    std::bitset<kNumPfcClasses> classEnable;
    std::array<uint16_t, kNumPfcClasses> quanta{};
    bool dirty{false};
  };

  // The owning port, whose queues are paused and resumed.
  EthernetPort& port_;
  Pending pending_;
  // Fires when a peer-requested pause expires and transmit may resume.
  std::vector<Scheduler::EventHandle> resumeEvents_;
  // Fires when this port's own pause request must be re-asserted.
  std::vector<Scheduler::EventHandle> refreshEvents_;
  // Pause quanta advertised in an outgoing XOFF, and how long to wait before
  // refreshing it. The refresh is deliberately shorter than the pause it
  // renews, so the peer is never briefly unpaused between frames.
  uint16_t txQuanta_{65535};
  uint32_t refreshQuanta_{26214};
};

} // namespace packsim
