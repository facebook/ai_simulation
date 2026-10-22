/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

#include "packsim/if/gen-cpp2/switches_types.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

class EthernetPort;

/**
 * What an `EthernetPort` tells the layer above it about each frame.
 *
 * One observer per port, installed with `EthernetPort::setObserver`.
 * The observer must outlive the port.
 *
 * `onPacketRcvd` is pure virtual because it carries the data path: a port
 * with no way to deliver a frame upward is a wiring error, not a configuration.
 * The port delivers through `onPacketRcvdByMove`, which reports to
 * `onPacketRcvd` unless an observer that keeps the frame overrides it. The
 * other three default to doing nothing, which is what "optional diagnostic"
 * should look like in the type rather than in a convention.
 *
 * Example usage:
 *
 *      class FbSwitch : public FlexNode, public IPortObserver {
 *        void onPacketRcvd(EthernetPort&, const PacketPtr&) override;
 *      };
 *      ethPort->setObserver(this);
 *
 * NOTE: Called synchronously from the port's own event callbacks, so an
 * implementation must not re-enter the port's transmit path.
 */
class IPortObserver {
 public:
  virtual ~IPortObserver() = default;

  /**
   * A frame arrived and is ready for the layer above.
   *
   * @param ethPort  The receiving port.
   * @param pkt      The received frame.
   */
  virtual void onPacketRcvd(EthernetPort& ethPort, const PacketPtr& pkt) = 0;

  /**
   * The same event as `onPacketRcvd`, with the port's handle to the frame
   * handed over. An observer that keeps the frame past the call, such as a
   * switch forwarding it, overrides this to move the handle instead of copying
   * it: a copy is an atomic reference-count update on every hop. The default
   * reports the frame through `onPacketRcvd`.
   *
   * @param ethPort  The receiving port.
   * @param pkt      The received frame, which the observer may move from.
   */
  virtual void onPacketRcvdByMove(EthernetPort& ethPort, PacketPtr&& pkt) {
    onPacketRcvd(ethPort, pkt);
  }

  /**
   * A frame finished transmitting. Not called for an LLR retransmit, which
   * already reported on its first transmit, nor for a frame this port's own
   * MAC generated.
   *
   * @param ethPort  The transmitting port.
   * @param pkt      The transmitted frame.
   */
  virtual void onPacketSent(
      EthernetPort& /* ethPort */,
      const PacketPtr& /* pkt */) {}

  /**
   * A frame was dropped on the transmit path.
   *
   * @param ethPort  The port that dropped it.
   * @param pkt      The dropped frame.
   * @param cos      The queue charged for the drop.
   * @param reason   Why it was dropped.
   */
  virtual void onPacketDroppedOnSend(
      EthernetPort& /* ethPort */,
      const PacketPtr& /* pkt */,
      uint8_t /* cos */,
      packsim::switches::thrift::PacketDropReason /* reason */) {}

  /**
   * A frame was dropped on the receive path.
   *
   * @param ethPort  The port that dropped it.
   * @param pkt      The dropped frame.
   * @param cos      The queue charged for the drop.
   * @param reason   Why it was dropped.
   */
  virtual void onPacketDroppedOnReceive(
      EthernetPort& /* ethPort */,
      const PacketPtr& /* pkt */,
      uint8_t /* cos */,
      packsim::switches::thrift::PacketDropReason /* reason */) {}
};

} // namespace packsim
