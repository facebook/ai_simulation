/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>

#include "packsim/link-layer/port-observer.h"

namespace packsim {

/**
 * An `IPortObserver` that forwards each event to a lambda.
 *
 * For tests. Production code implements the interface directly, because the
 * class that owns a port already has the handlers; a test usually wants one
 * throwaway lambda over local state and no new type.
 *
 * Each event has its own setter, so a fixture subscribes only to the events
 * its case asserts on and can replace one of them without disturbing the
 * others. An unsubscribed event is ignored -- deliberately, and unlike
 * production raisers, which assert. There is no owner here whose wiring could
 * be wrong, so none of the four raisers below repeats this.
 *
 * Example usage:
 *
 *      LambdaPortObserver observer;
 *      observer.setOnPacketRcvdCallback(
 *          [&](EthernetPort&, const PacketPtr& p) {
 *            got.push_back(p);
 *          });
 *      ethPort->setObserver(&observer);
 *
 * NOTE: Must outlive the port it is installed on. Declare it before the
 * port in a fixture so it is destroyed after.
 */
class LambdaPortObserver : public IPortObserver {
 public:
  // One alias per event even where two share a signature, so a member or
  // parameter declares which event it carries.
  using OnPacketRcvdCallback =
      std::function<void(EthernetPort&, const PacketPtr&)>;
  using OnPacketSentCallback =
      std::function<void(EthernetPort&, const PacketPtr&)>;
  using OnPacketDroppedOnSendCallback = std::function<void(
      EthernetPort&,
      const PacketPtr&,
      uint8_t,
      packsim::switches::thrift::PacketDropReason)>;
  using OnPacketDroppedOnReceiveCallback = std::function<void(
      EthernetPort&,
      const PacketPtr&,
      uint8_t,
      packsim::switches::thrift::PacketDropReason)>;

  void setOnPacketRcvdCallback(OnPacketRcvdCallback cb) {
    onPacketRcvd_ = std::move(cb);
  }

  void setOnPacketSentCallback(OnPacketSentCallback cb) {
    onPacketSent_ = std::move(cb);
  }

  void setOnPacketDroppedOnSendCallback(OnPacketDroppedOnSendCallback cb) {
    onPacketDroppedOnSend_ = std::move(cb);
  }

  void setOnPacketDroppedOnReceiveCallback(
      OnPacketDroppedOnReceiveCallback cb) {
    onPacketDroppedOnReceive_ = std::move(cb);
  }

  void onPacketRcvd(EthernetPort& ethPort, const PacketPtr& pkt) override {
    if (onPacketRcvd_) {
      onPacketRcvd_(ethPort, pkt);
    }
  }

  void onPacketSent(EthernetPort& ethPort, const PacketPtr& pkt) override {
    if (onPacketSent_) {
      onPacketSent_(ethPort, pkt);
    }
  }

  void onPacketDroppedOnSend(
      EthernetPort& ethPort,
      const PacketPtr& pkt,
      uint8_t cos,
      packsim::switches::thrift::PacketDropReason reason) override {
    if (onPacketDroppedOnSend_) {
      onPacketDroppedOnSend_(ethPort, pkt, cos, reason);
    }
  }

  void onPacketDroppedOnReceive(
      EthernetPort& ethPort,
      const PacketPtr& pkt,
      uint8_t cos,
      packsim::switches::thrift::PacketDropReason reason) override {
    if (onPacketDroppedOnReceive_) {
      onPacketDroppedOnReceive_(ethPort, pkt, cos, reason);
    }
  }

 private:
  OnPacketRcvdCallback onPacketRcvd_;
  OnPacketSentCallback onPacketSent_;
  OnPacketDroppedOnSendCallback onPacketDroppedOnSend_;
  OnPacketDroppedOnReceiveCallback onPacketDroppedOnReceive_;
};

} // namespace packsim
