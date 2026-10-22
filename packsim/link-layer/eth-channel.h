/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <cmath>
#include <cstdint>

#include <folly/CppAttributes.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/time.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

enum class ChannelState {
  DOWN,
  UP,
};

class EthernetPort;

/**
 * The cable between two EthernetPorts: the propagation delay a frame
 * takes to reach the far end, the bit error rate the far end applies to it,
 * and an optional link flap.
 *
 * The channel holds no transmit state. The sending port serializes a frame
 * itself, and the channel only schedules the receive on the other attached
 * port once the propagation delay has passed. Both directions share one
 * delay config: a fixed `delayNs` plus, when `expDelayNs` is set, an
 * exponentially distributed extra delay drawn per frame.
 */
class EthernetChannel {
 public:
  explicit EthernetChannel(
      const packsim::thrift::FlexLinkDelayConfig& delayCfg,
      uint64_t flapDelayNs,
      double log10Ber);

  explicit EthernetChannel(uint64_t delayNs);

  /**
   * Connect `port` as the next free end of the channel. When the channel was
   * built with a nonzero `flapDelayNs`, this also schedules `port`'s link to
   * go down that long after the call, in `port`'s own context.
   *
   * @param port      The port to connect; the first call fills end 0, the
   *                  second end 1.
   *
   * @throw std::runtime_error if both ends are already connected.
   */
  void attach(EthernetPort* port);

  /**
   * Begin local wire serialization. Samples the propagation delay at
   * serialization start (one RNG (random number generator) draw when
   * `expDelayNs` is nonzero, none otherwise), emits the `TRACE-TX` log line,
   * and returns the sampled `propDelay` for the caller to hold and pass to
   * `scheduleRemoteReceive` once serialization actually finishes.
   *
   * Does NOT schedule a remote-receive event. Splice safety: because no event
   * is scheduled here, a PCS (Physical Coding Sublayer) splice (CtlOS
   * interrupting an in-flight TX) simply delays the eventual
   * `scheduleRemoteReceive` call and never needs a cancel + reschedule.
   *
   * `[[nodiscard]]`: the returned `propDelay` MUST be held by the caller
   * and passed to a matching `scheduleRemoteReceive` once serialization
   * completes. Dropping it silently desynchronises the wire timeline —
   * the trace and RNG draw happen at `startSerialize` time, but no
   * remote-receive event is ever scheduled.
   *
   * @param p    Packet whose serialization is starting; used only for
   *             the TRACE-TX log line (not stored).
   * @param src  Local source port; identifies the sender in the
   *             TRACE-TX line, and the RNG draw uses
   *             `src->getSimCtx()->random`.
   * @return     The propagation delay sampled for this packet — the
   *             caller MUST pass it to a matching
   *             `scheduleRemoteReceive` once serialization completes.
   */
  [[nodiscard]] Time startSerialize(
      const PacketPtr& p,
      EthernetPort* FOLLY_NONNULL src) const;

  /**
   * Schedule the remote-receive event for `p` at `now + delayUntilReceive`.
   * Resolves the destination port from `src` (the channel has exactly two
   * attached ports).
   *
   * `delayUntilReceive` is the FULL remaining time until the receiver
   * sees the last bit of `p`. `LinkLayerScheduler` calls this once the
   * wire-time stopwatch expires and passes the `propDelay` returned by the
   * matching `startSerialize`: `now` has already advanced by
   * `txCompleteTime`, so `now + propDelay` is the receiver arrival time.
   *
   * Returns the handle of the receive event, which belongs to the receiving
   * port's context: like any `Scheduler::EventHandle`, use it only from that
   * context or while the engine is not running. A link flap or BER change
   * needs no cancel: the receiving port applies link state and bit errors
   * when the frame arrives. Caller is responsible for not invoking this twice
   * for the same `startSerialize` call — the channel keeps no state to detect
   * that.
   *
   * @param p                 Packet whose receive event will fire on
   *                          the remote port.
   * @param src               Local source port; the channel infers
   *                          the destination as the other attached
   *                          port.
   * @param delayUntilReceive Remaining sim-time until the receiver
   *                          should see the last bit of `p`.
   * @return                  Handle of the scheduled receive, owned by the
   *                          receiving port's context.
   */
  Scheduler::EventHandle scheduleRemoteReceive(
      const PacketPtr& p,
      EthernetPort* FOLLY_NONNULL src,
      Time delayUntilReceive) const;

  Scheduler::EventHandle sendPacket(
      const PacketPtr& p,
      EthernetPort* FOLLY_NONNULL dst,
      Time pktDelay) const;

  /**
   * The port attached at the other end of this channel from `self`.
   *
   * @param self    A port attached to one end of this channel.
   *
   * @return        The port at the other end; never null.
   *
   * @throw std::runtime_error if `self` is not attached to this channel, or
   *        if nothing other than `self` is attached at the other end.
   */
  [[nodiscard]] EthernetPort* FOLLY_NONNULL
  getPeer(const EthernetPort& self) const;

  /**
   * Get the Bit Error Rate (BER) for this channel.
   *
   * @return The BER value: below 1.0 when bit errors are on; >= 1.0
   *         (log10 BER >= 0) disables them
   */
  double getBer() const {
    return ber_;
  }

  // Get the log10 BER for this channel (e.g. -6.0 means BER = 1e-6).
  double getLog10Ber() const {
    return log10Ber_;
  }

  // Set the log10 BER (e.g. -6.0 for 1e-6). Both attached ports read it as
  // frames arrive, each from its own context, so call this only while the
  // engine is not running; to change a BER mid-run, call
  // `EthernetPort::setRxLog10BerOverride` on each receiving port, from that
  // port's own context.
  void setBer(double log10Ber) {
    log10Ber_ = log10Ber;
    ber_ = std::pow(10, log10Ber);
  }

 protected:
  // Bit errors are disabled when this is >= 0 (i.e. BER >= 1)
  double log10Ber_ = 1;
  double ber_ = 10;
  uint64_t flapDelayNs_{0};

  // A point-to-point channel has exactly two ends.
  static constexpr uint8_t kNumEnds = 2;

  // Propagation delay config
  packsim::thrift::FlexLinkDelayConfig delayCfg_;
  uint8_t numAttached_{0};
  // The port at end 0 and end 1, in attach order; null until attached.
  std::array<EthernetPort*, kNumEnds> ends_{nullptr, nullptr};
};

} // namespace packsim
