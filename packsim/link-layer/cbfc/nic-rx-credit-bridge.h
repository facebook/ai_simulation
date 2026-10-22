/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/CPortability.h>
#include <folly/logging/xlog.h>

#include "packsim/link-layer/cbfc/cbfc.h"
#include "packsim/link-layer/cbfc/credit-observer.h"
#include "packsim/packet/base.h"

namespace packsim::cbfc {

/**
 * Holds an endpoint's CBFC receive credit for as long as the packet occupies
 * NIC memory (UEC 1.0.3 §5.2.5.7.2).
 *
 * `Endpoint::onPacketRcvd` counts `R_VC_CC` on arrival and frees the credit on
 * scope exit. That is correct only for packets nothing buffers (ACK, NACK,
 * CNP, PING, duplicates, RNR'd frames, any transport with no receive buffer).
 * For a packet that enters a `NicRxBuffer`, freeing on scope exit would tell
 * the upstream switch the buffer is empty the instant a frame lands in it,
 * making the lossless invariant vacuously true.
 *
 * The endpoint `arm`s a packet before dispatch; a buffer that stores it claims
 * it, which cancels the immediate free, and the credit is returned when the
 * buffer actually drains. Required call sequence:
 *
 *      cbfc->onPacketStored(*pkt);   // R_VC_CC, unconditional on arrival
 *      bridge->arm(*pkt);
 *      SCOPE_EXIT {
 *        if (!bridge->disarmAndWasClaimed()) {
 *          cbfc->onPacketDrained(*pkt); // nothing buffered it
 *        }
 *      };
 *      transport->onPacketRcvd(pkt, &ethPort);
 *
 * Lifetime is NOT "alive exactly while CBFC is enabled": the buffers this is
 * installed on are owned by the transport and hold a RAW pointer that nothing
 * on the port side can clear, so the bridge is created on the first
 * `enableCbfc`, `detach`ed but kept alive on `disableCbfc`, `reattach`ed on a
 * later enable, and destroyed only with the owning port.
 *
 * NOTE: Not thread-safe and not re-entrant; it lives on one PDES context.
 */
class NicRxCreditBridge : public ICreditObserver {
 public:
  /**
   * @param cbfc  The receiving link's CBFC module. It must outlive its
   *              attachment: `disableCbfc` calls `detach()` before it destroys
   *              the module, and a later enable calls `reattach()`.
   */
  explicit NicRxCreditBridge(Cbfc* FOLLY_NONNULL cbfc) : cbfc_(cbfc) {}

  /**
   * Claim `pkt` if it is the one currently armed, cancelling the endpoint's
   * immediate free.
   */
  void onPacketStored(const Packet& pkt) override {
    if (pending_ == &pkt) {
      claimed_ = true;
    }
  }

  /**
   * Return the credit for a packet leaving the buffer.
   *
   * Frees unconditionally rather than only for packets this bridge claimed.
   * The only way into the buffer is `Endpoint::onPacketRcvd` ->
   * `RoceSocketBase::onPacketRcvd` -> `writePacketToMemory` ->
   * `queuePacketToMemory`, and the endpoint arms before that dispatch, so
   * every packet here was armed and counted. Filtering to claimed packets
   * would leak a credit for any packet discarded without draining, such as
   * teardown with the buffer non-empty.
   */
  void onPacketDrained(const Packet& pkt) override {
    if (cbfc_ == nullptr) {
      return; // detached; the module it reported to is gone
    }
    cbfc_->onPacketDrained(pkt);
  }

  /**
   * Make this bridge inert without destroying it. Credits held for packets
   * still in the buffer are not returned; the module that counted them is
   * about to be destroyed.
   *
   * LIMITATION: packets still buffered at detach time will, after a
   * `reattach`, drain into a module that never counted them, advancing
   * `R_VC_CF` past `R_VC_CC` (`CbfcCreditTracker::onDrained` does not clamp).
   * Safe only because `enableCbfc` runs once per link before any traffic and
   * `disableCbfc` only from port teardown; making CBFC runtime-toggleable
   * requires fixing this first.
   */
  void detach() noexcept {
    cbfc_ = nullptr;
    pending_ = nullptr;
    claimed_ = false;
  }

  /**
   * Point a detached bridge at a new module, for a CBFC re-enable. A re-enable
   * must revive THIS object rather than construct a replacement, because a
   * `NicRxBuffer` still holds a raw pointer to it. Shares the limitation
   * documented on `detach`.
   *
   * @param cbfc  The module to report subsequent drains to.
   */
  void reattach(Cbfc* FOLLY_NONNULL cbfc) noexcept {
    cbfc_ = cbfc;
    pending_ = nullptr;
    claimed_ = false;
  }

  /**
   * Offer `pkt` to whatever runs next. Call immediately before transport
   * dispatch; pair with `disarmAndWasClaimed`.
   *
   * NOTE: arming over an already-armed packet aborts via `XCHECK`; it does
   * not throw.
   */
  void arm(const Packet& pkt) noexcept {
    // Non-reentrancy is an invariant: an inner arm/disarm pair would clear
    // `claimed_` behind the outer frame, which would then free a credit the
    // drain has already returned.
    XCHECK(pending_ == nullptr) << "NicRxCreditBridge::arm is not re-entrant";
    pending_ = &pkt;
    claimed_ = false;
  }

  /**
   * Disarm, reporting whether a buffer took ownership.
   *
   * @return  True if a buffer stored the armed packet, meaning the caller must
   *          NOT free the credit -- the drain will.
   */
  [[nodiscard]] bool disarmAndWasClaimed() noexcept {
    pending_ = nullptr;
    const bool wasClaimed = claimed_;
    claimed_ = false;
    return wasClaimed;
  }

 private:
  // Drains are reported here. Not owned; null while detached. Must outlive
  // the attachment; see the constructor.
  Cbfc* FOLLY_NULLABLE cbfc_;
  // Compared by address only, never dereferenced, and cleared on every disarm,
  // so a recycled packet can at worst fail to match -- which falls back to the
  // endpoint's immediate free.
  const Packet* pending_{nullptr};
  // Set once a buffer has stored `pending_`: the drain, not the endpoint, owes
  // the credit. Meaningful only between `arm` and `disarmAndWasClaimed`.
  bool claimed_{false};
};

} // namespace packsim::cbfc
