/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <string_view>

#include "packsim/link-layer/cbfc/cbfc-types.h"

namespace packsim::cbfc {

/**
 * Init-time argument checks shared by the sender ledger and the receiver
 * tracker, so both enforce the same bound and report it the same way.
 */
struct CbfcValidate {
  /**
   * Reject a VC count outside 1..`kUecMaxVcs`, the capacity every per-VC
   * structure is sized for. A link config's own limit is `kCbfcLimits.maxVcs`,
   * checked where the config is validated.
   *
   * @param numVcs The configured VC count.
   * @throw std::invalid_argument if it is 0 or above `kUecMaxVcs`.
   */
  static void validateNumVcs(uint8_t numVcs);

  /**
   * `validateNumVcs` in expression form, for use in a member initializer.
   *
   * @param numVcs The configured VC count.
   * @return The same value, once it is known good.
   * @throw std::invalid_argument if it is 0 or above `kUecMaxVcs`.
   */
  [[nodiscard]] static uint8_t checkedNumVcs(uint8_t numVcs);

  /**
   * Reject a VC id that is not addressable on a port carrying `numVcs`
   * channels.
   *
   * @param vc      The VC id to check.
   * @param numVcs  The number of VCs configured on the port.
   * @param source  Config field the id came from, for example "vcMap", named
   *                in the message. Omit when the id is a plain call argument
   *                rather than a config value.
   * @throw std::invalid_argument if `vc` is at or above `numVcs`.
   */
  static void
  validateVc(uint8_t vc, uint8_t numVcs, std::string_view source = {});
};

} // namespace packsim::cbfc
