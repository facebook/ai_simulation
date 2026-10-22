/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstdint>

namespace packsim {

// Priority classes in a PFC frame's class-enable vector (IEEE 802.1Qbb): the
// vector is one octet, one bit per class, each with its own pause-quanta field.
constexpr size_t kNumPfcClasses = 8;

/**
 * Priority Flow Control (PFC) state.
 *
 * Represents whether flow control is active (XOFF) or inactive (XON).
 */
enum class PfcState : uint8_t {
  // Flow control active - stop sending.
  XOFF = 0,
  // Flow control inactive - resume sending.
  XON = 1
};

} // namespace packsim
