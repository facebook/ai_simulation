/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

namespace packsim::cbfc {

/**
 * The capability limits a CBFC link config is validated against: how many
 * VCs and VC groups it may use, which PktOvhd values it may program, and the
 * credit size used when neither the config nor a receiver sets one.
 * `kCbfcLimits` (cbfc-types.h) is the set the simulator enforces.
 *
 * Example usage:
 *
 *      if (!kCbfcLimits.isPktOvhdSupported(pktOvhdBytes)) { ... }
 */
struct CbfcLimits {
  // Most VCs a link may configure.
  uint8_t maxVcs;
  // Most VC groups a link may configure.
  uint8_t maxVcGroups;
  // Most VCs one VC group may hold.
  uint8_t maxVcsPerGroup;
  // Accepted PktOvhd values: min..max in steps of the step, in bytes.
  int32_t minPktOvhdBytes;
  int32_t maxPktOvhdBytes;
  int32_t pktOvhdStepBytes;
  // Bytes per credit when the config leaves `expectedCreditSizeBytes == 0`
  // and no receiver on the link quantizes its buffer.
  uint32_t defaultCreditSizeBytes;

  // Whether `pktOvhdBytes` is one of the accepted PktOvhd values.
  [[nodiscard]] constexpr bool isPktOvhdSupported(
      const int32_t pktOvhdBytes) const noexcept {
    return pktOvhdBytes >= minPktOvhdBytes && pktOvhdBytes <= maxPktOvhdBytes &&
        (pktOvhdBytes - minPktOvhdBytes) % pktOvhdStepBytes == 0;
  }

  friend constexpr bool operator==(const CbfcLimits&, const CbfcLimits&) =
      default;

  /**
   * UEC 1.0.3 §5.2 as PackSim models it.
   *
   * - `maxVcs` 32: §5.2.5.2, 1 <= NumVCs <= 32.
   * - `maxVcGroups`, `maxVcsPerGroup` 32: UEC has no VC groups (they are a
   *   PackSim extension), and a VC is in at most one group, so neither can
   *   usefully exceed `maxVcs`.
   * - PktOvhd 0..+127 in 1 B steps: §5.2.10.2 allows -16..+127 and requires
   *   the programmed value to be at least the receiver's actual overhead.
   *   PackSim's receive buffers store the whole frame, CRC included, so the
   *   negative half would return more credit at dequeue than the packet was
   *   charged, and is left out.
   * - `defaultCreditSizeBytes` 64: in §5.2.10.1's must-support set
   *   {32, 64, ..., 2048}, and the minimum Ethernet frame, so a minimum frame
   *   costs one credit.
   *
   * @return  The UEC limits.
   */
  [[nodiscard]] static constexpr CbfcLimits uec() noexcept {
    return {
        .maxVcs = 32,
        .maxVcGroups = 32,
        .maxVcsPerGroup = 32,
        .minPktOvhdBytes = 0,
        .maxPktOvhdBytes = 127,
        .pktOvhdStepBytes = 1,
        .defaultCreditSizeBytes = 64,
    };
  }
};

} // namespace packsim::cbfc
