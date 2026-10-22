/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "packsim/link-layer/cbfc/cbfc-types.h"

namespace packsim::cbfc {

/**
 * Maps a packet's header fields to a CBFC virtual channel (UEC 1.0.3 §5.2.5.2;
 * R_PktID_Sel §5.3.2.2.15.4). Rules from `CbfcConfig::vcMap` are evaluated in
 * order; the first rule whose `(field & mask) == value` assigns the VC. A
 * packet matching no rule has no VC and is treated as best-effort (not
 * credit-gated).
 */
class VcMapper {
 public:
  /**
   * Build a mapper from a link's CBFC config.
   *
   * Copies `config.vcMap` and `config.losslessVc`; retains no reference to
   * `config`, so later edits to it are not seen.
   *
   * @param config  Link config to read the rules and lossless flags from.
   *
   * @throws std::invalid_argument  `config.losslessVc` does not hold exactly
   *                                one flag per VC, or a rule names a `vcId`
   *                                at or past `config.numVcs`.
   */
  explicit VcMapper(const CbfcConfig& config);

  /**
   * Resolve the VC for a packet's selector fields.
   *
   * @param dscp     6-bit L3 DSCP value (`ip.dscp`).
   * @param pcpDei   4-bit L2 PCP/DEI value (`mac.vlan.pcp_dei`).
   *
   * @return         The matched VC id, or `nullopt` for best-effort.
   */
  [[nodiscard]] std::optional<uint8_t> vcFor(uint8_t dscp, uint8_t pcpDei)
      const noexcept;

  /**
   * Whether VC `vc` is configured lossless (credit-gated). Ids at or past the
   * end of the copied `CbfcConfig::losslessVc` read as best-effort rather than
   * failing; the constructor rejects a config with fewer flags than VCs, so an
   * id below `numVcs` never takes that path.
   *
   * @param vc   VC id to query.
   *
   * @return     True iff the VC is lossless.
   */
  [[nodiscard]] bool isLossless(uint8_t vc) const noexcept;

 private:
  // `CbfcConfig::vcMap`. Evaluation order is significant: first match wins.
  std::vector<VcMapRule> rules_;
  // `CbfcConfig::losslessVc`, indexed by VC id.
  std::vector<bool> losslessVc_;
};

} // namespace packsim::cbfc
