/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/vc-mapper.h"

#include <stdexcept>

#include <fmt/format.h>

#include "packsim/link-layer/cbfc/cbfc-validate.h"

namespace packsim::cbfc {

VcMapper::VcMapper(const CbfcConfig& config)
    : rules_(config.vcMap), losslessVc_(config.losslessVc) {
  // `isLossless` reports best-effort for any id past the end of `losslessVc_`,
  // so an out-of-range `vcId` or a short `losslessVc` must fail here rather
  // than silently send lossless traffic ungated.
  CbfcValidate::validateNumVcs(config.numVcs);
  if (losslessVc_.size() != config.numVcs) {
    throw std::invalid_argument(
        fmt::format(
            "CBFC losslessVc has {} entries but the port has {} VCs; every VC "
            "needs a lossless flag.",
            losslessVc_.size(),
            int{config.numVcs}));
  }
  for (const auto& rule : rules_) {
    CbfcValidate::validateVc(rule.vcId, config.numVcs, "vcMap");
  }
}

std::optional<uint8_t> VcMapper::vcFor(uint8_t dscp, uint8_t pcpDei)
    const noexcept {
  for (const auto& rule : rules_) {
    const uint32_t field = (rule.selector == VcSelector::DSCP) ? dscp : pcpDei;
    if ((field & rule.mask) == rule.value) {
      return rule.vcId;
    }
  }
  return std::nullopt;
}

bool VcMapper::isLossless(uint8_t vc) const noexcept {
  return vc < losslessVc_.size() && losslessVc_[vc];
}

} // namespace packsim::cbfc
