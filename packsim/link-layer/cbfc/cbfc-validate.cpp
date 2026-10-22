/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/cbfc-validate.h"

#include <stdexcept>

#include <fmt/format.h>

namespace packsim::cbfc {

uint8_t CbfcValidate::checkedNumVcs(const uint8_t numVcs) {
  CbfcValidate::validateNumVcs(numVcs);
  return numVcs;
}

void CbfcValidate::validateNumVcs(uint8_t numVcs) {
  if (numVcs == 0 || numVcs > kUecMaxVcs) {
    throw std::invalid_argument(
        fmt::format(
            "CBFC numVcs {} is outside 1..{} (UEC 1.0.3 §5.2.5.2).",
            int{numVcs},
            int{kUecMaxVcs}));
  }
}

void CbfcValidate::validateVc(
    uint8_t vc,
    uint8_t numVcs,
    std::string_view source) {
  if (vc < numVcs) {
    return;
  }
  throw std::invalid_argument(
      source.empty()
          ? fmt::format(
                "CBFC VC {} is outside 0..{}.", int{vc}, int{numVcs} - 1)
          : fmt::format(
                "CBFC {} names VC {}, outside 0..{}.",
                source,
                int{vc},
                int{numVcs} - 1));
}

} // namespace packsim::cbfc
