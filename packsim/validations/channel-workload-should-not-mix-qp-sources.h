/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <optional>

#include "packsim/validations/validation.h"

namespace packsim::details {

class ChannelWorkloadShouldNotMixQpSources : public Validation {
 public:
  ChannelWorkloadShouldNotMixQpSources()
      : Validation(
            "ChannelWorkloadShouldNotMixQpSources",
            "A workload with channelSettings should not number its notify QPs apart from its channel QPs.") {
  }

  /**
   * Flag every top-level workload with channelSettings whose notify QPs would
   * be numbered apart from its channel QPs. Channel QPs come from qpnPairMap,
   * while a notify takes the last QP of a rank pair's first NIC pair in
   * nicQpnPairMap, which is numbered separately unless it is built from
   * qpnPairMap. A channel QP can then be a notify QP, and channel data is
   * matched against the notify receives.
   *
   * @param simCfg          The simulation configuration to validate
   * @param ignoreWarnings  Whether to ignore warnings during validation
   * @return                Validation result if validation issues are found
   */
  std::optional<ValidationResult> validate(
      thrift::Simulation const& simCfg,
      bool const ignoreWarnings) const final;
};
} // namespace packsim::details
