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

class NicConfigShouldExist : public Validation {
 public:
  NicConfigShouldExist();

  /**
   * For all the ACCEL nodes in the simulation, test that the node's endpoints
   * reference NICs that are present in the nicConfigMap. When that map is
   * empty, the default NIC config key (kDefaultNicConfig) is accepted:
   * Simulation.nic supplies it when the simulation is built.
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
