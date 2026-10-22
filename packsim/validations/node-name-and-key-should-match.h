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

/**
 * Validates that thrift::Node::name matches the key used in the
 * FlexTopology::nodes map.
 */
class NodeNameAndKeyShouldMatch : public Validation {
 public:
  NodeNameAndKeyShouldMatch()
      : Validation(
            "NodeNameAndKeyShouldMatch",
            "thrift::Node::name should match the key used in the map FlexTopology::nodes.") {
  }

  /**
   * Validates the simulation configuration.
   *
   * @param simCfg          The simulation configuration to validate
   * @param ignoreWarnings  Whether to ignore warnings during validation
   * @return                Validation result if validation fails, std::nullopt
   * otherwise
   */
  std::optional<ValidationResult> validate(
      thrift::Simulation const& simCfg,
      bool const ignoreWarnings) const final;
};
} // namespace packsim::details
