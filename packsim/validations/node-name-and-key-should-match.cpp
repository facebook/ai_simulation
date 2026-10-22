/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/validations/node-name-and-key-should-match.h"

#include <optional>
#include <set>
#include <string>
#include <utility>

#include <fmt/format.h>

namespace packsim::details {

std::optional<ValidationResult> NodeNameAndKeyShouldMatch::validate(
    thrift::Simulation const& simCfg,
    bool const /* ignoreWarnings */) const {
  std::set<std::string> errors;
  for (auto const& [key, node] :
       simCfg.flex_topology().value().nodes().value()) {
    if (key != node.name().value()) {
      errors.emplace(
          fmt::format(
              "The key for the node {0} in the map FlexTopology::nodes is not the same, it is {1}",
              node.name().value(),
              key));
    }
  }

  if (errors.empty()) {
    return std::nullopt;
  }

  return ValidationResult{
      .name = getName(),
      .description = getDescription(),
      .errors = std::move(errors),
      .warnings = {}};
}

} // namespace packsim::details
