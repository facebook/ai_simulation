/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/validations/nic-config-should-exist.h"

#include <optional>
#include <set>
#include <string>
#include <utility>

#include <fmt/format.h>

#include "packsim/if/gen-cpp2/topology_constants.h"

namespace packsim::details {

NicConfigShouldExist::NicConfigShouldExist()
    : Validation(
          "NicConfigShouldExist",
          fmt::format(
              "thrift::EndpointConfig::nicConfig should be present in the map "
              "Simulation::nicConfigMap, or be \"{}\" when that map is empty "
              "(Simulation.nic supplies it).",
              thrift::topology_constants::kDefaultNicConfig())) {}

std::optional<ValidationResult> NicConfigShouldExist::validate(
    thrift::Simulation const& simCfg,
    bool const ignoreWarnings) const {
  // Track the used nic config, so that we can warn about the unused
  // configurations.
  std::set<std::string> usedNicConfigs;

  // An empty nicConfigMap is valid: building the simulation copies
  // Simulation.nic into it under kDefaultNicConfig (fixNicCfg), after
  // validation runs.
  const auto hasNicConfig = [&simCfg](const std::string& name) {
    return simCfg.nicConfigMap()->contains(name) ||
        (simCfg.nicConfigMap()->empty() &&
         name == thrift::topology_constants::kDefaultNicConfig());
  };

  std::set<std::string> errors;
  for (auto const& [_, node] : simCfg.flex_topology().value().nodes().value()) {
    if (node.type().value() != thrift::NodeType::ACCEL) {
      continue;
    }

    for (auto const& endpoint : node.endpointCfgs().value()) {
      const auto& nicConfig = *endpoint.nicConfig();

      if (!hasNicConfig(nicConfig)) {
        errors.emplace(
            fmt::format(
                "The endpoints for the node {0} references a nic config {1}, which is not present in the Simulation::nicConfigMap",
                node.name().value(),
                nicConfig));
      } else {
        usedNicConfigs.emplace(nicConfig);
      }
    }

    // Use the default if the list is empty.
    if (node.endpointCfgs()->size() == 0) {
      const std::string nicConfig{
          thrift::topology_constants::kDefaultNicConfig()};
      if (!hasNicConfig(nicConfig)) {
        errors.emplace(
            fmt::format(
                "No endpoints for the node {0} present, so using {1} which is not present in the Simulation::nicConfigMap",
                node.name().value(),
                nicConfig));
      } else {
        usedNicConfigs.emplace(nicConfig);
      }
    }
  }

  std::set<std::string> warnings;
  if (!ignoreWarnings &&
      usedNicConfigs.size() != simCfg.nicConfigMap()->size()) {
    for (auto const& [nicConfigName, nicConfig] :
         simCfg.nicConfigMap().value()) {
      if (!usedNicConfigs.contains(nicConfigName)) {
        warnings.emplace(
            fmt::format(
                "The nic config {0} is not used in any endpoints",
                nicConfigName));
      }
    }
  }

  if (errors.empty() && warnings.empty()) {
    return std::nullopt;
  }

  return ValidationResult{
      .name = getName(),
      .description = getDescription(),
      .errors = std::move(errors),
      .warnings = std::move(warnings)};
}

} // namespace packsim::details
