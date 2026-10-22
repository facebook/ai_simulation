/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/validations/link-node-should-match.h"

#include <set>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <folly/String.h>

namespace packsim::details {

namespace {

void validateLink(
    thrift::Simulation const& simCfg,
    thrift::Node const& node,
    std::vector<std::string> const& scope,
    thrift::LinkId const id,
    std::set<std::string>& errors) {
  const auto pos = simCfg.flex_topology()->links()->find(id);

  if (pos == simCfg.flex_topology()->links()->end()) {
    errors.emplace(
        fmt::format(
            "Invalid reference to link id {0} for the node {1} at {2}",
            id,
            node.name().value(),
            folly::join(".", scope)));
    return;
  }

  if (pos->second.node1().value() == node.name().value() ||
      pos->second.node2().value() == node.name().value()) {
    return;
  }

  errors.emplace(
      fmt::format(
          "The link id {0} for the node {1} at {2} has node1 = {3} and node2 = {4}",
          id,
          node.name().value(),
          folly::join(".", scope),
          pos->second.node1().value(),
          pos->second.node2().value()));
}

void validateLinks(
    thrift::Simulation const& simCfg,
    thrift::Node const& node,
    thrift::LpmTableEntry const& tableEntry,
    std::vector<std::string>& scope,
    std::set<std::string>& errors) {
  scope.emplace_back("links");
  size_t idx = 0;
  for (auto const& id : tableEntry.links().value()) {
    scope.emplace_back(std::to_string(idx++));
    validateLink(simCfg, node, scope, id, errors);
    scope.pop_back();
  }
  scope.pop_back();
}

void validateLinks(
    thrift::Simulation const& simCfg,
    thrift::Node const& node,
    thrift::LpmTableSpec const& table,
    std::vector<std::string>& scope,
    std::set<std::string>& errors) {
  {
    scope.emplace_back("defaultRoute");
    validateLinks(simCfg, node, table.defaultRoute().value(), scope, errors);
    scope.pop_back();
  }

  {
    scope.emplace_back("prefixTable");
    for (auto const& [prefixId, tableEntry] : table.prefixTable().value()) {
      scope.emplace_back(prefixId);

      validateLinks(simCfg, node, tableEntry, scope, errors);
      scope.pop_back();
    }
    scope.pop_back();
  }
}

void validateLinks(
    thrift::Simulation const& simCfg,
    thrift::Node const& node,
    thrift::EndpointConfig const& endpoint,
    std::vector<std::string>& scope,
    std::set<std::string>& errors) {
  {
    scope.emplace_back("linkIds");
    size_t idx = 0;
    for (auto const& id : endpoint.linkIds().value()) {
      scope.emplace_back(std::to_string(idx++));
      validateLink(simCfg, node, scope, id, errors);
      scope.pop_back();
    }
    scope.pop_back();
  }

  {
    scope.emplace_back("lpmSpec");
    validateLinks(simCfg, node, endpoint.lpmSpec().value(), scope, errors);
    scope.pop_back();
  }
}

void validateLinks(
    thrift::Simulation const& simCfg,
    thrift::Node const& node,
    std::set<std::string>& errors) {
  std::vector<std::string> scope;

  {
    scope.emplace_back("lpmSpec");
    validateLinks(simCfg, node, node.lpmSpec().value(), scope, errors);
    scope.pop_back();
  }

  {
    scope.emplace_back("endpointCfgs");
    size_t idx = 0;
    for (auto const& endpoint : node.endpointCfgs().value()) {
      scope.emplace_back(std::to_string(idx++));
      validateLinks(simCfg, node, endpoint, scope, errors);
      scope.pop_back();
    }
    scope.pop_back();
  }
}

} // namespace

std::optional<ValidationResult> LinkNodeShouldMatch::validate(
    thrift::Simulation const& simCfg,
    bool const /* ignoreWarnings */) const {
  std::set<std::string> errors;

  for (auto const& [_, node] : simCfg.flex_topology()->nodes().value()) {
    validateLinks(simCfg, node, errors);
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
