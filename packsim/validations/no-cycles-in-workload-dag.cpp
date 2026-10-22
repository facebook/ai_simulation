/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/validations/no-cycles-in-workload-dag.h"

#include <cstddef>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <folly/String.h>
#include <folly/container/F14Set.h>

namespace packsim::details {

namespace {

std::optional<std::vector<std::string>> getCycle(
    workload::thrift::WorkloadDag const& dag,
    std::string const& v,
    folly::F14FastSet<std::string>& done,
    folly::F14FastSet<std::string>& visited) {
  if (done.contains(v) || !dag.workloadDependencies()->contains(v)) {
    done.emplace(v);
    return std::nullopt;
  }
  if (visited.contains(v)) {
    std::vector<std::string> cycle;
    cycle.emplace_back(v);

    return cycle;
  }
  visited.emplace(v);
  for (auto const& neighbour : dag.workloadDependencies()->at(v)) {
    if (auto cycle = getCycle(dag, neighbour, done, visited); cycle) {
      if (cycle->size() == 1 || cycle->back() != cycle->front()) {
        cycle->emplace_back(v);
      }
      return cycle;
    }
  }
  done.emplace(v);
  return std::nullopt;
}

std::optional<std::string> detectCycle(
    size_t const idx,
    workload::thrift::WorkloadDag const& dag) {
  folly::F14FastSet<std::string> done;
  folly::F14FastSet<std::string> visited;

  for (auto const& [vertex, _] : dag.workloadDependencies().value()) {
    if (const auto cycle = getCycle(dag, vertex, done, visited); cycle) {
      const auto suffix = [](size_t const idx) -> char const* {
        switch (idx) {
          case 1:
            return "st";
          case 2:
            return "nd";
          case 3:
            return "rd";
          default:
            return "th";
        }
      };
      return fmt::format(
          "Cycle detected in {0}{1} dag: {2}",
          idx,
          suffix(idx),
          folly::join("->", cycle.value()));
    }
  }
  return std::nullopt;
}

} // namespace

std::optional<ValidationResult> NoCyclesInWorkloadDag::validate(
    thrift::Simulation const& simCfg,
    bool const /* ignoreWarnings */) const {
  if (!simCfg.newWorkloadSchedule()->workloadDags()) {
    return std::nullopt;
  }

  std::set<std::string> errors;

  size_t idx = 0;
  for (auto const& dag : simCfg.newWorkloadSchedule()->workloadDags().value()) {
    if (auto e = detectCycle(idx, dag); e) {
      errors.emplace(std::move(e.value()));
    }
    ++idx;
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
